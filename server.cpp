// server.cpp
// Purpose: Edge-triggered epoll reactor that accepts IoT clients,
// reads raw telemetry payloads, and hands them to a worker pool via a
// thread-safe queue. Designed for low CPU idle and clean shutdown.
//
// How it works:
// - Main thread is the reactor: sleeps in epoll_wait (no timeout), accepts, reads, and splits each
//   connection's byte stream into '\n'-terminated lines (one packet per line, max 128 bytes of content; CRLF accepted).
// - Worker threads block on a condition variable, validate "temperature,humidity" lines
//   (parse_reading returns std::expected<Reading, ParseError>), and drain the queue on shutdown.
// - SIGINT/SIGTERM set an atomic flag and write to an eventfd that wakes the reactor instantly.
// - Every descriptor is a UniqueFd, so early returns and shutdown close everything, including clients.
// - Packet payloads are scrubbed with explicit_bzero when the packet is destroyed.

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <memory>
#include <print>
#include <span>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "line_framer.hpp"
#include "net_util.hpp"
#include "pipeline.hpp"
#include "reading.hpp"

constexpr size_t   READ_CHUNK_SIZE = 4096;
constexpr int      MAX_EVENTS      = 64;
constexpr uint16_t SERVER_PORT     = 8883;

std::atomic<bool> g_running{true};

// Line statistics, reported at shutdown
std::atomic<uint64_t> g_valid_lines{0}, g_invalid_lines{0}, g_oversize_disconnects{0}, g_unterminated_discarded{0};

// Plain int copy of the wake eventfd for the signal handler (owned by a UniqueFd in main)
int g_wake_fd = -1;

void signal_handler(int) {
    int saved_errno = errno;
    g_running.store(false, std::memory_order_relaxed);
    uint64_t one = 1;
    if (g_wake_fd != -1) (void)!write(g_wake_fd, &one, sizeof(one)); // async-signal-safe wake
    errno = saved_errno;
}

// One accepted client: its socket and the bytes of a line whose '\n' has not arrived yet
struct Connection {
    UniqueFd fd;
    LineFramer framer;
};

[[nodiscard]] UniqueFd open_spare_fd() { return UniqueFd{open("/dev/null", O_RDONLY | O_CLOEXEC)}; }

[[nodiscard]] uint64_t now_ns() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

void worker_loop(TelemetryPipeline& pipeline) {
    // Keep consuming until shutdown is requested and the queue is fully drained
    while (auto packet = pipeline.pop()) {
        if (auto reading = parse_reading(packet->payload())) {
            g_valid_lines.fetch_add(1, std::memory_order_relaxed);
            std::println("[WORKER] fd={} temperature={:.2f} C humidity={} %",
                         packet->device_id, reading->temperature_c, reading->humidity_pct);
        } else {
            g_invalid_lines.fetch_add(1, std::memory_order_relaxed);
            std::println("[WORKER] Rejected line from fd={} (len={}): {}",
                         packet->device_id, packet->payload_len, to_string(reading.error()));
        }
    }
}

int main() {
    // Network setup happens before any worker exists, so every error path can simply return;
    // UniqueFd closes whatever was already opened.
    auto wake = make_eventfd();
    if (!wake) { std::cerr << "[ERROR] " << wake.error() << "\n"; return 1; }
    g_wake_fd = wake->get(); // Set before the handlers can fire

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    auto listener = create_listener(SERVER_PORT);
    if (!listener) {
        std::cerr << "[ERROR] Port " << SERVER_PORT << ": " << listener.error() << "\n";
        return 1;
    }
    auto epoll = make_epoll();
    if (!epoll) { std::cerr << "[ERROR] " << epoll.error() << "\n"; return 1; }

    const int epoll_fd  = epoll->get();
    const int server_fd = listener->get();
    if (auto r = epoll_add(epoll_fd, server_fd, EPOLLIN | EPOLLET); !r) {
        std::cerr << "[ERROR] " << r.error() << "\n";
        return 1;
    }
    if (auto r = epoll_add(epoll_fd, g_wake_fd, EPOLLIN); !r) {
        std::cerr << "[ERROR] " << r.error() << "\n";
        return 1;
    }

    // Spare descriptor released on EMFILE so a pending connection can be accepted and closed
    UniqueFd spare_fd = open_spare_fd();

    TelemetryPipeline pipeline;

    unsigned int cores = std::thread::hardware_concurrency();
    if (cores == 0) cores = 2;
    const unsigned int pool_size = cores * 2;
    std::vector<std::jthread> workers;
    std::cout << "[SYSTEM] Starting " << pool_size << " worker threads.\n";
    for (unsigned int i = 0; i < pool_size; ++i)
        workers.emplace_back(worker_loop, std::ref(pipeline));

    std::cout << "[REACTOR] Listening on port " << SERVER_PORT << " (Ctrl-C to stop)\n";
    std::array<epoll_event, MAX_EVENTS> events{};
    std::array<char, READ_CHUNK_SIZE> chunk; // Reused for every read; packets are allocated only per complete line
    std::unordered_map<int, Connection> connections;

    while (g_running.load(std::memory_order_relaxed)) {
        int num_events = epoll_wait(epoll_fd, events.data(), MAX_EVENTS, -1); // Sleep until I/O or shutdown
        if (num_events == -1) {
            if (errno == EINTR) continue;
            std::cerr << "[REACTOR] epoll_wait() failed: " << std::strerror(errno) << "\n";
            break;
        }

        for (const epoll_event& ev : std::span(events).first(static_cast<size_t>(num_events))) {
            const int current_fd = ev.data.fd;

            if (current_fd == g_wake_fd) {
                uint64_t counter = 0;
                (void)!read(g_wake_fd, &counter, sizeof(counter)); // Drain the doorbell
                g_running.store(false, std::memory_order_relaxed);
                break;
            }

            if (current_fd == server_fd) {
                while (true) { // Edge-triggered: drain every pending connection
                    UniqueFd client{accept4(server_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC)};
                    if (!client) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        if (errno == EINTR || errno == ECONNABORTED) continue;
                        if ((errno == EMFILE || errno == ENFILE) && spare_fd) {
                            // Out of descriptors: free the spare, accept and close the connection,
                            // so it does not sit in the backlog with no further edge to wake us.
                            // (Linux reports EMFILE even when the backlog is empty, so stop on EAGAIN.)
                            spare_fd.reset();
                            UniqueFd rejected{accept4(server_fd, nullptr, nullptr, SOCK_CLOEXEC)};
                            const bool got_one = static_cast<bool>(rejected);
                            rejected.reset();
                            spare_fd = open_spare_fd();
                            if (!got_one) break;
                            std::cerr << "[REACTOR] Out of file descriptors; rejected a connection.\n";
                            continue;
                        }
                        std::cerr << "[REACTOR] accept() failed: " << std::strerror(errno) << "\n";
                        break;
                    }
                    const int client_fd = client.get();
                    if (auto r = epoll_add(epoll_fd, client_fd, EPOLLIN | EPOLLET | EPOLLRDHUP); !r) {
                        std::cerr << "[REACTOR] " << r.error() << "; dropping fd=" << client_fd << "\n";
                        continue; // UniqueFd closes it
                    }
                    connections.try_emplace(client_fd, Connection{std::move(client), {}});
                    std::cout << "[REACTOR] Accepted fd=" << client_fd << "\n";
                }
                continue;
            }

            auto it = connections.find(current_fd);
            if (it == connections.end()) continue;
            LineFramer& framer = it->second.framer;

            // Read first: a client that sends data and then closes delivers EPOLLIN together with
            // EPOLLRDHUP/EPOLLHUP, and that final data must be drained before the socket is closed.
            bool connection_closed = (ev.events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0;
            while (!framer.oversize()) {
                ssize_t bytes_read = read(current_fd, chunk.data(), chunk.size());
                if (bytes_read > 0) {
                    // Split the stream on '\n': one packet per complete line (CRLF accepted)
                    const std::string_view data(chunk.data(), static_cast<size_t>(bytes_read));
                    const auto ts = now_ns();
                    auto st = framer.feed(data, [&](std::string_view line) {
                        pipeline.push(make_packet(current_fd, ts, line));
                    });
                    if (st == LineFramer::Status::Oversize) break;
                } else if (bytes_read == 0) {
                    connection_closed = true;
                    break;
                } else {
                    if (errno == EINTR) continue;
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    connection_closed = true;
                    break;
                }
            }

            if (framer.oversize()) {
                // Policy: a line longer than MAX_PACKET_SIZE means a broken or hostile sender;
                // the stream cannot be trusted to resynchronise, so the client is disconnected.
                g_oversize_disconnects.fetch_add(1, std::memory_order_relaxed);
                std::cerr << "[REACTOR] Line longer than " << MAX_PACKET_SIZE
                          << " bytes from fd=" << current_fd << "; disconnecting.\n";
                connection_closed = true;
            }

            if (connection_closed) {
                if (!framer.pending().empty()) {
                    // A final line without '\n' may be truncated, so it is not processed
                    g_unterminated_discarded.fetch_add(1, std::memory_order_relaxed);
                    std::cerr << "[REACTOR] Discarded " << framer.pending().size()
                              << "-byte unterminated line from fd=" << current_fd << "\n";
                }
                connections.erase(it); // UniqueFd closes the socket, which also removes it from epoll
                std::cout << "[REACTOR] Client disconnected fd=" << current_fd << "\n";
            }
        }
    }

    // Reactor has stopped reading; let workers finish everything already queued
    g_running.store(false, std::memory_order_relaxed);
    std::cout << "[SYSTEM] Closing " << connections.size() << " client connection(s).\n";
    connections.clear();
    const size_t pending = pipeline.shutdown();
    std::cout << "[SYSTEM] Shutting down. Draining " << pending << " queued packet(s)...\n";
    workers.clear(); // jthreads join here once the queue is empty
    std::cout << "[SYSTEM] Lines: valid=" << g_valid_lines << " invalid=" << g_invalid_lines
              << " oversize_disconnects=" << g_oversize_disconnects
              << " unterminated_discarded=" << g_unterminated_discarded
              << " dropped_queue_full=" << pipeline.dropped() << "\n";
    std::cout << "[SYSTEM] Shutdown complete.\n";
    return 0;
}
