// server.cpp
// Purpose: Edge-triggered epoll reactor that accepts IoT clients,
// reads raw telemetry payloads, and hands them to a worker pool via a
// thread-safe queue. Designed for low CPU idle and clean shutdown.
//
// How it works:
// - Main thread is the reactor: sleeps in epoll_wait (no timeout), accepts, reads, enqueues.
// - Worker threads block on a condition variable, process packets, and drain the queue on shutdown.
// - SIGINT/SIGTERM set an atomic flag and write to an eventfd that wakes the reactor instantly.
// - RAII destructors scrub packet memory.

#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <csignal>
#include <chrono>
#include <span>

#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

constexpr size_t MAX_PACKET_SIZE = 128;
constexpr size_t MAX_EVENTS      = 64;
constexpr size_t MAX_QUEUE_SIZE  = 10000;
constexpr int    SERVER_PORT     = 8883;

struct TelemetryPacket {
    int32_t  device_id = -1;
    uint64_t timestamp_ns = 0;
    uint8_t  raw_payload[MAX_PACKET_SIZE]{};
    size_t   payload_len = 0;

    // RAII: scrub sensitive buffer on destruction
    ~TelemetryPacket() {
        device_id = -1;
        timestamp_ns = 0;
        payload_len = 0;
        std::memset(raw_payload, 0, MAX_PACKET_SIZE);
    }
};

class TelemetryPipeline {
private:
    std::queue<std::unique_ptr<TelemetryPacket>> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_requested_ = false; // Guarded by mutex_

public:
    void push(std::unique_ptr<TelemetryPacket> packet) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.size() >= MAX_QUEUE_SIZE) {
            queue_.pop(); // Drop oldest when full (bounded memory)
        }
        queue_.push(std::move(packet));
        cv_.notify_one();
    }

    // Returns queued packets until the queue is empty; nullptr only once shutdown
    // was requested AND every queued packet has been handed out (drain on exit).
    std::unique_ptr<TelemetryPacket> pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]{ return !queue_.empty() || stop_requested_; });
        if (queue_.empty()) return nullptr;
        auto packet = std::move(queue_.front());
        queue_.pop();
        return packet;
    }

    // Returns how many packets were still queued; workers drain them before exiting
    size_t shutdown() {
        size_t pending;
        {
            std::lock_guard<std::mutex> lock(mutex_); // Set under the lock so no worker misses the wake-up
            stop_requested_ = true;
            pending = queue_.size();
        }
        cv_.notify_all();
        return pending;
    }
};

bool make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

std::atomic<bool> g_running{true};

// Shutdown "doorbell": eventfd watched by epoll so the reactor can sleep with no timeout
int g_wake_fd = -1;

void signal_handler(int) {
    int saved_errno = errno;
    g_running.store(false, std::memory_order_relaxed);
    uint64_t one = 1;
    if (g_wake_fd != -1) (void)!write(g_wake_fd, &one, sizeof(one)); // async-signal-safe wake
    errno = saved_errno;
}

int main() {
    // Create the shutdown eventfd before handlers can fire
    g_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_wake_fd == -1) {
        std::cerr << "[ERROR] eventfd() failed\n";
        return 1;
    }

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Network setup happens before any worker exists, so every error path can simply return
    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (server_fd == -1) {
        std::cerr << "[ERROR] socket() failed\n";
        close(g_wake_fd);
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(SERVER_PORT);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
        listen(server_fd, SOMAXCONN) < 0 ||
        !make_socket_non_blocking(server_fd)) {
        std::cerr << "[ERROR] bind/listen/nonblock failed on port " << SERVER_PORT
                  << ": " << std::strerror(errno) << "\n";
        close(server_fd);
        close(g_wake_fd);
        return 1;
    }

    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd == -1) {
        close(server_fd);
        close(g_wake_fd);
        return 1;
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd;
    epoll_event wake_ev{};
    wake_ev.events = EPOLLIN;
    wake_ev.data.fd = g_wake_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1 ||
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_wake_fd, &wake_ev) == -1) {
        close(epoll_fd);
        close(server_fd);
        close(g_wake_fd);
        return 1;
    }

    // Spare descriptor released on EMFILE so a pending connection can be accepted and closed
    int spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);

    auto pipeline = std::make_shared<TelemetryPipeline>();

    unsigned int cores = std::thread::hardware_concurrency();
    if (cores == 0) cores = 2;
    unsigned int pool_size = cores * 2;
    std::vector<std::jthread> workers;

    std::cout << "[SYSTEM] Starting " << pool_size << " worker threads.\n";

    for (unsigned int i = 0; i < pool_size; ++i) {
        workers.emplace_back([pipeline]() {
            // Keep consuming until shutdown is requested and the queue is fully drained
            while (auto packet = pipeline->pop()) {
                std::span<const uint8_t> view(packet->raw_payload, packet->payload_len);
                std::cout << "[WORKER] Processed fd=" << packet->device_id
                          << " len=" << view.size() << "\n";
            }
        });
    }

    std::cout << "[REACTOR] Listening on port " << SERVER_PORT << " (Ctrl-C to stop)\n";
    std::vector<epoll_event> events(MAX_EVENTS);

    while (g_running.load(std::memory_order_relaxed)) {
        int num_events = epoll_wait(epoll_fd, events.data(), MAX_EVENTS, -1); // Sleep until I/O or shutdown
        if (num_events == -1) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < num_events; ++i) {
            int current_fd = events[i].data.fd;

            if (current_fd == g_wake_fd) {
                uint64_t counter = 0;
                (void)!read(g_wake_fd, &counter, sizeof(counter)); // Drain the doorbell
                g_running.store(false, std::memory_order_relaxed);
                break;
            }

            if (current_fd == server_fd) {
                while (true) { // Edge-triggered: drain every pending connection
                    int client_fd = accept4(server_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (client_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        if (errno == EINTR || errno == ECONNABORTED) continue;
                        if ((errno == EMFILE || errno == ENFILE) && spare_fd != -1) {
                            // Out of descriptors: free the spare, accept and close the connection,
                            // so it does not sit in the backlog with no further edge to wake us.
                            // (Linux reports EMFILE even when the backlog is empty, so stop on EAGAIN.)
                            close(spare_fd);
                            int rejected = accept(server_fd, nullptr, nullptr);
                            if (rejected != -1) close(rejected);
                            spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
                            if (rejected == -1) break;
                            std::cerr << "[REACTOR] Out of file descriptors; rejected a connection.\n";
                            continue;
                        }
                        std::cerr << "[REACTOR] accept() failed: " << std::strerror(errno) << "\n";
                        break;
                    }
                    epoll_event client_ev{};
                    client_ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
                    client_ev.data.fd = client_fd;
                    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_ev) == -1) {
                        close(client_fd);
                        continue;
                    }
                    std::cout << "[REACTOR] Accepted fd=" << client_fd << "\n";
                }
            } else {
                // Read first: a client that sends data and then closes delivers EPOLLIN together with
                // EPOLLRDHUP/EPOLLHUP, and that final data must be drained before the socket is closed.
                auto packet = std::make_unique<TelemetryPacket>();
                bool connection_closed = (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0;

                while (true) {
                    ssize_t bytes_read = read(current_fd, packet->raw_payload, MAX_PACKET_SIZE);
                    if (bytes_read > 0) {
                        packet->payload_len = static_cast<size_t>(bytes_read);
                        packet->timestamp_ns = static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now().time_since_epoch()).count());
                        packet->device_id = current_fd;
                        pipeline->push(std::move(packet));
                        packet = std::make_unique<TelemetryPacket>();
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

                if (connection_closed) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, nullptr);
                    close(current_fd);
                    std::cout << "[REACTOR] Client disconnected fd=" << current_fd << "\n";
                }
            }
        }
    }

    // Reactor has stopped reading; let workers finish everything already queued
    g_running.store(false, std::memory_order_relaxed);
    size_t pending = pipeline->shutdown();
    std::cout << "[SYSTEM] Shutting down. Draining " << pending << " queued packet(s)...\n";
    workers.clear(); // jthreads join here once the queue is empty
    if (spare_fd != -1) close(spare_fd);
    close(server_fd);
    close(epoll_fd);
    close(g_wake_fd);
    std::cout << "[SYSTEM] Shutdown complete.\n";
    return 0;
}
