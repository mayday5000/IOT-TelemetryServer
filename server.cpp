// server.cpp
#include <span>
#include <expected>
// Purpose: Edge-triggered epoll reactor that accepts IoT clients,
// reads raw telemetry payloads, and hands them to a worker pool via a
// thread-safe queue. Designed for low CPU idle and clean shutdown.
//
// How it works:
// - Main thread is the reactor: waits on epoll, accepts, reads, enqueues.
// - Worker threads block on a condition variable, process packets, exit on shutdown.
// - SIGINT/SIGTERM set an atomic flag; 500 ms epoll timeout lets the loop notice it.
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
#include <csignal>
#include <chrono>
#include <optional>
#include <span>
#include <expected>
#include <thread>

#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

constexpr size_t MAX_PACKET_SIZE = 128;
constexpr size_t MAX_EVENTS      = 64;
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

struct LargeProcessingBuffer {
    uint8_t scratchpad[32 * 1024]{};
};

class TelemetryPipeline {
private:
    std::queue<std::unique_ptr<TelemetryPacket>> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_requested_{false};
    const size_t max_capacity_ = 10000;

public:
    void push(std::unique_ptr<TelemetryPacket> packet) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.size() >= max_capacity_) {
            queue_.pop(); // drop oldest under back-pressure
        }
        queue_.push(std::move(packet));
        cv_.notify_one();
    }

    std::unique_ptr<TelemetryPacket> pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]{ return !queue_.empty() || stop_requested_; });
        if (stop_requested_ && queue_.empty()) return nullptr;
        auto packet = std::move(queue_.front());
        queue_.pop();
        return packet;
    }

    void shutdown() {
        stop_requested_ = true;
        cv_.notify_all();
    }
};


bool make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

// C++23: expected-based wrapper
std::expected<void, std::string> ensure_non_blocking(int fd) {
    if (!make_socket_non_blocking(fd))
        return std::unexpected("fcntl non-blocking failed");
    return {};
}

std::atomic<bool> g_running{true};

void signal_handler(int) {
    g_running.store(false, std::memory_order_relaxed);
}

int main() {
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    auto pipeline = std::make_shared<TelemetryPipeline>();

    unsigned int cores = std::thread::hardware_concurrency();
    if (cores == 0) cores = 2;
    unsigned int pool_size = cores * 2;
    std::vector<std::jthread> workers;

    std::cout << "[SYSTEM] Starting " << pool_size << " worker threads.\n";

    for (unsigned int i = 0; i < pool_size; ++i) {
        workers.emplace_back([pipeline]() {
            while (g_running.load(std::memory_order_relaxed)) {
                auto packet = pipeline->pop();
                if (!packet) break;

                auto workspace = std::make_unique<LargeProcessingBuffer>();
                std::span<const uint8_t> view(packet->raw_payload, packet->payload_len);
                if (view.size() <= sizeof(workspace->scratchpad)) {
                    std::memcpy(workspace->scratchpad, view.data(), view.size());
                }
                std::cout << "[WORKER] Processed fd=" << packet->device_id
                          << " len=" << packet->payload_len << "\n";
            }
        });
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        std::cerr << "[ERROR] socket() failed\n";
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
        std::cerr << "[ERROR] bind/listen/nonblock failed\n";
        close(server_fd);
        return 1;
    }

    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        close(server_fd);
        return 1;
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
        close(epoll_fd);
        close(server_fd);
        return 1;
    }

    std::cout << "[REACTOR] Listening on port " << SERVER_PORT << " (Ctrl-C to stop)\n";
    std::vector<epoll_event> events(MAX_EVENTS);

    while (g_running.load(std::memory_order_relaxed)) {
        int num_events = epoll_wait(epoll_fd, events.data(), MAX_EVENTS, 500);
        if (num_events == -1) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < num_events; ++i) {
            int current_fd = events[i].data.fd;

            if (current_fd == server_fd) {
                while (true) {
                    int client_fd = accept(server_fd, nullptr, nullptr);
                    if (client_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }
                    if (make_socket_non_blocking(client_fd)) {
                        epoll_event client_ev{};
                        client_ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
                        client_ev.data.fd = client_fd;
                        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_ev);
                        std::cout << "[REACTOR] Accepted fd=" << client_fd << "\n";
                    } else {
                        close(client_fd);
                    }
                }
            } else if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, nullptr);
                close(current_fd);
                std::cout << "[REACTOR] Closed fd=" << current_fd << "\n";
            } else if (events[i].events & EPOLLIN) {
                auto packet = std::make_unique<TelemetryPacket>();
                bool connection_closed = false;

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

    std::cout << "[SYSTEM] Shutting down...\n";
    g_running.store(false, std::memory_order_relaxed);
    pipeline->shutdown();
    // jthread joins on destruction
    close(server_fd);
    close(epoll_fd);
    std::cout << "[SYSTEM] Shutdown complete.\n";
    return 0;
}
