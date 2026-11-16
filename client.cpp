// client.cpp
// Purpose: Simple IoT edge client that connects to the telemetry server
// and transmits random sensor readings at a regular interval.
//
// How it works:
// - Connects (blocking once), then sends lines of the form "temperature,humidity\n"
// - Sleeps in short slices so SIGINT is observed quickly.
// - Global atomic flag set by signal handler controls the send loop.

#include <iostream>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <csignal>
#include <cstdint>
#include <span>
#include <expected>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

constexpr int SERVER_PORT = 8883;
constexpr int DEFAULT_INTERVAL_MS = 1000;

std::atomic<bool> g_client_active{true};

void signal_handler(int) {
    g_client_active.store(false, std::memory_order_relaxed);
}


std::expected<int, std::string> try_connect(const std::string& ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) return std::unexpected("socket failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return std::unexpected("bad address");
    }
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return std::unexpected("connect failed");
    }
    return fd;
}

int main(int argc, char* argv[]) {
    std::string server_ip = (argc > 1) ? argv[1] : "127.0.0.1";
    int interval_ms = (argc > 2) ? std::stoi(argv[2]) : DEFAULT_INTERVAL_MS;

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    auto conn = try_connect(server_ip, SERVER_PORT);
    if (!conn) {
        std::cerr << "[CLIENT] " << conn.error() << "\n";
        return 1;
    }
    int sock_fd = *conn;
    std::cout << "[CLIENT] Connecting to " << server_ip << ":" << SERVER_PORT << "...\n";
    std::cout << "[CLIENT] Connected. Interval=" << interval_ms << " ms. Ctrl-C to stop.\n";

    std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<float> temp_dist(18.0f, 32.0f);
    std::uniform_int_distribution<int> humidity_dist(30, 80);

    while (g_client_active.load(std::memory_order_relaxed)) {
        float temp = temp_dist(rng);
        int humidity = humidity_dist(rng);
        std::string payload = std::to_string(temp) + "," + std::to_string(humidity) + "\n";

        if (send(sock_fd, payload.data(), payload.size(), 0) < 0) {
            std::cerr << "[CLIENT] send failed\n";
            break;
        }
        std::cout << "[CLIENT] Sent: " << payload;

        for (int slept = 0; slept < interval_ms && g_client_active.load(std::memory_order_relaxed); slept += 100) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    close(sock_fd);
    std::cout << "[CLIENT] Shutdown complete.\n";
    return 0;
}
