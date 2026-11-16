// client.cpp
// Purpose: Simple IoT edge client that connects to the telemetry server
// and transmits random sensor readings at a regular interval.
//
// How it works:
// - Connects once, then sends lines of the form "temperature,humidity\n".
// - Between sends it sleeps in poll() on an eventfd and the socket, with the interval as timeout:
//   one wake-up per reading, instant Ctrl-C, and immediate notice if the server closes.
// - SIGINT/SIGTERM set an atomic flag and write to the eventfd (async-signal-safe).
// - Writes use MSG_NOSIGNAL and SIGPIPE is ignored, so a reset server never kills the client.

#include <iostream>
#include <string>
#include <string_view>
#include <atomic>
#include <chrono>
#include <random>
#include <format>
#include <charconv>
#include <cerrno>
#include <cstring>
#include <csignal>
#include <cstdint>
#include <expected>

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

constexpr int SERVER_PORT = 8883;
constexpr int DEFAULT_INTERVAL_MS = 1000;

std::atomic<bool> g_client_active{true};

// Shutdown "doorbell": eventfd polled together with the socket
int g_wake_fd = -1;

void signal_handler(int) {
    int saved_errno = errno;
    g_client_active.store(false, std::memory_order_relaxed);
    uint64_t one = 1;
    if (g_wake_fd != -1) (void)!write(g_wake_fd, &one, sizeof(one)); // async-signal-safe wake
    errno = saved_errno;
}

enum class IoResult { Ok, Stopped, PeerGone };

// Waits for `fd` to report `events`, or for the shutdown eventfd. timeout_ms = -1 waits forever.
// Returns Ok when fd is ready, Stopped on shutdown, PeerGone on poll failure.
IoResult wait_ready(int fd, short events, int timeout_ms, bool* timed_out = nullptr) {
    pollfd pfds[2] = {{g_wake_fd, POLLIN, 0}, {fd, events, 0}};
    while (true) {
        int n = poll(pfds, 2, timeout_ms);
        if (n == -1) {
            if (errno == EINTR) continue;
            return IoResult::PeerGone;
        }
        if (timed_out) *timed_out = (n == 0);
        if (pfds[0].revents & POLLIN) return IoResult::Stopped;
        return IoResult::Ok;
    }
}

// Non-blocking connect that can be interrupted by Ctrl-C
std::expected<int, std::string> try_connect(const std::string& ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd == -1) return std::unexpected("socket failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return std::unexpected("bad address: " + ip);
    }
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) {
            std::string err = std::strerror(errno);
            close(fd);
            return std::unexpected("connect failed: " + err);
        }
        if (wait_ready(fd, POLLOUT, -1) != IoResult::Ok) {
            close(fd);
            return std::unexpected("connect interrupted");
        }
        int so_error = 0;
        socklen_t len = sizeof(so_error);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
        if (so_error != 0) {
            close(fd);
            return std::unexpected(std::string("connect failed: ") + std::strerror(so_error));
        }
    }
    return fd;
}

// Sends the whole buffer, handling partial sends and a full socket buffer
IoResult send_all(int fd, std::string_view data) {
    while (!data.empty()) {
        ssize_t n = send(fd, data.data(), data.size(), MSG_NOSIGNAL);
        if (n > 0) {
            data.remove_prefix(static_cast<size_t>(n));
        } else if (n == -1 && errno == EINTR) {
            continue;
        } else if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            IoResult r = wait_ready(fd, POLLOUT, -1);
            if (r != IoResult::Ok) return r;
        } else {
            return IoResult::PeerGone; // EPIPE, ECONNRESET, ...
        }
    }
    return IoResult::Ok;
}

// Returns false when the server has closed the connection (EOF or error)
bool drain_incoming(int fd) {
    char buf[256];
    while (true) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n > 0) continue;                       // The server does not send data; discard
        if (n == 0) return false;                  // Orderly close
        if (errno == EINTR) continue;
        return errno == EAGAIN || errno == EWOULDBLOCK;
    }
}

int main(int argc, char* argv[]) {
    std::string server_ip = (argc > 1) ? argv[1] : "127.0.0.1";
    int interval_ms = DEFAULT_INTERVAL_MS;
    if (argc > 2) {
        std::string_view arg = argv[2];
        auto [ptr, ec] = std::from_chars(arg.data(), arg.data() + arg.size(), interval_ms);
        if (ec != std::errc{} || ptr != arg.data() + arg.size() || interval_ms < 1) {
            std::cerr << "[CLIENT] Invalid interval '" << arg << "': expected an integer >= 1 (milliseconds).\n"
                      << "Usage: " << argv[0] << " [server_ip] [interval_ms]\n";
            return 2;
        }
    }

    g_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_wake_fd == -1) {
        std::cerr << "[CLIENT] eventfd failed\n";
        return 1;
    }

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Belt and braces with MSG_NOSIGNAL: writing to a reset server must never kill the client
    struct sigaction ignore_pipe{};
    ignore_pipe.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ignore_pipe, nullptr);

    std::cout << "[CLIENT] Connecting to " << server_ip << ":" << SERVER_PORT << "...\n";
    auto conn = try_connect(server_ip, SERVER_PORT);
    if (!conn) {
        std::cerr << "[CLIENT] " << conn.error() << "\n";
        close(g_wake_fd);
        return g_client_active.load() ? 1 : 0;
    }
    int sock_fd = *conn;
    std::cout << "[CLIENT] Connected. Interval=" << interval_ms << " ms. Ctrl-C to stop.\n";

    std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<float> temp_dist(18.0f, 32.0f);
    std::uniform_int_distribution<int> humidity_dist(30, 80);

    int exit_code = 0;
    auto next_send = std::chrono::steady_clock::now();

    while (g_client_active.load(std::memory_order_relaxed)) {
        auto now = std::chrono::steady_clock::now();
        if (now >= next_send) {
            std::string payload = std::format("{:.2f},{}\n", temp_dist(rng), humidity_dist(rng));
            IoResult r = send_all(sock_fd, payload);
            if (r == IoResult::Stopped) break;
            if (r == IoResult::PeerGone) {
                std::cerr << "[CLIENT] Connection lost (" << std::strerror(errno) << "). Stopping.\n";
                exit_code = 1;
                break;
            }
            std::cout << "[CLIENT] Sent: " << payload;
            next_send += std::chrono::milliseconds(interval_ms);
            if (next_send < now) next_send = now + std::chrono::milliseconds(interval_ms); // Don't burst after a stall
            continue;
        }

        // Sleep until the next reading is due, Ctrl-C, or the server closing the connection
        auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(next_send - now).count() + 1;
        bool timed_out = false;
        IoResult r = wait_ready(sock_fd, POLLIN | POLLRDHUP, static_cast<int>(wait_ms), &timed_out);
        if (r == IoResult::Stopped) break;
        if (r == IoResult::PeerGone || (!timed_out && !drain_incoming(sock_fd))) {
            std::cerr << "[CLIENT] Server closed the connection. Stopping.\n";
            exit_code = 1;
            break;
        }
    }

    close(sock_fd);
    close(g_wake_fd);
    std::cout << "[CLIENT] Shutdown complete.\n";
    return exit_code;
}
