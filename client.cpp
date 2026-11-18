// client.cpp
// Purpose: IoT edge client that connects to the telemetry server and transmits
// random sensor readings at a regular interval, reconnecting by itself when the
// server is down or the connection drops.
//
// How it works:
// - Connects (non-blocking, 5 s timeout), then sends lines of the form "temperature,humidity\n".
//   Nothing is sent until the connection is established.
// - Between sends it sleeps in poll() on an eventfd and the socket, with the interval as timeout:
//   one wake-up per reading, instant Ctrl-C, and immediate notice if the server closes.
// - On a failed connect or a dropped connection it prints the reason and retries with
//   exponential backoff (1 s, 2 s, 4 s ... capped at 30 s, reset after a successful connect).
//   The backoff wait also polls the eventfd, so Ctrl-C exits at once.
// - SIGINT/SIGTERM set an atomic flag and write to the eventfd (async-signal-safe).
// - Writes use MSG_NOSIGNAL and SIGPIPE is ignored, so a reset server never kills the client.

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <expected>
#include <format>
#include <iostream>
#include <random>
#include <span>
#include <string_view>

#include <unistd.h>
#include <poll.h>
#include <sys/socket.h>

#include "net_util.hpp"
#include "reading.hpp"

using namespace std::chrono_literals;

constexpr uint16_t SERVER_PORT = 8883;
constexpr int DEFAULT_INTERVAL_MS = 1000;
constexpr auto CONNECT_TIMEOUT = 5s;

std::atomic<bool> g_client_active{true};

// Plain int copy of the wake eventfd for the signal handler (owned by a UniqueFd in main)
int g_wake_fd = -1;

void signal_handler(int) {
    int saved_errno = errno;
    g_client_active.store(false, std::memory_order_relaxed);
    uint64_t one = 1;
    if (g_wake_fd != -1) (void)!write(g_wake_fd, &one, sizeof(one)); // async-signal-safe wake
    errno = saved_errno;
}

// Reads and discards anything the server sent (it normally sends nothing).
// Fails when the server has closed the connection (EOF or error).
[[nodiscard]] std::expected<void, SysError> drain_incoming(int fd) {
    std::array<char, 256> buf;
    while (true) {
        ssize_t n = recv(fd, buf.data(), buf.size(), 0);
        if (n > 0) continue;
        if (n == 0) return std::unexpected(SysError{Op::PeerClosed}); // Orderly close
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return {};
        return std::unexpected(SysError{Op::Send, errno});           // ECONNRESET, ...
    }
}

// Streams readings on a connected socket until shutdown (Interrupted) or the connection drops
[[nodiscard]] std::expected<void, SysError> run_session(int sock_fd, std::chrono::milliseconds interval) {
    static std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<float> temp_dist(18.0f, 32.0f);
    std::uniform_int_distribution<int> humidity_dist(30, 80);

    auto next_send = std::chrono::steady_clock::now();
    while (g_client_active.load(std::memory_order_relaxed)) {
        auto now = std::chrono::steady_clock::now();
        if (now >= next_send) {
            std::array<char, 32> buf; // "-40.00,100\n" fits easily; no heap per reading
            auto out = std::format_to_n(buf.data(), buf.size(), "{:.2f},{}\n", temp_dist(rng), humidity_dist(rng));
            std::span<const char> payload(buf.data(), out.out);
            if (auto r = send_all(sock_fd, payload, g_wake_fd); !r) return r;
            std::cout << "[CLIENT] Sent: " << std::string_view(payload.data(), payload.size());
            next_send += interval;
            if (next_send < now) next_send = now + interval; // Don't burst after a stall
            continue;
        }

        // Sleep until the next reading is due, Ctrl-C, or the server closing the connection
        auto wait = std::chrono::ceil<std::chrono::milliseconds>(next_send - now);
        switch (wait_fd(sock_fd, POLLIN | POLLRDHUP, g_wake_fd, wait)) {
            case Wait::Woken:   return std::unexpected(SysError{Op::Interrupted});
            case Wait::Timeout: break;
            case Wait::Error:   return std::unexpected(SysError{Op::Send, errno});
            case Wait::Ready:
                if (auto r = drain_incoming(sock_fd); !r) return r;
                break;
        }
    }
    return std::unexpected(SysError{Op::Interrupted});
}

int main(int argc, char* argv[]) {
    std::span<char*> args(argv, static_cast<size_t>(argc));
    auto usage = [&] { std::cerr << "Usage: " << args[0] << " [server_ip] [interval_ms]\n"; };

    // argv entries and the literal are NUL-terminated, so .data() is safe for inet_pton
    const std::string_view server_ip = (args.size() > 1) ? args[1] : "127.0.0.1";
    auto addr = parse_ipv4(server_ip.data(), SERVER_PORT);
    if (!addr) {
        std::cerr << "[CLIENT] '" << server_ip << "': " << addr.error() << "\n";
        usage();
        return 2; // Configuration error: retrying cannot fix it
    }

    int interval_ms = DEFAULT_INTERVAL_MS;
    if (args.size() > 2) {
        auto parsed = parse_interval_ms(args[2]);
        if (!parsed) {
            std::cerr << "[CLIENT] Invalid interval '" << args[2] << "': " << parsed.error() << ".\n";
            usage();
            return 2;
        }
        interval_ms = *parsed;
    }

    auto wake = make_eventfd();
    if (!wake) { std::cerr << "[CLIENT] " << wake.error() << "\n"; return 1; }
    g_wake_fd = wake->get();

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Belt and braces with MSG_NOSIGNAL: writing to a reset server must never kill the client
    struct sigaction ignore_pipe{};
    ignore_pipe.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ignore_pipe, nullptr);

    Backoff backoff;
    while (g_client_active.load(std::memory_order_relaxed)) {
        std::cout << "[CLIENT] Connecting to " << server_ip << ":" << SERVER_PORT << "...\n";
        auto sock = connect_to(*addr, g_wake_fd, CONNECT_TIMEOUT);
        SysError err{Op::Interrupted};
        if (sock) {
            backoff.reset();
            std::cout << "[CLIENT] Connected. Interval=" << interval_ms << " ms. Ctrl-C to stop.\n";
            auto session = run_session(sock->get(), std::chrono::milliseconds(interval_ms));
            sock->reset(); // Close before waiting to reconnect
            if (!session) err = session.error();
        } else {
            err = sock.error();
        }
        if (err.op == Op::Interrupted) break;

        const auto delay = backoff.next();
        std::cerr << "[CLIENT] " << err << ". Retrying in "
                  << std::chrono::duration_cast<std::chrono::seconds>(delay).count() << " s.\n";
        if (wait_fd(-1, 0, g_wake_fd, delay) == Wait::Woken) break; // Ctrl-C during backoff
    }

    std::cout << "[CLIENT] Shutdown complete.\n";
    return 0;
}
