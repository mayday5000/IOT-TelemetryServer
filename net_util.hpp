// net_util.hpp
// Purpose: Small Linux networking toolkit shared by server.cpp, client.cpp and tests.cpp:
// UniqueFd (RAII file descriptor), SysError (allocation-free error value),
// [[nodiscard]] std::expected helpers for sockets, epoll, eventfd and poll waits,
// span-based send_all and the client's reconnect Backoff.
#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <ostream>
#include <span>
#include <utility>

#include <unistd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

// ============================================================================
// UniqueFd: owns one file descriptor and closes it exactly once (move-only)
// ============================================================================
class UniqueFd {
    int fd_ = -1;

public:
    UniqueFd() = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}
    UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) reset(std::exchange(other.fd_, -1));
        return *this;
    }
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    ~UniqueFd() { reset(); }

    [[nodiscard]] int get() const noexcept { return fd_; }
    [[nodiscard]] explicit operator bool() const noexcept { return fd_ != -1; }

    void reset(int fd = -1) noexcept {
        if (fd_ != -1) ::close(fd_);
        fd_ = fd;
    }
};

// ============================================================================
// SysError: which step failed plus the saved errno. Two small integers, no heap.
// ============================================================================
enum class Op : uint8_t {
    Socket, SetSockOpt, Bind, Listen, EpollCreate, EpollCtl, EventFd, // setup
    BadAddress, Connect, ConnectTimeout, Send, PeerClosed, Interrupted // client
};

struct SysError {
    Op  op;
    int code = 0; // errno (0 when not applicable)
};

inline std::ostream& operator<<(std::ostream& os, const SysError& e) {
    auto sys = [&](const char* what) -> std::ostream& { return os << what << ": " << std::strerror(e.code); };
    switch (e.op) {
        case Op::Socket:         return sys("socket() failed");
        case Op::SetSockOpt:     return sys("setsockopt() failed");
        case Op::Bind:           return sys("bind() failed");
        case Op::Listen:         return sys("listen() failed");
        case Op::EpollCreate:    return sys("epoll_create1() failed");
        case Op::EpollCtl:       return sys("epoll_ctl() failed");
        case Op::EventFd:        return sys("eventfd() failed");
        case Op::BadAddress:     return os << "bad IP address (expected IPv4 dotted quad, e.g. 192.168.1.10)";
        case Op::Connect:
            if (e.code == ECONNREFUSED) return os << "connection refused (is the server running?)";
            return sys("connect failed");
        case Op::ConnectTimeout: return os << "connect timeout";
        case Op::Send:           return sys("server closed connection");
        case Op::PeerClosed:     return os << "server closed connection";
        case Op::Interrupted:    return os << "interrupted by shutdown";
    }
    return os << "unknown error";
}

// ============================================================================
// Resource helpers
// ============================================================================
[[nodiscard]] inline std::expected<UniqueFd, SysError> make_epoll() {
    UniqueFd fd{::epoll_create1(EPOLL_CLOEXEC)};
    if (!fd) return std::unexpected(SysError{Op::EpollCreate, errno});
    return fd;
}

[[nodiscard]] inline std::expected<UniqueFd, SysError> make_eventfd() {
    UniqueFd fd{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
    if (!fd) return std::unexpected(SysError{Op::EventFd, errno});
    return fd;
}

[[nodiscard]] inline std::expected<void, SysError> epoll_add(int epoll_fd, int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) return std::unexpected(SysError{Op::EpollCtl, errno});
    return {};
}

// Non-blocking IPv4 listening socket bound to all interfaces
[[nodiscard]] inline std::expected<UniqueFd, SysError> create_listener(uint16_t port) {
    UniqueFd fd{::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    if (!fd) return std::unexpected(SysError{Op::Socket, errno});

    int opt = 1;
    if (::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1)
        return std::unexpected(SysError{Op::SetSockOpt, errno});

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);
    if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) == -1)
        return std::unexpected(SysError{Op::Bind, errno});
    if (::listen(fd.get(), SOMAXCONN) == -1)
        return std::unexpected(SysError{Op::Listen, errno});
    return fd;
}

// ip must be NUL-terminated: inet_pton is a C API
[[nodiscard]] inline std::expected<sockaddr_in, SysError> parse_ipv4(const char* ip, uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, ip, &addr.sin_addr) != 1) return std::unexpected(SysError{Op::BadAddress});
    return addr;
}

// ============================================================================
// Waiting without busy loops: poll() on one fd plus the shutdown eventfd
// ============================================================================
enum class Wait : uint8_t { Ready, Timeout, Woken, Error };

// Sleeps until `fd` reports `events`, `wake_fd` becomes readable (shutdown), or the
// timeout expires (negative timeout = wait forever). A negative `fd` is ignored by
// poll(), which turns this into an interruptible sleep. wake_fd is left undrained.
[[nodiscard]] inline Wait wait_fd(int fd, short events, int wake_fd, std::chrono::milliseconds timeout) {
    const bool forever = timeout.count() < 0;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        int left_ms = -1;
        if (!forever) {
            auto left = std::chrono::ceil<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
            left_ms = static_cast<int>(std::max<std::chrono::milliseconds::rep>(left.count(), 0));
        }
        std::array<pollfd, 2> fds{{{fd, events, 0}, {wake_fd, POLLIN, 0}}};
        int n = ::poll(fds.data(), fds.size(), left_ms);
        if (n == -1) {
            if (errno == EINTR) continue; // The signal handler also rang wake_fd; poll again to see it
            return Wait::Error;
        }
        if (fds[1].revents & POLLIN) return Wait::Woken;
        if (n == 0) return Wait::Timeout;
        if (fds[0].revents) return Wait::Ready; // Includes POLLERR/POLLHUP: the next syscall reports why
    }
}

// Non-blocking TCP connect with a timeout; Ctrl+C (wake_fd) aborts it at once.
// `addr` comes from parse_ipv4, so a bad IP is caught once at startup, not on every retry.
[[nodiscard]] inline std::expected<UniqueFd, SysError> connect_to(const sockaddr_in& addr, int wake_fd,
                                                                std::chrono::milliseconds timeout) {
    UniqueFd fd{::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    if (!fd) return std::unexpected(SysError{Op::Socket, errno});

    if (::connect(fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0) return fd;
    if (errno != EINPROGRESS) return std::unexpected(SysError{Op::Connect, errno});

    switch (wait_fd(fd.get(), POLLOUT, wake_fd, timeout)) {
        case Wait::Woken:   return std::unexpected(SysError{Op::Interrupted});
        case Wait::Timeout: return std::unexpected(SysError{Op::ConnectTimeout});
        case Wait::Error:   return std::unexpected(SysError{Op::Connect, errno});
        case Wait::Ready:   break;
    }
    int so_error = 0;
    socklen_t len = sizeof(so_error);
    if (::getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &so_error, &len) == -1) so_error = errno;
    if (so_error != 0) return std::unexpected(SysError{Op::Connect, so_error});
    return fd;
}

// Sends the whole buffer on a non-blocking socket, handling partial sends and a full
// socket buffer. MSG_NOSIGNAL: a reset peer returns EPIPE instead of raising SIGPIPE.
[[nodiscard]] inline std::expected<void, SysError> send_all(int fd, std::span<const char> data, int wake_fd) {
    while (!data.empty()) {
        ssize_t n = ::send(fd, data.data(), data.size(), MSG_NOSIGNAL);
        if (n > 0) {
            data = data.subspan(static_cast<size_t>(n));
        } else if (n == -1 && errno == EINTR) {
            continue;
        } else if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            Wait w = wait_fd(fd, POLLOUT, wake_fd, std::chrono::milliseconds(-1));
            if (w == Wait::Woken) return std::unexpected(SysError{Op::Interrupted});
            if (w == Wait::Error) return std::unexpected(SysError{Op::Send, errno});
        } else {
            return std::unexpected(SysError{Op::Send, errno}); // EPIPE, ECONNRESET, ...
        }
    }
    return {};
}

// ============================================================================
// Reconnect backoff: 1 s, 2 s, 4 s ... capped at 30 s; reset after a successful connect
// ============================================================================
class Backoff {
public:
    static constexpr std::chrono::milliseconds initial{1000};
    static constexpr std::chrono::milliseconds cap{30000};

    // Returns the delay to wait now and doubles the next one
    [[nodiscard]] std::chrono::milliseconds next() noexcept {
        auto d = delay_;
        delay_ = std::min(delay_ * 2, cap);
        return d;
    }
    void reset() noexcept { delay_ = initial; }

private:
    std::chrono::milliseconds delay_ = initial;
};
