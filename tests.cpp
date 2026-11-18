// tests.cpp
// Purpose: Unit tests for the real code shared with server.cpp and client.cpp:
// LineFramer, parse_reading (every ParseError), parse_interval_ms, TelemetryPipeline,
// UniqueFd and the net_util.hpp helpers (listener, connect, send_all, wait_fd, Backoff).

#include <cmath>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/socket.h>

#include "line_framer.hpp"
#include "net_util.hpp"
#include "pipeline.hpp"
#include "reading.hpp"

using namespace std::chrono_literals;

int passed = 0, failed = 0;
#define CHECK(name, cond) do { if (cond) { std::cout << "[PASS] " << name << "\n"; ++passed; } else { std::cout << "[FAIL] " << name << "\n"; ++failed; } } while(0)

static std::string n_bytes(size_t n, char c = 'A') { return std::string(n, c); }

static bool fd_is_open(int fd) { return fcntl(fd, F_GETFD) != -1; }

static void test_line_framer() {
    {
        LineFramer framer;
        std::vector<std::string> lines;
        auto st = framer.feed(n_bytes(128) + "\n", lines);
        CHECK("framer: 128-byte LF line accepted",
              st == LineFramer::Status::Ok && lines.size() == 1 && lines[0].size() == 128 && !framer.oversize());
    }
    {
        LineFramer framer;
        std::vector<std::string> lines;
        auto st = framer.feed(n_bytes(128) + "\r\n", lines);
        CHECK("framer: 128-byte CRLF line accepted",
              st == LineFramer::Status::Ok && lines.size() == 1 && lines[0] == n_bytes(128));
    }
    {
        LineFramer framer;
        std::vector<std::string> lines;
        auto st = framer.feed(n_bytes(129) + "\n", lines);
        CHECK("framer: 129-byte LF line rejected",
              st == LineFramer::Status::Oversize && lines.empty() && framer.oversize() && framer.pending().empty());
    }
    {
        LineFramer framer;
        std::vector<std::string> lines;
        auto st = framer.feed(n_bytes(129) + "\r\n", lines);
        CHECK("framer: 129-byte CRLF line rejected", st == LineFramer::Status::Oversize && lines.empty());
    }
    {
        LineFramer framer;
        std::vector<std::string> lines;
        auto st1 = framer.feed(n_bytes(128) + "\r", lines);
        CHECK("framer: 128-byte content + CR held as pending",
              st1 == LineFramer::Status::Ok && lines.empty() && framer.pending().size() == 129);
        auto st2 = framer.feed("\n", lines);
        CHECK("framer: CRLF split across reads accepted at 128",
              st2 == LineFramer::Status::Ok && lines.size() == 1 && lines[0].size() == 128 && framer.pending().empty());
    }
    {
        LineFramer framer;
        std::vector<std::string> lines;
        auto st = framer.feed("\n\r\n24.5,55\n\n", lines);
        CHECK("framer: blank lines skipped", st == LineFramer::Status::Ok && lines.size() == 1 && lines[0] == "24.5,55");
    }
    {
        LineFramer framer;
        std::vector<std::string> lines;
        auto st1 = framer.feed("24.", lines);
        auto st2 = framer.feed("5,55\n18,3", lines);
        CHECK("framer: line split across reads, remainder kept pending",
              st1 == LineFramer::Status::Ok && st2 == LineFramer::Status::Ok &&
              lines.size() == 1 && lines[0] == "24.5,55" && framer.pending() == "18,3");
    }
}

static bool fails_with(std::string_view line, ParseError e) {
    auto r = parse_reading(line);
    return !r && r.error() == e;
}

static void test_parse_reading() {
    auto ok = parse_reading("24.51,55");
    CHECK("parse: valid line", ok && std::fabs(ok->temperature_c - 24.51f) < 0.001f && ok->humidity_pct == 55);
    CHECK("parse: range boundaries accepted", parse_reading("-40,0") && parse_reading("125,100"));
    CHECK("parse: TooLong", fails_with(n_bytes(MAX_PACKET_SIZE + 1), ParseError::TooLong));
    CHECK("parse: MissingComma", fails_with("24.5 55", ParseError::MissingComma));
    CHECK("parse: BadTemperature (text)", fails_with("abc,55", ParseError::BadTemperature));
    CHECK("parse: BadTemperature (empty)", fails_with(",55", ParseError::BadTemperature));
    CHECK("parse: BadTemperature (trailing chars)", fails_with("24.5C,55", ParseError::BadTemperature));
    CHECK("parse: BadHumidity (decimal)", fails_with("24.5,55.5", ParseError::BadHumidity));
    CHECK("parse: BadHumidity (empty)", fails_with("24.5,", ParseError::BadHumidity));
    CHECK("parse: BadHumidity (extra field)", fails_with("24.5,55,1", ParseError::BadHumidity));
    CHECK("parse: TemperatureOutOfRange", fails_with("125.5,55", ParseError::TemperatureOutOfRange));
    CHECK("parse: TemperatureOutOfRange (nan)", fails_with("nan,55", ParseError::TemperatureOutOfRange));
    CHECK("parse: HumidityOutOfRange", fails_with("24.5,101", ParseError::HumidityOutOfRange) &&
                                       fails_with("24.5,-1", ParseError::HumidityOutOfRange));
    CHECK("parse: every error has a message", to_string(ParseError::TooLong) == "line too long");
}

static void test_parse_interval() {
    CHECK("interval: 250 accepted", parse_interval_ms("250").value_or(0) == 250);
    CHECK("interval: 0 rejected", !parse_interval_ms("0"));
    CHECK("interval: -5 rejected", !parse_interval_ms("-5"));
    CHECK("interval: '10ms' rejected", !parse_interval_ms("10ms"));
    CHECK("interval: empty rejected", !parse_interval_ms(""));
}

static void test_pipeline() {
    TelemetryPipeline pipeline(2);
    for (int id = 1; id <= 3; ++id) pipeline.push(make_packet(id, 0, std::string_view("24.5,55")));
    CHECK("pipeline: drop-oldest when full", pipeline.dropped() == 1);
    CHECK("pipeline: shutdown reports queued packets", pipeline.shutdown() == 2);
    auto a = pipeline.pop();
    auto b = pipeline.pop();
    auto c = pipeline.pop();
    CHECK("pipeline: drains remaining packets after shutdown, oldest dropped",
          a && b && !c && a->device_id == 2 && b->device_id == 3 && a->payload() == "24.5,55");
}

static void test_unique_fd() {
    auto e = make_eventfd();
    CHECK("UniqueFd: make_eventfd succeeds", e && *e);
    const int raw = e->get();
    UniqueFd moved = std::move(*e);
    CHECK("UniqueFd: move transfers ownership", moved.get() == raw && !*e && fd_is_open(raw));
    moved = UniqueFd{};
    CHECK("UniqueFd: assigning closes the old fd", !fd_is_open(raw));
    {
        auto ep = make_epoll();
        CHECK("UniqueFd: make_epoll succeeds", ep.has_value());
        int ep_raw = ep ? ep->get() : -1;
        auto add = epoll_add(ep_raw, -1, EPOLLIN);
        CHECK("epoll_add: bad fd reports EpollCtl", !add && add.error().op == Op::EpollCtl && add.error().code == EBADF);
        ep = std::unexpected(SysError{Op::EpollCreate});
        CHECK("UniqueFd: destroyed when expected is reassigned", !fd_is_open(ep_raw));
    }
}

static uint16_t local_port(int fd) {
    sockaddr_in a{};
    socklen_t len = sizeof(a);
    getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    return ntohs(a.sin_port);
}

static std::string message(const SysError& e) { std::ostringstream os; os << e; return os.str(); }

static void test_net() {
    auto wake = make_eventfd();
    if (!wake) { CHECK("net: eventfd", false); return; }

    CHECK("parse_ipv4: bad IP is BadAddress", !parse_ipv4("300.1.1.1", 1) &&
          parse_ipv4("not-an-ip", 1).error().op == Op::BadAddress);
    CHECK("SysError: bad IP message", message(SysError{Op::BadAddress}).starts_with("bad IP address"));

    auto listener = create_listener(0);
    CHECK("create_listener: ephemeral port", listener.has_value());
    if (!listener) return;
    const uint16_t port = local_port(listener->get());
    auto clash = create_listener(port);
    CHECK("create_listener: port in use reports Bind/EADDRINUSE",
          !clash && clash.error().op == Op::Bind && clash.error().code == EADDRINUSE);

    auto addr = parse_ipv4("127.0.0.1", port);
    auto conn = connect_to(*addr, wake->get(), 1s);
    CHECK("connect_to: connects to a listener", conn.has_value());

    listener->reset(); // Nobody listens on this port any more
    auto refused = connect_to(*addr, wake->get(), 1s);
    CHECK("connect_to: closed port reports connection refused",
          !refused && refused.error().op == Op::Connect && refused.error().code == ECONNREFUSED &&
          message(refused.error()).starts_with("connection refused"));

    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv) != 0) { CHECK("net: socketpair", false); return; }
    UniqueFd left{sv[0]}, right{sv[1]};
    const std::string_view msg = "24.5,55\n";
    auto sent = send_all(left.get(), msg, wake->get());
    char buf[16]{};
    ssize_t got = recv(right.get(), buf, sizeof(buf), 0);
    CHECK("send_all: whole span delivered", sent && got == static_cast<ssize_t>(msg.size()) && std::string_view(buf, 8) == msg);

    CHECK("wait_fd: times out with nothing to read", wait_fd(right.get(), POLLIN, wake->get(), 20ms) == Wait::Timeout);
    right.reset();
    auto broken = send_all(left.get(), msg, wake->get());
    CHECK("send_all: closed peer reports Send/EPIPE (no SIGPIPE)",
          !broken && broken.error().op == Op::Send && broken.error().code == EPIPE);

    uint64_t one = 1;
    (void)!write(wake->get(), &one, sizeof(one));
    auto t0 = std::chrono::steady_clock::now();
    Wait w = wait_fd(-1, 0, wake->get(), 10s);
    CHECK("wait_fd: eventfd interrupts a backoff sleep at once",
          w == Wait::Woken && std::chrono::steady_clock::now() - t0 < 100ms);
}

static void test_backoff() {
    Backoff b;
    std::vector<long> seq;
    for (int i = 0; i < 7; ++i) seq.push_back(static_cast<long>(b.next().count()));
    CHECK("backoff: 1,2,4,8,16,30,30 s", (seq == std::vector<long>{1000, 2000, 4000, 8000, 16000, 30000, 30000}));
    b.reset();
    CHECK("backoff: reset returns to 1 s", b.next() == 1000ms);
}

int main() {
    test_line_framer();
    test_parse_reading();
    test_parse_interval();
    test_pipeline();
    test_unique_fd();
    test_net();
    test_backoff();
    std::cout << "passed=" << passed << " failed=" << failed << "\n";
    return failed ? 1 : 0;
}
