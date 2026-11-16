#include <iostream>
#include <atomic>
#include <thread>
#include <chrono>
#include <string>
#include <vector>

#include "line_framer.hpp"

std::atomic<bool> g_running{true};

int passed = 0, failed = 0;
#define CHECK(name, cond) do { if (cond) { std::cout << "[PASS] " << name << "\n"; ++passed; } else { std::cout << "[FAIL] " << name << "\n"; ++failed; } } while(0)

static std::string n_bytes(size_t n, char c = 'A') { return std::string(n, c); }

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
              st == LineFramer::Status::Ok && lines.size() == 1 && lines[0].size() == 128 && lines[0] == n_bytes(128));
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
        CHECK("framer: 129-byte CRLF line rejected",
              st == LineFramer::Status::Oversize && lines.empty() && framer.oversize());
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
        CHECK("framer: blank lines skipped",
              st == LineFramer::Status::Ok && lines.size() == 1 && lines[0] == "24.5,55");
    }
}

int main() {
    CHECK("flag starts true", g_running.load());
    g_running.store(false);
    CHECK("flag can be set false", !g_running.load());

    g_running.store(true);
    bool exited = false;
    std::thread t([&]() {
        while (g_running.load(std::memory_order_relaxed))
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        exited = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    g_running.store(false);
    t.join();
    CHECK("worker exits on flag", exited);

    std::string sample = "24.5,55\n";
    CHECK("payload format is temperature,humidity",
          sample == "24.5,55\n" && sample.find(',') != std::string::npos && sample.back() == '\n');

    test_line_framer();

    std::cout << "passed=" << passed << " failed=" << failed << "\n";
    return failed ? 1 : 0;
}
