// line_framer.hpp
// Purpose: Per-connection newline framer shared by server.cpp and tests.cpp.
// Splits a TCP byte stream on '\n', accepts CRLF, skips blank lines, and
// caps content at MAX_PACKET_SIZE after a trailing '\r' is stripped.
#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cstddef>

constexpr size_t MAX_PACKET_SIZE = 128; // Max line content in bytes, excluding '\n' and a trailing '\r'

class LineFramer {
public:
    enum class Status { Ok, Oversize };

    // Feed a chunk of the TCP stream. Each complete, non-blank line is passed to
    // on_line as a view of the content (no '\n', trailing '\r' already stripped).
    // The view is valid only for the duration of that callback.
    //
    // Returns Oversize if a line's content would exceed MAX_PACKET_SIZE; the
    // pending buffer is then cleared and further feed calls stay Oversize.
    // The buffer stays bounded: at most MAX_PACKET_SIZE + 1 bytes, and the extra
    // byte is allowed only when it is a trailing '\r' waiting for a '\n'.
    template <typename F>
    Status feed(std::string_view data, F&& on_line) {
        if (oversize_) return Status::Oversize;

        while (true) {
            const size_t newline = data.find('\n');
            if (newline == std::string_view::npos) break;

            const size_t raw = pending_.size() + newline;
            const bool has_cr = (newline > 0)
                ? data[newline - 1] == '\r'
                : (!pending_.empty() && pending_.back() == '\r');
            const size_t content = raw - (has_cr ? 1 : 0);
            if (content > MAX_PACKET_SIZE) {
                oversize_ = true;
                pending_.clear();
                return Status::Oversize;
            }

            pending_.append(data.substr(0, newline));
            if (!pending_.empty() && pending_.back() == '\r') pending_.pop_back();
            if (!pending_.empty()) on_line(std::string_view{pending_});
            pending_.clear();
            data.remove_prefix(newline + 1);
        }

        if (data.empty()) return Status::Ok;

        // A remainder may include a trailing CR of a CRLF split across reads,
        // so allow one extra byte only when the last byte of the remainder is '\r'.
        const size_t would = pending_.size() + data.size();
        const size_t allowed = MAX_PACKET_SIZE + (data.back() == '\r' ? 1 : 0);
        if (would > allowed) {
            oversize_ = true;
            pending_.clear();
            return Status::Oversize;
        }
        pending_.append(data);
        return Status::Ok;
    }

    Status feed(std::string_view data, std::vector<std::string>& lines) {
        return feed(data, [&](std::string_view line) { lines.emplace_back(line); });
    }

    const std::string& pending() const { return pending_; }
    bool oversize() const { return oversize_; }

private:
    std::string pending_;
    bool oversize_ = false;
};
