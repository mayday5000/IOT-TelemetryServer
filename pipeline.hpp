// pipeline.hpp
// Purpose: Packet type and bounded drop-oldest queue between the reactor and the worker pool.
#pragma once

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <queue>
#include <span>
#include <string_view>

#include <string.h> // explicit_bzero

#include "line_framer.hpp" // MAX_PACKET_SIZE

constexpr size_t MAX_QUEUE_SIZE = 10000;

struct TelemetryPacket {
    int32_t  device_id = -1;
    uint64_t timestamp_ns = 0;
    uint8_t  raw_payload[MAX_PACKET_SIZE]; // Filled by make_packet; not zeroed first
    size_t   payload_len = 0;

    [[nodiscard]] std::string_view payload() const noexcept {
        return {reinterpret_cast<const char*>(raw_payload), payload_len};
    }

    // RAII: scrub the payload on destruction. explicit_bzero cannot be optimised away like memset.
    ~TelemetryPacket() { explicit_bzero(raw_payload, sizeof(raw_payload)); }
};

// Allocates a packet only once a complete line exists. make_unique_for_overwrite skips
// zero-filling the payload, which is overwritten right away. line.size() <= MAX_PACKET_SIZE.
[[nodiscard]] inline std::unique_ptr<TelemetryPacket> make_packet(int device_id, uint64_t timestamp_ns,
                                                                  std::span<const char> line) {
    auto packet = std::make_unique_for_overwrite<TelemetryPacket>();
    packet->device_id = device_id;
    packet->timestamp_ns = timestamp_ns;
    packet->payload_len = std::min(line.size(), MAX_PACKET_SIZE);
    std::memcpy(packet->raw_payload, line.data(), packet->payload_len);
    return packet;
}

class TelemetryPipeline {
public:
    explicit TelemetryPipeline(size_t capacity = MAX_QUEUE_SIZE) : capacity_(capacity) {}

    void push(std::unique_ptr<TelemetryPacket> packet) {
        {
            std::lock_guard lock(mutex_);
            if (queue_.size() >= capacity_) {
                queue_.pop(); // Drop oldest when full (bounded memory)
                ++dropped_;
            }
            queue_.push(std::move(packet));
        }
        cv_.notify_one();
    }

    // Returns queued packets until the queue is empty; nullptr only once shutdown
    // was requested AND every queued packet has been handed out (drain on exit).
    [[nodiscard]] std::unique_ptr<TelemetryPacket> pop() {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty() || stop_requested_; });
        if (queue_.empty()) return nullptr;
        auto packet = std::move(queue_.front());
        queue_.pop();
        return packet;
    }

    // Returns how many packets were still queued; workers drain them before exiting
    size_t shutdown() {
        size_t pending;
        {
            std::lock_guard lock(mutex_); // Set under the lock so no worker misses the wake-up
            stop_requested_ = true;
            pending = queue_.size();
        }
        cv_.notify_all();
        return pending;
    }

    [[nodiscard]] uint64_t dropped() {
        std::lock_guard lock(mutex_);
        return dropped_;
    }

private:
    std::queue<std::unique_ptr<TelemetryPacket>> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t capacity_;
    uint64_t dropped_ = 0;        // Guarded by mutex_
    bool stop_requested_ = false; // Guarded by mutex_
};
