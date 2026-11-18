// reading.hpp
// Purpose: Telemetry line parser and command-line value parser shared by server.cpp,
// client.cpp and tests.cpp. Errors are small enums returned through
// [[nodiscard]] std::expected, so rejecting a line never allocates.
#pragma once

#include <charconv>
#include <cstddef>
#include <expected>
#include <string_view>

#include "line_framer.hpp" // MAX_PACKET_SIZE

struct Reading {
    float temperature_c;
    int   humidity_pct;
};

constexpr float MIN_TEMPERATURE_C = -40.0f;  // Typical sensor range
constexpr float MAX_TEMPERATURE_C = 125.0f;
constexpr int   MIN_HUMIDITY_PCT  = 0;
constexpr int   MAX_HUMIDITY_PCT  = 100;

enum class ParseError {
    TooLong,               // More than MAX_PACKET_SIZE bytes
    MissingComma,          // No ',' separator
    BadTemperature,        // Temperature is not a plain decimal number
    BadHumidity,           // Humidity is not a plain integer
    TemperatureOutOfRange, // Outside -40..125 C (also NaN / inf)
    HumidityOutOfRange,    // Outside 0..100 %
};

[[nodiscard]] constexpr std::string_view to_string(ParseError e) noexcept {
    switch (e) {
        case ParseError::TooLong:               return "line too long";
        case ParseError::MissingComma:          return "missing ',' separator";
        case ParseError::BadTemperature:        return "temperature is not a number";
        case ParseError::BadHumidity:           return "humidity is not an integer";
        case ParseError::TemperatureOutOfRange: return "temperature out of range (-40..125 C)";
        case ParseError::HumidityOutOfRange:    return "humidity out of range (0..100 %)";
    }
    return "unknown error";
}

// Parses one framed line "temperature,humidity" (e.g. "24.51,55"); extra characters are rejected.
[[nodiscard]] inline std::expected<Reading, ParseError> parse_reading(std::string_view line) {
    if (line.size() > MAX_PACKET_SIZE) return std::unexpected(ParseError::TooLong);

    const auto comma = line.find(',');
    if (comma == std::string_view::npos) return std::unexpected(ParseError::MissingComma);
    const std::string_view temp = line.substr(0, comma);
    const std::string_view hum  = line.substr(comma + 1);

    Reading r{};
    auto [tp, tec] = std::from_chars(temp.data(), temp.data() + temp.size(), r.temperature_c);
    if (temp.empty() || tec != std::errc{} || tp != temp.data() + temp.size())
        return std::unexpected(ParseError::BadTemperature);
    auto [hp, hec] = std::from_chars(hum.data(), hum.data() + hum.size(), r.humidity_pct);
    if (hum.empty() || hec != std::errc{} || hp != hum.data() + hum.size())
        return std::unexpected(ParseError::BadHumidity);

    // Written as !(in range) so NaN is rejected too
    if (!(r.temperature_c >= MIN_TEMPERATURE_C && r.temperature_c <= MAX_TEMPERATURE_C))
        return std::unexpected(ParseError::TemperatureOutOfRange);
    if (r.humidity_pct < MIN_HUMIDITY_PCT || r.humidity_pct > MAX_HUMIDITY_PCT)
        return std::unexpected(ParseError::HumidityOutOfRange);
    return r;
}

// Client send interval in milliseconds: a plain integer >= 1. A typo is an error, never a silent default.
[[nodiscard]] inline std::expected<int, std::string_view> parse_interval_ms(std::string_view arg) {
    int value = 0;
    auto [ptr, ec] = std::from_chars(arg.data(), arg.data() + arg.size(), value);
    if (arg.empty() || ec != std::errc{} || ptr != arg.data() + arg.size())
        return std::unexpected("expected an integer number of milliseconds");
    if (value < 1) return std::unexpected("interval must be at least 1 ms");
    return value;
}
