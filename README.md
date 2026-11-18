# IoT Telemetry Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-performance, low-power telemetry ingestion server and client written in modern C++23 for Linux IoT edge deployments, where every CPU wake-up costs battery. It is designed for battery-powered, solar-powered and other energy-constrained edge gateways and devices.

Built on the classic Reactor pattern, a single server thread demultiplexes non-blocking socket I/O using Linux epoll (edge-triggered), splits each connection's byte stream into newline-terminated lines, and dispatches one packet per line to a static worker thread pool via a bounded, thread-safe queue. Workers validate each `temperature,humidity` reading.

**How it stays low-power:** nothing in the system busy-waits or polls on a timer. The server's reactor sleeps inside the kernel in `epoll_wait` with no timeout and is woken only when a socket is ready or when the shutdown `eventfd` is signaled, so an idle server adds zero periodic wake-ups. Idle worker threads block on a condition variable instead of polling the queue. The worker pool is created once at startup (no thread churn), packets are small fixed-size buffers, and the queue is capped, so memory stays bounded. The client sleeps in a single `poll()` call between readings (one wake-up per reading) and sends compact lines of about 9 bytes.

The companion client connects over TCP and emits simulated sensor readings in a lightweight line format (`temperature,humidity\n`, for example `23.38,48`). If the server is down or the connection drops, it prints a clear reason and reconnects by itself with exponential backoff, sending nothing until it is connected again. Both processes support deterministic, graceful shutdown on SIGINT and SIGTERM, woken instantly through an `eventfd` instead of a polling timeout.

## Key Features

- Zero Idle Wake-Ups (eventfd Shutdown): The reactor calls `epoll_wait(..., -1)` with no timeout. SIGINT/SIGTERM handlers write to an `eventfd` registered in the epoll set, so shutdown does not depend on a periodic wake-up to check a flag.
- Sleep-Driven Event Loop: The reactor is woken by the kernel only when a socket is ready or shutdown is requested, so the CPU can drop into idle states instead of spinning.
- Zero Busy-Waiting Workers: Worker threads sleep on a `std::condition_variable` and wake only when a line is queued or shutdown is requested.
- No Runtime Thread Churn: The fixed worker pool is created once at startup, avoiding the CPU and memory cost of spawning threads per connection.
- Bounded Memory Footprint: Each packet holds at most one 128-byte line, and the queue is capped at 10,000 packets (roughly 1.5 MB at most), keeping RAM usage predictable on constrained edge hardware.
- Low-Duty-Cycle Client: The client sends one short reading per interval (default 1 s) and sleeps in `poll()` in between; Ctrl-C or a closed server wakes it immediately.
- Line Framing & Validation: A per-connection buffer splits the TCP byte stream on `\n`, so readings are reassembled correctly whether they arrive byte by byte or many per read. Workers parse `temperature,humidity` with `std::from_chars` and count invalid lines.
- Edge-Triggered epoll Reactor: Single-threaded I/O multiplexing with EPOLLET and EPOLLRDHUP. Data that arrives together with a hang-up is read before the socket is closed, so a device's last reading is not lost.
- Non-Blocking Accept & Read Loops: Exhaustive EAGAIN drain loops; `accept4()` also handles EINTR/ECONNABORTED and descriptor exhaustion (EMFILE/ENFILE) with a spare descriptor, so the edge-triggered listener never stalls.
- Static Worker Thread Pool: Sized to 2 × std::thread::hardware_concurrency() (4 if the core count is unknown).
- Bounded Queue (Drop-Oldest): Capped at 10,000 packets; when full, the oldest packet is dropped so memory cannot grow without limit and the reactor never stalls.
- RAII Descriptors: Every socket, epoll instance and eventfd is owned by a move-only `UniqueFd`, so early error returns cannot leak descriptors and connected clients are closed at shutdown.
- Explicit Error Values: Setup and connect helpers return `[[nodiscard]] std::expected<UniqueFd, SysError>` (failed step + errno, no heap), and every `socket`/`setsockopt`/`bind`/`listen`/`epoll_ctl`/`inet_pton` result is checked.
- Reasoned Rejects: `parse_reading` returns `std::expected<Reading, ParseError>`, and workers log why a line was rejected (missing separator, not a number, out of range, ...).
- No Wasted Allocations: The reactor reads into one reusable stack buffer and allocates a packet only for a complete line.
- RAII Memory Scrubbing: The TelemetryPacket destructor zeroes the payload buffer with `explicit_bzero` (which, unlike `memset`, cannot be optimised away) whenever a packet is destroyed (processed, dropped on overflow, or unwound by an exception).
- Auto-Reconnecting Client: On connect failure or a dropped connection (EOF, EPIPE, ECONNRESET, hang-up) the client prints the reason and retries after 1 s, 2 s, 4 s ... up to 30 s, resetting to 1 s after a successful connect. The backoff wait sleeps in `poll()` on the shutdown eventfd: no wake-ups while waiting, and Ctrl-C exits at once.
- Reset-Safe Client Writes: The client sends with `send(..., MSG_NOSIGNAL)`, ignores SIGPIPE, and handles partial sends; a server that resets or closes the connection triggers a reconnect instead of killing the client.
- Graceful Shutdown: SIGINT/SIGTERM set an atomic stop flag and ring the shutdown `eventfd`, waking the reactor instantly from whichever thread received the signal; workers then finish every line already queued before they are joined.

## System Architecture

### 1. Reactor & Worker Pool Model

Instead of blocking one thread per client, one reactor thread monitors all file descriptors and hands off packet ownership (std::unique_ptr) to background workers.

```
MAIN THREAD (Reactor)
  g_running = true
  create shutdown eventfd, install SIGINT/SIGTERM handlers
  socket / bind / listen / epoll setup   (any failure exits here)
  spawn std::jthread worker pool
  ┌─────────────────────────────────────────┐
  │ while (g_running)                       │
  │   epoll_wait(..., -1)                   │ ◄── Sleeps in kernel, zero idle wake-ups
  │   handle eventfd / accept / read /      │
  │          hang-ups                       │
  └─────────────────────────────────────────┘
  pipeline->shutdown() ──► notify_all()
  workers.clear() joins the std::jthread pool

WORKER THREADS
  ┌─────────────────────────────────────────┐
  │ while (packet = pipeline->pop())        │ ◄── Blocks on condition_variable
  │   parse "temperature,humidity"          │
  │   log reading or count invalid line     │
  │   scrub packet memory (RAII)            │
  └─────────────────────────────────────────┘
  pop() returns nullptr only after shutdown() AND once the queue is empty
```

### 2. Reactor State Machine

The reactor never blocks on application work; parsing and logging of readings happen in the worker pool.

```
          ┌──────────────┐
          │   epoll_wait │◄──────────────────────────┐
          └──────┬───────┘                           │
                 │ events                            │
     ┌───────────┼──────────────┬──────────┐         │
     ▼           ▼              ▼          ▼         │
  eventfd     accept4        EPOLLIN /   (line too   │
  (stop       (drain)        hang-up      long →     │
   loop)                     (drain read, disconnect)│
                              split lines,           │
                              push, then             │
                              close on hang-up)      │
                 │                                   │
                 └───────────────────────────────────┘
```

1. Setup: Create the shutdown eventfd, a non-blocking TCP listening socket and an epoll instance; register the socket with EPOLLIN | EPOLLET and the eventfd with EPOLLIN. Workers start only after setup succeeds.
2. Wait: epoll_wait() blocks with no timeout until at least one registered file descriptor becomes ready.
3. Dispatch:
   - Shutdown eventfd Ready: Read (drain) the eventfd counter and leave the loop.
   - Listening Socket Ready: Drain accept4() until EAGAIN, creating non-blocking client sockets registered with EPOLLIN | EPOLLET | EPOLLRDHUP. On EMFILE/ENFILE the spare descriptor is released, the pending connection is accepted and closed, and the spare is reopened.
   - Client Readable or Hang-Up (EPOLLIN, EPOLLRDHUP, EPOLLHUP, EPOLLERR): Drain read() in 4 KB chunks until EAGAIN or end of stream. Bytes are appended to the connection's line buffer; every complete line (CRLF accepted, blank lines skipped) becomes one TelemetryPacket pushed to TelemetryPipeline. Then, on hang-up, the socket is removed from epoll and closed.
4. Loop: Return to epoll_wait().

### 3. Line Framing Contract

- One reading per line: `temperature,humidity\n`, e.g. `23.38,48\n`. Temperature is a decimal number in -40..125 °C, humidity an integer in 0..100 %. Anything else is counted and logged as invalid by the workers.
- Maximum line length is 128 bytes of content (MAX_PACKET_SIZE), excluding `\n` and a trailing `\r` of a CRLF terminator. A 128-byte line ending in `\r\n` is accepted; a longer line disconnects the client, because the stream can no longer be trusted to resynchronise.
- A final line without `\n` when the connection closes may be truncated, so it is discarded and logged.
- At shutdown the server prints `valid`, `invalid`, `oversize_disconnects` and `unterminated_discarded` counts.

### 4. Producer-Consumer Pipeline & Client Architecture

```
SERVER PIPELINE:
Reactor Thread                  Worker Threads
     │                               │
     │  read() + split lines         │
     │  pipeline->push() ──────────► │  pop() (blocks until data or shutdown)
     │  (O(1), drop-oldest when full)│  validate, log & scrub
     │                               │  loop

CLIENT (single thread):
  parse IP and interval              ── bad value: print error, exit 2
  loop:
    connect_to (non-blocking, 5 s timeout, interruptible)
      └─ failed → print reason, poll(eventfd, backoff) → retry
    backoff.reset()
    loop:
      send_all("23.38,48\n")            ── std::span, partial sends handled, MSG_NOSIGNAL
      poll(eventfd, socket, interval)   ── one wake-up per reading
        ├─ timeout        → next reading
        ├─ eventfd        → Ctrl-C: stop
        └─ socket EOF/err → print reason, close, poll(eventfd, backoff) → reconnect
  backoff: 1 s, 2 s, 4 s, 8 s, 16 s, 30 s, 30 s ...
```

## Project Structure

```
├── README.md        # Project documentation
├── LICENSE          # MIT License
├── deploy.sh        # Idempotent Linux dependency & build script
├── .gitignore       # Keeps build outputs (server, client, tests) out of git
├── net_util.hpp     # UniqueFd, SysError, expected-returning socket/epoll/eventfd helpers, send_all, wait_fd, Backoff
├── line_framer.hpp  # Newline/CRLF line framer
├── reading.hpp      # parse_reading -> expected<Reading, ParseError>, parse_interval_ms
├── pipeline.hpp     # TelemetryPacket and the bounded drop-oldest TelemetryPipeline
├── server.cpp       # Epoll reactor and jthread worker pool
├── client.cpp       # Simulated sensor telemetry publisher with auto-reconnect
└── tests.cpp        # Unit tests of the shared headers (the real parser, framer, queue and net helpers)
```

## Quick Start

### Prerequisites

- Compiler: g++ 14 or newer with C++23 support (`-std=c++23`, `<expected>`, `<print>`, `<format>`, `std::jthread`, floating-point `std::from_chars`); verified with g++ 14.2. Other C++23 toolchains should work but are untested.
- OS: Linux only. Uses epoll (`epoll_create1`, `EPOLLET`, `EPOLLRDHUP`), `accept4` and `eventfd` with `EFD_NONBLOCK | EFD_CLOEXEC`, so kernel 2.6.28 or newer.
- Libraries: pthread (via `-pthread`); no third-party dependencies.

### Automated Setup & Build (deploy.sh)

deploy.sh is the single, idempotent entry point on Linux:

1. Inspects the system for g++ and make, installing only what is missing (build-essential on Debian/Ubuntu, gcc-c++ on Fedora/RHEL/yum). It does not check the compiler version, so on distributions whose default g++ is older than 14, install a newer g++ first.
2. Compiles server, client, and tests with -std=c++23 -O2 -Wall -Wextra -Wpedantic -pthread (make is checked for but not used).
3. Runs ./tests and stops on failure (set -euo pipefail).

```bash
chmod +x deploy.sh && ./deploy.sh
```

### Manual Installation & Build

Debian / Ubuntu:

```bash
sudo apt-get update && sudo apt-get install -y build-essential g++ make
```

Fedora / RHEL:

```bash
sudo dnf install -y gcc-c++ make
```

Compile & Run Tests:

```bash
# Release build
g++ -std=c++23 -O2 -Wall -Wextra -Wpedantic -pthread -o server server.cpp
g++ -std=c++23 -O2 -Wall -Wextra -Wpedantic -pthread -o client client.cpp
g++ -std=c++23 -O2 -Wall -Wextra -Wpedantic -pthread -o tests tests.cpp && ./tests

# Debug build (with Address & Undefined Behavior Sanitizers)
g++ -std=c++23 -g -O1 -Wall -Wextra -Wpedantic -pthread -fsanitize=address,undefined -o server_debug server.cpp
```

### Running the Server & Client

Usage:
- `./server` takes no arguments and listens on TCP port 8883 on all IPv4 interfaces (`SERVER_PORT` in server.cpp).
- `./client [server_ipv4] [interval_ms]` defaults to `127.0.0.1` and `1000`. The IP must be an IPv4 dotted quad and the interval an integer of at least 1; otherwise the client prints the reason and its usage and exits with code 2 (retrying cannot fix a configuration error).

Terminal 1 (Start Server):

```bash
./server
```

```
[SYSTEM] Starting 16 worker threads.
[REACTOR] Listening on port 8883 (Ctrl-C to stop)
[REACTOR] Accepted fd=7
[WORKER] fd=7 temperature=23.38 C humidity=48 %
[WORKER] fd=7 temperature=25.27 C humidity=71 %
[WORKER] fd=7 temperature=27.85 C humidity=42 %
[REACTOR] Client disconnected fd=7
[SYSTEM] Closing 0 client connection(s).
[SYSTEM] Shutting down. Draining 0 queued packet(s)...
[SYSTEM] Lines: valid=3 invalid=0 oversize_disconnects=0 unterminated_discarded=0 dropped_queue_full=0
[SYSTEM] Shutdown complete.
```

Terminal 2 (Start Client):

```bash
./client 127.0.0.1 1000
```

```
[CLIENT] Connecting to 127.0.0.1:8883...
[CLIENT] Connected. Interval=1000 ms. Ctrl-C to stop.
[CLIENT] Sent: 23.38,48
[CLIENT] Sent: 25.27,71
[CLIENT] Sent: 27.85,42
[CLIENT] Shutdown complete.
```

Client started before the server, then the server stopped mid-stream:

```
[CLIENT] Connecting to 127.0.0.1:8883...
[CLIENT] connection refused (is the server running?). Retrying in 1 s.
[CLIENT] Connecting to 127.0.0.1:8883...
[CLIENT] connection refused (is the server running?). Retrying in 2 s.
[CLIENT] Connecting to 127.0.0.1:8883...
[CLIENT] Connected. Interval=1000 ms. Ctrl-C to stop.
[CLIENT] Sent: 23.38,48
[CLIENT] server closed connection. Retrying in 1 s.
[CLIENT] Connecting to 127.0.0.1:8883...
[CLIENT] connection refused (is the server running?). Retrying in 2 s.
^C[CLIENT] Shutdown complete.
```

Client error messages:

| Message | Meaning | Client action |
|---------|---------|---------------|
| `bad IP address (expected IPv4 dotted quad ...)` | `inet_pton` rejected the server IP argument | Exit code 2 |
| `Invalid interval '...'` | Interval is not an integer >= 1 | Exit code 2 |
| `connection refused (is the server running?)` | Nothing listens on port 8883 at that IP | Retry with backoff |
| `connect timeout` | No answer within 5 s (host down, firewall drop) | Retry with backoff |
| `connect failed: <reason>` | Other connect error, e.g. network unreachable | Retry with backoff |
| `server closed connection` | Orderly close (EOF / hang-up) | Reconnect with backoff |
| `server closed connection: <reason>` | Reset or broken pipe (ECONNRESET, EPIPE) | Reconnect with backoff |

The worker count depends on the machine (2 × cores; 16 on an 8-core host). The client simulates readings (temperature 18–32 °C, humidity 30–80 %). The `fd` shown by the server is the connection's socket descriptor; descriptor numbers are reused after a client disconnects.

Press Ctrl-C in either terminal to trigger a graceful shutdown, including while the client is connecting or waiting to reconnect. The client exits with code 0 after Ctrl-C or SIGTERM, 1 if it cannot create its eventfd, and 2 for an invalid IP or interval.

## Modern C++ Features (C++17/20/23)

The codebase is compiled as C++23 and uses these modern C++ (C++17/20/23) features, kept deliberately simple:

- RAII `UniqueFd` (net_util.hpp): a move-only owner of one file descriptor that closes it exactly once. Sockets, the epoll instance, the eventfd, the EMFILE spare descriptor and every client connection are UniqueFds, so an early `return` on a setup error cannot leak, and clearing the connection map at shutdown closes all clients. The signal handlers keep only a plain `int` copy of the wake eventfd, so they stay async-signal-safe.
- `[[nodiscard]] std::expected<T, E>`: `create_listener`, `make_epoll`, `make_eventfd` and `connect_to` return `std::expected<UniqueFd, SysError>`; `epoll_add` and `send_all` return `std::expected<void, SysError>`. `SysError` is an enum naming the failed step plus the saved errno, so error paths never allocate, and a forgotten check is a compiler warning. Unlike an out-parameter plus error code, the descriptor only exists on success; unlike `std::optional`, the reason travels with the failure; unlike exceptions, control flow stays visible.
- `std::expected<Reading, ParseError>`: `parse_reading` reports exactly why a line was rejected (`TooLong`, `MissingComma`, `BadTemperature`, `BadHumidity`, `TemperatureOutOfRange` including NaN, `HumidityOutOfRange`); `parse_interval_ms` does the same for the client argument, so a typo is an error instead of a silent default.
- `std::span`: buffer + length pairs travel as one value: `send_all(fd, std::span<const char>, ...)`, `make_packet(..., std::span<const char> line)`, the `epoll_event` array (`std::span(events).first(n)`), and the client's arguments (`std::span<char*> args(argv, argc)`).
- `std::string_view`: the framer, parser, packet `payload()` and command-line arguments are non-owning views. Strings handed to C APIs (`inet_pton`) are NUL-terminated (argv entries or literals).
- `[[nodiscard]]` on fallible functions and accessors (`LineFramer::feed`, `parse_reading`, `wait_fd`, `Backoff::next`, ...).
- `std::jthread`: workers join automatically when the `std::vector<std::jthread>` is cleared, so there are no manual join loops.
- `std::unique_ptr<TelemetryPacket>` with `std::make_unique_for_overwrite`: each packet has exactly one owner from reactor to worker, and its payload is not zero-filled before being overwritten by the line.
- `std::from_chars`: locale-independent, allocation-free parsing of the interval and of each reading.
- `std::println` / `std::format` / `std::format_to_n`: single-call worker log lines, and client readings formatted into a stack buffer with no heap allocation per reading.
- `std::chrono`: typed durations for the send interval, connect timeout and backoff (`std::chrono::ceil` for poll timeouts).
- `std::atomic`: the stop flags and line counters are safe to write from a signal handler or several workers.

Monadic `std::expected` chaining (`.and_then`/`.transform`) is not used: each setup step yields a different resource that later code needs by name, so plain early returns read more clearly here. A fallback via `.or_else`/`.value_or` would turn configuration typos into silent defaults, so the code reports them instead.

## Ownership & Memory Scrubbing Flow

1. Reactor reads into a reusable stack buffer and, for each complete line only, copies it into a new std::unique_ptr<TelemetryPacket> (make_packet).
2. Reactor moves ownership into TelemetryPipeline::push().
3. Worker thread receives exclusive ownership from TelemetryPipeline::pop().
4. When the packet is destroyed (after processing, when evicted by the drop-oldest policy, or during exception unwinding), ~TelemetryPacket() zeroes the payload buffer with explicit_bzero() before the memory returns to the allocator; unlike a plain memset, it cannot be removed by the optimizer. Partial-line buffers and the reusable read buffer are not scrubbed.
5. On shutdown, workers drain the queue and the std::jthread pool is joined.

## Edge-Triggered & Overflow Contracts

- Edge-Triggered Drain Contract: Because sockets are registered with EPOLLET, every ready file descriptor must be drained until EAGAIN. Both accept4() and read() are wrapped in loops that exit only on EAGAIN/EWOULDBLOCK, end of stream or a hard error; failing to drain would silently lose subsequent data until the next edge transition.
- Hang-Up Contract: EPOLLRDHUP/EPOLLHUP often arrive together with EPOLLIN when a device sends its last reading and closes, so the socket is always drained before it is closed.
- Overflow Policy: TelemetryPipeline is capped at 10,000 packets. When full, the oldest packet is dropped before inserting the newest, prioritizing fresh telemetry while bounding memory without stalling the reactor. No back-pressure is applied to clients; dropped packets are counted and reported at shutdown as `dropped_queue_full`.
- Queue Wake-Up Policy: push() calls notify_one() to wake a single worker; shutdown() sets the stop flag under the queue mutex and calls notify_all(), so no worker can miss the wake-up.

## Detailed Shutdown Sequence

The reactor waits with no timeout, so shutdown is delivered through an eventfd registered in the epoll set. This costs one extra file descriptor and removes both periodic idle wake-ups and shutdown delay:

1. SIGINT/SIGTERM handler performs only async-signal-safe work: it stores g_running = false and write()s a uint64_t 1 to the eventfd (preserving errno).
2. The eventfd becomes readable, so epoll_wait returns immediately, no matter which thread received the signal. If the signal arrives just before epoll_wait is entered, the counter is already set and epoll_wait returns at once. If epoll_wait is interrupted with EINTR, the loop re-checks the flag.
3. The reactor stops reading and closes every client connection, so no new lines are queued. pipeline->shutdown() sets the stop flag under the queue mutex, reports how many packets are still queued, and broadcasts notify_all().
4. Workers keep calling pop() and process every line that was already queued; pop() returns nullptr only when shutdown was requested and the queue is empty.
5. Client connections (and any partial lines they hold) are closed by clearing the connection map, workers.clear() joins the std::jthread pool, and the line counters are printed. The listening socket, epoll fd, eventfd and spare descriptor are UniqueFds closed when main returns.

Draining is unbounded in time: with a large backlog and slow processing, shutdown takes as long as the workers need to finish the queue (at most 10,000 packets). Process managers such as systemd (TimeoutStopSec, 90 s by default) or Kubernetes (terminationGracePeriodSeconds, 30 s by default) send SIGKILL if that limit is exceeded.

The client uses the same eventfd doorbell: its poll() wakes immediately on Ctrl-C, including while it is connecting, waiting out a reconnect backoff, or waiting for a full socket buffer to drain.

## Testing, Troubleshooting & Production Hardening

### Unit Testing (tests.cpp)

tests.cpp includes the same headers the server and client use (line_framer.hpp, reading.hpp, pipeline.hpp, net_util.hpp), so it tests the real code (48 checks):

- LineFramer: 128-byte LF and CRLF lines accepted, 129-byte lines rejected, CRLF and lines split across reads, blank lines skipped.
- parse_reading: a valid line, range boundaries, and every ParseError (including NaN as TemperatureOutOfRange); parse_interval_ms rejects `0`, `-5`, `10ms` and empty input.
- TelemetryPipeline: drop-oldest with a counted drop, shutdown reports the queued count, workers still drain after shutdown.
- UniqueFd: move transfers ownership, reassignment closes, an `std::expected` holding a UniqueFd closes it when replaced.
- net_util: bad IP, port-in-use (`Bind`/EADDRINUSE), connect success and `connection refused`, `send_all` of a span over a socketpair, EPIPE without SIGPIPE after the peer closes, `wait_fd` timeout and eventfd wake-up, and the backoff sequence 1, 2, 4, 8, 16, 30, 30 s.

The epoll reactor loop and the client's session loop are verified by running server and client together (normal run, client started before the server, server killed mid-stream, Ctrl-C during backoff), under AddressSanitizer/UBSan, and with `strace -c` to confirm there are no idle wake-ups.

### Troubleshooting

| Symptom | Likely Cause & Fix |
|---------|--------------------|
| Address already in use | Another process holds port 8883 (`ss -tulpn \| grep 8883`). Stop it or change SERVER_PORT. The server exits with code 1. |
| Client keeps printing "connection refused" or "connect timeout" | It is retrying with backoff. Confirm ./server is running and local firewall rules (ufw/iptables) allow TCP traffic on port 8883; the client connects by itself once the server is reachable. |
| Client exits with "bad IP address" | Pass an IPv4 dotted quad, e.g. `./client 192.168.1.10`; host names are not resolved. |
| Client exits with "Invalid interval" | The interval must be an integer of at least 1 ms, e.g. `./client 127.0.0.1 500`. |
| "Rejected line from fd=N ...: <reason>" | The line is not `temperature,humidity` with temperature in -40..125 and humidity an integer in 0..100; the reason names which part failed. |
| "Line longer than 128 bytes ... disconnecting" | The sender is not using newline-terminated lines or sent an oversized line. |
| "Discarded N-byte unterminated line" | The connection closed in the middle of a line; terminate every reading with `\n`. |
| Ctrl-C takes a long time on the server | Workers are draining a large backlog. Each line already queued is processed before exit. |
| "Out of file descriptors; rejected a connection" | The process hit its descriptor limit (`ulimit -n`); raise it or reduce concurrent clients. |
| High idle CPU usage | Ensure epoll_wait is not called with a zero timeout (it should block with -1) and that accept4()/read() loops exit cleanly on EAGAIN. |

### Scaling & Production Hardening

- Horizontal Scaling: Run multiple server processes behind a load balancer, or set SO_REUSEPORT (not set today) and adopt a multi-reactor design (one epoll instance per core) for extreme connection rates.
- Security & Observability: Wrap sockets in TLS (OpenSSL/mbedTLS), add device authentication, redirect stdout/stderr to journald or a log daemon, and expose the atomic counters (plus accepted connections, dropped packets, queue depth) to Prometheus.
- Possible extensions: Add an outbound write buffer for server-to-client responses, identify devices by an ID instead of the socket descriptor, persist telemetry to disk or a time-series database, scrub partial-line buffers, and buffer readings on the client while it is reconnecting.

## Limitations

This is a compact teaching and reference implementation:

- No TLS and no authentication: traffic is plain TCP on port 8883 (the port conventionally used for MQTT over TLS), and any client may send readings.
- Devices are identified only by the server-side socket descriptor, which is reused after disconnects.
- Readings are validated and logged but not stored or forwarded.
- Every processed line is logged to stdout.
- Client: IPv4 only (no host-name resolution), port fixed at 8883, simulated sensor values; readings due while disconnected are not buffered.
- The server never writes to clients, so it needs no SIGPIPE handling; the client handles it as described above.

## Scripts

deploy.sh checks for g++ and make, installs any missing packages, builds the three binaries (server, client, tests) with -std=c++23 -O2 -Wall -Wextra -Wpedantic -pthread, and runs the tests. See [Automated Setup & Build](#automated-setup--build-deploysh) for details.

Example:

```bash
./deploy.sh
```

## Further Operational Notes

The server listens on TCP port 8883 on all IPv4 interfaces. A failed bind almost always indicates that another process already owns the port; ss -tulpn | grep 8883 identifies the occupant.

All diagnostic output appears on standard output and standard error. In production these streams should be captured by the system logging facility. The reactor thread only accepts, reads, splits lines and enqueues; it reads into one reusable buffer, allocates one TelemetryPacket per complete line, and grows a small per-connection string only while a line is incomplete.

The worker count is computed as twice the hardware concurrency reported by the standard library, with a fallback of four threads when the query returns zero. This sizing provides enough concurrency to absorb short processing bursts while remaining modest for a light workload.

## License

This project is licensed under the [MIT License](LICENSE). Copyright (c) 2026 Damian Andres Ulanowicz.
