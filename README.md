# IoT Telemetry Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-performance, low-power telemetry ingestion server and client written in modern C++23 for Linux IoT edge deployments, where every CPU wake-up costs battery. It is designed for battery-powered, solar-powered and other energy-constrained edge gateways and devices.

Built on the classic Reactor pattern, a single server thread demultiplexes non-blocking socket I/O using Linux epoll (edge-triggered), splits each connection's byte stream into newline-terminated lines, and dispatches one packet per line to a static worker thread pool via a bounded, thread-safe queue. Workers validate each `temperature,humidity` reading.

**How it stays low-power:** nothing in the system busy-waits or polls on a timer. The server's reactor sleeps inside the kernel in `epoll_wait` with no timeout and is woken only when a socket is ready or when the shutdown `eventfd` is signaled, so an idle server adds zero periodic wake-ups. Idle worker threads block on a condition variable instead of polling the queue. The worker pool is created once at startup (no thread churn), packets are small fixed-size buffers, and the queue is capped, so memory stays bounded. The client sleeps in a single `poll()` call between readings (one wake-up per reading) and sends compact lines of about 9 bytes.

The companion client connects over TCP and emits simulated sensor readings in a lightweight line format (`temperature,humidity\n`, for example `23.38,48`). Both processes support deterministic, graceful shutdown on SIGINT and SIGTERM, woken instantly through an `eventfd` instead of a polling timeout.

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
- RAII Memory Scrubbing: The TelemetryPacket destructor zeroes the payload buffer with std::memset whenever a packet is destroyed (processed, dropped on overflow, or unwound by an exception).
- Reset-Safe Client Writes: The client sends with `send(..., MSG_NOSIGNAL)`, ignores SIGPIPE, and handles partial sends; a server that resets or closes the connection ends the client cleanly instead of killing it.
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
  connect (non-blocking, interruptible)
  loop:
    send_all("23.38,48\n")              ── partial sends handled, MSG_NOSIGNAL
    poll(eventfd, socket, interval)     ── one wake-up per reading
      ├─ timeout        → next reading
      ├─ eventfd        → Ctrl-C: stop
      └─ socket EOF/err → server gone: stop
```

## Project Structure

```
├── README.md        # Project documentation
├── LICENSE          # MIT License
├── deploy.sh        # Idempotent Linux dependency & build script
├── line_framer.hpp  # Newline/CRLF line framer (shared by server.cpp and tests)
├── server.cpp       # Epoll reactor, line framing, bounded pipeline, and jthread worker pool
├── client.cpp       # Simulated sensor telemetry publisher
└── tests.cpp        # Line-framer unit tests and basic sanity checks
```

## Quick Start

### Prerequisites

- Compiler: g++ 13 or newer with C++23 support (`-std=c++23`, `<expected>`, `<format>`, `std::jthread`, floating-point `std::from_chars`); verified with g++ 14.2. Other C++23 toolchains should work but are untested.
- OS: Linux only. Uses epoll (`epoll_create1`, `EPOLLET`, `EPOLLRDHUP`), `accept4` and `eventfd` with `EFD_NONBLOCK | EFD_CLOEXEC`, so kernel 2.6.28 or newer.
- Libraries: pthread (via `-pthread`); no third-party dependencies.

### Automated Setup & Build (deploy.sh)

deploy.sh is the single, idempotent entry point on Linux:

1. Inspects the system for g++ and make, installing only what is missing (build-essential on Debian/Ubuntu, gcc-c++ on Fedora/RHEL/yum). It does not check the compiler version, so on distributions whose default g++ is older than 13, install a newer g++ first.
2. Compiles server, client, and tests with -std=c++23 -O2 -pthread (make is checked for but not used).
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
g++ -std=c++23 -O2 -Wall -Wextra -DNDEBUG -pthread -o server server.cpp
g++ -std=c++23 -O2 -Wall -Wextra -DNDEBUG -pthread -o client client.cpp
g++ -std=c++23 -O2 -Wall -Wextra -pthread -o tests tests.cpp && ./tests

# Debug build (with Address & Undefined Behavior Sanitizers)
g++ -std=c++23 -g -O0 -Wall -Wextra -pthread -fsanitize=address,undefined -o server_debug server.cpp
```

### Running the Server & Client

Usage:
- `./server` takes no arguments and listens on TCP port 8883 on all IPv4 interfaces (`SERVER_PORT` in server.cpp).
- `./client [server_ipv4] [interval_ms]` defaults to `127.0.0.1` and `1000`. The interval must be an integer of at least 1; otherwise the client prints its usage and exits with code 2.

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
[SYSTEM] Shutting down. Draining 0 queued packet(s)...
[SYSTEM] Lines: valid=3 invalid=0 oversize_disconnects=0 unterminated_discarded=0
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

The worker count depends on the machine (2 × cores; 16 on an 8-core host). The client simulates readings (temperature 18–32 °C, humidity 30–80 %). The `fd` shown by the server is the connection's socket descriptor; descriptor numbers are reused after a client disconnects.

Press Ctrl-C in either terminal to trigger a graceful shutdown. The client exits with code 0 after Ctrl-C, 1 if it cannot connect or the server closes the connection, and 2 for invalid arguments.

## C++23 Language Dialect & Contracts

The codebase is compiled as C++23 and uses standard-library facilities for ownership and thread safety:

- std::jthread: Workers join automatically when the std::vector<std::jthread> is cleared or destroyed, so there are no manual join loops.
- std::unique_ptr<TelemetryPacket>: Each packet has exactly one owner as it moves from the reactor to a worker.
- std::expected<int, std::string>: The client's try_connect() returns the connected socket or an explanatory error string.
- std::from_chars: Locale-independent, allocation-free parsing of the interval argument and of each `temperature,humidity` reading.
- std::format: Fixed-precision formatting of readings and single-call log lines from worker threads.
- std::atomic: The stop flags and line counters use atomics and are safe to write from a signal handler or several workers.

## Ownership & Memory Scrubbing Flow

1. Reactor copies one complete line into a newly allocated std::unique_ptr<TelemetryPacket>.
2. Reactor moves ownership into TelemetryPipeline::push().
3. Worker thread receives exclusive ownership from TelemetryPipeline::pop().
4. When the packet is destroyed (after processing, when evicted by the drop-oldest policy, or during exception unwinding), ~TelemetryPacket() zeroes the payload buffer via std::memset before the memory returns to the allocator. Because a plain memset of memory that is about to be freed can legally be optimized away, use explicit_bzero() if scrubbing must be guaranteed. Partial-line buffers are not scrubbed.
5. On shutdown, workers drain the queue and the std::jthread pool is joined.

## Edge-Triggered & Overflow Contracts

- Edge-Triggered Drain Contract: Because sockets are registered with EPOLLET, every ready file descriptor must be drained until EAGAIN. Both accept4() and read() are wrapped in loops that exit only on EAGAIN/EWOULDBLOCK, end of stream or a hard error; failing to drain would silently lose subsequent data until the next edge transition.
- Hang-Up Contract: EPOLLRDHUP/EPOLLHUP often arrive together with EPOLLIN when a device sends its last reading and closes, so the socket is always drained before it is closed.
- Overflow Policy: TelemetryPipeline is capped at 10,000 packets. When full, the oldest packet is dropped before inserting the newest, prioritizing fresh telemetry while bounding memory without stalling the reactor. No back-pressure is applied to clients; dropped packets are not counted.
- Queue Wake-Up Policy: push() calls notify_one() to wake a single worker; shutdown() sets the stop flag under the queue mutex and calls notify_all(), so no worker can miss the wake-up.

## Detailed Shutdown Sequence

The reactor waits with no timeout, so shutdown is delivered through an eventfd registered in the epoll set. This costs one extra file descriptor and removes both periodic idle wake-ups and shutdown delay:

1. SIGINT/SIGTERM handler performs only async-signal-safe work: it stores g_running = false and write()s a uint64_t 1 to the eventfd (preserving errno).
2. The eventfd becomes readable, so epoll_wait returns immediately, no matter which thread received the signal. If the signal arrives just before epoll_wait is entered, the counter is already set and epoll_wait returns at once. If epoll_wait is interrupted with EINTR, the loop re-checks the flag.
3. The reactor stops reading, so no new lines are queued. pipeline->shutdown() sets the stop flag under the queue mutex, reports how many packets are still queued, and broadcasts notify_all().
4. Workers keep calling pop() and process every line that was already queued; pop() returns nullptr only when shutdown was requested and the queue is empty.
5. workers.clear() joins the std::jthread pool, the line counters are printed, and the listening socket, epoll fd, eventfd and spare descriptor are closed. Client sockets (and any partial lines they hold) are closed by the kernel at process exit.

Draining is unbounded in time: with a large backlog and slow processing, shutdown takes as long as the workers need to finish the queue (at most 10,000 packets). Process managers such as systemd (TimeoutStopSec, 90 s by default) or Kubernetes (terminationGracePeriodSeconds, 30 s by default) send SIGKILL if that limit is exceeded.

The client uses the same eventfd doorbell: its poll() wakes immediately on Ctrl-C, including while it is connecting or waiting for a full socket buffer to drain.

## Testing, Troubleshooting & Production Hardening

### Unit Testing (tests.cpp)

tests.cpp links against line_framer.hpp (the same framer the reactor uses) and checks that a 128-byte LF line is accepted, a 128-byte CRLF line is accepted, a 129-byte line is rejected, a CRLF terminator split across two reads (`\r` then `\n`) is accepted at 128 bytes, and blank lines are skipped. It also keeps a few sanity checks: the atomic stop flag starts true and can be cleared, a polling thread exits when the flag is cleared, and a sample `temperature,humidity` line has the expected format. It does not include server.cpp or client.cpp, so it does not exercise the epoll reactor, worker validation or shutdown path; those are verified by running the server and client together.

### Troubleshooting

| Symptom | Likely Cause & Fix |
|---------|--------------------|
| Address already in use | Another process holds port 8883 (`ss -tulpn \| grep 8883`). Stop it or change SERVER_PORT. The server exits with code 1. |
| Client cannot connect | Confirm ./server is running and local firewall rules (ufw/iptables) allow TCP traffic on port 8883. |
| Client exits with "Invalid interval" | The interval must be an integer of at least 1 ms, e.g. `./client 127.0.0.1 500`. |
| "Invalid reading ... dropped" | The line is not `temperature,humidity` with temperature in -40..125 and humidity an integer in 0..100. |
| "Line longer than 128 bytes ... disconnecting" | The sender is not using newline-terminated lines or sent an oversized line. |
| "Discarded N-byte unterminated line" | The connection closed in the middle of a line; terminate every reading with `\n`. |
| Ctrl-C takes a long time on the server | Workers are draining a large backlog. Each line already queued is processed before exit. |
| "Out of file descriptors; rejected a connection" | The process hit its descriptor limit (`ulimit -n`); raise it or reduce concurrent clients. |
| High idle CPU usage | Ensure epoll_wait is not called with a zero timeout (it should block with -1) and that accept4()/read() loops exit cleanly on EAGAIN. |

### Scaling & Production Hardening

- Horizontal Scaling: Run multiple server processes behind a load balancer, or set SO_REUSEPORT (not set today) and adopt a multi-reactor design (one epoll instance per core) for extreme connection rates.
- Security & Observability: Wrap sockets in TLS (OpenSSL/mbedTLS), add device authentication, redirect stdout/stderr to journald or a log daemon, and expose the atomic counters (plus accepted connections, dropped packets, queue depth) to Prometheus.
- Possible extensions: Add an outbound write buffer for server-to-client responses, identify devices by an ID instead of the socket descriptor, persist telemetry to disk or a time-series database, and scrub partial-line buffers.

## Limitations

This is a compact teaching and reference implementation:

- No TLS and no authentication: traffic is plain TCP on port 8883 (the port conventionally used for MQTT over TLS), and any client may send readings.
- Devices are identified only by the server-side socket descriptor, which is reused after disconnects.
- Readings are validated and logged but not stored or forwarded.
- Dropped packets (queue overflow) are not counted or reported, and every processed line is logged to stdout.
- Client: IPv4 only, port fixed at 8883, no reconnect logic, simulated sensor values.
- The server never writes to clients, so it needs no SIGPIPE handling; the client handles it as described above.

## Scripts

deploy.sh checks for g++ and make, installs any missing packages, builds the three binaries (server, client, tests) with -std=c++23 -O2 -pthread, and runs the tests. See [Automated Setup & Build](#automated-setup--build-deploysh) for details.

Example:

```bash
./deploy.sh
```

## Further Operational Notes

The server listens on TCP port 8883 on all IPv4 interfaces. A failed bind almost always indicates that another process already owns the port; ss -tulpn | grep 8883 identifies the occupant.

All diagnostic output appears on standard output and standard error. In production these streams should be captured by the system logging facility. The reactor thread only accepts, reads, splits lines and enqueues; it allocates one TelemetryPacket per complete line and grows a small per-connection string only while a line is incomplete.

The worker count is computed as twice the hardware concurrency reported by the standard library, with a fallback of four threads when the query returns zero. This sizing provides enough concurrency to absorb short processing bursts while remaining modest for a light workload.

## License

This project is licensed under the [MIT License](LICENSE). Copyright (c) 2026 Damian Andres Ulanowicz.
