# IoT Telemetry Server

A high-performance, low-CPU telemetry ingestion server and client written in modern C++23 for edge IoT deployments.

Built on the classic Reactor pattern, a single server thread demultiplexes non-blocking socket I/O using Linux epoll (edge-triggered) and dispatches complete payloads to a static worker thread pool via a bounded, thread-safe queue. Decoupling I/O from processing enables the server to ingest thousands of concurrent sensor streams with near-zero idle CPU usage.

The companion client connects over TCP and emits simulated sensor readings in a lightweight line format (temperature,humidity). Both processes support deterministic, graceful shutdown on SIGINT and SIGTERM.

## Key Features

- Edge-Triggered epoll Reactor: Single-threaded I/O multiplexing with EPOLLET and EPOLLRDHUP for near-zero idle CPU usage (~0%).
- Non-Blocking Accept & Read Loops: Exhaustive EAGAIN drain loops ensure the reactor never stalls on slow connections.
- Static Worker Thread Pool: Sized to 2 × std::thread::hardware_concurrency() to absorb bursty I/O workloads without dynamic thread creation overhead.
- Bounded Queue with Back-Pressure: Capped at 10,000 packets with a drop-oldest eviction policy to prevent unbounded memory growth under load.
- RAII Memory Scrubbing: TelemetryPacket destructors automatically zero out payload buffers via std::memset upon scope exit—even if an exception is thrown.
- Modern C++23 Primitives: Uses std::jthread, std::expected, std::span, and std::atomic with explicit memory ordering for safe resource management and explicit error handling.

## System Architecture

### 1. Reactor & Worker Pool Model

Instead of blocking one thread per client, one reactor thread monitors all file descriptors and hands off packet ownership (std::unique_ptr) to background workers.

```
MAIN THREAD (Reactor)
  g_running = true
  install SIGINT/SIGTERM handlers
  spawn std::jthread worker pool
  ┌─────────────────────────────────────────┐
  │ while (g_running)                       │
  │   epoll_wait(..., 500 ms)               │ ◄── ~0% CPU while waiting
  │   handle accept / read / hangups        │
  └─────────────────────────────────────────┘
  g_running = false
  pipeline->shutdown() ──► notify_all()
  workers join automatically (std::jthread)

WORKER THREADS
  ┌─────────────────────────────────────────┐
  │ while (g_running)                       │
  │   packet = pipeline->pop()              │ ◄── Blocks on condition_variable
  │   if (!packet) break                    │ ◄── Woken by shutdown()
  │   process payload & scrub memory (RAII) │
  └─────────────────────────────────────────┘
```

### 2. Reactor State Machine

The reactor never blocks on application work; all CPU-heavy or blocking operations live in the worker pool.

```
          ┌──────────────┐
          │   epoll_wait │◄────────────────┐
          └──────┬───────┘                 │
                 │ events                  │
     ┌───────────┼───────────┐             │
     ▼           ▼           ▼             │
  accept      EPOLLIN     hang-up          │
  (drain)     (drain      (DEL + close)    │
              read,                        │
              push)                        │
                 │                         │
                 └─────────────────────────┘
```

1. Setup: Create a non-blocking TCP listening socket, initialize an epoll instance, and register the socket with EPOLLIN | EPOLLET.
2. Wait: epoll_wait() blocks until at least one registered file descriptor becomes ready or the 500 ms timeout expires.
3. Dispatch:
   - Listening Socket Ready: Drain accept() in a while (true) loop until EAGAIN. Configure each accepted client socket as non-blocking and register it with EPOLLIN | EPOLLET | EPOLLRDHUP.
   - Socket Error / Hang-Up: Remove the descriptor from epoll (EPOLL_CTL_DEL) and close it immediately.
   - Socket Readable (EPOLLIN): Drain read() completely until EAGAIN, package the bytes into a TelemetryPacket, and push it to TelemetryPipeline.
4. Loop: Return to epoll_wait().

### 3. Producer-Consumer Pipeline & Client Architecture

```
SERVER PIPELINE:
Reactor Thread                  Worker Threads
     │                               │
     │  read() payload               │
     │  pipeline->push() ──────────► │  pop() (blocks until data or shutdown)
     │  (O(1) non-blocking)          │  process & scrub payload
     │                               │  loop

CLIENT ARCHITECTURE:
┌─────────────────────────────┐     ┌──────────────────────────────┐
│  Telemetry Worker (Main)    │     │  Network I/O (No Reactor)    │
│                             │     │                              │
│  Every interval (default 1s)│     │  Simple blocking send()      │
│    Build sensor line    │────►│  Short sleep slices for fast │
│    send()                   │     │  SIGINT/SIGTERM response     │
└─────────────────────────────┘     └──────────────────────────────┘
```

## Project Structure

```
├── README.md     # Project documentation
├── LICENSE       # MIT License
├── deploy.sh     # Idempotent Linux dependency & build script
├── server.cpp    # Epoll reactor, bounded pipeline, and jthread worker pool
├── client.cpp    # Simulated sensor telemetry publisher
└── tests.cpp     # Deterministic unit tests for shutdown and packet formatting
```

## Quick Start

### Prerequisites

- Compiler: C++23-capable compiler (g++ 12+ or clang++ 16+)
- OS: Linux (Kernel 2.6.17+ for epoll and EPOLLRDHUP). Note: The server is Linux-specific; the client is portable across POSIX systems with minor header adjustments.
- Libraries: pthread

### Automated Setup & Build (deploy.sh)

deploy.sh is the single, idempotent entry point on Linux:

1. Inspects the system for g++ and make, installing only what is missing (build-essential on Debian/Ubuntu, gcc-c++ on Fedora/RHEL).
2. Compiles server, client, and tests with -std=c++23 -O2 -pthread.
3. Runs the test binary to verify the build.

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

Terminal 1 (Start Server):

```bash
./server
```

```
[SYSTEM] Starting 8 worker threads.
[REACTOR] Listening on port 8883 (Ctrl-C to stop)
[REACTOR] Accepted fd=5
[WORKER] Processed fd=5 len=18
```

Terminal 2 (Start Client):

Parameters: ./client [server_ip] [interval_ms] (defaults: 127.0.0.1, 1000 ms)

```bash
./client 127.0.0.1 1000
```

```
[CLIENT] Connecting to 127.0.0.1:8883...
[CLIENT] Connected. Interval=1000 ms. Ctrl-C to stop.
[CLIENT] Sent: 24.5123,55
[CLIENT] Sent: 29.1034,62
```

Press Ctrl-C in either terminal to trigger a graceful shutdown.

## C++23 Language Dialect & Contracts

The codebase targets C++23 and leverages standard-library facilities to enforce memory safety and clear ownership:

- std::jthread: Workers join automatically when the std::vector<std::jthread> is destroyed, eliminating manual join loops and preventing forgotten joins.
- std::expected<void, std::string>: Fallible setup helpers (ensure_non_blocking, try_connect) return explicit success or an explanatory error string without out-parameters or scattered errno checks.
- std::span<const uint8_t>: Provides non-owning, bounds-aware views over packet buffers instead of raw pointer-and-length pairs.
- std::atomic: Uses explicit memory orders on the stop flag to guarantee cross-core visibility without data races.

## Ownership & Memory Scrubbing Flow

1. Reactor allocates std::unique_ptr<TelemetryPacket>.
2. Reactor fills the payload and moves ownership into TelemetryPipeline::push().
3. Worker thread receives exclusive ownership from TelemetryPipeline::pop().
4. On scope exit, ~TelemetryPacket() zeros the entire raw buffer via std::memset before memory returns to the allocator—guaranteeing sensitive bytes are cleared even if an exception is thrown.
5. std::jthread destructor automatically joins the worker on shutdown.

## Edge-Triggered & Back-Pressure Contracts

- Edge-Triggered Drain Contract: Because sockets are registered with EPOLLET, every ready file descriptor must be drained until EAGAIN. Both accept() and read() are wrapped in while (true) loops that exit only on EAGAIN/EWOULDBLOCK or a hard error; failing to drain would silently lose subsequent data until the next edge transition.
- Back-Pressure Policy: TelemetryPipeline is capped at 10,000 packets. When full, the oldest packet is dropped before inserting the newest sample—prioritizing fresh telemetry values while bounding memory without stalling the reactor.
- Queue Wake-Up Policy: push() calls notify_one() to wake a single worker and minimize context switches; shutdown() calls notify_all() to unblock all waiting threads simultaneously.

## Detailed Shutdown Sequence

The 500 ms epoll_wait timeout caps shutdown latency at half a second while avoiding the overhead of an extra eventfd:

1. SIGINT/SIGTERM handler executes an async-signal-safe store (g_running = false).
2. Next epoll_wait returns (via timeout or EINTR).
3. Reactor loop condition evaluates to false and exits.
4. pipeline->shutdown() sets the internal stop flag and broadcasts notify_all().
5. Workers blocked in pop() wake, receive nullptr, and exit their loops.
6. std::vector<std::jthread> destruction automatically joins every worker.
7. Listening socket and epoll file descriptor are closed.

## Testing, Troubleshooting & Production Hardening

### Unit Testing (tests.cpp)

tests.cpp validates the deterministic stop-flag contract, worker thread teardown, and payload formatting helpers without requiring live network sockets.

### Troubleshooting

| Symptom | Likely Cause & Fix |
|---------|--------------------|
| Address already in use | Another process holds port 8883. Stop the conflicting process or update SERVER_PORT. |
| Client cannot connect | Confirm ./server is running and local firewall rules (ufw/iptables) allow TCP traffic on port 8883. |
| High idle CPU usage | Ensure epoll_wait uses a non-zero timeout (500 ms) and that accept()/read() loops exit cleanly on EAGAIN. |

### Scaling & Production Hardening

- Horizontal Scaling: Run multiple server processes behind a load balancer (SO_REUSEPORT), or adopt a multi-reactor design (one epoll instance per core) for extreme connection rates.
- Security & Observability: Wrap sockets in TLS (OpenSSL/mbedTLS), add device authentication, redirect stdout/stderr to journald or a log daemon, and expose std::atomic metrics (accepted connections, dropped packets, queue depth) to Prometheus.
- Possible extensions: Add an AsyncWriteBuffer for server-to-client responses, adopt length-prefixed binary framing, use an eventfd for instant shutdown wake-ups, and persist telemetry to disk or a time-series database.

## Scripts

deploy.sh checks for g++ and make, installs any missing packages, builds the three binaries with -std=c++23 -O2 -pthread, and runs the tests.

Example:

```bash
./deploy.sh
```

## Further Operational Notes

The server listens on TCP port 8883. A failed bind almost always indicates that another process already owns the port; ss -tulpn | grep 8883 identifies the occupant. The client accepts an optional IP address and an optional interval in milliseconds; both default to values suitable for local testing.

All diagnostic output appears on standard output and standard error. In production these streams should be captured by the system logging facility. The reactor thread performs no heap allocation after start-up except for the TelemetryPacket objects that are immediately moved into the pipeline, keeping its latency predictable.

The worker count is computed as twice the hardware concurrency reported by the standard library, with a fallback of four threads when the query returns zero. This sizing provides enough concurrency to absorb short processing bursts while remaining modest for a pure I/O-bound workload.


**License**

This project is licensed under the [<u>MIT License</u>](https://github.com/mayday5000/IOT-TelemetryServer/blob/main/LICENSE). Copyright (c) 2026 Damian Andres Ulanowicz.
