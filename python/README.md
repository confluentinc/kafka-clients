# Python Binding

Python bindings for the Rust Kafka client (`confluent-kafka-rust-python`). The
modules sit over a hand-written CPython extension, `_confluentkafka`, which
links the Rust library's C API (`../rust`, feature `ffi`).

- **Modules:** `producer.py`, `consumer.py` and `admin.py`, mirroring the Java
  client's producer, consumer and admin APIs.
- **Two shapes of each client:** a synchronous class (`KafkaProducer`, whose
  sends return `concurrent.futures.Future`; `KafkaConsumer`, whose operations
  block like Java's; `AdminClient`), and an asyncio class whose methods are
  coroutines (`AsyncKafkaProducer`, `AsyncKafkaConsumer`, `AsyncAdminClient`).
- **Mocks:** `MockProducer`, `MockConsumer`, `MockAdminClient` and their async
  variants, backed by the Rust client's in-memory mocks.

## Consumer threading model

The consumer binding sits directly over the C API's two entry-point families
and has no thread of its own:

- **`KafkaConsumer` / `MockConsumer` (sync)** call the *blocking* C functions
  with the GIL released. Every method that blocks in Java blocks here
  (`poll`, `commit`, `position`, `committed`, `seek`, `subscribe`, `close`,
  ...). A rebalance listener (`subscribe(topics, listener=...)`) or a
  `commit_async(callback=...)` completion callback runs **on the calling
  thread, inside the call that triggers it**, and that call does not return
  until the callback has -- Java's "callbacks run on the polling thread"
  guarantee. A callback that raises fails the triggering operation with a
  `KafkaError` carrying its message. The Rust consumer is single-owner: a
  second thread calling into the consumer while an operation is in flight gets
  a `KafkaError` (LocalConcurrentModification), and the non-blocking state
  reads (`assignment()`, `subscription()`, `paused()`) return empty
  collections meanwhile. Use `wakeup()` from another thread to abort a
  blocking call (it fails with a `Wakeup` error); `Ctrl-C` only takes effect
  once the native call returns.
- **`AsyncKafkaConsumer` / `AsyncMockConsumer` (asyncio)** call the `_cb`
  twins. The operation runs on the Rust runtime and queues its completion --
  and every listener / commit-callback invocation it triggers -- on the
  client's callback queue. The client's notify hook fires once each time that
  queue goes from empty to non-empty, and the binding schedules the pump
  (`Consumer_execute_callbacks`) on the event loop with
  `call_soon_threadsafe`, so **every callback runs on the loop thread**.
  Listener methods and commit callbacks may therefore be coroutines: a
  coroutine listener method is scheduled as a task and the rebalance does not
  advance until it finishes (its exception, if any, is reported to the client
  as the listener's failure). `commit_async` and `seek` are coroutines too,
  because the Rust consumer awaits its background task for them.
- **Re-entrancy.** A callback that must reach back into its consumer (e.g.
  `commit_sync` from `on_partitions_revoked`) does so through a
  `ConsumerHandle` obtained beforehand from `consumer.handle()`
  (`AsyncConsumerHandle`, with coroutine methods, on the asyncio consumer).
  Handle operations bypass the single-owner guard; the consumer's own methods
  would be rejected as concurrent access. On a mock-derived handle only
  `wakeup()` works -- drive the `MockConsumer` directly.
- **Records.** `record.key` / `record.value` are `bytes` (or `None`), copied
  out of the record batch when the record object is created, so they outlive
  the `ConsumerRecords` that produced them. `record.headers` is a
  `list[(str, bytes | None)]`.
- **Mock usage.** `MockConsumer` mirrors Java's: `add_record`,
  `update_beginning_offsets` / `update_end_offsets` / `update_duration_offsets`,
  `update_partitions`, `set_poll_error` / `set_offsets_error`,
  `set_max_poll_records`, `should_rebalance` / `reset_should_rebalance`,
  `last_poll_timeout`, and `rebalance(partitions)`, which drives the registered
  listener (a topic subscription is required). `AsyncMockConsumer.rebalance`
  is a coroutine.

## Status

**In Development:** the API is subject to large changes and the stability is not guaranteed.


## Directory Structure

```
producer.py, consumer.py, admin.py   # The Python API
_confluentkafka.c       # Hand-written CPython extension over the C API
_error_code.py          # Generated from ../rust/src/ffi/common.rs
tinycthread.{c,h}, c11threads_compat.h  # C11 threads shim (macOS)
setup.py, pyproject.toml
grpc_server.py, grpc_server_async.py, grpc_translate.py
                        # gRPC servers for the multilanguage tests
Dockerfile.grpc, Dockerfile.grpc.async  # Their images
test/
├── unit/               # pytest unit tests, no broker needed
├── static/             # Source checks: format arity, _error_code.py staleness
└── performance/        # Latency-budget and benchmark tests (need a broker)
tools/                  # format_arity.py, generate_error_code.py
soak/                   # Long-running soak client — see soak/README.md
Makefile                # Targets the repository root's Makefile delegates to
```

## Where commands run

Run the commands below **from this directory**, inside a Python environment
(3.10+). The repository root's `make` targets use the shared `../venv/`,
which `make init` at the root creates.

## Prerequisites

The extension links the Rust library and includes its C header, so build the
Rust client first:

```bash
make -C ../rust build-all-features   # or: cd ../rust && cargo build --all-features --release
```

This produces `../rust/target/release/libconfluent_kafka.*` and
`../rust/target/include/confluent_kafka.h`. `setup.py` finds them there;
set `CONFLUENT_KAFKA_LIB_DIR` to link a library from another directory
(e.g. `../rust/target/debug`).

## Building

```bash
pip install -e .          # builds _confluentkafka in place
pip install .[dev]        # test and benchmark dependencies
```

## Running Tests

```bash
python -m pytest test/unit      # unit tests
python -m pytest test/static    # static checks of the extension source
python -m pytest soak/test      # soak client unit tests
```

The static checks read source files only, so they need neither the Rust
library nor the extension built. When `../rust/src/ffi/common.rs` changes,
regenerate the error codes with `python tools/generate_error_code.py`.

The Python arms of the multilanguage integration suite (`__grpc_python`,
`__grpc_python_async`) run the Rust test harness against the gRPC servers
above; `make test-integration` builds the images and runs them (Linux only,
Docker required).

## Make targets

`Makefile` wraps the commands above. The repository root's Makefile delegates
to it (activating the venv and building the Rust library first); it can also
be run directly:

```bash
make build                # pip install -e . against ../rust/target/$(PROFILE)
make test                 # unit, static and soak tests, then test-integration
make test-unit            # unit tests only
make check-static         # static checks only
make test-integration     # gRPC images + the __grpc_python arms (Linux, Docker)
make test-performance     # latency-budget tests (idle machine, broker needed)
make consumer-perf-test producer-perf-test   # env-driven benchmarks
make grpc-image grpc-image-async
make clean
```

`PROFILE` (`release` by default, or `debug`) selects which Rust build to link.

## License

Apache License 2.0. See [LICENSE](../LICENSE) for details.
