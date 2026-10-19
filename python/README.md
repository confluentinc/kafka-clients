# confluent_kafka (Rust-core Python client)

The Python client built on the Rust core's C FFI. Its surface is the Java Kafka
client (Apache Kafka 4.4.0) mapped to idiomatic Python, ruled by the
`## Python Binding Conventions` section of the repository's `CLAUDE.md`. The
package sits over a hand-written CPython extension, `_confluentkafka`, which
links the Rust library's C API (`../rust`, feature `ffi`).

## Status

**In Development:** the API is subject to large changes and the stability is not guaranteed.

## Import paths

The package mirrors Java's package tree with the `clients` segment dropped
(`CLAUDE.md`, Python Binding Conventions, Modules). Import each name from its own
module — the package root does **not** re-export the everyday client names; it
exports only `Duration` and the Java built-in exception classes:

```python
from confluent_kafka.producer import (
    KafkaProducer, MockProducer, AsyncKafkaProducer, AsyncMockProducer,
    ProducerRecord, RecordMetadata, Callback,
)
from confluent_kafka.admin import Admin, AsyncAdmin, NewTopic
from confluent_kafka.consumer import (
    KafkaConsumer, MockConsumer, AsyncKafkaConsumer, AsyncMockConsumer,
    ConsumerRebalanceListener, OffsetAndMetadata, CloseOptions,
    SubscriptionPattern,
)
from confluent_kafka.common import (
    TopicPartition, TopicIdPartition, Node, PartitionInfo, Uuid,
    MetricName, Metric, KafkaMetric, TimestampType, Headers,
    KafkaError,  # Java's org.apache.kafka.common.KafkaException
    KafkaFuture, AsyncKafkaFuture,  # every future the package hands out
)
from confluent_kafka.common.errors import RetriableError  # typed hierarchy
from confluent_kafka.common.serialization import (
    bytes_serializer, string_serializer, int_serializer, json_serializer,  # etc.
)

# The Java built-in exception classes and the Duration alias live at the
# package root:
from confluent_kafka import (
    IllegalArgumentError, IllegalStateError, ConcurrentModificationError,
    TimeoutError, NoSuchElementError, ExecutionError, Duration,
)
```

The native extension is `_confluentkafka` (private): the package imports it; it
is never a public import path.

## Async peers

Each async client lives in the same module as its sync peer, `Async`-prefixed
(`AsyncKafkaProducer`, `AsyncKafkaConsumer`). A method is `async def` on the
async class iff Java waits in it (on the background thread, the network, or a
rebalance listener it runs); the others (`assignment()`, `subscription()`,
`metrics()`, `group_metadata()`, `wakeup()`) stay plain `def` on both.
`commit_nowait()` is `async def` on the async consumers, as every method whose
entry point has a `_cb` twin: `_nowait` means the call does not wait for the
broker's answer, so `await consumer.commit_nowait(callback=cb)` returns once the
commit is sent (raising the call's own failure) and `cb` gets the result later.

## Futures

One future type for every future the package hands out: `KafkaFuture[T]` on a
sync client, `AsyncKafkaFuture[T]` on an async one (both in
`confluent_kafka.common`), with Java's `KafkaFuture` methods (`get`, `get_now`,
`then_apply`, `when_complete`, `all_of`, `cancel`, `is_done`, …).

```python
md = producer.send(record=r).get()                 # sync
md = await (await producer.send(record=r)).get()   # async: send() and get() are awaited
```

`get()` on a failed future raises `confluent_kafka.ExecutionError`, as Java's
`ExecutionException`, and the real error is its `__cause__`:

```python
try:
    producer.send(record=r).get()
except ExecutionError as e:
    if isinstance(e.__cause__, RetriableError): ...
```

A `get(timeout=…)` that runs out raises `TimeoutError`. A future's
`then_apply` functions and `when_complete` actions run where the completion
runs: on the producer's completion thread or the sync admin client's pump
thread, on the event loop for an async client.

## Producer

- `send(record=…, callback=cb)` returns a `KafkaFuture` for the record's
  `RecordMetadata` (`.get()` waits for it); like Java's, it cannot be cancelled,
  and a send failure reaches `get()` as `ExecutionError`. A serializer is called
  with a `None` key or value as given and decides what null maps to. `cb(metadata,
  exception)` is Java's `Callback`: it runs on the producer's background
  completion thread (`KafkaProducer`) or on the event loop
  (`AsyncKafkaProducer`), one at a time in completion order, before the future
  completes; a raising callback fails nothing, and the client's core logs its
  exception once, with Java's message. `metadata` is never `None`: on
  failure every field but the topic and partition is -1.
- `close()` waits for the records sent before it; `close(timeout=…)` waits at
  most `timeout`, then fails what is left. Neither cancels a future.
- A `send()` that returned belongs to the `flush()` or transaction call after it
  (`commit_transaction()` includes it, `abort_transaction()` discards it).
- `MockProducer` is Java's `MockProducer`: sends complete, and callbacks run, on
  the calling thread, and a raising callback's exception propagates out of the
  call that completed the record.

## Consumer

- The calls that wait take Java's `Duration` overloads as an optional keyword:
  `commit(timeout=…)`, `position(…, timeout=…)`, `committed(…, timeout=…)`,
  `partitions_for(…, timeout=…)`, `list_topics(timeout=…)`,
  `offsets_for_times(…, timeout=…)`, `beginning_offsets(…, timeout=…)` and
  `end_offsets(…, timeout=…)`; omitted or `None` calls the overload without it
  (`default.api.timeout.ms` applies). A negative timeout raises
  `IllegalArgumentError`.
- `poll(timeout=…)` runs the deserializers on the caller's thread. A null key or
  value is not deserialized: the record's field is `None`. A failing one
  raises `RecordDeserializationError` (after the records before it, if any, were
  returned) and leaves the position at the record:
  `seek(partition=e.topic_partition(), offset=e.offset() + 1)` skips it.
- `commit()` is Java's `commitSync` and waits; `commit_nowait(callback=cb)` is
  Java's `commitAsync`. `cb(offsets, exception)` is Java's
  `OffsetCommitCallback`; `offsets` is `None` when the commit failed and for
  an explicit empty `offsets`, as Java passes `null`.
- The `ConsumerRebalanceListener` methods and the `commit_nowait()` callback run
  on the caller's thread, inside the call that delivers them (`poll()`,
  `commit()`, `commit_nowait()`, `unsubscribe()`, `close()`, …), and the
  rebalance does not advance until the listener returns. A listener may call
  back into its consumer (`commit()`, `seek()`, `position()`, …); on
  `AsyncKafkaConsumer` those calls block the event loop for their duration.
  On `AsyncKafkaConsumer` a listener method may be `async def`; it is awaited
  on the event loop. A listener's `KafkaError` is raised as it is from the
  call that delivered it; any other exception is wrapped as
  `KafkaError("User rebalance callback throws an error")` with it as the
  cause.
- `KafkaConsumer` is not thread-safe: a call while another thread is inside it
  raises `ConcurrentModificationError`. `wakeup()` is the exception: from any
  thread, it makes the waiting call raise `WakeupError`.
- `close()` closes with Java's default timeout; `close(option=CloseOptions(timeout=…))`
  bounds it. Java's deprecated `close(Duration)` is not generated.
- A deserializer defaults to the `key.deserializer` / `value.deserializer`
  config key, else to `bytes_deserializer()`; the consumer closes both when it
  closes.
- `MockConsumer` is Java's `MockConsumer`: `add_record(record=…)` takes the
  `ConsumerRecord` the test builds and `poll()` returns it as is (the mock has no
  deserializers). Nothing binds its type parameters, so annotate it as Java
  writes them: `c: MockConsumer[str, str] =
  MockConsumer(offset_reset_strategy="earliest")`.

## Admin

`Admin.create(configs={...})` returns a `KafkaAdminClient` (`AsyncAdmin.create` an
`AsyncKafkaAdminClient`); the client is a context manager, and leaving the block
closes it. The package is `confluent_kafka.admin`: the client, one `…Options` per
call, one `…Result` per call, and the value types (`NewTopic`, `TopicDescription`, …).

```python
from confluent_kafka import ExecutionError
from confluent_kafka.admin import Admin, NewTopic
from confluent_kafka.common.errors import TopicExistsError

with Admin.create(configs={"bootstrap.servers": "localhost:9092"}) as admin:
    result = admin.create_topics(
        new_topics=[NewTopic(name="demo", num_partitions=3, replication_factor=1)])
    try:
        result.all().get()                  # one future per topic: result.values()
    except ExecutionError as e:
        if not isinstance(e.__cause__, TopicExistsError):
            raise
```

- Every call returns its `…Result` at once, as Java's does; the Result holds one
  `KafkaFuture` per key (or one for the whole call) and `.get()` waits. A failed key
  raises `ExecutionError`, whose `__cause__` is the error as its class.
- The sync client has one daemon pump thread that completes its futures, as Java's
  admin thread does: `then_apply` functions and `when_complete` actions run there,
  so a blocking action blocks the client's other completions.
- On the async peer the calls are plain `def` too, and the Result holds
  `AsyncKafkaFuture`s, whose `get()` is awaited; their actions run on the event loop:

  ```python
  from confluent_kafka.admin import AsyncAdmin, NewTopic

  async with AsyncAdmin.create(configs={"bootstrap.servers": "localhost:9092"}) as admin:
      result = admin.create_topics(new_topics=[NewTopic(name="demo", num_partitions=3, replication_factor=1)])
      await result.all().get()
  ```

- Errors are classes: `isinstance(e.__cause__, RetriableError)`.
- `MockAdminClient` (and `AsyncMockAdminClient`) is Java's `MockAdminClient`, for tests: no
  broker, in-memory state, `MockAdminClient.create()...build()` (`MockAdminClient()` has one `Node.no_node()` broker, as Java's, so topic calls need real brokers). Where Java's mock
  throws "Not implemented yet", it raises `UnsupportedVersionError`.
- Overloads whose entry point the core lacks (streams and share groups, metadata quorum, …)
  are not generated; they are listed in the docstring of `confluent_kafka.admin.admin`.

## Errors

Errors are a typed hierarchy caught by class, not a flat error with a `code()`.
There is no `code()` / `is_retriable()` / `is_fatal()` on the surface —
retriability is `except RetriableError`, and each Java exception class has a
`…Error` Python analog in the module of its Java package (generated from the
Java sources). The Java built-in exception classes (`IllegalStateError`,
`IllegalArgumentError`, `ConcurrentModificationError`, `TimeoutError`,
`NoSuchElementError`, `ExecutionError`) live at the package root.

## Directory Structure

```
confluent_kafka/        # The client package, one module per Java package
_confluentkafka.c       # Hand-written CPython extension over the C API
tinycthread.{c,h}, c11threads_compat.h  # C11 threads shim (macOS)
setup.py, pyproject.toml
grpc_server.py, grpc_server_async.py, grpc_translate.py
                        # gRPC servers for the multilanguage tests
Dockerfile.grpc, Dockerfile.grpc.async  # Their images
examples/               # Runnable producer, consumer and admin examples
test/
├── unit/               # pytest unit tests, no broker needed
├── integration/        # Broker-backed binding tests (testcontainers; skip without Docker)
├── static/             # Source checks: format arity
└── performance/        # Latency-budget and benchmark tests (need a broker)
tools/                  # format_arity.py
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
python -m pytest test/integration   # broker-backed tests (Docker)
python -m pytest test/static    # static checks of the extension source
python -m pytest soak/test      # soak client unit tests
python -m mypy --strict confluent_kafka test/unit/test_typing.py   # typecheck
```

The static checks read source files only, so they need neither the Rust
library nor the extension built.

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
make test                 # typecheck, unit, integration, static and soak tests, then test-integration
make test-unit            # unit tests only
make typecheck            # mypy --strict over the package and test/unit/test_typing.py
make check-static         # static checks only
make test-integration     # gRPC images + the __grpc_python arms (Linux, Docker)
make test-performance     # latency-budget tests (idle machine, broker needed)
make consumer-perf-test producer-perf-test   # env-driven benchmarks
make grpc-image grpc-image-async
make clean
```

`PROFILE` (`release` by default, or `debug`) selects which Rust build to link.
`CLIENT_VERSION=2` runs the benchmarks against the PyPI librdkafka client in
its own venv (`make init-venv-librdkafka` at the root): it installs the same
top-level `confluent_kafka` package as this client, so the two cannot share one.

## License

Apache License 2.0. See [LICENSE](../LICENSE) for details.
