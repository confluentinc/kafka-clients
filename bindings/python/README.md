# confluent_kafka (Rust-core Python client)

The Python client built on the Rust core's C FFI. Its surface is the Java Kafka
client (Apache Kafka 4.3.1) mapped to idiomatic Python, ruled by the
`## Python Binding Conventions` section of the repository's `CLAUDE.md`.

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
from confluent_kafka.consumer import (
    KafkaConsumer, MockConsumer, AsyncKafkaConsumer, AsyncMockConsumer,
    ConsumerRebalanceListener, OffsetAndMetadata, CloseOptions,
    SubscriptionPattern,
)
from confluent_kafka.common import (
    TopicPartition, TopicIdPartition, Node, PartitionInfo, Uuid,
    MetricName, Metric, KafkaMetric, TimestampType, Headers,
    KafkaError,  # Java's org.apache.kafka.common.KafkaException
)
from confluent_kafka.common.errors import RetriableError  # typed hierarchy
from confluent_kafka.common.serialization import (
    bytes_serializer, string_serializer, int_serializer, json_serializer,  # etc.
)

# The Java built-in exception classes and the Duration alias live at the
# package root:
from confluent_kafka import (
    IllegalArgumentError, IllegalStateError, ConcurrentModificationError,
    TimeoutError, NoSuchElementError, NullPointerError, Duration,
)
```

The native extension is `_confluentkafka` (private): the package imports it; it
is never a public import path.

## Async peers

Each async client lives in the same module as its sync peer, `Async`-prefixed
(`AsyncKafkaProducer`, `AsyncKafkaConsumer`). A method is `async def` on the
async class iff Java waits in it (on the background thread, the network, or a
rebalance listener it runs); the others (`assignment()`, `subscription()`,
`metrics()`, `group_metadata()`, `commit_nowait()`, `wakeup()`) stay plain
`def` on both.

## Producer

- `send(record=…, callback=cb)` returns a future for the record's
  `RecordMetadata`; like Java's, it cannot be cancelled. `cb(metadata,
  exception)` is Java's `Callback`: it runs on the producer's background
  completion thread (`KafkaProducer`) or on the event loop
  (`AsyncKafkaProducer`), one at a time in completion order, before the future
  completes; a raising callback is logged. `metadata` is never `None`: on
  failure every field but the topic and partition is -1.
- `close()` waits for the records sent before it; `close(timeout=…)` waits at
  most `timeout`, then fails what is left. Neither cancels a future.
- A `send()` that returned belongs to the `flush()` or transaction call after it
  (`commit_transaction()` includes it, `abort_transaction()` discards it).
- `MockProducer` is Java's `MockProducer`: sends complete, and callbacks run, on
  the calling thread.

## Consumer

- `poll(timeout=…)` runs the deserializers on the caller's thread. A failing one
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
- `close(option=CloseOptions.timeout(…))` bounds the close;
  `close(timeout=…)` is Java's deprecated `close(Duration)` and warns.
- A deserializer defaults to the `key.deserializer` / `value.deserializer`
  config key, else to `bytes_deserializer()`; the consumer closes both when it
  closes.
- `MockConsumer` is Java's `MockConsumer`: `add_record(record=…)` takes the
  `ConsumerRecord` the test builds and `poll()` returns it as is (the mock has no
  deserializers). Nothing binds its type parameters, so annotate it as Java
  writes them: `c: MockConsumer[str, str] =
  MockConsumer(offset_reset_strategy="earliest")`.

## Errors

Errors are a typed hierarchy caught by class, not a flat error with a `code()`.
There is no `code()` / `is_retriable()` / `is_fatal()` on the surface —
retriability is `except RetriableError`, and each Java exception class has a
`…Error` Python analog in the module of its Java package (generated from the
Java sources). The Java built-in exception classes (`IllegalStateError`,
`IllegalArgumentError`, `ConcurrentModificationError`, `TimeoutError`,
`NoSuchElementError`, `NullPointerError`) live at the package root.

## Migrating from `confluent-kafka-python`

This is a different API, not a drop-in bump. The two rules that explain most
call-site changes:

- **Every argument is keyword-only.** `poll(1.0)` → `poll(timeout=1.0)`,
  `subscribe(topics)` → `subscribe(topics=topics)`, a producer's `close(5)`
  → `close(timeout=5)`.
- **Collections come back as Java's types** — `set` where Java returns `Set`,
  `dict` where Java returns `Map` — not the old client's lists.

Highlights:

| Old client | New client |
|---|---|
| `Producer(conf)` / `Consumer(conf)` | `KafkaProducer(configs=conf)` / `KafkaConsumer(configs=conf)` (`Producer`/`Consumer` are non-instantiable bases) |
| `produce(topic, value, key, …, on_delivery=cb)` | `send(record=ProducerRecord(topic=…, key=…, value=…), callback=cb)` → `Future[RecordMetadata]` |
| `p.poll(0)` in the produce loop | delete it — completions are delivered by the core |
| `poll(timeout)` → `Message \| None` | `poll(*, timeout=…)` → `ConsumerRecords` (a batch, never `None`) |
| `msg.error()` in band / `_PARTITION_EOF` | `except KafkaError` (typed) — no EOF event |
| `subscribe(topics, on_assign=…, on_revoke=…)` | `subscribe(*, topics=…, callback=ConsumerRebalanceListener())` |
| `commit()` (fire-and-forget by default) | `commit()` blocks (Java `commitSync`); `commit_nowait(callback=cb)` is Java `commitAsync` |
| flat `err.code` / `err.is_retriable()` | typed `except SpecificError` / `except RetriableError` |
| `AIOProducer` / `AIOConsumer` (thread-pool async) | `AsyncKafkaProducer` / `AsyncKafkaConsumer` (native async, no worker knobs) |
| `error_cb`, `logger`, `on_delivery`, … in the config dict | not config entries: an unknown key is accepted and logged once as unused (Java's `logUnused()`); errors raise or fail their future, logs go to the `confluent_kafka.*` Python loggers |

## Development

```
make devel-build-python                 # build the extension into the repo venv
cd bindings/python && ../../venv/bin/python -m pytest test/unit
make -C bindings/python typecheck       # mypy --strict (needs the venv python)
```

`make verify` (repo root) runs the Rust, C and Python suites plus the
Docker-based gRPC integration arm (`make test-integration-python`).
