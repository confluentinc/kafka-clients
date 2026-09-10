# confluent_kafka (Rust-core Python client)

The Python client built on the Rust core's C FFI. Its surface is the Java Kafka
client (Apache Kafka 4.3.1) mapped to idiomatic Python, ruled by
`design/current/python-client-interface-spec.md`.

## Import paths

The package mirrors Java's package tree with the `clients` segment dropped
(spec §4). Import each name from its own module — the package root does **not**
re-export the everyday client names (that is undecided; spec §4):

```python
from confluent_kafka.producer import (
    KafkaProducer, MockProducer, AsyncKafkaProducer, AsyncMockProducer,
    ProducerRecord, RecordMetadata,
)
from confluent_kafka.consumer import (
    KafkaConsumer, MockConsumer, AsyncKafkaConsumer, AsyncMockConsumer,
    ConsumerRebalanceListener, OffsetAndMetadata, CloseOptions,
    SubscriptionPattern,
)
from confluent_kafka.common import (
    TopicPartition, TopicIdPartition, Node, PartitionInfo, Uuid,
    MetricName, Metric, KafkaMetric, TimestampType, Headers,
)
from confluent_kafka.common.errors import KafkaError, RetriableError  # typed hierarchy
from confluent_kafka.common.serialization import (
    bytes_serializer, string_serializer, int_serializer, json_serializer,  # etc.
)

# JDK-type analogs and the Duration alias live at the package root:
from confluent_kafka import (
    IllegalArgumentError, IllegalStateError, ConcurrentModificationError,
    TimeoutError, Duration,
)
```

The native extension is `_confluentkafka` (private): the package imports it; it
is never a public import path.

## Async peers

Each async client lives in the same module as its sync peer, `Async`-prefixed
(`AsyncKafkaProducer`, `AsyncKafkaConsumer`). A method is `async def` on the
async class iff it blocks in Java or awaits the background task; in-memory reads
(`assignment()`, `subscription()`, `metrics()`, `group_metadata()`) stay plain
`def` on both.

## Errors

Errors are a typed hierarchy caught by class, not a flat error with a `code()`.
There is no `code()` / `is_retriable()` / `is_fatal()` on the surface —
retriability is `except RetriableError`, and each Java exception class has a
`…Error` Python analog (generated from the Java sources). The JDK analogs
(`IllegalStateError`, `IllegalArgumentError`, `ConcurrentModificationError`,
`TimeoutError`) live at the package root.

## Migrating from `confluent-kafka-python`

This is a different API, not a drop-in bump — see **spec §11** for the full
migration surface. The two rules that explain most call-site changes:

- **Every argument is keyword-only.** `poll(1.0)` → `poll(timeout=1.0)`,
  `subscribe(topics)` → `subscribe(topics=topics)`, `close(5)` →
  `close(timeout=5)`.
- **Collections come back as Java's types** — `set` where Java returns `Set`,
  `dict` where Java returns `Map` — not the old client's lists.

Highlights (spec §11 is authoritative):

| Old client | New client |
|---|---|
| `Producer(conf)` / `Consumer(conf)` | `KafkaProducer(config=conf)` / `KafkaConsumer(config=conf)` (`Producer`/`Consumer` are non-instantiable bases) |
| `produce(topic, value, key, …, on_delivery=cb)` | `send(record=ProducerRecord(topic=…, key=…, value=…), on_delivery=cb)` → `Future[RecordMetadata]` |
| `p.poll(0)` in the produce loop | delete it — completions are delivered by the core |
| `poll(timeout)` → `Message \| None` | `poll(*, timeout=…)` → `ConsumerRecords` (a batch, never `None`) |
| `msg.error()` in band / `_PARTITION_EOF` | `except KafkaError` (typed) — no EOF event |
| `subscribe(topics, on_assign=…, on_revoke=…)` | `subscribe(*, topics=…, listener=ConsumerRebalanceListener())` |
| `commit()` (fire-and-forget by default) | `commit()` blocks (Java `commitSync`); `commit_nowait(on_commit=cb)` is Java `commitAsync` |
| flat `err.code` / `err.is_retriable()` | typed `except SpecificError` / `except RetriableError` |
| `AIOProducer` / `AIOConsumer` (thread-pool async) | `AsyncKafkaProducer` / `AsyncKafkaConsumer` (native async, no worker knobs) |

## Development

```
make devel-build-python                 # build the extension into the repo venv
cd bindings/python && ../../venv/bin/python -m pytest test/unit
make -C bindings/python typecheck       # mypy --strict (needs the venv python)
```

`make verify` (repo root) runs the Rust, C and Python suites plus the
Docker-based gRPC integration arm (`make test-integration-python`).
