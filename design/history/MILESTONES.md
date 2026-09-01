# Milestones

## Milestone 1

The client should be able to connect to a Kafka broker with PLAINTEXT connection and execute RPC request and responses of ApiVersions and Metadata RPCs.

## Milestone 2

The Producer should be able to accumulate the messages into batches to produce in a Produce RPC.

## Milestone 3

The Producer should be able to connect to a SSL endpoint, do a SaslHandshake and
then a SaslAuthenticate with PLAIN credential

## Milestone 4

The MockProducer implementation is translated and a C FFI is added to call
the Rust client from many languages.

Python CPython C extension binding module wrapping the Rust C FFI, with a high-level Pythonic wrapper, unit tests and a performance test.

C binding (confluent-kafka-c) that uses the Rust C FFI directly with a CMake build system, providing both static and dynamic linking against the Rust library. Uses MockProducer for initial testing.

## Milestone 5

Performance optimizations on the hot path

## Milestone 6

A multilanguage integration test harness so the producer integration tests run against three backends from a shared set of scenarios: native Rust (`KafkaProducer` against a testcontainers broker), Python (via gRPC → `bindings/python`), and C (via gRPC → the C FFI).

## Milestone 7

A translation agent that watches new commits in Apache Kafka and opens PRs in this repository with the corresponding Rust translations. Implemented as a Python application backed by a SQLite database (tracking AK/Rust branch-commit correspondence) and driven by the Semaphore CI pipeline.

## Milestone 8

The Java `AsyncKafkaConsumer<K, V>` (the new KIP-848 consumer group protocol) and its dependency closure are translated to Rust, exposing a `Box<dyn Consumer<K, V>>` public API with `AsyncKafkaConsumer` and `MockConsumer` implementations. The threading model (single background task, `wakeup()` cancellation, `Arc<Mutex<SubscriptionState>>`, listener-on-caller-task invocation, receive-path zero-copy) is governed by `.claude/rules/consumer-threading.md`. The classic group protocol is out of scope.

## Milestone 9

A C FFI for the Rust consumer (`AsyncKafkaConsumer` and `MockConsumer`), exposing the full method surface — sync and async (callback-based) — to C, Python, and other languages. Because the `Consumer` trait is `Send` but not `Sync` and every blocking method takes `&mut self`, the binding owns the consumer behind a non-reentrant single-owner access guard that fails fast with `ConcurrentModificationError` on concurrent access — mirroring Java's `KafkaConsumer.acquire()/release()` contract — rather than silently serializing through an actor task. The async surface is one-operation-in-flight; `wakeup()` bypasses the guard. Shared completion/dispatcher/error machinery is extracted into `ffi/common.rs` and reused by the producer FFI. Unity C tests cover the mock-driven consume loop, the access guard, and wakeup. See `Milestone-9/consumer-ffi-plan.md`.

## Milestone 10

The consumer reaches the Python and C bindings and the multilanguage integration harness, completing parity with the producer:

- **Python consumer binding** (`bindings/python/consumer.py`): a Pythonic sync + asyncio-native `Consumer` mirroring the Java interface, over the consumer C FFI through a marshaling-only CPython extension. Key/value/headers are zero-copy `memoryview`s backed by the record batch (a buffer-exporter keeps the batch alive). Both APIs drive the *async* C bindings — even the sync API submits an async op and waits interruptibly — so Python signal handlers run while Rust executes; `KeyboardInterrupt`/cancellation translate to a Rust-side `wakeup()`. This required adding async FFI variants for the remaining network-blocking consumer methods (`committed`, `position`, `offsets_for_times`, `beginning/end_offsets`, `partitions_for`, `list_topics`, the commits) so no host thread parks inside `block_on`. Callback bridging (rebalance listeners, commit callbacks) and pattern subscription are out of scope (the FFI bridges no callbacks).

- **Multilanguage consumer integration tests**: a `ConsumerService` gRPC (sibling of the producer service) implemented by the Python (`grpc_server.py`) and C++ (`grpc_server/server.cc`) servers, a Rust `MultilanguageConsumer` gRPC client + `ConsumerBackendFactory` + `multilanguage_consumer_test!` macro, so consumer scenarios run against native Rust, Python, and C backends. Coverage spans the supported operation surface (assign/subscribe/poll/commit/committed/position/seek/seek-ends/pause-resume/begin-end-offsets/offsets-for-times/partitions-for/list-topics/assignment/subscription/unsubscribe). Callback/listener/regex tests remain native-Rust-only by FFI design.

- **Producer `partitions_for`** is exposed in the producer FFI (reusing the consumer's `PartitionInfoList` handle), `producer.py`, and both gRPC servers, closing the last producer-surface gap and adding a producer `partitions_for` multilanguage scenario.

## Milestone 11 (planned)

`org.apache.kafka.clients.admin.Admin` (`KafkaAdminClient`, `MockAdminClient`), the third and last major Java client surface, translated to Rust with a C FFI (sync + async) and Python bindings (sync + async, over the async C API) — completing parity with Producer and Consumer. Scoped into priority tiers (Topics/Partitions/Cluster/Configs → Consumer groups & offsets → ACLs/quotas/SCRAM/tokens/transactions/features → Streams Groups/Share Groups/KRaft raft-voter admin deferred), executed as vertical slices (Rust → C → Python, sync+async, per RPC group) rather than horizontally by layer. `Admin`'s RPC methods stay plain `fn` returning `KafkaFuture`-backed results (they are non-blocking in Java — only `close()` and `client_instance_id()` are `async fn`), and Java's dual `Call`-retry / `AdminApiDriver` dispatch engines are both preserved. See `Milestone-11/PLAN.md`.

## Milestone 12

Producer metrics: the `org.apache.kafka.common.metrics` registry (`Metrics`, `Sensor`, `MetricName`) and the producer's metric families are translated to Rust, mirroring the Milestone-9 consumer-metrics work. `KafkaProducer` owns an `Arc<Metrics>` built from the producer config; `KafkaProducerMetrics`, the `Sender`/throttle metrics, and `BufferPool`/`RecordAccumulator` metrics record at their Java-faithful call sites, and `Producer::metrics()` exposes a snapshot. The C FFI and Python bindings gain a metric-map surface. See `Milestone-12-producer-metrics/`.

## Milestone 13

The Rust client is brought up to **Apache Kafka 4.3.1**: the `kafka/` submodule reference moves from 4.2.0 to 4.3.1, the wire-spec corpus (`generator/messages/`) is synced to the 4.3.1 specs, and the 4.2.0→4.3.1 Java clients delta (188 main files / 117 test files across 81 in-scope commits) is translated for every Java file with a Rust counterpart. The largest piece is the KIP-848 consumer rebalance-handshake reshape (KAFKA-20106/20321/20332): the bg reconcile now ends with a `PartitionsAssignedEvent` that the app thread answers with an awaited `ApplyAssignmentEvent`, guaranteeing `assignment()` changes only within `poll()`. Also: the `common/record` → `common/record/internal` module move (KAFKA-20128) with generated `ControlRecordTypeSchema` (KAFKA-10863); `CommitRequestManager` retriable-partition-error handling on `OffsetFetch` (KAFKA-20165); AdminClient stale-leader lookup retry (KAFKA-20673) and cordoned log dirs (KIP-1066); the producer 2PC public-API revert; and assorted consumer CPU/busy-loop/unsubscribe fixes (KAFKA-20535/20426/20428). Scope exclusions (no Rust counterpart to drift): Share consumer (KIP-932), Streams-integration internals, Classic consumer/Coordinator, OAuth, telemetry (KIP-714), the runtime Schema/protocol-types machinery, `ConfigDef`, the compression hierarchy, and the monolithic `Utils`/`Bytes`/`Shell`. The Phase-6 close-out (`Milestone-13/sweep.md`, `Milestone-13/PLAN.md` §6) audits all 176 changed files and 81 commits to a phase or an explicit skip-with-reason, with zero uncovered in-scope residue. See `Milestone-13/PLAN.md`.
