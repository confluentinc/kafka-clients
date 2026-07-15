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

## Milestone 11

The client-side KIP-932 share consumer flow. `KafkaShareConsumer` is built on `ShareConsumerImpl` and reuses the existing KIP-848 consumer engine (network thread, request-manager registry, event plumbing), adding the share-specific fetch, acknowledge, heartbeat, and membership managers along with the acknowledgement types and the public `ShareConsumer<K, V>` API. Records are fetched from share-group partitions, acknowledged (accept/release/reject/renew) individually or in batches, and committed synchronously or asynchronously through the share coordinator. Metrics are deferred to KIP-714; broker, persister, admin, and tools code stay out of scope.
