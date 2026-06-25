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