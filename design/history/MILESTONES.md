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

## Milestone 9

The client-side KIP-932 share consumer flow. `KafkaShareConsumer` is built on `ShareConsumerImpl` and reuses the existing KIP-848 consumer engine (network thread, request-manager registry, event plumbing), adding the share-specific fetch, acknowledge, heartbeat, and membership managers along with the acknowledgement types and the public `ShareConsumer<K, V>` API. Records are fetched from share-group partitions, acknowledged (accept/release/reject/renew) individually or in batches, and committed synchronously or asynchronously through the share coordinator. Metrics are deferred to KIP-714; broker, persister, admin, and tools code stay out of scope.