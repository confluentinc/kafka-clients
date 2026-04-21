# Milestone 6: C Binding (confluent-kafka-c)

## Goal

Create a C binding (`bindings/confluent-kafka-c/`) that uses the Rust C FFI directly, with a CMake build system supporting both static and dynamic linking. Uses MockProducer for initial testing.

## Context

The Rust project already exposes a complete C API via `src/ffi/producer.rs` with header at `target/include/confluent_kafka_rust.h`. This milestone creates a proper C project that consumes it.

## What Already Exists

- Rust FFI: `src/ffi/producer.rs` — all `kafka_producer_*`, `kafka_common_KafkaError_*` functions
- Generated header: `target/include/confluent_kafka_rust.h`
- `Cargo.toml`: `crate-type = ["lib", "cdylib"]` — needs `"staticlib"` added

## Changes Required

### 1. `Cargo.toml` — add staticlib
```toml
crate-type = ["lib", "staticlib", "cdylib"]
```
Produces both `libconfluent_kafka_rust.a` (static) and `libconfluent_kafka_rust.so` (shared).

### 2. `bindings/confluent-kafka-c/CMakeLists.txt`
- Project: `confluent-kafka-c`
- Finds the Rust library (static or shared via option)
- Includes: `target/include/` for `confluent_kafka_rust.h`
- Builds `test_mock_producer` from `tests/test_mock_producer.c`
- Links against Rust lib + system deps (`pthread`, `dl`, `m`)

### 3. `bindings/confluent-kafka-c/tests/test_mock_producer.c`
Adapted from Java project's C test patterns, using `kafka_*` API directly with MockProducer (no Properties, no GraalVM):

```c
#include <confluent_kafka_rust.h>

// 1. Create MockProducer
kafka_producer_Producer_t *producer = kafka_producer_MockProducer_new(true);

// 2. Send a record
kafka_producer_FutureRecordMetadata_t *future = NULL;
kafka_common_KafkaError_t *err = kafka_producer_Producer_send(
    producer, "test-topic", -1, -1,
    key, key_len, value, value_len, &future);

// 3. Get metadata
kafka_producer_RecordMetadata_t *metadata = NULL;
err = kafka_producer_FutureRecordMetadata_get(future, &metadata);

// 4. Read fields
int64_t offset = kafka_producer_RecordMetadata_offset(metadata);
int32_t partition = kafka_producer_RecordMetadata_partition(metadata);
const char *topic = kafka_producer_RecordMetadata_topic(metadata);

// 5. Cleanup
kafka_producer_RecordMetadata_destroy(metadata);
kafka_producer_FutureRecordMetadata_destroy(future);
kafka_producer_Producer_close(producer);
kafka_producer_Producer_destroy(producer);
```

The test exercises:
- Create/destroy lifecycle
- Send with key+value, verify metadata (offset, partition, topic)
- Send without key (null key)
- Multiple sends (incrementing offsets)
- Manual complete mode (auto_complete=false, complete_next/error_next)
- Batch send via `kafka_producer_Producer_send_batch`
- Close then send (expect error)
- Error inspection (code, message, is_retriable, is_fatal)

## Files

```
Cargo.toml                                      -- add "staticlib" to crate-type
bindings/confluent-kafka-c/
  CMakeLists.txt                                 -- build system (static + shared)
  tests/
    test_mock_producer.c                         -- comprehensive test using kafka_* API
```

## Verification

1. `cargo build --features ffi` (produces both `.a` and `.so`)
2. `cd bindings/confluent-kafka-c && mkdir build && cd build && cmake .. && make`
3. `./test_mock_producer` passes all tests

## Definition of Done

1. `cargo build --features ffi` succeeds producing both static and shared libraries
2. CMake builds successfully with both static and shared linking
3. `test_mock_producer` passes all test cases
4. No memory leaks (all handles properly destroyed)
