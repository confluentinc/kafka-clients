# Phase 5: C FFI Layer

## Goal

Expose the Producer API via C-callable functions using cbindgen to generate headers.
The C API wraps a `Box<dyn Producer>` behind opaque pointers. Initially wired to MockProducer.

## Feature Flag

`ffi` in `Cargo.toml`. All FFI code is behind `#[cfg(feature = "ffi")]`.

## Files

- `src/ffi/mod.rs` — module definition
- `src/ffi/producer.rs` — `extern "C"` functions
- `cbindgen.toml` — header generation config
- `Cargo.toml` — add `ffi` feature, `crate-type = ["lib", "cdylib"]`

## Cargo.toml Changes

```toml
[features]
integration-tests = []
ffi = []

[lib]
crate-type = ["lib", "cdylib"]
```

## Design Decision: Fixed-Width Types Only

All struct fields and function parameters use fixed-width types (`i32`, `i64`, `bool`, pointers)
instead of platform-dependent types (`size_t`/`usize`). This ensures the generated C header
produces identical struct layouts on 32-bit and 64-bit platforms. Lengths and counts use `i32`,
which matches Kafka's protocol limits (max message size ~2GB) and Java's `int` semantics.
Negative values (-1) signal "not set" for optional fields (partition, key_len, value_len).

## C API Design

### Opaque Handles

```rust
/// Opaque producer handle. Wraps Box<Mutex<dyn Producer>>.
#[repr(C)]
pub struct CProducer {
    _private: [u8; 0],
}

/// Opaque record metadata handle.
#[repr(C)]
pub struct CRecordMetadata {
    _private: [u8; 0],
}

/// Opaque future handle.
#[repr(C)]
pub struct CFutureRecordMetadata {
    _private: [u8; 0],
}
```

Internally these are `Box<Mutex<dyn Producer>>`, `Box<RecordMetadata>`,
`Box<FutureRecordMetadata>` cast to/from raw pointers.

### Batch Record for send_batch

```rust
/// A single record in a batch send call.
/// All fields use fixed-width types for cross-platform FFI portability.
#[repr(C)]
pub struct CProducerRecord {
    pub topic: *const c_char,
    pub partition: i32,           // -1 for unset
    pub key: *const u8,
    pub key_len: i32,             // -1 for no key, >= 0 for length
    pub value: *const u8,
    pub value_len: i32,           // -1 for no value, >= 0 for length
}
```

### Functions

```c
// ---- Lifecycle ----

// Create a mock producer. Returns NULL on failure.
CProducer* kafka_producer_new_mock(bool auto_complete);

// Destroy a producer. Safe to call with NULL.
void kafka_producer_destroy(CProducer* producer);

// ---- Send ----

// Send a single record. Writes the future handle to *out_future.
// partition: use -1 for no partition hint.
// key/value: NULL with len=-1 for no key/value, or ptr with len>=0.
// All sizes are fixed-width i32 for cross-platform portability.
// Returns 0 on success, non-zero error code on failure.
int32_t kafka_producer_send(
    CProducer* producer,
    const char* topic,
    int32_t partition,
    const uint8_t* key, int32_t key_len,
    const uint8_t* value, int32_t value_len,
    CFutureRecordMetadata** out_future
);

// Send a batch of records. Writes futures to out_futures array
// (caller must allocate out_futures with at least `count` entries).
// Returns 0 on success, non-zero error code on first failure (remaining
// records are not sent).
int32_t kafka_producer_send_batch(
    CProducer* producer,
    const CProducerRecord* records,
    int32_t count,
    CFutureRecordMetadata** out_futures
);

// ---- Future ----

// Check if a future is resolved.
// Note: not const because is_done() eagerly polls the channel.
bool kafka_future_is_done(CFutureRecordMetadata* future);

// Block until the future resolves. Writes metadata to *out_metadata.
// Returns 0 on success, non-zero error code on failure.
// On failure, *out_metadata is set to NULL.
int32_t kafka_future_get(
    CFutureRecordMetadata* future,
    CRecordMetadata** out_metadata
);

// Destroy a future handle.
void kafka_future_destroy(CFutureRecordMetadata* future);

// ---- RecordMetadata ----

int64_t kafka_record_metadata_offset(const CRecordMetadata* metadata);
const char* kafka_record_metadata_topic(const CRecordMetadata* metadata);
int32_t kafka_record_metadata_partition(const CRecordMetadata* metadata);
void kafka_record_metadata_destroy(CRecordMetadata* metadata);

// ---- Producer Operations ----

// Flush all pending records. Returns 0 on success.
int32_t kafka_producer_flush(CProducer* producer);

// Close the producer.
void kafka_producer_close(CProducer* producer);

// ---- Mock-specific Operations ----

// Complete the next pending send successfully.
// Returns true if there was a pending completion.
bool kafka_mock_producer_complete_next(CProducer* producer);

// Complete the next pending send with an error.
// Returns true if there was a pending completion.
bool kafka_mock_producer_error_next(
    CProducer* producer,
    int32_t error_code,
    const char* error_message
);

// Get the number of records in the sent history.
int32_t kafka_mock_producer_history_count(const CProducer* producer);

// Clear the sent history and pending completions.
void kafka_mock_producer_clear(CProducer* producer);

// ---- Error ----

// Get a human-readable description for an error code.
const char* kafka_error_message(int32_t error_code);
```

## Implementation Pattern

Each function follows this pattern:

```rust
#[no_mangle]
pub unsafe extern "C" fn kafka_producer_send(
    producer: *mut CProducer,
    topic: *const c_char,
    // ...
    out_future: *mut *mut CFutureRecordMetadata,
) -> i32 {
    // 1. Null checks
    if producer.is_null() || topic.is_null() || out_future.is_null() {
        return Errors::InvalidRequest as i32;
    }

    // 2. Convert pointer to reference
    let producer = &*(producer as *mut Mutex<Box<dyn Producer>>);

    // 3. Convert C strings/bytes to Rust types
    let topic = CStr::from_ptr(topic).to_string_lossy().into_owned();

    // 4. Build ProducerRecord
    let record = ProducerRecord::new(topic);

    // 5. Call trait method
    let guard = producer.lock().unwrap();
    match guard.send(record) {
        Ok(future) => {
            *out_future = Box::into_raw(Box::new(future)) as *mut CFutureRecordMetadata;
            0  // success
        }
        Err(e) => e.code()  // error code
    }
}
```

For `send_batch`:
```rust
#[no_mangle]
pub unsafe extern "C" fn kafka_producer_send_batch(
    producer: *mut CProducer,
    records: *const CProducerRecord,
    count: i32,
    out_futures: *mut *mut CFutureRecordMetadata,
) -> i32 {
    // Validate count >= 0, then iterate records[0..count as usize],
    // call send for each, write futures to out_futures[0..count].
    // On first error, stop and return error code.
}
```

## Mock-specific Functions

The mock-specific functions (`kafka_mock_producer_complete_next`, etc.) downcast the
`dyn Producer` to `MockProducer` using `Any`:

```rust
// MockProducer needs to implement a way to access its mock methods
// through the trait object. Options:
// 1. Store as Box<MockProducer> + Box<dyn Producer> (wasteful)
// 2. Use Any downcast
// 3. Add mock methods directly to the CProducer wrapper

// Recommended: CProducer internally stores an enum:
enum ProducerKind {
    Mock(MockProducer),
    // Real(KafkaProducer),  // later
}
```

This avoids `Any` complexity and makes the FFI layer type-safe.

## cbindgen.toml

```toml
language = "C"
include_guard = "CONFLUENT_KAFKA_RUST_H"
no_includes = true
sys_includes = ["stdint.h", "stdbool.h", "stddef.h"]

[defines]
"feature = ffi" = "CONFLUENT_KAFKA_FFI"

[fn]
prefix = ""
args = "Vertical"

[enum]
prefix_with_name = true
```

## Verification

1. `cargo build --features ffi`
2. `cargo test --features ffi`
3. `cargo xtask format-check`
4. `cargo xtask lint`
5. Verify `target/include/confluent_kafka_rust.h` is generated (or run cbindgen manually)
6. Optionally: compile a minimal C program that includes the header and links the library
