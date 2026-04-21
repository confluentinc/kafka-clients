# Phase 1: Foundation Types -- Review by Critic 0

Reviewed commit: e713928

---

## Issue 1: Compression.wrap_for_output/wrap_for_input missing messageVersion parameter

- **File**: `src/common/compress/mod.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/compress/Compression.java:43-56`
- **Description**: The Java `Compression.wrapForOutput(ByteBufferOutputStream, byte messageVersion)` and `wrapForInput(ByteBuffer, byte messageVersion, BufferSupplier)` both accept a `messageVersion` parameter. This is critical because for LZ4, when `messageVersion == RecordBatch.MAGIC_VALUE_V0`, a broken flag-descriptor checksum is used (see `Lz4Compression.java:49`). The Rust `wrap_for_output` and `wrap_for_input` do not accept a message version at all.

  While the producer will only produce v2 records, the consumer may need to read v0/v1 records. This missing parameter means the Rust compression layer cannot reproduce the Java behavior for older record versions. The signature should be extended to accept a message version (even if initially only v2 behavior is implemented), so that later phases don't need to change the public API.

- **Expected**: `fn wrap_for_output(&self, writer: W, message_version: i8) -> io::Result<CompressingWriter<W>>`
- **Actual**: `fn wrap_for_output(&self, writer: W) -> io::Result<CompressingWriter<W>>`

---

## Issue 2: LZ4 compression level is silently ignored

- **File**: `src/common/compress/mod.rs:171-175`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/compress/Lz4Compression.java:47-50`, `kafka/clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockOutputStream.java`
- **Description**: The Rust code uses `lz4_flex::frame::FrameEncoder` which does not support compression levels. The comment on line 172 acknowledges this: "lz4_flex does not support compression levels in the frame encoder." The API accepts a level (via `Compression::lz4_with_level`) and validates it, but then silently ignores it during compression.

  In Java, LZ4 compression levels 1-17 produce different compression ratios through `net.jpountz.lz4.LZ4Compressor` instantiation within `Lz4BlockOutputStream`. This means the `Compression::Lz4 { level }` field is dead data -- it is stored but never used.

  Either use a different LZ4 crate that supports levels (e.g., `lz4` crate which wraps the C library), or remove the level configuration from LZ4 and document the limitation.

- **Expected**: LZ4 compression level affects compression output.
- **Actual**: Level is accepted, validated, stored, but silently ignored during actual compression.

---

## Issue 3: Serializer trait missing serialize-with-headers overload

- **File**: `src/common/serialization/mod.rs:36-50`
- **Severity**: Missing Requirement
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/serialization/Serializer.java:82-84`
- **Description**: The Java `Serializer<T>` interface has a default method `serialize(String topic, Headers headers, T data)` that delegates to `serialize(topic, data)`. This method is used by the Kafka producer when serializing records that have headers. The Rust `Serializer` trait only defines `serialize(&self, topic: &str, data: Option<&T>)`, missing the headers-aware overload.

  While the default implementation just delegates, having this method in the trait is important because custom serializer implementations may need to access headers during serialization (e.g., for schema registry integration).

- **Expected**: The `Serializer` trait should include a default method for serialization with headers.
- **Actual**: Only `serialize(topic, data)` exists.

---

## Issue 4: CompressionRatioEstimator is instance-based instead of static/global like Java

- **File**: `src/common/record/compression_ratio_estimator.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/record/CompressionRatioEstimator.java:27-32`
- **Description**: In Java, `CompressionRatioEstimator` is a class with all **static** methods and a **static** `ConcurrentHashMap`. This makes it a process-wide singleton that shares compression ratio estimates across all producers and consumers in the same JVM.

  The Rust implementation makes it an instance (`pub struct CompressionRatioEstimator`) that must be explicitly created and shared. This means each user needs to wire up sharing, or multiple producers will have independent estimates, differing from Java behavior.

  This is not necessarily wrong (Rust idioms favor explicit state over globals), but it is a behavioral difference that should be documented and ensured to be correctly wired in later phases. If the intent is to match Java behavior, consider using `std::sync::LazyLock` for a global instance, or document the requirement to share instances.

- **Expected**: Either a global singleton or clear documentation that the instance must be shared.
- **Actual**: Instance-based with no guidance on sharing.

---

## Issue 5: RecordHeaders.add_key_value takes owned String and Vec instead of borrowed forms

- **File**: `src/common/header/mod.rs:59`
- **Severity**: Design Flaw
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/header/Headers.java:44`
- **Description**: Per CLAUDE.md rule 12: "Accept the most general borrowed form for input parameters." The `Headers` trait method `add_key_value(&mut self, key: String, value: Option<Vec<u8>>)` takes owned `String` and `Option<Vec<u8>>`, forcing callers to transfer ownership. The more idiomatic Rust signature per the project rules would be `add_key_value(&mut self, key: &str, value: Option<&[u8]>)`, doing the allocation internally.

- **Expected**: `fn add_key_value(&mut self, key: &str, value: Option<&[u8]>) -> Result<(), IllegalStateError>`
- **Actual**: `fn add_key_value(&mut self, key: String, value: Option<Vec<u8>>) -> Result<(), IllegalStateError>`

---

## Issue 6: TimestampType.for_name returns None instead of erroring like Java

- **File**: `src/common/record/timestamp_type.rs:50-57`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/record/TimestampType.java:35-39`
- **Description**: In Java, `TimestampType.forName(String name)` throws `NoSuchElementException` when the name is not recognized. The Rust `for_name` returns `Option<Self>`, silently returning `None` for invalid names.

  Per CLAUDE.md rule 10.2: "Return a `Result` when Java code throws an exception even if unchecked but recoverable." The Java behavior throws an exception for invalid names, indicating this is an error condition. The Rust version should return `Result<Self, KafkaError>` to match the Java error semantics.

- **Expected**: `pub fn for_name(name: &str) -> Result<Self, KafkaError>`
- **Actual**: `pub fn for_name(name: &str) -> Option<Self>`

---

## Issue 7: CompressionType.for_id and for_name return None instead of erroring like Java

- **File**: `src/common/record/compression_type.rs:67-89`
- **Severity**: Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/record/CompressionType.java:144-174`
- **Description**: In Java, both `CompressionType.forId(int id)` and `CompressionType.forName(String name)` throw `IllegalArgumentException` for unknown values. The Rust versions return `Option<Self>`, silently returning `None`.

  Same reasoning as Issue 6: per CLAUDE.md rule 10.2, these should return `Result<Self, KafkaError>` since the Java code explicitly throws recoverable exceptions for invalid inputs.

- **Expected**: Both methods should return `Result<Self, KafkaError>`.
- **Actual**: Both return `Option<Self>`.
