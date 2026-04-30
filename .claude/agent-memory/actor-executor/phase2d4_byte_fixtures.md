---
name: Phase 2d-4 byte-vector fixtures and produce-path emit fixes
description: How Java-authoritative byte fixtures were captured + the two generator emit fixes Phase 2d-4 needed
type: project
---

Phase 2d-4 (commits `16d05a1` and `f7e277b`) closed Phase 2 by:

1. Wiring `produce_request_data` and `produce_response_data` into
   `src/common/message/mod.rs` with round-trip tests at v3, v9, v13
   (request) and v3, v8, v10, v13 (response).

2. Adding 11 byte-vector encoding tests against fixtures captured from
   Apache Kafka 4.2.0 Java client.

**Two new generator emit fixes added in Phase 2d-4:**

- `crate::common::protocol::ByteBufferAccessor::from_bytes(struct_bytes)`
  → `wrap(struct_bytes)`. The runtime exposes `wrap(Vec<u8>)` per Phase
  2c; `from_bytes` was emitted on the tagged-field struct-read path but
  never existed. Surfaced for the first time on `ProduceResponse`'s
  `CurrentLeader` tagged field at v10. (lib.rs ~line 2397.)
- `writable.write_byte_array(&{accessor})` → `.as_slice()`. When
  `accessor` is `_nv` from `if let Some(ref _nv) = self.records {`, it's
  already a `&Vec<u8>`, so prepending `&` produced `&&Vec<u8>` and
  `clippy::needless_borrow` fired. `.as_slice()` always coerces to
  `&[u8]` regardless of whether the field is nullable or not. (lib.rs
  ~line 3935; emitted by `FieldType::Bytes | FieldType::Records`.)

Both fixes are additive — no other generated file's emit changed.

**How byte fixtures were captured (Java-authoritative):**

The Apache Kafka submodule at `kafka/clients/build/libs/` already has
`kafka-clients-4.2.0.jar` built. A one-off Java program named
`CaptureFixtures.java` (NOT committed; lives in `/tmp/kafka-fixture-capture/`)
constructs each `*Data` payload and emits hex via:

```java
ByteBuffer buf = MessageUtil.toByteBufferAccessor(data, version).buffer();
// hex-encode buf
```

Compile + run:

```bash
cp kafka/clients/build/libs/kafka-clients-4.2.0.jar /tmp/kafka-fixture-capture/
cp ~/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-api/1.7.36/.../slf4j-api-1.7.36.jar \
   /tmp/kafka-fixture-capture/
cd /tmp/kafka-fixture-capture
javac -cp "kafka-clients-4.2.0.jar:slf4j-api-1.7.36.jar" CaptureFixtures.java
java  -cp ".:kafka-clients-4.2.0.jar:slf4j-api-1.7.36.jar" CaptureFixtures
```

Hex strings are pasted into `src/common/message/tests.rs` as
`*_HEX: &str` constants alongside a comment recording the Java
construction code that produced them. To re-capture (e.g. when bumping
Apache Kafka version), recreate the Java tool from the values
constructed in each Rust fixture test.

**Subtlety: ProduceRequest `records` field at v3.**

Java's `PartitionProduceData.setRecords(BaseRecords)` accepts a
`MemoryRecords.readableRecords(ByteBuffer)`. At v3 (non-flexible), the
wire encoding is i32 length + raw bytes — the broker reads the bytes
verbatim regardless of whether they form a valid record batch.
Therefore the Rust round-trip test using `Option<Vec<u8>>` placeholder
with raw bytes (e.g. `b"hello"`) produces byte-identical output to
Java passing the same raw bytes via `MemoryRecords.readableRecords`.
Phase 3 will replace the placeholder with `MemoryRecords` proper.

**Phase 2 status after Phase 2d-4:**

Per `PLAN.md` lines 184–186, Phase 2 DoD requires byte-vector encoding
tests against known-good vectors captured from the Java client. Phase
2d-4 satisfies this for all 8 wire types generated in Phase 2:
RequestHeader, ResponseHeader, ApiVersionsRequest/Response,
MetadataRequest/Response, ProduceRequest/Response. Phase 2e
(common/requests/* wrappers) is the only remaining Phase 2 work.
