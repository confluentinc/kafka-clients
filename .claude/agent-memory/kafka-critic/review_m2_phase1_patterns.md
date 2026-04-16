---
name: Milestone 2 Phase 1 review patterns
description: Common translation issues found in Phase 1 (Headers, Compression, Serializers) - Option vs Result for Java exceptions, missing method overloads, compression wire compatibility
type: feedback
---

Recurring pattern: Java methods that throw exceptions for invalid inputs are being translated to return `Option<Self>` instead of `Result<Self, KafkaError>`. CLAUDE.md rule 10.2 says to return Result when Java throws even unchecked recoverable exceptions.

**Why:** Callers using `Option` will silently drop errors, while Java callers would get exceptions. This changes error propagation behavior.

**How to apply:** When reviewing any `for_id`, `for_name`, `lookup`, or similar factory methods, check if Java throws on invalid input. If so, the Rust version should return `Result`.

---

Java's Kafka compression layer (especially LZ4) uses custom framing implementations (`Lz4BlockOutputStream`/`Lz4BlockInputStream`) with Kafka-specific quirks (broken FD checksum for magic v0). Standard Rust compression crate frame encoders may not be wire-compatible for all record versions.

**Why:** Wire incompatibility means data produced by the Rust client may not be readable by Java consumers (or vice versa) when older record format versions are involved.

**How to apply:** When reviewing compression code, verify the framing format matches Kafka's expectations, not just generic codec compatibility. Check if `messageVersion` parameter is threaded through.

---

Java `Serializer<T>` interface includes `serialize(topic, headers, data)` overload. This is commonly missed in translation but needed by schema registry and custom serializers.

**How to apply:** When reviewing serializer/deserializer translations, check all method overloads in the Java interface including default methods.
