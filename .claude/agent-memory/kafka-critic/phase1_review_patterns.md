---
name: Phase-1 review patterns
description: Recurring translation-bug shapes and high-yield review areas from the Phase-1 foundations review
type: reference
---

High-yield areas to scrutinize when reviewing Java→Rust translations of leaf utilities:

1. **`Instant::now().elapsed()` is a trap.** It returns the duration since *that very call*, i.e. effectively zero. `System.nanoTime()` in Java returns nanoseconds from a fixed but arbitrary epoch (a large value). Any translation of `MockTime`, `Time.nanoseconds`, or anything that snapshots a "high-res clock reference" will be wrong if it uses `Instant::now().elapsed()`. Use `Instant::now().duration_since(reference)` against a process-start `Instant`, or `SystemTime::UNIX_EPOCH.elapsed()`.

2. **`String.length()` (UTF-16 code units) ≠ `str.len()` (bytes).** When Java does a length check followed by `substring`, the Rust translation must use `chars().count()` and `chars().take(n).collect::<String>()` — otherwise non-ASCII inputs either produce off-by-some errors or panic at byte boundaries inside multi-byte UTF-8 chars. Search for `s[..N]` slices following length checks in any class that takes user-supplied strings.

3. **`ConfigException` formatting uses `Object.toString()` (Display), not `Debug`.** Translations that use `format!("{value:?}")` will produce diverged error messages (`"foo"` vs `foo`, `Int(5)` vs `5`). Look for `config_exception::new`, `IllegalArgumentException` translations, and any error-message format string that interpolates a typed value with `{:?}`.

4. **`#[derive(Hash)]` does not match Java's `hashCode()`.** When the Java class defines a custom `hashCode` whose values are asserted in a `*Test.java` fixture (e.g. `Uuid.testHashCode`), the Rust translation must implement an explicit `hash_code()` method matching the Java formula. The Rust standard `Hash` trait can stay derived (only used for in-memory `HashMap`s), but the explicit `hash_code` method is the wire/test contract.

5. **Java's `RetriableException` hierarchy must be traced exactly.** `InvalidMetadataException` extends `RefreshRetriableException` extends `RetriableException` — so `NetworkException`, `LeaderNotAvailableException`, `NotLeaderOrFollowerException`, and `UnknownTopicOrPartitionException` are retriable transitively. `ProducerFencedException` and `InvalidProducerEpochException` extend `ApplicationRecoverableException` (a non-Retriable `ApiException` subclass) — they are *fatal*, not retriable. `OutOfOrderSequenceException` is a plain `ApiException` but the producer treats it as fatal in the idempotent path (this is enforced in `Sender.completeBatch`, not in the exception hierarchy).

6. **Wire codes are stable.** Cross-check every `code()` mapping against `kafka/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java`. Watch for code-2 (`CORRUPT_MESSAGE`/`CorruptRecordException`), code-87 (`INVALID_RECORD`/`InvalidRecordException`), code-90 (`PRODUCER_FENCED`).

7. **Test fixtures with byte vectors.** `ByteUtilsTest`/`Crc32CTest` have specific byte-vector assertions (e.g. CRC of `"Some String"` = 608_512_271). Any varint/varlong/CRC translation that only does round-trips will pass even if the encoding is wrong on the wire. Check that the Rust tests assert against the same byte sequences as the Java tests.

8. **Test counts: `@RepeatedTest(N)`.** Java's `@RepeatedTest(100)` annotations should become a `for _ in 0..N` loop in Rust, NOT a single invocation. Look for `RepeatedTest` in any Java test that touched random/non-deterministic code paths (typically `random_does_not_return_reserved`, retry/backoff jitter assertions).

9. **Module structure.** No `clients` in any path; each Java class in its own Rust file; internal imports use the parent module re-export (`use crate::common::utils::Time;` not `use crate::common::utils::time::Time;`); constants only exported by their defining file (no re-exports through `mod.rs`).

10. **Dropping the `is_fatal` set.** When idempotent/transactional logic is added in later phases, `is_fatal` must include `OutOfOrderSequence` and `UnknownProducerId` (Java's `Sender.failBatch` treats them as fatal-idempotent). Phase 1 doesn't need them but the docstring should flag the coming change.
