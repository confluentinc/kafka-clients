---
name: Milestone 4 Phase 1/2 patterns
description: Ownership-consuming async methods break Java retry patterns, panic vs Result, close/Arc incompatibility
type: project
---

Key patterns found in Milestone 4:

**Phase 1 (producer foundation types):**

1. **Ownership-consuming async methods** (RESOLVED): `FutureRecordMetadata::get(self)` consumed the future, unlike Java's `Future.get()`. Fixed to `get(&mut self)` with `poll_fn` + `Option<Receiver>` + result caching.

2. **Test fidelity when adapting ownership** (RESOLVED): Tests that create new objects where Java reuses the same one are a sign of API signature mismatch.

3. **panic vs Result in public API** (RESOLVED): All constructors/builders now return `Result<Self, KafkaError>`. `KafkaError::illegal_argument()` uses `Errors::InvalidConfig` as error code for Java `IllegalArgumentException`.

4. **Clone derive for caching**: `RecordMetadata` needed `Clone` for result caching in `FutureRecordMetadata`.

**Phase 2 (Producer trait):**

5. **close() return type**: Java `KafkaProducer.close()` throws `InterruptException` and `KafkaException`. Rust trait returns `()` — violates CLAUDE.md rule 10.2.

6. **`&mut self` vs `Arc<dyn Producer>`**: Trait doc says `Arc<dyn Producer>` is enabled but `close(&mut self)` is incompatible with `Arc`. Java uses internal synchronization, not exclusive ownership.
