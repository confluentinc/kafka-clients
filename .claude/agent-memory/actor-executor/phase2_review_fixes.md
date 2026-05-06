---
name: Phase 2 review fixes
description: Patterns I used while addressing Phase 2 Critic comments — useful for similar generator/wrapper review rounds
type: project
---

Round of Critic comments addressed for Phase 2 (commits 443af50..HEAD on
fresh-impl branch). Notes only on patterns/decisions worth carrying forward;
the per-fix details live in `design/history/Milestone-1/Phase-2/COMMENTS.DONE.0.md`.

**Why:** Capture decisions that took longer than expected to converge on so
the Phase 3+ Actor doesn't re-derive them.

**How to apply:** When reviewing similar generator emit changes or wrapper
class translations, consult these.

## Decisions worth keeping

- **`write_byte_buffer` vs `write_byte_array` split for `FieldType::Records`** — These produce identical wire bytes on `ByteBufferAccessor` (both delegate to `write_byte_array`). The behavioral difference is only on `SendBuilder`, where `write_byte_buffer` triggers the zero-copy chunk path (Arc<[u8]>). Splitting the generator emit *now* (without waiting for Phase 3's BaseRecords) is safe and locks in CLAUDE.md rule 12 at the right layer.

- **`ProduceRequest::clearPartitionRecords` shape** — Java throws `IllegalStateException` from `data()` post-clear; the Rust trait method `AbstractRequestResponse::data() -> &dyn Message` is infallible. Solution: keep an empty `cleared_stub: ProduceRequestData` on the struct, point `data()` at it post-clear, but make the dedicated `request_data()` accessor return `Result`. Document the contract: callers must not serialize after clear (Java has the same contract).

- **Eager partition-key cache** — Java's `partitionSizes` is lazy (synchronized double-checked init). Rust translation: just compute eagerly at `new()`. Avoids `RwLock` ceremony, and the cost is one extra pass over `topic_data` at construction.

- **`ListenerType` deduplication** — Two enums (hand-coded `api_keys.rs` + generated `api_message_type.rs`) silently disagreeing was the actual bug Issue 4 was flagging. Solution: re-export the generated enum from the hand-coded file. Single source of truth wins.

- **`OnceLock<&'static ApiKey>` for `api_key()` methods** — Pattern: `static FOO: OnceLock<&'static ApiKey> = OnceLock::new(); FOO.get_or_init(|| ApiKeys::for_id(N).expect("..."))`. Removes `expect()` from the public-API entry point per CLAUDE.md rule 10.1, with the panic relegated to the (unreachable) `OnceLock` initializer.

- **Tagged-field exact-size allocation** — For Bytes/Records, compute `prefix_size + len` inline and write directly into the outer `writable` (no temp buffer needed at all). For Struct/Array, run a fresh `MessageSizeAccumulator` (with its own `ObjectSerializationCache`) to compute the size, then `ByteBufferAccessor::allocate(size as usize)`. The cache must be local — `write` doesn't have a `cache` parameter (only `add_size` does).

- **Generated nested struct schemas** — Originally only top-level `*Data` structs had `schema(version) -> Result<Schema, KafkaError>`. To support `Type::Schema(Box::new(<NestedStruct>::schema(version)?))` in array-of-struct emits, every nested and common struct needs its own `schema(version)` method. Hook into `generate_nested_struct` and `generate_common_struct`.

- **Generator test-string parens trick** — Rust's parser flags unbalanced `(` inside string literals when surrounded by other tokens in confusing ways. If you hit "missing open `(`" errors, normalize the body (`body.chars().filter(|c| !c.is_whitespace()).collect::<String>()`) and check the normalized form against a paren-free target.

## Process notes

- The COMMENTS.\d.md and COMMENTS.DONE.\d.md files are gitignored (per `.gitignore` line 6). Use `git add -f` to commit them — Phase 1 set the precedent (`816ed59 Phase 1 review: move Issue 1 (MockTime nanoseconds) to DONE`).

- The `cargo xtask format` step touched several files post-edit; rerun `format-check` after each batch of edits to keep the working set in sync.

- Critic's "DEFER with TODO" items get a `// TODO Phase N: <rationale>` marker at the call site AND a "Status: DEFERRED" line in `COMMENTS.0.md` pointing at the marker location. Both are required.
