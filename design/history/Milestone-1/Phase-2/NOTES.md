# Phase 2c Design Notes

These notes capture the design choices made during Phase 2c that future
phase actors should know. They are not memory: they are checked into the
repo so subsequent agents can read them on first contact.

## Readable / Writable trait signatures

Both traits expose primitives in the same order as the Java interfaces.
Default methods (`read_string`, `read_uuid`, `read_unsigned_short`,
`read_unsigned_int`, `read_unknown_tagged_field` on Readable;
`write_uuid`, `write_unsigned_short`, `write_unsigned_int` on Writable)
are translated as provided trait methods so every implementor inherits
them automatically — exactly mirroring Java's `default` interface methods.

`Readable::slice()` returns a `Box<dyn Readable + '_>` borrowing from the
parent, not a `'static`. Java's `slice()` shares storage with the source;
in Rust the borrowed return type is the closest faithful translation.
The parent buffer is mutably borrowed for the lifetime of the slice.
This is fine for the single-pass parsing use cases the producer client
exercises (parse a tagged-field block, drop the slice, continue).

`Writable::write_records(BaseRecords)` is *not* on the trait yet because
`BaseRecords` is in `common/record/*` which is Phase 3. The `SendBuilder`
zero-copy path goes through `write_byte_buffer(&[u8])` for now; Phase 3
will extend this once the record types exist.

## ByteBufferAccessor

Backed by a `Vec<u8>` (not a fixed-size buffer) with explicit `position`
and `limit`. Java's `ByteBuffer` lifecycle has three phases — write,
flip, read. The Rust accessor mirrors this:

- `allocate(size)` → write mode, `position=0`, `limit=size`.
- `wrap(vec)` → read mode, `position=0`, `limit=vec.len()`.
- `flip()` → swaps to read mode.

Writes that exceed the original capacity *grow* the buffer rather than
throwing, matching the practical use-case of `MessageUtil::to_byte_buffer_accessor`
(which always pre-sizes exactly). The grow path is purely defensive.

`raw_buffer()` returns the *full* backing storage (positions ignored)
because `RawTaggedFieldWriterTest` asserts on positions including the
unwritten tail. `buffer()` returns up to `limit`, suitable for sending
on the wire.

## ApiKeys

Java's `ApiKeys` enum constructor pulls metadata (request schemas,
response schemas, header versions, listener set) from the *generated*
`ApiMessageType` enum. `ApiMessageType` lives in Phase 2d
(`src/common/message/`). Rather than block on Phase 2d, Phase 2c uses a
**hand-coded `ALL_API_KEYS: &[ApiKey]` table** with the fields we can
populate without the generator: `id`, `name`, `cluster_action`,
`forwardable`, `listeners`. Methods that need request/response schemas
(`latest_version`, `oldest_version`, `request_header_version`,
`response_header_version`, `to_api_version`, `to_html`,
`response_throttle_time_ms` test) are deferred until Phase 2d.

The Phase 2d Actor should:

1. Wire up the generated `ApiMessageType` table.
2. Refactor `ApiKey` to either pull schemas/versions from `ApiMessageType`
   directly, or replace the hand-coded `ALL_API_KEYS` with one populated
   from the generator output.
3. Translate the deferred tests
   (`testResponseThrottleTime`, `testHasValidVersions`,
   `testHtmlOnlyHaveStableApi`).

The hand-coded catalogue uses a `pub struct ApiKey` and a
`pub static ALL_API_KEYS: &[ApiKey]` rather than a Rust enum because:

- The 93-variant enum would be unwieldy to switch on.
- Java exposes per-instance fields (`apiKey.clusterAction`, `apiKey.id`,
  `apiKey.name`) that are awkward to model on a fieldless Rust enum.
- Tests like `for_id` are a simple linear scan over the slice; no perf
  concern at the catalogue size of 93 entries.
- Phase 2d will swap the table for the generator-emitted version
  in-place; downstream code that pattern-matches on
  `ApiKey::Produce` would have to be rewritten anyway, so we avoid
  encouraging that style.

## Errors

Java's `Errors` enum maps wire codes to `ApiException` subclasses. Our
`KafkaError` (Phase 1) already collapses that exception hierarchy. For
Phase 2c we add a separate `pub enum Errors` (1:1 with Java) whose
`exception()` returns the corresponding `KafkaError`. The two enums are
deliberately kept separate:

- `KafkaError` is the *runtime* error type clients return from
  `Result`s. It carries a message string and is `Display + Error`.
- `Errors` is the *catalogue* used to look up a wire code and produce a
  fresh `KafkaError` with a default message. Mirrors Java's
  `Errors.forCode(short).exception(message)` pattern.

`Errors::None` returns `None` from `exception()` to match Java's `null`
return for `Errors.NONE.exception()`.

## SendBuilder return type

Java's `SendBuilder.build()` returns a `Send` (interface). `Send`,
`MemoryRecords`, and `MultiRecordsSend` live in
`common/network/*` and `common/record/*` (Phase 3).

For Phase 2c the builder returns a `SendChunks` (a `Vec<Arc<[u8]>>`).
This is exactly the information the network layer will hand to
`write_vectored` later — each chunk becomes one `IoSlice`. The Phase 3+
Actor can either:

- Wrap `SendChunks` in a `Send` impl, or
- Replace `SendChunks` with `Send` and refactor callers.

Either is fine; no Phase-2c caller besides the tests is affected because
the producer hasn't started using `SendBuilder` yet.

The `write_byte_buffer` path now does an `Arc::from(&[u8])` allocation
per zero-copy chunk. Java retains the `ByteBuffer` reference verbatim;
in Rust we cannot tie the chunk lifetime to the caller's `&[u8]`
(network sends outlive the call), so a single `Arc<[u8]>` allocation is
unavoidable. This still avoids copying the bytes a second time when the
network layer ships them, preserving the zero-copy *intent* of the Java
class. The Phase 3+ Actor may switch to `bytes::Bytes` if the
`Arc::from(&[u8])` clone proves measurable.

## ObjectSerializationCache identity-key trick

Java uses `IdentityHashMap<Object, Object>` — keyed by reference
identity. Rust's `HashMap` cannot match this; instead the cache stores
`HashMap<usize, _>` keyed by the address of the passed-in `&T`. This
works because the generator emits code that always passes the *same*
Rust reference to `add_size(...)` and the matching `write(...)` call.
If a future codegen change starts cloning the value between the two
passes, the cache entry will be invisible — which is correct
(recompute), but the optimisation is lost. Watch for this when
auditing generator output in Phase 2d.

## What was *not* translated this phase

- `ApiKeys.toHtml()` — needs response schemas (Phase 2d).
- `ApiKeys.toApiVersion(...)` — needs `ApiVersion` data class (generated
  in Phase 2d) and request/response schema versions.
- `MessageUtil.jsonNodeTo*` helpers — used at generator build time, not
  on a runtime path. Pulling Jackson into runtime is unjustified;
  defer until/if a runtime JSON-driven message reflection path appears.
- `Writable.writeRecords(BaseRecords)` — needs `BaseRecords` (Phase 3).
- `SendBuilderTest#testZeroCopyRecords` and `testZeroCopyUnalignedRecords`
  — need `MemoryRecords` (Phase 3).
- The deferred ApiKeys tests listed above.
