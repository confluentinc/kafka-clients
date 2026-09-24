# APPSEC-7665 / NONJAVACLI-4520 — fetch decode hardening

Manager plan for the Actor/Critic loop **78**. Branch
`fix/appsec-7665-4520-batch-header-checks`, base `master` at `d7f38f14`.
Ticket text (AppSec, Launchpad review): "malformed broker data can crash or
OOM the consumer". Seven sub-findings, all confirmed open against master on
2026-09-24. This file is the specification the Actor implements and the
Critic reviews against; the Java source in `kafka/` (4.3.1) is the contract
wherever this file cites it.

## 0. The problem in one paragraph

A fetch response is one buffer holding several record batches back to back.
Java walks them with `ByteBufferLogInputStream.nextBatch()`, which runs
`nextBatchSize()` for **every** batch (sign, minimum size, magic) and then
hands out a slice limited to exactly the declared size
(`ByteBufferLogInputStream.java:41-58`, `:66-84`). Rust runs the equivalent
check once, for the first batch only (`MemoryRecords::first_batch_size`,
called from `FetchCollector::initialize` at `fetch_collector.rs:768`), then
moves between batches with its own cursor, `CompletedFetch::load_next_batch`
(`completed_fetch.rs:1089-1229`), which trusts the header: it computes
`size_in_bytes()` with an unchecked `as usize` on a signed length, builds a
`DefaultRecordBatchRef` over the slice from the batch start to the **end of
the payload**, reads header fields at fixed offsets from it, only rejects a
sub-61-byte batch when `check.crcs` is on, treats a negative record count as
an empty batch, and decompresses compressed batches with `read_to_end` and no
bound. Three allocation sites elsewhere size a buffer from an
attacker-declared length before any bytes exist, and the production
selectors accept a receive of any size.

## 1. Decisions (Manager, 2026-09-24)

These are settled. The Actor implements them; the Critic checks them. Where
this file and Java disagree on *values*, that is deliberate and recorded
under §4 as a justified deviation (definition-of-done §7).

| # | Decision | Value / shape |
|---|---|---|
| D1 | Per-batch header validation is a translation of Java's `ByteBufferLogInputStream`, in its own file (CLAUDE.md §2: one Java class per file). | `src/common/record/internal/byte_buffer_log_input_stream.rs`, `pub(crate)` (package is `internal`). `next_batch_size()` and `next_batch()` mirror `ByteBufferLogInputStream.java:41-84`; `next_batch()` returns a `DefaultRecordBatchRef` whose slice is **limited to the declared size** (Java `batchSlice.limit(batchSize)`, `:51`). `MemoryRecords::first_batch_size` keeps its signature and delegates (Java `MemoryRecords.java:121-124` does the same). `BatchIterator::next` and `CompletedFetch::load_next_batch` go through it. |
| D2 | The 61-byte floor for a v2 batch is enforced **unconditionally**, before any header accessor runs. Only the CRC comparison stays behind `check.crcs`. | Error text is Java's `ensureValid` text: `Record batch is corrupt (the size {n} is smaller than the minimum allowed overhead 61)` (`DefaultRecordBatch.java:151-154`), wrapped by the existing `Record batch for partition {tp} at offset {o} is invalid, cause: …` (`CompletedFetch.maybeEnsureValid`). Why unconditional: with `check.crcs=false` Java throws a plain `IndexOutOfBoundsException`/`BufferUnderflowException` from the absolute buffer reads; Rust's equivalent is a slice panic, which is finding 4521's abort inside the C/Python bindings. |
| D3 | A negative record count is rejected before the batch is installed. | `Error::InvalidRecord(InvalidRecordError::new("Found invalid record count {n} in magic v{m} batch"))` — Java `DefaultRecordBatch.java:584-587`, thrown unwrapped from `RecordIterator()`; the owned path already does this (`default_record_batch.rs:336-342`). |
| D4 | Decompressed output per batch is capped by a **constant**, no new public config key. | `pub const MAX_DECOMPRESSED_BATCH_BYTES: usize = 1 << 30` (1 GiB) in `default_record_batch.rs`. Rationale: an order of magnitude above the largest request a default broker accepts (`socket.request.max.bytes` default `100 * 1024 * 1024`, `SocketServerConfigs.java:96`), so no legitimately stored batch reaches it, while a zip bomb is bounded. `decompress_records` takes the cap as a parameter; growth must be bounded (peak capacity ≤ cap + one read chunk, never `read_to_end`'s unbounded doubling) and must use `try_reserve` so an allocation failure is an error, not an abort. Exceeding the cap is an `InvalidRecordError` naming the cap and wrapped like any other decompression failure at `completed_fetch.rs:1165`. Java has no cap: it streams record by record (`DefaultRecordBatch.java:279-297`), which conflicts with `consumer-threading.md` §27 (one decompression buffer per batch, records borrow from it). Promote to a config only if a user hits it. |
| D5 | The production selectors get a real receive cap, no new public config key. | `pub const DEFAULT_MAX_RECEIVE_SIZE: i32 = 100 * 1024 * 1024` in `network_receive.rs` beside `UNLIMITED` (same broker default as D4). Producer (`kafka_producer.rs:505`) and admin (`kafka_admin_client.rs:318`) pass it. Consumer (`async_kafka_consumer.rs:1869`) passes `fetch.max.bytes + DEFAULT_MAX_RECEIVE_SIZE`, saturating at `i32::MAX`: Java documents `fetch.max.bytes` as a soft limit (the first batch of the first non-empty partition is returned even if larger), and that batch is bounded by what the broker itself accepted, i.e. its `socket.request.max.bytes`. The cap is enforced by the existing `NetworkReceive` check (`network_receive.rs:194-199`); an oversize receive already closes the connection like Java's `Selector` (`selector.rs:664-667`). Java's own client is `UNLIMITED` (`Selector.java:229`, `:233`, via `ClientUtils.java:198`) — deviation, §4. |
| D6 | `MemoryRecords::batches()` stays an infallible iterator. | On a header that fails `next_batch_size`, `BatchIterator::next` logs at error level and ends iteration — the precedent is `MemoryRecords::records()` (`memory_records.rs:108-116`), which documents why a fallible signature is not worth its call-site count for a corrupt local buffer. Wire data never reaches `batches()`: the consumer uses `load_next_batch`. The four `ByteBufferLogInputStreamTest` tests are translated against `ByteBufferLogInputStream` directly, where the error is observable. |
| D7 | Legacy magic (v0/v1) on the fetch path is rejected explicitly, not parsed as v2. | `load_next_batch` returns an error naming the magic when `magic < 2` (CLAUDE.md §5: an unimplemented Java path fails loudly). Kafka 4.0 removed support for message formats v0/v1 (KIP-724), so a 4.x broker never sends them; the check exists so a hostile one cannot make a 34-byte legacy header trip the 61-byte floor with a misleading "corrupt" message or, with the floor bypassed, be read as a v2 header. No legacy parsing is added. |

## 2. Work items

Each item names the Rust site, the Java contract and the required test.
"Exact message" means the test asserts the full error string.

### 2.1 Per-batch header validation (D1, D2, D3, D7)

1. **New `ByteBufferLogInputStream`** (`byte_buffer_log_input_stream.rs`).
   Fields mirror Java: the buffer slice, a position, `max_message_size: i32`.
   `next_batch_size(&self) -> Result<Option<usize>, Error>` returns
   `Ok(None)` when fewer than `LOG_OVERHEAD` (12) bytes remain, or fewer than
   `HEADER_SIZE_UP_TO_MAGIC` (17) after the size checks; `Err(CorruptMessage)`
   with Java's exact texts for `recordSize < 14`
   (`Record size {n} is less than the minimum record overhead (14)`),
   `recordSize > max_message_size`
   (`Record size {n} exceeds the largest allowable message size ({max}).`)
   and a magic outside `0..=2` (`Invalid magic found in record: {m}`). The
   length is read as `i32` and compared **signed**; the returned size is
   `LOG_OVERHEAD + record_size` computed only after the checks, so it cannot
   overflow. `next_batch(&mut self) -> Result<Option<DefaultRecordBatchRef<'_>>, Error>`
   returns `Ok(None)` when `remaining < batch_size` (Java `:44-45`), else the
   batch view **sliced to exactly `batch_size`** and advances the position.
   Move the local `LEGACY_RECORD_OVERHEAD_V0 = 14` constant out of
   `first_batch_size` to where Java keeps it (a `legacy_record` constant is
   acceptable if no `LegacyRecord` translation exists; say so in a comment).
2. **`MemoryRecords::first_batch_size`** delegates with `i32::MAX`
   (`MemoryRecords.java:124`). `has_complete_first_batch` keeps its
   behaviour but calls `next_batch_size` directly (Actor 78, see §6): with
   Java's `HEADER_SIZE_UP_TO_MAGIC` early return (`:122-123`) in
   `first_batch_size`, building `has_complete_first_batch` on it would turn
   `hasNext()`'s `CorruptRecordException` for a 12-16 byte buffer into "no
   batch".
3. **`BatchIterator::next`** (`memory_records.rs`) uses `next_batch_size`;
   no `as usize` on a signed length, no unchecked add (D6 for the error).
4. **`CompletedFetch::load_next_batch`** (`completed_fetch.rs:1089-1229`):
   for the batch at `next_batch_start`, run `next_batch_size` with
   `i32::MAX`; `Ok(None)` or `remaining < batch_size` ends iteration exactly
   as today; `Err` propagates (a corrupt header is an error, not "no batch" —
   same reasoning as `has_complete_first_batch`'s doc comment). Then, before
   any header accessor: reject `magic < 2` (D7) and `batch_size < 61` (D2,
   unconditional). Only then build the `DefaultRecordBatchRef` over
   `&buffer[batch_start..batch_start + batch_size]`, never to the end of the
   payload. Keep the existing `check.crcs`-gated `ensure_valid()` call for
   the CRC half. Reject `records_count() < 0` (D3) before installing the
   batch. `DefaultRecordBatchRef::new`'s doc comment currently says the slice
   "may extend past the end of this batch" — either remove that allowance or
   add a checked constructor; no accessor may be reachable on a view shorter
   than `RECORD_BATCH_OVERHEAD`.
5. **`DefaultRecordBatchRef::size_in_bytes`** (`default_record_batch.rs:762`)
   must not wrap on a negative length (`i32 as usize`); the view is
   validated before construction, so a debug assertion or a checked
   conversion documenting the invariant is enough. Same for the owned
   `DefaultRecordBatch::size_in_bytes` (`:179`) if it shares the helper.

Tests: translate the four `ByteBufferLogInputStreamTest` cases
(`kafka/.../ByteBufferLogInputStreamTest.java:37-129`) against the new type,
including `iteratorRaisesOnTooLargeRecords` with `max_message_size = 60`.
Add `CompletedFetch`-level tests using the existing `new_multi_batch_records`
/ `new_completed_fetch` helpers (`completed_fetch.rs:1506`, `:1539`) where
batch 1 is valid and batch 2 is corrupted, driven through `fetch_records`:
length `-5`; length `9`; length in `14..61` (e.g. 30) with `check_crcs`
**false** and **true**, both erroring with the D2 text; magic `37`; magic `1`
(D7); record count `-1` (D3); declared length larger than the remaining bytes
(ends iteration, no error, as Java `nextBatch()` returns null); a trailing
batch of exactly 12..61 bytes with `check_crcs=false` (no panic). Every
error test asserts the exact message. Confirm none of these paths allocate
per record: `test_collect_fetch_per_record_allocation_budget`
(`fetch_collector.rs:2187`) must still pass unchanged.

### 2.2 Decompression cap and declared-length allocations (D4)

1. **`DefaultRecordBatchRef::decompress_records`**
   (`default_record_batch.rs:855-865`): take `max_bytes: usize`; replace
   `read_to_end` with bounded growth (`try_reserve`, peak capacity ≤
   `max_bytes` + one chunk); exceeding the cap is an `InvalidRecordError`
   whose message names the cap. `load_next_batch` passes the value carried by
   `FetchConfig` (a `pub(crate)` field defaulted to
   `MAX_DECOMPRESSED_BATCH_BYTES`; keep `FetchConfig::new`'s Java arity — a
   `with_…` setter or defaulting inside the constructors is fine) so a test
   can drive the cap down to a few hundred bytes. Document the field as a
   Rust-only addition (§4). The owned `DefaultRecordBatch::iter_records`
   compressed path (`:384-397`) applies the same cap by wrapping the
   decompressing reader in `take(MAX_DECOMPRESSED_BATCH_BYTES)`.
2. **`DefaultRecord::read_from_stream`** (`default_record.rs:237`):
   `vec![0u8; size_of_body]` before any byte is read is the finding; Java
   does the same (`DefaultRecord.java:286`, inherited). Grow with the bytes
   actually present (`take(size).read_to_end` or equivalent) so a declared
   2 GiB record with three bytes behind it allocates three bytes and fails
   with the existing "reached EOF" message.
3. **`XerialSnappyReader::read_next_block`** (`compress/mod.rs:140-157`):
   `vec![0u8; compressed_len]` from a 4-byte length read off the stream, then
   `decompress_vec`, which allocates the block's *declared* decompressed
   length. Neither may drive an allocation beyond the bytes actually present
   or the per-batch budget. Read the compressed block with bounded growth;
   check `snap::raw::decompress_len(&compressed)` against a bound before
   `decompress_vec`. Java precedent: snappy-java bounded its chunk length for
   CVE-2023-34455 (Kafka 4.3.1 ships snappy-java 1.1.10.7,
   `kafka/gradle/dependencies.gradle:131`). How the reader learns the budget
   (constructor parameter, or a fixed block bound with the rationale) is the
   Actor's call; document it.
4. **`Vec::with_capacity(num_records)`** in `iter_records`
   (`default_record_batch.rs:356`, `:390`): capacity must be bounded by a
   quantity derived from bytes actually present (each uncompressed record is
   at least one byte, so `min(num_records, records_data.len())` is a hard
   bound there; for the compressed path use plain growth or a similarly
   derived hint). Java's `new ArrayList<>(count())`
   (`DefaultRecordBatch.java:332`) is the inherited over-allocation.

Tests: a compressed batch (each codec: gzip, snappy, lz4, zstd) whose
decompressed size exceeds a small cap fails with the exact message and one
just under it succeeds, driven through `fetch_records` with the lowered
`FetchConfig` value; assert the decompressed buffer's capacity never exceeds
cap + chunk (the crate already has an allocation-counting test harness in
`fetch_collector.rs:2187` — reuse it or assert on the returned `Vec`).
`read_from_stream` with a declared size of `i32::MAX` and a three-byte body
errors without allocating gigabytes. A xerial block with `compressed_len =
0xFFFF_FFFF` and a xerial block whose snappy header declares a huge
decompressed length both error. `iter_records` with `records_count =
i32::MAX` returns an error rather than panicking on capacity.

### 2.3 Receive cap (D5)

1. Add `DEFAULT_MAX_RECEIVE_SIZE` to `network_receive.rs`.
2. Producer and admin: replace `with_defaults_and_log_context` with the
   `with_log_context(max_receive_size, …)` form (`selector.rs:313`) passing
   the constant. Consumer: a small private function (e.g. in
   `async_kafka_consumer.rs`) derives `fetch_max_bytes.saturating_add(DEFAULT_MAX_RECEIVE_SIZE)`
   and passes it. Leave `Selector::with_defaults*` in place for tests and
   `MockSelector`; document that production no longer uses them.

Tests: the derivation function at the default (`50 MiB + 100 MiB`), at a
large `fetch.max.bytes` (saturates to `i32::MAX`), and a `NetworkReceive`
built with the constant rejecting `DEFAULT_MAX_RECEIVE_SIZE + 1` with Java's
message (`Invalid receive (size = … larger than …)`). If a `Selector` getter
is needed to assert what the producer/consumer/admin constructors pass, keep
it `pub(crate)` and name it after the field.

### 2.4 This file

Commit this file with the change (it is the §7 deviation record). Correct
anything in it the code proves wrong, and say so in the commit message rather
than silently diverging.

## 3. Gates and constraints

- `cargo build`, `cargo build --features ffi`, `cargo test --lib`,
  `cargo test --lib --features ffi ffi::` (quote the `test result` line; `0
  passed` on that filter means not run), `cargo xtask format-check`,
  `cargo xtask lint`. Run the long ones in the background with a 600000 ms
  timeout. If `docker info` succeeds, also run
  `cargo test --features integration-tests --test integration consumer`
  in the background and report the result line; it is not a blocker if
  Docker is unavailable, but say so.
- Commit with `git commit --no-verify` after running those gates directly
  (the pre-commit hook runs a multi-minute Docker build that exceeds the
  agent timeout), and note both facts in the commit message. Add files by
  path; never `git add -A`. Do not commit the untracked scratch files in the
  tree (`APPSEC-7665-CHANGES.md`, `INVESTIGATION-*.md`, `OPEN-BUGS.md`,
  `issues.md`, `examples/eos_app.rs`, `bindings/python/examples/`,
  `bindings/python/macos_compat/`) or other agents' memory files.
- No edits to `.claude/rules/*` or `CLAUDE.md`. No new crates (`snap` is
  already a dependency). The word "exception" appears only in comments about
  Java. Apache 2.0 header (Confluent Inc.) on the new file. No `TODO`/`FIXME`.
- `#[async_trait]`, `tokio::spawn`, locks: none of this touches them; the
  hot-path rule that applies is `consumer-threading.md` §27 — the per-batch
  checks add no per-record allocation and no copy of record bytes.

## 4. Justified deviations from Java (definition-of-done §7)

- `ProduceRequest::validate_records` (added by Actor 78, §6): a v2 first
  batch shorter than its own 61-byte header fails validation with the
  `ensureValid()` size text. Java passes it (it reads only the magic and the
  attributes) and would fail later; this client has no view of such a batch.

- D4 cap value and D5 cap values: Java has neither; both are Rust-only
  constants with the rationale above. A `FetchConfig` field that Java's
  `FetchConfig` lacks carries D4 to the fetch path.
- D2 unconditional floor: Java gates the same check on `check.crcs` and
  otherwise throws a generic runtime exception from a buffer read; Rust
  cannot let that be a panic.
- D6 infallible `batches()`: see the `records()` precedent.
- D7 explicit legacy rejection: Java parses v0/v1; this client does not
  implement them and says so instead of misparsing.

## 5. Status

- 2026-09-24: plan written; Actor 78 spawned.
- 2026-09-24: Actor 78, §2.1 landed (commit 1). Discrepancies in §6.
- 2026-09-24: Actor 78, §2.2 landed (commit 2). Choices in §6, items 9-13.
- 2026-09-24: Actor 78, §2.3 landed (commit 3). Note in §6, item 14.

## 6. Implementation notes (Actor 78)

Where the code proved a premise of this plan incomplete or wrong, the code was
followed and the difference is recorded here.

1. **`has_complete_first_batch` could not stay literally unchanged** (§2.1
   item 2). It was built on `first_batch_size`; Java's `hasNext()` is
   `nextBatch() != null`, which runs `nextBatchSize()` without
   `firstBatchSize()`'s early return. It now calls `next_batch_size` directly,
   so its behaviour is unchanged, which
   `fetch_collector.rs::test_initialize_corrupt_batch_size_propagates_corrupt_record_error`
   (a 12-byte buffer) pins.
2. **`ProduceRequest::validate_records` also walked batches with
   `BatchIterator`** (`produce_request.rs`), which the plan did not list. Under
   D6/D7 the iterator ends at a v0/v1 batch, which would have turned Java's
   "only allowed to contain record batches with magic version 2" into "must
   have at least one record batch" for a legacy first batch, and let a legacy
   second batch pass the "exactly one record batch" check that Java's
   `hasNext()` fails. It now asks `ByteBufferLogInputStream` Java's questions
   directly (a complete batch, then its magic read off the header as
   `nextBatch()` reads it, then a complete second batch). That also removes the
   copy of every partition's batch that `BatchIterator` made on each produce
   request just to read three header fields. A corrupt header now fails with
   the stream's `CORRUPT_MESSAGE` text, as Java's `hasNext()` throws, where it
   used to be reported as a missing batch.
3. **The existing batch wrapper printed the cause's `Display`**, which carries
   a class-name prefix (`InvalidRecordError: ...`), where Java's
   `maybeEnsureValid` appends `e.getMessage()`. All four per-batch wraps in
   `load_next_batch` (D7, D2, the checksum, decompression) now go through one
   `invalid_batch_error` that appends the message only, so D2's text is
   exactly Java's.

   Three older wrappers outside `load_next_batch` had the same prefix (Critic
   78 named the first two): `peek_current_record`, `contains_abort_marker` and
   the headers wrap in `fetch_records`. They now append `message()` too, and
   tests pin the first two texts exactly. The headers wrap has no such test:
   its only trigger is a header key that is not UTF-8, which Java decodes with
   replacement characters instead of failing (`RecordHeader.key()`), a
   separate pre-existing divergence. The wrappers themselves are a
   pre-existing structural deviation outside this ticket: Java propagates the
   `InvalidRecordException` from `records.next()` / `batchIterator.next()`
   unwrapped, where this client wraps it in a message naming the partition
   and offset.
4. **D7's error** is the crate's unsupported-path error
   (`Error::unsupported_version`, code `UNSUPPORTED_VERSION`) from
   `ByteBufferLogInputStream::next_batch`; `load_next_batch` reports the same
   text through the batch wrapper, with the partition and offset.
5. **D3 is also applied on the control-batch path.** Under READ_COMMITTED
   Java's `containsAbortMarker` builds `batch.iterator()` (`CompletedFetch.java:376-386`),
   whose `RecordIterator` rejects a negative count; `contains_abort_marker`
   now does the same. An aborted data batch is still skipped before its count
   is read, in Java's order. The check changes the outcome only for a
   non-ABORT control batch whose producer id is in the aborted set — any other
   batch reaches the install-point check, which raises the same text — so its
   test builds exactly that fetch; without the check the batch is silently
   skipped as aborted (Critic 78; the test was shown to fail with the check
   disabled).
6. **`records_section` and the CRC are bounded by the view's slice**, which is
   Java's `buffer.limit()` (`DefaultRecordBatch.java:273-277`, `:399-401`),
   not by the declared size. With the slice limited to the declared size the
   two agree; bounding by the slice is what makes a corrupt length unable to
   drive a slice index.
7. **`LEGACY_RECORD_OVERHEAD_V0`** moved to a constants-only
   `record::internal::legacy_record` module (`LegacyRecord.java:45-69`); no
   `LegacyRecord` translation exists or is added.
8. The premise "treats a negative record count as an empty batch" (§0) held
   only when the batch's records section was empty; with records present the
   old cursor raised "records still remaining". Either way D3 now rejects it
   with Java's text.
9. **The bounded read** (`default_record_batch.rs::read_decompressed`)
   starts its capacity at the compressed size, doubles it, clamps it to
   `max_bytes + DECOMPRESSION_READ_CHUNK_BYTES` (16 KiB) and offers each
   `read` at most one chunk, so no capacity above that ceiling is ever
   requested; every reservation is `try_reserve_exact`. The limit's text,
   shared with the snappy reader so every codec reports it identically, is
   `decompressed size exceeds the limit of {n} bytes per record batch`, under
   the existing `Failed to decompress record stream: ` prefix.
10. **How the xerial reader learns the budget** (§2.2 item 3, the Actor's
    call): a constructor parameter, reached through a Rust-only
    `pub(crate) Compression::wrap_for_input_with_limit`; the public
    `wrap_for_input` keeps its signature and passes no limit. Independently of
    any limit, every block's declared decompressed length is checked against
    what its bytes can encode — snappy's densest element is a 3-byte copy of 64
    bytes, so at most 64/3 of the block size — which rejects only headers that
    would fail decompression anyway, and bounds the public path too. The
    compressed block is read with `take(len).read_to_end`, so a declared
    `0xFFFF_FFFF` over three bytes allocates for three.
11. **The owned `iter_records` path gets its limit from `take` alone**, not
    from the reader: a snappy reader refusing a block surfaces there as a
    failed record read, which the path cannot tell from any other. Its snappy
    blocks remain bounded by the encoding check, and owned batches are ones
    this client built or validated. The limit is a parameter of a private
    `iter_records_with_limit`, so a test drives it down.
12. **`Vec::with_capacity(num_records)`** became
    `min(num_records, records_section.len())` on both owned paths: a hard
    bound for uncompressed records (each is at least one byte), a starting
    capacity for compressed ones.
13. Observation, not changed: zstd's decoder sizes its window from the frame
    header up to libzstd's default `windowLogMax` (27, 128 MiB), as Java's
    zstd-jni does; lz4 frames cap blocks at 4 MiB; gzip's window is fixed.
14. **No `Selector` getter was added** (§2.3 left it optional). What the
    producer and admin constructors pass is not observable from a test without
    new hooks — the producer's `NetworkClient` moves into its spawned `Sender`
    — so the tests cover the consumer's derivation (`max_receive_size`: the
    default, the saturation edge, `i32::MAX`) and the rejection of
    `DEFAULT_MAX_RECEIVE_SIZE + 1` on both `NetworkReceive` read paths, with
    Java's message and no payload buffer allocated. The three call sites are
    direct `with_log_context(cap, ...)` calls.
