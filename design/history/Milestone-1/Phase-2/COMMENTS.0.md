# Phase 2 Review — Critic N=0

**Scope:** 27 commits in `443af50..HEAD` (Phase 2a–e). Reviewer = Critic 0.
**Headline:** Phase 2 is in good shape; G1/G2/G3 generator locks are solid and the
Java-authoritative byte fixtures cover all 8 wire types. No BLOCKER.

Resolved items have moved to `COMMENTS.DONE.0.md`. The items below are
deferred to Phase 4 per Manager direction (TODO markers added inline in
the source so the deferral is visible at the call site).

---

## DEFERRED (TODO marker present in source)

### Issue 6: `ApiMessageType::request_schema` / `response_schema` are empty stubs

- **File:** generated `api_message_type.rs` (Phase 2d-1 stub, kept through
  2d-4)
- **Severity:** MINOR (Deferred per actor's notes)
- **Java reference:** Java's `ApiMessageType.requestSchemas[]` is an array
  of `Schema` objects per version, populated from the per-message
  `SCHEMA_0` … `SCHEMA_N` constants.
- **What Rust does:** Returns an empty `Schema`. The actor flagged this in
  `phase2d_generator_runtime_gap.md` as deferred. Since Phase 2's wire
  encoding does not rely on these methods (each `*Data::write` is
  authoritative), nothing in the producer path is affected. **However:** the
  parameterized `ApiVersionsResponseTest` tests over
  `messageType.requestSchemas()[i]` (listed as deferred in
  `phase2e_requests_layout.md`) cannot be brought back without filling
  these in — flag for the Phase 4 actor.

**Status:** DEFERRED to Phase 4 per Manager direction. Comment marker
added at `generator/src/lib.rs:466` (the `// TODO Phase 4: replace these
stubs with a per-API match dispatching to each *Data::schema(version)`
block in front of the `request_schema`/`response_schema` emit).

---

### Issue 8: `RequestUtils::serialize` uses `to_vec()` of the inline buffer

- **File:** `src/common/requests/request_utils.rs:51`
- **Severity:** MINOR (Performance — not on hot path right now)
- **Java reference:** Returns a `byte[]`; in Java the underlying `ByteBuffer`
  may be the same array.
- **What Rust does:** `writable.flip(); Ok(writable.buffer().to_vec())` —
  one extra copy per serialize call.
- **Why it matters:** `serialize_with_header` flows through this. For
  Phase 2 it's only used by tests and (per actor notes) Phase 4 will
  rebuild the producer to use `SendBuilder` directly. Worth noting because
  the rest of the wire codepath has been carefully kept zero-copy and this
  is the one helper that buys an extra copy unconditionally.
- **Suggested fix:** Return `ByteBufferAccessor` (read-mode) directly so the
  caller can choose to slice or `to_vec()`. Change the few callers
  accordingly.

**Status:** DEFERRED to Phase 4 per Manager direction. Comment marker
added at `src/common/requests/request_utils.rs` (TODO Phase 4 in the
docstring of `serialize`).

---

### Issue 10: Wrapper-level fixture tests don't lock against Java directly

- **File:** `src/common/requests/tests.rs`
- **Severity:** NIT
- **Description:** The 6 framing tests (e.g. `request_header_framing_produce_v3`)
  compare the wrapper's `serialize_with_header(...)` output against
  `encode_message(header) ++ encode_message(body)` — both Rust-side. The
  per-`*Data` Java fixtures live in `src/common/message/tests.rs` and lock
  the inner encoding, so a bug consistent across `add_size` and `write` would
  pass these. The actor's `phase2e_requests_layout.md` argues this is
  sufficient because the per-`*Data` fixtures cover the inner encoding;
  agreed in principle, but a single end-to-end fixture
  (`header_v2 ++ produce_request_v9`) captured from Java would close the
  loop without much effort.

**Status:** DEFERRED to Phase 4 per Manager direction. Comment marker
added at `src/common/requests/tests.rs` (module-level TODO Phase 4).

---

## Items considered but rejected (not bugs)

These are concerns I considered and decided not to flag — listed so the
Manager can see I weighed them.

- **`MetadataResponse::error_counts` includes top-level `error_code`?**
  Java does NOT include the top-level `data.errorCode()` from v13+. Rust
  matches. ✓
- **`record_errors` at v9+ rejects `len == 0`?** `RecordErrors` is
  not nullable per spec; Java throws on null at compact non-nullable
  arrays. Matches. ✓
- **`ApiVersionsResponse` header version returning 0 (KIP-511)?**
  Generated `api_message_type.rs:1474` correctly emits 0 with comment.
  ✓
- **`MetadataRequest v0` fixture lacks `allow_auto_topic_creation`?** Field
  is `versions: "4+"` per spec; correct to skip at v0. ✓
- **`RequestHeaderData::api_key()` returns `-1`?** Matches Java's generated
  `RequestHeaderData.apiKey()`. ✓
- **`Uuid::zero()` const fn vs `ZERO_UUID` const?** Both exist; harmless
  duplication, the actor explained the choice in
  `phase2d_generator_runtime_gap.md`.
- **`ProduceRequestTest`'s `MemoryRecords`-driven tests not translated?**
  Correctly deferred to Phase 3 per `phase2e_requests_layout.md`. ✓
- **Phase 2c `SendChunks` returning `Vec<Arc<[u8]>>` instead of `Send`?**
  Actor's `phase2c_protocol_layout.md` justifies this as a Phase 3+
  contract; not a Phase 2 bug.
- **`RawTaggedField` derive `Hash`?** Required because every generated
  `*Data` derives `Hash`; no functional concern.
- **No Java RepeatedTest annotations in the request tests we touch.** No
  loops to translate.

---

## Suggestions for CLAUDE.md / rules

None this round. The G1/G2/G3 rules added in Phase 2a are pulling their
weight (the byte fixture tests confirm G1; the Phase 2d-1 generator
alignment is the correct place to enforce G2/G3 across all emit sites).
