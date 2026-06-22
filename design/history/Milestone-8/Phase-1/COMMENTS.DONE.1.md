# Resolved Critic Comments — Milestone-8 Phase 1 (N=1)

Resolutions for the 4 issues raised in `COMMENTS.1.md` (review of commit
range `9f004fb..86b7ffc`). For the full original text of each issue see
the git history of `COMMENTS.1.md`.

## Resolution

### #1 — `ConsumerConfig::from_properties` missing several `atLeast(..)` validators

- **Resolution**: DEFERRED to Phase 11 (when `post_process_parsed_config`
  is translated). Documented with an in-code `NOTE:` comment at the top
  of `from_properties` listing the 18 deferred keys with their Java line
  numbers and the validator each one is missing. Per CLAUDE.md §5 the
  pointer uses `NOTE:` (not `TODO`/`FIXME`).
- **Commit**: `8d2da6e` — "fixup! Phase 1 (5/7 + 3/7): annotate deferrals
  for COMMENTS.1.md #1 and #3".
- **Original commit**: `5533d91` (Phase 1 (5/7): translate ConsumerConfig).

### #2 — `CloseOptions::timeout` signature uses `Option<Duration>` instead of `Duration`

- **Resolution**: FIXED. Aligned both `CloseOptions::timeout(...)` and
  `CloseOptions::with_timeout(...)` to take `Duration` (matching
  `CloseOptions.java:68` and `:89`). The internal field stays
  `Option<Duration>` — Java's field type is `Optional<Duration>` —
  populated as `Some(timeout)` on the call. Updated the in-file test
  and the translated `CloseOptionsTest.timeoutCouldBeNull` to use
  `CloseOptions::default()` as the equivalent path for "no timeout
  set" (Rust `Duration` is non-null so the literal `null` path has no
  direct equivalent).
- **Commit**: `7c8a503` — "fixup! Phase 1 (1/7): CloseOptions::timeout
  takes Duration (addresses COMMENTS.1.md #2)".
- **Original commit**: `7898012` (Phase 1 (1/7): consumer module
  skeleton + value types).

### #3 — `From<ConsumerError>` collapses structured info on conversion

- **Resolution**: DEFERRED. The flattening of `OffsetOutOfRange`,
  `NoOffsetForPartition`, `LogTruncation`, and `InvalidOffset` into
  `KafkaError::IllegalState` is by design for Phase 1; callers wanting
  structured classification should pattern-match on `ConsumerError`
  directly before propagating with `?`. Documented in a `NOTE:` doc
  comment on the `impl From<ConsumerError> for KafkaError` block,
  reflecting the Critic's caveat. No behavior change.
- **Commit**: `8d2da6e` — "fixup! Phase 1 (5/7 + 3/7): annotate deferrals
  for COMMENTS.1.md #1 and #3".
- **Original commit**: `bf18a1f` (Phase 1 (3/7): ConsumerGroupMetadata
  + ConsumerError hierarchy).

### #4 — `ConsumerGroupMetadata` constructors not marked `#[deprecated]`

- **Resolution**: FIXED. Added
  `#[deprecated(since = "4.2.0", note = "Use Consumer::group_metadata()
  instead. This struct will become a trait in a future release.")]` to
  both public constructors (`new` and `with_details`). Suppressed the
  resulting `deprecated` warning on the in-file `tests` module and on
  the integration test file `tests/consumer/consumer_group_metadata_test.rs`
  (`#![allow(deprecated)]`), and on the single internal `new -> with_details`
  call site. `since = "4.2.0"` is semver-compliant (clippy's
  `deprecated_semver` lint required `.0`); the Java attribute string is
  `since = "4.2"` (marketing version).
- **Commits**:
  - `16a403e` — "fixup! Phase 1 (3/7): mark ConsumerGroupMetadata
    constructors deprecated (addresses COMMENTS.1.md #4)".
  - `8dcdad1` — "fixup! Phase 1 (3/7): semver-compliant since in
    #[deprecated]" (clippy `deprecated_semver` follow-up).
- **Original commit**: `bf18a1f` (Phase 1 (3/7): ConsumerGroupMetadata
  + ConsumerError hierarchy).

## Verification gate

After all four resolutions:

- `cargo build` — clean.
- `cargo test` — all tests pass (27 consumer, 8 producer, plus
  common/integration/doc tests).
- `cargo xtask format-check` — clean.
- `cargo xtask lint` — clean.

`cargo doc --no-deps` was not re-run (pre-existing failures, out of
scope for this iteration).
