# Phase 7c review — resolutions for COMMENTS.1.md

Reviewed at SHA `678a65b` by Critic agent N=3. 6 findings; 2 fixed by
Manager in `3c748c2` (sub-Actor was sandboxed away from the worktree
path; Manager applied the mechanical fixes directly). 4 are
non-actionable / forward-looking.

## Fixed

### #1 — `signal_close()` gate dropped to mirror Java

Removed `closing: bool` field, init, and `signal_close` override. Now
inherits `RequestManager::signal_close` no-op default — matches Java's
`TopicMetadataRequestManager` exactly (Java does not override
`signalClose`). Removed the `test_signal_close_stops_polls` test.

Commit: `3c748c2` (fixup of `678a65b`).

### #2 — `InvalidTopicException` interpolates topic name

Changed the `Errors::InvalidTopicException` arm in
`handle_topic_metadata_response` from
`Err(KafkaError::invalid_topics({topic}))` to
`Err(KafkaError::with_message(Errors::InvalidTopicException, format!("Topic '{topic}' is invalid")))`
to match Java's `TopicMetadataRequestManager.java:259-260` message.

Commit: `3c748c2` (same).

## Not actionable / forward-looking

### #3 — `MetadataResponse::errors()` panic on `None` topic

Unreachable today (the manager always requests by topic name; brokers
reply with topic names per protocol). Accepted as documented
divergence; broader `MetadataResponse::errors() -> Result<...>` refactor
is out of Phase-7c scope. Re-evaluate in Phase 10 if the bg task ever
catches this panic in production.

### #4 — `UnsentRequest` ↔ `request_id` wiring

Phase 10 scope. Today's tests work around it by reading
`manager.inflight_requests()[0].id()` because tests issue exactly one
request. Phase 10 will need to attach a per-request completion token
or callback to `UnsentRequest`. Tagged for the Phase 10 plan.

### #5 — Bundled commit

Already vetted. Inline `#[cfg(test)] mod tests` made the 2-commit split
artificial.

### #6 — Memory file out-of-date (Phase 6 patterns)

Cosmetic memory update; not affecting code. Will be folded into a
larger Phase 7 memory-cleanup pass.

---

## Final state

- Lib tests: 1234 (was 1235; dropped the now-obsolete
  `test_signal_close_stops_polls`).
- `cargo build`, `cargo test --lib topic_metadata`, `cargo xtask
  format-check`, `cargo xtask lint`: all clean.
- Phase 7c can close on this worktree. Ready for merge back to
  `consumer-impl` once 7b and 7d also close.
