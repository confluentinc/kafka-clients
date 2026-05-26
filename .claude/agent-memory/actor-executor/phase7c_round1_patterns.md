---
name: Phase 7c Round 1 patterns
description: Patterns from Round 1 fixups on KafkaProducer skeleton — log-vs-tracing, JoinHandle-test-strengthening, stake-handle-before-drop
type: feedback
---

Patterns I hit during Phase 7c Round 1 fixups that future me should watch for.

**Translating Java `log.X` while crate uses a different logging shim**

**Why:** Critic recommended `tracing::warn!` / `tracing::info_span!` in Suggestion 1+2 because that's the common Rust observability surface. But this crate uses `log` (Cargo.toml: `log = "0.4"`), not `tracing`. Don't blindly translate the Critic's suggested API call — translate the *intent* to whatever logging shim the project already uses. For span instrumentation specifically: there's no `log` equivalent (spans are a tracing-specific concept), so the right call is to skip the span instrumentation entirely (and document why) rather than add `tracing` for a single use site.

**How to apply:** Before translating a `log.warn` / `log.info` recommendation, grep `Cargo.toml` for `log` vs `tracing` to see which is already a dep. If only `log`: translate as `log::warn!`. If neither: ask the user before adding either dep. For span recommendations without `tracing` already in scope: defer the constant/span entirely and document the deferral inline in the source (the Sender's existing `LogContext` prefix often suffices for per-request context).

**Test strengthening pattern: peek-handle-before-drop for `Drop`-side-effect verification**

**Why:** Phase 7c had `drop_aborts_sender_task` that only asserted the flag flip — but the flag flip happens on every drop path regardless of whether the JoinHandle abort actually fired. To verify side effects of `Drop`, you need a handle that the producer doesn't own — capture an `Arc<AtomicBool>` of the running flag, OR `take()` the JoinHandle out of an `Option<JoinHandle>` field before drop, then drop the producer, then `tokio::time::timeout(1s, handle).await`. Accept either `Err(JoinError::cancelled())` (abort path) or `Ok(())` (cooperative shutdown) since both prove the task actually exited.

**How to apply:** When testing a `Drop` impl that has multiple side effects (flag flip + JoinHandle abort + channel close), pull each independently observable handle out *before* drop, then assert each one independently. The Drop impl's `Option<JoinHandle>::take()` is the natural lever — the test can also call `.take()` first, which makes Drop a no-op for that handle but lets the test observe the future's completion directly.

**`AbstractConfig` already has `log_unused()` from Phase 1**

**Why:** Critic Suggestion 3 said "translate `config.logUnused()`" — and offered the option of deferring if `AbstractConfig` doesn't track accessed keys. This crate's `AbstractConfig` already has `log_unused()` and tracks used keys via `touch()` on every typed accessor. Don't backfill what's already there.

**How to apply:** Before deferring with a TODO, grep the existing `AbstractConfig` impl. If `log_unused()` exists, just call it from the constructor tail (`config.inner().log_unused()` because `ProducerConfig` exposes `inner()`).
