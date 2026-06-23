---
name: review-m8-phase12-round3
description: Phase 12 scope-question — 3 RMs (coordinator/heartbeat/fetch) drop response oneshot receiver
metadata:
  type: feedback
---

When a Java RM uses `unsentRequest.whenComplete(callback)` to register
its own response handler INSIDE the `make*Request` method, the Rust
translation can choose between two patterns:

1. **RM-internal `tokio::spawn`** — `commit_request_manager.rs`,
   `offsets_request_manager.rs`, `topic_metadata_request_manager.rs`
   all use this: `take_response_receiver()` + `tokio::spawn` from
   inside the `make_*_request` method.
2. **Bg-task router** — Walk every `PollResult.unsent_requests`,
   `take_response_receiver()` per request, spawn a per-request
   dispatch task that knows which RM to call. None of the existing
   RMs use this — it would require the bg task to dispatch back into
   each RM through some trait.

**Smell to flag**: A `make_*_request` method that builds
`UnsentRequest::new(...)` and returns it WITHOUT a `tokio::spawn`
and without `take_response_receiver` — and the surrounding rustdoc
claims "the bg task takes the receiver". Grep for
`take_response_receiver` across the entire `src/consumer/internals/`
directory; if it doesn't appear in the RM's own file, the response
is silently dropped.

**Why:** `UnsentRequest::new` ALWAYS creates an oneshot pair; the
receiver lives inside `UnsentRequest.response_rx: Some(rx)`. When
`do_send` drops the `UnsentRequest` after handing off the request
builder to a `ClientRequest`, the receiver drops too. Later,
`FutureCompletionHandler::on_complete` writes into a closed sender —
the `let _ = tx.send(...)` swallows the SendError. **The completion
is observed nowhere.** Verified against Phase 12 commits (4)-(6),
where 4/4 integration tests `#[ignore]` because the consumer hangs
in JOINING.

**How to apply during review:**

- For every `RequestManager::poll` impl that emits a `PollResult`,
  trace where the response goes. If you can't find the dispatch
  callsite, file a Bug.
- Rustdoc claims like "the bg task takes the receiver via
  take_response_receiver" are not evidence — verify by grep.
- The Actor may describe the fix as a "substantial refactor". It
  rarely is — the working RMs (commit/offsets/topic_metadata) prove
  the fix is ~30 LOC per RM, plus a `Arc<Mutex<Self>>`-or-interior-mutability
  decision per RM. Push back on overstated scope.

**Files most likely affected** (audit any new RM in this list):
- `coordinator_request_manager.rs::make_find_coordinator_request`
- `consumer_heartbeat_request_manager.rs::build_heartbeat_request`
- `fetch_request_manager.rs::poll_internal` (request build loop)
