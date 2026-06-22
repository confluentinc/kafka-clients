---
name: review-m8-phase20-recv-cpu
description: Phase 20 receive-path copy/clone elimination review patterns (into_response_data move, drain_completed_receives, alloc-budget soundness)
metadata:
  type: project
---

Phase 20 (Milestone-8) eliminated 3 receive-path deep copies for CPU. All clean. Patterns worth reusing:

**Move-vs-clone twin methods.** When a clone-based accessor (`FetchResponse::response_data(&self)`) is refactored to a consuming move variant (`into_response_data(self)`), verify byte-for-byte parity of the *resolution logic* (version gating `<13`, `continue`-skip of unresolved v13+ topic IDs, IndexMap insertion order) — only ownership should differ. A keys-only variant (`response_partition_keys`) must equal `response_data().keys().collect()`.

**Use-after-move ordering.** `handle_fetch_success` calls `handler.handle_response(&response, ...)` (borrow) THEN `response.into_response_data(...)` (move). Order matters: validate-borrow before move. Lookups into `request_data.to_send`/`session_partitions` use the borrowed `&TopicPartition` key produced fresh by the move — unaffected.

**Alloc-budget test soundness check (how to prove non-tautology).** `test_alloc_tracker` counts allocation *events*, not bytes (thread-local `TRACK_ENABLED` Cell + GlobalAlloc wrapper). So a payload-size-independent budget genuinely proves no byte copy: a reintroduced `Vec<u8>` clone = exactly +1 alloc/partition. Verify the margin (move-path baseline vs budget) is SMALLER than the clone delta (allocs/partition × partitions). Phase 20: 55 baseline, 58 budget (margin 3), clone delta 8 → discriminates. Run with `--nocapture` to read the printed count.

**Selector drain lifecycle.** `drain_completed_receives(&mut self)` via `mem::take` empties the list at poll END; `selector.clear()` runs at poll START. Next clear is a no-op on empty list. Confirm: (1) sole reader after network poll, (2) within-poll progress checks read the list *during* poll before drain, (3) all `Selectable` impls (`Selector`, `MockSelector`) add the new required method. `ByteBufferAccessor::from_bytes(Vec<u8>)` consumes by value → parse is zero-copy from moved buffer.

**Retained clone() callers.** When a hot-path clone is replaced, the cloning method is often kept for non-hot callers that need the whole struct (e.g. `ApiVersions::get` retained for `offsets_request_manager`/`subscription_state` which do multi-API lookups off `NodeApiVersions`, not a single range query). Confirm those callers are genuinely per-round/per-validation, not per-record/per-request.

**Acceptable deviation:** returning moved `String` source (not `Arc<str>`) from a drain when the field is already `String` — it eliminates the `to_string()` realloc (the actual target) without a broad type refactor; the large payload `Vec<u8>` is what must move zero-copy.

**Pre-existing-not-regression trap.** `CompletedFetch::new_full` does `Arc::from(partition.topic())` (re-allocs Arc<str> from an existing Arc<str> — could clone instead) and `TopicPartition::new(name: String, ...)` allocs Arc<str> per partition from wire String. Both pre-date Phase 20 (send-side build path / §27 topic-name area). Not in receive-copy-elimination scope; note as context, don't flag as Phase 20 bug.
