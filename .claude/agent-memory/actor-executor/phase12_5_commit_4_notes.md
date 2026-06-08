---
name: phase12_5_commit_4_notes
description: Milestone-8 Phase 12.5 (4/N) — fetch_request_manager mpsc channel-back; pending-set re-insertion gotcha; metadata_update_with(N,...) seeds nodes 0..N
metadata:
  type: project
---

Phase 12.5 (4/N) wires response routing for `FetchRequestManager` using the **mpsc channel-back** pattern (same shape as commit (3) `1950caf` for heartbeat) rather than the `Arc<Inner>` interior-mutability pattern used for coordinator + topic_metadata.

**Why channel-back not Arc<Inner> for fetch:** `AbstractFetch::handle_fetch_success` takes `&mut self` and updates `nodes_with_pending_fetch_requests` + builds `CompletedFetch` entries. The bg task already owns `&mut FetchRequestManager` via the `RequestManagers` slot, so we don't need interior mutability — defer the decode into the next `poll(now)` cycle via mpsc channel, where `&mut self` access is naturally serialized.

**Implementation shape (identical to heartbeat):**
- `PendingFetchCompletion { Response { fetch_target, request_data, response, request_version, for_close } | Failure { fetch_target, request_data, error, for_close } }` envelope. **Carry `for_close: bool`** so the drain dispatches to `handle_fetch_*` vs `handle_close_fetch_session_*`.
- `pending_completion_tx: mpsc::UnboundedSender` + `pending_completion_rx: mpsc::UnboundedReceiver` fields, direct ownership (no outer Mutex) — single-owner bg-task.
- Each per-node `UnsentRequest` in `poll_internal`'s loop gets `take_response_receiver()` + a spawned forwarder that captures `tx.clone()`. **Forwarder does NOT decode** — just moves `FetchResponse` (owns `Bytes` payloads) by ownership through the channel. §27 preserved.
- `drain_pending_completions()` at top of `poll(now)` AND `poll_on_close(now)`.

**§16 audit:** spawned forwarder's only `.await` is `response_rx.await`; no `.lock()`. Drain is fully synchronous (`try_recv` loop). Cross-call into `handle_fetch_failure` holds `subscriptions` mutex briefly but no `.await` inside that scope.

**Carry-overs / gotchas:**

1. **Pending-set re-insertion gotcha in close-path tests.** `prepare_close_fetch_session_requests` does NOT mutate `nodes_with_pending_fetch_requests`, but the subsequent `create_fetch_request(&target, &data)` call at the end of `poll_internal` does — line 290 of `abstract_fetch.rs` unconditionally inserts the target node into the pending set. So a test that calls `poll_on_close(now)` in a loop to drain the channel will see the pending-set RE-INSERTED on each iteration, masking the drain's removal. **Fix:** drive `poll(now)` (not `poll_on_close`) in the wait-for-drain loop — `poll(now)` short-circuits on `pending_fetch_requests == None` after running drain, never re-inserting. Both new regression tests use `poll(200)` in the wait loop with comments explaining why.

2. **`request_test_utils::metadata_update_with(num_nodes, ...)` seeds nodes 0..num_nodes.** Not 1..num_nodes. So `metadata_update_with(1, ...)` only gives node id=0, NOT node id=1. The bootstrap helper in my test had a `debug_assert!(node_id == 0)` to make this explicit. Reference: `request_test_utils.rs:188` `Node::new(i, ...)` for `i in 0..num_nodes`.

3. **`request_version` is in the `ClientResponse::request_header().api_version()`.** No need to capture it at the build site — it's already on the header that flows through the response receiver. The forwarder reads it inside the `Ok(Ok(mut client_response))` arm.

4. **`for_close` is a build-time captured flag, NOT a runtime classification.** The forwarder for a fetch request built in the close-path is always for_close=true, regardless of whether the broker actually closed or succeeded. The drain uses for_close to pick handler family. This matches Java's separate `whenComplete` lambdas attached to normal vs close requests.

5. **Dead-code helpers deleted (`on_fetch_response` / `on_fetch_failure`).** They were Phase-7b stubs staged for Phase-10 wire-up that never gained any callers. The channel-back path covers their job. Pattern: when audit-driven refactor renders test-or-shim helpers unused, delete them in the same commit (per Phase 10 "delete dead-code helpers" pattern).

6. **Test uses session-handler bootstrap path.** Hard to drive `prepare_fetch_requests` (regular path) end-to-end in a unit test — requires subscriptions + cluster metadata + assigned partitions in a fetchable state. **Simpler:** drive `poll_on_close(now)` which only needs (a) a session handler for the target node, (b) the node in the cluster snapshot. Set up via `mgr.abstract_fetch.session_handler_or_create(0)` + `metadata_update_with(1, &counts)` + `add_transient_topics`. Then fire `on_complete/on_failure` on the unsent's handler and observe `pending_fetch_node_ids()` change via `poll(now)` drain.

7. **Production callsite count for `take_response_receiver` after this commit: 9.** Pre-existing 5 (commit_request_manager x2, offsets_request_manager x3) + 4 from Phase 12.5 (coordinator, topic_metadata, heartbeat, fetch). Run `grep -n take_response_receiver src/consumer/internals/*.rs | grep -v test` for the canonical list.
