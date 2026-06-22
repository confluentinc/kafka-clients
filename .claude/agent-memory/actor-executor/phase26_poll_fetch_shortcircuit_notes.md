---
name: phase26-poll-fetch-shortcircuit
description: Milestone-8 Phase 26 — two CPU micro-opts porting stock Kafka; Selector::poll send-only break + prepare_fetch_requests up-front skip; test+mutation patterns
metadata:
  type: project
---

Phase 26 (N=26): two independent CPU micro-optimizations, two commits. Behavior-identical to stock Apache Kafka, zero latency change.

**Fix #1 — `Selector::poll` made_progress break (src/common/network/selector.rs ~line 1091).**
Removed `completed_sends` from the break; kept `completed_receives`, `connected`, `disconnected`. Stock Java `NetworkClient.poll` loops `while completedReceives().isEmpty() && disconnected().isEmpty()` — never returns on completed sends. `connected` MUST stay (§10 join path: `poll_channel_reads` pushes onto `self.connected` on post-handshake ready transition so poll() exits and api-version fires). All consumer sends expect a response, so no fire-and-forget blocks forever; deadline still bounds the wait.
- Test `test_send_only_poll_does_not_return_early`: new `SinkServer` (drains inbound bytes, never echoes) constructs a send-only round. Gotcha: must SETTLE first (3× `poll(20)`) to clear `immediately_connected_keys` — otherwise `effective_timeout==0` → `deadline=None` → the timeout-0 `_ =>` arm breaks regardless of the fix and the test measures the wrong path. Then drive send to completion, then a fresh send + measure that a 300ms-deadline poll parks ~full deadline.

**Fix #2 — `prepare_fetch_requests` up-front skip (src/consumer/internals/abstract_fetch.rs top, ~line 575).**
After `cluster = metadata_arc().fetch()` and BEFORE the SubscriptionState lock / fetchable scan / compute_buffered_nodes: `if cluster.nodes().iter().all(|n| nodes_with_pending_fetch_requests.contains(&n.id()) || is_unavailable(n)) { return Ok(HashMap::new()) }`. STATELESS short-circuit, NOT a cache (stale cache would strand a partition). Empty cluster → `all()` over empty = true → empty (matches Java 0==0).
- Test fixtures: `bootstrap_nodes` helper = `metadata.add_transient_topics(...)` + `request_test_utils::metadata_update_with(num_nodes, counts)` + `metadata_arc().update_with_current_request_version(&resp, false, 0)`. `metadata_update_with_full` makes node ids `0..num_nodes`, partition leader = node[p % nodes]. For a fetchable partition with a resolvable leader use `seek_validated(tp, FetchPosition::with_leader(off, Some(epoch), LeaderAndEpoch::new(Some(node), Some(epoch))))` (goes straight to Fetching).
- "skips SubscriptionState lock" proof pattern: POISON the `subs` mutex via `catch_unwind` panic-while-holding-guard, then assert `prepare_fetch_requests` returns `Ok(empty)` without panicking (the lock sites use `.expect("...poisoned")` so any lock attempt panics).

**Mutation-check both fixes** (revert-and-confirm-failure): Fix #1 reinstating `completed_sends` in break → send-only test fails; Fix #2 removing short-circuit → poisoned-lock test panics, making it unconditionally `true` → freeing-node test stall-fails.

**Format/lint gotcha**: repo had pre-existing dirty files at session start (`completed_fetch.rs`, `fetch_request_manager.rs` — `created_at`/`fetch_diag` diagnostic). `cargo xtask format-check` flags them but they're NOT mine — `rustfmt --edition 2024 <single-file>` on only my two files, stage only my files, leave the rest dirty.
