---
name: review-m13-phase5-admin
description: M13 Phase 5 admin (AK 4.3.1) review — KAFKA-20673 stale-leader lookup retry + KIP-1066 cordoned log dirs; CLEAN, 0 findings, with reusable adjudication heuristics
metadata:
  type: project
---

M13 Phase 5 (commit 7165f913, branch milestone-12-ak-4.3.1) reviewed CLEAN — 0
findings. Reusable heuristics that resolved the near-misses:

- **if-let-chain MutexGuard drop (edition 2024).** `if let Some(x)=a && ... &&
  driver.lock().unwrap().method() { body }` — in edition 2024 (RFC 3606) the
  condition temporaries (incl. the `MutexGuard`) drop BEFORE the body, so
  `body` can re-lock the same `std::sync::Mutex` without deadlock. In ≤2021 the
  guard would live across the body → deadlock. Don't flag it on 2024 without
  first running a test that reaches the true branch; a fast-passing test proves
  no deadlock. Verified via `test_list_offsets_retries_lookup_when_cached_leader_leaves_cluster`.

- **"Java throws vs Rust unwrap_or(false)" is often a non-issue if the throw is
  reproduced UPSTREAM.** The KAFKA-20673 hook uses `mm.is_ready().unwrap_or(false)`
  where Java `isReady()` throws on fatal metadata. Benign because
  `NodeProvider::ConstantNodeId::provide` already does `is_ready()?` first
  (Java `ConstantNodeIdProvider.provide` calls `isReady()` first too) — a fatal
  state surfaces as `Err` from provide() → runnable `Err` arm → fail_call, so
  the `Ok(None)` arm (which calls the hook) is unreachable while fatal. Always
  check whether the same guard fires earlier on the call path before flagging a
  swallowed-error divergence.

- **Commit-cite adjudication method.** To confirm "commit X is client-side, not
  broker-only": `git -C kafka show X --stat | grep clients/src` and
  `git log 4.2.0..4.3.1 -- <ClientFile.java>` to find the introducing commit.
  Here cf9f8ad376 (KAFKA-20441) had 31 `isCordoned` refs but ZERO clients/src
  files (broker-only); a45d36ca5d (KIP-1066) is the real client-side change.
  Actor's PLAN correction (cf9→a45d) was CORRECT.

- **KafkaAdminClientTest +166/−17 delta was 2 new tests + isCordoned assertions
  + mkMap→Map.of refactors + updateFeatures(map) overload usage.** Enumerate via
  `git diff 4.2.0..4.3.1 -- ...KafkaAdminClientTest.java | grep '^@@'` then read
  each hunk; the small hunks were all the recorded-skip refactors.

- **KAFKA-20673 mechanism (faithful):** `Call.handle_node_unavailable_fn`
  (default None→false, base) set only in `new_driver_call` for ALL driver calls
  (Java sets it in `newCall`). Runnable `Ok(None)` arm calls it and drops the
  call on true. `AdminApiDriver::maybe_retry_lookup` = clear_inflight_request +
  retry_lookup, `any(destination_broker_id.is_none())` gate. Deadline NOT
  extended: driver holds fixed `deadline_ms` field; re-emitted specs reuse it,
  so timeout still fires at original deadline. Futures live in the shared driver,
  not the Call, so dropping the Call strands nothing.
