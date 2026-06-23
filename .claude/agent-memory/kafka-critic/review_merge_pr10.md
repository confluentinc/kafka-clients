---
name: review-merge-pr10
description: Audit heuristics for the origin/master PR#10 (perf+logging) merge into consumer-impl — selector wakeup, &mut serialize, Arc<Cluster> metadata
metadata:
  type: project
---

Reviewed merge `47b8579` (consumer-impl `e7fe517` × master `c2d0e1b`). Verdict: clean.

**Why:** PR#10 brought master's Milestone-5 perf (zero-copy SendBuilder, sync
try_read drain) + LogContext logging into the consumer branch. Highest risk was
the conflict resolution silently dropping the consumer's join-stall fix or
wakeup machinery.

**How to apply (re-usable merge-audit heuristics for this repo):**
- Selector/wakeup: confirm `run_once` Phase-4 poll is poke-not-cancel
  (`tokio::pin!(poll_fut)` + `&mut poll_fut` in select!, arms only
  `notify_one()` + set `poked`). Confirm selector poll loop keeps
  `any_channel_mid_receive()`+`deferred_wakeup` and the `notify.notified()=>true`
  arm. Any plain `break`-on-wakeup or `select!` cancelling `poll_default` =
  join-stall regression (see [[review_join_stall_fix]]).
- `&mut self` serialize change: the destructive part is generated `write` doing
  `.take()`/`mem::take` on Records fields. Only ProduceRequest/FetchResponse
  carry Records. Consumer requests have none; FetchResponse is decode-only on the
  consumer — so `data_mut()` threading is benign for consumer types. Verify build
  happens once + serialize once on the production send path (network_client.rs).
- `Metadata::fetch()` now returns `Arc<Cluster>`; `metadata_snapshot` is
  `Arc<MetadataSnapshot>`. Consumer's `retain_topic_with_id_fn` must still be
  built into both `retain` (name-only) and `retain_with_id` closures and threaded
  into `handle_metadata_response`. `ConsumerMetadata::new` uses
  `LogContext::empty()` = logging-prefix gap only, acceptable.

**Latent hazard found (not a bug):** `Selectable::wakeup_notify` default impl
returns a fresh `Arc::new(Notify::new())` that no poll() awaits. All real impls
override it; only the never-blocking MockSelector uses the default. A future impl
forgetting the override would silently lose wakeups (same class as join stall).
Suggested hardening: make it a required method. Flagged in COMMENTS.18.md.
