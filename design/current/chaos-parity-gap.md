# Chaos harness — parity gap vs. librdkafka `chaos.py`

Status: living checklist · Date: 2026-09-04 (code-level audit) · Owner: Pranav Rathi

Compares our chaos / fault-injection harness (`tests/chaos/`,
`tests/common/broker_control.rs`) against librdkafka's
`tests/chaos/chaos.py` on the `dev_kip-932_chaos-test-harness` branch.
Legend: ✅ done · 🟡 partial · ❌ missing · ⏸ deferred by scope decision.

Scope reminder: our harness targets the **producer + KIP-848 consumer**.
librdkafka's targets the **KIP-932 share consumer** (its default workload is
`share_consume_verify` / `rdkafka_performance -S`). So every share-specific
row below is out of scope until KIP-932 lands in `src/`
(`consumer-threading.md` §20).

---

## 1. Invocation model

| Capability | librdkafka | Ours | Notes |
|---|---|---|---|
| Single flag-driven command | ✅ `python3 chaos.py --flags` | ✅ | `cargo xtask chaos --brokers … --cycles … --unclean --workload role:backend …` parses flags → `CHAOS_*` env → generic `run_test::chaos_run`. Verified end-to-end. |
| Pluggable workload (workload decoupled from orchestrator) | ✅ spawns any workload binary | ✅ | `Workload` trait + `WorkloadSpec` "role:backend"; orchestrator does only chaos. |
| Mix backends in one run | ✅ (via separate binaries) | 🟡 | Trait + `--workload` support it (`producer:rust` + `consumer:python`); only rust actually runs today (§4). |

## 2. CLI flags

| Flag | librdkafka default | Ours |
|---|---|---|
| `--brokers` | 3 | ✅ CLI flag |
| `--partitions` | 3 | ✅ CLI flag |
| `--replication` | 3 | 🟡 derived (min(brokers,3)), not a flag |
| `--cycles` | 3 | ✅ CLI flag (multi-cycle) |
| `--stop-s` / `--up-s` | 5 / 5 | ✅ `--stop-s` / `--up-wait-s` |
| `--drain-s` | 30 | ✅ `--drain-s` |
| `--idle-threshold-s` (early drain exit) | 0 | ✅ `--idle-threshold-s` (default 3; A2) |
| `--unclean` (SIGKILL) | off | ✅ CLI flag (verified) |
| `--consumers N` | 3 | ✅ via repeatable `--workload consumer:<backend>` |
| `--leave-broker-down IDX` | — | ✅ `--leave-broker-down` |
| `--reassign-mode change-leader\|reassign-partitions` | — | ✅ `--change-leader` and `--reassign-partitions` (both effect-verified) |
| `--topic-chaos recreate-immediate\|recreate-delayed N` | — | ✅ `--topic-recreate` + `--dwell-s N` (effect-verified) |
| `--rebalance-add-cycle N` / `--rebalance-remove-cycle N` | — | ✅ (verified) |
| `--seed` (deterministic roll order) | — | ✅ `--seed` |
| `--manual` (REPL) | off | ❌ (§5) |
| `--log-dir` / `--log-budget-bytes` | ./logs, 1 GB | ✅ `--reports` (dir under `target/chaos-runs/`) + `--log-budget-mb` |
| `--scenario` (cluster scenario file) | default | 🟡 `--scenario NAME` runs a named smoke test (different meaning) |
| `--rps N` (producer rate) | — (perf tool flag) | ✅ `--rps` |
| `--commit sync\|async` | — | ✅ `--commit` |
| `--topic NAME` | — | ✅ `--topic` |

## 3. Chaos actions

| Action | librdkafka | Ours | Backing API (ours) |
|---|---|---|---|
| Broker roll — clean (SIGTERM) | ✅ | ✅ | `docker stop` |
| Broker roll — unclean (SIGKILL) | ✅ | ✅ `--unclean` (verified) | `docker kill` |
| Multi-cycle roll, seeded order | ✅ | ✅ `--cycles` + `--seed` (verified 3 brokers × 1 cycle) | — |
| `change-leader` (preferred election, no data move) | ✅ | ✅ `--change-leader` (effect-verified) | reorder replicas (same set, no data move) → elect preferred → **verify EACH partition's leader == planned first replica** (A3, ≥⅔ tolerance for transient election failures) |
| `reassign-partitions` (data move) | ✅ | ✅ `--reassign-partitions` (verified) | describe_topics → rotate replicas → alter → poll until complete → elect preferred leaders → **assert replica set changed AND verify EACH partition's leader == planned first replica** (A3) |
| Topic delete/recreate (immediate) | ✅ | ✅ `--topic-recreate` (effect-verified) | delete → wait-absent → recreate; auto-create disabled; expected-loss accounted; **asserts the topic_id changed** (new generation, not the old topic lingering) |
| Topic delete/recreate (delayed dwell) | ✅ | ✅ `--topic-recreate --dwell-s N` (effect-verified) | " |
| Consumer add/remove mid-run (rebalance) | ✅ | ✅ `--rebalance-add-cycle N` / `--rebalance-remove-cycle N` (verified) | FuturesUnordered live set + WorkloadPool add/remove |
| Leave one broker down permanently | ✅ | ✅ `--leave-broker-down` | `docker stop` without start |
| **Compose multiple fault *types* in one run** (broker roll **and** topic-chaos, layered) | ✅ (`--topic-chaos` / `--rebalance-mid-roll` overlay a broker roll) | ✅ (A1) broker rolling is the default fault; layer more with `--topic-recreate [N]` / `--reassign-partitions [N]` / `--change-leader [N]` (optional per-N-cycle cadence), `--no-broker-roll` for pure migration | e.g. `--topic-recreate 2 --reassign-partitions 3` rolls brokers every cycle, recreates on even cycles, reassigns on cycle 3. Mirrors chaos.py (rolling implicit, faults layered on). |

**Composition note (methodology fix):** rows above audit whether each fault
*exists* individually — they do. librdkafka's distinguishing capability is
*composing* them (its default scenario rolls brokers **while** injecting
topic-chaos and rebalances). A1 matches this: broker rolling is the default
fault and the others layer on with their own cadences. An earlier version of
this doc omitted this row and so overstated action-matrix parity.

## 4. Workload backends

| Backend | librdkafka | Ours |
|---|---|---|
| Native client | ✅ (C, in-proc-ish) | ✅ Rust in-process |
| Python binding | — (N/A) | ✅ wired: `--workload consumer:python` starts the python gRPC-server container (auto-selects `multilanguage-tests`) |
| C binding | ✅ (it *is* C) | ✅ wired: `--workload consumer:c` starts the c gRPC-server container |
| External arbitrary binary | ✅ (spawns any) | ❌ (deliberately not in first cut) |
| Producer restart accounting | ✅ | N/A — our workloads are in-process, they don't restart |

## 5. Modes

| Mode | librdkafka | Ours |
|---|---|---|
| Auto | ✅ | 🟡 one cycle, one action |
| Manual REPL (`stop/kill/start/change-leader/reassign/add-consumer/show/leaders/status/sleep/…`) | ✅ | ❌ |

## 6. Reports

| Report | librdkafka file | Ours |
|---|---|---|
| Conservation (delivered vs consumed) | `conservation.txt` | ✅ verdict, persisted to `verdict.txt` with `--reports` |
| Per-record bookkeeping (never-acked/lost) | `verify.txt` | 🟡 loss + dup(by index & offset) + expected-lost counts. NO ack buckets (never-acked / acked-with-err / acked-ok) and NO delivery-count distribution — see §10 #2 |
| Partition coverage | `partition-coverage.txt` | 🟡 "each partition carried ≥1 record ever", threshold = half partitions. NO pre/post rxmsgs observation window — see §10 #3 |
| Leader-change log | `leader-log.txt` | ✅ `leader-changes.txt` records change-leader/reassign diffs AND (A4) samples leaders before-stop/while-down/after-start on each broker roll, logging migrations |
| Metadata-refresh histogram | `metadata-trigger.txt` | 🟡 NOT grouped by (module, reason); folded into the 7 flat substring counts in `summary.txt` |
| Gap-signature summary | `summary.txt` | 🟡 7 flat substring counts from the captured client log (the `"leader"` substring over-counts); librdkafka has ~10 regex signatures with first/last timestamps |
| On-disk report files | ✅ | ✅ `target/chaos-runs/<id>/` with `--reports` |
| Captured client log | ✅ (per-consumer stderr) | ✅ `client.log` — process-global `log::Log` capture with `--reports` |
| Per-workload log files + rotation + budget | ✅ | ✅ `client-<clientId>.log` per workload, rotating at `--log-budget-mb` (one backup) |

## 7. Advanced / operational

| Feature | librdkafka | Ours |
|---|---|---|
| `chaos_until_fail.sh` loop | ✅ | ✅ `--repeat N` — stop on first failure |
| Run archival `runs/<id>/iter-NNN-<verdict>/` | ✅ | 🟡 per-iteration dir under `target/chaos-runs/<id>/` + `run-history.tsv` (no verdict-named dirs) |
| Idle-based early drain exit | ✅ | ✅ (A2) `--idle-threshold-s` (default 3): drain ends once consume-progress is flat, capped by `--drain-s` |
| Conservation ratio bound (fail on `consumed > 2× delivered`) | ✅ | ❌ duplicates counted but NEVER fail the run — see §10 #9 |
| Roll order per cycle | ✅ random `rng.shuffle` | 🟡 seeded rotation `rotate_left((seed+cycle)%n)` (reproducible, fewer orderings) |
| Observation-window (pre/post) partition snapshots | ✅ | ❌ see §10 #3 |

## 8. Out of scope now (share-consumer / KIP-932)

⏸ Ack matrix (Accept/Release/Reject/Renew), delivery-count distribution,
orphan-ack accounting, `share.auto.offset.reset` seeding, `-S` workload,
`share_consume_verify`. All return when the share consumer is implemented
(`release-test-plan-share-consumer.md` §4–§5).

## 10. Verification-depth gaps (from a code-level audit of chaos.py)

The row checklist above tracks whether each *fault* and *report file* exists.
This section records the deeper finding from auditing chaos.py's actual
computations: we reproduced the **fault injection** well but under-built the
**verification**, which is librdkafka's real purpose. Ranked by value:

1. ~~**Compose fault types in one run**~~ — DONE (A1). Broker rolling is the
   default fault; topic-recreate/reassign/change-leader layer on via their own
   flags with per-N-cycle cadences (chaos.py-style); verified end-to-end.
2. **Per-record ack classification + delivery-count** — chaos.py buckets each
   record into never-acked / acked-with-err / acked-ok and builds a
   delivery-count (`dc`) distribution. We have neither (the `Acked`/
   `DeliveryCount` events are dead-code). Partly share-consumer-shaped, but even
   for the KIP-848 consumer we don't track per-record commit outcome.
3. **PRE/POST observation window** — chaos.py snapshots each partition's
   `rxmsgs` before cooldown and after drain and FAILS if a partition didn't grow
   *during the window* ("orphaned in observation window"). Ours only checks a
   partition carried ≥1 record ever, so a partition that goes dark mid-run
   passes. Applies directly to our current consumer — high value.
4. **Per-partition HWM accounting across topic-recreate** — chaos.py snapshots
   pre-delete high-watermarks, computes expected-loss per partition, and
   verifies the NEW generation consumed `[0..hwm)` fully. Ours blanket-marks all
   delivered-so-far as expected-lost with no HWM diff or new-gen coverage check.
5. ~~**Leader sampling around broker rolls**~~ — DONE (A4). Each broker roll
   now samples leaders before-stop/while-down/after-start and logs the
   migration diff to `leader-changes.txt`.
6. ~~**Per-partition leader-plan verification**~~ — DONE (A3). Both change-leader
   and reassign now verify EACH partition's leader == planned first replica
   (≥⅔ tolerance for transient election failures), not just an aggregate count.
7. **(module, reason) metadata-trigger grouping** — we do flat substring counts.
9. **Conservation ratio bound** — chaos.py fails on `consumed > 2× delivered`;
   we never fail on duplicates.

Honest note: an earlier framing called this harness "close to parity." That was
wrong on verification depth. Fault injection is strong; #2/#3/#4/#6 are where we
are materially behind, and #3/#6 apply to the current (non-share) consumer.

## 9. Deliberate divergences (not gaps)

- **No Python/trivup runner** — CLAUDE.md §6 (xtask over scripts). Docker
  `stop`/`kill`/`start` already equals trivup SIGTERM/SIGKILL.
- **In-process Rust workload** instead of a spawned binary — so no
  producer/consumer *restart* accounting is needed (they never restart).
- **Docker/testcontainers** cluster instead of native processes.

---

## Suggested next-step ordering

1. **CLI runner** (§1, §2) — turn `cargo xtask chaos` into the flag-driven
   single command consuming `--workload role:backend`, `--brokers`,
   `--partitions`, `--cycles`, `--unclean`, `--drain-s`, `--rps`, `--seed`.
   Biggest UX win; the trait already makes it thin.
2. **Action matrix** (§3) — multi-cycle, unclean, change-leader, reassign,
   topic-chaos, leave-broker-down, consumer add/remove.
3. **gRPC backend launch** (§4) — make `producer:python` / `:c` actually run.
4. **On-disk reports + leader log** (§6).
5. **Manual REPL** (§5) and **chaos-until-fail / archival** (§7).
