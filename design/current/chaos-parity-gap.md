# Chaos harness — parity gap vs. librdkafka `chaos.py`

Status: living checklist · Date: 2026-09-03 · Owner: Pranav Rathi

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
| `--idle-threshold-s` (early drain exit) | 0 | ❌ |
| `--unclean` (SIGKILL) | off | ✅ CLI flag (verified) |
| `--consumers N` | 3 | ✅ via repeatable `--workload consumer:<backend>` |
| `--leave-broker-down IDX` | — | ✅ `--leave-broker-down` |
| `--reassign-mode change-leader\|reassign-partitions` | — | 🟡 `--action change-leader` ✅; `reassign-partitions` errors (not wired) |
| `--topic-chaos recreate-immediate\|recreate-delayed N` | — | 🟡 `--action topic-recreate` errors (not wired) |
| `--rebalance-add-cycle N` / `--rebalance-remove-cycle N` | — | ❌ |
| `--seed` (deterministic roll order) | — | ✅ `--seed` |
| `--manual` (REPL) | off | ❌ (§5) |
| `--log-dir` / `--log-budget-bytes` | ./logs, 1 GB | ❌ (stderr only) |
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
| `change-leader` (preferred election, no data move) | ✅ | ✅ `--action change-leader` | AdminClient `elect_leaders(Preferred)` |
| `reassign-partitions` (data move) | ✅ | ✅ `--action reassign-partitions` (verified) | describe_topics → rotate replicas → alter_partition_reassignments → poll list until empty |
| Topic delete/recreate (immediate) | ✅ | ✅ `--action topic-recreate` (verified) | delete → wait-absent → recreate; auto-create disabled; expected-loss accounted |
| Topic delete/recreate (delayed dwell) | ✅ | ✅ `--action topic-recreate --dwell-s N` (verified) | " |
| Consumer add/remove mid-run (rebalance) | ✅ | ❌ | trait supports extra specs; no *dynamic* add mid-run yet |
| Leave one broker down permanently | ✅ | ✅ `--leave-broker-down` | `docker stop` without start |

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
| Conservation (delivered vs consumed) | `conservation.txt` | ✅ in-memory verdict |
| Per-record bookkeeping (never-acked/lost) | `verify.txt` | 🟡 loss + dup + expected-lost (topic-recreate) counts (ack-callback buckets are share-specific) |
| Partition coverage | `partition-coverage.txt` | 🟡 count only, no pre/post rxmsgs window |
| Leader-change log | `leader-log.txt` | ❌ |
| Metadata-refresh histogram | `metadata-trigger.txt` | ❌ (needs Rust `log` capture) |
| Gap-signature summary | `summary.txt` | ❌ (greps client debug log) |
| On-disk report files | ✅ | ❌ stdout only |
| Per-workload log files + rotation + budget | ✅ | ❌ |

## 7. Advanced / operational

| Feature | librdkafka | Ours |
|---|---|---|
| `chaos_until_fail.sh` loop | ✅ | ❌ (could be a `--repeat` flag or `/loop`) |
| Run archival `runs/<id>/iter-NNN-<verdict>/` | ✅ | ❌ |
| Idle-based early drain exit | ✅ | ❌ |
| Deterministic seed | ✅ | ❌ |
| Observation-window (pre/post) partition snapshots | ✅ | ❌ |

## 8. Out of scope now (share-consumer / KIP-932)

⏸ Ack matrix (Accept/Release/Reject/Renew), delivery-count distribution,
orphan-ack accounting, `share.auto.offset.reset` seeding, `-S` workload,
`share_consume_verify`. All return when the share consumer is implemented
(`release-test-plan-share-consumer.md` §4–§5).

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
