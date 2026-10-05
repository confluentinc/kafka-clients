// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The generic, flag-driven chaos runner.
//!
//! `cargo xtask chaos --flags` parses CLI flags into `CHAOS_*` environment
//! variables (see `xtask`), and this single `#[test]` reads them into a
//! [`ChaosConfig`] and runs the configured scenario end to end. This is the
//! single-command entry point at parity with librdkafka's `chaos.py`
//! (`design/current/chaos-parity-gap.md` §1).
//!
//! Run:
//! ```text
//! cargo xtask chaos --brokers 3 --cycles 3 --unclean \
//!     --workload producer:rust --workload consumer:rust
//! ```

use std::sync::Arc;
use std::time::Duration;

use super::actions::{ChaosAction, LEADER_PLAN_SETTLE, REASSIGN_COMPLETE_MAX, ReassignMode};
use super::common::broker_control::{BrokerControl, CLEAN_STOP_GRACE, StopKind};
use super::config::{ActionKind, ChaosConfig};
use super::harness::{CONSUMER_STOP_DEADLINE, ChaosHarness, PRODUCER_STOP_DEADLINE, RunningWorkloads, WorkloadPool};
use super::isolation::{self, ForcedExit, signals};
use super::reports::{self, ReportsHandle, RunReports};
use super::workload::{Backend, Role};
use confluent_kafka::admin::Admin;
use futures_util::FutureExt as _;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// A fully-resolved action for one cycle of `--random` mode: the fault to run
/// plus every parameter the RNG chose for it. Unlike the fixed-cadence path
/// (which reads parameters from the config), a random action carries its own
/// resolved parameters so the same seed reproduces it exactly.
enum PlannedAction {
    /// Roll one broker: chosen node, clean/unclean, and how long it stays down.
    BrokerRoll {
        node_id: u16,
        kind: StopKind,
        down: Duration,
    },
    /// Preferred-leader change (no data move).
    ChangeLeader,
    /// Partition reassignment (data moves).
    ReassignPartitions,
    /// Delete + recreate ONE chosen topic with the chosen dwell (0 = immediate).
    TopicRecreate { topic: String, dwell: Duration },
}

/// Longest a randomly-planned broker roll keeps the broker down, in seconds.
const RANDOM_MAX_DOWN_S: u64 = 12;
/// Longest a randomly-planned topic recreate dwells between delete and create.
const RANDOM_MAX_DWELL_S: u64 = 8;

/// How long the scenario task may go without a heartbeat before the heartbeat
/// watchdog ends the run. Every await in the scenario yields well within this;
/// only a blocked task reaches it.
const HEARTBEAT_STALE: Duration = Duration::from_secs(120);

/// How long an abandoned run waits for its workloads to close.
const ABORT_STOP_WAIT: Duration = Duration::from_secs(30);

/// How long after teardown the process is forced to exit if it has not.
const FORCED_EXIT_AFTER: Duration = Duration::from_secs(60);

/// How long the whole wind-down after the drive may take before the process
/// is forced to exit: stopping an abandoned run's workloads, the verdict and
/// the reports (a few seconds), and the cluster teardown, then the usual grace
/// period. Armed before any of it, so a wind-down step that hangs or panics
/// cannot leave the process running.
const WIND_DOWN_BUDGET: Duration = Duration::from_secs(
    ABORT_STOP_WAIT.as_secs() + 60 + isolation::TEARDOWN_BUDGET.as_secs() + FORCED_EXIT_AFTER.as_secs(),
);

/// Bound on one `describe_topics` in the actions' leader sampling
/// (`partition_state` in `actions.rs`).
const DESCRIBE_TIMEOUT_S: u64 = 10;

/// Bound on building one gRPC-backed workload: the first one per backend
/// starts its server container (`backend_pool::get_or_start`).
const GRPC_BUILD_S: u64 = 60;

/// Printed when a passing run is forced to exit; the matrix runner classifies
/// it as a pass (`xtask/src/chaos_matrix.rs`).
const FORCED_EXIT_AFTER_PASS: &str =
    "chaos: FORCED EXIT after a PASS verdict — the process did not terminate after teardown";

/// Draw one cycle's action in `--random` mode from the seeded `rng`. Returns
/// `None` for a quiet cycle (probability `1 - action_prob`). All four fault
/// types are candidates (topic-recreate only when
/// [`ChaosConfig::random_excludes_topic_recreate`] allows it); every parameter
/// is drawn from `rng` so the run is reproducible for a given seed.
fn random_plan(rng: &mut StdRng, cfg: &ChaosConfig) -> Option<PlannedAction> {
    if rng.random::<f64>() >= cfg.action_prob {
        return None; // quiet cycle
    }
    // Candidate faults, all equally likely, broker-roll included. TopicRecreate
    // (candidate 3) is EXCLUDED under multi-topic unless
    // `--allow-multi-topic-recreate`: the KIP-848 consumer keeps stale positions
    // on some partitions of a recreated topic when it is one of several
    // subscribed topics and never reads its new generation — the same known
    // consumer-side limitation for which `ChaosConfig::from_env` rejects
    // `--num-topics > 1 --topic-recreate` without that flag. Otherwise all 4
    // are candidates; an excluded run draws only broker-roll / change-leader /
    // reassign (the runner prints a notice).
    let candidates = if cfg.random_excludes_topic_recreate() { 3 } else { 4 };
    let plan = match rng.random_range(0..candidates) {
        0 => {
            // Pick a broker that is not permanently left down. `from_env`
            // guarantees at least one is eligible.
            let eligible: Vec<u16> = (1..=cfg.brokers).filter(|b| cfg.leave_broker_down != Some(*b)).collect();
            let node_id = eligible[rng.random_range(0..eligible.len())];
            // Clean vs unclean chosen at random; `--unclean` is rejected in
            // random mode (`from_env`) because the monkey does both.
            let kind = if rng.random::<bool>() {
                StopKind::Unclean
            } else {
                StopKind::Clean
            };
            // Down duration: 3..=12 s, drawn (`--stop-s` is rejected in random
            // mode). `RANDOM_MAX_DOWN_S` is the upper bound the watchdog budgets.
            let down = Duration::from_secs(rng.random_range(3..=RANDOM_MAX_DOWN_S));
            PlannedAction::BrokerRoll { node_id, kind, down }
        },
        1 => PlannedAction::ChangeLeader,
        2 => PlannedAction::ReassignPartitions,
        _ => {
            // Dwell 0 (immediate) or 3..=8 s (delayed), 50/50.
            // `RANDOM_MAX_DWELL_S` is the upper bound the watchdog budgets.
            let dwell = if rng.random::<bool>() {
                Duration::ZERO
            } else {
                Duration::from_secs(rng.random_range(3..=RANDOM_MAX_DWELL_S))
            };
            // Pick one random topic (librdkafka's rng.choice(topics)).
            let topics = cfg.topics();
            let topic = topics[rng.random_range(0..topics.len())].clone();
            PlannedAction::TopicRecreate { topic, dwell }
        },
    };
    Some(plan)
}

/// Execute one randomly-planned action (its parameters are already resolved).
async fn run_planned(
    plan: PlannedAction,
    cfg: &ChaosConfig,
    brokers: &BrokerControl<'_>,
    admin: &dyn Admin,
    harness: &ChaosHarness,
    reports: &ReportsHandle,
) {
    match plan {
        PlannedAction::BrokerRoll { node_id, kind, down } => {
            ChaosAction::BrokerRoll {
                node_id,
                kind,
                down,
                wait_up: cfg.up_wait_dur(),
                topics: harness.topics().to_vec(),
            }
            .execute(brokers, admin, reports)
            .await;
        },
        PlannedAction::ChangeLeader => {
            ChaosAction::Migrate {
                topics: harness.topics().to_vec(),
                mode: ReassignMode::ChangeLeader,
                live_brokers: cfg.live_broker_ids(),
            }
            .execute(brokers, admin, reports)
            .await;
        },
        PlannedAction::ReassignPartitions => {
            ChaosAction::Migrate {
                topics: harness.topics().to_vec(),
                mode: ReassignMode::ReassignPartitions,
                live_brokers: cfg.live_broker_ids(),
            }
            .execute(brokers, admin, reports)
            .await;
        },
        PlannedAction::TopicRecreate { topic, dwell } => {
            harness.recreate_topic(&topic, dwell).await;
        },
    }
}

/// Deterministic broker-roll order for a cycle: a seeded rotation of
/// `1..=brokers`, skipping any broker kept permanently down.
fn roll_order(config: &ChaosConfig, cycle: u32) -> Vec<u16> {
    let mut order: Vec<u16> = (1..=config.brokers).filter(|b| config.leave_broker_down != Some(*b)).collect();
    if order.is_empty() {
        return order;
    }
    // Seeded rotation: offset by (seed + cycle) so runs are reproducible for a
    // given --seed but each cycle rolls a different broker first.
    let rot = ((config.seed.wrapping_add(u64::from(cycle))) % order.len() as u64) as usize;
    order.rotate_left(rot);
    order
}

/// Run one chaos action for the current cycle: every fault flag that fires
/// this cycle, and the per-cycle overlays (A1 — compose fault types).
#[expect(clippy::too_many_arguments)]
async fn run_action(
    action: ActionKind,
    cfg: &ChaosConfig,
    cycle: u32,
    stop_kind: StopKind,
    brokers: &BrokerControl<'_>,
    admin: &dyn Admin,
    harness: &ChaosHarness,
    reports: &ReportsHandle,
    rng: &mut StdRng,
) {
    match action {
        ActionKind::BrokerRoll => {
            for node in roll_order(cfg, cycle) {
                ChaosAction::BrokerRoll {
                    node_id: node,
                    kind: stop_kind,
                    down: cfg.stop_dur(),
                    wait_up: cfg.up_wait_dur(),
                    topics: harness.topics().to_vec(),
                }
                .execute(brokers, admin, reports)
                .await;
            }
        },
        ActionKind::ChangeLeader => {
            ChaosAction::Migrate {
                topics: harness.topics().to_vec(),
                mode: ReassignMode::ChangeLeader,
                live_brokers: cfg.live_broker_ids(),
            }
            .execute(brokers, admin, reports)
            .await;
        },
        ActionKind::ReassignPartitions => {
            ChaosAction::Migrate {
                topics: harness.topics().to_vec(),
                mode: ReassignMode::ReassignPartitions,
                live_brokers: cfg.live_broker_ids(),
            }
            .execute(brokers, admin, reports)
            .await;
        },
        ActionKind::TopicRecreate => {
            // librdkafka's topic-chaos picks ONE random topic per firing
            // (`rng.choice(topics)`). Seeded so a given --seed reproduces it.
            let topic = pick_topic(rng, harness);
            harness.recreate_topic(&topic, Duration::from_secs(cfg.dwell_s)).await;
        },
        ActionKind::AllBrokersDown => {
            // Every broker that is up; one kept down by --leave-broker-down
            // stays down.
            let nodes = (1..=cfg.brokers).filter(|b| cfg.leave_broker_down != Some(*b)).collect();
            ChaosAction::AllBrokersDown {
                nodes,
                outage: Duration::from_secs(cfg.outage_s),
                wait_up: cfg.up_wait_dur(),
            }
            .execute(brokers, admin, reports)
            .await;
        },
    }
}

/// Pick one random topic from the run's set, seeded by `rng` (librdkafka's
/// `rng.choice(topics)` in `_topic_chaos_thread`). For a single-topic run this
/// always returns that topic.
fn pick_topic(rng: &mut StdRng, harness: &ChaosHarness) -> String {
    let topics = harness.topics();
    topics[rng.random_range(0..topics.len())].clone()
}

/// One consumer-churn tick (`--consumer-churn-min`/`--consumer-churn-max`):
/// stop a random batch of dynamically-added consumers, then start a random batch,
/// keeping the live consumer count within `[min, max]`. Mirrors librdkafka's C
/// chaos reference (`tests/8003-chaos-testing-consumer-group.c`):
/// `to_stop = rand()%running + 1`, `to_start = rand()%can_start + 1`, bounded by
/// `[1, CONSUMER_CNT]` — here bounded by `[min, max]`. The `min` base consumers
/// are fixed (built up front, never removed); churn moves the `max - min`
/// headroom on top of them, so live count = `min + dynamic ∈ [min, max]`.
async fn churn_consumers(pool: &WorkloadPool<'_>, backend: Backend, min: u32, max: u32, rng: &mut StdRng) {
    // Stop a random batch of the currently-added dynamic consumers (down to the
    // fixed `min` floor — `dynamic` is exactly how many are removable).
    let dynamic = pool.dynamic_consumer_count() as u32;
    if dynamic > 0 {
        let to_stop = rng.random_range(1..=dynamic);
        for _ in 0..to_stop {
            pool.remove_consumer();
        }
    }
    // Start a random batch, up to the headroom below `max`.
    let headroom = (max - min).saturating_sub(pool.dynamic_consumer_count() as u32);
    if headroom > 0 {
        let to_start = rng.random_range(1..=headroom);
        for _ in 0..to_start {
            pool.add_consumer(backend).await;
        }
    }
}

/// Roll every broker of the cycle, but inject `hook` into the down-window of the
/// **first** broker only (`--rebalance-mid-roll`). The remaining brokers roll
/// normally. This is what makes the group reassignment kicked off by the hook
/// overlap the leader migration caused by the first broker being down.
#[expect(clippy::too_many_arguments)]
async fn run_broker_roll_with_hook<F>(
    cfg: &ChaosConfig,
    cycle: u32,
    stop_kind: StopKind,
    brokers: &BrokerControl<'_>,
    admin: &dyn Admin,
    harness: &ChaosHarness,
    reports: &ReportsHandle,
    hook: F,
) where
    F: std::future::Future<Output = ()>,
{
    let order = roll_order(cfg, cycle);
    let mut hook = Some(hook);
    for node in order {
        let this_hook = hook.take(); // Some only for the first node.
        ChaosAction::BrokerRoll {
            node_id: node,
            kind: stop_kind,
            down: cfg.stop_dur(),
            wait_up: cfg.up_wait_dur(),
            topics: harness.topics().to_vec(),
        }
        .execute_with_hook(brokers, admin, reports, this_hook)
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "chaos: flag-driven runner; invoked via `cargo xtask chaos`"]
async fn chaos_run() {
    let mut config = match ChaosConfig::from_env() {
        Ok(c) => c,
        Err(err) => panic!("invalid chaos configuration: {err}"),
    };
    // `cargo xtask chaos-matrix` validates every run's configuration this way
    // before starting the first cluster, so a bad matrix line fails in seconds
    // rather than hours into the matrix.
    if std::env::var("CHAOS_CHECK_CONFIG").is_ok_and(|v| v == "1") {
        eprintln!("chaos: configuration OK");
        return;
    }
    // Before any cluster exists: a Ctrl-C / SIGTERM at any point of the run
    // must tear its cluster down (see `isolation::signals`).
    signals::install();

    // Resolve the reproducibility seed. `0` means "unset": pick a fresh one and
    // print it so a run that finds a bug can be replayed with `--seed <printed>`.
    // Every random decision below is driven by an RNG seeded from this value, so
    // the same seed reproduces the entire run (broker-roll order, and in
    // `--random` mode which fault fires each cycle, its parameters, and timing).
    if config.seed == 0 {
        config.seed = rand::random::<u64>() | 1; // never 0, so replay is unambiguous
    }
    if config.random {
        eprintln!("chaos: RANDOM mode — reproduce this exact run with --seed {}", config.seed);
        if config.random_excludes_topic_recreate() {
            eprintln!(
                "chaos: NOTE topic-recreate is excluded from RANDOM candidates under --num-topics > 1 (known \
                 consumer-side limitation: a recreated topic among several keeps stale positions; see \
                 tests/chaos/README.md). Pass --allow-multi-topic-recreate to include it"
            );
        }
    }
    eprintln!("chaos: {}", config.summary());

    let harness = ChaosHarness::start_with_topics(
        &config.topics(),
        config.brokers,
        config.partitions,
        config.replication_factor,
        config.security_protocol,
        config.msg_size,
        Arc::new(super::verifier::ConservationVerifier::new()),
    )
    .await;

    // If a broker is to be left permanently down, stop it before workloads
    // ramp up (librdkafka's --leave-broker-down).
    if let Some(node) = config.leave_broker_down {
        eprintln!("chaos: leaving broker {node} down for the whole run");
        harness.brokers().stop(node, StopKind::Clean).await;
    }

    let workloads = harness
        .build_workloads(&config.workloads, config.rps, config.msg_size, config.commit_mode)
        .await;
    let verifier = harness.verifier();

    // The drain ends as soon as this verifier reports every acknowledged record
    // observed (`Verifier::outstanding`), so the idle threshold is never
    // consulted. Say so when the flag was passed rather than let it look honoured.
    if verifier.outstanding().is_some() && std::env::var("CHAOS_IDLE_THRESHOLD_S").is_ok_and(|v| !v.is_empty()) {
        eprintln!(
            "chaos: NOTE --idle-threshold-s has no effect: this verifier drains until every acknowledged \
             record has been observed (bounded by --drain-s {}s); the idle threshold only applies to a \
             verifier that does not report outstanding records",
            config.drain_s
        );
    }

    let stop_kind = if config.unclean {
        StopKind::Unclean
    } else {
        StopKind::Clean
    };
    let brokers = harness.brokers();
    let admin = harness.admin();
    let harness_ref = &harness;

    // On-disk reports + client-log capture (opt-in via --reports). `echo` is
    // off so the captured client log goes to file only, keeping stderr readable.
    let reports: ReportsHandle = config.reports.then(|| {
        Arc::new(RunReports::new(
            &reports::new_run_id(),
            false,
            config.log_budget_mb * 1024 * 1024,
        ))
    });
    let reports_ref = &reports;

    // The chaos timeline: warm up, then N cycles of the chosen action. The
    // captured `pool` lets it add/remove consumers mid-run (rebalance chaos).
    let cfg = &config;
    let pool = harness.workload_pool();
    let scenario_pool = pool.clone();
    // One RNG for the whole run, seeded from the resolved seed. Threaded serially
    // through the single scenario loop, so the sequence of draws — and therefore
    // the whole random run — is reproducible for a given seed.
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let scenario = async move {
        let pool = scenario_pool;
        tokio::time::sleep(Duration::from_secs(cfg.warmup_s)).await;

        for cycle in 0..cfg.cycles {
            let cycle_1based = cycle + 1;
            eprintln!("chaos: cycle {}/{}", cycle_1based, cfg.cycles);

            // Consumer churn: stop a random batch of dynamically-added consumers,
            // then start a random batch, keeping the live count within
            // [min, max] — librdkafka's chaos consumer-churn (the C reference's
            // stop-random-batch/start-random-batch bounded by [1, CONSUMER_CNT],
            // with min/max the bounds). The `min` base consumers are fixed and
            // never removed here; churn moves the `max - min` headroom on top.
            if let (Some(min), Some(max)) = (cfg.consumer_churn_min, cfg.consumer_churn_max) {
                churn_consumers(&pool, cfg.added_consumer_backend(), min, max, &mut rng).await;
            }

            // --- Random (chaos-monkey) mode: draw one action + its parameters
            // from the seeded RNG, optionally after a random delay, then move to
            // the next cycle. Rebalance add/remove still honour their cycles.
            if cfg.random {
                if cfg.rebalance_add_cycle == Some(cycle_1based) {
                    pool.add_consumer(cfg.added_consumer_backend()).await;
                }
                if cfg.rebalance_remove_cycle == Some(cycle_1based) {
                    pool.remove_consumer();
                }
                // Random pre-action delay within the cycle (0..=between_s
                // seconds), so actions land at varying wall-clock offsets, not
                // cycle-aligned.
                if cfg.between_s > 0 {
                    let jitter = rng.random_range(0..=cfg.between_s);
                    tokio::time::sleep(Duration::from_secs(jitter)).await;
                }
                match random_plan(&mut rng, cfg) {
                    Some(plan) => {
                        run_planned(plan, cfg, &brokers, admin, harness_ref, reports_ref).await;
                    },
                    None => eprintln!("chaos:   quiet cycle (no action)"),
                }
                if cycle + 1 < cfg.cycles {
                    tokio::time::sleep(Duration::from_secs(cfg.between_s)).await;
                }
                continue;
            }

            // --- Fixed-cadence mode (default) ---
            // Is a rebalance scheduled this cycle, and does a broker roll fire
            // this cycle to overlap it with?
            let do_add = cfg.rebalance_add_cycle == Some(cycle_1based);
            let do_remove = cfg.rebalance_remove_cycle == Some(cycle_1based);
            let roll_fires_this_cycle = cfg
                .actions
                .iter()
                .any(|s| s.kind == ActionKind::BrokerRoll && s.fires(cycle_1based));
            // Mid-roll: hold the add/remove and inject it into the roll's
            // down-window so the group reassignment overlaps the leader
            // migration (§`--rebalance-mid-roll`). Only when a roll actually
            // fires this cycle — otherwise there is no down-window and we fall
            // back to the top-of-cycle placement below.
            let mut inject_mid_roll = cfg.rebalance_mid_roll && roll_fires_this_cycle && (do_add || do_remove);

            // Rebalance chaos at the top of the cycle — unless we are deferring
            // it into the roll's down-window (mid-roll).
            if !inject_mid_roll {
                if do_add {
                    pool.add_consumer(cfg.added_consumer_backend()).await;
                }
                if do_remove {
                    pool.remove_consumer();
                }
            }

            // Run every configured action that fires this cycle, in listed
            // order (A1 — compose fault types). Each fault flag's cadence N
            // fires on cycles N, 2N, … (N = 1 → every cycle).
            for spec in &cfg.actions {
                if !spec.fires(cycle_1based) {
                    continue;
                }
                // Mid-roll injection: the FIRST broker roll of the cycle carries
                // the deferred rebalance in its down-window. `inject_mid_roll`
                // is cleared after firing, so a cycle with more than one roll
                // injects once.
                if inject_mid_roll && spec.kind == ActionKind::BrokerRoll {
                    let pool_hook = &pool;
                    let added_backend = cfg.added_consumer_backend();
                    let hook = async move {
                        if do_add {
                            pool_hook.add_consumer(added_backend).await;
                        }
                        if do_remove {
                            pool_hook.remove_consumer();
                        }
                    };
                    run_broker_roll_with_hook(cfg, cycle, stop_kind, &brokers, admin, harness_ref, reports_ref, hook)
                        .await;
                    inject_mid_roll = false;
                } else {
                    run_action(
                        spec.kind,
                        cfg,
                        cycle,
                        stop_kind,
                        &brokers,
                        admin,
                        harness_ref,
                        reports_ref,
                        &mut rng,
                    )
                    .await;
                }
            }

            if cycle + 1 < cfg.cycles {
                tokio::time::sleep(Duration::from_secs(cfg.between_s)).await;
            }
        }

        // Let traffic settle after the last fault before draining.
        tokio::time::sleep(Duration::from_secs(cfg.between_s)).await;
    };

    // Global watchdog: a generous overall wall-clock cap on the whole
    // scenario + drive. A healthy run finishes in a small fraction of this; the
    // cap exists ONLY so a WEDGED cluster — all brokers down and unable to
    // recover, a broker that never rejoins, a cluster degraded past making any
    // progress — fails fast (with container cleanup) instead of hanging
    // indefinitely. It scales with the run: warmup + per-cycle worst case (a
    // roll may wait up to `up_wait_s` for a rejoin) + the close phases + drain,
    // plus margin. Every per-call path already has its own timeout (broker
    // wait_operational, describe sampling); this is the last-resort net for any
    // wedge those don't individually cover.
    let watchdog = watchdog_budget(&config);
    drive_protected(
        harness_ref,
        workloads,
        scenario,
        ProtectedRun {
            name: "chaos run",
            watchdog,
            drain: config.drain_dur(),
            idle_threshold: config.idle_threshold_dur(),
            min_partitions: config.min_partitions(),
            reports: reports_ref,
        },
    )
    .await;
}

/// The global watchdog budget of a run configured by `config`.
///
/// Per cycle, budget every action that can fire: a broker-roll cycle rolls
/// EVERY broker in turn (each waiting `stop_s`, up to `up_wait_s` for the
/// rejoin, plus three leader samples of up to [`DESCRIBE_TIMEOUT_S`] per
/// topic), a recreate waits for the delete and the create (30 s each) plus its
/// dwell, and reassign / change-leader, per topic, wait for the reassignment
/// (bounded by [`REASSIGN_COMPLETE_MAX`]) and the leader plan to settle. Budgeting a single roll per cycle, as this once did, aborted
/// healthy multi-broker runs as "wedged".
///
/// `--random` ignores `config.actions`: any of the four faults can fire on
/// any cycle, with one broker rolled for up to `RANDOM_MAX_DOWN_S` and a
/// dwell of up to `RANDOM_MAX_DWELL_S`, plus the pre-action jitter of up to
/// `between_s`. Budget for all of them, or a random run that happens to draw
/// recreates and reassignments is aborted as wedged.
///
/// Outside the cycles: building the gRPC workloads (and every gRPC consumer
/// the run adds), and the two close phases, which the drive bounds itself
/// ([`PRODUCER_STOP_DEADLINE`], [`CONSUMER_STOP_DEADLINE`]) so that a close
/// hang is reported as such before this budget runs out.
fn watchdog_budget(config: &ChaosConfig) -> Duration {
    let num_topics = u64::from(config.num_topics.max(1));
    let has = |pred: fn(&ActionKind) -> bool| config.random || config.actions.iter().any(|a| pred(&a.kind));
    let (rolled_per_cycle, down_s, dwell_s, jitter_s) = if config.random {
        (1, RANDOM_MAX_DOWN_S, RANDOM_MAX_DWELL_S, config.between_s)
    } else if has(|k| matches!(k, ActionKind::BrokerRoll)) {
        (
            u64::from(config.brokers) - u64::from(config.leave_broker_down.is_some()),
            config.stop_s,
            config.dwell_s,
            0,
        )
    } else {
        (0, 0, config.dwell_s, 0)
    };
    // One roll: the down window, the rejoin wait, three leader samples (before
    // the stop, while down, after the start) of one describe per topic each,
    // and 40 s for the stop / start themselves and the ISR check's last
    // describe overrunning `up_wait_s`. A clean stop (also possible in random
    // mode) may take up to its full `docker stop` grace.
    let clean_stop_s = if config.random || !config.unclean {
        CLEAN_STOP_GRACE.as_secs()
    } else {
        0
    };
    let per_roll = down_s + config.up_wait_s + 3 * DESCRIBE_TIMEOUT_S * num_topics + 40 + clean_stop_s;
    // One migration (reassign or change-leader) of one topic: the wait for any
    // reassignment in progress plus the wait for its own (one shared
    // `REASSIGN_COMPLETE_MAX` deadline), the before- and after-describes, the
    // alter (30 s) and elect RPCs, and the leader-plan settle with its last
    // describe overrunning it.
    let per_migrated_topic = REASSIGN_COMPLETE_MAX.as_secs() + LEADER_PLAN_SETTLE.as_secs() + 90;
    let per_cycle = rolled_per_cycle * per_roll
        + jitter_s
        + if has(|k| matches!(k, ActionKind::TopicRecreate)) {
            60 + dwell_s
        } else {
            0
        }
        + if has(|k| matches!(k, ActionKind::ReassignPartitions)) {
            num_topics * per_migrated_topic
        } else {
            0
        }
        + if has(|k| matches!(k, ActionKind::ChangeLeader)) {
            num_topics * per_migrated_topic
        } else {
            0
        }
        + if config.actions.iter().any(|a| a.kind == ActionKind::AllBrokersDown) {
            // The outage, then the brokers restart together and are waited
            // for one after another (each bounded by `up_wait_s`, but they
            // recover in parallel, so one wait dominates).
            config.outage_s + config.up_wait_s + 60
        } else {
            0
        }
        // A gRPC consumer added by churn (up to `max - min` per cycle) or by
        // `--rebalance-add-cycle` (once) is built inside the scenario.
        + if config.added_consumer_backend().is_grpc() {
            u64::from(
                config
                    .consumer_churn_max
                    .zip(config.consumer_churn_min)
                    .map_or(0, |(max, min)| max - min),
            ) * GRPC_BUILD_S
        } else {
            0
        }
        + config.between_s
        + 30;
    // gRPC workload builds outside the per-cycle figure: one per gRPC spec
    // before the scenario (a producer spec builds one producer per topic),
    // plus the one consumer `--rebalance-add-cycle` adds in its cycle.
    let grpc_builds: u64 = config
        .workloads
        .iter()
        .filter(|w| w.backend.is_grpc())
        .map(|w| if w.role == Role::Producer { num_topics } else { 1 })
        .sum::<u64>()
        + u64::from(config.rebalance_add_cycle.is_some() && config.added_consumer_backend().is_grpc());
    Duration::from_secs(
        config.warmup_s
            + u64::from(config.cycles) * per_cycle
            + grpc_builds * GRPC_BUILD_S
            + PRODUCER_STOP_DEADLINE.as_secs()
            + config.drain_s
            + CONSUMER_STOP_DEADLINE.as_secs()
            + 120,
    )
}

/// Everything [`drive_protected`] needs besides the harness, the workloads and
/// the scenario.
pub(super) struct ProtectedRun<'a> {
    /// Names the run in the final assertion (`"<name> verdict was not PASS"`).
    pub name: &'static str,
    /// Wall-clock cap on the whole drive ([`watchdog_budget`]).
    pub watchdog: Duration,
    /// See [`RunningWorkloads::drive`].
    pub drain: Duration,
    /// See [`RunningWorkloads::drive`].
    pub idle_threshold: Duration,
    /// Passed to the verdict.
    pub min_partitions: usize,
    /// The run's reports, if any (`--reports`).
    pub reports: &'a ReportsHandle,
}

/// Drive `workloads` and `scenario` with every lifecycle protection, then
/// judge the run, write its reports, tear the cluster down and fail the test
/// if the run failed. Shared by every chaos scenario, so each gets:
///
/// - the heartbeat watchdog, for a scenario task that blocks outright;
/// - the global wall-clock watchdog ([`ProtectedRun::watchdog`]);
/// - an orderly abort on the first Ctrl-C / SIGTERM ([`signals`]; the caller
///   installs the handler at the start of its run);
/// - a bounded stop of the workloads when the drive is abandoned;
/// - a forced exit armed BEFORE the wind-down, so a hanging or panicking
///   report or teardown step cannot leave the process running;
/// - reports written even when the drive panicked, and the drive's own panic
///   re-raised as the failure cause even if a wind-down step panicked too.
///
/// Every fault action fails by panicking (a broker that never rejoins, a
/// describe that cannot complete, ...), and those unwinds used to skip the
/// report-writing — losing verdict.txt and summary.txt for exactly the runs
/// that need them. Hence the panic is caught, reported, torn down, then
/// re-raised.
pub(super) async fn drive_protected<Fut>(
    harness: &ChaosHarness,
    workloads: RunningWorkloads,
    scenario: Fut,
    run: ProtectedRun<'_>,
) where
    Fut: std::future::Future<Output = ()>,
{
    let verifier = harness.verifier();
    let watchdog = run.watchdog;
    // Last-resort net for the scenario task itself blocking, which would stop
    // the watchdog below as well (see `isolation`).
    isolation::start_heartbeat_watchdog(harness.heartbeat(), HEARTBEAT_STALE, harness.cluster_teardown());
    let drive = workloads.drive(
        run.drain,
        run.idle_threshold,
        verifier.clone(),
        harness.recreate_settle(),
        scenario,
    );

    enum DriveOutcome {
        Finished,
        Wedged,
        Panicked(Box<dyn std::any::Any + Send>),
        Interrupted,
    }
    signals::enable_orderly_abort();
    let outcome = tokio::select! {
        biased;
        res = tokio::time::timeout(watchdog, std::panic::AssertUnwindSafe(drive).catch_unwind()) => match res {
            Ok(Ok(())) => DriveOutcome::Finished,
            Ok(Err(payload)) => DriveOutcome::Panicked(payload),
            Err(_) => DriveOutcome::Wedged,
        },
        () = signals::abort_requested() => DriveOutcome::Interrupted,
    };
    // From here a first signal lets the wind-down below finish (it is the
    // orderly abort) and a second tears down and exits at once. One that
    // arrived just as the drive ended is carried out here, as a normal
    // wind-down.
    signals::end_orderly_abort();
    // The drive no longer beats, whichever way it ended.
    let heartbeat = harness.heartbeat();
    heartbeat.disarm();

    // From here the run only winds down. Bound all of it, before any of it can
    // hang (a workload that never stops, a docker call, the runtime's own
    // shutdown) or panic past this point. Re-armed with the outcome once the
    // cluster is gone.
    let forced_exit = ForcedExit::arm(
        WIND_DOWN_BUDGET,
        101,
        format!(
            "chaos: FORCED EXIT — the wind-down (stopping workloads, verdict, reports, teardown) did not complete \
             within {WIND_DOWN_BUDGET:?}"
        ),
    );

    // An abandoned drive leaves its workloads running on their threads. Stop
    // them and give them a bounded time to close. A producer's close settles
    // its in-flight sends, so the verdict does not score sends that only the
    // abort left open. A workload that does not finish is named and left
    // behind; the forced exit ends it.
    if !matches!(outcome, DriveOutcome::Finished) {
        let stuck = harness.workload_threads().stop_and_wait(ABORT_STOP_WAIT).await;
        if !stuck.is_empty() {
            eprintln!(
                "chaos: {} workload(s) did not stop within {ABORT_STOP_WAIT:?} of the abort and are abandoned: {}",
                stuck.len(),
                stuck.join(", ")
            );
        }
    }

    let failure_header = match &outcome {
        DriveOutcome::Finished => None,
        DriveOutcome::Wedged => {
            let phase = heartbeat.phase();
            eprintln!(
                "chaos: WATCHDOG — run exceeded {watchdog:?} without finishing (stuck in the {phase} phase); the \
                 cluster is wedged (e.g. brokers down and unable to recover). Aborting; partial verdict below."
            );
            Some(format!(
                "=== Chaos verdict: FAIL (watchdog) ===\n  run exceeded {watchdog:?} without completing (stuck in the \
                 {phase} phase); cluster wedged"
            ))
        },
        DriveOutcome::Panicked(payload) => {
            let message = isolation::panic_message(payload.as_ref());
            eprintln!("chaos: ABORTED by a panic in the run: {message}\nPartial verdict below.");
            Some(format!("=== Chaos verdict: FAIL (panic) ===\n  {message}"))
        },
        DriveOutcome::Interrupted => {
            eprintln!("chaos: INTERRUPTED (Ctrl-C / SIGTERM); tearing the cluster down. Partial verdict below.");
            Some("=== Chaos verdict: FAIL (interrupted) ===\n  run interrupted by a signal".to_string())
        },
    };

    // Judge and persist the reports BEFORE the pass/fail assertion so a
    // failing, wedged, panicked or interrupted run still leaves its verdict,
    // leader-change log, client log, and signature summary on disk for
    // diagnosis. Caught: a panic here (a poisoned verifier, say) must neither
    // skip the teardown nor mask the drive's own failure.
    let judged = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let verdict = verifier.verdict(run.min_partitions);
        eprintln!("{verdict}");
        if let Some(r) = run.reports {
            let body = match &failure_header {
                Some(header) => format!("{header}\n{verdict}"),
                None => verdict.to_string(),
            };
            r.write_verdict(&body);
            eprintln!("chaos: reports written to {}", r.finish().display());
        }
        verdict
    }));
    let judged = judged.map_err(|payload| {
        let message = isolation::panic_message(payload.as_ref());
        eprintln!("chaos: the verdict/report step panicked: {message}");
        // Still leave a verdict.txt saying why there is no verdict.
        if let Some(r) = run.reports {
            let header = failure_header.as_deref().unwrap_or("=== Chaos verdict: FAIL (panic) ===");
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                r.write_verdict(&format!("{header}\n  no verdict: the verdict/report step panicked: {message}"));
                r.finish();
            }));
        }
        message
    });

    harness.teardown();

    // Reports are on disk and the cluster is gone. A client task that never
    // yields keeps the test runtime from shutting down, so the process could
    // still hang here (P3-1MiB, Sep 2026 matrix). Bound it.
    let passed = matches!(outcome, DriveOutcome::Finished)
        && judged.as_ref().is_ok_and(|verdict| verdict.is_pass() && verdict.delivered > 0);
    if passed {
        forced_exit.rearm(FORCED_EXIT_AFTER, 0, FORCED_EXIT_AFTER_PASS.to_string());
    } else {
        forced_exit.rearm(
            FORCED_EXIT_AFTER,
            101,
            "chaos: FORCED EXIT — the process did not terminate after teardown (the run had already failed)"
                .to_string(),
        );
    }

    // The drive's failure comes first: a wind-down panic is at most its
    // consequence (and was printed above).
    match outcome {
        DriveOutcome::Finished => {},
        DriveOutcome::Wedged => panic!(
            "{} WEDGED: exceeded watchdog {watchdog:?} without finishing (cluster could not make progress)",
            run.name
        ),
        DriveOutcome::Panicked(payload) => std::panic::resume_unwind(payload),
        DriveOutcome::Interrupted => panic!("{} interrupted by a signal (cluster torn down)", run.name),
    }
    let verdict = match judged {
        Ok(verdict) => verdict,
        Err(message) => panic!("{}: the verdict/report step panicked: {message}", run.name),
    };
    assert!(verdict.is_pass(), "{} verdict was not PASS:\n{verdict}", run.name);
    assert!(verdict.delivered > 0, "no records were acknowledged — workload never ran");
}
