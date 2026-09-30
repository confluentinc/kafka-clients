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

use super::actions::{ChaosAction, LEADER_PLAN_SETTLE, ReassignMode};
use super::common::broker_control::{BrokerControl, StopKind};
use super::config::{ActionKind, ChaosConfig};
use super::harness::{ChaosHarness, WorkloadPool};
use super::isolation;
use super::reports::{self, ReportsHandle, RunReports};
use super::workload::Backend;
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
#[allow(clippy::too_many_arguments)]
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

/// Run one chaos action for the current cycle. Used for both the primary
/// `--action` and the per-cycle overlays (A1 — compose fault types).
#[allow(clippy::too_many_arguments)]
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
#[allow(clippy::too_many_arguments)]
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
            // order (A1 — compose fault types). Each `--action KIND[:everyN]`
            // fires on cycles every, 2*every, … (`every`=1 → every cycle).
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
    // progress — fails fast (with container cleanup via `shutdown`) instead of
    // hanging indefinitely. It scales with the run: warmup + per-cycle worst
    // case (a roll may wait up to `up_wait_s` for a rejoin) + drain, plus
    // margin. Every per-call path already has its own timeout (broker
    // wait_operational, describe sampling); this is the last-resort net for any
    // wedge those don't individually cover.
    //
    // Per cycle, budget every action that can fire: a broker-roll cycle rolls
    // EVERY broker in turn (each waiting `stop_s`, up to `up_wait_s` for the
    // rejoin, plus bounded describe sampling), a recreate waits for the delete
    // and the create (30 s each) plus its dwell, and reassign / change-leader
    // wait for reassignments and the leader plan to settle. Budgeting a single
    // roll per cycle, as this once did, aborted healthy multi-broker runs as
    // "wedged".
    //
    // `--random` ignores `config.actions`: any of the four faults can fire on
    // any cycle, with one broker rolled for up to `RANDOM_MAX_DOWN_S` and a
    // dwell of up to `RANDOM_MAX_DWELL_S`, plus the pre-action jitter of up to
    // `between_s`. Budget for all of them, or a random run that happens to draw
    // recreates and reassignments is aborted as wedged.
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
    let leader_plan_extra = u64::from(config.num_topics.max(1)) * LEADER_PLAN_SETTLE.as_secs().saturating_sub(10);
    let per_cycle = rolled_per_cycle * (config.up_wait_s + down_s + 30)
        + jitter_s
        + if has(|k| matches!(k, ActionKind::TopicRecreate)) {
            60 + dwell_s
        } else {
            0
        }
        // The leader-plan check waits up to `LEADER_PLAN_SETTLE` per topic
        // (the base figures below were sized for a 10 s settle).
        + if has(|k| matches!(k, ActionKind::ReassignPartitions)) {
            90 + leader_plan_extra
        } else {
            0
        }
        + if has(|k| matches!(k, ActionKind::ChangeLeader)) {
            30 + leader_plan_extra
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
        + config.between_s
        + 30;
    let watchdog = Duration::from_secs(config.warmup_s + u64::from(config.cycles) * per_cycle + config.drain_s + 120);
    // Last-resort net for the scenario task itself blocking, which would stop
    // the watchdog below as well (see `isolation`).
    isolation::start_heartbeat_watchdog(harness.heartbeat(), HEARTBEAT_STALE, harness.cluster_teardown());
    let drive = workloads.drive(
        config.drain_dur(),
        config.idle_threshold_dur(),
        verifier.clone(),
        harness.recreate_settle(),
        scenario,
    );

    // How the drive ended. Every fault action fails by panicking (a broker that
    // never rejoins, a describe that cannot complete, ...), and those unwinds
    // used to skip the report-writing below — losing verdict.txt and
    // summary.txt for exactly the runs that need them. Catch the panic, report,
    // tear down, then re-raise it. Ctrl-C is handled the same way: without a
    // handler the process dies mid-run and leaks the whole cluster.
    enum DriveOutcome {
        Finished,
        Wedged,
        Panicked(Box<dyn std::any::Any + Send>),
        Interrupted,
    }
    let outcome = tokio::select! {
        biased;
        res = tokio::time::timeout(watchdog, std::panic::AssertUnwindSafe(drive).catch_unwind()) => match res {
            Ok(Ok(())) => DriveOutcome::Finished,
            Ok(Err(payload)) => DriveOutcome::Panicked(payload),
            Err(_) => DriveOutcome::Wedged,
        },
        _ = tokio::signal::ctrl_c() => DriveOutcome::Interrupted,
    };
    // The drive no longer beats, whichever way it ended.
    harness.heartbeat().disarm();

    // An abandoned drive leaves its workloads running on their threads. Stop
    // them and give them a bounded time to close. A producer's close settles
    // its in-flight sends, so the verdict does not score sends that only the
    // abort left open. A workload that does not finish is named and left
    // behind; the forced exit below ends it.
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

    let verdict = verifier.verdict(config.min_partitions());
    let failure_header = match &outcome {
        DriveOutcome::Finished => None,
        DriveOutcome::Wedged => {
            eprintln!(
                "chaos: WATCHDOG — run exceeded {watchdog:?} without finishing; the cluster is wedged \
                 (e.g. brokers down and unable to recover). Aborting; partial verdict below."
            );
            Some(format!(
                "=== Chaos verdict: FAIL (watchdog) ===\n  run exceeded {watchdog:?} without completing; cluster wedged"
            ))
        },
        DriveOutcome::Panicked(payload) => {
            let message = isolation::panic_message(payload.as_ref());
            eprintln!("chaos: ABORTED by a panic in the run: {message}\nPartial verdict below.");
            Some(format!("=== Chaos verdict: FAIL (panic) ===\n  {message}"))
        },
        DriveOutcome::Interrupted => {
            eprintln!("chaos: INTERRUPTED (Ctrl-C); tearing the cluster down. Partial verdict below.");
            Some("=== Chaos verdict: FAIL (interrupted) ===\n  run interrupted by Ctrl-C".to_string())
        },
    };
    eprintln!("{verdict}");

    // Persist reports BEFORE the pass/fail assertion so a failing, wedged,
    // panicked or interrupted run still leaves its verdict, leader-change log,
    // client log, and signature summary on disk for diagnosis.
    if let Some(r) = reports {
        let body = match &failure_header {
            Some(header) => format!("{header}\n{verdict}"),
            None => verdict.to_string(),
        };
        r.write_verdict(&body);
        let dir = Arc::try_unwrap(r).ok().expect("sole owner of reports at finish").finish();
        eprintln!("chaos: reports written to {}", dir.display());
    }

    harness.shutdown();

    // Reports are on disk and the cluster is gone. A client task that never
    // yields keeps the test runtime from shutting down, so the process could
    // still hang here (P3-1MiB, Sep 2026 matrix). Bound it.
    let passed = matches!(outcome, DriveOutcome::Finished) && verdict.is_pass() && verdict.delivered > 0;
    if passed {
        isolation::arm_forced_exit(FORCED_EXIT_AFTER, 0, FORCED_EXIT_AFTER_PASS.to_string());
    } else {
        isolation::arm_forced_exit(
            FORCED_EXIT_AFTER,
            101,
            "chaos: FORCED EXIT — the process did not terminate after teardown (the run had already failed)"
                .to_string(),
        );
    }

    match outcome {
        DriveOutcome::Finished => {},
        DriveOutcome::Wedged => panic!(
            "chaos run WEDGED: exceeded watchdog {watchdog:?} without finishing (cluster could not make progress)"
        ),
        DriveOutcome::Panicked(payload) => std::panic::resume_unwind(payload),
        DriveOutcome::Interrupted => panic!("chaos run interrupted by Ctrl-C (cluster torn down)"),
    }
    assert!(verdict.is_pass(), "chaos run verdict was not PASS:\n{verdict}");
    assert!(verdict.delivered > 0, "no records were acknowledged — workload never ran");
}
