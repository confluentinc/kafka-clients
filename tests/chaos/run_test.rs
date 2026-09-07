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

use super::actions::{ChaosAction, ReassignMode};
use super::common::broker_control::{BrokerControl, StopKind};
use super::config::{ActionKind, ChaosConfig};
use super::harness::ChaosHarness;
use super::reports::{self, ReportsHandle, RunReports};
use confluent_kafka::admin::Admin;
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

/// Draw one cycle's action in `--random` mode from the seeded `rng`. Returns
/// `None` for a quiet cycle (probability `1 - action_prob`). All four fault
/// types are candidates; every parameter is drawn from `rng` so the run is
/// reproducible for a given seed.
fn random_plan(rng: &mut StdRng, cfg: &ChaosConfig) -> Option<PlannedAction> {
    if rng.random::<f64>() >= cfg.action_prob {
        return None; // quiet cycle
    }
    // Candidate faults, all equally likely. Broker-roll included. TopicRecreate
    // (candidate 3) is EXCLUDED under multi-topic: multi-topic + recreate is a
    // known limitation (a new-generation recreate leaves the consumer positioned
    // past the new tail without an offset reset — see `ChaosConfig::from_env`),
    // so a single-topic run has all 4 candidates while a multi-topic run draws
    // only broker-roll / change-leader / reassign.
    let candidates = if cfg.num_topics > 1 { 3 } else { 4 };
    let plan = match rng.random_range(0..candidates) {
        0 => {
            // Pick a broker that is not permanently left down.
            let eligible: Vec<u16> = (1..=cfg.brokers).filter(|b| cfg.leave_broker_down != Some(*b)).collect();
            let node_id = eligible[rng.random_range(0..eligible.len())];
            // Clean vs unclean chosen at random (ignores the --unclean flag in
            // random mode — the monkey does both).
            let kind = if rng.random::<bool>() {
                StopKind::Unclean
            } else {
                StopKind::Clean
            };
            // Down duration: 3..=12s around the configured --stop-s midpoint.
            let down = Duration::from_secs(rng.random_range(3..=12));
            PlannedAction::BrokerRoll { node_id, kind, down }
        },
        1 => PlannedAction::ChangeLeader,
        2 => PlannedAction::ReassignPartitions,
        _ => {
            // Dwell 0 (immediate) or 3..=8s (delayed), 50/50.
            let dwell = if rng.random::<bool>() {
                Duration::ZERO
            } else {
                Duration::from_secs(rng.random_range(3..=8))
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
            ChaosAction::Migrate { topics: harness.topics().to_vec(), mode: ReassignMode::ChangeLeader }
                .execute(brokers, admin, reports)
                .await;
        },
        PlannedAction::ReassignPartitions => {
            ChaosAction::Migrate { topics: harness.topics().to_vec(), mode: ReassignMode::ReassignPartitions }
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
            ChaosAction::Migrate { topics: harness.topics().to_vec(), mode: ReassignMode::ChangeLeader }
                .execute(brokers, admin, reports)
                .await;
        },
        ActionKind::ReassignPartitions => {
            ChaosAction::Migrate { topics: harness.topics().to_vec(), mode: ReassignMode::ReassignPartitions }
                .execute(brokers, admin, reports)
                .await;
        },
        ActionKind::TopicRecreate => {
            // librdkafka's topic-chaos picks ONE random topic per firing
            // (`rng.choice(topics)`). Seeded so a given --seed reproduces it.
            let topic = pick_topic(rng, harness);
            harness.recreate_topic(&topic, Duration::from_secs(cfg.dwell_s)).await;
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
        if config.num_topics > 1 {
            eprintln!(
                "chaos: NOTE topic-recreate is excluded from RANDOM candidates under --num-topics > 1 \
                 (multi-topic + recreate is a known limitation)"
            );
        }
    }
    eprintln!("chaos: {}", config.summary());

    let harness = ChaosHarness::start_with_topics(
        &config.topics(),
        config.brokers,
        config.partitions,
        config.replication_factor,
        Arc::new(super::verifier::ConservationVerifier::new()),
    )
    .await;

    // If a broker is to be left permanently down, stop it before workloads
    // ramp up (librdkafka's --leave-broker-down).
    if let Some(node) = config.leave_broker_down {
        eprintln!("chaos: leaving broker {node} down for the whole run");
        harness.brokers().stop(node, StopKind::Clean);
    }

    let workloads = harness
        .build_workloads(&config.workloads, config.rps, config.msg_size, config.commit_mode)
        .await;
    let verifier = harness.verifier();

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

            // --- Random (chaos-monkey) mode: draw one action + its parameters
            // from the seeded RNG, optionally after a random delay, then move to
            // the next cycle. Rebalance add/remove still honour their cycles.
            if cfg.random {
                if cfg.rebalance_add_cycle == Some(cycle_1based) {
                    pool.add_consumer(super::workload::Backend::Rust).await;
                }
                if cfg.rebalance_remove_cycle == Some(cycle_1based) {
                    pool.remove_consumer();
                }
                // Random pre-action delay within the cycle (0..between_s), so
                // actions land at varying wall-clock offsets, not cycle-aligned.
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
            let inject_mid_roll = cfg.rebalance_mid_roll && roll_fires_this_cycle && (do_add || do_remove);

            // Rebalance chaos at the top of the cycle — unless we are deferring
            // it into the roll's down-window (mid-roll).
            if !inject_mid_roll {
                if do_add {
                    pool.add_consumer(super::workload::Backend::Rust).await;
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
                // the deferred rebalance in its down-window. (`inject_mid_roll`
                // is cleared after firing so a multi-roll cycle injects once.)
                if inject_mid_roll && spec.kind == ActionKind::BrokerRoll {
                    let pool_hook = &pool;
                    let hook = async move {
                        if do_add {
                            pool_hook.add_consumer(super::workload::Backend::Rust).await;
                        }
                        if do_remove {
                            pool_hook.remove_consumer();
                        }
                    };
                    run_broker_roll_with_hook(cfg, cycle, stop_kind, &brokers, admin, harness_ref, reports_ref, hook)
                        .await;
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

    workloads
        .drive(
            &pool,
            config.drain_dur(),
            config.idle_threshold_dur(),
            verifier.clone(),
            harness.recreate_settle(),
            scenario,
        )
        .await;

    let verdict = verifier.verdict(config.min_partitions());
    eprintln!("{verdict}");

    // Persist reports BEFORE the pass/fail assertion so a failing run still
    // leaves its verdict, leader-change log, client log, and signature summary
    // on disk for diagnosis.
    if let Some(r) = reports {
        r.write_verdict(&verdict.to_string());
        let dir = Arc::try_unwrap(r).ok().expect("sole owner of reports at finish").finish();
        eprintln!("chaos: reports written to {}", dir.display());
    }

    harness.shutdown();

    assert!(verdict.is_pass(), "chaos run verdict was not PASS:\n{verdict}");
    assert!(verdict.delivered > 0, "no records were acknowledged — workload never ran");
}
