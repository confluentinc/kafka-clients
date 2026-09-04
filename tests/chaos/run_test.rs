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
) {
    match action {
        ActionKind::BrokerRoll => {
            for node in roll_order(cfg, cycle) {
                ChaosAction::BrokerRoll {
                    node_id: node,
                    kind: stop_kind,
                    down: cfg.stop_dur(),
                    wait_up: cfg.up_wait_dur(),
                }
                .execute(brokers, admin, reports)
                .await;
            }
        },
        ActionKind::ChangeLeader => {
            ChaosAction::Migrate { topic: cfg.topic.clone(), mode: ReassignMode::ChangeLeader }
                .execute(brokers, admin, reports)
                .await;
        },
        ActionKind::ReassignPartitions => {
            ChaosAction::Migrate { topic: cfg.topic.clone(), mode: ReassignMode::ReassignPartitions }
                .execute(brokers, admin, reports)
                .await;
        },
        ActionKind::TopicRecreate => {
            harness.recreate_topic(Duration::from_secs(cfg.dwell_s)).await;
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "chaos: flag-driven runner; invoked via `cargo xtask chaos`"]
async fn chaos_run() {
    let config = match ChaosConfig::from_env() {
        Ok(c) => c,
        Err(err) => panic!("invalid chaos configuration: {err}"),
    };
    eprintln!("chaos: {}", config.summary());

    let harness = ChaosHarness::start(&config.topic, config.brokers, config.partitions).await;

    // If a broker is to be left permanently down, stop it before workloads
    // ramp up (librdkafka's --leave-broker-down).
    if let Some(node) = config.leave_broker_down {
        eprintln!("chaos: leaving broker {node} down for the whole run");
        harness.brokers().stop(node, StopKind::Clean);
    }

    let workloads = harness.build_workloads(&config.workloads, config.rps, config.commit_mode).await;
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
    let scenario = async move {
        let pool = scenario_pool;
        tokio::time::sleep(Duration::from_secs(cfg.warmup_s)).await;

        for cycle in 0..cfg.cycles {
            let cycle_1based = cycle + 1;
            eprintln!("chaos: cycle {}/{}", cycle_1based, cfg.cycles);

            // Rebalance chaos: add/remove a consumer at the configured cycle.
            if cfg.rebalance_add_cycle == Some(cycle_1based) {
                pool.add_consumer(super::workload::Backend::Rust).await;
            }
            if cfg.rebalance_remove_cycle == Some(cycle_1based) {
                pool.remove_consumer();
            }

            // Primary action for this cycle.
            run_action(cfg.action, cfg, cycle, stop_kind, &brokers, admin, harness_ref, reports_ref).await;

            // Overlays: additional fault types layered on top of the primary
            // action this cycle, on their own cadences (A1 — compose faults).
            // `every N` fires on cycles N, 2N, 3N, … (1-based).
            let fires = |every: Option<u32>| every.is_some_and(|n| n > 0 && cycle_1based % n == 0);
            if fires(cfg.change_leader_every) {
                eprintln!("chaos:   overlay change-leader (cycle {cycle_1based})");
                run_action(
                    ActionKind::ChangeLeader,
                    cfg,
                    cycle,
                    stop_kind,
                    &brokers,
                    admin,
                    harness_ref,
                    reports_ref,
                )
                .await;
            }
            if fires(cfg.reassign_every) {
                eprintln!("chaos:   overlay reassign-partitions (cycle {cycle_1based})");
                run_action(
                    ActionKind::ReassignPartitions,
                    cfg,
                    cycle,
                    stop_kind,
                    &brokers,
                    admin,
                    harness_ref,
                    reports_ref,
                )
                .await;
            }
            if fires(cfg.topic_recreate_every) {
                eprintln!("chaos:   overlay topic-recreate (cycle {cycle_1based})");
                run_action(
                    ActionKind::TopicRecreate,
                    cfg,
                    cycle,
                    stop_kind,
                    &brokers,
                    admin,
                    harness_ref,
                    reports_ref,
                )
                .await;
            }

            if cycle + 1 < cfg.cycles {
                tokio::time::sleep(Duration::from_secs(cfg.between_s)).await;
            }
        }

        // Let traffic settle after the last fault before draining.
        tokio::time::sleep(Duration::from_secs(cfg.between_s)).await;
    };

    workloads.drive(&pool, config.drain_dur(), scenario).await;

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
