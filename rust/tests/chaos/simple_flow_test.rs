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

//! Phase 1 — the simple end-to-end chaos flow
//! (`design/current/chaos-fault-injection-harness.md` §6, Phase 1).
//!
//! 3-broker RF=3 cluster, a Rust producer + Rust consumer plug-in, a single
//! **clean** broker roll mid-run, then assert conservation (no acknowledged
//! record lost). This is a thin smoke test over the same engine the
//! `cargo xtask chaos` CLI runner drives.

use std::time::Duration;

use super::actions::ChaosAction;
use super::common::broker_control::StopKind;
use super::harness::{CONSUMER_STOP_DEADLINE, ChaosHarness, PRODUCER_STOP_DEADLINE};
use super::isolation::signals;
use super::run_test::{ProtectedRun, drive_protected};
use super::workload::{CommitMode, WorkloadSpec};

const WARMUP: Duration = Duration::from_secs(5);
const DOWN: Duration = Duration::from_secs(5);
const WAIT_UP: Duration = Duration::from_secs(60);
const SETTLE: Duration = Duration::from_secs(5);
const DRAIN: Duration = Duration::from_secs(15);

/// Produce and consume continuously while broker 2 is cleanly rolled once;
/// no acknowledged record may be lost.
///
/// Driven through the same protected path as `chaos_run`
/// ([`drive_protected`]): watchdogs, signal handling, bounded close phases,
/// a bounded workload stop when the scenario panics, and a forced exit.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "chaos: slow, Docker-heavy, destructive; run with --ignored"]
async fn simple_flow_clean_broker_roll() {
    signals::install();
    let harness = ChaosHarness::start("chaos-simple-flow", 3, 6).await;

    // Plug in a Rust producer and a Rust consumer.
    let specs = [
        WorkloadSpec::parse("producer:rust", 1).unwrap(),
        WorkloadSpec::parse("consumer:rust", 1).unwrap(),
    ];
    let workloads = harness.build_workloads(&specs, 1000, 100, CommitMode::Sync).await;

    // The chaos timeline: warm up, roll broker 2 cleanly once, let traffic
    // flow after recovery. Runs concurrently with the workloads.
    let brokers = harness.brokers();
    let admin = harness.admin();
    let scenario = async {
        tokio::time::sleep(WARMUP).await;
        ChaosAction::BrokerRoll {
            node_id: 2,
            kind: StopKind::Clean,
            down: DOWN,
            wait_up: WAIT_UP,
            topics: vec!["chaos-simple-flow".to_string()],
        }
        .execute(&brokers, admin, &None)
        .await;
        tokio::time::sleep(SETTLE).await;
    };

    // The roll: its down window, the rejoin wait, three leader samples of up
    // to 10 s and 30 s for the stop / start (as `watchdog_budget` counts a
    // roll); then both close phases, the drain, and a margin.
    let roll = DOWN + WAIT_UP + Duration::from_secs(3 * 10 + 30);
    let watchdog =
        WARMUP + roll + SETTLE + PRODUCER_STOP_DEADLINE + DRAIN + CONSUMER_STOP_DEADLINE + Duration::from_secs(120);
    drive_protected(
        &harness,
        workloads,
        scenario,
        ProtectedRun {
            name: "chaos simple-flow",
            watchdog,
            drain: DRAIN,
            idle_threshold: Duration::from_secs(3),
            min_partitions: 3,
            reports: &None,
        },
    )
    .await;
}
