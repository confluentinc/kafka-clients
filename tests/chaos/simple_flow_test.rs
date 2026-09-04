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
use super::harness::ChaosHarness;
use super::workload::{CommitMode, WorkloadSpec};

/// Produce and consume continuously while broker 2 is cleanly rolled once;
/// no acknowledged record may be lost.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "chaos: slow, Docker-heavy, destructive; run with --ignored"]
async fn simple_flow_clean_broker_roll() {
    let harness = ChaosHarness::start("chaos-simple-flow", 3, 6).await;

    // Plug in a Rust producer and a Rust consumer.
    let specs = [
        WorkloadSpec::parse("producer:rust", 1).unwrap(),
        WorkloadSpec::parse("consumer:rust", 1).unwrap(),
    ];
    let workloads = harness.build_workloads(&specs, 200, CommitMode::Sync).await;
    let verifier = harness.verifier();

    // The chaos timeline: warm up, roll broker 2 cleanly once, let traffic
    // flow after recovery. Runs concurrently with the workloads.
    let brokers = harness.brokers();
    let admin = harness.admin();
    // This smoke test does not add/remove consumers, but drive still needs a pool.
    let pool = harness.workload_pool();
    let scenario = async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        ChaosAction::BrokerRoll {
            node_id: 2,
            kind: StopKind::Clean,
            down: Duration::from_secs(5),
            wait_up: Duration::from_secs(60),
        }
        .execute(&brokers, admin, &None)
        .await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    };

    workloads
        .drive(
            &pool,
            Duration::from_secs(15),
            Duration::from_secs(3),
            verifier.clone(),
            scenario,
        )
        .await;

    let verdict = verifier.verdict(3);
    eprintln!("{verdict}");

    harness.shutdown();

    assert!(verdict.is_pass(), "chaos simple-flow verdict was not PASS:\n{verdict}");
    assert!(verdict.delivered > 0, "no records were acknowledged — workload never ran");
}
