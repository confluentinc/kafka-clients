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

//! Chaos actions (`design/current/chaos-fault-injection-harness.md` §5.1).
//!
//! Implemented here: `BrokerRoll` (clean/unclean) and `Migrate`
//! (`ChangeLeader` = preferred election, no data move; `ReassignPartitions` =
//! rotate replicas, data moves). Topic delete/recreate lives on
//! [`super::harness::ChaosHarness::recreate_topic`] because it needs the topic
//! config and the producer ledger (to mark expected-lost).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use confluent_kafka::admin::{
    Admin, AlterPartitionReassignmentsOptions, DescribeTopicsOptions, ElectLeadersOptions,
    ListPartitionReassignmentsOptions, NewPartitionReassignment,
};
use confluent_kafka::common::{ElectionType, TopicCollection, TopicPartition};

use super::common::broker_control::{BrokerControl, StopKind};
use super::reports::ReportsHandle;

/// Leader-migration mechanism for `--action change-leader|reassign-partitions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReassignMode {
    /// Preferred-leader election — no data movement.
    ChangeLeader,
    /// Move replicas — data movement.
    ReassignPartitions,
}

/// A single fault to inject.
#[derive(Debug, Clone)]
pub enum ChaosAction {
    /// Stop a broker (clean = SIGTERM / unclean = SIGKILL), leave it down for
    /// `down`, then start it and wait until it re-registers with the quorum
    /// (bounded by `wait_up`).
    BrokerRoll {
        node_id: u16,
        kind: StopKind,
        down: Duration,
        wait_up: Duration,
        /// Topics to sample leaders for (before-stop / while-down / after-start).
        /// librdkafka's roll samples leaders across ALL test topics; a
        /// single-topic run passes one.
        topics: Vec<String>,
    },
    /// Trigger a leader migration on every listed topic without bouncing
    /// brokers (librdkafka's change-leader / reassign act across all topics).
    Migrate { topics: Vec<String>, mode: ReassignMode },
}

impl ChaosAction {
    /// Execute the action against the cluster, using `admin` for readiness
    /// detection and reassignment/election RPCs. `reports`, when present,
    /// receives per-action leader/replica-change lines for `leader-changes.txt`.
    pub async fn execute(&self, brokers: &BrokerControl<'_>, admin: &dyn Admin, reports: &ReportsHandle) {
        // No mid-down hook: the down-window is a plain sleep.
        self.execute_with_hook(brokers, admin, reports, None::<std::future::Ready<()>>)
            .await;
    }

    /// Like [`execute`], but for a `BrokerRoll` the caller may supply a future
    /// that runs **inside the down-window** — after the broker is stopped and
    /// before it is restarted. This is the seam that makes a rebalance overlap a
    /// leader migration in time (librdkafka's `--rebalance-mid-roll`): the group
    /// reassignment is kicked off while the broker is down, so it is in flight
    /// exactly as leaders migrate away from the stopped node and back when it
    /// recovers.
    ///
    /// The hook fires once, immediately after the stop, and the roll still
    /// honours the full `down` duration around it (it waits out whatever time
    /// remains after the hook returns), so the down-window is never shorter than
    /// configured. For non-`BrokerRoll` actions the hook is ignored.
    ///
    /// [`execute`]: ChaosAction::execute
    pub async fn execute_with_hook<F>(
        &self,
        brokers: &BrokerControl<'_>,
        admin: &dyn Admin,
        reports: &ReportsHandle,
        during_down: Option<F>,
    ) where
        F: std::future::Future<Output = ()>,
    {
        match self {
            ChaosAction::BrokerRoll { node_id, kind, down, wait_up, topics } => {
                eprintln!("chaos: rolling broker {node_id} ({kind:?}), down for {down:?}");
                if let Some(r) = reports {
                    r.record_leader_change(&format!("broker-roll node={node_id} kind={kind:?} down={down:?}"));
                }

                // Sample leaders 3× around the roll (librdkafka's roll leader
                // sampling): before stop, while down, after start — logging which
                // partitions migrated away and which came back, PER topic.
                let before = leaders_of_topics(topics, admin).await;
                brokers.stop(*node_id, *kind);

                // Down-window. If a mid-down hook was supplied, run it here (so a
                // rebalance overlaps the leader migration), then wait out the
                // remainder of `down`; otherwise just sleep the whole window.
                let window_start = Instant::now();
                if let Some(hook) = during_down {
                    eprintln!("chaos:   broker {node_id} down — firing mid-down hook (rebalance overlaps roll)");
                    hook.await;
                    if let Some(remaining) = down.checked_sub(window_start.elapsed()) {
                        tokio::time::sleep(remaining).await;
                    }
                } else {
                    tokio::time::sleep(*down).await;
                }

                let down_state = leaders_of_topics(topics, admin).await;
                log_leader_migration_all(&format!("broker {node_id} down"), &before, &down_state, reports);

                brokers.start(*node_id);
                let up = brokers.wait_operational(admin, *node_id, *wait_up).await;
                assert!(up, "broker {node_id} did not become operational within {wait_up:?}");
                let after = leaders_of_topics(topics, admin).await;
                log_leader_migration_all(&format!("broker {node_id} back up"), &down_state, &after, reports);
                eprintln!("chaos: broker {node_id} back up");
            },
            ChaosAction::Migrate { topics, mode } => {
                for topic in topics {
                    match mode {
                        ReassignMode::ChangeLeader => {
                            change_leader(topic, admin, reports).await;
                        },
                        ReassignMode::ReassignPartitions => {
                            reassign_partitions(topic, admin, reports).await;
                        },
                    }
                }
            },
        }
    }
}

/// Change the leader of every partition of `topic` **without moving data**:
/// reorder each partition's replica list so a different replica becomes the
/// preferred leader (same replica *set*, so no resync), then elect preferred
/// leaders. This is librdkafka's change-leader mechanism — distinct from
/// reassign-partitions, which changes the replica set and moves data.
///
/// Asserts the leader actually moved on at least one partition (a bare
/// preferred election is a no-op when leaders are already preferred, which
/// would otherwise pass silently).
async fn change_leader(topic: &str, admin: &dyn Admin, reports: &ReportsHandle) {
    eprintln!("chaos: change-leader for topic {topic}");

    let before = partition_state(topic, admin).await;

    // Reorder replicas (rotate) — same set, different preferred leader. `plan`
    // records the intended new leader per partition (rotated[0]).
    let mut reassignments: HashMap<TopicPartition, Option<NewPartitionReassignment>> = HashMap::new();
    let mut plan: HashMap<i32, i32> = HashMap::new();
    for (&partition, state) in &before {
        if state.replicas.len() < 2 {
            continue;
        }
        let mut rotated = state.replicas.clone();
        rotated.rotate_left(1);
        plan.insert(partition, rotated[0]);
        reassignments.insert(
            TopicPartition::new(topic, partition),
            Some(NewPartitionReassignment::new(rotated).expect("non-empty replicas")),
        );
    }
    if reassignments.is_empty() {
        eprintln!("chaos: no partitions with >=2 replicas to change leader for");
        return;
    }

    admin
        .alter_partition_reassignments(&reassignments, AlterPartitionReassignmentsOptions::new())
        .all()
        .get()
        .await
        .expect("change-leader reorder failed");
    wait_reassignments_complete(topic, admin).await;

    // Elect the new preferred leaders.
    if let Err(err) = admin
        .elect_leaders(ElectionType::Preferred, None, ElectLeadersOptions::new())
        .all()
        .get()
        .await
    {
        eprintln!("chaos: preferred-leader election returned: {err} (often benign)");
    }

    verify_leader_plan("change-leader", topic, &before, &plan, admin, reports).await;
}

/// Move replicas around for every partition of `topic`: read the current
/// replica sets via `describe_topics`, rotate each (first replica → last, so a
/// different broker becomes preferred leader and one replica must resync),
/// submit via `alter_partition_reassignments`, then poll
/// `list_partition_reassignments` until the cluster reports no in-progress
/// reassignments (the `kafka-reassign-partitions.sh --verify` analog). Data
/// moves — this is the reassign-partitions mechanism, distinct from
/// change-leader.
async fn reassign_partitions(topic: &str, admin: &dyn Admin, reports: &ReportsHandle) {
    eprintln!("chaos: reassigning partitions for topic {topic}");

    // 1. Read current leader + replica assignments (the BEFORE snapshot).
    let before = partition_state(topic, admin).await;

    // 2. Rotate each partition's replica list by one. `plan` records the
    //    intended new leader per partition (rotated[0]).
    let mut reassignments: HashMap<TopicPartition, Option<NewPartitionReassignment>> = HashMap::new();
    let mut plan: HashMap<i32, i32> = HashMap::new();
    for (&partition, state) in &before {
        if state.replicas.len() < 2 {
            continue; // nothing to move with a single replica
        }
        let mut rotated = state.replicas.clone();
        rotated.rotate_left(1);
        plan.insert(partition, rotated[0]);
        let reassignment = NewPartitionReassignment::new(rotated).expect("non-empty replicas");
        reassignments.insert(TopicPartition::new(topic, partition), Some(reassignment));
    }

    if reassignments.is_empty() {
        eprintln!("chaos: no partitions with >=2 replicas to reassign");
        return;
    }

    // 3. Submit.
    admin
        .alter_partition_reassignments(&reassignments, AlterPartitionReassignmentsOptions::new())
        .all()
        .get()
        .await
        .expect("alter_partition_reassignments failed");

    // 4. Wait for the reassignments to complete (the --verify analog).
    wait_reassignments_complete(topic, admin).await;

    // 5. Elect the new preferred leaders. A reassignment changes the replica
    //    order (hence the *preferred* leader) but does not itself move the
    //    *current* leader unless the broker's auto-rebalance happens to fire.
    //    Trigger the election explicitly so the leader change is deterministic
    //    and observable — the same step `kafka-reassign-partitions.sh` users
    //    take.
    if let Err(err) = admin
        .elect_leaders(ElectionType::Preferred, None, ElectLeadersOptions::new())
        .all()
        .get()
        .await
    {
        eprintln!("chaos: preferred-leader election after reassign returned: {err} (often benign)");
    }

    // 6. Prove the replica sets actually changed (an empty pending list is also
    //    the state where nothing moved). Reassign moves data, so this is a
    //    distinct check from the leader-plan verification below.
    let after = partition_state(topic, admin).await;
    let mut replicas_changed = 0usize;
    for (partition, b) in &before {
        if b.replicas.len() < 2 {
            continue;
        }
        let a = after.get(partition).expect("partition present after reassignment");
        if a.replicas != b.replicas {
            replicas_changed += 1;
        }
    }
    assert!(
        replicas_changed > 0,
        "reassign-partitions did not change any partition's replica set for {topic} \
         (before == after) — the reassignment had no effect"
    );

    // 7. Verify each partition's leader matches the plan (rotated[0]), per
    //    partition, not just an aggregate count (A3).
    verify_leader_plan("reassign", topic, &before, &plan, admin, reports).await;
}

/// Verify, **per partition**, that the post-action leader matches the plan
/// (`plan[partition]` = the intended new leader = rotated `replicas[0]`) — the
/// librdkafka `_verify_plan_leaders` analog, stronger than an aggregate
/// "some leader changed" count.
///
/// The preferred-leader election can transiently fail to place the exact
/// preferred leader when that broker is momentarily down (e.g. mid-roll in a
/// composed run — "preferred leader was not available"). So this polls up to
/// 10s for the planned leaders to settle, then asserts a **strong majority**
/// (≥⅔) of eligible partitions reached their planned leader, logging every
/// per-partition before→after vs plan and any mismatch. It also asserts at
/// least one leader actually moved (the old no-op guard).
async fn verify_leader_plan(
    label: &str,
    topic: &str,
    before: &std::collections::BTreeMap<i32, PartitionState>,
    plan: &HashMap<i32, i32>,
    admin: &dyn Admin,
    reports: &ReportsHandle,
) {
    let eligible: Vec<i32> = plan.keys().copied().collect();
    if eligible.is_empty() {
        return;
    }

    // Poll for the planned leaders to settle.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut after;
    loop {
        after = partition_state(topic, admin).await;
        let matched = eligible
            .iter()
            .filter(|p| after.get(p).and_then(|s| s.leader) == plan.get(p).copied())
            .count();
        if matched == eligible.len() || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let mut matched_plan = 0usize;
    let mut leaders_changed = 0usize;
    for &partition in &eligible {
        let b = &before[&partition];
        let a = after.get(&partition).expect("partition present after action");
        let planned = plan[&partition];
        let on_plan = a.leader == Some(planned);
        if on_plan {
            matched_plan += 1;
        }
        if a.leader != b.leader {
            leaders_changed += 1;
        }
        let line = format!(
            "{label} {topic} p{partition} leader {:?}->{:?} (planned {planned}{})",
            b.leader,
            a.leader,
            if on_plan { "" } else { " MISMATCH" }
        );
        eprintln!("chaos:   {line}");
        if let Some(r) = reports {
            r.record_leader_change(&line);
        }
    }

    assert!(
        leaders_changed > 0,
        "{label}: no partition's leader moved for {topic} — the action had no effect"
    );
    // Tolerate a few transient election failures, but the plan must mostly hold.
    let need = eligible.len().div_ceil(3) * 2; // ceil(2/3 * n)
    assert!(
        matched_plan >= need,
        "{label}: only {matched_plan}/{} partitions reached their planned leader for {topic} \
         (need >= {need}) — the leader plan did not take effect",
        eligible.len()
    );
    eprintln!(
        "chaos: {label} complete for topic {topic} \
         ({matched_plan}/{} partitions on planned leader, {leaders_changed} leader(s) changed)",
        eligible.len()
    );
}

/// Current leader per `(topic, partition)` across `topics` (the multi-topic
/// broker-roll leader sampling). A partition with no leader (e.g. its broker is
/// down) maps to `None`.
async fn leaders_of_topics(
    topics: &[String],
    admin: &dyn Admin,
) -> std::collections::BTreeMap<(String, i32), Option<i32>> {
    let mut out = std::collections::BTreeMap::new();
    for topic in topics {
        for (p, s) in partition_state(topic, admin).await {
            out.insert((topic.clone(), p), s.leader);
        }
    }
    out
}

/// Log the leader migration between two `(topic, partition)`-keyed snapshots to
/// `leader-changes.txt` (and stderr): which partitions changed leader across
/// `phase`, across every sampled topic. The librdkafka roll leader-diff analog.
fn log_leader_migration_all(
    phase: &str,
    from: &std::collections::BTreeMap<(String, i32), Option<i32>>,
    to: &std::collections::BTreeMap<(String, i32), Option<i32>>,
    reports: &ReportsHandle,
) {
    let mut moved = 0usize;
    for ((topic, partition), from_leader) in from {
        let to_leader = to.get(&(topic.clone(), *partition)).copied().flatten();
        if *from_leader != to_leader {
            moved += 1;
            let line = format!("roll {phase}: {topic} p{partition} leader {from_leader:?}->{to_leader:?}");
            eprintln!("chaos:   {line}");
            if let Some(r) = reports {
                r.record_leader_change(&line);
            }
        }
    }
    if moved == 0 {
        eprintln!("chaos:   roll {phase}: no leader changes");
    }
}

/// Poll `list_partition_reassignments` until the cluster reports none in
/// progress (the `kafka-reassign-partitions.sh --verify` analog), bounded by a
/// 60s deadline.
async fn wait_reassignments_complete(topic: &str, admin: &dyn Admin) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let pending = admin
            .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
            .reassignments()
            .get()
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        if pending == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "reassignment for {topic} still has {pending} in-progress after 60s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The observed state of one partition from `describe_topics`.
struct PartitionState {
    /// Current leader broker id, or `None` if the partition has no leader.
    leader: Option<i32>,
    /// Replica set (broker ids, in order); the first is the preferred leader.
    replicas: Vec<i32>,
}

/// Current `(leader, replicas)` for each partition of `topic`.
async fn partition_state(topic: &str, admin: &dyn Admin) -> std::collections::BTreeMap<i32, PartitionState> {
    let described = admin
        .describe_topics(
            TopicCollection::of_topic_names(vec![topic.to_string()]),
            DescribeTopicsOptions::new(),
        )
        .all_topic_names()
        .expect("describe_topics by name yields a name-keyed result");
    let descriptions = described.get().await.expect("describe_topics failed");
    let description = descriptions.get(topic).expect("described topic present");
    description
        .partitions()
        .iter()
        .map(|info| {
            (
                info.partition(),
                PartitionState {
                    leader: info.leader().map(|n| n.id()),
                    replicas: info.replicas().iter().map(|n| n.id()).collect(),
                },
            )
        })
        .collect()
}
