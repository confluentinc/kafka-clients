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
    },
    /// Trigger a leader migration for `topic` without bouncing brokers.
    Migrate { topic: String, mode: ReassignMode },
}

impl ChaosAction {
    /// Execute the action against the cluster, using `admin` for readiness
    /// detection and reassignment/election RPCs.
    pub async fn execute(&self, brokers: &BrokerControl<'_>, admin: &dyn Admin) {
        match self {
            ChaosAction::BrokerRoll { node_id, kind, down, wait_up } => {
                eprintln!("chaos: rolling broker {node_id} ({kind:?}), down for {down:?}");
                brokers.stop(*node_id, *kind);
                tokio::time::sleep(*down).await;
                brokers.start(*node_id);
                let up = brokers.wait_operational(admin, *node_id, *wait_up).await;
                assert!(up, "broker {node_id} did not become operational within {wait_up:?}");
                eprintln!("chaos: broker {node_id} back up");
            },
            ChaosAction::Migrate { topic, mode } => match mode {
                ReassignMode::ChangeLeader => {
                    eprintln!("chaos: preferred-leader election for topic {topic}");
                    // `None` partitions = elect preferred leaders for all
                    // partitions the controller knows about. Faithful to
                    // librdkafka's change-leader (no data movement).
                    let result = admin.elect_leaders(ElectionType::Preferred, None, ElectLeadersOptions::new());
                    // Some brokers return "election not needed" when leaders are
                    // already preferred; that is not a failure for chaos.
                    if let Err(err) = result.all().get().await {
                        eprintln!("chaos: preferred-leader election returned: {err} (often benign)");
                    }
                },
                ReassignMode::ReassignPartitions => {
                    reassign_partitions(topic, admin).await;
                },
            },
        }
    }
}

/// Move replicas around for every partition of `topic`: read the current
/// replica sets via `describe_topics`, rotate each (first replica → last, so a
/// different broker becomes preferred leader and one replica must resync),
/// submit via `alter_partition_reassignments`, then poll
/// `list_partition_reassignments` until the cluster reports no in-progress
/// reassignments (the `kafka-reassign-partitions.sh --verify` analog). Data
/// moves — this is the reassign-partitions mechanism, distinct from
/// change-leader.
async fn reassign_partitions(topic: &str, admin: &dyn Admin) {
    eprintln!("chaos: reassigning partitions for topic {topic}");

    // 1. Read current leader + replica assignments (the BEFORE snapshot).
    let before = partition_state(topic, admin).await;

    // 2. Rotate each partition's replica list by one.
    let mut reassignments: HashMap<TopicPartition, Option<NewPartitionReassignment>> = HashMap::new();
    for (&partition, state) in &before {
        if state.replicas.len() < 2 {
            continue; // nothing to move with a single replica
        }
        let mut rotated = state.replicas.clone();
        rotated.rotate_left(1);
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
            break;
        }
        assert!(
            Instant::now() < deadline,
            "reassignment for {topic} still has {pending} in-progress after 60s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

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

    // 6. Prove BOTH the replica sets AND the leaders actually changed (an empty
    //    pending list is also the state where nothing moved, so assert the
    //    AFTER snapshot differs from BEFORE).
    let after = partition_state(topic, admin).await;
    let mut replicas_changed = 0usize;
    let mut leaders_changed = 0usize;
    for (partition, b) in &before {
        if b.replicas.len() < 2 {
            continue;
        }
        let a = after.get(partition).expect("partition present after reassignment");
        eprintln!(
            "chaos:   p{partition} leader {:?}->{:?}  replicas {:?}->{:?}",
            b.leader, a.leader, b.replicas, a.replicas
        );
        if a.replicas != b.replicas {
            replicas_changed += 1;
        }
        if a.leader != b.leader {
            leaders_changed += 1;
        }
    }
    assert!(
        replicas_changed > 0,
        "reassign-partitions did not change any partition's replica set for {topic} \
         (before == after) — the reassignment had no effect"
    );
    assert!(
        leaders_changed > 0,
        "reassign-partitions changed replicas but no partition's leader moved for {topic} \
         — the preferred-leader election did not take effect"
    );
    eprintln!(
        "chaos: reassignment complete for topic {topic} \
         ({replicas_changed} partition(s) with changed replicas, {leaders_changed} with changed leader)"
    );
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
