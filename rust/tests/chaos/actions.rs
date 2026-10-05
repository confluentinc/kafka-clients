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
//! swap a replica onto a broker outside the set, data moves). Topic delete/recreate lives on
//! [`super::harness::ChaosHarness::recreate_topic`] because it needs the topic
//! config and the producer ledger (to mark expected-lost).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use confluent_kafka::admin::{Admin, NewPartitionReassignment};
use confluent_kafka::common::{ElectionType, TopicCollection, TopicPartition};

use super::common::broker_control::{BrokerControl, StopKind};
use super::reports::ReportsHandle;

/// How long [`verify_leader_plan`] waits for the planned leaders.
pub const LEADER_PLAN_SETTLE: Duration = Duration::from_secs(30);

/// How often [`verify_leader_plan`] re-issues the preferred election while it
/// waits.
const LEADER_PLAN_REELECT_EVERY: Duration = Duration::from_secs(5);

/// Leader-migration mechanism for `--action change-leader|reassign-partitions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReassignMode {
    /// Preferred-leader election — no data movement.
    ChangeLeader,
    /// Move replicas — data movement (when a broker outside the replica set
    /// exists; see [`reassign_target`]).
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
    /// `live_brokers` are the ids a reassignment may move a replica onto (every
    /// broker except one kept down by `--leave-broker-down`).
    Migrate {
        topics: Vec<String>,
        mode: ReassignMode,
        live_brokers: Vec<i32>,
    },
    /// Kill every listed broker at once (SIGKILL, a whole-cluster crash), keep
    /// the cluster down for `outage`, then start them all and wait until each
    /// has re-registered (bounded by `wait_up`). Every partition, the group
    /// coordinator and the controller quorum are unavailable for the whole
    /// outage, so the clients must buffer, retry and rediscover everything.
    /// Automates the manual `docker kill` / `docker start` of the whole
    /// cluster the Sep 2026 matrix did by hand.
    AllBrokersDown {
        nodes: Vec<u16>,
        outage: Duration,
        wait_up: Duration,
    },
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
                brokers.stop(*node_id, *kind).await;

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

                let started_at = std::time::SystemTime::now();
                brokers.start(*node_id).await;
                wait_restarted(brokers, admin, *node_id, started_at, *wait_up, topics).await;
                let after = leaders_of_topics(topics, admin).await;
                log_leader_migration_all(&format!("broker {node_id} back up"), &down_state, &after, reports);
                eprintln!("chaos: broker {node_id} back up");
            },
            ChaosAction::Migrate { topics, mode, live_brokers } => {
                for topic in topics {
                    match mode {
                        ReassignMode::ChangeLeader => {
                            change_leader(topic, live_brokers, admin, reports).await;
                        },
                        ReassignMode::ReassignPartitions => {
                            reassign_partitions(topic, live_brokers, admin, reports).await;
                        },
                    }
                }
            },
            ChaosAction::AllBrokersDown { nodes, outage, wait_up } => {
                eprintln!("chaos: killing every running broker {nodes:?} at once (SIGKILL), down for {outage:?}");
                if let Some(r) = reports {
                    r.record_leader_change(&format!("all-brokers-down nodes={nodes:?} outage={outage:?}"));
                }
                // All at once, not one after another: a staggered kill would
                // let leadership hop to the survivors, which is a roll, not a
                // cluster crash.
                futures_util::future::join_all(nodes.iter().map(|n| brokers.stop(*n, StopKind::Unclean))).await;
                tokio::time::sleep(*outage).await;
                let started_at = std::time::SystemTime::now();
                futures_util::future::join_all(nodes.iter().map(|n| brokers.start(*n))).await;
                // The brokers recover in parallel; wait for each in turn (the
                // first wait dominates). No topic list: after a whole-cluster
                // crash every replica restarts together, so there is no ISR to
                // rejoin that the process start does not already cover.
                for node_id in nodes {
                    wait_restarted(brokers, admin, *node_id, started_at, *wait_up, &[]).await;
                }
                eprintln!("chaos: all brokers back up after the outage");
            },
        }
    }
}

/// Wait, within `wait_up` overall, until a broker started at `started_at` is
/// genuinely back:
///
/// 1. the new process has logged `Kafka Server started`
///    ([`BrokerControl::wait_server_started`]);
/// 2. it is registered with the quorum ([`BrokerControl::wait_operational`]);
/// 3. it is back in the ISR of every partition of `topics` it replicates.
///
/// Steps 1–2 are required: a broker that never comes back is an environment
/// failure and panics the run, as before. Checking registration alone was not
/// enough, because a broker killed seconds ago is still listed until its
/// controller session expires (~9 s). The roll then moved on while the broker
/// was still down, so the next roll could take a second broker down with it.
/// Step 3 is best-effort. A replica still catching up (large messages, a short
/// `wait_up`) is logged, not asserted: it is cluster state, not a client
/// verdict.
async fn wait_restarted(
    brokers: &BrokerControl<'_>,
    admin: &dyn Admin,
    node_id: u16,
    started_at: std::time::SystemTime,
    wait_up: Duration,
    topics: &[String],
) {
    let deadline = Instant::now() + wait_up;
    let started = brokers.wait_server_started(node_id, started_at, wait_up).await;
    assert!(
        started,
        "broker {node_id} did not log `Kafka Server started` within {wait_up:?} of its restart"
    );
    let remaining = deadline.saturating_duration_since(Instant::now()).max(Duration::from_secs(5));
    let up = brokers.wait_operational(admin, node_id, remaining).await;
    assert!(up, "broker {node_id} did not become operational within {wait_up:?}");

    if topics.is_empty() {
        return;
    }
    let node = i32::from(node_id);
    let mut lagging: Vec<String> = Vec::new();
    loop {
        lagging.clear();
        for topic in topics {
            match partition_state(topic, admin).await {
                Some(state) => lagging.extend(
                    state
                        .iter()
                        .filter(|(_, s)| s.replicas.contains(&node) && !s.isr.contains(&node))
                        .map(|(p, _)| format!("{topic}-{p}")),
                ),
                None => lagging.push(format!("{topic} (describe failed)")),
            }
        }
        if lagging.is_empty() {
            eprintln!("chaos:   broker {node_id} is back in the ISR of all its partitions");
            return;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "chaos: WARN broker {node_id} is operational but not yet back in the ISR of {} partition(s) \
                 after {wait_up:?}: {} — continuing",
                lagging.len(),
                lagging.join(", ")
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
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
///
/// The new preferred leader is always one of `live_brokers` (see
/// [`change_leader_target`]), so a broker kept down by `--leave-broker-down`
/// is never planned as leader.
async fn change_leader(topic: &str, live_brokers: &[i32], admin: &dyn Admin, reports: &ReportsHandle) {
    eprintln!("chaos: change-leader for topic {topic}");

    // A describe that fails or times out on a churning cluster is an environment
    // hiccup, not a client verdict: skip the action loudly instead of panicking
    // the run before it can produce a verdict.
    let Some(before) = partition_state(topic, admin).await else {
        eprintln!("chaos: change-leader for {topic} SKIPPED: could not describe the topic (before-snapshot)");
        return;
    };

    // Reorder replicas (rotate) — same set, different preferred leader. `plan`
    // records the intended new leader per partition (rotated[0]).
    let mut reassignments: HashMap<TopicPartition, Option<NewPartitionReassignment>> = HashMap::new();
    let mut plan: HashMap<i32, i32> = HashMap::new();
    for (&partition, state) in &before {
        let Some(rotated) = change_leader_target(&state.replicas, state.leader, live_brokers) else {
            continue;
        };
        plan.insert(partition, rotated[0]);
        reassignments.insert(
            TopicPartition::new(topic, partition),
            Some(NewPartitionReassignment::new(rotated).expect("non-empty replicas")),
        );
    }
    if reassignments.is_empty() {
        eprintln!("chaos: no partitions with another live replica to change leader to");
        return;
    }

    admin
        .alter_partition_reassignments(&reassignments)
        .all()
        .get()
        .await
        .expect("change-leader reorder failed");
    wait_reassignments_complete(topic, admin).await;

    // Elect the new preferred leaders.
    if let Err(err) = admin.elect_leaders(ElectionType::Preferred, None).all().get().await {
        eprintln!("chaos: preferred-leader election returned: {err} (often benign)");
    }

    verify_leader_plan("change-leader", topic, &before, &plan, admin, reports).await;
}

/// Move replicas around for every partition of `topic`: read the current
/// replica sets via `describe_topics`, swap one replica of each onto a live
/// broker outside the set ([`reassign_target`]: the new replica must copy the
/// partition from the leader, and a different broker becomes preferred
/// leader), submit via `alter_partition_reassignments`, then poll
/// `list_partition_reassignments` until the topic has no in-progress
/// reassignments (the `kafka-reassign-partitions.sh --verify` analog). Data
/// moves — this is the reassign-partitions mechanism, distinct from
/// change-leader.
///
/// When the replica set already spans every live broker (e.g. the default
/// 3 brokers at replication 3) there is no broker to move onto. The partition
/// is then only reordered, which is the change-leader mechanism and moves no
/// data; the run says so, and the replica-set check is skipped for it.
async fn reassign_partitions(topic: &str, live_brokers: &[i32], admin: &dyn Admin, reports: &ReportsHandle) {
    eprintln!("chaos: reassigning partitions for topic {topic}");

    // 1. Read current leader + replica assignments (the BEFORE snapshot).
    let Some(before) = partition_state(topic, admin).await else {
        eprintln!("chaos: reassign-partitions for {topic} SKIPPED: could not describe the topic (before-snapshot)");
        return;
    };

    // 2. Compute each partition's target replica list. `plan` records the
    //    intended new leader per partition (target[0]); `moved` the partitions
    //    whose replica set changes (a replica swapped onto another broker).
    let mut reassignments: HashMap<TopicPartition, Option<NewPartitionReassignment>> = HashMap::new();
    let mut plan: HashMap<i32, i32> = HashMap::new();
    let mut moved: Vec<i32> = Vec::new();
    for (&partition, state) in &before {
        if state.replicas.len() < 2 {
            continue; // nothing to move with a single replica
        }
        let (target, moves_data) = reassign_target(partition, &state.replicas, live_brokers);
        plan.insert(partition, target[0]);
        if moves_data {
            moved.push(partition);
        }
        let reassignment = NewPartitionReassignment::new(target).expect("non-empty replicas");
        reassignments.insert(TopicPartition::new(topic, partition), Some(reassignment));
    }

    if reassignments.is_empty() {
        eprintln!("chaos: no partitions with >=2 replicas to reassign");
        return;
    }
    if moved.len() < reassignments.len() {
        eprintln!(
            "chaos: NOTE reassign-partitions for {topic}: {} of {} partition(s) already have a replica on every \
             live broker ({live_brokers:?}), so they are only reordered (a preferred-leader change, no data \
             move); run more brokers than --replication-factor to move data",
            reassignments.len() - moved.len(),
            reassignments.len()
        );
    }

    // 3. Submit.
    admin
        .alter_partition_reassignments(&reassignments)
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
    if let Err(err) = admin.elect_leaders(ElectionType::Preferred, None).all().get().await {
        eprintln!("chaos: preferred-leader election after reassign returned: {err} (often benign)");
    }

    // 6. Prove the replica sets actually changed (an empty pending list is also
    //    the state where nothing moved). Reassign moves data, so this is a
    //    distinct check from the leader-plan verification below. Sets are
    //    compared unordered: a reorder alone moves no data. Partitions that
    //    could only be reordered (step 2) have nothing to check here.
    if moved.is_empty() {
        eprintln!("chaos: reassign-partitions for {topic}: no replica set was planned to change; effect check SKIPPED");
        verify_leader_plan("reassign", topic, &before, &plan, admin, reports).await;
        return;
    }
    let Some(after) = partition_state(topic, admin).await else {
        eprintln!(
            "chaos: reassign-partitions for {topic}: could not describe the topic after the reassignment; \
             effect check SKIPPED"
        );
        return;
    };
    let mut replicas_changed = 0usize;
    for partition in &moved {
        let (Some(b), Some(a)) = (before.get(partition), after.get(partition)) else {
            continue;
        };
        if !same_replica_set(&a.replicas, &b.replicas) {
            replicas_changed += 1;
        }
    }
    assert!(
        replicas_changed > 0,
        "reassign-partitions did not change any partition's replica set for {topic} \
         (before == after) — the reassignment had no effect"
    );

    // 7. Verify each partition's leader matches the plan (target[0]), per
    //    partition, not just an aggregate count (A3).
    verify_leader_plan("reassign", topic, &before, &plan, admin, reports).await;
}

/// The replica list reassign-partitions moves one partition to, and whether
/// that changes the replica *set* (so data moves).
///
/// One replica is removed — one on a broker outside `live_brokers` if the set
/// has one (a broker kept down by `--leave-broker-down`), else the preferred
/// leader. The rest are rotated by one, so a different broker becomes preferred
/// leader, and a live broker outside the set is appended; it has to copy the
/// partition from the leader. Which outside broker is picked by `partition`,
/// spreading the new replicas across the candidates reproducibly.
///
/// When every live broker already holds a replica there is nothing to move
/// onto, and the list is only rotated: the same set in a different order, which
/// moves no data (`false`).
fn reassign_target(partition: i32, replicas: &[i32], live_brokers: &[i32]) -> (Vec<i32>, bool) {
    let outside: Vec<i32> = live_brokers.iter().copied().filter(|b| !replicas.contains(b)).collect();
    if outside.is_empty() {
        let mut rotated = replicas.to_vec();
        rotated.rotate_left(1);
        return (rotated, false);
    }
    let drop = replicas.iter().position(|r| !live_brokers.contains(r)).unwrap_or(0);
    let mut target: Vec<i32> = replicas.to_vec();
    target.remove(drop);
    target.rotate_left(1);
    target.push(outside[partition.unsigned_abs() as usize % outside.len()]);
    (target, true)
}

/// The replica list change-leader reorders one partition to: the replicas
/// rotated by the fewest steps that put a live broker other than the current
/// leader first, so that broker becomes preferred leader. Same set, no data
/// moves.
///
/// A broker outside `live_brokers` (kept down by `--leave-broker-down`) is
/// never put first: the preferred election could not move leadership there, so
/// the partition would never reach its plan. `None` when no other replica is
/// live, and the partition is skipped.
fn change_leader_target(replicas: &[i32], leader: Option<i32>, live_brokers: &[i32]) -> Option<Vec<i32>> {
    (1..replicas.len()).find_map(|steps| {
        let head = replicas[steps];
        (live_brokers.contains(&head) && Some(head) != leader).then(|| {
            let mut rotated = replicas.to_vec();
            rotated.rotate_left(steps);
            rotated
        })
    })
}

/// Whether two replica lists hold the same brokers, in any order.
fn same_replica_set(a: &[i32], b: &[i32]) -> bool {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

/// How many of `eligible` partitions must reach their planned leader for
/// [`verify_leader_plan`] to count the plan as reached: `ceil(2/3 * eligible)`.
fn leader_plan_quorum(eligible: usize) -> usize {
    (2 * eligible).div_ceil(3)
}

/// Verify, **per partition**, that the post-action leader matches the plan
/// (`plan[partition]` = the intended new leader = rotated `replicas[0]`) — the
/// librdkafka `_verify_plan_leaders` analog, stronger than an aggregate
/// "some leader changed" count.
///
/// The preferred-leader election can transiently fail to place the exact
/// preferred leader when that broker is momentarily down (e.g. mid-roll in a
/// composed run — "preferred leader was not available"). So this polls up to
/// [`LEADER_PLAN_SETTLE`] for the planned leaders to settle, re-electing every
/// few seconds, then checks that a **strong majority** (≥⅔) of eligible
/// partitions reached their planned leader and at least one leader moved,
/// logging every per-partition before→after vs plan. A plan that is not
/// reached is reported as `leader plan NOT REACHED … SKIPPED`, not a panic.
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

    // Poll for the planned leaders to settle, re-issuing the preferred election
    // every few seconds. One election is not enough: it declines any partition
    // whose preferred replica is not yet in the ISR (for example a broker that
    // has only just restarted), and nothing re-runs it for us.
    let deadline = Instant::now() + LEADER_PLAN_SETTLE;
    let mut next_election = Instant::now() + LEADER_PLAN_REELECT_EVERY;
    let mut after = std::collections::BTreeMap::new();
    loop {
        if let Some(state) = partition_state(topic, admin).await {
            after = state;
            let matched = eligible
                .iter()
                .filter(|p| after.get(p).and_then(|s| s.leader) == plan.get(p).copied())
                .count();
            if matched == eligible.len() {
                break;
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        if Instant::now() >= next_election {
            next_election = Instant::now() + LEADER_PLAN_REELECT_EVERY;
            if let Err(err) = admin.elect_leaders(ElectionType::Preferred, None).all().get().await {
                eprintln!("chaos:   {label}: preferred-leader re-election returned: {err} (often benign)");
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    if after.is_empty() {
        // Every describe in the settle window failed or timed out (tolerated
        // by `partition_state`): nothing to verify against. Say so instead of
        // failing the run on an environment hiccup.
        eprintln!(
            "chaos: {label} for {topic}: could not describe the topic after the action; leader-plan check SKIPPED"
        );
        return;
    }

    let mut matched_plan = 0usize;
    let mut leaders_changed = 0usize;
    for &partition in &eligible {
        let b = &before[&partition];
        let Some(a) = after.get(&partition) else {
            continue;
        };
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

    // Whether the controller placed the planned leaders is cluster behaviour,
    // not a client verdict. Panicking here aborted a whole run before it could
    // produce a verdict (P3-1MiB, Sep 2026 matrix). Report it loudly instead:
    // the run still exercised the leader movement that did happen.
    let need = leader_plan_quorum(eligible.len());
    if leaders_changed == 0 || matched_plan < need {
        let line = format!(
            "{label} for {topic}: leader plan NOT REACHED — {matched_plan}/{} partitions on their planned \
             leader (want >= {need}), {leaders_changed} leader(s) changed after {LEADER_PLAN_SETTLE:?}; \
             plan check SKIPPED",
            eligible.len()
        );
        eprintln!("chaos: WARN {line}");
        if let Some(r) = reports {
            r.record_leader_change(&line);
        }
        return;
    }
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
        if let Some(state) = partition_state(topic, admin).await {
            for (p, s) in state {
                out.insert((topic.clone(), p), s.leader);
            }
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

/// Poll `list_partition_reassignments` until none of `topic`'s partitions has a
/// reassignment in progress (the `kafka-reassign-partitions.sh --verify`
/// analog), bounded by a 60s deadline. Reassignments of other topics are not
/// counted.
async fn wait_reassignments_complete(topic: &str, admin: &dyn Admin) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let pending = admin
            .list_partition_reassignments()
            .reassignments()
            .get()
            .await
            .map(|m| m.keys().filter(|tp| tp.topic() == topic).count())
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
    /// In-sync replica set (broker ids).
    isr: Vec<i32>,
}

/// Current `(leader, replicas)` for each partition of `topic`, or `None` if the
/// describe could not complete (tolerated on the leader-sampling path).
async fn partition_state(topic: &str, admin: &dyn Admin) -> Option<std::collections::BTreeMap<i32, PartitionState>> {
    let described = admin
        .describe_topics_with_topics(TopicCollection::of_topic_names(vec![topic.to_string()]))
        .all_topic_names()
        .expect("describe_topics by name yields a name-keyed result");
    // Bound the describe: on a degraded cluster (a broker just killed for the
    // roll) the admin client can retry `describe_topics` internally without ever
    // resolving, which would hang the down/up leader sampling — and thus the whole
    // run — indefinitely, before the roll ever reaches the bounded
    // `wait_operational`. A timeout here converts that hang into a tolerated skip
    // (same as an outright describe error): leader sampling is best-effort, so
    // `None` just means "couldn't sample this time", and the roll proceeds.
    let descriptions = match tokio::time::timeout(Duration::from_secs(10), described.get()).await {
        Ok(Ok(d)) => d,
        Ok(Err(err)) => {
            eprintln!("chaos: describe_topics for {topic} failed (tolerated, sampling continues): {err}");
            return None;
        },
        Err(_) => {
            eprintln!("chaos: describe_topics for {topic} timed out (tolerated, sampling continues)");
            return None;
        },
    };
    let Some(description) = descriptions.get(topic) else {
        eprintln!("chaos: describe_topics returned no entry for {topic} (tolerated)");
        return None;
    };
    Some(
        description
            .partitions()
            .iter()
            .map(|info| {
                (
                    info.partition(),
                    PartitionState {
                        leader: info.leader().map(|n| n.id()),
                        replicas: info.replicas().iter().map(|n| n.id()).collect(),
                        isr: info.isr().iter().map(|n| n.id()).collect(),
                    },
                )
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_plan_quorum_is_two_thirds_rounded_up() {
        for (eligible, need) in [(1, 1), (2, 2), (3, 2), (4, 3), (5, 4), (6, 4), (7, 5), (9, 6)] {
            assert_eq!(leader_plan_quorum(eligible), need, "for {eligible} eligible partition(s)");
            assert!(leader_plan_quorum(eligible) <= eligible);
        }
    }

    /// With a broker outside the set, the target swaps one replica onto it: the
    /// set changes (data moves) and a different broker becomes preferred leader.
    #[test]
    fn reassign_target_moves_a_replica_onto_an_outside_broker() {
        let live = [1, 2, 3, 4, 5];
        let (target, moves) = reassign_target(0, &[1, 2, 3], &live);
        assert!(moves);
        assert_eq!(target, vec![3, 2, 4]);
        assert!(!same_replica_set(&target, &[1, 2, 3]));
        // The outside broker is spread by partition.
        assert_eq!(reassign_target(1, &[1, 2, 3], &live).0, vec![3, 2, 5]);
        // Replication 2: the old second replica leads, the new one follows.
        assert_eq!(reassign_target(0, &[2, 4], &live), (vec![4, 1], true));
    }

    /// A replica on a broker that is not live (kept down) is the one dropped,
    /// so the target leads on a live broker and never adds the dead one.
    #[test]
    fn reassign_target_drops_the_replica_on_a_down_broker() {
        let live = [1, 3, 4];
        let (target, moves) = reassign_target(0, &[1, 2, 3], &live);
        assert!(moves);
        assert_eq!(target, vec![3, 1, 4]);
        assert!(target.iter().all(|b| live.contains(b)));
    }

    /// Every live broker already holds a replica: only a reorder is possible.
    #[test]
    fn reassign_target_only_reorders_when_no_broker_is_outside_the_set() {
        let (target, moves) = reassign_target(0, &[1, 2, 3], &[1, 2, 3]);
        assert!(!moves);
        assert_eq!(target, vec![2, 3, 1]);
        assert!(same_replica_set(&target, &[1, 2, 3]));
    }

    /// All replicas live: rotate by one, so the next replica leads.
    #[test]
    fn change_leader_target_rotates_by_one_when_every_replica_is_live() {
        assert_eq!(change_leader_target(&[1, 2, 3], Some(1), &[1, 2, 3]), Some(vec![2, 3, 1]));
    }

    /// The next replica is on a broker kept down: skip past it, never plan it
    /// as leader.
    #[test]
    fn change_leader_target_skips_a_replica_on_a_down_broker() {
        assert_eq!(change_leader_target(&[1, 2, 3], Some(1), &[1, 3]), Some(vec![3, 1, 2]));
        // Replication 2 with the other replica down: no live broker to lead.
        assert_eq!(change_leader_target(&[1, 2], Some(1), &[1, 3]), None);
        assert_eq!(change_leader_target(&[3, 2], Some(3), &[1, 3]), None);
    }

    /// The next replica already leads (leadership moved off the preferred
    /// replica): pick one that changes the leader.
    #[test]
    fn change_leader_target_skips_the_current_leader() {
        assert_eq!(change_leader_target(&[1, 2, 3], Some(2), &[1, 2, 3]), Some(vec![3, 1, 2]));
        // A single replica has nothing to rotate to.
        assert_eq!(change_leader_target(&[1], Some(1), &[1, 2]), None);
    }

    #[test]
    fn same_replica_set_ignores_order() {
        assert!(same_replica_set(&[1, 2, 3], &[3, 1, 2]));
        assert!(!same_replica_set(&[1, 2, 3], &[1, 2, 4]));
        assert!(!same_replica_set(&[1, 2], &[1, 2, 3]));
    }
}
