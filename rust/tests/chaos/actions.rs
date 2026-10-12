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

use confluent_kafka::admin::{Admin, NewPartitionReassignment, PartitionReassignment};
use confluent_kafka::common::{ElectionType, TopicCollection, TopicPartition};

use super::common::broker_control::{BrokerControl, StopKind};
use super::reports::ReportsHandle;

/// How long [`verify_leader_plan`] waits for the planned leaders.
pub const LEADER_PLAN_SETTLE: Duration = Duration::from_secs(30);

/// How often [`verify_leader_plan`] re-issues the preferred election while it
/// waits.
const LEADER_PLAN_REELECT_EVERY: Duration = Duration::from_secs(5);

/// How long a change-leader or reassign-partitions action waits, per topic, for
/// the topic's reassignments to finish: one leftover from an earlier action
/// before planning, plus its own after submitting it. Moving a partition copies
/// all of its data, which grows every cycle under a large-message workload, so
/// this is generous. Past it the action's checks are SKIPPED with a WARN rather
/// than failing the run: how fast the cluster copies data is not a client
/// verdict.
pub const REASSIGN_COMPLETE_MAX: Duration = Duration::from_secs(300);

/// Bound on one `describe_topics` in [`partition_state`]. On a degraded cluster
/// the admin client can retry internally without ever resolving.
const DESCRIBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on one `list_partition_reassignments` attempt in
/// [`wait_reassignments_complete`].
const LIST_REASSIGNMENTS_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on submitting a reassignment (`alter_partition_reassignments`).
const ALTER_REASSIGNMENTS_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on one preferred-leader election request ([`elect_preferred`]).
const ELECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How often [`wait_reassignments_complete`] logs that it is still waiting.
const REASSIGN_PROGRESS_EVERY: Duration = Duration::from_secs(30);

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
                if !brokers.stop(*node_id, *kind).await
                    && let Some(r) = reports
                {
                    // `stop` has already logged the WARN.
                    r.record_leader_change(&format!(
                        "broker-roll node={node_id}: WARN clean stop was NOT clean (SIGKILLed after the grace period)"
                    ));
                }

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

                let started_at = brokers.start(*node_id).await;
                wait_restarted(brokers, admin, *node_id, &started_at, *wait_up, topics).await;
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
                let started_at = futures_util::future::join_all(nodes.iter().map(|n| brokers.start(*n))).await;
                // The brokers recover in parallel; wait for each in turn (the
                // first wait dominates). No topic list: after a whole-cluster
                // crash every replica restarts together, so there is no ISR to
                // rejoin that the process start does not already cover.
                for (node_id, started_at) in nodes.iter().zip(&started_at) {
                    wait_restarted(brokers, admin, *node_id, started_at, *wait_up, &[]).await;
                }
                eprintln!("chaos: all brokers back up after the outage");
            },
        }
    }
}

/// Wait, within `wait_up` overall, until a broker started at `started_at` (the
/// daemon-side start time [`BrokerControl::start`] returned) is genuinely back:
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
    started_at: &str,
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
        // All topics at once, each describe bounded by what is left of
        // `wait_up` (at least 1 s, so the check at the deadline still runs):
        // one describe per topic in turn could overrun `wait_up` by 10 s per
        // topic.
        let bound = deadline
            .saturating_duration_since(Instant::now())
            .clamp(Duration::from_secs(1), DESCRIBE_TIMEOUT);
        let states =
            futures_util::future::join_all(topics.iter().map(|topic| partition_state_within(topic, admin, bound)))
                .await;
        for (topic, state) in topics.iter().zip(states) {
            match state {
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
/// Checks the leader actually moved on enough partitions (a bare preferred
/// election is a no-op when leaders are already preferred, which would
/// otherwise pass silently); see [`verify_leader_plan`].
///
/// The new preferred leader is always one of `live_brokers` (see
/// [`change_leader_target`]), so a broker kept down by `--leave-broker-down`
/// is never planned as leader.
///
/// Whatever the cluster gets in the way of (a describe or submit that fails on
/// a churning cluster, a reassignment that does not finish within
/// [`REASSIGN_COMPLETE_MAX`]) skips the action's checks with a WARN instead of
/// panicking the run before it can produce a verdict: it is an environment
/// hiccup, not a client verdict.
async fn change_leader(topic: &str, live_brokers: &[i32], admin: &dyn Admin, reports: &ReportsHandle) {
    const LABEL: &str = "change-leader";
    eprintln!("chaos: change-leader for topic {topic}");
    let deadline = Instant::now() + REASSIGN_COMPLETE_MAX;

    let Some(before) = settled_partition_state(LABEL, topic, admin, deadline, reports).await else {
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
        warn_skipped(
            LABEL,
            topic,
            &format!("no partition has another live replica ({live_brokers:?}) to make leader, nothing changed"),
            reports,
        );
        return;
    }

    if !submit_and_wait(LABEL, topic, &reassignments, admin, deadline, reports).await {
        return;
    }

    // Elect the new preferred leaders.
    elect_preferred(LABEL, admin).await;

    verify_leader_plan(LABEL, topic, &before, &plan, admin, reports).await;
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
///
/// Cluster-side failures skip the checks with a WARN, as for
/// [`change_leader`].
async fn reassign_partitions(topic: &str, live_brokers: &[i32], admin: &dyn Admin, reports: &ReportsHandle) {
    const LABEL: &str = "reassign-partitions";
    eprintln!("chaos: reassigning partitions for topic {topic}");
    let deadline = Instant::now() + REASSIGN_COMPLETE_MAX;

    // 1. Read current leader + replica assignments (the BEFORE snapshot).
    let Some(before) = settled_partition_state(LABEL, topic, admin, deadline, reports).await else {
        return;
    };

    // 2. Compute each partition's target replica list. `plan` records the
    //    intended new leader per partition (target[0]); `moved` the partitions
    //    whose replica set changes (a replica moved onto another broker).
    let mut reassignments: HashMap<TopicPartition, Option<NewPartitionReassignment>> = HashMap::new();
    let mut plan: HashMap<i32, i32> = HashMap::new();
    let mut moved: Vec<i32> = Vec::new();
    for (&partition, state) in &before {
        let Some((target, moves_data)) = reassign_target(partition, &state.replicas, state.leader, live_brokers) else {
            continue;
        };
        plan.insert(partition, target[0]);
        if moves_data {
            moved.push(partition);
        }
        let reassignment = NewPartitionReassignment::new(target).expect("non-empty replicas");
        reassignments.insert(TopicPartition::new(topic, partition), Some(reassignment));
    }

    if reassignments.is_empty() {
        warn_skipped(
            LABEL,
            topic,
            &format!(
                "no partition has a live broker ({live_brokers:?}) to move a replica onto or another live replica \
                 to make leader, nothing changed"
            ),
            reports,
        );
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

    // 3. Submit, and 4. wait for the reassignments to complete (the --verify
    //    analog).
    if !submit_and_wait(LABEL, topic, &reassignments, admin, deadline, reports).await {
        return;
    }

    // 5. Elect the new preferred leaders. A reassignment changes the replica
    //    order (hence the *preferred* leader) but does not itself move the
    //    *current* leader unless the broker's auto-rebalance happens to fire.
    //    Trigger the election explicitly so the leader change is deterministic
    //    and observable — the same step `kafka-reassign-partitions.sh` users
    //    take.
    elect_preferred(LABEL, admin).await;

    // 6. Prove the replica sets actually changed (an empty pending list is also
    //    the state where nothing moved). Reassign moves data, so this is a
    //    distinct check from the leader-plan verification below. Sets are
    //    compared unordered: a reorder alone moves no data. Partitions that
    //    could only be reordered (step 2) have nothing to check here.
    if moved.is_empty() {
        eprintln!("chaos: reassign-partitions for {topic}: no replica set was planned to change; effect check SKIPPED");
        verify_leader_plan(LABEL, topic, &before, &plan, admin, reports).await;
        return;
    }
    let Some(after) = partition_state(topic, admin).await else {
        warn_skipped(
            LABEL,
            topic,
            "could not describe the topic after the reassignment, effect check",
            reports,
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
    verify_leader_plan(LABEL, topic, &before, &plan, admin, reports).await;
}

/// The BEFORE snapshot a migrate action plans from, once `topic` has no
/// reassignment in progress; `None` (after a WARN) if that cannot be had by
/// `deadline`.
///
/// A reassignment still running (one an earlier action gave up waiting for)
/// lists its adding and removing replicas together as the partition's
/// replicas, e.g. 4 replicas at replication 3. Planning from that list and
/// submitting it would cancel the move and keep every replica, permanently
/// raising the replication factor, so wait for it to finish first.
async fn settled_partition_state(
    label: &str,
    topic: &str,
    admin: &dyn Admin,
    deadline: Instant,
    reports: &ReportsHandle,
) -> Option<std::collections::BTreeMap<i32, PartitionState>> {
    if let Err(reason) = wait_reassignments_complete(topic, admin, deadline).await {
        warn_skipped(
            label,
            topic,
            &format!(
                "an earlier reassignment did not finish within {REASSIGN_COMPLETE_MAX:?} ({reason}), nothing \
                 submitted"
            ),
            reports,
        );
        return None;
    }
    let state = partition_state(topic, admin).await;
    if state.is_none() {
        warn_skipped(
            label,
            topic,
            "could not describe the topic (before-snapshot), nothing submitted",
            reports,
        );
    }
    state
}

/// Submit `reassignments`, then wait (until `deadline`) for `topic` to have no
/// reassignment in progress. `true` when both succeeded; otherwise a WARN says
/// why the action's checks are skipped.
///
/// The wait runs even when the submit failed: the request may have been
/// applied to some partitions, and the next action must not start in the
/// middle of their move.
async fn submit_and_wait(
    label: &str,
    topic: &str,
    reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
    admin: &dyn Admin,
    deadline: Instant,
    reports: &ReportsHandle,
) -> bool {
    let result = admin.alter_partition_reassignments(reassignments);
    let submit = result.all();
    let submitted = match tokio::time::timeout(ALTER_REASSIGNMENTS_TIMEOUT, submit.get()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(format!("alter_partition_reassignments failed: {err}")),
        Err(_) => Err(format!(
            "alter_partition_reassignments timed out after {ALTER_REASSIGNMENTS_TIMEOUT:?}"
        )),
    };
    let completed = wait_reassignments_complete(topic, admin, deadline).await;
    if let Err(reason) = submitted {
        warn_skipped(label, topic, &format!("{reason}, checks"), reports);
        return false;
    }
    if let Err(reason) = completed {
        warn_skipped(
            label,
            topic,
            &format!("reassignment not complete within {REASSIGN_COMPLETE_MAX:?} ({reason}), checks"),
            reports,
        );
        return false;
    }
    true
}

/// Log a WARN (and record it in `leader-changes.txt`) that a migrate action on
/// `topic` skipped something: `what` says what and why, and reads before
/// "SKIPPED".
fn warn_skipped(label: &str, topic: &str, what: &str, reports: &ReportsHandle) {
    let line = format!("{label} for {topic}: {what} SKIPPED");
    eprintln!("chaos: WARN {line}");
    if let Some(r) = reports {
        r.record_leader_change(&line);
    }
}

/// Issue a preferred-leader election for every partition, bounded by
/// [`ELECT_TIMEOUT`]. A failure is only logged: the election reports partitions
/// whose preferred leader already leads, or is not yet in the ISR, as errors,
/// and the leader-plan check is what judges the outcome.
async fn elect_preferred(label: &str, admin: &dyn Admin) {
    let result = admin.elect_leaders(ElectionType::Preferred, None);
    let all = result.all();
    match tokio::time::timeout(ELECT_TIMEOUT, all.get()).await {
        Ok(Ok(())) => {},
        Ok(Err(err)) => eprintln!("chaos:   {label}: preferred-leader election returned: {err} (often benign)"),
        Err(_) => {
            eprintln!("chaos:   {label}: preferred-leader election timed out after {ELECT_TIMEOUT:?} (tolerated)")
        },
    }
}

/// The replica list reassign-partitions moves one partition to, and whether
/// that changes the replica *set* (so data moves); `None` when the partition
/// cannot be changed at all.
///
/// A live broker outside the set is added; it has to copy the partition from
/// the leader. Which outside broker is picked by `partition`, spreading the new
/// replicas across the candidates reproducibly. One replica is removed — one on
/// a broker outside `live_brokers` if the set has one (a broker kept down by
/// `--leave-broker-down`), else the preferred leader — and the rest are rotated
/// by one, the new replica going last. A lone replica is simply replaced by the
/// new one. If that list would lead on the current `leader` (or on a broker that
/// is not live), it is rotated to the first live broker that is not the leader,
/// so the leader-plan check proves a move; the new replica always qualifies.
///
/// When every live broker already holds a replica there is nothing to move
/// onto, and the list is only reordered as change-leader does
/// ([`change_leader_target`]): the same set in a different order, which moves
/// no data (`false`), or `None` if no other replica is live.
fn reassign_target(
    partition: i32,
    replicas: &[i32],
    leader: Option<i32>,
    live_brokers: &[i32],
) -> Option<(Vec<i32>, bool)> {
    if replicas.is_empty() {
        return None;
    }
    let outside: Vec<i32> = live_brokers.iter().copied().filter(|b| !replicas.contains(b)).collect();
    if outside.is_empty() {
        return change_leader_target(replicas, leader, live_brokers).map(|rotated| (rotated, false));
    }
    let drop = replicas.iter().position(|r| !live_brokers.contains(r)).unwrap_or(0);
    let mut target: Vec<i32> = replicas.to_vec();
    target.remove(drop);
    if !target.is_empty() {
        target.rotate_left(1);
    }
    target.push(outside[partition.unsigned_abs() as usize % outside.len()]);
    if let Some(head) = target.iter().position(|b| live_brokers.contains(b) && Some(*b) != leader) {
        target.rotate_left(head);
    }
    Some((target, true))
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
            elect_preferred(label, admin).await;
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
///
/// The topics are described concurrently, so one sample takes at most
/// [`DESCRIBE_TIMEOUT`] however many topics there are; a roll takes three.
async fn leaders_of_topics(
    topics: &[String],
    admin: &dyn Admin,
) -> std::collections::BTreeMap<(String, i32), Option<i32>> {
    let states = futures_util::future::join_all(topics.iter().map(|topic| partition_state(topic, admin))).await;
    let mut out = std::collections::BTreeMap::new();
    for (topic, state) in topics.iter().zip(states) {
        for (p, s) in state.into_iter().flatten() {
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

/// Poll `list_partition_reassignments` until none of `topic`'s partitions has a
/// reassignment in progress (the `kafka-reassign-partitions.sh --verify`
/// analog), until `deadline`. Reassignments of other topics are not counted.
///
/// `Err` with the last observation if `deadline` passes first. A listing that
/// fails or times out is retried and never read as "nothing in progress": that
/// returned while data was still moving, so the replica-set check passed
/// vacuously and the next change-leader could plan from the combined adding +
/// removing replica list (see [`settled_partition_state`]).
async fn wait_reassignments_complete(topic: &str, admin: &dyn Admin, deadline: Instant) -> Result<(), String> {
    let start = Instant::now();
    let mut next_progress = start + REASSIGN_PROGRESS_EVERY;
    loop {
        // At least 1 s, so the attempt at the deadline still gets an answer.
        let bound = deadline
            .saturating_duration_since(Instant::now())
            .clamp(Duration::from_secs(1), LIST_REASSIGNMENTS_TIMEOUT);
        let result = admin.list_partition_reassignments();
        let listing = result.reassignments();
        let last = match tokio::time::timeout(bound, listing.get()).await {
            Ok(Ok(reassignments)) => match pending_for_topic(&reassignments, topic) {
                0 => return Ok(()),
                pending => format!("{pending} partition(s) still reassigning"),
            },
            Ok(Err(err)) => format!("list_partition_reassignments failed: {err}"),
            Err(_) => format!("list_partition_reassignments timed out after {bound:?}"),
        };
        if Instant::now() >= deadline {
            return Err(last);
        }
        if Instant::now() >= next_progress {
            next_progress = Instant::now() + REASSIGN_PROGRESS_EVERY;
            eprintln!("chaos:   {topic}: {last} after {}s, still waiting", start.elapsed().as_secs());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// How many of `topic`'s partitions are in a `list_partition_reassignments`
/// listing.
fn pending_for_topic(reassignments: &HashMap<TopicPartition, PartitionReassignment>, topic: &str) -> usize {
    reassignments.keys().filter(|tp| tp.topic() == topic).count()
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
/// describe could not complete within [`DESCRIBE_TIMEOUT`] (tolerated on the
/// leader-sampling path).
async fn partition_state(topic: &str, admin: &dyn Admin) -> Option<std::collections::BTreeMap<i32, PartitionState>> {
    partition_state_within(topic, admin, DESCRIBE_TIMEOUT).await
}

/// [`partition_state`] with the describe bounded by `timeout` instead.
async fn partition_state_within(
    topic: &str,
    admin: &dyn Admin,
    timeout: Duration,
) -> Option<std::collections::BTreeMap<i32, PartitionState>> {
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
    let descriptions = match tokio::time::timeout(timeout, described.get()).await {
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
        let (target, moves) = reassign_target(0, &[1, 2, 3], Some(1), &live).unwrap();
        assert!(moves);
        assert_eq!(target, vec![3, 2, 4]);
        assert!(!same_replica_set(&target, &[1, 2, 3]));
        // The outside broker is spread by partition.
        assert_eq!(reassign_target(1, &[1, 2, 3], Some(1), &live), Some((vec![3, 2, 5], true)));
        // Replication 2: the old second replica leads, the new one follows.
        assert_eq!(reassign_target(0, &[2, 4], Some(2), &live), Some((vec![4, 1], true)));
        // No current leader: any live head will do.
        assert_eq!(reassign_target(0, &[1, 2, 3], None, &live), Some((vec![3, 2, 4], true)));
    }

    /// The rotated list would lead on the current leader (leadership had moved
    /// off the preferred replica): lead on the next live non-leader instead,
    /// still moving a replica onto the outside broker.
    #[test]
    fn reassign_target_never_plans_the_current_leader() {
        let live = [1, 2, 3, 4, 5];
        let (target, moves) = reassign_target(0, &[1, 2, 3], Some(3), &live).unwrap();
        assert!(moves);
        assert_eq!(target, vec![2, 4, 3]);
        assert!(same_replica_set(&target, &[2, 3, 4]));
        // Replication 2 led by the replica that would stay first: the new
        // replica leads.
        assert_eq!(reassign_target(0, &[2, 4], Some(4), &live), Some((vec![1, 4], true)));
    }

    /// A lone replica (replication 1) is moved to a live broker outside the
    /// set, which becomes the leader: a real data move, not a no-op.
    #[test]
    fn reassign_target_moves_a_lone_replica() {
        let live = [1, 2, 3];
        assert_eq!(reassign_target(0, &[2], Some(2), &live), Some((vec![1], true)));
        assert_eq!(reassign_target(1, &[2], Some(2), &live), Some((vec![3], true)));
        // Leaderless (its broker is down): moved all the same.
        assert_eq!(reassign_target(0, &[2], None, &live), Some((vec![1], true)));
        // Nowhere to move it, and nothing to reorder.
        assert_eq!(reassign_target(0, &[2], Some(2), &[2]), None);
        // No replicas at all: nothing to plan from.
        assert_eq!(reassign_target(0, &[], None, &live), None);
    }

    /// A replica on a broker that is not live (kept down) is the one dropped,
    /// so the target leads on a live broker and never adds the dead one.
    #[test]
    fn reassign_target_drops_the_replica_on_a_down_broker() {
        let live = [1, 3, 4];
        let (target, moves) = reassign_target(0, &[1, 2, 3], Some(1), &live).unwrap();
        assert!(moves);
        assert_eq!(target, vec![3, 1, 4]);
        assert!(target.iter().all(|b| live.contains(b)));
        // ... and the head still skips the current leader.
        assert_eq!(reassign_target(0, &[1, 2, 3], Some(3), &live), Some((vec![1, 4, 3], true)));
    }

    /// Every live broker already holds a replica: only a reorder is possible,
    /// planned as change-leader plans it (live head, not the current leader).
    #[test]
    fn reassign_target_only_reorders_when_no_broker_is_outside_the_set() {
        let (target, moves) = reassign_target(0, &[1, 2, 3], Some(1), &[1, 2, 3]).unwrap();
        assert!(!moves);
        assert_eq!(target, vec![2, 3, 1]);
        assert!(same_replica_set(&target, &[1, 2, 3]));
        assert_eq!(
            reassign_target(0, &[1, 2, 3], Some(2), &[1, 2, 3]),
            Some((vec![3, 1, 2], false))
        );
        // Replication 2 with the other replica down: no move, no reorder.
        assert_eq!(reassign_target(0, &[1, 2], Some(1), &[1]), None);
    }

    /// Only the topic's own partitions count as in progress.
    #[test]
    fn pending_for_topic_counts_only_that_topic() {
        let moving = || PartitionReassignment::new(vec![1, 2, 3, 4], vec![4], vec![1]);
        let reassignments: HashMap<TopicPartition, PartitionReassignment> = [
            (TopicPartition::new("a", 0), moving()),
            (TopicPartition::new("a", 2), moving()),
            (TopicPartition::new("ab", 0), moving()),
            (TopicPartition::new("b", 1), moving()),
        ]
        .into_iter()
        .collect();
        assert_eq!(pending_for_topic(&reassignments, "a"), 2);
        assert_eq!(pending_for_topic(&reassignments, "b"), 1);
        assert_eq!(pending_for_topic(&reassignments, "c"), 0);
        assert_eq!(pending_for_topic(&HashMap::new(), "a"), 0);
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
