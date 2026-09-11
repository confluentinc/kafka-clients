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

//! Run configuration for the flag-driven chaos runner.
//!
//! The `cargo xtask chaos --flags` front-end parses CLI flags and forwards
//! them as `CHAOS_*` environment variables (the same env-driven pattern the
//! producer perf test uses); the generic `chaos_run` scenario reads them into
//! this [`ChaosConfig`]. Flag names mirror librdkafka's `chaos.py`
//! (`design/current/chaos-parity-gap.md` §2).

use std::time::Duration;

use super::workload::{Backend, CommitMode, Role, WorkloadSpec};

/// The kind of fault a cycle injects, chosen by `--action`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    /// Roll each broker (stop → down → start → wait) per cycle.
    BrokerRoll,
    /// Rotate the preferred leader via `elect_leaders` (no data move).
    ChangeLeader,
    /// Move replicas via `alter_partition_reassignments` (data move).
    ReassignPartitions,
    /// Delete then recreate the topic each cycle.
    TopicRecreate,
}

/// One scheduled fault: a kind and a cadence in cycles. Built from the
/// per-fault CLI flags (broker rolling default-on; others layered via
/// `--topic-recreate [N]` etc.). `every = 1` = every cycle.
#[derive(Debug, Clone, Copy)]
pub struct ActionSpec {
    pub kind: ActionKind,
    /// Fires on cycles `every, 2*every, 3*every, …` (1-based). Always ≥ 1.
    pub every: u32,
}

impl ActionSpec {
    /// Whether this action fires on the given 1-based cycle.
    pub fn fires(&self, cycle_1based: u32) -> bool {
        cycle_1based.is_multiple_of(self.every)
    }
}

/// Fully-resolved chaos run configuration.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    pub brokers: u16,
    pub partitions: i32,
    pub cycles: u32,
    /// Faults to inject, each with its own cadence (`--action KIND[:everyN]`,
    /// repeatable). All fire on their matching cycles, in listed order.
    pub actions: Vec<ActionSpec>,
    /// SIGKILL instead of SIGTERM for broker roll (`--unclean`).
    pub unclean: bool,
    /// Seconds a broker stays down during a roll (`--stop-s`).
    pub stop_s: u64,
    /// Max seconds to wait for a broker to become operational (`--up-s`
    /// scaled; we wait until it rejoins, capped here).
    pub up_wait_s: u64,
    /// Warm-up before the first fault.
    pub warmup_s: u64,
    /// Cooldown between cycles.
    pub between_s: u64,
    /// Max drain window at the end (`--drain-s`).
    pub drain_s: u64,
    /// Idle-based early-drain threshold (`--idle-threshold-s`): end the drain
    /// once consumption has been quiet this long (0 = wait the full `drain_s`).
    pub idle_threshold_s: u64,
    /// Producer target records/sec (0 = max).
    pub rps: u32,
    /// One broker index kept permanently down before rolling (`--leave-broker-down`).
    pub leave_broker_down: Option<u16>,
    /// Reproducibility seed (`--seed`). Drives the broker-roll order and, in
    /// `--random` mode, EVERY random decision (which action fires each cycle,
    /// its parameters, the timing). The same seed reproduces the whole run. `0`
    /// means "unset": the runner picks a fresh seed and prints it so a run that
    /// finds a bug can be replayed with `--seed <printed>`.
    pub seed: u64,
    /// Chaos-monkey mode (`--random`): ignore the per-fault cadences and, each
    /// cycle, let the seeded RNG choose whether an action fires, WHICH fault
    /// (broker-roll / topic-recreate / reassign / change-leader — all are
    /// candidates for a single-topic run; topic-recreate is EXCLUDED from the
    /// candidate set under `--num-topics > 1`, a known limitation), and that
    /// fault's parameters (broker index, clean/unclean, down duration, dwell).
    /// Fully reproducible for a given `seed`.
    pub random: bool,
    /// In `--random` mode, the per-cycle probability that some action fires
    /// (`--action-prob`, default 0.7). Cycles below the draw are quiet.
    pub action_prob: f64,
    /// Dwell between delete and recreate for topic-recreate (`--dwell-s`); 0 =
    /// recreate-immediate.
    pub dwell_s: u64,
    /// Write on-disk report files under target/chaos-runs/<id>/ and capture the
    /// Rust client log (`--reports`).
    pub reports: bool,
    /// Add a consumer at the start of cycle N to force a rebalance
    /// (`--rebalance-add-cycle`, 1-based). `None` = never.
    pub rebalance_add_cycle: Option<u32>,
    /// Remove a dynamically-added consumer at the start of cycle N
    /// (`--rebalance-remove-cycle`, 1-based). `None` = never.
    pub rebalance_remove_cycle: Option<u32>,
    /// Fire the rebalance add/remove **inside the broker-roll down-window**
    /// rather than at the top of the cycle (`--rebalance-mid-roll`), so the
    /// group reassignment overlaps the leader migration in time. Requires a
    /// `BrokerRoll` action to be firing on the same cycle (the default unless
    /// `--no-broker-roll`); otherwise there is no down-window to inject into and
    /// the add/remove falls back to the top of the cycle.
    pub rebalance_mid_roll: bool,
    /// Consumer-churn lower/upper bounds (`--consumer-churn-min`/`--consumer-churn-max`).
    /// When set (both together), every cycle stops a random batch of consumers and
    /// then starts a random batch, keeping the live consumer count within
    /// `[min, max]` — librdkafka's chaos consumer-churn (the C reference's
    /// stop-random-batch / start-random-batch bounded by `[1, CONSUMER_CNT]`, with
    /// `min`/`max` replacing those bounds). `None` = no churn. `min` consumers are
    /// the fixed floor (built up front); churn adds/removes up to `max - min` more.
    pub consumer_churn_min: Option<u32>,
    pub consumer_churn_max: Option<u32>,
    /// Per-workload client-log rotation budget in MiB (`--log-budget-mb`).
    pub log_budget_mb: u64,
    /// Workloads to run (`--workload role:backend`, repeatable).
    pub workloads: Vec<WorkloadSpec>,
    /// Consumer commit mode.
    pub commit_mode: CommitMode,
    /// Topic name (or, with `num_topics > 1`, the prefix — topics are then
    /// named `<topic>_0`, `<topic>_1`, …). See [`Self::topics`].
    pub topic: String,
    /// Number of test topics (`--num-topics`, default 1). With `> 1`, one
    /// producer runs per topic (each at `rps`, so aggregate = `num_topics *
    /// rps`) and consumers subscribe to all topics — mirrors librdkafka.
    pub num_topics: u16,
    /// Producer value payload size in bytes (`--msg-size`, default 100). The
    /// 8-byte big-endian logical index is written into the first bytes of the
    /// value, padded to this size; the key stays the 8-byte index.
    pub msg_size: usize,
    /// Explicit replication factor override (`--replication-factor`). `None` =
    /// librdkafka's default of `min(brokers, 3)`.
    pub replication_factor: Option<i16>,
}

impl ChaosConfig {
    /// Read the configuration from `CHAOS_*` environment variables, applying
    /// librdkafka's defaults for anything unset.
    pub fn from_env() -> Result<Self, String> {
        let brokers = env_parse("CHAOS_BROKERS", 3)?;
        let num_topics: u16 = env_parse("CHAOS_NUM_TOPICS", 1)?;
        let random = env_str("CHAOS_RANDOM", "0") == "1";
        // Consumer churn (`--consumer-churn-min`/`--consumer-churn-max`): both
        // must be given together; `min >= 1` and `max >= min`. When set, churn
        // owns the consumer set (1 producer + `min` fixed consumers built up
        // front; churn adds/removes up to `max - min` more), so it is mutually
        // exclusive with --consumers and --workload.
        let consumer_churn_min = env_opt_u32("CHAOS_CONSUMER_CHURN_MIN")?;
        let consumer_churn_max = env_opt_u32("CHAOS_CONSUMER_CHURN_MAX")?;
        match (consumer_churn_min, consumer_churn_max) {
            (Some(_), None) | (None, Some(_)) => {
                return Err("use both --consumer-churn-min and --consumer-churn-max together".to_string());
            },
            (Some(lo), Some(hi)) => {
                if lo < 1 {
                    return Err("--consumer-churn-min must be >= 1".to_string());
                }
                if hi < lo {
                    return Err("--consumer-churn-max must be >= --consumer-churn-min".to_string());
                }
            },
            (None, None) => {},
        }

        // `--consumers N`: 1 rust producer + N rust consumers (librdkafka's
        // --consumers). Mutually exclusive with --workload.
        let workloads = if let Some(lo) = consumer_churn_min {
            if env_opt_u32("CHAOS_CONSUMERS")?.is_some() {
                return Err("use either --consumer-churn-* or --consumers, not both".to_string());
            }
            if std::env::var("CHAOS_WORKLOADS").is_ok_and(|v| !v.is_empty()) {
                return Err("use either --consumer-churn-* or --workload, not both".to_string());
            }
            let mut spec = String::from("producer:rust");
            for _ in 0..lo {
                spec.push_str(",consumer:rust");
            }
            parse_workloads(&spec)?
        } else {
            match env_opt_u32("CHAOS_CONSUMERS")? {
                Some(n) => {
                    if std::env::var("CHAOS_WORKLOADS").is_ok_and(|v| !v.is_empty()) {
                        return Err("use either --consumers or --workload, not both".to_string());
                    }
                    if n == 0 {
                        return Err("--consumers must be >= 1".to_string());
                    }
                    let mut spec = String::from("producer:rust");
                    for _ in 0..n {
                        spec.push_str(",consumer:rust");
                    }
                    parse_workloads(&spec)?
                },
                None => parse_workloads(&env_str("CHAOS_WORKLOADS", "producer:rust,consumer:rust"))?,
            }
        };
        if workloads.is_empty() {
            return Err("CHAOS_WORKLOADS resolved to no workloads".to_string());
        }

        // Faults, librdkafka-style: broker rolling is implicit (default-on)
        // unless `--no-broker-roll`. Other faults are layered on via valued
        // flags, each with an optional per-N-cycle cadence (env value = the
        // cadence; "1" when the flag was given with no number). Order:
        // broker-roll first, then change-leader, reassign, topic-recreate — so
        // a cycle bounces brokers before migrating/recreating on top.
        let mut actions = Vec::new();
        if env_str("CHAOS_NO_BROKER_ROLL", "0") != "1" {
            actions.push(ActionSpec { kind: ActionKind::BrokerRoll, every: 1 });
        }
        for (env_key, kind) in [
            ("CHAOS_CHANGE_LEADER", ActionKind::ChangeLeader),
            ("CHAOS_REASSIGN_PARTITIONS", ActionKind::ReassignPartitions),
            ("CHAOS_TOPIC_RECREATE", ActionKind::TopicRecreate),
        ] {
            if let Some(every) = env_opt_u32(env_key)? {
                actions.push(ActionSpec { kind, every });
            }
        }
        if actions.is_empty() {
            return Err(
                "no faults configured: broker rolling is disabled (--no-broker-roll) and no other \
                 fault flag (--topic-recreate / --reassign-partitions / --change-leader) was given"
                    .to_string(),
            );
        }

        // Multi-topic + topic-recreate is supported: the consumer does read each
        // recreated topic's post-recreate tail, given enough drain for it to
        // re-discover the new generation under churn. It was previously rejected
        // as a "known limitation" written before the wait-until-observed drain
        // (`Verifier::outstanding`) existed; validated across many `--num-topics 2`
        // recreate runs with `--drain-s 180`. Use a generous `--drain-s` (≥180)
        // for recreate scenarios so the post-recreate tail is not cut off.

        let commit_mode = match env_str("CHAOS_COMMIT", "sync").as_str() {
            "sync" => CommitMode::Sync,
            "async" => CommitMode::Async,
            other => return Err(format!("CHAOS_COMMIT must be sync or async, got '{other}'")),
        };

        let leave_broker_down = match std::env::var("CHAOS_LEAVE_BROKER_DOWN") {
            Ok(v) if !v.is_empty() => Some(
                v.parse()
                    .map_err(|_| "CHAOS_LEAVE_BROKER_DOWN must be a broker index".to_string())?,
            ),
            _ => None,
        };

        Ok(Self {
            brokers,
            partitions: env_parse("CHAOS_PARTITIONS", 6)?,
            cycles: env_parse("CHAOS_CYCLES", 3)?,
            actions,
            unclean: env_str("CHAOS_UNCLEAN", "0") == "1",
            stop_s: env_parse("CHAOS_STOP_S", 5)?,
            up_wait_s: env_parse("CHAOS_UP_WAIT_S", 60)?,
            warmup_s: env_parse("CHAOS_WARMUP_S", 5)?,
            between_s: env_parse("CHAOS_BETWEEN_S", 3)?,
            drain_s: env_parse("CHAOS_DRAIN_S", 15)?,
            idle_threshold_s: env_parse("CHAOS_IDLE_THRESHOLD_S", 3)?,
            rps: env_parse("CHAOS_RPS", 200)?,
            leave_broker_down,
            seed: env_parse("CHAOS_SEED", 0)?,
            random,
            action_prob: env_parse("CHAOS_ACTION_PROB", 0.7_f64)?,
            dwell_s: env_parse("CHAOS_DWELL_S", 0)?,
            reports: env_str("CHAOS_REPORTS", "0") == "1",
            rebalance_add_cycle: env_opt_u32("CHAOS_REBALANCE_ADD_CYCLE")?,
            rebalance_remove_cycle: env_opt_u32("CHAOS_REBALANCE_REMOVE_CYCLE")?,
            rebalance_mid_roll: env_str("CHAOS_REBALANCE_MID_ROLL", "0") == "1",
            consumer_churn_min,
            consumer_churn_max,
            log_budget_mb: env_parse("CHAOS_LOG_BUDGET_MB", 64)?,
            workloads,
            commit_mode,
            topic: env_str("CHAOS_TOPIC", "chaos-run"),
            num_topics,
            msg_size: env_parse("CHAOS_MSG_SIZE", 100)?,
            replication_factor: match std::env::var("CHAOS_REPLICATION_FACTOR") {
                Ok(v) if !v.is_empty() => Some(
                    v.parse()
                        .map_err(|_| "CHAOS_REPLICATION_FACTOR must be an integer".to_string())?,
                ),
                _ => None,
            },
        })
    }

    /// The resolved topic names for this run (librdkafka's topic-naming rule):
    /// `num_topics == 1` yields `[topic]`; `num_topics > 1` yields
    /// `[topic_0, topic_1, …, topic_{n-1}]` using `topic` as the prefix.
    pub fn topics(&self) -> Vec<String> {
        if self.num_topics <= 1 {
            vec![self.topic.clone()]
        } else {
            (0..self.num_topics).map(|i| format!("{}_{i}", self.topic)).collect()
        }
    }

    pub fn stop_dur(&self) -> Duration {
        Duration::from_secs(self.stop_s)
    }
    pub fn up_wait_dur(&self) -> Duration {
        Duration::from_secs(self.up_wait_s)
    }
    pub fn drain_dur(&self) -> Duration {
        Duration::from_secs(self.drain_s)
    }
    pub fn idle_threshold_dur(&self) -> Duration {
        Duration::from_secs(self.idle_threshold_s)
    }

    /// Number of distinct partitions expected to carry records (coverage
    /// guard). For a healthy multi-partition topic this is all partitions;
    /// we require at least half to tolerate assignment skew under churn.
    pub fn min_partitions(&self) -> usize {
        ((self.partitions as usize) / 2).max(1)
    }

    /// Pretty one-line summary for the run header.
    pub fn summary(&self) -> String {
        let wl: Vec<String> = self.workloads.iter().map(WorkloadSpec::label).collect();
        // In random mode the fixed cadence is not used; describe the mode
        // instead so the header reflects what actually drives the run.
        let actions_desc = if self.random {
            format!("RANDOM(prob={}, all-faults)", self.action_prob)
        } else {
            let actions: Vec<String> = self
                .actions
                .iter()
                .map(|a| {
                    if a.every == 1 {
                        format!("{:?}", a.kind)
                    } else {
                        format!("{:?}:every{}", a.kind, a.every)
                    }
                })
                .collect();
            format!("[{}]", actions.join(", "))
        };
        format!(
            "brokers={} topics={} partitions={} replication={} msg_size={} cycles={} actions={} \
             unclean={} rps={} seed={} workloads=[{}]",
            self.brokers,
            self.num_topics,
            self.partitions,
            self.replication_factor
                .map(|r| r.to_string())
                .unwrap_or_else(|| format!("min({},3)", self.brokers)),
            self.msg_size,
            self.cycles,
            actions_desc,
            self.unclean,
            self.rps,
            self.seed,
            wl.join(", ")
        )
    }
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> Result<T, String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v.parse().map_err(|_| format!("{key} is not a valid value: '{v}'")),
        _ => Ok(default),
    }
}

/// Parse an optional u32 env var: unset/empty → `None`, else parsed.
fn env_opt_u32(key: &str) -> Result<Option<u32>, String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v.parse().map(Some).map_err(|_| format!("{key} must be a cycle number: '{v}'")),
        _ => Ok(None),
    }
}

/// Parse a comma-separated list of `role:backend` specs, numbering instances
/// per (role, backend) so client ids are stable and unique.
fn parse_workloads(s: &str) -> Result<Vec<WorkloadSpec>, String> {
    let mut specs = Vec::new();
    let mut counters: std::collections::HashMap<(Role, Backend), u32> = std::collections::HashMap::new();
    for tok in s.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        let (role, backend) = tok
            .split_once(':')
            .ok_or_else(|| format!("workload '{tok}' must be role:backend"))?;
        let role = match role {
            "producer" => Role::Producer,
            "consumer" => Role::Consumer,
            _ => return Err(format!("workload role must be producer or consumer, got '{role}'")),
        };
        let backend = Backend::parse(backend)
            .ok_or_else(|| format!("workload backend must be rust, python or c, got '{backend}'"))?;
        let n = counters.entry((role, backend)).or_insert(0);
        *n += 1;
        specs.push(WorkloadSpec { role, backend, instance: *n });
    }
    Ok(specs)
}
