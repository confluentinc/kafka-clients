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

impl ActionKind {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "broker-roll" => Some(ActionKind::BrokerRoll),
            "change-leader" => Some(ActionKind::ChangeLeader),
            "reassign-partitions" => Some(ActionKind::ReassignPartitions),
            "topic-recreate" => Some(ActionKind::TopicRecreate),
            _ => None,
        }
    }
}

/// One scheduled fault: a kind and a cadence in cycles. Parsed from
/// `--action KIND[:everyN]` (repeatable). `every = 1` = every cycle.
#[derive(Debug, Clone, Copy)]
pub struct ActionSpec {
    pub kind: ActionKind,
    /// Fires on cycles `every, 2*every, 3*every, …` (1-based). Always ≥ 1.
    pub every: u32,
}

impl ActionSpec {
    /// Parse `"KIND"` or `"KIND:everyN"`.
    fn parse(s: &str) -> Result<Self, String> {
        let (kind_str, every) = match s.split_once(':') {
            Some((k, n)) => {
                let n: u32 = n.parse().map_err(|_| format!("action cadence must be a number: '{s}'"))?;
                if n == 0 {
                    return Err(format!("action cadence must be >= 1: '{s}'"));
                }
                (k, n)
            },
            None => (s, 1),
        };
        let kind = ActionKind::parse(kind_str).ok_or_else(|| {
            format!(
                "action '{kind_str}' must be one of: broker-roll, change-leader, reassign-partitions, topic-recreate"
            )
        })?;
        Ok(Self { kind, every })
    }

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
    /// Drain window at the end (`--drain-s`).
    pub drain_s: u64,
    /// Producer target records/sec (0 = max).
    pub rps: u32,
    /// One broker index kept permanently down before rolling (`--leave-broker-down`).
    pub leave_broker_down: Option<u16>,
    /// Deterministic seed for broker-roll order (`--seed`).
    pub seed: u64,
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
    /// Per-workload client-log rotation budget in MiB (`--log-budget-mb`).
    pub log_budget_mb: u64,
    /// Workloads to run (`--workload role:backend`, repeatable).
    pub workloads: Vec<WorkloadSpec>,
    /// Consumer commit mode.
    pub commit_mode: CommitMode,
    /// Topic name.
    pub topic: String,
}

impl ChaosConfig {
    /// Read the configuration from `CHAOS_*` environment variables, applying
    /// librdkafka's defaults for anything unset.
    pub fn from_env() -> Result<Self, String> {
        let brokers = env_parse("CHAOS_BROKERS", 3)?;
        let workloads = parse_workloads(&env_str("CHAOS_WORKLOADS", "producer:rust,consumer:rust"))?;
        if workloads.is_empty() {
            return Err("CHAOS_WORKLOADS resolved to no workloads".to_string());
        }

        // `--action KIND[:everyN]` is repeatable; the front-end joins them into
        // a comma-separated CHAOS_ACTIONS. Default: one broker-roll every cycle.
        let mut actions = Vec::new();
        for tok in env_str("CHAOS_ACTIONS", "broker-roll")
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            actions.push(ActionSpec::parse(tok)?);
        }
        if actions.is_empty() {
            return Err("CHAOS_ACTIONS resolved to no actions".to_string());
        }

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
            rps: env_parse("CHAOS_RPS", 200)?,
            leave_broker_down,
            seed: env_parse("CHAOS_SEED", 0)?,
            dwell_s: env_parse("CHAOS_DWELL_S", 0)?,
            reports: env_str("CHAOS_REPORTS", "0") == "1",
            rebalance_add_cycle: env_opt_u32("CHAOS_REBALANCE_ADD_CYCLE")?,
            rebalance_remove_cycle: env_opt_u32("CHAOS_REBALANCE_REMOVE_CYCLE")?,
            log_budget_mb: env_parse("CHAOS_LOG_BUDGET_MB", 64)?,
            workloads,
            commit_mode,
            topic: env_str("CHAOS_TOPIC", "chaos-run"),
        })
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

    /// Number of distinct partitions expected to carry records (coverage
    /// guard). For a healthy multi-partition topic this is all partitions;
    /// we require at least half to tolerate assignment skew under churn.
    pub fn min_partitions(&self) -> usize {
        ((self.partitions as usize) / 2).max(1)
    }

    /// Pretty one-line summary for the run header.
    pub fn summary(&self) -> String {
        let wl: Vec<String> = self.workloads.iter().map(WorkloadSpec::label).collect();
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
        format!(
            "brokers={} partitions={} cycles={} actions=[{}] unclean={} rps={} workloads=[{}]",
            self.brokers,
            self.partitions,
            self.cycles,
            actions.join(", "),
            self.unclean,
            self.rps,
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
