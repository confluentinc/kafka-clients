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
    /// Kill every broker at once, keep the cluster down for `--outage-s`,
    /// then bring it all back (`--all-brokers-down`).
    AllBrokersDown,
}

/// The broker listener (and matching client `security.protocol`) every client
/// in the run connects through, chosen by `--security-protocol`.
///
/// Every broker the harness starts exposes all four listeners at once (see
/// `tests/common/kafka_cluster.rs`), so this only selects which one the admin
/// client and the workloads use; the fault injection itself is unchanged. The
/// integration suite's `INTEGRATION_TEST_PROTOCOL` is the same idea.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecurityProtocol {
    /// The broker's PLAINTEXT listener (default).
    #[default]
    Plaintext,
    /// One-way TLS: the client trusts the cluster CA; no client certificate.
    Ssl,
    /// SASL/PLAIN over a plain TCP connection (`admin` / `admin-secret`).
    SaslPlaintext,
    /// SASL/PLAIN over TLS.
    SaslSsl,
}

impl SecurityProtocol {
    /// Parse the `--security-protocol` value. Case-insensitive; `-` and `_`
    /// are interchangeable (`sasl_ssl` / `sasl-ssl` / `SASL_SSL`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "plaintext" => Some(Self::Plaintext),
            "ssl" => Some(Self::Ssl),
            "sasl_plaintext" => Some(Self::SaslPlaintext),
            "sasl_ssl" => Some(Self::SaslSsl),
            _ => None,
        }
    }

    /// The value the client accepts for `security.protocol` (Java's
    /// `SecurityProtocol.name`).
    pub fn config_value(self) -> &'static str {
        match self {
            Self::Plaintext => "PLAINTEXT",
            Self::Ssl => "SSL",
            Self::SaslPlaintext => "SASL_PLAINTEXT",
            Self::SaslSsl => "SASL_SSL",
        }
    }

    /// Whether the connection is TLS-wrapped (the client needs the cluster CA).
    pub fn uses_tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }

    /// Whether the connection authenticates with SASL (the client needs the
    /// PLAIN credentials).
    pub fn uses_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }
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
    /// Only consulted for a verifier that does not report
    /// `Verifier::outstanding`; the default `ConservationVerifier` does, and
    /// its drain ends as soon as every acknowledged record has been observed
    /// (bounded by `drain_s`), so this flag has no effect on a default run
    /// (`drain_wait` in `harness.rs`).
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
    /// (broker-roll / topic-recreate / reassign / change-leader — all four are
    /// candidates for a single-topic run; topic-recreate is EXCLUDED under
    /// `--num-topics > 1`, the same known consumer-side limitation that makes
    /// `from_env` reject the fixed-cadence combination) and that fault's
    /// parameters (broker index, clean/unclean, down duration, dwell). The
    /// per-fault flags are rejected in this mode (`from_env`). Fully
    /// reproducible for a given `seed`.
    pub random: bool,
    /// In `--random` mode, the per-cycle probability that some action fires
    /// (`--action-prob`, default 0.7). Cycles below the draw are quiet.
    pub action_prob: f64,
    /// Dwell between delete and recreate for topic-recreate (`--dwell-s`); 0 =
    /// recreate-immediate.
    pub dwell_s: u64,
    /// How long the whole cluster stays down for `--all-brokers-down`
    /// (`--outage-s`, default 30).
    pub outage_s: u64,
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
    /// Broker listener / client `security.protocol` for every client in the run
    /// (`--security-protocol`, default `plaintext`).
    pub security_protocol: SecurityProtocol,
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
                // `max - min` is the headroom churn works in; with none there is
                // never anything to add or remove, so the flags configure no
                // churn at all. Say so rather than run a quiet fixed-consumer run.
                if hi == lo {
                    return Err(format!(
                        "--consumer-churn-min {lo} == --consumer-churn-max {hi} leaves no headroom: churn adds and \
                         removes consumers within (min, max], so nothing would ever churn; use --consumers {lo} \
                         for a fixed set"
                    ));
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

        // `--security-protocol`: which broker listener every client uses. The
        // gRPC (python / c) backends run in a sibling container and reach the
        // broker through its container-network listener, which on this branch
        // exists only for PLAINTEXT — so a secured run is Rust-workload only.
        let security_protocol = {
            let raw = env_str("CHAOS_SECURITY_PROTOCOL", "plaintext");
            let protocol = SecurityProtocol::parse(&raw).ok_or_else(|| {
                format!("CHAOS_SECURITY_PROTOCOL must be plaintext, ssl, sasl_plaintext or sasl_ssl, got '{raw}'")
            })?;
            if protocol != SecurityProtocol::Plaintext
                && let Some(spec) = workloads.iter().find(|w| w.backend.is_grpc())
            {
                return Err(format!(
                    "--security-protocol {} is only supported for rust workloads: '{}' runs through the \
                     gRPC bridge, whose container-network listener is PLAINTEXT only",
                    raw.to_ascii_lowercase(),
                    spec.label()
                ));
            }
            protocol
        };

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
        for (env_key, flag, kind) in [
            ("CHAOS_CHANGE_LEADER", "--change-leader", ActionKind::ChangeLeader),
            (
                "CHAOS_REASSIGN_PARTITIONS",
                "--reassign-partitions",
                ActionKind::ReassignPartitions,
            ),
            ("CHAOS_TOPIC_RECREATE", "--topic-recreate", ActionKind::TopicRecreate),
            ("CHAOS_ALL_BROKERS_DOWN", "--all-brokers-down", ActionKind::AllBrokersDown),
        ] {
            if let Some(every) = env_opt_u32(env_key)? {
                // `cycle % 0` never matches: a cadence of 0 would configure a
                // fault that never fires, and the run would PASS having injected
                // nothing.
                if every == 0 {
                    return Err(format!("{flag} cadence must be >= 1 (0 would never fire the fault)"));
                }
                actions.push(ActionSpec { kind, every });
            }
        }
        if actions.is_empty() {
            return Err(
                "no faults configured: broker rolling is disabled (--no-broker-roll) and no other \
                 fault flag (--topic-recreate / --reassign-partitions / --change-leader / \
                 --all-brokers-down) was given"
                    .to_string(),
            );
        }

        // `--random` draws the fault, the broker, clean/unclean, the down
        // duration and the dwell itself, every cycle. The fixed-cadence and
        // per-fault flags have no effect in that mode, so a run that passes them
        // would silently not do what its command line says (a `--random
        // --unclean` run rolls cleanly half the time). Reject them instead.
        if random {
            let ignored: Vec<&str> = [
                ("CHAOS_UNCLEAN", "--unclean"),
                ("CHAOS_STOP_S", "--stop-s"),
                ("CHAOS_DWELL_S", "--dwell-s"),
                ("CHAOS_NO_BROKER_ROLL", "--no-broker-roll"),
                ("CHAOS_TOPIC_RECREATE", "--topic-recreate"),
                ("CHAOS_REASSIGN_PARTITIONS", "--reassign-partitions"),
                ("CHAOS_CHANGE_LEADER", "--change-leader"),
                ("CHAOS_ALL_BROKERS_DOWN", "--all-brokers-down"),
                ("CHAOS_OUTAGE_S", "--outage-s"),
                ("CHAOS_REBALANCE_MID_ROLL", "--rebalance-mid-roll"),
            ]
            .into_iter()
            .filter(|(key, _)| std::env::var(key).is_ok_and(|v| !v.is_empty()))
            .map(|(_, flag)| flag)
            .collect();
            if !ignored.is_empty() {
                return Err(format!(
                    "--random draws the fault and its parameters (broker, clean/unclean, down duration, \
                     dwell) itself every cycle, so {} would have no effect; drop {} or drop --random",
                    ignored.join(", "),
                    if ignored.len() == 1 { "it" } else { "them" }
                ));
            }
        }

        // Multi-topic + topic-recreate is a KNOWN LIMITATION, rejected up front.
        // When one of several subscribed topics is recreated under a new topic id,
        // the KIP-848 consumer keeps the old generation's fetch positions (and
        // leader epochs) for some of that topic's partitions after the
        // revoke/assign cycle: it never resets onto the new generation, so those
        // partitions' post-recreate records are never consumed, and it keeps
        // committing the stale positions under the old topic id (a revoke-time
        // `commit_sync` then runs into its 60 s timeout). Reproduced on
        // 2026-09-25 with `--num-topics 2 --topic-recreate --cycles 2 --drain-s 180
        // --seed 5`: 461 records lost on 3 of 6 partitions of the recreated topic
        // after the second recreate. Single-topic recreate does recover (the whole
        // assignment is rebuilt). This is a consumer-side defect the harness
        // surfaces, not a harness artifact; the guard stays until it is fixed.
        if num_topics > 1 && actions.iter().any(|a| a.kind == ActionKind::TopicRecreate) {
            return Err(format!(
                "--topic-recreate with --num-topics {num_topics} is not supported yet: after a recreate of one of \
                 several subscribed topics the KIP-848 consumer keeps stale positions on some partitions of the \
                 recreated topic and never reads its new generation (a known consumer-side gap, see \
                 tests/chaos/README.md). Use --num-topics 1 with --topic-recreate, or drop --topic-recreate"
            ));
        }

        let commit_mode = match env_str("CHAOS_COMMIT", "sync").as_str() {
            "sync" => CommitMode::Sync,
            "async" => CommitMode::Async,
            other => return Err(format!("CHAOS_COMMIT must be sync or async, got '{other}'")),
        };

        if brokers == 0 {
            return Err("--brokers must be >= 1".to_string());
        }
        let leave_broker_down: Option<u16> = match std::env::var("CHAOS_LEAVE_BROKER_DOWN") {
            Ok(v) if !v.is_empty() => Some(
                v.parse()
                    .map_err(|_| "CHAOS_LEAVE_BROKER_DOWN must be a broker index".to_string())?,
            ),
            _ => None,
        };
        // Brokers are addressed by 1-based node id; an index outside the
        // cluster would panic inside `BrokerControl::container_id` mid-run, and
        // leaving the only broker down leaves nothing to roll.
        if let Some(node) = leave_broker_down {
            if node < 1 || node > brokers {
                return Err(format!(
                    "--leave-broker-down {node} is out of range: brokers are numbered 1..={brokers}"
                ));
            }
            if brokers == 1 {
                return Err("--leave-broker-down with --brokers 1 leaves no broker to roll".to_string());
            }
        }

        let cycles: u32 = env_parse("CHAOS_CYCLES", 3)?;
        if cycles == 0 {
            return Err("--cycles must be >= 1".to_string());
        }
        // Rebalance add/remove are 1-based cycle numbers; a cycle past the end
        // never fires, and a remove needs something to remove (a consumer added
        // earlier in the run, or churn-added consumers).
        let rebalance_add_cycle = env_opt_u32("CHAOS_REBALANCE_ADD_CYCLE")?;
        let rebalance_remove_cycle = env_opt_u32("CHAOS_REBALANCE_REMOVE_CYCLE")?;
        for (flag, value) in [
            ("--rebalance-add-cycle", rebalance_add_cycle),
            ("--rebalance-remove-cycle", rebalance_remove_cycle),
        ] {
            if let Some(cycle) = value
                && (cycle < 1 || cycle > cycles)
            {
                return Err(format!("{flag} {cycle} would never fire: cycles are numbered 1..={cycles}"));
            }
        }
        match (rebalance_add_cycle, rebalance_remove_cycle) {
            (Some(add), Some(remove)) if remove <= add => {
                return Err(format!(
                    "--rebalance-remove-cycle {remove} must come after --rebalance-add-cycle {add}: the consumer \
                     it removes is added at the top of cycle {add}"
                ));
            },
            (None, Some(remove)) if consumer_churn_min.is_none() => {
                return Err(format!(
                    "--rebalance-remove-cycle {remove} has nothing to remove: no --rebalance-add-cycle and no \
                     consumer churn adds a consumer"
                ));
            },
            _ => {},
        }

        let outage_s: u64 = env_parse("CHAOS_OUTAGE_S", 30)?;
        if std::env::var("CHAOS_OUTAGE_S").is_ok_and(|v| !v.is_empty())
            && !actions.iter().any(|a| a.kind == ActionKind::AllBrokersDown)
        {
            return Err("--outage-s only applies with --all-brokers-down".to_string());
        }
        if outage_s == 0 && actions.iter().any(|a| a.kind == ActionKind::AllBrokersDown) {
            return Err("--outage-s must be >= 1".to_string());
        }

        let action_prob: f64 = env_parse("CHAOS_ACTION_PROB", 0.7_f64)?;
        if !(0.0..=1.0).contains(&action_prob) {
            return Err(format!("--action-prob must be within 0..=1, got {action_prob}"));
        }
        if !random && std::env::var("CHAOS_ACTION_PROB").is_ok_and(|v| !v.is_empty()) {
            return Err("--action-prob only applies with --random".to_string());
        }

        // A zero budget would rotate the client log on every line (each line
        // creates a fresh file), producing nothing usable.
        let log_budget_mb: u64 = env_parse("CHAOS_LOG_BUDGET_MB", 64)?;
        if log_budget_mb == 0 {
            return Err("--log-budget-mb must be >= 1".to_string());
        }

        Ok(Self {
            brokers,
            partitions: env_parse("CHAOS_PARTITIONS", 6)?,
            cycles,
            actions,
            unclean: env_str("CHAOS_UNCLEAN", "0") == "1",
            stop_s: env_parse("CHAOS_STOP_S", 5)?,
            up_wait_s: env_parse("CHAOS_UP_WAIT_S", 60)?,
            warmup_s: env_parse("CHAOS_WARMUP_S", 5)?,
            between_s: env_parse("CHAOS_BETWEEN_S", 3)?,
            drain_s: env_parse("CHAOS_DRAIN_S", 15)?,
            idle_threshold_s: env_parse("CHAOS_IDLE_THRESHOLD_S", 3)?,
            rps: env_parse("CHAOS_RPS", 1000)?,
            leave_broker_down,
            seed: env_parse("CHAOS_SEED", 0)?,
            random,
            action_prob,
            outage_s,
            dwell_s: env_parse("CHAOS_DWELL_S", 0)?,
            reports: env_str("CHAOS_REPORTS", "0") == "1",
            rebalance_add_cycle,
            rebalance_remove_cycle,
            rebalance_mid_roll: env_str("CHAOS_REBALANCE_MID_ROLL", "0") == "1",
            consumer_churn_min,
            consumer_churn_max,
            log_budget_mb,
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
            security_protocol,
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
        // In random mode the fixed cadence is not used, and clean/unclean is
        // drawn per roll; describe the mode instead so the header reflects what
        // actually drives the run.
        let unclean_desc = if self.random {
            "random".to_string()
        } else {
            self.unclean.to_string()
        };
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
             unclean={} rps={} seed={} security={} workloads=[{}]",
            self.brokers,
            self.num_topics,
            self.partitions,
            self.replication_factor
                .map(|r| r.to_string())
                .unwrap_or_else(|| format!("min({},3)", self.brokers)),
            self.msg_size,
            self.cycles,
            actions_desc,
            unclean_desc,
            self.rps,
            self.seed,
            self.security_protocol.config_value(),
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
            .ok_or_else(|| format!("workload backend must be rust, python, python-async or c, got '{backend}'"))?;
        let n = counters.entry((role, backend)).or_insert(0);
        *n += 1;
        specs.push(WorkloadSpec { role, backend, instance: *n });
    }
    Ok(specs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn security_protocol_parses_every_listener_case_insensitively() {
        for (input, expected) in [
            ("plaintext", SecurityProtocol::Plaintext),
            ("PLAINTEXT", SecurityProtocol::Plaintext),
            ("ssl", SecurityProtocol::Ssl),
            ("SSL", SecurityProtocol::Ssl),
            ("sasl_plaintext", SecurityProtocol::SaslPlaintext),
            ("sasl-plaintext", SecurityProtocol::SaslPlaintext),
            ("SASL_PLAINTEXT", SecurityProtocol::SaslPlaintext),
            ("sasl_ssl", SecurityProtocol::SaslSsl),
            ("sasl-ssl", SecurityProtocol::SaslSsl),
            (" SASL_SSL ", SecurityProtocol::SaslSsl),
        ] {
            assert_eq!(SecurityProtocol::parse(input), Some(expected), "for input {input:?}");
        }
        assert_eq!(SecurityProtocol::parse("tls"), None);
        assert_eq!(SecurityProtocol::parse(""), None);
    }

    #[test]
    fn security_protocol_config_value_is_the_java_name() {
        assert_eq!(SecurityProtocol::Plaintext.config_value(), "PLAINTEXT");
        assert_eq!(SecurityProtocol::Ssl.config_value(), "SSL");
        assert_eq!(SecurityProtocol::SaslPlaintext.config_value(), "SASL_PLAINTEXT");
        assert_eq!(SecurityProtocol::SaslSsl.config_value(), "SASL_SSL");
        // Round-trips through the parser, so a printed header can be replayed.
        for p in [
            SecurityProtocol::Plaintext,
            SecurityProtocol::Ssl,
            SecurityProtocol::SaslPlaintext,
            SecurityProtocol::SaslSsl,
        ] {
            assert_eq!(SecurityProtocol::parse(p.config_value()), Some(p));
        }
    }

    #[test]
    fn security_protocol_tls_and_sasl_flags() {
        assert!(!SecurityProtocol::Plaintext.uses_tls() && !SecurityProtocol::Plaintext.uses_sasl());
        assert!(SecurityProtocol::Ssl.uses_tls() && !SecurityProtocol::Ssl.uses_sasl());
        assert!(!SecurityProtocol::SaslPlaintext.uses_tls() && SecurityProtocol::SaslPlaintext.uses_sasl());
        assert!(SecurityProtocol::SaslSsl.uses_tls() && SecurityProtocol::SaslSsl.uses_sasl());
    }
}
