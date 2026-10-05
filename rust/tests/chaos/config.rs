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
    /// `--num-topics > 1` unless `--allow-multi-topic-recreate`, the same known
    /// consumer-side limitation for which `from_env` rejects the fixed-cadence
    /// combination without that flag) and that fault's
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
    /// Run topic-recreate under `--num-topics > 1` despite the known consumer
    /// defect (`--allow-multi-topic-recreate`); a failure with the defect's
    /// signature is labelled KNOWN DEFECT. In `--random` mode this puts
    /// topic-recreate back among the candidates of a multi-topic run.
    pub allow_multi_topic_recreate: bool,
    /// Producer value payload size in bytes (`--msg-size`, default 100). The
    /// 8-byte big-endian logical index is written into the first bytes of the
    /// value, padded to this size; the key stays the 8-byte index.
    pub msg_size: usize,
    /// Explicit replication factor override (`--replication-factor`). `None` =
    /// librdkafka's default of `min(brokers, 3)`. Always within
    /// `1..=live brokers`; with `--leave-broker-down` and no explicit factor it
    /// is resolved to `min(live brokers, 3)` (see `from_env`).
    pub replication_factor: Option<i16>,
    /// Broker listener / client `security.protocol` for every client in the run
    /// (`--security-protocol`, default `plaintext`).
    pub security_protocol: SecurityProtocol,
}

impl ChaosConfig {
    /// Read the configuration from `CHAOS_*` environment variables, applying
    /// librdkafka's defaults for anything unset.
    pub fn from_env() -> Result<Self, String> {
        Self::from_vars(&|key| std::env::var(key).ok())
    }

    /// [`Self::from_env`] over an arbitrary variable source (`lookup(key)` is
    /// the value of `key`, if set), so the validation can be unit-tested without
    /// mutating the process environment. An empty value counts as unset.
    fn from_vars(lookup: &dyn Fn(&str) -> Option<String>) -> Result<Self, String> {
        let var = |key: &str| lookup(key).filter(|v| !v.is_empty());
        let var: &dyn Fn(&str) -> Option<String> = &var;
        let brokers: u16 = env_parse(var, "CHAOS_BROKERS", 3)?;
        if brokers == 0 {
            return Err("--brokers must be >= 1".to_string());
        }
        let partitions: i32 = env_parse(var, "CHAOS_PARTITIONS", 6)?;
        if partitions < 1 {
            return Err(format!("--partitions must be >= 1, got {partitions}"));
        }
        let num_topics: u16 = env_parse(var, "CHAOS_NUM_TOPICS", 1)?;
        let random = env_str(var, "CHAOS_RANDOM", "0") == "1";
        // Consumer churn (`--consumer-churn-min`/`--consumer-churn-max`): both
        // must be given together; `min >= 1` and `max >= min`. When set, churn
        // owns the consumer set (1 producer + `min` fixed consumers built up
        // front; churn adds/removes up to `max - min` more), so it is mutually
        // exclusive with --consumers and --workload.
        let consumer_churn_min = env_opt_u32(var, "CHAOS_CONSUMER_CHURN_MIN")?;
        let consumer_churn_max = env_opt_u32(var, "CHAOS_CONSUMER_CHURN_MAX")?;
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
            if env_opt_u32(var, "CHAOS_CONSUMERS")?.is_some() {
                return Err("use either --consumer-churn-* or --consumers, not both".to_string());
            }
            if var("CHAOS_WORKLOADS").is_some() {
                return Err("use either --consumer-churn-* or --workload, not both".to_string());
            }
            let mut spec = String::from("producer:rust");
            for _ in 0..lo {
                spec.push_str(",consumer:rust");
            }
            parse_workloads(&spec)?
        } else {
            match env_opt_u32(var, "CHAOS_CONSUMERS")? {
                Some(n) => {
                    if var("CHAOS_WORKLOADS").is_some() {
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
                None => parse_workloads(&env_str(var, "CHAOS_WORKLOADS", "producer:rust,consumer:rust"))?,
            }
        };
        if workloads.is_empty() {
            return Err("CHAOS_WORKLOADS resolved to no workloads".to_string());
        }
        // The verifier keys every record on `(topic, index)` and each producer
        // numbers its records from 0, so two producer specs would write the same
        // keys: their records would read as idempotence violations (a false
        // FAIL) and one copy's ack would hide the other's loss (a false PASS).
        let producers: Vec<String> = workloads
            .iter()
            .filter(|w| w.role == Role::Producer)
            .map(WorkloadSpec::label)
            .collect();
        if producers.len() > 1 {
            return Err(format!(
                "only one producer workload is supported, got {} ({}): the verifier identifies a record by \
                 (topic, index) and every producer numbers its records from 0. Use one producer:<backend> and \
                 as many consumer:<backend> as needed",
                producers.len(),
                producers.join(", ")
            ));
        }

        // `--security-protocol`: which broker listener every client uses. A
        // gRPC (python / c) backend whose server runs in a sibling container
        // reaches the broker through its container-network listeners, which
        // exist for PLAINTEXT, SSL and SASL_SSL but not SASL_PLAINTEXT. A
        // natively launched server uses the host listeners like the Rust
        // client, so every protocol is available to it.
        let security_protocol = {
            let raw = env_str(var, "CHAOS_SECURITY_PROTOCOL", "plaintext");
            let protocol = SecurityProtocol::parse(&raw).ok_or_else(|| {
                format!("CHAOS_SECURITY_PROTOCOL must be plaintext, ssl, sasl_plaintext or sasl_ssl, got '{raw}'")
            })?;
            if let Some(spec) = workloads.iter().find(|w| w.backend.is_grpc())
                && grpc_backends_use_containers(var)?
                && protocol == SecurityProtocol::SaslPlaintext
            {
                return Err(format!(
                    "--security-protocol sasl_plaintext is not supported for '{}' with \
                     MULTILANG_BACKEND_MODE=container: the gRPC server's container reaches the brokers through \
                     their container-network listeners, which exist for plaintext, ssl and sasl_ssl only",
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
        if env_str(var, "CHAOS_NO_BROKER_ROLL", "0") != "1" {
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
            if let Some(every) = env_opt_u32(var, env_key)? {
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
            .filter(|(key, _)| var(key).is_some())
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
        //
        // `--allow-multi-topic-recreate` runs the combination anyway, for a matrix
        // that wants the original scenarios: the verifier then labels a failure
        // matching this defect's exact signature as KNOWN DEFECT (see
        // `ChaosVerdict::known_defect`), so the matrix runner can retry it, while
        // any other failure stays an ordinary failure.
        //
        // `--random` has no `--topic-recreate` flag to reject: under multi-topic
        // it leaves topic-recreate out of its candidates instead, and puts it back
        // with `--allow-multi-topic-recreate` (`random_plan` in `run_test.rs`).
        let allow_multi_topic_recreate = env_str(var, "CHAOS_ALLOW_MULTI_TOPIC_RECREATE", "0") == "1";
        let multi_topic_recreate = num_topics > 1 && actions.iter().any(|a| a.kind == ActionKind::TopicRecreate);
        if multi_topic_recreate && !allow_multi_topic_recreate {
            return Err(format!(
                "--topic-recreate with --num-topics {num_topics} is not supported yet: after a recreate of one of \
                 several subscribed topics the KIP-848 consumer keeps stale positions on some partitions of the \
                 recreated topic and never reads its new generation (a known consumer-side gap, see \
                 tests/chaos/README.md). Use --num-topics 1 with --topic-recreate, drop --topic-recreate, or pass \
                 --allow-multi-topic-recreate to run it with the defect labelled"
            ));
        }
        if allow_multi_topic_recreate && !(multi_topic_recreate || (random && num_topics > 1)) {
            return Err(
                "--allow-multi-topic-recreate only applies with --num-topics > 1 and either --topic-recreate or \
                 --random"
                    .to_string(),
            );
        }

        let commit_mode = match env_str(var, "CHAOS_COMMIT", "sync").as_str() {
            "sync" => CommitMode::Sync,
            "async" => CommitMode::Async,
            other => return Err(format!("CHAOS_COMMIT must be sync or async, got '{other}'")),
        };

        let leave_broker_down: Option<u16> = match var("CHAOS_LEAVE_BROKER_DOWN") {
            Some(v) => Some(
                v.parse()
                    .map_err(|_| "CHAOS_LEAVE_BROKER_DOWN must be a broker index".to_string())?,
            ),
            None => None,
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

        // Replication factor. The broker kept down by --leave-broker-down is
        // stopped before any workload starts, so a topic created afterwards
        // (every topic-recreate) can only be placed on the live brokers: a
        // factor above that count fails with InvalidReplicationFactor mid-run.
        // So the factor must fit the live brokers, and with a broker left down
        // the default `min(brokers, 3)` becomes `min(live brokers, 3)`, resolved
        // here so the harness creates every topic with it.
        let live_brokers = brokers - u16::from(leave_broker_down.is_some());
        let replication_factor: Option<i16> = match var("CHAOS_REPLICATION_FACTOR") {
            Some(v) => Some(
                v.parse()
                    .map_err(|_| "CHAOS_REPLICATION_FACTOR must be an integer".to_string())?,
            ),
            None => None,
        };
        if let Some(rf) = replication_factor {
            if rf < 1 {
                return Err(format!("--replication-factor must be >= 1, got {rf}"));
            }
            if i32::from(rf) > i32::from(live_brokers) {
                return Err(match leave_broker_down {
                    Some(node) => format!(
                        "--replication-factor {rf} exceeds the {live_brokers} live broker(s): --brokers {brokers} \
                         with broker {node} kept down by --leave-broker-down"
                    ),
                    None => format!("--replication-factor {rf} exceeds --brokers {brokers}"),
                });
            }
        }
        let replication_factor = match (replication_factor, leave_broker_down) {
            (None, Some(_)) => Some(live_brokers.min(3) as i16),
            (rf, _) => rf,
        };

        let cycles: u32 = env_parse(var, "CHAOS_CYCLES", 3)?;
        if cycles == 0 {
            return Err("--cycles must be >= 1".to_string());
        }
        // Rebalance add/remove are 1-based cycle numbers; a cycle past the end
        // never fires, and a remove needs something to remove (a consumer added
        // earlier in the run, or churn-added consumers).
        let rebalance_add_cycle = env_opt_u32(var, "CHAOS_REBALANCE_ADD_CYCLE")?;
        let rebalance_remove_cycle = env_opt_u32(var, "CHAOS_REBALANCE_REMOVE_CYCLE")?;
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

        let outage_s: u64 = env_parse(var, "CHAOS_OUTAGE_S", 30)?;
        if var("CHAOS_OUTAGE_S").is_some() && !actions.iter().any(|a| a.kind == ActionKind::AllBrokersDown) {
            return Err("--outage-s only applies with --all-brokers-down".to_string());
        }
        if outage_s == 0 && actions.iter().any(|a| a.kind == ActionKind::AllBrokersDown) {
            return Err("--outage-s must be >= 1".to_string());
        }

        let action_prob: f64 = env_parse(var, "CHAOS_ACTION_PROB", 0.7_f64)?;
        if !(0.0..=1.0).contains(&action_prob) {
            return Err(format!("--action-prob must be within 0..=1, got {action_prob}"));
        }
        if !random && var("CHAOS_ACTION_PROB").is_some() {
            return Err("--action-prob only applies with --random".to_string());
        }

        // A zero budget would rotate the client log on every line (each line
        // creates a fresh file), producing nothing usable.
        let log_budget_mb: u64 = env_parse(var, "CHAOS_LOG_BUDGET_MB", 64)?;
        if log_budget_mb == 0 {
            return Err("--log-budget-mb must be >= 1".to_string());
        }

        Ok(Self {
            brokers,
            partitions,
            cycles,
            actions,
            unclean: env_str(var, "CHAOS_UNCLEAN", "0") == "1",
            stop_s: env_parse(var, "CHAOS_STOP_S", 5)?,
            up_wait_s: env_parse(var, "CHAOS_UP_WAIT_S", 60)?,
            warmup_s: env_parse(var, "CHAOS_WARMUP_S", 5)?,
            between_s: env_parse(var, "CHAOS_BETWEEN_S", 3)?,
            drain_s: env_parse(var, "CHAOS_DRAIN_S", 15)?,
            idle_threshold_s: env_parse(var, "CHAOS_IDLE_THRESHOLD_S", 3)?,
            rps: env_parse(var, "CHAOS_RPS", 1000)?,
            leave_broker_down,
            seed: env_parse(var, "CHAOS_SEED", 0)?,
            random,
            action_prob,
            outage_s,
            dwell_s: env_parse(var, "CHAOS_DWELL_S", 0)?,
            reports: env_str(var, "CHAOS_REPORTS", "0") == "1",
            rebalance_add_cycle,
            rebalance_remove_cycle,
            rebalance_mid_roll: env_str(var, "CHAOS_REBALANCE_MID_ROLL", "0") == "1",
            consumer_churn_min,
            consumer_churn_max,
            log_budget_mb,
            workloads,
            commit_mode,
            topic: env_str(var, "CHAOS_TOPIC", "chaos-run"),
            num_topics,
            allow_multi_topic_recreate,
            msg_size: env_parse(var, "CHAOS_MSG_SIZE", 100)?,
            replication_factor,
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

    /// Ids of the brokers that are up for the run: `1..=brokers` minus the one
    /// kept down by `--leave-broker-down`.
    pub fn live_broker_ids(&self) -> Vec<i32> {
        (1..=self.brokers)
            .filter(|b| self.leave_broker_down != Some(*b))
            .map(i32::from)
            .collect()
    }

    /// The backend of consumers the run adds mid-run (`--rebalance-add-cycle`,
    /// consumer churn): the backend of the first consumer workload, so a Python
    /// run's rebalances exercise Python consumers joining and leaving the
    /// group. Rust when the run has no consumer workload.
    pub fn added_consumer_backend(&self) -> Backend {
        self.workloads
            .iter()
            .find(|w| w.role == Role::Consumer)
            .map_or(Backend::Rust, |w| w.backend)
    }

    /// Whether `--random` leaves topic-recreate out of its candidates: under
    /// `--num-topics > 1` (the known multi-topic recreate defect), unless
    /// `--allow-multi-topic-recreate` was given.
    pub fn random_excludes_topic_recreate(&self) -> bool {
        self.num_topics > 1 && !self.allow_multi_topic_recreate
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
            let faults = if self.random_excludes_topic_recreate() {
                "no-topic-recreate"
            } else {
                "all-faults"
            };
            format!("RANDOM(prob={}, {faults})", self.action_prob)
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

fn env_str(var: &dyn Fn(&str) -> Option<String>, key: &str, default: &str) -> String {
    var(key).unwrap_or_else(|| default.to_string())
}

/// Whether the gRPC backends' servers will run in containers, from
/// `MULTILANG_BACKEND_MODE` with the same rule as `backend_pool::BackendMode`
/// (unset: containers on Linux, host processes elsewhere). Read here rather
/// than from `backend_pool`, which only exists with `multilanguage-tests`, so
/// the check also runs in the config pre-validation pass.
fn grpc_backends_use_containers(var: &dyn Fn(&str) -> Option<String>) -> Result<bool, String> {
    match var("MULTILANG_BACKEND_MODE").as_deref() {
        Some("container") => Ok(true),
        Some("native") => Ok(false),
        Some(other) => Err(format!("MULTILANG_BACKEND_MODE must be container or native, got '{other}'")),
        None => Ok(cfg!(target_os = "linux")),
    }
}

fn env_parse<T: std::str::FromStr>(var: &dyn Fn(&str) -> Option<String>, key: &str, default: T) -> Result<T, String> {
    match var(key) {
        Some(v) => v.parse().map_err(|_| format!("{key} is not a valid value: '{v}'")),
        None => Ok(default),
    }
}

/// Parse an optional u32 env var: unset/empty → `None`, else parsed.
fn env_opt_u32(var: &dyn Fn(&str) -> Option<String>, key: &str) -> Result<Option<u32>, String> {
    match var(key) {
        Some(v) => v.parse().map(Some).map_err(|_| format!("{key} must be a cycle number: '{v}'")),
        None => Ok(None),
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

    /// `ChaosConfig::from_vars` over the given `CHAOS_*` variables only.
    fn parse(vars: &[(&str, &str)]) -> Result<ChaosConfig, String> {
        let vars: std::collections::HashMap<String, String> =
            vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        ChaosConfig::from_vars(&|key| vars.get(key).cloned())
    }

    fn parse_err(vars: &[(&str, &str)]) -> String {
        parse(vars).expect_err("configuration should be rejected")
    }

    /// Consumers added mid-run use the backend of the run's first consumer.
    #[test]
    fn added_consumers_use_the_first_consumer_backend() {
        assert_eq!(parse(&[]).expect("defaults parse").added_consumer_backend(), Backend::Rust);
        let python = parse(&[("CHAOS_WORKLOADS", "producer:python-async,consumer:python,consumer:python-async")])
            .expect("python workloads parse");
        assert_eq!(python.added_consumer_backend(), Backend::Python);
        let producer_only = parse(&[("CHAOS_WORKLOADS", "producer:python")]).expect("producer-only parses");
        assert_eq!(producer_only.added_consumer_backend(), Backend::Rust);
    }

    /// A gRPC-backed workload takes the run's security protocol like a Rust
    /// one. Only SASL_PLAINTEXT with a containerised server is rejected: the
    /// brokers' container-network listeners cover PLAINTEXT, SSL and SASL_SSL.
    #[test]
    fn grpc_workloads_accept_every_protocol_their_listeners_cover() {
        let workloads = ("CHAOS_WORKLOADS", "producer:python,consumer:c");
        for mode in ["container", "native"] {
            for protocol in ["plaintext", "ssl", "sasl_ssl"] {
                let cfg = parse(&[
                    workloads,
                    ("CHAOS_SECURITY_PROTOCOL", protocol),
                    ("MULTILANG_BACKEND_MODE", mode),
                ])
                .unwrap_or_else(|e| panic!("{protocol} in {mode} mode should be accepted: {e}"));
                assert_eq!(cfg.security_protocol, SecurityProtocol::parse(protocol).unwrap());
            }
        }

        let native = parse(&[
            workloads,
            ("CHAOS_SECURITY_PROTOCOL", "sasl_plaintext"),
            ("MULTILANG_BACKEND_MODE", "native"),
        ])
        .expect("a host-process server uses the host SASL_PLAINTEXT listener");
        assert_eq!(native.security_protocol, SecurityProtocol::SaslPlaintext);

        assert_eq!(
            parse_err(&[
                workloads,
                ("CHAOS_SECURITY_PROTOCOL", "sasl_plaintext"),
                ("MULTILANG_BACKEND_MODE", "container"),
            ]),
            "--security-protocol sasl_plaintext is not supported for 'producer-python-1' with \
             MULTILANG_BACKEND_MODE=container: the gRPC server's container reaches the brokers through their \
             container-network listeners, which exist for plaintext, ssl and sasl_ssl only"
        );

        // Rust-only runs never consult the backend mode.
        parse(&[
            ("CHAOS_SECURITY_PROTOCOL", "sasl_plaintext"),
            ("MULTILANG_BACKEND_MODE", "container"),
        ])
        .expect("rust workloads connect through the host listeners");
        assert_eq!(
            parse_err(&[workloads, ("MULTILANG_BACKEND_MODE", "docker")]),
            "MULTILANG_BACKEND_MODE must be container or native, got 'docker'"
        );
    }

    #[test]
    fn defaults_parse() {
        let cfg = parse(&[]).expect("defaults are valid");
        assert_eq!((cfg.brokers, cfg.partitions, cfg.cycles), (3, 6, 3));
        assert_eq!(cfg.replication_factor, None);
        assert_eq!(cfg.live_broker_ids(), vec![1, 2, 3]);
    }

    #[test]
    fn empty_value_counts_as_unset() {
        let cfg = parse(&[("CHAOS_BROKERS", ""), ("CHAOS_TOPIC_RECREATE", "")]).expect("empty values are unset");
        assert_eq!(cfg.brokers, 3);
        assert!(cfg.actions.iter().all(|a| a.kind != ActionKind::TopicRecreate));
    }

    #[test]
    fn rejects_zero_brokers_and_non_positive_partitions() {
        assert_eq!(parse_err(&[("CHAOS_BROKERS", "0")]), "--brokers must be >= 1");
        assert_eq!(parse_err(&[("CHAOS_PARTITIONS", "0")]), "--partitions must be >= 1, got 0");
        assert_eq!(parse_err(&[("CHAOS_PARTITIONS", "-2")]), "--partitions must be >= 1, got -2");
    }

    #[test]
    fn rejects_leave_broker_down_outside_the_cluster() {
        for node in ["0", "4"] {
            assert_eq!(
                parse_err(&[("CHAOS_LEAVE_BROKER_DOWN", node)]),
                format!("--leave-broker-down {node} is out of range: brokers are numbered 1..=3")
            );
        }
        // Nothing would be left to roll, and `--random`'s broker draw would have
        // no eligible broker.
        for random in ["0", "1"] {
            assert_eq!(
                parse_err(&[
                    ("CHAOS_BROKERS", "1"),
                    ("CHAOS_LEAVE_BROKER_DOWN", "1"),
                    ("CHAOS_RANDOM", random)
                ]),
                "--leave-broker-down with --brokers 1 leaves no broker to roll"
            );
        }
        let cfg = parse(&[("CHAOS_LEAVE_BROKER_DOWN", "2")]).expect("broker 2 of 3 may be left down");
        assert_eq!(cfg.live_broker_ids(), vec![1, 3]);
    }

    /// `ActionSpec::fires` divides by the cadence, so 0 must never get through.
    #[test]
    fn rejects_a_zero_fault_cadence() {
        for (key, flag) in [
            ("CHAOS_CHANGE_LEADER", "--change-leader"),
            ("CHAOS_REASSIGN_PARTITIONS", "--reassign-partitions"),
            ("CHAOS_TOPIC_RECREATE", "--topic-recreate"),
            ("CHAOS_ALL_BROKERS_DOWN", "--all-brokers-down"),
        ] {
            assert_eq!(
                parse_err(&[(key, "0")]),
                format!("{flag} cadence must be >= 1 (0 would never fire the fault)")
            );
        }
        let cfg = parse(&[("CHAOS_TOPIC_RECREATE", "2")]).expect("cadence 2 is valid");
        let recreate = cfg.actions.iter().find(|a| a.kind == ActionKind::TopicRecreate).unwrap();
        assert!(!recreate.fires(1) && recreate.fires(2) && !recreate.fires(3) && recreate.fires(4));
    }

    #[test]
    fn replication_factor_must_fit_the_live_brokers() {
        assert_eq!(
            parse_err(&[("CHAOS_REPLICATION_FACTOR", "0")]),
            "--replication-factor must be >= 1, got 0"
        );
        assert_eq!(
            parse_err(&[("CHAOS_REPLICATION_FACTOR", "4")]),
            "--replication-factor 4 exceeds --brokers 3"
        );
        // The left-down broker is stopped before any workload starts, so it
        // cannot host a replica of a topic created during the run.
        assert_eq!(
            parse_err(&[("CHAOS_REPLICATION_FACTOR", "3"), ("CHAOS_LEAVE_BROKER_DOWN", "2")]),
            "--replication-factor 3 exceeds the 2 live broker(s): --brokers 3 with broker 2 kept down by \
             --leave-broker-down"
        );
        let cfg = parse(&[("CHAOS_REPLICATION_FACTOR", "2"), ("CHAOS_LEAVE_BROKER_DOWN", "2")])
            .expect("replication 2 fits 2 live brokers");
        assert_eq!(cfg.replication_factor, Some(2));
        // With a broker left down, the default is sized from the live brokers.
        let cfg = parse(&[("CHAOS_LEAVE_BROKER_DOWN", "2")]).expect("default replication fits");
        assert_eq!(cfg.replication_factor, Some(2));
        let cfg = parse(&[("CHAOS_BROKERS", "5"), ("CHAOS_LEAVE_BROKER_DOWN", "2")]).expect("valid");
        assert_eq!(cfg.replication_factor, Some(3));
        assert_eq!(parse(&[("CHAOS_REPLICATION_FACTOR", "3")]).unwrap().replication_factor, Some(3));
    }

    #[test]
    fn rejects_more_than_one_producer_workload() {
        assert_eq!(
            parse_err(&[("CHAOS_WORKLOADS", "producer:rust,producer:rust,consumer:rust")]),
            "only one producer workload is supported, got 2 (producer-rust-1, producer-rust-2): the verifier \
             identifies a record by (topic, index) and every producer numbers its records from 0. Use one \
             producer:<backend> and as many consumer:<backend> as needed"
        );
        assert!(parse_err(&[("CHAOS_WORKLOADS", "producer:rust,producer:python,consumer:rust")]).contains("got 2"));
        let cfg = parse(&[("CHAOS_WORKLOADS", "producer:rust,consumer:rust,consumer:rust")])
            .expect("one producer with several consumers is valid");
        assert_eq!(cfg.workloads.len(), 3);
        // The --consumers and churn shorthands always build a single producer.
        assert!(parse(&[("CHAOS_CONSUMERS", "4")]).is_ok());
        assert!(parse(&[("CHAOS_CONSUMER_CHURN_MIN", "1"), ("CHAOS_CONSUMER_CHURN_MAX", "3")]).is_ok());
    }

    #[test]
    fn random_multi_topic_excludes_topic_recreate_unless_allowed() {
        let cfg = parse(&[("CHAOS_RANDOM", "1"), ("CHAOS_NUM_TOPICS", "2")]).expect("valid");
        assert!(cfg.random_excludes_topic_recreate());
        assert!(cfg.summary().contains("RANDOM(prob=0.7, no-topic-recreate)"));

        let cfg = parse(&[
            ("CHAOS_RANDOM", "1"),
            ("CHAOS_NUM_TOPICS", "2"),
            ("CHAOS_ALLOW_MULTI_TOPIC_RECREATE", "1"),
        ])
        .expect("--allow-multi-topic-recreate applies to a multi-topic --random run");
        assert!(!cfg.random_excludes_topic_recreate());
        assert!(cfg.summary().contains("RANDOM(prob=0.7, all-faults)"));

        let cfg = parse(&[("CHAOS_RANDOM", "1")]).expect("valid");
        assert!(!cfg.random_excludes_topic_recreate());
    }

    #[test]
    fn allow_multi_topic_recreate_needs_multi_topic_and_recreate_or_random() {
        let expected = "--allow-multi-topic-recreate only applies with --num-topics > 1 and either --topic-recreate \
                        or --random";
        for vars in [
            &[("CHAOS_ALLOW_MULTI_TOPIC_RECREATE", "1"), ("CHAOS_NUM_TOPICS", "2")][..],
            &[("CHAOS_ALLOW_MULTI_TOPIC_RECREATE", "1"), ("CHAOS_RANDOM", "1")][..],
            &[("CHAOS_ALLOW_MULTI_TOPIC_RECREATE", "1"), ("CHAOS_TOPIC_RECREATE", "1")][..],
        ] {
            assert_eq!(parse_err(vars), expected, "for {vars:?}");
        }
        assert!(
            parse_err(&[("CHAOS_NUM_TOPICS", "2"), ("CHAOS_TOPIC_RECREATE", "1")])
                .starts_with("--topic-recreate with --num-topics 2 is not supported yet")
        );
        let cfg = parse(&[
            ("CHAOS_NUM_TOPICS", "2"),
            ("CHAOS_TOPIC_RECREATE", "1"),
            ("CHAOS_ALLOW_MULTI_TOPIC_RECREATE", "1"),
        ])
        .expect("the fixed-cadence combination runs with the flag");
        assert!(cfg.allow_multi_topic_recreate);
    }

    #[test]
    fn security_protocol_tls_and_sasl_flags() {
        assert!(!SecurityProtocol::Plaintext.uses_tls() && !SecurityProtocol::Plaintext.uses_sasl());
        assert!(SecurityProtocol::Ssl.uses_tls() && !SecurityProtocol::Ssl.uses_sasl());
        assert!(!SecurityProtocol::SaslPlaintext.uses_tls() && SecurityProtocol::SaslPlaintext.uses_sasl());
        assert!(SecurityProtocol::SaslSsl.uses_tls() && SecurityProtocol::SaslSsl.uses_sasl());
    }
}
