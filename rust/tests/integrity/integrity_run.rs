// Copyright 2026 Confluent Inc.
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

//! The env-driven long run. Every setting has a default; see `README.md`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use confluent_kafka::common::TopicCollection;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::integrity::checker::{Requirements, Verdict};
use crate::integrity::record::PayloadProfile;
use crate::integrity::workload::{
    CopierSpec, GroupSpec, Props, Restart, RestartMode, RunSpec, TopicSpec, WriterSpec, admin, run,
};

const CODECS: [&str; 5] = ["none", "gzip", "snappy", "lz4", "zstd"];

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    match env_opt(key) {
        Some(value) => value.trim().parse().unwrap_or_else(|_| panic!("{key}={value} is not valid")),
        None => default,
    }
}

/// The settings of a run, from the environment.
#[derive(Debug)]
struct LongRunConfig {
    bootstrap_servers: String,
    security_protocol: Option<String>,
    sasl_mechanism: Option<String>,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
    client_config_file: Option<String>,
    duration: Duration,
    rate_per_writer: u32,
    large_rate: u32,
    partitions: i32,
    replication_factor: i16,
    consumers: usize,
    consumer_restart: Duration,
    max_poll_interval: Duration,
    producer_restart: Duration,
    drain: Duration,
    seed: u64,
    topic_prefix: String,
    keep_topics: bool,
    report_dir: PathBuf,
}

impl LongRunConfig {
    fn from_env() -> Self {
        Self {
            bootstrap_servers: env_or("BOOTSTRAP_SERVERS", ""),
            security_protocol: env_opt("SECURITY_PROTOCOL"),
            sasl_mechanism: env_opt("SASL_MECHANISM"),
            sasl_username: env_opt("SASL_USERNAME"),
            sasl_password: env_opt("SASL_PASSWORD"),
            client_config_file: env_opt("CLIENT_CONFIG_FILE"),
            duration: Duration::from_secs(env_parse("DURATION_S", 1800)),
            rate_per_writer: env_parse("RATE_PER_WRITER", 5000),
            large_rate: env_parse("LARGE_RATE", 5),
            partitions: env_parse("PARTITIONS", 6),
            replication_factor: env_parse("REPLICATION_FACTOR", 3),
            consumers: env_parse("CONSUMERS", 3),
            consumer_restart: Duration::from_secs(env_parse("CONSUMER_RESTART_S", 60)),
            max_poll_interval: Duration::from_secs(env_parse("MAX_POLL_INTERVAL_S", 30)),
            producer_restart: Duration::from_secs(env_parse("PRODUCER_RESTART_S", 90)),
            drain: Duration::from_secs(env_parse("DRAIN_S", 300)),
            seed: env_parse("INTEGRITY_SEED", rand::random()),
            topic_prefix: env_or("TOPIC_PREFIX", "integrity"),
            keep_topics: env_or("KEEP_TOPICS", "false") == "true",
            report_dir: PathBuf::from(env_or("REPORT_DIR", "target/integrity-runs")),
        }
    }

    /// Every setting but the credentials, for the report.
    fn describe(&self) -> serde_json::Value {
        serde_json::json!({
            "bootstrap_servers": if self.bootstrap_servers.is_empty() { "local cluster" } else { &self.bootstrap_servers },
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "duration_s": self.duration.as_secs(),
            "rate_per_writer": self.rate_per_writer,
            "large_rate": self.large_rate,
            "partitions": self.partitions,
            "replication_factor": self.replication_factor,
            "consumers": self.consumers,
            "consumer_restart_s": self.consumer_restart.as_secs(),
            "max_poll_interval_s": self.max_poll_interval.as_secs(),
            "producer_restart_s": self.producer_restart.as_secs(),
            "drain_s": self.drain.as_secs(),
            "seed": self.seed,
        })
    }

    /// The connection properties every client of the run starts from.
    fn client_props(&self, bootstrap_servers: &str) -> Props {
        let mut props = Props::new();
        if let Some(path) = &self.client_config_file {
            props.extend(read_properties(path));
        }
        // The variables override the file, the cluster address first.
        props.insert("bootstrap.servers".to_string(), bootstrap_servers.to_string());
        if let Some(protocol) = &self.security_protocol {
            props.insert("security.protocol".to_string(), protocol.clone());
        }
        if let Some(mechanism) = &self.sasl_mechanism {
            props.insert("sasl.mechanism".to_string(), mechanism.clone());
        }
        // Credentials build the login line for whichever mechanism is set, by
        // the variable or by the file.
        if let (Some(user), Some(pass)) = (&self.sasl_username, &self.sasl_password) {
            let mechanism = props.get("sasl.mechanism").cloned().unwrap_or_else(|| "PLAIN".to_string());
            let module = if mechanism.starts_with("SCRAM") {
                "org.apache.kafka.common.security.scram.ScramLoginModule"
            } else {
                "org.apache.kafka.common.security.plain.PlainLoginModule"
            };
            props.insert("sasl.mechanism".to_string(), mechanism);
            let quoted = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
            props.insert(
                "sasl.jaas.config".to_string(),
                format!("{module} required username=\"{}\" password=\"{}\";", quoted(user), quoted(pass)),
            );
        }
        props
    }
}

/// `key=value` lines of a Java properties file; `#` and `!` start comments.
fn read_properties(path: &str) -> Props {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("CLIENT_CONFIG_FILE {path}: {e}"));
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with('!'))
        .filter_map(|line| line.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

/// The local cluster: three combined broker/controller nodes, replicated
/// three ways with `min.insync.replicas=2`, as a production cluster would be.
fn local_cluster() -> ClusterConfig {
    let props: BTreeMap<String, String> = [
        ("KAFKA_MIN_INSYNC_REPLICAS", "2"),
        ("KAFKA_DEFAULT_REPLICATION_FACTOR", "3"),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3"),
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3"),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2"),
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let mut config = ClusterConfig::with_brokers(3).set_server_properties(props);
    config.dedicated = true;
    config
}

/// One stream of records: a writer on a topic of its own, and for `Copy` a
/// transactional copier from that topic into another.
#[derive(Clone, Copy, Debug)]
enum Stream {
    Codec(&'static str),
    Txn,
    Large,
    Copy,
}

impl Stream {
    const ALL: [Stream; 8] = [
        Stream::Codec("none"),
        Stream::Codec("gzip"),
        Stream::Codec("snappy"),
        Stream::Codec("lz4"),
        Stream::Codec("zstd"),
        Stream::Txn,
        Stream::Large,
        Stream::Copy,
    ];

    fn name(self) -> &'static str {
        match self {
            Stream::Codec(codec) => codec,
            Stream::Txn => "txn",
            Stream::Large => "large",
            Stream::Copy => "copy",
        }
    }

    /// The writer id, unique across the streams.
    fn writer(self) -> u32 {
        match self {
            Stream::Codec(codec) => CODECS.iter().position(|c| *c == codec).expect("a known codec") as u32,
            Stream::Txn => 5,
            Stream::Large => 6,
            Stream::Copy => 7,
        }
    }
}

/// Adds `stream`'s topics, writer and copier to `spec`.
fn add_stream(spec: &mut RunSpec, config: &LongRunConfig, name: &dyn Fn(&str) -> String, stream: Stream) {
    let (partitions, rf) = (config.partitions, config.replication_factor);
    let restart = Restart::Every(config.producer_restart);
    let topic = name(stream.name());
    let writer = WriterSpec::new(stream.writer(), &topic)
        .rate(config.rate_per_writer)
        .restart(restart);
    match stream {
        Stream::Codec(codec) => {
            spec.topics.push(TopicSpec::source(&topic, partitions, rf));
            spec.writers.push(writer.prop("compression.type", codec));
        },
        Stream::Txn => {
            spec.topics.push(TopicSpec::source(&topic, partitions, rf));
            spec.writers
                .push(writer.transactions(&name("txn-writer"), 50, 4).prop("compression.type", "lz4"));
        },
        Stream::Large => {
            spec.topics.push(TopicSpec::source(&topic, partitions, rf));
            spec.writers.push(
                writer
                    .profile(PayloadProfile::sized(128 * 1024, 900 * 1024))
                    .rate(config.large_rate),
            );
        },
        Stream::Copy => {
            let input = name("copy-in");
            spec.topics.push(TopicSpec::source(&input, partitions, rf));
            spec.writers.push(
                WriterSpec::new(stream.writer(), &input)
                    .rate(config.rate_per_writer)
                    .restart(restart),
            );
            spec.topics.push(TopicSpec::mirror(&topic, &input, partitions, rf));
            spec.copiers.push(CopierSpec {
                input,
                output: topic,
                group_id: name("copier"),
                transactional_id: name("copier"),
                abort_one_in: 5,
                // A transaction costs a few round trips to the coordinator,
                // so the copier keeps up with its writer only with large ones.
                props: [("max.poll.records".to_string(), "5000".to_string())].into(),
            });
        },
    }
}

/// Every stream at once for `DURATION_S`, read by one restarting group, on
/// topics named `<prefix>-<run id>-<stream>`.
fn run_spec(config: &LongRunConfig, run_id: u64) -> RunSpec {
    let name = |stream: &str| format!("{}-{run_id:016x}-{stream}", config.topic_prefix);
    let mut spec = RunSpec::new(run_id, config.seed);
    for &stream in &Stream::ALL {
        add_stream(&mut spec, config, &name, stream);
    }
    let topics = spec.topics.iter().map(|t| t.name.clone()).collect();
    // A hard-stopped member holds its partitions for `max.poll.interval.ms`.
    let mut group = GroupSpec::new(&name("readers"), topics, config.consumers)
        .restart(config.consumer_restart, &[RestartMode::Clean, RestartMode::Hard])
        .prop("max.poll.interval.ms", &config.max_poll_interval.as_millis().to_string());
    group.readback_every = Duration::from_secs(5);
    spec.group = Some(group);
    spec.duration = Some(config.duration);
    spec.drain_timeout = config.drain;
    spec.final_timeout = config.drain;
    // A hard-stopped member keeps its partitions for `max.poll.interval.ms`,
    // so a quiet minute is not a hang.
    spec.watchdog = (config.max_poll_interval * 3).max(Duration::from_secs(180));
    spec.status_every = Some(Duration::from_secs(30));
    let restarts_due = u64::from(config.duration > config.consumer_restart * 2);
    spec.requirements = Requirements {
        min_records_per_stream: 100,
        min_txns_committed: 1,
        min_txns_aborted: 1,
        min_clean_restarts: restarts_due,
        min_hard_stops: restarts_due,
        min_producer_restarts: u64::from(config.duration > config.producer_restart),
        min_copier_aborts: 1,
        every_partition_read: true,
    };
    spec
}

/// Writes the run's `report.json` and `verdict.txt` into `dir`.
fn write_report(dir: &Path, verdict: &Verdict, config: &LongRunConfig, run_id: u64, topics: &[String]) {
    std::fs::create_dir_all(dir).expect("create the report directory");
    let mut report = verdict.to_json();
    report["run_id"] = serde_json::json!(format!("{run_id:016x}"));
    report["config"] = config.describe();
    report["topics"] = serde_json::json!(topics);
    std::fs::write(dir.join("report.json"), serde_json::to_string_pretty(&report).expect("json"))
        .expect("write report.json");
    std::fs::write(dir.join("verdict.txt"), verdict.to_string()).expect("write verdict.txt");
}

/// The long run: every stream at once for `DURATION_S`, then the drain, the
/// final read-back and the verdict, written to `REPORT_DIR/<run id>/`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "long-running; start it with `cargo xtask integrity-test`"]
async fn integrity_run() {
    // The client's warnings, or `RUST_LOG`; a local cluster's test context
    // would install a logger too, but an external run has none.
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).try_init();
    let config = LongRunConfig::from_env();
    let run_id: u64 = rand::random();
    // A local cluster lives as long as its context.
    let (local, bootstrap) = if config.bootstrap_servers.is_empty() {
        eprintln!("integrity: BOOTSTRAP_SERVERS is not set, starting a local three-broker cluster");
        let ctx = TestContext::new(local_cluster()).await;
        let bootstrap = ctx.bootstrap_servers().to_string();
        (Some(ctx), bootstrap)
    } else {
        (None, config.bootstrap_servers.clone())
    };
    let base = config.client_props(&bootstrap);
    eprintln!(
        "integrity: {} cluster, {:?}",
        if local.is_some() { "local" } else { "external" },
        config.duration
    );

    let spec = run_spec(&config, run_id);
    let topics: Vec<String> = spec.topics.iter().map(|t| t.name.clone()).collect();
    eprintln!("integrity: topics {topics:?}");
    let verdict = run(spec, base.clone()).await;
    eprintln!("{verdict}");
    let dir = config.report_dir.join(format!("{run_id:016x}"));
    write_report(&dir, &verdict, &config, run_id, &topics);
    eprintln!("integrity: report written to {}", dir.display());

    if !config.keep_topics && local.is_none() {
        let admin = admin(&base);
        if let Err(e) = admin
            .delete_topics_with_topic_collection(TopicCollection::of_topic_names(topics))
            .all()
            .get()
            .await
        {
            eprintln!("integrity: could not delete the run's topics: {e}");
        }
        admin.close().await;
    }
    assert!(verdict.passed(), "the integrity run failed:\n{verdict}");
}
