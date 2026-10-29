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

//! Runs the admin examples (`examples/admin`) against a broker, as a user would:
//! each example is a separate binary given its command line, and the test checks
//! its exit code and output.
//!
//! The binaries are built by `cargo build --examples`, which `cargo test
//! --all-features` does too; `cargo test --test integration` does not, so the
//! Makefile builds them first. Each scenario uses the cluster its RPCs need: an
//! authorizer for the ACLs, a transaction coordinator for the transactions,
//! three brokers for reassignments and elections, two log directories for a
//! replica move, and SASL with a token secret for the delegation tokens.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::producer::{
    KafkaProducer, Producer, ProducerConfig, ProducerRecord, ProducerRecordOptionsBuilder,
};

use crate::common::cluster_config::{ClusterConfig, authorizer_single_broker, two_log_dir_cluster, txn_single_broker};
use crate::common::kafka_cluster::{SASL_PASSWORD, SASL_USERNAME};
use crate::common::test_context::{TestContext, plain_jaas_config};

/// What an example printed and how it exited.
struct Output {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Output {
    /// Asserts the example succeeded and returns its standard output.
    fn ok(&self) -> &str {
        assert_eq!(
            self.code, 0,
            "example failed\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        );
        &self.stdout
    }

    /// Asserts the example exited with `code` and returns its standard error.
    fn failed(&self, code: i32) -> &str {
        assert_eq!(
            self.code, code,
            "unexpected exit code\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        );
        &self.stderr
    }
}

/// The path of the example binary `admin_<name>`, next to the test binary's
/// `deps` directory.
fn example_path(name: &str) -> PathBuf {
    let exe = std::env::current_exe().expect("test binary path");
    let target = exe.parent().and_then(|deps| deps.parent()).expect("target directory");
    let path = target
        .join("examples")
        .join(format!("admin_{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(
        path.exists(),
        "{} not found: run `cargo build --examples` first",
        path.display()
    );
    path
}

/// Runs the example `admin_<name>` with the client configuration `config`
/// (passed as `-X key=value`) and `args`.
async fn run(name: &str, config: &HashMap<String, String>, args: &[&str]) -> Output {
    let mut command = Command::new(example_path(name));
    for (key, value) in config {
        command.arg("-X").arg(format!("{key}={value}"));
    }
    command.args(args);
    let output = tokio::task::spawn_blocking(move || command.output())
        .await
        .expect("example task")
        .expect("example started");
    Output {
        code: output.status.code().expect("example exited normally"),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// Runs the example until its output contains `expected`, for changes the
/// broker applies asynchronously; returns the last output.
async fn run_until(name: &str, config: &HashMap<String, String>, args: &[&str], expected: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let output = run(name, config, args).await;
        let stdout = output.ok();
        if stdout.contains(expected) {
            return stdout.to_string();
        }
        assert!(
            Instant::now() < deadline,
            "{expected:?} not in the output of admin_{name}:\n{stdout}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The client configuration of the run's protocol (bootstrap and security).
fn client_config(ctx: &TestContext) -> HashMap<String, String> {
    let mut config = HashMap::new();
    ctx.configure(&mut config);
    config
}

/// The text between `prefix` and the next `end` (or the end of the line) on the
/// first line of `text` containing `prefix`.
fn between<'a>(text: &'a str, prefix: &str, end: char) -> &'a str {
    let line = text
        .lines()
        .find(|line| line.contains(prefix))
        .unwrap_or_else(|| panic!("{prefix:?} not in:\n{text}"));
    let rest = &line[line.find(prefix).unwrap() + prefix.len()..];
    rest.split(end).next().unwrap()
}

/// The comma-terminated field that follows `prefix`, as in `producer id 7, ...`.
fn field<'a>(text: &'a str, prefix: &str) -> &'a str {
    between(text, prefix, ',')
}

async fn produce(ctx: &TestContext, topic: &str, partition: i32, count: usize) {
    let mut props = client_config(ctx);
    props.insert("linger.ms".to_string(), "0".to_string());
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> = KafkaProducer::new(
        ProducerConfig::new(&props).expect("producer config"),
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("producer");
    for index in 0..count {
        let record = ProducerRecord::with_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(topic.to_string())
                .set_value(Some(format!("value-{index}").into_bytes()))
                .set_partition(Some(partition))
                .build()
                .unwrap(),
        )
        .unwrap();
        Producer::send(&producer, record)
            .await
            .expect("send accepted")
            .get_with_timeout(Duration::from_secs(30))
            .await
            .expect("record acked");
    }
    producer.close().await.expect("producer closed");
}

#[tokio::test]
async fn usage_error_exits_with_code_2() {
    // A usage error is reported before any request is sent, so no broker is needed.
    let config = HashMap::from([("bootstrap.servers".to_string(), "localhost:1".to_string())]);
    let stderr = run("create_topics", &config, &[]).await.failed(2).to_string();
    assert!(stderr.contains("no topic given"), "{stderr}");
    assert!(stderr.contains("usage: admin_create_topics"), "{stderr}");

    let stderr = run("list_topics", &config, &["--bogus"]).await.failed(2).to_string();
    assert!(stderr.contains("unknown option --bogus"), "{stderr}");

    let output = run("list_topics", &config, &["--help"]).await;
    assert!(output.ok().starts_with("usage: admin_list_topics"), "{}", output.stdout);

    // An argument that may be a secret is never repeated in a usage error.
    let stderr = run("list_topics", &config, &["-X", "hunter2"]).await.failed(2).to_string();
    assert!(
        stderr.contains("-X needs a key=value argument") && !stderr.contains("hunter2"),
        "{stderr}"
    );
    let stderr = run(
        "alter_user_scram_credentials",
        &config,
        &["UPSERT", "alice", "hunter2", "8192", "secret"],
    )
    .await
    .failed(2)
    .to_string();
    assert!(stderr.contains("invalid mechanism") && !stderr.contains("hunter2"), "{stderr}");
}

#[tokio::test]
async fn topic_and_record_examples() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let config = client_config(&ctx);
    let topic = ctx.topic("examples");

    let stdout = run("create_topics", &config, &["--validate-only", &format!("{topic}:2:1")]).await;
    assert!(stdout.ok().contains(&format!("Topic {topic} validated: ")), "{}", stdout.stdout);
    let stdout = run("create_topics", &config, &[&format!("{topic}:2:1")]).await;
    assert!(
        stdout.ok().contains(&format!("Topic {topic} created: ")) && stdout.stdout.contains("2 partitions"),
        "{}",
        stdout.stdout
    );
    // Creating it again fails for that topic only, with exit code 1.
    let stderr = run("create_topics", &config, &[&topic]).await.failed(1).to_string();
    assert!(stderr.contains(&format!("Failed to create topic {topic}")), "{stderr}");

    run_until("list_topics", &config, &[], &format!("Topic {topic} (id ")).await;
    let stdout = run("describe_topics", &config, &[&topic]).await;
    assert!(stdout.ok().contains("  partition 1: leader "), "{}", stdout.stdout);

    let stdout = run("create_partitions", &config, &[&topic, "3"]).await;
    assert!(
        stdout.ok().contains(&format!("Topic {topic} now has 3 partitions")),
        "{}",
        stdout.stdout
    );

    produce(&ctx, &topic, 0, 10).await;
    let stdout = run("list_offsets", &config, &["read_uncommitted", &topic, "0", "latest"]).await;
    assert!(stdout.ok().contains(&format!("{topic}-0: offset 10,")), "{}", stdout.stdout);
    let stdout = run("delete_records", &config, &[&topic, "0", "4"]).await;
    assert!(
        stdout
            .ok()
            .contains(&format!("Deleted records of partition {topic}-0: low watermark 4")),
        "{}",
        stdout.stdout
    );
    let stdout = run("list_offsets", &config, &["read_committed", &topic, "0", "earliest"]).await;
    assert!(stdout.ok().contains(&format!("{topic}-0: offset 4,")), "{}", stdout.stdout);

    let stdout = run(
        "incremental_alter_configs",
        &config,
        &["topic", &topic, "retention.ms=SET:123456"],
    )
    .await;
    assert!(
        stdout.ok().contains(&format!("Altered configs of topic {topic}")),
        "{}",
        stdout.stdout
    );
    let stdout = run_until("describe_configs", &config, &["topic", &topic], "retention.ms = 123456").await;
    assert!(stdout.contains(&format!("Configs of topic {topic}:")), "{stdout}");

    // The single broker of a combined-mode cluster is node 1.
    let stdout = run("describe_log_dirs", &config, &["1"]).await;
    assert!(
        stdout.ok().contains(&format!("    replica {topic}-0: size ")),
        "{}",
        stdout.stdout
    );
    let stdout = run("describe_replica_log_dirs", &config, &[&topic, "0", "1"]).await;
    assert!(
        stdout.ok().contains(&format!("Replica {topic}-0-1: log dir /")),
        "{}",
        stdout.stdout
    );

    let stdout = run("delete_topics", &config, &[&topic]).await;
    assert!(stdout.ok().contains(&format!("Topic {topic} deleted")), "{}", stdout.stdout);
}

#[tokio::test]
async fn cluster_config_and_feature_examples() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let config = client_config(&ctx);

    let stdout = run("describe_cluster", &config, &["--authorized-operations"]).await;
    let stdout = stdout.ok();
    assert!(stdout.contains("Cluster id: "), "{stdout}");
    assert!(stdout.contains("Broker 1 at "), "{stdout}");

    // A client metrics resource exists once a configuration is set on it.
    let name = ctx.group_id("metrics");
    let stdout = run(
        "incremental_alter_configs",
        &config,
        &["client_metrics", &name, "metrics=SET:org.apache.kafka.producer."],
    )
    .await;
    assert!(
        stdout.ok().contains(&format!("Altered configs of client_metrics {name}")),
        "{}",
        stdout.stdout
    );
    run_until(
        "list_config_resources",
        &config,
        &["client_metrics"],
        &format!("client_metrics {name}"),
    )
    .await;
    let stdout = run(
        "incremental_alter_configs",
        &config,
        &["client_metrics", &name, "metrics=DELETE"],
    )
    .await;
    stdout.ok();

    let stdout = run("describe_features", &config, &[]).await;
    let metadata_version = between(stdout.ok(), "Finalized feature metadata.version: ", '\n');
    let max_level = between(metadata_version, "max level ", '\n').to_string();
    let supported_max: i32 = between(stdout.ok(), "Supported feature metadata.version: ", '\n')
        .rsplit("max version ")
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let stdout = run(
        "update_features",
        &config,
        &["--validate-only", "metadata.version", &max_level, "upgrade"],
    )
    .await;
    assert!(
        stdout
            .ok()
            .contains(&format!("Feature metadata.version validated: max level {max_level}")),
        "{}",
        stdout.stdout
    );

    // No controller has this id. metadata.version 33 is the first to support
    // controller unregistration: a broker that cannot reach it does not offer the
    // RPC, and one finalized below it refuses the request.
    const UNREGISTER_CONTROLLER_LEVEL: i32 = 33;
    let finalized_max: i32 = max_level.parse().unwrap();
    let expected = if supported_max < UNREGISTER_CONTROLLER_LEVEL {
        "The node does not support UNREGISTER_CONTROLLER".to_string()
    } else if finalized_max < UNREGISTER_CONTROLLER_LEVEL {
        "The current MetadataVersion is too old to support controller unregistration.".to_string()
    } else {
        "Controller ID 987654 is not currently registered.".to_string()
    };
    let stderr = run("unregister_controller", &config, &["987654"]).await.failed(1).to_string();
    assert!(stderr.starts_with("Error: ") && stderr.contains(&expected), "{stderr}");
}

#[tokio::test]
async fn consumer_group_examples() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let config = client_config(&ctx);
    let topic = ctx.topic("group_examples");
    let group = ctx.group_id("group_examples");
    run("create_topics", &config, &[&format!("{topic}:1:1")]).await.ok();

    // Committing an offset creates the group, empty and of the classic type.
    let stdout = run("alter_consumer_group_offsets", &config, &[&group, &topic, "0", "5"]).await;
    assert!(
        stdout.ok().contains(&format!("Group {group}: {topic}-0 offset set to 5")),
        "{}",
        stdout.stdout
    );
    let stdout = run("list_consumer_group_offsets", &config, &[&group]).await;
    let stdout = stdout.ok();
    assert!(stdout.contains(&format!("Group {group}: 1 partitions")), "{stdout}");
    assert!(
        stdout.contains(&format!("  {topic}-0: offset 5, leader epoch none, metadata \"\"")),
        "{stdout}"
    );

    run_until("list_groups", &config, &[], &format!("Group {group}: ")).await;
    let stdout = run("describe_consumer_groups", &config, &[&group]).await;
    assert!(stdout.ok().contains(&format!("Group {group}: type ")), "{}", stdout.stdout);
    let stdout = run("describe_classic_groups", &config, &[&group]).await;
    assert!(stdout.ok().contains(&format!("Group {group}: state ")), "{}", stdout.stdout);

    // The group has no member with this instance id.
    let stderr = run("remove_members_from_consumer_group", &config, &[&group, "no-such-instance"])
        .await
        .failed(1)
        .to_string();
    assert!(
        stderr.contains(&format!("Failed to remove member no-such-instance from group {group}")),
        "{stderr}"
    );

    let stdout = run("delete_consumer_group_offsets", &config, &[&group, &topic, "0"]).await;
    assert!(
        stdout.ok().contains(&format!("Group {group}: {topic}-0 offset deleted")),
        "{}",
        stdout.stdout
    );
    let stdout = run("delete_consumer_groups", &config, &[&group]).await;
    assert!(stdout.ok().contains(&format!("Group {group} deleted")), "{}", stdout.stdout);
}

#[tokio::test]
async fn quota_and_scram_examples() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let config = client_config(&ctx);
    let user = ctx.group_id("quota_user");

    let entity = format!("user={user}");
    let stdout = run("alter_client_quotas", &config, &[&entity, "producer_byte_rate=1024"]).await;
    assert!(
        stdout.ok().contains(&format!("Client quotas of {entity} altered")),
        "{}",
        stdout.stdout
    );
    run_until(
        "describe_client_quotas",
        &config,
        &["user", &user],
        &format!("{entity}: producer_byte_rate=1024"),
    )
    .await;
    run("alter_client_quotas", &config, &[&entity, "producer_byte_rate=REMOVE"])
        .await
        .ok();
    run_until("describe_client_quotas", &config, &["user", &user], "0 entities match").await;

    let stdout = run(
        "alter_user_scram_credentials",
        &config,
        &["UPSERT", &user, "SCRAM-SHA-256", "8192", "secret"],
    )
    .await;
    assert!(
        stdout.ok().contains(&format!("Altered SCRAM credentials of user {user}")),
        "{}",
        stdout.stdout
    );
    run_until(
        "describe_user_scram_credentials",
        &config,
        &[&user],
        &format!("User {user}: SCRAM-SHA-256 iterations=8192"),
    )
    .await;
    let stdout = run("alter_user_scram_credentials", &config, &["DELETE", &user, "SCRAM-SHA-256"]).await;
    stdout.ok();
}

#[tokio::test]
async fn transaction_examples() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    let config = client_config(&ctx);
    let topic = ctx.topic("txn_examples");
    let transactional_id = ctx.group_id("txn_examples");
    run("create_topics", &config, &[&format!("{topic}:1:1")]).await.ok();

    // A transaction left open, as a hanging transaction would be.
    let mut props = client_config(&ctx);
    props.insert("transactional.id".to_string(), transactional_id.clone());
    props.insert("linger.ms".to_string(), "0".to_string());
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> = KafkaProducer::new(
        ProducerConfig::new(&props).expect("producer config"),
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("producer");
    producer.init_transactions().await.expect("init_transactions");
    producer.begin_transaction().expect("begin_transaction");
    let record = ProducerRecord::with_options(
        ProducerRecordOptionsBuilder::new()
            .set_topic(topic.clone())
            .set_value(Some(b"open".to_vec()))
            .set_partition(Some(0))
            .build()
            .unwrap(),
    )
    .unwrap();
    Producer::send(&producer, record)
        .await
        .expect("send accepted")
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("record acked");

    let stdout = run_until(
        "list_transactions",
        &config,
        &["--states=ongoing"],
        &format!("Transaction {transactional_id}: "),
    )
    .await;
    assert!(
        stdout.contains(&format!("Transaction {transactional_id}: producer id ")),
        "{stdout}"
    );
    let stdout = run("describe_transactions", &config, &[&transactional_id]).await;
    let stdout = stdout.ok();
    assert!(
        stdout.contains(&format!("Transaction {transactional_id}: state Ongoing")),
        "{stdout}"
    );
    assert!(stdout.contains(&format!("  partition {topic}-0")), "{stdout}");

    let stdout = run("describe_producers", &config, &[&topic, "0"]).await;
    let stdout = stdout.ok().to_string();
    assert!(stdout.contains(&format!("Partition {topic}-0: 1 active producers")), "{stdout}");
    let producer_id = field(&stdout, "producer id ");
    let producer_epoch = field(&stdout, "producer epoch ");
    let coordinator_epoch = field(&stdout, "coordinator epoch ");

    let stdout = run(
        "abort_transaction",
        &config,
        &[&topic, "0", producer_id, producer_epoch, coordinator_epoch],
    )
    .await;
    assert!(
        stdout.ok().contains(&format!(
            "Aborted transaction of producer id {producer_id}, producer epoch {producer_epoch} on {topic}-0"
        )),
        "{}",
        stdout.stdout
    );

    let stdout = run("fence_producers", &config, &[&transactional_id]).await;
    assert!(
        stdout
            .ok()
            .contains(&format!("Fenced {transactional_id}: producer id {producer_id}, epoch ")),
        "{}",
        stdout.stdout
    );
    let stdout = run("force_terminate_transaction", &config, &[&transactional_id]).await;
    assert!(
        stdout.ok().contains(&format!("Transaction {transactional_id} terminated")),
        "{}",
        stdout.stdout
    );

    // The producer was fenced: closing it may report that.
    let _ = producer.close().await;
}

#[tokio::test]
async fn acl_examples() {
    let mut ctx = TestContext::new(authorizer_single_broker()).await;
    let config = client_config(&ctx);
    let topic = ctx.topic("acl_examples");
    let binding = [topic.as_str(), "LITERAL", "User:alice", "*", "READ", "ALLOW"];
    let args: Vec<&str> = std::iter::once("TOPIC").chain(binding).collect();
    let text = format!("TOPIC {topic} LITERAL User:alice * READ ALLOW");

    let stdout = run("create_acls", &config, &args).await;
    assert!(stdout.ok().contains(&format!("Created ACL {text}")), "{}", stdout.stdout);
    let stdout = run_until("describe_acls", &config, &args, &format!("ACL {text}")).await;
    assert!(stdout.contains("1 ACLs match"), "{stdout}");
    let stdout = run("delete_acls", &config, &args).await;
    let stdout = stdout.ok();
    assert!(stdout.contains(": 1 matching ACLs"), "{stdout}");
    assert!(stdout.contains(&format!("  deleted ACL {text}")), "{stdout}");
    run_until("describe_acls", &config, &args, "0 ACLs match").await;
}

#[tokio::test]
async fn reassignment_and_election_examples() {
    let mut ctx = TestContext::new(ClusterConfig::with_brokers(3)).await;
    let config = client_config(&ctx);
    let topic = ctx.topic("reassign_examples");
    run("create_topics", &config, &[&format!("{topic}:1:3")]).await.ok();

    let stdout = run("describe_topics", &config, &[&topic]).await;
    let leader = field(stdout.ok(), "  partition 0: leader ").to_string();
    let replicas = between(&stdout.stdout, "replicas [", ']')
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    // The same replicas, with the current leader last: the preferred leader changes.
    let mut target: Vec<String> = replicas.iter().filter(|id| **id != leader).cloned().collect();
    target.push(leader.clone());
    let target = target.join(",");

    let stdout = run("alter_partition_reassignments", &config, &[&topic, "0", &target]).await;
    assert!(
        stdout
            .ok()
            .contains(&format!("Reassignment of partition {topic}-0 to [{target}] started")),
        "{}",
        stdout.stdout
    );
    run_until(
        "list_partition_reassignments",
        &config,
        &[&topic, "0"],
        "No ongoing reassignments",
    )
    .await;
    run_until("describe_topics", &config, &[&topic], &format!("replicas [{target}]")).await;
    // Nothing is left to cancel.
    let stderr = run("alter_partition_reassignments", &config, &[&topic, "0", "cancel"])
        .await
        .failed(1)
        .to_string();
    assert!(
        stderr.contains(&format!("Failed to cancel the reassignment of partition {topic}-0")),
        "{stderr}"
    );

    // The broker's automatic leader rebalance may elect the preferred leader
    // first, and the election is then not needed; either way it leads below.
    let output = run("elect_leaders", &config, &["preferred", &topic, "0"]).await;
    if output.code == 0 {
        assert!(
            output.stdout.contains(&format!("Leader elected for {topic}-0")),
            "{}",
            output.stdout
        );
    } else {
        let stderr = output.failed(1);
        assert!(
            stderr.contains(&format!("Failed to elect leader for {topic}-0: ElectionNotNeededError")),
            "{stderr}"
        );
    }
    let preferred = target.split(',').next().unwrap();
    run_until(
        "describe_topics",
        &config,
        &[&topic],
        &format!("  partition 0: leader {preferred},"),
    )
    .await;
}

#[tokio::test]
async fn replica_log_dir_examples() {
    let mut ctx = TestContext::new(two_log_dir_cluster()).await;
    let config = client_config(&ctx);
    let topic = ctx.topic("log_dir_examples");
    run("create_topics", &config, &[&format!("{topic}:1:1")]).await.ok();

    let stdout = run("describe_replica_log_dirs", &config, &[&topic, "0", "1"]).await;
    let current = field(stdout.ok(), &format!("Replica {topic}-0-1: log dir ")).to_string();
    let other = if current == "/tmp/kafka-logs-0" {
        "/tmp/kafka-logs-1"
    } else {
        "/tmp/kafka-logs-0"
    };

    let stdout = run("alter_replica_log_dirs", &config, &[&topic, "0", "1", other]).await;
    assert!(
        stdout.ok().contains(&format!("Replica {topic}-0-1 moving to {other}")),
        "{}",
        stdout.stdout
    );
    run_until(
        "describe_replica_log_dirs",
        &config,
        &[&topic, "0", "1"],
        &format!("Replica {topic}-0-1: log dir {other}, offset lag 0\n"),
    )
    .await;
}

/// A broker that can mint delegation tokens: the token secret is set, and the
/// client authenticates over SASL, since the broker refuses token requests on
/// PLAINTEXT and one-way SSL connections.
fn delegation_token_cluster() -> ClusterConfig {
    ClusterConfig::with_properties(BTreeMap::from([(
        "KAFKA_DELEGATION_TOKEN_SECRET_KEY".to_string(),
        "admin-examples-token-secret".to_string(),
    )]))
}

#[tokio::test]
async fn delegation_token_examples() {
    let ctx = TestContext::new(delegation_token_cluster()).await;
    let config = HashMap::from([
        (
            "bootstrap.servers".to_string(),
            ctx.sasl_plaintext_bootstrap_servers().to_string(),
        ),
        ("security.protocol".to_string(), "SASL_PLAINTEXT".to_string()),
        ("sasl.mechanism".to_string(), "PLAIN".to_string()),
        ("sasl.jaas.config".to_string(), plain_jaas_config(SASL_USERNAME, SASL_PASSWORD)),
    ]);

    let stdout = run("create_delegation_token", &config, &["--renewer=User:renewer"]).await;
    let stdout = stdout.ok().to_string();
    let token_id = between(&stdout, "Created delegation token ", '\n').to_string();
    let hmac = between(&stdout, "HMAC (hex): ", '\n').to_string();
    assert!(stdout.contains(&format!("Owner: User:{SASL_USERNAME}")), "{stdout}");
    assert!(stdout.contains("Renewers: User:renewer"), "{stdout}");

    // The HMAC is the token's password: describing shows it only when asked.
    let stdout = run_until("describe_delegation_token", &config, &[], &format!("Token {token_id}: ")).await;
    assert!(!stdout.contains(&hmac), "{stdout}");
    let stdout = run("describe_delegation_token", &config, &["--show-hmac"]).await;
    assert!(stdout.ok().contains(&format!("  HMAC (hex): {hmac}")), "{}", stdout.stdout);
    // A malformed HMAC is not repeated back.
    let stderr = run("renew_delegation_token", &config, &["not-hex-secret"])
        .await
        .failed(2)
        .to_string();
    assert!(!stderr.contains("not-hex-secret"), "{stderr}");
    let stdout = run("describe_delegation_token", &config, &["User:nobody"]).await;
    assert!(stdout.ok().contains("No delegation tokens"), "{}", stdout.stdout);

    // Only the owner and the renewers may renew; the owner is the caller here.
    let stdout = run("renew_delegation_token", &config, &["--period-ms=600000", &hmac]).await;
    assert!(
        stdout.ok().contains("Delegation token renewed, expiry timestamp: "),
        "{}",
        stdout.stdout
    );
    let stdout = run("expire_delegation_token", &config, &["--period-ms=-1", &hmac]).await;
    assert!(
        stdout.ok().contains("Delegation token expired, expiry timestamp: "),
        "{}",
        stdout.stdout
    );
    run_until("describe_delegation_token", &config, &[], "No delegation tokens").await;
}
