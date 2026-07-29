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

//! Integration tests for the transaction-domain `KafkaAdminClient` RPCs against a
//! real Kafka 4.2.0 broker: `describeProducers`, `describeTransactions`,
//! `listTransactions`, `fenceProducers`, and `forceTerminateTransaction`
//! (Milestone 11 Tier 3 Phase 6).
//!
//! Scope note (why the plan's scenarios (a)-(c) are not fully realized here):
//! several of the plan's end-to-end scenarios require an **ongoing** transaction
//! produced by a transactional `KafkaProducer` (so that `describe_producers`
//! reports in-flight producer state, `describe_transactions` reports
//! `TransactionState::Ongoing`, and `abort_transaction` moves it to
//! `CompleteAbort`). The Rust `Producer` trait deliberately does **not** yet
//! implement the transactional API (`init_transactions` / `begin_transaction` /
//! `commit_transaction` / `abort_transaction` / `send_offsets_to_transaction` —
//! see `src/producer/producer_trait.rs`), so no ongoing transaction can be
//! created from this client. Those scenarios are therefore deferred to the
//! milestone that lands the transactional producer; each affected test below is
//! marked `#[ignore]` with the reason.
//!
//! What *is* exercised end-to-end (no running producer required), covering every
//! Phase-6 strategy against a real broker:
//! - `list_transactions` — the `AllBrokersStrategy` debut (fan-out to all brokers).
//! - `describe_producers` — `PartitionLeaderStrategy` + `DescribeProducers` wire.
//! - `describe_transactions` on an unknown id — `CoordinatorStrategy(TRANSACTION)`.
//! - `fence_producers` / `force_terminate_transaction` on a fresh id — the
//!   `InitProducerId` path allocates a brand-new producer id/epoch even with no
//!   prior producer, so these complete successfully.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    AbortTransactionSpec, Admin, AdminClientConfig, CreateTopicsOptions, DescribeProducersOptions,
    DescribeTransactionsOptions, FenceProducersOptions, ListTransactionsOptions, NewTopic, TerminateTransactionOptions,
    new_admin_client,
};
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::protocol::Errors;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// A single-broker cluster whose transaction-state log is replicated with a
/// factor of 1, so the transaction coordinator is usable on one node (the
/// defaults require three replicas).
fn txn_single_broker() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR".to_string(), "1".to_string());
    props.insert("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR".to_string(), "1".to_string());
    ClusterConfig::with_properties(props)
}

/// Build an admin client pointed at the cluster's PLAINTEXT listener.
fn admin_for(bootstrap_servers: &str) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "integration-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    let config = AdminClientConfig::from_properties(&props).expect("valid admin config");
    new_admin_client(config).expect("admin client")
}

/// Scenario (d), partial: `list_transactions` fans out to every broker and
/// returns a (here empty) listing collection. This is the first real exercise of
/// `AllBrokersStrategy` against a live broker.
#[tokio::test]
async fn test_list_transactions_returns_empty_when_none_active() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let listings = admin
        .list_transactions(ListTransactionsOptions::new())
        .all()
        .get()
        .await
        .expect("list_transactions should succeed");
    assert!(listings.is_empty(), "no transactions should be active, got {listings:?}");

    // `all_by_broker_id` returns a per-broker map; with one broker it has one key.
    let by_broker = admin
        .list_transactions(ListTransactionsOptions::new())
        .all_by_broker_id()
        .get()
        .await
        .expect("list_transactions all_by_broker_id should succeed");
    assert_eq!(by_broker.len(), 1, "single-broker cluster should report exactly one broker");

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// Scenario (a), partial: `describe_producers` for a freshly-created topic
/// partition round-trips through `PartitionLeaderStrategy` + the
/// `DescribeProducers` wire types and reports no active producers.
#[tokio::test]
async fn test_describe_producers_reports_no_active_producers() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_describe_producers");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");

    let tp = TopicPartition::new(topic.clone(), 0);
    let state = admin
        .describe_producers(std::slice::from_ref(&tp), DescribeProducersOptions::new())
        .partition_result(&tp)
        .expect("partition was requested")
        .get()
        .await
        .expect("describe_producers should succeed");
    assert!(state.active_producers().is_empty(), "a fresh partition has no active producers");

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// Scenario (b), partial: `describe_transactions` for an id that was never used
/// exercises `CoordinatorStrategy(TRANSACTION)` + the `DescribeTransactions` wire
/// types and fails with `TRANSACTIONAL_ID_NOT_FOUND`.
#[tokio::test]
async fn test_describe_transactions_unknown_id_not_found() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let transactional_id = "admin-unknown-txn-id";
    let err = admin
        .describe_transactions(&[transactional_id.to_string()], DescribeTransactionsOptions::new())
        .description(transactional_id)
        .expect("id was requested")
        .get()
        .await
        .expect_err("describing an unknown transactional id should fail");
    assert_eq!(
        err.error(),
        Errors::TransactionalIdNotFound,
        "expected TRANSACTIONAL_ID_NOT_FOUND, got {err:?}"
    );

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// Scenario (e), partial: `fence_producers` for a fresh transactional id
/// exercises `CoordinatorStrategy(TRANSACTION)` + `InitProducerId`, which
/// allocates a brand-new producer id/epoch even with no prior producer.
/// (The follow-on "old producer's send now fails with `ProducerFenced`" half of
/// the scenario needs a transactional producer and is deferred — see the
/// module note.)
#[tokio::test]
async fn test_fence_producers_allocates_producer_id_for_fresh_id() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let transactional_id = "admin-fence-fresh-id";
    let result = admin.fence_producers(&[transactional_id.to_string()], FenceProducersOptions::new());
    result.all().get().await.expect("fence_producers should succeed for a fresh id");
    let producer_id = result
        .producer_id(transactional_id)
        .expect("id requested")
        .get()
        .await
        .expect("producer id");
    assert!(
        producer_id >= 0,
        "coordinator should allocate a valid producer id, got {producer_id}"
    );
    // A newly-initialized producer id starts at epoch 0.
    let epoch = result
        .epoch_id(transactional_id)
        .expect("id requested")
        .get()
        .await
        .expect("epoch");
    assert_eq!(epoch, 0, "a fresh producer id should be fenced at epoch 0");

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// Scenario (f), partial: `force_terminate_transaction` for a fresh id succeeds
/// via the same `InitProducerId` path (it is a thin wrapper over
/// `fence_producers`). The fencing-of-an-active-producer assertion is deferred
/// (needs a transactional producer — see the module note).
#[tokio::test]
async fn test_force_terminate_transaction_fresh_id() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let transactional_id = "admin-force-terminate-fresh-id";
    admin
        .force_terminate_transaction(transactional_id, TerminateTransactionOptions::new())
        .result()
        .get()
        .await
        .expect("force_terminate_transaction should succeed for a fresh id");

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// Scenario (a)+(b)+(c): an uncommitted transaction observed via
/// `describe_producers` / `describe_transactions`, then aborted with
/// `abort_transaction`. Deferred: constructing an ongoing transaction (and a
/// valid `AbortTransactionSpec` with the live producer id / epoch / coordinator
/// epoch) requires the transactional `KafkaProducer` API, which is not yet
/// implemented (see the module note). Kept as a compiling, `#[ignore]`d skeleton
/// so it is trivially re-enabled once the producer lands.
#[tokio::test]
#[ignore = "needs transactional KafkaProducer API (not yet implemented) to create an ongoing transaction"]
async fn test_abort_ongoing_transaction() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_abort_txn");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");

    // Would: start a transactional producer, begin a txn, send an uncommitted
    // record, read the producer id/epoch via describe_producers, build the
    // AbortTransactionSpec, then abort_transaction and re-describe.
    let tp = TopicPartition::new(topic.clone(), 0);
    let spec = AbortTransactionSpec::new(
        tp, /* producer_id */ 0, /* producer_epoch */ 0, /* coordinator_epoch */ 0,
    );
    let _ = admin.abort_transaction(spec, Default::default());

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}
