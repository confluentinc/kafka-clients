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

//! Integration tests for the `KafkaAdminClient` client-quota RPCs against a
//! real Kafka 4.2.0 broker.
//!
//! Mirrors the describeClientQuotas / alterClientQuotas scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine end to
//! end rather than the `MockClient` unit-test harness.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, AlterClientQuotasOptions, DescribeClientQuotasOptions, new_admin_client,
};
use confluent_kafka::common::quota::client_quota_entity::CLIENT_ID;
use confluent_kafka::common::quota::{
    ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent, Op,
};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

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

/// Constructs a client-id quota entity.
fn client_id_entity(name: &str) -> ClientQuotaEntity {
    ClientQuotaEntity::new(HashMap::from([(CLIENT_ID.to_string(), Some(name.to_string()))]))
}

#[tokio::test]
async fn test_alter_then_describe_round_trips_a_byte_rate_quota() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let entity = client_id_entity("quota-client-roundtrip");
    let alteration = ClientQuotaAlteration::new(entity.clone(), vec![Op::new("consumer_byte_rate", Some(1_048_576.0))]);
    admin
        .alter_client_quotas(&[alteration], AlterClientQuotasOptions::new())
        .all()
        .get()
        .await
        .expect("alter client quota");

    let filter = ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity(
        CLIENT_ID,
        "quota-client-roundtrip",
    )]);
    let described = admin
        .describe_client_quotas(&filter, DescribeClientQuotasOptions::new())
        .entities()
        .get()
        .await
        .expect("describe client quotas");

    let values = described.get(&entity).expect("quota entity should be reported");
    assert!(
        (values.get("consumer_byte_rate").copied().unwrap_or_default() - 1_048_576.0).abs() < 1e-6,
        "consumer_byte_rate should round-trip: {values:?}"
    );

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_remove_quota_is_no_longer_reported() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let entity = client_id_entity("quota-client-remove");

    // First set a quota.
    admin
        .alter_client_quotas(
            &[ClientQuotaAlteration::new(
                entity.clone(),
                vec![Op::new("producer_byte_rate", Some(2_097_152.0))],
            )],
            AlterClientQuotasOptions::new(),
        )
        .all()
        .get()
        .await
        .expect("set producer_byte_rate");

    // Then remove it via an `Op` with a `None` value (Java `null`).
    admin
        .alter_client_quotas(
            &[ClientQuotaAlteration::new(
                entity.clone(),
                vec![Op::new("producer_byte_rate", None)],
            )],
            AlterClientQuotasOptions::new(),
        )
        .all()
        .get()
        .await
        .expect("remove producer_byte_rate");

    let filter =
        ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity(CLIENT_ID, "quota-client-remove")]);
    let described = admin
        .describe_client_quotas(&filter, DescribeClientQuotasOptions::new())
        .entities()
        .get()
        .await
        .expect("describe client quotas");

    // The producer_byte_rate must no longer be reported for the entity.
    let has_producer_rate = described
        .get(&entity)
        .map(|values| values.contains_key("producer_byte_rate"))
        .unwrap_or(false);
    assert!(!has_producer_rate, "removed quota should not be reported: {described:?}");

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_entity_type_filter_returns_only_matching_entities() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let entity = client_id_entity("quota-client-typed");
    admin
        .alter_client_quotas(
            &[ClientQuotaAlteration::new(
                entity.clone(),
                vec![Op::new("consumer_byte_rate", Some(4_194_304.0))],
            )],
            AlterClientQuotasOptions::new(),
        )
        .all()
        .get()
        .await
        .expect("set consumer_byte_rate");

    // A filter on the CLIENT_ID entity type must only return client-id entities.
    let filter = ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity_type(CLIENT_ID)]);
    let described = admin
        .describe_client_quotas(&filter, DescribeClientQuotasOptions::new())
        .entities()
        .get()
        .await
        .expect("describe client quotas");

    assert!(
        described.contains_key(&entity),
        "the configured client-id entity should be reported: {described:?}"
    );
    for reported in described.keys() {
        assert!(
            reported.entries().contains_key(CLIENT_ID),
            "every reported entity must have a client-id component: {reported}"
        );
    }

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}
