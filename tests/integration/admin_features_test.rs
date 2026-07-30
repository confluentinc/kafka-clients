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

//! Integration tests for the `KafkaAdminClient` feature RPCs against a real
//! Kafka 4.2.0 broker.
//!
//! Mirrors the describeFeatures / updateFeatures scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine end to
//! end rather than the `MockClient` unit-test harness.
//!
//! Scope note: only the read path (`describe_features`) and a
//! validate-only/rejected `update_features` are exercised. A *successful*
//! feature upgrade is deliberately omitted — finalized feature levels
//! (`metadata.version`, ...) are cluster-wide, persistent, and not trivially
//! reversible, so mutating them on a shared test cluster is unsafe.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, DescribeFeaturesOptions, FeatureUpdate, UpdateFeaturesOptions, UpgradeType,
    new_admin_client,
};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// The canonical finalized feature present on every KRaft cluster.
const METADATA_VERSION_FEATURE: &str = "metadata.version";

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

/// (a) `describe_features` reports a sane `metadata.version` range.
#[tokio::test]
async fn test_describe_features_reports_metadata_version() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let metadata = admin
        .describe_features(DescribeFeaturesOptions::new())
        .feature_metadata()
        .get()
        .await
        .expect("describe features");

    assert!(
        !metadata.supported_features().is_empty(),
        "cluster should advertise at least one supported feature"
    );

    // Every KRaft cluster advertises the metadata.version feature.
    let supported = metadata
        .supported_features()
        .get(METADATA_VERSION_FEATURE)
        .expect("metadata.version should be a supported feature");
    assert!(
        supported.min_version() <= supported.max_version(),
        "supported range [{}, {}] should be non-empty",
        supported.min_version(),
        supported.max_version()
    );

    // The finalized metadata.version should sit within the supported range.
    if let Some(finalized) = metadata.finalized_features().get(METADATA_VERSION_FEATURE) {
        assert!(
            finalized.min_version_level() >= 1,
            "finalized metadata.version min level should be >= 1"
        );
        assert!(
            finalized.max_version_level() <= supported.max_version(),
            "finalized metadata.version {} should not exceed the supported max {}",
            finalized.max_version_level(),
            supported.max_version()
        );
    }

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// (b) `update_features` above the maximum supported level is rejected. Uses
/// `validate_only` so the cluster is never actually mutated.
#[tokio::test]
async fn test_update_features_above_max_is_rejected() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    // A level far beyond any real supported maximum.
    let updates = HashMap::from([(
        METADATA_VERSION_FEATURE.to_string(),
        FeatureUpdate::new(9999, UpgradeType::Upgrade).expect("valid feature update"),
    )]);
    let result = admin
        .update_features(&updates, UpdateFeaturesOptions::new().validate_only(true))
        .expect("update_features enqueues the call");

    let outcome = result.values()[METADATA_VERSION_FEATURE].get().await;
    assert!(
        outcome.is_err(),
        "upgrading metadata.version beyond its supported max should be rejected"
    );

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}
