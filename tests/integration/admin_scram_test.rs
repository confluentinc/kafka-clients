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

//! Integration tests for the `KafkaAdminClient` SASL/SCRAM credential RPCs
//! against a real Kafka 4.2.0 broker.
//!
//! Mirrors the describeUserScramCredentials / alterUserScramCredentials
//! scenarios in Java's `KafkaAdminClientIntegrationTest`, exercising the real
//! network engine and — crucially — the real PBKDF2 salting path end to end
//! rather than the `MockClient` unit-test harness.
//!
//! Isolation: each test scopes its assertions to a unique username derived from
//! the per-test `TestContext` prefix, so it is safe on the shared pooled
//! broker. It never asserts global emptiness or credential counts.
//!
//! Scope note (SASL-auth step deferred): the Java integration plan's optional
//! step of actually SASL/SCRAM-*authenticating* a client with the created
//! credential is NOT translated. The admin client on this branch has no SASL
//! client support — `AdminClientConfig` exposes no `security.protocol`/`sasl.*`
//! keys and `KafkaAdminClient::from_config` hardcodes a PLAINTEXT channel
//! builder (same gap that scoped down the Phase 4 delegation-token
//! integration). The upsert -> describe -> delete round-trip below is the real
//! correctness signal that the PBKDF2 salted password is well-formed: the
//! broker validates and stores the credential, and describe round-trips its
//! mechanism + iterations. A vacuous SASL test is intentionally not written.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, AlterUserScramCredentialsOptions, DescribeUserScramCredentialsOptions,
    ScramCredentialInfo, ScramMechanism, UserScramCredentialAlteration, UserScramCredentialDeletion,
    UserScramCredentialUpsertion, new_admin_client,
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

#[tokio::test]
async fn test_upsert_describe_delete_scram_credential_round_trips() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    // A unique, self-scoped username (per-test prefix) so assertions never
    // touch other tests' credentials on the shared broker.
    let user = ctx.group_id("scram_roundtrip_user");
    let mechanism = ScramMechanism::ScramSha256;
    let iterations = 8192;

    // 1. Upsert a SCRAM-SHA-256 credential. This runs the real PBKDF2 salting
    //    path; the broker rejects a malformed salted password.
    let upsertion =
        UserScramCredentialUpsertion::new(&user, ScramCredentialInfo::new(mechanism, iterations), "password");
    let alteration: UserScramCredentialAlteration = upsertion.into();
    admin
        .alter_user_scram_credentials(&[alteration], AlterUserScramCredentialsOptions::new())
        .all()
        .get()
        .await
        .expect("upsert SCRAM-SHA-256 credential");

    // 2. Describe (only our user) and assert the mechanism + iterations round
    //    trip. The salted password is NEVER returned by the broker, so it is
    //    not (and cannot be) asserted.
    let described = admin
        .describe_user_scram_credentials(std::slice::from_ref(&user), DescribeUserScramCredentialsOptions::new())
        .all()
        .get()
        .await
        .expect("describe SCRAM credentials");

    let description = described.get(&user).expect("our user should have a credential");
    assert_eq!(description.name(), user);
    assert_eq!(
        description.credential_infos().len(),
        1,
        "expected exactly one credential: {description:?}"
    );
    assert_eq!(description.credential_infos()[0].mechanism(), mechanism);
    assert_eq!(description.credential_infos()[0].iterations(), iterations);

    // 3. Delete the credential.
    let deletion: UserScramCredentialAlteration = UserScramCredentialDeletion::new(&user, mechanism).into();
    admin
        .alter_user_scram_credentials(&[deletion], AlterUserScramCredentialsOptions::new())
        .all()
        .get()
        .await
        .expect("delete SCRAM credential");

    // 4. Describe again: our user no longer has any credential. `users()`
    //    filters out RESOURCE_NOT_FOUND, so the described-users list for our
    //    single requested user is now empty.
    let users_after = admin
        .describe_user_scram_credentials(std::slice::from_ref(&user), DescribeUserScramCredentialsOptions::new())
        .users()
        .get()
        .await
        .expect("describe after delete");
    assert!(
        !users_after.contains(&user),
        "credential should be gone after delete, but user still listed: {users_after:?}"
    );

    admin.close(Duration::from_secs(5)).await;
}
