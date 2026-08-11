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

//! Integration tests for the admin SASL/SCRAM credential RPCs against a real
//! Kafka 4.2.0 broker.
//!
//! Mirrors the describeUserScramCredentials / alterUserScramCredentials scenarios
//! in Java's `KafkaAdminClientIntegrationTest`, exercising the real network engine
//! and — crucially — the real PBKDF2 salting path end to end rather than the
//! `MockClient` unit-test harness.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and the
//! C FFI. That makes the salting path itself comparable: the salt is carried on
//! the wire, so all four backends derive the salted password from the *same* salt
//! and the same password, and a broker that accepted one but not another would be
//! a finding. (`MockAdminClient` is no help here — Java's mock throws
//! `UnsupportedOperationException` for both SCRAM RPCs,
//! `MockAdminClient.java:1251-1259`, which the Rust mock mirrors.)
//!
//! Isolation: the scenario scopes its assertions to a unique username derived
//! from the per-test `TestContext` prefix, so it is safe on the shared pooled
//! broker and never asserts global emptiness or credential counts.
//!
//! # Two things the broker will not return, and one it cannot
//!
//! The **salted password and the salt are write-only**: the broker returns only
//! the mechanism and the iteration count, so those two are what the round-trip
//! asserts, and they are the strongest available signal that the client's PBKDF2
//! output was well-formed enough to be stored.
//!
//! Actually SASL/SCRAM-*authenticating* with the created credential is not
//! translated. The admin client on this branch cannot authenticate at all:
//! `AdminClientConfig` recognises no `security.protocol` / `sasl.*` key and
//! `KafkaAdminClient::from_config` (`src/admin/kafka_admin_client.rs:283-291`)
//! passes a literal `SecurityProtocol::Plaintext`. That is a production gap
//! recorded in `design/current/status.md:606-609` and out of scope for this
//! harness; a vacuous SASL test is deliberately not written in its place.

use std::time::Duration;

use confluent_kafka::admin::{
    AlterUserScramCredentialsOptions, DescribeUserScramCredentialsOptions, ScramCredentialInfo, ScramMechanism,
    UserScramCredentialAlteration, UserScramCredentialDeletion, UserScramCredentialUpsertion,
};

use crate::common::admin_backend::{AdminBackend, admin_for, all_of_exactly};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::common::test_utils::wait_until_true_with_timeout;
use crate::multilanguage_admin_test;

/// How long to poll for a credential change to reach the brokers. Mirrors the
/// `5000L` that `ClientQuotasRequestTest` passes to
/// `TestUtils.retryOnExceptionWithTimeout`.
const SCRAM_PROPAGATION_TIMEOUT_MS: u64 = 5_000;
const SCRAM_PROPAGATION_PAUSE_MS: u64 = 100;

/// Applies one alteration and asserts the user was accepted.
async fn alter<B: AdminBackend>(admin: &B, user: &str, alteration: UserScramCredentialAlteration, what: &str) {
    let altered = admin
        .alter_user_scram_credentials(std::slice::from_ref(&alteration), AlterUserScramCredentialsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: {what}: {e}", admin.name()));
    // `all_of_exactly` rather than `all_of`: the fold alone returns Ok for an
    // empty map, so a backend answering with no entries would pass silently.
    all_of_exactly(admin, &altered, std::slice::from_ref(&user.to_string()), what);
}

/// Describes exactly one user, returning that user's outcome (or `None` when the
/// response carried no entry for them at all).
async fn describe_one<B: AdminBackend>(
    admin: &B,
    user: &str,
) -> Option<Result<confluent_kafka::admin::UserScramCredentialsDescription, confluent_kafka::common::KafkaError>> {
    admin
        .describe_user_scram_credentials(
            std::slice::from_ref(&user.to_string()),
            DescribeUserScramCredentialsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{} backend: describe SCRAM credentials: {e}", admin.name()))
        .get(user)
        .cloned()
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// Upsert -> describe -> delete round-trip for a SCRAM-SHA-256 credential.
async fn upsert_describe_delete_scram_credential_round_trips<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    // A unique, self-scoped username so assertions never touch another test's
    // credentials on the shared broker.
    let user = ctx.group_id("scram_roundtrip_user");
    let mechanism = ScramMechanism::ScramSha256;
    let iterations = 8192;

    // 1. Upsert. This runs the real PBKDF2 salting path — with the *same* salt on
    //    every backend, since the harness carries it — and the broker rejects a
    //    malformed salted password.
    let upsertion =
        UserScramCredentialUpsertion::new(&user, ScramCredentialInfo::new(mechanism, iterations), "password");
    alter(
        &admin,
        &user,
        upsertion.into(),
        "alterUserScramCredentials upserting SCRAM-SHA-256",
    )
    .await;

    // 2. Describe our user and assert the mechanism and iteration count round
    //    trip. Credential changes reach the brokers asynchronously, so the whole
    //    read-back is polled (Java: `TestUtils.retryOnExceptionWithTimeout`).
    wait_until_true_with_timeout(
        || async {
            matches!(describe_one(&admin, &user).await, Some(Ok(description))
                if description.credential_infos().len() == 1)
        },
        &format!("{backend} backend: {user} should have exactly one SCRAM credential after the upsert"),
        SCRAM_PROPAGATION_TIMEOUT_MS,
        SCRAM_PROPAGATION_PAUSE_MS,
    )
    .await;

    let description = describe_one(&admin, &user)
        .await
        .unwrap_or_else(|| panic!("{backend} backend: {user} should have an outcome in the describe result"))
        .unwrap_or_else(|e| panic!("{backend} backend: describe {user}: {e}"));
    assert_eq!(
        description.name(),
        user,
        "{backend} backend: the description should name the requested user"
    );
    assert_eq!(
        description.credential_infos().len(),
        1,
        "{backend} backend: expected exactly one credential, got {description:?}"
    );
    let info = &description.credential_infos()[0];
    assert_eq!(
        info.mechanism(),
        mechanism,
        "{backend} backend: the mechanism should round-trip, got {:?}",
        info.mechanism()
    );
    assert_eq!(
        info.iterations(),
        iterations,
        "{backend} backend: the iteration count should round-trip, got {}",
        info.iterations()
    );

    // 3. Delete the credential.
    let deletion: UserScramCredentialAlteration = UserScramCredentialDeletion::new(&user, mechanism).into();
    alter(&admin, &user, deletion, "alterUserScramCredentials deleting the credential").await;

    // 4. Describe again. Our user now has *no* credential, which the per-user shape
    //    reports as a successful description with an **empty** credential list
    //    rather than as an error — Java's `all()` treats the broker's
    //    RESOURCE_NOT_FOUND that way, and the FFI's composition preserves it. The
    //    distinction matters: an `Err` here would mean the describe failed, which
    //    is a different outcome from "the user exists with nothing configured".
    wait_until_true_with_timeout(
        || async {
            matches!(describe_one(&admin, &user).await, Some(Ok(description))
                if description.credential_infos().is_empty())
        },
        &format!("{backend} backend: {user} should be described with no credentials after the delete"),
        SCRAM_PROPAGATION_TIMEOUT_MS,
        SCRAM_PROPAGATION_PAUSE_MS,
    )
    .await;

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_upsert_describe_delete_scram_credential_round_trips,
    upsert_describe_delete_scram_credential_round_trips
);
