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

//! Integration tests for the admin client-quota RPCs against a real Kafka 4.2.0
//! broker.
//!
//! Mirrors the describeClientQuotas / alterClientQuotas scenarios in Java's
//! `KafkaAdminClientIntegrationTest` and `ClientQuotasRequestTest`, exercising the
//! real network engine end to end rather than the `MockClient` unit-test harness.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and
//! the C FFI. With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion.
//!
//! # The one distinction this file exists to pin
//!
//! An `Op` whose value is `None` **removes** a quota; every finite double
//! including `0.0` is a legal quota value. Nothing else in the suite could tell
//! the two apart — an earlier probe showed the Python binding collapsing
//! `None` into `0.0` (`float(o.value or 0.0)`) and passing over 200 checks — so
//! [`remove_is_not_a_zero_quota`] pins both halves. The mechanism turned out to
//! be sharper than expected: a real 4.2 broker **rejects** a zero byte-rate quota
//! ("Quota producer_byte_rate must be greater than 0"), so a collapse would not
//! silently zero the quota — it would fail the call. The scenario asserts that
//! rejection directly, so the detection mechanism is itself tested rather than
//! assumed, and then asserts that a genuine `None` makes the key disappear.
//!
//! `MockAdminClient` cannot substitute for a real broker here: Java's mock throws
//! `UnsupportedOperationException` for both quota RPCs
//! (`MockAdminClient.java:1243-1250`), which the Rust mock mirrors, so the
//! removal is only observable against a broker that echoes its stored state back.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{AlterClientQuotasOptions, DescribeClientQuotasOptions};
use confluent_kafka::common::quota::client_quota_entity::CLIENT_ID;
use confluent_kafka::common::quota::{
    ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent, Op,
};

use crate::common::admin_backend::{AdminBackend, admin_for, all_of_exactly};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::common::test_utils::wait_until_true_with_timeout;
use crate::multilanguage_admin_test;

/// How long to poll for a quota change to reach the brokers.
///
/// Matches the `5000L` that `ClientQuotasRequestTest` passes to
/// `TestUtils.retryOnExceptionWithTimeout` around its quota read-back assertions
/// (`verifyIpQuotas`, `testDescribeClientQuotasMatchExact`).
const QUOTA_PROPAGATION_TIMEOUT_MS: u64 = 5_000;
const QUOTA_PROPAGATION_PAUSE_MS: u64 = 100;

const CONSUMER_BYTE_RATE: &str = "consumer_byte_rate";
const PRODUCER_BYTE_RATE: &str = "producer_byte_rate";

/// Constructs a client-id quota entity.
fn client_id_entity(name: &str) -> ClientQuotaEntity {
    ClientQuotaEntity::new(HashMap::from([(CLIENT_ID.to_string(), Some(name.to_string()))]))
}

/// A `contains` filter selecting exactly one client id.
fn client_id_filter(name: &str) -> ClientQuotaFilter {
    ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity(CLIENT_ID, name)])
}

/// Applies one alteration and asserts the entity was accepted.
///
/// `all_of_exactly` rather than `all_of`: the fold alone returns `Ok` for an empty
/// map, and the key here is the whole [`ClientQuotaEntity`], so this also pins
/// that the entity survived the round trip as a key.
async fn alter<B: AdminBackend>(admin: &B, entity: &ClientQuotaEntity, ops: Vec<Op>, what: &str) {
    let alteration = ClientQuotaAlteration::new(entity.clone(), ops);
    let altered = admin
        .alter_client_quotas(std::slice::from_ref(&alteration), AlterClientQuotasOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: {what}: {e}", admin.name()));
    all_of_exactly(admin, &altered, std::slice::from_ref(entity), what);
}

/// The quota values currently reported for `entity` under `filter`, or `None` when
/// the entity is not reported at all.
async fn reported_quotas<B: AdminBackend>(
    admin: &B,
    filter: &ClientQuotaFilter,
    entity: &ClientQuotaEntity,
) -> Option<HashMap<String, f64>> {
    admin
        .describe_client_quotas(filter, DescribeClientQuotasOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: describe client quotas: {e}", admin.name()))
        .get(entity)
        .cloned()
}

/// The value reported for one quota key of `entity`, or `None` when the key (or
/// the entity) is absent.
async fn reported_value<B: AdminBackend>(
    admin: &B,
    filter: &ClientQuotaFilter,
    entity: &ClientQuotaEntity,
    key: &str,
) -> Option<f64> {
    reported_quotas(admin, filter, entity)
        .await
        .and_then(|values| values.get(key).copied())
}

/// Polls until `key` is reported for `entity` with `expected`.
async fn wait_for_value<B: AdminBackend>(
    admin: &B,
    filter: &ClientQuotaFilter,
    entity: &ClientQuotaEntity,
    key: &str,
    expected: f64,
) {
    wait_until_true_with_timeout(
        || async {
            reported_value(admin, filter, entity, key)
                .await
                .is_some_and(|value| (value - expected).abs() < 1e-6)
        },
        &format!("{} backend: {key} should be reported as {expected} for {entity}", admin.name()),
        QUOTA_PROPAGATION_TIMEOUT_MS,
        QUOTA_PROPAGATION_PAUSE_MS,
    )
    .await;
}

/// Polls until `key` is no longer reported for `entity` at all.
async fn wait_for_absence<B: AdminBackend>(
    admin: &B,
    filter: &ClientQuotaFilter,
    entity: &ClientQuotaEntity,
    key: &str,
) {
    wait_until_true_with_timeout(
        || async { reported_value(admin, filter, entity, key).await.is_none() },
        &format!("{} backend: {key} should no longer be reported for {entity}", admin.name()),
        QUOTA_PROPAGATION_TIMEOUT_MS,
        QUOTA_PROPAGATION_PAUSE_MS,
    )
    .await;
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// A byte-rate quota round-trips through alter -> describe.
async fn alter_then_describe_round_trips_a_byte_rate_quota<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let name = ctx.group_id("quota_client_roundtrip");
    let entity = client_id_entity(&name);
    alter(
        &admin,
        &entity,
        vec![Op::new(CONSUMER_BYTE_RATE, Some(1_048_576.0))],
        "alterClientQuotas setting consumer_byte_rate",
    )
    .await;

    // `alterClientQuotas` returns once the controller accepted the change; the
    // brokers observe it asynchronously, so a describe issued immediately after
    // can legitimately report nothing. Retry the read-back, mirroring
    // `ClientQuotasRequestTest.testDescribeClientQuotasMatchExact`.
    wait_for_value(&admin, &client_id_filter(&name), &entity, CONSUMER_BYTE_RATE, 1_048_576.0).await;

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// Removing a quota is not the same as setting it to zero — and the broker itself
/// is what makes the difference observable.
///
/// The original asserted only that a removed key stops being reported. This adds
/// the step that pins *why* that catches a collapse of `None` into `0.0`:
/// **the broker rejects a zero byte-rate quota outright** ("Quota
/// producer_byte_rate must be greater than 0", measured against a real 4.2
/// broker). So a layer that encoded a removal as `0.0` would not silently zero
/// the quota — it would make the call fail. Asserting the rejection directly
/// turns that from an assumption about the broker into a tested property, and
/// asserting the removal separately covers the other direction.
async fn remove_is_not_a_zero_quota<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let name = ctx.group_id("quota_client_remove");
    let entity = client_id_entity(&name);
    let filter = client_id_filter(&name);

    // 1. A non-zero value is reported as itself.
    alter(
        &admin,
        &entity,
        vec![Op::new(PRODUCER_BYTE_RATE, Some(2_097_152.0))],
        "alterClientQuotas setting producer_byte_rate",
    )
    .await;
    wait_for_value(&admin, &filter, &entity, PRODUCER_BYTE_RATE, 2_097_152.0).await;

    // 2. **Zero is not a legal byte-rate quota**, and the rejection arrives in the
    //    per-entity slot. This is exactly the request a `None`-collapsed-to-`0.0`
    //    encoder would send, so it establishes that such a collapse cannot pass
    //    unnoticed anywhere in the stack — and it exercises the per-key error arm
    //    of `alterClientQuotas`, which nothing else in the suite reaches.
    let zero = ClientQuotaAlteration::new(entity.clone(), vec![Op::new(PRODUCER_BYTE_RATE, Some(0.0))]);
    let rejected = admin
        .alter_client_quotas(std::slice::from_ref(&zero), AlterClientQuotasOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: a zero quota should be rejected per entity, not per call: {e}"));
    let outcome = rejected.get(&entity).unwrap_or_else(|| {
        panic!(
            "{backend} backend: the response must carry an outcome for {entity}, got {:?}",
            rejected.keys().collect::<Vec<_>>()
        )
    });
    let error = outcome.as_ref().expect_err("a zero byte-rate quota is rejected");
    assert!(
        error.message().contains("must be greater than 0"),
        "{backend} backend: the broker should reject a zero byte-rate quota by name, got {:?}",
        error.message()
    );
    // The rejected alteration changed nothing, so the original value stands. That
    // is what makes step 3's assertion meaningful rather than vacuous — the key is
    // still there to be removed.
    assert_eq!(
        reported_value(&admin, &filter, &entity, PRODUCER_BYTE_RATE).await,
        Some(2_097_152.0),
        "{backend} backend: a rejected alteration must leave the stored quota untouched"
    );

    // 3. **`None` removes.** The key must disappear from the entity's map
    //    entirely — not become 0.0, which step 2 just proved the broker would not
    //    even accept.
    alter(
        &admin,
        &entity,
        vec![Op::new(PRODUCER_BYTE_RATE, None)],
        "alterClientQuotas removing producer_byte_rate",
    )
    .await;
    wait_for_absence(&admin, &filter, &entity, PRODUCER_BYTE_RATE).await;

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// An entity-type filter returns only entities of that type, and `contains_only`
/// (Java's `strict`) is honoured.
///
/// The original covered the `of_entity_type` component alone. `strict` is a wire
/// field of the request that no scenario read, so a `contains_only` read-back is
/// asserted too: it must still report our client-id entity, since that entity has
/// *exactly* the one component the filter names.
async fn entity_type_filter_returns_only_matching_entities<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let name = ctx.group_id("quota_client_typed");
    let entity = client_id_entity(&name);
    alter(
        &admin,
        &entity,
        vec![Op::new(CONSUMER_BYTE_RATE, Some(4_194_304.0))],
        "alterClientQuotas setting consumer_byte_rate",
    )
    .await;

    // A filter on the CLIENT_ID entity *type* — `ClientQuotaMatch::Any`, Java's
    // null name — must return our entity and only client-id entities.
    let by_type = ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity_type(CLIENT_ID)]);
    wait_until_true_with_timeout(
        || async {
            reported_quotas(&admin, &by_type, &entity)
                .await
                .is_some_and(|values| values.contains_key(CONSUMER_BYTE_RATE))
        },
        &format!("{backend} backend: the configured client-id entity should be reported by an entity-type filter"),
        QUOTA_PROPAGATION_TIMEOUT_MS,
        QUOTA_PROPAGATION_PAUSE_MS,
    )
    .await;

    let described = admin
        .describe_client_quotas(&by_type, DescribeClientQuotasOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe client quotas: {e}"));
    for reported in described.keys() {
        assert!(
            reported.entries().contains_key(CLIENT_ID),
            "{backend} backend: every entity reported by a client-id type filter must have a client-id component, \
             got {reported}"
        );
    }

    // `contains_only` is the same components with `strict` set. Our entity has
    // exactly the one named component, so it must still be reported — a backend
    // that dropped or inverted the `strict` field shows up as this entity going
    // missing (or as unrelated multi-component entities appearing).
    let strict = ClientQuotaFilter::contains_only(vec![ClientQuotaFilterComponent::of_entity_type(CLIENT_ID)]);
    let strictly_described = admin
        .describe_client_quotas(&strict, DescribeClientQuotasOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe client quotas (strict): {e}"));
    assert!(
        strictly_described.contains_key(&entity),
        "{backend} backend: a strict client-id filter must still report a pure client-id entity, got {:?}",
        strictly_described.keys().collect::<Vec<_>>()
    );
    for reported in strictly_described.keys() {
        assert_eq!(
            reported.entries().len(),
            1,
            "{backend} backend: a strict single-component filter must report only single-component entities, \
             got {reported}"
        );
    }

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_alter_then_describe_round_trips_a_byte_rate_quota,
    alter_then_describe_round_trips_a_byte_rate_quota
);
multilanguage_admin_test!(test_remove_is_not_a_zero_quota, remove_is_not_a_zero_quota);
multilanguage_admin_test!(
    test_entity_type_filter_returns_only_matching_entities,
    entity_type_filter_returns_only_matching_entities
);
