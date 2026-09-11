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

//! Integration tests for the admin feature RPCs against a real Kafka 4.2.0
//! broker.
//!
//! Mirrors the describeFeatures / updateFeatures scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine end to
//! end rather than the `MockClient` unit-test harness.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and the
//! C FFI. With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion.
//!
//! # Why the error path is the realistic one for `updateFeatures`
//!
//! A *successful* feature upgrade is very nearly unreachable on a fresh 4.2
//! cluster, and not only because mutating a shared broker would be unsafe. Every
//! finalized feature the broker reports is already at the maximum its own
//! `supported_features` range allows — a cluster finalizes each feature at the
//! highest level all its brokers support at format time — so an `UPGRADE` to any
//! level the broker would accept is a level it is already at, and `UPGRADE` to the
//! *same* level is rejected too
//! (`FeatureUpdate`/`validateFeatureUpdate`: an upgrade must strictly increase).
//! Going the other way needs `SAFE_DOWNGRADE`, which permanently mutates
//! cluster-wide, persistent metadata. So the two error paths below are the
//! realistic coverage, and they are *different* errors reaching the caller by
//! *different* mechanisms, which is the part worth having on four backends:
//!
//!   - [`update_features_above_max_is_rejected`] — a **per-feature** error, from
//!     the broker, in the response's per-key slot.
//!   - [`update_features_rejects_an_empty_map`] — a **whole-call** error thrown
//!     *synchronously by the client*, before any request is built. It is the only
//!     RPC in this harness whose Rust submission is fallible
//!     (`Admin::update_features` returns a `Result`), so it is the only scenario
//!     that exercises the top-level-error arm for a synchronous throw.
//!
//! # And the mock closes what the real broker cannot
//!
//! The paragraph above is about a real 4.2 cluster, and an earlier revision
//! stopped there — leaving `updateFeatures`' per-key **success** arm, and every
//! `UpgradeType` except `UPGRADE`, never crossing a language boundary. But
//! `MockAdminClient` implements both feature RPCs with real per-upgrade-type
//! validation (Java `MockAdminClient.updateFeatures`, mirrored across all four
//! `UpgradeType` arms at `src/admin/mock_admin_client.rs:1886-1968`), and the
//! harness can already build a mock on all four backends. So
//! [`update_features_on_the_mock_client`] drives the success arm and two more
//! upgrade types, which is the same question the delegation-token module already
//! asks of its own "unreachable" claim.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{DescribeFeaturesOptions, FeatureUpdate, UpdateFeaturesOptions, UpgradeType};
use confluent_kafka::common::protocol::Errors;

use crate::common::admin_backend::{AdminBackend, admin_for};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::multilanguage_admin_test;

/// The canonical finalized feature present on every KRaft cluster.
const METADATA_VERSION_FEATURE: &str = "metadata.version";

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// (a) `describe_features` reports a sane `metadata.version` range.
async fn describe_features_reports_metadata_version<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let metadata = admin
        .describe_features(DescribeFeaturesOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe features: {e}"));

    assert!(
        !metadata.supported_features.is_empty(),
        "{backend} backend: the cluster should advertise at least one supported feature"
    );

    // Every KRaft cluster advertises the metadata.version feature.
    let supported = metadata
        .supported_features
        .get(METADATA_VERSION_FEATURE)
        .unwrap_or_else(|| panic!("{backend} backend: metadata.version should be a supported feature"));
    assert!(
        supported.min_version() <= supported.max_version(),
        "{backend} backend: the supported range [{}, {}] should be non-empty",
        supported.min_version(),
        supported.max_version()
    );

    // The finalized metadata.version should sit within the supported range. The
    // two maps are independent, so a feature may be supported without being
    // finalized — hence the conditional rather than an unwrap.
    if let Some(finalized) = metadata.finalized_features.get(METADATA_VERSION_FEATURE) {
        assert!(
            finalized.min_version_level() >= 1,
            "{backend} backend: the finalized metadata.version min level should be >= 1, got {}",
            finalized.min_version_level()
        );
        assert!(
            finalized.max_version_level() <= supported.max_version(),
            "{backend} backend: the finalized metadata.version {} should not exceed the supported max {}",
            finalized.max_version_level(),
            supported.max_version()
        );
        // A real cluster always reports an epoch alongside its finalized features,
        // so the `Option` is exercised on its `Some` side here.
        //
        // The bound is `> 0`, not `>= 0`, and that is the whole point: the
        // tempting decoding mistake is to turn an absent `Optional<Long>` into
        // epoch 0, and `assert!(epoch >= 0)` *admits* exactly that value — an
        // earlier revision made that claim while writing the assertion that
        // cannot catch it. The epoch is the metadata-log offset at which the
        // features were finalized, and a bootstrapped KRaft cluster has already
        // written records before that point, so a genuine epoch is at least 1.
        let epoch = metadata.finalized_features_epoch.unwrap_or_else(|| {
            panic!("{backend} backend: a cluster with finalized features should report their epoch")
        });
        assert!(
            epoch > 0,
            "{backend} backend: the finalized-features epoch should be a real metadata offset; 0 is what an absent \
             Optional<Long> decodes to if a backend drops the field. Got {epoch}"
        );
    }

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (b) `update_features` above the maximum supported level is rejected, in the
/// **per-feature** slot. `validate_only` keeps the cluster unmutated.
async fn update_features_above_max_is_rejected<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    // A level far beyond any real supported maximum.
    let updates = HashMap::from([(
        METADATA_VERSION_FEATURE.to_string(),
        FeatureUpdate::new(9999, UpgradeType::Upgrade).expect("valid feature update"),
    )]);
    let outcomes = admin
        .update_features(&updates, UpdateFeaturesOptions::new().validate_only(true))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: update_features should enqueue the call: {e}"));

    // The rejection must arrive in the per-key slot, not as a whole-call error:
    // this request is well-formed, so the client submits it and the *broker*
    // refuses. Reading the key rather than folding is what distinguishes the two,
    // and a response with no entry for the feature fails here instead of folding
    // to a silent success.
    let outcome = outcomes.get(METADATA_VERSION_FEATURE).unwrap_or_else(|| {
        panic!(
            "{backend} backend: the response must carry an outcome for {METADATA_VERSION_FEATURE}, got {:?}",
            outcomes.keys().collect::<Vec<_>>()
        )
    });
    let error = outcome
        .as_ref()
        .expect_err("upgrading metadata.version beyond its supported max should be rejected");
    // The message names the feature and the rejected level; asserting on it rather
    // than on the variant keeps the assertion identical across all four backends,
    // since the C FFI drops the `Error` discriminator.
    let message = error.message();
    assert!(
        message.contains(METADATA_VERSION_FEATURE),
        "{backend} backend: the rejection should name the feature, got {message:?}"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (c) An **empty** update map is rejected synchronously by the client, as a
/// whole-call error with Java's exact message.
///
/// `KafkaAdminClient.updateFeatures` throws
/// `IllegalArgumentException("Feature updates can not be null or empty.")`
/// (`KafkaAdminClient.java:4578`), reproduced verbatim by
/// `src/admin/kafka_admin_client.rs:4586`. No request is built, so this is the
/// only path in the harness that exercises the response's top-level error for a
/// *synchronous* throw rather than for a transport or submission failure.
///
/// The message is asserted rather than the variant: the C FFI does not surface the
/// `Error` discriminator, so `Error::LocalIllegalArgument` is not
/// reconstructable on the gRPC backends from anything but the message — which is
/// exactly why both servers' `guess_variant` has an arm keyed to these two Java
/// strings. Asserting the message makes the scenario independent of that guess.
async fn update_features_rejects_an_empty_map<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let error = admin
        .update_features(&HashMap::new(), UpdateFeaturesOptions::new())
        .await
        .expect_err("an empty feature-update map is rejected before any request is built");
    assert!(
        error.message().contains("can not be null or empty"),
        "{backend} backend: an empty update map should be rejected with Java's message, got {:?}",
        error.message()
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (d) `update_features` against [`MockAdminClient`]: the per-key **success**
/// arm, and `SAFE_DOWNGRADE` / `UNKNOWN` on top of the `UPGRADE` the real broker
/// exercises.
///
/// # Why the mock, and why no seeding is needed
///
/// A successful upgrade is unreachable on a real 4.2 cluster (see the module
/// doc), so without this scenario `updateFeatures`' per-key success arm is
/// encoded by three servers and decoded by one client without ever carrying a
/// value, and three of the four `UpgradeType` codes never leave the request
/// builder. The mock's validation runs against its seeded version bounds, which
/// default to `cur = min = max = 0` for an unknown feature
/// (`mock_admin_client.rs`: `feature_levels.get(feature).copied().unwrap_or(0)`),
/// and that default is already enough to separate the upgrade types by the
/// *message* the rejection carries — so the scenario needs no seeding plumbing
/// through the wire.
///
/// | update | mock outcome | why |
/// |---|---|---|
/// | `SAFE_DOWNGRADE` to 0 | success | `cur < next` is false, and 0 is within `[min, max] = [0, 0]` |
/// | `SAFE_DOWNGRADE` to 1 | `"Can't downgrade to newer version."` | only the `SAFE_DOWNGRADE` branch produces this message; an `UPGRADE` to 1 would answer `"Can't upgrade above 0"` instead, so a backend that dropped or defaulted `upgrade_type` fails here |
/// | `UNKNOWN` | `"Invalid upgrade type."` | a third distinguishable branch |
///
/// `UNSAFE_DOWNGRADE` is **not** separable from `SAFE_DOWNGRADE` on an unseeded
/// mock: both reject a higher level with the same message, and both accept level 0
/// (the `UNSAFE_DOWNGRADE` branch's extra `while next != cur` walk does nothing
/// when they are equal). Separating them would need the mock's version bounds
/// seeded through the wire, which is real plumbing — recorded rather than implied
/// away.
async fn update_features_on_the_mock_client<F: AdminBackendFactory>(_ctx: &mut TestContext, factory: &F) {
    let admin = factory
        .create_mock(1)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create mock admin client: {e}", factory.name()));
    let backend = factory.name();

    // 1. The per-key success arm. `validate_only` on top, so the mock does not
    //    even record the new level — the point is the resolved future, not the
    //    mutation.
    let updates = HashMap::from([(
        METADATA_VERSION_FEATURE.to_string(),
        FeatureUpdate::new(0, UpgradeType::SafeDowngrade).expect("valid feature update"),
    )]);
    let outcomes = admin
        .update_features(&updates, UpdateFeaturesOptions::new().validate_only(true))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: update features on the mock: {e}"));
    let outcome = outcomes.get(METADATA_VERSION_FEATURE).unwrap_or_else(|| {
        panic!(
            "{backend} backend: the response must carry an outcome for {METADATA_VERSION_FEATURE}, got {:?}",
            outcomes.keys().collect::<Vec<_>>()
        )
    });
    outcome
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: a safe downgrade to level 0 is accepted by the mock: {e}"));

    // 2. The same upgrade type, rejected — and the message is the one *only* the
    //    SAFE_DOWNGRADE branch produces, which is what makes this an assertion
    //    about the transported `upgrade_type` rather than about the level.
    let updates = HashMap::from([(
        METADATA_VERSION_FEATURE.to_string(),
        FeatureUpdate::new(1, UpgradeType::SafeDowngrade).expect("valid feature update"),
    )]);
    let outcomes = admin
        .update_features(&updates, UpdateFeaturesOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: update features on the mock: {e}"));
    let error = outcomes[METADATA_VERSION_FEATURE]
        .as_ref()
        .expect_err("a safe downgrade to a *higher* level is rejected");
    assert!(
        error.message().contains("Can't downgrade to newer version."),
        "{backend} backend: the rejection must be the SAFE_DOWNGRADE one; an UPGRADE would say \"Can't upgrade \
         above 0\". Got {:?}",
        error.message()
    );
    assert_eq!(
        error.error(),
        Errors::InvalidRequest,
        "{backend} backend: the mock composes Java's InvalidRequestException, got {error:?}"
    );

    // 3. A third branch, so the code is not merely "one of two".
    let updates = HashMap::from([(
        METADATA_VERSION_FEATURE.to_string(),
        FeatureUpdate::new(1, UpgradeType::Unknown).expect("valid feature update"),
    )]);
    let outcomes = admin
        .update_features(&updates, UpdateFeaturesOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: update features on the mock: {e}"));
    let error = outcomes[METADATA_VERSION_FEATURE]
        .as_ref()
        .expect_err("an UNKNOWN upgrade type is rejected");
    assert!(
        error.message().contains("Invalid upgrade type."),
        "{backend} backend: an UNKNOWN upgrade type has its own rejection message, got {:?}",
        error.message()
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
}

multilanguage_admin_test!(
    test_describe_features_reports_metadata_version,
    describe_features_reports_metadata_version
);
multilanguage_admin_test!(
    test_update_features_above_max_is_rejected,
    update_features_above_max_is_rejected
);
multilanguage_admin_test!(test_update_features_rejects_an_empty_map, update_features_rejects_an_empty_map);
multilanguage_admin_test!(test_update_features_on_the_mock_client, update_features_on_the_mock_client);
