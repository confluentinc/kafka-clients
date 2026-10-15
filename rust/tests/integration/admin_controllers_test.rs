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

//! Integration tests for `Admin::unregister_controller` (KAFKA-20395, Kafka 4.4)
//! against a real broker.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and the
//! C FFI. With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion.
//!
//! # Which broker answers what
//!
//! The scenarios run on whatever `INTEGRATION_TEST_BROKER_TAG` selects (4.2.0 by
//! default), so each one first asks the cluster which of three regimes it is in,
//! using `describeFeatures`' `metadata.version` (`MetadataVersion.IBP_4_4_IV2` is
//! feature level 33, the first that supports unregistration):
//!
//!   - **A pre-4.4 broker** (supported `metadata.version` max below 33) does not
//!     advertise API key 94 at all, so the client fails the call itself with
//!     `UNSUPPORTED_VERSION` before anything is sent.
//!   - **A 4.4 broker on an older metadata version** (finalized level below 33)
//!     forwards the request, and the controller's `ClusterControlManager` throws
//!     `UnsupportedVersionException("The current MetadataVersion is too old to
//!     support controller unregistration.")` — but only after `QuorumController`'s
//!     own active-controller check, which runs first.
//!   - **A 4.4 broker on `metadata.version` 33+** reaches the registration check.
//!
//! The pooled fixture is one combined (`broker,controller`) node, so its node id
//! is the active controller's id. That makes two Java error arms reachable here,
//! with Java's exact messages (`KRaftClusterTest.testUnregisterControllerError`):
//! an unknown id gives `CONTROLLER_ID_NOT_REGISTERED`, and the active
//! controller's own id gives `INVALID_REQUEST`.
//!
//! The **success** arm is not reachable on a pooled cluster: Java's
//! `KRaftClusterTest.testUnregisterController` first shuts a controller down in a
//! three-controller quorum, and the Rust harness can stop neither a pooled node
//! nor a controller (`KafkaCluster::shutdown_broker` takes broker ids of a
//! dedicated `Type::Kraft` cluster only). It is covered against `MockAdminClient`
//! with its Raft-controller option in the unit, C and Python suites; across the
//! four backends here only the mock's default, unsupported answer can be seeded.

use std::time::Duration;

use confluent_kafka::admin::{DescribeClusterOptions, DescribeFeaturesOptions, UnregisterControllerOptions};
use confluent_kafka::common::Error;

use crate::common::admin_backend::{AdminBackend, admin_for};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::multilanguage_admin_test;

/// `MetadataVersion.IBP_4_4_IV2.featureLevel()`: the first metadata version that
/// supports controller unregistration (`isControllerUnregistrationSupported`).
const CONTROLLER_UNREGISTRATION_LEVEL: i16 = 33;

/// An id no node of the fixture uses (Java's test uses the same one).
const UNKNOWN_CONTROLLER_ID: i32 = 9999;

/// What the cluster under test can do with an `UnregisterController` request.
#[derive(Debug, PartialEq, Eq)]
enum Regime {
    /// The broker predates the API, so the client rejects the call.
    ApiUnsupported,
    /// A 4.4 broker whose finalized `metadata.version` is below 33.
    MetadataVersionTooOld,
    /// The controller reaches its registration check.
    Supported,
}

async fn regime<A: AdminBackend>(admin: &A, backend: &str) -> Regime {
    let features = admin
        .describe_features(DescribeFeaturesOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe features: {e}"));
    let supported_max = features
        .supported_features
        .get("metadata.version")
        .unwrap_or_else(|| panic!("{backend} backend: metadata.version should be a supported feature"))
        .max_version();
    if supported_max < CONTROLLER_UNREGISTRATION_LEVEL {
        return Regime::ApiUnsupported;
    }
    let finalized = features
        .finalized_features
        .get("metadata.version")
        .unwrap_or_else(|| panic!("{backend} backend: metadata.version should be finalized"))
        .max_version_level();
    if finalized < CONTROLLER_UNREGISTRATION_LEVEL {
        Regime::MetadataVersionTooOld
    } else {
        Regime::Supported
    }
}

fn assert_unsupported(error: &Error, backend: &str, regime: &Regime) {
    assert!(
        matches!(error, Error::UnsupportedVersion(_)),
        "{backend} backend ({regime:?}): expected UNSUPPORTED_VERSION, got {error:?}"
    );
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// (a) An id that is not registered: `CONTROLLER_ID_NOT_REGISTERED` (136) with
/// the controller's message, or `UNSUPPORTED_VERSION` where the cluster cannot
/// unregister controllers at all.
async fn unregister_controller_rejects_an_unregistered_id<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();
    let regime = regime(&admin, backend).await;

    let error = admin
        .unregister_controller(
            UNKNOWN_CONTROLLER_ID,
            UnregisterControllerOptions::new().set_timeout_ms(Some(30_000)),
        )
        .await
        .expect_err("an unregistered controller id cannot be unregistered");
    match regime {
        Regime::ApiUnsupported => assert_unsupported(&error, backend, &regime),
        Regime::MetadataVersionTooOld => {
            assert_unsupported(&error, backend, &regime);
            assert_eq!(
                error.message(),
                "The current MetadataVersion is too old to support controller unregistration.",
                "{backend} backend"
            );
        },
        Regime::Supported => {
            assert!(
                matches!(error, Error::ControllerIdNotRegistered(_)),
                "{backend} backend: expected CONTROLLER_ID_NOT_REGISTERED, got {error:?}"
            );
            assert_eq!(
                error.message(),
                format!("Controller ID {UNKNOWN_CONTROLLER_ID} is not currently registered."),
                "{backend} backend"
            );
        },
    }

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (b) The active controller's own id: `INVALID_REQUEST` on any 4.4 broker —
/// `QuorumController.unregisterController` checks it before the metadata
/// version — or `UNSUPPORTED_VERSION` on a broker that predates the API.
async fn unregister_controller_rejects_the_active_controller<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();
    let regime = regime(&admin, backend).await;

    // The pooled fixture is a single combined node, so the one node is the
    // active controller.
    let cluster = admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe cluster: {e}"));
    assert_eq!(cluster.nodes.len(), 1, "{backend} backend: the fixture is one combined node");
    let active_id = cluster.nodes[0].id();

    let error = admin
        .unregister_controller(active_id, UnregisterControllerOptions::new().set_timeout_ms(Some(30_000)))
        .await
        .expect_err("the active controller cannot unregister itself");
    match regime {
        Regime::ApiUnsupported => assert_unsupported(&error, backend, &regime),
        Regime::MetadataVersionTooOld | Regime::Supported => {
            assert!(
                matches!(error, Error::InvalidRequest(_)),
                "{backend} backend ({regime:?}): expected INVALID_REQUEST, got {error:?}"
            );
            assert_eq!(
                error.message(),
                "Controller cannot unregister itself while it is active.",
                "{backend} backend"
            );
        },
    }

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (c) `MockAdminClient`'s default answer: Java's mock is built without a Raft
/// controller, so `unregisterController` fails with
/// `UnsupportedVersionException("")` — an empty message on every backend.
async fn unregister_controller_on_the_mock_client<F: AdminBackendFactory>(_ctx: &mut TestContext, factory: &F) {
    let admin = factory
        .create_mock(1)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create mock admin client: {e}", factory.name()));
    let backend = factory.name();

    let error = admin
        .unregister_controller(1, UnregisterControllerOptions::new())
        .await
        .expect_err("the default mock has no Raft controller");
    assert!(
        matches!(error, Error::UnsupportedVersion(_)),
        "{backend} backend: expected UNSUPPORTED_VERSION, got {error:?}"
    );
    assert_eq!(error.message(), "", "{backend} backend: Java's mock passes an empty message");

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

multilanguage_admin_test!(
    test_unregister_controller_rejects_an_unregistered_id,
    unregister_controller_rejects_an_unregistered_id
);
multilanguage_admin_test!(
    test_unregister_controller_rejects_the_active_controller,
    unregister_controller_rejects_the_active_controller
);
multilanguage_admin_test!(
    test_unregister_controller_on_the_mock_client,
    unregister_controller_on_the_mock_client
);
