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

//! Admin integration tests run against all four backends (native Rust, Python
//! sync, Python asyncio, C) via [`multilanguage_admin_test!`].
//!
//! Scenarios are written once against
//! [`AdminBackend`](crate::common::admin_backend::AdminBackend) — the
//! harness-local admin surface — and executed once per backend, so a
//! disagreement between the Rust core, the C FFI and the Python binding shows up
//! as three backends agreeing and one not. See
//! `design/history/Milestone-11/PLAN-multilanguage-admin.md`.
//!
//! Slice G0 covers lifecycle only: construct an admin client and close it. The
//! RPCs arrive in G1..G6.
//!
//! Requires `--features multilanguage-tests`.

use std::collections::HashMap;
use std::time::Duration;

use crate::common::admin_backend::AdminBackend;
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::multilanguage_admin_test;

/// Admin config for the backend under test. `bootstrap` must be reachable from
/// the backend (container listener for python/c, host loopback for rust).
fn admin_config(bootstrap: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "multilang-admin".to_string()),
    ])
}

fn bootstrap_for<F: AdminBackendFactory>(factory: &F, ctx: &TestContext) -> String {
    if factory.needs_container_bootstrap() {
        ctx.container_bootstrap_servers().to_string()
    } else {
        ctx.bootstrap_servers().to_string()
    }
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// Construct both flavours of admin client on the backend and close each one.
///
/// This is the G0 vertical: it proves the whole path — proto, Rust client,
/// backend factory, macro, and the `AdminService` handlers in all three servers
/// — before any RPC depends on it. Both constructors are covered because the
/// later slices drive scenarios through each (`MockAdminClient` reaches the
/// cases a single PLAINTEXT broker cannot).
async fn create_and_close<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = factory
        .create(admin_config(&bootstrap_for(factory, ctx)))
        .await
        .unwrap_or_else(|e| panic!("{} backend: create admin client: {e}", factory.name()));
    assert_eq!(admin.name(), factory.name(), "backend label must match the factory");
    admin
        .close(Some(Duration::from_secs(30)))
        .await
        .unwrap_or_else(|e| panic!("{} backend: close admin client: {e}", factory.name()));

    let mock = factory
        .create_mock(1)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create mock admin client: {e}", factory.name()));
    mock.close(Some(Duration::from_secs(30)))
        .await
        .unwrap_or_else(|e| panic!("{} backend: close mock admin client: {e}", factory.name()));
}

multilanguage_admin_test!(test_ml_admin_create_and_close, create_and_close);
