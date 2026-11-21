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

//! The `multilanguage_admin_test!` declarative macro — the admin twin of
//! [`crate::multilanguage_test`] and [`crate::multilanguage_consumer_test`].
//!
//! Each invocation expands to seven `#[tokio::test(flavor = "multi_thread")]`
//! wrappers (`name__rust`, `name__grpc_python`, `name__grpc_python_async`,
//! `name__grpc_c`, `name__grpc_dotnet`, `name__grpc_dotnet_async`, `name__grpc_js`)
//! calling the same generic body
//! `async fn body<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F)`.
//!
//! The two .NET arms drive different managed code: `__grpc_dotnet` the sync
//! `AdminServiceImpl` over `IAdmin`, `__grpc_dotnet_async` the `AsyncAdminServiceImpl`
//! over `IAsyncAdmin`. Until the M15/P14 split there was only `IAdmin`, so an async
//! arm would have driven the same code twice and was left out (M15/P12 D1).
//!
//! The `__grpc_` infix is load-bearing: `make -C rust test-all-features` excludes
//! every container-backed arm across the producer, consumer and admin suites
//! with a single `--skip __grpc`.
//!
//! `__grpc_js` drives the Node.js admin client through
//! `javascript/grpc-server/grpc_admin.js`. As in `multilanguage_test!`, its arm
//! is the JavaScript-only macro [`crate::multilanguage_admin_js_test`], which
//! the main macro expands to last.
//!
//! As in `multilanguage_test!` and `multilanguage_consumer_test!`, the six
//! container-backed arms are individually `#[cfg(feature =
//! "multilanguage-tests")]` and the macro itself is available with only
//! `integration-tests`. That is what lets the committed admin integration tests
//! be *converted* to scenarios rather than duplicated by them: under
//! `integration-tests` alone (`make test-integration`, `verify-commit`) an
//! invocation still expands to the `__rust` arm, which drives the same
//! production `Admin` trait against the same broker as the single-backend test
//! it replaced — so converting costs no coverage at any feature level.
//!
//! Every arm prints one timing line to stderr when it ends, through
//! [`crate::common::arm_timer::ArmTimer`], which shows in CI logs for passing
//! tests too.
//!
//! Multi-thread flavor matches the other two macros. The native admin client
//! spawns its own background task and the gRPC backends block a tonic worker,
//! so a current-thread runtime would be a needless constraint.

/// Expand a generic admin test body into seven `#[tokio::test]` wrappers, one per
/// backend (rust / grpc_python / grpc_python_async / grpc_c / grpc_dotnet /
/// grpc_dotnet_async / grpc_js). Two forms, mirroring `multilanguage_test!`: with or without
/// an explicit `ClusterConfig`.
///
/// Leading outer attributes (`#[ignore = "<reason>"]`) are applied to every
/// arm: the form for a scenario that exposes a defect in the shared Rust core,
/// which every backend reaches.
#[macro_export]
macro_rules! multilanguage_admin_test {
    ($(#[$attr:meta])* $name:ident, $body:ident) => {
        $crate::multilanguage_admin_test!(
            $(#[$attr])* $name,
            $body,
            $crate::common::cluster_config::ClusterConfig::default()
        );
    };
    ($(#[$attr:meta])* $name:ident, $body:ident, $cluster_config:expr) => {
        ::paste::paste! {
            $(#[$attr])*
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ rust>]() {
                let mut timer = $crate::common::arm_timer::ArmTimer::start(stringify!($name), "rust");
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let factory = $crate::common::backend_factory::RustNativeFactory;
                timer.body_started();
                $body(&mut ctx, &factory).await;
            }

            $(#[$attr])*
            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_python>]() {
                let mut timer = $crate::common::arm_timer::ArmTimer::start(stringify!($name), "grpc_python");
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::Python,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::PythonGrpcFactory::new(handle.channel().await, handle.untimed_channel().await);
                timer.body_started();
                $body(&mut ctx, &factory).await;
            }

            $(#[$attr])*
            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_python_async>]() {
                let mut timer = $crate::common::arm_timer::ArmTimer::start(stringify!($name), "grpc_python_async");
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::PythonAsync,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::PythonAsyncGrpcFactory::new(handle.channel().await, handle.untimed_channel().await);
                timer.body_started();
                $body(&mut ctx, &factory).await;
            }

            $(#[$attr])*
            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_c>]() {
                let mut timer = $crate::common::arm_timer::ArmTimer::start(stringify!($name), "grpc_c");
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::C,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::CGrpcFactory::new(handle.channel().await, handle.untimed_channel().await);
                timer.body_started();
                $body(&mut ctx, &factory).await;
            }

            $(#[$attr])*
            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_dotnet>]() {
                let mut timer = $crate::common::arm_timer::ArmTimer::start(stringify!($name), "grpc_dotnet");
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::Dotnet,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::DotnetGrpcFactory::new(handle.channel().await, handle.untimed_channel().await);
                timer.body_started();
                $body(&mut ctx, &factory).await;
            }

            $(#[$attr])*
            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_dotnet_async>]() {
                let mut timer = $crate::common::arm_timer::ArmTimer::start(stringify!($name), "grpc_dotnet_async");
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::DotnetAsync,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::DotnetAsyncGrpcFactory::new(handle.channel().await, handle.untimed_channel().await);
                timer.body_started();
                $body(&mut ctx, &factory).await;
            }
        }
        $crate::multilanguage_admin_js_test!($(#[$attr])* $name, $body, $cluster_config);
    };
}

/// The `__grpc_js` arm of [`crate::multilanguage_admin_test`] alone: one
/// `#[tokio::test]` wrapper that runs `$body` on the JavaScript backend, named
/// `<name>__grpc_js`. `multilanguage_admin_test!` expands to it for its own
/// `__grpc_js` arm, as `multilanguage_test!` does to
/// [`crate::multilanguage_js_test`]; a scenario that runs on the JavaScript
/// backend only invokes it directly, with the same forms and attributes.
#[macro_export]
macro_rules! multilanguage_admin_js_test {
    ($(#[$attr:meta])* $name:ident, $body:ident) => {
        $crate::multilanguage_admin_js_test!(
            $(#[$attr])* $name,
            $body,
            $crate::common::cluster_config::ClusterConfig::default()
        );
    };
    ($(#[$attr:meta])* $name:ident, $body:ident, $cluster_config:expr) => {
        ::paste::paste! {
            $(#[$attr])*
            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_js>]() {
                let mut timer = $crate::common::arm_timer::ArmTimer::start(stringify!($name), "grpc_js");
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::Js,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::JsGrpcFactory::new(handle.channel().await, handle.untimed_channel().await);
                timer.body_started();
                $body(&mut ctx, &factory).await;
            }
        }
    };
}
