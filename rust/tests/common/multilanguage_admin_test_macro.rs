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
//! Each invocation expands to six `#[tokio::test(flavor = "multi_thread")]`
//! wrappers (`name__rust`, `name__grpc_python`, `name__grpc_python_async`,
//! `name__grpc_c`, `name__grpc_dotnet`, `name__grpc_dotnet_async`) calling the same
//! generic body
//! `async fn body<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F)`.
//!
//! The two .NET arms drive different managed code: `__grpc_dotnet` the sync
//! `AdminServiceImpl` over `IAdmin`, `__grpc_dotnet_async` the `AsyncAdminServiceImpl`
//! over `IAsyncAdmin`. Until the M15/P14 split there was only `IAdmin`, so an async
//! arm would have driven the same code twice and was left out (M15/P12 D1).
//!
//! The `__grpc_` infix is load-bearing: `make test-rust-all-features` excludes
//! every container-backed arm across the producer, consumer and admin suites
//! with a single `--skip __grpc`.
//!
//! Unlike `multilanguage_test!` and `multilanguage_consumer_test!`, the five
//! container-backed arms are individually `#[cfg(feature =
//! "multilanguage-tests")]` and the macro itself is available with only
//! `integration-tests`. That is what lets the committed admin integration tests
//! be *converted* to scenarios rather than duplicated by them: under
//! `integration-tests` alone (`make test-integration`, `verify-sandbox`) an
//! invocation still expands to the `__rust` arm, which drives the same
//! production `Admin` trait against the same broker as the single-backend test
//! it replaced — so converting costs no coverage at any feature level. The
//! producer suite took the other route (`mod producer_test` is gated whole), and
//! its scenarios do not run under `make test-integration` as a result.
//!
//! Multi-thread flavor matches the other two macros. The native admin client
//! spawns its own background task and the gRPC backends block a tonic worker,
//! so a current-thread runtime would be a needless constraint.

/// Expand a generic admin test body into six `#[tokio::test]` wrappers, one per
/// backend (rust / grpc_python / grpc_python_async / grpc_c / grpc_dotnet /
/// grpc_dotnet_async). Two forms, mirroring `multilanguage_test!`: with or without
/// an explicit `ClusterConfig`.
#[macro_export]
macro_rules! multilanguage_admin_test {
    ($name:ident, $body:ident) => {
        $crate::multilanguage_admin_test!($name, $body, $crate::common::cluster_config::ClusterConfig::default());
    };
    ($name:ident, $body:ident, $cluster_config:expr) => {
        ::paste::paste! {
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ rust>]() {
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let factory = $crate::common::backend_factory::RustNativeFactory;
                $body(&mut ctx, &factory).await;
            }

            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_python>]() {
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::Python,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::PythonGrpcFactory::new(handle.channel().await);
                $body(&mut ctx, &factory).await;
            }

            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_python_async>]() {
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::PythonAsync,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::PythonAsyncGrpcFactory::new(handle.channel().await);
                $body(&mut ctx, &factory).await;
            }

            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_c>]() {
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::C,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::CGrpcFactory::new(handle.channel().await);
                $body(&mut ctx, &factory).await;
            }

            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_dotnet>]() {
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::Dotnet,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::DotnetGrpcFactory::new(handle.channel().await);
                $body(&mut ctx, &factory).await;
            }

            #[cfg(feature = "multilanguage-tests")]
            #[expect(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_dotnet_async>]() {
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::DotnetAsync,
                    ctx.broker_network_name(),
                )
                .await;
                let factory =
                    $crate::common::backend_factory::DotnetAsyncGrpcFactory::new(handle.channel().await);
                $body(&mut ctx, &factory).await;
            }
        }
    };
}
