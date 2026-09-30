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

//! The `multilanguage_test!` declarative macro.
//!
//! Each invocation expands to six `#[tokio::test(flavor = "multi_thread")]`
//! wrappers — one per backend — that call the same generic test body. The
//! body must be `async fn body<F: ProducerBackendFactory>(factory: &F)`.
//!
//! Naming convention: `name__rust`, `name__grpc_python`,
//! `name__grpc_python_async`, `name__grpc_c`, `name__grpc_dotnet`,
//! `name__grpc_dotnet_async`. The double underscore is intentional so the
//! backend label is easy to grep for in `cargo test` output.
//!
//! Every backend that runs in a container behind gRPC shares the `__grpc_`
//! infix, so a single `--skip __grpc` excludes all of them — `make
//! test-rust-all-features` relies on this. That makes the exclusion
//! **fail-safe**: a new gRPC backend is left out of the Rust-only run by
//! default and is opted in by adding its own target, rather than silently
//! joining a job that has no image for it. `__rust` deliberately lacks the
//! infix — it drives the native client with no container and no gRPC hop.
//!
//! Example:
//!
//! ```ignore
//! async fn produce_single_record_inner<F: ProducerBackendFactory>(factory: &F) {
//!     // ... shared assertions ...
//! }
//! multilanguage_test!(test_produce_single_record, produce_single_record_inner);
//! ```

/// Expand a generic test body into six `#[tokio::test]` wrappers — one
/// per backend (rust / grpc_python / grpc_python_async / grpc_c / grpc_dotnet /
/// grpc_dotnet_async). See module docs for the expected signature of `$body`.
///
/// Two forms are supported:
///   - `multilanguage_test!(name, body)` — uses `ClusterConfig::default()`
///   - `multilanguage_test!(name, body, cfg_expr)` — uses the given
///     `ClusterConfig` expression (e.g. for tests that need a broker
///     with `auto.create.topics.enable=false`).
///
/// Two-arg form delegates to three-arg with the default config; existing
/// callers don't change.
#[macro_export]
macro_rules! multilanguage_test {
    ($name:ident, $body:ident) => {
        $crate::multilanguage_test!($name, $body, $crate::common::cluster_config::ClusterConfig::default());
    };
    ($name:ident, $body:ident, $cluster_config:expr) => {
        ::paste::paste! {
            // Double underscore is intentional so backend labels are
            // easy to grep in cargo test output, hence non_snake_case.
            #[allow(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ rust>]() {
                let mut ctx = $crate::common::test_context::TestContext::new($cluster_config).await;
                let factory = $crate::common::backend_factory::RustNativeFactory;
                $body(&mut ctx, &factory).await;
            }

            #[allow(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ grpc_python>]() {
                // Create the TestContext first so the broker network
                // exists before we attempt to attach the python
                // container to it.
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

            #[allow(non_snake_case)]
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

            #[allow(non_snake_case)]
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

            #[allow(non_snake_case)]
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

            #[allow(non_snake_case)]
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
