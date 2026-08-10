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

//! The `multilanguage_consumer_test!` declarative macro — the consumer twin of
//! [`crate::multilanguage_test`].
//!
//! Each invocation expands to four `#[tokio::test(flavor = "multi_thread")]`
//! wrappers (`name__rust`, `name__grpc_python`, `name__grpc_python_async`,
//! `name__grpc_c`)
//! calling the same generic body
//! `async fn body<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F)`.
//! Multi-thread flavor is required: the gRPC consumer's sync trait methods
//! (`assignment`, `subscription`, `paused`, `wakeup`) use `block_in_place`.
//!
//! Consumer test bodies typically produce setup records with a *native* Rust
//! producer (the producer is incidental fixture) and exercise the consumer
//! under test through `factory`.

/// Expand a generic consumer test body into four `#[tokio::test]` wrappers,
/// one per backend (rust / grpc_python / grpc_python_async / grpc_c). Two forms, mirroring
/// `multilanguage_test!`: with or without an explicit `ClusterConfig`.
#[macro_export]
macro_rules! multilanguage_consumer_test {
    ($name:ident, $body:ident) => {
        $crate::multilanguage_consumer_test!($name, $body, $crate::common::cluster_config::ClusterConfig::default());
    };
    ($name:ident, $body:ident, $cluster_config:expr) => {
        ::paste::paste! {
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
        }
    };
}
