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
//! Each invocation expands to three `#[tokio::test(flavor = "multi_thread")]`
//! wrappers — one per backend — that call the same generic test body. The
//! body must be `async fn body<F: ProducerBackendFactory>(factory: &F)`.
//!
//! Naming convention: `name__rust`, `name__python`, `name__c`. The double
//! underscore is intentional so the backend label is easy to grep for in
//! `cargo test` output.
//!
//! Example:
//!
//! ```ignore
//! async fn produce_single_record_inner<F: ProducerBackendFactory>(factory: &F) {
//!     // ... shared assertions ...
//! }
//! multilanguage_test!(test_produce_single_record, produce_single_record_inner);
//! ```

/// Expand a generic test body into three `#[tokio::test]` wrappers — one
/// per backend (rust / python / c). See module docs for the expected
/// signature of `$body`.
#[macro_export]
macro_rules! multilanguage_test {
    ($name:ident, $body:ident) => {
        ::paste::paste! {
            // Double underscore is intentional so backend labels are
            // easy to grep in cargo test output, hence non_snake_case.
            #[allow(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ rust>]() {
                let factory = $crate::common::backend_factory::RustNativeFactory;
                $body(&factory).await;
            }

            #[allow(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ python>]() {
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::Python,
                )
                .await;
                let factory =
                    $crate::common::backend_factory::PythonGrpcFactory::new(handle.channel().await);
                $body(&factory).await;
            }

            #[allow(non_snake_case)]
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$name __ c>]() {
                let handle = $crate::common::backend_pool::get_or_start(
                    $crate::common::backend_pool::BackendKind::C,
                )
                .await;
                let factory =
                    $crate::common::backend_factory::CGrpcFactory::new(handle.channel().await);
                $body(&factory).await;
            }
        }
    };
}
