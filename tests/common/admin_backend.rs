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

//! The harness-local admin surface driven by the multilanguage integration
//! tests, plus the native-Rust implementation of it.
//!
//! See `design/history/Milestone-11/PLAN-multilanguage-admin.md` §D1 for the
//! decision this file implements.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{Admin, AdminClientConfig, MockAdminClient, new_admin_client};
use confluent_kafka::common::KafkaError;

/// Timeout that stands for Java's no-argument `Admin.close()`, which delegates
/// to `close(Duration.ofMillis(Long.MAX_VALUE))`. Same convention the C FFI
/// uses for a negative `timeout_ms` (`src/ffi/admin.rs::close_timeout`).
fn close_timeout(timeout: Option<Duration>) -> Duration {
    timeout.unwrap_or_else(|| Duration::from_millis(i64::MAX as u64))
}

/// The admin surface the multilanguage scenarios are written against.
///
/// # Why this is not the production [`Admin`] trait
///
/// The consumer harness implements the real `Consumer` trait, so consumer
/// scenarios are generic over production code. Admin cannot do that, for two
/// independent reasons:
///
///   - 33 of the 46 `*Result` types declare `pub(crate) fn new` (e.g.
///     `src/admin/list_topics_result.rs`), so an integration test — a separate
///     crate — cannot construct one to return from `Admin::list_topics`.
///   - Completing a future later needs `KafkaFutureImpl`
///     (`src/common/kafka_future.rs`), also `pub(crate)`; the only public
///     constructor is `KafkaFuture::completed`.
///
/// Both visibilities are faithful to Java (`CreateTopicsResult`'s constructor is
/// package-private and `KafkaFutureImpl` lives in
/// `org.apache.kafka.common.internals`, which CLAUDE.md maps to `pub(crate)`),
/// so widening them to make a test helper compile is not an option.
///
/// # DoD #7 justification (a type with no Java counterpart)
///
/// `AdminBackend` is test scaffolding, exactly as
/// [`ProducerBackendFactory`](crate::common::backend_factory::ProducerBackendFactory)
/// and
/// [`ConsumerBackendFactory`](crate::common::backend_factory::ConsumerBackendFactory)
/// already are — neither exists in Java either. It models what actually crosses
/// a language boundary: both bindings collapse per-key futures *before*
/// returning (Python's `_run_sync` hands back a resolved dict; the C `_async`
/// entry points fire their callback with a fully-built result struct), so
/// already-resolved plain data is the honest wire shape rather than a
/// simplification. Per-key granularity is still asserted — later slices carry it
/// as `HashMap<K, Result<V, KafkaError>>` rather than as futures.
///
/// Knowingly accepted consequence: the harness cannot assert that an Admin
/// method *returns before* its futures resolve. That property is untestable
/// through any binding (both are eager at the boundary) and stays covered by the
/// unit tests in `src/admin`, which use the real trait.
///
/// Methods are `async fn` in the trait (hence `#[allow(async_fn_in_trait)]`,
/// matching the existing backend factories) and return already-resolved plain
/// data; the gRPC implementation awaits one round-trip per call.
///
/// # Signature conventions for the RPC methods (slices G1..G6)
///
/// The 38 committed admin integration tests under `tests/integration/admin_*_test.rs`
/// are the scenario source, and they are converted to run on this trait rather
/// than rewritten. These four conventions are what make that conversion
/// mechanical; a method that departs from them forces its call sites to be
/// restructured.
///
///   1. **Per-key methods return `HashMap<K, Result<V, KafkaError>>`.** Today a
///      body reads one key out of a per-key accessor and awaits it —
///      `result.values()[&topic].get().await`,
///      `result.topic_name_values().unwrap()[&topic].get().await`,
///      `result.low_watermarks()[&tp].get().await`. Against an owned map of
///      per-key `Result`s that becomes `map[&topic].clone()` /
///      `map.get(&topic).unwrap()`, with the same `expect` / `expect_err` and
///      the same `err.error() == Errors::X` assertion after it. `K` needs
///      `Hash + Eq`; the key types in use are `String`, `TopicPartition` and
///      `ConfigResource`.
///   2. **The `.all()`-shaped call sites are served by the same map.** Most
///      bodies use only `.all().get().await.expect(...)`, i.e. "every key
///      succeeded". That is a fold over the returned map, not a second method,
///      so no `*_all` variants are needed.
///   3. **Options stay positional parameters**, exactly as on the production
///      trait — every existing call site passes a bare `XOptions::new()` and
///      keeps doing so. (No body uses a builder method on an options struct
///      yet, but the parameter must be there for the ones that will.)
///   4. **An RPC whose Java result exposes several independent futures returns
///      one struct with all of them resolved.** `describe_cluster` is the case:
///      `admin_cluster_configs_test.rs` holds the result and awaits
///      `.nodes()`, `.controller()` and `.cluster_id()` off it separately. A
///      method returning a single future cannot express that; a method
///      returning a struct of the three resolved values can, and eager
///      resolution is already the shape both bindings hand back.
#[allow(async_fn_in_trait)]
pub trait AdminBackend {
    /// Close the admin client, joining its background task.
    ///
    /// `timeout` of `None` is Java's no-argument `close()`. Java's
    /// `Admin.close(Duration)` is `void` and so is the Rust `Admin::close`; the
    /// `Result` here exists because the gRPC backends can fail at the transport
    /// or binding level, which is a harness failure the scenario must see rather
    /// than a Kafka-level error.
    async fn close(&self, timeout: Option<Duration>) -> Result<(), KafkaError>;

    /// Short backend label used in assertion messages.
    fn name(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// RustNativeAdmin — drives src/admin in-process.
// ---------------------------------------------------------------------------

/// Backend that drives the native Rust [`Admin`] implementation directly. This
/// is the baseline the python / python_async / c backends are compared against.
///
/// Every method calls the real (sync) `Admin` method and then awaits the
/// `KafkaFuture`s it returned, which is exactly what the bindings do internally
/// before handing a result back to their caller.
pub struct RustNativeAdmin {
    admin: Box<dyn Admin>,
}

impl RustNativeAdmin {
    /// Build a network-backed admin client from `config`.
    pub fn from_config(config: &HashMap<String, String>) -> Result<Self, KafkaError> {
        let config = AdminClientConfig::from_properties(config)?;
        Ok(Self { admin: new_admin_client(config)? })
    }

    /// Build a broker-less [`MockAdminClient`] with `num_brokers` brokers.
    pub fn mock(num_brokers: i32) -> Self {
        Self { admin: Box::new(MockAdminClient::create(num_brokers)) }
    }
}

impl AdminBackend for RustNativeAdmin {
    async fn close(&self, timeout: Option<Duration>) -> Result<(), KafkaError> {
        self.admin.close(close_timeout(timeout)).await;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "rust"
    }
}
