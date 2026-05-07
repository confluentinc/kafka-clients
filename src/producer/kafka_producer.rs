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

//! Translation of `org.apache.kafka.clients.producer.KafkaProducer`.
//!
//! Phase 7c lands the **construction path** of `KafkaProducer`: the
//! struct, fields, public and pkg-private constructors, the spawn of
//! the [`Sender`] task, and the `Drop` impl that aborts the spawned
//! task. The [`Producer`] trait is **not** implemented yet — the trait
//! methods (`send`, `flush`, `close`, etc.) land in Phase 7d/7e. This
//! mirrors the Java field-init block in `KafkaProducer.java`
//! lines 332-467 (the visible-for-testing constructor) and the public
//! constructors at lines 283-329.
//!
//! ## Generic parameters
//!
//! `KafkaProducer<K, V, C>` is generic over:
//!
//! * `K` — the record key type (matches the Java `<K, V>` parameters);
//! * `V` — the record value type;
//! * `C` — the [`KafkaClient`] implementation. The Java translation of
//!   [`Sender`] is also generic over `C: KafkaClient` (Phase 6e), so the
//!   producer threads the same parameter through. Production code uses a
//!   real [`NetworkClient`]; tests inject [`crate::producer::internals::sender::tests::MockClientImpl`].
//!
//! ## Serializer dispatch: `Box<dyn Serializer<T>>`
//!
//! Java's `Serializer<K>` / `Serializer<V>` are interfaces resolved at
//! `configure` time. The Rust translation holds them as
//! `Box<dyn Serializer<T>>`:
//!
//! * Trait-object dispatch costs one extra indirection per `serialize`
//!   call. Serialization itself usually allocates (`Vec<u8>` / `String`),
//!   so the dispatch cost is dwarfed by the serializer body — measured to
//!   be a non-issue per `phase3b_serializer_design`.
//! * The alternative — adding generic `KS: Serializer<K>, VS: Serializer<V>`
//!   parameters — would push two extra type parameters all the way through
//!   the public surface (`KafkaProducer<K, V, C, KS, VS>`), forcing every
//!   caller to spell five type names.
//! * Java reads the serializer FQCN from config and reflectively
//!   instantiates it. Rust has no reflection; callers pass the serializer
//!   instance directly via [`KafkaProducer::with_serializers`] (or via the
//!   `key.serializer` / `value.serializer` config keys, with construction
//!   handled by the caller pre-Phase-7d).
//!
//! ## Sender task ownership
//!
//! The constructor spawns the [`Sender::run_loop`] future on `tokio::spawn`
//! **last** — after every other field has been initialised — so failure
//! anywhere earlier in the constructor returns `Err` without leaking a
//! background task. The resulting [`tokio::task::JoinHandle`] is stored on
//! the producer, and [`Drop`] aborts it (Phase 7e adds the async `close`
//! that gracefully drains and then awaits the handle).
//!
//! [`KafkaClient`]: crate::KafkaClient
//! [`NetworkClient`]: crate::NetworkClient
//! [`Sender`]: crate::producer::internals::sender::Sender

#![allow(dead_code)] // Phase 7d/7e wire send/flush/close on top of this skeleton.

use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::KafkaClient;
use crate::common::compress::Compression;
use crate::common::serialization::Serializer;
use crate::common::utils::log_context::LogContext;
use crate::common::utils::time::Time;
use crate::producer::internals::producer_interceptors::ProducerInterceptors;
use crate::producer::internals::producer_metadata::ProducerMetadata;
use crate::producer::internals::record_accumulator::RecordAccumulator;
use crate::producer::internals::transaction_manager::TransactionManager;
use crate::producer::partitioner::Partitioner;
use crate::producer::producer_config::ProducerConfig;

/// Java's `KafkaProducer.JMX_PREFIX`.
pub const JMX_PREFIX: &str = "kafka.producer";

/// Java's `KafkaProducer.NETWORK_THREAD_PREFIX`. Used to build the
/// background-task name so log lines correlate with the Java client.
pub const NETWORK_THREAD_PREFIX: &str = "kafka-producer-network-thread";

/// Java's `KafkaProducer.PRODUCER_METRIC_GROUP_NAME`.
pub const PRODUCER_METRIC_GROUP_NAME: &str = "producer-metrics";

/// A Kafka client that publishes records to the Kafka cluster.
///
/// Translation of `org.apache.kafka.clients.producer.KafkaProducer<K, V>`.
/// Phase 7c provides only the construction path; the [`Producer`] trait
/// methods land in Phase 7d (`send`) and 7e (`flush`, `close`,
/// `partitions_for`, `metrics`, etc.).
///
/// # Thread safety
///
/// Java's `KafkaProducer` is documented as thread-safe; Rust mirrors this
/// by making every field `Send + Sync` (typically `Arc<…>` over a
/// `Mutex` / `RwLock` / `AtomicX`). `&KafkaProducer` is sharable across
/// Tokio tasks; mutating methods are routed through the spawned
/// [`Sender`] task.
///
/// [`Producer`]: crate::producer::Producer
pub struct KafkaProducer<K, V, C: KafkaClient> {
    // ---- Identifiers / time / context ----
    /// Java: `private final String clientId`. Hot-path identifier kept as
    /// `Arc<str>` so per-batch `client_id` clones are reference bumps
    /// rather than allocations (CLAUDE.md rule 11).
    client_id: Arc<str>,
    /// Java: `private final Time time`.
    time: Arc<dyn Time>,
    /// Per-instance log prefix (`[Producer clientId=...]`). Java's
    /// `LogContext` carries the prefix and is passed to every component.
    log_context: LogContext,

    // ---- Config-derived scalars ----
    /// Java: `private final long maxBlockTimeMs`.
    max_block_time_ms: i64,
    /// Java: `private final long totalMemorySize`.
    total_memory_size: i64,
    /// Java: `private final int maxRequestSize`.
    max_request_size: i32,
    /// Java: `private final boolean partitionerIgnoreKeys`.
    partitioner_ignore_keys: bool,
    /// Java: `private final ProducerConfig producerConfig`. Retained for
    /// post-construction access (e.g. tests inspecting the resolved
    /// config).
    producer_config: ProducerConfig,
    /// Java: `private final Compression compression`. The producer holds
    /// the configured codec and forwards its [`compression_type()`] to
    /// the accumulator at construction time. The accumulator itself
    /// stores only [`crate::common::record::CompressionType`] (Phase 6d).
    ///
    /// [`compression_type()`]: Compression::compression_type
    compression: Box<dyn Compression>,

    // ---- Serializers (trait-object dispatch — see module docs). ----
    /// Java: `Plugin<Serializer<K>> keySerializerPlugin`. Plugin is the
    /// Java metrics shim and is dropped — the metrics integration is a
    /// milestone-deferred concern (PLAN.md Phase-6 skip note, same as
    /// [`ProducerInterceptors`]).
    key_serializer: Box<dyn Serializer<K>>,
    /// Java: `Plugin<Serializer<V>> valueSerializerPlugin`.
    value_serializer: Box<dyn Serializer<V>>,

    // ---- Pluggable producer collaborators ----
    /// Java: `Plugin<Partitioner> partitionerPlugin`. `None` selects the
    /// built-in adaptive partitioner (the Java field is null in that
    /// case; the [`RecordAccumulator`] handles per-topic
    /// [`crate::producer::internals::built_in_partitioner::BuiltInPartitioner`]
    /// instances internally).
    partitioner: Option<Arc<dyn Partitioner>>,
    /// Java: `private final ProducerInterceptors<K, V> interceptors`.
    interceptors: Arc<ProducerInterceptors<K, V>>,

    // ---- Phase-6 internals ----
    /// Java: `private final ProducerMetadata metadata`.
    metadata: Arc<ProducerMetadata>,
    /// Java: `private final RecordAccumulator accumulator`.
    accumulator: Arc<RecordAccumulator>,
    /// Java: `private final TransactionManager transactionManager`. Always
    /// `None` this milestone (Milestone-1 rejects `transactional.id` at
    /// config-validation time, Phase 7a).
    transaction_manager: Option<TransactionManager>,
    /// Java: `private final ApiVersions apiVersions`. Held as `Arc` so the
    /// network client and the (future) telemetry path can share it.
    api_versions: Arc<crate::ApiVersions>,

    // ---- Sender task lifecycle ----
    /// Java: `private final Sender sender`. The background runner that
    /// drives the produce-request lifecycle. We hold an [`Arc`] handle
    /// only to expose a few read-only inspectors (`is_running`,
    /// `force_close_arc`); the owning instance is moved into
    /// [`Self::sender_task`] at construction time and is otherwise
    /// inaccessible from the producer.
    ///
    /// We hold this as an `Arc<…>` of the wakeup half — Phase 7d/7e
    /// only need the `wakeup` / `force_close` / `initiate_close` surface
    /// of the [`Sender`]; the run loop itself runs on the spawned task.
    /// **Phase 7c stores no `Arc<Sender>` field** because the run loop
    /// borrows `&mut self` for its lifetime. The closeable handles
    /// (`running`, `force_close`) are held as `Arc<AtomicBool>` directly
    /// so [`Drop`] can flip them without re-entering the moved Sender.
    sender_running: Arc<std::sync::atomic::AtomicBool>,
    sender_force_close: Arc<std::sync::atomic::AtomicBool>,

    /// Java: `private final Sender.SenderThread ioThread`. Replaced with
    /// the `JoinHandle` of the `tokio::spawn` task running the Sender's
    /// run loop. `Option` so [`Drop`] can `take()` it during cleanup.
    sender_task: Option<JoinHandle<()>>,

    // ---- Phantom for the C parameter on the inherent skeleton ----
    /// `C` only appears in the [`Sender<C>`] type parameter, which is
    /// owned by [`Self::sender_task`]. After spawn the producer no longer
    /// references `C` directly. We carry a `PhantomData<fn() -> C>` so the
    /// type parameter survives compile-time checks without imposing
    /// `Send`/`Sync` bounds on `C` beyond what [`KafkaClient`] already
    /// requires.
    _client_marker: std::marker::PhantomData<fn() -> C>,
}

#[cfg(test)]
mod tests {
    //! Phase 7c skeleton tests live in the `kafka_producer.rs` file but
    //! are gated to compile-time checks until commit 5 lands the
    //! construction tests. The struct is skeleton-only at this point.

    use super::*;

    /// Compile-only check that the struct's type parameters compose.
    /// Real instantiation requires the public constructor (commit 2).
    fn _assert_type_compiles<K, V, C: KafkaClient>(_p: KafkaProducer<K, V, C>) {}
}
