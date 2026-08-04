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

// Staged ahead of its callers: the idempotent send path (Phase 4) and the public
// producer transaction API (Phase 6) are the only consumers, so under
// `#![deny(warnings)]` most of this file is dead code until then. Same mechanism
// as `txn_partition_map.rs:18`.
#![allow(dead_code)]

//! State for transactions, and the state needed to ensure idempotent production.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;

use crate::ApiVersions;
use crate::client_response::ClientResponse;
use crate::common::protocol::Errors;
use crate::common::record::RecordBatch;
use crate::common::requests::find_coordinator_request::CoordinatorType;
use crate::common::requests::produce_response::INVALID_OFFSET;
use crate::common::requests::{ConcreteResponse, InitProducerIdRequestBuilder, PartitionResponse};
use crate::common::utils::{LogContext, ProducerIdAndEpoch};
use crate::common::{KafkaError, TopicPartition};
use crate::init_producer_id_request_data::InitProducerIdRequestData;
use crate::producer::internals::{
    InFlightBatchKey, ProducerBatch, TransactionalRequestResult, TxnPartitionEntry, TxnPartitionMap,
};
use crate::{kafka_debug, kafka_error, kafka_info, kafka_trace};

/// Sentinel for "no transactional request is currently in flight".
const NO_INFLIGHT_REQUEST_CORRELATION_ID: i32 = -1;

/// The per-partition in-flight batch pool supplied by the batches' owners.
///
/// [`TxnPartitionEntry`] tracks in-flight batch *ordering keys* rather than
/// owning the batches (see `.claude/rules/producer-transactions.md` §7), so any
/// method that Java implements by mutating the tracked batches takes them from
/// their owner instead. Java reaches them through the entry's own references.
///
/// The map MUST be keyed by partition: [`InFlightBatchKey`] is
/// `(producer_id, producer_epoch, base_sequence)` and is **not**
/// partition-scoped, so two partitions routinely hold batches with identical
/// keys. Handing a cross-partition pool to a single entry would let it rewrite
/// another partition's batch.
///
/// A partition with no in-flight batches maps to an empty slice, or may be
/// absent from the map entirely — the two are equivalent. That case is normal,
/// not an error: see [`TransactionManager::bump_idempotent_producer_epoch`].
///
/// This is a type alias rather than a new type, so it adds no struct that Java
/// does not have (`definition-of-done.md` §7).
pub(crate) type InFlightBatchPool<'a> = HashMap<TopicPartition, Vec<&'a mut ProducerBatch>>;

/// Which side of the producer is driving a state transition.
///
/// Replaces Java's `Thread.currentThread() instanceof Sender.SenderThread`
/// (`TransactionManager.java:287-289`), which has no Rust analogue. See
/// `.claude/rules/producer-transactions.md` §1: the poison-vs-throw distinction
/// is load-bearing for the transactional guarantee, so it is threaded explicitly
/// rather than inferred from task identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Caller {
    /// The application task, i.e. a `Producer` API call.
    App,
    /// The Sender task.
    Sender,
}

impl Caller {
    /// Whether an invalid transition should poison the state machine before
    /// returning the error.
    ///
    /// Translated from `shouldPoisonStateOnInvalidTransition()` (Java 287).
    /// An invalid transition detected on the Sender side means the
    /// transaction's integrity is already compromised, so the manager moves to
    /// [`State::FatalError`]; on the application side the state is left alone so
    /// the user can recover.
    fn should_poison_state_on_invalid_transition(self) -> bool {
        matches!(self, Self::Sender)
    }
}

/// The internal state of the transaction manager.
///
/// Translated from `TransactionManager.State` (Java 151-189).
///
/// All nine Java variants are present even though the Phase-3 idempotence slice
/// can only reach five of them (see [`State::is_transition_valid`] for why the
/// transition table is translated whole).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    /// No producer id has been requested yet.
    Uninitialized,
    /// An `InitProducerId` request is outstanding.
    Initializing,
    /// A producer id has been acquired; no transaction is in progress.
    Ready,
    /// A transaction has been started.
    InTransaction,
    /// A transaction has been prepared for a two-phase commit (KIP-939).
    PreparedTransaction,
    /// An `EndTxn(COMMIT)` is in progress.
    CommittingTransaction,
    /// An `EndTxn(ABORT)` is in progress.
    AbortingTransaction,
    /// An error occurred that requires the transaction to be aborted.
    AbortableError,
    /// An unrecoverable error occurred.
    FatalError,
}

impl State {
    /// Whether a transition from `source` to `self` is permitted.
    ///
    /// Translated verbatim from `State.isTransitionValid(State, State)`
    /// (Java 162-188), which switches on the **target** — `self` here — and
    /// enumerates the permitted sources.
    ///
    /// The whole nine-variant table is translated even though Phase 3 reaches
    /// only five states, because the table is a single self-contained piece of
    /// logic and splitting it would mean writing it twice. Note two arms that
    /// are easy to get wrong: [`Self::AbortableError`] permits itself as a
    /// source (a self-loop), and [`Self::Ready`] does **not** — `READY → READY`
    /// is invalid.
    fn is_transition_valid(&self, source: State) -> bool {
        match self {
            Self::Uninitialized => source == Self::Ready || source == Self::AbortableError,
            Self::Initializing => {
                source == Self::Uninitialized
                    || source == Self::CommittingTransaction
                    || source == Self::AbortingTransaction
            },
            Self::Ready => {
                source == Self::Initializing
                    || source == Self::CommittingTransaction
                    || source == Self::AbortingTransaction
            },
            Self::InTransaction => source == Self::Ready,
            Self::PreparedTransaction => source == Self::InTransaction || source == Self::Initializing,
            Self::CommittingTransaction => source == Self::InTransaction || source == Self::PreparedTransaction,
            Self::AbortingTransaction => {
                source == Self::InTransaction || source == Self::PreparedTransaction || source == Self::AbortableError
            },
            Self::AbortableError => {
                source == Self::InTransaction
                    || source == Self::CommittingTransaction
                    || source == Self::AbortableError
                    || source == Self::Initializing
            },
            // We can transition to FATAL_ERROR unconditionally.
            // FATAL_ERROR is never a valid starting state for any transition. So the only option is to close the
            // producer or do purely non transactional requests.
            Self::FatalError => true,
        }
    }
}

impl fmt::Display for State {
    /// Formats as the Java enum constant name.
    ///
    /// Java's invalid-transition message interpolates `State.name()`, so the
    /// exact spelling is part of the error contract that tests assert on.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Uninitialized => "UNINITIALIZED",
            Self::Initializing => "INITIALIZING",
            Self::Ready => "READY",
            Self::InTransaction => "IN_TRANSACTION",
            Self::PreparedTransaction => "PREPARED_TRANSACTION",
            Self::CommittingTransaction => "COMMITTING_TRANSACTION",
            Self::AbortingTransaction => "ABORTING_TRANSACTION",
            Self::AbortableError => "ABORTABLE_ERROR",
            Self::FatalError => "FATAL_ERROR",
        };
        f.write_str(name)
    }
}

/// The order in which pending transactional requests must be sent.
///
/// Translated from `TransactionManager.Priority` (Java 195-207).
///
/// We use the priority to determine the order in which requests need to be sent out. For instance, if we have
/// a pending FindCoordinator request, that must always go first. Next, If we need a producer id, that must go second.
/// The endTxn request must always go last, unless we are bumping the epoch (a special case of InitProducerId) as
/// part of ending the transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Priority {
    /// `FindCoordinator`.
    FindCoordinator = 0,
    /// `InitProducerId` for the initial producer id.
    InitProducerId = 1,
    /// `AddPartitionsToTxn` / `AddOffsetsToTxn` / `TxnOffsetCommit`.
    AddPartitionsOrOffsets = 2,
    /// `EndTxn`.
    EndTxn = 3,
    /// `InitProducerId` sent to bump the epoch.
    EpochBump = 4,
}

/// The request-specific half of a pending transactional request.
///
/// Java models the six request handlers as subclasses of the abstract inner
/// class `TxnRequestHandler` (Java 1345-1459). Rust has neither inheritance nor
/// an inner class's implicit `TransactionManager.this` reference, so the
/// hierarchy becomes an enum carried by [`TxnRequestHandler`], and the methods
/// that Java implements on the subclass while touching the enclosing manager
/// (`handleResponse`, `coordinatorType`) become methods on
/// [`TransactionManager`].
///
/// This mirrors how the crate already translates Java's `AbstractRequest` /
/// `AbstractResponse` hierarchies — as the `ConcreteRequest` / `ConcreteResponse`
/// enums — so it introduces no pattern the codebase does not already use.
///
/// Only `InitProducerIdHandler` is reachable from the idempotence slice; the
/// other five arrive in Phase 5.
pub(crate) enum TxnRequestHandlerKind {
    /// `InitProducerIdHandler` (Java 1461-1539).
    InitProducerId {
        /// The request being sent.
        builder: InitProducerIdRequestBuilder,
        /// Whether this request bumps an existing epoch rather than acquiring a
        /// producer id for the first time.
        is_epoch_bump: bool,
    },
}

/// A pending transactional request and its completion handle.
///
/// Translated from the abstract inner class `TxnRequestHandler`
/// (Java 1345-1459). See [`TxnRequestHandlerKind`] for why the subclass
/// hierarchy became an enum.
pub(crate) struct TxnRequestHandler {
    /// The handle the application awaits.
    ///
    /// `Arc` because `handle_cached_transaction_request_result` (Phase 5) must
    /// hand the *same* result object to both the caller and the
    /// pending-transition slot — see
    /// `.claude/rules/producer-transactions.md` §5.
    result: Arc<TransactionalRequestResult>,
    /// Whether this request has already been retried.
    is_retry: bool,
    /// How long to back off before retrying this request.
    ///
    /// A field rather than a read-through to the manager because
    /// `AddPartitionsToTxnHandler` (Phase 5) overrides it per instance
    /// (Java 1543).
    retry_backoff_ms: i64,
    /// The request-specific state.
    kind: TxnRequestHandlerKind,
}

impl TxnRequestHandler {
    /// Creates a handler with a fresh result for `operation`.
    ///
    /// Corresponds to `TxnRequestHandler(String operation)` (Java 1353).
    fn new(operation: &str, retry_backoff_ms: i64, kind: TxnRequestHandlerKind) -> Self {
        Self {
            result: Arc::new(TransactionalRequestResult::new(operation)),
            is_retry: false,
            retry_backoff_ms,
            kind,
        }
    }

    /// The handle the application awaits.
    pub(crate) fn result(&self) -> &Arc<TransactionalRequestResult> {
        &self.result
    }

    /// The request-specific state.
    pub(crate) fn kind(&self) -> &TxnRequestHandlerKind {
        &self.kind
    }

    /// The request builder, for the Sender to build and send.
    ///
    /// Corresponds to the abstract `requestBuilder()` (Java 1454). Returns
    /// `&mut` because [`crate::common::requests::RequestBuilder::build`] takes
    /// `&mut self` in this crate.
    pub(crate) fn request_builder(&mut self) -> &mut InitProducerIdRequestBuilder {
        match &mut self.kind {
            TxnRequestHandlerKind::InitProducerId { builder, .. } => builder,
        }
    }

    /// The priority of this request.
    ///
    /// Corresponds to the abstract `priority()` (Java 1458). Note
    /// `InitProducerIdHandler.priority()` (Java 1477) is *dynamic*: an epoch
    /// bump sorts after `EndTxn`, an initial acquisition before it.
    pub(crate) fn priority(&self) -> Priority {
        match &self.kind {
            TxnRequestHandlerKind::InitProducerId { is_epoch_bump, .. } => {
                if *is_epoch_bump {
                    Priority::EpochBump
                } else {
                    Priority::InitProducerId
                }
            },
        }
    }

    /// Whether this is an `EndTxn` request.
    ///
    /// Corresponds to `isEndTxn()` (Java 1450), whose base implementation
    /// returns `false`; only `EndTxnHandler` (Phase 5) overrides it.
    pub(crate) fn is_end_txn(&self) -> bool {
        match &self.kind {
            TxnRequestHandlerKind::InitProducerId { .. } => false,
        }
    }

    /// Whether this request has already been retried.
    ///
    /// Corresponds to `isRetry()` (Java 1446).
    pub(crate) fn is_retry(&self) -> bool {
        self.is_retry
    }

    /// Marks this request as a retry.
    ///
    /// Corresponds to `setRetry()` (Java 1442).
    fn set_retry(&mut self) {
        self.is_retry = true;
    }

    /// How long to back off before retrying this request.
    ///
    /// Corresponds to `retryBackoffMs()` (Java 1401).
    pub(crate) fn retry_backoff_ms(&self) -> i64 {
        self.retry_backoff_ms
    }

    /// The operation name this handler's result was created for.
    pub(crate) fn operation(&self) -> &str {
        self.result.operation()
    }

    /// Fails this handler's result without touching the manager's state.
    ///
    /// Corresponds to `fail(RuntimeException)` (Java 1390). Distinct from
    /// [`TransactionManager::fatal_error`] and
    /// [`TransactionManager::abortable_error`], which also transition.
    fn fail(&self, error: KafkaError) {
        self.result.fail(error);
    }
}

impl fmt::Debug for TxnRequestHandler {
    /// Formats as the wrapped request builder.
    ///
    /// Java's log statements interpolate `requestBuilder()`, whose
    /// `AbstractRequest.Builder.toString()` prints the request data. This crate
    /// only exposes the builder as `&mut` (because `RequestBuilder::build` takes
    /// `&mut self`), so the equivalent is reached through `Debug` instead.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            TxnRequestHandlerKind::InitProducerId { builder, .. } => write!(f, "{builder:?}"),
        }
    }
}

/// A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
///
/// Translated from
/// `org.apache.kafka.clients.producer.internals.TransactionManager`.
///
/// # Scope
///
/// This is the idempotence slice (Milestone 11 Phase 3). The transactional state
/// machine, the remaining five request handlers, the priority queue, KIP-890
/// Transaction V2 and KIP-939 two-phase commit arrive in Phase 5. Construction
/// with a `transactional_id` is refused until then — see [`Self::new`].
///
/// # Lock topology
///
/// `.claude/rules/producer-transactions.md` §2 requires a deliberate split when
/// this type becomes shared: four Java fields (`inFlightRequestCorrelationId`,
/// `transactionCoordinator`, `consumerGroupCoordinator`,
/// `coordinatorSupportsBumpingEpoch`) plus `pendingRequests` are non-volatile
/// and not consistently guarded by Java's `synchronized` blocks, because only
/// the Sender thread touches them. They must NOT go behind the shared mutex.
///
/// Phase 3 introduces no mutex, so the rule cannot be *violated* here — but it
/// is already **constrained**, and that is the operative point. PLAN §10.5
/// deviation 2 hosts `on_complete` / `handle_response` on the manager (Java's
/// inner class reaches its owner through an implicit `TransactionManager.this`,
/// which Rust cannot express), so every touch of `pending_requests` and
/// `in_flight_request_correlation_id` is a `&mut TransactionManager` method.
/// Complying with §2 in Phase 4 is therefore not a field move: it means
/// reshaping ten signatures. PLAN §10.5 deviation 7 names them and PLAN
/// §Phase-4 budgets the work.
///
/// Phase 4 wraps the manager as `Arc<Mutex<TransactionManager>>` (see PLAN
/// §6.3) and is where the split has to be made; the fields that belong to the
/// Sender are marked below.
///
/// # Send-path allocations
///
/// The methods the drain path reaches — [`Self::sequence_number`],
/// [`Self::increment_sequence_number`], [`Self::add_in_flight_batch`],
/// [`Self::maybe_update_producer_id_and_epoch`] — run once per **batch**, not
/// per record, and allocate no more than Java: a `TopicPartition` clone only
/// where Java also inserts into a map or set. Nothing here is per-record, so
/// `definition-of-done.md` §10's per-message budget is unaffected.
///
/// Two methods collect a `Vec` of partition keys where Java iterates its
/// collection in place ([`Self::bump_idempotent_producer_epoch`] and
/// [`Self::maybe_resolve_sequences`]), because the loop bodies need `&mut self`.
/// Both run once per `Sender.runOnce`, i.e. per network poll, and only over
/// partitions in an error state.
pub(crate) struct TransactionManager {
    log_context: LogContext,
    /// `None` for a purely idempotent producer.
    transactional_id: Option<String>,
    transaction_timeout_ms: i32,
    /// Read by `handleCoordinatorReady` and `maybeUpdateTransactionV2Enabled`,
    /// both Phase 5. Held from Phase 3 so the constructor mirrors Java's.
    api_versions: Arc<ApiVersions>,

    txn_partition_map: TxnPartitionMap,

    // If a batch bound for a partition expired locally after being sent at least once, the partition is considered
    // to have an unresolved state. We keep track of such partitions here, and cannot assign any more sequence numbers
    // for this partition until the unresolved state gets cleared. This may happen if other inflight batches returned
    // successfully (indicating that the expired batch actually made it to the broker). If we don't get any successful
    // responses for the partition once the inflight request count falls to zero, we reset the producer id and
    // consequently clear this data structure as well.
    // The value of the map is the sequence number of the batch following the expired one, computed by adding its
    // record count to its sequence number. This is used to tell if a subsequent batch is the one immediately following
    // the expired one.
    partitions_with_unresolved_sequences: HashMap<TopicPartition, i32>,

    // The partitions that have received an error that triggers an epoch bump. When the epoch is bumped, these
    // partitions will have the sequences of their in-flight batches rewritten
    partitions_to_rewrite_sequences: HashSet<TopicPartition>,

    /// Sender-owned (rules §2).
    ///
    /// Java's `PriorityQueue<TxnRequestHandler>` ordered by
    /// [`Priority`] (Java 224). A `VecDeque` suffices for the idempotence
    /// slice: the only handler it can enqueue is `InitProducerId`, and
    /// [`Self::bump_idempotent_epoch_and_reset_id_if_needed`] enqueues at most
    /// one at a time (it is guarded on `!has_producer_id()`), so FIFO and
    /// priority order coincide. Phase 5 introduces the ordered queue along with
    /// the handlers that make ordering observable.
    pending_requests: VecDeque<TxnRequestHandler>,

    // This is used by the TxnRequestHandlers to control how long to back off before a given request is retried.
    // For instance, this value is lowered by the AddPartitionsToTxnHandler when it receives a CONCURRENT_TRANSACTIONS
    // error for the first AddPartitionsRequest in a transaction.
    retry_backoff_ms: i64,

    /// Sender-owned (rules §2).
    in_flight_request_correlation_id: i32,

    current_state: State,
    last_error: Option<KafkaError>,
    producer_id_and_epoch: ProducerIdAndEpoch,
    client_side_epoch_bump_required: bool,
    /// Always `false` in Phase 3: only `maybeUpdateTransactionV2Enabled`
    /// (Java 492, Phase 5) sets it, and that method is transactional. Kept as a
    /// field so [`Self::set_producer_id_and_epoch`]'s log-level fork
    /// (Java 605) can be translated verbatim rather than approximated.
    is_transaction_v2_enabled: bool,
    enable_2pc: bool,
}

impl TransactionManager {
    /// Creates a transaction manager.
    ///
    /// # Errors
    ///
    /// MILESTONE-11 GUARD: returns [`Errors::UnsupportedVersion`] when
    /// `transactional_id` is `Some`. Java's constructor accepts it, but this
    /// phase translates only the idempotence slice, so every transactional
    /// entry point and the transactional arm of the five internally-forked
    /// methods (`maybeTransitionToErrorState`, `handleFailedBatch`,
    /// `maybeResolveSequences`, `nextRequest`, `canRetry`) are absent. Refusing
    /// construction makes those paths unreachable instead of silently taking the
    /// idempotent branch, which CLAUDE.md §5 requires. It mirrors the guard
    /// already in `KafkaProducer::from_config` (PLAN §7.1); Phase 5 removes it.
    pub(crate) fn new(
        log_context: LogContext,
        transactional_id: Option<String>,
        transaction_timeout_ms: i32,
        retry_backoff_ms: i64,
        api_versions: Arc<ApiVersions>,
        enable_2pc: bool,
    ) -> Result<Self, KafkaError> {
        if transactional_id.is_some() {
            return Err(KafkaError::unsupported_version(
                "The transactional producer is not yet implemented in this client \
                 (Milestone 11, Phase 5); construct the TransactionManager without a \
                 transactional id.",
            ));
        }
        Ok(Self {
            txn_partition_map: TxnPartitionMap::new(log_context.clone()),
            log_context,
            transactional_id,
            transaction_timeout_ms,
            api_versions,
            partitions_with_unresolved_sequences: HashMap::new(),
            partitions_to_rewrite_sequences: HashSet::new(),
            pending_requests: VecDeque::new(),
            retry_backoff_ms,
            in_flight_request_correlation_id: NO_INFLIGHT_REQUEST_CORRELATION_ID,
            current_state: State::Uninitialized,
            last_error: None,
            producer_id_and_epoch: ProducerIdAndEpoch::NONE,
            client_side_epoch_bump_required: false,
            is_transaction_v2_enabled: false,
            enable_2pc,
        })
    }

    // -- Identity and configuration ----------------------------------------

    /// The configured transactional id, or `None` for an idempotent producer.
    ///
    /// Corresponds to `transactionalId()` (Java 472).
    pub(crate) fn transactional_id(&self) -> Option<&str> {
        self.transactional_id.as_deref()
    }

    /// Whether a producer id has been acquired.
    ///
    /// Corresponds to `hasProducerId()` (Java 476).
    pub(crate) fn has_producer_id(&self) -> bool {
        self.producer_id_and_epoch.is_valid()
    }

    /// Whether this producer is transactional.
    ///
    /// Corresponds to `isTransactional()` (Java 480).
    pub(crate) fn is_transactional(&self) -> bool {
        self.transactional_id.is_some()
    }

    /// Whether two-phase commit is enabled (KIP-939).
    ///
    /// Corresponds to `is2PCEnabled()` (Java 510).
    pub(crate) fn is_2pc_enabled(&self) -> bool {
        self.enable_2pc
    }

    /// The configured transaction timeout.
    ///
    /// Java reads the field directly from `initializeTransactions` (Java 319);
    /// there is no accessor. Exposed here so the field has a reader before
    /// Phase 5 adds that method.
    pub(crate) fn transaction_timeout_ms(&self) -> i32 {
        self.transaction_timeout_ms
    }

    /// The API versions this manager was constructed with.
    ///
    /// Java reads the field directly from `handleCoordinatorReady` (Java 1104)
    /// and `maybeUpdateTransactionV2Enabled` (Java 493), both Phase 5.
    pub(crate) fn api_versions(&self) -> &Arc<ApiVersions> {
        &self.api_versions
    }

    // -- Error state --------------------------------------------------------

    /// The error that moved this manager into an error state, if any.
    ///
    /// Corresponds to `lastError()` (Java 462).
    pub(crate) fn last_error(&self) -> Option<&KafkaError> {
        self.last_error.as_ref()
    }

    /// Whether the manager is in either error state.
    ///
    /// Corresponds to `hasError()` (Java 522).
    pub(crate) fn has_error(&self) -> bool {
        self.current_state == State::AbortableError || self.current_state == State::FatalError
    }

    /// Whether the manager is in an unrecoverable error state.
    ///
    /// Corresponds to `hasFatalError()` (Java 986).
    pub(crate) fn has_fatal_error(&self) -> bool {
        self.current_state == State::FatalError
    }

    /// Whether the manager is in an abortable error state.
    ///
    /// Corresponds to `hasAbortableError()` (Java 991).
    pub(crate) fn has_abortable_error(&self) -> bool {
        self.current_state == State::AbortableError
    }

    /// The current state. Visible for testing, as Java's package-private field
    /// access is.
    #[cfg(test)]
    fn current_state(&self) -> State {
        self.current_state
    }

    /// Enqueues an `InitProducerId` handler unconditionally and hands back its
    /// result, so tests can observe the bulk-failure methods
    /// ([`Self::fail_pending_requests`], [`Self::authentication_failed`],
    /// [`Self::close`]) with a non-empty queue.
    ///
    /// Java's tests reach this by driving `Sender.runOnce` against a
    /// `MockClient`; `enqueue_request` is private and
    /// `bump_idempotent_epoch_and_reset_id_if_needed` is state-guarded, so a
    /// test-only door is needed instead.
    #[cfg(test)]
    fn force_enqueue_init_producer_id_for_test(&mut self) -> Arc<TransactionalRequestResult> {
        let mut request_data = InitProducerIdRequestData::new();
        request_data.set_transactional_id(None).set_transaction_timeout_ms(i32::MAX);
        let handler = TxnRequestHandler::new(
            "InitProducerId",
            self.retry_backoff_ms,
            TxnRequestHandlerKind::InitProducerId {
                builder: InitProducerIdRequestBuilder::new(request_data),
                is_epoch_bump: false,
            },
        );
        let result = Arc::clone(handler.result());
        self.enqueue_request(handler);
        result
    }

    /// Moves to [`State::FatalError`].
    ///
    /// Corresponds to `transitionToFatalError(RuntimeException)` (Java 541).
    ///
    /// `caller` is a parameter rather than a constant because Java reaches this
    /// from both sides: `TxnRequestHandler.fatalError` (Java 1359) runs on the
    /// Sender, while `KafkaProducer`'s transactional API (Phase 6) reaches it
    /// from the application task.
    pub(crate) fn transition_to_fatal_error(&mut self, error: KafkaError, caller: Caller) -> Result<(), KafkaError> {
        kafka_info!(self.log_context, "Transiting to fatal error state due to {}", error);
        self.transition_to(State::FatalError, Some(error), caller)
        // Java also fails `pendingTransition` here (Java 545-547).
        // `pendingTransition` is only ever set by
        // `handleCachedTransactionRequestResult` (Java 1281), which begins with
        // `ensureTransactional()`, so it is always null for an idempotent
        // producer. Phase 5 adds the field and this branch together.
    }

    /// Moves to [`State::AbortableError`].
    ///
    /// Corresponds to `transitionToAbortableError(RuntimeException)`
    /// (Java 530).
    ///
    /// Reachable from the idempotence slice despite the "abortable" name:
    /// `InitProducerIdHandler.handleResponse` calls `abortableError` for
    /// `CLUSTER_AUTHORIZATION_FAILED` (Java 1524-1528) **without** checking
    /// `isTransactional()`, and a non-transactional `InitProducerId` gets that
    /// error code when the principal lacks `IdempotentWrite` on the cluster. The
    /// transition itself is permitted because the manager is in
    /// [`State::Initializing`] when the response arrives, and
    /// `INITIALIZING → ABORTABLE_ERROR` is a valid arm of the table (Java 180).
    pub(crate) fn transition_to_abortable_error(
        &mut self,
        error: KafkaError,
        caller: Caller,
    ) -> Result<(), KafkaError> {
        if self.current_state == State::AbortingTransaction {
            kafka_debug!(
                self.log_context,
                "Skipping transition to abortable error state since the transaction is already being aborted. \
                 Underlying exception: {}",
                error
            );
            return Ok(());
        }

        kafka_info!(self.log_context, "Transiting to abortable error state due to {}", error);
        self.transition_to(State::AbortableError, Some(error), caller)
    }

    /// Moves back to [`State::Uninitialized`], clearing [`Self::last_error`], so
    /// a fresh `InitProducerId` can be requested.
    ///
    /// Translated from `transitionToUninitialized(RuntimeException)` (Java 756).
    ///
    /// This is the **exit** from [`State::AbortableError`] on the idempotent
    /// path, and the reason the table admits `UNINITIALIZED ← ABORTABLE_ERROR`
    /// (Java 165). `Sender.runOnce` tests `hasAbortableError()`
    /// (`Sender.java:325`) and calls `shouldHandleAuthorizationError(lastError)`
    /// (`:351-360`), which runs [`Self::fail_pending_requests`],
    /// `maybeAbortBatches` and then this method. For an idempotent producer the
    /// `instanceof` test at `Sender.java:352` is **always** satisfied — the only
    /// entry to `ABORTABLE_ERROR` is `InitProducerIdHandler`'s authorization arm
    /// (Java 1524-1528), whose `lastError` is exactly one of the two exceptions
    /// that test matches — so Java always recovers here. The Java comment at
    /// `Sender.java:348-350` states the intent: "transition the state to
    /// UNINITIALIZED so that the user doesn't need to instantiate the producer
    /// again."
    ///
    /// Takes no error argument. Java passes the exception solely to
    /// `pendingTransition.result.fail(..)` (Java 759), and `pendingTransition` is
    /// only ever set by `handleCachedTransactionRequestResult` (Java 1281), which
    /// begins with `ensureTransactional()` — so it is always null for an
    /// idempotent producer. Phase 5 adds the field and the parameter together,
    /// the same treatment [`Self::maybe_resolve_sequences`] gets for its
    /// [`Caller`].
    pub(crate) fn transition_to_uninitialized(&mut self, caller: Caller) -> Result<(), KafkaError> {
        self.transition_to(State::Uninitialized, None, caller)?;
        // Redundant — `transition_to` already clears `last_error` on a
        // non-error target — but Java assigns it explicitly (Java 761), so the
        // assignment is kept rather than silently relied upon.
        self.last_error = None;
        Ok(())
    }

    /// Fails every pending transactional request with `error` and moves to
    /// [`State::AbortableError`] once per request.
    ///
    /// Translated from `failPendingRequests(RuntimeException)` (Java 944).
    /// Reached from `Sender.shouldHandleAuthorizationError`
    /// (`Sender.java:354`), immediately before
    /// [`Self::transition_to_uninitialized`].
    ///
    /// Java iterates the live `PriorityQueue` and does **not** clear it, so a
    /// failed handler stays queued; that is preserved. Iterating by index rather
    /// than with an iterator is required because
    /// [`Self::transition_to_abortable_error`] needs `&mut self`, which Java gets
    /// for free from the enclosing monitor.
    pub(crate) fn fail_pending_requests(&mut self, error: &KafkaError, caller: Caller) -> Result<(), KafkaError> {
        for index in 0..self.pending_requests.len() {
            // Java: handler.abortableError(exception), i.e. result.fail(e) then
            // transitionToAbortableError(e), per handler and in that order.
            self.pending_requests[index].fail(error.clone());
            self.transition_to_abortable_error(error.clone(), caller)?;
        }
        Ok(())
    }

    /// Fails every pending transactional request with `error` and moves to
    /// [`State::FatalError`].
    ///
    /// Translated from `authenticationFailed(AuthenticationException)`
    /// (Java 939). Reached from `Sender.runOnce`'s
    /// `catch (AuthenticationException e)` (`Sender.java:336-340`), which wraps
    /// the whole transactional block. Reachable on the idempotent path:
    /// `maybeSendAndPollTransactionalRequest` takes the `coordinatorType == null`
    /// branch (`Sender.java:479-484`) and still calls `awaitNodeReady`, i.e.
    /// `NetworkClientUtils.awaitReady`, which throws `AuthenticationException`.
    ///
    /// Java's parameter is narrowed to `AuthenticationException`. This crate has
    /// no `KafkaError` variant for that family — a genuine authentication failure
    /// is an [`AuthenticationError`](crate::common::network::authentication_error::AuthenticationError)
    /// carried inside an `io::Error` at the transport layer — so the parameter is
    /// a plain [`KafkaError`] and the caller supplies it. The body treats it as a
    /// `RuntimeException` in Java too.
    pub(crate) fn authentication_failed(&mut self, error: &KafkaError, caller: Caller) -> Result<(), KafkaError> {
        for index in 0..self.pending_requests.len() {
            // Java: request.fatalError(e), i.e. result.fail(e) then
            // transitionToFatalError(e).
            self.pending_requests[index].fail(error.clone());
            self.transition_to_fatal_error(error.clone(), caller)?;
        }
        Ok(())
    }

    /// Fails every pending transactional request because the producer is being
    /// closed forcefully.
    ///
    /// Translated from `close()` (Java 949). Reached from `Sender.run`'s
    /// `forceClose` branch (`Sender.java:287-293`).
    ///
    /// Not reachable from [`State::AbortableError`] — this is the forced-shutdown
    /// path — but it is reachable for a purely idempotent producer, and without
    /// it a force close would leave a pending `InitProducerId`'s
    /// [`TransactionalRequestResult`] never completed, i.e. a hanging future,
    /// which CLAUDE.md §5 forbids. Landed here rather than with its
    /// `Sender`-side call site in Phase 6 for that reason.
    pub(crate) fn close(&mut self, caller: Caller) -> Result<(), KafkaError> {
        let shutdown_error = KafkaError::with_message(Errors::UnknownServerError, "The producer closed forcefully");
        for index in 0..self.pending_requests.len() {
            self.pending_requests[index].fail(shutdown_error.clone());
            self.transition_to_fatal_error(shutdown_error.clone(), caller)?;
        }
        // Java also fails `pendingTransition` here (Java 953-955); see
        // [`Self::transition_to_fatal_error`] for why that field arrives in
        // Phase 5.
        Ok(())
    }

    /// Moves the state machine to `target`.
    ///
    /// Translated from `transitionTo(State, RuntimeException)` (Java 1118).
    /// The no-argument overload (Java 1114) is expressed by passing `None`.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::IllegalState`] when the transition is not permitted.
    ///   When `caller` is [`Caller::Sender`] the manager first moves to
    ///   [`State::FatalError`] and records the error as [`Self::last_error`]
    ///   ("poisons" itself).
    /// - [`KafkaError::IllegalArgument`] when moving to an error state without
    ///   an error, mirroring Java's `IllegalArgumentException` (Java 1133).
    fn transition_to(&mut self, target: State, error: Option<KafkaError>, caller: Caller) -> Result<(), KafkaError> {
        if !target.is_transition_valid(self.current_state) {
            let id_string = match &self.transactional_id {
                Some(id) => format!("TransactionalId {id}: "),
                None => String::new(),
            };
            let message = format!(
                "{id_string}Invalid transition attempted from state {} to state {target}",
                self.current_state
            );

            let error = KafkaError::illegal_state(message);
            if caller.should_poison_state_on_invalid_transition() {
                self.current_state = State::FatalError;
                self.last_error = Some(error.clone());
            }
            return Err(error);
        } else if target == State::FatalError || target == State::AbortableError {
            match error {
                None => {
                    return Err(KafkaError::illegal_argument(format!(
                        "Cannot transition to {target} with a null exception"
                    )));
                },
                Some(error) => self.last_error = Some(error),
            }
        } else {
            self.last_error = None;
        }

        match &self.last_error {
            Some(last_error) => kafka_debug!(
                self.log_context,
                "Transition from state {} to error state {} ({})",
                self.current_state,
                target,
                last_error
            ),
            None => kafka_debug!(self.log_context, "Transition from state {} to {}", self.current_state, target),
        }

        self.current_state = target;
        Ok(())
    }

    /// Rejects a transactional operation on a non-transactional producer.
    ///
    /// Corresponds to `ensureTransactional()` (Java 1147).
    fn ensure_transactional(&self) -> Result<(), KafkaError> {
        if !self.is_transactional() {
            return Err(KafkaError::illegal_state(
                "Transactional method invoked on a non-transactional producer.",
            ));
        }
        Ok(())
    }

    /// Returns the recorded error if the manager is in an error state.
    ///
    /// Translated from `maybeFailWithError()` (Java 1152).
    ///
    /// Java chains `lastError` as the cause of the thrown exception for the
    /// `IllegalStateException` and bare-`KafkaException` cases.
    /// [`KafkaError`] has no cause chain, and Java's `getMessage()` does not
    /// include the cause either, so the message text is reproduced exactly and
    /// the cause stays reachable through [`Self::last_error`].
    fn maybe_fail_with_error(&self) -> Result<(), KafkaError> {
        if !self.has_error() {
            return Ok(());
        }

        // Java interpolates a null transactionalId as the text "null".
        let transactional_id = self.transactional_id.as_deref().unwrap_or("null");
        let producer_id_and_epoch = self.producer_id_and_epoch;

        match &self.last_error {
            // for ProducerFencedException, do not wrap it as a KafkaException
            // but create a new instance without the call trace since it was not thrown because of the current call
            Some(error) if error.error() == Errors::ProducerFenced => Err(KafkaError::with_message(
                Errors::ProducerFenced,
                format!(
                    "Producer with transactionalId '{transactional_id}' and {producer_id_and_epoch} has been \
                         fenced by another producer with the same transactionalId"
                ),
            )),
            Some(error) if error.error() == Errors::InvalidProducerEpoch => Err(KafkaError::with_message(
                Errors::InvalidProducerEpoch,
                format!(
                    "Producer with transactionalId '{transactional_id}' and {producer_id_and_epoch} attempted to \
                         produce with an old epoch"
                ),
            )),
            Some(KafkaError::IllegalState(_)) => Err(KafkaError::illegal_state(format!(
                "Producer with transactionalId '{transactional_id}' and {producer_id_and_epoch} cannot execute \
                     transactional method because of previous invalid state transition attempt"
            ))),
            // Java: new KafkaException("Cannot execute transactional method because we are in an error state",
            // lastError). A bare KafkaException carries no wire code, which this
            // crate spells as `Errors::UnknownServerError` (cf.
            // `record_accumulator.rs:1095`).
            _ => Err(KafkaError::with_message(
                Errors::UnknownServerError,
                "Cannot execute transactional method because we are in an error state",
            )),
        }
    }

    /// Records an error that arose on the send path, moving to an error state
    /// when the error class requires it.
    ///
    /// Translated from `maybeTransitionToErrorState(RuntimeException)`
    /// (Java 764).
    ///
    /// The five error classes Java tests with `instanceof` map onto wire codes:
    /// `ClusterAuthorizationException` → [`Errors::ClusterAuthorizationFailed`],
    /// `TransactionalIdAuthorizationException` →
    /// [`Errors::TransactionalIdAuthorizationFailed`],
    /// `ProducerFencedException` → [`Errors::ProducerFenced`],
    /// `UnsupportedVersionException` → [`Errors::UnsupportedVersion`],
    /// `InvalidPidMappingException` → [`Errors::InvalidProducerIdMapping`].
    pub(crate) fn maybe_transition_to_error_state(
        &mut self,
        error: &KafkaError,
        caller: Caller,
    ) -> Result<(), KafkaError> {
        if matches!(
            error.error(),
            Errors::ClusterAuthorizationFailed
                | Errors::TransactionalIdAuthorizationFailed
                | Errors::ProducerFenced
                | Errors::UnsupportedVersion
                | Errors::InvalidProducerIdMapping
        ) {
            return self.transition_to_fatal_error(error.clone(), caller);
        }
        if self.is_transactional() {
            // Java 771-785 converts retriable and InvalidTxnState errors into a
            // TransactionAbortableException, may request a client-side epoch
            // bump, and transitions to the abortable error state. All three need
            // Phase 5 state (`needToTriggerEpochBumpFromClient`, `isCompleting`).
            // Unreachable while `new` refuses a transactional id.
            return Err(KafkaError::unsupported_version(
                "The transactional error path is not yet implemented in this client \
                 (Milestone 11, Phase 5).",
            ));
        }
        Ok(())
    }

    // -- Producer id lifecycle ---------------------------------------------

    /// Get the current producer id and epoch without blocking. Callers must use [`ProducerIdAndEpoch::is_valid`] to
    /// verify that the result is valid.
    ///
    /// Corresponds to `producerIdAndEpoch()` (Java 581).
    pub(crate) fn producer_id_and_epoch(&self) -> ProducerIdAndEpoch {
        self.producer_id_and_epoch
    }

    /// Restarts `topic_partition`'s sequence numbering when the partition is
    /// still on a stale producer id/epoch and has drained.
    ///
    /// Translated from `maybeUpdateProducerIdAndEpoch(TopicPartition)`
    /// (Java 585).
    ///
    /// `batches` is the partition's in-flight batches; it is only consulted when
    /// the rewrite happens, and the guard means the entry tracks none at that
    /// point, so an empty slice is always correct here. It is still a parameter
    /// because [`TxnPartitionMap::start_sequences_at_beginning`] requires the
    /// pool by contract (rules §7).
    pub(crate) fn maybe_update_producer_id_and_epoch(
        &mut self,
        topic_partition: &TopicPartition,
        batches: &mut [&mut ProducerBatch],
    ) -> Result<(), KafkaError> {
        if self.has_fatal_error() {
            kafka_debug!(
                self.log_context,
                "Ignoring producer ID and epoch update request since the producer is in fatal error state"
            );
            return Ok(());
        }

        if self.has_stale_producer_id_and_epoch(topic_partition) && !self.has_inflight_batches(topic_partition) {
            // If the batch was on a different ID and/or epoch (due to an epoch bump) and all its in-flight batches
            // have completed, reset the partition sequence so that the next batch (with the new epoch) starts from 0
            let producer_id_and_epoch = self.producer_id_and_epoch;
            self.txn_partition_map
                .start_sequences_at_beginning(topic_partition, producer_id_and_epoch, batches)?;
            kafka_debug!(
                self.log_context,
                "ProducerId of partition {} set to {} with epoch {}. Reinitialize sequence at beginning.",
                topic_partition,
                producer_id_and_epoch.producer_id,
                producer_id_and_epoch.epoch
            );
        }
        Ok(())
    }

    /// Set the producer id and epoch atomically.
    ///
    /// Corresponds to `setProducerIdAndEpoch(ProducerIdAndEpoch)` (Java 603).
    fn set_producer_id_and_epoch(&mut self, producer_id_and_epoch: ProducerIdAndEpoch) {
        // With TV2, the epoch bump is common and frequent. Only log if it is at debug level or the producer ID is
        // changed.
        if !self.is_transactional()
            || !self.is_transaction_v2_enabled
            || producer_id_and_epoch.producer_id != self.producer_id_and_epoch.producer_id
        {
            kafka_info!(
                self.log_context,
                "ProducerId set to {} with epoch {}",
                producer_id_and_epoch.producer_id,
                producer_id_and_epoch.epoch
            );
        } else {
            kafka_debug!(
                self.log_context,
                "ProducerId set to {} with epoch {}",
                producer_id_and_epoch.producer_id,
                producer_id_and_epoch.epoch
            );
        }
        self.producer_id_and_epoch = producer_id_and_epoch;
    }

    /// This method resets the producer ID and epoch and sets the state to [`State::Uninitialized`], which will trigger
    /// a new `InitProducerId` request. This method is only called when the producer epoch is exhausted; we will bump
    /// the epoch instead.
    ///
    /// Corresponds to `resetIdempotentProducerId()` (Java 618).
    fn reset_idempotent_producer_id(&mut self, caller: Caller) -> Result<(), KafkaError> {
        if self.is_transactional() {
            return Err(KafkaError::illegal_state(
                "Cannot reset producer state for a transactional producer. You must either abort the ongoing \
                 transaction or reinitialize the transactional producer instead",
            ));
        }
        kafka_debug!(
            self.log_context,
            "Resetting idempotent producer ID. ID and epoch before reset are {}",
            self.producer_id_and_epoch
        );
        self.set_producer_id_and_epoch(ProducerIdAndEpoch::NONE);
        self.transition_to(State::Uninitialized, None, caller)
    }

    /// Drops all sequence bookkeeping for `topic_partition`.
    ///
    /// Corresponds to `resetSequenceForPartition(TopicPartition)` (Java 627).
    fn reset_sequence_for_partition(&mut self, topic_partition: &TopicPartition) {
        self.txn_partition_map.remove(topic_partition);
        self.partitions_with_unresolved_sequences.remove(topic_partition);
    }

    /// Drops all sequence bookkeeping for every partition.
    ///
    /// Corresponds to `resetSequenceNumbers()` (Java 632).
    fn reset_sequence_numbers(&mut self) {
        self.txn_partition_map.reset();
        self.partitions_with_unresolved_sequences.clear();
    }

    /// This method is used to trigger an epoch bump for non-transactional idempotent producers.
    ///
    /// Corresponds to `requestIdempotentEpochBumpForPartition(TopicPartition)`
    /// (Java 640).
    pub(crate) fn request_idempotent_epoch_bump_for_partition(&mut self, topic_partition: &TopicPartition) {
        self.client_side_epoch_bump_required = true;
        self.partitions_to_rewrite_sequences.insert(topic_partition.clone());
    }

    /// Bumps the local epoch (or resets the producer id when the epoch is
    /// exhausted) and rewrites the queued partitions' in-flight sequences.
    ///
    /// Translated from `bumpIdempotentProducerEpoch()` (Java 645).
    ///
    /// # A queued partition with no in-flight batches
    ///
    /// This is normal, not an error, and the rewrite must still run. Java
    /// reaches [`TxnPartitionEntry::start_sequences_at_beginning`] through
    /// `TxnPartitionMap.get` (Java 78), which throws only when the partition has
    /// no **entry**; an entry with an empty `inflightBatchesBySequence` simply
    /// runs the reset loop zero times and still lands `nextSequence = 0`,
    /// `lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER` and the new producer
    /// id/epoch. Resetting the counter is the whole point of the call, so
    /// skipping it would leave the partition numbering from the old epoch.
    ///
    /// `testProducerIdReset` (`TransactionManagerTest.java:865`) pins exactly
    /// this: `tp0` gets an entry and sequence 3 from `incrementSequenceNumber`
    /// but never an in-flight batch, and after the bump `sequenceNumber(tp0)`
    /// must be 0 while `tp1`, which was not queued, must still be 3.
    ///
    /// So a queued partition absent from `batches` is passed an **empty slice**
    /// rather than being skipped. A queued partition with no *entry* propagates
    /// [`TxnPartitionMap::get_mut`]'s error, matching Java's throw; that
    /// combination is unreachable today, because the only caller of
    /// [`Self::request_idempotent_epoch_bump_for_partition`] that could remove
    /// an entry is `handleFailedBatch`'s `UnknownProducerId` arm, and rules §9
    /// routes an idempotent `UnknownProducerId` to the epoch-bump branch
    /// instead.
    fn bump_idempotent_producer_epoch(
        &mut self,
        batches: &mut InFlightBatchPool<'_>,
        caller: Caller,
    ) -> Result<(), KafkaError> {
        if self.producer_id_and_epoch.epoch == i16::MAX {
            self.reset_idempotent_producer_id(caller)?;
        } else {
            self.set_producer_id_and_epoch(ProducerIdAndEpoch::new(
                self.producer_id_and_epoch.producer_id,
                self.producer_id_and_epoch.epoch + 1,
            ));
            kafka_debug!(
                self.log_context,
                "Incremented producer epoch, current producer ID and epoch are now {}",
                self.producer_id_and_epoch
            );
        }

        // When the epoch is bumped, rewrite all in-flight sequences for the partition(s) that triggered the epoch bump
        let producer_id_and_epoch = self.producer_id_and_epoch;
        // Java iterates the `HashSet` directly; the order does not matter
        // because each partition is rewritten independently. Collected here only
        // to release the borrow on `self`.
        let queued: Vec<TopicPartition> = self.partitions_to_rewrite_sequences.iter().cloned().collect();
        let mut no_batches: [&mut ProducerBatch; 0] = [];
        for topic_partition in queued {
            // A queued partition with no in-flight batches gets an empty slice
            // rather than being skipped — see the method docs.
            let partition_batches: &mut [&mut ProducerBatch] = match batches.get_mut(&topic_partition) {
                Some(partition_batches) => partition_batches.as_mut_slice(),
                None => &mut no_batches,
            };
            self.txn_partition_map.start_sequences_at_beginning(
                &topic_partition,
                producer_id_and_epoch,
                partition_batches,
            )?;
            self.partitions_with_unresolved_sequences.remove(&topic_partition);
        }
        self.partitions_to_rewrite_sequences.clear();

        self.client_side_epoch_bump_required = false;
        Ok(())
    }

    /// Bumps the epoch when one was requested, and enqueues an
    /// `InitProducerId` request when no producer id is held.
    ///
    /// Translated from `bumpIdempotentEpochAndResetIdIfNeeded()` (Java 663).
    /// Called once per `Sender.runOnce` (`Sender.java:331`), hence
    /// [`Caller::Sender`] at the production call site.
    pub(crate) fn bump_idempotent_epoch_and_reset_id_if_needed(
        &mut self,
        batches: &mut InFlightBatchPool<'_>,
        caller: Caller,
    ) -> Result<(), KafkaError> {
        if !self.is_transactional() {
            if self.client_side_epoch_bump_required {
                self.bump_idempotent_producer_epoch(batches, caller)?;
            }
            if self.current_state != State::Initializing && !self.has_producer_id() {
                self.transition_to(State::Initializing, None, caller)?;
                let mut request_data = InitProducerIdRequestData::new();
                request_data.set_transactional_id(None).set_transaction_timeout_ms(i32::MAX);
                let handler = TxnRequestHandler::new(
                    "InitProducerId",
                    self.retry_backoff_ms,
                    TxnRequestHandlerKind::InitProducerId {
                        builder: InitProducerIdRequestBuilder::new(request_data),
                        is_epoch_bump: false,
                    },
                );
                self.enqueue_request(handler);
            }
        }
        Ok(())
    }

    // -- Sequence numbers ---------------------------------------------------

    /// Returns the next sequence number to be written to the given `TopicPartition`.
    ///
    /// Corresponds to `sequenceNumber(TopicPartition)` (Java 682).
    pub(crate) fn sequence_number(&mut self, topic_partition: &TopicPartition) -> i32 {
        self.txn_partition_map.get_or_create(topic_partition).next_sequence()
    }

    /// Returns the current producer id/epoch of the given `TopicPartition`.
    ///
    /// Corresponds to the `producerIdAndEpoch(TopicPartition)` overload
    /// (Java 689). Renamed because Rust has no overloading and
    /// [`Self::producer_id_and_epoch`] already takes the no-argument form.
    pub(crate) fn producer_id_and_epoch_for_partition(
        &mut self,
        topic_partition: &TopicPartition,
    ) -> ProducerIdAndEpoch {
        self.txn_partition_map.get_or_create(topic_partition).producer_id_and_epoch()
    }

    /// Advances `topic_partition`'s next sequence by `increment`.
    ///
    /// Corresponds to `incrementSequenceNumber(TopicPartition, int)`
    /// (Java 693).
    pub(crate) fn increment_sequence_number(
        &mut self,
        topic_partition: &TopicPartition,
        increment: i32,
    ) -> Result<(), KafkaError> {
        self.txn_partition_map.get_mut(topic_partition)?.increment_sequence(increment);
        Ok(())
    }

    /// Records `batch` as in flight.
    ///
    /// Corresponds to `addInFlightBatch(ProducerBatch)` (Java 697).
    pub(crate) fn add_in_flight_batch(&mut self, batch: &ProducerBatch) -> Result<(), KafkaError> {
        if !batch.has_sequence() {
            return Err(KafkaError::illegal_state(format!(
                "Can't track batch for partition {} when sequence is not set.",
                batch.topic_partition
            )));
        }
        self.txn_partition_map
            .get_mut(&batch.topic_partition)?
            .add_inflight_batch(batch);
        Ok(())
    }

    /// Returns the first inflight sequence for a given partition. This is the base sequence of an inflight batch with
    /// the lowest sequence number.
    ///
    /// Corresponds to `firstInFlightSequence(TopicPartition)` (Java 710).
    ///
    /// Returns the lowest inflight sequence if the transaction manager is tracking inflight requests for this
    /// partition. If there are no inflight requests being tracked for this partition, this method will return
    /// [`RecordBatch::NO_SEQUENCE`].
    pub(crate) fn first_in_flight_sequence(&mut self, topic_partition: &TopicPartition) -> Result<i32, KafkaError> {
        if !self.has_inflight_batches(topic_partition) {
            return Ok(RecordBatch::NO_SEQUENCE);
        }
        Ok(self
            .next_batch_by_sequence(topic_partition)?
            .map_or(RecordBatch::NO_SEQUENCE, |(_, _, base_sequence)| base_sequence))
    }

    /// The key of the lowest-sequence in-flight batch for `topic_partition`.
    ///
    /// Corresponds to `nextBatchBySequence(TopicPartition)` (Java 717). Java
    /// returns the `ProducerBatch`; this returns its ordering key, because this
    /// type does not own the batches (rules §7).
    pub(crate) fn next_batch_by_sequence(
        &self,
        topic_partition: &TopicPartition,
    ) -> Result<Option<InFlightBatchKey>, KafkaError> {
        self.txn_partition_map.next_batch_by_sequence(topic_partition)
    }

    /// Removes `batch` from the in-flight set.
    ///
    /// Corresponds to `removeInFlightBatch(ProducerBatch)` (Java 721).
    pub(crate) fn remove_in_flight_batch(&mut self, batch: &ProducerBatch) -> Result<(), KafkaError> {
        if self.has_inflight_batches(&batch.topic_partition) {
            self.txn_partition_map.remove_in_flight_batch(batch)?;
        }
        Ok(())
    }

    /// Raises `topic_partition`'s last acknowledged sequence to `sequence`.
    ///
    /// Corresponds to `maybeUpdateLastAckedSequence(TopicPartition, int)`
    /// (Java 726).
    fn maybe_update_last_acked_sequence(&mut self, topic_partition: &TopicPartition, sequence: i32) -> i32 {
        self.txn_partition_map
            .maybe_update_last_acked_sequence(topic_partition, sequence)
    }

    /// The last acknowledged sequence for `topic_partition`.
    ///
    /// Corresponds to `lastAckedSequence(TopicPartition)` (Java 730).
    pub(crate) fn last_acked_sequence(&self, topic_partition: &TopicPartition) -> Option<i32> {
        self.txn_partition_map.last_acked_sequence(topic_partition)
    }

    /// The last acknowledged offset for `topic_partition`.
    ///
    /// Corresponds to `lastAckedOffset(TopicPartition)` (Java 734).
    pub(crate) fn last_acked_offset(&self, topic_partition: &TopicPartition) -> Option<i64> {
        self.txn_partition_map.last_acked_offset(topic_partition)
    }

    /// Records the last offset acknowledged for `batch`'s partition.
    ///
    /// Corresponds to `updateLastAckedOffset(PartitionResponse, ProducerBatch)`
    /// (Java 738).
    fn update_last_acked_offset(
        &mut self,
        response: &PartitionResponse,
        batch: &ProducerBatch,
    ) -> Result<(), KafkaError> {
        if response.base_offset == INVALID_OFFSET {
            return Ok(());
        }
        let last_offset = response.base_offset + i64::from(batch.record_count) - 1;
        let is_transactional = self.is_transactional();
        self.txn_partition_map
            .update_last_acked_offset(&batch.topic_partition, is_transactional, last_offset)
    }

    /// Records a successful produce response for `batch`.
    ///
    /// Corresponds to `handleCompletedBatch(ProducerBatch, PartitionResponse)`
    /// (Java 745).
    pub(crate) fn handle_completed_batch(
        &mut self,
        batch: &ProducerBatch,
        response: &PartitionResponse,
    ) -> Result<(), KafkaError> {
        let last_acked_sequence = self.maybe_update_last_acked_sequence(&batch.topic_partition, batch.last_sequence());
        kafka_trace!(
            self.log_context,
            "ProducerId: {}; Set last ack'd sequence number for topic-partition {} to {}",
            batch.producer_id(),
            batch.topic_partition,
            last_acked_sequence
        );

        self.update_last_acked_offset(response, batch)?;
        self.remove_in_flight_batch(batch)
    }

    /// Records a failed produce response for `batch`.
    ///
    /// Translated from
    /// `handleFailedBatch(ProducerBatch, RuntimeException, boolean)`
    /// (Java 788).
    ///
    /// `batches` supplies the partition's *remaining* in-flight batches for the
    /// transactional sequence adjustment (Java 818); the idempotent path never
    /// reads it, so an empty slice is fine there.
    ///
    /// # `UnknownProducerId` on an idempotent producer takes the epoch-bump arm
    ///
    /// Java's first branch tests `exception instanceof OutOfOrderSequenceException`,
    /// and `UnknownProducerIdException` **extends** it, so an idempotent
    /// producer's `UnknownProducerId` matches the first branch and requests an
    /// epoch bump; only a transactional producer reaches the second branch. The
    /// two wire codes are unrelated `Errors` values in Rust, so the relation is
    /// spelled out by [`is_out_of_order_sequence`] — see
    /// `.claude/rules/producer-transactions.md` §9.
    pub(crate) fn handle_failed_batch(
        &mut self,
        batch: &ProducerBatch,
        error: &KafkaError,
        adjust_sequence_numbers: bool,
        batches: &mut [&mut ProducerBatch],
        caller: Caller,
    ) -> Result<(), KafkaError> {
        self.maybe_transition_to_error_state(error, caller)?;
        self.remove_in_flight_batch(batch)?;

        if self.has_fatal_error() {
            kafka_debug!(
                self.log_context,
                "Ignoring batch {} with producer id {}, epoch {}, and sequence number {} since the producer is \
                 already in fatal error state ({})",
                batch.topic_partition,
                batch.producer_id(),
                batch.producer_epoch(),
                batch.base_sequence(),
                error
            );
            return Ok(());
        }

        if is_out_of_order_sequence(error.error()) && !self.is_transactional() {
            kafka_error!(
                self.log_context,
                "The broker returned {} for topic-partition {} with producerId {}, epoch {}, and sequence number {}",
                error,
                batch.topic_partition,
                batch.producer_id(),
                batch.producer_epoch(),
                batch.base_sequence()
            );

            // If we fail with an OutOfOrderSequenceException, we have a gap in the log. Bump the epoch for this
            // partition, which will reset the sequence number to 0 and allow us to continue
            self.request_idempotent_epoch_bump_for_partition(&batch.topic_partition);
        } else if error.error() == Errors::UnknownProducerId {
            // If we get an UnknownProducerId for a partition, then the broker has no state for that producer. It will
            // therefore accept a write with sequence number 0. We reset the sequence number for the partition here so
            // that the producer can continue after aborting the transaction. All inflight-requests to this partition
            // will also fail with an UnknownProducerId error, so the sequence will remain at 0. Note that if the
            // broker supports bumping the epoch, we will later reset all sequence numbers after calling InitProducerId
            //
            // Only a transactional producer reaches this arm: the branch above
            // already claims `UnknownProducerId` when `!isTransactional()`.
            self.reset_sequence_for_partition(&batch.topic_partition);
        } else if adjust_sequence_numbers {
            if !self.is_transactional() {
                self.request_idempotent_epoch_bump_for_partition(&batch.topic_partition);
            } else {
                self.txn_partition_map.adjust_sequences_due_to_failed_batch(batch, batches)?;
            }
        }
        Ok(())
    }

    /// Whether any batches are in flight for `topic_partition`.
    ///
    /// Corresponds to `hasInflightBatches(TopicPartition)` (Java 824).
    pub(crate) fn has_inflight_batches(&mut self, topic_partition: &TopicPartition) -> bool {
        self.txn_partition_map.get_or_create(topic_partition).has_inflight_batches()
    }

    /// Whether `topic_partition` is still numbering under an older producer
    /// id/epoch than the manager's current one.
    ///
    /// Corresponds to `hasStaleProducerIdAndEpoch(TopicPartition)` (Java 828).
    pub(crate) fn has_stale_producer_id_and_epoch(&mut self, topic_partition: &TopicPartition) -> bool {
        let producer_id_and_epoch = self.producer_id_and_epoch;
        producer_id_and_epoch != self.txn_partition_map.get_or_create(topic_partition).producer_id_and_epoch()
    }

    /// Whether any partition has an unresolved sequence.
    ///
    /// Corresponds to `hasUnresolvedSequences()` (Java 832).
    pub(crate) fn has_unresolved_sequences(&self) -> bool {
        !self.partitions_with_unresolved_sequences.is_empty()
    }

    /// Whether `topic_partition` has an unresolved sequence.
    ///
    /// Corresponds to `hasUnresolvedSequence(TopicPartition)` (Java 836).
    pub(crate) fn has_unresolved_sequence(&self, topic_partition: &TopicPartition) -> bool {
        self.partitions_with_unresolved_sequences.contains_key(topic_partition)
    }

    /// Marks `batch`'s partition unresolved after the batch expired locally.
    ///
    /// Corresponds to `markSequenceUnresolved(ProducerBatch)` (Java 840).
    pub(crate) fn mark_sequence_unresolved(&mut self, batch: &ProducerBatch) {
        let next_sequence = batch.last_sequence() + 1;
        let recorded = self
            .partitions_with_unresolved_sequences
            .entry(batch.topic_partition.clone())
            .and_modify(|value| *value = (*value).max(next_sequence))
            .or_insert(next_sequence);
        kafka_debug!(
            self.log_context,
            "Marking partition {} unresolved with next sequence number {}",
            batch.topic_partition,
            recorded
        );
    }

    /// Attempts to resolve unresolved sequences. If all in-flight requests are complete and some partitions are still
    /// unresolved, either bump the epoch if possible, or transition to a fatal error.
    ///
    /// Translated from `maybeResolveSequences()` (Java 850). Called once per
    /// `Sender.runOnce` (`Sender.java:313`).
    ///
    /// Takes no [`Caller`]: the idempotent arm performs no state transition, it
    /// only requests an epoch bump. Phase 5's transactional arm transitions and
    /// will need the parameter.
    pub(crate) fn maybe_resolve_sequences(&mut self) -> Result<(), KafkaError> {
        // Java removes through the key-set iterator. Collected here because the
        // loop body needs `&mut self`; each partition is handled independently,
        // so `HashMap` iteration order is not observable.
        let unresolved: Vec<TopicPartition> = self.partitions_with_unresolved_sequences.keys().cloned().collect();
        for topic_partition in unresolved {
            if self.has_inflight_batches(&topic_partition) {
                continue;
            }
            // The partition has been fully drained. At this point, the last ack'd sequence should be one less than
            // next sequence destined for the partition. If so, the partition is fully resolved. If not, we should
            // reset the sequence number if necessary.
            let sequence = self.sequence_number(&topic_partition);
            if self.is_next_sequence(&topic_partition, sequence) {
                // This would happen when a batch was expired, but subsequent batches succeeded.
                self.partitions_with_unresolved_sequences.remove(&topic_partition);
                continue;
            }

            // We would enter this branch if all in flight batches were ultimately expired in the producer.
            if self.is_transactional() {
                // Java 862-870 bumps the epoch if the coordinator supports it and
                // otherwise moves to a fatal error, via
                // `transitionToAbortableErrorOrFatalError`. That needs Phase 5
                // state (`coordinatorSupportsBumpingEpoch`,
                // `isTransactionV2Enabled`). Unreachable while `new` refuses a
                // transactional id.
                return Err(KafkaError::unsupported_version(format!(
                    "Resolving unresolved sequences for partition {topic_partition} on a transactional producer is \
                     not yet implemented in this client (Milestone 11, Phase 5)."
                )));
            }
            // For the idempotent producer, bump the epoch
            kafka_info!(
                self.log_context,
                "No inflight batches remaining for {}, last ack'd sequence for partition is {}, next sequence is {}. \
                 Going to bump epoch and reset sequence numbers.",
                topic_partition,
                self.last_acked_sequence(&topic_partition)
                    .unwrap_or(TxnPartitionEntry::NO_LAST_ACKED_SEQUENCE_NUMBER),
                sequence
            );
            self.request_idempotent_epoch_bump_for_partition(&topic_partition);
            self.partitions_with_unresolved_sequences.remove(&topic_partition);
        }
        Ok(())
    }

    /// Whether `sequence` is exactly one past `topic_partition`'s last
    /// acknowledged sequence.
    ///
    /// Corresponds to `isNextSequence(TopicPartition, int)` (Java 885).
    fn is_next_sequence(&self, topic_partition: &TopicPartition, sequence: i32) -> bool {
        sequence
            - self
                .last_acked_sequence(topic_partition)
                .unwrap_or(TxnPartitionEntry::NO_LAST_ACKED_SEQUENCE_NUMBER)
            == 1
    }

    /// Whether `sequence` is the batch immediately following the expired one on
    /// an unresolved partition.
    ///
    /// Corresponds to `isNextSequenceForUnresolvedPartition(TopicPartition, int)`
    /// (Java 889).
    fn is_next_sequence_for_unresolved_partition(&self, topic_partition: &TopicPartition, sequence: i32) -> bool {
        self.has_unresolved_sequence(topic_partition)
            && self.partitions_with_unresolved_sequences.get(topic_partition) == Some(&sequence)
    }

    // -- Pending transactional requests ------------------------------------

    /// Enqueues `handler` for the Sender to pick up.
    ///
    /// Corresponds to `enqueueRequest(TxnRequestHandler)` (Java 1186).
    fn enqueue_request(&mut self, handler: TxnRequestHandler) {
        kafka_debug!(self.log_context, "Enqueuing transactional request {:?}", handler);
        self.pending_requests.push_back(handler);
    }

    /// The next transactional request to send, if any.
    ///
    /// Translated from `nextRequest(boolean)` (Java 894).
    ///
    /// Java's first statement enqueues an `AddPartitionsToTxn` when
    /// `newPartitionsInTransaction` is non-empty, and its `isEndTxn` branch
    /// short-circuits an `EndTxn` for a transaction that never started. Both are
    /// transaction-only, and [`TxnRequestHandler::is_end_txn`] is `false` for
    /// every handler this phase can build, so neither is reachable; Phase 5 adds
    /// them with the handlers they need.
    pub(crate) fn next_request(&mut self, has_incomplete_batches: bool) -> Option<TxnRequestHandler> {
        let next_request_handler = self.pending_requests.front()?;

        // Do not send the EndTxn until all batches have been flushed
        if next_request_handler.is_end_txn() && has_incomplete_batches {
            return None;
        }

        let next_request_handler = self.pending_requests.pop_front()?;
        if self.maybe_terminate_request_with_error(&next_request_handler) {
            kafka_trace!(
                self.log_context,
                "Not sending transactional request {:?} because we are in an error state",
                next_request_handler
            );
            return None;
        }

        kafka_trace!(self.log_context, "Request {:?} dequeued for sending", next_request_handler);
        Some(next_request_handler)
    }

    /// Whether any transactional request is pending.
    ///
    /// Corresponds to `hasPendingRequests()` (Java 1005).
    pub(crate) fn has_pending_requests(&self) -> bool {
        !self.pending_requests.is_empty()
    }

    /// Fails `handler` when the manager is in an error state.
    ///
    /// Translated from `maybeTerminateRequestWithError(TxnRequestHandler)`
    /// (Java 1174). Java's `hasAbortableError() && handler instanceof
    /// FindCoordinatorHandler` escape hatch cannot match here — that handler
    /// arrives in Phase 5 — so it is omitted rather than written as an
    /// always-false test.
    fn maybe_terminate_request_with_error(&self, handler: &TxnRequestHandler) -> bool {
        if self.has_error() {
            if let Some(last_error) = &self.last_error {
                handler.fail(last_error.clone());
            }
            return true;
        }
        false
    }

    /// Re-enqueues `handler` as a retry.
    ///
    /// Corresponds to `retry(TxnRequestHandler)` (Java 934), which is what
    /// `Sender` calls; `TxnRequestHandler.reenqueue()` (Java 1394) is the same
    /// two statements reached from the response path.
    pub(crate) fn retry(&mut self, mut handler: TxnRequestHandler) {
        handler.set_retry();
        self.enqueue_request(handler);
    }

    /// Records the correlation id of the transactional request now in flight.
    ///
    /// Corresponds to `setInFlightCorrelationId(int)` (Java 973).
    pub(crate) fn set_in_flight_correlation_id(&mut self, correlation_id: i32) {
        self.in_flight_request_correlation_id = correlation_id;
    }

    /// Clears the in-flight correlation id.
    ///
    /// Corresponds to `clearInFlightCorrelationId()` (Java 977).
    fn clear_in_flight_correlation_id(&mut self) {
        self.in_flight_request_correlation_id = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    /// Whether a transactional request is in flight.
    ///
    /// Corresponds to `hasInFlightRequest()` (Java 981).
    pub(crate) fn has_in_flight_request(&self) -> bool {
        self.in_flight_request_correlation_id != NO_INFLIGHT_REQUEST_CORRELATION_ID
    }

    /// Fails `handler` and moves the manager to [`State::FatalError`].
    ///
    /// Corresponds to `TxnRequestHandler.fatalError(RuntimeException)`
    /// (Java 1357).
    fn fatal_error(&mut self, handler: &TxnRequestHandler, error: KafkaError) -> Result<(), KafkaError> {
        handler.result.fail(error.clone());
        // Every caller is on the response path, which runs on the Sender task.
        self.transition_to_fatal_error(error, Caller::Sender)
    }

    /// Fails `handler` and moves the manager to [`State::AbortableError`].
    ///
    /// Corresponds to `TxnRequestHandler.abortableError(RuntimeException)`
    /// (Java 1362).
    fn abortable_error(&mut self, handler: &TxnRequestHandler, error: KafkaError) -> Result<(), KafkaError> {
        handler.result.fail(error.clone());
        // Every caller is on the response path, which runs on the Sender task.
        self.transition_to_abortable_error(error, Caller::Sender)
    }

    /// The coordinator `handler` must be routed to, or `None` when it can go to
    /// any broker.
    ///
    /// Corresponds to `coordinatorType()` (Java 1434), overridden by
    /// `InitProducerIdHandler` (Java 1482) to return `null` for a
    /// non-transactional producer — which is why the whole `FindCoordinator`
    /// subsystem is out of scope for the idempotence slice.
    ///
    /// A method on the manager rather than the handler because Java's override
    /// reads the enclosing instance's `transactionalId`.
    pub(crate) fn coordinator_type(&self, handler: &TxnRequestHandler) -> Option<CoordinatorType> {
        match handler.kind {
            TxnRequestHandlerKind::InitProducerId { .. } => {
                if self.is_transactional() {
                    Some(CoordinatorType::Transaction)
                } else {
                    None
                }
            },
        }
    }

    /// The key identifying the coordinator `handler` must be routed to.
    ///
    /// Corresponds to `coordinatorKey()` (Java 1438). The base implementation
    /// returns the transactional id; only `TxnOffsetCommitHandler` (Phase 5)
    /// overrides it.
    pub(crate) fn coordinator_key(&self, handler: &TxnRequestHandler) -> Option<&str> {
        match handler.kind {
            TxnRequestHandlerKind::InitProducerId { .. } => self.transactional_id(),
        }
    }

    /// Whether `handler` needs a coordinator before it can be sent.
    ///
    /// Corresponds to `needsCoordinator()` (Java 1430), i.e.
    /// `coordinatorType() != null`.
    pub(crate) fn needs_coordinator(&self, handler: &TxnRequestHandler) -> bool {
        self.coordinator_type(handler).is_some()
    }

    /// Handles the response to a transactional request.
    ///
    /// Translated from `TxnRequestHandler.onComplete(ClientResponse)`
    /// (Java 1406). Moved from the handler onto the manager because Java's inner
    /// class reaches the manager through an implicit `TransactionManager.this`,
    /// which Rust has no equivalent for.
    ///
    /// `handler` is taken by value: Java's `reenqueue()` puts `this` back on the
    /// pending queue, so ownership has to move.
    ///
    /// The `Caller` is always [`Caller::Sender`]: Java invokes this from
    /// `NetworkClient.poll`, i.e. on the Sender thread, at every call site.
    pub(crate) fn on_complete(
        &mut self,
        handler: TxnRequestHandler,
        response: &ClientResponse,
    ) -> Result<(), KafkaError> {
        if response.request_header().correlation_id() != self.in_flight_request_correlation_id {
            return self.fatal_error(
                &handler,
                KafkaError::with_message(
                    Errors::UnknownServerError,
                    "Detected more than one in-flight transactional request.",
                ),
            );
        }

        self.clear_in_flight_correlation_id();
        if response.was_disconnected() {
            kafka_debug!(self.log_context, "Disconnected from {}. Will retry.", response.destination());
            if self.needs_coordinator(&handler) {
                // Java 1414 looks the coordinator up again. Unreachable for an
                // idempotent producer, whose `coordinatorType()` is null; Phase 5
                // adds `lookupCoordinator` with the FindCoordinator handler.
                return Err(KafkaError::unsupported_version(
                    "Coordinator lookup is not yet implemented in this client (Milestone 11, Phase 5).",
                ));
            }
            self.retry(handler);
            return Ok(());
        }
        if let Some(version_mismatch) = response.version_mismatch() {
            let error = KafkaError::unsupported_version(version_mismatch.to_string());
            return self.fatal_error(&handler, error);
        }
        match response.response_body() {
            Some(response_body) => {
                kafka_trace!(
                    self.log_context,
                    "Received transactional response {} for request {:?}",
                    response_body,
                    handler
                );
                self.handle_response(handler, response_body)
            },
            None => self.fatal_error(
                &handler,
                KafkaError::with_message(
                    Errors::UnknownServerError,
                    "Could not execute transactional request for unknown reasons",
                ),
            ),
        }
    }

    /// Dispatches a parsed response body to the handler that requested it.
    ///
    /// Corresponds to the abstract `handleResponse(AbstractResponse)`
    /// (Java 1456).
    fn handle_response(&mut self, handler: TxnRequestHandler, response: &ConcreteResponse) -> Result<(), KafkaError> {
        // One handler kind in this phase, so no dispatch is needed yet; Phase 5
        // matches on `handler.kind` here as Java dispatches on the subclass.
        self.handle_init_producer_id_response(handler, response)
    }

    /// Handles an `InitProducerId` response.
    ///
    /// Translated from `InitProducerIdHandler.handleResponse(AbstractResponse)`
    /// (Java 1491).
    fn handle_init_producer_id_response(
        &mut self,
        handler: TxnRequestHandler,
        response: &ConcreteResponse,
    ) -> Result<(), KafkaError> {
        // Irrefutable while `TxnRequestHandlerKind` has a single variant; Phase 5
        // moves the dispatch up into `handle_response`.
        let TxnRequestHandlerKind::InitProducerId { builder, is_epoch_bump } = &handler.kind;
        let ConcreteResponse::InitProducerId(init_producer_id_response) = response else {
            // Java casts unconditionally; a mismatch would be a
            // ClassCastException. Surfaced as an error per CLAUDE.md §10.2.
            return Err(KafkaError::illegal_state(format!(
                "Expected an InitProducerId response for an InitProducerId request, got {response}"
            )));
        };
        let keep_prepared_txn = builder.data().keep_prepared_txn;
        let is_epoch_bump = *is_epoch_bump;
        let error = init_producer_id_response.error();

        if error == Errors::None {
            let producer_id_and_epoch = ProducerIdAndEpoch::new(
                init_producer_id_response.data().producer_id,
                init_producer_id_response.data().producer_epoch,
            );
            self.set_producer_id_and_epoch(producer_id_and_epoch);
            // If this is a transaction with keepPreparedTxn=true, transition directly
            // to PREPARED_TRANSACTION state IFF there is an ongoing transaction.
            if keep_prepared_txn
                && init_producer_id_response.data().ongoing_txn_producer_id != RecordBatch::NO_PRODUCER_ID
            {
                // Java 1504-1510 moves to PREPARED_TRANSACTION and records
                // `preparedTxnState`. `keepPreparedTxn` can only be set by
                // `initializeTransactions`, which is transactional, so this is
                // unreachable while `new` refuses a transactional id. Phase 5
                // adds the state field.
                return Err(KafkaError::unsupported_version(
                    "Two-phase commit is not yet implemented in this client (Milestone 11, Phase 5).",
                ));
            }
            self.transition_to(State::Ready, None, Caller::Sender)?;
            self.last_error = None;
            if is_epoch_bump {
                self.reset_sequence_numbers();
            }
            handler.result.done();
            return Ok(());
        }
        if error == Errors::NotCoordinator || error == Errors::CoordinatorNotAvailable {
            // Java 1520 looks the transaction coordinator up again and retries.
            // Only a transactional `InitProducerId` is routed to a coordinator,
            // so this is unreachable while `new` refuses a transactional id.
            return Err(KafkaError::unsupported_version(
                "Coordinator lookup is not yet implemented in this client (Milestone 11, Phase 5).",
            ));
        }
        if error.is_retriable() {
            self.retry(handler);
            return Ok(());
        }
        if error == Errors::TransactionalIdAuthorizationFailed || error == Errors::ClusterAuthorizationFailed {
            kafka_info!(
                self.log_context,
                "Abortable authorization error: {}.  Transition the producer state to {}",
                error.message(),
                State::AbortableError
            );
            let error = KafkaError::new(error);
            self.last_error = Some(error.clone());
            return self.abortable_error(&handler, error);
        }
        if error == Errors::InvalidProducerEpoch || error == Errors::ProducerFenced {
            // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
            // just treat it the same as PRODUCE_FENCED.
            return self.fatal_error(&handler, KafkaError::new(Errors::ProducerFenced));
        }
        if error == Errors::TransactionAbortable {
            let error = KafkaError::new(error);
            return self.abortable_error(&handler, error);
        }
        self.fatal_error(
            &handler,
            KafkaError::with_message(
                Errors::UnknownServerError,
                format!("Unexpected error in InitProducerIdResponse; {}", error.message()),
            ),
        )
    }

    // -- Send path ----------------------------------------------------------

    /// Validates that a record may be appended for `topic_partition`, adding it
    /// to the transaction when one is in progress.
    ///
    /// Translated from `maybeAddPartition(TopicPartition)` (Java 437).
    ///
    /// Java's second statement is `throwIfPendingState("send")` (Java 439),
    /// which inspects `pendingTransition`. That field is only ever set by
    /// `handleCachedTransactionRequestResult` (Java 1281), which starts with
    /// `ensureTransactional()`, so it is always null for an idempotent producer
    /// and the call can do nothing. Phase 5 adds the field and the call
    /// together.
    pub(crate) fn maybe_add_partition(&mut self, topic_partition: &TopicPartition) -> Result<(), KafkaError> {
        self.maybe_fail_with_error()?;

        if self.is_transactional() {
            // Java 441-459 validates the transaction state and registers the
            // partition. Unreachable while `new` refuses a transactional id.
            return Err(KafkaError::unsupported_version(format!(
                "Adding partition {topic_partition} to a transaction is not yet implemented in this client \
                 (Milestone 11, Phase 5)."
            )));
        }
        Ok(())
    }

    /// Whether the failed produce response for `batch` should be retried.
    ///
    /// Translated from `canRetry(PartitionResponse, ProducerBatch)`
    /// (Java 1015).
    ///
    /// `batches` supplies the partition's in-flight batches for the
    /// transactional log-truncation rewrite (Java 1048); the idempotent path
    /// never reads it.
    pub(crate) fn can_retry(
        &mut self,
        response: &PartitionResponse,
        batch: &ProducerBatch,
        batches: &mut [&mut ProducerBatch],
    ) -> Result<bool, KafkaError> {
        let error = response.error;

        // An UNKNOWN_PRODUCER_ID means that we have lost the producer state on the broker. Depending on the log start
        // offset, we may want to retry these, as described for each case below. If none of those apply, then for the
        // idempotent producer, we will locally bump the epoch and reset the sequence numbers of in-flight batches from
        // sequence 0, then retry the failed batch, which should now succeed. For the transactional producer, allow the
        // batch to fail. When processing the failed batch, we will transition to an abortable error and set a flag
        // indicating that we need to bump the epoch (if supported by the broker).
        if error == Errors::UnknownProducerId {
            if response.log_start_offset == -1 {
                // We don't know the log start offset with this response. We should just retry the request until we get
                // it. The UNKNOWN_PRODUCER_ID error code was added along with the new ProduceResponse which includes
                // the logStartOffset. So the '-1' sentinel is not for backward compatibility. Instead, it is possible
                // for a broker to not know the logStartOffset at when it is returning the response because the
                // partition may have moved away from the broker from the time the error was initially raised to the
                // time the response was being constructed. In these cases, we should just retry the request: we are
                // guaranteed to eventually get a logStartOffset once things settle down.
                return Ok(true);
            }

            if batch.sequence_has_been_reset() {
                // When the first inflight batch fails due to the truncation case, then the sequences of all the other
                // in flight batches would have been restarted from the beginning. However, when those responses
                // come back from the broker, they would also come with an UNKNOWN_PRODUCER_ID error. In this case, we
                // should not reset the sequence numbers to the beginning.
                return Ok(true);
            }
            // Java defaults the missing offset to `NO_LAST_ACKED_SEQUENCE_NUMBER`
            // here rather than `INVALID_OFFSET`; both are -1, and the constant
            // Java names is kept so the two stay in step.
            if self
                .last_acked_offset(&batch.topic_partition)
                .unwrap_or(i64::from(TxnPartitionEntry::NO_LAST_ACKED_SEQUENCE_NUMBER))
                < response.log_start_offset
            {
                // The head of the log has been removed, probably due to the retention time elapsing. In this case,
                // we expect to lose the producer state. For the transactional producer, reset the sequences of all
                // inflight batches to be from the beginning and retry them, so that the transaction does not need to
                // be aborted. For the idempotent producer, bump the epoch to avoid reusing (sequence, epoch) pairs
                if self.is_transactional() {
                    let producer_id_and_epoch = self.producer_id_and_epoch;
                    self.txn_partition_map.start_sequences_at_beginning(
                        &batch.topic_partition,
                        producer_id_and_epoch,
                        batches,
                    )?;
                } else {
                    self.request_idempotent_epoch_bump_for_partition(&batch.topic_partition);
                }
                return Ok(true);
            }

            if !self.is_transactional() {
                // For the idempotent producer, always retry UNKNOWN_PRODUCER_ID errors. If the batch has the current
                // producer ID and epoch, request a bump of the epoch. Otherwise just retry the produce.
                self.request_idempotent_epoch_bump_for_partition(&batch.topic_partition);
                return Ok(true);
            }
        } else if error == Errors::OutOfOrderSequenceNumber {
            if !self.has_unresolved_sequence(&batch.topic_partition)
                && (batch.sequence_has_been_reset()
                    || !self.is_next_sequence(&batch.topic_partition, batch.base_sequence()))
            {
                // We should retry the OutOfOrderSequenceException if the batch is _not_ the next batch, ie. its base
                // sequence isn't the lastAckedSequence + 1.
                return Ok(true);
            } else if !self.is_transactional() {
                // For the idempotent producer, retry all OUT_OF_ORDER_SEQUENCE_NUMBER errors. If there are no
                // unresolved sequences, or this batch is the one immediately following an unresolved sequence, we know
                // there is actually a gap in the sequences, and we bump the epoch. Otherwise, retry without bumping
                // and wait to see if the sequence resolves
                if !self.has_unresolved_sequence(&batch.topic_partition)
                    || self.is_next_sequence_for_unresolved_partition(&batch.topic_partition, batch.base_sequence())
                {
                    self.request_idempotent_epoch_bump_for_partition(&batch.topic_partition);
                }
                return Ok(true);
            }
        }

        // If neither of the above cases are true, retry if the exception is retriable
        Ok(error.is_retriable())
    }
}

/// Whether `code` satisfies Java's `instanceof OutOfOrderSequenceException`.
///
/// `UnknownProducerIdException extends OutOfOrderSequenceException`, so both wire
/// codes match. The relation is stated once here rather than open-coded at each
/// dispatch site — see `.claude/rules/producer-transactions.md` §9.
pub(crate) fn is_out_of_order_sequence(code: Errors) -> bool {
    matches!(code, Errors::OutOfOrderSequenceNumber | Errors::UnknownProducerId)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeApiVersions;
    use crate::api_versions_response_data::{ApiVersion, FinalizedFeatureKey, SupportedFeatureKey};
    use crate::common::compress::Compression;
    use crate::common::protocol::ApiKeys;
    use crate::common::record::TimestampType;
    use crate::common::record::memory_records::MemoryRecords;
    use crate::common::requests::{InitProducerIdResponse, RequestHeader};
    use crate::init_producer_id_response_data::InitProducerIdResponseData;

    // Constants mirroring `TransactionManagerTest`'s fields (Java 125-155).
    const TRANSACTION_TIMEOUT_MS: i32 = 1121;
    const DEFAULT_RETRY_BACKOFF_MS: i64 = 100;
    const TOPIC: &str = "test";
    const PRODUCER_ID: i64 = 13131;
    const EPOCH: i16 = 1;
    /// Correlation id used for every simulated transactional round trip. Java's
    /// `MockClient` allocates it; here the test plays the Sender's part, which is
    /// what sets it (`Sender.java:508`).
    const CORRELATION_ID: i32 = 7;

    fn tp0() -> TopicPartition {
        TopicPartition::new(TOPIC.to_string(), 0)
    }

    fn tp1() -> TopicPartition {
        TopicPartition::new(TOPIC.to_string(), 1)
    }

    /// Java's `new KafkaException()`, which carries no wire error code.
    fn kafka_exception() -> KafkaError {
        KafkaError::with_message(Errors::UnknownServerError, "")
    }

    /// Java's `new TimeoutException()`.
    fn timeout_exception() -> KafkaError {
        KafkaError::timeout("")
    }

    /// Builds an idempotent (non-transactional) manager.
    ///
    /// Mirrors `initializeTransactionManager(Optional.empty(), transactionV2Enabled)`
    /// (Java 174-221), including the `ApiVersions` contents, so the
    /// `transactionV2Enabled` parameterisation is reproduced faithfully.
    ///
    /// # `transaction_v2_enabled` is not observable in this phase
    ///
    /// Java threads the flag only into `apiVersions`, and the manager reads
    /// `apiVersions` from exactly two methods — `handleCoordinatorReady`
    /// (Java 1104) and `maybeUpdateTransactionV2Enabled` (Java 493) — both of
    /// which are Phase 5. `isTransactionV2Enabled` therefore stays `false` in
    /// both iterations, and its only Phase-3 reader
    /// ([`TransactionManager::set_producer_id_and_epoch`], Java 605) is
    /// short-circuited by `!isTransactional()` anyway. So both parameterisations
    /// execute identical code here. The loops are kept regardless: they cost
    /// nothing and will start discriminating in Phase 5.
    fn idempotent_manager(transaction_v2_enabled: bool) -> TransactionManager {
        fn api_version(api_key: &ApiKeys, max_version: i16) -> ApiVersion {
            let mut version = ApiVersion::new();
            version.set_api_key(api_key.id());
            version.set_min_version(0);
            version.set_max_version(max_version);
            version
        }

        let transaction_version = if transaction_v2_enabled { 2 } else { 1 };
        let mut supported_feature = SupportedFeatureKey::new();
        supported_feature.set_name("transaction.version".to_string());
        supported_feature.set_max_version(transaction_version);
        supported_feature.set_min_version(0);
        let mut finalized_feature = FinalizedFeatureKey::new();
        finalized_feature.set_name("transaction.version".to_string());
        finalized_feature.set_max_version_level(transaction_version);
        finalized_feature.set_min_version_level(transaction_version);

        let api_versions = Arc::new(ApiVersions::new());
        api_versions.update(
            "0",
            NodeApiVersions::new(
                &[
                    api_version(&ApiKeys::INIT_PRODUCER_ID, 6),
                    api_version(
                        &ApiKeys::PRODUCE,
                        if transaction_v2_enabled {
                            ApiKeys::PRODUCE.latest_version()
                        } else {
                            11
                        },
                    ),
                    api_version(
                        &ApiKeys::TXN_OFFSET_COMMIT,
                        if transaction_v2_enabled {
                            ApiKeys::TXN_OFFSET_COMMIT.latest_version()
                        } else {
                            4
                        },
                    ),
                ],
                &[supported_feature],
                &[finalized_feature],
                0,
            ),
        );

        TransactionManager::new(
            LogContext::empty(),
            None,
            TRANSACTION_TIMEOUT_MS,
            DEFAULT_RETRY_BACKOFF_MS,
            api_versions,
            false,
        )
        .expect("an idempotent manager is constructible")
    }

    /// A single-record batch, mirroring `batchWithValue` (Java 840).
    fn batch_with_value(topic_partition: &TopicPartition, value: &str) -> ProducerBatch {
        let builder = MemoryRecords::builder(64, Compression::none(), TimestampType::CreateTime, 0);
        let mut batch = ProducerBatch::new(topic_partition.clone(), builder, 0);
        assert!(
            batch.try_append(0, Some(&[]), Some(value.as_bytes()), &[], None, 0).is_ok(),
            "a 64-byte batch has room for one small record"
        );
        batch
    }

    /// Assigns the next sequence to a new batch and tracks it in flight.
    ///
    /// Mirrors `writeIdempotentBatchWithValue` (Java 812).
    ///
    /// `maybe_update_producer_id_and_epoch` is given an empty batch pool: its
    /// guard is `has_stale_producer_id_and_epoch && !has_inflight_batches`, so
    /// the entry tracks nothing whenever the rewrite runs.
    fn write_idempotent_batch_with_value(
        manager: &mut TransactionManager,
        topic_partition: &TopicPartition,
        value: &str,
    ) -> ProducerBatch {
        manager
            .maybe_update_producer_id_and_epoch(topic_partition, &mut [])
            .expect("no in-flight batches to rewrite");
        let sequence = manager.sequence_number(topic_partition);
        manager.increment_sequence_number(topic_partition, 1).expect("the entry exists");
        let mut batch = batch_with_value(topic_partition, value);
        let producer_id_and_epoch = manager.producer_id_and_epoch();
        batch.set_producer_state(producer_id_and_epoch.producer_id, producer_id_and_epoch.epoch, sequence, false);
        manager.add_in_flight_batch(&batch).expect("the sequence is set");
        batch.close();
        batch
    }

    /// The ordering key `next_batch_by_sequence` returns for `batch`.
    ///
    /// Java compares the returned `ProducerBatch` by identity; this type tracks
    /// keys rather than owning batches (rules §7), so the key is compared
    /// instead.
    fn in_flight_key(batch: &ProducerBatch) -> InFlightBatchKey {
        (batch.producer_id(), batch.producer_epoch(), batch.base_sequence())
    }

    /// Feeds an `InitProducerId` response back into `manager`, playing the part
    /// `Sender` plus `MockClient` play in Java.
    fn complete_init_producer_id(
        manager: &mut TransactionManager,
        handler: TxnRequestHandler,
        error: Errors,
        producer_id: i64,
        epoch: i16,
    ) -> Result<(), KafkaError> {
        manager.set_in_flight_correlation_id(CORRELATION_ID);
        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(error.code())
            .set_producer_id(producer_id)
            .set_producer_epoch(epoch)
            .set_throttle_time_ms(0);
        let header = RequestHeader::new(&ApiKeys::INIT_PRODUCER_ID, 0, "", CORRELATION_ID)
            .expect("INIT_PRODUCER_ID is a known api key");
        let response = ClientResponse::new(
            header,
            None,
            "0",
            0,
            0,
            false,
            None,
            None,
            Some(ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data))),
        );
        manager.on_complete(handler, &response)
    }

    /// Acquires a producer id for an idempotent producer.
    ///
    /// Mirrors `initializeIdempotentProducerId` (Java 4333). Java drives
    /// `Sender.runOnce` against a `MockClient`; the send path is Phase 4, so the
    /// same manager path is driven directly: the pending `InitProducerId` is
    /// dequeued through [`TransactionManager::next_request`] exactly as
    /// `Sender.java:472` does, and the response is fed back through
    /// [`TransactionManager::on_complete`] exactly as `NetworkClient.poll` does.
    fn initialize_idempotent_producer_id(manager: &mut TransactionManager, producer_id: i64, epoch: i16) {
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
            .expect("enqueueing the initial InitProducerId succeeds");
        let mut handler = manager.next_request(false).expect("an InitProducerId request must be pending");
        // Java's helper asserts the same thing on the outgoing request.
        assert!(
            handler.request_builder().data().transactional_id.is_none(),
            "an idempotent producer must not send a transactional id"
        );
        complete_init_producer_id(manager, handler, Errors::None, producer_id, epoch)
            .expect("a successful InitProducerId response is handled");
        assert!(manager.has_producer_id());
    }

    /// Whether `error` is one of the two authorization exceptions that
    /// `Sender.shouldHandleAuthorizationError` matches (`Sender.java:352-353`).
    ///
    /// A `Sender` private method, modelled here so the test harness can mirror
    /// `runOnce`'s transaction block. Phase 4 owns the real one.
    fn should_handle_authorization_error(error: &KafkaError) -> bool {
        matches!(
            error.error(),
            Errors::TransactionalIdAuthorizationFailed | Errors::ClusterAuthorizationFailed
        )
    }

    /// Mirrors the whole `transactionManager != null` block of `Sender.runOnce`
    /// (`Sender.java:311-335`), guards included, and reports whether `runOnce`
    /// would have returned early.
    ///
    /// Java's tests reach the epoch bump through
    /// `runUntil(() -> transactionManager.producerIdAndEpoch().epoch == N)`,
    /// which spins `Sender.runOnce`. The send path is Phase 4, so the manager
    /// entry points are invoked directly — but in `runOnce`'s order and behind
    /// `runOnce`'s guards, because skipping the `:318` / `:325` guards and going
    /// straight from `:313` to `:331` produces exactly the wrong behaviour on the
    /// abortable-error path (an `ABORTABLE_ERROR → INITIALIZING` attempt Java
    /// never makes).
    ///
    /// The two `Sender`-side steps with no Phase-3 counterpart are skipped and
    /// noted: `maybeAbortBatches` (`:320`, `:355`) needs the accumulator, and
    /// `client.poll` (`:322`) needs the network client. Both are Phase 4.
    fn run_sender_transaction_phase(
        manager: &mut TransactionManager,
        batches: &mut InFlightBatchPool<'_>,
    ) -> SenderPhaseOutcome {
        // Sender.java:313
        manager.maybe_resolve_sequences().expect("resolving sequences succeeds");

        // Sender.java:315
        let last_error = manager.last_error().cloned();

        // Sender.java:318-323 — do not continue sending in a fatal state.
        if manager.has_fatal_error() {
            return SenderPhaseOutcome::ReturnedOnFatalError;
        }

        // Sender.java:325-327 → shouldHandleAuthorizationError, :351-360.
        let authorization_error =
            last_error.filter(|error| manager.has_abortable_error() && should_handle_authorization_error(error));
        if let Some(error) = authorization_error {
            // Java wraps the cause in an AuthenticationException (Sender.java:354).
            manager
                .fail_pending_requests(
                    &KafkaError::fatal(Errors::SaslAuthenticationFailed, error.message()),
                    Caller::Sender,
                )
                .expect("failing pending requests succeeds");
            manager
                .transition_to_uninitialized(Caller::Sender)
                .expect("ABORTABLE_ERROR -> UNINITIALIZED is a valid transition");
            return SenderPhaseOutcome::RecoveredFromAuthorizationError;
        }

        // Sender.java:331
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(batches, Caller::Sender)
            .expect("bumping the epoch succeeds");
        SenderPhaseOutcome::Continued
    }

    /// Which arm of `Sender.runOnce`'s transaction block was taken.
    ///
    /// Test-harness only; Java's `runOnce` returns `void` and communicates this
    /// through control flow.
    #[derive(Debug, PartialEq, Eq)]
    enum SenderPhaseOutcome {
        /// `Sender.java:322` — returned because the manager is in a fatal state.
        ReturnedOnFatalError,
        /// `Sender.java:326` — returned after recovering to `UNINITIALIZED`.
        RecoveredFromAuthorizationError,
        /// Fell through to `:331` and beyond.
        Continued,
    }

    // ---------------------------------------------------------------------
    // Rust-side unit tests for the pieces Java covers only indirectly.
    // ---------------------------------------------------------------------

    /// The MILESTONE-11 GUARD: Phase 3 refuses a transactional id so the
    /// untranslated transactional arms cannot be reached.
    #[test]
    fn test_transactional_id_is_refused_until_phase_5() {
        let result = TransactionManager::new(
            LogContext::empty(),
            Some("foobar".to_string()),
            TRANSACTION_TIMEOUT_MS,
            DEFAULT_RETRY_BACKOFF_MS,
            Arc::new(ApiVersions::new()),
            false,
        );
        let Err(error) = result else {
            panic!("a transactional manager is not constructible yet");
        };
        assert_eq!(error.error(), Errors::UnsupportedVersion);
        assert!(
            error.message().contains("Milestone 11, Phase 5"),
            "unexpected message: {}",
            error.message()
        );
    }

    /// The full nine-by-nine transition table, checked against Java 162-188.
    ///
    /// Java has no direct test for `isTransitionValid`; it is exercised
    /// indirectly through the state machine. Asserting the table wholesale is
    /// the only way to verify the four states Phase 3 cannot reach, which is
    /// the point of translating it whole.
    #[test]
    fn test_transition_table_matches_java() {
        use State::*;
        let all = [
            Uninitialized,
            Initializing,
            Ready,
            InTransaction,
            PreparedTransaction,
            CommittingTransaction,
            AbortingTransaction,
            AbortableError,
            FatalError,
        ];
        // (target, permitted sources). FATAL_ERROR accepts every source.
        let permitted: [(State, &[State]); 9] = [
            (Uninitialized, &[Ready, AbortableError]),
            (Initializing, &[Uninitialized, CommittingTransaction, AbortingTransaction]),
            (Ready, &[Initializing, CommittingTransaction, AbortingTransaction]),
            (InTransaction, &[Ready]),
            (PreparedTransaction, &[InTransaction, Initializing]),
            (CommittingTransaction, &[InTransaction, PreparedTransaction]),
            (AbortingTransaction, &[InTransaction, PreparedTransaction, AbortableError]),
            (
                AbortableError,
                &[InTransaction, CommittingTransaction, AbortableError, Initializing],
            ),
            (FatalError, &all),
        ];

        for (target, sources) in permitted {
            for source in all {
                let expected = sources.contains(&source);
                assert_eq!(
                    target.is_transition_valid(source),
                    expected,
                    "{source} -> {target} should be {}",
                    if expected { "valid" } else { "invalid" }
                );
            }
        }

        // The two arms most easily got wrong, called out explicitly.
        assert!(
            AbortableError.is_transition_valid(AbortableError),
            "ABORTABLE_ERROR self-loop is valid"
        );
        assert!(!Ready.is_transition_valid(Ready), "READY -> READY is invalid");
    }

    /// An invalid transition poisons the manager on the Sender side and leaves
    /// it alone on the application side (Java 234-289, KAFKA-14831).
    #[test]
    fn test_invalid_transition_poisons_only_on_the_sender_side() {
        for transaction_v2_enabled in [true, false] {
            // Application side: state unchanged, error returned.
            let mut manager = idempotent_manager(transaction_v2_enabled);
            let error = manager
                .transition_to(State::Ready, None, Caller::App)
                .expect_err("UNINITIALIZED -> READY is invalid");
            assert_eq!(
                error.message(),
                "Invalid transition attempted from state UNINITIALIZED to state READY",
                "the message interpolates the Java enum constant names"
            );
            assert_eq!(manager.current_state(), State::Uninitialized);
            assert!(manager.last_error().is_none());

            // Sender side: state poisoned to FATAL_ERROR and the error recorded.
            let mut manager = idempotent_manager(transaction_v2_enabled);
            let error = manager
                .transition_to(State::Ready, None, Caller::Sender)
                .expect_err("UNINITIALIZED -> READY is invalid");
            assert_eq!(
                error.message(),
                "Invalid transition attempted from state UNINITIALIZED to state READY"
            );
            assert_eq!(manager.current_state(), State::FatalError);
            assert!(manager.has_fatal_error());
            assert_eq!(manager.last_error().expect("poisoned").message(), error.message());
        }
    }

    /// Moving to an error state without an error is rejected (Java 1131-1134).
    #[test]
    fn test_transition_to_error_state_requires_an_error() {
        for (target, name) in [
            (State::FatalError, "FATAL_ERROR"),
            (State::AbortableError, "ABORTABLE_ERROR"),
        ] {
            let mut manager = idempotent_manager(false);
            // Reach a state from which ABORTABLE_ERROR is a valid target.
            manager
                .transition_to(State::Initializing, None, Caller::App)
                .expect("UNINITIALIZED -> INITIALIZING is valid");
            let error = manager
                .transition_to(target, None, Caller::App)
                .expect_err("an error is required");
            assert_eq!(error.message(), format!("Cannot transition to {name} with a null exception"));
        }
    }

    /// `ABORTABLE_ERROR` is reachable without a transactional id: a
    /// non-transactional `InitProducerId` can come back
    /// `CLUSTER_AUTHORIZATION_FAILED`, and Java's handler calls `abortableError`
    /// for it without testing `isTransactional()` (Java 1524-1528). See
    /// [`TransactionManager::transition_to_abortable_error`].
    #[test]
    fn test_cluster_authorization_failure_moves_an_idempotent_producer_to_abortable_error() {
        for error_code in [
            Errors::ClusterAuthorizationFailed,
            Errors::TransactionalIdAuthorizationFailed,
        ] {
            let mut manager = idempotent_manager(false);
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("the initial InitProducerId is enqueued");
            let handler = manager.next_request(false).expect("an InitProducerId request is pending");
            let result = Arc::clone(handler.result());

            complete_init_producer_id(&mut manager, handler, error_code, -1, -1)
                .expect("the authorization error is handled, not propagated");

            assert!(manager.has_abortable_error(), "{error_code:?} must produce an abortable error");
            assert!(manager.has_error());
            assert!(!manager.has_fatal_error());
            assert_eq!(manager.last_error().expect("recorded").error(), error_code);
            assert!(result.is_completed());
            assert!(!result.is_successful());
            assert!(!manager.has_producer_id());

            // While in ABORTABLE_ERROR every send is rejected (Java 438 →
            // maybeFailWithError). This is the state the producer must be able to
            // leave; see the recovery test below.
            let send_error = manager
                .maybe_add_partition(&tp0())
                .expect_err("sends are rejected in an error state");
            assert_eq!(
                send_error.message(),
                "Cannot execute transactional method because we are in an error state"
            );
        }
    }

    /// An idempotent producer in `ABORTABLE_ERROR` recovers to `UNINITIALIZED`
    /// on the next `Sender.runOnce` and acquires a fresh producer id.
    ///
    /// This is the exit path Issue 1 identified as missing. `Sender.runOnce`
    /// tests `hasAbortableError()` (`Sender.java:325`) and calls
    /// `shouldHandleAuthorizationError(lastError)` (`:351-360`) →
    /// `failPendingRequests` + `maybeAbortBatches` + `transitionToUninitialized`,
    /// then returns. For an idempotent producer the `instanceof` at `:352` is
    /// always satisfied, so this is the only outcome — Java never reaches `:331`
    /// with an abortable error outstanding, and never attempts
    /// `ABORTABLE_ERROR → INITIALIZING`.
    #[test]
    fn test_idempotent_producer_recovers_from_abortable_error_to_uninitialized() {
        for error_code in [
            Errors::ClusterAuthorizationFailed,
            Errors::TransactionalIdAuthorizationFailed,
        ] {
            let mut manager = idempotent_manager(false);
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("the initial InitProducerId is enqueued");
            let handler = manager.next_request(false).expect("an InitProducerId request is pending");
            complete_init_producer_id(&mut manager, handler, error_code, -1, -1).expect("the error is handled");
            assert!(manager.has_abortable_error());

            // Sender.runOnce intercepts at :325 and recovers; it does NOT reach
            // the epoch bump at :331.
            assert_eq!(
                run_sender_transaction_phase(&mut manager, &mut pool),
                SenderPhaseOutcome::RecoveredFromAuthorizationError
            );
            assert_eq!(manager.current_state(), State::Uninitialized);
            assert!(
                manager.last_error().is_none(),
                "transitionToUninitialized clears lastError (Java 761)"
            );
            assert!(!manager.has_error());
            assert!(!manager.has_abortable_error());
            assert!(
                !manager.has_pending_requests(),
                "the failed handler was consumed by on_complete, so nothing is queued"
            );

            // Sends are accepted again — the state is escapable.
            manager
                .maybe_add_partition(&tp0())
                .expect("sends are allowed once the error is cleared");

            // The next iteration enqueues a fresh InitProducerId and it succeeds.
            assert_eq!(
                run_sender_transaction_phase(&mut manager, &mut pool),
                SenderPhaseOutcome::Continued
            );
            assert_eq!(manager.current_state(), State::Initializing);
            let handler = manager.next_request(false).expect("a fresh InitProducerId is pending");
            complete_init_producer_id(&mut manager, handler, Errors::None, PRODUCER_ID, EPOCH)
                .expect("the retry succeeds");
            assert_eq!(manager.current_state(), State::Ready);
            assert_eq!(manager.producer_id_and_epoch(), ProducerIdAndEpoch::new(PRODUCER_ID, EPOCH));
        }
    }

    /// `failPendingRequests` (Java 944), `authenticationFailed` (Java 939) and
    /// `close` (Java 949) each fail every queued handler and transition.
    ///
    /// The recovery path above reaches `fail_pending_requests` with an empty
    /// queue, because `on_complete` consumed the only handler. These drive the
    /// non-empty case, which is where the per-handler loop is observable.
    #[test]
    fn test_pending_requests_are_failed_in_bulk() {
        // failPendingRequests → abortableError per handler.
        let mut manager = idempotent_manager(false);
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        // Reach ABORTABLE_ERROR from INITIALIZING, the transition
        // `InitProducerIdHandler`'s authorization arm makes (Java 1528), while
        // leaving a handler queued for `fail_pending_requests` to fail.
        manager
            .transition_to_abortable_error(KafkaError::new(Errors::ClusterAuthorizationFailed), Caller::Sender)
            .expect("INITIALIZING -> ABORTABLE_ERROR is valid");
        let queued_result = manager.force_enqueue_init_producer_id_for_test();
        assert!(manager.has_pending_requests());
        manager
            .fail_pending_requests(&KafkaError::fatal(Errors::SaslAuthenticationFailed, "authn"), Caller::Sender)
            .expect("ABORTABLE_ERROR self-loop is valid");
        assert!(queued_result.is_completed());
        assert_eq!(queued_result.error().expect("failed").error(), Errors::SaslAuthenticationFailed);
        assert!(manager.has_abortable_error(), "the state stays ABORTABLE_ERROR (self-loop)");
        assert!(manager.has_pending_requests(), "Java does not clear the queue (Java 945-946)");

        // authenticationFailed → fatalError per handler.
        let mut manager = idempotent_manager(false);
        let queued_result = manager.force_enqueue_init_producer_id_for_test();
        manager
            .authentication_failed(&KafkaError::fatal(Errors::SaslAuthenticationFailed, "authn"), Caller::Sender)
            .expect("FATAL_ERROR is always a valid target");
        assert!(manager.has_fatal_error());
        assert_eq!(queued_result.error().expect("failed").error(), Errors::SaslAuthenticationFailed);

        // close → fatalError with Java's message.
        let mut manager = idempotent_manager(false);
        let queued_result = manager.force_enqueue_init_producer_id_for_test();
        manager.close(Caller::Sender).expect("FATAL_ERROR is always a valid target");
        assert!(manager.has_fatal_error());
        assert_eq!(
            queued_result.error().expect("failed").message(),
            "The producer closed forcefully"
        );
        assert_eq!(
            manager.last_error().expect("recorded").message(),
            "The producer closed forcefully"
        );
    }

    /// A `PRODUCER_FENCED` / `INVALID_PRODUCER_EPOCH` `InitProducerId` response
    /// is fatal, and both report `PRODUCER_FENCED` (Java 1529-1532).
    #[test]
    fn test_producer_fenced_init_producer_id_response_is_fatal() {
        for error_code in [Errors::InvalidProducerEpoch, Errors::ProducerFenced] {
            let mut manager = idempotent_manager(false);
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("the initial InitProducerId is enqueued");
            let handler = manager.next_request(false).expect("an InitProducerId request is pending");
            let result = Arc::clone(handler.result());

            complete_init_producer_id(&mut manager, handler, error_code, -1, -1).expect("the error is handled");

            assert!(manager.has_fatal_error());
            assert_eq!(
                manager.last_error().expect("recorded").error(),
                Errors::ProducerFenced,
                "INVALID_PRODUCER_EPOCH is reported as PRODUCER_FENCED"
            );
            assert_eq!(result.error().expect("failed").error(), Errors::ProducerFenced);
        }
    }

    /// A retriable `InitProducerId` error re-enqueues the request rather than
    /// failing it (Java 1522-1523).
    #[test]
    fn test_retriable_init_producer_id_response_is_reenqueued() {
        let mut manager = idempotent_manager(false);
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        let handler = manager.next_request(false).expect("an InitProducerId request is pending");
        let result = Arc::clone(handler.result());

        complete_init_producer_id(&mut manager, handler, Errors::CoordinatorLoadInProgress, -1, -1)
            .expect("a retriable error is handled");

        assert!(!result.is_completed(), "a retried request must not complete");
        assert!(!manager.has_error());
        assert!(manager.has_pending_requests());
        let handler = manager.next_request(false).expect("the request was re-enqueued");
        assert!(handler.is_retry());
        assert!(!manager.has_in_flight_request(), "the correlation id is cleared on completion");
    }

    /// An unexpected `InitProducerId` error is fatal, with Java's message
    /// (Java 1536).
    #[test]
    fn test_unexpected_init_producer_id_response_is_fatal() {
        let mut manager = idempotent_manager(false);
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        let handler = manager.next_request(false).expect("an InitProducerId request is pending");

        complete_init_producer_id(&mut manager, handler, Errors::InvalidRequest, -1, -1).expect("the error is handled");

        assert!(manager.has_fatal_error());
        assert_eq!(
            manager.last_error().expect("recorded").message(),
            format!(
                "Unexpected error in InitProducerIdResponse; {}",
                Errors::InvalidRequest.message()
            )
        );
    }

    /// A response whose correlation id does not match the in-flight one is
    /// fatal (Java 1407-1408).
    #[test]
    fn test_mismatched_correlation_id_is_fatal() {
        let mut manager = idempotent_manager(false);
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        let handler = manager.next_request(false).expect("an InitProducerId request is pending");

        manager.set_in_flight_correlation_id(CORRELATION_ID + 1);
        let header = RequestHeader::new(&ApiKeys::INIT_PRODUCER_ID, 0, "", CORRELATION_ID)
            .expect("INIT_PRODUCER_ID is a known api key");
        let response = ClientResponse::new(header, None, "0", 0, 0, false, None, None, None);
        manager
            .on_complete(handler, &response)
            .expect("the mismatch is handled, not propagated");

        assert!(manager.has_fatal_error());
        assert_eq!(
            manager.last_error().expect("recorded").message(),
            "Detected more than one in-flight transactional request."
        );
        assert!(
            manager.has_in_flight_request(),
            "a mismatch must not clear the in-flight correlation id"
        );
    }

    /// A disconnect re-enqueues the request; an idempotent `InitProducerId`
    /// needs no coordinator, so no lookup is attempted (Java 1411-1415, 1482).
    #[test]
    fn test_disconnect_reenqueues_without_a_coordinator_lookup() {
        let mut manager = idempotent_manager(false);
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        let handler = manager.next_request(false).expect("an InitProducerId request is pending");
        assert_eq!(
            manager.coordinator_type(&handler),
            None,
            "InitProducerIdHandler.coordinatorType() is null when non-transactional (Java 1482)"
        );
        assert_eq!(manager.coordinator_key(&handler), None);
        assert!(!manager.needs_coordinator(&handler));
        let result = Arc::clone(handler.result());

        manager.set_in_flight_correlation_id(CORRELATION_ID);
        let header = RequestHeader::new(&ApiKeys::INIT_PRODUCER_ID, 0, "", CORRELATION_ID)
            .expect("INIT_PRODUCER_ID is a known api key");
        let response = ClientResponse::new(header, None, "0", 0, 0, true, None, None, None);
        manager.on_complete(handler, &response).expect("a disconnect is handled");

        assert!(!result.is_completed());
        assert!(!manager.has_error());
        assert!(manager.next_request(false).expect("re-enqueued").is_retry());
    }

    /// A pending request is failed rather than sent while the manager is in an
    /// error state (Java 1174-1184).
    #[test]
    fn test_next_request_terminates_pending_requests_in_an_error_state() {
        let mut manager = idempotent_manager(false);
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        manager
            .transition_to_fatal_error(kafka_exception(), Caller::Sender)
            .expect("FATAL_ERROR is always a valid target");

        assert!(manager.has_pending_requests());
        assert!(manager.next_request(false).is_none(), "the request is terminated, not returned");
        assert!(!manager.has_pending_requests());
    }

    /// `add_in_flight_batch` rejects a batch with no sequence (Java 698-699).
    #[test]
    fn test_add_in_flight_batch_requires_a_sequence() {
        let mut manager = idempotent_manager(false);
        let batch = batch_with_value(&tp0(), "1");
        assert!(!batch.has_sequence());
        let error = manager.add_in_flight_batch(&batch).expect_err("a sequence is required");
        assert_eq!(
            error.message(),
            format!("Can't track batch for partition {} when sequence is not set.", tp0())
        );
    }

    /// `firstInFlightSequence` reports [`RecordBatch::NO_SEQUENCE`] when nothing
    /// is in flight, and the lowest base sequence otherwise (Java 710-715).
    #[test]
    fn test_first_in_flight_sequence() {
        let mut manager = idempotent_manager(false);
        initialize_idempotent_producer_id(&mut manager, PRODUCER_ID, EPOCH);
        assert_eq!(
            manager.first_in_flight_sequence(&tp0()).expect("no entry needed"),
            RecordBatch::NO_SEQUENCE
        );

        let b1 = write_idempotent_batch_with_value(&mut manager, &tp0(), "1");
        let b2 = write_idempotent_batch_with_value(&mut manager, &tp0(), "2");
        assert_eq!(manager.first_in_flight_sequence(&tp0()).expect("in flight"), 0);
        assert_eq!(
            manager.next_batch_by_sequence(&tp0()).expect("entry exists"),
            Some(in_flight_key(&b1))
        );

        manager.remove_in_flight_batch(&b1).expect("the entry exists");
        assert_eq!(manager.first_in_flight_sequence(&tp0()).expect("in flight"), 1);
        assert_eq!(
            manager.next_batch_by_sequence(&tp0()).expect("entry exists"),
            Some(in_flight_key(&b2))
        );
    }

    /// The two halves of "a queued partition has no in-flight batches", which
    /// [`TransactionManager::bump_idempotent_producer_epoch`] distinguishes:
    /// an existing entry with an empty in-flight set is rewritten, while a
    /// partition with no entry at all surfaces Java's `IllegalStateException`
    /// from `TxnPartitionMap.get` (Java 78).
    ///
    /// The first half is also what `testProducerIdReset` pins; this test adds
    /// the second, which Java has no coverage for because it is unreachable
    /// there too. Keeping both in one place makes the distinction reviewable.
    #[test]
    fn test_epoch_bump_distinguishes_an_empty_in_flight_set_from_a_missing_entry() {
        // No entry for tp0: `request_idempotent_epoch_bump_for_partition` alone
        // does not create one, so the rewrite has nothing to rewrite *into*.
        let mut manager = idempotent_manager(false);
        initialize_idempotent_producer_id(&mut manager, PRODUCER_ID, i16::MAX);
        manager.request_idempotent_epoch_bump_for_partition(&tp0());
        let mut pool = InFlightBatchPool::new();
        let error = manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
            .expect_err("a queued partition with no entry is Java's IllegalStateException");
        assert_eq!(
            error.message(),
            format!(
                "Trying to get txnPartitionEntry for {}, but it was never set for this partition.",
                tp0()
            )
        );

        // An entry with an empty in-flight set is rewritten instead, and an
        // exhausted epoch resets the producer id (Java 646-647).
        let mut manager = idempotent_manager(false);
        initialize_idempotent_producer_id(&mut manager, PRODUCER_ID, i16::MAX);
        manager.increment_sequence_number(&tp0(), 4).expect_err("no entry yet");
        assert_eq!(manager.sequence_number(&tp0()), 0, "the accessor creates the entry");
        manager.increment_sequence_number(&tp0(), 4).expect("the entry now exists");
        manager.request_idempotent_epoch_bump_for_partition(&tp0());
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
            .expect("an empty in-flight set is rewritten, not rejected");
        assert_eq!(
            manager.producer_id_and_epoch(),
            ProducerIdAndEpoch::NONE,
            "an exhausted epoch resets the id"
        );
        assert!(!manager.has_producer_id());
        assert_eq!(manager.sequence_number(&tp0()), 0, "the sequence counter was rewound");
    }

    // ---------------------------------------------------------------------
    // TransactionManagerTest translations.
    //
    // Only the methods that call `initializeTransactionManager(Optional.empty(),
    // ..)` are in scope for this phase (PLAN §2). Three of those need the
    // Phase-4 send path and are named in the block at the end of this module.
    // ---------------------------------------------------------------------

    /// Translated from `testFailIfNotReadyForSendIdempotentProducer`
    /// (Java 267-273).
    #[test]
    fn test_fail_if_not_ready_for_send_idempotent_producer() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            manager
                .maybe_add_partition(&tp0())
                .expect("an idempotent producer may send to any partition");
        }
    }

    /// Translated from `testFailIfNotReadyForSendIdempotentProducerFatalError`
    /// (Java 275-281).
    #[test]
    fn test_fail_if_not_ready_for_send_idempotent_producer_fatal_error() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            manager
                .transition_to_fatal_error(kafka_exception(), Caller::App)
                .expect("FATAL_ERROR is always a valid target");
            let error = manager.maybe_add_partition(&tp0()).expect_err("a fatal error fails the send");
            assert_eq!(
                error.message(),
                "Cannot execute transactional method because we are in an error state"
            );
        }
    }

    /// Translated from `testDefaultSequenceNumber` (Java 623-630).
    #[test]
    fn test_default_sequence_number() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            assert_eq!(manager.sequence_number(&tp0()), 0);
            manager.increment_sequence_number(&tp0(), 3).expect("the entry exists");
            assert_eq!(manager.sequence_number(&tp0()), 3);
        }
    }

    /// Translated from
    /// `testBumpEpochAndResetSequenceNumbersAfterUnknownProducerId`
    /// (Java 632-668).
    #[test]
    fn test_bump_epoch_and_reset_sequence_numbers_after_unknown_producer_id() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            initialize_idempotent_producer_id(&mut manager, PRODUCER_ID, EPOCH);

            let b1 = write_idempotent_batch_with_value(&mut manager, &tp0(), "1");
            let mut b2 = write_idempotent_batch_with_value(&mut manager, &tp0(), "2");
            let mut b3 = write_idempotent_batch_with_value(&mut manager, &tp0(), "3");
            let mut b4 = write_idempotent_batch_with_value(&mut manager, &tp0(), "4");
            let mut b5 = write_idempotent_batch_with_value(&mut manager, &tp0(), "5");
            assert_eq!(manager.sequence_number(&tp0()), 5);

            // First batch succeeds
            let b1_append_time = 0;
            let b1_response = PartitionResponse::new(Errors::None, 500, b1_append_time, 0, Vec::new(), None);
            b1.complete(500, b1_append_time);
            manager
                .handle_completed_batch(&b1, &b1_response)
                .expect("the completion is recorded");

            // We get an UNKNOWN_PRODUCER_ID, so bump the epoch and set sequence numbers back to 0
            let b2_response = PartitionResponse::new(Errors::UnknownProducerId, -1, -1, 500, Vec::new(), None);
            assert!(
                manager
                    .can_retry(&b2_response, &b2, &mut [])
                    .expect("the retry decision is made")
            );

            {
                // Java reaches the bump through `runUntil(.. epoch == 2)`.
                let mut pool = InFlightBatchPool::new();
                pool.insert(tp0(), vec![&mut b2, &mut b3, &mut b4, &mut b5]);
                run_sender_transaction_phase(&mut manager, &mut pool);
            }
            assert_eq!(manager.producer_id_and_epoch().epoch, 2);
            assert_eq!(b2.producer_epoch(), 2);
            assert_eq!(b2.base_sequence(), 0);
            assert_eq!(b3.base_sequence(), 1);
            assert_eq!(b4.base_sequence(), 2);
            assert_eq!(b5.base_sequence(), 3);
        }
    }

    /// Translated from `testBatchFailureAfterProducerReset` (Java 670-710).
    #[test]
    fn test_batch_failure_after_producer_reset() {
        // This tests a scenario where the producerId is reset while pending requests are still inflight.
        // The partition(s) that triggered the reset will have their sequence number reset, while any others will not
        for transaction_v2_enabled in [true, false] {
            let epoch = i16::MAX;

            let mut manager = idempotent_manager(transaction_v2_enabled);
            initialize_idempotent_producer_id(&mut manager, PRODUCER_ID, epoch);

            let tp0b1 = write_idempotent_batch_with_value(&mut manager, &tp0(), "1");
            let tp1b1 = write_idempotent_batch_with_value(&mut manager, &tp1(), "1");

            let tp0b1_response = PartitionResponse::new(Errors::None, -1, -1, 400, Vec::new(), None);
            manager
                .handle_completed_batch(&tp0b1, &tp0b1_response)
                .expect("the completion is recorded");

            let tp1b1_response = PartitionResponse::new(Errors::None, -1, -1, 400, Vec::new(), None);
            manager
                .handle_completed_batch(&tp1b1, &tp1b1_response)
                .expect("the completion is recorded");

            let mut tp0b2 = write_idempotent_batch_with_value(&mut manager, &tp0(), "2");
            let mut tp1b2 = write_idempotent_batch_with_value(&mut manager, &tp1(), "2");
            assert_eq!(manager.sequence_number(&tp0()), 2);
            assert_eq!(manager.sequence_number(&tp1()), 2);

            let b1_response = PartitionResponse::new(Errors::UnknownProducerId, -1, -1, 400, Vec::new(), None);
            assert!(
                manager
                    .can_retry(&b1_response, &tp0b1, &mut [])
                    .expect("the retry decision is made")
            );

            let b2_response = PartitionResponse::new(Errors::None, -1, -1, 400, Vec::new(), None);
            manager
                .handle_completed_batch(&tp1b1, &b2_response)
                .expect("the completion is recorded");

            let tp0b2_key = in_flight_key(&tp0b2);
            let tp1b2_key = in_flight_key(&tp1b2);
            {
                let mut pool = InFlightBatchPool::new();
                pool.insert(tp0(), vec![&mut tp0b2]);
                pool.insert(tp1(), vec![&mut tp1b2]);
                manager
                    .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                    .expect("an exhausted epoch resets the producer id");
            }

            assert_eq!(manager.sequence_number(&tp0()), 1);
            assert_eq!(
                manager.next_batch_by_sequence(&tp0()).expect("entry exists"),
                Some(in_flight_key(&tp0b2))
            );
            assert_ne!(in_flight_key(&tp0b2), tp0b2_key, "tp0's batch was rewritten");
            assert_eq!(manager.sequence_number(&tp1()), 2);
            assert_eq!(
                manager.next_batch_by_sequence(&tp1()).expect("entry exists"),
                Some(in_flight_key(&tp1b2))
            );
            assert_eq!(
                in_flight_key(&tp1b2),
                tp1b2_key,
                "tp1 was not queued, so its batch is untouched"
            );
        }
    }

    /// Translated from `testBatchCompletedAfterProducerReset` (Java 712-747).
    #[test]
    fn test_batch_completed_after_producer_reset() {
        for transaction_v2_enabled in [true, false] {
            let epoch = i16::MAX;

            let mut manager = idempotent_manager(transaction_v2_enabled);
            initialize_idempotent_producer_id(&mut manager, PRODUCER_ID, epoch);

            let b1 = write_idempotent_batch_with_value(&mut manager, &tp0(), "1");
            // Java discards this reference because its `TreeSet` holds the batch;
            // Rust tracks ordering keys only (rules §7), so the caller has to
            // keep it to supply it to the rewrite below.
            let mut tp1b1 = write_idempotent_batch_with_value(&mut manager, &tp1(), "1");

            let b2 = write_idempotent_batch_with_value(&mut manager, &tp0(), "2");
            assert_eq!(manager.sequence_number(&tp0()), 2);

            // The producerId might be reset due to a failure on another partition
            manager.request_idempotent_epoch_bump_for_partition(&tp1());
            {
                let mut pool = InFlightBatchPool::new();
                pool.insert(tp1(), vec![&mut tp1b1]);
                manager
                    .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                    .expect("an exhausted epoch resets the producer id");
            }
            initialize_idempotent_producer_id(&mut manager, PRODUCER_ID + 1, 0);

            // We continue to track the state of tp0 until in-flight requests complete
            let b1_response = PartitionResponse::new(Errors::None, 500, 0, 0, Vec::new(), None);
            manager
                .handle_completed_batch(&b1, &b1_response)
                .expect("the completion is recorded");

            assert_eq!(manager.sequence_number(&tp0()), 2);
            assert_eq!(manager.last_acked_sequence(&tp0()), Some(0));
            assert_eq!(
                manager.next_batch_by_sequence(&tp0()).expect("entry exists"),
                Some(in_flight_key(&b2))
            );
            assert_eq!(
                manager
                    .next_batch_by_sequence(&tp0())
                    .expect("entry exists")
                    .map(|(_, batch_epoch, _)| batch_epoch),
                Some(epoch)
            );

            let b2_response = PartitionResponse::new(Errors::None, 500, 0, 0, Vec::new(), None);
            manager
                .handle_completed_batch(&b2, &b2_response)
                .expect("the completion is recorded");

            manager
                .maybe_update_producer_id_and_epoch(&tp0(), &mut [])
                .expect("tp0 has drained, so there is nothing to rewrite");
            assert_eq!(manager.sequence_number(&tp0()), 0);
            assert_eq!(manager.last_acked_sequence(&tp0()), None);
            assert_eq!(manager.next_batch_by_sequence(&tp0()).expect("entry exists"), None);
        }
    }

    /// Translated from `testSequenceNumberOverflow` (Java 849-861).
    #[test]
    fn test_sequence_number_overflow() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            assert_eq!(manager.sequence_number(&tp0()), 0);
            manager.increment_sequence_number(&tp0(), i32::MAX).expect("the entry exists");
            assert_eq!(manager.sequence_number(&tp0()), i32::MAX);
            manager.increment_sequence_number(&tp0(), 100).expect("the entry exists");
            assert_eq!(manager.sequence_number(&tp0()), 99);
            manager.increment_sequence_number(&tp0(), i32::MAX).expect("the entry exists");
            assert_eq!(manager.sequence_number(&tp0()), 98);
        }
    }

    /// Translated from `testProducerIdReset` (Java 863-880).
    ///
    /// This is the test that pins the "queued partition with no in-flight
    /// batches" behaviour: `tp0` gets an entry and sequence 3 from
    /// `increment_sequence_number` but never an in-flight batch, and the epoch
    /// bump must still reset its sequence to 0 while leaving the unqueued `tp1`
    /// at 3. See [`TransactionManager::bump_idempotent_producer_epoch`].
    #[test]
    fn test_producer_id_reset() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            initialize_idempotent_producer_id(&mut manager, 15, i16::MAX);
            assert_eq!(manager.sequence_number(&tp0()), 0);
            assert_eq!(manager.sequence_number(&tp1()), 0);
            manager.increment_sequence_number(&tp0(), 3).expect("the entry exists");
            assert_eq!(manager.sequence_number(&tp0()), 3);
            manager.increment_sequence_number(&tp1(), 3).expect("the entry exists");
            assert_eq!(manager.sequence_number(&tp1()), 3);

            manager.request_idempotent_epoch_bump_for_partition(&tp0());
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("a queued partition with no in-flight batches is still rewritten");
            assert_eq!(manager.sequence_number(&tp0()), 0);
            assert_eq!(manager.sequence_number(&tp1()), 3);
        }
    }

    /// Translated from `testBumpEpochAfterTimeoutWithoutPendingInflightRequests`
    /// (Java 3038-3081).
    #[test]
    fn test_bump_epoch_after_timeout_without_pending_inflight_requests() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            let producer_id = 15;
            let epoch = 5;
            let producer_id_and_epoch = ProducerIdAndEpoch::new(producer_id, epoch);
            initialize_idempotent_producer_id(&mut manager, producer_id, epoch);

            // Nothing to resolve, so no reset is needed
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);

            let tp0 = TopicPartition::new("foo".to_string(), 0);
            assert_eq!(manager.sequence_number(&tp0), 0);

            let b1 = write_idempotent_batch_with_value(&mut manager, &tp0, "1");
            assert_eq!(manager.sequence_number(&tp0), 1);
            manager
                .handle_completed_batch(&b1, &PartitionResponse::new(Errors::None, 500, 0, 0, Vec::new(), None))
                .expect("the completion is recorded");
            assert_eq!(manager.last_acked_sequence(&tp0), Some(0));

            // Marking sequence numbers unresolved without inflight requests is basically a no-op.
            manager.mark_sequence_unresolved(&b1);
            manager.maybe_resolve_sequences().expect("resolving succeeds");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(!manager.has_unresolved_sequences());

            // We have a new batch which fails with a timeout
            let b2 = write_idempotent_batch_with_value(&mut manager, &tp0, "2");
            assert_eq!(manager.sequence_number(&tp0), 2);
            manager.mark_sequence_unresolved(&b2);
            manager
                .handle_failed_batch(&b2, &timeout_exception(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");
            assert!(manager.has_unresolved_sequences());

            // We only had one inflight batch, so we should be able to clear the unresolved status
            // and bump the epoch
            manager.maybe_resolve_sequences().expect("resolving succeeds");
            assert!(!manager.has_unresolved_sequences());

            // Java reaches the bump through `runUntil(.. epoch == 6)`.
            let mut pool = InFlightBatchPool::new();
            run_sender_transaction_phase(&mut manager, &mut pool);
            assert_eq!(manager.producer_id_and_epoch().epoch, 6);
        }
    }

    /// Translated from `testNoProducerIdResetAfterLastInFlightBatchSucceeds`
    /// (Java 3083-3121).
    #[test]
    fn test_no_producer_id_reset_after_last_in_flight_batch_succeeds() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            let producer_id = 15;
            let epoch = 5;
            let producer_id_and_epoch = ProducerIdAndEpoch::new(producer_id, epoch);
            initialize_idempotent_producer_id(&mut manager, producer_id, epoch);

            let tp0 = TopicPartition::new("foo".to_string(), 0);
            let b1 = write_idempotent_batch_with_value(&mut manager, &tp0, "1");
            let b2 = write_idempotent_batch_with_value(&mut manager, &tp0, "2");
            let b3 = write_idempotent_batch_with_value(&mut manager, &tp0, "3");
            assert_eq!(manager.sequence_number(&tp0), 3);

            // The first batch fails with a timeout
            manager.mark_sequence_unresolved(&b1);
            manager
                .handle_failed_batch(&b1, &timeout_exception(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");
            assert!(manager.has_unresolved_sequences());

            // The reset should not occur until sequence numbers have been resolved
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(manager.has_unresolved_sequences());

            // The second batch fails as well with a timeout
            manager
                .handle_failed_batch(&b2, &timeout_exception(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(manager.has_unresolved_sequences());

            // The third batch succeeds, which should resolve the sequence number without
            // requiring a producerId reset.
            manager
                .handle_completed_batch(&b3, &PartitionResponse::new(Errors::None, 500, 0, 0, Vec::new(), None))
                .expect("the completion is recorded");
            manager.maybe_resolve_sequences().expect("resolving succeeds");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(!manager.has_unresolved_sequences());
            assert_eq!(manager.sequence_number(&tp0), 3);
        }
    }

    /// Translated from
    /// `testEpochBumpAfterLastInFlightBatchFailsIdempotentProducer`
    /// (Java 3123-3155).
    #[test]
    fn test_epoch_bump_after_last_in_flight_batch_fails_idempotent_producer() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            let producer_id_and_epoch = ProducerIdAndEpoch::new(PRODUCER_ID, EPOCH);
            initialize_idempotent_producer_id(&mut manager, PRODUCER_ID, EPOCH);

            let tp0 = TopicPartition::new("foo".to_string(), 0);
            let b1 = write_idempotent_batch_with_value(&mut manager, &tp0, "1");
            let b2 = write_idempotent_batch_with_value(&mut manager, &tp0, "2");
            let b3 = write_idempotent_batch_with_value(&mut manager, &tp0, "3");
            assert_eq!(manager.sequence_number(&tp0), 3);

            // The first batch fails with a timeout
            manager.mark_sequence_unresolved(&b1);
            manager
                .handle_failed_batch(&b1, &timeout_exception(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");
            assert!(manager.has_unresolved_sequences());

            // The second batch succeeds, but sequence numbers are still not resolved
            manager
                .handle_completed_batch(&b2, &PartitionResponse::new(Errors::None, 500, 0, 0, Vec::new(), None))
                .expect("the completion is recorded");
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(manager.has_unresolved_sequences());

            // When the last inflight batch fails, we have to bump the epoch
            manager
                .handle_failed_batch(&b3, &timeout_exception(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");

            // Java reaches the bump through `runUntil(.. epoch == 2)`.
            run_sender_transaction_phase(&mut manager, &mut pool);
            assert_eq!(manager.producer_id_and_epoch().epoch, 2);
            assert!(!manager.has_unresolved_sequences());
            assert_eq!(manager.sequence_number(&tp0), 0);
        }
    }

    /// Translated from `testNoFailedBatchHandlingWhenTxnManagerIsInFatalError`
    /// (Java 3243-3266).
    #[test]
    fn test_no_failed_batch_handling_when_txn_manager_is_in_fatal_error() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            let producer_id = 15;
            let epoch = 5;
            initialize_idempotent_producer_id(&mut manager, producer_id, epoch);

            let tp0 = TopicPartition::new("foo".to_string(), 0);
            let b1 = write_idempotent_batch_with_value(&mut manager, &tp0, "1");
            // Handling b1 should bump the epoch after OutOfOrderSequenceException
            manager
                .handle_failed_batch(
                    &b1,
                    &KafkaError::with_message(Errors::OutOfOrderSequenceNumber, "out of sequence"),
                    false,
                    &mut [],
                    Caller::Sender,
                )
                .expect("the failure is recorded");
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("the epoch is bumped");
            let id_and_epoch_after_first_batch = ProducerIdAndEpoch::new(producer_id, epoch + 1);
            assert_eq!(manager.producer_id_and_epoch(), id_and_epoch_after_first_batch);

            manager
                .transition_to_fatal_error(kafka_exception(), Caller::App)
                .expect("FATAL_ERROR is always a valid target");

            // The second batch should not bump the epoch as txn manager is already in fatal error state
            let b2 = write_idempotent_batch_with_value(&mut manager, &tp0, "2");
            manager
                .handle_failed_batch(&b2, &timeout_exception(), true, &mut [], Caller::Sender)
                .expect("the failure is ignored in a fatal state");
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), id_and_epoch_after_first_batch);
        }
    }

    // `TransactionManagerTest` methods that call
    // `initializeTransactionManager(Optional.empty(), ..)` but cannot be
    // translated in this phase, with the surface each one needs. All three are
    // Phase 4 (`Sender` / `RecordAccumulator` idempotence integration), not
    // skipped:
    //
    //   - `testDuplicateSequenceAfterProducerReset` (Java 748-810) — builds its
    //     own `RecordAccumulator` and `Sender`, appends through
    //     `accumulator.append(..)` and drives `sender.runOnce()` across request
    //     and delivery timeouts. Needs the per-batch sequence assignment in
    //     `RecordAccumulator.drainBatchesForOneNode` and
    //     `Sender.failExpiredBatches` → `markSequenceUnresolved`, both Phase 4.
    //   - `testHealthyPartitionRetriesDuringEpochBump` (Java 3600-3672) — after
    //     the epoch bump it asserts on `accumulator.getDeque(tp1)` to check that
    //     new batches are not drained while a partition has in-flight batches on
    //     the old epoch. Needs `shouldStopDrainBatchesForPartition`
    //     (`RecordAccumulator.java:815`), Phase 4.
    //   - `testFailedInflightBatchAfterEpochBump` (Java 3725-3810) — same
    //     accumulator/Sender surface plus `accumulator.reenqueue(..)`.
    //
    // Everything else in `TransactionManagerTest` runs against the transactional
    // manager built by `setup()` (Java 163) and belongs to Phases 5 and 8
    // (PLAN §2).
}
