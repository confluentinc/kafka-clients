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

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::ApiVersions;
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
///
/// `pub(crate)` because the field it guards lives on the [`Sender`] task
/// (rules §2); see [`PendingRequests`].
///
/// [`Sender`]: crate::producer::internals::Sender
pub(crate) const NO_INFLIGHT_REQUEST_CORRELATION_ID: i32 = -1;

/// The Sender task's queue of transactional requests waiting to be sent.
///
/// # Why this is not a field on [`TransactionManager`]
///
/// `.claude/rules/producer-transactions.md` §2 requires Java's
/// `pendingRequests` (`TransactionManager.java:121`) and
/// `inFlightRequestCorrelationId` (`:136`) to live on the **Sender task's own
/// unshared state**, not behind the shared `Arc<Mutex<TransactionManager>>`.
/// Java touches them from the Sender thread only, and does so *outside* its
/// `synchronized` blocks: `clearInFlightCorrelationId` is called from
/// `TxnRequestHandler.onComplete` at `:1410` while that method's `synchronized`
/// block only begins at `:1421` and wraps `handleResponse` alone, and
/// `pendingRequests` is mutated through the **unsynchronized**
/// `lookupCoordinator(TxnRequestHandler)` (`:969`) that `Sender.java:522` calls
/// directly.
///
/// So the queue is a plain field on `Sender` and every manager method that Java
/// implements by mutating it takes it as a parameter instead. That is the same
/// shape [`InFlightBatchPool`] already uses for the in-flight batches (rules §7):
/// *a method that mutates state this type does not own receives that state from
/// its owner*. The property a reviewer can check mechanically is that neither
/// name appears as a **field** in this file — only as a parameter — so nothing
/// else holding the shared mutex can reach either one.
///
/// # Why a struct, and how the ordering is expressed
///
/// Java's collection is
/// `new PriorityQueue<>(10, Comparator.comparingInt(o -> o.priority().priority))`
/// (`:224`) — a **min-heap** keyed on [`Priority`]. Phase 3/4 got away with a
/// `VecDeque` because the only handler the idempotence slice could enqueue was
/// `InitProducerId` and at most one was ever pending, so FIFO coincided with
/// priority order; Phase 5a adds `FindCoordinator`, which must overtake a queued
/// `InitProducerId`, so the real ordering is now observable.
///
/// Two Rust-specific concerns are folded into [`QueuedRequest`]'s [`Ord`] rather
/// than open-coded at the call sites:
///
///   - [`BinaryHeap`] is a **max**-heap, so the comparison is inverted to
///     reproduce Java's min-heap.
///   - Java's `PriorityQueue` is **unstable** for equal priorities, so the order
///     in which two `AddPartitionsOrOffsets` requests come back out is
///     unspecified there. An insertion-sequence tiebreaker makes it FIFO here, so
///     tests can assert on it (PLAN §Phase-5 recommends exactly this). That is a
///     strict refinement of Java's contract: any order Java may produce for equal
///     keys is admissible, and the one chosen is the order the requests were
///     enqueued in.
///
/// The priority is **snapshotted at insertion** into `QueuedRequest`, never read
/// back off the handler while it sits in the heap. `InitProducerIdHandler.priority()`
/// (Java 1477) is dynamic — `EPOCH_BUMP` when bumping, `INIT_PRODUCER_ID`
/// otherwise — and although `is_epoch_bump` happens to be immutable after
/// construction, rules §6 forbids keying an ordered collection on a field read
/// through the element: the snapshot makes that structurally impossible.
///
/// A struct rather than a bare `BinaryHeap<QueuedRequest>` alias, so no call site
/// can build an element with a hand-made key. It stands in for Java's
/// `pendingRequests` field itself and adds no concept Java lacks
/// (`definition-of-done.md` §7).
pub(crate) struct PendingRequests {
    queue: BinaryHeap<QueuedRequest>,
    /// Supplies [`QueuedRequest::sequence`]. Monotonic for the life of the
    /// `Sender`; at one transactional request per round trip, `u64` cannot wrap.
    next_sequence: u64,
}

impl PendingRequests {
    /// Creates an empty queue.
    ///
    /// Java sizes its `PriorityQueue` at 10 (`:224`); a `BinaryHeap` grows on
    /// demand and the initial capacity is not observable.
    pub(crate) fn new() -> Self {
        Self { queue: BinaryHeap::new(), next_sequence: 0 }
    }

    /// Enqueues `handler` at its current priority.
    ///
    /// Corresponds to `pendingRequests.add(requestHandler)` (Java 1188).
    pub(crate) fn add(&mut self, handler: TxnRequestHandler) {
        let priority = handler.priority();
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.queue.push(QueuedRequest { priority, sequence, handler });
    }

    /// The highest-priority queued request, without removing it.
    ///
    /// Corresponds to `pendingRequests.peek()` (Java 897).
    pub(crate) fn peek(&self) -> Option<&TxnRequestHandler> {
        self.queue.peek().map(|queued| &queued.handler)
    }

    /// Removes and returns the highest-priority queued request.
    ///
    /// Corresponds to `pendingRequests.poll()` (Java 905, 920).
    pub(crate) fn poll(&mut self) -> Option<TxnRequestHandler> {
        self.queue.pop().map(|queued| queued.handler)
    }

    /// Whether the queue is empty.
    ///
    /// Corresponds to `pendingRequests.isEmpty()` (Java 1006).
    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// The number of queued requests.
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    /// Iterates the queued requests in **unspecified** order.
    ///
    /// Corresponds to `pendingRequests.forEach(..)` (Java 940, 945, 951) and to
    /// the `for (TxnRequestHandler request : pendingRequests)` loop at Java 939.
    /// `PriorityQueue.iterator()` is documented as *not* traversing in any
    /// particular order, and `BinaryHeap::iter` matches: it walks the backing
    /// vector in heap order. All three Java callers fail every handler with the
    /// same exception, so no caller can observe the difference.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &TxnRequestHandler> {
        self.queue.iter().map(|queued| &queued.handler)
    }
}

impl Default for PendingRequests {
    fn default() -> Self {
        Self::new()
    }
}

/// A [`TxnRequestHandler`] together with the sort key it was enqueued under.
///
/// This is Java's `Comparator.comparingInt(o -> o.priority().priority)`
/// (`TransactionManager.java:224`) reified, plus the insertion tiebreaker — see
/// [`PendingRequests`] for both. Private to this module so the key can only ever
/// be produced by [`PendingRequests::add`].
struct QueuedRequest {
    priority: Priority,
    sequence: u64,
    handler: TxnRequestHandler,
}

impl Ord for QueuedRequest {
    /// Orders so that [`BinaryHeap`]'s max-heap yields Java's min-heap: the
    /// *lowest* [`Priority`] first, and among equal priorities the *earliest*
    /// insertion first.
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for QueuedRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for QueuedRequest {}

impl PartialEq for QueuedRequest {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

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
    /// `Arc` because [`TransactionManager::handle_cached_transaction_request_result`] must
    /// hand the *same* result object to both the caller and the
    /// pending-transition slot — see
    /// `.claude/rules/producer-transactions.md` §5.
    result: Arc<TransactionalRequestResult>,
    /// Whether this request has already been retried.
    is_retry: bool,
    /// How long to back off before retrying this request.
    ///
    /// A field rather than a read-through to the manager because
    /// `AddPartitionsToTxnHandler` (Phase 5b) overrides it per instance
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
    /// returns `false`; only `EndTxnHandler` (Phase 5b) overrides it.
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
    ///
    /// `pub(crate)` because the [`Sender`] reaches it while implementing the
    /// unsynchronized half of `TxnRequestHandler.onComplete` (Java 1406-1420).
    ///
    /// [`Sender`]: crate::producer::internals::Sender
    pub(crate) fn fail(&self, error: KafkaError) {
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

/// A transactional operation whose result the caller has not yet acknowledged.
///
/// Translated from the private static nested class `PendingStateTransition`
/// (Java 1953-1967).
///
/// The `result` is an `Arc<TransactionalRequestResult>` because
/// [`TransactionManager::handle_cached_transaction_request_result`] must hand the
/// **same** result object to both the caller and this slot — that identity is the
/// whole point of the mechanism, and it is what
/// `.claude/rules/producer-transactions.md` §5 means by "return the same result
/// object". Java gets it for free from reference semantics.
struct PendingStateTransition {
    result: Arc<TransactionalRequestResult>,
    state: State,
    operation: String,
}

impl PendingStateTransition {
    fn new(result: Arc<TransactionalRequestResult>, state: State, operation: &str) -> Self {
        Self { result, state, operation: operation.to_string() }
    }
}

/// A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
///
/// Translated from
/// `org.apache.kafka.clients.producer.internals.TransactionManager`.
///
/// # Scope
///
/// Milestone 11 Phase 3 landed the idempotence slice; Phase 4 integrated it with
/// the send path; **Phase 5a** adds the transactional state machine: the
/// priority-ordered pending-request queue, the pending-state-transition
/// machinery, `initializeTransactions` / `beginTransaction` /
/// `resetTransactionState`, the abortable-versus-fatal error machine,
/// `FindCoordinatorHandler` and the coordinator-routed `InitProducerId` path.
///
/// Phase 5b adds the four remaining request handlers
/// (`AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn`), the
/// entry points that construct them (`beginCommit`, `beginAbort`,
/// `beginCompletingTransaction`, `sendOffsetsToTransaction`,
/// `maybeAddPartition`'s registration branch), KIP-890 Transaction V2 and KIP-939
/// two-phase commit. Every deferred arm returns
/// [`Errors::UnsupportedVersion`] naming Phase 5b rather than silently taking
/// another branch (CLAUDE.md §5).
///
/// Three of the nine [`State`] variants are therefore not yet *enterable*:
/// `PREPARED_TRANSACTION` (needs `prepareTransaction` / the `keepPreparedTxn`
/// response arm), `COMMITTING_TRANSACTION` (needs `beginCommit`) and
/// `ABORTING_TRANSACTION` (needs `beginAbort`) — each blocked on a Phase-5b
/// entry point, not on transition logic. The full 9×9 table has been translated
/// since Phase 3; see [`State::is_transition_valid`].
///
/// # Lock topology
///
/// The manager is shared as `Arc<Mutex<TransactionManager>>` between
/// `KafkaProducer`, `Sender` and `RecordAccumulator` (PLAN §6.3), and
/// `.claude/rules/producer-transactions.md` §2 requires a deliberate split:
/// five pieces of Java state are non-volatile and not consistently guarded by
/// Java's `synchronized` blocks, because only the Sender thread touches them, and
/// they must NOT go behind the shared mutex.
///
/// Phase 4 makes that split. `pendingRequests` and
/// `inFlightRequestCorrelationId` are now fields on `Sender`, and the thirteen
/// manager methods that Java implements by touching them take them as
/// parameters instead — see [`PendingRequests`] for the mechanism and PLAN §10.5
/// deviation 7 for the enumeration. Four of those thirteen touch *only*
/// Sender-confined state and so moved to `Sender` outright:
/// `setInFlightCorrelationId` (Java 973), `clearInFlightCorrelationId` (977),
/// `hasInFlightRequest` (981) and `hasPendingRequests` (1005), together with the
/// unsynchronized half of `TxnRequestHandler.onComplete` (1406-1420).
///
/// The other three pieces of §2 state — `transactionCoordinator` (Java 137),
/// `consumerGroupCoordinator` (138) and `coordinatorSupportsBumpingEpoch` (139)
/// — have **no field here to move**: the whole coordinator subsystem
/// (`coordinator` 958, `lookupCoordinator` 969, `handleCoordinatorReady` 1103)
/// is Phase 5, because `InitProducerIdHandler.coordinatorType()` returns `null`
/// for a non-transactional producer. When Phase 5 adds them they belong on
/// `Sender` for the same reason, not here.
///
/// Two hard rules apply to every holder of the guard (rules §4): no guard may be
/// held across an `.await`, and the network poll is never raced in a
/// `tokio::select!`.
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

    /// Partitions added to the transaction locally but not yet sent in an
    /// `AddPartitionsToTxn` request (Java 122).
    ///
    /// Always empty in Phase 5a: the only writer is `maybeAddPartition`'s
    /// registration branch (Java 458), which lands with
    /// `AddPartitionsToTxnHandler` in Phase 5b — see [`Self::maybe_add_partition`].
    new_partitions_in_transaction: HashSet<TopicPartition>,
    /// Partitions whose `AddPartitionsToTxn` request is in flight (Java 123).
    /// Written only by `addPartitionsToTransactionHandler` (Java 1315), Phase 5b.
    pending_partitions_in_transaction: HashSet<TopicPartition>,
    /// Partitions the broker has confirmed as part of the transaction (Java 124).
    /// Written only by `AddPartitionsToTxnHandler.handleResponse` and
    /// `maybeAddPartition`'s Transaction V2 arm, both Phase 5b.
    partitions_in_transaction: HashSet<TopicPartition>,
    /// The operation whose [`TransactionalRequestResult`] the caller has not yet
    /// acknowledged (Java 125).
    ///
    /// See [`Self::handle_cached_transaction_request_result`] for the semantics;
    /// `.claude/rules/producer-transactions.md` §5 is the binding contract.
    pending_transition: Option<PendingStateTransition>,

    // NOTE (rules §2): Java's `pendingRequests` (Java 121) and
    // `inFlightRequestCorrelationId` (136) are deliberately absent here. They are
    // fields on `Sender`, and the manager methods that touch them take them as
    // parameters — see [`PendingRequests`].
    //
    // This is used by the TxnRequestHandlers to control how long to back off before a given request is retried.
    // For instance, this value is lowered by the AddPartitionsToTxnHandler when it receives a CONCURRENT_TRANSACTIONS
    // error for the first AddPartitionsRequest in a transaction.
    retry_backoff_ms: i64,

    /// Whether anything has been added to the current transaction, so an `EndTxn`
    /// would have work to do (Java 143).
    ///
    /// Always `false` in Phase 5a: every writer (Java 420, 451, 1629, 1831) is a
    /// Transaction V2 arm or a Phase-5b handler. [`Self::reset_transaction_state`]
    /// clears it, which is why the field is here rather than in 5b.
    transaction_started: bool,

    current_state: State,
    last_error: Option<KafkaError>,
    producer_id_and_epoch: ProducerIdAndEpoch,
    client_side_epoch_bump_required: bool,
    /// Always `false` in Phase 5a: only `maybeUpdateTransactionV2Enabled`
    /// (Java 492, Phase 5b) sets it, and that method needs KIP-890 feature
    /// discovery. Kept as a field so [`Self::set_producer_id_and_epoch`]'s
    /// log-level fork (Java 605) and the three predicates that read it
    /// ([`Self::need_to_trigger_epoch_bump_from_client`],
    /// [`Self::can_handle_abortable_error`], [`Self::maybe_add_partition`]) can be
    /// translated verbatim rather than approximated.
    is_transaction_v2_enabled: bool,
    enable_2pc: bool,
    /// Whether the transaction coordinator's `InitProducerId` version supports a
    /// client-triggered epoch bump (Java 139).
    ///
    /// # Why this is not Sender-owned like the coordinator nodes
    ///
    /// `.claude/rules/producer-transactions.md` §2 and PLAN §6.5 list this field
    /// with `transactionCoordinator` / `consumerGroupCoordinator` /
    /// `inFlightRequestCorrelationId` as state that is "touched exclusively by the
    /// Sender thread" and must therefore live on the `Sender`. That premise is
    /// true of the other three and **false of this one**: Java reads it from the
    /// *application* thread on every failed send, through
    /// `KafkaProducer.doSend`'s `catch (ApiException e)`
    /// (`KafkaProducer.java:1066`) → [`Self::maybe_transition_to_error_state`]
    /// (`:781`) → [`Self::need_to_trigger_epoch_bump_from_client`] (`:1310`). That
    /// call site already exists here, at `kafka_producer.rs:775`, so a
    /// Sender-confined field could not serve it.
    ///
    /// Keeping it behind the shared mutex costs nothing, which is the other half
    /// of the argument. §2's objection to the mutex is that it would be "slower
    /// and less faithful", and neither applies:
    ///
    ///   - Every reader is a `TransactionManager` method whose caller already
    ///     holds the guard for other reasons, so no lock is added.
    ///   - Its only writer, [`Self::handle_coordinator_ready`], must hold the
    ///     guard regardless: Java's version reads `apiVersions` (Java 1104), a
    ///     manager field. It runs once per coordinator connection.
    ///
    /// Recorded as a deviation in PLAN §10.7. The coordinator *nodes* do live on
    /// the `Sender` as §2 requires — `Sender.java:481` is their only reader.
    coordinator_supports_bumping_epoch: bool,
    /// The producer id and epoch of the transaction prepared for a two-phase
    /// commit (Java 148).
    ///
    /// Write-only in Phase 5a. Its writers (`prepareTransaction` Java 342,
    /// `InitProducerIdHandler`'s `keepPreparedTxn` arm Java 1507) and its reader
    /// (`preparedTransactionState()` Java 1976) are all KIP-939, Phase 5b; the
    /// field is here because [`Self::reset_transaction_state`] clears it.
    prepared_txn_state: ProducerIdAndEpoch,
}

impl TransactionManager {
    /// Creates a transaction manager.
    ///
    /// Translated from `TransactionManager(LogContext, String, int, long,
    /// ApiVersions, boolean)` (Java 208).
    ///
    /// Phase 3's MILESTONE-11 GUARD, which refused a `transactional_id` so that
    /// the untranslated transactional arms stayed unreachable, is gone: Phase 5a
    /// implements the transactional state machine, and the arms it still defers
    /// (Transaction V2, two-phase commit, and the four Phase-5b request handlers)
    /// each fail loudly with [`Errors::UnsupportedVersion`] naming Phase 5b, per
    /// CLAUDE.md §5. `KafkaProducer::from_config` keeps its own guard on
    /// `transactional.id` until Phase 6 wires the public API (PLAN §7.1), so the
    /// only way to build a transactional manager today is directly.
    pub(crate) fn new(
        log_context: LogContext,
        transactional_id: Option<String>,
        transaction_timeout_ms: i32,
        retry_backoff_ms: i64,
        api_versions: Arc<ApiVersions>,
        enable_2pc: bool,
    ) -> Self {
        Self {
            txn_partition_map: TxnPartitionMap::new(log_context.clone()),
            log_context,
            transactional_id,
            transaction_timeout_ms,
            api_versions,
            partitions_with_unresolved_sequences: HashMap::new(),
            partitions_to_rewrite_sequences: HashSet::new(),
            new_partitions_in_transaction: HashSet::new(),
            pending_partitions_in_transaction: HashSet::new(),
            partitions_in_transaction: HashSet::new(),
            pending_transition: None,
            retry_backoff_ms,
            transaction_started: false,
            current_state: State::Uninitialized,
            last_error: None,
            producer_id_and_epoch: ProducerIdAndEpoch::NONE,
            client_side_epoch_bump_required: false,
            is_transaction_v2_enabled: false,
            enable_2pc,
            coordinator_supports_bumping_epoch: false,
            prepared_txn_state: ProducerIdAndEpoch::NONE,
        }
    }

    // -- Transactional entry points ----------------------------------------

    /// Acquires or bumps the producer id for a transactional producer, returning
    /// the handle the application awaits.
    ///
    /// Translated from `initializeTransactions(boolean keepPreparedTxn)`
    /// (Java 295), the public overload `KafkaProducer.initTransactions` calls.
    ///
    /// # Not blocking here
    ///
    /// Java's caller blocks on `result.await(maxBlockTimeMs, ..)`
    /// (`KafkaProducer.java:654`). This returns the [`TransactionalRequestResult`]
    /// instead, and Phase 6's `KafkaProducer::init_transactions` awaits it — the
    /// manager must not await anything, because the caller holds the shared mutex
    /// and rules §4 forbids holding it across an `.await`.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::IllegalState`] on a non-transactional producer
    ///   (`ensureTransactional`), when the manager is already in an error state
    ///   (`maybeFailWithError`), when a *different* operation's result is still
    ///   unacknowledged, or when `UNINITIALIZED → INITIALIZING` is not a valid
    ///   transition — which is what rejects a second `initTransactions` after the
    ///   first has been acknowledged (`testInitializeTransactionsTwiceRaisesError`).
    pub(crate) fn initialize_transactions(
        &mut self,
        keep_prepared_txn: bool,
        pending_requests: &mut PendingRequests,
    ) -> Result<Arc<TransactionalRequestResult>, KafkaError> {
        self.initialize_transactions_internal(ProducerIdAndEpoch::NONE, keep_prepared_txn, pending_requests)
    }

    /// Bumps the epoch of an existing producer id as part of ending a
    /// transaction.
    ///
    /// Translated from the package-private overload
    /// `initializeTransactions(ProducerIdAndEpoch)` (Java 291), whose only Java
    /// caller is `beginCompletingTransaction` (`:1200`) when
    /// `clientSideEpochBumpRequired` holds — Phase 5b. Renamed because Rust has no
    /// overloading, the same treatment
    /// [`Self::producer_id_and_epoch_for_partition`] gets (PLAN §10.5
    /// deviation 4).
    ///
    /// Passing a valid id and epoch is what makes this an *epoch bump*: the
    /// request carries them, no `INITIALIZING` transition happens here (the
    /// `EndTxn` response drives it), and the handler sorts at
    /// [`Priority::EpochBump`].
    pub(crate) fn initialize_transactions_with_producer_id_and_epoch(
        &mut self,
        producer_id_and_epoch: ProducerIdAndEpoch,
        pending_requests: &mut PendingRequests,
    ) -> Result<Arc<TransactionalRequestResult>, KafkaError> {
        self.initialize_transactions_internal(producer_id_and_epoch, false, pending_requests)
    }

    /// Translated from the package-private
    /// `initializeTransactions(ProducerIdAndEpoch, boolean)` (Java 299), which
    /// both public overloads delegate to.
    ///
    /// # `keep_prepared_txn` reaches only the log statement
    ///
    /// Java uses the flag for two `log.info` lines (`:309`, `:312`) and **does not
    /// put it on the request**: the `InitProducerIdRequestData` built at `:316-320`
    /// sets `transactionalId`, `transactionTimeoutMs`, `producerId` and
    /// `producerEpoch` only, and `setKeepPreparedTxn` appears nowhere in
    /// `clients/src` in Apache Kafka 4.2. So `builder.data.keepPreparedTxn()`, the
    /// condition guarding the two-phase-commit response arm at `:1501`, is always
    /// `false` on this path. Translated as-is rather than "fixed": the arm's
    /// [`Errors::UnsupportedVersion`] guard in
    /// [`Self::handle_init_producer_id_response`] is therefore unreachable from
    /// here in Rust exactly as it is in Java.
    fn initialize_transactions_internal(
        &mut self,
        producer_id_and_epoch: ProducerIdAndEpoch,
        keep_prepared_txn: bool,
        pending_requests: &mut PendingRequests,
    ) -> Result<Arc<TransactionalRequestResult>, KafkaError> {
        self.maybe_fail_with_error()?;

        let is_epoch_bump = producer_id_and_epoch != ProducerIdAndEpoch::NONE;
        self.handle_cached_transaction_request_result(
            |manager| {
                // If this is an epoch bump, we will transition the state as part of handling the EndTxnRequest
                if !is_epoch_bump {
                    // Java reaches this from `KafkaProducer.initTransactions` only,
                    // so the transition is application-side (rules §1).
                    manager.transition_to(State::Initializing, None, Caller::App)?;
                    kafka_info!(
                        manager.log_context,
                        "Invoking InitProducerId for the first time in order to acquire a producer ID"
                    );
                    if keep_prepared_txn {
                        kafka_info!(
                            manager.log_context,
                            "Invoking InitProducerId with keepPreparedTxn set to true for 2PC transactions"
                        );
                    }
                } else {
                    kafka_info!(
                        manager.log_context,
                        "Invoking InitProducerId with current producer ID and epoch {} in order to bump the epoch",
                        producer_id_and_epoch
                    );
                }

                let mut request_data = InitProducerIdRequestData::new();
                request_data
                    .set_transactional_id(manager.transactional_id.clone())
                    .set_transaction_timeout_ms(manager.transaction_timeout_ms)
                    .set_producer_id(producer_id_and_epoch.producer_id)
                    .set_producer_epoch(producer_id_and_epoch.epoch);

                let handler = TxnRequestHandler::new(
                    "InitProducerId",
                    manager.retry_backoff_ms,
                    TxnRequestHandlerKind::InitProducerId {
                        builder: InitProducerIdRequestBuilder::new(request_data),
                        is_epoch_bump,
                    },
                );
                let result = Arc::clone(handler.result());
                manager.enqueue_request(pending_requests, handler);
                Ok(result)
            },
            State::Initializing,
            "initTransactions",
        )
    }

    /// Starts a transaction.
    ///
    /// Translated from `beginTransaction()` (Java 330). Stays synchronous: Java's
    /// body is four calls and no wait (PLAN §Phase-6 makes the same point).
    ///
    /// Reached from `KafkaProducer.beginTransaction` only, so the transition is
    /// application-side (rules §1).
    ///
    /// # Errors
    ///
    /// [`KafkaError::IllegalState`] on a non-transactional producer, while another
    /// operation's result is unacknowledged, or when the manager is in an error
    /// state; and from `READY → IN_TRANSACTION` being the table's only arm into
    /// [`State::InTransaction`], which is what rejects `beginTransaction` before
    /// `initTransactions` completes.
    pub(crate) fn begin_transaction(&mut self) -> Result<(), KafkaError> {
        self.ensure_transactional()?;
        self.throw_if_pending_state("beginTransaction")?;
        self.maybe_fail_with_error()?;
        self.transition_to(State::InTransaction, None, Caller::App)
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
    /// there is no accessor. Kept because it is the only reader outside the
    /// module, and `SenderTest`'s harness asserts on it.
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

    /// Whether an `EndTxn` is in progress.
    ///
    /// Corresponds to `isCompleting()` (Java 518).
    ///
    /// Scheduled nowhere in the plan, added here because `Sender.run`'s
    /// shutdown loop calls it (`Sender.java:268`) and that call site is reachable
    /// for a purely idempotent producer — see [`Self::has_ongoing_transaction`].
    /// Always `false` idempotently: neither `COMMITTING_TRANSACTION` nor
    /// `ABORTING_TRANSACTION` is reachable without a transactional id.
    pub(crate) fn is_completing(&self) -> bool {
        self.current_state == State::CommittingTransaction || self.current_state == State::AbortingTransaction
    }

    /// Whether an `EndTxn(ABORT)` is in progress.
    ///
    /// Corresponds to `isAborting()` (Java 526). Called from
    /// `maybeSendAndPollTransactionalRequest` (`Sender.java:468`), which Phase 4
    /// translates. Always `false` idempotently, for the same reason as
    /// [`Self::is_completing`].
    pub(crate) fn is_aborting(&self) -> bool {
        self.current_state == State::AbortingTransaction
    }

    /// Whether a transaction is considered ongoing.
    ///
    /// Corresponds to `hasOngoingTransaction()` (Java 1010): "transactions are
    /// considered ongoing once started until completion or a fatal error".
    ///
    /// # This is reachable for an idempotent producer
    ///
    /// The third disjunct is [`Self::has_abortable_error`], and `ABORTABLE_ERROR`
    /// *is* reachable without a transactional id (PLAN §9.15:
    /// `InitProducerIdHandler.handleResponse` calls `abortableError` for
    /// `CLUSTER_AUTHORIZATION_FAILED` at Java 1524-1528 without testing
    /// `isTransactional()`). So both of this method's Java call sites are live for
    /// an idempotent producer:
    ///
    ///   - `Sender.hasPendingTransactionalRequests()` (`Sender.java:234`), which
    ///     gates the first shutdown loop at `:258`;
    ///   - the second shutdown loop's own condition at `:267`, whose body calls
    ///     `beginAbort()` — see [`Self::begin_abort`] for what Java does with the
    ///     `IllegalStateException` that produces.
    pub(crate) fn has_ongoing_transaction(&self) -> bool {
        self.current_state == State::InTransaction || self.is_completing() || self.has_abortable_error()
    }

    /// Begins aborting the transaction.
    ///
    /// Translated from `beginAbort()` (Java 361), whose body is wrapped in
    /// `handleCachedTransactionRequestResult(.., "abortTransaction")` and so begins
    /// with `ensureTransactional()` (Java 1266).
    ///
    /// Phase 4 needs it because `Sender.run`'s shutdown loop calls it at
    /// `Sender.java:273`, inside a `try`/`catch` that force-closes the producer if
    /// it throws (`:274-278`). For every producer this client can build today the
    /// `ensureTransactional()` guard is what throws, so translating that guard is
    /// what makes the shutdown path behave as Java's does; the rest of the body
    /// (`transitionTo(ABORTING_TRANSACTION)`, `beginCompletingTransaction`, the
    /// `EndTxn` handler) is Phase 6 and is unreachable while [`Self::new`] refuses
    /// a transactional id.
    ///
    /// # Errors
    ///
    /// [`KafkaError::IllegalState`] on a non-transactional producer, with Java's
    /// message.
    pub(crate) fn begin_abort(&mut self) -> Result<(), KafkaError> {
        self.ensure_transactional()?;
        Err(KafkaError::unsupported_version(
            "Aborting a transaction is not yet implemented in this client (Milestone 11, Phase 6).",
        ))
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
    fn force_enqueue_init_producer_id_for_test(
        &mut self,
        pending_requests: &mut PendingRequests,
    ) -> Arc<TransactionalRequestResult> {
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
        self.enqueue_request(pending_requests, handler);
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
        self.transition_to(State::FatalError, Some(error.clone()), caller)?;

        // Java 545-547. [`State::FatalError`] is an unconditionally valid target,
        // so `transition_to` above cannot fail and this always runs — matching
        // Java, where the two statements are sequential.
        if let Some(pending) = self.pending_transition.as_ref() {
            pending.result.fail(error);
        }
        Ok(())
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

    /// Moves to [`State::AbortableError`] when the coordinator can recover from
    /// one, and to [`State::FatalError`] when it cannot.
    ///
    /// Translated from
    /// `transitionToAbortableErrorOrFatalError(RuntimeException, RuntimeException)`
    /// (Java 557), whose only caller is [`Self::maybe_resolve_sequences`]'s
    /// transactional arm (Java 870).
    ///
    /// Recovering from an abortable error requires an epoch bump. If the
    /// coordinator supports a client-triggered one, request it and take the
    /// abortable path; if Transaction V2 handles it server-side, take the
    /// abortable path without requesting anything; otherwise there is no way back
    /// and the error is fatal.
    fn transition_to_abortable_error_or_fatal_error(
        &mut self,
        abortable_error: KafkaError,
        fatal_error: KafkaError,
        caller: Caller,
    ) -> Result<(), KafkaError> {
        if self.can_handle_abortable_error() {
            if self.need_to_trigger_epoch_bump_from_client() {
                self.client_side_epoch_bump_required = true;
            }
            self.transition_to_abortable_error(abortable_error, caller)
        } else {
            self.transition_to_fatal_error(fatal_error, caller)
        }
    }

    /// Determines if an epoch bump can be triggered manually based on the api versions.
    ///
    /// Translated from `needToTriggerEpochBumpFromClient()` (Java 1309).
    ///
    /// **NOTE:** This method should only be used for transactional producers. For
    /// non-transactional producers epoch bumping is always allowed.
    ///
    /// 1. **Client-Triggered Epoch Bump**: if the coordinator supports epoch
    ///    bumping (`initProducerIdVersion.maxVersion() >= 3`), client-triggered
    ///    epoch bumping is allowed, returns true.
    ///    `clientSideEpochBumpRequired` must be set to true in this case.
    /// 2. **No Epoch Bump Allowed**: if the coordinator does not support epoch
    ///    bumping, returns false.
    /// 3. **Server-Triggered Only**: when Transaction V2 is enabled, epoch bumping
    ///    is handled automatically by the server in `EndTxn`, so manual epoch
    ///    bumping is not required, returns false.
    pub(crate) fn need_to_trigger_epoch_bump_from_client(&self) -> bool {
        self.coordinator_supports_bumping_epoch && !self.is_transaction_v2_enabled
    }

    /// Determines if the coordinator can handle an abortable error.
    ///
    /// Translated from `canHandleAbortableError()` (Java 1326).
    ///
    /// Recovering from an abortable error requires an epoch bump which can be
    /// triggered by the client or automatically taken care of at the end of every
    /// transaction (Transaction V2). Use
    /// [`Self::need_to_trigger_epoch_bump_from_client`] to check whether the epoch
    /// bump needs to be triggered manually.
    ///
    /// **NOTE:** This method should only be used for transactional producers.
    /// There is no concept of abortable errors for idempotent producers.
    fn can_handle_abortable_error(&self) -> bool {
        self.coordinator_supports_bumping_epoch || self.is_transaction_v2_enabled
    }

    /// Clears the per-transaction state once a transaction has completed.
    ///
    /// Translated from `resetTransactionState()` (Java 1330).
    ///
    /// Both Java call sites run on the Sender thread — `nextRequest`'s
    /// "EndTxn for a transaction that never started" branch (Java 923) and
    /// `EndTxnHandler.handleResponse` (Java 1767) — so [`Caller::Sender`] is
    /// hardcoded per rules §1 rather than taken as a parameter. Both are Phase 5b,
    /// which is why this method has no caller yet; it is translated now because it
    /// is the only writer that clears the per-transaction sets and
    /// `prepared_txn_state`, and splitting it from the state machine would mean
    /// writing it twice.
    fn reset_transaction_state(&mut self) -> Result<(), KafkaError> {
        if self.client_side_epoch_bump_required {
            self.transition_to(State::Initializing, None, Caller::Sender)?;
        } else {
            self.transition_to(State::Ready, None, Caller::Sender)?;
        }
        self.last_error = None;
        self.client_side_epoch_bump_required = false;
        self.transaction_started = false;
        self.new_partitions_in_transaction.clear();
        self.pending_partitions_in_transaction.clear();
        self.partitions_in_transaction.clear();
        self.prepared_txn_state = ProducerIdAndEpoch::NONE;
        Ok(())
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
    /// `error` reaches only `pendingTransition.result.fail(..)` (Java 759), so it
    /// has no consumer for an idempotent producer — `pendingTransition` is set
    /// only by [`Self::handle_cached_transaction_request_result`], which begins
    /// with `ensureTransactional()`. Phase 3 therefore omitted the parameter and
    /// Phase 5a adds it back together with the field, as PLAN §10.5 deviation 8
    /// said it would.
    ///
    /// `Sender.shouldHandleAuthorizationError` passes the **raw** exception here
    /// (`Sender.java:356`), not the `new AuthenticationException(exception)`
    /// wrapper it hands to [`Self::fail_pending_requests`] one line earlier
    /// (`:354`). Preserved.
    pub(crate) fn transition_to_uninitialized(&mut self, error: &KafkaError, caller: Caller) -> Result<(), KafkaError> {
        self.transition_to(State::Uninitialized, None, caller)?;
        // Java 758-760.
        if let Some(pending) = self.pending_transition.as_ref() {
            pending.result.fail(error.clone());
        }
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
    /// failed handler stays queued; that is preserved. The iteration is now a
    /// direct `forEach` equivalent — before the rules §2 split the queue was a
    /// field on this struct, so the loop had to go by index to leave `&mut self`
    /// free for [`Self::transition_to_abortable_error`]; with the queue passed in
    /// the two borrows are disjoint.
    pub(crate) fn fail_pending_requests(
        &mut self,
        pending_requests: &mut PendingRequests,
        error: &KafkaError,
        caller: Caller,
    ) -> Result<(), KafkaError> {
        for handler in pending_requests.iter() {
            // Java: handler.abortableError(exception), i.e. result.fail(e) then
            // transitionToAbortableError(e), per handler and in that order.
            handler.fail(error.clone());
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
    pub(crate) fn authentication_failed(
        &mut self,
        pending_requests: &mut PendingRequests,
        error: &KafkaError,
        caller: Caller,
    ) -> Result<(), KafkaError> {
        for handler in pending_requests.iter() {
            // Java: request.fatalError(e), i.e. result.fail(e) then
            // transitionToFatalError(e).
            handler.fail(error.clone());
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
    /// # Why this landed in Phase 3 rather than with its call site in Phase 6
    ///
    /// **On the idempotent path this method has no observable effect at all**, and
    /// the reason it is here is scheduling, not behaviour:
    ///
    ///   - It was unscheduled in every phase of the plan — the same gap that left
    ///     [`Self::authentication_failed`] out (Critic 43 issue 1).
    ///   - Twelve lines, and its only Phase-5 dependency is the
    ///     `pendingTransition` branch that is always null idempotently.
    ///   - Its call site is reachable for an idempotent producer:
    ///     `transactionManager != null` holds at `Sender.java:290`.
    ///
    /// Its **behavioural** payoff is Phase 6 and transactional. Java states it two
    /// lines above the call (`Sender.java:288-289`): "fail all the incomplete
    /// transactional requests and batches and *wake up the threads waiting on the
    /// futures*". Those threads are `KafkaProducer.initTransactions` &c. blocked in
    /// `result.await(maxBlockTimeMs, ..)` (`KafkaProducer.java:654`). Phase 5a
    /// makes half of that real — [`Self::initialize_transactions`] now hands out a
    /// result and this method's `pendingTransition` branch fails it — and Phase 6
    /// adds the `KafkaProducer` method that awaits it.
    ///
    /// Nothing awaits an idempotent `InitProducerId` result, so there is no
    /// hanging future to prevent here: the handler is built inside
    /// `bumpIdempotentEpochAndResetIdIfNeeded` (Java 663-676) and its
    /// [`TransactionalRequestResult`] never leaves it, because the only method
    /// that hands a result to a caller is `initializeTransactions` via
    /// `handleCachedTransactionRequestResult`, whose first statement is
    /// `ensureTransactional()` (Java 1266).
    ///
    /// The [`State::FatalError`] this writes is also never read on the idempotent
    /// path: `close()` is the Sender task's terminal act. Its only call site
    /// (`Sender.java:292`) sits inside `if (forceClose)` at `:287`, *after* all
    /// three `run()` loops, and both post-shutdown loops (`:258`, `:267`) are
    /// `!forceClose`-guarded, so once `forceClose` is set no `runOnce` executes at
    /// all. Only `accumulator.abortIncompleteBatches()` (`:295`) and
    /// `client.close()` (`:298`) follow.
    ///
    /// Note the transition happens **inside** the loop, so an empty queue means no
    /// transition at all — Java's behaviour, preserved.
    ///
    /// Two earlier revisions of this comment claimed a present-tense idempotent
    /// payoff — first a hanging future (Critic 43 issue 5), then the `FATAL_ERROR`
    /// transition stopping a subsequent `runOnce` at `:318` (issue 7). Both were
    /// false, the second refuted by the twenty lines of `Sender.run` around the
    /// call site it cited. Recorded because the pull toward inventing a
    /// present-tense payoff is what produced both.
    pub(crate) fn close(&mut self, pending_requests: &mut PendingRequests, caller: Caller) -> Result<(), KafkaError> {
        let shutdown_error = KafkaError::with_message(Errors::UnknownServerError, "The producer closed forcefully");
        for handler in pending_requests.iter() {
            handler.fail(shutdown_error.clone());
            self.transition_to_fatal_error(shutdown_error.clone(), caller)?;
        }
        // Java 953-955. Note this runs even when the queue is empty, so a
        // transactional caller blocked in `initTransactions` is woken by a force
        // close with no request outstanding — which is the point of the method
        // (`Sender.java:288-289`). Reached independently of the loop above, since
        // `transitionToFatalError` fails the *same* slot per iteration.
        if let Some(pending) = self.pending_transition.as_ref() {
            pending.result.fail(shutdown_error);
        }
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

    /// Rejects an operation while a previous one's result is still
    /// unacknowledged.
    ///
    /// Translated from `throwIfPendingState(String)` (Java 1249).
    ///
    /// Takes `&mut self` because Java clears `pendingTransition` here: an
    /// *acknowledged* result means the previous operation is genuinely finished,
    /// so the slot is released and the new operation proceeds. An unacknowledged
    /// one means the caller's `await` timed out and must be retried — the *same*
    /// operation, not a different one — so anything else is rejected. That
    /// `isAcked()` key, rather than `isCompleted()`, is what
    /// `.claude/rules/producer-transactions.md` §5 exists to protect: a completed
    /// but never-awaited `commitTransaction` must still be retryable.
    fn throw_if_pending_state(&mut self, operation: &str) -> Result<(), KafkaError> {
        if let Some(pending) = self.pending_transition.as_ref() {
            if pending.result.is_acked() {
                self.pending_transition = None;
            } else {
                return Err(KafkaError::illegal_state(format!(
                    "Cannot attempt operation `{operation}` because the previous call to `{}` timed out and must \
                     be retried",
                    pending.operation
                )));
            }
        }
        Ok(())
    }

    /// Runs `supplier` unless an equivalent operation is already pending, in
    /// which case its existing result is handed back.
    ///
    /// Translated from
    /// `handleCachedTransactionRequestResult(Supplier<TransactionalRequestResult>, State, String)`
    /// (Java 1261).
    ///
    /// The three outcomes, keyed on [`TransactionalRequestResult::is_acked`] and
    /// **not** `is_completed` (`.claude/rules/producer-transactions.md` §5):
    ///
    ///   1. The pending result has been acknowledged — the previous operation is
    ///      finished, so the slot is released and `supplier` runs.
    ///   2. It has not, and `next_state` differs — the caller's `await` timed out
    ///      and a *different* operation is being attempted. Rejected with
    ///      [`KafkaError::IllegalState`]; the pending operation stays retryable.
    ///   3. It has not, and `next_state` matches — the caller is retrying the same
    ///      operation. The **same** `Arc` is returned, so a `commitTransaction`
    ///      that already completed is not sent twice.
    ///
    /// # Why the supplier takes `&mut Self`
    ///
    /// Java's `Supplier` closes over `TransactionManager.this`. A Rust closure
    /// cannot capture `self` while `self` is borrowed by this method, so the
    /// manager is passed in as an argument instead. It returns `Result` because
    /// Java's suppliers can throw: `initializeTransactions`'s calls `transitionTo`
    /// (`:308`) and `beginCommit`'s calls `maybeFailWithError` (`:354`). When it
    /// does, `pendingTransition` is left unset, exactly as in Java.
    fn handle_cached_transaction_request_result<F>(
        &mut self,
        supplier: F,
        next_state: State,
        operation: &str,
    ) -> Result<Arc<TransactionalRequestResult>, KafkaError>
    where
        F: FnOnce(&mut Self) -> Result<Arc<TransactionalRequestResult>, KafkaError>,
    {
        self.ensure_transactional()?;

        if let Some(pending) = self.pending_transition.as_ref() {
            if pending.result.is_acked() {
                self.pending_transition = None;
            } else if next_state != pending.state {
                return Err(KafkaError::illegal_state(format!(
                    "Cannot attempt operation `{operation}` because the previous call to `{}` timed out and must \
                     be retried",
                    pending.operation
                )));
            } else {
                return Ok(Arc::clone(&pending.result));
            }
        }

        let result = supplier(self)?;
        self.pending_transition = Some(PendingStateTransition::new(Arc::clone(&result), next_state, operation));
        Ok(result)
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
            // RetriableExceptions from the Sender thread are converted to Abortable errors
            // because they indicate that the transaction cannot be completed after all retry attempts.
            // This conversion ensures the application layer treats these errors as abortable,
            // preventing duplicate message delivery.
            //
            // Java tests `instanceof RetriableException || instanceof
            // InvalidTxnStateException`. `InvalidTxnStateException` is **not** a
            // `RetriableException`, so both tests are needed. The `TransactionAbortableException`
            // Java builds chains the original as its cause; `KafkaError` has no cause chain
            // (PLAN §10.5 deviation 5), so the message is reproduced exactly and the original
            // stays reachable through the caller's own value.
            let error = if error.is_retriable() || error.error() == Errors::InvalidTxnState {
                KafkaError::with_message(
                    Errors::TransactionAbortable,
                    "Transaction Request was aborted after exhausting retries.",
                )
            } else {
                error.clone()
            };

            if self.need_to_trigger_epoch_bump_from_client() && !self.is_completing() {
                self.client_side_epoch_bump_required = true;
            }
            return self.transition_to_abortable_error(error, caller);
        }
        Ok(())
    }

    /// Whether the accumulator may drain batches for `topic_partition`.
    ///
    /// Translated from `isSendToPartitionAllowed(TopicPartition)` (Java 466),
    /// called from `RecordAccumulator.shouldStopDrainBatchesForPartition`
    /// (`RecordAccumulator.java:818`) — which Phase 4 translates, hence this method
    /// arriving before the rest of the transactional entry points.
    ///
    /// Phase 5a completes the transactional arm. `partitions_in_transaction` is
    /// necessarily empty until Phase 5b adds `AddPartitionsToTxnHandler`, so a
    /// transactional producer is refused every partition — which is Java's own
    /// answer for an empty set, and is consistent, because
    /// [`Self::maybe_add_partition`] refuses to register one in the first place.
    pub(crate) fn is_send_to_partition_allowed(&self, topic_partition: &TopicPartition) -> bool {
        if self.has_fatal_error() {
            return false;
        }
        !self.is_transactional() || self.partitions_in_transaction.contains(topic_partition)
    }

    /// Whether any partition still needs adding to the transaction.
    ///
    /// Corresponds to `hasPartitionsToAdd()` (Java 514).
    pub(crate) fn has_partitions_to_add(&self) -> bool {
        !self.new_partitions_in_transaction.is_empty() || !self.pending_partitions_in_transaction.is_empty()
    }

    /// Whether `partition` has been added to the transaction locally but not yet
    /// confirmed by the coordinator.
    ///
    /// Corresponds to `isPartitionPendingAdd(TopicPartition)` (Java 571).
    pub(crate) fn is_partition_pending_add(&self, partition: &TopicPartition) -> bool {
        self.new_partitions_in_transaction.contains(partition)
            || self.pending_partitions_in_transaction.contains(partition)
    }

    /// Whether the coordinator has confirmed `topic_partition` as part of the
    /// transaction.
    ///
    /// Corresponds to `transactionContainsPartition(TopicPartition)` (Java 993).
    pub(crate) fn transaction_contains_partition(&self, topic_partition: &TopicPartition) -> bool {
        self.partitions_in_transaction.contains(topic_partition)
    }

    /// Whether a transactional producer's `InitProducerId` is still outstanding.
    ///
    /// Corresponds to `isInitializing()` (Java 1090). Java has no caller for it in
    /// either the client or its tests; translated because it is part of the class
    /// (`definition-of-done.md` §2).
    pub(crate) fn is_initializing(&self) -> bool {
        self.is_transactional() && self.current_state == State::Initializing
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
        pending_requests: &mut PendingRequests,
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
                self.enqueue_request(pending_requests, handler);
            }
        }
        Ok(())
    }

    /// Whether a client-side epoch bump has been requested and not yet applied.
    ///
    /// Java reads the `clientSideEpochBumpRequired` field (Java 141) directly
    /// from inside the class; this accessor exists because the `Sender` must know
    /// whether [`Self::bump_idempotent_epoch_and_reset_id_if_needed`] is going to
    /// need a populated [`InFlightBatchPool`] *before* it takes the manager lock —
    /// assembling the pool means locking the accumulator's per-partition deques
    /// first (rules §3), which is pure waste on the overwhelmingly common path
    /// where no bump is pending. Java needs no equivalent because its
    /// `TxnPartitionEntry` holds live references to the batches (rules §7).
    pub(crate) fn client_side_epoch_bump_required(&self) -> bool {
        self.client_side_epoch_bump_required
    }

    /// The partitions whose in-flight sequences will be rewritten by the next
    /// epoch bump.
    ///
    /// Java reads the `partitionsToRewriteSequences` field (Java 133) directly.
    /// Exposed for the same reason as [`Self::client_side_epoch_bump_required`]:
    /// the `Sender` assembles the [`InFlightBatchPool`] for exactly these
    /// partitions and no others, because
    /// [`Self::bump_idempotent_producer_epoch`] iterates only this set.
    pub(crate) fn partitions_to_rewrite_sequences(&self) -> &HashSet<TopicPartition> {
        &self.partitions_to_rewrite_sequences
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
    /// Takes a [`Caller`] as of Phase 5a: the idempotent arm performs no state
    /// transition, but the transactional arm reaches
    /// [`Self::transition_to_abortable_error_or_fatal_error`] and so needs it
    /// (PLAN §10.5 deviation 6 said this phase would add it).
    pub(crate) fn maybe_resolve_sequences(&mut self, caller: Caller) -> Result<(), KafkaError> {
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
                // For the transactional producer, we bump the epoch if possible, otherwise we transition to a
                // fatal error.
                //
                // Java's two `new KafkaException(..)` instances carry no wire code,
                // which this crate spells as `Errors::UnknownServerError` (the same
                // convention `maybe_fail_with_error` and `close` use).
                const UNACKED_MESSAGES_ERR: &str = "The client hasn't received acknowledgment for some previously \
                                                    sent messages and can no longer retry them. ";
                let abortable_error = KafkaError::with_message(
                    Errors::UnknownServerError,
                    format!("{UNACKED_MESSAGES_ERR}It is safe to abort the transaction and continue."),
                );
                let fatal_error = KafkaError::with_message(
                    Errors::UnknownServerError,
                    format!("{UNACKED_MESSAGES_ERR}It isn't safe to continue."),
                );
                self.transition_to_abortable_error_or_fatal_error(abortable_error, fatal_error, caller)?;
                self.partitions_with_unresolved_sequences.remove(&topic_partition);
                continue;
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
    /// Corresponds to `enqueueRequest(TxnRequestHandler)` (Java 1186). Takes the
    /// queue from its owner (rules §2, see [`PendingRequests`]); `&self` is only
    /// needed for the log prefix.
    fn enqueue_request(&self, pending_requests: &mut PendingRequests, handler: TxnRequestHandler) {
        kafka_debug!(self.log_context, "Enqueuing transactional request {:?}", handler);
        pending_requests.add(handler);
    }

    /// The next transactional request to send, if any.
    ///
    /// Translated from `nextRequest(boolean)` (Java 894).
    ///
    /// Java's first statement enqueues an `AddPartitionsToTxn` when
    /// `newPartitionsInTransaction` is non-empty, and its `isEndTxn` branch
    /// short-circuits an `EndTxn` for a transaction that never started. Both are
    /// transaction-only, and [`TxnRequestHandler::is_end_txn`] is `false` for
    /// every handler Phase 5a can build, so neither is reachable; Phase 5b adds
    /// them with the handlers they need — `addPartitionsToTransactionHandler`
    /// (Java 1313) for the first and `EndTxnHandler` for the second. What keeps
    /// `new_partitions_in_transaction` empty is
    /// [`Self::maybe_add_partition`]'s deferred registration arm.
    pub(crate) fn next_request(
        &mut self,
        pending_requests: &mut PendingRequests,
        has_incomplete_batches: bool,
    ) -> Option<TxnRequestHandler> {
        let next_request_handler = pending_requests.peek()?;

        // Do not send the EndTxn until all batches have been flushed
        if next_request_handler.is_end_txn() && has_incomplete_batches {
            return None;
        }

        let next_request_handler = pending_requests.poll()?;
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

    // `hasPendingRequests()` (Java 1005) is `Sender::has_pending_requests`: its
    // body reads only the Sender-confined queue (rules §2).

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
    pub(crate) fn retry(&self, pending_requests: &mut PendingRequests, mut handler: TxnRequestHandler) {
        handler.set_retry();
        self.enqueue_request(pending_requests, handler);
    }

    // `setInFlightCorrelationId` (Java 973), `clearInFlightCorrelationId` (977)
    // and `hasInFlightRequest` (981) are `Sender` methods: each is a pure accessor
    // of `inFlightRequestCorrelationId`, which is Sender-confined (rules §2, see
    // [`PendingRequests`]). Java leaves all three unsynchronized, which is the
    // evidence for the confinement.

    /// Fails `handler` and moves the manager to [`State::FatalError`].
    ///
    /// Corresponds to `TxnRequestHandler.fatalError(RuntimeException)`
    /// (Java 1357).
    ///
    /// `pub(crate)` because the [`Sender`] reaches it from the unsynchronized half
    /// of `onComplete` (Java 1408, 1417, 1425).
    ///
    /// [`Sender`]: crate::producer::internals::Sender
    pub(crate) fn fatal_error(&mut self, handler: &TxnRequestHandler, error: KafkaError) -> Result<(), KafkaError> {
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
    /// returns the transactional id; only `TxnOffsetCommitHandler` (Phase 5b)
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

    // `TxnRequestHandler.onComplete(ClientResponse)` (Java 1406-1428) is split.
    // Everything Java runs *outside* the `synchronized (TransactionManager.this)`
    // block that begins at `:1421` — the correlation-id check, the clear, the
    // disconnect branch and the version-mismatch branch — is
    // `Sender::on_transactional_response`, because it touches the Sender-confined
    // correlation id (rules §2) and because taking the shared lock for it would
    // block the application task's `maybe_add_partition` where Java does not. The
    // `synchronized` half is [`Self::handle_response`] below, which the Sender
    // calls under the lock exactly as Java's block does. PLAN §10.5 deviation 7
    // named this split as the price of hosting the handler methods here.

    /// Dispatches a parsed response body to the handler that requested it.
    ///
    /// Corresponds to the abstract `handleResponse(AbstractResponse)`
    /// (Java 1456), invoked inside `onComplete`'s `synchronized` block
    /// (Java 1421-1423).
    ///
    /// `handler` is taken by value: Java's `reenqueue()` puts `this` back on the
    /// pending queue, so ownership has to move. `pending_requests` is the
    /// Sender-owned queue that the retriable-error arm re-enqueues into
    /// (Java 1533 → `reenqueue()` at 1394).
    ///
    /// The [`Caller`] is always [`Caller::Sender`]: Java invokes this from
    /// `NetworkClient.poll`, i.e. on the Sender thread, at every call site.
    pub(crate) fn handle_response(
        &mut self,
        handler: TxnRequestHandler,
        response: &ConcreteResponse,
        pending_requests: &mut PendingRequests,
    ) -> Result<(), KafkaError> {
        // One handler kind in this phase, so no dispatch is needed yet; Phase 5
        // matches on `handler.kind` here as Java dispatches on the subclass.
        self.handle_init_producer_id_response(handler, response, pending_requests)
    }

    /// Handles an `InitProducerId` response.
    ///
    /// Translated from `InitProducerIdHandler.handleResponse(AbstractResponse)`
    /// (Java 1491).
    fn handle_init_producer_id_response(
        &mut self,
        handler: TxnRequestHandler,
        response: &ConcreteResponse,
        pending_requests: &mut PendingRequests,
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
                // `preparedTxnState`. Still unreachable, and for Java's own
                // reason rather than a translation gap: nothing in Apache Kafka
                // 4.2's `clients/src` calls `setKeepPreparedTxn`, so
                // `builder.data.keepPreparedTxn()` is always false — see
                // [`Self::initialize_transactions_internal`]. Phase 5b lands the
                // KIP-939 surface that would set it.
                return Err(KafkaError::unsupported_version(
                    "Two-phase commit is not yet implemented in this client (Milestone 11, Phase 5b).",
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
            self.retry(pending_requests, handler);
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
    /// # Where Phase 5a stops
    ///
    /// The transactional arm is an ordered `if / else if` chain (Java 441-459).
    /// Phase 5a translates the three branches that need no request handler — the
    /// two state guards and the already-added short-circuit — because they are
    /// pure state validation and are what the `testFailIfNotReadyForSend*` /
    /// `testNotReadyForSend*` family asserts on. The last two branches belong with
    /// `AddPartitionsToTxnHandler` in Phase 5b and fail loudly (CLAUDE.md §5):
    ///
    ///   - the Transaction V2 arm (Java 448-451), which registers the partition
    ///     directly because TV2 sends no `AddPartitionsToTxn`. Unreachable until
    ///     Phase 5b adds `maybeUpdateTransactionV2Enabled`, the only writer of
    ///     `is_transaction_v2_enabled`; kept in position so the chain's order is
    ///     the Java one.
    ///   - the registration arm (Java 456-458), which populates
    ///     `new_partitions_in_transaction` for `addPartitionsToTransactionHandler`
    ///     (Java 1313) to drain. Deferring it is what keeps that set empty, and so
    ///     keeps `nextRequest`'s first statement and
    ///     [`Self::is_send_to_partition_allowed`]'s set lookup consistent in 5a.
    ///
    /// The split is recorded in PLAN §10.7: the task's boundary places
    /// `maybe_add_partition`'s transactional arm in 5b, but the tests that pin its
    /// state guards are named as 5a's, and those guards depend on nothing 5b owns.
    pub(crate) fn maybe_add_partition(&mut self, topic_partition: &TopicPartition) -> Result<(), KafkaError> {
        self.maybe_fail_with_error()?;
        self.throw_if_pending_state("send")?;

        if self.is_transactional() {
            if !self.has_producer_id() {
                return Err(KafkaError::illegal_state(format!(
                    "Cannot add partition {topic_partition} to transaction before completing a call to \
                     initTransactions"
                )));
            } else if self.current_state != State::InTransaction {
                // Java's message has two spaces before the state; reproduced so
                // message assertions keep matching (Java 447).
                return Err(KafkaError::illegal_state(format!(
                    "Cannot add partition {topic_partition} to transaction while in state  {}",
                    self.current_state
                )));
            } else if self.is_transaction_v2_enabled {
                return Err(KafkaError::unsupported_version(format!(
                    "Adding partition {topic_partition} to a Transaction V2 transaction is not yet implemented in \
                     this client (Milestone 11, Phase 5b)."
                )));
            } else if self.transaction_contains_partition(topic_partition)
                || self.is_partition_pending_add(topic_partition)
            {
                return Ok(());
            } else {
                return Err(KafkaError::unsupported_version(format!(
                    "Adding partition {topic_partition} to a transaction is not yet implemented in this client \
                     (Milestone 11, Phase 5b)."
                )));
            }
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
    use std::time::Duration;

    use super::*;
    use crate::NodeApiVersions;
    use crate::api_versions_response_data::{ApiVersion, FinalizedFeatureKey, SupportedFeatureKey};
    use crate::common::compress::Compression;
    use crate::common::protocol::ApiKeys;
    use crate::common::record::TimestampType;
    use crate::common::record::memory_records::MemoryRecords;
    use crate::common::requests::InitProducerIdResponse;
    use crate::init_producer_id_response_data::InitProducerIdResponseData;
    use crate::producer::internals::sender::is_authorization_error_handled_by_sender;

    // Constants mirroring `TransactionManagerTest`'s fields (Java 125-155).
    const TRANSACTIONAL_ID: &str = "foobar";
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
    /// (Java 174-221).
    fn idempotent_manager(transaction_v2_enabled: bool) -> TransactionManager {
        manager_with_transactional_id(None, transaction_v2_enabled)
    }

    /// Builds a transactional manager, mirroring
    /// `initializeTransactionManager(Optional.of(transactionalId), transactionV2Enabled)`
    /// — which is what `setup()` (Java 163) calls.
    fn transactional_manager(transaction_v2_enabled: bool) -> TransactionManager {
        manager_with_transactional_id(Some(TRANSACTIONAL_ID.to_string()), transaction_v2_enabled)
    }

    /// Mirrors `initializeTransactionManager` (Java 174-221), including the
    /// `ApiVersions` contents, so the `transactionV2Enabled` parameterisation is
    /// reproduced faithfully.
    ///
    /// # `transaction_v2_enabled` is still not observable in Phase 5a
    ///
    /// Java threads the flag only into `apiVersions`, and the manager reads
    /// `apiVersions` from exactly two methods:
    /// [`TransactionManager::handle_coordinator_ready`] (Java 1104), which looks at
    /// the `INIT_PRODUCER_ID` version and not at features, and
    /// `maybeUpdateTransactionV2Enabled` (Java 493), which is Phase 5b and is the
    /// only writer of `is_transaction_v2_enabled`. So the flag stays `false` in
    /// both iterations and both parameterisations execute identical code. The
    /// loops are kept: they cost nothing and start discriminating in Phase 5b.
    fn manager_with_transactional_id(
        transactional_id: Option<String>,
        transaction_v2_enabled: bool,
    ) -> TransactionManager {
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
            transactional_id,
            TRANSACTION_TIMEOUT_MS,
            DEFAULT_RETRY_BACKOFF_MS,
            api_versions,
            false,
        )
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

    /// Feeds an `InitProducerId` response body into `manager`, playing the part
    /// the `synchronized` block of `TxnRequestHandler.onComplete`
    /// (Java 1421-1423) plays.
    ///
    /// Java's `onComplete` also compares the response's correlation id against
    /// `inFlightRequestCorrelationId` and handles disconnect / version-mismatch
    /// (Java 1407-1417). Since Phase 4 that half is
    /// `Sender::on_transactional_response` — the field it reads is Sender-confined
    /// (rules §2) — so those three branches are tested in `sender.rs` against a
    /// `MockClient` rather than here.
    fn complete_init_producer_id(
        manager: &mut TransactionManager,
        pending_requests: &mut PendingRequests,
        handler: TxnRequestHandler,
        error: Errors,
        producer_id: i64,
        epoch: i16,
    ) -> Result<(), KafkaError> {
        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(error.code())
            .set_producer_id(producer_id)
            .set_producer_epoch(epoch)
            .set_throttle_time_ms(0);
        let response = ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data));
        manager.handle_response(handler, &response, pending_requests)
    }

    /// Acquires a producer id for an idempotent producer.
    ///
    /// Mirrors `initializeIdempotentProducerId` (Java 4333). Java drives
    /// `Sender.runOnce` against a `MockClient`; these manager-level tests drive
    /// the same manager path directly: the pending `InitProducerId` is dequeued
    /// through [`TransactionManager::next_request`] exactly as `Sender.java:472`
    /// does, and the response body is fed back through
    /// [`TransactionManager::handle_response`] exactly as `NetworkClient.poll` →
    /// `onComplete`'s `synchronized` block does.
    fn initialize_idempotent_producer_id(
        manager: &mut TransactionManager,
        pending_requests: &mut PendingRequests,
        producer_id: i64,
        epoch: i16,
    ) {
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, pending_requests, Caller::Sender)
            .expect("enqueueing the initial InitProducerId succeeds");
        let mut handler = manager
            .next_request(pending_requests, false)
            .expect("an InitProducerId request must be pending");
        // Java's helper asserts the same thing on the outgoing request.
        assert!(
            handler.request_builder().data().transactional_id.is_none(),
            "an idempotent producer must not send a transactional id"
        );
        complete_init_producer_id(manager, pending_requests, handler, Errors::None, producer_id, epoch)
            .expect("a successful InitProducerId response is handled");
        assert!(manager.has_producer_id());
    }

    /// Drives a transactional producer from `UNINITIALIZED` to `READY`, leaving
    /// the `initTransactions` result **acknowledged** so subsequent operations are
    /// not rejected by `throwIfPendingState`.
    ///
    /// Mirrors `doInitTransactions(long, short)` (Java 4348). Java's helper also
    /// drives a `FindCoordinator` round trip, because it spins `Sender.runOnce`
    /// and the Sender has no coordinator yet. `initializeTransactions` itself
    /// enqueues **only** the `InitProducerId` (Java 323) — the `FindCoordinator`
    /// comes from `Sender.maybeFindCoordinatorAndRetry` (`Sender.java:522`) — so a
    /// manager-level drive legitimately has no such step. The coordinator round
    /// trip is covered where it belongs, in `sender.rs`.
    ///
    /// Java's `result.await()` blocks the test thread; here it is an `.await`,
    /// which is why every test using this helper is a `#[tokio::test]`. The result
    /// is already completed by `handle_response`, so the await returns without
    /// yielding — and it is the real awaiting method, the only thing allowed to set
    /// `is_acked` (rules §5).
    async fn do_init_transactions(
        manager: &mut TransactionManager,
        pending_requests: &mut PendingRequests,
        producer_id: i64,
        epoch: i16,
    ) -> Arc<TransactionalRequestResult> {
        let result = manager
            .initialize_transactions(false, pending_requests)
            .expect("initTransactions is valid from UNINITIALIZED");
        let mut handler = manager
            .next_request(pending_requests, false)
            .expect("an InitProducerId request must be pending");
        assert_eq!(
            handler.request_builder().data().transactional_id.as_deref(),
            Some(TRANSACTIONAL_ID),
            "a transactional producer must send its transactional id"
        );
        assert_eq!(handler.request_builder().data().transaction_timeout_ms, TRANSACTION_TIMEOUT_MS);
        complete_init_producer_id(manager, pending_requests, handler, Errors::None, producer_id, epoch)
            .expect("a successful InitProducerId response is handled");
        assert!(manager.has_producer_id());

        // Java: `result.await(); assertTrue(result.isSuccessful()); assertTrue(result.isAcked());`
        assert!(result.is_successful());
        result.await_result().await.expect("initTransactions succeeded");
        assert!(result.is_acked());
        result
    }

    /// Drives the manager entry points of `Sender.runOnce`'s
    /// `transactionManager != null` block (`Sender.java:311-335`) in `runOnce`'s
    /// order and behind `runOnce`'s guards, and reports which of the four exits
    /// `runOnce` would have taken.
    ///
    /// # This is no longer the reference implementation
    ///
    /// Phase 3 had no `Sender`, so this harness *was* the only model of the block.
    /// Since Phase 4 the production implementation is
    /// `Sender::run_transaction_phase`, tested against a `MockClient` in
    /// `sender.rs` (`test_run_once_returns_on_a_fatal_transaction_manager_error`,
    /// `test_run_once_recovers_an_idempotent_producer_from_an_authorization_error`,
    /// `test_run_once_returns_without_producing_while_init_producer_id_is_pending`,
    /// `test_run_once_produces_once_a_producer_id_is_held`) — that is what
    /// PLAN §Phase-4 means by "replaces the predicate with the real call".
    ///
    /// This harness survives because the tests below assert on *manager* state
    /// (sequence numbers, epochs, unresolved partitions) and need neither a
    /// network client nor an accumulator. Java's own tests reach the epoch bump
    /// through `runUntil(() -> transactionManager.producerIdAndEpoch().epoch == N)`,
    /// which spins `Sender.runOnce`; this is the manager-level equivalent.
    ///
    /// Its order and guards still matter: skipping the `:318` / `:325` guards and
    /// going straight from `:313` to `:331` produces exactly the wrong behaviour on
    /// the abortable-error path (an `ABORTABLE_ERROR → INITIALIZING` attempt Java
    /// never makes).
    ///
    /// # What is not executed here, and why
    ///
    /// Three of `runOnce`'s steps need collaborators this harness does not have:
    ///
    ///   - `maybeAbortBatches` (`:320`, `:355`) needs the accumulator;
    ///   - `client.poll` (`:322`, and inside
    ///     `maybeSendAndPollTransactionalRequest`) needs the network client;
    ///   - `maybeSendAndPollTransactionalRequest` (`:333-335`) needs both, so this
    ///     cannot *send*. It is nonetheless **modelled**, as an outcome, because
    ///     its return value decides whether `runOnce` reaches `sendProducerData` at
    ///     `:344` — and the tests supply the send by hand immediately afterwards
    ///     through [`TransactionManager::next_request`] and
    ///     `complete_init_producer_id`.
    ///
    /// # Why a predicate is a faithful model of `:333-335`
    ///
    /// `maybeSendAndPollTransactionalRequest` (`Sender.java:459-518`) has exactly
    /// **one** `return false` — `:474`, when `nextRequest` yields nothing. Its six
    /// other exits all return `true` (`:463` a request already in flight, `:487`
    /// node not ready, `:492` coordinator unknown, `:497` no node available,
    /// `:510` request sent, `:516` `IOException`). So `runOnce` returns at `:334`
    /// whenever a transactional request is in flight or `nextRequest` yields one,
    /// and falls through to the produce path only when it does not.
    ///
    /// `next_request` cannot be *called* here without consuming the handler the
    /// tests need, so the predicate is
    /// `has_in_flight_request() || (has_pending_requests() && !has_error())`,
    /// which reproduces `nextRequest`'s two Phase-3-reachable outcomes: it returns
    /// the head of a non-empty queue, unless `maybe_terminate_request_with_error`
    /// fails it first (`has_error()`). The remaining Java case — `nextRequest`
    /// returning `null` for an `EndTxn` with incomplete batches (`:903`) — cannot
    /// arise, because [`TxnRequestHandler::is_end_txn`] is `false` for every
    /// handler this phase can build.
    ///
    /// `pending_requests` and `in_flight_request_correlation_id` are passed in
    /// because they are Sender-confined (rules §2, see [`PendingRequests`]); the
    /// callers all pass [`NO_INFLIGHT_REQUEST_CORRELATION_ID`] because these tests
    /// complete each `InitProducerId` synchronously and so never leave one in
    /// flight. The term is kept rather than dropped so the predicate stays the one
    /// Java computes.
    fn run_manager_transaction_phase(
        manager: &mut TransactionManager,
        batches: &mut InFlightBatchPool<'_>,
        pending_requests: &mut PendingRequests,
        in_flight_request_correlation_id: i32,
    ) -> SenderPhaseOutcome {
        // Sender.java:313
        manager
            .maybe_resolve_sequences(Caller::Sender)
            .expect("resolving sequences succeeds");

        // Sender.java:315
        let last_error = manager.last_error().cloned();

        // Sender.java:318-323 — do not continue sending in a fatal state.
        if manager.has_fatal_error() {
            return SenderPhaseOutcome::ReturnedOnFatalError;
        }

        // Sender.java:325-327 → shouldHandleAuthorizationError, :351-360.
        let authorization_error =
            last_error.filter(|error| manager.has_abortable_error() && is_authorization_error_handled_by_sender(error));
        if let Some(error) = authorization_error {
            // Java wraps the cause in `new AuthenticationException(exception)`
            // (Sender.java:354). Java's `AuthenticationException` base class
            // carries no wire code — only its subclasses do — so it maps to
            // `Errors::UnknownServerError`, the convention `maybe_fail_with_error`
            // and `close` already use for a codeless Java exception. NOT
            // `SaslAuthenticationFailed`: the cause here is a cluster
            // authorization failure and nothing about it is SASL.
            manager
                .fail_pending_requests(
                    pending_requests,
                    &KafkaError::fatal(Errors::UnknownServerError, error.message()),
                    Caller::Sender,
                )
                .expect("failing pending requests succeeds");
            manager
                .transition_to_uninitialized(&error, Caller::Sender)
                .expect("ABORTABLE_ERROR -> UNINITIALIZED is a valid transition");
            return SenderPhaseOutcome::RecoveredFromAuthorizationError;
        }

        // Sender.java:331
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(batches, pending_requests, Caller::Sender)
            .expect("bumping the epoch succeeds");

        // Sender.java:333-335 — see the doc comment for why this is a predicate.
        if in_flight_request_correlation_id != NO_INFLIGHT_REQUEST_CORRELATION_ID
            || (!pending_requests.is_empty() && !manager.has_error())
        {
            return SenderPhaseOutcome::ReturnedOnTransactionalRequest;
        }
        SenderPhaseOutcome::Continued
    }

    /// Which exit `Sender.runOnce`'s transaction block took.
    ///
    /// Test-harness only; Java's `runOnce` returns `void` and communicates this
    /// through control flow.
    #[derive(Debug, PartialEq, Eq)]
    enum SenderPhaseOutcome {
        /// `Sender.java:322` — returned because the manager is in a fatal state.
        ReturnedOnFatalError,
        /// `Sender.java:326` — returned after recovering to `UNINITIALIZED`.
        RecoveredFromAuthorizationError,
        /// `Sender.java:334` — returned because
        /// `maybeSendAndPollTransactionalRequest` sent or awaited a transactional
        /// request. `sendProducerData` (`:344`) is **not** reached in this
        /// iteration.
        ReturnedOnTransactionalRequest,
        /// Fell through past `:335` to `sendProducerData` at `:344`.
        Continued,
    }

    // ---------------------------------------------------------------------
    // Rust-side unit tests for the pieces Java covers only indirectly.
    // ---------------------------------------------------------------------

    /// Phase 3's MILESTONE-11 GUARD is gone: a transactional manager is
    /// constructible, and the arms Phase 5b still owes fail loudly with
    /// [`Errors::UnsupportedVersion`] instead (CLAUDE.md §5).
    ///
    /// Pins the guard's *replacement*, so removing it cannot silently turn a
    /// deferred transactional path into a wrong-branch success. The two arms
    /// asserted here are the ones an application can reach first: registering a
    /// partition in a live transaction, and the `AddPartitionsToTxn` that would
    /// carry it.
    #[tokio::test]
    async fn test_transactional_manager_is_constructible_and_defers_phase_5b_arms() {
        let mut manager = transactional_manager(false);
        assert!(manager.is_transactional());
        assert_eq!(manager.transactional_id(), Some(TRANSACTIONAL_ID));
        assert_eq!(manager.current_state(), State::Uninitialized);

        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        let error = manager
            .maybe_add_partition(&tp0())
            .expect_err("registering a new partition needs AddPartitionsToTxn (Phase 5b)");
        assert_eq!(error.error(), Errors::UnsupportedVersion);
        assert!(
            error.message().contains("Milestone 11, Phase 5b"),
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
            let mut pending = PendingRequests::new();
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("the initial InitProducerId is enqueued");
            let handler = manager
                .next_request(&mut pending, false)
                .expect("an InitProducerId request is pending");
            let result = Arc::clone(handler.result());

            complete_init_producer_id(&mut manager, &mut pending, handler, error_code, -1, -1)
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
            let mut pending = PendingRequests::new();
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("the initial InitProducerId is enqueued");
            let handler = manager
                .next_request(&mut pending, false)
                .expect("an InitProducerId request is pending");
            complete_init_producer_id(&mut manager, &mut pending, handler, error_code, -1, -1)
                .expect("the error is handled");
            assert!(manager.has_abortable_error());

            // Sender.runOnce intercepts at :325 and recovers; it does NOT reach
            // the epoch bump at :331.
            assert_eq!(
                run_manager_transaction_phase(
                    &mut manager,
                    &mut pool,
                    &mut pending,
                    NO_INFLIGHT_REQUEST_CORRELATION_ID
                ),
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
                pending.is_empty(),
                "the failed handler was consumed by on_complete, so nothing is queued"
            );

            // Sends are accepted again — the state is escapable.
            manager
                .maybe_add_partition(&tp0())
                .expect("sends are allowed once the error is cleared");

            // The next iteration enqueues a fresh InitProducerId. Java's
            // `maybeSendAndPollTransactionalRequest` then returns `true` — it has a
            // handler to send — so `runOnce` returns at `:334` and does NOT reach
            // `sendProducerData` in the same iteration that acquires the producer
            // id. Asserting `Continued` here would document the opposite.
            assert_eq!(
                run_manager_transaction_phase(
                    &mut manager,
                    &mut pool,
                    &mut pending,
                    NO_INFLIGHT_REQUEST_CORRELATION_ID
                ),
                SenderPhaseOutcome::ReturnedOnTransactionalRequest
            );
            assert_eq!(manager.current_state(), State::Initializing);
            let handler = manager
                .next_request(&mut pending, false)
                .expect("a fresh InitProducerId is pending");
            complete_init_producer_id(&mut manager, &mut pending, handler, Errors::None, PRODUCER_ID, EPOCH)
                .expect("the retry succeeds");
            assert_eq!(manager.current_state(), State::Ready);
            assert_eq!(manager.producer_id_and_epoch(), ProducerIdAndEpoch::new(PRODUCER_ID, EPOCH));
        }
    }

    /// `failPendingRequests` (Java 944), `authenticationFailed` (Java 939) and
    /// `close` (Java 949) each fail every queued handler and transition.
    ///
    /// The recovery path above reaches `fail_pending_requests` with an empty
    /// queue, because `handle_response` consumed the only handler. These drive the
    /// non-empty case, which is where the per-handler loop is observable.
    #[test]
    fn test_pending_requests_are_failed_in_bulk() {
        // failPendingRequests → abortableError per handler.
        let mut manager = idempotent_manager(false);
        let mut pending = PendingRequests::new();
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        // Reach ABORTABLE_ERROR from INITIALIZING, the transition
        // `InitProducerIdHandler`'s authorization arm makes (Java 1528), while
        // leaving a handler queued for `fail_pending_requests` to fail.
        manager
            .transition_to_abortable_error(KafkaError::new(Errors::ClusterAuthorizationFailed), Caller::Sender)
            .expect("INITIALIZING -> ABORTABLE_ERROR is valid");
        let queued_result = manager.force_enqueue_init_producer_id_for_test(&mut pending);
        assert!(!pending.is_empty());
        // `Sender.java:354` passes `new AuthenticationException(exception)`. Java's
        // `AuthenticationException` base class has no wire code, which this crate
        // spells as `Errors::UnknownServerError` — the same convention
        // `maybe_fail_with_error` and `close` use.
        let authentication_error = KafkaError::fatal(Errors::UnknownServerError, "authentication failed");
        manager
            .fail_pending_requests(&mut pending, &authentication_error, Caller::Sender)
            .expect("ABORTABLE_ERROR self-loop is valid");
        assert!(queued_result.is_completed());
        assert_eq!(queued_result.error().expect("failed").message(), "authentication failed");
        assert!(manager.has_abortable_error(), "the state stays ABORTABLE_ERROR (self-loop)");
        assert_eq!(manager.last_error().expect("recorded").message(), "authentication failed");
        assert!(!pending.is_empty(), "Java does not clear the queue (Java 945-946)");

        // authenticationFailed → fatalError per handler. A fresh manager gets a
        // fresh queue, as Java's `initializeTransactionManager` does.
        let mut manager = idempotent_manager(false);
        let mut pending = PendingRequests::new();
        let queued_result = manager.force_enqueue_init_producer_id_for_test(&mut pending);
        manager
            .authentication_failed(&mut pending, &authentication_error, Caller::Sender)
            .expect("FATAL_ERROR is always a valid target");
        assert!(manager.has_fatal_error());
        assert_eq!(queued_result.error().expect("failed").message(), "authentication failed");
        assert_eq!(manager.last_error().expect("recorded").message(), "authentication failed");

        // close → fatalError with Java's message.
        let mut manager = idempotent_manager(false);
        let mut pending = PendingRequests::new();
        let queued_result = manager.force_enqueue_init_producer_id_for_test(&mut pending);
        manager
            .close(&mut pending, Caller::Sender)
            .expect("FATAL_ERROR is always a valid target");
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

    /// Builds an `InitProducerId` handler directly, bypassing the state guards on
    /// [`TransactionManager::bump_idempotent_epoch_and_reset_id_if_needed`], so the
    /// queue's ordering can be exercised with more than one pending request.
    ///
    /// `is_epoch_bump` is what makes `InitProducerIdHandler.priority()` (Java 1477)
    /// dynamic, and therefore the only priority difference the 5a handler set can
    /// express without a `FindCoordinator`.
    fn init_producer_id_handler(operation: &str, is_epoch_bump: bool) -> TxnRequestHandler {
        let mut request_data = InitProducerIdRequestData::new();
        request_data.set_transactional_id(None).set_transaction_timeout_ms(i32::MAX);
        TxnRequestHandler::new(
            operation,
            DEFAULT_RETRY_BACKOFF_MS,
            TxnRequestHandlerKind::InitProducerId {
                builder: InitProducerIdRequestBuilder::new(request_data),
                is_epoch_bump,
            },
        )
    }

    /// The queue is Java's min-heap on [`Priority`] (Java 224): a lower priority
    /// value is dequeued first, regardless of insertion order.
    ///
    /// An epoch bump sorts *after* everything else ([`Priority::EpochBump`] = 4),
    /// so enqueueing it first must not make it come out first.
    #[test]
    fn test_pending_requests_are_ordered_by_priority() {
        let mut pending = PendingRequests::new();
        pending.add(init_producer_id_handler("EpochBump", true));
        pending.add(init_producer_id_handler("InitProducerId", false));
        assert_eq!(pending.len(), 2);

        assert_eq!(
            pending.peek().expect("two are queued").priority(),
            Priority::InitProducerId,
            "peek must report the lowest priority, not the head of insertion order"
        );
        assert_eq!(pending.poll().expect("two are queued").operation(), "InitProducerId");
        assert_eq!(pending.poll().expect("one is queued").operation(), "EpochBump");
        assert!(pending.poll().is_none());
        assert!(pending.is_empty());
    }

    /// Equal priorities come out in insertion order.
    ///
    /// Java's `PriorityQueue` leaves this unspecified; the insertion-sequence
    /// tiebreaker makes it deterministic so tests can assert on it (PLAN
    /// §Phase-5). Three elements, because a two-element `BinaryHeap` would agree
    /// with FIFO by accident.
    #[test]
    fn test_pending_requests_break_priority_ties_by_insertion_order() {
        let mut pending = PendingRequests::new();
        for operation in ["first", "second", "third"] {
            pending.add(init_producer_id_handler(operation, false));
        }
        let dequeued: Vec<String> = std::iter::from_fn(|| pending.poll())
            .map(|handler| handler.operation().to_string())
            .collect();
        assert_eq!(dequeued, vec!["first", "second", "third"]);
    }

    /// The sort key is snapshotted at insertion, so a handler re-enqueued by
    /// [`TransactionManager::retry`] is re-keyed rather than keeping a stale slot.
    #[test]
    fn test_reenqueued_request_is_rekeyed_at_its_current_priority() {
        let manager = idempotent_manager(false);
        let mut pending = PendingRequests::new();
        pending.add(init_producer_id_handler("InitProducerId", false));
        pending.add(init_producer_id_handler("EpochBump", true));

        // Dequeue the InitProducerId and put it back: it must still overtake the
        // epoch bump, i.e. the sequence tiebreaker must not have promoted the bump.
        let handler = pending.poll().expect("two are queued");
        assert_eq!(handler.operation(), "InitProducerId");
        manager.retry(&mut pending, handler);

        let handler = pending.poll().expect("two are queued");
        assert_eq!(handler.operation(), "InitProducerId");
        assert!(
            handler.is_retry(),
            "retry() marks the handler before re-enqueueing (Java 934-937)"
        );
        assert_eq!(pending.poll().expect("one is queued").operation(), "EpochBump");
    }

    /// A `PRODUCER_FENCED` / `INVALID_PRODUCER_EPOCH` `InitProducerId` response
    /// is fatal, and both report `PRODUCER_FENCED` (Java 1529-1532).
    #[test]
    fn test_producer_fenced_init_producer_id_response_is_fatal() {
        for error_code in [Errors::InvalidProducerEpoch, Errors::ProducerFenced] {
            let mut manager = idempotent_manager(false);
            let mut pending = PendingRequests::new();
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("the initial InitProducerId is enqueued");
            let handler = manager
                .next_request(&mut pending, false)
                .expect("an InitProducerId request is pending");
            let result = Arc::clone(handler.result());

            complete_init_producer_id(&mut manager, &mut pending, handler, error_code, -1, -1)
                .expect("the error is handled");

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
        let mut pending = PendingRequests::new();
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        let handler = manager
            .next_request(&mut pending, false)
            .expect("an InitProducerId request is pending");
        let result = Arc::clone(handler.result());

        complete_init_producer_id(&mut manager, &mut pending, handler, Errors::CoordinatorLoadInProgress, -1, -1)
            .expect("a retriable error is handled");

        assert!(!result.is_completed(), "a retried request must not complete");
        assert!(!manager.has_error());
        assert!(!pending.is_empty());
        let handler = manager.next_request(&mut pending, false).expect("the request was re-enqueued");
        assert!(handler.is_retry());
        // Java also clears the in-flight correlation id here (Java 1410), but that
        // now happens in `Sender::on_transactional_response` before the response
        // body reaches `handle_response` (rules §2). `sender.rs`'s
        // `test_transactional_response_clears_the_in_flight_correlation_id` covers
        // it.
    }

    /// An unexpected `InitProducerId` error is fatal, with Java's message
    /// (Java 1536).
    #[test]
    fn test_unexpected_init_producer_id_response_is_fatal() {
        let mut manager = idempotent_manager(false);
        let mut pending = PendingRequests::new();
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        let handler = manager
            .next_request(&mut pending, false)
            .expect("an InitProducerId request is pending");

        complete_init_producer_id(&mut manager, &mut pending, handler, Errors::InvalidRequest, -1, -1)
            .expect("the error is handled");

        assert!(manager.has_fatal_error());
        assert_eq!(
            manager.last_error().expect("recorded").message(),
            format!(
                "Unexpected error in InitProducerIdResponse; {}",
                Errors::InvalidRequest.message()
            )
        );
    }

    // `test_mismatched_correlation_id_is_fatal` (Java 1407-1408) and
    // `test_disconnect_reenqueues_without_a_coordinator_lookup`
    // (Java 1411-1415, 1482) moved to `sender.rs` in Phase 4, along with the
    // unsynchronized half of `onComplete` that they exercise
    // (`Sender::on_transactional_response`, rules §2). Their coordinator-typing
    // assertions live on in `test_idempotent_init_producer_id_needs_no_coordinator`
    // below, which needs no `ClientResponse`.

    /// An idempotent `InitProducerId` is routed to no coordinator, which is why
    /// the whole `FindCoordinator` subsystem is out of scope for the idempotence
    /// slice (Java 1482-1488).
    #[test]
    fn test_idempotent_init_producer_id_needs_no_coordinator() {
        let mut manager = idempotent_manager(false);
        let mut pending = PendingRequests::new();
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        let handler = manager
            .next_request(&mut pending, false)
            .expect("an InitProducerId request is pending");
        assert_eq!(
            manager.coordinator_type(&handler),
            None,
            "InitProducerIdHandler.coordinatorType() is null when non-transactional (Java 1482)"
        );
        assert_eq!(manager.coordinator_key(&handler), None);
        assert!(!manager.needs_coordinator(&handler));
    }

    /// A pending request is failed rather than sent while the manager is in an
    /// error state (Java 1174-1184).
    #[test]
    fn test_next_request_terminates_pending_requests_in_an_error_state() {
        let mut manager = idempotent_manager(false);
        let mut pending = PendingRequests::new();
        let mut pool = InFlightBatchPool::new();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        manager
            .transition_to_fatal_error(kafka_exception(), Caller::Sender)
            .expect("FATAL_ERROR is always a valid target");

        assert!(!pending.is_empty());
        assert!(
            manager.next_request(&mut pending, false).is_none(),
            "the request is terminated, not returned"
        );
        assert!(pending.is_empty());
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
        let mut pending = PendingRequests::new();
        initialize_idempotent_producer_id(&mut manager, &mut pending, PRODUCER_ID, EPOCH);
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
        let mut pending = PendingRequests::new();
        initialize_idempotent_producer_id(&mut manager, &mut pending, PRODUCER_ID, i16::MAX);
        manager.request_idempotent_epoch_bump_for_partition(&tp0());
        let mut pool = InFlightBatchPool::new();
        let error = manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
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
        initialize_idempotent_producer_id(&mut manager, &mut pending, PRODUCER_ID, i16::MAX);
        manager.increment_sequence_number(&tp0(), 4).expect_err("no entry yet");
        assert_eq!(manager.sequence_number(&tp0()), 0, "the accessor creates the entry");
        manager.increment_sequence_number(&tp0(), 4).expect("the entry now exists");
        manager.request_idempotent_epoch_bump_for_partition(&tp0());
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
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

    /// Translated from `testFailIfNotReadyForSendNoProducerId` (Java 262-265).
    ///
    /// Also covers `testNotReadyForSendBeforeInitTransactions` (Java 505-508),
    /// whose body is character-identical. Java carries both; one Rust test covers
    /// the pair rather than duplicating it (`definition-of-done.md` §6), and the
    /// accounting records the pairing.
    #[test]
    fn test_fail_if_not_ready_for_send_no_producer_id() {
        let mut manager = transactional_manager(false);
        let error = manager
            .maybe_add_partition(&tp0())
            .expect_err("a transactional producer must call initTransactions first");
        assert_eq!(
            error.message(),
            format!(
                "Cannot add partition {} to transaction before completing a call to initTransactions",
                tp0()
            )
        );
        assert!(matches!(error, KafkaError::IllegalState(_)));
    }

    /// Translated from `testFailIfNotReadyForSendNoOngoingTransaction`
    /// (Java 282-286), which is character-identical to
    /// `testNotReadyForSendBeforeBeginTransaction` (Java 510-514).
    #[tokio::test]
    async fn test_fail_if_not_ready_for_send_no_ongoing_transaction() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;

        let error = manager.maybe_add_partition(&tp0()).expect_err("no transaction is in progress");
        // Java's message has two spaces before the state.
        assert_eq!(
            error.message(),
            format!("Cannot add partition {} to transaction while in state  READY", tp0())
        );
        assert!(matches!(error, KafkaError::IllegalState(_)));
    }

    /// Translated from `testFailIfNotReadyForSendAfterAbortableError`
    /// (Java 288-295), which is character-identical to
    /// `testNotReadyForSendAfterAbortableError` (Java 516-523).
    #[tokio::test]
    async fn test_fail_if_not_ready_for_send_after_abortable_error() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        manager
            .transition_to_abortable_error(kafka_exception(), Caller::App)
            .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");

        let error = manager
            .maybe_add_partition(&tp0())
            .expect_err("an abortable error fails the send");
        assert_eq!(
            error.message(),
            "Cannot execute transactional method because we are in an error state"
        );
    }

    /// Translated from `testFailIfNotReadyForSendAfterFatalError`
    /// (Java 296-302), which is character-identical to
    /// `testNotReadyForSendAfterFatalError` (Java 524-530).
    #[tokio::test]
    async fn test_fail_if_not_ready_for_send_after_fatal_error() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager
            .transition_to_fatal_error(kafka_exception(), Caller::App)
            .expect("FATAL_ERROR is always a valid target");

        let error = manager.maybe_add_partition(&tp0()).expect_err("a fatal error fails the send");
        assert_eq!(
            error.message(),
            "Cannot execute transactional method because we are in an error state"
        );
    }

    /// Translated from `testIsSendToPartitionAllowedWithPartitionNotAdded`
    /// (Java 616-621).
    #[tokio::test]
    async fn test_is_send_to_partition_allowed_with_partition_not_added() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(!manager.is_send_to_partition_allowed(&tp0()));
    }

    /// Translated from `testInitializeTransactionsTwiceRaisesError`
    /// (Java 1093-1098).
    ///
    /// The second call is rejected by the transition table, not by the
    /// pending-transition slot: `do_init_transactions` acknowledges the first
    /// result, so `handleCachedTransactionRequestResult` clears the slot and runs
    /// the supplier, whose `READY → INITIALIZING` is not a valid arm (Java 168).
    #[tokio::test]
    async fn test_initialize_transactions_twice_raises_error() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(manager.has_producer_id());

        let error = manager
            .initialize_transactions(false, &mut pending)
            .expect_err("initTransactions may not run twice");
        assert!(matches!(error, KafkaError::IllegalState(_)));
        assert_eq!(
            error.message(),
            format!(
                "TransactionalId {TRANSACTIONAL_ID}: Invalid transition attempted from state READY to state INITIALIZING"
            )
        );
        // The failed supplier must not have installed a pending transition, and
        // the application-side transition must not have poisoned the state.
        assert_eq!(manager.current_state(), State::Ready);
        assert!(!manager.has_error());
    }

    /// Translated from `testRetryInitTransactionsAfterTimeout` (Java 1713-1744),
    /// minus the three assertions that need Phase 5b's `beginAbort` / `beginCommit`
    /// — recorded in the accounting block as a named 5b tail.
    ///
    /// This is the core of `.claude/rules/producer-transactions.md` §5: an
    /// `initTransactions` whose caller timed out before acknowledging the result
    /// must (a) block every *other* operation, (b) hand back the **same** result
    /// object when retried, and (c) release the slot once acknowledged.
    #[tokio::test]
    async fn test_retry_init_transactions_after_timeout() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();

        let result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");

        // Java: `assertThrows(TimeoutException.class, () -> result.await(0, MILLISECONDS))`.
        let timeout = result
            .await_result_timeout(Duration::from_millis(0))
            .await
            .expect_err("nothing has answered the InitProducerId yet");
        assert!(
            matches!(timeout, KafkaError::Timeout(_)),
            "Java raises TimeoutException: {timeout:?}"
        );
        assert!(!result.is_acked());

        let handler = manager
            .next_request(&mut pending, false)
            .expect("an InitProducerId request must be pending");
        complete_init_producer_id(&mut manager, &mut pending, handler, Errors::None, PRODUCER_ID, EPOCH)
            .expect("a successful InitProducerId response is handled");
        assert!(manager.has_producer_id());
        assert!(result.is_successful());
        assert!(!result.is_acked(), "completing does not acknowledge — only awaiting does");

        // At this point, the InitProducerId call has returned, but the user has yet
        // to complete the call to `initTransactions`. Other transitions should be
        // rejected until they do.
        let expected = format!(
            "Cannot attempt operation `{}` because the previous call to `initTransactions` timed out and must be \
             retried",
            "beginTransaction"
        );
        assert_eq!(
            manager
                .begin_transaction()
                .expect_err("beginTransaction is blocked by the unacknowledged result")
                .message(),
            expected
        );
        assert_eq!(
            manager
                .maybe_add_partition(&tp0())
                .expect_err("send is blocked by the unacknowledged result")
                .message(),
            expected.replace("`beginTransaction`", "`send`")
        );

        // Java: `assertSame(result, transactionManager.initializeTransactions(false))`.
        let retried = manager
            .initialize_transactions(false, &mut pending)
            .expect("retrying the same operation is allowed");
        assert!(
            Arc::ptr_eq(&result, &retried),
            "the same result object must come back, or the InitProducerId would be sent twice"
        );
        assert!(pending.is_empty(), "the retry must not enqueue a second InitProducerId");

        result.await_result().await.expect("initTransactions succeeded");
        assert!(result.is_acked());

        // Once acknowledged the slot is released, so a *new* initTransactions is
        // rejected by the transition table instead.
        let error = manager
            .initialize_transactions(false, &mut pending)
            .expect_err("initTransactions may not run twice");
        assert_eq!(
            error.message(),
            format!(
                "TransactionalId {TRANSACTIONAL_ID}: Invalid transition attempted from state READY to state INITIALIZING"
            )
        );

        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(manager.has_ongoing_transaction());
    }

    /// A *different* operation attempted while a result is unacknowledged is
    /// rejected with Java's message (Java 1272-1274), and the pending operation
    /// stays retryable.
    ///
    /// `initTransactions` is the only Phase-5a operation that installs a slot, so
    /// the "different operation" is driven through
    /// [`TransactionManager::throw_if_pending_state`] — the same rejection Java
    /// produces from `beginTransaction` (Java 332) and `send` (Java 439). The
    /// `nextState != pendingTransition.state` arm of
    /// `handleCachedTransactionRequestResult` itself needs a second
    /// result-returning entry point (`beginCommit` / `beginAbort` /
    /// `sendOffsetsToTransaction`), all Phase 5b.
    #[tokio::test]
    async fn test_pending_transition_blocks_other_operations_and_stays_retryable() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");

        for _ in 0..2 {
            let error = manager
                .begin_transaction()
                .expect_err("an unacknowledged result blocks other operations");
            assert_eq!(
                error.message(),
                "Cannot attempt operation `beginTransaction` because the previous call to `initTransactions` timed \
                 out and must be retried"
            );
            assert!(matches!(error, KafkaError::IllegalState(_)));
        }
        // Rejecting must not disturb the state machine or the pending result.
        assert_eq!(manager.current_state(), State::Initializing);
        assert!(!result.is_completed());
        assert!(Arc::ptr_eq(
            &result,
            &manager
                .initialize_transactions(false, &mut pending)
                .expect("the same operation is still retryable")
        ));
    }

    /// [`TransactionManager::transition_to_fatal_error`] fails the pending
    /// transition's result (Java 545-547), so a caller blocked in
    /// `initTransactions` is woken with the error rather than hanging.
    #[tokio::test]
    async fn test_fatal_error_fails_the_pending_transition() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");
        assert!(!result.is_completed());

        manager
            .transition_to_fatal_error(
                KafkaError::with_message(Errors::InvalidProducerIdMapping, "pid mapping is gone"),
                Caller::Sender,
            )
            .expect("FATAL_ERROR is always a valid target");

        assert!(result.is_completed());
        let error = result.await_result().await.expect_err("the pending operation failed");
        assert_eq!(error.error(), Errors::InvalidProducerIdMapping);
        assert_eq!(error.message(), "pid mapping is gone");
    }

    /// [`TransactionManager::close`] fails the pending transition even when the
    /// request queue is empty (Java 953-955).
    ///
    /// This is the behaviour PLAN §10.5's `close` note said was Phase-6
    /// transactional payoff: `Sender.java:288-289`'s "wake up the threads waiting
    /// on the futures". With an empty queue the loop body never runs, so only this
    /// branch can complete the result.
    #[tokio::test]
    async fn test_close_fails_the_pending_transition_with_an_empty_queue() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");
        // Consume the queued request, as the Sender does before sending it.
        manager
            .next_request(&mut pending, false)
            .expect("an InitProducerId request must be pending");
        assert!(pending.is_empty());

        manager
            .close(&mut pending, Caller::Sender)
            .expect("FATAL_ERROR is always a valid target");

        assert!(result.is_completed(), "an empty queue must not leave the caller hanging");
        let error = result.await_result().await.expect_err("the pending operation failed");
        assert_eq!(error.message(), "The producer closed forcefully");
        assert!(
            !manager.has_fatal_error(),
            "with an empty queue Java performs no transition — only the pending result is failed"
        );
    }

    /// [`TransactionManager::transition_to_uninitialized`] fails the pending
    /// transition with the raw error (Java 758-760), which is the argument PLAN
    /// §10.5 deviation 8 said Phase 5 would add.
    #[tokio::test]
    async fn test_transition_to_uninitialized_fails_the_pending_transition() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");
        manager
            .transition_to_abortable_error(KafkaError::new(Errors::ClusterAuthorizationFailed), Caller::Sender)
            .expect("INITIALIZING -> ABORTABLE_ERROR is valid");

        let authorization_error = KafkaError::new(Errors::ClusterAuthorizationFailed);
        manager
            .transition_to_uninitialized(&authorization_error, Caller::Sender)
            .expect("ABORTABLE_ERROR -> UNINITIALIZED is valid");

        assert_eq!(manager.current_state(), State::Uninitialized);
        assert!(manager.last_error().is_none(), "Java clears lastError at :761");
        let error = result.await_result().await.expect_err("the pending operation failed");
        assert_eq!(error.error(), Errors::ClusterAuthorizationFailed);
    }

    /// Translated from `testBackgroundInvalidStateTransitionIsFatal`
    /// (Java 3818-3838), minus the three follow-up calls that need Phase 5b
    /// (`beginAbort`, `beginCommit`, `sendOffsetsToTransaction`).
    ///
    /// Java forces the poison with
    /// `setShouldPoisonStateOnInvalidTransitionOverride(true)` on a test subclass;
    /// rules §1 replaces the whole mechanism with an explicit [`Caller`], so
    /// passing [`Caller::Sender`] *is* the override.
    #[tokio::test]
    async fn test_background_invalid_state_transition_is_fatal() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(manager.is_transactional());

        // Intentionally perform an operation that will cause an invalid state transition. The detection of this
        // will result in a poisoning of the transaction manager for all subsequent transactional operations since
        // it was performed in the background.
        //
        // `handleFailedBatch` → `maybeTransitionToErrorState` converts the bare
        // `KafkaException` to `TRANSACTION_ABORTABLE` and attempts
        // `READY → ABORTABLE_ERROR`, which the table forbids (Java 180).
        let batch = batch_with_value(&tp0(), "test");
        let error = manager
            .handle_failed_batch(&batch, &kafka_exception(), false, &mut [], Caller::Sender)
            .expect_err("READY -> ABORTABLE_ERROR is not a valid transition");
        assert!(matches!(error, KafkaError::IllegalState(_)));
        assert!(manager.has_fatal_error());

        // Validate that these operations fail after the invalid state transition attempt above.
        for message in [
            manager.begin_transaction().expect_err("poisoned").message(),
            manager.maybe_add_partition(&tp0()).expect_err("poisoned").message(),
            manager
                .initialize_transactions(false, &mut pending)
                .expect_err("poisoned")
                .message(),
        ] {
            assert_eq!(
                message,
                format!(
                    "Producer with transactionalId '{TRANSACTIONAL_ID}' and (producerId={PRODUCER_ID}, epoch={EPOCH}) cannot execute transactional method because of previous invalid state transition attempt"
                )
            );
        }
    }

    /// [`TransactionManager::maybe_transition_to_error_state`]'s transactional arm
    /// (Java 770-785): a retriable error and an `INVALID_TXN_STATE` both become
    /// `TRANSACTION_ABORTABLE`, anything else is passed through unchanged, and the
    /// five fatal classes bypass the arm entirely.
    #[tokio::test]
    async fn test_maybe_transition_to_error_state_transactional_arm() {
        // Retriable and InvalidTxnState are rewritten (Java 774-777).
        for original in [
            KafkaError::new(Errors::NotLeaderOrFollower),
            KafkaError::new(Errors::InvalidTxnState),
            timeout_exception(),
        ] {
            assert!(
                original.is_retriable() || original.error() == Errors::InvalidTxnState,
                "the fixture must exercise the rewrite arm"
            );
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
            manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

            manager
                .maybe_transition_to_error_state(&original, Caller::App)
                .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");
            assert!(manager.has_abortable_error());
            let last_error = manager.last_error().expect("recorded");
            assert_eq!(last_error.error(), Errors::TransactionAbortable);
            assert_eq!(
                last_error.message(),
                "Transaction Request was aborted after exhausting retries."
            );
        }

        // A non-retriable, non-InvalidTxnState error is carried through as-is
        // (Java 780-784 with the `if` at 774 not taken).
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        let original = KafkaError::with_message(Errors::RecordListTooLarge, "too big");
        assert!(!original.is_retriable());
        manager
            .maybe_transition_to_error_state(&original, Caller::App)
            .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");
        assert_eq!(manager.last_error().expect("recorded").error(), Errors::RecordListTooLarge);
        assert_eq!(manager.last_error().expect("recorded").message(), "too big");

        // The five classes tested before `isTransactional()` are fatal for a
        // transactional producer too (Java 765-770).
        for code in [
            Errors::ClusterAuthorizationFailed,
            Errors::TransactionalIdAuthorizationFailed,
            Errors::ProducerFenced,
            Errors::UnsupportedVersion,
            Errors::InvalidProducerIdMapping,
        ] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
            manager
                .maybe_transition_to_error_state(&KafkaError::new(code), Caller::App)
                .expect("FATAL_ERROR is always a valid target");
            assert!(manager.has_fatal_error(), "{code} must be fatal");
        }
    }

    /// [`TransactionManager::reset_transaction_state`] (Java 1330) clears the
    /// per-transaction state, and picks `INITIALIZING` over `READY` exactly when a
    /// client-side epoch bump is pending.
    ///
    /// Both Java call sites are Phase 5b, so this drives the method directly.
    #[tokio::test]
    async fn test_reset_transaction_state() {
        for epoch_bump_required in [false, true] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
            manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
            if epoch_bump_required {
                manager.request_idempotent_epoch_bump_for_partition(&tp0());
                assert!(manager.client_side_epoch_bump_required());
            }
            // Reach a state `resetTransactionState`'s targets are valid from:
            // Java always calls it while completing an EndTxn.
            manager
                .transition_to(State::CommittingTransaction, None, Caller::Sender)
                .expect("IN_TRANSACTION -> COMMITTING_TRANSACTION is valid");

            manager.reset_transaction_state().expect("both targets are valid");

            assert_eq!(
                manager.current_state(),
                if epoch_bump_required {
                    State::Initializing
                } else {
                    State::Ready
                }
            );
            assert!(manager.last_error().is_none());
            assert!(!manager.client_side_epoch_bump_required());
            assert!(!manager.has_partitions_to_add());
            assert!(!manager.transaction_contains_partition(&tp0()));
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
            let mut pending = PendingRequests::new();
            initialize_idempotent_producer_id(&mut manager, &mut pending, PRODUCER_ID, EPOCH);

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
                // The producer id is still valid, so the bump enqueues nothing and
                // `runOnce` falls through to the produce path — the other side of
                // the `:333-335` guard from the recovery test.
                assert_eq!(
                    run_manager_transaction_phase(
                        &mut manager,
                        &mut pool,
                        &mut pending,
                        NO_INFLIGHT_REQUEST_CORRELATION_ID
                    ),
                    SenderPhaseOutcome::Continued
                );
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
            let mut pending = PendingRequests::new();
            initialize_idempotent_producer_id(&mut manager, &mut pending, PRODUCER_ID, epoch);

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
                    .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
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
            let mut pending = PendingRequests::new();
            initialize_idempotent_producer_id(&mut manager, &mut pending, PRODUCER_ID, epoch);

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
                    .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                    .expect("an exhausted epoch resets the producer id");
            }
            initialize_idempotent_producer_id(&mut manager, &mut pending, PRODUCER_ID + 1, 0);

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
            let mut pending = PendingRequests::new();
            initialize_idempotent_producer_id(&mut manager, &mut pending, 15, i16::MAX);
            assert_eq!(manager.sequence_number(&tp0()), 0);
            assert_eq!(manager.sequence_number(&tp1()), 0);
            manager.increment_sequence_number(&tp0(), 3).expect("the entry exists");
            assert_eq!(manager.sequence_number(&tp0()), 3);
            manager.increment_sequence_number(&tp1(), 3).expect("the entry exists");
            assert_eq!(manager.sequence_number(&tp1()), 3);

            manager.request_idempotent_epoch_bump_for_partition(&tp0());
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
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
            let mut pending = PendingRequests::new();
            let producer_id = 15;
            let epoch = 5;
            let producer_id_and_epoch = ProducerIdAndEpoch::new(producer_id, epoch);
            initialize_idempotent_producer_id(&mut manager, &mut pending, producer_id, epoch);

            // Nothing to resolve, so no reset is needed
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
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
            manager.maybe_resolve_sequences(Caller::Sender).expect("resolving succeeds");
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
            manager.maybe_resolve_sequences(Caller::Sender).expect("resolving succeeds");
            assert!(!manager.has_unresolved_sequences());

            // Java reaches the bump through `runUntil(.. epoch == 6)`.
            let mut pool = InFlightBatchPool::new();
            run_manager_transaction_phase(&mut manager, &mut pool, &mut pending, NO_INFLIGHT_REQUEST_CORRELATION_ID);
            assert_eq!(manager.producer_id_and_epoch().epoch, 6);
        }
    }

    /// Translated from `testNoProducerIdResetAfterLastInFlightBatchSucceeds`
    /// (Java 3083-3121).
    #[test]
    fn test_no_producer_id_reset_after_last_in_flight_batch_succeeds() {
        for transaction_v2_enabled in [true, false] {
            let mut manager = idempotent_manager(transaction_v2_enabled);
            let mut pending = PendingRequests::new();
            let producer_id = 15;
            let epoch = 5;
            let producer_id_and_epoch = ProducerIdAndEpoch::new(producer_id, epoch);
            initialize_idempotent_producer_id(&mut manager, &mut pending, producer_id, epoch);

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
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(manager.has_unresolved_sequences());

            // The second batch fails as well with a timeout
            manager
                .handle_failed_batch(&b2, &timeout_exception(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(manager.has_unresolved_sequences());

            // The third batch succeeds, which should resolve the sequence number without
            // requiring a producerId reset.
            manager
                .handle_completed_batch(&b3, &PartitionResponse::new(Errors::None, 500, 0, 0, Vec::new(), None))
                .expect("the completion is recorded");
            manager.maybe_resolve_sequences(Caller::Sender).expect("resolving succeeds");
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
            let mut pending = PendingRequests::new();
            let producer_id_and_epoch = ProducerIdAndEpoch::new(PRODUCER_ID, EPOCH);
            initialize_idempotent_producer_id(&mut manager, &mut pending, PRODUCER_ID, EPOCH);

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
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(manager.has_unresolved_sequences());

            // When the last inflight batch fails, we have to bump the epoch
            manager
                .handle_failed_batch(&b3, &timeout_exception(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");

            // Java reaches the bump through `runUntil(.. epoch == 2)`.
            run_manager_transaction_phase(&mut manager, &mut pool, &mut pending, NO_INFLIGHT_REQUEST_CORRELATION_ID);
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
            let mut pending = PendingRequests::new();
            let producer_id = 15;
            let epoch = 5;
            initialize_idempotent_producer_id(&mut manager, &mut pending, producer_id, epoch);

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
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
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
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
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
