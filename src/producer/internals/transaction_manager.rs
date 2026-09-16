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

use crate::common::requests::ProduceResponse;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::AddOffsetsToTxnRequestData;
use crate::AddPartitionsToTxnRequestData;
use crate::ApiVersions;
use crate::EndTxnRequestData;
use crate::FindCoordinatorRequestData;
use crate::InitProducerIdRequestData;
use crate::TxnOffsetCommitRequestData;
use crate::common::errors::TransactionAbortableError;
use crate::common::protocol::{ApiKeys, Errors};
use crate::common::record::internal::RecordBatch;
use crate::common::requests::CoordinatorType;
use crate::common::requests::{
    AddOffsetsToTxnRequestBuilder, AddPartitionsToTxnRequestBuilder, AddPartitionsToTxnResponse, CommittedOffset,
    ConcreteResponse, EndTxnRequestBuilder, FindCoordinatorRequestBuilder, InitProducerIdRequestBuilder,
    PartitionResponse, RequestBuilder, TransactionResult, TxnOffsetCommitRequestBuilder,
    TxnOffsetCommitRequestBuilderOptionsBuilder,
};
use crate::common::utils::{LogContext, ProducerIdAndEpoch};
use crate::common::{Error, KafkaError, LocalIllegalStateError, Node, TopicPartition};
use crate::consumer::{ConsumerCommitFailedError, ConsumerGroupMetadata, OffsetAndMetadata};
use crate::producer::internals::{
    InFlightBatchKey, ProducerBatch, TransactionalRequestResult, TxnPartitionEntry, TxnPartitionMap,
};
use crate::{kafka_debug, kafka_error, kafka_info, kafka_trace};

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

/// The Sender task's record of the coordinators it has discovered.
///
/// # Why this is not on [`TransactionManager`]
///
/// `.claude/rules/producer-transactions.md` §2 and PLAN §6.5 require Java's
/// `transactionCoordinator` (`TransactionManager.java:137`) and
/// `consumerGroupCoordinator` (`:138`) to live on the Sender task's own unshared
/// state: they are non-volatile, and every touch is on the Sender thread. In full —
///
///   - **Writers**: `lookupCoordinator` (`:1194`, `:1197`, itself unsynchronized)
///     and `FindCoordinatorHandler.handleResponse` (`:1696`, `:1699`).
///   - **Readers**: `coordinator(CoordinatorType)` (`:961`, `:963`), whose only
///     production caller is `Sender.java:481`; **and** `handleCoordinatorReady`
///     (`:1104-1105`), which reads `transactionCoordinator` as a field rather than
///     through the accessor.
///
/// Single-thread confinement is what makes that safe there, and a plain field on
/// `Sender` is what makes it safe here.
///
/// That second reader is why [`TransactionManager::handle_coordinator_ready`] takes
/// `&CoordinatorNodes` — it needs the node, and the node is not here. Do not
/// "simplify" that parameter away.
///
/// So, as with [`PendingRequests`] and [`InFlightBatchPool`], the manager methods
/// Java implements by touching this state take it as a parameter:
/// [`TransactionManager::lookup_coordinator`],
/// [`TransactionManager::handle_coordinator_ready`] and
/// [`TransactionManager::handle_response`]. The property a reviewer can check
/// mechanically is that neither node appears as a **field** in this file.
///
/// # Why a struct and not two `Option<Node>` parameters
///
/// Java's `lookupCoordinator` and `FindCoordinatorHandler.handleResponse` both
/// `switch` on a [`CoordinatorType`] to pick *which* of the two slots to write,
/// so both arrive together or the switch cannot be expressed. Grouping them adds
/// no concept Java lacks (`definition-of-done.md` §7) — it is the pair of fields,
/// with Java's own `coordinator(CoordinatorType)` accessor (`:958`) attached.
///
/// `coordinatorSupportsBumpingEpoch` (`:139`) is deliberately **not** here; see
/// the field of that name on [`TransactionManager`] for why.
#[derive(Debug, Default)]
pub(crate) struct CoordinatorNodes {
    transaction: Option<Node>,
    consumer_group: Option<Node>,
}

impl CoordinatorNodes {
    /// Creates an empty record, mirroring the constructor's
    /// `this.transactionCoordinator = null; this.consumerGroupCoordinator = null;`
    /// (Java 215-216).
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The coordinator of the given type, or `None` if it has not been
    /// discovered yet.
    ///
    /// Translated from `coordinator(FindCoordinatorRequest.CoordinatorType)`
    /// (Java 958).
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] for [`CoordinatorType::Share`], mirroring
    /// Java's `default:` arm and its message. Java's enum has the same variant, so
    /// this is a translated branch rather than a Rust artefact.
    pub(crate) fn coordinator(&self, coordinator_type: CoordinatorType) -> Result<Option<&Node>, Error> {
        match coordinator_type {
            CoordinatorType::Group => Ok(self.consumer_group.as_ref()),
            CoordinatorType::Transaction => Ok(self.transaction.as_ref()),
            CoordinatorType::Share => Err(Error::local_illegal_state(format!(
                "Received an invalid coordinator type: {}",
                coordinator_type.name()
            ))),
        }
    }

    /// Forgets the coordinator of the given type.
    ///
    /// Translated from `lookupCoordinator`'s `switch` (Java 1192-1201), whose
    /// `default:` arm carries a **different** message from
    /// [`Self::coordinator`]'s.
    fn clear(&mut self, coordinator_type: CoordinatorType) -> Result<(), Error> {
        match coordinator_type {
            CoordinatorType::Group => self.consumer_group = None,
            CoordinatorType::Transaction => self.transaction = None,
            CoordinatorType::Share => {
                return Err(Error::local_illegal_state(format!(
                    "Invalid coordinator type: {}",
                    coordinator_type.name()
                )));
            },
        }
        Ok(())
    }

    /// Records a discovered coordinator.
    ///
    /// Translated from `FindCoordinatorHandler.handleResponse`'s `switch`
    /// (Java 1694-1704), whose `default:` arm logs and calls `fatalError` rather
    /// than throwing directly — so the caller, not this method, decides what to do
    /// with the error.
    fn set(&mut self, coordinator_type: CoordinatorType, node: Node) -> Result<(), Error> {
        match coordinator_type {
            CoordinatorType::Group => self.consumer_group = Some(node),
            CoordinatorType::Transaction => self.transaction = Some(node),
            CoordinatorType::Share => {
                return Err(Error::local_illegal_state(
                    "Group coordinator lookup failed: Unexpected coordinator type in response",
                ));
            },
        }
        Ok(())
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

/// The set of caller operations that may be rejected while a previous
/// transactional operation's result is still pending.
///
/// Translated from the private nested enum `TransactionManager.TransactionOperation`
/// (Java 208-225, added in AK 4.3.1). Java uses it to type the argument to
/// `throwIfPendingState` in place of a raw `String`; its `toString()` returns the
/// `displayName`, so the rejection message text is unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionOperation {
    /// `send` — `maybeAddPartition`.
    Send,
    /// `beginTransaction`.
    BeginTransaction,
    /// `prepareTransaction`.
    PrepareTransaction,
    /// `sendOffsetsToTransaction`.
    SendOffsetsToTransaction,
}

impl std::fmt::Display for TransactionOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display_name = match self {
            TransactionOperation::Send => "send",
            TransactionOperation::BeginTransaction => "beginTransaction",
            TransactionOperation::PrepareTransaction => "prepareTransaction",
            TransactionOperation::SendOffsetsToTransaction => "sendOffsetsToTransaction",
        };
        f.write_str(display_name)
    }
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
/// All six Java handlers are translated: `InitProducerIdHandler` and
/// `FindCoordinatorHandler` in Phase 5a, and `AddPartitionsToTxn`, `EndTxn`,
/// `AddOffsetsToTxn` and `TxnOffsetCommit` in Phase 5b.
pub(crate) enum TxnRequestHandlerKind {
    /// `InitProducerIdHandler` (Java 1461-1539).
    InitProducerId {
        /// The request being sent.
        builder: InitProducerIdRequestBuilder,
        /// Whether this request bumps an existing epoch rather than acquiring a
        /// producer id for the first time.
        is_epoch_bump: bool,
    },
    /// `FindCoordinatorHandler` (Java 1651-1721).
    FindCoordinator {
        /// The request being sent.
        builder: FindCoordinatorRequestBuilder,
    },
    /// `EndTxnHandler` (Java 1723-1794).
    EndTxn {
        /// The request being sent.
        builder: EndTxnRequestBuilder,
    },
    /// `AddOffsetsToTxnHandler` (Java 1796-1854).
    AddOffsetsToTxn {
        /// The request being sent.
        builder: AddOffsetsToTxnRequestBuilder,
        /// The offsets the follow-on `TxnOffsetCommit` will carry (Java 1798).
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        /// The consumer group the offsets belong to (Java 1799).
        group_metadata: ConsumerGroupMetadata,
    },
    /// `TxnOffsetCommitHandler` (Java 1856-1951).
    TxnOffsetCommit {
        /// The request being sent.
        builder: TxnOffsetCommitRequestBuilder,
    },
    /// `AddPartitionsToTxnHandler` (Java 1541-1649).
    AddPartitionsToTxn {
        /// The request being sent.
        builder: AddPartitionsToTxnRequestBuilder,
        /// This handler's own backoff, lowered to
        /// [`ADD_PARTITIONS_RETRY_BACKOFF_MS`] by
        /// [`TransactionManager::maybe_override_retry_backoff_ms`] on the first
        /// `CONCURRENT_TRANSACTIONS` of a transaction (Java 1543, 1645).
        ///
        /// The only handler kind Java gives a per-instance backoff to, which is
        /// why [`TxnRequestHandler::retry_backoff_ms`] is the manager's value for
        /// every other kind.
        retry_backoff_ms: i64,
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
    /// `AddPartitionsToTxnHandler` overrides it per instance (Java 1543).
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

    /// Creates a handler that completes an *existing* result.
    ///
    /// Corresponds to `TxnRequestHandler(TransactionalRequestResult result)`
    /// (Java 1349), whose only user is `TxnOffsetCommitHandler` (Java 1860): the
    /// `AddOffsetsToTxn` that preceded it owns the result the application awaits, and
    /// the commit must complete *that* handle rather than a new one.
    fn with_result(
        result: Arc<TransactionalRequestResult>,
        retry_backoff_ms: i64,
        kind: TxnRequestHandlerKind,
    ) -> Self {
        Self { result, is_retry: false, retry_backoff_ms, kind }
    }

    /// The handle the application awaits.
    pub(crate) fn result(&self) -> &Arc<TransactionalRequestResult> {
        &self.result
    }

    /// The request-specific state.
    pub(crate) fn kind(&self) -> &TxnRequestHandlerKind {
        &self.kind
    }

    /// A copy of the request builder, for the Sender to build and send.
    ///
    /// Corresponds to the abstract `requestBuilder()` (Java 1454). Java hands the
    /// builder itself to `newClientRequest`, keeping the handler's own reference
    /// alive for a possible retry; this crate only accepts
    /// `Box<dyn RequestBuilder>` at that boundary, so the builder is cloned —
    /// once per transactional request, never on a per-record or per-batch path.
    pub(crate) fn clone_request_builder(&self) -> Box<dyn RequestBuilder> {
        match &self.kind {
            TxnRequestHandlerKind::InitProducerId { builder, .. } => Box::new(builder.clone()),
            TxnRequestHandlerKind::FindCoordinator { builder } => Box::new(builder.clone()),
            TxnRequestHandlerKind::AddPartitionsToTxn { builder, .. } => Box::new(builder.clone()),
            TxnRequestHandlerKind::EndTxn { builder } => Box::new(builder.clone()),
            TxnRequestHandlerKind::AddOffsetsToTxn { builder, .. } => Box::new(builder.clone()),
            TxnRequestHandlerKind::TxnOffsetCommit { builder } => Box::new(builder.clone()),
        }
    }

    /// The API key of the request this handler will send.
    ///
    /// Java reads `requestBuilder().apiKey()` (`Sender.java:490`); the builder is
    /// only available here as a clone, so the key is exposed directly rather than
    /// allocating a boxed builder for a log statement.
    pub(crate) fn api_key(&self) -> &'static ApiKeys {
        match &self.kind {
            TxnRequestHandlerKind::InitProducerId { .. } => &ApiKeys::INIT_PRODUCER_ID,
            TxnRequestHandlerKind::FindCoordinator { .. } => &ApiKeys::FIND_COORDINATOR,
            TxnRequestHandlerKind::AddPartitionsToTxn { .. } => &ApiKeys::ADD_PARTITIONS_TO_TXN,
            TxnRequestHandlerKind::EndTxn { .. } => &ApiKeys::END_TXN,
            TxnRequestHandlerKind::AddOffsetsToTxn { .. } => &ApiKeys::ADD_OFFSETS_TO_TXN,
            TxnRequestHandlerKind::TxnOffsetCommit { .. } => &ApiKeys::TXN_OFFSET_COMMIT,
        }
    }

    /// The `InitProducerId` request data, or `None` for another request kind.
    ///
    /// Java reads `builder.data` directly from inside the handler subclass
    /// (Java 1501) and its tests cast the request; an accessor is the Rust
    /// equivalent of both.
    pub(crate) fn init_producer_id_request_data(&self) -> Option<&InitProducerIdRequestData> {
        match &self.kind {
            TxnRequestHandlerKind::InitProducerId { builder, .. } => Some(builder.data()),
            _ => None,
        }
    }

    /// The `FindCoordinator` request data, or `None` for another request kind.
    pub(crate) fn find_coordinator_request_data(&self) -> Option<&FindCoordinatorRequestData> {
        match &self.kind {
            TxnRequestHandlerKind::FindCoordinator { builder } => Some(builder.data()),
            _ => None,
        }
    }

    /// The `AddPartitionsToTxn` request data, or `None` for another request kind.
    pub(crate) fn add_partitions_to_txn_request_data(&self) -> Option<&AddPartitionsToTxnRequestData> {
        match &self.kind {
            TxnRequestHandlerKind::AddPartitionsToTxn { builder, .. } => Some(builder.data()),
            _ => None,
        }
    }

    /// The `EndTxn` request data, or `None` for another request kind.
    pub(crate) fn end_txn_request_data(&self) -> Option<&EndTxnRequestData> {
        match &self.kind {
            TxnRequestHandlerKind::EndTxn { builder } => Some(builder.data()),
            _ => None,
        }
    }

    /// The `AddOffsetsToTxn` request data, or `None` for another request kind.
    pub(crate) fn add_offsets_to_txn_request_data(&self) -> Option<&AddOffsetsToTxnRequestData> {
        match &self.kind {
            TxnRequestHandlerKind::AddOffsetsToTxn { builder, .. } => Some(builder.data()),
            _ => None,
        }
    }

    /// The `TxnOffsetCommit` request data, or `None` for another request kind.
    pub(crate) fn txn_offset_commit_request_data(&self) -> Option<&TxnOffsetCommitRequestData> {
        match &self.kind {
            TxnRequestHandlerKind::TxnOffsetCommit { builder } => Some(builder.data()),
            _ => None,
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
            // Java 1665: a pending FindCoordinator must always go first.
            TxnRequestHandlerKind::FindCoordinator { .. } => Priority::FindCoordinator,
            // Java 1556.
            TxnRequestHandlerKind::AddPartitionsToTxn { .. } => Priority::AddPartitionsOrOffsets,
            // Java 1739: the EndTxn request must always go last, unless we are
            // bumping the epoch as part of ending the transaction.
            TxnRequestHandlerKind::EndTxn { .. } => Priority::EndTxn,
            // Java 1817 and 1889.
            TxnRequestHandlerKind::AddOffsetsToTxn { .. } | TxnRequestHandlerKind::TxnOffsetCommit { .. } => {
                Priority::AddPartitionsOrOffsets
            },
        }
    }

    /// Whether this is an `EndTxn` request.
    ///
    /// Corresponds to `isEndTxn()` (Java 1450), whose base implementation
    /// returns `false`; only `EndTxnHandler` overrides it (Java 1744).
    pub(crate) fn is_end_txn(&self) -> bool {
        match &self.kind {
            TxnRequestHandlerKind::InitProducerId { .. }
            | TxnRequestHandlerKind::FindCoordinator { .. }
            | TxnRequestHandlerKind::AddPartitionsToTxn { .. }
            | TxnRequestHandlerKind::AddOffsetsToTxn { .. }
            | TxnRequestHandlerKind::TxnOffsetCommit { .. } => false,
            TxnRequestHandlerKind::EndTxn { .. } => true,
        }
    }

    /// Whether this is a `FindCoordinator` request.
    ///
    /// Replaces Java's `requestHandler instanceof FindCoordinatorHandler`
    /// (Java 1176), which the enum translation cannot express directly.
    fn is_find_coordinator(&self) -> bool {
        matches!(self.kind, TxnRequestHandlerKind::FindCoordinator { .. })
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
    /// Corresponds to `retryBackoffMs()` (Java 1401), plus
    /// `AddPartitionsToTxnHandler`'s override (Java 1636-1638), which is the only
    /// one: `Math.min(TransactionManager.this.retryBackoffMs, this.retryBackoffMs)`.
    /// `self.retry_backoff_ms` is the manager's value, snapshotted at construction
    /// because Java's field is `final`.
    pub(crate) fn retry_backoff_ms(&self) -> i64 {
        match &self.kind {
            TxnRequestHandlerKind::AddPartitionsToTxn { retry_backoff_ms, .. } => {
                self.retry_backoff_ms.min(*retry_backoff_ms)
            },
            _ => self.retry_backoff_ms,
        }
    }

    /// The operation name this handler's result was created for.
    pub(crate) fn operation(&self) -> &str {
        self.result.operation()
    }

    /// Sets `keepPreparedTxn` on a pending `InitProducerId` request.
    ///
    /// Java's `initializeTransactions` never calls `setKeepPreparedTxn` — see
    /// [`TransactionManager::initialize_transactions_internal`] — so the only way to
    /// exercise the KIP-939 response arm at Java 1501 is to set the flag on the
    /// request, which is what Java's `prepareInitPidResponse` overload asserts the
    /// broker sees. Test-only, because production code has no reason to reach it.
    #[cfg(test)]
    fn set_keep_prepared_txn_for_test(&mut self, keep_prepared_txn: bool) {
        if let TxnRequestHandlerKind::InitProducerId { builder, .. } = &mut self.kind {
            builder.data_mut().set_keep_prepared_txn(keep_prepared_txn);
        }
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
    pub(crate) fn fail(&self, error: Error) {
        self.result.fail(error);
    }
}

impl fmt::Debug for TxnRequestHandler {
    /// Formats as the wrapped request builder.
    ///
    /// Java's log statements interpolate `requestBuilder()`, whose
    /// `AbstractRequest.Builder.toString()` prints the request data. This crate
    /// exposes the builder only as a clone (see
    /// [`TxnRequestHandler::clone_request_builder`]), so the equivalent is reached
    /// through `Debug` instead.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            TxnRequestHandlerKind::InitProducerId { builder, .. } => write!(f, "{builder:?}"),
            TxnRequestHandlerKind::FindCoordinator { builder } => write!(f, "{builder:?}"),
            TxnRequestHandlerKind::AddPartitionsToTxn { builder, .. } => write!(f, "{builder:?}"),
            TxnRequestHandlerKind::EndTxn { builder } => write!(f, "{builder:?}"),
            TxnRequestHandlerKind::AddOffsetsToTxn { builder, .. } => write!(f, "{builder:?}"),
            TxnRequestHandlerKind::TxnOffsetCommit { builder } => write!(f, "{builder:?}"),
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

// =========================================================================
// PHASE-5B METHOD ACCOUNTING (`definition-of-done.md` §2)
//
// `TransactionManager.java` declares **90** distinct method names at class level.
// After Phase 5b, **all 90** have a Rust `fn`, here or on `Sender` — the four that
// moved there are named in the "Lock topology" section below.
//
// Two exclusions are enforced by the derivation rather than asserted beside it,
// because Critic 45 issue 4 was exactly the failure of asserting them: an earlier
// revision claimed the constructor "has no return type and so is not counted" while
// its regex counted it anyway (the modifier group is `*`, so the engine backtracks
// to zero repetitions and reads `public` as the return type), and then scored that
// phantom entry *present* against `fn transaction_manager` — a `#[cfg(test)]`
// `SenderTestContext` accessor, not a translation.
//
//   1. The negative lookahead after the modifier run forces it to consume every
//      modifier, so a return type **and** a name are both required. A constructor
//      has only a name and cannot match.
//   2. Each Rust file is cut at its `#[cfg(test)]` module before the `fn` scan, so
//      no test-only item can satisfy a production-method claim.
//
// Derivation (real output below it):
//
//   python3 - <<'PY'
//   import re
//   J = ("kafka/clients/src/main/java/org/apache/kafka/clients/producer/"
//        "internals/TransactionManager.java")
//   MODS = r'(?:public|private|protected|synchronized|static|final|abstract)'
//   decl = re.compile(rf'^    (?:{MODS}\s+)*(?!{MODS}\s)'
//                     rf'[A-Za-z_][A-Za-z0-9_<>,\.\[\]]*(?:<[^>]*>)?\s+'
//                     rf'([a-zA-Z_][A-Za-z0-9_]*)\s*\(')
//   names = set()
//   for line in open(J):
//       if any(k in line for k in ('class ', 'enum ', 'interface ')): continue
//       m = decl.match(line)
//       if m: names.add(m.group(1))
//   snake = lambda n: re.sub(r'(?<!^)(?=[A-Z])', '_', n).lower().replace('2_p_c', '2pc')
//   production = lambda f: open(f).read().split("\n#[cfg(test)]\n")[0]
//   rust = "".join(production(f) for f in
//                  ("src/producer/internals/transaction_manager.rs",
//                   "src/producer/internals/sender.rs"))
//   defs = set(re.findall(r'\bfn ([a-z_0-9]+)\s*[(<]', rust))
//   missing = sorted(n for n in names if snake(n) not in defs)
//   print(len(names), len(missing), len(names) - len(missing))
//   print(missing)
//   PY
//
//   90 1 89
//   ['is2PCEnabled']
//
// The single reported miss is a snake-conversion artefact, not a gap: the crate
// spells it `is_2pc_enabled`, which the naive conversion renders as `is2pc_enabled`.
// So 89 + 1 = **90** are present and **0** have no `fn`.
//
// Phase 5b landed the nine Phase 5a owed. Where each went:
//
//   `beginCommit` (353), `beginAbort` (361) and `beginCompletingTransaction` (373)
//     — the `EndTxnHandler` and the two entry points that build it. `beginAbort`
//     was previously present only as the `ensureTransactional()` guard
//     `Sender.run`'s shutdown loop depends on (PLAN §10.6 deviation 10), which the
//     derivation could not report as owed because a partial translation still
//     defines the `fn`; that hole is now closed.
//   `sendOffsetsToTransaction` (404), `txnOffsetCommitHandler` (1221) and
//     `hasPendingOffsetCommits` (1001) — the offsets path, with the
//     `pendingTxnOffsetCommits` field (103) the last of those reads.
//   `addPartitionsToTransactionHandler` (1210) — the `AddPartitionsToTxn` factory.
//   `maybeUpdateTransactionV2Enabled` (492) — KIP-890 feature discovery, with the
//     `latestFinalizedFeaturesEpoch` field (145).
//   `prepareTransaction` (342) — KIP-939 two-phase commit.
//
// Arithmetic, read off the derivation rather than maintained beside it: 1 reported
// − 1 artefact = 0 owed; 90 − 0 = **90** translated.
//
// The derivation covers *names*, not bodies. Two things it cannot see, and where
// they are pinned instead:
//
//   - A method present but partially translated. Phase 5a's `beginAbort` was
//     exactly that, and it is why this block's prose has to name what changed
//     rather than lean on the count. Every arm the phase deferred previously
//     returned `Errors::UnsupportedVersion` naming Phase 5b. The mechanical check
//     that none survives has to skip comment lines, or it matches its own
//     documentation and can never reach 0:
//       awk '!/^ *\/\// && /unsupported_version/' \
//         src/producer/internals/transaction_manager.rs | wc -l
//     Real output: `0`.
//   - A method whose *behaviour* is untested. That is the test accounting block's
//     job (`definition-of-done.md` §3), at the end of this file.
// =========================================================================

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
/// **Phase 5b** completes the class: the four remaining request handlers
/// (`AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn`), the
/// entry points that construct them (`beginCommit`, `beginAbort`,
/// `beginCompletingTransaction`, `sendOffsetsToTransaction`,
/// `maybeAddPartition`'s registration branch), KIP-890 Transaction V2 and KIP-939
/// two-phase commit. All 90 of Java's methods now have a Rust `fn` — see the
/// PHASE-5B METHOD ACCOUNTING block above — and all nine [`State`] variants are
/// enterable; the full 9×9 table has been translated since Phase 3, see
/// [`State::is_transition_valid`].
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
/// Phase 5a adds the coordinator subsystem (`coordinator` 958,
/// `lookupCoordinator` 969/1191, `handleCoordinatorReady` 1103) and splits the
/// remaining three pieces of §2 state two ways. The two coordinator nodes —
/// `transactionCoordinator` (Java 137) and `consumerGroupCoordinator` (138) — go
/// to `Sender` as [`CoordinatorNodes`], for the same confinement reason.
/// `coordinatorSupportsBumpingEpoch` (139) does **not**, because Java reads it
/// from the application thread; the field's own docs give the evidence, and PLAN
/// §10.7 records it as a deviation.
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
    /// Read by [`Self::handle_coordinator_ready`] and
    /// [`Self::maybe_update_transaction_v2_enabled`].
    api_versions: Arc<ApiVersions>,

    txn_partition_map: TxnPartitionMap,

    /// Offsets awaiting a successful `TxnOffsetCommit` (Java 103).
    ///
    /// Filled by [`Self::txn_offset_commit_handler`] (Java 1226) and drained
    /// per-partition by [`Self::handle_txn_offset_commit_response`] (Java 1930).
    ///
    /// The handler builds its request from *this* map rather than from the offsets it
    /// was handed, so each construction picks up whatever is still outstanding — but
    /// a *retry* of an already-built handler re-sends that construction's snapshot,
    /// not a shrunken one. See
    /// [`Self::handle_txn_offset_commit_response`] for why, and for why narrowing the
    /// retry would be a wire-level divergence.
    pending_txn_offset_commits: HashMap<TopicPartition, CommittedOffset>,

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
    /// Written by [`Self::maybe_add_partition`]'s registration branch (Java 458)
    /// and drained by [`Self::add_partitions_to_transaction_handler`] (Java 1211).
    new_partitions_in_transaction: HashSet<TopicPartition>,
    /// Partitions whose `AddPartitionsToTxn` request is in flight (Java 123).
    ///
    /// Filled by [`Self::add_partitions_to_transaction_handler`] (Java 1211) and
    /// cleared per-partition by
    /// [`Self::handle_add_partitions_to_txn_response`] (Java 1620).
    pending_partitions_in_transaction: HashSet<TopicPartition>,
    /// Partitions the broker has confirmed as part of the transaction (Java 124).
    ///
    /// Written by [`Self::handle_add_partitions_to_txn_response`] (Java 1627) and,
    /// under Transaction V2, directly by [`Self::maybe_add_partition`] (Java 450).
    partitions_in_transaction: HashSet<TopicPartition>,
    /// The operation whose [`TransactionalRequestResult`] the caller has not yet
    /// acknowledged (Java 125).
    ///
    /// See [`Self::handle_cached_transaction_request_result`] for the semantics;
    /// `.claude/rules/producer-transactions.md` §5 is the binding contract.
    pending_transition: Option<PendingStateTransition>,

    /// How many times [`Self::close`] has been called.
    ///
    /// Test-only, and the direct analogue of Java's
    /// `verify(transactionManager, times(1)).close()` in
    /// `SenderTest.testSenderShouldCloseWhenTransactionManagerInErrorState`
    /// (`SenderTest.java:3413`). A counter rather than a flag, because the assertion is
    /// `times(1)` — a boolean could not tell one call from three.
    #[cfg(test)]
    close_call_count: u32,

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
    /// Set by `maybeAddPartition`'s Transaction V2 arm (Java 451),
    /// `sendOffsetsToTransaction`'s Transaction V2 arm (420),
    /// `AddPartitionsToTxnHandler.handleResponse` (1629) and
    /// `AddOffsetsToTxnHandler.handleResponse` (1831); cleared by
    /// [`Self::reset_transaction_state`].
    transaction_started: bool,

    current_state: State,
    last_error: Option<Error>,
    producer_id_and_epoch: ProducerIdAndEpoch,
    client_side_epoch_bump_required: bool,
    /// The finalized-features epoch the KIP-890 feature read last observed
    /// (Java 145).
    ///
    /// Written only by [`Self::maybe_update_transaction_v2_enabled`], which uses it
    /// to skip the read when `ApiVersions` has learned nothing new.
    latest_finalized_features_epoch: i64,
    /// Whether the cluster has finalized `transaction.version` at level 2 or
    /// above, i.e. whether KIP-890 Transaction V2 is in force (Java 146).
    ///
    /// Written only by [`Self::maybe_update_transaction_v2_enabled`] (Java 492).
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
    /// the `Sender` as §2 requires: both of their readers are Sender-side —
    /// `coordinator(CoordinatorType)` (Java 961/963), reached from
    /// `Sender.java:481`, and `handleCoordinatorReady` (Java 1104-1105), which is
    /// why [`Self::handle_coordinator_ready`] receives them. See
    /// [`CoordinatorNodes`].
    coordinator_supports_bumping_epoch: bool,
    /// The producer id and epoch of the transaction prepared for a two-phase
    /// commit (Java 148).
    ///
    /// Written by [`Self::prepare_transaction`] (Java 342) and by
    /// `InitProducerIdHandler`'s `keepPreparedTxn` arm (Java 1507); cleared by
    /// [`Self::reset_transaction_state`] and read by
    /// [`Self::prepared_transaction_state`].
    prepared_txn_state: ProducerIdAndEpoch,
}

impl TransactionManager {
    /// Sentinel for "no transactional request is currently in flight".
    ///
    /// `pub(crate)` because the field it guards lives on the [`Sender`] task
    /// (rules §2); see [`PendingRequests`].
    ///
    /// [`Sender`]: crate::producer::internals::Sender
    pub(crate) const NO_INFLIGHT_REQUEST_CORRELATION_ID: i32 = -1;

    /// The KIP-890 feature flag whose finalized level decides whether Transaction V2
    /// is in force.
    ///
    /// Java writes the string literal inline at `TransactionManager.java:499`; named
    /// here because [`TransactionManager::maybe_update_transaction_v2_enabled`] and the
    /// tests in this file and in `sender.rs` all need it. Per CLAUDE.md §2 it is
    /// exported only by this file — reached as
    /// `producer::internals::transaction_manager::TRANSACTION_VERSION_FEATURE`, never
    /// re-exported through the parent module.
    pub(crate) const TRANSACTION_VERSION_FEATURE: &str = "transaction.version";

    /// The `retryBackoffMs` an `AddPartitionsToTxn` retry uses after the first
    /// `CONCURRENT_TRANSACTIONS` error of a transaction.
    ///
    /// Translated from `TransactionManager.ADD_PARTITIONS_RETRY_BACKOFF_MS`
    /// (Java 133).
    const ADD_PARTITIONS_RETRY_BACKOFF_MS: i64 = 20;

    /// Creates a transaction manager.
    ///
    /// Translated from `TransactionManager(LogContext, String, int, long,
    /// ApiVersions, boolean)` (Java 208).
    ///
    /// Phase 3's MILESTONE-11 GUARD, which refused a `transactional_id` so that
    /// the untranslated transactional arms stayed unreachable, is gone: Phase 5a
    /// implemented the transactional state machine and Phase 5b the four request
    /// handlers, Transaction V2 and two-phase commit, so nothing is deferred here
    /// any more. `KafkaProducer::new` keeps its own guard on
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
            pending_txn_offset_commits: HashMap::new(),
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
            latest_finalized_features_epoch: -1,
            is_transaction_v2_enabled: false,
            enable_2pc,
            coordinator_supports_bumping_epoch: false,
            prepared_txn_state: ProducerIdAndEpoch::NONE,
            #[cfg(test)]
            close_call_count: 0,
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
    /// - [`Error::LocalIllegalState`] on a non-transactional producer
    ///   (`ensureTransactional`), when the manager is already in an error state
    ///   (`maybeFailWithError`), when a *different* operation's result is still
    ///   unacknowledged, or when `UNINITIALIZED → INITIALIZING` is not a valid
    ///   transition — which is what rejects a second `initTransactions` after the
    ///   first has been acknowledged (`testInitializeTransactionsTwiceRaisesError`).
    pub(crate) fn initialize_transactions(
        &mut self,
        keep_prepared_txn: bool,
        pending_requests: &mut PendingRequests,
    ) -> Result<Arc<TransactionalRequestResult>, Error> {
        self.initialize_transactions_internal(ProducerIdAndEpoch::NONE, keep_prepared_txn, pending_requests)
    }

    /// Bumps the epoch of an existing producer id as part of ending a
    /// transaction.
    ///
    /// Translated from the package-private overload
    /// `initializeTransactions(ProducerIdAndEpoch)` (Java 291), whose only Java
    /// caller is [`Self::begin_completing_transaction`] (`:1200`) when
    /// `clientSideEpochBumpRequired` holds. Renamed because Rust has no
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
    ) -> Result<Arc<TransactionalRequestResult>, Error> {
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
    ) -> Result<Arc<TransactionalRequestResult>, Error> {
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
    /// [`Error::LocalIllegalState`] on a non-transactional producer, while another
    /// operation's result is unacknowledged, or when the manager is in an error
    /// state; and from `READY → IN_TRANSACTION` being the table's only arm into
    /// [`State::InTransaction`], which is what rejects `beginTransaction` before
    /// `initTransactions` completes.
    pub(crate) fn begin_transaction(&mut self) -> Result<(), Error> {
        self.ensure_transactional()?;
        self.return_error_if_pending_state(TransactionOperation::BeginTransaction)?;
        self.maybe_fail_with_error()?;
        self.transition_to(State::InTransaction, None, Caller::App)
    }

    /// Prepares a transaction for a two-phase commit (KIP-939).
    ///
    /// Translated from `prepareTransaction()` (Java 342). This transitions the
    /// transaction to [`State::PreparedTransaction`] and records the current
    /// producer id and epoch in `preparedTxnState`, so
    /// [`Self::prepared_transaction_state`] can hand them to an external
    /// transaction coordinator that will later commit or abort on the producer's
    /// behalf.
    ///
    /// Stays synchronous: Java's body is four calls and a field write, with no wait.
    ///
    /// # Reachable only from `KafkaProducer`
    ///
    /// PLAN §Phase-6 records that `prepareTransaction` is on `KafkaProducer` and
    /// **not** on the `Producer` interface. It is nonetheless a public method of
    /// this class with no other Java caller, so the transition is application-side
    /// (rules §1).
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] on a non-transactional producer, while another
    /// operation's result is unacknowledged, when the manager is in an error state,
    /// or when `→ PREPARED_TRANSACTION` is not valid — its only sources are
    /// `IN_TRANSACTION` and `INITIALIZING` (Java 172).
    pub(crate) fn prepare_transaction(&mut self) -> Result<(), Error> {
        self.ensure_transactional()?;
        self.return_error_if_pending_state(TransactionOperation::PrepareTransaction)?;
        self.maybe_fail_with_error()?;
        self.transition_to(State::PreparedTransaction, None, Caller::App)?;
        self.prepared_txn_state =
            ProducerIdAndEpoch::new(self.producer_id_and_epoch.producer_id, self.producer_id_and_epoch.epoch);
        Ok(())
    }

    /// Begins committing the transaction, returning the handle the application
    /// awaits.
    ///
    /// Translated from `beginCommit()` (Java 353). Both transitions are
    /// application-side: Java reaches this from `KafkaProducer.commitTransaction`
    /// only (rules §1).
    ///
    /// # Not blocking here
    ///
    /// Java's caller blocks on `result.await(maxBlockTimeMs, ..)`
    /// (`KafkaProducer.java:742`); this returns the
    /// [`TransactionalRequestResult`] and Phase 6's
    /// `KafkaProducer::commit_transaction` awaits it, because the caller holds the
    /// shared mutex and rules §4 forbids holding it across an `.await`.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] on a non-transactional producer, when a
    /// *different* operation's result is still unacknowledged, when the manager is
    /// in an error state (`maybeFailWithError`), or when
    /// `→ COMMITTING_TRANSACTION` is not a valid transition — which is what rejects
    /// a commit outside a transaction.
    pub(crate) fn begin_commit(
        &mut self,
        pending_requests: &mut PendingRequests,
    ) -> Result<Arc<TransactionalRequestResult>, Error> {
        self.handle_cached_transaction_request_result(
            |manager| {
                manager.maybe_fail_with_error()?;
                manager.transition_to(State::CommittingTransaction, None, Caller::App)?;
                manager.begin_completing_transaction(TransactionResult::Commit, pending_requests)
            },
            State::CommittingTransaction,
            "commitTransaction",
        )
    }

    /// Begins aborting the transaction, returning the handle the caller awaits.
    ///
    /// Translated from `beginAbort()` (Java 361).
    ///
    /// # Why this one takes a `Caller` when its siblings do not
    ///
    /// `beginAbort()` is the **only** transactional entry point Java reaches from two
    /// threads (`grep -rn '\.beginAbort(' producer/`):
    ///
    ///   - `KafkaProducer.abortTransaction` (`KafkaProducer.java:818`) — application;
    ///   - `Sender.run`'s shutdown abort loop (`Sender.java:273`) — the Sender task.
    ///
    /// So rules §1's "where a method is reachable from both, it takes `caller` as a
    /// parameter and forwards it — do NOT default it" applies here and nowhere else in
    /// this phase: `beginCommit` (`KafkaProducer.java:783`),
    /// `sendOffsetsToTransaction` (`:740`), `maybeAddPartition` (`:1045`),
    /// `initializeTransactions` (`:652`) and `beginTransaction` (`:679`) each have
    /// exactly one Java caller, all application-side.
    ///
    /// The difference is not cosmetic. An invalid `→ ABORTING_TRANSACTION` on the
    /// Sender side must **poison**: `FATAL_ERROR` plus a recorded `lastError` before
    /// the error propagates (Java 1124-1127). Java anticipates exactly that throw at
    /// this call site — `Sender.java:269-271` reads *"It is possible for the
    /// transaction manager to throw errors when aborting. Catch these so as not to
    /// interfere with the rest of the shutdown logic"* — and force-closes on it.
    ///
    /// The window is real in both languages: the shutdown loop's guard admits
    /// `IN_TRANSACTION` and `ABORTABLE_ERROR` (both valid sources), and the
    /// application task can move the state between that guard read and this call,
    /// because the manager lock is released in between — Java's separate
    /// `synchronized` calls and [`Sender::begin_abort`]'s separate `lock()`s behave
    /// identically there.
    ///
    /// The Sender is currently the *only* live caller, since
    /// `KafkaProducer::new` still rejects `transactional.id` until Phase 6
    /// (PLAN §7.1) — so hardcoding [`Caller::App`] was wrong for the one caller that
    /// exists.
    ///
    /// # Everything below the transition needs no `caller`
    ///
    /// [`Self::begin_completing_transaction`] performs no transition of its own, and
    /// the one it can reach — `initializeTransactions`'s `!isEpochBump` arm — is
    /// unreachable from here: this path always passes a valid `producerIdAndEpoch`
    /// (an abort requires a live transaction, which requires a producer id), so
    /// `is_epoch_bump` is always true. `resetTransactionState`, which the resulting
    /// `EndTxn` response drives, hardcodes [`Caller::Sender`] for its own reason
    /// (PLAN §10.7 deviation 11).
    ///
    /// Note `maybeFailWithError` is **skipped** when the manager is already in
    /// [`State::AbortableError`] (Java 363-364) — that is the whole point of an
    /// abortable error, and it is why `assertAbortableError` can abort after a
    /// failed commit.
    ///
    /// # Errors
    ///
    /// As [`Self::begin_commit`], with `→ ABORTING_TRANSACTION` as the transition.
    /// With [`Caller::Sender`] an invalid transition additionally leaves the manager
    /// in [`State::FatalError`].
    ///
    /// [`Sender::begin_abort`]: crate::producer::internals::Sender
    pub(crate) fn begin_abort(
        &mut self,
        pending_requests: &mut PendingRequests,
        caller: Caller,
    ) -> Result<Arc<TransactionalRequestResult>, Error> {
        self.handle_cached_transaction_request_result(
            |manager| {
                if manager.current_state != State::AbortableError {
                    manager.maybe_fail_with_error()?;
                }
                manager.transition_to(State::AbortingTransaction, None, caller)?;

                // We're aborting the transaction, so there should be no need to add new partitions
                manager.new_partitions_in_transaction.clear();
                manager.begin_completing_transaction(TransactionResult::Abort, pending_requests)
            },
            State::AbortingTransaction,
            "abortTransaction",
        )
    }

    /// Registers consumer-group offsets as part of the transaction, returning the
    /// handle the application awaits.
    ///
    /// Translated from
    /// `sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata>, ConsumerGroupMetadata)`
    /// (Java 404). Note this entry point is **not** wrapped in
    /// [`Self::handle_cached_transaction_request_result`] — Java calls
    /// `throwIfPendingState` directly instead, so a timed-out
    /// `sendOffsetsToTransaction` is not retryable the way a commit is.
    ///
    /// # Transaction V2 skips `AddOffsetsToTxn`
    ///
    /// Under TV2 the client sends `TxnOffsetCommit` straight away (Java 415-418) and
    /// marks the transaction started itself, because there is no `AddOffsetsToTxn`
    /// response to do it. Under V1 the `AddOffsetsToTxn` registers the group with
    /// the transaction coordinator first, and its success arm enqueues the
    /// `TxnOffsetCommit` carrying the *same* result object — which is why the
    /// caller's handle does not complete until the second round trip returns.
    ///
    /// # Not blocking here
    ///
    /// As [`Self::begin_commit`]: Java's caller blocks on
    /// `result.await(maxBlockTimeMs, ..)` (`KafkaProducer.java:820`); Phase 6 awaits
    /// the returned handle.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] on a non-transactional producer, while another
    /// operation's result is unacknowledged, when the manager is in an error state,
    /// or when no transaction is in progress.
    pub(crate) fn send_offsets_to_transaction(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: ConsumerGroupMetadata,
        pending_requests: &mut PendingRequests,
    ) -> Result<Arc<TransactionalRequestResult>, Error> {
        self.ensure_transactional()?;
        self.return_error_if_pending_state(TransactionOperation::SendOffsetsToTransaction)?;
        self.maybe_fail_with_error()?;

        if self.current_state != State::InTransaction {
            return Err(Error::local_illegal_state(format!(
                "Cannot send offsets if a transaction is not in progress (currentState= {})",
                self.current_state
            )));
        }

        // In transaction V2, the client will skip sending AddOffsetsToTxn before sending txnOffsetCommit.
        let handler = if self.is_transaction_v2_enabled() {
            kafka_debug!(
                self.log_context,
                "Begin adding offsets {:?} for consumer group {} to transaction with transaction protocol V2",
                offsets,
                group_metadata.group_id()
            );
            let handler = self.txn_offset_commit_handler(None, &offsets, &group_metadata);
            self.transaction_started = true;
            handler
        } else {
            kafka_debug!(
                self.log_context,
                "Begin adding offsets {:?} for consumer group {} to transaction",
                offsets,
                group_metadata.group_id()
            );
            let mut request_data = AddOffsetsToTxnRequestData::new();
            request_data
                // `ensureTransactional()` has already run, so the id is present.
                .set_transactional_id(self.transactional_id.clone().unwrap_or_default())
                .set_producer_id(self.producer_id_and_epoch.producer_id)
                .set_producer_epoch(self.producer_id_and_epoch.epoch)
                .set_group_id(group_metadata.group_id().to_string());
            TxnRequestHandler::new(
                "AddOffsetsToTxn",
                self.retry_backoff_ms,
                TxnRequestHandlerKind::AddOffsetsToTxn {
                    builder: AddOffsetsToTxnRequestBuilder::new(request_data),
                    offsets,
                    group_metadata,
                },
            )
        };

        let result = Arc::clone(handler.result());
        self.enqueue_request(pending_requests, handler);
        Ok(result)
    }

    /// Records `offsets` as pending and builds the `TxnOffsetCommit` that commits
    /// every offset still outstanding.
    ///
    /// Translated from
    /// `txnOffsetCommitHandler(TransactionalRequestResult, Map, ConsumerGroupMetadata)`
    /// (Java 1221).
    ///
    /// `result` is `None` where Java passes `null`, which it does on the Transaction
    /// V2 path only (Java 417): there is no `AddOffsetsToTxn` whose result the
    /// commit must complete, so the handler gets a fresh one.
    fn txn_offset_commit_handler(
        &mut self,
        result: Option<Arc<TransactionalRequestResult>>,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: &ConsumerGroupMetadata,
    ) -> TxnRequestHandler {
        for (topic_partition, offset_and_metadata) in offsets {
            // Java's `OffsetAndMetadata.metadata()` is nullable and passed straight
            // through; this crate normalises it to a `String` at construction (as
            // Java's own constructor does for null), so it is always present here.
            let committed_offset = CommittedOffset::new(
                offset_and_metadata.offset(),
                Some(offset_and_metadata.metadata().to_string()),
                offset_and_metadata.leader_epoch(),
            );
            self.pending_txn_offset_commits
                .insert(topic_partition.clone(), committed_offset);
        }

        let builder = TxnOffsetCommitRequestBuilder::with_options(
            // `ensureTransactional()` has already run, so the id is present.
            TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                .set_transactional_id(self.transactional_id.clone().unwrap_or_default())
                .set_consumer_group_id(group_metadata.group_id())
                .set_producer_id(self.producer_id_and_epoch.producer_id)
                .set_producer_epoch(self.producer_id_and_epoch.epoch)
                .set_pending_txn_offset_commits(&self.pending_txn_offset_commits)
                .set_is_transaction_v2_enabled(self.is_transaction_v2_enabled())
                .set_member_id(group_metadata.member_id().to_string())
                .set_generation_id(group_metadata.generation_id())
                .set_group_instance_id(group_metadata.group_instance_id().map(ToString::to_string))
                .build()
                .expect("TxnOffsetCommitRequestBuilderOptionsBuilder::build: every mandatory parameter is set above"),
        );
        let kind = TxnRequestHandlerKind::TxnOffsetCommit { builder };
        match result {
            Some(result) => TxnRequestHandler::with_result(result, self.retry_backoff_ms, kind),
            None => TxnRequestHandler::new("TxnOffsetCommitHandler", self.retry_backoff_ms, kind),
        }
    }

    /// Enqueues the `EndTxn` that completes the transaction, plus any
    /// `AddPartitionsToTxn` still owed, and re-reads the Transaction V2 feature.
    ///
    /// Translated from
    /// `beginCompletingTransaction(TransactionResult)` (Java 373).
    ///
    /// # The three orderings Java's own comment calls out
    ///
    /// `maybeUpdateTransactionV2Enabled(false)` sits **between** building the
    /// `EndTxnRequest.Builder` and enqueueing the handler (Java 386), and Java
    /// explains why: the builder must capture the version the transaction *started*
    /// with, while the `clientSideEpochBumpRequired` the read may set has to be
    /// visible to the check below — and doing the read after the handler is enqueued
    /// would race the `EndTxn`'s completion.
    ///
    /// # Errors
    ///
    /// Only through the `clientSideEpochBumpRequired` tail, which re-enters
    /// [`Self::initialize_transactions_with_producer_id_and_epoch`]; that path's own
    /// `maybeFailWithError` can reject.
    fn begin_completing_transaction(
        &mut self,
        transaction_result: TransactionResult,
        pending_requests: &mut PendingRequests,
    ) -> Result<Arc<TransactionalRequestResult>, Error> {
        if !self.new_partitions_in_transaction.is_empty() {
            let handler = self.add_partitions_to_transaction_handler();
            self.enqueue_request(pending_requests, handler);
        }

        let mut request_data = EndTxnRequestData::new();
        request_data
            // `ensureTransactional()` has already run, so the id is present; Java
            // would interpolate a null as the text "null".
            .set_transactional_id(self.transactional_id.clone().unwrap_or_default())
            .set_producer_id(self.producer_id_and_epoch.producer_id)
            .set_producer_epoch(self.producer_id_and_epoch.epoch)
            .set_committed(transaction_result.id());
        let builder = EndTxnRequestBuilder::new(request_data, self.is_transaction_v2_enabled);

        // Maybe update the transaction version here before we enqueue the EndTxn request so there are no races with
        // completion of the EndTxn request. Since this method may update clientSideEpochBumpRequired, we want to update
        // before the check below, but we also want to call it after the EndTxnRequest.Builder so we complete the
        // transaction with the same version as it started.
        self.maybe_update_transaction_v2_enabled(false);

        let handler = TxnRequestHandler::new(
            // Java: `super("EndTxn(" + builder.data.committed() + ")")` (Java 1726),
            // which interpolates a Java `boolean`.
            &format!("EndTxn({})", transaction_result.id()),
            self.retry_backoff_ms,
            TxnRequestHandlerKind::EndTxn { builder },
        );
        let result = Arc::clone(handler.result());
        self.enqueue_request(pending_requests, handler);

        // If an epoch bump is required for recovery, initialize the transaction after completing the EndTxn request.
        // If we are upgrading to TV2 transactions on the next transaction, also bump the epoch.
        if self.client_side_epoch_bump_required {
            let producer_id_and_epoch = self.producer_id_and_epoch;
            return self.initialize_transactions_with_producer_id_and_epoch(producer_id_and_epoch, pending_requests);
        }

        Ok(result)
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

    /// Checks all the finalized features from `api_versions` to verify whether
    /// Transaction V2 is enabled.
    ///
    /// Translated from `maybeUpdateTransactionV2Enabled(boolean)` (Java 492).
    ///
    /// Sets `client_side_epoch_bump_required` if upgrading to V2 since we need to
    /// bump the epoch. This is because V2 no longer adds partitions explicitly and
    /// there are some edge cases on upgrade that can be avoided by fencing the old
    /// V1 transaction epoch. For example, we won't consider partitions from the
    /// previous transaction as already added to the new V2 transaction if the epoch
    /// is fenced.
    ///
    /// # A missing feature map is Java's unreachable NPE
    ///
    /// Java dereferences `info.finalizedFeatures` (Java 499) without a null check,
    /// and the field starts out `null` (`ApiVersions.java:34`). The guard above it
    /// is what makes that safe: `latestFinalizedFeaturesEpoch` and
    /// `maxFinalizedFeaturesEpoch` both start at `-1`, so the method returns early
    /// until some node reports a features epoch — and `ApiVersions.update` writes
    /// the map and the epoch together (`ApiVersions.java:47-50`). This crate models
    /// the field as an `Option`, and treats `None` as an empty map: the same answer
    /// (`transaction.version` absent ⇒ V2 off) without a panic (CLAUDE.md §10.1).
    pub(crate) fn maybe_update_transaction_v2_enabled(&mut self, on_initialization: bool) {
        if self.latest_finalized_features_epoch >= self.api_versions.max_finalized_features_epoch() {
            return;
        }
        let info = self.api_versions.finalized_features_info();
        self.latest_finalized_features_epoch = info.finalized_features_epoch;
        let transaction_version = info
            .finalized_features
            .as_ref()
            .and_then(|features| features.get(TransactionManager::TRANSACTION_VERSION_FEATURE).copied());
        let was_transaction_v2_enabled = self.is_transaction_v2_enabled;
        self.is_transaction_v2_enabled = transaction_version.is_some_and(|version| version >= 2);
        kafka_debug!(
            self.log_context,
            "Updating isTV2 enabled to {} with FinalizedFeaturesEpoch {}",
            self.is_transaction_v2_enabled,
            self.latest_finalized_features_epoch
        );
        if !on_initialization && !was_transaction_v2_enabled && self.is_transaction_v2_enabled {
            self.client_side_epoch_bump_required = true;
        }
    }

    /// Whether KIP-890 Transaction V2 is in use.
    ///
    /// Corresponds to `isTransactionV2Enabled()` (Java 506).
    /// [`Self::maybe_update_transaction_v2_enabled`] is the only writer, and
    /// `Sender.sendProduceRequest` (`Sender.java:924-926`) reads it from outside
    /// this module in Phase 6.
    pub(crate) fn is_transaction_v2_enabled(&self) -> bool {
        self.is_transaction_v2_enabled
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
    /// Java reads the field directly from `handleCoordinatorReady` (Java 1104) and
    /// `maybeUpdateTransactionV2Enabled` (Java 493). Exposed because
    /// `TransactionManagerTest` reaches `apiVersions` too
    /// (`testNeedToTriggerEpochBumpFromClientDuringCoordinatorDisconnect`,
    /// Java 3719).
    pub(crate) fn api_versions(&self) -> &Arc<ApiVersions> {
        &self.api_versions
    }

    // -- Error state --------------------------------------------------------

    /// The error that moved this manager into an error state, if any.
    ///
    /// Corresponds to `lastError()` (Java 462).
    pub(crate) fn last_error(&self) -> Option<&Error> {
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

    /// The current state. Visible for testing, as Java's package-private field
    /// access is.
    #[cfg(test)]
    fn current_state(&self) -> State {
        self.current_state
    }

    /// How many times [`Self::close`] has been called — Java's
    /// `verify(transactionManager, times(n)).close()`.
    #[cfg(test)]
    pub(crate) fn close_call_count(&self) -> u32 {
        self.close_call_count
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
    pub(crate) fn force_enqueue_init_producer_id_for_test(
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
    pub(crate) fn transition_to_fatal_error(&mut self, error: Error, caller: Caller) -> Result<(), Error> {
        // Fatality is recorded by the transition itself, not on the error: Java
        // keeps it in `currentState` (`hasFatalError()` == `currentState ==
        // FATAL_ERROR`) and stores a plain `RuntimeException` in `lastError`.
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
    pub(crate) fn transition_to_abortable_error(&mut self, error: Error, caller: Caller) -> Result<(), Error> {
        if self.current_state == State::AbortingTransaction {
            kafka_debug!(
                self.log_context,
                "Skipping transition to abortable error state since the transaction is already being aborted. \
                 Underlying error: {}",
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
        abortable_error: Error,
        fatal_error: Error,
        caller: Caller,
    ) -> Result<(), Error> {
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
    /// hardcoded per rules §1 rather than taken as a parameter. Both call sites
    /// arrived with Phase 5b's `EndTxnHandler`; the method itself was translated in
    /// Phase 5a because it is the only writer that clears the per-transaction sets
    /// and `prepared_txn_state`, and splitting it from the state machine would have
    /// meant writing it twice.
    fn reset_transaction_state(&mut self) -> Result<(), Error> {
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
    pub(crate) fn transition_to_uninitialized(&mut self, error: &Error, caller: Caller) -> Result<(), Error> {
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
        error: &Error,
        caller: Caller,
    ) -> Result<(), Error> {
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
    /// no `Error` variant for that family — a genuine authentication failure
    /// is an [`AuthenticationError`](crate::common::errors::AuthenticationError)
    /// carried inside an `io::Error` at the transport layer — so the parameter is
    /// a plain [`Error`] and the caller supplies it. The body treats it as a
    /// `RuntimeException` in Java too.
    pub(crate) fn authentication_failed(
        &mut self,
        pending_requests: &mut PendingRequests,
        error: &Error,
        caller: Caller,
    ) -> Result<(), Error> {
        // Java routes this through `TxnRequestHandler.fatalError` (Java 941 →
        // 1357-1360), so the failure is a fatal transition. Rust inlines the two
        // statements rather than calling `self.fatal_error`. The error is passed
        // through unchanged; fatality is recorded by the transition into
        // `State::FatalError`, which `has_fatal_error()` reads — Java keeps it in
        // `currentState`, not on the exception.
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
    pub(crate) fn close(&mut self, pending_requests: &mut PendingRequests, caller: Caller) -> Result<(), Error> {
        #[cfg(test)]
        {
            self.close_call_count += 1;
        }
        // Java routes each handler through `TxnRequestHandler.fatalError`
        // (Java 952 → 1357-1360) and fails the pending slot directly (Java
        // 953-955) — all fatal transitions. Every copy carries the same message;
        // the fatality of the situation lives in the state machine, so a woken
        // caller learns it from `has_fatal_error()` rather than from the error.
        let shutdown_error = Error::kafka_message("The producer closed forcefully");
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
    /// - [`Error::LocalIllegalState`] when the transition is not permitted.
    ///   When `caller` is [`Caller::Sender`] the manager first moves to
    ///   [`State::FatalError`] and records the error as [`Self::last_error`]
    ///   ("poisons" itself).
    /// - [`Error::LocalIllegalArgument`] when moving to an error state without
    ///   an error, mirroring Java's `IllegalArgumentException` (Java 1133).
    fn transition_to(&mut self, target: State, error: Option<Error>, caller: Caller) -> Result<(), Error> {
        if !target.is_transition_valid(self.current_state) {
            let id_string = match &self.transactional_id {
                Some(id) => format!("TransactionalId {id}: "),
                None => String::new(),
            };
            let message = format!(
                "{id_string}Invalid transition attempted from state {} to state {target}",
                self.current_state
            );

            let error = Error::local_illegal_state(message);
            if caller.should_poison_state_on_invalid_transition() {
                self.current_state = State::FatalError;
                self.last_error = Some(error.clone());
            }
            return Err(error);
        } else if target == State::FatalError || target == State::AbortableError {
            match error {
                None => {
                    return Err(Error::local_illegal_argument(format!(
                        "Cannot transition to {target} with a null error"
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
    /// Translated from `throwIfPendingState(TransactionOperation)` (Java 1267).
    ///
    /// Takes `&mut self` because Java clears `pendingTransition` here: an
    /// *acknowledged* result means the previous operation is genuinely finished,
    /// so the slot is released and the new operation proceeds. An unacknowledged
    /// one means the caller's `await` timed out and must be retried — the *same*
    /// operation, not a different one — so anything else is rejected. That
    /// `isAcked()` key, rather than `isCompleted()`, is what
    /// `.claude/rules/producer-transactions.md` §5 exists to protect: a completed
    /// but never-awaited `commitTransaction` must still be retryable.
    fn return_error_if_pending_state(&mut self, operation: TransactionOperation) -> Result<(), Error> {
        if let Some(pending) = self.pending_transition.as_ref() {
            if pending.result.is_acked() {
                self.pending_transition = None;
            } else {
                return Err(Error::local_illegal_state(format!(
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
    ///      [`Error::LocalIllegalState`]; the pending operation stays retryable.
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
    ) -> Result<Arc<TransactionalRequestResult>, Error>
    where
        F: FnOnce(&mut Self) -> Result<Arc<TransactionalRequestResult>, Error>,
    {
        self.ensure_transactional()?;

        if let Some(pending) = self.pending_transition.as_ref() {
            if pending.result.is_acked() {
                self.pending_transition = None;
            } else if next_state != pending.state {
                return Err(Error::local_illegal_state(format!(
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
    fn ensure_transactional(&self) -> Result<(), Error> {
        if !self.is_transactional() {
            return Err(Error::local_illegal_state(
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
    /// [`Error`] has no cause chain, and Java's `getMessage()` does not
    /// include the cause either, so the message text is reproduced exactly and
    /// the cause stays reachable through [`Self::last_error`].
    fn maybe_fail_with_error(&self) -> Result<(), Error> {
        if !self.has_error() {
            return Ok(());
        }

        // Java interpolates a null transactionalId as the text "null".
        let transactional_id = self.transactional_id.as_deref().unwrap_or("null");
        let producer_id_and_epoch = self.producer_id_and_epoch;

        let error = match &self.last_error {
            // for ProducerFencedException, do not wrap it as a KafkaException
            // but create a new instance without the call trace since it was not thrown because of the current call
            Some(error) if error.error() == Errors::ProducerFenced => Error::with_message(
                Errors::ProducerFenced,
                format!(
                    "Producer with transactionalId '{transactional_id}' and {producer_id_and_epoch} has been \
                         fenced by another producer with the same transactionalId"
                ),
            ),
            Some(error) if error.error() == Errors::InvalidProducerEpoch => Error::with_message(
                Errors::InvalidProducerEpoch,
                format!(
                    "Producer with transactionalId '{transactional_id}' and {producer_id_and_epoch} attempted to \
                         produce with an old epoch"
                ),
            ),
            // Java: `new IllegalStateException(msg, lastError)` — the cause is
            // carried, so the caller can see which transition poisoned the manager.
            Some(cause @ Error::LocalIllegalState(_)) => Error::LocalIllegalState(LocalIllegalStateError::with_source(
                format!(
                    "Producer with transactionalId '{transactional_id}' and {producer_id_and_epoch} cannot execute \
                     transactional method because of previous invalid state transition attempt"
                ),
                cause.clone(),
            )),
            // Java: new KafkaException("Cannot execute transactional method because we are in an error state",
            // lastError). A bare KafkaException carries no wire code, which this
            // crate spells as `Errors::UnknownServerError` (cf.
            // `record_accumulator.rs:1095`). The cause is what lets the application
            // tell a fatal condition from an abortable one on this path — the
            // message and code are identical for both.
            _ => {
                const MESSAGE: &str = "Cannot execute transactional method because we are in an error state";
                match &self.last_error {
                    Some(cause) => Error::KafkaError(KafkaError::with_message_source(
                        Errors::UnknownServerError,
                        MESSAGE,
                        cause.clone(),
                    )),
                    // `has_error()` is state-driven, so a set state with no recorded
                    // error is reachable; Java would pass a null cause here.
                    None => Error::kafka_message(MESSAGE),
                }
            },
        };
        // The error is returned as-is regardless of which error state we are in.
        // Java's `maybeFailWithError` distinguishes the two states only through the
        // exception *type* it throws (`ProducerFencedException`,
        // `InvalidProducerEpochException`, `IllegalStateException`, else
        // `KafkaException`) — built above; the state itself stays queryable through
        // `has_fatal_error()` / `has_abortable_error()`.
        Err(error)
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
    pub(crate) fn maybe_transition_to_error_state(&mut self, error: &Error, caller: Caller) -> Result<(), Error> {
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
            // `RetriableException`, so both tests are needed. Java `:778` chains the
            // original as the new error's cause
            // (`new TransactionAbortableException(msg, exception)`), and the chain is
            // observable: `transition_to_abortable_error` stores only the rewritten
            // error as `last_error`, so this is the value `maybe_fail_with_error`
            // hands the application, and its `source()` must be the original.
            let error = if error.is_retriable_error() || error.error() == Errors::InvalidTxnState {
                Error::TransactionAbortable(TransactionAbortableError::with_source(
                    "Transaction Request was aborted after exhausting retries.",
                    error.clone(),
                ))
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
    /// Phase 5a completed the transactional arm; since Phase 5b filled
    /// `partitions_in_transaction` — through `AddPartitionsToTxnHandler`, or directly
    /// under Transaction V2 — the set lookup discriminates rather than always
    /// refusing.
    pub(crate) fn is_send_to_partition_allowed(&self, topic_partition: &TopicPartition) -> bool {
        if self.has_fatal_error() {
            return false;
        }
        !self.is_transactional() || self.partitions_in_transaction.contains(topic_partition)
    }

    /// Whether any consumer-group offset is still awaiting a `TxnOffsetCommit`.
    ///
    /// Corresponds to `hasPendingOffsetCommits()` (Java 1001), which Java marks
    /// "visible for testing".
    pub(crate) fn has_pending_offset_commits(&self) -> bool {
        !self.pending_txn_offset_commits.is_empty()
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

    /// Whether a transactional producer has a producer id and no transaction in
    /// progress.
    ///
    /// Corresponds to `isReady()` (Java 1085). Java has no caller for it in either
    /// the client or its tests; translated because it is part of the class
    /// (`definition-of-done.md` §2).
    pub(crate) fn is_ready(&self) -> bool {
        self.is_transactional() && self.current_state == State::Ready
    }

    /// Whether a transactional producer's `InitProducerId` is still outstanding.
    ///
    /// Corresponds to `isInitializing()` (Java 1090). Java has no caller for it in
    /// either the client or its tests; translated because it is part of the class
    /// (`definition-of-done.md` §2).
    pub(crate) fn is_initializing(&self) -> bool {
        self.is_transactional() && self.current_state == State::Initializing
    }

    /// Check if the transaction is in the prepared state.
    ///
    /// Corresponds to `isPrepared()` (Java 1099). [`State::PreparedTransaction`] is
    /// entered by [`Self::prepare_transaction`] (Java 342) and by
    /// `InitProducerIdHandler`'s `keepPreparedTxn` arm (1504), both KIP-939.
    pub(crate) fn is_prepared(&self) -> bool {
        self.current_state == State::PreparedTransaction
    }

    /// Returns a `ProducerIdAndEpoch` containing the producer ID and epoch of the
    /// ongoing transaction. This is used when preparing a transaction for a
    /// two-phase commit.
    ///
    /// Corresponds to `preparedTransactionState()` (Java 1976). The value an
    /// external transaction coordinator uses to commit or abort a prepared
    /// transaction on the producer's behalf (KIP-939).
    pub(crate) fn prepared_transaction_state(&self) -> ProducerIdAndEpoch {
        self.prepared_txn_state
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
    ) -> Result<(), Error> {
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
    fn reset_idempotent_producer_id(&mut self, caller: Caller) -> Result<(), Error> {
        if self.is_transactional() {
            return Err(Error::local_illegal_state(
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
    ) -> Result<(), Error> {
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
    ) -> Result<(), Error> {
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
    ) -> Result<(), Error> {
        self.txn_partition_map.get_mut(topic_partition)?.increment_sequence(increment);
        Ok(())
    }

    /// Records `batch` as in flight.
    ///
    /// Corresponds to `addInFlightBatch(ProducerBatch)` (Java 697).
    pub(crate) fn add_in_flight_batch(&mut self, batch: &ProducerBatch) -> Result<(), Error> {
        if !batch.has_sequence() {
            return Err(Error::local_illegal_state(format!(
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
    pub(crate) fn first_in_flight_sequence(&mut self, topic_partition: &TopicPartition) -> Result<i32, Error> {
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
    ) -> Result<Option<InFlightBatchKey>, Error> {
        self.txn_partition_map.next_batch_by_sequence(topic_partition)
    }

    /// Removes `batch` from the in-flight set.
    ///
    /// Corresponds to `removeInFlightBatch(ProducerBatch)` (Java 721).
    pub(crate) fn remove_in_flight_batch(&mut self, batch: &ProducerBatch) -> Result<(), Error> {
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
    fn update_last_acked_offset(&mut self, response: &PartitionResponse, batch: &ProducerBatch) -> Result<(), Error> {
        if response.base_offset == ProduceResponse::INVALID_OFFSET {
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
    ) -> Result<(), Error> {
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
    /// relation is recovered by [`Error::is_out_of_order_sequence_error`], the
    /// single predicate encoding that `extends` edge (CLAUDE.md §10.4) — see
    /// `.claude/rules/producer-transactions.md` §9.
    pub(crate) fn handle_failed_batch(
        &mut self,
        batch: &ProducerBatch,
        error: &Error,
        adjust_sequence_numbers: bool,
        batches: &mut [&mut ProducerBatch],
        caller: Caller,
    ) -> Result<(), Error> {
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

        if error.is_out_of_order_sequence_error() && !self.is_transactional() {
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
    pub(crate) fn maybe_resolve_sequences(&mut self, caller: Caller) -> Result<(), Error> {
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
                // Java's two `new KafkaException(..)` instances (Java 866, 868) are
                // BARE `KafkaException`s: `is_kafka_error()` is `true` and
                // `is_api_error()` is `false`. `Error::kafka` is the only spelling that
                // gives that — `Error::with_message(Errors::UnknownServerError, ..)`
                // resolves the code to `UnknownServerException`, an `ApiException`.
                const UNACKED_MESSAGES_ERR: &str = "The client hasn't received acknowledgment for some previously \
                                                    sent messages and can no longer retry them. ";
                let abortable_error = Error::kafka_message(format!(
                    "{UNACKED_MESSAGES_ERR}It is safe to abort the transaction and continue."
                ));
                let fatal_error = Error::kafka_message(format!("{UNACKED_MESSAGES_ERR}It isn't safe to continue."));
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
    /// # Errors
    ///
    /// Java's method returns `TxnRequestHandler` and can only *throw* through
    /// `resetTransactionState`'s `transitionTo`, on the "EndTxn for a transaction
    /// that never started" path (Java 923). `Result` carries that, so the outer
    /// `Option` keeps meaning "nothing to send" rather than doubling as an error
    /// channel.
    pub(crate) fn next_request(
        &mut self,
        pending_requests: &mut PendingRequests,
        has_incomplete_batches: bool,
    ) -> Result<Option<TxnRequestHandler>, Error> {
        if !self.new_partitions_in_transaction.is_empty() {
            let handler = self.add_partitions_to_transaction_handler();
            self.enqueue_request(pending_requests, handler);
        }

        let Some(next_request_handler) = pending_requests.peek() else {
            return Ok(None);
        };

        // Do not send the EndTxn until all batches have been flushed
        if next_request_handler.is_end_txn() && has_incomplete_batches {
            return Ok(None);
        }

        let Some(next_request_handler) = pending_requests.poll() else {
            return Ok(None);
        };
        if self.maybe_terminate_request_with_error(&next_request_handler) {
            kafka_trace!(
                self.log_context,
                "Not sending transactional request {:?} because we are in an error state",
                next_request_handler
            );
            return Ok(None);
        }

        // Java 913-925: an EndTxn for a transaction nothing was ever added to needs
        // no round trip. Java rebinds `nextRequestHandler` from a second
        // `pendingRequests.poll()`, which may be null — hence the `Option` below.
        let mut next_request_handler = Some(next_request_handler);
        if let Some(handler) = next_request_handler.as_ref().filter(|handler| handler.is_end_txn())
            && !self.transaction_started
        {
            handler.result.done();
            if self.current_state != State::FatalError {
                if self.is_transaction_v2_enabled {
                    kafka_debug!(
                        self.log_context,
                        "Not sending EndTxn for completed transaction since no send or sendOffsetsToTransaction were \
                         triggered"
                    );
                } else {
                    kafka_debug!(
                        self.log_context,
                        "Not sending EndTxn for completed transaction since no partitions or offsets were \
                         successfully added"
                    );
                }
                // Java calls this from the Sender thread here (`Sender.java:472` →
                // `nextRequest`), which rules §1's "called only from the Sender task"
                // clause is why `reset_transaction_state` hardcodes `Caller::Sender`.
                self.reset_transaction_state()?;
            }
            next_request_handler = pending_requests.poll();
        }

        if let Some(handler) = next_request_handler.as_ref() {
            kafka_trace!(self.log_context, "Request {:?} dequeued for sending", handler);
        }
        Ok(next_request_handler)
    }

    // `hasPendingRequests()` (Java 1005) is `Sender::has_pending_requests`: its
    // body reads only the Sender-confined queue (rules §2).

    /// Fails `handler` when the manager is in an error state.
    ///
    /// Translated from `maybeTerminateRequestWithError(TxnRequestHandler)`
    /// (Java 1174), including the escape hatch Phase 3 had to omit for want of a
    /// `FindCoordinatorHandler`: a coordinator lookup is still allowed to go out
    /// while the producer is heading for an abort, because the abort itself needs
    /// the coordinator (`testFindCoordinatorAllowedInAbortableErrorState`).
    fn maybe_terminate_request_with_error(&self, handler: &TxnRequestHandler) -> bool {
        if self.has_error() {
            if self.has_abortable_error() && handler.is_find_coordinator() {
                // No harm letting the FindCoordinator request go through if we're expecting to abort
                return false;
            }
            if let Some(last_error) = &self.last_error {
                // The failed result is what a blocked commit_transaction() /
                // send_offsets_to_transaction() call surfaces. Java passes
                // `lastError` through unchanged (`requestHandler.fail(lastError)`);
                // whether abort is the way out is read from the state
                // (`has_abortable_error()`), not from the error.
                handler.fail(last_error.clone());
            }
            return true;
        }
        false
    }

    /// Forgets the coordinator `handler` needs and enqueues a `FindCoordinator`
    /// request to rediscover it.
    ///
    /// Translated from `lookupCoordinator(TxnRequestHandler)` (Java 969), which
    /// forwards the handler's own coordinator type and key to the two-argument
    /// overload.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] when `handler` needs no coordinator. Java's
    /// `switch (null)` would raise a `NullPointerException` there; both callers
    /// guard on `needsCoordinator()` (`Sender.java:521`,
    /// `TransactionManager.java:1413`), so it is unreachable in either language.
    pub(crate) fn lookup_coordinator_for(
        &self,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
        handler: &TxnRequestHandler,
    ) -> Result<(), Error> {
        let Some(coordinator_type) = self.coordinator_type(handler) else {
            return Err(Error::local_illegal_state(
                "Invalid coordinator type: null — the request needs no coordinator",
            ));
        };
        let coordinator_key = self.coordinator_key(handler).unwrap_or_default().to_string();
        self.lookup_coordinator(coordinators, pending_requests, coordinator_type, &coordinator_key)
    }

    /// Forgets the coordinator of the given type and enqueues a `FindCoordinator`
    /// request to rediscover it.
    ///
    /// Translated from `lookupCoordinator(CoordinatorType, String)` (Java 1191).
    ///
    /// Takes both the coordinator record and the request queue from their owner,
    /// because Java touches both from this unsynchronized method (rules §2) — see
    /// [`CoordinatorNodes`] and [`PendingRequests`].
    pub(crate) fn lookup_coordinator(
        &self,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
        coordinator_type: CoordinatorType,
        coordinator_key: &str,
    ) -> Result<(), Error> {
        coordinators.clear(coordinator_type)?;

        let mut data = FindCoordinatorRequestData::new();
        data.set_key_type(coordinator_type.id()).set_key(coordinator_key.to_string());
        let handler = TxnRequestHandler::new(
            "FindCoordinator",
            self.retry_backoff_ms,
            TxnRequestHandlerKind::FindCoordinator { builder: FindCoordinatorRequestBuilder::new(data) },
        );
        self.enqueue_request(pending_requests, handler);
        Ok(())
    }

    /// Moves every locally-registered partition into the pending set and builds the
    /// `AddPartitionsToTxn` that announces them to the coordinator.
    ///
    /// Translated from `addPartitionsToTransactionHandler()` (Java 1210).
    ///
    /// Java takes `new ArrayList<>(pendingPartitionsInTransaction)`, whose order a
    /// `HashSet` leaves unspecified. This sorts, so the encoding is deterministic
    /// (rules §10) — the wire builder's own `build_txn_topic_collection` already
    /// sorts by topic name, and this settles the partition order within a topic,
    /// which that grouping preserves.
    fn add_partitions_to_transaction_handler(&mut self) -> TxnRequestHandler {
        self.pending_partitions_in_transaction
            .extend(self.new_partitions_in_transaction.iter().cloned());
        self.new_partitions_in_transaction.clear();

        let mut partitions: Vec<TopicPartition> = self.pending_partitions_in_transaction.iter().cloned().collect();
        // `TopicPartition` is deliberately not `Ord` — Java's is not `Comparable`
        // either — so the key is spelled out, as `cluster.rs:349` does.
        partitions.sort_by_key(|partition| (partition.topic_arc().clone(), partition.partition()));

        let builder = AddPartitionsToTxnRequestBuilder::for_client(
            // `ensureTransactional()` has already run at every call site, so the id
            // is present; Java would interpolate a null as the text "null".
            self.transactional_id.as_deref().unwrap_or_default(),
            self.producer_id_and_epoch.producer_id,
            self.producer_id_and_epoch.epoch,
            &partitions,
        );
        TxnRequestHandler::new(
            "AddPartitionsToTxn",
            self.retry_backoff_ms,
            TxnRequestHandlerKind::AddPartitionsToTxn { builder, retry_backoff_ms: self.retry_backoff_ms },
        )
    }

    /// Records whether the transaction coordinator's `InitProducerId` version
    /// supports a client-triggered epoch bump.
    ///
    /// Translated from `handleCoordinatorReady()` (Java 1103), called from
    /// `Sender.awaitNodeReady` once the transaction coordinator's connection is
    /// ready (`Sender.java:569`). Its comment there: "this allows us to bump
    /// transactional epochs even if the coordinator is temporarily unavailable at
    /// the time when the abortable error is handled".
    ///
    /// Takes the coordinator record from the `Sender` (rules §2) but writes
    /// `coordinator_supports_bumping_epoch` here, because it also reads
    /// `api_versions` — a manager field. See that field's docs for why it is not
    /// Sender-owned.
    pub(crate) fn handle_coordinator_ready(&mut self, coordinators: &CoordinatorNodes) {
        // `coordinator(TRANSACTION)` cannot error, so the node is read directly.
        let node_api_versions = coordinators
            .transaction
            .as_ref()
            .and_then(|node| self.api_versions.get(node.id_string()));
        let init_producer_id_version = node_api_versions
            .as_ref()
            .and_then(|versions| versions.api_version(&ApiKeys::INIT_PRODUCER_ID));
        self.coordinator_supports_bumping_epoch =
            init_producer_id_version.is_some_and(|version| version.max_version >= 3);
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
    pub(crate) fn fatal_error(&mut self, handler: &TxnRequestHandler, error: Error) -> Result<(), Error> {
        // The error is failed onto the handler unchanged. Java signals fatality
        // through the state machine plus the exception *type* that
        // `maybeFailWithError` throws — never a flag on the error itself.
        handler.result.fail(error.clone());
        // Every caller is on the response path, which runs on the Sender task.
        self.transition_to_fatal_error(error, Caller::Sender)
    }

    /// Fails `handler` and moves the manager to [`State::AbortableError`].
    ///
    /// Corresponds to `TxnRequestHandler.abortableError(RuntimeException)`
    /// (Java 1362).
    fn abortable_error(&mut self, handler: &TxnRequestHandler, error: Error) -> Result<(), Error> {
        handler.result.fail(error.clone());
        // Every caller is on the response path, which runs on the Sender task.
        self.transition_to_abortable_error(error, Caller::Sender)
    }

    /// Fails `handler` and moves the manager to [`State::AbortableError`] when the
    /// coordinator can recover from one, or to [`State::FatalError`] when it cannot.
    ///
    /// Corresponds to `TxnRequestHandler.abortableErrorIfPossible(RuntimeException)`
    /// (Java 1379).
    ///
    /// Java's javadoc there: an abortable error can be handled effectively if epoch
    /// bumping is supported — either because Transaction V2 bumps automatically at
    /// the end of every transaction, or because the client can trigger a bump. If
    /// epoch bumping is not supported the system cannot recover and the error must
    /// be treated as fatal.
    ///
    /// Distinct from [`Self::transition_to_abortable_error_or_fatal_error`]
    /// (Java 557), which takes *two* exceptions and does not touch a handler
    /// result: that one serves `maybeResolveSequences`, this one the response path.
    fn abortable_error_if_possible(&mut self, handler: &TxnRequestHandler, error: Error) -> Result<(), Error> {
        if self.can_handle_abortable_error() {
            if self.need_to_trigger_epoch_bump_from_client() {
                self.client_side_epoch_bump_required = true;
            }
            self.abortable_error(handler, error)
        } else {
            self.fatal_error(handler, error)
        }
    }

    /// The coordinator `handler` must be routed to, or `None` when it can go to
    /// any broker.
    ///
    /// Corresponds to `coordinatorType()` (Java 1434), whose base implementation
    /// returns `TRANSACTION`. `InitProducerIdHandler` (Java 1482) overrides it to
    /// return `null` for a non-transactional producer — which is why the whole
    /// `FindCoordinator` subsystem was out of scope for the idempotence slice —
    /// and `FindCoordinatorHandler` (Java 1671) returns `null` unconditionally,
    /// since a coordinator lookup goes to any broker.
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
            TxnRequestHandlerKind::FindCoordinator { .. } => None,
            // The base implementation (Java 1434).
            TxnRequestHandlerKind::AddPartitionsToTxn { .. }
            | TxnRequestHandlerKind::EndTxn { .. }
            | TxnRequestHandlerKind::AddOffsetsToTxn { .. } => Some(CoordinatorType::Transaction),
            // Java 1894: the offsets go to the *group* coordinator.
            TxnRequestHandlerKind::TxnOffsetCommit { .. } => Some(CoordinatorType::Group),
        }
    }

    /// The key identifying the coordinator `handler` must be routed to.
    ///
    /// Corresponds to `coordinatorKey()` (Java 1438). The base implementation
    /// returns the transactional id; `FindCoordinatorHandler` (Java 1676) returns
    /// `null`, and only `TxnOffsetCommitHandler` (Java 1899) returns something
    /// else — its consumer group id.
    ///
    /// The explicit lifetime is needed because `TxnOffsetCommitHandler`'s override
    /// returns the group id off the *handler*, while the base implementation returns
    /// the manager's transactional id.
    pub(crate) fn coordinator_key<'a>(&'a self, handler: &'a TxnRequestHandler) -> Option<&'a str> {
        match handler.kind {
            // The base implementation (Java 1438).
            TxnRequestHandlerKind::InitProducerId { .. }
            | TxnRequestHandlerKind::AddPartitionsToTxn { .. }
            | TxnRequestHandlerKind::EndTxn { .. }
            | TxnRequestHandlerKind::AddOffsetsToTxn { .. } => self.transactional_id(),
            TxnRequestHandlerKind::FindCoordinator { .. } => None,
            // Java 1899.
            TxnRequestHandlerKind::TxnOffsetCommit { ref builder } => Some(&builder.data().group_id),
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
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
    ) -> Result<(), Error> {
        // Where Java dispatches virtually on the handler subclass, this matches on
        // the kind (PLAN §10.5 deviation 2).
        match &handler.kind {
            TxnRequestHandlerKind::InitProducerId { .. } => {
                self.handle_init_producer_id_response(handler, response, coordinators, pending_requests)
            },
            TxnRequestHandlerKind::FindCoordinator { .. } => {
                self.handle_find_coordinator_response(handler, response, coordinators, pending_requests)
            },
            TxnRequestHandlerKind::AddPartitionsToTxn { .. } => {
                self.handle_add_partitions_to_txn_response(handler, response, coordinators, pending_requests)
            },
            TxnRequestHandlerKind::EndTxn { .. } => {
                self.handle_end_txn_response(handler, response, coordinators, pending_requests)
            },
            TxnRequestHandlerKind::AddOffsetsToTxn { .. } => {
                self.handle_add_offsets_to_txn_response(handler, response, coordinators, pending_requests)
            },
            TxnRequestHandlerKind::TxnOffsetCommit { .. } => {
                self.handle_txn_offset_commit_response(handler, response, coordinators, pending_requests)
            },
        }
    }

    /// Handles a `FindCoordinator` response.
    ///
    /// Translated from `FindCoordinatorHandler.handleResponse(AbstractResponse)`
    /// (Java 1680-1720).
    ///
    /// # Java's `coordinators.size() != 1` branch does not return
    ///
    /// Java calls `fatalError(..)` and then **falls through** to
    /// `coordinators.get(0)` (Java 1685-1689). `fatalError` does not rethrow — it
    /// fails the result and transitions, and `FATAL_ERROR` is an unconditionally
    /// valid target — so for a response carrying two coordinators Java records the
    /// fatal error *and* then goes on to install one of them and call
    /// `result.done()`. That is preserved. For an **empty** list Java would raise
    /// `IndexOutOfBoundsException`; Rust cannot index and must not panic
    /// (CLAUDE.md §10.1), so the fatal error is returned instead — an error either
    /// way, with the same state left behind.
    fn handle_find_coordinator_response(
        &mut self,
        handler: TxnRequestHandler,
        response: &ConcreteResponse,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
    ) -> Result<(), Error> {
        let TxnRequestHandlerKind::FindCoordinator { builder } = &handler.kind else {
            return Err(Error::local_illegal_state(
                "handle_find_coordinator_response called for another request kind",
            ));
        };
        let ConcreteResponse::FindCoordinator(find_coordinator_response) = response else {
            // Java casts unconditionally; a mismatch would be a
            // ClassCastException. Surfaced as an error per CLAUDE.md §10.2.
            return Err(Error::local_illegal_state(format!(
                "Expected a FindCoordinator response for a FindCoordinator request, got {response}"
            )));
        };

        let coordinator_type = CoordinatorType::for_id(builder.data().key_type)
            .map_err(|error| Error::local_illegal_state(error.to_string()))?;
        let request_key = builder.data().key.clone();
        let response_coordinators = find_coordinator_response.coordinators();

        let mut size_error = None;
        if response_coordinators.len() != 1 {
            kafka_error!(
                self.log_context,
                "Group coordinator lookup failed: Invalid response containing more than a single coordinator"
            );
            let error = Error::local_illegal_state(
                "Group coordinator lookup failed: Invalid response containing more than a single coordinator",
            );
            self.fatal_error(&handler, error.clone())?;
            size_error = Some(error);
        }
        let Some(coordinator_data) = response_coordinators.first() else {
            // See the method docs: Java raises IndexOutOfBoundsException here.
            return Err(size_error.expect("an empty list cannot have length 1"));
        };

        // For older versions without batching, obtain key from request data since it is not included in response.
        // Java tests `coordinatorData.key() == null`; the Rust message spec defaults
        // a nullable string without an explicit `"default": "null"` to the empty
        // string, and `FindCoordinatorResponse::coordinators` synthesises exactly
        // that for a v<=3 response, so the empty key is the null case.
        let key = if coordinator_data.key.is_empty() {
            request_key
        } else {
            coordinator_data.key.clone()
        };
        let error = Errors::for_code(coordinator_data.error_code);

        if error == Errors::None {
            let node = Node::new(coordinator_data.node_id, coordinator_data.host.clone(), coordinator_data.port);
            if let Err(error) = coordinators.set(coordinator_type, node.clone()) {
                kafka_error!(
                    self.log_context,
                    "Group coordinator lookup failed: Unexpected coordinator type in response"
                );
                return self.fatal_error(&handler, error);
            }
            handler.result.done();
            kafka_info!(
                self.log_context,
                "Discovered {} coordinator {}",
                coordinator_type.name().to_lowercase(),
                node
            );
            return Ok(());
        }
        if error.error().is_some_and(|e| e.is_retriable_error()) {
            self.retry(pending_requests, handler);
            return Ok(());
        }
        if error == Errors::TransactionalIdAuthorizationFailed {
            return self.fatal_error(&handler, Error::new(error));
        }
        if error == Errors::GroupAuthorizationFailed {
            // Java: GroupAuthorizationException.forGroupId(key).
            let error = Error::with_message(
                Errors::GroupAuthorizationFailed,
                format!("Not authorized to access group: {key}"),
            );
            return self.abortable_error(&handler, error);
        }
        if error == Errors::TransactionAbortable {
            return self.abortable_error(&handler, Error::new(error));
        }
        // Java interpolates a null errorMessage as the text "null".
        let error_message = coordinator_data.error_message.as_deref().unwrap_or("null");
        // Java 1716: `new KafkaException(String.format(..))` — a bare `KafkaException`.
        self.fatal_error(
            &handler,
            Error::kafka_message(format!(
                "Could not find a coordinator with type {} with key {key} due to unexpected error: {error_message}",
                coordinator_type.name()
            )),
        )
    }

    /// Handles an `InitProducerId` response.
    ///
    /// Translated from `InitProducerIdHandler.handleResponse(AbstractResponse)`
    /// (Java 1491).
    fn handle_init_producer_id_response(
        &mut self,
        handler: TxnRequestHandler,
        response: &ConcreteResponse,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
    ) -> Result<(), Error> {
        let TxnRequestHandlerKind::InitProducerId { builder, is_epoch_bump } = &handler.kind else {
            return Err(Error::local_illegal_state(
                "handle_init_producer_id_response called for another request kind",
            ));
        };
        let ConcreteResponse::InitProducerId(init_producer_id_response) = response else {
            // Java casts unconditionally; a mismatch would be a
            // ClassCastException. Surfaced as an error per CLAUDE.md §10.2.
            return Err(Error::local_illegal_state(format!(
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
                // Java 1504-1510. Unreachable through this crate's own request
                // builders, and for **Java's own reason** rather than a translation
                // gap: nothing in Apache Kafka 4.2's `clients/src` calls
                // `setKeepPreparedTxn`, so `builder.data.keepPreparedTxn()` is
                // always false on the `initializeTransactions` path — see
                // [`Self::initialize_transactions_internal`]. Translated in full
                // anyway, because it is the KIP-939 arm a broker-driven
                // recovery would take once a caller does set the flag, and because
                // leaving it out would make `PREPARED_TRANSACTION` reachable from
                // only one of its two Java sources.
                self.transition_to(State::PreparedTransaction, None, Caller::Sender)?;
                // Update the preparedTxnState with the ongoing pid and epoch from the response.
                // This will be used to complete the transaction later.
                self.prepared_txn_state = ProducerIdAndEpoch::new(
                    init_producer_id_response.data().ongoing_txn_producer_id,
                    init_producer_id_response.data().ongoing_txn_producer_epoch,
                );
            } else {
                self.transition_to(State::Ready, None, Caller::Sender)?;
            }
            self.last_error = None;
            if is_epoch_bump {
                self.reset_sequence_numbers();
            }
            handler.result.done();
            return Ok(());
        }
        if error == Errors::NotCoordinator || error == Errors::CoordinatorNotAvailable {
            // Java 1520-1521.
            let transactional_id = self.transactional_id().unwrap_or_default().to_string();
            self.lookup_coordinator(coordinators, pending_requests, CoordinatorType::Transaction, &transactional_id)?;
            self.retry(pending_requests, handler);
            return Ok(());
        }
        if error.error().is_some_and(|e| e.is_retriable_error()) {
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
            let error = Error::new(error);
            self.last_error = Some(error.clone());
            return self.abortable_error(&handler, error);
        }
        if error == Errors::InvalidProducerEpoch || error == Errors::ProducerFenced {
            // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
            // just treat it the same as PRODUCE_FENCED.
            return self.fatal_error(&handler, Error::new(Errors::ProducerFenced));
        }
        if error == Errors::TransactionAbortable {
            let error = Error::new(error);
            return self.abortable_error(&handler, error);
        }
        // Java 1536: `new KafkaException("Unexpected error in InitProducerIdResponse; " + ..)`.
        self.fatal_error(
            &handler,
            Error::kafka_message(format!("Unexpected error in InitProducerIdResponse; {}", error.message())),
        )
    }

    /// Handles an `AddPartitionsToTxn` response.
    ///
    /// Translated from
    /// `AddPartitionsToTxnHandler.handleResponse(AbstractResponse)` (Java 1559).
    ///
    /// # Two orderings a reviewer should check against the Java
    ///
    /// The per-partition loop's early `return`s (Java 1580, 1584, 1588, 1593, 1598,
    /// 1605, 1608) abandon the whole response, leaving
    /// `pendingPartitionsInTransaction` **untouched** — only the fall-through path
    /// at Java 1620 clears it. And the `if / else if` chain's order is load-bearing:
    /// `CONCURRENT_TRANSACTIONS` is itself a retriable code, so its backoff
    /// override (Java 1585) only happens because that arm precedes the generic
    /// retriable arm.
    ///
    /// # A response with no v3-and-below results
    ///
    /// Java reads `errors().get(AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID)` (Java 1561) and then iterates
    /// it unchecked. `errors()` omits that key entirely when the response carries no
    /// v3-and-below topic results, so a malformed or v4+-shaped response makes Java
    /// raise a `NullPointerException` inside `NetworkClient.poll`. Rust must not
    /// panic (CLAUDE.md §10.1), so the absent key becomes an error — the same
    /// treatment `handle_find_coordinator_response` gives Java's
    /// `IndexOutOfBoundsException` (PLAN §10.7 deviation 7). Unreachable in
    /// practice: the request is only built from a non-empty pending set.
    fn handle_add_partitions_to_txn_response(
        &mut self,
        mut handler: TxnRequestHandler,
        response: &ConcreteResponse,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
    ) -> Result<(), Error> {
        let ConcreteResponse::AddPartitionsToTxn(add_partitions_to_txn_response) = response else {
            // Java casts unconditionally; a mismatch would be a
            // ClassCastException. Surfaced as an error per CLAUDE.md §10.2.
            return Err(Error::local_illegal_state(format!(
                "Expected an AddPartitionsToTxn response for an AddPartitionsToTxn request, got {response}"
            )));
        };
        let Some(errors) = add_partitions_to_txn_response
            .errors()
            .remove(AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID)
        else {
            // See the method docs: Java raises NullPointerException here.
            return Err(Error::local_illegal_state(
                "AddPartitionsToTxn response carries no results for this client's transaction",
            ));
        };

        let mut has_partition_errors = false;
        let mut unauthorized_topics = HashSet::new();
        self.reset_add_partitions_retry_backoff_ms(&mut handler);

        // Java iterates a `HashMap`, so its order is unspecified. Sorted here for a
        // reproducible outcome: the arms that `return` make the loop
        // order-sensitive, and Java's own `KafkaException` message below
        // interpolates the map (rules §10's reasoning applied to a log message).
        for (topic_partition, error) in sorted_partition_errors(&errors) {
            if error == Errors::None {
                continue;
            } else if error == Errors::CoordinatorNotAvailable || error == Errors::NotCoordinator {
                let transactional_id = self.transactional_id().unwrap_or_default().to_string();
                self.lookup_coordinator(
                    coordinators,
                    pending_requests,
                    CoordinatorType::Transaction,
                    &transactional_id,
                )?;
                self.retry(pending_requests, handler);
                return Ok(());
            } else if error == Errors::ConcurrentTransactions {
                self.maybe_override_retry_backoff_ms(&mut handler);
                self.retry(pending_requests, handler);
                return Ok(());
            } else if error.error().is_some_and(|e| e.is_retriable_error()) {
                self.retry(pending_requests, handler);
                return Ok(());
            } else if error == Errors::InvalidProducerEpoch || error == Errors::ProducerFenced {
                // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                // just treat it the same as PRODUCE_FENCED.
                return self.fatal_error(&handler, Error::new(Errors::ProducerFenced));
            } else if error == Errors::TransactionalIdAuthorizationFailed
                || error == Errors::InvalidTxnState
                || error == Errors::InvalidProducerIdMapping
            {
                return self.fatal_error(&handler, Error::new(error));
            } else if error == Errors::TopicAuthorizationFailed {
                unauthorized_topics.insert(topic_partition.topic().to_string());
            } else if error == Errors::OperationNotAttempted {
                kafka_debug!(
                    self.log_context,
                    "Did not attempt to add partition {} to transaction because other partitions in the batch had \
                     errors.",
                    topic_partition
                );
                has_partition_errors = true;
            } else if error == Errors::UnknownProducerId {
                return self.abortable_error_if_possible(&handler, Error::new(error));
            } else if error == Errors::TransactionAbortable {
                return self.abortable_error(&handler, Error::new(error));
            } else {
                kafka_error!(
                    self.log_context,
                    "Could not add partition {} due to unexpected error {:?}",
                    topic_partition,
                    error
                );
                has_partition_errors = true;
            }
        }

        // Remove the partitions from the pending set regardless of the result. We use the presence
        // of partitions in the pending set to know when it is not safe to send batches. However, if
        // the partitions failed to be added and we enter an error state, we expect the batches to be
        // aborted anyway. In this case, we must be able to continue sending the batches which are in
        // retry for partitions that were successfully added.
        self.pending_partitions_in_transaction
            .retain(|partition| !errors.contains_key(partition));

        if !unauthorized_topics.is_empty() {
            return self.abortable_error(&handler, Error::topic_authorization(unauthorized_topics));
        }
        if has_partition_errors {
            // Java 1625: `new KafkaException("Could not add partitions to transaction
            // due to errors: " + errors)`, which interpolates a `HashMap`. A bare
            // `KafkaException` — `Error::kafka`, not a code-resolved
            // `UnknownServerError` — and the map is rendered in sorted order so the
            // message is reproducible.
            return self.abortable_error(
                &handler,
                Error::kafka_message(format!(
                    "Could not add partitions to transaction due to errors: {}",
                    format_partition_errors(&errors)
                )),
            );
        }

        kafka_debug!(
            self.log_context,
            "Successfully added partitions {:?} to transaction",
            errors.keys().map(ToString::to_string).collect::<Vec<_>>()
        );
        self.partitions_in_transaction.extend(errors.into_keys());
        self.transaction_started = true;
        handler.result.done();
        Ok(())
    }

    /// Handles an `EndTxn` response.
    ///
    /// Translated from `EndTxnHandler.handleResponse(AbstractResponse)` (Java 1747).
    ///
    /// # The `isAbort` arm must precede the plain `TRANSACTION_ABORTABLE` arm
    ///
    /// Both arms test the same wire code (Java 1783 and 1787). Java's comment on the
    /// first: when aborting a transaction we must convert `TRANSACTION_ABORTABLE`
    /// errors to a `KafkaException`, because if an abort operation itself encounters
    /// an abortable error, retrying the abort would create a cycle — so it is treated
    /// as fatal at the application layer to ensure the transaction can be cleanly
    /// terminated. Swapping the two arms would make an abort retry itself forever.
    fn handle_end_txn_response(
        &mut self,
        handler: TxnRequestHandler,
        response: &ConcreteResponse,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
    ) -> Result<(), Error> {
        let TxnRequestHandlerKind::EndTxn { builder } = &handler.kind else {
            return Err(Error::local_illegal_state(
                "handle_end_txn_response called for another request kind",
            ));
        };
        let ConcreteResponse::EndTxn(end_txn_response) = response else {
            // Java casts unconditionally; a mismatch would be a
            // ClassCastException. Surfaced as an error per CLAUDE.md §10.2.
            return Err(Error::local_illegal_state(format!(
                "Expected an EndTxn response for an EndTxn request, got {response}"
            )));
        };
        let is_abort = !builder.data().committed;
        let error = end_txn_response.error();

        if error == Errors::None {
            // For End Txn version 5+, the broker includes the producerId and producerEpoch in the EndTxnResponse.
            // For versions lower than 5, the producer Id and epoch are set to -1 by default.
            // When Transaction Version 2 is enabled, the end txn request 5+ is used,
            // it mandates bumping the epoch after every transaction.
            // If the epoch overflows, a new producerId is returned with epoch set to 0.
            // Note, we still may see EndTxn TV1 (< 5) responses when the producer has upgraded to TV2 due to the
            // upgrade occurring at the end of beginCompletingTransaction. The next transaction started should be TV2.
            //
            // Java spells the sentinel as the literal `-1`; it is
            // `RecordBatch.NO_PRODUCER_ID`, and the constant is used so the two stay
            // in step.
            if end_txn_response.data().producer_id != RecordBatch::NO_PRODUCER_ID {
                let producer_id_and_epoch = ProducerIdAndEpoch::new(
                    end_txn_response.data().producer_id,
                    end_txn_response.data().producer_epoch,
                );
                self.set_producer_id_and_epoch(producer_id_and_epoch);
                self.reset_sequence_numbers();
            }
            self.reset_transaction_state()?;
            handler.result.done();
            return Ok(());
        }
        if error == Errors::CoordinatorNotAvailable || error == Errors::NotCoordinator {
            let transactional_id = self.transactional_id().unwrap_or_default().to_string();
            self.lookup_coordinator(coordinators, pending_requests, CoordinatorType::Transaction, &transactional_id)?;
            self.retry(pending_requests, handler);
            return Ok(());
        }
        if error.error().is_some_and(|e| e.is_retriable_error()) {
            self.retry(pending_requests, handler);
            return Ok(());
        }
        if error == Errors::InvalidProducerEpoch || error == Errors::ProducerFenced {
            // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
            // just treat it the same as PRODUCE_FENCED.
            return self.fatal_error(&handler, Error::new(Errors::ProducerFenced));
        }
        if error == Errors::TransactionalIdAuthorizationFailed
            || error == Errors::InvalidTxnState
            || error == Errors::InvalidProducerIdMapping
        {
            return self.fatal_error(&handler, Error::new(error));
        }
        if error == Errors::UnknownProducerId {
            return self.abortable_error_if_possible(&handler, Error::new(error));
        }
        if is_abort && error == Errors::TransactionAbortable {
            // Java 1787: `new KafkaException("Failed to abort transaction",
            // error.exception())` — a bare `KafkaException` whose message is exactly
            // "Failed to abort transaction" and whose *cause* is the wire error.
            // Folding the cause into the message changed both the class and the text.
            return self.fatal_error(
                &handler,
                Error::kafka_message_source("Failed to abort transaction", Error::new(error)),
            );
        }
        if error == Errors::TransactionAbortable {
            return self.abortable_error(&handler, Error::new(error));
        }
        // Java 1791: `new KafkaException("Unhandled error in EndTxnResponse: " + ..)`.
        self.fatal_error(
            &handler,
            Error::kafka_message(format!("Unhandled error in EndTxnResponse: {}", error.message())),
        )
    }

    /// Handles an `AddOffsetsToTxn` response.
    ///
    /// Translated from
    /// `AddOffsetsToTxnHandler.handleResponse(AbstractResponse)` (Java 1820).
    ///
    /// The success arm does **not** complete the result: it enqueues a
    /// `TxnOffsetCommit` carrying the same handle (Java 1828), so the caller's await
    /// resolves only once the offsets are actually committed. Java's comment on that
    /// line says exactly this.
    fn handle_add_offsets_to_txn_response(
        &mut self,
        handler: TxnRequestHandler,
        response: &ConcreteResponse,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
    ) -> Result<(), Error> {
        let TxnRequestHandlerKind::AddOffsetsToTxn { builder, offsets, group_metadata } = &handler.kind else {
            return Err(Error::local_illegal_state(
                "handle_add_offsets_to_txn_response called for another request kind",
            ));
        };
        let ConcreteResponse::AddOffsetsToTxn(add_offsets_to_txn_response) = response else {
            // Java casts unconditionally; a mismatch would be a
            // ClassCastException. Surfaced as an error per CLAUDE.md §10.2.
            return Err(Error::local_illegal_state(format!(
                "Expected an AddOffsetsToTxn response for an AddOffsetsToTxn request, got {response}"
            )));
        };
        let group_id = builder.data().group_id.clone();
        let error = Errors::for_code(add_offsets_to_txn_response.data().error_code);

        if error == Errors::None {
            kafka_debug!(
                self.log_context,
                "Successfully added partition for consumer group {} to transaction",
                group_id
            );

            // note the result is not completed until the TxnOffsetCommit returns
            let offsets = offsets.clone();
            let group_metadata = group_metadata.clone();
            let result = Arc::clone(&handler.result);
            let commit_handler = self.txn_offset_commit_handler(Some(result), &offsets, &group_metadata);
            // Java pushes straight onto `pendingRequests` here rather than through
            // `enqueueRequest`, so it skips the debug log; the queue is the same.
            pending_requests.add(commit_handler);

            self.transaction_started = true;
            return Ok(());
        }
        if error == Errors::CoordinatorNotAvailable || error == Errors::NotCoordinator {
            let transactional_id = self.transactional_id().unwrap_or_default().to_string();
            self.lookup_coordinator(coordinators, pending_requests, CoordinatorType::Transaction, &transactional_id)?;
            self.retry(pending_requests, handler);
            return Ok(());
        }
        if error.error().is_some_and(|e| e.is_retriable_error()) {
            self.retry(pending_requests, handler);
            return Ok(());
        }
        if error == Errors::UnknownProducerId {
            return self.abortable_error_if_possible(&handler, Error::new(error));
        }
        if error == Errors::InvalidProducerEpoch || error == Errors::ProducerFenced {
            // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
            // just treat it the same as PRODUCE_FENCED.
            return self.fatal_error(&handler, Error::new(Errors::ProducerFenced));
        }
        if error == Errors::TransactionalIdAuthorizationFailed
            || error == Errors::InvalidTxnState
            || error == Errors::InvalidProducerIdMapping
        {
            return self.fatal_error(&handler, Error::new(error));
        }
        if error == Errors::GroupAuthorizationFailed {
            // Java: GroupAuthorizationException.forGroupId(builder.data.groupId()).
            return self.abortable_error(&handler, Error::group_authorization(group_id));
        }
        if error == Errors::TransactionAbortable {
            return self.abortable_error(&handler, Error::new(error));
        }
        // Java 1851: `new KafkaException("Unexpected error in AddOffsetsToTxnResponse: " + ..)`.
        self.fatal_error(
            &handler,
            Error::kafka_message(format!("Unexpected error in AddOffsetsToTxnResponse: {}", error.message())),
        )
    }

    /// Handles a `TxnOffsetCommit` response.
    ///
    /// Translated from
    /// `TxnOffsetCommitHandler.handleResponse(AbstractResponse)` (Java 1904).
    ///
    /// # Three structural details a reviewer should check
    ///
    /// This handler's loop uses `break`, not `return` — so the tail at Java 1944
    /// runs on *every* path, and it is the tail that decides between completing,
    /// clearing and retrying. `coordinatorReloaded` makes the group-coordinator
    /// lookup happen at most once per response even when several partitions report
    /// it (Java 1922). And `Errors.NONE` *removes* the partition from
    /// `pendingTxnOffsetCommits` while the retriable arm leaves it in place.
    ///
    /// # What `pendingTxnOffsetCommits` does **not** do: shrink the retry
    ///
    /// Both builders snapshot the topic collection at construction — Java's
    /// `TxnOffsetCommitRequest.Builder` calls `setTopics(getTopics(pendingTxnOffsetCommits))`,
    /// and
    /// [`TxnOffsetCommitRequestBuilder::with_options`]
    /// takes the map by reference and
    /// copies it the same way (it cannot borrow `self.pending_txn_offset_commits`,
    /// since the handler outlives the call). `reenqueue()` (Java 1394) and
    /// [`Self::retry`] both re-enqueue the *same handler with the same builder*, and
    /// nothing between the two touches `builder.data`. So a retry re-sends the **full
    /// original** offset list, including partitions that already returned `NONE`.
    ///
    /// What the map governs is (a) the tail's three-way choice above, and (b) the
    /// contents of the *next* [`Self::txn_offset_commit_handler`] construction — e.g.
    /// a subsequent `sendOffsetsToTransaction`, which is where leftovers are folded
    /// in.
    ///
    /// **Do not "fix" this by rebuilding the builder before `retry`.** It would send
    /// a reduced request where Java sends the full one — a wire-level divergence
    /// introduced by making the code match a comment rather than the Java.
    fn handle_txn_offset_commit_response(
        &mut self,
        handler: TxnRequestHandler,
        response: &ConcreteResponse,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
    ) -> Result<(), Error> {
        let TxnRequestHandlerKind::TxnOffsetCommit { builder } = &handler.kind else {
            return Err(Error::local_illegal_state(
                "handle_txn_offset_commit_response called for another request kind",
            ));
        };
        let ConcreteResponse::TxnOffsetCommit(txn_offset_commit_response) = response else {
            // Java casts unconditionally; a mismatch would be a
            // ClassCastException. Surfaced as an error per CLAUDE.md §10.2.
            return Err(Error::local_illegal_state(format!(
                "Expected a TxnOffsetCommit response for a TxnOffsetCommit request, got {response}"
            )));
        };
        let group_id = builder.data().group_id.clone();
        let mut coordinator_reloaded = false;
        let errors = txn_offset_commit_response.errors();

        kafka_debug!(
            self.log_context,
            "Received TxnOffsetCommit response for consumer group {}: {}",
            group_id,
            format_partition_errors(&errors)
        );

        // Java iterates a `HashMap`; sorted here so which error wins a `break` is
        // reproducible (see [`sorted_partition_errors`]).
        for (topic_partition, error) in sorted_partition_errors(&errors) {
            if error == Errors::None {
                self.pending_txn_offset_commits.remove(topic_partition);
            } else if error == Errors::CoordinatorNotAvailable
                || error == Errors::NotCoordinator
                || error == Errors::RequestTimedOut
            {
                if !coordinator_reloaded {
                    coordinator_reloaded = true;
                    self.lookup_coordinator(coordinators, pending_requests, CoordinatorType::Group, &group_id)?;
                }
            } else if error.error().is_some_and(|e| e.is_retriable_error()) {
                // If the topic is unknown, the coordinator is loading, or is another retriable error, retry with the
                // current coordinator
                continue;
            } else if error == Errors::GroupAuthorizationFailed {
                // Java: GroupAuthorizationException.forGroupId(builder.data.groupId()).
                self.abortable_error(&handler, Error::group_authorization(group_id.clone()))?;
                break;
            } else if error == Errors::FencedInstanceId || error == Errors::TransactionAbortable {
                self.abortable_error(&handler, Error::new(error))?;
                break;
            } else if error == Errors::UnknownMemberId || error == Errors::IllegalGeneration {
                // Java 1923: `new CommitFailedException("Transaction offset Commit
                // failed due to consumer group metadata mismatch: " + ..)`.
                // `CommitFailedException extends KafkaException` directly, so it is a
                // `KafkaException` that is NOT an `ApiException` — exactly what
                // `ConsumerCommitFailedError` encodes. (A code-resolved
                // `Errors::UnknownServerError` made `is_api_error()` answer `true`.)
                self.abortable_error(
                    &handler,
                    Error::ConsumerCommitFailed(ConsumerCommitFailedError::new(format!(
                        "Transaction offset Commit failed due to consumer group metadata mismatch: {}",
                        error.message()
                    ))),
                )?;
                break;
            } else if error == Errors::InvalidProducerEpoch || error == Errors::ProducerFenced {
                // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                // just treat it the same as PRODUCE_FENCED.
                self.fatal_error(&handler, Error::new(Errors::ProducerFenced))?;
                break;
            } else if error == Errors::TransactionalIdAuthorizationFailed
                || error == Errors::UnsupportedForMessageFormat
            {
                self.fatal_error(&handler, Error::new(error))?;
                break;
            } else {
                // Java 1937: `new KafkaException("Unexpected error in
                // TxnOffsetCommitResponse: " + ..)` — a bare `KafkaException`.
                self.fatal_error(
                    &handler,
                    Error::kafka_message(format!("Unexpected error in TxnOffsetCommitResponse: {}", error.message())),
                )?;
                break;
            }
        }

        if handler.result.is_completed() {
            self.pending_txn_offset_commits.clear();
        } else if self.pending_txn_offset_commits.is_empty() {
            handler.result.done();
        } else {
            // Retry the commits which failed with a retriable error
            self.retry(pending_requests, handler);
        }
        Ok(())
    }

    /// Restores `handler`'s backoff to the manager's configured value.
    ///
    /// Java's `AddPartitionsToTxnHandler.handleResponse` opens with
    /// `retryBackoffMs = TransactionManager.this.retryBackoffMs` (Java 1565), which
    /// undoes a previous [`Self::maybe_override_retry_backoff_ms`] before the new
    /// response's errors are examined.
    fn reset_add_partitions_retry_backoff_ms(&self, handler: &mut TxnRequestHandler) {
        if let TxnRequestHandlerKind::AddPartitionsToTxn { retry_backoff_ms, .. } = &mut handler.kind {
            *retry_backoff_ms = self.retry_backoff_ms;
        }
    }

    /// Lowers `handler`'s backoff to [`ADD_PARTITIONS_RETRY_BACKOFF_MS`] when this
    /// is the transaction's *first* `AddPartitionsToTxn`.
    ///
    /// Translated from `AddPartitionsToTxnHandler.maybeOverrideRetryBackoffMs()`
    /// (Java 1641).
    ///
    /// Java's comment: we only want to reduce the backoff when retrying the first
    /// AddPartition which errored out due to a `CONCURRENT_TRANSACTIONS` error,
    /// since this means that the previous transaction is still completing and we
    /// don't want to wait too long before trying to start the new one. This is only
    /// a temporary fix; the long-term solution is tracked in KAFKA-5482.
    ///
    /// A manager method rather than a handler one because Java's version reads the
    /// enclosing instance's `partitionsInTransaction`.
    fn maybe_override_retry_backoff_ms(&self, handler: &mut TxnRequestHandler) {
        if !self.partitions_in_transaction.is_empty() {
            return;
        }
        if let TxnRequestHandlerKind::AddPartitionsToTxn { retry_backoff_ms, .. } = &mut handler.kind {
            *retry_backoff_ms = TransactionManager::ADD_PARTITIONS_RETRY_BACKOFF_MS;
        }
    }

    // -- Send path ----------------------------------------------------------

    /// Validates that a record may be appended for `topic_partition`, adding it
    /// to the transaction when one is in progress.
    ///
    /// Translated from `maybeAddPartition(TopicPartition)` (Java 437).
    ///
    /// # The transactional arm's chain order is load-bearing
    ///
    /// Java writes an ordered `if / else if` chain (Java 441-459) whose Transaction
    /// V2 arm (`:448`) precedes the already-added short-circuit (`:452`). Under TV2
    /// the client sends no `AddPartitionsToTxn` at all, so the partition is
    /// registered straight into `partitionsInTransaction` — and re-registering one
    /// already there is idempotent, which is why that arm can sit ahead of the
    /// short-circuit.
    ///
    /// # Hot-path allocation audit (`definition-of-done.md` §10)
    ///
    /// This method is on the send path: `KafkaProducer.doSend` calls it once per
    /// record. Every branch added by the transactional arm is behind
    /// `is_transactional()`, so the idempotent path is byte-for-byte what Phase 4
    /// audited.
    ///
    /// For a transactional producer, per record in the steady state:
    ///
    ///   - **V1** — the `transaction_contains_partition || is_partition_pending_add`
    ///     short-circuit returns before touching anything, so two hash lookups and no
    ///     allocation. The registration arm below it runs once per *partition*, not
    ///     per record.
    ///   - **V2** — the arm runs every time, as Java's does: `get_or_create` and the
    ///     `partitions_in_transaction` insert each hash once and each clone the
    ///     `TopicPartition`. That clone is an `Arc<str>` refcount bump plus an `i32`,
    ///     which is the representation CLAUDE.md §11 prescribes precisely so it is
    ///     not a heap allocation — the set already holds the key, so no bucket is
    ///     allocated and both clones are dropped again. Java pays the two lookups too
    ///     (`computeIfAbsent` + `HashSet.add`) and, having references, no refcount
    ///     traffic.
    ///
    /// Guarding the V2 arm on `partitions_in_transaction.contains(..)` would remove
    /// that traffic, and is deliberately **not** done: `txn_partition_map` has other
    /// writers (`reset` from the producer-id lifecycle) that
    /// `partitions_in_transaction` does not, so the two sets can legitimately
    /// disagree, and a `contains`-guard would then skip a `get_or_create` Java
    /// performs. Trading a correctness edge for two atomic increments is the wrong
    /// way round.
    pub(crate) fn maybe_add_partition(&mut self, topic_partition: &TopicPartition) -> Result<(), Error> {
        self.maybe_fail_with_error()?;
        self.return_error_if_pending_state(TransactionOperation::Send)?;

        if self.is_transactional() {
            if !self.has_producer_id() {
                return Err(Error::local_illegal_state(format!(
                    "Cannot add partition {topic_partition} to transaction before completing a call to \
                     initTransactions"
                )));
            } else if self.current_state != State::InTransaction {
                // Java's message has two spaces before the state; reproduced so
                // message assertions keep matching (Java 447).
                return Err(Error::local_illegal_state(format!(
                    "Cannot add partition {topic_partition} to transaction while in state  {}",
                    self.current_state
                )));
            } else if self.is_transaction_v2_enabled {
                self.txn_partition_map.get_or_create(topic_partition);
                self.partitions_in_transaction.insert(topic_partition.clone());
                self.transaction_started = true;
            } else if self.transaction_contains_partition(topic_partition)
                || self.is_partition_pending_add(topic_partition)
            {
                return Ok(());
            } else {
                kafka_debug!(
                    self.log_context,
                    "Begin adding new partition {} to transaction",
                    topic_partition
                );
                self.txn_partition_map.get_or_create(topic_partition);
                self.new_partitions_in_transaction.insert(topic_partition.clone());
            }
        }
        Ok(())
    }

    /// Whether the failed produce response for the failing batch should be retried.
    ///
    /// Translated from `canRetry(PartitionResponse, ProducerBatch)`
    /// (Java 1015).
    ///
    /// The failing batch is **not** passed by reference. It is still tracked in the
    /// txn partition map at this point (`can_retry` runs before any
    /// `remove_in_flight_batch`), so the transactional log-truncation rewrite
    /// (`start_sequences_at_beginning`, Java 1048) needs it inside `batches`. Passing
    /// it *also* as a `&ProducerBatch` would alias the `&mut` reference the pool holds,
    /// so instead the caller injects it into `batches` and identifies it here by its
    /// ordering key `batch_key` (`.claude/rules/producer-transactions.md` §6/§7, PLAN
    /// §9.25). `sequence_has_been_reset` is the one failing-batch attribute the ordering
    /// key does not encode, so it is passed explicitly.
    ///
    /// `batches` is the partition's full in-flight pool — accumulator deques,
    /// `Sender::in_flight_batches`, and the failing batch. The idempotent path never
    /// reads it, but the transactional log-truncation rewrite does.
    pub(crate) fn can_retry(
        &mut self,
        response: &PartitionResponse,
        topic_partition: &TopicPartition,
        batch_key: InFlightBatchKey,
        sequence_has_been_reset: bool,
        batches: &mut [&mut ProducerBatch],
    ) -> Result<bool, Error> {
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

            if sequence_has_been_reset {
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
                .last_acked_offset(topic_partition)
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
                        topic_partition,
                        producer_id_and_epoch,
                        batches,
                    )?;
                } else {
                    self.request_idempotent_epoch_bump_for_partition(topic_partition);
                }
                return Ok(true);
            }

            if !self.is_transactional() {
                // For the idempotent producer, always retry UNKNOWN_PRODUCER_ID errors. If the batch has the current
                // producer ID and epoch, request a bump of the epoch. Otherwise just retry the produce.
                self.request_idempotent_epoch_bump_for_partition(topic_partition);
                return Ok(true);
            }
        } else if error == Errors::OutOfOrderSequenceNumber {
            if !self.has_unresolved_sequence(topic_partition)
                && (sequence_has_been_reset || !self.is_next_sequence(topic_partition, batch_key.2))
            {
                // We should retry the OutOfOrderSequenceException if the batch is _not_ the next batch, ie. its base
                // sequence isn't the lastAckedSequence + 1.
                return Ok(true);
            } else if !self.is_transactional() {
                // For the idempotent producer, retry all OUT_OF_ORDER_SEQUENCE_NUMBER errors. If there are no
                // unresolved sequences, or this batch is the one immediately following an unresolved sequence, we know
                // there is actually a gap in the sequences, and we bump the epoch. Otherwise, retry without bumping
                // and wait to see if the sequence resolves
                if !self.has_unresolved_sequence(topic_partition)
                    || self.is_next_sequence_for_unresolved_partition(topic_partition, batch_key.2)
                {
                    self.request_idempotent_epoch_bump_for_partition(topic_partition);
                }
                return Ok(true);
            }
        }

        // If neither of the above cases are true, retry if the exception is retriable
        Ok(error.error().is_some_and(|e| e.is_retriable_error()))
    }
}

/// A per-partition error map in topic-then-partition order.
///
/// Java iterates these maps in `HashMap` order, which is unspecified. The response
/// handlers that walk one contain arms that `return` mid-loop, so the order decides
/// *which* error is reported when a response carries several — sorting makes that
/// choice reproducible, the same reasoning
/// `.claude/rules/producer-transactions.md` §10 applies to encodings.
fn sorted_partition_errors(errors: &HashMap<TopicPartition, Errors>) -> Vec<(&TopicPartition, Errors)> {
    let mut entries: Vec<(&TopicPartition, Errors)> = errors.iter().map(|(tp, error)| (tp, *error)).collect();
    entries.sort_by_key(|(partition, _)| (partition.topic_arc().clone(), partition.partition()));
    entries
}

/// A per-partition error map rendered as Java's `AbstractMap.toString()` would
/// render it, in topic-then-partition order.
///
/// Java interpolates the map straight into a `KafkaException` message
/// (`TransactionManager.java:1625`); `HashMap` order would make that text
/// unreproducible, so it is sorted here.
///
/// # Each error prints as its Rust variant name, not Java's constant name
///
/// Java's `Errors` is an enum, so interpolating one yields
/// `Enum.toString()` = `name()`, e.g. `TOPIC_AUTHORIZATION_FAILED`. This crate's
/// [`Errors`] renders `Display` as the human-readable `message()` and has no
/// `name()`, so the variant identifier is printed instead:
/// `TopicAuthorizationFailed`. That identifier *is* the translation of Java's
/// constant under CLAUDE.md §2's PascalCase rule, and no Java test asserts on this
/// message — adding a 134-arm `name()` to a shared file for one diagnostic string
/// would be out of proportion. Recorded as a deviation in PLAN §10.8.
fn format_partition_errors(errors: &HashMap<TopicPartition, Errors>) -> String {
    let entries: Vec<String> = sorted_partition_errors(errors)
        .into_iter()
        .map(|(partition, error)| format!("{partition}={error:?}"))
        .collect();
    format!("{{{}}}", entries.join(", "))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::AddOffsetsToTxnResponseData;
    use crate::AddPartitionsToTxnResponseData;
    use crate::EndTxnResponseData;
    use crate::InitProducerIdResponseData;
    use crate::NodeApiVersions;
    use crate::api_versions_response_data::{ApiVersion, FinalizedFeatureKey, SupportedFeatureKey};
    use crate::common::ApiKeys;
    use crate::common::compress::Compression;
    use crate::common::record::TimestampType;
    use crate::common::record::internal::MemoryRecords;
    use crate::common::requests::{
        AddOffsetsToTxnResponse, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, EndTxnResponse,
        FindCoordinatorResponse, InitProducerIdResponse, PartitionResponseOptionsBuilder, TxnOffsetCommitResponse,
    };
    use crate::producer::internals::SenderStatics;

    // Constants mirroring `TransactionManagerTest`'s fields (Java 125-155).
    const TRANSACTIONAL_ID: &str = "foobar";
    const TRANSACTION_TIMEOUT_MS: i32 = 1121;
    const DEFAULT_RETRY_BACKOFF_MS: i64 = 100;
    const TOPIC: &str = "test";
    const PRODUCER_ID: i64 = 13131;
    const EPOCH: i16 = 1;
    const ONGOING_PRODUCER_ID: i64 = 999;
    const BUMPED_ONGOING_EPOCH: i16 = 11;
    const CONSUMER_GROUP_ID: &str = "myConsumerGroup";
    const MEMBER_ID: &str = "member";
    const GENERATION_ID: i32 = 5;
    const GROUP_INSTANCE_ID: &str = "instance";
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
    fn bare_kafka_error() -> Error {
        Error::with_message(Errors::UnknownServerError, "")
    }

    /// Java's `new TimeoutException()`.
    fn timeout_error() -> Error {
        Error::timeout("")
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
    /// # How `transaction_v2_enabled` reaches the manager
    ///
    /// Java threads the flag only into `apiVersions`, as a finalized
    /// `transaction.version` level of 2 (enabled) or 1 (disabled) at features epoch
    /// 0. The manager reads `apiVersions` from exactly two methods:
    /// [`TransactionManager::handle_coordinator_ready`] (Java 1104), which looks at
    /// the `INIT_PRODUCER_ID` version and not at features, and
    /// [`TransactionManager::maybe_update_transaction_v2_enabled`] (Java 493), the
    /// only writer of `is_transaction_v2_enabled`. So a manager built here is
    /// Transaction V2 *capable* but not yet V2 *enabled*: [`do_init_transactions`]
    /// makes the feature read, mirroring Java 4359.
    fn manager_with_transactional_id(
        transactional_id: Option<String>,
        transaction_v2_enabled: bool,
    ) -> TransactionManager {
        manager_with_transactional_id_and_2pc(transactional_id, transaction_v2_enabled, false)
    }

    /// Mirrors the three-argument
    /// `initializeTransactionManager(Optional, boolean, boolean)` (Java 177).
    fn manager_with_transactional_id_and_2pc(
        transactional_id: Option<String>,
        transaction_v2_enabled: bool,
        enable_2pc: bool,
    ) -> TransactionManager {
        fn api_version(api_key: &ApiKeys, max_version: i16) -> ApiVersion {
            let mut version = ApiVersion::new();
            version.set_api_key(api_key.id());
            version.set_min_version(0);
            version.set_max_version(max_version);
            version
        }

        let api_versions = Arc::new(ApiVersions::new());
        api_versions.update(
            "0",
            NodeApiVersions::with_node_finalized_features_finalized_features_epoch(
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
                &transaction_version_supported_features(if transaction_v2_enabled { 2 } else { 1 }),
                &transaction_version_finalized_features(if transaction_v2_enabled { 2 } else { 1 }),
                0,
            ),
        );

        TransactionManager::new(
            LogContext::empty(),
            transactional_id,
            TRANSACTION_TIMEOUT_MS,
            DEFAULT_RETRY_BACKOFF_MS,
            api_versions,
            enable_2pc,
        )
    }

    /// The `supportedFeatures` list Java's fixture builds (Java 198-201).
    fn transaction_version_supported_features(level: i16) -> Vec<SupportedFeatureKey> {
        let mut supported_feature = SupportedFeatureKey::new();
        supported_feature.set_name(TransactionManager::TRANSACTION_VERSION_FEATURE.to_string());
        supported_feature.set_max_version(level);
        supported_feature.set_min_version(0);
        vec![supported_feature]
    }

    /// The `finalizedFeatures` list Java's fixture builds (Java 202-205).
    fn transaction_version_finalized_features(level: i16) -> Vec<FinalizedFeatureKey> {
        let mut finalized_feature = FinalizedFeatureKey::new();
        finalized_feature.set_name(TransactionManager::TRANSACTION_VERSION_FEATURE.to_string());
        finalized_feature.set_max_version_level(level);
        finalized_feature.set_min_version_level(level);
        vec![finalized_feature]
    }

    /// Republishes node `"0"`'s API versions with `transaction.version` finalized at
    /// `level` and features epoch `epoch`.
    ///
    /// Mirrors the mid-test `apiVersions.update("0", .., 2)` that
    /// `testTransactionManagerEnablesV2` (Java 941-954) performs to move the cluster
    /// onto Transaction V2 while the manager is running.
    fn finalize_transaction_version(api_versions: &ApiVersions, level: i16, epoch: i64) {
        let mut init_producer_id = ApiVersion::new();
        init_producer_id.set_api_key(ApiKeys::INIT_PRODUCER_ID.id());
        init_producer_id.set_min_version(0);
        init_producer_id.set_max_version(3);
        api_versions.update(
            "0",
            NodeApiVersions::with_node_finalized_features_finalized_features_epoch(
                &[init_producer_id],
                &transaction_version_supported_features(level),
                &transaction_version_finalized_features(level),
                epoch,
            ),
        );
    }

    /// A single-record batch, mirroring `batchWithValue` (Java 840).
    fn batch_with_value(topic_partition: &TopicPartition, value: &str) -> ProducerBatch {
        let builder =
            MemoryRecords::builder_with_initial_capacity(64, Compression::none(), TimestampType::CreateTime, 0);
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
    ) -> Result<(), Error> {
        let mut coordinators = CoordinatorNodes::new();
        complete_init_producer_id_with_coordinators(
            manager,
            &mut coordinators,
            pending_requests,
            handler,
            error,
            producer_id,
            epoch,
        )
    }

    /// As [`complete_init_producer_id`], but with a caller-supplied coordinator
    /// record, for the arms that rediscover the coordinator (Java 1520).
    #[allow(clippy::too_many_arguments)]
    fn complete_init_producer_id_with_coordinators(
        manager: &mut TransactionManager,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
        handler: TxnRequestHandler,
        error: Errors,
        producer_id: i64,
        epoch: i16,
    ) -> Result<(), Error> {
        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(error.code())
            .set_producer_id(producer_id)
            .set_producer_epoch(epoch)
            .set_throttle_time_ms(0);
        let response = ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data));
        manager.handle_response(handler, &response, coordinators, pending_requests)
    }

    /// The `AddPartitionsToTxnResponse` Java's `prepareAddPartitionsToTxn`
    /// (Java 4027) builds: the per-partition errors filed under
    /// [`AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID`], which is the only shape a client request produces.
    fn add_partitions_to_txn_response(errors: &HashMap<TopicPartition, Errors>) -> ConcreteResponse {
        let result =
            AddPartitionsToTxnResponse::result_for_transaction(AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID, errors);
        let mut data = AddPartitionsToTxnResponseData::new();
        data.set_results_by_topic_v3_and_below(result.topic_results)
            .set_throttle_time_ms(0);
        ConcreteResponse::AddPartitionsToTxn(AddPartitionsToTxnResponse::new(data))
    }

    /// Dequeues the pending `AddPartitionsToTxn`, checks the outgoing request the
    /// way Java's request matcher does, and feeds back a response carrying `errors`.
    ///
    /// Combines `prepareAddPartitionsToTxn` (Java 4027) — including its assertion
    /// that the request's partition set equals the errors' key set — with the
    /// `runUntil` that lets `Sender` send it and dispatch the reply.
    /// `addPartitionsRequestMatcher` (Java 4165) checks the producer id, epoch and
    /// transactional id too, so those are checked here.
    fn run_add_partitions_to_txn(
        manager: &mut TransactionManager,
        pending_requests: &mut PendingRequests,
        errors: &[(TopicPartition, Errors)],
    ) -> Result<(), Error> {
        let mut coordinators = CoordinatorNodes::new();
        run_add_partitions_to_txn_with_coordinators(manager, &mut coordinators, pending_requests, errors)
    }

    /// As [`run_add_partitions_to_txn`], but with a caller-supplied coordinator
    /// record, for the arms that rediscover the coordinator (Java 1578).
    fn run_add_partitions_to_txn_with_coordinators(
        manager: &mut TransactionManager,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
        errors: &[(TopicPartition, Errors)],
    ) -> Result<(), Error> {
        let handler = manager
            .next_request(pending_requests, false)
            .expect("next_request does not fail on this path")
            .expect("an AddPartitionsToTxn request must be pending");
        let data = handler
            .add_partitions_to_txn_request_data()
            .expect("an AddPartitionsToTxn handler");
        assert_eq!(data.v3_and_below_transactional_id, TRANSACTIONAL_ID);
        assert_eq!(data.v3_and_below_producer_id, manager.producer_id_and_epoch().producer_id);
        assert_eq!(data.v3_and_below_producer_epoch, manager.producer_id_and_epoch().epoch);
        let requested: HashSet<TopicPartition> = AddPartitionsToTxnRequest::get_partitions(&data.v3_and_below_topics)
            .into_iter()
            .collect();
        let expected: HashSet<TopicPartition> = errors.iter().map(|(partition, _)| partition.clone()).collect();
        assert_eq!(requested, expected, "the request must carry exactly the pending partitions");

        let error_map: HashMap<TopicPartition, Errors> = errors.iter().cloned().collect();
        let response = add_partitions_to_txn_response(&error_map);
        manager.handle_response(handler, &response, coordinators, pending_requests)
    }

    /// Dequeues the pending `EndTxn`, checks the outgoing request the way Java's
    /// `endTxnMatcher` (Java 4262) does, and feeds back a response carrying `error`.
    ///
    /// Combines the two-overload `prepareEndTxnResponse` family (Java 4184, 4224)
    /// with the `runUntil` that lets `Sender` send it. `response_producer_id` /
    /// `response_epoch` are what Java's seven-argument overload puts on the response
    /// only when the negotiated version is v5+; passing
    /// [`RecordBatch::NO_PRODUCER_ID`] reproduces the four-argument overload, which
    /// asserts the version is below 5.
    fn run_end_txn(
        manager: &mut TransactionManager,
        pending_requests: &mut PendingRequests,
        result: TransactionResult,
        error: Errors,
        response_producer_id: i64,
        response_epoch: i16,
    ) -> Result<(), Error> {
        let mut coordinators = CoordinatorNodes::new();
        let handler = manager
            .next_request(pending_requests, false)
            .expect("next_request does not fail on this path")
            .expect("an EndTxn request must be pending");
        assert!(handler.is_end_txn(), "the EndTxn must be at the head of the queue");
        let data = handler.end_txn_request_data().expect("an EndTxn handler");
        assert_eq!(data.transactional_id, TRANSACTIONAL_ID);
        assert_eq!(data.producer_id, manager.producer_id_and_epoch().producer_id);
        assert_eq!(data.producer_epoch, manager.producer_id_and_epoch().epoch);
        assert_eq!(TransactionResult::for_id(data.committed), result);

        let mut data = EndTxnResponseData::new();
        data.set_error_code(error.code())
            .set_throttle_time_ms(0)
            .set_producer_id(response_producer_id)
            .set_producer_epoch(response_epoch);
        let response = ConcreteResponse::EndTxn(EndTxnResponse::new(data));
        manager.handle_response(handler, &response, &mut coordinators, pending_requests)
    }

    /// [`run_end_txn`] with the pre-v5 response shape: no producer id or epoch, so
    /// the handler's epoch-absorption arm (Java 1760) is not taken.
    ///
    /// Mirrors the four-argument `prepareEndTxnResponse` (Java 4184).
    fn run_end_txn_v4(
        manager: &mut TransactionManager,
        pending_requests: &mut PendingRequests,
        result: TransactionResult,
        error: Errors,
    ) -> Result<(), Error> {
        run_end_txn(
            manager,
            pending_requests,
            result,
            error,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
    }

    /// Dequeues the pending `AddOffsetsToTxn`, checks the outgoing request the way
    /// Java's `prepareAddOffsetsToTxnResponse` (Java 4283) does, and feeds back a
    /// response carrying `error`.
    fn run_add_offsets_to_txn(
        manager: &mut TransactionManager,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
        consumer_group_id: &str,
        error: Errors,
    ) -> Result<(), Error> {
        let handler = manager
            .next_request(pending_requests, false)
            .expect("next_request does not fail on this path")
            .expect("an AddOffsetsToTxn request must be pending");
        let data = handler.add_offsets_to_txn_request_data().expect("an AddOffsetsToTxn handler");
        assert_eq!(data.group_id, consumer_group_id);
        assert_eq!(data.transactional_id, TRANSACTIONAL_ID);
        assert_eq!(data.producer_id, manager.producer_id_and_epoch().producer_id);
        assert_eq!(data.producer_epoch, manager.producer_id_and_epoch().epoch);

        let mut data = AddOffsetsToTxnResponseData::new();
        data.set_error_code(error.code());
        let response = ConcreteResponse::AddOffsetsToTxn(AddOffsetsToTxnResponse::new(data));
        manager.handle_response(handler, &response, coordinators, pending_requests)
    }

    /// Discovers the group coordinator for the pending `TxnOffsetCommit`, the way
    /// `Sender` does when it finds the slot empty.
    ///
    /// Java's tests reach this through `runUntil`, which spins `Sender.runOnce`;
    /// these manager-level tests play the Sender's part explicitly, mirroring
    /// `Sender.java:479-492` (coordinator unknown → `maybeFindCoordinatorAndRetry`)
    /// and `:520-529` (`lookupCoordinator` then `retry`). Reproduces
    /// `prepareFindCoordinatorResponse(NONE, false, GROUP, consumerGroupId)`
    /// (Java 4043) plus the round trip that installs the node.
    fn discover_group_coordinator(
        manager: &mut TransactionManager,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
        consumer_group_id: &str,
    ) -> Result<(), Error> {
        let handler = manager
            .next_request(pending_requests, false)
            .expect("next_request does not fail on this path")
            .expect("a TxnOffsetCommit request must be pending");
        assert!(manager.needs_coordinator(&handler));
        assert_eq!(manager.coordinator_type(&handler), Some(CoordinatorType::Group));
        assert_eq!(manager.coordinator_key(&handler), Some(consumer_group_id));
        assert!(
            coordinators.coordinator(CoordinatorType::Group).expect("valid type").is_none(),
            "the group coordinator must still be unknown"
        );

        manager.lookup_coordinator_for(coordinators, pending_requests, &handler)?;
        manager.retry(pending_requests, handler);

        let find_coordinator = manager
            .next_request(pending_requests, false)
            .expect("next_request does not fail on this path")
            .expect("the FindCoordinator overtakes the TxnOffsetCommit");
        let data = find_coordinator
            .find_coordinator_request_data()
            .expect("a FindCoordinator handler");
        assert_eq!(
            CoordinatorType::for_id(data.key_type).expect("a valid coordinator type id"),
            CoordinatorType::Group
        );
        assert_eq!(data.key, consumer_group_id);
        complete_find_coordinator(
            manager,
            coordinators,
            pending_requests,
            find_coordinator,
            Errors::None,
            consumer_group_id,
            &broker_node(),
        )?;
        assert!(
            coordinators.coordinator(CoordinatorType::Group).expect("valid type").is_some(),
            "the group coordinator must now be known"
        );
        Ok(())
    }

    /// Dequeues the pending `TxnOffsetCommit`, checks the outgoing request the way
    /// Java's `prepareTxnOffsetCommitResponse` (Java 4300) does, and feeds back a
    /// response carrying `errors`.
    fn run_txn_offset_commit(
        manager: &mut TransactionManager,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
        consumer_group_id: &str,
        errors: &[(TopicPartition, Errors)],
    ) -> Result<(), Error> {
        run_txn_offset_commit_with_group_metadata(
            manager,
            coordinators,
            pending_requests,
            consumer_group_id,
            None,
            errors,
        )
    }

    /// As [`run_txn_offset_commit`], with the consumer-group metadata assertions of
    /// Java's seven-argument `prepareTxnOffsetCommitResponse` (Java 4313).
    fn run_txn_offset_commit_with_group_metadata(
        manager: &mut TransactionManager,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
        consumer_group_id: &str,
        group_metadata: Option<&ConsumerGroupMetadata>,
        errors: &[(TopicPartition, Errors)],
    ) -> Result<(), Error> {
        let handler = manager
            .next_request(pending_requests, false)
            .expect("next_request does not fail on this path")
            .expect("a TxnOffsetCommit request must be pending");
        let data = handler.txn_offset_commit_request_data().expect("a TxnOffsetCommit handler");
        assert_eq!(data.group_id, consumer_group_id);
        assert_eq!(data.producer_id, manager.producer_id_and_epoch().producer_id);
        assert_eq!(data.producer_epoch, manager.producer_id_and_epoch().epoch);
        if let Some(group_metadata) = group_metadata {
            assert_eq!(data.group_instance_id.as_deref(), group_metadata.group_instance_id());
            assert_eq!(data.member_id, group_metadata.member_id());
            assert_eq!(data.generation_id, group_metadata.generation_id());
        }

        let error_map: HashMap<TopicPartition, Errors> = errors.iter().cloned().collect();
        let response = ConcreteResponse::TxnOffsetCommit(
            TxnOffsetCommitResponse::with_request_throttle_ms_response_data(0, &error_map),
        );
        manager.handle_response(handler, &response, coordinators, pending_requests)
    }

    /// `new OffsetAndMetadata(offset)`, which cannot fail for a non-negative offset.
    fn offset(offset: i64) -> OffsetAndMetadata {
        OffsetAndMetadata::new(offset).expect("a non-negative offset")
    }

    /// Drives a 2PC-enabled transactional producer from `UNINITIALIZED` through
    /// `InitProducerId`, mirroring `doInitTransactionsWith2PCEnabled(boolean)`
    /// (Java 4367).
    ///
    /// Java's helper is **declared and never called** in Apache Kafka 4.2 (the
    /// PHASE-5B TEST ACCOUNTING block below carries the check), so this has no Java
    /// test above it; it exists because it is the only way to reach the
    /// `keepPreparedTxn` response arm (Java 1501), which
    /// `initialize_transactions`'s own request never sets — see
    /// [`TransactionManager::initialize_transactions_internal`]. The flag is
    /// therefore forced onto the outgoing request here, which is exactly what
    /// Java's own `prepareInitPidResponse(.., keepPreparedTxn = true, ..)` overload
    /// (Java 4076) asserts the broker would see.
    async fn do_init_transactions_with_2pc_enabled(
        manager: &mut TransactionManager,
        pending_requests: &mut PendingRequests,
        keep_prepared: bool,
    ) -> Arc<TransactionalRequestResult> {
        let result = manager
            .initialize_transactions(keep_prepared, pending_requests)
            .expect("initTransactions is valid from UNINITIALIZED");
        let mut handler = manager
            .next_request(pending_requests, false)
            .expect("next_request does not fail on this path")
            .expect("an InitProducerId request must be pending");
        assert_eq!(
            handler
                .init_producer_id_request_data()
                .expect("an InitProducerId handler")
                .transactional_id
                .as_deref(),
            Some(TRANSACTIONAL_ID)
        );
        handler.set_keep_prepared_txn_for_test(keep_prepared);

        let (response_producer_id, response_epoch, ongoing_producer_id, ongoing_epoch) = if keep_prepared {
            // Simulate an ongoing prepared transaction (ongoingProducerId != -1).
            (
                ONGOING_PRODUCER_ID,
                BUMPED_ONGOING_EPOCH,
                ONGOING_PRODUCER_ID,
                BUMPED_ONGOING_EPOCH - 1,
            )
        } else {
            (PRODUCER_ID, EPOCH, RecordBatch::NO_PRODUCER_ID, RecordBatch::NO_PRODUCER_EPOCH)
        };
        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(Errors::None.code())
            .set_producer_id(response_producer_id)
            .set_producer_epoch(response_epoch)
            .set_ongoing_txn_producer_id(ongoing_producer_id)
            .set_ongoing_txn_producer_epoch(ongoing_epoch)
            .set_throttle_time_ms(0);
        let response = ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data));
        let mut coordinators = CoordinatorNodes::new();
        manager
            .handle_response(handler, &response, &mut coordinators, pending_requests)
            .expect("a successful InitProducerId response is handled");

        assert!(manager.has_producer_id());
        manager.maybe_update_transaction_v2_enabled(true);

        assert!(result.is_successful());
        result.await_result().await.expect("initTransactions succeeded");
        assert!(result.is_acked());
        result
    }

    /// Mirrors `assertAbortableError(Class)` (Java 4408): a commit is refused, the
    /// error survives the refusal, and an abort clears it.
    ///
    /// Java identifies the recorded error by `e.getCause().getClass()`.
    /// [`Error`] has no cause chain (PLAN §10.5 deviation 5), and
    /// `maybeFailWithError` reproduces Java's message without it, so the cause is
    /// asserted where it actually lives — [`TransactionManager::last_error`] — by
    /// wire code.
    fn assert_abortable_error(manager: &mut TransactionManager, pending_requests: &mut PendingRequests, cause: Errors) {
        assert_eq!(
            manager.last_error().expect("an error is recorded").error(),
            cause,
            "the recorded cause must be {cause:?}"
        );
        manager
            .begin_commit(pending_requests)
            .expect_err("committing after an abortable error must be refused");
        assert!(manager.has_error());

        manager
            .begin_abort(pending_requests, Caller::App)
            .expect("an abort clears an abortable error");
        assert!(!manager.has_error());
    }

    /// Mirrors `assertFatalError(Class)` (Java 4422): an abort is refused, twice —
    /// "transaction abort cannot clear fatal error state".
    ///
    /// See [`assert_abortable_error`] for why the cause is asserted on
    /// [`TransactionManager::last_error`].
    fn assert_fatal_error(manager: &mut TransactionManager, pending_requests: &mut PendingRequests, cause: Errors) {
        assert!(manager.has_error());
        for attempt in 0..2 {
            assert_eq!(
                manager.last_error().expect("an error is recorded").error(),
                cause,
                "the recorded cause must be {cause:?} on attempt {attempt}"
            );
            manager
                .begin_abort(pending_requests, Caller::App)
                .expect_err("aborting after a fatal error must be refused");
            assert!(manager.has_error());
        }
    }

    /// `new ConsumerGroupMetadata(consumerGroupId)`.
    fn consumer_group_metadata() -> ConsumerGroupMetadata {
        #[allow(deprecated)]
        ConsumerGroupMetadata::new(CONSUMER_GROUP_ID)
    }

    /// The throwaway group metadata Java's fence check passes — `"dummyId"`
    /// (Java 2054) — where the call is expected to fail before the group is used.
    fn dummy_group_metadata() -> ConsumerGroupMetadata {
        #[allow(deprecated)]
        ConsumerGroupMetadata::new("dummyId")
    }

    /// As [`dummy_group_metadata`], for Java's `"fake-group-id"` (Java 3837).
    fn fake_group_metadata() -> ConsumerGroupMetadata {
        #[allow(deprecated)]
        ConsumerGroupMetadata::new("fake-group-id")
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
        let handler = manager
            .next_request(pending_requests, false)
            .expect("next_request does not fail on this path")
            .expect("an InitProducerId request must be pending");
        // Java's helper asserts the same thing on the outgoing request.
        assert!(
            handler
                .init_producer_id_request_data()
                .expect("an InitProducerId handler")
                .transactional_id
                .is_none(),
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
        let handler = manager
            .next_request(pending_requests, false)
            .expect("next_request does not fail on this path")
            .expect("an InitProducerId request must be pending");
        let request_data = handler.init_producer_id_request_data().expect("an InitProducerId handler");
        assert_eq!(
            request_data.transactional_id.as_deref(),
            Some(TRANSACTIONAL_ID),
            "a transactional producer must send its transactional id"
        );
        assert_eq!(request_data.transaction_timeout_ms, TRANSACTION_TIMEOUT_MS);
        complete_init_producer_id(manager, pending_requests, handler, Errors::None, producer_id, epoch)
            .expect("a successful InitProducerId response is handled");
        assert!(manager.has_producer_id());
        // Java 4359. `on_initialization = true` suppresses the
        // `client_side_epoch_bump_required` side effect (Java 500-501), so this only
        // latches the fixture's finalized `transaction.version` level.
        manager.maybe_update_transaction_v2_enabled(true);

        // Java's helper spins `Sender.runOnce`, which connects to the transaction
        // coordinator and so runs `handleCoordinatorReady` (`Sender.java:569`). That
        // is a *manager* method with observable state — it is the only writer of
        // `coordinatorSupportsBumpingEpoch`, which decides whether an abortable
        // error is recoverable (Java 1326) — so the manager-level drive has to make
        // the same call or every `abortableErrorIfPossible` arm below would take the
        // fatal branch that Java does not.
        let mut coordinators = CoordinatorNodes::new();
        coordinators
            .set(CoordinatorType::Transaction, broker_node())
            .expect("TRANSACTION is a valid coordinator type");
        manager.handle_coordinator_ready(&coordinators);
        assert!(
            manager.can_handle_abortable_error(),
            "the fixture advertises InitProducerId v6, so a client-side bump is supported"
        );

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
        let authorization_error = last_error.filter(|error| {
            manager.has_abortable_error() && SenderStatics::is_authorization_error_handled_by_sender(error)
        });
        if let Some(error) = authorization_error {
            // Java wraps the cause in `new AuthenticationException(exception)`
            // (Sender.java:354), so the class is `AuthenticationException` and the
            // cause is carried. NOT `SaslAuthenticationFailed`: the cause here is a
            // cluster or transactional-id authorization failure and nothing about it
            // is SASL — the wire code stays `UNKNOWN_SERVER_ERROR` (-1), which is
            // correct, because `Errors.java` maps only `SaslAuthenticationException`
            // and `UnsupportedByAuthenticationException`, never the base class, so
            // `Errors.forException`'s superclass walk falls through.
            //
            // This is the same spelling `Sender::handle_authorization_error` uses:
            // `Error::with_message(Errors::UnknownServerError, ..)` resolved to an
            // `ApiException`, for which `is_authentication_error()` — and hence
            // `request_utils::RequestUtils::is_fatal_error` — answered `false`.
            manager
                .fail_pending_requests(
                    pending_requests,
                    &Error::Authentication(crate::common::errors::AuthenticationError::with_source(
                        error.message(),
                        error.clone(),
                    )),
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
        if in_flight_request_correlation_id != TransactionManager::NO_INFLIGHT_REQUEST_CORRELATION_ID
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

    /// `maybeUpdateTransactionV2Enabled` (Java 492) latches the finalized
    /// `transaction.version` level and, on an *upgrade* after initialization, sets
    /// `clientSideEpochBumpRequired`.
    ///
    /// Java covers the method only end to end, through
    /// `testTransactionManagerEnablesV2` / `testTransactionManagerDisablesV2` /
    /// `testTransactionV2AddPartitionAndOffsets`; each of those pins one path
    /// through it. This pins all four branches directly, which is the only way to
    /// separate the epoch guard from the `onInitialization` suppression — the two
    /// reasons the flag can fail to move.
    #[test]
    fn test_maybe_update_transaction_v2_enabled_reads_the_finalized_feature_level() {
        // Level 1 finalized at epoch 0: the read happens (epoch -1 < 0) and leaves
        // Transaction V2 off.
        let mut manager = transactional_manager(false);
        manager.maybe_update_transaction_v2_enabled(true);
        assert!(!manager.is_transaction_v2_enabled());
        assert!(!manager.client_side_epoch_bump_required());

        // Level 2 finalized at epoch 0: the read turns Transaction V2 on. With
        // `on_initialization = true` no epoch bump is required (Java 500-501).
        let mut manager = transactional_manager(true);
        manager.maybe_update_transaction_v2_enabled(true);
        assert!(manager.is_transaction_v2_enabled());
        assert!(!manager.client_side_epoch_bump_required());
    }

    /// The guard at Java 493-495: once a features epoch has been observed, a later
    /// call with no *newer* epoch does not re-read.
    ///
    /// This is also what makes Java's unchecked `info.finalizedFeatures.get(..)`
    /// (Java 499) safe on a fresh `ApiVersions`, whose map is `null` and whose epoch
    /// is `-1` — hence the second half.
    #[test]
    fn test_maybe_update_transaction_v2_enabled_skips_an_epoch_it_has_already_seen() {
        let mut manager = transactional_manager(false);
        manager.maybe_update_transaction_v2_enabled(false);
        assert!(!manager.is_transaction_v2_enabled());

        // Re-finalize at level 2 but at the *same* epoch the manager already read.
        // `ApiVersions::update` fences the stale epoch, so nothing changes.
        finalize_transaction_version(manager.api_versions(), 2, 0);
        manager.maybe_update_transaction_v2_enabled(false);
        assert!(!manager.is_transaction_v2_enabled());
        assert!(!manager.client_side_epoch_bump_required());

        // A manager whose `ApiVersions` has learned nothing at all: epoch -1 on both
        // sides, so the read is skipped and the absent feature map is never touched.
        let mut manager = TransactionManager::new(
            LogContext::empty(),
            Some(TRANSACTIONAL_ID.to_string()),
            TRANSACTION_TIMEOUT_MS,
            DEFAULT_RETRY_BACKOFF_MS,
            Arc::new(ApiVersions::new()),
            false,
        );
        manager.maybe_update_transaction_v2_enabled(false);
        assert!(!manager.is_transaction_v2_enabled());
    }

    /// Java 500-501: an upgrade observed *after* initialization requires a
    /// client-side epoch bump, so the old V1 epoch is fenced before the first V2
    /// transaction.
    ///
    /// The three suppressing conditions are pinned alongside it: an upgrade seen at
    /// initialization time, a level that does not change, and a *downgrade*.
    #[test]
    fn test_maybe_update_transaction_v2_enabled_requires_an_epoch_bump_only_on_a_late_upgrade() {
        let mut manager = transactional_manager(false);
        manager.maybe_update_transaction_v2_enabled(true);
        assert!(!manager.is_transaction_v2_enabled());

        finalize_transaction_version(manager.api_versions(), 2, 2);
        manager.maybe_update_transaction_v2_enabled(false);
        assert!(manager.is_transaction_v2_enabled());
        assert!(manager.client_side_epoch_bump_required());

        // Already enabled: a further read at a newer epoch does not re-arm the bump.
        let mut manager = transactional_manager(true);
        manager.maybe_update_transaction_v2_enabled(true);
        assert!(manager.is_transaction_v2_enabled());
        finalize_transaction_version(manager.api_versions(), 2, 3);
        manager.maybe_update_transaction_v2_enabled(false);
        assert!(manager.is_transaction_v2_enabled());
        assert!(!manager.client_side_epoch_bump_required());

        // A downgrade turns the flag off and requires no bump.
        let mut manager = transactional_manager(true);
        manager.maybe_update_transaction_v2_enabled(true);
        assert!(manager.is_transaction_v2_enabled());
        finalize_transaction_version(manager.api_versions(), 1, 4);
        manager.maybe_update_transaction_v2_enabled(false);
        assert!(!manager.is_transaction_v2_enabled());
        assert!(!manager.client_side_epoch_bump_required());
    }

    /// Phase 3's MILESTONE-11 GUARD is gone: a transactional manager is
    /// constructible and drives a full transaction.
    ///
    /// Pins the guard's *replacement* end to end — `initTransactions`,
    /// `beginTransaction`, a partition registration and the `AddPartitionsToTxn`
    /// round trip that confirms it — so nothing can silently regress the manager to
    /// refusing a transactional producer.
    #[tokio::test]
    async fn test_transactional_manager_is_constructible_and_drives_a_transaction() {
        let mut manager = transactional_manager(false);
        assert!(manager.is_transactional());
        assert_eq!(manager.transactional_id(), Some(TRANSACTIONAL_ID));
        assert_eq!(manager.current_state(), State::Uninitialized);

        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        manager
            .maybe_add_partition(&tp0())
            .expect("registering a new partition succeeds");
        assert!(manager.has_partitions_to_add());
        run_add_partitions_to_txn(&mut manager, &mut pending, &[(tp0(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&tp0()));
        assert!(manager.is_send_to_partition_allowed(&tp0()));
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

    /// `beginAbort` carries the caller's origin through to `transitionTo`, so an
    /// invalid `→ ABORTING_TRANSACTION` poisons on the Sender side and does not on
    /// the application side.
    ///
    /// The pair matters because `beginAbort` is the *only* transactional entry point
    /// Java reaches from both threads — `KafkaProducer.java:818` and
    /// `Sender.java:273` — and the Sender is currently this crate's only live caller
    /// (see [`TransactionManager::begin_abort`]). Hardcoding [`Caller::App`] there
    /// compiled, passed every other test, and silently dropped the poisoning contract
    /// Java's shutdown loop depends on (`Sender.java:269-271`), which is rules §1's
    /// named anti-pattern.
    ///
    /// # What this pair does and does not claim about reachability
    ///
    /// `READY` is used simply because it is an invalid source for
    /// `→ ABORTING_TRANSACTION` that the fixture reaches directly. It is **not** a
    /// state the shutdown window can present to `begin_abort` today, and an earlier
    /// revision of this comment said it was — the correction is Critic 45 5b pass 2.
    /// `State::Ready` has exactly two production writers,
    /// [`TransactionManager::reset_transaction_state`] and
    /// [`TransactionManager::handle_init_producer_id_response`], **both**
    /// [`Caller::Sender`]; and `reset_transaction_state`'s only callers
    /// ([`TransactionManager::next_request`] and
    /// `handle_end_txn_response`) run on the Sender's own response path inside
    /// `run_once`, so the shutdown loop re-evaluates `has_ongoing_transaction()` and
    /// *exits* before reaching `begin_abort`. No interleaving delivers `READY` here.
    ///
    /// Nor is some other state a live path: `COMMITTING_TRANSACTION` — the app calling
    /// `commit_transaction` in the window, and not a valid source — is intercepted by
    /// [`TransactionManager::handle_cached_transaction_request_result`]'s
    /// pending-transition guard before the supplier runs, and
    /// `prepare_transaction` lands on `PREPARED_TRANSACTION`, which *is* valid.
    ///
    /// So what the pair pins is a **contract**, not a live path: the poisoning
    /// asymmetry must already hold when Phase 6 opens the application-side caller
    /// (`KafkaProducer.abortTransaction`) and removes
    /// `KafkaProducer::new`'s `transactional.id` guard. That is worth as much
    /// — it is the guarantee Java's shutdown loop is written against
    /// (`Sender.java:269-271`) — and it is why an unreachable-today invalid source is
    /// a fine choice.
    #[tokio::test]
    async fn test_begin_abort_poisons_only_on_the_sender_side() {
        let expected = format!(
            "TransactionalId {TRANSACTIONAL_ID}: Invalid transition attempted from state READY to state \
             ABORTING_TRANSACTION"
        );

        // Application side: the error is returned and nothing moves.
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(manager.is_ready());
        let error = manager
            .begin_abort(&mut pending, Caller::App)
            .expect_err("READY -> ABORTING_TRANSACTION is invalid");
        assert_eq!(error.message(), expected);
        assert_eq!(manager.current_state(), State::Ready);
        assert!(manager.last_error().is_none());
        assert!(!manager.has_error());

        // Sender side: the state is poisoned to FATAL_ERROR and the error recorded,
        // before the same error propagates (Java 1124-1127).
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(manager.is_ready());
        let error = manager
            .begin_abort(&mut pending, Caller::Sender)
            .expect_err("READY -> ABORTING_TRANSACTION is invalid");
        assert_eq!(error.message(), expected);
        assert_eq!(manager.current_state(), State::FatalError);
        assert!(manager.has_fatal_error());
        assert_eq!(manager.last_error().expect("poisoned").message(), expected);
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
            assert_eq!(error.message(), format!("Cannot transition to {name} with a null error"));
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
                .expect("next_request does not fail on this path")
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
            // Java records the condition in the state machine, not on the error:
            // `hasAbortableError()` == `currentState == ABORTABLE_ERROR`, and the
            // javadoc's answer to the thrown `KafkaException` is to abort.
            assert!(manager.has_abortable_error());
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
                .expect("next_request does not fail on this path")
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
                    TransactionManager::NO_INFLIGHT_REQUEST_CORRELATION_ID
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
                    TransactionManager::NO_INFLIGHT_REQUEST_CORRELATION_ID
                ),
                SenderPhaseOutcome::ReturnedOnTransactionalRequest
            );
            assert_eq!(manager.current_state(), State::Initializing);
            let handler = manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
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
            .transition_to_abortable_error(Error::new(Errors::ClusterAuthorizationFailed), Caller::Sender)
            .expect("INITIALIZING -> ABORTABLE_ERROR is valid");
        let queued_result = manager.force_enqueue_init_producer_id_for_test(&mut pending);
        assert!(!pending.is_empty());
        // `Sender.java:354` passes `new AuthenticationException(exception)`, so the
        // class is `Error::Authentication` — see
        // `Sender::handle_authorization_error`, and
        // `sender::tests::handle_authorization_error_fails_pending_requests_with_an_authentication_error`
        // which pins it. The same error value drives all three sub-cases below; what
        // distinguishes them is the state each one transitions into — abortable
        // (recoverable) versus the two fatal paths.
        let authentication_error =
            Error::Authentication(crate::common::errors::AuthenticationError::new("authentication failed"));
        manager
            .fail_pending_requests(&mut pending, &authentication_error, Caller::Sender)
            .expect("ABORTABLE_ERROR self-loop is valid");
        assert!(queued_result.is_completed());
        assert_eq!(queued_result.error().expect("failed").message(), "authentication failed");
        // `fail_pending_requests` → `abortableError`: an abortable error is
        // recoverable, so the manager must NOT land in FATAL_ERROR.
        assert!(!manager.has_fatal_error(), "an abortable error must not be fatal");
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
        // `authentication_failed` → per-handler `fatalError`: one transition into
        // FATAL_ERROR, and the raw error reaches both the handler's result and
        // `last_error` unchanged (Java stores a plain RuntimeException).
        assert!(manager.has_fatal_error(), "authentication_failed transitions to FATAL_ERROR");
        assert_eq!(
            queued_result.error().expect("failed").message(),
            "authentication failed",
            "the handler's result carries the raw error"
        );
        assert_eq!(
            manager.last_error().expect("recorded").message(),
            "authentication failed",
            "and so does last_error"
        );

        // close → fatalError with Java's message.
        let mut manager = idempotent_manager(false);
        let mut pending = PendingRequests::new();
        let queued_result = manager.force_enqueue_init_producer_id_for_test(&mut pending);
        manager
            .close(&mut pending, Caller::Sender)
            .expect("FATAL_ERROR is always a valid target");
        // `close` builds its own error and fails handlers/pending directly, so the
        // same message must reach both objects, and the transition records the
        // fatality once.
        assert!(manager.has_fatal_error(), "close transitions to FATAL_ERROR");
        assert_eq!(
            queued_result.error().expect("failed").message(),
            "The producer closed forcefully",
            "the handler's result carries close's own error"
        );
        assert_eq!(
            manager.last_error().expect("recorded").message(),
            "The producer closed forcefully",
            "and so does last_error"
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
                .expect("next_request does not fail on this path")
                .expect("an InitProducerId request is pending");
            let result = Arc::clone(handler.result());

            complete_init_producer_id(&mut manager, &mut pending, handler, error_code, -1, -1)
                .expect("the error is handled");

            assert!(manager.has_fatal_error());
            let last_error = manager.last_error().expect("recorded");
            assert_eq!(
                last_error.error(),
                Errors::ProducerFenced,
                "INVALID_PRODUCER_EPOCH is reported as PRODUCER_FENCED"
            );
            // The state is fatal, so the error the app observes must report it
            // producer is dead: the state is FATAL_ERROR and the app-visible error
            // is typed `ProducerFenced`, which is how Java tells the caller not to
            // retry (`maybeFailWithError` rethrows `ProducerFencedException`).
            assert!(manager.has_fatal_error(), "the manager must be fatal for {error_code:?}");
            let app_error = result.error().expect("failed");
            assert_eq!(app_error.error(), Errors::ProducerFenced);
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
            .expect("next_request does not fail on this path")
            .expect("an InitProducerId request is pending");
        let result = Arc::clone(handler.result());

        complete_init_producer_id(&mut manager, &mut pending, handler, Errors::CoordinatorLoadInProgress, -1, -1)
            .expect("a retriable error is handled");

        assert!(!result.is_completed(), "a retried request must not complete");
        assert!(!manager.has_error());
        assert!(!pending.is_empty());
        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("the request was re-enqueued");
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
            .expect("next_request does not fail on this path")
            .expect("an InitProducerId request is pending");

        complete_init_producer_id(&mut manager, &mut pending, handler, Errors::InvalidRequest, -1, -1)
            .expect("the error is handled");

        assert!(manager.has_fatal_error());
        let last_error = manager.last_error().expect("recorded");
        assert_eq!(
            last_error.message(),
            format!(
                "Unexpected error in InitProducerIdResponse; {}",
                Errors::InvalidRequest.message()
            )
        );
        // Java 1536: `new KafkaException(..)` — bare, so not an `ApiException`.
        assert!(matches!(last_error, Error::KafkaError(_)), "got {last_error:?}");
        assert!(last_error.is_kafka_error(), "Java throws KafkaException here");
        assert!(!last_error.is_api_error(), "a bare KafkaException is not an ApiException");
        // Stamping fatal preserves the custom message and reports fatal.
        assert!(manager.has_fatal_error(), "an unexpected InitProducerId error must be fatal");
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
            .expect("next_request does not fail on this path")
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
            .transition_to_fatal_error(bare_kafka_error(), Caller::Sender)
            .expect("FATAL_ERROR is always a valid target");

        assert!(!pending.is_empty());
        assert!(
            manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .is_none(),
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
            .expect_err("a queued partition with no entry is Java's illegal-state case");
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
                .transition_to_fatal_error(bare_kafka_error(), Caller::App)
                .expect("FATAL_ERROR is always a valid target");
            let error = manager.maybe_add_partition(&tp0()).expect_err("a fatal error fails the send");
            assert_eq!(
                error.message(),
                "Cannot execute transactional method because we are in an error state"
            );
            // librdkafka keeps fatal and requires-abort disjoint (CLAUDE.md
            // §10.3): a fatal-state error must NOT tell the app to abort.
            assert!(!manager.has_abortable_error());
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
        assert!(matches!(error, Error::LocalIllegalState(_)));
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
        assert!(matches!(error, Error::LocalIllegalState(_)));
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
            .transition_to_abortable_error(bare_kafka_error(), Caller::App)
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
            .transition_to_fatal_error(bare_kafka_error(), Caller::App)
            .expect("FATAL_ERROR is always a valid target");

        let error = manager.maybe_add_partition(&tp0()).expect_err("a fatal error fails the send");
        assert_eq!(
            error.message(),
            "Cannot execute transactional method because we are in an error state"
        );
    }

    /// `maybe_fail_with_error` picks the exception it hands the application by the
    /// *type* of `last_error`, exactly as Java's `maybeFailWithError`
    /// (`TransactionManager.java:1096-1112`) does: `ProducerFencedException` and
    /// `InvalidProducerEpochException` are rethrown as themselves,
    /// `IllegalStateException` is rethrown as an `IllegalStateException` naming the
    /// invalid transition, and anything else becomes a bare `KafkaException`.
    ///
    /// This asserts the returned value for each of the three reachable arms — the
    /// falsifiable part. The manager's state is checked once per arm as the
    /// precondition that got us there, not as the subject: `has_fatal_error()` and
    /// `has_abortable_error()` both read the single `current_state` field, so
    /// asserting they disagree would only restate that a Rust enum holds one
    /// variant.
    ///
    /// The poison arm is the behaviour that changed when the librdkafka-style
    /// `into_fatal` stamp was removed: an `LocalIllegalState` reaching
    /// `transition_to_fatal_error` now stays `LocalIllegalState` instead of being
    /// promoted to a wire-code error, which is what Java does.
    #[tokio::test]
    async fn test_maybe_fail_with_error_picks_the_error_java_throws() {
        // Fatal state (a producer fence): FATAL_ERROR, and NOT abortable.
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager
            .transition_to_fatal_error(Error::new(Errors::ProducerFenced), Caller::App)
            .expect("FATAL_ERROR is always a valid target");
        assert!(manager.has_fatal_error(), "precondition: the fence lands in FATAL_ERROR");
        let fatal = manager.maybe_fail_with_error().expect_err("a fatal state fails the operation");
        // Java rethrows `ProducerFencedException` itself, not a wrapping KafkaException.
        assert!(matches!(fatal, Error::ProducerFenced(_)), "got {fatal:?}");
        assert_eq!(fatal.error(), Errors::ProducerFenced);
        assert_eq!(
            fatal.message(),
            format!(
                "Producer with transactionalId '{TRANSACTIONAL_ID}' and \
                 (producerId={PRODUCER_ID}, epoch={EPOCH}) has been fenced by another producer \
                 with the same transactionalId"
            )
        );

        // Abortable state: ABORTABLE_ERROR, and NOT fatal.
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        manager
            .transition_to_abortable_error(bare_kafka_error(), Caller::App)
            .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");
        assert!(manager.has_abortable_error(), "precondition: the manager is ABORTABLE_ERROR");
        let abortable = manager
            .maybe_fail_with_error()
            .expect_err("an abortable state fails the operation");
        // Java's final `else`: a bare KafkaException, which carries no wire code.
        assert_eq!(abortable.error(), Errors::UnknownServerError, "got {abortable:?}");
        assert_eq!(
            abortable.message(),
            "Cannot execute transactional method because we are in an error state"
        );

        // Poison path (KAFKA-14831): a Sender-side invalid transition moves the
        // manager to FATAL_ERROR and stores an `LocalIllegalState` `last_error`, which
        // `maybe_fail_with_error` rebuilds as a fresh `LocalIllegalState` — matching Java's
        // `maybeFailWithError`, which rethrows `IllegalStateException` there. The
        // fatality of the situation lives in the state, asserted below.
        let mut manager = idempotent_manager(false);
        manager
            .transition_to(State::Ready, None, Caller::Sender)
            .expect_err("UNINITIALIZED -> READY is invalid and poisons on the Sender side");
        assert!(manager.has_fatal_error(), "the poison transition lands in FATAL_ERROR");
        let poisoned = manager
            .maybe_fail_with_error()
            .expect_err("the poisoned state fails the operation");
        // Stays `LocalIllegalState` — Java rethrows `IllegalStateException` here
        // (`TransactionManager.java:1104-1107`). Under the removed `into_fatal`
        // stamp this was promoted to a wire-code error instead.
        assert!(matches!(poisoned, Error::LocalIllegalState(_)), "got {poisoned:?}");
        assert!(
            poisoned
                .message()
                .contains("cannot execute transactional method because of previous invalid state transition attempt"),
            "got {}",
            poisoned.message()
        );
    }

    /// The bare-`KafkaException` arm of `maybe_fail_with_error` carries `last_error`
    /// as its source, so an application can tell a **fatal** condition from an
    /// **abortable** one — which that arm's message and code cannot express, being
    /// identical for both.
    ///
    /// Java does this with `new KafkaException(msg, lastError)` and the caller's
    /// `e.getCause() instanceof ClusterAuthorizationException`
    /// (`TransactionManager.java:1112`).
    #[tokio::test]
    async fn test_error_state_error_carries_last_error_as_its_source() {
        // Fatal: a cluster-authorization failure the producer cannot recover from.
        let mut fatal_mgr = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut fatal_mgr, &mut pending, PRODUCER_ID, EPOCH).await;
        fatal_mgr
            .transition_to_fatal_error(Error::new(Errors::ClusterAuthorizationFailed), Caller::App)
            .expect("FATAL_ERROR is always a valid target");
        let fatal = fatal_mgr
            .maybe_fail_with_error()
            .expect_err("a fatal state fails the operation");

        // Abortable: an invalid transaction state, which `abort_transaction` clears.
        let mut abortable_mgr = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut abortable_mgr, &mut pending, PRODUCER_ID, EPOCH).await;
        abortable_mgr.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        abortable_mgr
            .transition_to_abortable_error(Error::new(Errors::InvalidTxnState), Caller::App)
            .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");
        let abortable = abortable_mgr
            .maybe_fail_with_error()
            .expect_err("an abortable state fails the operation");

        // The wrapper itself is indistinguishable — this is why the source matters.
        assert_eq!(fatal.error(), abortable.error());
        assert_eq!(fatal.message(), abortable.message());

        // The source tells them apart.
        assert_eq!(
            fatal.source().expect("the fatal cause is carried").error(),
            Errors::ClusterAuthorizationFailed
        );
        assert_eq!(
            abortable.source().expect("the abortable cause is carried").error(),
            Errors::InvalidTxnState
        );

        // And it is reachable through the std trait, so `Box<dyn Error>` chains work.
        use std::error::Error as StdError;
        assert!(StdError::source(&fatal).is_some());
    }

    /// Drives `sendOffsetsToTransaction` to the point where the `TxnOffsetCommit` is
    /// pending against a discovered group coordinator, and returns the caller's
    /// handle.
    ///
    /// The shared prologue of the `testRetriableErrorInTxnOffsetCommit` /
    /// `testFatalErrorInTxnOffsetCommit` families (Java 2487-2508, 2545-2566) and of
    /// the three `*ByGroupMetadata` tests.
    async fn send_offsets_and_discover_group_coordinator(
        manager: &mut TransactionManager,
        coordinators: &mut CoordinatorNodes,
        pending: &mut PendingRequests,
        group_metadata: ConsumerGroupMetadata,
        offsets: &[(TopicPartition, i64)],
    ) -> Arc<TransactionalRequestResult> {
        do_init_transactions(manager, pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        let offsets: HashMap<TopicPartition, OffsetAndMetadata> = offsets
            .iter()
            .map(|(partition, value)| (partition.clone(), offset(*value)))
            .collect();
        let add_offsets_result = manager
            .send_offsets_to_transaction(offsets, group_metadata, pending)
            .expect("sendOffsetsToTransaction is valid in IN_TRANSACTION");
        run_add_offsets_to_txn(manager, coordinators, pending, CONSUMER_GROUP_ID, Errors::None)
            .expect("a successful AddOffsetsToTxn response is handled");
        // The request should complete only after the TxnOffsetCommit completes.
        assert!(!add_offsets_result.is_completed());
        assert!(manager.has_pending_offset_commits());

        discover_group_coordinator(manager, coordinators, pending, CONSUMER_GROUP_ID)
            .expect("the group coordinator is discovered");
        add_offsets_result
    }

    /// The shared body of `testRetriableErrorInTxnOffsetCommit` (Java 2487).
    ///
    /// A retriable per-partition error leaves that partition in
    /// `pendingTxnOffsetCommits` and re-enqueues the request, so the caller's handle
    /// only completes once every offset has been accepted.
    async fn retriable_error_in_txn_offset_commit(error: Errors) {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        let add_offsets_result = send_offsets_and_discover_group_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            consumer_group_metadata(),
            &[(tp0(), 1), (tp1(), 1)],
        )
        .await;

        run_txn_offset_commit(
            &mut manager,
            &mut coordinators,
            &mut pending,
            CONSUMER_GROUP_ID,
            &[(tp0(), Errors::None), (tp1(), error)],
        )
        .expect("a retriable TxnOffsetCommit response re-enqueues the request");
        // The TxnOffsetCommit failed.
        assert!(manager.has_pending_offset_commits());
        // We should only be done after both RPCs complete successfully.
        assert!(!add_offsets_result.is_completed());

        run_txn_offset_commit(
            &mut manager,
            &mut coordinators,
            &mut pending,
            CONSUMER_GROUP_ID,
            &[(tp0(), Errors::None), (tp1(), Errors::None)],
        )
        .expect("a successful TxnOffsetCommit response completes the result");
        assert!(add_offsets_result.is_completed());
        assert!(add_offsets_result.is_successful());
    }

    /// Translated from `testHandlingOfUnknownTopicPartitionErrorOnTxnOffsetCommit`
    /// (Java 2472-2475).
    #[tokio::test]
    async fn test_handling_of_unknown_topic_partition_error_on_txn_offset_commit() {
        retriable_error_in_txn_offset_commit(Errors::UnknownTopicOrPartition).await;
    }

    /// Translated from `testHandlingOfCoordinatorLoadingErrorOnTxnOffsetCommit`
    /// (Java 2477-2480).
    #[tokio::test]
    async fn test_handling_of_coordinator_loading_error_on_txn_offset_commit() {
        retriable_error_in_txn_offset_commit(Errors::CoordinatorLoadInProgress).await;
    }

    /// Translated from `testHandlingOfNetworkExceptionOnTxnOffsetCommit`
    /// (Java 2482-2485).
    #[tokio::test]
    async fn test_handling_of_network_error_on_txn_offset_commit() {
        retriable_error_in_txn_offset_commit(Errors::NetworkError).await;
    }

    /// The shared body of `testFatalErrorInTxnOffsetCommit(Errors, Errors)`
    /// (Java 2547).
    ///
    /// `resulting_error` differs from `triggered_error` only for
    /// `INVALID_PRODUCER_EPOCH`, which the handler converts to `PRODUCER_FENCED`
    /// (Java 1936-1939).
    async fn fatal_error_in_txn_offset_commit(triggered_error: Errors, resulting_error: Errors) {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        let add_offsets_result = send_offsets_and_discover_group_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            consumer_group_metadata(),
            &[(tp0(), 1), (tp1(), 1)],
        )
        .await;

        run_txn_offset_commit(
            &mut manager,
            &mut coordinators,
            &mut pending,
            CONSUMER_GROUP_ID,
            &[(tp0(), Errors::None), (tp1(), triggered_error)],
        )
        .expect("a fatal TxnOffsetCommit response fails the result");
        assert!(add_offsets_result.is_completed());
        assert!(!add_offsets_result.is_successful());
        // Java asserts on the exception *class*; the wire code is the closest
        // reviewable equivalent (PLAN §10.5 deviation 5).
        assert_eq!(
            add_offsets_result.error().expect("the result carries an error").error(),
            resulting_error
        );
    }

    /// Translated from `testHandlingOfProducerFencedErrorOnTxnOffsetCommit`
    /// (Java 2522-2525).
    #[tokio::test]
    async fn test_handling_of_producer_fenced_error_on_txn_offset_commit() {
        fatal_error_in_txn_offset_commit(Errors::ProducerFenced, Errors::ProducerFenced).await;
    }

    /// Translated from
    /// `testHandlingOfTransactionalIdAuthorizationFailedErrorOnTxnOffsetCommit`
    /// (Java 2527-2530).
    #[tokio::test]
    async fn test_handling_of_transactional_id_authorization_failed_error_on_txn_offset_commit() {
        fatal_error_in_txn_offset_commit(
            Errors::TransactionalIdAuthorizationFailed,
            Errors::TransactionalIdAuthorizationFailed,
        )
        .await;
    }

    /// Translated from `testHandlingOfInvalidProducerEpochErrorOnTxnOffsetCommit`
    /// (Java 2532-2535).
    #[tokio::test]
    async fn test_handling_of_invalid_producer_epoch_error_on_txn_offset_commit() {
        fatal_error_in_txn_offset_commit(Errors::InvalidProducerEpoch, Errors::ProducerFenced).await;
    }

    /// Translated from
    /// `testHandlingOfUnsupportedForMessageFormatErrorOnTxnOffsetCommit`
    /// (Java 2537-2540).
    #[tokio::test]
    async fn test_handling_of_unsupported_for_message_format_error_on_txn_offset_commit() {
        fatal_error_in_txn_offset_commit(Errors::UnsupportedForMessageFormat, Errors::UnsupportedForMessageFormat)
            .await;
    }

    /// Translated from `testFencedInstanceIdInTxnOffsetCommitByGroupMetadata`
    /// (Java 1158-1189), minus its `assertAbortableError` tail, which needs a
    /// `beginCommit` / `beginAbort` pair — covered by
    /// [`assert_abortable_error`] in the tests that use it.
    #[tokio::test]
    async fn test_fenced_instance_id_in_txn_offset_commit_by_group_metadata() {
        let fenced_member_id = "fenced_member";
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        #[allow(deprecated)]
        let group_metadata = ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id(
            CONSUMER_GROUP_ID,
            GENERATION_ID,
            fenced_member_id,
            Some(GROUP_INSTANCE_ID.to_string()),
        );
        let partition = TopicPartition::new("foo".to_string(), 0);
        let send_offsets_result = send_offsets_and_discover_group_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            group_metadata.clone(),
            &[(partition.clone(), 39)],
        )
        .await;

        run_txn_offset_commit_with_group_metadata(
            &mut manager,
            &mut coordinators,
            &mut pending,
            CONSUMER_GROUP_ID,
            Some(&group_metadata),
            &[(partition, Errors::FencedInstanceId)],
        )
        .expect("a FENCED_INSTANCE_ID response moves to an abortable error");

        assert_eq!(
            manager.last_error().expect("an abortable error is recorded").error(),
            Errors::FencedInstanceId
        );
        assert!(send_offsets_result.is_completed());
        assert!(!send_offsets_result.is_successful());
        assert_eq!(
            send_offsets_result.error().expect("the result carries an error").error(),
            Errors::FencedInstanceId
        );
        assert!(manager.has_abortable_error());
    }

    /// Translated from `testUnknownMemberIdInTxnOffsetCommitByGroupMetadata`
    /// (Java 1191-1223) and `testIllegalGenerationInTxnOffsetCommitByGroupMetadata`
    /// (Java 1225-1257), which differ only in the triggering code: both map to
    /// Java's `CommitFailedException` (Java 1929-1931).
    ///
    /// `CommitFailedException` extends `KafkaException` *directly* — so it is a
    /// `KafkaException` that is NOT an `ApiException` — and it has no wire code. That
    /// is exactly [`ConsumerCommitFailedError`], which is what the arm builds; the
    /// message is asserted exactly too (`definition-of-done.md` §3).
    #[tokio::test]
    async fn test_group_metadata_mismatch_in_txn_offset_commit_by_group_metadata() {
        for (member_id, generation_id, error, expected_message) in [
            (
                "unknownMember",
                GENERATION_ID,
                Errors::UnknownMemberId,
                "Transaction offset Commit failed due to consumer group metadata mismatch: The coordinator is not \
                 aware of this member.",
            ),
            (
                MEMBER_ID,
                1,
                Errors::IllegalGeneration,
                "Transaction offset Commit failed due to consumer group metadata mismatch: Specified group generation \
                 id is not valid.",
            ),
        ] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            let mut coordinators = CoordinatorNodes::new();
            #[allow(deprecated)]
            let group_metadata = ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id(
                CONSUMER_GROUP_ID,
                generation_id,
                member_id,
                None,
            );
            let partition = TopicPartition::new("foo".to_string(), 0);
            let send_offsets_result = send_offsets_and_discover_group_coordinator(
                &mut manager,
                &mut coordinators,
                &mut pending,
                group_metadata.clone(),
                &[(partition.clone(), 39)],
            )
            .await;

            run_txn_offset_commit_with_group_metadata(
                &mut manager,
                &mut coordinators,
                &mut pending,
                CONSUMER_GROUP_ID,
                Some(&group_metadata),
                &[(partition, error)],
            )
            .expect("a group-metadata mismatch moves to an abortable error");

            let last_error = manager.last_error().expect("an abortable error is recorded");
            assert_eq!(last_error.message(), expected_message);
            // Java 1923: `new CommitFailedException(..)`. It extends `KafkaException`
            // directly, so `is_kafka_error()` is `true` and `is_api_error()` is `false`.
            assert!(
                matches!(last_error, Error::ConsumerCommitFailed(_)),
                "expected ConsumerCommitFailed, got {last_error:?}"
            );
            assert!(last_error.is_kafka_error(), "CommitFailedException extends KafkaException");
            assert!(!last_error.is_api_error(), "CommitFailedException is not an ApiException");
            assert!(send_offsets_result.is_completed());
            assert!(!send_offsets_result.is_successful());
            assert_eq!(
                send_offsets_result.error().expect("the result carries an error").message(),
                expected_message
            );
            assert!(manager.has_abortable_error());
        }
    }

    /// KIP-939 `prepareTransaction()` (Java 342) moves `IN_TRANSACTION` to
    /// `PREPARED_TRANSACTION` and records the current producer id and epoch, which
    /// `preparedTransactionState()` (Java 1976) then hands to an external
    /// coordinator. From `PREPARED_TRANSACTION` both `COMMITTING_TRANSACTION` and
    /// `ABORTING_TRANSACTION` remain reachable (Java 174, 176).
    ///
    /// `TransactionManagerTest` has **no** two-phase-commit test in Apache Kafka
    /// 4.2 — `prepareTransaction`, `preparedTransactionState` and `enable2pc` appear
    /// in no method body, and `doInitTransactionsWith2PCEnabled` is declared and
    /// never called (the accounting block below carries the check). So this and the
    /// two tests after it are Rust-side, covering a Java surface Java itself leaves
    /// untested; PLAN §Phase-6 owns the `KafkaProducerTest` cover.
    #[tokio::test]
    async fn test_prepare_transaction_records_the_prepared_state() {
        let mut manager = manager_with_transactional_id_and_2pc(Some(TRANSACTIONAL_ID.to_string()), false, true);
        let mut pending = PendingRequests::new();
        assert!(manager.is_2pc_enabled());
        assert_eq!(manager.prepared_transaction_state(), ProducerIdAndEpoch::NONE);

        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(!manager.is_prepared());

        // A partition must be added while IN_TRANSACTION — `maybeAddPartition`'s
        // second guard rejects any other state (Java 445) — and it is what sets
        // `transactionStarted`, without which `nextRequest` short-circuits the
        // `EndTxn` (Java 913).
        manager.maybe_add_partition(&tp0()).expect("a new partition is registered");
        run_add_partitions_to_txn(&mut manager, &mut pending, &[(tp0(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");

        manager
            .prepare_transaction()
            .expect("IN_TRANSACTION -> PREPARED_TRANSACTION is valid");
        assert!(manager.is_prepared());
        assert_eq!(
            manager.prepared_transaction_state(),
            ProducerIdAndEpoch::new(PRODUCER_ID, EPOCH)
        );
        // Java 1012: a prepared transaction is not "ongoing" — it is neither
        // IN_TRANSACTION nor completing.
        assert!(!manager.has_ongoing_transaction());

        // A commit is still reachable from PREPARED_TRANSACTION (Java 174), and
        // completing it clears the prepared state (Java 1342).
        manager
            .begin_commit(&mut pending)
            .expect("PREPARED_TRANSACTION -> COMMITTING is valid");
        run_end_txn_v4(&mut manager, &mut pending, TransactionResult::Commit, Errors::None)
            .expect("a successful EndTxn response is handled");
        assert_eq!(manager.prepared_transaction_state(), ProducerIdAndEpoch::NONE);
        assert!(!manager.is_prepared());
    }

    /// `prepareTransaction` outside a transaction is refused by the transition
    /// table: `→ PREPARED_TRANSACTION` has only `IN_TRANSACTION` and `INITIALIZING`
    /// as sources (Java 172).
    #[tokio::test]
    async fn test_prepare_transaction_is_refused_outside_a_transaction() {
        let mut manager = manager_with_transactional_id_and_2pc(Some(TRANSACTIONAL_ID.to_string()), false, true);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;

        let error = manager
            .prepare_transaction()
            .expect_err("READY -> PREPARED_TRANSACTION is not a valid transition");
        assert!(matches!(error, Error::LocalIllegalState(_)), "unexpected error: {error:?}");
        assert_eq!(manager.prepared_transaction_state(), ProducerIdAndEpoch::NONE);

        // An idempotent producer is refused earlier, by `ensureTransactional`.
        let mut idempotent = idempotent_manager(false);
        let error = idempotent
            .prepare_transaction()
            .expect_err("prepareTransaction is a transactional method");
        assert_eq!(error.message(), "Transactional method invoked on a non-transactional producer.");
    }

    /// `InitProducerIdHandler`'s `keepPreparedTxn` arm (Java 1501-1511): when the
    /// broker reports an ongoing transaction, the producer resumes it in
    /// `PREPARED_TRANSACTION` with the *ongoing* pid and epoch rather than going to
    /// `READY`.
    ///
    /// Note the two producer-id pairs differ: `setProducerIdAndEpoch` takes the
    /// response's `producerId`/`producerEpoch` (Java 1495-1498) while
    /// `preparedTxnState` takes its `ongoingTxnProducerId`/`ongoingTxnProducerEpoch`
    /// (Java 1507-1510), which is what lets the external coordinator finish the old
    /// transaction under the epoch it was prepared with.
    #[tokio::test]
    async fn test_init_producer_id_resumes_a_prepared_transaction() {
        let mut manager = manager_with_transactional_id_and_2pc(Some(TRANSACTIONAL_ID.to_string()), false, true);
        let mut pending = PendingRequests::new();
        do_init_transactions_with_2pc_enabled(&mut manager, &mut pending, true).await;

        assert!(manager.is_prepared());
        assert_eq!(
            manager.producer_id_and_epoch(),
            ProducerIdAndEpoch::new(ONGOING_PRODUCER_ID, BUMPED_ONGOING_EPOCH)
        );
        assert_eq!(
            manager.prepared_transaction_state(),
            ProducerIdAndEpoch::new(ONGOING_PRODUCER_ID, BUMPED_ONGOING_EPOCH - 1)
        );

        // Without an ongoing transaction the same flag leaves the producer READY.
        let mut manager = manager_with_transactional_id_and_2pc(Some(TRANSACTIONAL_ID.to_string()), false, true);
        let mut pending = PendingRequests::new();
        do_init_transactions_with_2pc_enabled(&mut manager, &mut pending, false).await;
        assert!(!manager.is_prepared());
        assert!(manager.is_ready());
        assert_eq!(manager.prepared_transaction_state(), ProducerIdAndEpoch::NONE);
    }

    /// Translated from `testEndTxnNotSentIfIncompleteBatches` (Java 248-260).
    ///
    /// `nextRequest(true)` — incomplete batches outstanding — must withhold the
    /// `EndTxn` (Java 902-903); `nextRequest(false)` releases it.
    #[tokio::test]
    async fn test_end_txn_not_sent_if_incomplete_batches() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        manager.maybe_add_partition(&tp0()).expect("a new partition is registered");
        run_add_partitions_to_txn(&mut manager, &mut pending, &[(tp0(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&tp0()));

        manager
            .begin_commit(&mut pending)
            .expect("IN_TRANSACTION -> COMMITTING is valid");
        assert!(
            manager
                .next_request(&mut pending, true)
                .expect("next_request does not fail on this path")
                .is_none()
        );
        assert!(
            manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .expect("the EndTxn is released once the batches are flushed")
                .is_end_txn()
        );
    }

    /// Translated from `testHasOngoingTransactionSuccessfulCommit` (Java 327-350).
    #[tokio::test]
    async fn test_has_ongoing_transaction_successful_commit() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();

        assert!(!manager.has_ongoing_transaction());
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(!manager.has_ongoing_transaction());

        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(manager.has_ongoing_transaction());

        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        assert!(manager.has_ongoing_transaction());

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(partition.clone(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&partition));

        manager
            .begin_commit(&mut pending)
            .expect("IN_TRANSACTION -> COMMITTING is valid");
        assert!(manager.has_ongoing_transaction());

        run_end_txn_v4(&mut manager, &mut pending, TransactionResult::Commit, Errors::None)
            .expect("a successful EndTxn response is handled");
        assert!(!manager.has_ongoing_transaction());
    }

    /// Translated from `testHasOngoingTransactionSuccessfulAbort` (Java 303-325).
    #[tokio::test]
    async fn test_has_ongoing_transaction_successful_abort() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();

        assert!(!manager.has_ongoing_transaction());
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(!manager.has_ongoing_transaction());

        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(manager.has_ongoing_transaction());

        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        assert!(manager.has_ongoing_transaction());

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(partition.clone(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&partition));

        manager
            .begin_abort(&mut pending, Caller::App)
            .expect("IN_TRANSACTION -> ABORTING is valid");
        assert!(manager.has_ongoing_transaction());

        run_end_txn_v4(&mut manager, &mut pending, TransactionResult::Abort, Errors::None)
            .expect("a successful EndTxn response is handled");
        assert!(!manager.has_ongoing_transaction());
    }

    /// Translated from `testHasOngoingTransactionAbortableError` (Java 351-377).
    #[tokio::test]
    async fn test_has_ongoing_transaction_abortable_error() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();

        assert!(!manager.has_ongoing_transaction());
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(!manager.has_ongoing_transaction());

        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(manager.has_ongoing_transaction());

        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        assert!(manager.has_ongoing_transaction());

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(partition.clone(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&partition));

        manager
            .transition_to_abortable_error(bare_kafka_error(), Caller::App)
            .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");
        assert!(manager.has_ongoing_transaction());

        // `beginAbort` skips `maybeFailWithError` in ABORTABLE_ERROR (Java 363).
        manager
            .begin_abort(&mut pending, Caller::App)
            .expect("ABORTABLE_ERROR -> ABORTING is valid");
        assert!(manager.has_ongoing_transaction());

        run_end_txn_v4(&mut manager, &mut pending, TransactionResult::Abort, Errors::None)
            .expect("a successful EndTxn response is handled");
        assert!(!manager.has_ongoing_transaction());
    }

    /// `EndTxnHandler`'s abort-specific arm (Java 1784-1787): when an *abort* itself
    /// gets `TRANSACTION_ABORTABLE`, retrying the abort would cycle, so Java converts
    /// it to a **fatal** bare `KafkaException("Failed to abort transaction", cause)`.
    ///
    /// `TransactionManagerTest` has no cover for this arm —
    /// `testTransactionAbortableExceptionInEndTxn` (Java 3925) drives a *commit*, so
    /// it takes the `abortableError` arm at `:1789` instead. Both halves of Java's
    /// value are asserted: the message is exactly "Failed to abort transaction" (the
    /// cause is NOT folded into it) and `source()` is the wire error.
    #[tokio::test]
    async fn test_abortable_error_while_aborting_is_fatal_with_the_cause_attached() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();

        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        run_add_partitions_to_txn(&mut manager, &mut pending, &[(partition, Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        manager
            .begin_abort(&mut pending, Caller::App)
            .expect("IN_TRANSACTION -> ABORTING is valid");

        run_end_txn_v4(
            &mut manager,
            &mut pending,
            TransactionResult::Abort,
            Errors::TransactionAbortable,
        )
        .expect("the error is handled, not propagated");

        assert!(
            manager.has_fatal_error(),
            "retrying the abort would cycle, so Java treats this as fatal"
        );
        let last_error = manager.last_error().expect("recorded");
        assert_eq!(
            last_error.message(),
            "Failed to abort transaction",
            "Java's message does not carry the cause's text"
        );
        // Java 1787: `new KafkaException(msg, error.exception())` — bare, so not an
        // `ApiException`.
        assert!(matches!(last_error, Error::KafkaError(_)), "got {last_error:?}");
        assert!(last_error.is_kafka_error(), "Java throws KafkaException here");
        assert!(!last_error.is_api_error(), "a bare KafkaException is not an ApiException");
        let cause =
            crate::common::error::ErrorSource::source(last_error).expect("Java passes error.exception() as the cause");
        assert_eq!(cause.error(), Errors::TransactionAbortable, "got {cause:?}");
    }

    /// Translated from `testHasOngoingTransactionFatalError` (Java 378-397).
    #[tokio::test]
    async fn test_has_ongoing_transaction_fatal_error() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();

        assert!(!manager.has_ongoing_transaction());
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(!manager.has_ongoing_transaction());

        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(manager.has_ongoing_transaction());

        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        assert!(manager.has_ongoing_transaction());

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(partition.clone(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&partition));

        manager
            .transition_to_fatal_error(bare_kafka_error(), Caller::App)
            .expect("FATAL_ERROR is always a valid target");
        assert!(!manager.has_ongoing_transaction());
    }

    /// Translated from `testMaybeAddPartitionToTransaction` (Java 399-422).
    #[tokio::test]
    async fn test_maybe_add_partition_to_transaction() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        assert!(manager.has_partitions_to_add());
        assert!(!manager.transaction_contains_partition(&partition));
        assert!(manager.is_partition_pending_add(&partition));

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(partition.clone(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&partition));
        assert!(!manager.has_partitions_to_add());
        assert!(!manager.is_partition_pending_add(&partition));

        // adding the partition again should not have any effect
        manager.maybe_add_partition(&partition).expect("re-registering is a no-op");
        assert!(!manager.has_partitions_to_add());
        assert!(manager.transaction_contains_partition(&partition));
        assert!(!manager.is_partition_pending_add(&partition));
    }

    /// Translated from `testMaybeAddPartitionToTransactionInTransactionV2`
    /// (Java 424-442).
    ///
    /// The Transaction V2 arm (Java 448-451) registers the partition directly,
    /// because the client sends no `AddPartitionsToTxn` under TV2 — so nothing is
    /// ever pending and the queue stays empty.
    #[tokio::test]
    async fn test_maybe_add_partition_to_transaction_in_transaction_v2() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(true);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(manager.is_transaction_v2_enabled());
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        // In V2, the maybeAddPartition should not add the partition to the pending list.
        assert!(!manager.has_partitions_to_add());
        assert!(manager.transaction_contains_partition(&partition));
        assert!(!manager.is_partition_pending_add(&partition));
        assert!(pending.is_empty(), "Transaction V2 sends no AddPartitionsToTxn");

        // Adding the partition again should not have any effect
        manager.maybe_add_partition(&partition).expect("re-registering is a no-op");
        assert!(!manager.has_partitions_to_add());
        assert!(manager.transaction_contains_partition(&partition));
        assert!(!manager.is_partition_pending_add(&partition));
    }

    /// Translated from
    /// `testAddPartitionToTransactionOverridesRetryBackoffForConcurrentTransactions`
    /// (Java 444-461).
    #[tokio::test]
    async fn test_add_partition_to_transaction_overrides_retry_backoff_for_concurrent_transactions() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        assert!(manager.has_partitions_to_add());
        assert!(!manager.transaction_contains_partition(&partition));
        assert!(manager.is_partition_pending_add(&partition));

        run_add_partitions_to_txn(
            &mut manager,
            &mut pending,
            &[(partition.clone(), Errors::ConcurrentTransactions)],
        )
        .expect("a CONCURRENT_TRANSACTIONS response re-enqueues the request");

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("the retry must be pending");
        assert_eq!(handler.retry_backoff_ms(), TransactionManager::ADD_PARTITIONS_RETRY_BACKOFF_MS);
    }

    /// Translated from
    /// `testAddPartitionToTransactionRetainsRetryBackoffForRegularRetriableError`
    /// (Java 463-479).
    ///
    /// `COORDINATOR_NOT_AVAILABLE` takes the coordinator-rediscovery arm
    /// (Java 1577), which re-enqueues without touching the backoff — so the queue
    /// holds a `FindCoordinator` at [`Priority::FindCoordinator`] ahead of the
    /// retry, and Java's `nextRequest(false)` yields *that* one. Java asserts
    /// `DEFAULT_RETRY_BACKOFF_MS`, which is the value both handlers carry.
    #[tokio::test]
    async fn test_add_partition_to_transaction_retains_retry_backoff_for_regular_retriable_error() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        assert!(manager.has_partitions_to_add());
        assert!(!manager.transaction_contains_partition(&partition));
        assert!(manager.is_partition_pending_add(&partition));

        run_add_partitions_to_txn(
            &mut manager,
            &mut pending,
            &[(partition.clone(), Errors::CoordinatorNotAvailable)],
        )
        .expect("a COORDINATOR_NOT_AVAILABLE response looks the coordinator up again");

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("the retry must be pending");
        assert_eq!(handler.retry_backoff_ms(), DEFAULT_RETRY_BACKOFF_MS);
    }

    /// Translated from
    /// `testAddPartitionToTransactionRetainsRetryBackoffWhenPartitionsAlreadyAdded`
    /// (Java 481-501).
    ///
    /// `maybeOverrideRetryBackoffMs` only lowers the backoff while
    /// `partitionsInTransaction` is empty (Java 1646), so the second partition's
    /// `CONCURRENT_TRANSACTIONS` keeps the configured value.
    ///
    /// Java asserts on `nextRequest(false)` *before* driving the second response,
    /// so the handler it inspects is the freshly-enqueued `AddPartitionsToTxn` —
    /// which already carries `DEFAULT_RETRY_BACKOFF_MS`. This drives the response
    /// first, so the assertion covers the post-override value as well, which is the
    /// point the test name makes.
    #[tokio::test]
    async fn test_add_partition_to_transaction_retains_retry_backoff_when_partitions_already_added() {
        let partition = TopicPartition::new("foo".to_string(), 0);
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        assert!(manager.has_partitions_to_add());
        assert!(!manager.transaction_contains_partition(&partition));
        assert!(manager.is_partition_pending_add(&partition));

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(partition.clone(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&partition));

        let other_partition = TopicPartition::new("foo".to_string(), 1);
        manager
            .maybe_add_partition(&other_partition)
            .expect("a second partition is registered");
        run_add_partitions_to_txn(&mut manager, &mut pending, &[(other_partition, Errors::ConcurrentTransactions)])
            .expect("a CONCURRENT_TRANSACTIONS response re-enqueues the request");

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("the retry must be pending");
        assert_eq!(handler.retry_backoff_ms(), DEFAULT_RETRY_BACKOFF_MS);
    }

    /// Translated from `testIsSendToPartitionAllowedWithPendingPartitionAfterAbortableError`
    /// (Java 531), `testIsSendToPartitionAllowedWithInFlightPartitionAddAfterAbortableError`
    /// (543), `testIsSendToPartitionAllowedWithPendingPartitionAfterFatalError` (558),
    /// `testIsSendToPartitionAllowedWithInFlightPartitionAddAfterFatalError` (570),
    /// `testIsSendToPartitionAllowedWithAddedPartitionAfterAbortableError` (585) and
    /// `testIsSendToPartitionAllowedWithAddedPartitionAfterFatalError` (601) — one
    /// loop iteration each.
    ///
    /// The distinction they pin: `isSendToPartitionAllowed` (Java 466) refuses a
    /// partition that is only *pending* — because nothing may be sent for it until
    /// the coordinator confirms it — and allows one already confirmed, unless the
    /// producer is in a fatal state, in which case nothing is allowed at all.
    ///
    /// Java's two "in-flight partition add" variants reach the in-flight state with
    /// `runUntil(transactionManager::hasInFlightRequest)`, i.e. by having the Sender
    /// send the `AddPartitionsToTxn` and leaving it unanswered. Here that is
    /// `next_request` without a following `handle_response`, which is the same
    /// manager-visible state: the partition has left `newPartitionsInTransaction`
    /// for `pendingPartitionsInTransaction`.
    #[tokio::test]
    async fn test_is_send_to_partition_allowed_after_an_error() {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Stage {
            /// Registered locally, request not yet sent (Java 532, 559).
            Pending,
            /// Request sent and unanswered (Java 544, 571).
            InFlight,
            /// Confirmed by the coordinator (Java 586, 602).
            Added,
        }

        for (stage, fatal, expected_allowed) in [
            (Stage::Pending, false, false),
            (Stage::InFlight, false, false),
            (Stage::Pending, true, false),
            (Stage::InFlight, true, false),
            (Stage::Added, false, true),
            (Stage::Added, true, false),
        ] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
            manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
            manager.maybe_add_partition(&tp0()).expect("a new partition is registered");

            match stage {
                Stage::Pending => {},
                Stage::InFlight => {
                    // Send the AddPartitionsToTxn request and leave it in-flight.
                    manager
                        .next_request(&mut pending, false)
                        .expect("next_request does not fail on this path")
                        .expect("an AddPartitionsToTxn request must be pending");
                    assert!(manager.is_partition_pending_add(&tp0()));
                },
                Stage::Added => {
                    run_add_partitions_to_txn(&mut manager, &mut pending, &[(tp0(), Errors::None)])
                        .expect("a successful AddPartitionsToTxn response is handled");
                    assert!(!manager.has_partitions_to_add());
                },
            }

            if fatal {
                manager
                    .transition_to_fatal_error(bare_kafka_error(), Caller::App)
                    .expect("FATAL_ERROR is always a valid target");
                assert!(manager.has_fatal_error());
            } else {
                manager
                    .transition_to_abortable_error(bare_kafka_error(), Caller::App)
                    .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");
                assert!(manager.has_abortable_error());
            }

            assert_eq!(manager.is_send_to_partition_allowed(&tp0()), expected_allowed);
        }
    }

    /// Translated from `testTransactionalIdAuthorizationFailureInInitProducerId`
    /// (Java 1365-1379).
    #[tokio::test]
    async fn test_transactional_id_authorization_failure_in_init_producer_id() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let init_pid_result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("an InitProducerId request must be pending");
        complete_init_producer_id(
            &mut manager,
            &mut pending,
            handler,
            Errors::TransactionalIdAuthorizationFailed,
            PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .expect("an authorization failure moves to an abortable error");

        assert!(manager.has_error());
        assert!(init_pid_result.is_completed());
        assert!(!init_pid_result.is_successful());
        assert_eq!(
            init_pid_result.error().expect("the result carries an error").error(),
            Errors::TransactionalIdAuthorizationFailed
        );
        // Java: `assertThrows(.., initPidResult::await)`. Awaiting is what marks the
        // result acknowledged (rules §5), which releases the pending-transition slot
        // so `assertAbortableError`'s `beginCommit` is not refused for the wrong
        // reason.
        let error = init_pid_result.await_result().await.expect_err("initTransactions failed");
        assert_eq!(error.error(), Errors::TransactionalIdAuthorizationFailed);
        assert_abortable_error(&mut manager, &mut pending, Errors::TransactionalIdAuthorizationFailed);
    }

    /// Translated from `testTransactionAbortableExceptionInInitProducerId`
    /// (Java 3871-3885).
    #[tokio::test]
    async fn test_transaction_abortable_error_in_init_producer_id() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let init_pid_result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("an InitProducerId request must be pending");
        complete_init_producer_id(
            &mut manager,
            &mut pending,
            handler,
            Errors::TransactionAbortable,
            PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .expect("TRANSACTION_ABORTABLE moves to an abortable error");

        assert!(manager.has_error());
        assert!(init_pid_result.is_completed());
        assert!(!init_pid_result.is_successful());
        assert_eq!(
            init_pid_result.error().expect("the result carries an error").error(),
            Errors::TransactionAbortable
        );
        // Java: `assertThrows(.., initPidResult::await)`. Awaiting is what marks the
        // result acknowledged (rules §5), which releases the pending-transition slot
        // so `assertAbortableError`'s `beginCommit` is not refused for the wrong
        // reason.
        let error = init_pid_result.await_result().await.expect_err("initTransactions failed");
        assert_eq!(error.error(), Errors::TransactionAbortable);
        assert_abortable_error(&mut manager, &mut pending, Errors::TransactionAbortable);
    }

    /// The shared prologue of the `*InAddOffsetsToTxn` family (Java 1451, 1471,
    /// 3950): a `sendOffsetsToTransaction` whose `AddOffsetsToTxn` fails.
    async fn add_offsets_to_txn_failure(
        error: Errors,
    ) -> (TransactionManager, PendingRequests, Arc<TransactionalRequestResult>) {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        let partition = TopicPartition::new("foo".to_string(), 0);
        let offsets = HashMap::from([(partition, offset(39))]);
        let send_offsets_result = manager
            .send_offsets_to_transaction(offsets, consumer_group_metadata(), &mut pending)
            .expect("sendOffsetsToTransaction is valid in IN_TRANSACTION");
        run_add_offsets_to_txn(&mut manager, &mut coordinators, &mut pending, CONSUMER_GROUP_ID, error)
            .expect("the AddOffsetsToTxn response is handled");

        assert!(manager.has_error());
        assert!(send_offsets_result.is_completed());
        assert!(!send_offsets_result.is_successful());
        (manager, pending, send_offsets_result)
    }

    /// Translated from `testTransactionalIdAuthorizationFailureInAddOffsetsToTxn`
    /// (Java 1450-1468).
    #[tokio::test]
    async fn test_transactional_id_authorization_failure_in_add_offsets_to_txn() {
        let (mut manager, mut pending, send_offsets_result) =
            add_offsets_to_txn_failure(Errors::TransactionalIdAuthorizationFailed).await;
        assert_eq!(
            send_offsets_result.error().expect("the result carries an error").error(),
            Errors::TransactionalIdAuthorizationFailed
        );
        assert_fatal_error(&mut manager, &mut pending, Errors::TransactionalIdAuthorizationFailed);
    }

    /// Translated from `testInvalidTxnStateFailureInAddOffsetsToTxn`
    /// (Java 1470-1488).
    #[tokio::test]
    async fn test_invalid_txn_state_failure_in_add_offsets_to_txn() {
        let (mut manager, mut pending, send_offsets_result) = add_offsets_to_txn_failure(Errors::InvalidTxnState).await;
        assert_eq!(
            send_offsets_result.error().expect("the result carries an error").error(),
            Errors::InvalidTxnState
        );
        assert_fatal_error(&mut manager, &mut pending, Errors::InvalidTxnState);
    }

    /// Translated from `testTransactionAbortableExceptionInAddOffsetsToTxn`
    /// (Java 3949-3967).
    #[tokio::test]
    async fn test_transaction_abortable_error_in_add_offsets_to_txn() {
        let (mut manager, mut pending, send_offsets_result) =
            add_offsets_to_txn_failure(Errors::TransactionAbortable).await;
        assert_eq!(
            send_offsets_result.error().expect("the result carries an error").error(),
            Errors::TransactionAbortable
        );
        assert_abortable_error(&mut manager, &mut pending, Errors::TransactionAbortable);
    }

    /// Translated from `testProducerFencedInAddOffSetsToTxn` (Java 2080-2084) and
    /// `testInvalidProducerEpochConvertToProducerFencedInAddOffSetsToTxn`
    /// (Java 2085-2089), which differ only in the triggering code: both are
    /// converted to `PRODUCER_FENCED` (Java 1836-1839).
    ///
    /// Java's `verifyProducerFenced` also asserts on the produce future, which needs
    /// the accumulator; that half belongs with the `SenderTest` group.
    #[tokio::test]
    async fn test_producer_fenced_in_add_offsets_to_txn() {
        for triggered in [Errors::ProducerFenced, Errors::InvalidProducerEpoch] {
            let (manager, _pending, send_offsets_result) = add_offsets_to_txn_failure(triggered).await;
            assert!(manager.has_fatal_error());
            assert_eq!(
                send_offsets_result.error().expect("the result carries an error").error(),
                Errors::ProducerFenced
            );
        }
    }

    /// The shared prologue of the `*InTxnOffsetCommit` family (Java 1137, 1406,
    /// 1491, 3970): a `sendOffsetsToTransaction` whose `TxnOffsetCommit` fails.
    async fn txn_offset_commit_failure(
        error: Errors,
    ) -> (TransactionManager, PendingRequests, Arc<TransactionalRequestResult>) {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        let partition = TopicPartition::new("foo".to_string(), 0);
        let send_offsets_result = send_offsets_and_discover_group_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            consumer_group_metadata(),
            &[(partition.clone(), 39)],
        )
        .await;

        run_txn_offset_commit(
            &mut manager,
            &mut coordinators,
            &mut pending,
            CONSUMER_GROUP_ID,
            &[(partition, error)],
        )
        .expect("the TxnOffsetCommit response is handled");

        assert!(manager.has_error());
        assert!(send_offsets_result.is_completed());
        assert!(!send_offsets_result.is_successful());
        (manager, pending, send_offsets_result)
    }

    /// Translated from `testUnsupportedForMessageFormatInTxnOffsetCommit`
    /// (Java 1136-1156).
    #[tokio::test]
    async fn test_unsupported_for_message_format_in_txn_offset_commit() {
        let (mut manager, mut pending, send_offsets_result) =
            txn_offset_commit_failure(Errors::UnsupportedForMessageFormat).await;
        assert_eq!(
            send_offsets_result.error().expect("the result carries an error").error(),
            Errors::UnsupportedForMessageFormat
        );
        assert_fatal_error(&mut manager, &mut pending, Errors::UnsupportedForMessageFormat);
    }

    /// Translated from `testTransactionalIdAuthorizationFailureInTxnOffsetCommit`
    /// (Java 1490-1513).
    #[tokio::test]
    async fn test_transactional_id_authorization_failure_in_txn_offset_commit() {
        let (mut manager, mut pending, send_offsets_result) =
            txn_offset_commit_failure(Errors::TransactionalIdAuthorizationFailed).await;
        assert_eq!(
            send_offsets_result.error().expect("the result carries an error").error(),
            Errors::TransactionalIdAuthorizationFailed
        );
        assert_fatal_error(&mut manager, &mut pending, Errors::TransactionalIdAuthorizationFailed);
    }

    /// Translated from `testTransactionAbortableExceptionInTxnOffsetCommit`
    /// (Java 3969-3988).
    #[tokio::test]
    async fn test_transaction_abortable_error_in_txn_offset_commit() {
        let (mut manager, mut pending, send_offsets_result) =
            txn_offset_commit_failure(Errors::TransactionAbortable).await;
        assert_eq!(
            send_offsets_result.error().expect("the result carries an error").error(),
            Errors::TransactionAbortable
        );
        assert_abortable_error(&mut manager, &mut pending, Errors::TransactionAbortable);
    }

    /// Translated from `testGroupAuthorizationFailureInTxnOffsetCommit`
    /// (Java 1405-1433).
    ///
    /// The extra assertions over the family's shared shape: the error carries the
    /// group id (Java's `GroupAuthorizationException.groupId()`), and the pending
    /// offsets are cleared — the `break` leaves `pendingTxnOffsetCommits` non-empty
    /// but the tail's `result.isCompleted()` arm clears it (Java 1945).
    #[tokio::test]
    async fn test_group_authorization_failure_in_txn_offset_commit() {
        let (mut manager, mut pending, send_offsets_result) =
            txn_offset_commit_failure(Errors::GroupAuthorizationFailed).await;
        let error = send_offsets_result.error().expect("the result carries an error");
        assert_eq!(error.error(), Errors::GroupAuthorizationFailed);
        // Java: `((GroupAuthorizationException) result.error()).groupId()`.
        let Error::GroupAuthorization(group_error) = &error else {
            panic!("expected a GroupAuthorization error, got {error:?}");
        };
        assert_eq!(group_error.group_id(), CONSUMER_GROUP_ID);
        assert!(!manager.has_pending_offset_commits());
        assert_abortable_error(&mut manager, &mut pending, Errors::GroupAuthorizationFailed);
    }

    /// Translated from `testGroupAuthorizationFailureInFindCoordinator`
    /// (Java 1380-1404) and `testTransactionAbortableExceptionInFindCoordinator`
    /// (Java 3902-3923), which differ only in the triggering code.
    ///
    /// Phase 5a covered the production branches through
    /// `test_find_coordinator_remaining_error_arms`; this drives them the way Java
    /// does, through `sendOffsetsToTransaction` → `AddOffsetsToTxn` → the group
    /// coordinator lookup, and adds the `assertAbortableError` tail.
    #[tokio::test]
    async fn test_group_coordinator_lookup_failure_after_add_offsets_to_txn() {
        for (triggered, expected) in [
            (Errors::GroupAuthorizationFailed, Errors::GroupAuthorizationFailed),
            (Errors::TransactionAbortable, Errors::TransactionAbortable),
        ] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            let mut coordinators = CoordinatorNodes::new();
            do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
            manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

            let partition = TopicPartition::new("foo".to_string(), 0);
            let offsets = HashMap::from([(partition, offset(39))]);
            let send_offsets_result = manager
                .send_offsets_to_transaction(offsets, consumer_group_metadata(), &mut pending)
                .expect("sendOffsetsToTransaction is valid in IN_TRANSACTION");
            run_add_offsets_to_txn(&mut manager, &mut coordinators, &mut pending, CONSUMER_GROUP_ID, Errors::None)
                .expect("a successful AddOffsetsToTxn response is handled");
            assert!(!manager.has_partitions_to_add());

            // The Sender finds the group coordinator unknown and looks it up
            // (`Sender.java:479-492`, `:520-529`); the response carries the error.
            let commit = manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .expect("a TxnOffsetCommit request must be pending");
            manager
                .lookup_coordinator_for(&mut coordinators, &mut pending, &commit)
                .expect("GROUP is a valid coordinator type");
            manager.retry(&mut pending, commit);
            let find_coordinator = manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .expect("the FindCoordinator overtakes the TxnOffsetCommit");
            complete_find_coordinator(
                &mut manager,
                &mut coordinators,
                &mut pending,
                find_coordinator,
                triggered,
                CONSUMER_GROUP_ID,
                &broker_node(),
            )
            .expect("the FindCoordinator error is handled");

            assert_eq!(
                manager.last_error().expect("an error is recorded").error(),
                expected,
                "unexpected error for {triggered:?}"
            );
            // Java: `runUntil(sendOffsetsResult::isCompleted)`. The FindCoordinator
            // fails its own handler; the *caller's* handle is failed by
            // `maybeTerminateRequestWithError` when the queued TxnOffsetCommit is
            // next dequeued (Java 1174-1183).
            assert!(
                manager
                    .next_request(&mut pending, false)
                    .expect("next_request does not fail on this path")
                    .is_none(),
                "the queued TxnOffsetCommit is terminated, not sent"
            );
            assert!(send_offsets_result.is_completed());
            assert!(!send_offsets_result.is_successful());
            assert_eq!(
                send_offsets_result.error().expect("the result carries an error").error(),
                expected
            );
            assert_abortable_error(&mut manager, &mut pending, expected);
        }
    }

    /// The shared prologue of the `*InAddPartitions` family (Java 1863, 1879,
    /// 3887): a registered partition whose `AddPartitionsToTxn` fails.
    async fn add_partitions_failure(error: Errors) -> (TransactionManager, PendingRequests) {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        let partition = TopicPartition::new("foo".to_string(), 0);
        manager.maybe_add_partition(&partition).expect("a new partition is registered");
        run_add_partitions_to_txn(&mut manager, &mut pending, &[(partition, error)])
            .expect("the AddPartitionsToTxn response is handled");
        assert!(manager.has_error());
        (manager, pending)
    }

    /// Translated from `testTransactionalIdAuthorizationFailureInAddPartitions`
    /// (Java 1862-1876).
    #[tokio::test]
    async fn test_transactional_id_authorization_failure_in_add_partitions() {
        let (mut manager, mut pending) = add_partitions_failure(Errors::TransactionalIdAuthorizationFailed).await;
        assert_fatal_error(&mut manager, &mut pending, Errors::TransactionalIdAuthorizationFailed);
    }

    /// Translated from `testInvalidTxnStateInAddPartitions` (Java 1878-1892).
    #[tokio::test]
    async fn test_invalid_txn_state_in_add_partitions() {
        let (mut manager, mut pending) = add_partitions_failure(Errors::InvalidTxnState).await;
        assert_fatal_error(&mut manager, &mut pending, Errors::InvalidTxnState);
    }

    /// Translated from `testTransactionAbortableExceptionInAddPartitions`
    /// (Java 3886-3900).
    #[tokio::test]
    async fn test_transaction_abortable_error_in_add_partitions() {
        let (mut manager, mut pending) = add_partitions_failure(Errors::TransactionAbortable).await;
        assert_abortable_error(&mut manager, &mut pending, Errors::TransactionAbortable);
    }

    /// Translated from `testProducerFencedInAddPartitionToTxn` (Java 2056-2060) and
    /// `testInvalidProducerEpochConvertToProducerFencedInAddPartitionToTxn`
    /// (Java 2061-2065), which differ only in the triggering code: both are
    /// converted to `PRODUCER_FENCED` (Java 1591-1594).
    ///
    /// Java's `verifyProducerFenced` also asserts the produce future fails, which
    /// needs the accumulator; that half is owed with the `SenderTest` group.
    #[tokio::test]
    async fn test_producer_fenced_in_add_partition_to_txn() {
        for triggered in [Errors::ProducerFenced, Errors::InvalidProducerEpoch] {
            let (manager, _pending) = add_partitions_failure(triggered).await;
            assert!(manager.has_fatal_error(), "unexpected state for {triggered:?}");
            assert_eq!(
                manager.last_error().expect("an error is recorded").error(),
                Errors::ProducerFenced
            );
        }
    }

    /// Translated from the parameterized `testRetriableErrors` (Java 1970-2010),
    /// whose `@EnumSource` names four codes — translated as a real loop rather than
    /// one invocation (`definition-of-done.md` §3).
    ///
    /// Covers a retry of every request family the phase adds except
    /// `TxnOffsetCommit`, which Java's own comment defers to
    /// `testRetriableErrorInTxnOffsetCommit`. Note Java substitutes
    /// `COORDINATOR_LOAD_IN_PROGRESS` for the `AddPartitionsToTxn` leg when the
    /// parameter is `CONCURRENT_TRANSACTIONS`, because that code takes the
    /// backoff-override arm (Java 1585) rather than the generic retriable one.
    #[tokio::test]
    async fn test_retriable_errors() {
        for error in [
            Errors::UnknownTopicOrPartition,
            Errors::RequestTimedOut,
            Errors::CoordinatorLoadInProgress,
            Errors::ConcurrentTransactions,
        ] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            let mut coordinators = CoordinatorNodes::new();
            let result = manager
                .initialize_transactions(false, &mut pending)
                .expect("initTransactions is valid from UNINITIALIZED");

            // Ensure FindCoordinator retries. Java's tests get the FindCoordinator
            // from `Sender.maybeFindCoordinatorAndRetry`; here it is enqueued
            // directly, which is what that method calls (`Sender.java:522`).
            manager
                .lookup_coordinator(&mut coordinators, &mut pending, CoordinatorType::Transaction, TRANSACTIONAL_ID)
                .expect("TRANSACTION is a valid coordinator type");
            for attempt_error in [error, Errors::None] {
                let handler = manager
                    .next_request(&mut pending, false)
                    .expect("next_request does not fail on this path")
                    .expect("a FindCoordinator request must be pending");
                assert!(
                    handler.find_coordinator_request_data().is_some(),
                    "the FindCoordinator sorts ahead of the InitProducerId"
                );
                complete_find_coordinator(
                    &mut manager,
                    &mut coordinators,
                    &mut pending,
                    handler,
                    attempt_error,
                    TRANSACTIONAL_ID,
                    &broker_node(),
                )
                .expect("the FindCoordinator response is handled");
            }
            assert_eq!(
                coordinators
                    .coordinator(CoordinatorType::Transaction)
                    .expect("valid type")
                    .cloned(),
                Some(broker_node())
            );

            // Ensure InitPid retries.
            for attempt_error in [error, Errors::None] {
                let handler = manager
                    .next_request(&mut pending, false)
                    .expect("next_request does not fail on this path")
                    .expect("an InitProducerId request must be pending");
                complete_init_producer_id_with_coordinators(
                    &mut manager,
                    &mut coordinators,
                    &mut pending,
                    handler,
                    attempt_error,
                    PRODUCER_ID,
                    EPOCH,
                )
                .expect("the InitProducerId response is handled");
            }
            assert!(manager.has_producer_id());

            result.await_result().await.expect("initTransactions succeeded");
            manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

            // Ensure AddPartitionsToTxn retries. Since CONCURRENT_TRANSACTIONS is handled differently here, we
            // substitute.
            let add_partitions_error = if error == Errors::ConcurrentTransactions {
                Errors::CoordinatorLoadInProgress
            } else {
                error
            };
            manager.maybe_add_partition(&tp0()).expect("a new partition is registered");
            for attempt_error in [add_partitions_error, Errors::None] {
                run_add_partitions_to_txn_with_coordinators(
                    &mut manager,
                    &mut coordinators,
                    &mut pending,
                    &[(tp0(), attempt_error)],
                )
                .expect("the AddPartitionsToTxn response is handled");
            }
            assert!(manager.transaction_contains_partition(&tp0()));

            // Ensure txnOffsetCommit retries is tested in testRetriableErrorInTxnOffsetCommit.

            // Ensure EndTxn retries.
            let abort_result = manager
                .begin_commit(&mut pending)
                .expect("IN_TRANSACTION -> COMMITTING is valid");
            for attempt_error in [error, Errors::None] {
                run_end_txn_v4(&mut manager, &mut pending, TransactionResult::Commit, attempt_error)
                    .expect("the EndTxn response is handled");
            }
            assert!(abort_result.is_completed(), "unexpected outcome for {error:?}");
            assert!(abort_result.is_successful());
        }
    }

    /// Translated from `verifyProducerFencedForInitProducerId(Errors)` (Java 2037),
    /// which `testProducerFencedExceptionInInitProducerId` (2027) and
    /// `testInvalidProducerEpochConvertToProducerFencedInInitProducerId` (2032)
    /// parameterise — translated as a loop.
    ///
    /// The payload is the four-method fence check: once fenced, `beginTransaction`,
    /// `beginCommit`, `beginAbort` and `sendOffsetsToTransaction` must all fail, and
    /// with Java's `ProducerFencedException` message rather than a wrapped
    /// `KafkaException` — `maybeFailWithError`'s first branch (Java 1160-1164).
    #[tokio::test]
    async fn test_producer_fenced_for_init_producer_id() {
        for triggered in [Errors::ProducerFenced, Errors::InvalidProducerEpoch] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            let result = manager
                .initialize_transactions(false, &mut pending)
                .expect("initTransactions is valid from UNINITIALIZED");

            let handler = manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .expect("an InitProducerId request must be pending");
            complete_init_producer_id(&mut manager, &mut pending, handler, triggered, PRODUCER_ID, EPOCH)
                .expect("the InitProducerId response is handled");
            assert!(manager.has_error());

            let error = result.await_result().await.expect_err("initTransactions was fenced");
            assert_eq!(error.error(), Errors::ProducerFenced, "unexpected error for {triggered:?}");

            let fenced_message = format!(
                "Producer with transactionalId '{TRANSACTIONAL_ID}' and {} has been fenced by another producer with \
                 the same transactionalId",
                manager.producer_id_and_epoch()
            );
            for message in [
                manager
                    .begin_transaction()
                    .expect_err("beginTransaction is fenced")
                    .message()
                    .to_string(),
                manager
                    .begin_commit(&mut pending)
                    .expect_err("beginCommit is fenced")
                    .message()
                    .to_string(),
                manager
                    .begin_abort(&mut pending, Caller::App)
                    .expect_err("beginAbort is fenced")
                    .message()
                    .to_string(),
                manager
                    .send_offsets_to_transaction(HashMap::new(), dummy_group_metadata(), &mut pending)
                    .expect_err("sendOffsetsToTransaction is fenced")
                    .message()
                    .to_string(),
            ] {
                assert_eq!(message, fenced_message);
            }
        }
    }

    /// Translated from `shouldNotSendAbortTxnRequestWhenOnlyAddPartitionsRequestFailed`
    /// (Java 2587-2602).
    ///
    /// The abort succeeds *without* an `EndTxn` round trip: the failed
    /// `AddPartitionsToTxn` never set `transactionStarted`, so `nextRequest`
    /// short-circuits the `EndTxn` (Java 913-925).
    #[tokio::test]
    async fn test_should_not_send_abort_txn_request_when_only_add_partitions_request_failed() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        manager.maybe_add_partition(&tp0()).expect("a new partition is registered");

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(tp0(), Errors::TopicAuthorizationFailed)])
            .expect("a TOPIC_AUTHORIZATION_FAILED response moves to an abortable error");

        let abort_result = manager
            .begin_abort(&mut pending, Caller::App)
            .expect("ABORTABLE_ERROR -> ABORTING is valid");
        assert!(!abort_result.is_completed());

        assert!(
            manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .is_none(),
            "no EndTxn is sent for a transaction that never started"
        );
        assert!(abort_result.is_completed());
        assert!(abort_result.is_successful());
        assert!(manager.is_ready());
    }

    /// Translated from `shouldNotSendAbortTxnRequestWhenOnlyAddOffsetsRequestFailed`
    /// (Java 2604-2621) and `shouldFailAbortIfAddOffsetsFailsWithFatalError`
    /// (Java 2623-2640).
    ///
    /// Same shape, opposite outcome: `GROUP_AUTHORIZATION_FAILED` is abortable, so
    /// the abort completes successfully and the manager returns to `READY`;
    /// `UNKNOWN_SERVER_ERROR` is fatal, so the abort fails and the manager stays in
    /// `FATAL_ERROR`. Both go through the same `nextRequest` short-circuit, because
    /// the failed `AddOffsetsToTxn` never set `transactionStarted`.
    ///
    /// Note the abort is requested *before* the `AddOffsetsToTxn` response arrives,
    /// which is what makes the queued `EndTxn` the thing that observes the error.
    #[tokio::test]
    async fn test_abort_after_add_offsets_to_txn_failure() {
        for (error, expect_successful) in [
            (Errors::GroupAuthorizationFailed, true),
            (Errors::UnknownServerError, false),
        ] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            let mut coordinators = CoordinatorNodes::new();
            do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
            manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

            let offsets = HashMap::from([(tp1(), offset(1))]);
            manager
                .send_offsets_to_transaction(offsets, consumer_group_metadata(), &mut pending)
                .expect("sendOffsetsToTransaction is valid in IN_TRANSACTION");
            let abort_result = manager
                .begin_abort(&mut pending, Caller::App)
                .expect("IN_TRANSACTION -> ABORTING is valid");

            run_add_offsets_to_txn(&mut manager, &mut coordinators, &mut pending, CONSUMER_GROUP_ID, error)
                .expect("the AddOffsetsToTxn response is handled");
            // The queued EndTxn is dequeued next; it is short-circuited because
            // nothing was ever added, and `maybeTerminateRequestWithError` fails it
            // first in the fatal case.
            assert!(
                manager
                    .next_request(&mut pending, false)
                    .expect("next_request does not fail on this path")
                    .is_none(),
                "no EndTxn is sent for a transaction that never started ({error:?})"
            );

            assert!(abort_result.is_completed());
            assert_eq!(
                abort_result.is_successful(),
                expect_successful,
                "unexpected outcome for {error:?}"
            );
            if expect_successful {
                assert!(manager.is_ready());
            } else {
                assert!(manager.has_fatal_error());
            }
        }
    }

    /// Translated from `testForegroundInvalidStateTransitionIsRecoverable`
    /// (Java 3840-3869).
    ///
    /// An invalid transition attempted from the *application* side leaves the state
    /// machine untouched (rules §1), so a full transaction still runs afterwards.
    #[tokio::test]
    async fn test_foreground_invalid_state_transition_is_recoverable() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();

        // Intentionally perform an operation that will cause an invalid state transition. The detection of this
        // will not poison the transaction manager since it was performed in the foreground.
        manager
            .begin_abort(&mut pending, Caller::App)
            .expect_err("UNINITIALIZED -> ABORTING_TRANSACTION is not valid");
        assert!(!manager.has_fatal_error());

        // Validate that the transactions can still run after the invalid state transition attempt above.
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(manager.is_transactional());

        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(!manager.has_fatal_error());

        manager.maybe_add_partition(&tp1()).expect("a new partition is registered");
        assert!(manager.has_ongoing_transaction());

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(tp1(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&tp1()));

        let retry_result = manager
            .begin_commit(&mut pending)
            .expect("IN_TRANSACTION -> COMMITTING is valid");
        assert!(manager.has_ongoing_transaction());

        run_end_txn_v4(&mut manager, &mut pending, TransactionResult::Commit, Errors::None)
            .expect("a successful EndTxn response is handled");
        assert!(!manager.has_ongoing_transaction());
        assert!(retry_result.is_completed());
        retry_result.await_result().await.expect("the commit succeeded");
        assert!(retry_result.is_acked());
    }

    /// Translated from `testTransactionManagerEnablesV2` (Java 933-981).
    ///
    /// The upgrade path: a V1 transaction runs to completion, the cluster finalizes
    /// `transaction.version` at 2 mid-transaction, and `beginCommit`'s
    /// `maybeUpdateTransactionV2Enabled(false)` (Java 386) picks it up — which sets
    /// `clientSideEpochBumpRequired`, so `beginCompletingTransaction` returns an
    /// `initializeTransactions` result rather than the `EndTxn`'s, and the next
    /// transaction starts on a bumped epoch.
    ///
    /// Java re-initializes the manager at features epoch 1 before updating to 2; the
    /// Rust fixture starts at epoch 0, so the intermediate update is a no-op with
    /// respect to the feature level and is skipped.
    #[tokio::test]
    async fn test_transaction_manager_enables_v2() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(!manager.has_fatal_error());
        assert!(!manager.is_transaction_v2_enabled());

        finalize_transaction_version(manager.api_versions(), 2, 2);

        // The manager stays in transaction V2 disabled.
        assert!(!manager.is_transaction_v2_enabled());

        manager.maybe_add_partition(&tp1()).expect("a new partition is registered");
        assert!(manager.has_ongoing_transaction());

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(tp1(), Errors::None)])
            .expect("a successful AddPartitionsToTxn response is handled");
        assert!(manager.transaction_contains_partition(&tp1()));

        let retry_result = manager
            .begin_commit(&mut pending)
            .expect("IN_TRANSACTION -> COMMITTING is valid");
        assert!(manager.has_ongoing_transaction());
        assert!(manager.is_transaction_v2_enabled());
        assert!(
            manager.client_side_epoch_bump_required(),
            "upgrading to V2 mid-transaction must fence the old epoch"
        );

        run_end_txn_v4(&mut manager, &mut pending, TransactionResult::Commit, Errors::None)
            .expect("a successful EndTxn response is handled");
        // `resetTransactionState` goes to INITIALIZING rather than READY while the
        // bump is pending (Java 1331-1332), so the queued epoch-bump
        // `InitProducerId` is what completes the caller's handle.
        assert_eq!(manager.current_state(), State::Initializing);
        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("the epoch-bump InitProducerId must be pending");
        assert_eq!(handler.priority(), Priority::EpochBump);
        complete_init_producer_id(&mut manager, &mut pending, handler, Errors::None, PRODUCER_ID, EPOCH + 1)
            .expect("a successful InitProducerId response is handled");

        assert!(!manager.has_ongoing_transaction());
        assert!(retry_result.is_completed());
        retry_result.await_result().await.expect("the commit succeeded");
        assert!(retry_result.is_acked());

        // After restart the transaction, the V2 is still enabled and epoch is bumped.
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(manager.is_transaction_v2_enabled());
        assert_eq!(manager.producer_id_and_epoch().epoch, EPOCH + 1);
    }

    /// Translated from `testTransactionManagerDisablesV2` (Java 1033-1075).
    ///
    /// Java's body is almost entirely fixture construction; the payload is the last
    /// two lines — with `transaction.version` finalized at 1, `doInitTransactions`'s
    /// `maybeUpdateTransactionV2Enabled(true)` leaves Transaction V2 off.
    #[tokio::test]
    async fn test_transaction_manager_disables_v2() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(!manager.is_transaction_v2_enabled());
        assert!(!manager.client_side_epoch_bump_required(), "no upgrade means no epoch bump");
    }

    /// Translated from `testTransactionV2AddPartitionAndOffsets` (Java 983-1031),
    /// minus its two `appendToAccumulator` legs, which need the accumulator and are
    /// owed with the `SenderTest` group.
    ///
    /// Under Transaction V2 both `maybeAddPartition` and `sendOffsetsToTransaction`
    /// skip their registration RPC: the partition is confirmed immediately, and the
    /// offsets go straight to `TxnOffsetCommit` with no `AddOffsetsToTxn`.
    #[tokio::test]
    async fn test_transaction_v2_add_partition_and_offsets() {
        let mut manager = transactional_manager(true);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        assert!(manager.is_transaction_v2_enabled());
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");

        manager.maybe_add_partition(&tp0()).expect("a new partition is registered");
        assert!(manager.transaction_contains_partition(&tp0()));
        assert!(manager.is_send_to_partition_allowed(&tp0()));
        assert!(pending.is_empty(), "Transaction V2 sends no AddPartitionsToTxn");

        // Now, test adding the offsets.
        let offsets = HashMap::from([(tp1(), offset(1))]);
        let add_offsets_result = manager
            .send_offsets_to_transaction(offsets, consumer_group_metadata(), &mut pending)
            .expect("sendOffsetsToTransaction is valid in IN_TRANSACTION");
        assert!(manager.has_pending_offset_commits());
        // the result doesn't complete until TxnOffsetCommit returns
        assert!(!add_offsets_result.is_completed());

        discover_group_coordinator(&mut manager, &mut coordinators, &mut pending, CONSUMER_GROUP_ID)
            .expect("the group coordinator is discovered");
        assert!(manager.has_pending_offset_commits());

        run_txn_offset_commit(
            &mut manager,
            &mut coordinators,
            &mut pending,
            CONSUMER_GROUP_ID,
            &[(tp1(), Errors::None)],
        )
        .expect("a successful TxnOffsetCommit response is handled");
        assert!(!manager.has_pending_offset_commits());
        // We should only be done after both RPCs complete.
        assert!(add_offsets_result.is_completed());

        manager
            .begin_commit(&mut pending)
            .expect("IN_TRANSACTION -> COMMITTING is valid");
        // Under Transaction V2 the broker returns the bumped epoch on the EndTxn
        // response (Java 1760-1766), which the handler absorbs.
        run_end_txn(
            &mut manager,
            &mut pending,
            TransactionResult::Commit,
            Errors::None,
            PRODUCER_ID,
            EPOCH + 1,
        )
        .expect("a successful EndTxn response is handled");
        assert!(!manager.has_ongoing_transaction());
        assert!(!manager.is_completing());
        assert_eq!(manager.producer_id_and_epoch(), ProducerIdAndEpoch::new(PRODUCER_ID, EPOCH + 1));
    }

    /// Translated from
    /// `testBumpTransactionalEpochOnRecoverableAddPartitionRequestError`
    /// (Java 3546-3564).
    ///
    /// An `UNKNOWN_PRODUCER_ID` on `AddPartitionsToTxn` takes
    /// `abortableErrorIfPossible` (Java 1606), which arms
    /// `clientSideEpochBumpRequired`; the abort then completes through the epoch-bump
    /// `InitProducerId` rather than the `EndTxn`, and the producer is `READY` on a
    /// bumped epoch.
    #[tokio::test]
    async fn test_bump_transactional_epoch_on_recoverable_add_partition_request_error() {
        const INITIAL_EPOCH: i16 = 1;
        const BUMPED_EPOCH: i16 = 2;

        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, INITIAL_EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        manager.maybe_add_partition(&tp0()).expect("a new partition is registered");

        run_add_partitions_to_txn(&mut manager, &mut pending, &[(tp0(), Errors::UnknownProducerId)])
            .expect("an UNKNOWN_PRODUCER_ID response moves to an abortable error");
        assert!(manager.has_abortable_error());
        assert!(manager.client_side_epoch_bump_required());

        let abort_result = manager
            .begin_abort(&mut pending, Caller::App)
            .expect("ABORTABLE_ERROR -> ABORTING is valid");
        // `nextRequest` short-circuits the EndTxn (nothing was ever added) and polls
        // again in the *same* call (Java 924), so it hands back the epoch-bump
        // `InitProducerId` directly.
        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("the epoch-bump InitProducerId must be pending");
        assert_eq!(handler.priority(), Priority::EpochBump);
        complete_init_producer_id(&mut manager, &mut pending, handler, Errors::None, PRODUCER_ID, BUMPED_EPOCH)
            .expect("a successful InitProducerId response is handled");

        assert!(abort_result.is_completed());
        assert_eq!(manager.producer_id_and_epoch().epoch, BUMPED_EPOCH);
        assert!(abort_result.is_successful());
        // make sure we are ready for a transaction now.
        assert!(manager.is_ready());
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
        assert!(matches!(error, Error::LocalIllegalState(_)));
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

    /// Translated from `testRetryInitTransactionsAfterTimeout` (Java 1713-1744).
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

        // Java: `assertThrows(TimeoutException.class, () -> result.await(0, MILLISECONDS, TEST_TIMEOUT_MSG))`.
        let timeout = result
            .await_result_timeout(Duration::from_millis(0), "Unexpected time out during the test.")
            .await
            .expect_err("nothing has answered the InitProducerId yet");
        assert!(matches!(timeout, Error::Timeout(_)), "Java raises a timeout error: {timeout:?}");
        // AK 4.3.1: the timeout message carries the caller-supplied reason.
        assert!(
            timeout.message().contains("Unexpected time out during the test."),
            "expected the timeout reason in the message, got {timeout}"
        );
        assert!(!result.is_acked());

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
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
                .begin_abort(&mut pending, Caller::App)
                .expect_err("beginAbort is blocked by the unacknowledged result")
                .message(),
            expected.replace("`beginTransaction`", "`abortTransaction`")
        );
        assert_eq!(
            manager
                .begin_commit(&mut pending)
                .expect_err("beginCommit is blocked by the unacknowledged result")
                .message(),
            expected.replace("`beginTransaction`", "`commitTransaction`")
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
    /// [`TransactionManager::return_error_if_pending_state`] — the same rejection Java
    /// produces from `beginTransaction` (Java 332) and `send` (Java 439). The
    /// `nextState != pendingTransition.state` arm of
    /// `handleCachedTransactionRequestResult` itself needs a second
    /// result-returning entry point (`beginCommit` / `beginAbort` /
    /// `sendOffsetsToTransaction`), all of which Phase 5b landed.
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
            assert!(matches!(error, Error::LocalIllegalState(_)));
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
                Error::with_message(Errors::InvalidProducerIdMapping, "pid mapping is gone"),
                Caller::Sender,
            )
            .expect("FATAL_ERROR is always a valid target");

        assert!(result.is_completed());
        let error = result.await_result().await.expect_err("the pending operation failed");
        assert_eq!(error.error(), Errors::InvalidProducerIdMapping);
        assert_eq!(error.message(), "pid mapping is gone");
        // The pending result carries the raw error (asserted above); the fatality
        // of the situation is the manager's state, which is where Java keeps it,
        // so a caller woken from `initTransactions` reads it from there.
        assert!(manager.has_fatal_error(), "the manager must be in the fatal state");
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
            .expect("next_request does not fail on this path")
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
        // There is deliberately NO per-error fatality assertion here. Java performs
        // no transition on this path (asserted above) and its exceptions carry no
        // `isFatal()` flag — `close` simply fails the pending result with
        // `KafkaException("The producer closed forcefully")`, whose message is
        // asserted above. That message is the whole contract Java offers here.
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
            .transition_to_abortable_error(Error::new(Errors::ClusterAuthorizationFailed), Caller::Sender)
            .expect("INITIALIZING -> ABORTABLE_ERROR is valid");

        let authorization_error = Error::new(Errors::ClusterAuthorizationFailed);
        manager
            .transition_to_uninitialized(&authorization_error, Caller::Sender)
            .expect("ABORTABLE_ERROR -> UNINITIALIZED is valid");

        assert_eq!(manager.current_state(), State::Uninitialized);
        assert!(manager.last_error().is_none(), "Java clears lastError at :761");
        let error = result.await_result().await.expect_err("the pending operation failed");
        assert_eq!(error.error(), Errors::ClusterAuthorizationFailed);
    }

    /// Translated from `testBackgroundInvalidStateTransitionIsFatal`
    /// (Java 3818-3838).
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
            .handle_failed_batch(&batch, &bare_kafka_error(), false, &mut [], Caller::Sender)
            .expect_err("READY -> ABORTABLE_ERROR is not a valid transition");
        assert!(matches!(error, Error::LocalIllegalState(_)));
        assert!(manager.has_fatal_error());

        // Validate that these operations fail after the invalid state transition attempt above.
        for message in [
            manager.begin_transaction().expect_err("poisoned").message().to_string(),
            manager
                .begin_abort(&mut pending, Caller::App)
                .expect_err("poisoned")
                .message()
                .to_string(),
            manager.begin_commit(&mut pending).expect_err("poisoned").message().to_string(),
            manager.maybe_add_partition(&tp0()).expect_err("poisoned").message().to_string(),
            manager
                .initialize_transactions(false, &mut pending)
                .expect_err("poisoned")
                .message()
                .to_string(),
            manager
                .send_offsets_to_transaction(HashMap::new(), fake_group_metadata(), &mut pending)
                .expect_err("poisoned")
                .message()
                .to_string(),
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
            Error::new(Errors::NotLeaderOrFollower),
            Error::new(Errors::InvalidTxnState),
            timeout_error(),
        ] {
            assert!(
                original.is_retriable_error() || original.error() == Errors::InvalidTxnState,
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
            // Java 778 chains the original as the cause:
            // `new TransactionAbortableException(msg, exception)`. `last_error` is what
            // `maybe_fail_with_error` hands the application, so the chain must survive
            // the store — the message and code are identical for every input, and the
            // cause is the only thing that says which error exhausted its retries.
            let cause = crate::common::error::ErrorSource::source(last_error)
                .expect("Java chains the original error as the cause");
            assert_eq!(
                cause.error(),
                original.error(),
                "the cause must be the error that exhausted its retries, got {cause:?}"
            );
        }

        // A non-retriable, non-InvalidTxnState error is carried through as-is
        // (Java 780-784 with the `if` at 774 not taken).
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        let original = Error::with_message(Errors::RecordListTooLarge, "too big");
        assert!(!original.is_retriable_error());
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
                .maybe_transition_to_error_state(&Error::new(code), Caller::App)
                .expect("FATAL_ERROR is always a valid target");
            assert!(manager.has_fatal_error(), "{code} must be fatal");
        }
    }

    /// [`TransactionManager::reset_transaction_state`] (Java 1330) clears the
    /// per-transaction state, and picks `INITIALIZING` over `READY` exactly when a
    /// client-side epoch bump is pending.
    ///
    /// Both Java call sites — `nextRequest`'s never-started `EndTxn` branch and
    /// `EndTxnHandler.handleResponse` — arrive through a full transaction; this drives
    /// the method directly so the field clears are pinned in isolation.
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

    /// The four state predicates Java exposes but never calls
    /// (`isReady` 1085, `isInitializing` 1090, `isPrepared` 1099,
    /// `preparedTransactionState` 1976) and `isTransactionV2Enabled` (506).
    ///
    /// Translated per `definition-of-done.md` §2; pinned here so none can drift
    /// into reading the wrong state, which is the only way a caller-less accessor
    /// can go wrong.
    #[tokio::test]
    async fn test_state_predicates_java_declares_without_calling() {
        // An idempotent producer answers `false` to all three transactional ones,
        // because each is `isTransactional() && ..` or tests a state it cannot enter.
        let manager = idempotent_manager(false);
        assert!(!manager.is_ready());
        assert!(!manager.is_initializing());
        assert!(!manager.is_prepared());
        assert!(!manager.is_transaction_v2_enabled());
        assert_eq!(manager.prepared_transaction_state(), ProducerIdAndEpoch::NONE);

        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        assert!(!manager.is_ready(), "UNINITIALIZED is not READY");
        let result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");
        assert!(manager.is_initializing());
        assert!(!manager.is_ready());

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("queued");
        complete_init_producer_id(&mut manager, &mut pending, handler, Errors::None, PRODUCER_ID, EPOCH)
            .expect("a successful InitProducerId response is handled");
        assert!(manager.is_ready());
        assert!(!manager.is_initializing());
        assert!(!manager.is_prepared());

        // `beginTransaction` needs the result acknowledged first
        // (`throwIfPendingState`, Java 332).
        result.await_result().await.expect("initTransactions succeeded");
        // The predicates remain false through a transaction: PREPARED_TRANSACTION
        // needs a Transaction V2 manager, which `transactional_manager(true)` plus
        // `do_init_transactions`'s feature read supplies; covered by
        // `test_transaction_v2_add_partition_and_offsets`.
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        assert!(!manager.is_ready());
        assert!(!manager.is_prepared());
        assert!(!manager.is_transaction_v2_enabled());
    }

    // ---------------------------------------------------------------------
    // FindCoordinator and coordinator state.
    // ---------------------------------------------------------------------

    /// Feeds a `FindCoordinator` response body into `manager`, playing the part
    /// `onComplete`'s `synchronized` block (Java 1421-1423) plays.
    fn complete_find_coordinator(
        manager: &mut TransactionManager,
        coordinators: &mut CoordinatorNodes,
        pending_requests: &mut PendingRequests,
        handler: TxnRequestHandler,
        error: Errors,
        key: &str,
        node: &Node,
    ) -> Result<(), Error> {
        let response = ConcreteResponse::FindCoordinator(FindCoordinatorResponse::prepare_response(error, key, node));
        manager.handle_response(handler, &response, coordinators, pending_requests)
    }

    /// `brokerNode` (Java 167).
    fn broker_node() -> Node {
        Node::new(0, "localhost".to_string(), 2211)
    }

    /// `lookupCoordinator` (Java 1191) forgets the node and enqueues a
    /// `FindCoordinator` carrying the right key type and key, and that request
    /// overtakes a queued `InitProducerId` (Java 224's priority order).
    #[tokio::test]
    async fn test_lookup_coordinator_clears_the_node_and_enqueues_a_find_coordinator_first() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();

        manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");
        assert_eq!(pending.len(), 1);

        manager
            .lookup_coordinator(&mut coordinators, &mut pending, CoordinatorType::Transaction, TRANSACTIONAL_ID)
            .expect("TRANSACTION is a valid coordinator type");
        assert_eq!(pending.len(), 2);
        assert!(
            coordinators
                .coordinator(CoordinatorType::Transaction)
                .expect("valid type")
                .is_none()
        );

        // The FindCoordinator must come out first even though it was enqueued last.
        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("two are queued");
        assert_eq!(handler.priority(), Priority::FindCoordinator);
        let request_data = handler.find_coordinator_request_data().expect("a FindCoordinator handler");
        assert_eq!(request_data.key_type, CoordinatorType::Transaction.id());
        assert_eq!(request_data.key, TRANSACTIONAL_ID);

        complete_find_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            handler,
            Errors::None,
            TRANSACTIONAL_ID,
            &broker_node(),
        )
        .expect("a successful FindCoordinator response is handled");
        assert_eq!(
            coordinators.coordinator(CoordinatorType::Transaction).expect("valid type"),
            Some(&broker_node())
        );
        // The InitProducerId is still queued and is now the head.
        assert_eq!(
            manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .expect("the InitProducerId is still queued")
                .priority(),
            Priority::InitProducerId
        );
    }

    /// Translated from `testCoordinatorNotAvailable` (Java 2011-2025): a
    /// `FindCoordinator` answered with a retriable error is re-enqueued rather than
    /// failed (Java 1708-1709).
    ///
    /// `COORDINATOR_NOT_AVAILABLE` is retriable, so it takes the same arm as any
    /// other retriable code.
    #[tokio::test]
    async fn test_coordinator_not_available() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        let result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");
        manager
            .lookup_coordinator(&mut coordinators, &mut pending, CoordinatorType::Transaction, TRANSACTIONAL_ID)
            .expect("TRANSACTION is a valid coordinator type");

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("queued");
        let find_coordinator_result = Arc::clone(handler.result());
        assert!(Errors::CoordinatorNotAvailable.error().is_some_and(|e| e.is_retriable_error()));
        complete_find_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            handler,
            Errors::CoordinatorNotAvailable,
            TRANSACTIONAL_ID,
            &broker_node(),
        )
        .expect("a retriable FindCoordinator error is handled");

        assert!(
            !find_coordinator_result.is_completed(),
            "a re-enqueued request must not complete"
        );
        assert!(!manager.has_error());
        let retried = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("re-enqueued");
        assert_eq!(retried.priority(), Priority::FindCoordinator);
        assert!(retried.is_retry());

        // The second attempt succeeds and the InitProducerId can then complete.
        complete_find_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            retried,
            Errors::None,
            TRANSACTIONAL_ID,
            &broker_node(),
        )
        .expect("a successful FindCoordinator response is handled");
        // Java 2019. Asserting on the node is the point of the test — that the
        // *retry* installs it, and in the TRANSACTION slot. Without this, a retry
        // that wrote `consumer_group` instead would leave the test green, because
        // the `InitProducerId` below is resolved through `next_request` directly
        // rather than through the `Sender` routing that reads `coordinators`.
        assert_eq!(
            coordinators.coordinator(CoordinatorType::Transaction).expect("valid type"),
            Some(&broker_node())
        );
        assert!(
            coordinators.coordinator(CoordinatorType::Group).expect("valid type").is_none(),
            "a TRANSACTION lookup must not populate the GROUP slot"
        );

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("the InitProducerId is queued");
        complete_init_producer_id_with_coordinators(
            &mut manager,
            &mut coordinators,
            &mut pending,
            handler,
            Errors::None,
            PRODUCER_ID,
            EPOCH,
        )
        .expect("a successful InitProducerId response is handled");
        result.await_result().await.expect("initTransactions succeeded");
    }

    /// Translated from `testTransactionalIdAuthorizationFailureInFindCoordinator`
    /// (Java 1350-1363).
    #[tokio::test]
    async fn test_transactional_id_authorization_failure_in_find_coordinator() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        let init_pid_result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");
        manager
            .lookup_coordinator(&mut coordinators, &mut pending, CoordinatorType::Transaction, TRANSACTIONAL_ID)
            .expect("TRANSACTION is a valid coordinator type");

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("queued");
        let find_coordinator_result = Arc::clone(handler.result());
        complete_find_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            handler,
            Errors::TransactionalIdAuthorizationFailed,
            TRANSACTIONAL_ID,
            &broker_node(),
        )
        .expect("the error is handled");

        assert!(manager.has_error());
        assert!(manager.has_fatal_error());
        assert_eq!(
            manager.last_error().expect("recorded").error(),
            Errors::TransactionalIdAuthorizationFailed
        );
        // Java 1360-1361 asserts on the **`InitProducerId`** result, which is the
        // object `handleCachedTransactionRequestResult` installed in
        // `pending_transition` and which only `transitionToFatalError`'s
        // `pendingTransition.result.fail(exception)` (Java 545-547) can fail. A bare
        // `is_completed()` would pass whether that slot was failed with the right
        // error, the wrong error, or merely `done()`.
        //
        // The `is_completed()` assertion stays, ahead of the await, so a regression
        // that stops failing the slot *fails* here instead of parking forever on a
        // result nothing will ever complete. Java's `assertThrows(.., ::await)` would
        // hang in the same situation; a fail-fast probe is strictly better and costs
        // no fidelity, since the await below still pins the error.
        assert!(
            init_pid_result.is_completed(),
            "the fatal transition must fail the pending slot"
        );
        assert!(!init_pid_result.is_successful());
        assert_eq!(
            init_pid_result
                .await_result()
                .await
                .expect_err("the pending initTransactions failed")
                .error(),
            Errors::TransactionalIdAuthorizationFailed
        );

        // The `FindCoordinator` result is a *different* object — `lookupCoordinator`
        // builds `FindCoordinatorHandler` with its own `TransactionalRequestResult`
        // (Java 1655 → `super("FindCoordinator")`) — so these assertions are in
        // addition to Java's, not a substitute for them. Java holds no reference to
        // it and so cannot assert on it.
        assert!(find_coordinator_result.is_completed());
        assert!(!find_coordinator_result.is_successful());
        assert_eq!(
            find_coordinator_result
                .await_result()
                .await
                .expect_err("the lookup failed")
                .error(),
            Errors::TransactionalIdAuthorizationFailed
        );

        assert_fatal_error(&mut manager, &mut pending, Errors::TransactionalIdAuthorizationFailed);
    }

    /// The remaining three error arms of
    /// `FindCoordinatorHandler.handleResponse` (Java 1710-1719).
    ///
    /// `GROUP_AUTHORIZATION_FAILED` needs a **group** lookup, which Java only
    /// reaches through `sendOffsetsToTransaction` → `AddOffsetsToTxn`; the arms are
    /// driven directly here, since `lookup_coordinator` takes the type as a
    /// parameter, and end to end in
    /// [`test_group_coordinator_lookup_failure_after_add_offsets_to_txn`], which is
    /// the port of Java's two tests.
    #[tokio::test]
    async fn test_find_coordinator_remaining_error_arms() {
        // GROUP_AUTHORIZATION_FAILED → abortable, with the group id in the message.
        const CONSUMER_GROUP_ID: &str = "myConsumerGroup";
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        manager
            .lookup_coordinator(&mut coordinators, &mut pending, CoordinatorType::Group, CONSUMER_GROUP_ID)
            .expect("GROUP is a valid coordinator type");
        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("queued");
        let result = Arc::clone(handler.result());
        complete_find_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            handler,
            Errors::GroupAuthorizationFailed,
            CONSUMER_GROUP_ID,
            &broker_node(),
        )
        .expect("the error is handled");
        assert!(manager.has_abortable_error());
        let error = manager.last_error().expect("recorded");
        assert_eq!(error.error(), Errors::GroupAuthorizationFailed);
        assert_eq!(error.message(), format!("Not authorized to access group: {CONSUMER_GROUP_ID}"));
        assert!(!result.is_successful());

        // TRANSACTION_ABORTABLE → abortable.
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
        manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
        manager
            .lookup_coordinator(&mut coordinators, &mut pending, CoordinatorType::Transaction, TRANSACTIONAL_ID)
            .expect("TRANSACTION is a valid coordinator type");
        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("queued");
        complete_find_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            handler,
            Errors::TransactionAbortable,
            TRANSACTIONAL_ID,
            &broker_node(),
        )
        .expect("the error is handled");
        assert!(manager.has_abortable_error());
        assert_eq!(manager.last_error().expect("recorded").error(), Errors::TransactionAbortable);

        // Anything else → fatal, with Java's formatted message (Java 1717-1719).
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        manager
            .lookup_coordinator(&mut coordinators, &mut pending, CoordinatorType::Transaction, TRANSACTIONAL_ID)
            .expect("TRANSACTION is a valid coordinator type");
        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("queued");
        let unexpected = Errors::InvalidRequest;
        assert!(!unexpected.error().is_some_and(|e| e.is_retriable_error()));
        complete_find_coordinator(
            &mut manager,
            &mut coordinators,
            &mut pending,
            handler,
            unexpected,
            TRANSACTIONAL_ID,
            &broker_node(),
        )
        .expect("the error is handled");
        assert!(manager.has_fatal_error());
        let last_error = manager.last_error().expect("recorded");
        assert_eq!(
            last_error.message(),
            format!(
                "Could not find a coordinator with type TRANSACTION with key {TRANSACTIONAL_ID} due to unexpected \
                 error: {}",
                unexpected.message()
            )
        );
        // Java 1716: `new KafkaException(String.format(..))` — bare.
        assert!(matches!(last_error, Error::KafkaError(_)), "got {last_error:?}");
        assert!(!last_error.is_api_error(), "a bare KafkaException is not an ApiException");
    }

    /// Translated from `testLookupCoordinatorOnNotCoordinatorError`
    /// (Java 1323-1348): a `NOT_COORDINATOR` (or `COORDINATOR_NOT_AVAILABLE`)
    /// `InitProducerId` response forgets the coordinator and re-enqueues both
    /// requests (Java 1519-1521).
    #[tokio::test]
    async fn test_lookup_coordinator_on_not_coordinator_error() {
        for error_code in [Errors::NotCoordinator, Errors::CoordinatorNotAvailable] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            let mut coordinators = CoordinatorNodes::new();
            let init_pid_result = manager
                .initialize_transactions(false, &mut pending)
                .expect("initTransactions is valid from UNINITIALIZED");
            coordinators
                .set(CoordinatorType::Transaction, broker_node())
                .expect("TRANSACTION is a valid coordinator type");

            let handler = manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .expect("queued");
            complete_init_producer_id_with_coordinators(
                &mut manager,
                &mut coordinators,
                &mut pending,
                handler,
                error_code,
                PRODUCER_ID,
                EPOCH,
            )
            .expect("the error is handled");

            assert!(
                coordinators
                    .coordinator(CoordinatorType::Transaction)
                    .expect("valid type")
                    .is_none(),
                "{error_code} must forget the coordinator"
            );
            assert!(!init_pid_result.is_completed());
            assert!(!manager.has_producer_id());
            assert_eq!(
                pending.len(),
                2,
                "both the FindCoordinator and the retried InitProducerId are queued"
            );
            assert_eq!(
                manager
                    .next_request(&mut pending, false)
                    .expect("next_request does not fail on this path")
                    .expect("queued")
                    .priority(),
                Priority::FindCoordinator
            );
            let retried = manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .expect("queued");
            assert_eq!(retried.priority(), Priority::InitProducerId);
            assert!(retried.is_retry());
        }
    }

    /// A `FindCoordinator` is allowed through in `ABORTABLE_ERROR` while any other
    /// request is failed (Java 1175-1178).
    ///
    /// Java's `testFindCoordinatorAllowedInAbortableErrorState` (Java 2354-2375)
    /// reaches the state through `maybeAddPartition` + a `NOT_COORDINATOR`
    /// `AddPartitionsToTxn` response; the escape hatch itself is
    /// driven here. Without it the abort could never find its coordinator.
    #[tokio::test]
    async fn test_find_coordinator_allowed_in_abortable_error_state() {
        let mut manager = transactional_manager(false);
        let mut pending = PendingRequests::new();
        let mut coordinators = CoordinatorNodes::new();
        let init_pid_result = manager
            .initialize_transactions(false, &mut pending)
            .expect("initTransactions is valid from UNINITIALIZED");
        manager
            .lookup_coordinator(&mut coordinators, &mut pending, CoordinatorType::Transaction, TRANSACTIONAL_ID)
            .expect("TRANSACTION is a valid coordinator type");
        manager
            .transition_to_abortable_error(bare_kafka_error(), Caller::Sender)
            .expect("INITIALIZING -> ABORTABLE_ERROR is valid");

        let handler = manager
            .next_request(&mut pending, false)
            .expect("next_request does not fail on this path")
            .expect("a FindCoordinator is not terminated in ABORTABLE_ERROR");
        assert_eq!(handler.priority(), Priority::FindCoordinator);
        assert!(!handler.result().is_completed());

        // The queued InitProducerId, by contrast, is failed and withheld.
        assert!(
            manager
                .next_request(&mut pending, false)
                .expect("next_request does not fail on this path")
                .is_none(),
            "any other request is terminated while in an error state"
        );
        assert!(init_pid_result.is_completed());
        assert!(manager.has_abortable_error(), "terminating a request must not change the state");
    }

    /// `handleCoordinatorReady` (Java 1103) drives
    /// `needToTriggerEpochBumpFromClient` (Java 1309) and
    /// `canHandleAbortableError` (Java 1326) off the coordinator's
    /// `InitProducerId` max version, and `>= 3` is the threshold.
    #[tokio::test]
    async fn test_handle_coordinator_ready_tracks_epoch_bump_support() {
        for (max_version, supports_bump) in [(2_i16, false), (3, true), (6, true)] {
            let api_versions = Arc::new(ApiVersions::new());
            let mut init_producer_id = ApiVersion::new();
            init_producer_id
                .set_api_key(ApiKeys::INIT_PRODUCER_ID.id())
                .set_min_version(0)
                .set_max_version(max_version);
            api_versions.update(
                broker_node().id_string(),
                NodeApiVersions::with_node_finalized_features_finalized_features_epoch(
                    &[init_producer_id],
                    &[],
                    &[],
                    0,
                ),
            );
            let mut manager = TransactionManager::new(
                LogContext::empty(),
                Some(TRANSACTIONAL_ID.to_string()),
                TRANSACTION_TIMEOUT_MS,
                DEFAULT_RETRY_BACKOFF_MS,
                api_versions,
                false,
            );
            let mut coordinators = CoordinatorNodes::new();

            // Before the coordinator is known, neither predicate holds.
            manager.handle_coordinator_ready(&coordinators);
            assert!(!manager.need_to_trigger_epoch_bump_from_client());
            assert!(!manager.can_handle_abortable_error());

            coordinators
                .set(CoordinatorType::Transaction, broker_node())
                .expect("TRANSACTION is a valid coordinator type");
            manager.handle_coordinator_ready(&coordinators);
            assert_eq!(
                manager.need_to_trigger_epoch_bump_from_client(),
                supports_bump,
                "InitProducerId v{max_version}"
            );
            assert_eq!(
                manager.can_handle_abortable_error(),
                supports_bump,
                "InitProducerId v{max_version}"
            );
        }
    }

    /// Translated from
    /// `testNeedToTriggerEpochBumpFromClientDuringCoordinatorDisconnect`
    /// (Java 3714-3722): once `handleCoordinatorReady` has recorded support, losing
    /// the coordinator's entry in `ApiVersions` must not withdraw it.
    ///
    /// That is what lets the client bump the epoch while recovering from an
    /// abortable error even if the coordinator is momentarily unreachable
    /// (`Sender.java:565-567`).
    #[tokio::test]
    async fn test_need_to_trigger_epoch_bump_from_client_during_coordinator_disconnect() {
        let mut manager = transactional_manager(false);
        let mut coordinators = CoordinatorNodes::new();
        let mut pending = PendingRequests::new();
        do_init_transactions(&mut manager, &mut pending, 0, 0).await;
        coordinators
            .set(CoordinatorType::Transaction, Node::new(0, "localhost".to_string(), 2211))
            .expect("TRANSACTION is a valid coordinator type");

        // `initializeTransactionManager` registers node "0" with InitProducerId v6.
        manager.handle_coordinator_ready(&coordinators);
        assert!(manager.need_to_trigger_epoch_bump_from_client());

        manager.api_versions().remove(
            coordinators
                .coordinator(CoordinatorType::Transaction)
                .expect("valid type")
                .expect("discovered")
                .id_string(),
        );
        assert!(manager.need_to_trigger_epoch_bump_from_client());
    }

    /// `maybeResolveSequences`'s transactional arm (Java 862-872) takes the
    /// abortable path when the coordinator supports an epoch bump and the fatal
    /// path when it does not, with Java's two messages.
    ///
    /// Java's `testMaybeResolveSequencesTransactionalProducer` (Java 3157-3188)
    /// drives this through `maybeAddPartition` + `AddPartitionsToTxn` and an expired
    /// batch, which needs the accumulator; the branch itself is driven here, which is
    /// why the test accounting block lists that method as translated-with-a-named-gap
    /// rather than fully.
    #[tokio::test]
    async fn test_maybe_resolve_sequences_transactional_producer() {
        const UNACKED: &str = "The client hasn't received acknowledgment for some previously sent messages and can \
                               no longer retry them. ";
        for coordinator_supports_bump in [true, false] {
            let mut manager = transactional_manager(false);
            let mut pending = PendingRequests::new();
            do_init_transactions(&mut manager, &mut pending, PRODUCER_ID, EPOCH).await;
            manager.begin_transaction().expect("READY -> IN_TRANSACTION is valid");
            if coordinator_supports_bump {
                // Already true — `do_init_transactions` runs `handleCoordinatorReady`
                // as `Sender.runOnce` does.
                assert!(manager.can_handle_abortable_error());
            } else {
                // Java reaches "no bump support" by losing the coordinator:
                // `handleCoordinatorReady` reads `apiVersions.get(null)` and records
                // `false` (Java 1104-1106). That is the mechanism
                // `testNeedToTriggerEpochBumpFromClientDuringCoordinatorDisconnect`
                // (Java 3715) exercises.
                manager.handle_coordinator_ready(&CoordinatorNodes::new());
                assert!(!manager.can_handle_abortable_error());
            }

            // A batch that was sent, marked unresolved, and whose sequence never
            // resolved — Java's `markSequenceUnresolved` + `handleFailedBatch`.
            manager.txn_partition_map.get_or_create(&tp0());
            let sequence = manager.sequence_number(&tp0());
            manager.increment_sequence_number(&tp0(), 1).expect("the entry exists");
            let mut batch = batch_with_value(&tp0(), "1");
            let producer_id_and_epoch = manager.producer_id_and_epoch();
            batch.set_producer_state(producer_id_and_epoch.producer_id, producer_id_and_epoch.epoch, sequence, false);
            batch.close();
            manager.mark_sequence_unresolved(&batch);
            assert!(manager.has_unresolved_sequences());

            manager.maybe_resolve_sequences(Caller::Sender).expect("both targets are valid");

            if coordinator_supports_bump {
                assert!(manager.has_abortable_error());
                assert!(manager.client_side_epoch_bump_required());
                let last_error = manager.last_error().expect("recorded");
                assert_eq!(
                    last_error.message(),
                    format!("{UNACKED}It is safe to abort the transaction and continue.")
                );
                // Java 866: `new KafkaException(..)` — bare. `maybe_transition_to_error_state`
                // passes a non-retriable, non-`INVALID_TXN_STATE` error straight through,
                // so this is the value the application receives.
                assert!(matches!(last_error, Error::KafkaError(_)), "got {last_error:?}");
                assert!(!last_error.is_api_error(), "a bare KafkaException is not an ApiException");
            } else {
                assert!(manager.has_fatal_error());
                let last_error = manager.last_error().expect("recorded");
                assert_eq!(last_error.message(), format!("{UNACKED}It isn't safe to continue."));
                // Java 868: the other `new KafkaException(..)`, also bare.
                assert!(matches!(last_error, Error::KafkaError(_)), "got {last_error:?}");
                assert!(!last_error.is_api_error(), "a bare KafkaException is not an ApiException");
            }
            assert!(!manager.has_unresolved_sequences(), "the partition is dropped either way");
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
            let b1_response = PartitionResponse::with_options(
                PartitionResponseOptionsBuilder::new()
                    .set_error(Errors::None)
                    .set_base_offset(500)
                    .set_log_append_time(b1_append_time)
                    .set_log_start_offset(0)
                    .set_record_errors(Vec::new())
                    .set_error_message(None)
                    .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                    .build()
                    .unwrap(),
            );
            b1.complete(500, b1_append_time);
            manager
                .handle_completed_batch(&b1, &b1_response)
                .expect("the completion is recorded");

            // We get an UNKNOWN_PRODUCER_ID, so bump the epoch and set sequence numbers back to 0
            let b2_response = PartitionResponse::with_options(
                PartitionResponseOptionsBuilder::new()
                    .set_error(Errors::UnknownProducerId)
                    .set_base_offset(-1)
                    .set_log_append_time(-1)
                    .set_log_start_offset(500)
                    .set_record_errors(Vec::new())
                    .set_error_message(None)
                    .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                    .build()
                    .unwrap(),
            );
            assert!(
                manager
                    .can_retry(
                        &b2_response,
                        &b2.topic_partition,
                        in_flight_key(&b2),
                        b2.sequence_has_been_reset(),
                        &mut [],
                    )
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
                        TransactionManager::NO_INFLIGHT_REQUEST_CORRELATION_ID
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

            let tp0b1_response = PartitionResponse::with_options(
                PartitionResponseOptionsBuilder::new()
                    .set_error(Errors::None)
                    .set_base_offset(-1)
                    .set_log_append_time(-1)
                    .set_log_start_offset(400)
                    .set_record_errors(Vec::new())
                    .set_error_message(None)
                    .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                    .build()
                    .unwrap(),
            );
            manager
                .handle_completed_batch(&tp0b1, &tp0b1_response)
                .expect("the completion is recorded");

            let tp1b1_response = PartitionResponse::with_options(
                PartitionResponseOptionsBuilder::new()
                    .set_error(Errors::None)
                    .set_base_offset(-1)
                    .set_log_append_time(-1)
                    .set_log_start_offset(400)
                    .set_record_errors(Vec::new())
                    .set_error_message(None)
                    .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                    .build()
                    .unwrap(),
            );
            manager
                .handle_completed_batch(&tp1b1, &tp1b1_response)
                .expect("the completion is recorded");

            let mut tp0b2 = write_idempotent_batch_with_value(&mut manager, &tp0(), "2");
            let mut tp1b2 = write_idempotent_batch_with_value(&mut manager, &tp1(), "2");
            assert_eq!(manager.sequence_number(&tp0()), 2);
            assert_eq!(manager.sequence_number(&tp1()), 2);

            let b1_response = PartitionResponse::with_options(
                PartitionResponseOptionsBuilder::new()
                    .set_error(Errors::UnknownProducerId)
                    .set_base_offset(-1)
                    .set_log_append_time(-1)
                    .set_log_start_offset(400)
                    .set_record_errors(Vec::new())
                    .set_error_message(None)
                    .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                    .build()
                    .unwrap(),
            );
            assert!(
                manager
                    .can_retry(
                        &b1_response,
                        &tp0b1.topic_partition,
                        in_flight_key(&tp0b1),
                        tp0b1.sequence_has_been_reset(),
                        &mut [],
                    )
                    .expect("the retry decision is made")
            );

            let b2_response = PartitionResponse::with_options(
                PartitionResponseOptionsBuilder::new()
                    .set_error(Errors::None)
                    .set_base_offset(-1)
                    .set_log_append_time(-1)
                    .set_log_start_offset(400)
                    .set_record_errors(Vec::new())
                    .set_error_message(None)
                    .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                    .build()
                    .unwrap(),
            );
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
            let b1_response = PartitionResponse::with_options(
                PartitionResponseOptionsBuilder::new()
                    .set_error(Errors::None)
                    .set_base_offset(500)
                    .set_log_append_time(0)
                    .set_log_start_offset(0)
                    .set_record_errors(Vec::new())
                    .set_error_message(None)
                    .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                    .build()
                    .unwrap(),
            );
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

            let b2_response = PartitionResponse::with_options(
                PartitionResponseOptionsBuilder::new()
                    .set_error(Errors::None)
                    .set_base_offset(500)
                    .set_log_append_time(0)
                    .set_log_start_offset(0)
                    .set_record_errors(Vec::new())
                    .set_error_message(None)
                    .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                    .build()
                    .unwrap(),
            );
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
                .handle_completed_batch(
                    &b1,
                    &PartitionResponse::with_options(
                        PartitionResponseOptionsBuilder::new()
                            .set_error(Errors::None)
                            .set_base_offset(500)
                            .set_log_append_time(0)
                            .set_log_start_offset(0)
                            .set_record_errors(Vec::new())
                            .set_error_message(None)
                            .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                            .build()
                            .unwrap(),
                    ),
                )
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
                .handle_failed_batch(&b2, &timeout_error(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");
            assert!(manager.has_unresolved_sequences());

            // We only had one inflight batch, so we should be able to clear the unresolved status
            // and bump the epoch
            manager.maybe_resolve_sequences(Caller::Sender).expect("resolving succeeds");
            assert!(!manager.has_unresolved_sequences());

            // Java reaches the bump through `runUntil(.. epoch == 6)`.
            let mut pool = InFlightBatchPool::new();
            run_manager_transaction_phase(
                &mut manager,
                &mut pool,
                &mut pending,
                TransactionManager::NO_INFLIGHT_REQUEST_CORRELATION_ID,
            );
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
                .handle_failed_batch(&b1, &timeout_error(), false, &mut [], Caller::Sender)
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
                .handle_failed_batch(&b2, &timeout_error(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(manager.has_unresolved_sequences());

            // The third batch succeeds, which should resolve the sequence number without
            // requiring a producerId reset.
            manager
                .handle_completed_batch(
                    &b3,
                    &PartitionResponse::with_options(
                        PartitionResponseOptionsBuilder::new()
                            .set_error(Errors::None)
                            .set_base_offset(500)
                            .set_log_append_time(0)
                            .set_log_start_offset(0)
                            .set_record_errors(Vec::new())
                            .set_error_message(None)
                            .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                            .build()
                            .unwrap(),
                    ),
                )
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
                .handle_failed_batch(&b1, &timeout_error(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");
            assert!(manager.has_unresolved_sequences());

            // The second batch succeeds, but sequence numbers are still not resolved
            manager
                .handle_completed_batch(
                    &b2,
                    &PartitionResponse::with_options(
                        PartitionResponseOptionsBuilder::new()
                            .set_error(Errors::None)
                            .set_base_offset(500)
                            .set_log_append_time(0)
                            .set_log_start_offset(0)
                            .set_record_errors(Vec::new())
                            .set_error_message(None)
                            .set_current_leader(crate::produce_response_data::LeaderIdAndEpoch::new())
                            .build()
                            .unwrap(),
                    ),
                )
                .expect("the completion is recorded");
            let mut pool = InFlightBatchPool::new();
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), producer_id_and_epoch);
            assert!(manager.has_unresolved_sequences());

            // When the last inflight batch fails, we have to bump the epoch
            manager
                .handle_failed_batch(&b3, &timeout_error(), false, &mut [], Caller::Sender)
                .expect("the failure is recorded");

            // Java reaches the bump through `runUntil(.. epoch == 2)`.
            run_manager_transaction_phase(
                &mut manager,
                &mut pool,
                &mut pending,
                TransactionManager::NO_INFLIGHT_REQUEST_CORRELATION_ID,
            );
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
                    &Error::with_message(Errors::OutOfOrderSequenceNumber, "out of sequence"),
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
                .transition_to_fatal_error(bare_kafka_error(), Caller::App)
                .expect("FATAL_ERROR is always a valid target");

            // The second batch should not bump the epoch as txn manager is already in fatal error state
            let b2 = write_idempotent_batch_with_value(&mut manager, &tp0, "2");
            manager
                .handle_failed_batch(&b2, &timeout_error(), true, &mut [], Caller::Sender)
                .expect("the failure is ignored in a fatal state");
            manager
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("nothing to bump");
            assert_eq!(manager.producer_id_and_epoch(), id_and_epoch_after_first_batch);
        }
    }

    // =====================================================================
    // PHASE-5B TEST ACCOUNTING (`definition-of-done.md` §3)
    //
    // `TransactionManagerTest.java` has 140 test methods. Every one lands in
    // exactly one of two groups, and the split is derived mechanically rather
    // than asserted in prose — Critic 44 issues 6 and 7 were the two failure
    // modes of the prose form: a hand-assembled list silently lost an entry while
    // claiming completeness, and the counts written beside the lists drifted from
    // them.
    //
    // SCOPE CRITERION (Phase 5a). A method was in Phase-5a scope iff its body
    // reached no Phase-5b surface, i.e. none of `beginCommit`, `beginAbort`,
    // `sendOffsetsToTransaction`, the four Phase-5b request families
    // (`AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn`),
    // `transactionContainsPartition` or `isTransactionV2Enabled` — nor any of the
    // six test helpers that reach one of those (`assertAbortableError`,
    // `assertFatalError`, `verifyProducerFenced`,
    // `verifyProducerFencedForInitProducerId`,
    // `verifyCommitOrAbortTransactionRetriable`, `writeTransactionalBatchWithValue`,
    // `prepareGroupMetadataCommit`).
    //
    // `maybeAddPartition` is a *conditional* marker, because Phase 5a translates
    // the three branches of its transactional arm that need no request handler
    // (see `TransactionManager::maybe_add_partition`). It puts a method out of
    // scope only where the call must *succeed* for a *transactional* producer:
    // suppressed on an `assertThrows` line, and in a method that builds an
    // idempotent manager with `initializeTransactionManager(Optional.empty()`.
    //
    // DERIVATION. Save as `/tmp/scope.awk` and run from the repo root:
    //
    //   /^    (public )?void [a-zA-Z0-9_]+\(/ {
    //     if (name != "") emit()
    //     match($0, /void [a-zA-Z0-9_]+\(/)
    //     name = substr($0, RSTART+5, RLENGTH-6); line = NR
    //     delete hard; soft = 0; idem = 0; on = 1; next }
    //   /^    private / { if (name != "") on = 0 }
    //   on {
    //     if (index($0, "initializeTransactionManager(Optional.empty()") > 0) idem = 1
    //     n = split(MARKERS, m, ",")
    //     for (i = 1; i <= n; i++) if (index($0, m[i]) > 0) hard[m[i]] = 1
    //     if (index($0, "maybeAddPartition") > 0 && index($0, "assertThrows") == 0) soft = 1 }
    //   END { if (name != "") emit() }
    //   function emit() { hits = ""
    //     n = split(MARKERS, m, ",")
    //     for (i = 1; i <= n; i++) if (m[i] in hard) hits = hits (hits == "" ? "" : "+") ABBREV[m[i]]
    //     if (!idem && soft) hits = hits (hits == "" ? "" : "+") "mAP"
    //     printf "%s\t%s\t%s\n", line, name, (hits == "" ? "-" : hits) }
    //   BEGIN {
    //     ABBREV["beginCommit"]="bC"; ABBREV["beginAbort"]="bA"
    //     ABBREV["sendOffsetsToTransaction"]="sOT"; ABBREV["transactionContainsPartition"]="tCP"
    //     ABBREV["isTransactionV2Enabled"]="TV2"; ABBREV["AddPartitionsToTxn"]="AP"
    //     ABBREV["AddOffsetsToTxn"]="AO"; ABBREV["TxnOffsetCommit"]="TOC"; ABBREV["EndTxn"]="ET"
    //     ABBREV["assertAbortableError"]="aAE"; ABBREV["assertFatalError"]="aFE"
    //     ABBREV["verifyProducerFenced"]="vPF"
    //     ABBREV["verifyProducerFencedForInitProducerId"]="vPFI"
    //     ABBREV["verifyCommitOrAbortTransactionRetriable"]="vCAR"
    //     ABBREV["writeTransactionalBatchWithValue"]="wTB"
    //     ABBREV["prepareGroupMetadataCommit"]="pGM" }
    //
    // Two properties of that program are load-bearing and were both wrong in the
    // revision Critic 45 issue 5 reviewed:
    //
    //   - `soft = 0` at the top, **not** `delete soft`. `delete` types the name as an
    //     array, and the later scalar assignment then aborts with "can't read value of
    //     soft; it's an array name" on this environment's `awk version 20200816` (BWK
    //     awk; neither `gawk` nor `mawk` is installed). Three of the five checks below
    //     printed `0` as a result — the headline 140 / 33 / 107 among them.
    //   - `emit` walks `MARKERS` in declaration order rather than `for (k in hard)`,
    //     whose order awk leaves unspecified. Without that, the GROUP B listing below
    //     is not reproducible across implementations, which matters precisely because
    //     it is pasted output.
    //
    //   J=kafka/clients/src/test/java/org/apache/kafka/clients/producer/internals/TransactionManagerTest.java
    //   M='beginCommit,beginAbort,sendOffsetsToTransaction,transactionContainsPartition,'
    //   M="$M"'isTransactionV2Enabled,AddPartitionsToTxn,AddOffsetsToTxn,TxnOffsetCommit,EndTxn,'
    //   M="$M"'assertAbortableError,assertFatalError,verifyProducerFenced,'
    //   M="$M"'verifyProducerFencedForInitProducerId,verifyCommitOrAbortTransactionRetriable,'
    //   M="$M"'writeTransactionalBatchWithValue,prepareGroupMetadataCommit'
    //   awk -v MARKERS="$M" -f /tmp/scope.awk "$J" > /tmp/scope.tsv     # exit 0
    //
    //   # the block splitter must break at `private` members too, or the helpers
    //   # sitting between two tests are absorbed into the earlier one — which
    //   # mis-blocked `testDuplicateSequenceAfterProducerReset` on the first pass
    //   awk -F'\t' '$2!="setup"' /tmp/scope.tsv | wc -l          # 140
    //   grep -c '^    @Test' "$J"                                # 122
    //   grep -c '^    @ParameterizedTest' "$J"                   # 18   (122+18=140)
    //   awk -F'\t' '$3=="-" && $2!="setup"' /tmp/scope.tsv | wc -l   # 33  = group A
    //   awk -F'\t' '$3!="-"' /tmp/scope.tsv | wc -l                  # 107 = group B
    //
    // Real output of all five, run from the repo root on the awk named above:
    // `awk exit=0`, then `140`, `122`, `18`, `33`, `107`.
    //
    // 33 + 107 = 140, so every method is placed exactly once.
    //
    // Two facts the criterion depends on, checkable with the same file:
    //
    //   - No group-A method builds a Transaction V2 manager, so the Rust
    //     `do_init_transactions` / `run_init_transactions` helpers may omit
    //     Java's `maybeUpdateTransactionV2Enabled(true)` (Java 4360) — with
    //     `transaction.version` finalized at level 1 the call leaves
    //     `isTransactionV2Enabled` false, and `onInitialization = true` suppresses
    //     its only side effect (Java 500-501). Check — all 15 lines printed pass
    //     `Optional.empty()`, i.e. an *idempotent* manager, which never reaches
    //     `maybeUpdateTransactionV2Enabled` at all:
    //       awk -F'\t' '$3=="-" && $2!="setup" {print $1}' /tmp/scope.tsv | while read a; do
    //         awk -v s=$a 'NR>s{if($0=="    }")exit;print}' "$J" \
    //           | grep -H --label="line $a" 'initializeTransactionManager('; done
    //     Nothing is printed for a method that inherits `setup()`'s
    //     `Optional.of(transactionalId), false, false` (Java 168) instead.
    //   - Java carries four *character-identical* pairs, which one Rust test each
    //     covers (`definition-of-done.md` §6); the pairing is named in each Rust
    //     test's rustdoc. Check (prints four `==` lines):
    //       for p in 263:506 283:511 289:517 297:525; do a=${p%%:*}; b=${p##*:}
    //         diff <(awk -v s=$a 'NR>s{if($0=="    }")exit;print}' "$J") \
    //              <(awk -v s=$b 'NR>s{if($0=="    }")exit;print}' "$J") \
    //           >/dev/null && echo "$a == $b"; done
    //
    // GROUP A — in Phase-5a scope (33), all translated. `→` names the Rust test
    // and the file it is in; `[Pn]` the phase that landed it.
    //
    //   `testFailIfNotReadyForSendNoProducerId` (263) [5a]
    //     → test_fail_if_not_ready_for_send_no_producer_id, with (506).
    //   `testFailIfNotReadyForSendIdempotentProducer` (269) [3]
    //     → test_fail_if_not_ready_for_send_idempotent_producer.
    //   `testFailIfNotReadyForSendIdempotentProducerFatalError` (276) [3]
    //     → test_fail_if_not_ready_for_send_idempotent_producer_fatal_error.
    //   `testFailIfNotReadyForSendNoOngoingTransaction` (283) [5a]
    //     → test_fail_if_not_ready_for_send_no_ongoing_transaction, with (511).
    //   `testFailIfNotReadyForSendAfterAbortableError` (289) [5a]
    //     → test_fail_if_not_ready_for_send_after_abortable_error, with (517).
    //   `testFailIfNotReadyForSendAfterFatalError` (297) [5a]
    //     → test_fail_if_not_ready_for_send_after_fatal_error, with (525).
    //   `testNotReadyForSendBeforeInitTransactions` (506) [5a] → paired with (263).
    //   `testNotReadyForSendBeforeBeginTransaction` (511) [5a] → paired with (283).
    //   `testNotReadyForSendAfterAbortableError` (517) [5a] → paired with (289).
    //   `testNotReadyForSendAfterFatalError` (525) [5a] → paired with (297).
    //   `testIsSendToPartitionAllowedWithPartitionNotAdded` (617) [5a]
    //     → test_is_send_to_partition_allowed_with_partition_not_added.
    //   `testDefaultSequenceNumber` (625) [3] → test_default_sequence_number.
    //   `testBumpEpochAndResetSequenceNumbersAfterUnknownProducerId` (634) [3]
    //     → test_bump_epoch_and_reset_sequence_numbers_after_unknown_producer_id.
    //   `testBatchFailureAfterProducerReset` (668) [3] → test_batch_failure_after_producer_reset.
    //   `testBatchCompletedAfterProducerReset` (710) [3] → test_batch_completed_after_producer_reset.
    //   `testDuplicateSequenceAfterProducerReset` (749) [4]
    //     → sender.rs test_duplicate_sequence_after_producer_reset.
    //   `testSequenceNumberOverflow` (851) [3] → test_sequence_number_overflow.
    //   `testProducerIdReset` (864) [3] → test_producer_id_reset.
    //   `testDisconnectAndRetry` (1081) [5a] → sender.rs test_disconnect_and_retry.
    //   `testInitializeTransactionsTwiceRaisesError` (1094) [5a]
    //     → test_initialize_transactions_twice_raises_error.
    //   `testUnsupportedFindCoordinator` (1101) [5a] → sender.rs test_unsupported_find_coordinator.
    //   `testUnsupportedInitTransactions` (1118) [5a]
    //     → sender.rs test_unsupported_init_transactions.
    //   `testLookupCoordinatorOnDisconnectAfterSend` (1261) [5a]
    //     → sender.rs test_lookup_coordinator_on_disconnect_after_send.
    //   `testLookupCoordinatorOnDisconnectBeforeSend` (1293) [5a]
    //     → sender.rs test_lookup_coordinator_on_disconnect_before_send.
    //   `testLookupCoordinatorOnNotCoordinatorError` (1324) [5a]
    //     → test_lookup_coordinator_on_not_coordinator_error.
    //   `testCoordinatorNotAvailable` (2013) [5a] → test_coordinator_not_available.
    //   `testBumpEpochAfterTimeoutWithoutPendingInflightRequests` (3040) [3]
    //     → test_bump_epoch_after_timeout_without_pending_inflight_requests.
    //   `testNoProducerIdResetAfterLastInFlightBatchSucceeds` (3084) [3]
    //     → test_no_producer_id_reset_after_last_in_flight_batch_succeeds.
    //   `testEpochBumpAfterLastInFlightBatchFailsIdempotentProducer` (3125) [3]
    //     → test_epoch_bump_after_last_in_flight_batch_fails_idempotent_producer.
    //   `testNoFailedBatchHandlingWhenTxnManagerIsInFatalError` (3245) [3]
    //     → test_no_failed_batch_handling_when_txn_manager_is_in_fatal_error.
    //   `testHealthyPartitionRetriesDuringEpochBump` (3601) [4]
    //     → sender.rs test_healthy_partition_retries_during_epoch_bump.
    //   `testNeedToTriggerEpochBumpFromClientDuringCoordinatorDisconnect` (3715) [5a]
    //     → test_need_to_trigger_epoch_bump_from_client_during_coordinator_disconnect.
    //   `testFailedInflightBatchAfterEpochBump` (3726) [4]
    //     → sender.rs test_failed_inflight_batch_after_epoch_bump.
    //
    // GROUP B — the 107 Phase 5b owns. Phase 5b translated 60 and owed 47; **Phase 8
    // landed the 47, so group B is now wholly translated and nothing is owed.** The
    // split is derived rather than asserted: a method counts as translated iff its name
    // appears in the Rust producer sources *outside* the accounting blocks, which is
    // exactly the property the "Translated from" header of every test above
    // establishes.
    //
    //   # the Rust sources with the three accounting blocks cut out. The line
    //   # numbers are read from the files rather than written here, so an edit that
    //   # moves a block cannot silently shrink the corpus. The `test -s` guard is
    //   # not decoration: an empty corpus would report all 140 as owed, and a
    //   # vacuous extraction is how both agents have been burned before.
    //   T=src/producer/internals/transaction_manager.rs
    //   S=src/producer/internals/sender.rs
    //   # anchored at the start of the line, because each title also appears inside
    //   # a doc comment and inside this very derivation, so an unanchored grep returns
    //   # **more than one** line number and `$(( .. - 1 ))` then fails loudly rather
    //   # than cutting the wrong range. How many varies per title and with every edit
    //   # to the surrounding prose, so the count is not written down here — check it
    //   # with, e.g.:
    //   #   grep -c 'PHASE-5B METHOD ACCOUNTING' $T   # unanchored: >1
    //   #   grep -c '^// PHASE-5B METHOD ACCOUNTING' $T   # anchored:   1
    //   ma=$(( $(grep -n '^// PHASE-5B METHOD ACCOUNTING' $T | cut -d: -f1) - 1 ))
    //   mb=$(awk -v s=$ma 'NR>s && /^\/\/ ={20,}/{print NR; exit}' $T)
    //   ta=$(( $(grep -n '^    // PHASE-5B TEST ACCOUNTING' $T | cut -d: -f1) - 1 ))
    //   tb=$(awk -v s=$ta 'NR>s && /^    \/\/ ={20,}/{print NR; exit}' $T)
    //   sa=$(( $(grep -n '^    // `SenderTest.java` accounting' $S | cut -d: -f1) - 1 ))
    //   sb=$(awk -v s=$sa 'NR>s && !/^    \/\//{print NR-1; exit}' $S)
    //   { sed "${ta},${tb}d;${ma},${mb}d" $T; sed "${sa},${sb}d" $S; \
    //     cat src/producer/internals/record_accumulator.rs; } > /tmp/rust_nonacct.txt
    //   test -s /tmp/rust_nonacct.txt && [ $(wc -l < /tmp/rust_nonacct.txt) -gt 15000 ]
    //
    //   # status of every one of the 140
    //   awk -F'\t' '$2!="setup"{print $1"\t"$2"\t"$3}' /tmp/scope.tsv \
    //   | while IFS=$'\t' read -r ln nm grp; do
    //       grep -q "$nm\`" /tmp/rust_nonacct.txt && st=HAVE || st=OWED
    //       printf "%s\t%s\t%s\t%s\n" "$ln" "$nm" "$grp" "$st"; done > /tmp/status.tsv
    //   awk -F'\t' '$3=="-" && $4=="OWED"' /tmp/status.tsv | wc -l    # 0 — group A intact
    //   awk -F'\t' '$3!="-" && $4=="HAVE"' /tmp/status.tsv | wc -l    # 107
    //   awk -F'\t' '$3!="-" && $4=="OWED"' /tmp/status.tsv | wc -l    # 0
    //
    // Real output of the four, run from the repo root over the Phase-8 tree: the guard
    // exits 0, then `0`, `107`, `0`. 33 + 107 + 0 = 140, so every method is still placed
    // exactly once — and now every one of them is translated. The named listing of what
    // is still owed is the empty set, which is checkable in one line rather than trusted:
    //
    //   awk -F'\t' '$4=="OWED" {print $1"\t"$2}' /tmp/status.tsv    # prints nothing
    //
    // Note the status grep matches `NAME`` rather than ``NAME`` — one Rust test
    // writes `TransactionManagerTest.testDuplicateSequenceAfterProducerReset`, and
    // requiring the opening backtick reported it as owed when it is not.
    //
    // WHY THE 47 WERE OWED, and the check that said so — kept because it is what made
    // the hand-forward to Phase 8 checkable rather than asserted, and because the
    // classifier below is still the cross-check that the marker set has not drifted.
    // Every one of the 47 drove the **accumulator or the `Sender`** —
    // `appendToAccumulator`, a produce response, a drain, `initiateClose`, or
    // `verifyCommitOrAbortTransactionRetriable`, the one helper that reaches them and is
    // itself reachable (it accounted for 4 of the 47). None was blocked on manager
    // surface: Phase 5b translated all 90 of `TransactionManager`'s methods (see the
    // PHASE-5B METHOD ACCOUNTING block), which is why Phase 8 needed only the harness in
    // `sender.rs` and no new production surface. **All 47 landed in `sender.rs`**, beside
    // the six group-A entries that were already there, for the reason its section header
    // gives: Java's `TransactionManagerTest` builds its own accumulator + `Sender` +
    // `MockClient` and every one of these bodies drives them.
    //
    // `verifyProducerFenced(` is in the classifier's alternation below and matches
    // **0 of the 107** — it is inert, and an earlier revision of this paragraph cited
    // it as one of "two helpers that do", over-stating the evidence for the phase's
    // most load-bearing check (Critic 45 5b issue 4a). Why it is inert is the more
    // useful fact: its only call sites (Java 2077, 2101) sit inside the *private*
    // helpers `verifyProducerFencedForAddPartitionsToTxn` / `..ForAddOffsetsToTxn`,
    // and the splitter stops collecting at `^    private `, so no test method's marker
    // set can contain it. Kept in the alternation rather than deleted, because
    // removing it must be a no-op and that is checkable:
    //
    //   # rerun the classifier with `verifyProducerFenced(` dropped from the grep
    //   diff /tmp/class.tsv /tmp/class_nofence.tsv   # empty
    //
    // **This is also the classifier's one error direction, and it is the harmless
    // one.** Not following private helpers can only under-report ACC, i.e. produce a
    // false *MGR* — never a false ACC. So it cannot manufacture the `OWED_MGR == 0`
    // below. The four methods it actually mis-labels are the `verifyProducerFenced`
    // family (Java 2057, 2062, 2081, 2086), and all four are `HAVE`, each translated
    // minus the produce-future assertion its own rustdoc names.
    // They belong with the transactional `SenderTest` group, whose harness is
    // `sender.rs`'s `SenderTestContext`; the `SenderTest.java` accounting block in
    // that file is where they are owed.
    //
    //   # classify each group-B method by whether its body reaches that machinery
    //   J=kafka/clients/src/test/java/org/apache/kafka/clients/producer/internals/TransactionManagerTest.java
    //   awk -F'\t' '$3!="-" {print $1"\t"$2}' /tmp/scope.tsv \
    //   | while IFS=$'\t' read -r ln nm; do
    //       body=$(awk -v s=$ln 'NR>s{if($0=="    }")exit;print}' "$J")
    //       needs=MGR
    //       echo "$body" | grep -q 'appendToAccumulator\|accumulator\.\|sender\.\|ProduceResponse\|drain\|writeTransactionalBatchWithValue\|initiateClose\|verifyCommitOrAbortTransactionRetriable\|verifyProducerFenced(' \
    //         && needs=ACC
    //       printf "%s\t%s\t%s\n" "$ln" "$nm" "$needs"; done > /tmp/class.tsv
    //   awk -F'\t' '$3=="MGR"' /tmp/class.tsv | wc -l                  # 57
    //   awk -F'\t' '$3=="ACC"' /tmp/class.tsv | wc -l                  # 50
    //
    //   # THE LOAD-BEARING ONE while anything was owed: no owed method was manager-only,
    //   # i.e. nothing was deferred that Phase 5b's own surface could have covered. It is
    //   # now trivially 0 because the OWED set is empty; kept so a reviewer re-running the
    //   # block gets the same transcript, and so the check is already in place if a future
    //   # phase ever defers one of these again.
    //   join -t$'\t' -1 2 -2 2 \
    //     <(awk -F'\t' '$3!="-" && $4=="OWED" {print $1"\t"$2}' /tmp/status.tsv | sort -t$'\t' -k2,2) \
    //     <(sort -t$'\t' -k2,2 /tmp/class.tsv) | awk -F'\t' '$4=="MGR"' | wc -l   # 0
    //
    // Real output of the three: `57`, `50`, `0`.
    //
    // 57 MGR + 50 ACC = 107, which is now also 107 HAVE + 0 OWED. The MGR/ACC split is
    // *not* the HAVE/OWED partition and never was — three ACC methods were translated by
    // Phase 5b anyway, minus a named accumulator leg:
    // `testTransactionV2AddPartitionAndOffsets` (984, minus its two
    // `appendToAccumulator` legs), `testMaybeResolveSequencesTransactionalProducer`
    // (3159, whose branch is driven directly) and
    // `testFindCoordinatorAllowedInAbortableErrorState` (2355, likewise). Each says so in
    // its own rustdoc, and each **keeps** that note: Phase 8 did not revisit them, so the
    // named legs are still elided and the rustdoc is still the record of it.
    //
    // GROUP B, TRANSLATED (107) — all of it. Columns: Java declaration line, Java name,
    // the blocking identifiers the derivation found (`ABBREV` above spells them). This
    // *is* pasted derivation output:
    //
    //   join -t$'\t' -1 2 -2 2 \
    //     <(awk -F'\t' '$3!="-" && $4=="HAVE" {print $1"\t"$2"\t"$3}' /tmp/status.tsv | sort -t$'\t' -k2,2) \
    //     <(sort -t$'\t' -k2,2 /tmp/class.tsv) \
    //   | awk -F'\t' '{print $2"\t"$1"\t"$3}' | sort -k1,1n \
    //   | awk -F'\t' '{printf "    //   %-4s %-74s %s\n", $1, $2, $3}'
    //
    //   228  testSenderShutdownWithPendingTransactions                                  bC+AP+ET+mAP
    //   249  testEndTxnNotSentIfIncompleteBatches                                       bC+tCP+AP+ET+mAP
    //   304  testHasOngoingTransactionSuccessfulAbort                                   bA+tCP+AP+ET+mAP
    //   328  testHasOngoingTransactionSuccessfulCommit                                  bC+tCP+AP+ET+mAP
    //   352  testHasOngoingTransactionAbortableError                                    bA+tCP+AP+ET+mAP
    //   379  testHasOngoingTransactionFatalError                                        tCP+AP+mAP
    //   400  testMaybeAddPartitionToTransaction                                         tCP+AP+mAP
    //   425  testMaybeAddPartitionToTransactionInTransactionV2                          tCP+mAP
    //   445  testAddPartitionToTransactionOverridesRetryBackoffForConcurrentTransactions tCP+AP+mAP
    //   464  testAddPartitionToTransactionRetainsRetryBackoffForRegularRetriableError   tCP+AP+mAP
    //   483  testAddPartitionToTransactionRetainsRetryBackoffWhenPartitionsAlreadyAdded tCP+AP+mAP
    //   532  testIsSendToPartitionAllowedWithPendingPartitionAfterAbortableError        mAP
    //   544  testIsSendToPartitionAllowedWithInFlightPartitionAddAfterAbortableError    AP+mAP
    //   559  testIsSendToPartitionAllowedWithPendingPartitionAfterFatalError            mAP
    //   571  testIsSendToPartitionAllowedWithInFlightPartitionAddAfterFatalError        AP+mAP
    //   586  testIsSendToPartitionAllowedWithAddedPartitionAfterAbortableError          AP+mAP
    //   602  testIsSendToPartitionAllowedWithAddedPartitionAfterFatalError              AP+mAP
    //   881  testBasicTransaction                                                       bC+sOT+tCP+AP+AO+TOC+ET+mAP
    //   934  testTransactionManagerEnablesV2                                            bC+tCP+TV2+AP+ET+mAP
    //   984  testTransactionV2AddPartitionAndOffsets                                    bC+sOT+tCP+TOC+ET+mAP
    //   1034 testTransactionManagerDisablesV2                                           TV2+TOC
    //   1137 testUnsupportedForMessageFormatInTxnOffsetCommit                           sOT+AO+TOC+aFE
    //   1159 testFencedInstanceIdInTxnOffsetCommitByGroupMetadata                       sOT+AO+TOC+aAE
    //   1193 testUnknownMemberIdInTxnOffsetCommitByGroupMetadata                        sOT+AO+TOC+aAE
    //   1226 testIllegalGenerationInTxnOffsetCommitByGroupMetadata                      sOT+AO+TOC+aAE
    //   1351 testTransactionalIdAuthorizationFailureInFindCoordinator                   aFE
    //   1366 testTransactionalIdAuthorizationFailureInInitProducerId                    aAE
    //   1381 testGroupAuthorizationFailureInFindCoordinator                             sOT+AO+aAE
    //   1406 testGroupAuthorizationFailureInTxnOffsetCommit                             sOT+AO+TOC+aAE
    //   1435 testFatalErrorWhenProduceResponseWithInvalidPidMapping                     mAP
    //   1451 testTransactionalIdAuthorizationFailureInAddOffsetsToTxn                   sOT+AO+aFE
    //   1471 testInvalidTxnStateFailureInAddOffsetsToTxn                                sOT+AO+aFE
    //   1491 testTransactionalIdAuthorizationFailureInTxnOffsetCommit                   sOT+AO+TOC+aFE
    //   1516 testTopicAuthorizationFailureInAddPartitions                               tCP+AP+aAE+mAP
    //   1553 testCommitWithTopicAuthorizationFailureInAddPartitionsInFlight             bC+AP+mAP
    //   1602 testRecoveryFromAbortableErrorTransactionNotStarted                        bC+bA+tCP+AP+ET+mAP
    //   1648 testRetryAbortTransactionAfterTimeout                                      bC+bA+tCP+AP+ET+mAP
    //   1680 testRetryCommitTransactionAfterTimeout                                     bC+bA+tCP+AP+ET+mAP
    //   1714 testRetryInitTransactionsAfterTimeout                                      bC+bA
    //   1746 testRecoveryFromAbortableErrorTransactionStarted                           bC+bA+tCP+AP+ET+mAP
    //   1799 testRecoveryFromAbortableErrorProduceRequestInRetry                        bC+bA+tCP+AP+ET+mAP
    //   1863 testTransactionalIdAuthorizationFailureInAddPartitions                     AP+aFE+mAP
    //   1879 testInvalidTxnStateInAddPartitions                                         AP+aFE+mAP
    //   1895 testFlushPendingPartitionsOnCommit                                         bC+tCP+AP+ET+mAP
    //   1932 testMultipleAddPartitionsPerForOneProduce                                  tCP+AP+mAP
    //   1979 testRetriableErrors                                                        bC+tCP+AP+TOC+ET+mAP
    //   2028 testProducerFencedExceptionInInitProducerId                                vPF+vPFI
    //   2033 testInvalidProducerEpochConvertToProducerFencedInInitProducerId            vPF+vPFI
    //   2057 testProducerFencedInAddPartitionToTxn                                      AP+vPF
    //   2062 testInvalidProducerEpochConvertToProducerFencedInAddPartitionToTxn         AP+vPF
    //   2081 testProducerFencedInAddOffSetsToTxn                                        AO+vPF
    //   2086 testInvalidProducerEpochConvertToProducerFencedInAddOffSetsToTxn           AO+vPF
    //   2125 testInvalidProducerEpochConvertToProducerFencedInEndTxn                    bC+bA+sOT+AP+ET+mAP
    //   2155 testInvalidProducerEpochFromProduce                                        bA+AP+ET+mAP
    //   2189 testDisallowCommitOnProduceFailure                                         bC+bA+AP+ET+mAP
    //   2217 testAllowAbortOnProduceFailure                                             bA+AP+ET+mAP
    //   2240 testAbortableErrorWhileAbortInProgress                                     bA+AP+ET+mAP
    //   2270 testCommitTransactionWithUnsentProduceRequest                              bC+AP+ET+mAP
    //   2313 testCommitTransactionWithInFlightProduceRequest                            bC+AP+ET+mAP
    //   2355 testFindCoordinatorAllowedInAbortableErrorState                            AP+mAP
    //   2377 testCancelUnsentAddPartitionsAndProduceOnAbort                             bA+ET+mAP
    //   2398 testAbortResendsAddPartitionErrorIfRetried                                 bA+AP+ET+mAP
    //   2424 testAbortResendsProduceRequestIfRetried                                    bA+AP+ET+mAP
    //   2452 testHandlingOfUnknownTopicPartitionErrorOnAddPartitions                    tCP+AP+mAP
    //   2473 testHandlingOfUnknownTopicPartitionErrorOnTxnOffsetCommit                  TOC
    //   2478 testHandlingOfCoordinatorLoadingErrorOnTxnOffsetCommit                     TOC
    //   2483 testHandlingOfNetworkExceptionOnTxnOffsetCommit                            TOC
    //   2523 testHandlingOfProducerFencedErrorOnTxnOffsetCommit                         TOC
    //   2528 testHandlingOfTransactionalIdAuthorizationFailedErrorOnTxnOffsetCommit     TOC
    //   2533 testHandlingOfInvalidProducerEpochErrorOnTxnOffsetCommit                   TOC
    //   2538 testHandlingOfUnsupportedForMessageFormatErrorOnTxnOffsetCommit            TOC
    //   2574 shouldNotAddPartitionsToTransactionWhenTopicAuthorizationFailed            tCP+AP+mAP
    //   2588 shouldNotSendAbortTxnRequestWhenOnlyAddPartitionsRequestFailed             bA+AP+mAP
    //   2605 shouldNotSendAbortTxnRequestWhenOnlyAddOffsetsRequestFailed                bA+sOT+AO
    //   2624 shouldFailAbortIfAddOffsetsFailsWithFatalError                             bA+sOT+AO
    //   2643 testSendOffsetsWithGroupMetadata                                           TOC+pGM
    //   2666 testSendOffsetWithGroupMetadataFailAsAutoDowngradeTxnCommitNotEnabled      TOC+aFE+pGM
    //   2712 testNoDrainWhenPartitionsPending                                           mAP
    //   2746 testAllowDrainInAbortableErrorState                                        tCP+AP+mAP
    //   2775 testRaiseErrorWhenNoPartitionsPendingOnDrain                               AP+mAP
    //   2811 resendFailedProduceRequestAfterAbortableError                              AP+mAP
    //   2832 testTransitionToAbortableErrorOnBatchExpiry                                tCP+AP+mAP
    //   2870 testTransitionToAbortableErrorOnMultipleBatchExpiry                        tCP+AP+mAP
    //   2924 testDropCommitOnBatchExpiry                                                bC+bA+tCP+AP+ET+mAP
    //   2979 testTransitionToFatalErrorWhenRetriedBatchIsExpired                        bC+tCP+AP+mAP
    //   3159 testMaybeResolveSequencesTransactionalProducer                             tCP+TV2+AP+wTB+mAP
    //   3191 testEpochUpdateAfterBumpFromEndTxnResponseInV2                             bA+ET+mAP
    //   3218 testProducerIdAndEpochUpdateAfterOverflowFromEndTxnResponseInV2            bC+ET+mAP
    //   3269 testAbortTransactionAndReuseSequenceNumberOnError                          bA+tCP+AP+ET+mAP
    //   3325 testAbortTransactionAndResetSequenceNumberOnUnknownProducerId              bA+tCP+AP+ET+mAP
    //   3395 testBumpTransactionalEpochOnAbortableError                                 bA+tCP+AP+ET+mAP
    //   3441 testBumpTransactionalEpochOnUnknownProducerIdError                         bA+tCP+AP+ET+mAP
    //   3488 testBumpTransactionalEpochOnTimeout                                        bA+tCP+AP+ET+mAP
    //   3547 testBumpTransactionalEpochOnRecoverableAddPartitionRequestError            bA+AP+mAP
    //   3567 testBumpTransactionalEpochOnRecoverableAddOffsetsRequestError              bA+sOT+AP+AO+ET+mAP
    //   3695 testRetryAbortTransaction                                                  vCAR
    //   3700 testRetryCommitTransaction                                                 vCAR
    //   3705 testRetryAbortTransactionAfterCommitTimeout                                vCAR
    //   3710 testRetryCommitTransactionAfterAbortTimeout                                vCAR
    //   3819 testBackgroundInvalidStateTransitionIsFatal                                bC+bA+sOT
    //   3841 testForegroundInvalidStateTransitionIsRecoverable                          bC+bA+tCP+AP+ET+mAP
    //   3872 testTransactionAbortableExceptionInInitProducerId                          aAE
    //   3887 testTransactionAbortableExceptionInAddPartitions                           AP+aAE+mAP
    //   3903 testTransactionAbortableExceptionInFindCoordinator                         sOT+AO+aAE
    //   3925 testTransactionAbortableExceptionInEndTxn                                  bC+AP+ET+aAE+mAP
    //   3950 testTransactionAbortableExceptionInAddOffsetsToTxn                         sOT+AO+aAE
    //   3970 testTransactionAbortableExceptionInTxnOffsetCommit                         sOT+AO+TOC+aAE
    //
    // GROUP B, OWED — **empty.** The same listing with `$4=="OWED"` prints nothing; the
    // one-line check is above. Before Phase 8 this section held 47 rows.
    //
    // The whole of group B as a histogram over the blocking identifiers (a method
    // may be blocked by several) — kept from the Phase-5a block because it is the
    // cheapest cross-check that the marker set itself has not drifted:
    //
    //   awk -F'\t' '$3!="-" {n=split($3,m,"+"); for(i=1;i<=n;i++) c[m[i]]++} \
    //       END {for (k in c) printf "%4d  %s\n", c[k], k}' /tmp/scope.tsv \
    //     | sort -k1,1rn -k2,2
    //
    // (`sort -k1,1rn -k2,2` rather than a bare `sort -rn`, so equal counts do not come
    // back in awk's unspecified hash order.) Real output:
    //
    //     68 mAP    61 AP     36 ET     36 tCP    30 bA     25 bC
    //     20 TOC    19 sOT    18 AO     13 aAE     8 aFE     6 vPF
    //      4 vCAR    3 TV2     2 pGM     2 vPFI    1 wTB
    //
    // Note `TransactionManagerTest` contains **no** KIP-939 two-phase-commit test
    // in Apache Kafka 4.2: `prepareTransaction`, `preparedTransactionState` and
    // `enable2pc` appear in no method body, and the `doInitTransactionsWith2PCEnabled`
    // helper (Java 4367) is declared and never called. So no group-B entry is
    // blocked on 2PC, and Phase 5b's 2PC cover is three Rust-side tests
    // (`test_prepare_transaction_records_the_prepared_state`,
    // `test_prepare_transaction_is_refused_outside_a_transaction`,
    // `test_init_producer_id_resumes_a_prepared_transaction`) plus the
    // `KafkaProducerTest` cover PLAN §Phase-6 owns. Check:
    //   grep -c 'doInitTransactionsWith2PCEnabled' "$J"   # 1 — the declaration only
    //
    // Line numbers are the `void` declaration line throughout, here and in the
    // `Translated from` header of every test above.
    // =====================================================================
}
