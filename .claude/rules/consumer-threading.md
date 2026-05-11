# Consumer translation rules

This file consolidates design decisions specific to translating the Kafka
consumer (`org.apache.kafka.clients.consumer.*`) from Java to Rust. It
supplements `CLAUDE.md` — when these rules conflict with general translation
guidance, the consumer-specific rule wins inside the consumer module.

Rules are grouped by topic. Each numbered section is a single design
decision: the rule itself, **Why** (rationale, often referencing the Java
contract), and **How to apply** (concrete guidance for Actor / Critic).

## 1. API surface: async-only, no sync facade

The `AsyncKafkaConsumer` API exposes async equivalents of every method that
blocks in Java:

  - `async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>>`
  - `async fn commit_sync(...)`, `async fn commit_async(...)`
  - `async fn position(...)`, `async fn committed(...)`
  - `async fn beginning_offsets(...)`, `async fn end_offsets(...)`,
    `async fn offsets_for_times(...)`
  - `async fn subscribe(...)`, `async fn unsubscribe()`, `async fn close(...)`
  - `async fn partitions_for(...)`, `async fn list_topics(...)`

Java methods that do not block remain synchronous:

  - `assignment()`, `subscription()`, `paused()`, `metrics()`, `client_id()`,
    `group_metadata()`, `wakeup()`

**Why:** The underlying network stack (`Selector`, `NetworkClient`) is async
and the background task is a `tokio::spawn`. A sync `poll()` facade would
internally `block_on(async_poll(...))`, which (a) deadlocks when called from
an existing async context on a current-thread runtime, and (b) forces every
user-supplied trait (`ConsumerRebalanceListener`, `OffsetCommitCallback`,
`Deserializer`) to be sync — a constraint that propagates outward forever.

**How to apply:**

  - Do NOT introduce a `block_on`-wrapped sync façade (`KafkaConsumer::poll`
    that hides the runtime) in Milestone 8.
  - Users needing blocking semantics call `Handle::block_on(consumer.poll(...))`
    themselves from sync code, or accept the async API.
  - Adding a sync façade later is non-breaking; removing one would be.
    Defer the decision.

**Out of scope for Milestone 8:**

  - A `BaseConsumer`-style sync API (cf. `rdkafka` crate). Revisit only if a
    concrete user need surfaces.

## 2. Consumer dispatch surface: `#[async_trait]` + `Box<dyn Consumer>`

The Rust equivalent of Java's `Consumer<K, V>` interface is an `#[async_trait]`
trait with `Box<dyn Consumer<K, V>>` for runtime dispatch:

    use async_trait::async_trait;

    #[async_trait]
    pub trait Consumer<K, V>: Send + Sync + 'static
    where
        K: Send + Sync + 'static,
        V: Send + Sync + 'static,
    {
        // Blocking-in-Java methods become async (see §1).
        async fn poll(&mut self, timeout: Duration)
            -> Result<ConsumerRecords<K, V>, KafkaError>;
        async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError>;
        async fn subscribe_with_listener(
            &mut self,
            topics: Vec<String>,
            listener: Arc<dyn ConsumerRebalanceListener>,
        ) -> Result<(), KafkaError>;
        async fn unsubscribe(&mut self) -> Result<(), KafkaError>;
        async fn commit_sync(&mut self) -> Result<(), KafkaError>;
        async fn commit_async(&mut self) -> Result<(), KafkaError>;
        async fn position(&mut self, partition: &TopicPartition) -> Result<i64, KafkaError>;
        async fn committed(&mut self, partitions: &HashSet<TopicPartition>)
            -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError>;
        async fn close(&mut self, options: CloseOptions) -> Result<(), KafkaError>;
        // ... other methods that block in Java ...

        // Methods that do not block in Java stay sync.
        fn assignment(&self) -> HashSet<TopicPartition>;
        fn subscription(&self) -> HashSet<String>;
        fn paused(&self) -> HashSet<TopicPartition>;
        fn wakeup(&self);
        fn group_metadata(&self) -> ConsumerGroupMetadata;
        fn client_id(&self) -> &str;
    }

    pub fn new_consumer<K, V>(config: ConsumerConfig)
        -> Result<Box<dyn Consumer<K, V>>, KafkaError>
    where K: Send + Sync + 'static, V: Send + Sync + 'static
    {
        match config.group_protocol() {
            GroupProtocol::Consumer =>
                Ok(Box::new(AsyncKafkaConsumer::<K, V>::new(config)?)),
            GroupProtocol::Classic => Err(KafkaError::unsupported_version(
                "Classic group protocol is not yet supported in this client; \
                 set group.protocol=consumer (KIP-848).",
            )),
        }
    }

Implementations in Milestone 8:

  - `AsyncKafkaConsumer<K, V>` — KIP-848 consumer.
  - `MockConsumer<K, V>` — user-facing test helper, mirrors Java's
    `MockConsumer`. Exposes mock-specific configuration methods
    (`add_record`, `set_pollable`, etc.) as inherent methods on the concrete
    type, not on the `Consumer` trait. In tests, hold a `MockConsumer`
    directly and pass `&mut consumer` where `&mut dyn Consumer<K, V>` is
    expected.

A `ClassicKafkaConsumer<K, V>` impl may be added later without breaking
changes; `new_consumer` just gains a new arm.

**Why `#[async_trait]` despite CLAUDE.md §11:**

CLAUDE.md §11 cautions against `Pin<Box<dyn Future>>` per call on **hot
paths**. The `Consumer` dispatch surface is *not* a hot path:

  - `poll()` is called at batch granularity (≤ ~100/sec realistically); the
    ~50 ns `Box::new` cost is <0.0001% of the call body.
  - `commit_*`, `subscribe`, `close`, `position`, `committed` are even less
    frequent.
  - The true hot path (per-record deserializer / interceptor / record build)
    runs *inside* `poll()` and does not go through the `Consumer` trait. See
    sections 27–28 for the per-record trait choices, which do NOT use
    `#[async_trait]`.

Benefits over an enum-dispatch alternative:

  - Library code: ~30 trait methods × 1 line each, vs. 3-line `match` per
    method on an enum (~90 lines of dispatch boilerplate avoided per new
    method or variant).
  - Test code: `&mut MockConsumer` upcasts directly to
    `&mut dyn Consumer<K, V>`; no wrap-then-unwrap pattern is needed.
  - Open extension surface: users can implement custom wrapping consumers
    (instrumentation, retry-on-poll, etc.). Java does not offer this either,
    but Rust users will expect it.

**How to apply:**

  - Define `Consumer<K, V>` as one `#[async_trait]` trait in
    `src/consumer/mod.rs`. Public method surface mirrors
    `Consumer.java` (Apache Kafka 4.2).
  - Expose `Box<dyn Consumer<K, V>>` from the `new_consumer` factory.
  - `AsyncKafkaConsumer` and `MockConsumer` both `impl Consumer<K, V> for ...`.
  - Tests construct `MockConsumer` directly and pass it as `&mut dyn
    Consumer<K, V>` to code-under-test. No enum match, no `as_mock_mut`
    helper.
  - Use default `#[async_trait]` (with `Send` bound). Do NOT use
    `#[async_trait(?Send)]` — non-`Send` futures cannot cross task
    boundaries, which is required for the tokio multi-thread runtime.

**Do NOT use `#[async_trait]` for:**

  - `Deserializer<T>` / `Serializer<T>` — per-record, sync trait
    (`fn deserialize(bytes: &[u8]) -> Result<T, KafkaError>`).
  - `ConsumerInterceptor<K, V>` — per-batch but with per-record processing
    inside; keep generic dispatch (`<I: ConsumerInterceptor<K, V>>`) where
    feasible.
  - Anything called per iteration inside the background-task event loop or
    on the `Fetcher` / `FetchCollector` per-record path.

For those, prefer generic dispatch or a sync trait. See sections 27–28 for
specific guidance.

## 10. Background task model: single `tokio::spawn`, not per-RequestManager

Translate Java's `ConsumerNetworkThread` as a single `tokio::spawn` per
consumer instance. Do NOT split request managers (Coordinator, Heartbeat,
Commit, Fetch) into separate tasks.

Mirror `ConsumerNetworkThread.runOnce()` phase-for-phase: drain application
events → poll each request manager in registration order → poll network
client → reap expired events. The Java source is the contract for phase
ordering and behavior.

**Why:** All request managers share `SubscriptionState`, `ConsumerMetadata`,
and the membership state machine. Java enforces serialization via single
thread; Rust enforces via single task. Splitting tasks turns shared state
into `Arc<Mutex<...>>` and lets the KIP-848 membership state machine
interleave with heartbeats / fetches — much harder to reason about, and
not behavior-faithful to Java. No throughput gain (all managers are
I/O-bound on the same `NetworkClient`).

**How to apply:**

  - One `tokio::spawn` per consumer instance, owning `NetworkClient`,
    `RequestManagers`, and the wakeup / shutdown tokens.
  - `RequestManagers.entries()` returns request managers in deterministic
    registration order — do NOT iterate over a `HashMap`.
  - Drain application events with `try_recv` in a `while let` loop, NOT
    `recv().await`, mirroring Java's `drainTo`. Drain unbounded (Java does).
  - The only `await` boundary that must be cancel-safe against the wakeup
    token is `network_client.poll(...)`. Wrap it in
    `tokio::select! { biased; shutdown; wakeup; network_poll }`.
  - Do NOT `tokio::spawn` inside the bg task for per-request or per-event
    work (CLAUDE.md §11).

## 11. `wakeup()` semantics: rotating `CancellationToken`

Translate Java's `KafkaConsumer.wakeup()` as a `tokio_util::sync::CancellationToken`
shared between the app side and the background task via
`tokio::sync::watch::channel<CancellationToken>`.

  - `wakeup()` is sync. It loads the current token from the watch channel
    and calls `token.cancel()`. Callable from any thread / task.
  - Every async public method (`poll`, `commit_sync`, `position`,
    `committed`, `close`, etc.) `select!`s `token.cancelled()` as a branch
    and returns `KafkaError::Wakeup` when it wins.
  - On returning `Wakeup`, the consumer **rotates** the token: sends a
    fresh `CancellationToken::new()` on the watch channel. The bg task
    observes the new token on its next loop iteration and uses it in its
    own `select!`. This mirrors Java clearing the volatile `wakeup` flag
    after throwing `WakeupException` once.

**Why a rotating token, not an `AtomicBool`:**

An `AtomicBool` flag does not unblock `await` points — a waiting `poll()`
would only check the flag when it wakes for some other reason. The
async-native primitive that both signals AND unblocks `await`s is
`CancellationToken`, equivalent to Java's `Selector.wakeup()` +
volatile-flag combo.

**How to apply:**

  - One `tokio::sync::watch::channel<CancellationToken>` per consumer
    instance. The sender lives on the app side; the receiver is cloned
    into the bg task at spawn time.
  - The bg task re-reads `wakeup_rx.borrow().clone()` at the top of each
    `run_once` iteration before constructing its `select!`.
  - Rotate the token by sending `CancellationToken::new()` on the watch
    channel immediately before returning `KafkaError::Wakeup` from a
    public method. Do NOT try to plug the (Java-equivalent) race where a
    concurrent `wakeup()` call between cancellation and rotation can be
    lost — Java has the same race and the behavior is intentional.
  - `wakeup()` itself stays sync (callable from non-async code, including
    signal handlers).
  - Do NOT use `AtomicBool` as the wakeup primitive.

## 16. `SubscriptionState` ownership: `Arc<Mutex<SubscriptionState>>`

`SubscriptionState` is shared mutable state read and written by both the
app side (in `subscribe`, `assign`, `seek`, `assignment`, `paused`, etc.)
and the background task (in fetchers, membership manager, commit manager).
Wrap it as `Arc<std::sync::Mutex<SubscriptionState>>`. Field shape mirrors
Java's `SubscriptionState` 1:1 — the Java source is the contract.

**Why std `Mutex` and not `tokio::Mutex`:**

Critical sections are short, CPU-bound, never awaiting (per CLAUDE.md §9.6).
`std::sync::Mutex` is faster (no async overhead) and `poisoned()` surfaces
panics, which is correct here. Do NOT use `parking_lot::Mutex` —
non-poisoning semantics silently leaves state inconsistent after a panic.

**Why `Arc<Mutex<...>>` and not `ArcSwap` snapshot:**

Writes are as frequent as reads (every fetch response updates offsets);
rebuild-on-write of `ArcSwap` does not amortize. `Arc<Mutex<...>>` mirrors
Java's `synchronized` blocks, simplifying behavior-parity review.

**How to apply:**

  - One `Arc<Mutex<SubscriptionState>>` per consumer instance, cloned into
    the bg task at spawn time.
  - Lock acquire → mutate / read → drop guard. NEVER hold the guard across
    an `.await` (CLAUDE.md §9.6).
  - In particular: drop the guard before invoking a
    `ConsumerRebalanceListener` callback, before sending on any mpsc
    channel that could block, and before any `network_client` call.
  - `std::sync::Mutex` is NOT reentrant. Public sync methods (`assignment`,
    `subscription`, `paused`) that lock `SubscriptionState` MUST NOT be
    called from inside a `ConsumerRebalanceListener` callback while the
    `poll()` path holds the lock. The listener invocation in
    `process_background_events` (section 31) drops the lock first; preserve
    this invariant.

**Anti-patterns to flag in review:**

  - `tokio::sync::Mutex<SubscriptionState>`.
  - `parking_lot::Mutex<SubscriptionState>` (non-poisoning).
  - `RwLock<SubscriptionState>` (writes are frequent — `RwLock` loses to
    `Mutex` here).
  - Holding the guard across `app_tx.send(...).await` or any
    `network_client` call.
  - Snapshot caches (`Arc<SubscriptionStateSnapshot>`) introduced "for
    sync reads" — sync reads through the mutex are fast enough; a snapshot
    layer adds drift and a second source of truth.

## 20. Group protocol scope: KIP-848 only

Milestone 8 translates only the new (KIP-848) consumer group protocol path.
The classic protocol is deferred to a later milestone for backwards
compatibility but is NOT in scope now.

**In scope (translate fully):**

  - `AsyncKafkaConsumer` and its dependency closure: `ConsumerNetworkThread`,
    `ConsumerMembershipManager`, `AbstractMembershipManager`, `MemberState`,
    `MemberStateListener`, `ConsumerHeartbeatRequestManager`,
    `AbstractHeartbeatRequestManager`, `HeartbeatRequestState`, `Heartbeat`,
    `CoordinatorRequestManager`, `CommitRequestManager`,
    `FetchRequestManager`, `FetchBuffer`, `FetchCollector`, `Fetcher`,
    `CompletedFetch`, `FetchConfig`, `OffsetFetcher`, `OffsetsRequestManager`,
    `NetworkClientDelegate`, `ApplicationEventHandler`,
    `BackgroundEventHandler`, `ApplicationEventProcessor`,
    `CompletableEventReaper`, all events under `internals/events/`,
    `ConsumerRebalanceListenerInvoker`, `ConsumerRebalanceListenerMethodName`,
    `SubscriptionState`, `ConsumerMetadata`, `AutoOffsetResetStrategy`,
    `Deserializers`, `ConsumerInterceptors`.
  - Public API types: `Consumer` trait, `KafkaConsumer`, `MockConsumer`,
    `ConsumerConfig`, `ConsumerRecord`, `ConsumerRecords`,
    `ConsumerGroupMetadata`, `ConsumerRebalanceListener`,
    `OffsetCommitCallback`, `OffsetAndMetadata`, `OffsetAndTimestamp`,
    `OffsetResetStrategy`, `GroupProtocol`, `CloseOptions`,
    `SubscriptionPattern`, the consumer exception hierarchy.

**Out of scope (do NOT translate):**

  - `ClassicKafkaConsumer`, `ConsumerCoordinator`, `AbstractCoordinator`,
    `ConsumerNetworkClient`, `BaseHeartbeatThread`.
  - All client-side assignors: `AbstractPartitionAssignor`, `RangeAssignor`,
    `RoundRobinAssignor`, `StickyAssignor`, `AbstractStickyAssignor`,
    `CooperativeStickyAssignor`, `ConsumerProtocol`,
    `ConsumerPartitionAssignor` trait. KIP-848 does server-side assignment.
  - `ConsumerDelegate`, `ConsumerDelegateCreator` (only needed for >1
    delegate; collapses to direct `Box::new(AsyncKafkaConsumer)` per
    section 2).
  - All share-consumer files (KIP-932): `KafkaShareConsumer`,
    `MockShareConsumer`, `ShareConsumer`, `ShareConsumerConfig`,
    `ShareConsumerImpl`, `Acknowledgements`, `AcknowledgeType`,
    `AcknowledgementCommitCallback`, `AcknowledgementCommitCallbackHandler`,
    `ShareCompletedFetch`, `ShareConsumeRequestManager`,
    `ShareAcknowledgementMode`, `ShareAcquireMode`, related tests.

**Tests out of scope** (do NOT translate, do not block DoD on these):

  - `ConsumerCoordinatorTest`, `AbstractCoordinatorTest`,
    `EagerConsumerCoordinatorTest`, `CooperativeConsumerCoordinatorTest`.
  - `RangeAssignorTest`, `RoundRobinAssignorTest`, `StickyAssignorTest`,
    `AbstractStickyAssignorTest`, `CooperativeStickyAssignorTest`,
    `ConsumerPartitionAssignorTest`, `ConsumerProtocolTest`.
  - All `Share*Test` files.

**Public API behavior for out-of-scope features:**

Match Java behavior. Specifically:

  - `ConsumerConfig::group_protocol` defaults to whatever the Java default
    is in 4.2 (currently `"classic"` per Java; we override to `"consumer"`
    only if Java's `AsyncKafkaConsumer` constructor rejects other values —
    otherwise match Java's default and let the user opt in to KIP-848
    explicitly). Resolve by reading the Java source during planning.
  - `partition.assignment.strategy` and other classic-protocol-only config
    keys: accept silently as Java does. Do NOT add Rust-side rejection.
  - `enforce_rebalance(reason)`: match Java's `AsyncKafkaConsumer`
    behavior (Java javadoc says it is classic-only; the AsyncKafkaConsumer
    implementation is the source of truth for the exact exception type).

**Future compatibility:**

When classic-protocol support is added later, all of the above out-of-scope
files become a new module/phase. The current public API does not change;
`new_consumer` gains a new arm in its match.

## 27. Receive-path zero-copy contract

CLAUDE.md §12 forbids copying key/value/header bytes through the producer
send path. The symmetric rule on the receive path:

  - `FetchResponse` bytes arriving from the network are owned by exactly
    one buffer (typically `Bytes` or `Vec<u8>`) inside `CompletedFetch`.
  - Everything downstream of `CompletedFetch` — record iteration, header
    parsing, deserializer input — borrows slices from that buffer.
  - The only allocation per record is the user-supplied `Deserializer`'s
    decoded output (`T` for key, `T` for value). That allocation is
    unavoidable; it is the user's choice of type.

**`Deserializer<T>` trait shape:**

    pub trait Deserializer<T>: Send + Sync + 'static {
        fn deserialize(&self, topic: &str, data: &[u8]) -> Result<T, KafkaError>;
        fn deserialize_with_headers(
            &self,
            topic: &str,
            headers: &Headers,
            data: &[u8],
        ) -> Result<T, KafkaError> {
            self.deserialize(topic, data)
        }
    }

Sync `fn`. No `#[async_trait]`. `data: &[u8]` borrowed from the fetch
buffer.

**Compression handling:**

Compressed batches (gzip / snappy / zstd / lz4) are decompressed into a
fresh `Vec<u8>` owned by the `CompletedFetch` for that batch. Per-record
borrowing proceeds from the decompressed buffer. Do NOT re-decompress
per record; do NOT clone the decompressed buffer per record.

**Topic name handling:**

`ConsumerRecord::topic` is `Arc<str>` (Phase 4a precedent), not `String`.
A single `Arc<str>` instance per topic-partition is held in
`SubscriptionState` and cloned cheaply for each emitted `ConsumerRecord`.
Do NOT allocate a fresh `String` per record.

**Headers handling — owned for Milestone 8:**

`ConsumerRecord` holds owned headers (`Headers` = `Vec<RecordHeader>` with
owned `Arc<str>` key and `Vec<u8>` value), matching Java's allocation
behavior. Do NOT add a lifetime parameter to `ConsumerRecord<K, V>` for
borrowed headers in this milestone — it would propagate through
`ConsumerRecords<K, V>` and every user-facing iterator type. Most users do
not read headers; allocating eagerly is acceptable. Revisit (option:
borrowed `Headers<'a>` or `Arc<Vec<RecordHeader>>` shared across records
in a batch) only if profiling shows header allocation is hot.

**Anti-patterns to flag in review:**

  - `Deserializer::deserialize(&self, topic: &str, data: Vec<u8>)` —
    forces a per-record copy.
  - `#[async_trait]` on `Deserializer`.
  - `Bytes::copy_from_slice(...)` where `Bytes::slice(...)` would do.
  - `CompletedFetch { records: Vec<ConsumerRecord<K, V>> }` — pre-decoded
    records in a vec require a second pass that copies all bytes. Hold the
    buffer + a parsing cursor instead, decode lazily.
  - `String::from_utf8(topic_bytes.clone())` per record — clone the
    `Arc<str>` from `SubscriptionState` instead.
  - Per-record `tokio::spawn` on the fetch path (CLAUDE.md §11).

**Tests required:**

Per-record allocation-budget test on the receive path, following the
existing producer hot-path allocation test precedent (Phase 6). Assert
the per-record allocation count matches the user-deserializer budget;
specifically, no allocations attributable to topic name, key/value bytes
on the buffer, or batch traversal.

## 31. `ConsumerRebalanceListener` & `OffsetCommitCallback` invocation thread

Listener callbacks (`ConsumerRebalanceListener::on_partitions_revoked`,
`on_partitions_assigned`, `on_partitions_lost`) and
`OffsetCommitCallback::on_complete` execute on the **caller's task** during
`poll()`, `commit_*()`, `unsubscribe()`, `close()`, and any timed query API
that drains the background event queue. They NEVER execute on the
background task or any other spawned helper.

The mechanism mirrors Java's bidirectional event handshake
(`AsyncKafkaConsumer.java` `processBackgroundEvents` /
`invokeRebalanceCallbacks` — the Java source is the contract):

  1. The background task encounters a state change requiring a listener
     callback. It enqueues a `RebalanceListenerCallbackNeeded` event on
     the background-events channel, carrying a
     `tokio::sync::oneshot::Sender<Result<(), KafkaError>>`.
  2. The bg task **awaits** the matching `oneshot::Receiver`. Rebalance
     state does not advance until the callback completes.
  3. The app side, inside its `poll()` / `commit_*()` / etc. loop, drains
     the background-events channel via `try_recv` in a `while let` loop
     and, for each `RebalanceListenerCallbackNeeded`, invokes the
     user-supplied listener method inline on its own task.
  4. The app side sends the listener's result on the `oneshot::Sender`;
     the bg task receives it and continues the rebalance.

**Why this matters:**

  - User code commonly calls `consumer.commit_sync()` from inside
    `on_partitions_revoked` to flush offsets before partitions are taken
    away. This only works because the listener runs on the same task that
    owns the consumer API.
  - Java guarantees rebalance does not complete before the listener
    callback returns. Translating to fire-and-forget silently changes the
    contract: partitions get reassigned before the user has flushed
    state, leading to duplicate processing or lost commits. Phase 6e
    flagged this exact pattern under "callback obligation."

**How to apply:**

  - `process_background_events` is called at the TOP of every public
    blocking-style API (`poll`, `commit_sync`, `commit_async`,
    `unsubscribe`, `close`, `position`, `committed`, `beginning_offsets`,
    `end_offsets`, `offsets_for_times`).
  - Invoke listener methods inline on the caller's task; do NOT
    `tokio::spawn` them.
  - Drop the `SubscriptionState` lock guard before invoking the listener
    (section 16) — listeners may call back into `consumer.assignment()`.
  - The bg task awaits the `oneshot::Receiver` for the callback result
    before continuing the rebalance state transition.
  - `OffsetCommitCallback` follows the same pattern: bg-side completion
    enqueues a callback-needed event; app-side drains and invokes.

**`ConsumerRebalanceListener` trait shape:**

    #[async_trait]
    pub trait ConsumerRebalanceListener: Send + Sync + 'static {
        async fn on_partitions_revoked(
            &self,
            partitions: &[TopicPartition],
        ) -> Result<(), KafkaError>;
        async fn on_partitions_assigned(
            &self,
            partitions: &[TopicPartition],
        ) -> Result<(), KafkaError>;
        async fn on_partitions_lost(
            &self,
            partitions: &[TopicPartition],
        ) -> Result<(), KafkaError> {
            // Matches Java's default behavior on the listener interface.
            self.on_partitions_revoked(partitions).await
        }
    }

`#[async_trait]` is correct here — listener invocation is per-rebalance,
not per-record; the cost of one `Box<Future>` per callback is irrelevant.

**Anti-patterns to flag in review:**

  - `tokio::spawn(listener.on_partitions_revoked(...))` anywhere.
  - Calling listener methods from inside the bg task's `run_once`.
  - Bg task continuing the rebalance state machine without awaiting the
    `oneshot::Receiver` for callback completion (fire-and-forget).
  - A public blocking-style API that does not call
    `process_background_events` before its main wait.
  - A separate task spawned to "drain the background events channel" —
    callbacks would run on that task, not the user's.
  - Holding `SubscriptionState`'s `MutexGuard` across the listener call.

**Tests required:**

Two regression tests must exist; the rule is not considered tested without
them:

  1. `on_partitions_revoked` calls `consumer.commit_sync()` from inside
     the callback and the commit succeeds (mirrors Java's canonical test).
     If the listener runs on the bg task, this deadlocks. If the bg task
     fire-and-forgets the callback, the commit races reassignment and
     the test is flaky.
  2. The rebalance does not advance until the listener future resolves —
     block the listener on a channel held by the test, observe that the
     membership state machine has not advanced, release the channel,
     observe advancement.
