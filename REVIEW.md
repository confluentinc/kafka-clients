# Consumer Translation Rules — Review Package

**Branch:** `consumer-impl` (worktree at `…/example-confluent-kafka-rust-consumer/`)
**Base:** `master` @ `442ecac`
**Commits under review (2):**
  - `c7253af` — Add consumer translation rules for KIP-848 Milestone 8
  - `9186733` — Backport translation-rule additions from `fresh-impl` branch (PR #70)

This package gathers all rule-file additions intended to govern the upcoming Rust translation of the Kafka consumer (Milestone 8). **No Rust source code is being changed.** Reviewers are asked to evaluate whether these rules will produce a correct, performant, behavior-faithful translation of `org.apache.kafka.clients.consumer.*` from Apache Kafka 4.2.

---

## 1. TL;DR

- **One new file**, `.claude/rules/consumer-threading.md` (~547 lines, 8 numbered sections covering the highest-risk design calls for the consumer translation).
- **Two existing files edited** with small, targeted additions on top of the rules already accepted on `fresh-impl` (PR #70):
  - `CLAUDE.md` — new naming-convention bullets (i64/nullable/flexibleVersions), §5 completeness clause, §9.4/9.5/9.6 concurrency rules (from `fresh-impl`); tightened §11 "hot path" definition, extended §12 receive-path zero-copy, added §13 pointer to consumer-threading.md (from this branch).
  - `.claude/rules/definition-of-done.md` — §3 test-translation sub-bullets, §10 hot-path allocation audit (from `fresh-impl`); §11 consumer trait surface check (from this branch).
- **Scope decision: KIP-848 only.** Classic group protocol and share consumer (KIP-932) are deferred to later milestones.

Appendix B at the bottom lists the `fresh-impl` (PR #70) additions specifically, for reviewers who want to evaluate them separately from the new consumer-specific work.

---

## 2. The 8 design decisions under review

These are the calls the rules codify. If any is wrong, large portions of the translation have to be redone.

| § | Decision | One-line rationale |
|---|---|---|
| 1 | Consumer API is **async-only**; no `block_on`-wrapped sync façade in Milestone 8 | Underlying network stack is async; sync façade deadlocks on current-thread runtimes and forces every user trait to be sync |
| 2 | `Consumer<K,V>` is `#[async_trait]` + `Box<dyn Consumer>`, **not** enum dispatch | Per-call `Box<Future>` cost is irrelevant at batch granularity (~50ns of ~ms); enum approach forces ~30-method match boilerplate and worse test ergonomics |
| 10 | **Single** `tokio::spawn` for the background task; not per-RequestManager | Mirrors Java's single `ConsumerNetworkThread`; preserves serialized state and `runOnce()` phase ordering; managers are I/O-bound on one `NetworkClient` so no throughput gain from splitting |
| 11 | `wakeup()` = rotating `CancellationToken` via `watch::channel` | `AtomicBool` doesn't unblock `.await` points; rotating token mirrors Java's volatile-flag-cleared-after-throw semantics |
| 16 | `SubscriptionState` is `Arc<std::sync::Mutex<SubscriptionState>>` | Mirrors Java's `synchronized` 1:1; std `Mutex` (not tokio, not parking_lot) — short CPU-only critical sections, no `.await` while held |
| 20 | **KIP-848 only**; classic protocol + share consumer deferred | ~30% LOC reduction; new API public surface is unchanged when classic is added later |
| 27 | Receive-path zero-copy: `Deserializer<T>` is **sync**, takes `&[u8]` borrowed from `CompletedFetch`'s owning buffer | Symmetric with existing send-path zero-copy rule; only user-deserializer allocates per record |
| 31 | `ConsumerRebalanceListener` & `OffsetCommitCallback` execute on the **caller's task**; bg task `await`s a `oneshot` for completion | Java's bidirectional event handshake; user calls `commit_sync()` from inside `on_partitions_revoked` and must not deadlock |

---

## 3. Full content — `.claude/rules/consumer-threading.md` (new file, 547 lines)

```markdown
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
    state, leading to duplicate processing or lost commits.

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
```

---

## 4. Diff against master — `CLAUDE.md`

```diff
@@ -59,11 +59,20 @@
     2. Return a `Result` when Java code throws an exception even if unchecked but recoverable.
     3. Use a `KafkaError` similar to the librdkafka one with functions `is_retriable` or `is_fatal` or `txn_requires_abort()` and 
        an error code that corresponds to the Java Kafka exceptions.
-11. **Language-related optimizations**: When the memory can be kept on the stack even if Java code creates a new object, keep it on the stack.
+11. **Language-related optimizations**: When the memory can be kept on the stack even if Java code creates a new object, keep it on the stack. On hot paths (send path, batch drain, wire framing, per-record processing), also account for costs Java's JIT/GC masks but Rust makes explicit:
+    - Identifiers cloned on every message (topic names, client IDs): prefer `Arc<str>` over `String` to make clones cheap
+    - A single numeric field shared across tasks: prefer `AtomicI64`/`AtomicU64` over `Mutex<i64>` to avoid lock contention
+    - Hot-path async dispatch: avoid `Pin<Box<dyn Future>>` per call — prefer concrete `async fn` return types or generic dispatch
+    - Per-message `tokio::spawn` on the send path: avoid — use a shared completion task with a channel instead
+
+    **"Hot path" definition**: per-record / per-message dispatch (send-path record build, batch drain, deserialize/serialize, wire framing). This does **not** include per-RPC or per-batch top-level API surfaces (e.g. the `Producer` / `Consumer` dispatch trait used at `send()` / `poll()` granularity) — there, one `Pin<Box<dyn Future>>` per call is amortized over many records and is negligible. `#[async_trait]` is acceptable for those top-level surfaces.
+
+    Outside hot paths, prefer the simpler type (`String`, `Mutex`) unless profiling shows otherwise.
 12. **Parameters and return values of public API**: Accept the most general borrowed form for input parameters. Borrow immutably, and return immutable values.
     Return a borrowed reference in case the data is still owned by the original struct (getter for example).
     When ownership is transferred to the caller prefer returning the struct (making use of RVO) over Box or Rc or Arc.
-    Don't copy byte arrays holding the key, value or headers passed to ProduceRecord or received in ConsumeRecord.
+    Don't copy byte arrays holding the key, value or headers passed to ProduceRecord or received in ConsumeRecord. This zero-copy requirement extends through the entire write path: serialized bytes must be written directly into the batch buffer (no intermediate buffer), batch finalization must not copy already-serialized bytes, and wire sends must use vectored I/O (`IoSlice` / `write_vectored`) so the framing header and payload are sent without assembling a single contiguous buffer. On the receive path, the symmetric rule applies: fetched bytes are owned by one buffer in `CompletedFetch`, every downstream type borrows slices from it, and the `Deserializer<T>` trait takes `&[u8]` (sync, no `#[async_trait]`) — see `consumer-threading.md` §27.
+13. **Consumer-specific rules**: see [consumer-threading.md](.claude/rules/consumer-threading.md) for `AsyncKafkaConsumer` API shape, background-task design, `wakeup()` cancellation, `SubscriptionState` ownership, group-protocol scope, receive-path zero-copy, and `ConsumerRebalanceListener` invocation thread. These rules supplement #8/#9/#11/#12 inside the consumer module.
```

**What the §11 edit does:**

The pre-existing §11 ("avoid `Pin<Box<dyn Future>>` per call") was easy to misread as banning `#[async_trait]` everywhere. The new wording adds:
- A bulleted list of hot-path optimizations to make the rule concrete.
- An explicit **"Hot path" definition** paragraph clarifying that `#[async_trait]` is OK on `Producer` / `Consumer` top-level dispatch surfaces — those are per-RPC / per-batch, not per-record.

**What the §12 edit does:**

Extends the existing send-path zero-copy sentence with:
- The send-path detail that was already discussed in producer phases (vectored I/O / IoSlice / write_vectored).
- The symmetric receive-path rule for the consumer (linking to consumer-threading.md §27).

**What the new §13 does:**

Adds a one-line pointer to consumer-threading.md.

---

## 5. Diff against master — `.claude/rules/definition-of-done.md`

```diff
@@ -19,4 +19,10 @@
 8. Are there any TODO or FIXME left in the code? In case finish everything that should be done before considering the change done.
 
 9. Are unit tests, integration tests, Python, C tests passing?
-   Use `make verify` to run all tests and format checks and lint checks. If there are any failing test or check fix them before considering the change done.
+   Use `make verify` to run all tests and format checks and lint checks. If there are any failing test or check fix them before considering the change done.
+
+10. **Consumer trait surface check** (when translating consumer files):
+    - The top-level `Consumer<K, V>` dispatch is a single `#[async_trait]` trait with `Box<dyn Consumer<K, V>>` from the factory. No enum dispatch wrapping `AsyncKafkaConsumer` / `MockConsumer`.
+    - Per-record traits (`Deserializer`, `Serializer`, anything on the `Fetcher` / `FetchCollector` path) do NOT use `#[async_trait]` — sync `fn` or generic dispatch only.
+    - No `block_on`-wrapped sync façade for any async consumer method (see `consumer-threading.md` §1).
+    - `#[async_trait]` is used with default `Send` bound (no `?Send`).
```

---

## 6. Open questions for reviewers

Things to specifically push back on if you disagree:

1. **§2 — `#[async_trait]` vs enum dispatch.** We're knowingly paying one `Box<Future>` per call (~50 ns) at batch granularity in exchange for less boilerplate and cleaner test ergonomics. If you'd prefer zero-allocation enum dispatch, this is the place to say so — flip cost is moderate (~90 lines of dispatch code).
2. **§11 — Wakeup token via `watch::channel`.** Alternative is `Arc<Mutex<CancellationToken>>` with both sides reading on each iteration. `watch` is more idiomatic for "broadcast latest value to subscribers" but adds a small dependency.
3. **§20 — KIP-848 only.** Eliminates ~30% of consumer LOC and all client-side assignors. If you want classic-protocol support in M8, scope must expand.
4. **§27 — Owned `Headers` in `ConsumerRecord` for M8.** Borrowing headers from the fetch buffer is the zero-copy answer but adds a lifetime parameter to `ConsumerRecord<'a, K, V>` that propagates through every user iterator. Most users don't read headers; allocating eagerly is acceptable. Revisit if profiling proves otherwise.
5. **§31 — Bidirectional `oneshot` handshake.** Java's behavior is that rebalance does not advance until the listener returns. We're preserving that. If you want fire-and-forget for performance, partitions can be reassigned before user state is flushed.

---

## Appendix A — Files in the commits

**Commit `c7253af`** (consumer-specific rules):
```
.claude/rules/consumer-threading.md     | 547 +++++++++++++++++++++++++++++ (new)
.claude/rules/definition-of-done.md     |   8 +-
CLAUDE.md                               |  13 +-
3 files changed, 565 insertions(+), 3 deletions(-)
```

**Commit `9186733`** (backport from `fresh-impl` / PR #70):
```
.claude/rules/definition-of-done.md     |  10 ++++++++--
CLAUDE.md                               |  10 +++++++++-
2 files changed, 17 insertions(+), 3 deletions(-)
```

---

## Appendix B — Detail of the `fresh-impl` (PR #70) backport

The producer-translation rules from `fresh-impl` are now on `consumer-impl` as commit `9186733`. They are itemised below for reviewers who want to evaluate them separately from the new consumer-specific work.

These rules are referenced by section number from `consumer-threading.md` (e.g. CLAUDE.md §9.6 "MutexGuard across `.await`"), so without them the section references would not resolve.

### B.1 — `fresh-impl` `CLAUDE.md` additions

```diff
@@ -29,6 +29,9 @@
    - Java `Exception` → Rust `Error` (e.g. `TopicAuthorizationException` → `TopicAuthorizationError`)
    - Java `throws` / `throw` → Rust `return Err(...)` (e.g. `maybeThrowAnyException` → `maybe_return_any_error`)
    - Preserve original architecture and logical structure
+   - Java `long` fields used in comparison (e.g. `Uuid`, producer IDs, offsets) must use `i64` in Rust, not `u64` — signed vs unsigned comparison produces different ordering for values with the high bit set
+   - Nullable `string`/`bytes` fields in the Kafka message specs without an explicit `"default": "null"` must default to empty (`Some(String::new())` / `Some(Vec::new())`), not `None`. Only use `None` when the spec explicitly sets `"default": "null"`
+   - When generating wire protocol code, always use per-field `flexibleVersions` overrides via `field_flexible_versions(field, msg_flex)` in the generator — never the raw message-level value. Some fields (e.g. `ClientId` in `RequestHeader`) override to `"none"` and must always use length-prefixed encoding
@@ -43,7 +46,7 @@
 3. **Tests**: Keep the same tests, after translating a class, also translate and run all its corresponding tests.
 4. **Comments and documentation**: Keep similar comments as the Java source,
 translate javadoc to rustdoc. Never change the contract of public API.
-5. **Completeness**: Don't leave any TODO or FIXME — finish everything that should be done
+5. **Completeness**: Don't leave any TODO or FIXME — finish everything that should be done. If a Java code path is not yet implemented, fail the affected records/operations with an appropriate `KafkaError` — silently completing or hanging futures is worse than an explicit error.
@@ -53,17 +56,28 @@
     2. Translate callbacks you find in Java client to code that is executed 
        after awaiting the corresponding call in Rust.
     3. In case the original method isn't blocking to await the callback response (for example awaiting a CompletableFuture), use Tokio `task::spawn` to create a coroutine that is detached from current flow.
+    4. When Java uses `thread.join()` or `Future.get()` to block until completion, the Rust translation must actually `.await` the corresponding handle — setting a flag or dropping a channel is not equivalent to joining.
+    5. Translating a Java callback to async does not eliminate the callback obligation. If Java guarantees exactly-once callback invocation per record at a specific lifecycle point (e.g. `completeFutureAndFireCallbacks`), the Rust translation must invoke the equivalent at the same point — not defer it or silently drop it.
+    6. **Tokio-specific pitfalls** (no Java equivalent — Java threads do not have cancellation semantics):
+       - `tokio::select!` cancels the losing branch's future mid-execution. Never put operations with side effects (incrementing a counter, sending on a channel, writing to a buffer) inside a `select!` arm unless the future is cancellation-safe. Use `biased;` when ordering matters.
+       - Holding a `MutexGuard` across an `.await` point deadlocks the async runtime — always drop locks before awaiting.
@@ -53 (also §11 and §12, but those overlap with consumer-impl edits — final merged form should be cross-checked)
```

(Note: `fresh-impl` also edited §11 and §12 with content largely subsumed by the `consumer-impl` edits — the final merged form should be cross-checked. The non-overlapping parts are: new naming-convention bullets at the top of §2, new §5 sentence, and the new §9.4 / §9.5 / §9.6 concurrency rules above.)

### B.2 — `fresh-impl` `definition-of-done.md` additions

```diff
@@ -6,7 +6,11 @@
 
 2. Are all methods from the translated classes implemented?
 
-3. Are all test using those classes translated? Never skip a test that is present in the Java codebase except if there are tests that are present in the Java codebase but not translated and they are not relevant to the Rust codebase, explain why they are not relevant and why they can be skipped.
+3. Are all test using those classes translated? Never skip a test that is present in the Java codebase except if there are tests that are present in the Java codebase but not translated and they are not relevant to the Rust codebase, explain why they are not relevant and why they can be skipped. When translating tests also verify:
+   - Dedicated per-message-type test files beyond the main `*Test.java` (e.g. `SimpleExampleMessageTest`, `NullableStructMessageTest`) are not missed
+   - `@RepeatedTest(N)` annotations become loops in Rust, not single invocations
+   - Error message content is asserted, not just `is_err()` — error messages are part of the behavioral contract
+   - Wire protocol types have byte-level encoding tests against known vectors, not just round-trip tests — a consistently wrong encoding passes round-trips but is wire-incompatible with Java
@@ -19,4 +23,6 @@
 8. Are there any TODO or FIXME left in the code? In case finish everything that should be done before considering the change done.
 
 9. Are unit tests, integration tests, Python, C tests passing?
-   Use `make verify` to run all tests and format checks and lint checks. If there are any failing test or check fix them before considering the change done.
+   Use `make verify` to run all tests and format checks and lint checks. If there are any failing test or check fix them before considering the change done.
+
+10. **Hot-path allocation audit**: For any class that sits on the producer send path, verify there are no avoidable per-message heap allocations: no intermediate copy buffers, no identifier `String` clones, no `Box<dyn Future>` per send. If the translated class is not on the send path, this check can be skipped.
```

**§10 numbering note:** Both commits add to DoD. Final ordering on the branch:
- §10 — "Hot-path allocation audit" (from `9186733`, fresh-impl backport)
- §11 — "Consumer trait surface check" (from `c7253af`)

---

## Appendix C — How to access the actual branch

If reviewers have repo access:

```bash
git fetch origin
git checkout consumer-impl   # branches off master, 2 commits ahead
# or to see the diff
git diff master..consumer-impl
# or to review commits separately
git log master..consumer-impl
```

If reviewers do not have repo access — this document contains the full text of all changes. The `consumer-threading.md` section above is verbatim.
