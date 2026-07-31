# Producer transaction translation rules

This file consolidates design decisions specific to translating the Kafka
producer's idempotence and transaction support
(`org.apache.kafka.clients.producer.internals.TransactionManager` and its
dependency closure) from Java to Rust. It supplements `CLAUDE.md` — when these
rules conflict with general translation guidance, the transaction-specific rule
wins inside `src/producer/`.

Rules are grouped by topic. Each numbered section is a single design decision:
the rule itself, **Why** (rationale, usually referencing the Java contract), and
**How to apply** (concrete guidance for Actor / Critic).

Introduced in Milestone 11 Phase 1. The Java source
(`kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/`,
Apache Kafka 4.2) is the contract for every rule below.

## 1. `Caller { App, Sender }` replaces `Thread.currentThread()`

Java decides whether an invalid state transition *throws* or *poisons* the
state machine by inspecting the current thread
(`TransactionManager.java:287-289`):

    protected boolean shouldPoisonStateOnInvalidTransition() {
        return Thread.currentThread() instanceof Sender.SenderThread;
    }

The Rust translation MUST thread an explicit `Caller` enum through
`transition_to` and every method that can trigger a transition:

    pub(crate) enum Caller {
        App,
        Sender,
    }

Every call site passes its origin literally. Do NOT infer it from
`tokio::task::id()`, a thread-local, or an `AtomicBool` "am I the sender" flag.

**Why:** The distinction is load-bearing for the transactional guarantee, and
is documented at length in Java 234-286 (KAFKA-14831). Application thread → the
transition throws, state is unchanged, and the user can recover. Sender task →
the manager moves to `FATAL_ERROR`, records `last_error`, and then throws
("poisons"), because a Sender-side invalid transition means the transaction's
integrity is already compromised and silently continuing would risk duplicate
or lost records.

`tokio::task::id()` is not a substitute: the Sender is a spawned task whose id
is not known to the app side at call time, and a caller may legitimately invoke
these methods from either side. Making the origin an explicit parameter also
makes it *reviewable* — a Critic can check each call site against the Java call
chain, which is impossible with an inferred value.

**How to apply:**

  - `transition_to(&mut self, target: State, caller: Caller)`.
  - Public `TransactionManager` methods called only from `KafkaProducer` pass
    `Caller::App`; methods called only from the Sender task or
    `RecordAccumulator`'s drain path pass `Caller::Sender`.
  - Where a method is reachable from both, it takes `caller` as a parameter and
    forwards it — do NOT default it.
  - A Critic MUST verify every call site against the Java call chain. There are
    roughly 20.

**Anti-patterns to flag in review:**

  - `Caller` defaulted, inferred, or derived from task/thread identity.
  - A method reachable from both sides that hardcodes one `Caller`.
  - `shouldPoisonStateOnInvalidTransition` translated as an unconditional
    `true` or `false`.

## 2. Lock topology: not everything belongs behind the shared lock

`TransactionManager` is shared three ways (`KafkaProducer`, `Sender`,
`RecordAccumulator`), so it lives behind `Arc<Mutex<TransactionManager>>`. But
four Java fields are **non-volatile and not consistently guarded** by Java's
`synchronized` blocks:

  - `inFlightRequestCorrelationId` (Java 136)
  - `transactionCoordinator` (137)
  - `consumerGroupCoordinator` (138)
  - `coordinatorSupportsBumpingEpoch` (139)

plus `pendingRequests`, a plain `PriorityQueue` mutated through the
**unsynchronized** `lookupCoordinator(TxnRequestHandler)` (969) that
`Sender.java:522` calls directly.

These belong to the **Sender task's own unshared state** in Rust, NOT behind the
shared mutex. The `volatile` fields and the partition maps go behind the shared
lock.

**Why:** They are not bugs in Java — they are safe by single-thread
confinement, because only the Sender thread touches them.
`clearInFlightCorrelationId` is called from `TxnRequestHandler.onComplete`
(1410) *outside* the `synchronized` block that starts at 1421, which proves the
confinement is deliberate rather than accidental.

Putting everything behind one mutex would be safe but slower and less faithful;
leaving them unsynchronized in Rust would not compile. A Critic reading only the
Java `synchronized` keywords will get this wrong in both directions — hence this
rule.

**How to apply:**

  - Sender-owned (plain fields on the Sender task's state, no lock):
    coordinator nodes, in-flight correlation id,
    `coordinator_supports_bumping_epoch`, the pending-request queue.
  - Shared (behind `Arc<Mutex<TransactionManager>>`): the state machine field,
    `last_error`, the producer id/epoch, `TxnPartitionMap`, the
    new-partitions/pending-partitions sets, and everything Java marks
    `volatile` or guards consistently.
  - Document the split in a comment on the struct definition so the next
    reviewer does not have to re-derive it.

## 3. Lock ordering is `deque` → `TransactionManager`, never inverted

`RecordAccumulator.java:877-926` assigns sequence numbers **inside**
`synchronized (deque)` while calling into `synchronized` `TransactionManager`
methods: `maybeUpdateProducerIdAndEpoch` (908), `sequenceNumber` (918),
`incrementSequenceNumber` (919), `addInFlightBatch` (924).

Rust MUST preserve that order: acquire the per-partition deque lock first, then
the `TransactionManager` lock. Never the reverse.

**Why:** Java never inverts it, so no deadlock is possible there. Inverting it
in Rust — for example by locking the manager in the drain loop's outer scope and
the deque inside — introduces a lock cycle against the Java-ordered path and
deadlocks under concurrent send + drain.

**How to apply:**

  - The Rust accumulator already holds per-partition deques as
    `DashMap<i32, Mutex<VecDeque<ProducerBatch>>>`
    (`record_accumulator.rs:128`). Lock the deque, then the manager.
  - The whole block is CPU-bound with no `.await`, so `std::sync::Mutex` is
    correct — same reasoning as `consumer-threading.md` §16 for
    `SubscriptionState`. Do NOT use `tokio::sync::Mutex` here.

## 4. Never hold the manager guard across `.await`; never race the network poll

Two hard rules for the Sender task, both extending
`consumer-threading.md` §10 to the producer:

  1. No `MutexGuard` on `TransactionManager` may be held across any `.await`.
  2. The network poll MUST NOT be raced in a `tokio::select!`.

**Why:** Rule 1 is CLAUDE.md §9.6.2 — holding a guard across an await deadlocks
the runtime. `Sender.java:459-518`
(`maybeSendAndPollTransactionalRequest`) calls roughly ten
`TransactionManager` methods interleaved with three `client.poll(...)` calls
(462, 496, 509), a blocking `awaitNodeReady` (484), and two
`time.sleep(retryBackoffMs)` (501, 525). In Rust all of those become `.await`
points, so the guard must be acquired and released repeatedly around them.

Rule 2 is the failure documented in
`design/current/consumer-join-stall-rootcause.md`: the poll is **not
cancel-safe**, because `initiate_connect` calls `connection_states.connecting()`
(a persisted side effect) and *then* awaits the TCP handshake. A `select!` that
drops the poll at that await strands the node in `Connecting` with no socket,
recovering only after the ~10 s connection-setup timeout. Milestone 8 learned
this the hard way; do not relearn it in the producer.

**How to apply:**

  - Acquire → read/mutate → drop the guard, then await. Re-acquire after.
  - Be aware this changes atomicity relative to Java, where the Sender thread's
    view is stable simply because it is the only writer. Each re-acquire is a
    point where Java's implicit consistency can break — Phase 6 must review
    `maybe_send_and_poll_transactional_request` method-by-method for state that
    Java assumed stable across the whole call.
  - To interrupt a poll, poke the selector's wakeup primitive as the consumer
    does; do not let a `select!` arm complete and drop the poll future.

## 5. `TransactionalRequestResult` keeps latch semantics — a `oneshot` is wrong

Java uses `CountDownLatch(1)` plus a **separate `volatile boolean isAcked` that
is set only inside `await()`** (`TransactionalRequestResult.java:62`). The Rust
translation MUST preserve both the re-awaitability and the
`is_acked` / `is_completed` distinction.

Required shape:

    pub(crate) struct TransactionalRequestResult {
        notify: Arc<Notify>,
        error: Mutex<Option<KafkaError>>,
        completed: AtomicBool,
        acked: AtomicBool,
        operation: String,
    }

Do NOT use `tokio::sync::oneshot`.

**Why:** `TransactionManager.handleCachedTransactionRequestResult`
(Java 1261-1283) keys off `isAcked()`, **not** `isCompleted()`. A
`commitTransaction` that completed but was never awaited must return the *same*
result object on retry; one that timed out must stay retryable while a
*different* operation is rejected with `IllegalStateException`. A `oneshot` is
single-consumer and not re-awaitable, so it cannot express this at all.

Getting this wrong produces a hang or a double-commit that **no Phase 1 unit
test would catch** — the covering tests live in `TransactionManagerTest`
(Phase 5). That is why the shape is mandated here rather than discovered later.

**How to apply:**

  - `is_acked` is set **only** by the awaiting methods, never by `done()` or
    `fail()`.
  - Java sets `isAcked = true` *before* checking `error` (62-65), so a failed
    result is still marked acked. Preserve that ordering.
  - `Notify::notify_waiters()` does **not** store a permit, so a waiter that
    arrives after `done()` would block forever. Create the `notified()` future
    **before** checking the `completed` flag, then check, then await — this
    closes the race. A test MUST cover `done()`-before-await.
  - Java's `InterruptException` path (66) has no Rust analogue (no thread
    interruption); note that in a doc comment rather than inventing one.

**Anti-patterns to flag in review:**

  - `tokio::sync::oneshot` or `tokio::sync::mpsc` as the completion primitive.
  - `is_acked` folded into `is_completed`, or set by `done()`.
  - Awaiting without the create-future-then-check ordering.

## 6. Never key a `BTreeSet`/`BTreeMap` on an interior-mutable sort key

`TxnPartitionEntry.inflightBatchesBySequence` is a `TreeSet<ProducerBatch>`
ordered by `(producerId, producerEpoch, baseSequence)`
(`TxnPartitionEntry.java:62-65`) — **all three of which mutate** via
`resetProducerState`. Java copes by rebuilding the set in `resetSequenceNumbers`
(154-161).

The Rust translation MUST key on an **explicit snapshot tuple**
`(i64, i16, i32)` and rebuild the collection wherever Java rebuilds the set. Do
NOT implement `Ord` for a batch type by reading its mutable fields.

**Why:** A `BTreeSet` whose `Ord` reads interior-mutable state violates the
collection's ordering invariant the moment a contained element mutates. The
result is logic corruption — lookups and removals silently miss — not a panic,
so it fails quietly and late. The 3-key comparator (rather than base sequence
alone) exists to fix a real bug; the comment at Java 58-61 records it and MUST
be carried across.

**How to apply:**

  - Key type `(producer_id, producer_epoch, base_sequence)` in that order,
    matching Java's comparator chain exactly.
  - Wherever Java rebuilds the `TreeSet`, the Rust code collects, mutates, and
    re-inserts under the new keys.
  - Keep the Java 58-61 comment explaining the 3-key rationale.

## 7. `TxnPartitionEntry` does not own `ProducerBatch`

Java's `inflightBatchesBySequence` holds **references** to batches that are
simultaneously owned by the accumulator's deque and the Sender's in-flight map.
Rust has no such second owner available: `RecordAccumulator` stores batches by
value in `DashMap<i32, Mutex<VecDeque<ProducerBatch>>>`
(`record_accumulator.rs:128`), `drain()` moves them out, and
`Sender::in_flight_batches: HashMap<TopicPartition, Vec<ProducerBatch>>`
(`sender.rs:122`) becomes the sole owner. `ProducerBatch` is not `Clone`.

Therefore `TxnPartitionEntry` MUST track in-flight batch **ordering keys**, not
batches. Methods that Java implements by mutating the contained batches take the
batches from their owner instead.

**Why:** Storing `ProducerBatch` by value in the entry would require a second
owner of a non-`Clone` type, which does not compile. The alternative —
`Arc<Mutex<ProducerBatch>>` throughout the accumulator and Sender — would add a
per-batch lock acquisition to the drain path, which CLAUDE.md §11 names as a hot
path, and would be a large refactor of the crate's most load-bearing code for no
behavioral gain.

**How to apply:**

  - The entry holds an ordered key collection under rule 6's key type.
  - `add_inflight_batch` / `remove_in_flight_batch` accept `&ProducerBatch` and
    derive the key from it — the batch is borrowed, never stored.
  - `next_batch_by_sequence` returns the ordering **key**; the caller (which
    owns the batches) resolves it.
  - `start_sequences_at_beginning` and `adjust_sequences_due_to_failed_batch`
    receive mutable access to the batches from the owner, and MUST iterate in
    key order to match Java's `TreeSet` iteration.
  - Record this as a justified deviation per `definition-of-done.md` §7 in any
    commit that touches these types — a Critic comparing field-by-field against
    Java will otherwise report the missing batch storage as a defect.

**Anti-patterns to flag in review:**

  - `BTreeMap<(i64, i16, i32), ProducerBatch>` or any field storing batches by
    value inside `TxnPartitionEntry`.
  - Converting the accumulator or Sender to `Arc<Mutex<ProducerBatch>>` solely
    to satisfy this type.
  - Iterating the batches in `HashMap` order rather than key order when
    resetting sequences.

## 8. `TxnPartitionEntry::decrement_sequence` does not wrap

Java's `TxnPartitionEntry.decrementSequence` (163-173) performs **plain
subtraction** and throws `IllegalStateException` when the result is negative. It
does **NOT** call `DefaultRecordBatch.decrementSequence`.

Only `incrementSequence` (104-106) delegates to the wrapping helper
`DefaultRecordBatch.incrementSequence`.

**Why:** The two helpers in `default_record_batch.rs` wrap at `i32::MAX` / 0.
Routing `TxnPartitionEntry`'s decrement through the wrapping helper would turn
an error case into a silent wrap to a large positive sequence — a behavioral
divergence that produces `OutOfOrderSequenceException` on the broker rather
than a clean local error.

**How to apply:**

  - `increment_sequence` → reuse
    `crate::common::record::default_record_batch::increment_sequence`.
  - `decrement_sequence` → plain subtraction, and return
    `Err(KafkaError)` (per CLAUDE.md §10.2, not `panic!`) when negative,
    preserving Java's message text. A test MUST assert the message.
