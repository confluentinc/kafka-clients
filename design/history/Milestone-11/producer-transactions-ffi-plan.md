# C FFI for Producer Transactions (`src/ffi/producer.rs`)

## Context

Milestone 11 translated the producer's idempotence and transaction support
(`TransactionManager` and its dependency closure) but deliberately shipped
**Rust only**. `PLAN.md` §7.2 and §9.6 record why the bindings were deferred, and
it was not merely phase count:

> The guard is "one operation in flight" and `wakeup()` bypasses it — but a
> *transaction* spans multiple FFI calls (`begin` → N× `send` → `commit`). So the
> guard needs a story for an open transaction: does it stay held from
> `begin_transaction` to `commit_transaction` (which would block the `send` calls
> that must happen in between), or does the FFI need transaction-state-aware
> admission control? That is a design question, not a translation question.

This document is the answer. It covers the C FFI only; `producer.py`, both gRPC
servers, and the multilanguage scenarios remain deferred (§9.6 is only partly
discharged).

The five methods being exposed, all on the `Producer` trait and implemented by
both `KafkaProducer` and `MockProducer`:

| Rust (`kafka_producer.rs`) | Java | Blocking? |
| --- | --- | --- |
| `init_transactions` (`:638`) | `initTransactions()` (`:648`) | yes (`async`) |
| `begin_transaction` (`:683`) | `beginTransaction()` (`:674`) | **no** (sync) |
| `send_offsets_to_transaction` (`:740`) | `sendOffsetsToTransaction(Map, ConsumerGroupMetadata)` (`:733`) | yes (`async`) |
| `commit_transaction` (`:806`) | `commitTransaction()` (`:779`) | yes (`async`) |
| `abort_transaction` (`:845`) | `abortTransaction()` (`:813`) | yes (`async`) |

## The design question, and the answer

### The premise the deferral rested on was half wrong

`PLAN.md` assumed the producer FFI inherits the Milestone-9 consumer access
guard. **It does not.** Reading `src/ffi/producer.rs` as it stands:

- there is no `UnsafeCell`, no `owner: AtomicU64`, no `acquire`/`release`, and no
  `ConcurrentModificationError` anywhere in the producer FFI;
- no send is *rejected* for concurrency. `send_async` takes no shared lock at
  all — it pushes onto an unbounded channel and returns. `send` and `send_batch`
  do take the `kind` mutex, and hold it across their `block_on` of the enqueue,
  so blocking senders are **serialized** (see "What the send path actually
  guarantees" below) — but never rejected.

That is correct and must not change: `KafkaProducer` is `Sync`, and Java documents
`send()` as safe to call concurrently from many threads. The consumer needed a
guard because `Consumer` is `!Sync` and every method takes `&mut self`, mirroring
`KafkaConsumer.acquire()/release()`; none of that applies here.

So the real question is not "how do we relax the existing guard for
transactions". It is "what mutual exclusion do transactions need, given there is
none today, without acquiring any over `send`".

### What Java actually guarantees

Two separate contracts, and conflating them is what makes the question look hard:

1. `send()` is thread-safe and stays callable concurrently at any time —
   **including while a transaction is open**. A transaction that spans
   `begin` → N× `send` → `commit` is the normal case, and the N sends may come
   from many threads.
2. The transaction-*control* methods are **not** safe to call concurrently with
   each other. `KafkaProducer` does not defend against it; the application is
   required to drive the lifecycle from one logical thread at a time. Overlapping
   `commitTransaction()` and `abortTransaction()` is an application bug whose
   consequence is a corrupted `TransactionManager` state machine.

Note what contract 2 does **not** say: it does not say the same thread must make
every call, and it does not say an open transaction is itself an operation. It
constrains *overlap between control calls*, nothing more.

### Decision: a per-call flag scoped to the five control functions

`ProducerHandle` gains one field:

```rust
/// Transaction-control mutual-exclusion flag. `true` while one of the five
/// transaction-control functions is executing.
txn_control_busy: std::sync::atomic::AtomicBool,
```

Each of the five FFI functions is one call to `with_txn_control(producer, op)`, which

1. rejects a null handle with `Errors::InvalidRequest` (the file's existing
   convention — see `kafka_producer_Producer_partitions_for`);
2. `compare_exchange(false, true)`, returning
   `KafkaError::concurrent_modification("Transactional methods of KafkaProducer
   are not safe for concurrent access.")` if it fails;
3. reads the inner producer and its runtime handle from **cached fields, taking
   no lock** (see below);
4. drains the submission channel so the call is ordered after every send the
   application already issued (see "The second design question" below);
5. runs the operation with a `TxnControlGuard` alive, whose `Drop` clears the flag
   on every exit path including a panic.

The flag is therefore held for exactly the duration of one control call, and
released on return *or* panic. The three properties this buys:

- **Overlapping control calls fail fast.** Concurrent `commit_transaction` and
  `abort_transaction`: one runs, the other returns a
  `ConcurrentModificationError` immediately, before the producer is touched.
- **`send` and transaction control cannot delay each other, in either
  direction.** Neither send function looks at the flag, and a control call holds
  no lock while it `block_on`s, so a concurrent `send` can proceed. Getting the
  reverse direction right needed step 3 above — see the next section.
- **An open transaction holds nothing.** `begin_transaction` releases the flag as
  it returns, so the N sends in between are unaffected and `commit_transaction`
  can acquire it in turn.

### Why the transaction-control entry point takes no lock

The first version of step 3 took the `kind` mutex briefly — long enough to clone
the runtime handle and build a `ProducerStaticRef`. That is the pattern
`flush_or_close_async` and `partitions_for_async` use, and it looked harmless
because the lock is not held across the `block_on`.

It was not harmless, and only proved half of what it claimed. It established that
a control call cannot block `send`, but not the reverse: `kafka_producer_Producer_send`
holds `handle.kind.lock()` *across* its call to `producer_send`, which
`rt.block_on`s the enqueue — and the enqueue waits on topic metadata, a broker
round-trip bounded by `max.block.ms`, whenever the topic is not cached. So a
control call could stall behind an unrelated `send` for up to `max.block.ms`, even
with `txn_control_busy` free. Measured on an unreachable broker with
`max.block.ms=4000`: `commit_transaction` took **3797 ms**.

`ProducerHandle` therefore caches both values, and the entry point reads them
with no lock at all:

```rust
runtime: tokio::runtime::Handle,                  // cloned at construction
inner:   std::sync::OnceLock<ProducerStaticRef>,  // set from the raw pointer, after Box::into_raw
```

Both are sound because `kind` is never reassigned after construction — every call
site only ever reads through `&*guard` — so neither the runtime nor the address of
the inner producer can change. `inner` is populated inside `build_producer_handle`
while nothing else can reach the handle, so the single lock it takes is guaranteed
uncontended. It is set **after** `Box::into_raw`, deriving the cached reference
from the raw pointer: an earlier revision did it before, from a `&*handle`
reborrow, but that reborrow is a *child* of the box's borrow that the field
move-out in `destroy` invalidates, so under Stacked/Tree Borrows the long-lived
cached reference would carry provenance that is later revoked. Deriving from the
raw pointer gives it the pointer's own provenance, which the move-out does not
touch. (The *address* is identical either way — `into_raw` returns the address the
box already had — the fix is purely about provenance.)

Reads go through `producer_inner`, which returns `Option` and **never** falls back
to the locking path. An earlier revision used `OnceLock::get_or_init` with
`producer_static_ref` as the fallback; that was wrong twice over — the fallback
takes the very mutex the cache exists to avoid, and it runs *after* the CAS has
claimed the flag, so if it ever fired it would block while rejecting every sibling
control call, inverting the property being bought. Its `.unwrap()` on a poisoned
mutex would also have panicked across the `extern "C"` boundary, which aborts.
`None` is unreachable in practice and is reported as an error instead.

Same measurement after the fix: **0.1 ms**.

The same reasoning then applies to three neighbours, so they were fixed with the
same cache. `flush_async`, `close_async` and `partitions_for_async` each took the
`kind` mutex **on the caller's thread**, before spawning anything — so three
functions documented "returns immediately" could block for up to `max.block.ms`
behind a blocking `send`. (An earlier revision of this document argued a lock wait
there "delays nothing the caller is waiting on". That was wrong: the caller is
waiting on the function to return.) And `submission_loop` rebuilt a
`ProducerStaticRef` per record, so a single blocking `send` on an uncached topic
head-of-line blocked the entire async send pipeline for every other thread —
exactly the lock contention on the send path CLAUDE.md §11 calls out. All four now
read the caches; `producer_static_ref` has one remaining caller, the constructor
that fills them.

What remains, and is inherent rather than an FFI artefact: `commit_transaction`
and a concurrent `send` both briefly take `KafkaProducer`'s own internal locks
(`pending_requests`, `transaction_manager`). Those are held for state transitions
only, never across an `.await` (CLAUDE.md §9.6.2), and they are the same
contention Java has inside its `synchronized` blocks.

### The second design question: async sends have no ordering with transactions

> **SUPERSEDED (CFFI round).** The whole transaction↔outbox ordering mechanism
> described in this section — the per-operation `QueuedSends::{Submit,Discard}`
> directive, the `discard_queued_sends` window, and the `ends_discard`-tagged
> `Barrier` — was **deleted**. The decision (see "## Decision update:
> async-in-transaction is unsupported (UB); machinery deleted" below) is that async
> sends inside a transaction are **unsupported and undefined**, documented rather
> than enforced. This section is retained as history of the road not taken; the
> `flush`/`close` drain it also introduced (a plain, untagged `Barrier`) is the only
> part that survives. Read the Decision-update section for the intermediate state.
>
> **Later re-supported (PR #168 review).** Async sends inside a transaction are
> supported again, *without* reviving the machinery named here: the surviving
> `flush`/`close` drain is now run before every transaction-control op too, so returned
> sends are committed/discarded with the transaction. See "## Decision update 2".

The guard above is about *control calls colliding with each other*. A separate
defect, found only once the C tests exercised the async path, is that control
calls had no ordering with the **send** path either — and unlike the guard
question, this one is a correctness bug with no Java analogue.

`send_async` does not call `producer.send()`. It validates the record, pushes a
`SendRequest` onto an unbounded channel, and returns; the real send happens later
on the submission task. Nothing ordered a transaction-control call against that
channel. So for `begin_transaction()` → `send_async(rec)` → `abort_transaction()`,
the record was typically still queued when the abort ran, and the submission task
handed it to the producer *afterwards*:

- On `MockProducer`, `send` routes on `transaction_in_flight` at the moment it
  runs (`mock_producer.rs:946`). With the transaction already aborted it pushed
  the record to `sent` — **an aborted record in committed history**. Reproduced
  10/10 through the public C ABI before the fix.
- On the real producer, `maybe_add_partition` rejects the late record with
  `IllegalState`, and `submission_loop` discarded that with `let _ =`. So
  `commit_transaction` returned **success** for a transaction that was missing a
  record the application had sent into it, and the stale batch could drain inside
  the *next* transaction.
- `flush()` before the commit does not help, which is what rules out documenting
  the hazard instead of fixing it: flush drains the producer's accumulator, and a
  queued record has not reached the accumulator yet. There was no correct way for
  a C caller to use `send_async` inside a transaction.

Java has no counterpart because Java's `send()` appends to the accumulator
synchronously before returning; the deferred enqueue is ours alone, so there was
nothing to translate.

**Chosen fix: an ordering barrier on the existing channel, per-operation.**
`SubmitRequest` becomes an enum of `Send(SendRequest)` and `Barrier { ack }`, and
each control call declares what it wants done with sends the application has
already submitted (`QueuedSends::{Ignore, Submit, Discard}`). Channel delivery is
FIFO and the submission task fully finishes each send before taking the next item,
so a barrier reaching the front *is* the proof that everything ahead of it is done.

The three modes are not uniform, because the requirement is not uniform — it is
about the transaction's **record boundary**:

- **`commit_transaction` → `Submit`.** Waits for queued records to be handed over,
  so they are inside the committed transaction. Required: this is the bug above.
- **`abort_transaction` → `Discard`.** Marks queued records to be *failed* rather
  than produced. Required for two reasons. Correctness: the records belong to the
  transaction being thrown away, so producing them is wrong, and failing them with
  `TransactionAborted` is exactly what Java does to accumulator records on abort.
  Availability: an earlier revision made abort *produce* them like commit, which
  meant 20 queued records against an unreachable broker blocked the abort for ~20 ×
  `max.block.ms` — while holding `txn_control_busy`, so a watchdog's abort got
  `ConcurrentModification`, which the docs correctly say is not a reason to abort.
  That left the caller no legal recovery move at all. Abort is *the* recovery
  operation; it must not wait on the broker that is the reason recovery is needed.

  Abort does still wait for the barrier marker, and that turned out to be
  load-bearing rather than optional. A first attempt made it fire-and-forget, which
  reproduced a narrower version of the original escape: at most one send can be
  mid-handover, and if abort returns before it finishes, the mock routes that record
  *after* `transaction_in_flight` has been cleared — straight into committed history.
  It failed 1 run in 15. So the wait stays, bounded by a **single in-flight
  handover** regardless of queue length, which is a different order of magnitude
  from commit's N and no worse than the `max.block.ms` Java's `abortTransaction`
  already blocks for.
- **`init_transactions`, `begin_transaction`, `send_offsets_to_transaction` →
  `Ignore`.** None has a record-boundary requirement: `init` runs before any send
  can exist, and staging offsets is independent of records. `begin` is the
  interesting one — a record queued before `begin` and handed over after it can be
  attributed to the new transaction on a mock. That is accepted rather than fixed,
  because barriering `begin` would falsify its documented "pure state transition
  that never waits" (the property that makes it synchronous in Rust at all,
  CLAUDE.md §4), and reaching that state requires an already-invalid program: a
  transactional `KafkaProducer` rejects a send with no transaction open.

**Cost: one atomic load in the normal case.** `ProducerHandle::queued_sends`
counts sends that are queued *or in flight* — decremented by a `Drop` guard only
after the handover completes, so an in-flight send still counts and a barrier
cannot skip past it. When the count is zero, and it is zero for every caller using
the blocking send, both `Submit` and `Discard` return immediately without touching
the channel. Proven by unit test: the fast path is handed a channel whose receiver
is already gone and still returns `Ok`, which a pushed barrier could not have done.

**Failure is reported, never swallowed.** An earlier revision did a bare `return`
when the barrier could not be pushed and `let _ =` on the acknowledgement, so both
paths reported success. That is the worst available failure mode here: a tokio
receiver dropped with items queued drops those items *and their callbacks*, so
`commit_transaction` would return NULL — transaction committed — for a transaction
whose records were never produced and will never be reported on. Both paths now
return `KafkaError::IllegalState` saying exactly that, and both are unit tested.
The earlier doc's reasoning was inverted too: it said a closed channel means
"nothing can still be queued", when the truth is the task is gone *and took the
queue with it*.

Deliberate boundaries of the guarantee:

- It covers everything submitted **before** the control call began. Sends queued
  concurrently by another thread are not ordered, and cannot be: an application
  that races `send` against `commit_transaction` has the same ambiguity in Java.
- `commit_transaction`'s wait is genuinely unbounded when records are queued and
  the broker is not answering: each send may spend up to `max.block.ms` on
  metadata. That is faithful rather than a regression — it is the same total the
  application would already have spent inside Java's blocking `send()` calls before
  reaching `commitTransaction` — and the escape hatch is `abort_transaction`, which
  never waits. A timeout was rejected: it would leave the transaction in a state
  where neither the caller nor the binding knows which records made it in.
- A send already mid-handover when `abort` runs cannot be recalled. Abort waits for
  it (see above) so the producer routes it while the transaction is still open, and
  it is then failed as part of the aborted transaction, as Java would.

**Rejected: make `send_async` append synchronously and defer only the await.**
More faithful to Java, and it would remove the ordering problem at the root. But
the enqueue is exactly the part that can block for `max.block.ms` on metadata, so
this turns `send_async` into a blocking call — breaking the contract of a shipped
API and undoing the reason the shared submission task exists (CLAUDE.md §11: no
per-message spawn, keep the caller's thread free). It is also a rewrite of the
whole send path rather than a change to transaction control. If the blocking-send
serialization noted below is ever addressed, this is worth revisiting as one
combined change.

### What the send path actually guarantees

Worth stating precisely, because two earlier revisions of this document and of the
module rustdoc overclaimed it — and the overclaim reached the generated public
header, where a C integrator would have relied on it:

| | rejected for concurrency? | serialized? |
| --- | --- | --- |
| `send`, `send_batch` | never | **yes** — they hold the `kind` mutex across the enqueue (`send_batch` across the whole batch), so N threads take turns and one metadata fetch blocks them all up to `max.block.ms` |
| `send_async`, `send_batch_async` | never | no — channel push only |

Both rows are outside `txn_control_busy`, which is the part that matters for
transactions: no send is ever rejected because a transaction-control call is
running, and none is delayed by one. But "unguarded" was the wrong word for the
blocking pair, and the module rustdoc plus all four send functions now say which
guarantee is which. Removing that serialization is a change to the blocking send
path, out of scope here.

### Why not the alternatives

**Hold the guard from `begin` to `commit`.** This is the option `PLAN.md` raised
and rejected in the same sentence, and it is genuinely wrong: it blocks the sends
that define the transaction. It also cannot work mechanically — `commit_transaction`
would have to acquire a flag it already holds, so it needs either reentrancy
tracking or an asymmetric release, and neither expresses anything Java promises.

**Transaction-state-aware admission control** (the other option `PLAN.md`
floated): a table of which control call is legal in which transaction state,
enforced in the FFI. Rejected as a layering error. `TransactionManager` *is* that
state machine, it already rejects illegal transitions with the right error and
the right message, and duplicating a subset of it in the FFI would produce two
sources of truth that drift. The FFI's job is the concurrency contract the type
system stops enforcing at the `*mut` boundary; the state contract is the
producer's.

**Reuse the consumer's `acquire`/`release` verbatim.** It is not generically
factored — `fn acquire(h: &ConsumerHandle)` is private to `consumer.rs` and typed
to that handle. What *is* reused is the part that matters for consistency: the
same fail-fast error, `KafkaError::concurrent_modification`. Nothing was moved out
of `consumer.rs`, so the consumer guard is untouched.

**Store an owner thread id (`AtomicU64` + `current_thread_id`), like the
consumer.** Deliberately not done. The consumer's is a single-*owner* guard: it
records which thread holds the consumer because a `KafkaConsumer` semantically
belongs to one thread. Ours is a single-*operation* flag with no thread affinity,
because contract 2 forbids overlap, not multi-thread use — an application may
legitimately drive `begin` from one pool thread and `commit` from another, in
sequence. An owner id would imply an affinity we do not want and cannot justify,
and it would need `current_thread_id` lifted out of `consumer.rs` for no gain.
`AtomicBool` says exactly what is true and no more.

## Decisions locked in

1. **Blocking variants only** — no `_async` twins, and therefore no new
   `*_callback_t` typedefs. `origin/dev/admin-bindings` pairs every RPC with an
   `_async` form, but that is because Java's `Admin` surface is already
   `KafkaFuture`-based: there, async is the faithful translation and blocking is
   the convenience wrapper. Transactions are the mirror image — Java's
   transaction API is exclusively blocking, so blocking *is* the contract
   (CLAUDE.md §4: never change the contract of a public API). A caller must know
   each step's outcome before taking the next, so an `_async` form would have to
   be immediately awaited to be usable; all it would add is a second way to
   violate contract 2, and a guard held across a submit→callback window — the very
   thing §7.2 warned about.
2. **No new handle type, and no `_destroy`.** Java's transaction methods all
   return `void`; nothing is handed back to own. The five FFI functions return
   `*mut kafka_common_KafkaError_t` (null = success), the file's existing
   convention for fallible void operations.
3. **`send_offsets_to_transaction` reuses the consumer FFI's marshaling
   wholesale.** No new C types were invented:
   - offsets arrive as the same five parallel arrays as
     `kafka_consumer_Consumer_commit_sync_offsets`
     (`topics`, `partitions`, `offsets`, `leader_epochs`, `metadata`, `count`),
     parsed by the *same* function — `read_offset_map` was widened from private to
     `pub(crate)`;
   - group metadata arrives as `*const kafka_consumer_ConsumerGroupMetadata_t`,
     the handle `kafka_consumer_Consumer_group_metadata` already returns, read
     through a new `pub(crate) group_metadata_ref`. This is Java's
     `producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())`
     expressed in C. The handle is borrowed, not consumed: the caller still owns
     and destroys it. The precedent for sharing a type across the two FFI modules
     is `kafka_consumer_PartitionInfoList_t`, which
     `kafka_producer_Producer_partitions_for` already returns.
4. **A negative `count` on `send_offsets_to_transaction` asserts.** The shared
   `read_offset_map` clamps with `count.max(0)`, which yields an empty map, and both
   backends short-circuit an empty map to `Ok(())`. A negative count therefore
   *succeeded* while staging nothing, and the following commit succeeded too —
   offsets silently not committed, which is an exactly-once violation with no error
   surfaced anywhere (picture a consume-transform-produce loop whose count comes
   from an underflowing signed subtraction: after a restart it reprocesses the whole
   range). All five other count-taking producer FFI functions assert; this one now
   matches them, and the assert is covered by a Rust `catch_unwind` unit test.

   A zero `count` is a separate matter, and the header now documents it honestly
   rather than claiming Java parity in one sentence: it stages nothing, but whether
   it *succeeds* is backend-specific, and **both behaviours are faithful to their
   own Java counterpart, which disagree with each other**. `KafkaProducer`
   short-circuits an empty map before consulting transaction state (Java
   `KafkaProducer:738`), so a zero count returns success even with no transaction
   open — a success that confirms nothing. `MockProducer` runs its state checks
   first (Java `MockProducer:186-193`, empty check at `:194-196`), so the same call
   errors. Inside an open transaction — the only state where they agree — both
   succeed, and that was the only case the C suite asserted, which is why the
   divergence was invisible.

5. **`txn_requires_abort` is exposed on the shared error handle.** A caller
   cannot otherwise distinguish "retry the commit" from "you must abort".
   `kafka_common_KafkaError_txn_requires_abort` sits in `common.rs` next to
   `_is_retriable` / `_is_fatal` and follows their shape exactly, including the
   null-handle-returns-false convention.
6. **Three new `MockProducer` driver hooks.** Two are read-only probes added so
   the offsets test can verify its round-trip rather than merely not crash:
   `MockProducer_sent_offsets` (Java's `sentOffsets()`) and
   `MockProducer_committed_offset`, which looks up one `(group, topic, partition)`
   and reports offset, leader epoch and metadata. It is backed by a new
   `MockProducer::committed_offset` rather than by `consumerGroupOffsetsHistory()`:
   that accessor deep-clones the entire
   `Vec<HashMap<String, HashMap<TopicPartition, OffsetAndMetadata>>>` on every call,
   which for a single lookup meant cloning the whole history under the `kind` mutex
   and discarding all but one entry. The new method scans under the mock's own lock
   and clones just the match. The probe also null-guards `group_id`/`topic` (a
   natural "is anything recorded for X" probe would otherwise segfault in
   `CStr::from_ptr(NULL)`) and truncates metadata on a UTF-8 character boundary, so
   a value that arrived through `to_string_lossy` can never reach C as an invalid
   string. The third is the error hook: `MockProducer` has Rust setters for all
   five control-method errors, but none were reachable from C, so the abortable
   commit path was untestable there. `kafka_producer_MockProducer_set_commit_transaction_error`
   exposes just the one the C tests need, next to the existing
   `MockProducer_error_next` and mirroring the consumer FFI's
   `kafka_consumer_MockConsumer_set_poll_error`. The four sibling setters are left
   unexposed — they have no C caller, and unused FFI surface is surface to
   maintain.

   Its signature is `(producer, clear: bool, error_code: i32, error_message)`.
   Clearing is a **separate flag, not a reserved code**: the obvious
   `error_code < 0` sentinel would have made `-1` (`UnknownServerError`, a real
   and legitimately installable code) permanently unreachable. `error_code` stays
   `i32` for the module's fixed-width-parameter rule but is *validated*, not cast:
   a value outside `i16` range is rejected rather than silently truncated to an
   unrelated code (`65656` would otherwise become `120`), and `0` is rejected
   because `Errors::None` would install an error handle that reports success. An
   in-range but unassigned code resolves to `UnknownServerError`, matching
   `MockProducer_error_next`.

   It is documented as **setup-only**: it writes the mock's installed-error field
   without taking `txn_control_busy`, so overlapping it with a live
   `commit_transaction` leaves it undefined which value that commit sees. The
   mock's own mutex keeps that a logical race rather than undefined behaviour, and
   routing it through the guard was rejected because a rejection would have to be
   reported through the same `false` return that already means "not a mock".
7. **No `cbindgen.toml` change.** `[export].include` is an allow-list of *types*
   and callback typedefs; `#[unsafe(no_mangle)] extern "C"` functions are emitted
   unconditionally. Since decisions 2–4 add no `_t` type and no `*_callback_t`,
   there is nothing to add. Verified by regenerating `target/include/confluent_kafka.h`
   and confirming all the new functions appear.

   One cbindgen behaviour did need accommodating: it copies rustdoc **verbatim**
   into the header, including reference-style link definitions. Eight
   `[`name`]: crate::ffi::...` lines from this diff were therefore appearing in the
   public C header as dangling Rust paths into a `pub(crate)` module. Doc comments
   on `#[unsafe(no_mangle)]` items now use plain names instead; `grep -c 'crate::'`
   on the generated header is back to 0. (Module-level `//!` docs are safe —
   cbindgen drops those — which is why the pre-existing two were never visible.)

## Implementation

**`src/ffi/common.rs`** — add `kafka_common_KafkaError_txn_requires_abort`.

**`src/ffi/consumer.rs`** — widen `read_offset_map` to `pub(crate)`; add
`pub(crate) unsafe fn group_metadata_ref`. No behavioural change; the consumer
access guard is not touched.

**`src/ffi/producer.rs`** —

- `SubmitRequest`, wrapping the existing `SendRequest` alongside the new
  `Barrier` variant on the submission channel;
- module rustdoc: a "Concurrency model" section stating the two contracts and
  pointing at this document (mirroring `consumer.rs`, whose module docs point at
  `consumer-ffi-plan.md`). All four send entry points — `Producer_send`,
  `Producer_send_async`, `Producer_send_batch`, `Producer_send_batch_async` —
  carry a one-line restatement too, because cbindgen does not copy module docs
  into the generated header and a C-only integrator reads only the header;
- `ProducerHandle::{txn_control_busy, runtime, inner, queued_sends}`, initialised
  in `build_producer_handle`. `txn_control_busy`
  is dropped in destroy with a comment: destroying a handle mid-control-call is the
  same C lifetime violation as destroying it mid-`send`, and per CLAUDE.md FFI §3 is
  not checked. `queued_sends` backs the `flush`/`close` outbox drain; the rewritten
  destroy is in the B1–B4 section. (A `discard_queued_sends` field this bullet
  originally also listed was **deleted** in the CFFI round — see "## Decision
  update" — because async-in-transaction is now unsupported;)
- `ProducerStaticRef` gains `#[derive(Clone, Copy)]` so it can be cached by value;
  `ProducerKind::Kafka` boxes its producer so destroy's move-out cannot relocate
  what the cache points at (B3);
- a `Transactions` section with `TxnControlGuard`, `with_txn_control`, and the
  five `extern "C"` functions, whose bodies are now one
  call each. (This bullet originally also listed a `drain_submitted_sends` wrapper
  and a `queued: QueuedSends` parameter on `with_txn_control`; both were **deleted**
  in the CFFI round — see "## Decision update" — so `with_txn_control` no longer
  touches the outbox at all.) `with_txn_control` takes a **closure** rather than returning the
  guard: a returned guard only lives as long as each caller keeps a binding named
  `_guard` alive, so a routine "unused variable" cleanup to `_` would silently
  disable the mutual exclusion for that function — no compile error, no failing
  test, and `#[must_use]` does not fire on `_`. With the closure the guard is
  unskippable, every early return and panic releases through one path, and the five
  byte-identical bodies collapse. It does **not** confine `ProducerStaticRef` to the
  closure body — an earlier revision claimed that, but the type is `Copy` (it has to
  be, to live in a `OnceLock`), so `op` can copy it into a captured variable and
  outlive the guard with it, compiling without a warning. Only the unskippability
  half of the argument holds, and the rustdoc now says so.
  `send_offsets_to_transaction` keeps a thin `extern "C"` wrapper over
  `send_offsets_to_transaction_inner`, mirroring `send_batch_inner`, so its `count`
  precondition is testable: a panic that unwinds out of an `extern "C"` function
  aborts the process, so the assert can only be exercised through the inner fn;
- `kafka_producer_MockProducer_set_commit_transaction_error` in the existing
  `Mock-specific operations` section, plus a shared `mock_error` helper for the
  message-or-default construction it has in common with `MockProducer_error_next`
  (whose own `i32`-truncating behaviour is left exactly as it was — changing a
  pre-existing API is out of scope).

Every function follows the file's established boilerplate: `#[unsafe(no_mangle)]
pub unsafe extern "C"`, `# Parameters` / `# Returns` / `# Safety` rustdoc, and
`match … { ProducerKind::Kafka(..) => …, ProducerKind::Mock(..) => … }` dispatch.

## Tests

`bindings/c/tests/test_mock_producer.c` — the lifecycle, broker-free and fully
deterministic. `MockProducer_history_count` is the transactional-isolation probe:
a record sent inside an open transaction is absent from the sent history until the
commit returns.

- `test_transaction_commit_publishes_records` — init → begin → send → history 0 →
  commit → history 1.
- `test_transaction_abort_discards_records` — a committed transaction establishes
  a non-zero baseline, then a second transaction's record is aborted and the
  history is unchanged.
- `test_transaction_send_offsets` — group metadata taken from a `MockConsumer`,
  two partitions with mixed present/absent leader epoch and metadata, then commit;
  also asserts `count == 0` is a no-op (Java parity). Every field is **read back**
  through the new `MockProducer_committed_offset` probe: an earlier revision built
  those inputs carefully and asserted nothing about them, so it passed green even
  if the forwarding transposed partitions against offsets, dropped the leader
  epochs, passed `count - 1`, or staged an empty map.
- `test_transaction_send_offsets_rejection_releases_guard` — the two paths that
  return early *inside* the guard (null group metadata, an offset the marshaling
  rejects) must still release the flag. Each rejection is followed by ordinary
  successful use, which is what fails if the flag leaks: a leaked flag wedges the
  producer permanently and every later control call is rejected.
- `test_transaction_abort_discards_async_sends` and
  `test_transaction_commit_publishes_async_sends` — the ordering barrier. Both
  drove the original bug: abort left three records in committed history (10/10 runs)
  and commit reported success having published none of them. Every other
  transactional C test uses the blocking send helper, so nothing covered this.
  **REMOVED in the CFFI round** — see "## Decision update": these tested the deleted
  ordering machinery. **Re-added in a different form (PR #168):** async sends inside a
  transaction are supported again via the `with_txn_control` drain, covered by
  `test_transaction_commit_drains_async_queued_send` /
  `test_transaction_abort_drains_async_queued_send` (which assert the drain, not the
  deleted `Submit`/`Discard` machinery). See "## Decision update 2".
- `test_transaction_commit_error_requires_abort` — install error code 120
  (`TRANSACTION_ABORTABLE`) via the new hook; the commit fails with
  `txn_requires_abort() == true` and `is_fatal() == false`. Also pins the hook's
  input validation: `0` and an out-of-`i16` value are rejected, and `-1` installs
  `UnknownServerError` rather than being read as "clear". `clear = true` is then
  verified **behaviourally against `commit_transaction` itself** — the call the hook
  affects — because an earlier revision followed the clear with
  `abort_transaction`, which reads a different field and so would have passed even
  if clearing did nothing.
- `test_transaction_success_does_not_require_abort` — an ordinary illegal-state
  failure (double `begin`) reports `txn_requires_abort() == false`, so the flag is
  not merely "some error happened"; plus the null-handle case.
- `test_transaction_requires_init_first`, `test_transaction_commit_flushes_pending_sends`
  (three sends pending under `auto_complete=false`, all resolved by the commit),
  `test_transaction_null_producer`.

`bindings/c/tests/test_kafka_producer.c` — what needs the real `KafkaProducer`.

- `test_transaction_methods_on_non_transactional_producer` — without
  `transactional.id` every control method fails and none hangs; the mock hook is a
  no-op on a real producer.
- `test_transaction_control_guard_rejects_concurrent_calls` — the guard test. It
  needs a control call slow enough to overlap, which no mock call is: with an
  unreachable broker `init_transactions` blocks for exactly `max.block.ms`, so a
  helper thread holds the guard for ~1 s while the main thread asserts that
  `commit_transaction`, `abort_transaction` and `begin_transaction` are each
  rejected — and rejected with the guard's *message*, not merely a non-null error,
  since `concurrent_modification` shares `IllegalState`'s error code and an
  ordinary state error would otherwise pass. It then asserts `send_async` returns
  in under a second while the guard is held (unguarded, per contract 1), and that
  a control call is accepted again after the helper joins (no leaked flag).

  Also, and separately from the guard: the whole non-transactional test now covers
  all five control methods rather than three. `init_transactions` and
  `send_offsets_to_transaction` were omitted, which left the entire
  `ProducerStaticRef::Kafka` arm of `send_offsets_to_transaction` — marshaling, the
  group-metadata clone, the real `block_on` — covered by no test at all, since every
  other transactional test takes the mock arm. Every failure there is asserted by
  *message*, not just non-null: a leaked transaction-control flag would otherwise
  turn each expected error into a silently passing guard rejection, because
  `concurrent_modification` shares `IllegalState`'s error code. The same treatment
  was applied to the four mock tests that used bare `TEST_ASSERT_NOT_NULL(err)`,
  per `definition-of-done.md` item 3.

  Two distinct races had to be closed for the guard test to be deterministic. **Opening
  the window:** the helper races the main thread for the same flag, and a
  *rejected* `init_transactions` returns instantly, opening no window at all — so
  the helper retries until one of its calls is not rejected (a rejection never
  touches the producer, so the retry still blocks for the full `max.block.ms` once
  it wins). Without this the test failed roughly half the time. **Closing the
  window:** the Rust guard is released the instant `init_transactions` returns, so
  the helper publishes `txn_init_returned` *before* freeing the returned error
  handle; otherwise the guard is free while the flag still reads 0, and a
  main-thread poll landing there sees an ordinary state error instead of the
  rejection. The store cannot be made simultaneous with the guard release — that
  happens inside Rust — so the residual window is minimised to a few instructions
  rather than eliminated.

  The test records every outcome into locals and asserts only after
  `pthread_join` and `Producer_destroy`. Unity's `TEST_ASSERT_*` longjmp out of
  the test, so asserting while the helper is still running would, on failure,
  leave a live thread issuing FFI calls into a producer that never gets destroyed
  for the rest of the binary. `test_kafka_consumer.c`'s wakeup-thread test has the
  same shape.

  Two further sharp edges in that test, both fixed: the `send_ms` threshold equalled
  the configured `max.block.ms`, so it bounded the very delay it was meant to detect
  and could never meaningfully fail (now 50 ms against a measured ~0.01 ms); and the
  delivery-callback counter was *sampled* rather than waited on, which on a loaded
  box reports a phantom guard regression, since the FFI only enqueues the completion.
  `wait_for` was duplicated across the two C test binaries and now lives in
  `bindings/c/tests/test_support.h`.

  15 consecutive runs of both producer suites pass.

`bindings/c/CMakeLists.txt` — `test_kafka_producer` now spawns a thread, so it
links `pthread` explicitly in addition to `${SYSTEM_LIBS}` (which is empty under
shared linking), exactly as `test_kafka_consumer` already does.

## Verification

- `cargo build --features ffi`
- `cargo xtask lint`, `cargo xtask format-check`
- `make build-c && (cd bindings/c/build && ctest --output-on-failure)` — all four
  suites, `mock_producer` at 41 tests.

## Pre-existing producer-FFI bugs fixed alongside (B1–B4)

The transactions work surfaced four bugs it did not create, in the general
async-send / lifecycle machinery. The user chose to fix them on this branch
rather than defer. B4 is a Rust-core change kept to its own files
(`transaction_manager.rs`, `kafka_error.rs`) so it can be cherry-picked to the
parent transactions branch independently of the FFI work.

**B1 — a queued send's delivery callback fired the wrong number of times.**
`submission_loop` originally did `let _ = kp.send(record, Some(callback)).await`,
which **dropped** the callback on `send`'s early-guard errors (`ensure_not_closed`,
`throw_if_in_prepared_state`, a non-`ApiException` metadata error, and the mock's
closed/fenced/`set_send_error`): the C callback never fired, so `user_data` leaked
and an app blocking on it hung.

The first fix (fire on every `Err`) then introduced a **double-free**: `send`'s
`Err` is ambiguous. On the *early-guard* paths the callback was dropped unfired, so
the task must fire it. But on the *post-append* path — `do_send_bytes`'s
`maybe_add_partition` failing after `accumulator.append` already moved the callback
into a batch (`kafka_producer.rs:1129`, the transactional "send with no open txn"
case) — the callback is in the batch and fires later, while `send` still returns
`Err`. Firing on that `Err` too meant two fires of the same `user_data`: a
double-free. The task cannot tell the two `Err`s apart, so "Err ⟺ not yet fired"
is **not** exact (the round-6 claim here was wrong; round 7 found it).

Final fix: an **at-most-once shared guard**. `make_record_callback` takes a
per-record `Arc<AtomicBool> fired`; every callback built for one record — the one
handed to `send` (fired by the batch or `handle_api_exception`) and the task's own
error re-fire — shares it and compare-exchanges before delivering. Whichever runs
first wins; the rest, including the free of `user_data`, are no-ops. This makes the
report fire **exactly once** on every path *by construction*, without depending on
classifying `send`'s error, and without touching `send`'s signature or its
throw-semantics for the blocking/Rust callers (why this was chosen over the two
options the reviewer floated: return-the-callback-on-non-consumption changes
`send`'s public error type and every caller/test; a post-append-returns-Ok variant
still rests on the fragile early-vs-post-append reasoning). `SendRequest` carries
the `Copy` `RecordCallbackTarget` so the task can rebuild the guarded callback.
Proven by `test_record_callback_fires_exactly_once_across_shared_guard`, which fails
(count 2) with the guard neutralized. (The post-append trigger needs cached
metadata + a transactional producer, unreproducible broker-free, so the guard
mechanism is tested directly — the two callbacks it builds are exactly the batch's
copy and the task's re-fire.) A residual pre-existing *zero-fire* leak — if
`accumulator.append` itself returns an `ApiException` error that drops the callback
(`do_send_bytes:1143` returns `Ok(failed_future)`) — is orthogonal, predates B1,
and the guard neither creates nor worsens it.

**B2 — flush/close overtook records still queued on the submission channel.** The
barrier (`drain_submitted_sends`) was wired only into the five control functions.
`Producer_flush`/`Producer_close` and `flush_or_close_async` returned before the
channel drained, so `send_async(r)` then `flush` reported success with `r` unsent
(breaks Java's `flush` contract, which blocks until every prior send completes),
and with `close` it was silent record loss. All three now drain first — `Submit`
semantics (hand the records over), because Java's `flush` blocks and `close`
flushes by default. The async twin awaits the drain's `.await` core rather than
nesting a `block_on`. Only the flushing `close` is exposed; a hypothetical
`close(Duration::ZERO)` (Java discards) is not, and if added later must `Discard`.

*Round-7 correction — discard-window scoping and memory ordering.*

> **ELIMINATED (CFFI round).** Both defects below lived entirely inside the abort
> **discard window**, which no longer exists: `abort_transaction` does not discard the
> outbox itself and there is no window to scope or order. Deleting the machinery
> removes this finding at the root rather than hiding it — see the Decision-update
> section below. The `flush`/`close` drain that this round-7 work also touched
> survives, and it uses only the plain untagged `Barrier` (no discard, no
> `ends_discard`), so neither defect can recur there.
>
> Note (PR #168): that same plain drain is now also run before `abort_transaction`
> (and the other control ops) so returned async sends are handed to the producer and
> discarded *by the producer's* accumulator abort — still no FFI-side discard window,
> so the two defects remain impossible. See "## Decision update 2".

Wiring
flush/close into the barrier exposed two defects in the abort discard window,
which had been a single handle-wide flag any barrier reset:

  - **Scoping (EOS).** A concurrent flush/close pushes its *own* barrier, and
    since flush is not under `txn_control_busy` it can interleave with an
    in-progress abort as `[R0, Barrier_flush, R, Barrier_abort]`. The flush
    barrier reset the window early, so `R` — a record of the aborting transaction
    still ahead of the abort barrier — was **produced** instead of failed. Fix:
    the `Barrier` variant carries `ends_discard`, `true` only for the aborting
    call's barrier; the loop clears the window only on that one, so no other
    barrier can end it. `test_only_abort_barrier_clears_discard_window` pins it
    (fails when reverted to "any barrier clears").
  - **Ordering (ARM only).** The abort's `store(true)` synchronized only with the
    barrier it pushed, not with an already-queued record whose recv synchronizes
    with its own `send_async`, so the loop's per-record `load` could read a stale
    `false` and produce the record. The flag's store and load are now `SeqCst`, so
    a record whose `send_async` is ordered-before the abort is reliably seen as
    to-be-discarded. A record *genuinely concurrent* with the abort stays ambiguous
    — as in Java, where a `send` racing `abortTransaction` may or may not be in the
    transaction; fully removing that would require holding records until the txn
    resolves (a larger redesign), which is noted, not done.

**B3 — `Producer_destroy` use-after-free — now fixed** (not deferred). Mechanics of
the bug: `let ProducerHandle { .. } = *handle` moved fields to the stack and freed
the box while tasks still dereferenced the pointer; `drop(submit_tx)` did not stop
an iteration past `recv()`; `ProducerKind::Kafka` stored `KafkaProducer` **inline**
so the move invalidated derived references; and `drop(kind)` dropped the producer
before the runtime, which *cancels* tasks rather than joining them. Fix, mirroring
`consumer.rs`:
  - `ProducerKind::Kafka` now boxes the producer, so the destroy move-out no longer
    relocates the allocation references point into.
  - Destroy shuts the runtime down **first**, then frees the box. A blocking
    runtime drop joins the worker threads — the submission task, the per-op tasks
    and the producer's own sender task have all stopped touching the producer by
    the time it returns — *then* the box is freed. This is the blocking join
    `shutdown_background` would skip; `consumer.rs` uses the background form, which
    is why the note there about "no longer aliased" is only approximately true —
    the producer takes the stronger blocking form.
  - *Round-7 correction — field-move ordering.* The runtime lives inside `kind`,
    and the first round-6 attempt destructured `*handle` (moving **all** fields to
    the stack and freeing the box) *before* shutting the runtime down. In that
    window the still-running tasks dereference the handle's own fields — `inner`,
    `queued_sends`, `discard_queued_sends`, `completion_tx` — through
    `&*(ptr as *const ProducerHandle)`, so the move-out was a latent
    use-after-free of those fields (benign only because they are `Copy`/POD and the
    bytes were still present). Fix: the runtime is now an `Option` inside `kind`,
    taken through the `kind` mutex (shared access, sound while tasks hold `&`)
    **without moving any field**, and shut down while the box is still intact; only
    once every task has stopped is `*handle` destructured and freed.
  - The two false comments are corrected: destroy no longer claims "dropping the
    runtime waits for the … tasks" via drop-order, and the `ProducerStaticRef` doc
    no longer claims teardown joins the tasks (it does not — the runtime shutdown,
    not any `JoinHandle`, provides the ordering).

  A `send_async`-then-`destroy` test (`test_send_async_then_destroy`, 50 iterations
  with a pending send) exercises the window. It cannot *prove* the absence of UB;
  Miri would, and none is configured in CI — noted, not added.

**B4 — fenced / FATAL_ERROR errors reported neither fatal nor abortable.**
`TransactionManager::maybe_fail_with_error` stamped `txn_requires_abort` when the
manager was in `ABORTABLE_ERROR` but left the `FATAL_ERROR` branch unstamped, so a
producer-fenced / transactional-id-authorization / invalid-pid error reported
`is_fatal() == false` and `txn_requires_abort() == false`. A C caller reading the
documented decision tree ("not abortable, not a timeout") retries forever against a
producer that can never recover. Fix: stamp `is_fatal` in that branch, by **state**
rather than by a hardcoded error-code set —

```rust
if self.has_abortable_error() {
    Err(error.with_txn_requires_abort())
} else {                       // has_error() ⟹ abortable or fatal
    Err(error.with_fatal())
}
```

`has_error()` is `abortable || fatal`, and the function returns early otherwise, so
the `else` is exactly the fatal state. Stamping by state is the faithful mirror of
Java, whose state machine already classified the error when it transitioned
(`maybe_transition_to_error_state` is the code that drives `FATAL_ERROR`:
`ClusterAuthorizationFailed`, `TransactionalIdAuthorizationFailed`, `ProducerFenced`,
`UnsupportedVersion`, `InvalidProducerIdMapping`) — so it neither blanket-stamps nor
duplicates that classification. `fatal` and `requires_abort` stay disjoint, as
librdkafka keeps them. `with_fatal` is a consuming builder symmetric to
`with_txn_requires_abort`; the string-payload variants (`IllegalState`, ...) carry
no base to hold the flag and pass through unchanged — in the fatal path that is only
the poisoned-invalid-transition `IllegalState`, already non-retriable and whose
message says the producer must be closed.

**Consistency with rules §9:** rule 9 forbids inventing typed error structs and
requires preserving Java's subtype dispatch relations. Setting the `fatal` bool that
`KafkaGenericError` already carries does neither — it is the existing model, not a
new abstraction — so this is consistent.

**Test impact:** zero existing assertions changed; all 109 `transaction_manager`
tests and the `kafka_error` tests still pass. No test asserted the old non-fatal
behavior (none called `is_fatal()` on a `maybe_fail_with_error` result), so nothing
encoded the bug. The faithful fence test `test_producer_fenced_for_init_producer_id`
(the translation of Java's `verifyProducerFencedForInitProducerId`) was *strengthened*:
it already drove a transactional manager to fenced-fatal and called all four control
methods asserting the message; it now also asserts each surfaced error
`is_fatal() && !txn_requires_abort()`. Confirmed to fail against the pre-fix core
(error surfaced unstamped), so it is real regression coverage.

**Separability:** B4 touches only `transaction_manager.rs` and `kafka_error.rs`
(the `with_fatal` builder) — no FFI file — so it is a clean stand-alone commit-scope
cherry-pickable to the parent transactions branch.

## Decision update: async-in-transaction is unsupported (UB); machinery deleted

> **REVERSED (PR #168 review, emasab).** Async sends inside a transaction are now
> **supported** again — but *not* by bringing back the deleted machinery this section
> describes. Instead every transaction-control op drains the submission queue before it
> runs (`with_txn_control` calls `drain_submitted_sends_via`), exactly as `flush`/`close`
> already do, so a `send_async` that had returned before the control call is handed to
> the producer and then committed (commit) or discarded (abort) by the producer's own
> accumulator handling. The `Submit`/`Discard`/`discard_queued_sends`/`ends_discard`
> machinery stays deleted. See "## Decision update 2" below for what the code does now;
> this section is retained as the history of the intermediate decision.

**Decision (user-directed, CFFI round).** `send_async` / `send_batch_async` are
**not supported inside a transaction**. A transactional producer must use the
synchronous `send()` / `send_batch()`, which register the record before returning.
This is **documented, NOT enforced** by a runtime guard, and the machinery that had
made async-in-transaction limp along was **deleted**.

**Why document, not enforce.** It is an obvious usage error with an obvious correct
alternative. A `send_async`-rejecting guard was judged unnecessary — the only thing
it would have protected was the transaction↔outbox ordering mechanism, and that
mechanism is gone. So calling `send_async` between `begin_transaction` and
`commit`/`abort` is now genuine **undefined behavior**: with no ordering against the
control calls, a queued record may be **published despite an abort**, or **lost /
rejected despite a commit**. The docs say exactly that and steer callers to the
synchronous path; `.claude/rules/producer-transactions.md` §13 records the rule.

**Symbols deleted from `src/ffi/producer.rs`:**

  - `ProducerHandle::discard_queued_sends` (the abort discard-window flag), its
    initialiser, its `Producer_destroy` destructure entry, and the
    submission-loop `SeqCst` load that failed queued records with
    `KafkaError::transaction_aborted()`.
  - `enum QueuedSends { Ignore, Submit, Discard }` in full — with commit no longer
    draining and abort no longer discarding, all three variants were dead.
  - The `ends_discard` field of `SubmitRequest::Barrier`, and the submission-loop
    branch that cleared the window on it.
  - `fn drain_submitted_sends(handle, discard)` — the transactional wrapper whose
    only caller was `with_txn_control`; the `discard`-side store/reset logic went
    with it.
  - The `queued: QueuedSends` parameter of `with_txn_control`, and the
    drain-before-`op` block inside it. All five control functions now call
    `with_txn_control(producer, op)` and none touches the outbox.

**Kept (non-transactional async path — do not break):**

  - `SubmitRequest::Barrier { ack }` (now a plain, untagged marker),
    `drain_submitted_sends_via` / `drain_submitted_sends_await` (with the
    `ends_discard` parameter removed), `ProducerHandle::queued_sends`, and
    `QueueDepthGuard`. `Producer_flush` / `Producer_close` (and their `_async`
    twins) still drain the outbox so a `send_async` on a **non-transactional**
    producer completes before flush/close returns — Java's `flush` blocks until
    prior sends complete and `close` flushes by default. Covered by
    `test_flush_drains_async_queued_send` / `test_close_drains_async_queued_send`
    and the three `drain_submitted_sends_via` unit tests.

**Finding disposition:**

  - The **flush/abort discard-window** finding (round-7: EOS scoping via
    `ends_discard`, ARM ordering via `SeqCst`) is **eliminated**, not hidden: there
    is no discard window any more, so the two defects it fixed cannot arise. Both
    the B2 round-7 note and "the second design question" section above carry a
    superseding banner.
  - The **`Producer_destroy` drops in-flight callbacks** item is **independent and
    remains a known item**: non-transactional producers still have an outbox, so a
    `send_async` still in flight when `Producer_destroy` shuts the runtime down can
    have its callback dropped without firing. That is unchanged by this decision
    (it is about the non-transactional outbox, not the transaction machinery) and is
    tracked as before. `test_send_async_then_destroy` still exercises the teardown
    window.

**Tests removed** (they asserted behavior that no longer exists — the escape-
prevention / ordering guarantees of the deleted machinery):

  - C `test_transaction_abort_discards_async_sends` and
    `test_transaction_commit_publishes_async_sends`
    (`bindings/c/tests/test_mock_producer.c`) — the `begin → send_async → abort` /
    `begin → send_async → commit` ordering tests.
  - Rust `test_only_abort_barrier_clears_discard_window`
    (`src/ffi/producer.rs`) — pinned the `ends_discard` discard-window scoping.

  The supported paths (`begin → send() → commit`, `begin → send() → abort`, e.g.
  `test_transaction_commit_publishes_records`,
  `test_transaction_abort_discards_records`,
  `test_transaction_commit_flushes_pending_sends`) and the non-transactional
  flush/close drain tests are kept.

## Decision update 2: async-in-transaction re-supported via the `flush`/`close` drain (PR #168)

**Decision (PR #168 review, emasab).** The prior decision above is **reversed**:
`send_async` / `send_batch_async` **are supported inside a transaction**. The reviewer's
direction was that they "need to call `drain_submitted_sends_await` in `with_txn_control`
as in `flush_or_close_async` … The contract is that when you call commit, abort or flush
all sends that had returned (not called) must be included in the operation."

**What changed in the code — one addition, nothing revived.** `with_txn_control`
(`src/ffi/producer.rs`) now calls `drain_submitted_sends_via(&handle.queued_sends,
&handle.submit_tx, &runtime)` immediately after cloning the runtime handle and before
running `op`. That is the exact drain `Producer_flush` / `Producer_close` already use on
the sync path, and the `.await` twin `flush_or_close_async` uses on the async path. It
pushes a FIFO `Barrier` behind everything already queued and waits for it, so every
`send_async` that had *returned* before the control call is handed to the producer via
`producer.send()` first. On a drain error the control op returns that error, and the RAII
`TxnControlGuard` still releases `txn_control_busy`.

All five control ops route through `with_txn_control`, so all five drain. Draining an
empty queue is a single atomic load (the normal case, since blocking sends never queue),
so `init` / `begin` / `send_offsets` pay nothing; `commit` / `abort` get the ordering
that makes async sends part of the transaction.

**Why the plain drain is enough — the deleted machinery stays deleted.** The
`QueuedSends::{Ignore,Submit,Discard}` directive, the `discard_queued_sends` window, and
the `ends_discard`-tagged `Barrier` (all deleted in the Decision-update above) are **not**
reintroduced. Once the drain hands a queued record to `producer.send()`, the producer's
own commit/abort logic — the very code the synchronous send path exercises — does the
Java-faithful thing: `commit_transaction` includes the record, `abort_transaction`
discards it (on `MockProducer`, the record lands in `uncommitted_sends`, which commit
moves to `sent` and abort clears; on the real producer, the accumulator is committed or
aborted). So there is nothing left for a per-operation directive to do. No
`send_async`-rejecting guard is added either.

**Deliberate boundary (unchanged from `flush`/`close`).** Only sends that had *returned*
before the control call are covered. A send racing *concurrently* on another thread is
not ordered against the control op — the same ambiguity Java has for a `send` racing
`commitTransaction`, and the same boundary `drain_submitted_sends_await` already
documents.

**Abort now waits for the drain.** Unlike the intermediate `Discard` design (which failed
queued records without producing them, specifically so abort never blocked on the broker),
abort now drains like commit: it waits for already-queued sends to be handed over before
discarding them. This is the behavior the reviewer asked for ("commit, abort or flush …
all sends that had returned must be included in the operation") and matches `flush`/`close`
exactly. The escape hatch for a wedged broker is unchanged in spirit — a queued send's
handover is bounded by `max.block.ms`, the same total the application would have spent in
Java's blocking `send()` before reaching `abortTransaction`.

**Tests re-added** (replacing the ones the Decision-update removed, but asserting the drain
rather than the deleted ordering machinery):

  - C `test_transaction_commit_drains_async_queued_send` and
    `test_transaction_abort_drains_async_queued_send`
    (`bindings/c/tests/test_mock_producer.c`) — `begin → send_async → commit` leaves the
    record in the committed history (count 1); `begin → send_async → abort` discards it
    (count unchanged). Both are deterministic because the drain makes the handover happen
    before the control op; without the drain they would be racy/failing.
  - Rust `test_with_txn_control_drains_before_commit` and
    `test_with_txn_control_drains_before_begin` (`src/ffi/producer.rs`) — a control op run
    against a handle whose submission task is gone and whose `queued_sends` is non-zero
    returns the drain's `IllegalState` "send-submission task has stopped" error, proving
    the drain runs (and its failure is surfaced) before `op`.

  Both `flush`/`close` drain tests and the synchronous-send transaction tests
  (`test_transaction_commit_publishes_records`, `test_transaction_abort_discards_records`,
  `test_transaction_commit_flushes_pending_sends`) are kept unchanged.

## Known issues and deferred follow-ups

### A guard rejection is not programmatically distinguishable from C

`KafkaError::concurrent_modification` carries no protocol code — `code()` returns
`-1`, identical to `IllegalState` and to a genuine broker `UNKNOWN_SERVER_ERROR`.
So a C caller cannot tell "you called two control methods concurrently" (a
caller-sequencing bug; the transaction is untouched) from "the transaction failed"
(may require an abort) except by matching the message string, which is what the C
test has to do. `bindings/python/consumer.py` already hardcodes that string for the
consumer guard, so the message is effectively load-bearing API.

Mitigated but not solved here: all five functions now document the rejection, its
exact message, and that it is **not** a reason to abort — previously only
`init_transactions` mentioned it, so a caller reading `commit_transaction` saw no
third case and might abort a healthy transaction.

Not solved because every clean option changes shared surface and deserves its own
decision rather than being decided inside a producer-transactions change:

1. **A dedicated accessor** — `kafka_common_KafkaError_is_concurrent_modification`,
   next to `_is_retriable` / `_is_fatal` / `_txn_requires_abort`. Cheapest, purely
   additive, and it would also give the consumer guard and the Python binding a
   real predicate instead of a string match. This is the recommendation.
2. Give `ConcurrentModification` a distinct synthetic code. Cheap to read from C,
   but it invents a code in the protocol's namespace and changes what `code()`
   returns for the existing consumer guard.
3. Reuse a real protocol code. Rejected: it would collide with a genuine broker
   error and break `txn_requires_abort`-style dispatch.

### Post-teardown inline callback can panic on a tokio worker

`enqueue_or_run_inline` (`common.rs`) runs the C callback **inline on the calling
thread** when the dispatcher is already gone (post-teardown). For the
submission/per-op tasks that thread is a tokio worker, so a user callback that
re-enters a `block_on`-based FFI call (`FutureRecordMetadata_get`,
`commit_transaction`, `flush`, …) hits tokio's "cannot start a runtime from within
a runtime" panic, which unwinds across the `extern "C"` boundary and aborts.
Narrow — it needs the dispatcher already torn down *and* a re-entrant blocking
callback — and documented at the call site. **Proposed** (not applied
unilaterally, per the reviewer's guidance): guard the `block_on` entry points with
`tokio::runtime::Handle::try_current().is_err()` and return a "cannot call a
blocking producer method from within a delivery callback" error instead of
blocking. That touches every blocking entry point and changes their error surface,
so it is its own reviewed change.

### The `kind: Mutex<ProducerKind>` is removable (Part A investigation, deferred)

The user asked whether the `Mutex<ProducerKind>` — and the `inner`/`runtime`
caches, the `OnceLock`, `producer_inner`/`producer_static_ref`, and the
blocking-send serialization it forces — can be deleted by making `kind` a plain
immutable field. The investigation says **yes**, and it is deferred to its own PR:

  - **No lock site mutates through the guard.** All ~15 `.lock()` sites take
    `&*guard`; every method reached (`send`/`flush`/`close`/`partitions_for`, the
    `MockProducer` drivers, `ProducerKind::runtime`, `producer_send`) is `&self`.
    A `&mut` method would not compile today, so this is compiler-checked.
  - **`KafkaProducer` and `MockProducer` are `Sync`** — compile-proven (an
    `assert_sync::<…>()` probe built), and already relied on: the spawned
    submission/per-op futures hold `&'static` refs across `.await` on a
    multi-thread runtime, which requires `Sync`.
  - **Nothing outside the FFI touches `kind`/`ProducerKind`/`ProducerHandle`** —
    all three are private to `src/ffi/producer.rs`.
  - Removing it would delete the mutex, both caches, the `OnceLock`, the two
    `producer_*` helpers, the "populate before publish / provenance" dance, the
    unreachable "not initialized" branches, and — the real payoff — the blocking-
    send serialization the module doc flags as a defect (finding C2), since `send`
    would no longer hold a lock across `block_on`.
  - **Risk: low**, and it is compatible with the boxed `Kafka` variant this change
    already introduced. Deferred only because it is a broad (~15-site) diff that
    deserves its own review, not folded into a bug-fix round. The `consumer.rs`
    single-owner guard is a *different* case (its `Consumer` is `!Sync`) and is not
    affected.

## Deliberately not done

- **`producer.py`, the two gRPC servers, and the multilanguage scenarios.**
  §9.6's remaining scope; this pass is the C layer, which is what unblocks them.
- **`_async` variants and their callback typedefs** — decision 1.
- **KIP-939 two-phase commit** (`prepare_transaction`, `complete_transaction`,
  `prepared_txn_state`, `is_prepared`). Not part of the five methods in scope.
  Nothing exposed here misbehaves in a prepared transaction: `begin_transaction`
  and `send` already call `throw_if_in_prepared_state`, and `commit`/`abort` are
  exactly the operations Java permits while prepared.
- **The four other `MockProducer` error hooks** — decision 5.
- **Any change to the consumer access guard, or to `send`/`send_async`.** The
  consumer guard is a different contract for a different reason; `send` staying
  unguarded is a requirement, not an omission.
