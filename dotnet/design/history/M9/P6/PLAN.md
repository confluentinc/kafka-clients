# M9/P6 — `IConsumerRebalanceListener` (.NET consumer callback parity)

**Status:** **ACTOR BRIEF — dispatch-ready.** Derived from the approved roadmap
`design/current/PLAN-M9-consumer-callback-parity.md` (approved in full
2026-08-29, all nine recommendations accepted). **Three P6-local decisions in §9
need a maintainer ruling before the Actor starts** — they are not Actor judgment
calls.

**Agent number:** **N=59.** **Milestone/Phase:** M9/P6. **Mode: A** (C# only).
**Branch:** `prashah_dev_dotnet_binding_consumer`, on top of `1a6c13f8` (M9/P5).
**Risk: medium-high** — the multi-shot `GCHandle` lifetime has no precedent in
this binding.

**Prerequisite — SATISFIED.** `cargo build --features ffi` was run 2026-08-29;
`target/include/confluent_kafka.h` (19:12, 159 563 B) carries all P6 symbols, and
`target/debug|release/libconfluent_kafka.dylib` are current. ⚠ *Not* satisfied for
P9: the Linux cross-builds are still stale. Not P6's problem.

---

## 1 · Scope

Java's `ConsumerRebalanceListener` and the `subscribe(topics, listener)` overload,
plus `MockConsumer.rebalance` as the deterministic broker-free driver for it.

### In scope

1. `IConsumerRebalanceListener` — new public interface.
2. `Subscribe(topics, listener)` on `IConsumer` + `IAsyncConsumer`, and all four
   implementations.
3. `MockConsumer<K,V>.Rebalance(...)` + `AsyncMockConsumer<K,V>.Rebalance(...)` —
   inherent mock helpers, **not** on the interfaces.
4. Six `NativeMethods` P/Invoke declarations (§3).
5. Three rooted Cdecl trampolines + the registration lifetime machinery (§5).
6. Tests per §7.

### Explicitly OUT of scope — do not touch

| Out | Belongs to |
|---|---|
| `IOffsetCommitCallback`, any `CommitAsync` overload, `Consumer_commit_async_*_with_callback` | **P7 (N=60)** |
| `ConsumerHandle`, `Consumer_handle`, any `ConsumerHandle_*` P/Invoke, in-callback reentrancy | **P8 (N=61)** |
| `bindings/dotnet/grpc-server/**`, `CallbackLog`, `LoggingRebalanceListener`, `Subscribe.with_listener` server handling, `GetCallbackLog` | **P9 (N=62)** |
| Anything under `src/` (Rust), `src/ffi/`, `cbindgen.toml`, `tests/` | Not this phase; Mode A |
| Pattern subscribe (`subscribe_pattern*`) | Not in the ABI; out of the whole roadmap |
| The producer (`Producer_send_with_callback`) | No .NET producer on this branch |

**Mode-A proof obligation:** `git diff <base> -- src/ cbindgen.toml tests/` must be
**empty** at close. State it in the final commit.

---

## 2 · Deliverables, with file paths

| # | Deliverable | Path |
|---|---|---|
| D1 | `IConsumerRebalanceListener` | `src/Confluent.Kafka/IConsumerRebalanceListener.cs` (new) |
| D2 | `void Subscribe(IReadOnlyCollection<string>, IConsumerRebalanceListener)` | `src/Confluent.Kafka/IConsumer.cs` (after `:99`) |
| D3 | `Task Subscribe(IReadOnlyCollection<string>, IConsumerRebalanceListener, CancellationToken = default)` | `src/Confluent.Kafka/IAsyncConsumer.cs` (after `:107`) |
| D4 | Four forwarders | `KafkaConsumer.cs`, `AsyncKafkaConsumer.cs`, `MockConsumer.cs`, `AsyncMockConsumer.cs` |
| D5 | `Rebalance(IReadOnlyCollection<TopicPartition>)` ×2 | `MockConsumer.cs` (near `:221-280`), `AsyncMockConsumer.cs` (near `:213-278`) |
| D6 | 6 P/Invokes | `src/Confluent.Kafka/Internal/Interop/NativeMethods.cs` |
| D7 | 3 trampolines + delegate types | `src/Confluent.Kafka/Internal/Interop/ConsumerCallbacks.cs` |
| D8 | Registration state + `SubscribeWithListener` / `Rebalance` | `src/Confluent.Kafka/Internal/NativeConsumer.cs` |
| D9 | Tests | `tests/Confluent.Kafka.UnitTests/` |
| D10 | Doc-sync: `bindings/dotnet/CLAUDE.md` §3 "Still to come" (drop the listener), and the §4 amendment sanctioned under roadmap Q5/Q6 | `bindings/dotnet/CLAUDE.md` |

**D10 is a deliverable, not a nicety.** M9/P5's single Critic finding was exactly a
silently-dropped in-scope doc refresh. Do not repeat it.

---

## 3 · The ABI surface — exact signatures

All line numbers are `target/include/confluent_kafka.h` as regenerated 2026-08-29,
cross-checked against `src/ffi/consumer.rs`.

### 3.1 Callback typedefs

```c
/* h:222 */ typedef kafka_common_KafkaError_t *(*..._on_partitions_revoked_callback_t)(
                kafka_consumer_TopicPartitionList_t*, void*);
/* h:235 */ typedef kafka_common_KafkaError_t *(*..._on_partitions_assigned_callback_t)(
                kafka_consumer_TopicPartitionList_t*, void*);
/* h:550 */ typedef kafka_common_KafkaError_t *(*..._on_partitions_lost_callback_t)(
                kafka_consumer_TopicPartitionList_t*, void*);
/* h:605 */ typedef void (*kafka_consumer_ConsumerRebalanceListener_user_data_destroy_t)(void*);
```

C# delegate shape for all three (identical):
`IntPtr Handler(IntPtr partitions, IntPtr userData)`, with
`[UnmanagedFunctionPointer(CallingConvention.Cdecl)]`.

### 3.2 Functions

```c
/* h:2073 */ kafka_consumer_ConsumerRebalanceListener_t *
kafka_consumer_ConsumerRebalanceListener_new(
    ..._on_partitions_revoked_callback_t   on_partitions_revoked,   /* required */
    ..._on_partitions_assigned_callback_t  on_partitions_assigned,  /* required */
    kafka_common_KafkaError_t *(*on_partitions_lost)(kafka_consumer_TopicPartitionList_t*, void*), /* NULLABLE */
    void *user_data,
    void (*user_data_destroy)(void*));                              /* NULLABLE */

/* h:2095 */ void kafka_consumer_ConsumerRebalanceListener_destroy(
                 kafka_consumer_ConsumerRebalanceListener_t *listener);

/* h:2139 */ kafka_common_KafkaError_t *kafka_consumer_Consumer_subscribe_with_listener(
    const kafka_consumer_Consumer_t *consumer,
    const char *const *topics, int32_t count,
    kafka_consumer_ConsumerRebalanceListener_t *listener);

/* h:2156 */ void kafka_consumer_Consumer_subscribe_with_listener_async(
    const kafka_consumer_Consumer_t *consumer,
    const char *const *topics, int32_t count,
    kafka_consumer_ConsumerRebalanceListener_t *listener,
    kafka_consumer_Consumer_op_callback_t callback, void *user_data);

/* h:1257 */ kafka_common_KafkaError_t *kafka_consumer_MockConsumer_rebalance(
    const kafka_consumer_Consumer_t *consumer,
    const char *const *topics, const int32_t *partitions, int32_t count);

/* h:638 */  kafka_common_KafkaError_t *kafka_common_KafkaError_new(
    int32_t code, const char *message);
```

**Note the two nullable params are spelled inline as raw fn pointers**, not via the
`_t` alias — cbindgen cannot render `Option<Alias>` (phase-3 notes item 1). Declare
them in C# as the delegate type anyway; pass `null` to mean NULL.

**Note `MockConsumer_rebalance` takes parallel arrays** (`topics[]`,
`partitions[]`, `count`) — **not** a `TopicPartitionList_t`. This is the shape the
existing `WithPinnedTopics` helper already produces.

**Every sync declaration takes `SafeConsumerHandle` as the parameter type**, per
the M9/P4 H1 convention (`NativeMethods.cs:1020-1021` is the template) — never
`DangerousGetHandle()`. `subscribe_with_listener_async` is an async submit and
follows the existing manual span-the-op AddRef path instead.

**Set `EntryPoint` explicitly** on all six or you get a runtime
`EntryPointNotFoundException` (`ffi-marshalling.md §0.1`).

### 3.3 Ownership contracts — quoted from the header, non-negotiable

**On the delivered partitions list** (`h:196-197`): *"the callback owns the handle
… must destroy it with `kafka_consumer_TopicPartitionList_destroy`, matching the
'callbacks own the handles delivered to them' convention."*
→ **`TopicPartitionListMarshal.CopyOutAndDestroy(IntPtr)`
(`Internal/Interop/TopicPartitionListMarshal.cs:52`) does exactly this already.
Use it. Do not hand-roll.**

**On the return value** (`h:200-207`): *"Return `NULL` for success. Returning a
non-null error handle (built with `kafka_common_KafkaError_new`) is the C
equivalent of the Java listener throwing: ownership of that handle transfers to
the client … **Do not destroy a handle you return.**"*

**On invocation context** (`h:209-221`): *"Invoked on the consumer's callback
dispatcher thread — never on a tokio worker, and never concurrently with another
callback of the same consumer. The rebalance does not proceed until the callback
returns … The callback must **not** call the plain `kafka_consumer_Consumer_*` API
of the consumer being rebalanced (the access guard is held by the app thread
driving the rebalance…); use `kafka_consumer_ConsumerHandle_t` … instead."*
→ The last clause is **P8**. In P6, document the limitation; do not build the handle.

**On listener-handle ownership** (`h:2081-2086`):
`ConsumerRebalanceListener_destroy` is for a listener **never passed to a subscribe
call**. A listener handed to either `subscribe_with_listener*` **has already been
consumed — destroying it afterwards is a double free.** Consumption is
**unconditional, including on the error path** (phase-3 notes item 2: the adapter
is built before any fallible step, so every early return drops it and fires the
destroy hook).

---

## 4 · Parity anchors — review against these

### 4.1 Python (`bindings/python/consumer.py`)

| P6 deliverable | Python anchor | Cite |
|---|---|---|
| listener contract | duck-typed: `on_partitions_revoked` + `on_partitions_assigned` **required**, `on_partitions_lost` **optional** (delegates to revoked, Java's default) | `:263-281`, `:284-292` |
| partitions payload | each method receives a `list[TopicPartition]` | `:307-309` |
| throw semantics | exception propagates to the trampoline → becomes a `KafkaError` returned to Rust → the rebalance **and the driving operation** fail with that message | `:275-280`; observable contract pinned by phase-6 notes item 6: **code `-1`, `str(exc)` verbatim** |
| `subscribe(topics, listener=None)` | `listener=None` → plain subscribe **and drops the stored adapter** | `:773-788`, `:962-985` |
| registration lifetime | released by a **replacing** `subscribe*` (incl. a listener-less one) or consumer destroy; **NOT** by `unsubscribe()` | `:977-981`; phase-5 notes item 1; phase-6 notes item 5 |
| keep-alive mirror | `self._listener_adapter` strong ref, cleared in `_destroy()` | `:578-582` |
| `MockConsumer.rebalance` | see §6.2 for the semantics it pins | `:868-886` |

### 4.2 C (`bindings/c/tests/test_consumer_callbacks.c`)

Use it for the **`user_data` lifetime** question only (§9, P6-D3). C keeps its
listener state alive for the session and passes `user_data_destroy == nullptr`
(`bindings/c/grpc_server/server.cc:253-288`) — but that was a fix for a
*commit/delivery* dispatcher-job UAF (`9465e197`), and the listener path may not
share it. **Verify before copying.**

---

## 5 · The lifetime rule — write this down before writing code

This is the phase's whole risk. Every existing trampoline in this binding is a
**one-shot per-operation completion** whose `GCHandle` the callback itself frees
(`OperationCompletionSource.cs:67-80`, `:254-266`). **That invariant does not
transfer.** A rebalance listener is:

- **multi-shot** — fires N times per registration;
- bound to a **subscription**, not an operation;
- **not** the owner of its own `GCHandle` free.

### 5.1 The registration state machine

```
Subscribe(topics, listener)
  ├─ alloc GCHandle over a managed ListenerRegistration (the .NET-side context)
  ├─ ConsumerRebalanceListener_new(revoked, assigned, lost?, GCHandle.ToIntPtr(g), destroyHook?)
  │     → listener handle; ownership is now the ABI's
  ├─ Consumer_subscribe_with_listener[_async](..., listenerHandle)
  │     → CONSUMES the handle unconditionally, success or failure
  └─ store the registration in NativeConsumer  ← the managed keep-alive mirror
        │
        ├── fire ×N  (dispatcher thread): CopyOutAndDestroy(list) → user method
        │                                 → NULL | KafkaError_new(-1, msg)
        │
        └─ released by: a REPLACING subscribe* (with or without a listener)
                     OR consumer destroy
           NOT released by: unsubscribe(), close()
```

### 5.2 The three delegate instances

`static readonly` fields in `ConsumerCallbacks.cs` (the established rooting
pattern — `:60`, `:130`, `:222`, `:280`, `:334`, `:406`, `:466`). **Not** per-call
lambdas. `[UnmanagedCallersOnly]` / `delegate* unmanaged` /
`Marshal.GetFunctionPointerForDelegate` are **forbidden** by the netstandard2.0
floor (`ffi-marshalling.md §0.1`).

### 5.3 The single sanctioned free site

Exactly **one** site frees the registration `GCHandle`. Which site is **P6-D3**
(§9) — either the `user_data_destroy` hook, or `NativeConsumer` teardown. Whichever
is chosen:

- it is the **only** place `GCHandle.Free()` is called for a registration;
- it must be idempotent-safe (a second free is a hard crash);
- a **replacing** subscribe must release the previous registration through that
  same site — never a second one;
- `Unsubscribe()` / `Close()` must **not** release it (Java-faithful).

### 5.4 Trampoline body — mandatory shape

```
try {
    partitions = TopicPartitionListMarshal.CopyOutAndDestroy(partitionsPtr);  // owns + frees
    registration = recover from GCHandle(userData);      // do NOT free here
    registration.Listener.OnPartitionsX(partitions);
    return IntPtr.Zero;                                  // success
}
catch (Exception e) {
    return NativeMethods.KafkaErrorNew(-1, e.Message);   // ownership transfers; do NOT destroy
}
```

**No exception may escape** — the frame above is Rust (`ffi-marshalling.md §B6`;
there is no caller frame to catch it, so an escape is UB). The `catch` must be
last-resort-total, including the `CopyOutAndDestroy` call, and must itself not
throw (guard against `e.Message` being null; `KafkaErrorNew` needs a pinned UTF-8
string — `Utf8Marshal.Pin`, §B3).

### 5.5 Anti-patterns a Critic will look for

- A per-subscribe delegate instance, or an inline lambda, passed to native.
- `GCHandle.Free()` in the trampoline (that is the one-shot pattern; wrong here).
- `TopicPartitionList_destroy` **and** `CopyOutAndDestroy` on the same handle.
- Destroying the listener handle after a subscribe call consumed it.
- An exception path that skips the partitions-list destroy (leak) or returns a
  handle it also destroyed (double free).
- Releasing the registration on `Unsubscribe()`/`Close()`.
- `DangerousGetHandle()` on a sync declaration (M9/P4 H1).

---

## 6 · Behaviors to pin

### 6.1 Sync listener (roadmap Q6 — SETTLED, do not reopen)

`IConsumerRebalanceListener` methods are **`void`, not `Task`-returning.** The ABI
callback is a synchronous C function pointer returning `KafkaError*` and the
rebalance blocks on it. Both Python servers use plain sync methods for exactly this
reason (a coroutine listener parks the dispatcher thread in
`run_coroutine_threadsafe(...).result()`, a documented deadlock source).

`bindings/dotnet/CLAUDE.md` §3's idiom-map row says "(async)" — that row describes
the **Rust core's** trait, which the C ABI has already flattened
(`bindings/CLAUDE.md §1.2`). **This is roadmap divergence D1; the sanctioned
wording is in §8.** Update the §3 row in D10 rather than leaving the contradiction.

### 6.2 `MockConsumer.Rebalance` semantics — pin every one in a test

From `src/ffi/consumer.rs:1372-1397`, phase-5 notes item 4, phase-6 notes item 6:

1. Requires an **`AutoTopics` subscription**. A manually-`Assign`ed consumer fails
   with **`"manual assignment in use"`** — assert that exact text
   (`definition-of-done.md §3`).
2. Fires `on_partitions_revoked` **only when something was removed**.
3. Fires `on_partitions_assigned` **unconditionally while a listener is
   registered** — with the **added** list (possibly **empty**), *not* the full
   assignment.
4. **Never** fires `on_partitions_lost`. → the Java-default `lost → revoked`
   delegation is **unreachable from the mock**; test it against the adapter
   directly (Rust does the same, `src/ffi/consumer.rs` `#[cfg(test)]`).
5. **Does not return until the callbacks have returned**, and propagates a
   callback error as its own return value. → this is what makes §7's blocking test
   real.

### 6.3 What P6 cannot do, and must say so

A listener **cannot call back into its own consumer** — the plain `Consumer_*` API
is rejected with `ConcurrentModification` while the app thread holds the guard.
The sanctioned path is `ConsumerHandle`, which is **P8**. Put this in the
`IConsumerRebalanceListener` XML doc with a forward reference, so a user hitting
it finds an explanation rather than a bug report. This is the deferral of
`consumer-threading.md` §31 test #1 (roadmap §6.3) — it is **recorded**, not
skipped.

---

## 7 · DoD and test obligations

### 7.1 Gates — ⚠ CORRECTED

Roadmap §6.4 replaced the stale gate text. **`cargo xtask lint` already runs a
second `--workspace --all-targets --all-features` pass**
(`xtask/src/main.rs:154-167`, since `4fd1435f` / #139, 2026-08-05). **Do not add a
separate `cargo clippy --features integration-tests,multilanguage-tests`
invocation** — it is redundant.

P6 is Mode A, so the Rust gates are only a no-regression check:

| Gate | Command |
|---|---|
| Build the native (already current) | `cargo build --features ffi` |
| .NET build, TFM matrix | `dotnet build` (net462-via-ns2.0 / net8.0 / net10.0) |
| .NET tests — **the execution gate** | `dotnet test -f net10.0` |
| .NET format | `dotnet format --verify-no-changes` |
| Rust no-regression | `cargo xtask lint`, `cargo test --all-features -- --skip __grpc` |

`dotnet` is **not** on `PATH`: use `/usr/local/share/dotnet/dotnet` (SDK 10.0.302).
`cargo` is **not** on the default `PATH` either: it is at
`~/.nix-profile/bin/cargo` (Rust 1.95.0). **net8.0 execution is a standing CI-only
gate** (the .NET 8 runtime is absent locally) — build-verify net8.0, execute on
net10.0.

### 7.2 Required tests

Root `definition-of-done.md` #1, #2, #3, #5, #8, #9 apply; #10 is **N/A**
(per-rebalance, not per-record) and must be stated, not skipped; #11 applies
(no `block_on` façade, `IDeserializer` untouched); #12 applies (fixture fidelity).

From `ffi-marshalling.md` §B6 "Tests required":

1. Each of the three callbacks delivers the correct partitions.
2. **Aggressive GC during a live registration does not collect the delegate** —
   mirror `ConsumerCompletionBridgeTests.InFlightOperation_SurvivesAggressiveGc`
   (`tests/…/Interop/ConsumerCompletionBridgeTests.cs:169`).
3. A throwing listener is caught, **does not unwind into native**, and surfaces
   as `KafkaError` **code `-1`** with **`str(exc)` verbatim** as the message —
   assert both, exactly.
4. The partitions handle and the registration `GCHandle` are each freed **exactly
   once** — mirror `FreeGcHandle_CalledTwice_FreesAndReleasesExactlyOnce` (`:187`).
5. `Dispose`/`DisposeAsync` return without hanging with a live registration.

Registration-lifetime tests:

6. A **replacing listener-less** `Subscribe` releases the registration.
7. `Unsubscribe()` **does not** release it. (Both are Java-faithful, phase-5 item 1.)

Marshalling:

8. A **non-ASCII topic name** round-trips through the delivered partitions
   (`ffi-marshalling.md §B3`; guards a `LPStr` mistake).

`consumer-threading.md` §31:

9. **Test #2 — "the rebalance does not advance until the listener returns."**
   `MockConsumer_rebalance` is synchronous (§6.2 item 5), so: block the listener on
   a `ManualResetEventSlim`, drive `Rebalance` from a worker thread, assert it has
   **not** returned, release, assert it has.
   ⚠ **Mutation check required** (phase-5 notes item 3): a "has NOT completed yet"
   assertion passes vacuously if the flag is never set for an unrelated reason.
   Prove the test **fails** when the block is removed, and record that you did.
10. **Test #1 is DEFERRED to P8** with the §6.3 rationale. Record the deferral in
    `COMMENTS.DONE.59.md` — do not let it read as an oversight.

---

## 8 · Divergences in force — sanctioned wording

Use this wording verbatim in code comments / the phase record. Anything **not**
listed is a defect, not a divergence.

- **D1 — sync listener.** *"`IConsumerRebalanceListener` is synchronous, not
  `Task`-returning. The ABI callback is a sync C function pointer returning
  `kafka_common_KafkaError_t*`, and the rebalance blocks until it returns
  (`confluent_kafka.h:209-213`). `bindings/dotnet/CLAUDE.md` §3's `(async)` row
  describes the Rust core's trait, which the C ABI flattens
  (`bindings/CLAUDE.md` §1.2); restoring 'blocks until it returns' faithfully in
  C# means a sync method. Settled as roadmap Q6."*
- **D2 — overload, not an optional parameter.** *"Java has two `subscribe`
  overloads; C# idiom is overloads. Python's `listener=None` is a Python idiom, not
  the Java shape."*
- **D3 — foreign dispatcher thread.** *"Listener callbacks run on the core's
  callback-dispatcher thread, not the caller's task.
  `consumer-threading.md` §31's caller's-task model is the Rust core's contract; the
  C ABI flattens it. Python documents the identical divergence
  (`consumer.py:977-981`)."*

---

## 9 · ⚠ DECISIONS FOR THE MAINTAINER — do not let the Actor choose

### P6-D1 — How does `OnPartitionsLost` get Java's default?

Java's `ConsumerRebalanceListener.onPartitionsLost` has a **default method**
delegating to `onPartitionsRevoked`. Python reproduces it by duck-typing
(`consumer.py:300-305`). The ABI reproduces it by accepting **NULL** for
`on_partitions_lost` (`h:2048-2050`).

C# on the **netstandard2.0 floor has no default interface methods** (net8.0+ only),
so a 3-method interface makes all three **required** — which loses the Java default.

- **(a) Three required methods; always register all three trampolines.** Simplest,
  most explicit. **Loses** the Java default — every implementer must write
  `OnPartitionsLost`, even to delegate. Diverges from Java *and* Python.
- **(b) Three required methods on the interface, plus a public
  `ConsumerRebalanceListenerBase` abstract class** whose `virtual OnPartitionsLost`
  delegates to `OnPartitionsRevoked`. Restores the Java default for anyone who
  wants it; the interface stays implementable directly. Costs one extra public type
  (a §7 DoD #7 "not in Java" justification — though it exists *because* of a Java
  behavior).
- **(c) Two required methods + a separate optional interface**, passing NULL to the
  ABI when the user's listener does not implement it. Closest to Python/the ABI;
  least idiomatic in C# (type-testing a listener at registration time).

**Recommendation: (b).** It is the only option that preserves the Java default
*and* stays idiomatic on the floor. But it adds public surface, which is a
maintainer call.

### P6-D2 — Does the sync `Subscribe` overload use the sync or the async ABI?

Both exist. Python's **sync** `Consumer.subscribe` routes through
`Consumer_subscribe_with_listener_**async**` via `_run_sync`
(`consumer.py:786-788`) — because Python must not hold the GIL across a callback
dispatch. **.NET has no GIL**, and .NET's existing sync `Subscribe` calls the sync
ABI directly (`NativeConsumer.cs:1213-1214`).

- **(a) sync → sync ABI, async → async ABI.** Matches the shipped .NET convention
  and roadmap D-none (no divergence to declare). Diverges from Python's *mechanism*
  — but for a Python-specific reason that does not apply.
- **(b) Both → the async ABI**, mirroring Python literally.

**Recommendation: (a)**, with a one-line comment recording *why* .NET diverges
from Python here (no GIL), so a Critic doing symbol-by-symbol parity does not file
it. Flagged because it is a visible, deliberate parity break.

### P6-D3 — Where is the registration `GCHandle` freed?

The one genuinely new lifetime question (§5.3).

- **(a) `user_data_destroy` hook.** The ABI fires it *"exactly once when the
  registration is released"* (`h:2053-2055`). Bounded — no accumulation across
  repeated `Subscribe` calls. **Risk:** the hook *"may run on any thread"*
  (`src/ffi/consumer.rs:3010`), and the C backend hit a real use-after-free in this
  general area (`9465e197`) because `Consumer_destroy` does **not** join the
  dispatcher.
- **(b) `NativeConsumer` teardown; pass `user_data_destroy = null`.** Mirrors what
  C actually shipped. No cross-thread free race. **Cost:** every superseded
  registration is retained until consumer teardown, so an app that re-subscribes N
  times holds N registrations. Bounded by consumer lifetime, but grows with
  subscribe count.

⚠ **The roadmap's §5.2 leaned toward (b). That lean should be treated as
provisional.** C's UAF was on the *commit/delivery* path, where the callback is
**enqueued as a dispatcher job**; the listener callbacks are invoked
**synchronously by the rebalance, which blocks on them**, so the straggler-job
window may simply not exist here.

**Recommendation: verify, then decide.** Have the Actor first establish, from
`src/ffi/consumer.rs`, whether the listener's `user_data_destroy` can fire
concurrently with — or after — an in-flight listener callback. If it provably
cannot, take **(a)** (bounded, no leak). If the answer is unclear, take **(b)** and
record the retention as an accepted residual. **Do not let the Actor pick silently
either way** — this is the finding a Critic is most likely to escalate, in either
direction.

---

## 10 · Mechanics

- Commit per step with clear messages; `fixup!` referencing the original commit
  when closing a `COMMENTS.59.md` item.
- Review file: `bindings/dotnet/COMMENTS.59.md`; resolved →
  `COMMENTS.DONE.59.md`. **Neither is ever `git add`ed.** The Manager archives
  `COMMENTS.DONE.59.md` to `design/history/M9/P6/` at close
  (`bindings/dotnet/CLAUDE.md` §8.4).
- Personas must be copied to the repo-root `.claude/agents/` to be invocable, and
  those root copies must **never** be committed.
- Required reading before starting — these carry contract facts **absent from the
  header**: `.claude/agent-memory/actor-executor/ffi_callback_bridging_phase{3,4,5,6,7}_notes.md`.
