# M9/P7 — `IOffsetCommitCallback` (.NET consumer callback parity)

**Status:** **ACTOR BRIEF — dispatch-ready.** Derived from the approved roadmap
`design/current/PLAN-M9-consumer-callback-parity.md`. **Three P7-local decisions
in §10 need a maintainer ruling before the Actor starts.**

**Agent number:** **N=60.** **Milestone/Phase:** M9/P7. **Mode: A** (C# only).
**Branch:** `prashah_dev_dotnet_binding_consumer`, on top of `e26e2499` (M9/P6).
**Risk: medium** — the non-nullable-callback asymmetry, and a callback family the
rulebook does not yet describe (§5).

**Prerequisite — SATISFIED.** Header and host natives were rebuilt under roadmap
Q1 and carry every P7 symbol. (Linux cross-builds are still stale — a **P9**
prerequisite only.)

---

## 1 · Scope

Java's `OffsetCommitCallback` and the two `commitAsync` overloads that take one.

### In scope

1. `IOffsetCommitCallback` — new public interface.
2. `CommitAsync` overloads on `IConsumerCommon` covering Java's remaining two of
   three `commitAsync` forms.
3. Two `NativeMethods` P/Invoke declarations (§3).
4. One rooted Cdecl trampoline + the completion-registration lifetime (§5).
5. `bindings/dotnet/CLAUDE.md` doc-sync — **including the maintainer-sanctioned
   §4 amendment, whose exact wording is given in §8**.
6. **`ffi-marshalling.md` section-sync — §B2, §B5, §B6, §B7, itemized in §7.**
7. Tests per §9.

### Explicitly OUT of scope — do not touch

| Out | Belongs to |
|---|---|
| `ConsumerHandle`, `Consumer_handle`, any `ConsumerHandle_*` P/Invoke, in-callback reentrancy | **P8 (N=61)** |
| `bindings/dotnet/grpc-server/**`, `CallbackLog`, `LoggingCommitCallback`, the `CommitAsync` / `GetCallbackLog` RPCs | **P9 (N=62)** |
| `IConsumerRebalanceListener` and anything under `ListenerRegistration.cs` | **shipped in P6** — do not refactor |
| `Commit()` / `Commit(offsets)` (Java `commitSync`) | shipped M5/P6 |
| Anything under `src/` (Rust), `src/ffi/`, `cbindgen.toml`, `tests/` | Not this phase; Mode A |
| The producer | No .NET producer on this branch |

**Mode-A proof obligation:** `git diff <base> HEAD -- src/ cbindgen.toml tests/`
must be **empty** at close (it has held since P5 — keep the streak and state it in
the final commit).

---

## 2 · Deliverables, with file paths

| # | Deliverable | Path |
|---|---|---|
| D1 | `IOffsetCommitCallback` | `src/Confluent.Kafka/IOffsetCommitCallback.cs` (new) |
| D2 | `void CommitAsync(IOffsetCommitCallback callback)` | `src/Confluent.Kafka/IConsumerCommon.cs` (beside `:160`) |
| D3 | `void CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, IOffsetCommitCallback? callback = null)` | same |
| D4 | Four forwarders ×2 overloads | `KafkaConsumer.cs`, `AsyncKafkaConsumer.cs`, `MockConsumer.cs`, `AsyncMockConsumer.cs` |
| D5 | 2 P/Invokes | `Internal/Interop/NativeMethods.cs` |
| D6 | 1 trampoline + delegate type + the discard trampoline | `Internal/Interop/ConsumerCallbacks.cs` |
| D7 | Registration state + `CommitAsyncWithCallback` / `CommitAsyncOffsets…` | `Internal/NativeConsumer.cs` (+ a `CommitCallbackRegistration.cs` if it mirrors `ListenerRegistration.cs`) |
| D8 | **`CLAUDE.md` doc-sync** — §3 `OffsetCommitCallback` idiom row, §3 "Still to come", §3 `IConsumerCommon` sketch, §4 sync-vs-async row + a new §4 divergence note. **Exact wording in §8.** | `bindings/dotnet/CLAUDE.md` |
| D9 | **`ffi-marshalling.md` section-sync — §B2, §B5, §B6, §B7.** Itemized in §7 | `bindings/dotnet/.claude/rules/ffi-marshalling.md` |
| D10 | Tests | `tests/Confluent.Kafka.UnitTests/` |

**D8 and D9 are itemized deliverables.** Both P5 and P6 closed with their only
finding in this exact category (roadmap §5.6). Dropping either silently is a
finding; deferring either **with a recorded rationale** is not.

---

## 3 · The ABI surface — exact signatures

`target/include/confluent_kafka.h` as regenerated 2026-08-29, cross-checked
against `src/ffi/consumer.rs`.

### 3.1 Typedefs

```c
/* h:264 */ typedef void (*kafka_consumer_Consumer_commit_async_callback_t)(
                kafka_consumer_OffsetMap_t*, kafka_common_KafkaError_t*, void*);
/* h:537 */ typedef void (*kafka_consumer_Consumer_commit_async_user_data_destroy_t)(void*);
```

C# delegate: `void Handler(IntPtr offsets, IntPtr error, IntPtr userData)`,
`[UnmanagedFunctionPointer(CallingConvention.Cdecl)]`. **Returns `void`** — unlike
the listener, there is **no error return channel** (§6.2).

### 3.2 The three entry points — note the asymmetry

```c
/* h:2454 */ kafka_common_KafkaError_t *kafka_consumer_Consumer_commit_async(
                 const kafka_consumer_Consumer_t *consumer);            /* SHIPPED (M5/P6) */

/* h:2502 */ kafka_common_KafkaError_t *kafka_consumer_Consumer_commit_async_with_callback(
                 const kafka_consumer_Consumer_t *consumer,
                 kafka_consumer_Consumer_commit_async_callback_t callback,   /* NON-nullable */
                 void *user_data,
                 void (*user_data_destroy)(void*));                          /* nullable */

/* h:2529 */ kafka_common_KafkaError_t *kafka_consumer_Consumer_commit_async_offsets_with_callback(
                 const kafka_consumer_Consumer_t *consumer,
                 const char *const *topics, const int32_t *partitions,
                 const int64_t *offsets, const int32_t *leader_epochs,
                 const char *const *metadata, int32_t count,
                 kafka_consumer_Consumer_commit_async_callback_t callback,   /* NON-nullable */
                 void *user_data,
                 void (*user_data_destroy)(void*));                          /* nullable */
```

⚠ **There is NO `kafka_consumer_Consumer_commit_async_offsets`.** Verified:
`/usr/bin/grep -n "kafka_consumer_Consumer_commit_async[a-z_]*(" confluent_kafka.h`
returns exactly the three lines above. `callback` is spelled as the `_t` alias in
both `_with_callback` forms → **non-nullable**; only `user_data_destroy` uses the
inline nullable spelling. **Passing NULL for `callback` is UB.**

**Consequence — the four-way branch** (this is the phase's headline complexity):

| offsets | callback | ABI call |
|---|---|---|
| — | — | `Consumer_commit_async` *(already shipped)* |
| — | yes | `Consumer_commit_async_with_callback` |
| yes | — | `Consumer_commit_async_offsets_with_callback` **+ a no-op discard trampoline** |
| yes | yes | `Consumer_commit_async_offsets_with_callback` |

The offsets are the **5-parallel-array** form
(`topics/partitions/offsets/leader_epochs/metadata` + `count`) — byte-identical to
`Consumer_commit_sync_offsets`, so **`WithPinnedCommitOffsets`
(`NativeConsumer.cs:3316`) is the exact marshaller**, already used by
`Commit(offsets)` at `:1399`. Do not write a new one.

All three sync entry points take `SafeConsumerHandle` as the P/Invoke parameter
type (M9/P4 H1). Set `EntryPoint` explicitly on both new declarations.

### 3.3 Ownership contracts — quoted, non-negotiable

**Delivered handles** (`h:243-248`): *"`offsets` is the (always non-null) map of
offsets the commit applies to, and `error` is null on success / non-null on
failure … **The callee owns both handles** and must free them with
`kafka_consumer_OffsetMap_destroy` / `kafka_common_KafkaError_destroy`."*

⚠ **Asymmetry with P6 — read this twice.** `TopicPartitionListMarshal` exposes
`CopyOutAndDestroy` (P6 used it). `OffsetMapMarshal` exposes only
**`CopyOut(IntPtr map)`** (`Internal/Interop/OffsetMapMarshal.cs:58`) — it does
**not** destroy. **The trampoline must call `OffsetMap_destroy` itself, in a
`finally`.** Copying P6's shape verbatim leaks the map on every commit.

**Invocation** (`h:2466-2467`): *"Fires **exactly once** per successful call, on
the consumer's callback dispatcher thread, when the commit completes."*

**Reentrancy** (`h:2470-2477`): the callback must **not** call the plain
`Consumer_*` API of this consumer (`ConcurrentModification`); the sanctioned path
is `ConsumerHandle` — **P8**. Document the limitation; do not build it.

**Dispatcher deadlock** (`h:2478-2481`): a callback that block-waits on *consumer
progress* deadlocks the single dispatcher queue. Put this in the
`IOffsetCommitCallback` XML doc.

**Mock behavior** (`h:2482-2485`): *"On a `MockConsumer` the core invokes the
callback inline during the commit (with `error` always null), so the callback has
already run by the time this function returns."* → **mock tests are fully
deterministic; no polling, no `wait_for`.**

**`user_data`** (`h:2488-2494`): *"handed to the consumer for the lifetime of the
registration … the hook fires exactly once, on an unspecified thread, and fires
**even when this function returns an error** (the transfer is unconditional)."*

---

## 4 · Parity anchors

### 4.1 Python (`bindings/python/consumer.py`)

| P7 deliverable | Python anchor | Cite |
|---|---|---|
| the whole surface | `commit_async(offsets=None, callback=None)` — **one method covering all three Java overloads** | `:668-705` |
| callback signature | `callback(offsets, exception)` — `dict[TopicPartition, OffsetAndMetadata]` + `KafkaError` or `None`; Java's `onComplete(Map, Exception)` | `:320-330` |
| **error policy** | *"Java's `onComplete` returns `void` and has nowhere to report a failure of its own, so an exception raised here is **logged and swallowed**."* | `:325-328`, impl `:363-372` |
| handle ownership | *"Both handles are owned by this call; draining / converting frees them."* | `:352-356` |
| dispatcher-thread warning | *"`callback` runs on the Rust dispatcher thread, not the caller's, and the operation that delivers it (a later `poll`/`commit`/`close`) does not return until it does — matching Java … To touch the consumer from inside it, use `handle()`."* | `:691-699` |

### 4.2 C — **the better anchor for the four-way branch**

Roadmap §2.4 named this in advance: Python hides the non-nullable-callback
asymmetry inside its C extension, so **only C shows the shape .NET must
reproduce**.

- `discard_commit_complete` (`bindings/c/grpc_server/server.cc:376-380`) — the
  no-op that still frees both handles:
  ```c
  extern "C" void discard_commit_complete(kafka_consumer_OffsetMap_t* offsets,
                                          kafka_common_KafkaError_t* error, void* /*user_data*/) {
    if (offsets != nullptr) kafka_consumer_OffsetMap_destroy(offsets);
    if (error != nullptr) kafka_common_KafkaError_destroy(error);
  }
  ```
- The selection site (`server.cc:965-966`):
  `req->with_callback() ? log_commit_complete : discard_commit_complete, state, /*user_data_destroy=*/nullptr`.
  ⚠ C passes `nullptr` for the destroy hook because **C has no GC handle to
  free** — its `LogState` is session-lifetime. **.NET must not copy that**: it has
  a `GCHandle`, so it needs the hook (§5, P7-D1).

---

## 5 · ⚠ The callback family — get this right or the free site is wrong

`ffi-marshalling.md` §B6 now carries **two** Rules (P6 added the second):

- **Rule (one-shot per-operation completion — the ~8 async ops)** — §B6:1212.
  The `GCHandle` is *"freed **exactly once** by the callback — including the
  inline guard-rejection error path."*
- **Rule (multi-shot registration — the rebalance listener)** — §B6:1230. The
  `GCHandle` is *"freed by the ABI's `user_data_destroy` hook, and by nothing
  else."*

**The commit callback is in the ONE-SHOT family** — it fires *"exactly once per
successful call"* and is bound to an **operation**, not a subscription.

**But the one-shot Rule's free site is WRONG for it**, and this is the trap:

1. The existing ~8 one-shot ops have **no `user_data_destroy` hook** — the
   callback is their only release path, and their callback *is* invoked on the
   guard-rejection error path, so "callback frees it" is total.
2. The commit callback **has** a hook, and its callback is **NOT** invoked on
   failure. `src/ffi/consumer.rs:4003-4009` proves it — the adapter is built
   *before* the fallible `read_offset_map`, so an early return **drops the
   adapter (firing the hook) without ever calling the callback**. The rustdoc
   states it outright: *"if the offsets fail to marshal (e.g. a negative offset)
   this returns the error **without registering the callback** — the callback
   never fires, but `user_data_destroy` still does."*

**Therefore: the `user_data_destroy` hook is the single free site** — the only one
that fires on every path. Freeing in the callback leaks the `GCHandle` on the
marshal-failure path *and* races the hook on the success path.

So P7 ships a **one-shot callback with a multi-shot free-site contract**. That
hybrid does not exist in the rulebook today → **it is exactly what D9/§7 must add
to §B6** (and it is why P7-D1 in §10 asks how to express it).

**Reuse the P6 machinery, do not reinvent it:** `ListenerRegistration.cs` already
implements idempotent-safe `GCHandle` release (`Interlocked` + `IsAllocated`)
driven from a destroy hook. A `CommitCallbackRegistration` should mirror it. **But
do not reuse P6's *rationale*** — see §6.3.

### 5.1 Trampoline body — mandatory shape

```
try {
    offsets = OffsetMapMarshal.CopyOut(offsetsPtr);          // does NOT destroy
    error   = KafkaException.FromHandle(errorPtr);            // frees the error handle
    registration = recover from GCHandle(userData);           // do NOT free here
    registration.Callback.OnComplete(offsets, error);
}
catch { /* swallow — Java onComplete returns void (§10, P7-D3) */ }
finally { NativeMethods.OffsetMapDestroy(offsetsPtr); }       // callee owns it
```

`KafkaException.FromHandle` already frees the `KafkaError` handle and returns null
on success — the established §B5 shape. The `OffsetMap` has **no** such helper;
destroy it explicitly in the `finally`.

### 5.2 Anti-patterns a Critic will look for

- `GCHandle.Free()` in the trampoline (that is the *other* one-shot rule).
- `OffsetMapMarshal.CopyOut` without a matching `OffsetMap_destroy` — a leak on
  **every** commit.
- A per-call delegate instance instead of a `static readonly` rooted field.
- Passing `null` for `callback` to either `_with_callback` entry point (UB).
- A hand-rolled offsets marshaller instead of `WithPinnedCommitOffsets`.
- An exception escaping the trampoline into Rust.
- Reusing P6's `Arc`-based rationale (§6.3).

---

## 6 · Behaviors to pin

### 6.1 `IOffsetCommitCallback` is SYNC — settled by the P6 precedent

`void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
KafkaException? exception)`. **Not `Task`-returning.**

The ABI typedef returns `void` (`h:264`) and fires on the dispatcher thread —
identical reasoning to roadmap Q6/D1, which the maintainer settled for the
listener and which `CLAUDE.md` §4 now records as the rebalance-listener
divergence. §3's idiom row still says "`IOffsetCommitCallback` (async)"; that is
the **stale pre-P6 wording** and D8 replaces it (§8).

### 6.2 One error channel, not two — and no return path

Unlike the listener, this callback returns `void`: there is **no** way to report a
callback-side failure back to the core, and no `KafkaError_new` involved.

The two error surfaces are distinct and must not be conflated:

- the **returned** `KafkaError*` from `CommitAsync(...)` → a **commit-initiation**
  failure, thrown synchronously to the caller (the existing `void CommitAsync()`
  shape at `NativeConsumer.cs:1168-1174`);
- the `error` **delivered to the callback** → the **commit's own** outcome.

A commit can initiate fine and fail later; both paths must be tested.

### 6.3 ⚠ Do NOT inherit P6's disproved rationale

M9/P6's implementation was right but its **written justification was wrong**: it
argued an owned `Arc` is held across the callback. `FfiRebalanceListener::invoke`
(`src/ffi/consumer.rs:3122`) **copies the pointer out before dispatching**, so the
closure carries a **raw copy**, not the `Arc`.

**What actually closes the window is (1) the ref-counted `SafeConsumerHandle` and
(2) the single serialised dispatcher** — now recorded in `ffi-marshalling.md` §B6
with *"if either property is weakened, re-derive this choice."* Any P7 reasoning
about `GCHandle` lifetime must cite **those two properties**, never an `Arc`.

---

## 7 · D9 — the itemized `ffi-marshalling.md` sections

Per roadmap §5.6. For each §: confirm the concept appears **and** that no existing
normative sentence forbids the shipped code. **(b) is the half missed twice.**

| § | What P7 must add or correct |
|---|---|
| **§B6** | The one-shot family gains a **release-hook variant**: a callback that fires only on success, whose `GCHandle` is freed by `user_data_destroy` and **not** by the callback. Today §B6:1212 says the one-shot `GCHandle` is *"freed exactly once by the callback"* — **that sentence forbids the shipped code** and must be scoped to the ~8 hookless ops. Follow the in-file §A7 precedent for adding a variant. |
| **§B7** | §B7 frames every callback as completing a `TaskCompletionSource`. The commit callback completes **none** — it is fire-and-forget, with no `Task`. Needs the same carve-out P6 added for registrations. |
| **§B2** | The delivered `OffsetMap_t` is a **Category 3** owned-by-callback handle, **but** `OffsetMapMarshal` has no `CopyOutAndDestroy` — record the asymmetry with `TopicPartitionListMarshal` explicitly (§3.3). Also state that a commit registration is **not** Category 5 (consumed-by-callee): the *listener handle* is consumed; a commit registration is not a handle at all. |
| **§B5** | The two error channels (§6.2) — returned-vs-delivered — and that `FromHandle` frees the delivered `KafkaError` while the `OffsetMap` needs an explicit destroy. |

Verify with `/usr/bin/grep` (bare `grep` is `ugrep` and **silently returns nothing
on a rejected pattern** — a false pass).

---

## 8 · D8 — the maintainer-sanctioned `CLAUDE.md` amendment, verbatim

Roadmap Q5 was approved: **§3 wins, §4 gets an amendment.** The P6 Actor
deliberately left both rows untouched because editing them would pre-empt a phase
whose surface did not exist. **They are P7's to change.** Do not improvise
rulebook text — use this.

**8.1 — §3 idiom map.** Replace:

> `| OffsetCommitCallback | IOffsetCommitCallback (async) | same caller's-task model — consumer-threading §31 |`

with:

> `| `OffsetCommitCallback` | `IOffsetCommitCallback` — **sync `void`** `OnComplete(offsets, exception)`; passed to the `CommitAsync(callback)` / `CommitAsync(offsets, callback)` overloads (M9/P7 — **shipped**) | ⚠ **neither async nor the caller's task** — the ABI callback returns `void` and fires on the core's **dispatcher thread**. See the §4 **commit-callback divergence**; ffi §B6, consumer-threading §31 |`

**8.2 — §4 sync-vs-async table.** Replace the third row's C# cell:

> `` `Task`/`Task<T>` on the async interface — the `Task` **replaces** the callback; do **not** add a callback-taking overload ``

with:

> `` `Task`/`Task<T>` on the async interface — the `Task` **replaces** the callback; do **not** add a callback-taking overload. **⚠ Exception — a callback carrying payload the `Task` cannot:** Java's `OffsetCommitCallback.onComplete(Map, Exception)` delivers the **offsets the commit applied to**, which a `Task` returning `void` cannot express, and `commitAsync` is one half of a Java sync/async **pair** whose other half (`commitSync`) already owns the `Task` mapping (`Commit`, M5/P6). So the commit family keeps the callback-taking overloads — see the §4 **commit-callback divergence** (M9/P7). ``

**8.3 — a new §4 divergence note**, placed immediately after the existing
rebalance-listener divergence and before the "Exception — Java sync/async pairs"
block, so the commit family's two notes sit together:

> ⚠ **§4 divergence — the commit callback is SYNC, and runs on the core's
> dispatcher thread (M9/P7).** `IOffsetCommitCallback.OnComplete` returns `void`,
> not `Task`, on both consumer flavors — the ABI typedef returns `void`
> (`confluent_kafka.h:264`) and fires on the callback-dispatcher thread. This is
> the same divergence, for the same reason, as the rebalance listener above (D1 /
> D3); §3's row previously read "(async)", describing the **Rust core's** trait,
> which the C ABI has flattened.
>
> Unlike the listener there is **no error return channel**: Java's `onComplete`
> returns `void` and has nowhere to report a failure of its own, so an exception
> raised by the callback is **swallowed** (Python does the same —
> `consumer.py:363-372`). Two error surfaces stay distinct: the `KafkaException`
> thrown *synchronously* by `CommitAsync` is a commit-**initiation** failure; the
> `exception` delivered to `OnComplete` is the **commit's** outcome.
>
> This is a **one-shot** completion, so — unlike the listener — the "takes a
> completion callback → the `Task` replaces the callback" row *would* textually
> apply; §8.2's exception is what carves it out, on the grounds that the callback
> carries offsets a `Task` cannot.

**8.4 — §3 "Still to come".** Remove `IOffsetCommitCallback` from the list and
add the commit overloads to the shipped sentence, mirroring how P6 added the
listener.

**8.5 — §3 `IConsumerCommon` sketch.** Add the two new overloads beside the
existing `void CommitAsync();`, with a `// Java commitAsync(cb) / commitAsync(Map, cb) — M9/P7` marker.

---

## 9 · DoD and test obligations

### 9.1 Gates — the corrected §6.4 map

**`cargo xtask lint` already runs a second `--workspace --all-targets
--all-features` pass** (`xtask/src/main.rs:154-167`, since `4fd1435f` / #139,
2026-08-05). **Do not add a separate clippy invocation** — the pre-correction text
demanding one was stale and is removed from the roadmap.

| Gate | Command |
|---|---|
| .NET build, TFM matrix | `dotnet build` (netstandard2.0/net462 · net8.0 · net10.0) |
| .NET tests — **the execution gate** | `dotnet test -f net10.0` |
| .NET format | `dotnet format --verify-no-changes` |
| Rust no-regression | `cargo xtask lint` · `cargo test --all-features -- --skip __grpc` |

`dotnet` → `/usr/local/share/dotnet/dotnet` (SDK 10.0.302); `cargo` →
`~/.nix-profile/bin/cargo` (1.95.0). **Neither is on the default `PATH`.** net8.0
is build-only (runtime absent locally); net10.0 is the execution gate.

**A `tests/`-only break is invisible to `cargo build --all-features`** (it
schedules zero `test`-kind targets). The gates that see it are `cargo xtask lint`
and `cargo test --all-features`.

### 9.2 Required tests

Root DoD #1, #2, #3, #5, #8, #9 apply. #10 is **N/A** (per-commit, not per-record)
— state it. #11 applies. #12 applies.

1. The callback receives the **committed offsets** and a null exception — assert
   the `OffsetAndMetadata` **values**, not merely non-empty.
2. A failing commit delivers a `KafkaException` with the right code **and exact
   message** (DoD #3).
3. **`CommitAsync(offsets)` with no callback** — the discard path frees both
   handles exactly once (mirror
   `ConsumerCompletionBridgeTests.FreeGcHandle_CalledTwice_FreesAndReleasesExactlyOnce:187`).
4. **The marshal-failure path** (e.g. a negative offset) — the call throws, the
   callback **never** fires, and the `GCHandle` is still freed exactly once. *This
   is the §5 trap; without this test the wrong free site passes.*
5. A **throwing** callback is swallowed, does not unwind into native, and the
   consumer stays usable.
6. Aggressive GC across a live registration does not collect the delegate
   (mirror `InFlightOperation_SurvivesAggressiveGc:169`).
7. Both error channels (§6.2) are exercised and distinguished.
8. `Dispose`/`DisposeAsync` return without hanging with a registration live.
9. A **non-ASCII topic name** round-trips through the delivered offsets map (§B3).

Mock determinism (`h:2482-2485`): the callback fires **inline** during the commit
with `error` always null, so every mock assertion is synchronous — **no polling,
no `wait_for`**. Getting a real error to the callback therefore needs the
initiation path or a broker; if test 2 has no mock vehicle, **say so explicitly**
rather than asserting a weaker thing.

---

## 10 · ⚠ DECISIONS FOR THE MAINTAINER

### P7-D1 — How should §B6 express a one-shot callback with a release hook?

§5 establishes the *behavior* is settled (the hook is the only free site — the
header and `src/ffi/consumer.rs:4003-4009` leave no room). What is **not** settled
is how the rulebook should say it, and rulebook shape is a maintainer call:

- **(a) A third Rule block** in §B6 — "one-shot with release hook" — alongside the
  existing two. Most legible; §B6 grows to three Rules.
- **(b) Amend the existing one-shot Rule** to split its free-site sentence by
  whether the entry point takes a `user_data_destroy`. Keeps two Rules; the
  one-shot Rule gets a conditional, which is easier to misread.

**Recommendation: (a).** P6's finding was that a *missing* category forced a
reader to interpolate; a conditional inside an existing Rule reproduces that.

### P7-D2 — Does `CommitAsync(offsets)` ship without a callback?

Java's `commitAsync(Map, null)` is legal; Python's `commit_async(offsets,
callback=None)` supports it; the ABI **cannot** (non-nullable `callback`), so .NET
must supply a discard trampoline to offer it.

- **(a) Ship it** — `IOffsetCommitCallback? callback = null` on the offsets
  overload, backed by the discard trampoline (C's `discard_commit_complete`
  shape). Full Java + Python parity; costs one extra trampoline and test 3.
- **(b) Require a callback** when offsets are given — no discard trampoline, less
  surface, but a Java form the binding cannot express.

**Recommendation: (a).** It is a legal Java call and Python has it; the cost is
one small no-op.

### P7-D3 — Is a callback exception silently swallowed, or observable?

Java's `onComplete` returns `void` and cannot report its own failure; Python
**logs and swallows** (`consumer.py:363-372`) via its module logger.

- **(a) Swallow silently**, documented on `IOffsetCommitCallback`. Simplest;
  strictly Java-faithful. **But** .NET has no ambient logger here, so unlike
  Python the failure leaves *no* trace — a debugging cliff.
- **(b) Swallow + emit to `System.Diagnostics.Trace`** (or `Debug`). Closer to
  Python's *observable* behavior; introduces the binding's first diagnostics
  dependency, which is a policy precedent.
- **(c) Swallow + a static opt-in hook** (e.g. an `Action<Exception>`). Most
  flexible, most invented surface (DoD #7).

**Recommendation: (b)** — Python's real behavior is *log and swallow*, not
*discard silently*, and (a) only matches the second half. But it sets a
diagnostics precedent for the binding, so it is the maintainer's call, not the
Actor's.

---

## 11 · Mechanics

- Commit per step; `fixup!` referencing the original commit when closing a
  `COMMENTS.60.md` item.
- `bindings/dotnet/COMMENTS.60.md` → `COMMENTS.DONE.60.md`; **never `git add`
  either.** The Manager archives the DONE file to `design/history/M9/P7/`.
- Personas must be copied to the repo-root `.claude/agents/` to be invocable, and
  those root copies must **never** be committed.
- ⚠ **`grep` is aliased to `ugrep`** and silently emits nothing on a rejected
  pattern — a false *pass*. Use `/usr/bin/grep` for any evidence claim.
- Required reading (contract facts **absent from the header**):
  `.claude/agent-memory/actor-executor/ffi_callback_bridging_phase{3,4,6}_notes.md`
  — phase 3 is the commit-callback phase; phase 6 item 4 is the non-nullable-callback
  discovery.
