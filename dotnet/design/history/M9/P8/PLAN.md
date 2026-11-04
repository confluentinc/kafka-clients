# M9/P8 — `ConsumerHandle` (in-callback reentrancy) — the final phase

**Status:** **ACTOR BRIEF — dispatch-ready.** Derived from the approved roadmap
`design/current/PLAN-M9-consumer-callback-parity.md`. **Four P8-local decisions in
§10 need a maintainer ruling before the Actor starts.**

**Agent number:** **N=61.** **Milestone/Phase:** M9/P8. **Mode: A** (C# only).
**Branch:** `prashah_dev_dotnet_binding_consumer`, on top of `62b360b4` (M9/P9).
**Risk: medium** — 23 declarations are mechanical; the lifetime interaction with
`SafeConsumerHandle` is not.

**This closes the roadmap.** P5/P6/P7/P9 are done; P8 is the last phase.

---

## 0 · Three things to get right before anything else

These are the errors most likely to be inherited. Read them before the deliverables.

### 0.1 ⚠ Cite the ref-count, NEVER the `Arc` — the `Arc` argument is disproved

P6's implementation was correct but its **written rationale was wrong**, and P8
sits directly on the same invariant (roadmap §5.7). The Actor there argued an owned
`Arc` is held across the callback. It is not:
**`FfiRebalanceListener::invoke` (`src/ffi/consumer.rs:3122`) copies the pointer
out *before* dispatching**, so the closure carries a **raw copy**, not the `Arc`. A
count ≥ 1 holds only while the awaiting *future* lives.

**What actually closes the window — cite exactly these two, together:**

1. the **ref-counted `SafeConsumerHandle`** — an op only ever runs while holding a
   count, so `Consumer_destroy` cannot run concurrently with one; **and**
2. the **single serialised dispatcher** — the deferred-destroy path fires from
   `FreeGcHandle` **on the dispatcher thread**, the same thread that would run a
   queued job.

`ffi-marshalling.md` §B6 already carries both, with *"if either property is ever
weakened, this rule must be re-derived."* **P8's `ConsumerHandle` ref-counts the
same `SafeConsumerHandle`, i.e. it rests on property (1) directly** — so any P8
reasoning about handle lifetime must name it. An `Arc`-based justification in a P8
comment is a **finding**, even where the code is right.

### 0.2 The free-site rule is total — and P8 is on the "no hook" side

P7 established (roadmap §5.8) that the discriminator is **"does the entry point
take a `user_data_destroy`?"** — not one-shot vs multi-shot. Exactly **three**
entry points take one, and P8 uses **none** of them:

| Entry point with a hook | Header | Used by |
|---|---|---|
| `ConsumerRebalanceListener_new` | `h:2078` | P6 |
| `Consumer_commit_async_with_callback` | `h:2505` | P7 |
| `Consumer_commit_async_offsets_with_callback` | `h:2538` | P7 |

**No P8 entry point takes a `user_data_destroy`, and P8 introduces no callbacks at
all** — `Consumer_handle` and all 22 `ConsumerHandle_*` functions are plain
synchronous calls. So **the free-site rule does not engage anywhere in P8**, and
there is no `GCHandle` in this phase. **State that explicitly in the phase record**
(§8) rather than leaving it unaddressed.

### 0.3 §31 test #1 is RESOLVED as a split, not deferred again

Roadmap §5.11 settles it. **P8 ships the mechanism proof** (§9.2 test 6); the
end-to-end half is follow-up **O3**. The verbatim wording for
`COMMENTS.DONE.61.md` is in §5.11 — **copy it, do not paraphrase**.

---

## 1 · Scope

Java's in-callback reentrancy — what Java gets for free by running callbacks on the
polling thread (`acquire()` is reentrant there), and which this binding must
provide explicitly because the C ABI's access guard rejects reentrant calls.

### In scope

1. A public `ConsumerHandle` type (`IDisposable`).
2. `ConsumerHandle Handle()` on `IConsumerCommon`.
3. **23 P/Invoke declarations** — `Consumer_handle` + 22 `ConsumerHandle_*` (§3).
4. Managed marshalling for the four result shapes (all marshallers already exist).
5. The `SafeConsumerHandle` ref-count interaction (§5, P8-D1).
6. `ffi-marshalling.md` §B1/§B2/§B5 + the §B6 no-op statement (§8).
7. `CLAUDE.md` doc-sync (§7).
8. Tests per §9.

### Explicitly OUT of scope

| Out | Why |
|---|---|
| `poll` / `subscribe` / `unsubscribe` / `close` on the handle | **Not in the ABI, deliberately** — *"Java never invokes those reentrantly from a callback"* (`src/ffi/consumer_handle.rs:76-80`). Do not add wrappers |
| Callback-taking commit variants on the handle | Absent from the core handle by design; use `Consumer_commit_async_with_callback` on the owning consumer (roadmap D9) |
| `bindings/dotnet/grpc-server/**` | P9 is done and clean. **A handle-using server listener is follow-up O3**, not P8 |
| Anything under `tests/` (Rust) | O1/O2/O3 are cross-backend follow-ups (roadmap §5.10) |
| `src/` (Rust), `src/ffi/`, `cbindgen.toml` | Mode A |
| Changes to P6/P7 shipped surfaces | Consume them; do not refactor |

**Mode-A proof obligation:** `git diff a7efb0d5 HEAD -- src/ cbindgen.toml` must
still be **empty**, and P8 must add nothing under `tests/`. Mode A has held for the
whole milestone — **state the re-verified result in the final commit.**

---

## 2 · Deliverables

| # | Deliverable | Path |
|---|---|---|
| D1 | `public sealed class ConsumerHandle : IDisposable` | `src/Confluent.Kafka/ConsumerHandle.cs` (new) |
| D2 | `ConsumerHandle Handle()` | `src/Confluent.Kafka/IConsumerCommon.cs` |
| D3 | Four forwarders | `KafkaConsumer.cs`, `AsyncKafkaConsumer.cs`, `MockConsumer.cs`, `AsyncMockConsumer.cs` |
| D4 | 23 P/Invokes | `Internal/Interop/NativeMethods.cs` |
| D5 | `SafeConsumerHandleRef` / ref-count wiring (§5) | `Internal/Interop/SafeConsumerHandle.cs`, `Internal/NativeConsumer.cs` |
| D6 | **`ffi-marshalling.md` §B1/§B2/§B5 + the §B6 no-op statement** | `.claude/rules/ffi-marshalling.md` |
| D7 | **`CLAUDE.md` doc-sync** (§7) | `bindings/dotnet/CLAUDE.md` |
| D8 | Tests | `tests/Confluent.Kafka.UnitTests/` |
| D9 | **`COMMENTS.DONE.61.md` carries the §5.11 split wording verbatim** | phase record |

D6 and D7 are **itemized deliverables** (roadmap §5.6). Every finding in P5–P7 was
in this category; P9 shipped them correctly and closed clean.

---

## 3 · The ABI surface — 23 symbols

`target/include/confluent_kafka.h`, regenerated 2026-08-29. Verified by
`/usr/bin/grep -n "^[a-z].*kafka_consumer_ConsumerHandle_[a-z_]*("` → exactly 22,
plus `Consumer_handle`.

### 3.1 Lifecycle

```c
/* h:2909 */ kafka_consumer_ConsumerHandle_t *kafka_consumer_Consumer_handle(
                 const kafka_consumer_Consumer_t *consumer);
/* h:2922 */ void kafka_consumer_ConsumerHandle_destroy(
                 kafka_consumer_ConsumerHandle_t *handle);
```

`Consumer_handle` *"does **not** acquire the guard, so this call never fails with
`ConcurrentModificationError`"* and returns *"A **non-null** handle"* (`h:2895-2903`).
`_destroy` is *"Safe to call with a null pointer (no-op)"* and *"never affects the
owning consumer or any other handle"* (`h:2912-2916`).

### 3.2 The 22 functions, by result shape

| Shape | Functions (header line) | Existing marshaller |
|---|---|---|
| **void, no guard** | `wakeup` `:2936` | — |
| **owned list, non-null** | `assignment` `:2950`, `paused` `:2978` | `TopicPartitionListMarshal.CopyOutAndDestroy` |
| **owned string list** | `subscription` `:2964` | `StringListMarshal` |
| **error-only** | `assign` `:2994`, `seek` `:3008`, `seek_with_metadata` `:3024`, `seek_to_beginning` `:3040`, `seek_to_end` `:3054`, `pause` `:3068`, `resume` `:3082`, `commit_sync` `:3202`, `commit_sync_offsets` `:3215`, `commit_async` `:3236`, `commit_async_offsets` `:3247` | `KafkaException.FromHandle` |
| **error + out-scalar** | `position` `:3098`, `position_timeout` `:3112` | existing scalar pattern |
| **error + out-map** | `committed` `:3130`, `beginning_offsets` `:3147`, `end_offsets` `:3164`, `offsets_for_times` `:3182` | `OffsetMapMarshal.CopyOutAndDestroy` / `LongOffsetMapMarshal` / `OffsetAndTimestampMapMarshal` |

**Every marshaller already exists** — P7 added `OffsetMapMarshal.CopyOutAndDestroy`
(`:69`), which removed the `CopyOut`-doesn't-destroy trap structurally (roadmap
§5.8). **Prefer `CopyOutAndDestroy` at any site that owns the map.** Write no new
marshaller; if you feel the need, you have the wrong shape.

**Every one of the 22 is synchronous**, so **every** P/Invoke takes the handle's
`SafeHandle` as the parameter type (M9/P4 H1 convention) — never
`DangerousGetHandle()`. Set `EntryPoint` explicitly on all 23.

### 3.3 The four contracts that define this type

Quoted from `src/ffi/consumer_handle.rs` (module docs, `:20-100`) and the header.

**(a) No access guard — by design.** *"Nothing in this module acquires the
single-owner access guard … That is deliberate and is the whole reason the type
exists … A handle operation therefore succeeds while another consumer operation is
in flight, whereas the equivalent `kafka_consumer_Consumer_*` call would be
rejected with `ConcurrentModificationError`."* (`:27-38`)

**(b) Threading — `block_on` on the *calling* thread.** *"Every operation that is
`async` in the core is exposed here as a **synchronous** C function that drives the
future to completion on the *calling* thread."* **Safe** from the dispatcher thread
and any embedder-owned OS thread. **Must NOT** be called from inside a tokio
runtime — and rather than panic across the FFI boundary, *"every entry point
detects that situation and fails with an `IllegalStateError`"* (`:40-58`).

**(c) Lifetime.** *"A handle is usable only while its consumer is alive. Destroy
every handle **before** destroying the consumer. Handles are independent."*
(`:60-72`) → this is **P8-D1** (§10).

**(d) Mock behavior.** *"On a handle obtained from a `MockConsumer`, `wakeup` works
and the sync getters return empty lists, but **every async operation fails with
`UnsupportedVersionError`** — the mock has no event pipeline … **This is core
behavior, not an FFI limitation.**"* (`:82-86`) → shapes §9 decisively.

---

## 4 · Parity anchors

### 4.1 Python — `bindings/python/consumer.py`

| P8 deliverable | Python anchor | Cite |
|---|---|---|
| the type | `class ConsumerHandle` | `:378-545` |
| accessor | `_ConsumerBase.handle()` — *"This is what a rebalance listener or commit callback uses to call back into the consumer — the consumer's own methods would be rejected as concurrent access"* | `:584-593` |
| disposal idiom | `__enter__` / `__exit__` / `destroy()`, idempotent | `:417-427` |
| ordering guidance | *"Destroy the handle (or use it as a context manager) **before closing the consumer**."* | `:591-592` |
| method set | `wakeup`, `assignment`, `subscription`, `paused`, `assign`, `seek`, `seek_to_beginning`, `seek_to_end`, `pause`, `resume`, `position`, `committed`, `beginning_offsets`, `end_offsets`, `offsets_for_times`, `commit_sync`, `commit_async` | `:435-549` |
| **no** callback-taking commit | `commit_async(self, offsets=None)` on the handle takes **no** `callback` — unlike `_ConsumerBase.commit_async` | `:535` vs `:668` |

Python has **no ref-count**: it documents the ordering and relies on the user
(`:591`). **.NET should not copy that** — see P8-D1.

### 4.2 C — the `Consumer_*`-vs-handle contrast

`bindings/c/tests/test_consumer_callbacks.c` demonstrates the guard-bypass proof
the phase-4 notes (item 3) describe: from inside a commit callback,
`Consumer_commit_sync` returns `ConcurrentModification` (code -1) while
`ConsumerHandle_*` ops do not — *"the guard-bypass proof needs no threads, sleeps
or `wait_for`."* **That is the shape of §9's test 6**, transposed to a
`MockConsumer.Rebalance`-driven listener.

---

## 5 · The lifetime interaction — the phase's real risk

Everything else is 23 mechanical declarations over existing marshallers. This is
not.

The ABI says *"Destroy every handle before destroying the consumer"*
(`consumer_handle.rs:69`). **.NET cannot force user ordering**, and a live handle
holding a raw pointer past `Consumer_destroy` is precisely the use-after-free class
that **M9/P4 (N=41) closed for every other path**.

**Roadmap Q7/D5 settled the approach: the managed `ConsumerHandle` takes a
`DangerousAddRef` on the consumer's `SafeConsumerHandle` for its own lifetime,
released in `Dispose`.** M9/P4's `ReleaseHandle` already runs `Consumer_destroy`
only at count zero, so a live handle **defers** the consumer destroy rather than
dangling it.

**Consequences to design deliberately, not discover:**

- A user who leaks a `ConsumerHandle` **defers `Consumer_destroy` indefinitely**.
  That is strictly better than a UAF, but it is a new deferred-destroy path and
  must be documented alongside the two residuals M9/P4 already accepted.
- The deferred destroy then fires from `Dispose` on **whatever thread disposes the
  handle** — which per §0.1 property (2) may not be the dispatcher thread. **P8-D2.**
- `Dispose` must be idempotent and safe under double-dispose (`Interlocked`), the
  `ListenerRegistration.cs` pattern.

**Anti-patterns a Critic will look for:**

- An `Arc`-based justification anywhere (§0.1).
- `DangerousGetHandle()` on any of the 23 declarations.
- A handle that does **not** ref-count its parent (Python parity is not a defence
  here — §10, P8-D1).
- A new marshaller where an existing one fits (§3.2).
- Treating `UnsupportedVersionError` on a mock handle as a bug to work around
  rather than core behavior to assert (§3.3d).
- Wrapping `poll`/`subscribe`/`unsubscribe`/`close` (§1).

---

## 6 · Behaviors to pin

- **`Handle()` never fails for concurrency** — `Consumer_handle` does not take the
  guard (`h:2900-2902`). It must still `ThrowIfClosed()`.
- **Two distinct error mappings**, both from §B5 and neither invented:
  - inside a tokio runtime → `IllegalStateError` → the binding's
    `InvalidOperationException` mapping;
  - on a mock-derived handle, any **async** op → `UnsupportedVersionError` →
    `KafkaException`.
- **`wakeup` and the three sync getters work on a mock**; the getters return
  **empty**, not null (`h:2942`).
- **`ConsumerHandle_assignment` is documented non-null**; the consumer's
  `Consumer_assignment` returns **null on guard rejection** (`h:2802-2803`). That
  asymmetry is the whole point, and it is what test 6 asserts.
- **A `TimeSpan`-taking `position_timeout`** exists alongside `position` — mirror
  both, matching the consumer's own overload pair.

---

## 7 · D7 — `CLAUDE.md` doc-sync

1. **§3 "Still to come"** — the listener and commit callback are shipped (P6/P7);
   remove any remaining reentrancy caveat and add `ConsumerHandle`.
2. **§3 `IConsumerCommon` sketch** — add `ConsumerHandle Handle();` with a
   `// Java: captured `consumer` in a callback — M9/P8` marker.
3. **§3 consumer surface** — add `ConsumerHandle` to the public types.
4. **A DoD #7 justification** (§9.1) — `ConsumerHandle` is **not** a Java type.
   State plainly: it exists because the C ABI's access guard rejects reentrant
   calls that Java permits (Java's `acquire()` is reentrant for the polling
   thread), and **Python has the identical type for the identical reason**. This is
   binding scaffolding restoring a Java behavior, not new API surface.
5. **Cross-reference from `IConsumerRebalanceListener` / `IOffsetCommitCallback`** —
   both carry a P6/P7 forward reference saying reentrancy "arrives in P8". **Those
   are now due** — point them at `ConsumerHandle`.

---

## 8 · D6 — the itemized `ffi-marshalling.md` sections

Roadmap §5.6 confirmed **§B1/§B2/§B5**, plus an explicit §B6 no-op. For each:
confirm the concept appears **and** that no existing normative sentence forbids the
shipped code — **(b) is the half that recurred three times.**

| § | What P8 must add |
|---|---|
| **§B2** | A new handle category: **caller-owned, independently destroyed, ref-counting its parent**. Distinct from Category 3 (owned-by-callback, freed in-call) and Category 5 (consumed-by-callee). Record the deferred-destroy consequence (§5) beside M9/P4's accepted residuals |
| **§B1** | The handle **deliberately bypasses the access guard**, and its ops `block_on` the **calling** thread — so they are safe from the dispatcher thread and any embedder OS thread, but **not** from inside a tokio runtime |
| **§B5** | Two new mappings: `IllegalStateError` (in-runtime) and `UnsupportedVersionError` (mock-derived async op); plus the guard-bypass contrast — the same logical call succeeds via the handle and is rejected via the consumer |
| **§B6** | **NO EDIT.** But the phase record must **state** that P8 introduces no callback and no entry point taking a `user_data_destroy`, so the free-site rule does not engage (§0.2). A blank row is what recurred three times |

Verify with `/usr/bin/grep`. **Bare `grep` is `ugrep`** and silently emits nothing
on a rejected pattern — a false *pass*.

---

## 9 · DoD, gates, tests

### 9.1 DoD

Root DoD #1, #2, #3, #5, #8, #9 apply. **#7 needs the §7.4 justification.**
**#10 N/A** (per-callback, not per-record) — state it. **#11 applies** (no
`block_on` façade added in managed code — the `block_on` is the **core's**, inside
the sync ABI, which is the shipped `Seek`/`CurrentLag` precedent, *not* the
forbidden managed sync-over-async). **#12 applies.**

### 9.2 Gates

**`cargo xtask lint` already runs a second `--workspace --all-targets
--all-features` pass** (`xtask/src/main.rs:154-167`). **Do not add a separate
clippy invocation.**

| Gate | Command |
|---|---|
| .NET build, TFM matrix | `dotnet build` (netstandard2.0/net462 · net8.0 · net10.0) |
| .NET tests — **the execution gate** | `dotnet test -f net10.0` (**must not regress; P7 left 543/543, P9 added none**) |
| format | `dotnet format --verify-no-changes` |
| Rust no-regression | `cargo xtask lint` · `cargo test --all-features -- --skip __grpc` |
| harness no-regression | `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet` |

`dotnet` → `/usr/local/share/dotnet/dotnet`; `cargo` → `~/.nix-profile/bin/cargo`.
Neither is on the default `PATH`. net8.0 is build-only; net10.0 executes.

⚠ **A libtest filter matching zero tests exits 0 printing `0 passed`.** The arms
are `__grpc_dotnet` / `__grpc_dotnet_async` — `__dotnet` matches **nothing** and
looks green. **Assert on per-test names in the output, never on the exit code.**

### 9.3 Required tests

1. `Handle()` returns a usable handle; **succeeds while an op is in flight**
   (it never takes the guard).
2. `Dispose` is idempotent; use-after-`Dispose` throws `ObjectDisposedException`.
3. `wakeup` + the three sync getters work on a mock-derived handle; the getters
   return **empty**, not null.
4. Every **async** op on a mock-derived handle throws `KafkaException` with the
   `UnsupportedVersion` code **and the exact message** (DoD #3). This is **core
   behavior asserted**, not a limitation worked around.
5. **A live handle defers `Consumer_destroy`**; disposing it releases the consumer.
   Directly exercises §5 / P8-D1 — mirror M9/P4's ref-count tests.
6. **§31 test #1, mechanism half (roadmap §5.11).** Inside a listener fired by
   `MockConsumer.Rebalance`: `handle.Assignment()` **succeeds** while the same
   listener's `consumer.Assignment()` **is rejected** as concurrent access.
   ⚠ **Mutation check required** (phase-5 notes item 3): prove the test **fails**
   if the handle call is swapped for the consumer call — otherwise a
   both-succeed bug passes silently. Record that you ran it.
7. Non-ASCII topic round-trip through a handle result (§B3).
8. `Dispose`/`DisposeAsync` on the consumer return without hanging with a live
   handle outstanding (the deferred-destroy path, §5).

**Test 4 is not a workaround.** `UnsupportedVersionError` on a mock handle is
documented core behavior (`consumer_handle.rs:82-86`); asserting it is coverage.

---

## 10 · ⚠ DECISIONS FOR THE MAINTAINER

### P8-D1 — Does `ConsumerHandle` ref-count the consumer? (roadmap Q7/D5 — reconfirm)

Q7 was settled at roadmap approval in favour of ref-counting. **Reconfirm it now
that the cost is concrete**, because the trade has a visible downside.

- **(a) Ref-count (`DangerousAddRef` for the handle's lifetime).** No UAF possible.
  **Cost:** a leaked handle **defers `Consumer_destroy` indefinitely** — a new
  deferred-destroy path to document beside M9/P4's accepted residuals.
- **(b) Document the ordering, no ref-count** (Python's approach, `:591`). No
  deferral; reintroduces the UAF class M9/P4 deliberately closed.

**Recommendation: (a), as approved.** M9/P4's thesis was that a raw pointer
outliving a concurrent destroy is the bug to eliminate; shipping a new long-lived
raw-pointer holder would undo it. A deferred destroy is a leak — a UAF is
corruption.

### P8-D2 — Which thread may run the deferred `Consumer_destroy`?

If (a), the last `Dispose` can trigger `Consumer_destroy` **on whatever thread
disposed the handle**. §0.1 property (2) — "the deferred-destroy path fires from
`FreeGcHandle` on the dispatcher thread" — is what P6/P7's free-site safety rests
on. **A handle disposed from an arbitrary user thread is a *new* trigger for that
path**, and §B6's *"if either property is weakened, this rule must be re-derived"*
may be engaged.

- **(a) Verify it is benign and document it.** Establish from
  `src/ffi/consumer.rs` whether `Consumer_destroy` from a non-dispatcher thread can
  race a queued job. Benign → document and move on.
- **(b) Constrain disposal** so the destroy is marshalled to the dispatcher thread.
  Strictly safer, materially more machinery.

**Recommendation: (a) verify first, then decide — and do NOT let the Actor pick
silently.** This is the same shape as P6-D3, which the coordinator noted "would
otherwise have been a silent, wrong-rationale choice." **If the verification is
inconclusive, escalate rather than assume benign.**

### P8-D3 — Does `Handle()` live on `IConsumerCommon`, or only on the concrete types?

- **(a) `IConsumerCommon`** — both flavors, matching Python's `_ConsumerBase.handle()`
  and every other flavor-independent member.
- **(b) Concrete types only** — keeps the interface smaller; a mock-derived handle
  is mostly `UnsupportedVersion`, so its interface value is arguably low.

**Recommendation: (a).** It is flavor-independent and Python puts it on the shared
base. (b) would make the reentrancy path unreachable through
`IConsumer`/`IAsyncConsumer`, which is where a user holds a consumer.

### P8-D4 — Is `ConsumerHandle` `IDisposable` only, or also `IAsyncDisposable`?

Every handle op is **synchronous** (§3.3b), so there is nothing to await; Python
uses a plain context manager.

- **(a) `IDisposable` only.** Honest about the type — no async surface exists.
- **(b) Both**, for symmetry with the consumers' `IAsyncDisposable`. Costs an
  `IAsyncDisposable` that only wraps a sync call — arguably misleading.

**Recommendation: (a).** Flagged only because every other disposable in this
binding implements both, so (a) is a visible, deliberate asymmetry that should be
commented rather than left to look like an oversight.

---

## 11 · Mechanics

- Commit per step; `fixup!` referencing the original commit when closing a
  `COMMENTS.61.md` item.
- `bindings/dotnet/COMMENTS.61.md` → `COMMENTS.DONE.61.md`; **never `git add`
  either.** The Manager archives the DONE file to `design/history/M9/P8/`.
- **`COMMENTS.DONE.61.md` must carry the §5.11 split wording verbatim** (D9).
- Personas must be copied to the repo-root `.claude/agents/` to be invocable; those
  root copies must **never** be committed.
- Shell caveats: **`grep` is `ugrep`** (use `/usr/bin/grep` for evidence);
  **zsh does not word-split** unquoted `$var` (`set -- $pair` silently yields
  nothing); **a zero-match libtest filter exits 0**.
- Required reading:
  `.claude/agent-memory/actor-executor/ffi_callback_bridging_phase4_notes.md` — the
  phase that *built* `ConsumerHandle`. Item 2 (the `block_on`-in-runtime →
  `IllegalState` decision), item 3 (the guard-probe technique behind test 6), item 4
  (mock handles cover only the `Mock` arm), item 5 (the surface is **17** async ops,
  not 15).
- **This is the last phase of the roadmap.** At close, follow-ups **O1, O2, O3**
  (roadmap §5.10) remain open as a separate cross-backend piece of work with CI as
  verifier — they are **not** P8's, and P8 closing does not close them.
