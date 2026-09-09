# M15/P1 — Admin foundation + the per-key bridge, proven on `CreateTopics`

**Status:** **APPROVED 2026-09-08.** Authorized to start. **N = 66.**

**Parent roadmap:** `bindings/dotnet/design/current/PLAN-M15-admin-client.md`
(approved 2026-09-08; decisions D1–D6 in its §1). Read it — this file is the
phase contract, the roadmap holds the full design derivation and the evidence.

**Mode:** **A**. No Rust, no new ABI function, no `cbindgen.toml` change.

**Branch:** `prashah_dev_dotnet_admin` (the maintainer's working branch). Do NOT
create a new branch. Do NOT merge or rebase onto anything.

⚠ **Authorization is P1-only.** P2 requires a fresh go-ahead. Do not start it.

---

## 1 · Why this phase is scoped to one RPC

P1 lands the **shared mechanism** that P2…P8 merely repeat: the per-key bridge,
`KafkaException.FromBorrowedHandle`, `KeyedResultMarshal` (result shapes 1 and
2), and the span-the-op `DangerousAddRef` on `SafeAdminHandle`. A defect in that
mechanism must be found **once, in a 1-RPC diff** — not propagated into eight
more phases.

**The bridge is the deliverable; `CreateTopics` is its proof.** If a choice
arises between making `CreateTopics` convenient and making the mechanism correct
and reusable, the mechanism wins.

---

## 2 · Deliverables

### 2.1 Interop foundation (`Internal/Interop/`)

- **`NativeMethods.Admin.cs`** — a `partial` extension of the existing
  `NativeMethods` class (CA1060 requires one class; the existing file has 218
  `internal static extern` declarations and must stay navigable). Declare only
  what P1 uses.
- **`SafeAdminHandle.cs`** — Category 1 (ffi §B2), `ReleaseHandle` →
  `kafka_admin_AdminClient_destroy`. Wraps **both** the real and the mock client:
  `kafka_admin_MockAdminClient_new` returns the **same**
  `kafka_admin_AdminClient_t*` type.
  ⚠ **Carries the span-the-op `DangerousAddRef`** — see §4.
- **`SafeAdminPropertiesHandle.cs`** — Category 1, short-lived. Copy the shape of
  `SafeProducerPropertiesHandle`.
- **`AdminCallbacks.cs`** — `static readonly` `Cdecl` delegate fields (never
  inline lambdas — they get collected).
- **`KeyedResultMarshal.cs`** — the shared walker over
  `count` / `get_key(i)` / `get_value(i)` / `get_error(i)` / `destroy`.
  Must handle **result shape 1** (per-key value) **and shape 2** (per-key void,
  **no `get_value` function exists** — a null error *is* the success value).
  Shapes 3–6 are later phases; do not build them speculatively, but do not
  design the walker so they cannot be added.

### 2.2 `KafkaException.FromBorrowedHandle`

A **non-destroying** sibling of the existing `FromHandle`. **Mandatory, and the
single most dangerous item in the phase.**

`..._get_error(i)` returns `const kafka_common_KafkaError_t*` — the header says
verbatim: *"The pointer is borrowed from the result handle — read it with the
`kafka_common_KafkaError_*` accessors, but do **not** destroy it."* The existing
`FromHandle` **destroys** in its `finally`. Reusing it is a double-free →
process abort, invisible to every managed assertion.

⚠ **The same C type appears on both sides of the ownership line.** The
callback's `error` parameter is a **non-const** `kafka_common_KafkaError_t*` that
the callback **owns and must free** (use `FromHandle` there). Const-ness in the
header is the only signal. Get this wrong in either direction and you get a leak
or an abort.

### 2.3 Public surface (`Admin/`, namespace `Confluent.Kafka.Admin`)

First topical public folder in this binding — sanctioned by
`bindings/dotnet/CLAUDE.md §2` by name.

- **`IAdmin.cs`** — the interface (**D1: `IAdmin`, not `IAdminClient`**).
  P1 declares `CreateTopics` + `Close`; later phases add methods. Shape:

  ```csharp
  public interface IAdmin : IDisposable, IAsyncDisposable
  {
      CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics,
                                      CreateTopicsOptions? options = null);
      Task Close(TimeSpan timeout);
  }
  ```

  **D2: ONE interface. No `IAsyncAdmin`.** Every RPC method is a plain **sync**
  method returning a `*Result`; only `Close` returns a `Task`.
- **`KafkaAdminClient.cs`** — real client over `kafka_admin_AdminClient_new`.
- **`MockAdminClient.cs`** — over `kafka_admin_MockAdminClient_new(numBrokers)`
  (**D4: lands in P1** — it is the only test vehicle). Mock-specific seeding
  methods are **inherent** methods on the concrete type, never on `IAdmin`.
- **`NewTopic.cs`** — over `NewTopic_new` / `_put_config` /
  `_set_replicas_assignment` / `_destroy`.
- **`CreateTopicsOptions.cs`** — plain C# POCO (**D5**), nullable ⇒ Java
  defaults. Destructured at the P/Invoke site (the ABI has **no** `*Options_t`).
- **`CreateTopicsResult.cs`**, **`TopicMetadataAndConfig.cs`**, **`Config.cs`**,
  **`ConfigEntry.cs`** — the Java shapes.

`CreateTopicsResult` mirrors Java exactly:

```csharp
public IReadOnlyDictionary<string, Task> Values { get; }  // values() — Map<String, KafkaFuture<Void>>
public Task<Config> Config(string topic);                 // config(String)
public Task<Uuid>   TopicId(string topic);                // topicId(String)
public Task<int>    NumPartitions(string topic);          // numPartitions
public Task<int>    ReplicationFactor(string topic);      // replicationFactor
public Task         All();                                // all()
```

⚠ **Corrected 2026-09-08 (Critic 66 findings 1 & 2) — the first draft of this
sketch was wrong in two places:**

  - `Values` is **`Task`, not `Task<TopicMetadataAndConfig>`**.
    `CreateTopicsResult.java:33` holds a *private*
    `Map<String, KafkaFuture<TopicMetadataAndConfig>>`, but `:43-48` publishes
    `Map<String, KafkaFuture<Void>> values()` — the metadata is deliberately
    erased with `thenApply(v -> null)` and is reachable **only** through the four
    typed accessors. Publishing the private map widens Java's surface.
  - `ReplicationFactor` is **`int`, not `short`** — Java's result side is
    `KafkaFuture<Integer>` (`:104`) / `int replicationFactor()` (`:141`), and the
    ABI agrees (`int32_t`). `short` belongs to the **request** side
    (`NewTopic.replicationFactor()`) and does not transfer.

**Rule for every later phase: a `*Result`'s public accessor signature is the
contract, not the private field it is derived from.**

### 2.4 `Internal/AdminOperation.cs`

Per-operation state: the per-key `TaskCompletionSource` map, the `GCHandle`, the
`DangerousAddRef` bookkeeping. Modelled on the consumer's `*_async` submit
helpers, **minus** the single-op-in-flight assumption — Admin has no access
guard and permits concurrent operations.

---

## 3 · The bridge (roadmap §4.3 — the core of the phase)

```
CreateTopics(topics, options)                       ← plain sync, returns immediately
  ├─ validate preconditions (ffi §B5) BEFORE any pin/marshal or P/Invoke
  ├─ build kafka_admin_NewTopic_t[]
  ├─ create ONE TaskCompletionSource<T> PER KEY, up front,
  │    each with TaskCreationOptions.RunContinuationsAsynchronously
  ├─ GCHandle.Alloc(per-key TCS map) → user_data     ← publish BEFORE the call
  ├─ DangerousAddRef on SafeAdminHandle              ← span-the-op (§4)
  ├─ kafka_admin_AdminClient_create_topics_async(…, s_createTopicsCb, ud)
  ├─ destroy the NewTopic_t[] in a finally           ← ABI copies out; caller retains ownership
  └─ return new CreateTopicsResult(perKeyTasks)      ← Java's shape, restored

s_createTopicsCb(result, error, ud):                 ← fires EXACTLY ONCE
  ├─ try { …                                         ← TOTAL no-throw boundary
  │    if (error != IntPtr.Zero)                     ← OWNED → FromHandle (destroys)
  │        fault EVERY per-key TCS
  │    else for i in 0 .. count-1:
  │        key = Utf8Marshal.PtrToString(get_key(i))            [borrowed → copy]
  │        err = get_error(i)
  │        if (err != IntPtr.Zero)
  │            tcs[key].TrySetException(FromBorrowedHandle(err)) [BORROWED → NEVER destroy]
  │        else
  │            tcs[key].TrySetResult(Marshal(get_value(i)))      [borrowed → copy out]
  │  } finally { CreateTopicsResult_destroy(result); FreeGcHandle(ud); DangerousRelease(); }
  └─ fault any TCS left uncompleted, so no Task can hang
```

**Why the aggregate callback can serve N per-key Tasks:** the result is **fully
settled** when it fires. `src/ffi/admin.rs:3197-3206` harvests every per-key
`KafkaFuture` into `KafkaFuture::join_map_results`, and `:809` awaits it before
enqueuing the completion job.

**Deviation to document on the public surface** (`definition-of-done.md` §7):
per-key **granularity** is fully preserved; per-key **timing independence** is
not — all N `Task`s complete at the same instant, because the ABI resolved them
together. Java can complete a fast topic before a slow one. Python has the
identical limitation for the identical reason.

---

## 4 · Threading & lifetime (roadmap §4.5 — the memory-safety core)

The async callback runs on **one of three** threads:

1. the handle's **dispatcher thread** (normal);
2. **synchronously on the calling thread, before the entry point returns** — for
   `create_topics_async` / `close_async` the header documents exactly one
   trigger: *"when the RPC cannot be submitted at all (a NULL `admin` handle)."*
   ⚠ The family-wide trigger set is larger and includes **ordinary bad input**
   (unparseable base64 topic id, unknown enum code), but that claim comes from
   `src/ffi/admin.rs:62`, **not** the header — cbindgen does not emit `//!`
   module docs. *(Corrected 2026-09-08, Critic 66 Observation: an earlier draft
   attributed it to the header. The conclusion below is unchanged; only the
   citation was wrong.)* For P1's two entry points, treat NULL-handle as the
   documented inline trigger and still write the code defensively.
3. a **tokio worker thread** if the dispatcher died.

> *"So callbacks are not guaranteed to be serialised on one thread. Do not hold a
> lock across this call and re-acquire it in the callback, and publish everything
> the callback needs (including `user_data`) before calling rather than after."*

Mandatory consequences:

- **`RunContinuationsAsynchronously` on every TCS.** Case 2 makes this sharper
  than for the consumer: without it, an awaiter's continuation runs
  **synchronously inside your own P/Invoke**, on ordinary bad input.
- **Total no-throw callback body.** A managed exception unwinding into Rust is
  UB, and there is no caller frame to catch it.
- **No managed lock spanning submit-and-callback.** Publish `user_data` before
  the call.
- **Do not assume serialisation.** Keep per-op state in the `GCHandle`.

**`user_data` free site:** **no admin entry point takes a `user_data_destroy`**
(header-wide grep returns 0; no such typedef exists for admin). So this is the
**hookless one-shot** family (ffi §B6 first Rule): the **callback is the sole
owner** of the `GCHandle` free, on every path including both inline cases. The
only other free site is `AbandonBeforeSubmit`, reachable only when the
submitting P/Invoke threw so native never ran.

⚠ **`AdminClient_destroy` has NO refcount and NO drain.** `admin.rs:550-568`
shuts down the runtime, drops the client, and **detaches** the dispatcher join
handle. The header: *"Destroying concurrently with an in-flight `_async`
operation is a C lifetime precondition the caller must uphold."* The consumer
ABI ref-counts internally; **Admin's does not**. So the span-the-op
`DangerousAddRef` (held submit → callback, released in the callback's `finally`)
is **the** mechanism making `Dispose` racing an in-flight op safe rather than a
use-after-free. This is structurally the same hazard as the consumer UAF already
fixed by this pattern.

---

## 5 · Tests — the two discriminating ones are not optional

Vehicle: **`MockAdminClient`**, no broker.

### 5.1 The two highest-risk items (P1 is NOT done without these)

1. **Borrowed per-key error is never destroyed — verify BY INJECTION.**
   Deliberately destroy it, confirm the test goes **red**, revert. A double-free
   aborts the process and no managed assertion catches it, so a test that merely
   passes proves nothing about its own sensitivity.
2. **Differential ref-count test for `Dispose` racing an in-flight op.** With no
   op in flight, `Dispose` releases the native handle immediately; with one in
   flight it does **not**; completing the op then releases it. **A single-case
   assertion cannot distinguish a working ref-count from a permanently
   unbalanced one** — all three cases are required.

### 5.2 Bridge correctness

- **Mixed outcome:** a multi-key call where **some keys succeed and some fail** —
  each `Task` carries *its own* outcome. **This is the test that discriminates a
  correct implementation from one that faults everything on any failure.**
- `All()` faults if any key fails; succeeds if all succeed.
- Awaiting **one** key's `Task` works without awaiting the others.
- Top-level submit failure (non-null `error`) faults **every** per-key `Task`,
  leaving none hanging.
- Error **message and code** asserted, not just "threw" (`definition-of-done.md`
  §3).
- **Shape 2 (void)** exercised at least once: success is a *null error*.

### 5.3 Lifetime & marshalling

- `*Result_t` root destroyed exactly once, including on the throwing path.
- `GCHandle` freed exactly once, including **both** inline-callback paths (§4
  case 2).
- The synchronous-callback case does not deadlock and does not run the awaiter's
  continuation inside the P/Invoke (proves `RunContinuationsAsynchronously`).
- Aggressive GC during an in-flight op does not collect the delegate.
- Double-`Dispose` safe; post-`Dispose` call → `ObjectDisposedException`.
- Non-ASCII topic name / config value / error message round-trip (guards
  `LPStr`).
- Every `bool` parameter correct (guards a missing `MarshalAs(I1)`):
  `validate_only`, `retry_on_quota_violation`.
- `null` timeout → **negative** `timeout_ms` (client default), **not** 0.
- `MockAdminClient_new` returns NULL for `num_brokers < 1` → map to an
  exception, do not deref.
- Preconditions (`ArgumentNullException` / `ArgumentOutOfRangeException`) fire
  **before** any P/Invoke; assert `ParamName` **and** message.
- `NewTopic.set_replicas_assignment` switches constructor semantics — once
  called, `num_partitions` / `replication_factor` are **not sent**. Model the
  either/or; do not let both be set.

### 5.4 Cross-cutting

- **TFM-matrix smoke:** construct a `MockAdminClient`, run `CreateTopics`, close
  — on **net462** (via netstandard2.0), **net8.0**, **net10.0**.
- **DoD §10 (hot-path allocation audit): N/A**, stated explicitly
  (`admin-client.md` §10) — Admin is batch/administrative, no per-record path.
  **State it; never skip silently.**
- **DoD §11:** N/A to `IAdmin`, but verify its spirit — `CreateTopics` stayed a
  plain `fn`, only `Close` returns `Task`, no `async` bled into the marshallers.

---

## 6 · Definition of Done

```
cargo build --features ffi            # header MUST be byte-identical (Mode-A proof)
dotnet build -c Release --no-incremental   # 0W/0E across all 6 TFM outputs
dotnet test -f net10.0 && dotnet test -f net8.0
dotnet format --verify-no-changes
cargo xtask format-check && cargo xtask lint     # from the REPO ROOT (false-fails from bindings/dotnet)
```

Plus: no `TODO`/`FIXME`; Apache-2.0 header on every new file; the §5 tests green
with **counts confirmed**, not exit codes.

**Mode-A proof, every round:**
`git diff <base>..HEAD -- src/ cbindgen.toml target/include/confluent_kafka.h generator/`
**empty**, header hash byte-identical. If P1 hits a genuine ABI gap, **STOP and
escalate to the Manager** — it is a Rust-core dependency for the root
`actor-executor`, not a licence for `dotnet-actor` to author Rust, and not a
licence to invent a managed workaround.

---

## 7 · Commit hygiene

- Incremental commits with clear messages; fixup commits reference the original
  when closing a comment.
- **Never `git add -A`** — the repo has many pre-existing untracked files.
- **Do NOT commit:** `bindings/dotnet/COMMENTS.66.md`,
  `bindings/dotnet/COMMENTS.DONE.66.md` (local working files, §8.4 — only the
  Manager's archived copy under `design/history/M15/P1-admin-foundation/` is
  tracked), the repo-root `.claude/agents/dotnet-*.md` discovery copies,
  anything under `.claude/agent-memory/`, `bin/`, `obj/`, `.DS_Store`.

---

## 8 · ⚠ Environment traps — these fabricate FALSE PASSES

This sandbox has a broken shell init. A check can look green **because the tool
never ran**.

1. **`PATH` is clobbered.** `git`, `cargo`, `sed` all appear absent. Start every
   Bash call with
   `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"`.
   `command -v` is **not** reliable here.
2. **`grep` is aliased to `ugrep`** — rejects some patterns and emits *nothing*,
   so a pipeline reads as a pass. Use `/usr/bin/grep` for anything relied on as
   evidence.
3. **`sed` may be missing** — use `awk` or `/usr/bin/sed` explicitly.
4. **`cat` is shadowed by a missing `bat` alias** — `cat > file <<'EOF'` silently
   writes a **0-byte file and continues**. Use `/bin/cat`.
5. **This is zsh** — unquoted `$var` is not word-split; unquoted globs like
   `--include=*.cs` abort the command with "no matches found". Always quote.
6. **A test filter matching zero tests exits 0** printing `0 passed`. **Always
   confirm the expected COUNT, never the exit code.**
