# M15/P2b — `ListTopics`, `CreatePartitions`, `DeleteRecords` (the two new result mechanisms)

**Status:** **PROPOSED — NOT APPROVED.** Design decisions **D7–D10 are ruled**
(recorded in full in `../P2a-key-generalization/PLAN.md` §1); the plan itself
awaits the maintainer's sign-off. No Actor or Critic has been spawned. No source
file has been modified.

**Agent number: N = 68.** (P2a is N = 67.)

**Depends on P2a (N=67).** P2b consumes G1 — the `TKey` generalization of
`KeyedAdminOperation` — and must not start before P2a closes. `DeleteRecords` in
particular needs G1's key parser in its `Func<IntPtr, int, TKey>` form (§3.3).

**Parent roadmap:** the M15 admin roadmap under `bindings/dotnet/design/current/`
(approved 2026-09-08, decisions D1–D6). ⚠ **Deliberately UNTRACKED** — still the
governing roadmap, still to be kept updated, but **never committed**, so it is
absent from a fresh clone. Named by description rather than by path so this
tracked file carries no citation that cannot resolve.

**Base:** P2a's final commit. ⚠ **Not `f24add9e`** — P1 was squashed and
force-pushed to `6aa5fc4a`; the five original P1 commits are gone from branch
history. Mode-A proof runs against **`6aa5fc4a`**.

**Mode:** **A**. No Rust, no new ABI function, no `cbindgen.toml` change.

**Branch:** `prashah_dev_dotnet_admin`. Do NOT create a branch, merge, or rebase.

⚠ **Authorization is per-phase.** P2b needs its own go-ahead after P2a closes.

---

## 0 · What P2b is for

P2 was split by **mechanism** (D7). P2a changed how a keyed operation is
**keyed**; **P2b adds the two shapes that are not keyed operations at all**, plus
one RPC that is:

| # | Generalization | What it is | Edits P1/P2a code? |
|---|---|---|---|
| **G2** | **Optional `GetError`** | `ListTopicsResult` has **no `get_error` function at all** — the ABI stating structurally that shape 3 has no per-key failure | Yes — one field on `KeyedResultMarshal.Accessors` |
| **G3** | **Non-keyed single-future op** | Shape 3 is ONE `KafkaFuture<Map<K,V>>`, not N per-key futures | No — a new sibling type |
| — | **Composite-key / inline-scalar sub-shape** | `DeleteRecordsResult` has **no `get_key`**; its key is `(get_topic(i), get_partition(i))` and its value is an inline `int64_t` | No, **if** P2a shipped G1's parser in the `(handle, index)` form |

The third is a **new sub-shape the roadmap's §4.4 taxonomy does not name** — a
variant of shape 1 that P1's mechanism does not cover. Adding it to the taxonomy
is a Manager-owned roadmap edit, already applied.

---

## 1 · G2 + G3 — shape 3 (`ListTopics`)

### 1.1 What the ABI says, structurally

`kafka_admin_ListTopicsResult_t` exposes **`count`, `get_key`, `get_value`,
`destroy` — and NO `get_error`** (verified against the header). That absence is
the ABI stating shape 3's semantics: there is **no per-key failure**. Either the
whole call fails, through the callback's `error` parameter, or the whole map
succeeds.

`kafka_admin_AdminClient_list_topics(admin, timeout_ms, list_internal, out_result)`
also takes **no key array** — so there is no per-key pre-registration and the keys
are discovered from the response. **That is precisely why `KeyedAdminOperation`
cannot express it**: P1's design creates one `TaskCompletionSource` per key
*before* the submit, which requires knowing the keys up front.

### 1.2 The two changes

  - **G2** — ~~`KeyedResultMarshal.Accessors.GetError` becomes **nullable**,
    joining `GetValue?`, so the two nullables *encode* the shape.~~

    ⚠⚠ **THIS SPEC WAS WRONG AND THE ACTOR CORRECTLY REFUSED IT (2026-09-09) —
    the FOURTH plan defect in M15, and the first caught BEFORE implementation
    rather than after.** Making `GetError` nullable would have **recreated the
    exact hazard P2b exists to remove**, just moved from the value axis to the
    error axis — and with a *worse* failure mode: a missed null check on a value
    accessor silently drops a number, while a missed null check on an error
    accessor is a **null dereference**.

    Encoding shape in *nullness* was the root defect, so the fix could not be
    another nullable field. **Shipped instead: the absence is structural.**
    `Accessors` now carries `(CountAccessor count, IndexedAccessor getError)` —
    **no `GetValue` at all, and `getError` is not nullable** — and shape 3 gets
    its own entry point, `CompleteAggregate<TKey, TValue>`, which takes **no
    `Accessors`**. Three entry points, each naming its shape: `Complete<TKey,
    TValue>` (per-key value), `Complete<TKey>` (per-key void),
    `CompleteAggregate<TKey, TValue>` (one future over the whole map).

    **The generalizable rule, and the reason this is recorded rather than just
    fixed:** when a design's defect is *"shape is encoded in whether a field is
    null"*, the remedy is to make each shape a **distinct callable**, not to add
    another nullable. A reviewer who sees a plan proposing a second nullable
    discriminator should reject it on sight.
  - **G3** — a new `SingleAdminOperation<TValue> : AdminOperation` holding **one**
    `TaskCompletionSource<TValue>` instead of a per-key map.

⚠ **`SingleAdminOperation` reuses `AdminOperation`'s `GCHandle` / `SetHandleRef` /
`AbandonBeforeSubmit` machinery unchanged.** That base class is the part P1 got
right across three review rounds — inherit it, do **not** reimplement or
"simplify" it. The only thing that differs is the completion payload.

### 1.3 The C# surface

From `ListTopicsResult.java:39, :46, :53`:

```csharp
public sealed class ListTopicsResult
{
    public Task<IReadOnlyDictionary<string, TopicListing>> NamesToListings();  // namesToListings()
    public Task<IReadOnlyCollection<TopicListing>>         Listings();         // listings()
    public Task<IReadOnlyCollection<string>>               Names();            // names()
}
```

  - ⚠ `Listings()` and `Names()` are **`thenApply` projections of the one future**,
    not independent futures — Java derives both from `namesToListings()`
    (`:46-58`). Three independent futures would be a different shape and could
    disagree.
  - Java's `names()` returns `Set<String>`; `IReadOnlySet<T>` post-dates
    netstandard2.0, so `IReadOnlyCollection<string>` is the mapping — the row
    already fixed in `bindings/dotnet/CLAUDE.md §4`'s idiom map.

`TopicListing` — ABI: `name`, `topic_id`, `is_internal`. `ListTopicsOptions`:
`timeoutMs`, `listInternal` (`ListTopicsOptions.java:35, :47, :55`), a plain POCO
per roadmap D5.

---

## 2 · `CreatePartitions` — shape 2, already-supported mechanism

`create_partitions(admin, topics, new_partitions, count, timeout_ms,
validate_only, retry_on_quota_violation, out_result)` — **parallel arrays**: a
`const char*const*` of topic names alongside a `const NewPartitions_t*const*`.

`CreatePartitionsResult` is `count`/`get_key`/`get_error`/`destroy` — **shape 2**,
which P1 already handles and P2a's G1 keeps working. This is the one RPC in P2b
that is mechanically routine.

From `CreatePartitionsResult.java:39, :46`:

```csharp
public IReadOnlyDictionary<string, Task> Values { get; }   // values()
public Task All();                                         // all()
```

`NewPartitions` is one of only **two** input handle types in the entire Admin ABI
(`NewPartitions_new`, `add_assignment`, `destroy`). Destroy the handle array in a
`finally` after the submit — the ABI copies out and the caller retains ownership
(roadmap §7 gate 10).

⚠ **Roadmap §7 gate 6 applies here and is the subtlest thing in P2b.**
`increaseTo(n, emptyList())` is legal Java and is a **different wire request**
from `increaseTo(n)` — the broker rejects present-but-empty with
`INVALID_REPLICA_ASSIGNMENT`. So C#'s `NewPartitions` must distinguish **null from
empty** (`IReadOnlyList<IReadOnlyList<int>>?`), and a test must prove the two
produce different calls. A `?? Array.Empty<...>()` anywhere on this path is the
defect.

⚠ **`MockAdminClient` does NOT implement `createPartitions` — verified, and the
test must assert that.** `src/admin/mock_admin_client.rs` completes **every key**
exceptionally with `KafkaError::unsupported_version("Not implemented yet")`,
citing Java's `MockAdminClient.java:626-628`, per `admin-client.md §9`. So the
test asserts a per-key unsupported failure with **that exact message** — not a
success. This is a faithful translation of Java's mock, **not** a gap, and the
binding is complete with it. Do **not** implement mock behaviour the Rust mock
does not have, and do **not** treat the failure as a defect to fix.

---

## 3 · `DeleteRecords` — the composite-key / inline-scalar sub-shape

### 3.1 What makes it different

```c
int32_t     ..._count(result);
const char *..._get_topic(result, index);         /* composite key, part 1 */
int32_t     ..._get_partition(result, index);     /* composite key, part 2 */
int64_t     ..._get_low_watermark(result, index); /* INLINE value; -1 if that partition failed */
const kafka_common_KafkaError_t *..._get_error(result, index);
void        ..._destroy(result);
```

Three departures from P1's assumptions, each needing explicit handling:

1. **No `get_key`.** The key is `(get_topic(i), get_partition(i))` → a
   `TopicPartition`. ⚠ **This is why P2a's G1 parser must take
   `Func<IntPtr, int, TKey>` (result handle + index), not `Func<string, TKey>`.**
   If P2a shipped the narrow signature, P2b re-opens P2a's foundation — the exact
   churn the split exists to prevent. **Check this first; if it is wrong, raise it
   rather than working around it.**
2. **The value is an inline scalar**, not a borrowed child handle. The **error**
   is still `const`/**borrowed**, so `FromBorrowedHandle` applies unchanged.

   ⚠⚠ **P2a's seam was generalized on the KEY axis only — the VALUE axis is still
   `IntPtr`-shaped, and this is the one real gap P2a leaves for you.** Found by
   Critic 67 (finding 2, graded Low because the fix is purely additive):
   `Accessors.GetValue` is typed to return `IntPtr` and `marshalValue` is
   `Func<IntPtr, TValue>`, but `get_low_watermark(i)` returns **`int64_t`**.

   ⚠ **`getValue: null` is NOT an escape hatch.** The walker reads a null
   `GetValue` as *shape 2* (per-key void) and will **silently discard the
   watermark** — a green test suite with the value dropped. That is the specific
   trap to avoid.

   ⚠ **Adding that overload will turn `AdminKeySeamShapeTests.cs:57` RED** — it
   asserts `.Single(m => m.Name == nameof(Complete))`, so a second `Complete`
   overload breaks the `Single`. **That is expected, not a regression.** Update
   that assertion as part of the overload; do **not** re-word P2a's seam note to
   pre-empt it (re-wording a forward-looking comparative is what round-5 of
   `ffi-marshalling.md §A6` warns produces the next stale sentence). Critic 67
   observation C.

   **So P2b must either add an inline-scalar overload to the walker or bypass the
   walker for `deleteRecords`.** Prefer the overload: it is additive, touches no
   existing call site, and keeps one walker. This is *not* re-opening P2a's key
   seam — that axis is correct and P2b must not change it (Critic 67 verified a
   coherent narrowing to `Func<string, TKey>` builds clean and turns exactly the
   two reflection tests red, so the key form is load-bearing and pinned).
3. **`-1` is the documented failure sentinel** — header: *"or -1 if that partition
   failed"*. ⚠ **Drive success/failure off `get_error(i) != null`**, the
   authoritative signal — **not** off the sentinel. A legitimate low watermark of
   `-1` must not be misread as a failure. A test must pin this.

### 3.2 The C# surface

From `DeleteRecordsResult.java:40, :47`:

```csharp
public IReadOnlyDictionary<TopicPartition, Task<DeletedRecords>> LowWatermarks { get; }  // lowWatermarks()
public Task All();                                                                        // all()
```

  - `DeletedRecords` — `public DeletedRecords(long lowWatermark)` +
    `public long LowWatermark()` (`DeletedRecords.java:32, :39`). Java's accessor
    is a **method**, not a property; mirror it.
  - `RecordsToDelete` — `public static RecordsToDelete BeforeOffset(long)` +
    `public long BeforeOffset()`, plus `Equals`/`GetHashCode`/`ToString`
    (`RecordsToDelete.java:39-67`). ⚠ The value-equality members are part of Java's
    public shape; do not drop them.
  - ⚠ **Reuse the shipped `Confluent.Kafka.TopicPartition`** as the key — do not
    declare a second one. It must have correct `Equals`/`GetHashCode` for the
    dictionary; **verify** before relying on it.

The input is **three parallel arrays** — `topics`, `partitions`, `before_offsets`
— plus `count` and `timeout_ms`. Note `delete_records` takes **no** option flags
beyond the timeout, so `DeleteRecordsOptions` carries only `TimeoutMs`.

⚠ **`MockAdminClient`'s non-empty `deleteRecords` path is also unsupported —
verified.** `src/admin/mock_admin_client.rs` completes every key exceptionally
with `unsupported_version("Not implemented yet")`, citing
`MockAdminClient.java:631-638`; an **empty** request returns an empty result. So
the tests are: empty request → empty `LowWatermarks`, succeeds; non-empty →
per-key unsupported failure with that exact message.

⚠ **Consequence: the `-1`-sentinel and composite-key tests cannot be driven
through the mock's success path.** They must be exercised the way P1 exercised its
marshaller — a **direct submit with a capturing callback**, so the test owns the
result root and walks it with production's own accessor set and value marshaller,
then destroys the root once. P1's `AdminKeyedResultMarshalTests` is the template.

---

## 4 · Tests

Vehicle: **`MockAdminClient`**, no broker — except where §3.2 forces the direct
marshaller harness. P2a's closing count is the floor; confirm the **count**, never
the exit code.

### 4.1 Non-negotiable, inherited from P1's three review rounds

1. **Reflection assertions on every new public signature, sensitivity proven by
   re-injection.** ⚠ **C# upcasts and widens silently** — P1's three shape defects
   survived a green build, 883 green tests **and a 0-High first Critic pass**. A
   behavioural test cannot catch a widened signature.
2. **Borrowed-vs-owned `KafkaError`, by injection.** Per-key `get_error(i)` is
   `const`/**borrowed** → `FromBorrowedHandle`, **never destroyed**; the
   callback's `error` **parameter** is **owned** → `FromHandle`. Same C type;
   const-ness is the only signal.
3. **Span-the-op `DangerousAddRef` on every new submit** — `AdminClient_destroy`
   still has **no refcount and no drain**. The **differential** check per RPC: no
   op in flight → `Dispose` releases; op in flight → it does **not**; op completes
   → it then does. **All three cases.**
4. **`GCHandle` freed exactly once on every path**, including the inline-callback
   path.

### 4.2 P2b-specific

  - **Shape 3 aggregate failure**: `ListTopics` faults the **whole** task, not one
    key. There is no per-key error channel to fault into — assert that.
  - **Shape 3 projections agree**: `Names()` and `Listings()` are derived from
    `NamesToListings()` and cannot disagree with it. Assert on the same completed
    result, not three separate calls.
  - **G2 discriminator**: a shape-3 `Accessors` with `GetError == null` does not
    fall through to a per-key error read (which would be a null-deref).
  - **`NewPartitions` null-vs-empty** assignments produce **different** calls.
  - **`CreatePartitions` mock**: per-key failure with the exact message
    `"Not implemented yet"`.
  - **`DeleteRecords` empty request** → empty `LowWatermarks`, succeeds.
  - **`DeleteRecords` non-empty via mock** → per-key unsupported with the exact
    message.
  - ⚠ **`DeleteRecords` `-1`-with-null-error is SUCCESS**, driven through the
    direct marshaller harness (§3.2). This is the test that discriminates a
    correct implementation from one that reads the sentinel as failure.
  - **`DeleteRecords` composite key**: `(topic, partition)` round-trips into the
    right `TopicPartition` dictionary key, including two partitions of the same
    topic and the same partition index across two topics.
  - **P2a regression**: P2a's and P1's tests pass **unmodified**, and one earlier
    defect re-injected through the G2-modified `Accessors` still goes red.

### 4.3 Cross-cutting

  - **TFM-matrix smoke** on net462 (via netstandard2.0), net8.0, net10.0.
  - **DoD §10 (hot-path allocation audit): N/A** — Admin is batch/administrative
    with no per-record path (`admin-client.md §10`). **State it; never skip
    silently.**
  - **DoD §11: N/A** to `IAdmin`, spirit verified — every new RPC is a plain sync
    `fn` returning a `*Result`; only `Close` returns `Task`; no `async` in the
    marshallers.

---

## 5 · Definition of Done

```
cargo build --features ffi            # header MUST be byte-identical (Mode-A proof)
dotnet build -c Release --no-incremental   # 0W/0E across all 6 TFM outputs
dotnet test -f net10.0 && dotnet test -f net8.0
dotnet format --verify-no-changes
cargo xtask format-check && cargo xtask lint     # from the REPO ROOT (false-fails from bindings/dotnet)
```

Plus no `TODO`/`FIXME`, Apache-2.0 header on every new file, §4's tests green with
**counts confirmed**.

**Mode-A proof, every round:**
`git diff 6aa5fc4a..HEAD -- src/ cbindgen.toml target/include/confluent_kafka.h generator/`
**empty**, header hash byte-identical. A genuine ABI gap **STOPS the phase and
escalates to the Manager** as a Rust-core dependency — `dotnet-actor` does not
author Rust and does not invent a managed workaround.

---

## 6 · Commit hygiene

  - Incremental commits; `fixup!` referencing the original when closing a comment.
  - **Never `git add -A`.**
  - **Do NOT commit:** `COMMENTS.68.md` / `COMMENTS.DONE.68.md` (local working
    files — only the Manager's archived copy under
    `design/history/M15/P2b-list-partitions-records/` is tracked); the repo-root
    `.claude/agents/dotnet-*.md` discovery copies; anything under
    `.claude/agent-memory/`; `bin`/`obj`; `.DS_Store`.
  - ⚠ **`design/current/PLAN-M15-admin-client.md` stays UNTRACKED.**

---

## 7 · The rule that produced P1's only real defects

⚠ **THE PLAN IS NOT REVIEW GROUND TRUTH.** P1's plan sketch was wrong **twice**,
and only the Java source caught it — after a green build and a 0-High first
review.

**A `*Result`'s public accessor signature is the contract, not the private field
it is derived from.** `CreateTopicsResult.java:33` is a private
`Map<String, KafkaFuture<TopicMetadataAndConfig>>`; `:43-48` publishes
`Map<String, KafkaFuture<Void>> values()`, erasing the metadata deliberately.

Every signature in §1.3, §2 and §3.2 was quoted from a Java **public accessor**
with a line citation, precisely so it can be checked. **Where this plan and the
Java source disagree, the Java source wins and this plan is the defect** — report
it rather than implementing it.

The Critic's ground truth is the **C ABI header** + the **Java public API shape**
(`bindings/dotnet/CLAUDE.md §8.2`) — not Rust internals, not Java implementation
logic, and not this document.

---

## 8 · ⚠ Environment traps — these fabricate FALSE PASSES

A check can look green **because the tool never ran**.

1. **`PATH` is clobbered.** `git`, `cargo`, `sed` all appear absent. Start every
   Bash call with
   `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"`.
   `command -v` is **not** reliable here.
2. **`grep` is aliased to `ugrep`** — rejects some patterns and emits *nothing*, so
   a pipeline reads as a pass. Use `/usr/bin/grep` for anything relied on as
   evidence.
3. **`sed` may be missing** — use `awk` or `/usr/bin/sed` explicitly.
4. **`cat` is shadowed by a missing `bat` alias** — `cat > f <<'EOF'` silently
   writes a **0-byte file and continues**. Use `/bin/cat`.
5. **This is zsh** — unquoted `$var` is not word-split; unquoted globs abort the
   command with "no matches found". Always quote.
6. **A test filter matching zero tests exits 0** printing `0 passed`. **Confirm the
   expected COUNT.**
7. ⚠ **An ABORTED test run ALSO exits 0** — P1's discovery. A double-free aborts
   the test host and `dotnet test` still returns 0; **`Test Run Aborted` in the
   output is the only signal.** Never certify a run by its exit code.

### 8.1 A FALSE-FAIL trap — do not burn a round on it

Two **pre-existing consumer** tests assert
`completingThreadId != continuationThreadId`, which is **unsound** because managed
thread ids are **recycled**:

  - `ConsumerPollBridgeTests.ResultBridge_RunsContinuationsAsynchronously_OffTheCompletingThread`
    (last touched by `26761aa4`, M6) — seen failing with both ids equal to 30.
  - `ConsumerRebalanceListenerBridgeTests.DisposeAsync_WithLiveRegistration_ReturnsWithoutHanging`
    (last touched by `b25bf7f0`, M9/P6).

**Neither file is in P2b's scope.** If one goes red it is **not** a P2b regression
— re-run, confirm, move on. Fixing them is a separate consumer-side slice.
