# Critic 85 — M15/P13.2 — resolved

## CP1 — configs (F1, G2-1, G2-4), commit `2070667f`

---

### 85.1 — minor — IAC's countdown is armed with the caller map's key count, not the ABI's distinct-resource count. CP1 extends a leaked operation to every collision that involves a zero-op key.

**Where:** `src/Confluent.Kafka/Internal/NativeAdminClient.cs:2189-2193`

```csharp
// … Every key is named by at least one row — its ops,
// or its sentinel — and the keys are a map's, so the count is the key count.
operation.SetPendingCallbacks(keys.Count);
```

Here `keys` is built by iterating the caller's `configs` (`:2077`, `:2101`).

**Evidence:**
- **Header** (`confluent_kafka.h:4930-4932`, `…_incremental_alter_configs_async`): the callback
  "is called once per **distinct** resource named across the input rows". The premise matches:
  `distinct_config_resources` de-duplicates on `(for_id(type), name)`.
- **Java:**
  - `KafkaAdminClient.java:2893-2895` puts each resource into a value-equality `HashMap`.
  - `:2886` returns `new AlterConfigsResult(new HashMap<>(allFutures))`.
  - So Java has exactly one future per distinct resource, whatever `Map` the caller passed, and
    it has nothing to leak.
- **.NET's own contract:** `KeyedAdminOperation`'s constructor documents `keys` as "already
  de-duplicated by the caller" (`Internal/AdminOperation.cs:263-266`).
  - It de-duplicates internally with `s_configResourceComparer` = `EqualityComparer<ConfigResource>.Default` (`:91-92`).
  - The count it is armed with is **not** de-duplicated.

**Why it is wrong:** `configs` is a public `IReadOnlyDictionary<ConfigResource, …>`, so a caller
can pass a map whose comparer is not `ConfigResource`'s value equality, such as
`new Dictionary<…>(ReferenceEqualityComparer.Instance)`. Two equal resources then give:

| Quantity | Value |
|---|---|
| `keys.Count` | 2 |
| `Tasks` entries | 1 |
| ABI callbacks | 1 |

The countdown never reaches zero. The `GCHandle` stays rooted and the span-the-op client reference
is never released, so `Dispose` never destroys the native client. The caller's `Task` does
complete, so nothing hangs; the defect is a permanent leak.

I measured this with a scratch console probe against the built DLL, using `MockAdminClient`
followed by `Dispose` and then reading `IsClosed` by reflection:

| Two equal keys in a reference-comparer map | `f7d4547b` (before CP1) | `2070667f` (CP1) |
|---|---|---|
| zero-op + zero-op | released | **leaked** |
| ops + zero-op | released | **leaked** |
| ops + ops | leaked (pre-existing) | leaked |
| control: 1 key, default comparer | released | released |

Before CP1, zero-op keys were subtracted from the count and completed locally, so collisions
involving them balanced. F1 moved them into the count without de-duplicating it. The code comment
"the keys are a map's, so the count is the key count" is therefore false for any map whose comparer
is not value equality.

**Expected fix:**
1. Arm the countdown with the number of keys that are distinct under the operation's own comparer:
   `operation.SetPendingCallbacks(operation.Tasks.Count)`. That comparer is the value equality the
   ABI de-duplicates on. This also closes the pre-existing ops + ops shape.
2. Correct the `:2189-2192` comment.
3. Add a test with a non-value-comparer map. `ReferenceEqualityComparer` is net5+, so on net462 use
   a small custom comparer. Cover three rows: zero-op + zero-op, ops + zero-op, and ops + ops. In
   each row assert a single `Values` entry, that the entry settles, and that `handle.IsClosed` is
   true after `Dispose`.

Out of scope and **not** part of this item: names that differ only in lone surrogates encode to the
same UTF-8 and collide in the ABI too. That residual affects every string-keyed per-key RPC and
predates this phase.

**Original commit:** `2070667f`


**Resolution (Actor 85):** fixed in fixup `d067deba` (`fixup! feat(dotnet): M15/P13.2 CP1 — …`).
- `src/Confluent.Kafka/Internal/NativeAdminClient.cs` (`IncrementalAlterConfigs`): the
  countdown is armed with `operation.SetPendingCallbacks(operation.Tasks.Count)`. Verified
  first: `Tasks` is keyed by `s_configResourceComparer` = `EqualityComparer<ConfigResource>.Default`
  (type + ordinal name), and the ABI fans out over `distinct_config_resources`, which
  de-duplicates on `(for_id(type), name)`; G2-1's ctor normalization makes the two agree
  (managed-equal ⇒ ABI-equal, so `Tasks.Count` never under-counts). The countdown comment is
  corrected, and the method remark now qualifies "two resources can never interleave" as
  holding under the caller map's own comparer (the ABI merges rows by value).
- Test (`tests/…/Interop/AdminConfigsLifetimeTests.cs`):
  `EqualResourcesInANonValueEqualityMap_AreOneResource_AndReleaseTheOperation`, a theory over
  zero-op + zero-op, ops + zero-op and ops + ops, through the real ABI with a private
  reference comparer. Each row asserts one `Values` entry, that it settles, and that the
  handle closes after `Dispose`. Mutation `Tasks.Count` → `keys.Count`: 3/3 red (and red at
  2070667f before the fix).
- The release is asserted through a bounded `DisposeAndAwaitRelease` helper: the trampoline
  resolves the key's `Task` before its `finally` releases the operation, so an awaiter can
  reach `Dispose` first (measured with a probe on a correctly released operation). CP1's
  `AnUndefinedTypeCollidingWithUnknown_SettlesAndReleases_ThroughTheRealAbi` had the same
  race and uses the helper too.
- Lone-surrogate collision: out of scope, untouched.

---

### 85.2 — minor — The newly public 8-arg `ConfigEntry` ctor accepts a null synonym element, and then `Equals`, `GetHashCode` and `ToString` throw `NullReferenceException`. Java's do not throw.

**Where:** `src/Confluent.Kafka/Admin/ConfigEntry.cs`
- `:173`: the only guard, which checks for a null *list*.
- `:246` → `SynonymsEqual` `:319` (`left[index].Equals(…)`).
- `:266`: `synonym.GetHashCode()`.
- `:290`: `_synonyms[index].ToString()`.

**Evidence:**
- **Java:** `ConfigEntry.java:59-75` stores `synonyms` as given. Its `equals`, `hashCode` and
  `toString` delegate to `List.equals`, `List.hashCode` and `List.toString`, which all tolerate a
  null element.
- **The binding's sibling public ctor:** `Config(IEnumerable<ConfigEntry>)` rejects a null element
  up front (`Admin/Config.cs:48-53`, "Config entries must not contain a null element.").
- **Reachable with no warning:** `ConfigSynonym`'s ctor is internal, but
  `new ConfigEntry(…, new ConfigEntry.ConfigSynonym[1], …)` compiles without a nullable warning.
  The probe against `2070667f` printed:
  - "ctor accepted a null synonym element"
  - `Equals` threw `NullReferenceException`
  - `GetHashCode` threw `NullReferenceException`
  - `ToString` threw `NullReferenceException`

**Why it is wrong:** Before G2-4 only the result marshaller could reach this ctor, and it never
produces a null element. Publishing the ctor makes a value type constructible whose `Equals`,
`GetHashCode` and `ToString` throw. That breaks .NET's rule that `Equals` and `GetHashCode` do not
throw, and it breaks any dictionary or `AlterConfigOp` equality that contains the entry. Java does
not do this.

**Expected fix:** pick one of these and record it next to the D8 deviation in the ctor remarks:
- **Reject it in the ctor**, matching D8's stricter-precondition choice and the `Config` precedent:
  `ArgumentException` with `ParamName == "synonyms"` and an exact message.
- **Make `SynonymsEqual`, `GetHashCode` and `ToString` null-tolerant**, which is Java-faithful:
  `ToString` renders `null`.

Either way, add a test that asserts the exact message and `ParamName`, or the tolerant behaviour of
all three members.

**Original commit:** `2070667f`

**Resolution (Actor 85):** fixed in fixup `d067deba` — the reject option, per the Manager's
ruling under D8 (the equality members were not made null-tolerant).
- `src/Confluent.Kafka/Admin/ConfigEntry.cs`: the 8-argument ctor rejects a null element with
  `ArgumentNullException`, `ParamName == "synonyms"`, message "Config synonyms must not contain
  a null element." — the `Config(IEnumerable<ConfigEntry>)` precedent. Recorded in the ctor
  remarks next to the D8 null-list note, as the same stricter-than-Java deviation.
- Copy: the ctor **stored the caller's list by reference** at `2070667f`, so the guard could
  be bypassed by mutating it afterwards. It now copies into a `ReadOnlyCollection` (so
  `Synonyms` cannot be cast back to a writable array); an empty list stays
  `Array.Empty<ConfigSynonym>()`.
- Tests (`tests/…/PublicAdminShapeParityTests.cs`):
  `ConfigEntry_TheEightArgumentConstructor_RejectsANullSynonymElement` (leading and trailing
  null; type, `ParamName` and exact message) and
  `ConfigEntry_TheEightArgumentConstructor_CopiesTheSynonyms` (mutate the caller's list after
  construction; `Synonyms`, `Equals`, `GetHashCode` and `ToString` are unchanged).
  Mutations: dropping the element check turns the first red; storing the caller's list, or
  exposing the bare copied array, turns the second red.

---

## CP1 re-review — fixup d067deba

### 85.3 — nit — The new countdown comment says the bridge's equality is the ABI's. That holds in one direction only, and the other direction is a live hang.

**Where:** `src/Confluent.Kafka/Internal/NativeAdminClient.cs:2196-2199`

> "…so the count is the number of DISTINCT keys, which is `Tasks.Count`: the bridge is keyed by
> `s_configResourceComparer`, the same value equality the ABI de-duplicates on…"

**Evidence:**
- The ABI does not compare the managed string. It compares what it reads back from the managed
  string:
  - `distinct_config_resources` takes `CStr::from_ptr(name).to_string_lossy()`
    (`src/ffi/admin.rs:4789`), so the name stops at the first NUL.
  - `Utf8Marshal.Pin` encodes the name with `Encoding.UTF8.GetBytes`, which turns every lone
    surrogate into U+FFFD.
  - So two names that differ under ordinal comparison can be one ABI resource.
- Probe at `d067deba`: a real-ABI `IncrementalAlterConfigs` with two `Group` resources, one op
  each, in a default-comparer map. After a 2 s wait on `Values[first]`, I called `Dispose` and
  then waited 5 s for `IsClosed`:

| Names | `Tasks` | `Values[first]` | Handle closed |
|---|---|---|---|
| `"x\0a"`, `"x\0b"` | 2 | **pending** | **false** |
| `"\uD800"`, `"\uDBFF"` | 2 | **pending** | **false** |
| control: `"same"`, `"same2"` | 2 | ok | true |

- The ABI sends one callback, keyed `"x"` or `"�"`. That key matches neither `Task`. The
  countdown is armed at 3 and reaches only 1, so `FailUncompleted` never runs. Both `Task`s hang
  forever, and the client leaks.

**Why it is wrong:** this comment is the safety argument for the arm, and it states an
equivalence. Only "managed-equal ⇒ ABI-equal" holds. That direction is the one that rules out
under-counting and a use-after-free, and the Actor's DONE note states it correctly. The converse
fails for names that do not survive NUL-terminated UTF-8. When it fails the result is a hang, the
outcome root `CLAUDE.md` §5 calls worse than an explicit error. A reader who trusts "the same value
equality" will not look for it.

The defect itself is not new: at `f7d4547b` the same inputs armed at 2 + 1 as well. It is the
cross-RPC lossy-name residual that 85.1 scoped out, and embedded NUL is a second trigger for it. I
am **not** asking for it to be fixed in this phase.

**Expected fix:** doc-only. Reword `:2196-2199` to the direction that holds. For example: managed
equality implies the ABI's `(for_id(type), name)` equality, so `Tasks.Count` never under-counts.
The converse fails only for names that do not round-trip through NUL-terminated UTF-8, which is the
pre-existing cross-RPC lossy-name residual. The fix can ride with the next checkpoint's fixup.

**Original commit:** `2070667f` (the text was introduced by its fixup `d067deba`).

**Resolution (Actor 85):** fixed in fixup `4f222126` (`fixup! feat(dotnet): M15/P13.2 CP1 — …`), doc-only.
- `src/Confluent.Kafka/Internal/NativeAdminClient.cs` (`IncrementalAlterConfigs`, the countdown
  comment above `operation.SetPendingCallbacks(operation.Tasks.Count)`): the "same value equality"
  claim is replaced by the one direction that holds — two keys equal under
  `s_configResourceComparer` are equal under the ABI's `(for_id(type), name)` — and why that makes
  the arm safe: the ABI can never fire more callbacks than `Tasks.Count`, so the countdown never
  under-counts and the operation is never freed before its last callback.
- One clause records that the converse fails for names that collapse under NUL-terminated UTF-8
  (an embedded NUL, lone surrogates encoded to U+FFFD) as a known pre-existing cross-RPC residual,
  not handled here. No code behaviour changed; the hang the probe shows is left as is.

---

## CP3 — topic value types and results, commit `3f802a2c`

### 85.4 — minor — `CreateTopicsResult.Values` is a construction-time snapshot, while `All()` and the four typed accessors read the caller's live map, and the new remark says the map is "held by reference, as Java holds its map"

- **File:line:** `bindings/dotnet/src/Confluent.Kafka/Admin/CreateTopicsResult.cs:62-63` (the remark), `:72` (`_values = futures`), `:82-89` (the `_erasedValues` copy), `:110` (`Values => _erasedValues`), `:117` and `:177` (`All()` / `Apply` read `_values`).
- **Evidence:** I added a throwaway probe to the unit-test project, detached at 3f802a2c, and it passed. The probe builds `var map = new Dictionary<string, Task<TopicMetadataAndConfig>>()` and `var r = new CreateTopicsResult(map)`, then sets `map["t"] = pending.Task`. Afterwards `r.Values` is empty, while `r.All()` and `r.NumPartitions("t")` are both pending on the new entry. The result disagrees with itself.
  - A comparer variant has the same root cause. With an `OrdinalIgnoreCase` map, `r.Values["T"]` misses, because the snapshot is rebuilt with `StringComparer.Ordinal`. `r.NumPartitions("T")` hits through the caller's comparer.
- **Why it is wrong:** before this commit the ctor was `internal`, and every caller handed over a fresh, never-mutated map, so the snapshot and the live reads could not diverge. CP3 made the ctor public (D5) specifically so that "a test or a mock can fabricate a result", which makes the divergence observable. The same commit documents Java's reference semantics.
  - Java has no such split. `CreateTopicsResult.java:35-37` stores the map, and `values()` (`:43-46`) recomputes from the live `futures` map on every call, as `all()` (`:51-53`) and the accessors do. So all Java views always agree.
  - In .NET only four of the five views honour the "held by reference" claim.
  - The sibling ctors this commit made public, `DeleteTopicsResult` and `DescribeTopicsResult`, hold the caller's maps directly, so they are consistent. `CreateTopicsResult` is the odd one out.
- **Expected fix:** make all five views read the same source. Either:
  - make `Values` a live erasing view over `_values` (a small `IReadOnlyDictionary<string, Task>` adapter, or a recompute per call as Java does); or
  - copy `futures` once at construction into an ordinal `Dictionary` and have `All()` and `Apply` read that copy.

  Then correct the remark to state whichever semantics was chosen. If the live form is chosen, extend a `PublicAdminTopicResultConstructionTests` case to mutate the map after construction and assert that `Values`, `All()` and one typed accessor agree.
- **Original commit:** 3f802a2c.

**Resolution (Actor 85):** fixed in fixup `3d35f27f` (`fixup! feat(dotnet): M15/P13.2 CP3 — …`).
I took the Manager's direction, the first option: `Values` mirrors Java's `values()`, which is
rebuilt from the live map on every call.
- `src/Confluent.Kafka/Admin/CreateTopicsResult.cs`:
  - The cached `_erasedValues` field and its copy in the constructor are removed. The constructor
    now only null-checks and stores `futures`.
  - The `Values` getter builds a new `Dictionary<string, Task>(StringComparer.Ordinal)` from
    `_values` on each access, so it reads the same source as `All()` and `Apply`.
  - Each entry is still the same `Task` instance, upcast, so no second `Task` is created.
- The remarks now state three things, citing `CreateTopicsResult.java`:
  - Each access returns a fresh snapshot of the live map (`values()` at `:43-46`).
  - Membership follows the caller's map. A snapshot already taken keeps its entries.
  - A non-ordinal caller comparer gives Java's own split. `Values` is ordinal, like the JDK
    `HashMap` that `Collectors.toMap` collects into. The typed accessors use the caller's map, like
    `futures.get` at `:66`, `:79`, `:92` and `:105`.
- The constructor remark now says every view, `Values` included, reads the map when called
  (`:35-37`).
- The existing `values()` cite is corrected from `:43-48` to `:43-46`.
- A code comment records one edge. Two ordinally equal keys make `Add` throw, and only a caller
  comparer that tells equal strings apart can produce them. Java's `toMap` throws on the same input,
  with `IllegalStateException`. I recorded this and did not change it.

Tests, in `PublicAdminTopicResultConstructionTests`:
- `CreateTopicsResult_CallerMapMutatedAfterConstruction_AllViewsAgree`: the test adds a pending
  entry, completes it, replaces it, and then adds a faulted one. After each step `Values`, `All()`
  and `NumPartitions` agree.
- `CreateTopicsResult_Values_EachAccessReflectsTheMapAtThatMoment`: three accesses, taken around an
  add and a remove. Each one reflects the map as it was when it was taken.
- `CreateTopicsResult_CaseInsensitiveCallerMap_SplitsAsJavaDoes`: with an `OrdinalIgnoreCase` map,
  `Values["T"]` misses and `NumPartitions("T")` hits.

Mutations:
- I ran the tests first against the unchanged `3f802a2c` file, which is the construction snapshot
  verbatim. The first two tests failed, with `Assert.Single() … empty` and `Expected: 2 Actual: 1`.
  The third passed, as expected: that split already held.
- Building the per-access snapshot with `OrdinalIgnoreCase` failed only the third test, with
  `Assert.False()`. I restored the file from a byte-identical snapshot.

Survey:
- The only reader of `CreateTopicsResult.Values` outside the type is
  `grpc-server/AdminServiceImpl.cs:167`, which enumerates it once in a `foreach`. `src/` has none.
- No test depends on `Values` returning the same instance twice, and none reflects on
  `_erasedValues`.
- `DeleteTopicsResult` and `DescribeTopicsResult` do not cache a view of a caller's map. Their
  public constructors store the caller's maps, and `DescribeTopicsResult.AllTopicNames()` /
  `AllTopicIds()` rebuild their aggregate on every call.
- `DeleteTopicsResult.Erase` copies once, but only on the internal factory path. There its source
  is the bridge's `KeyedAdminOperation.Tasks`, which is never mutated after construction.

Gates:
- `cargo build --features ffi`: header sha `a7560aa3…`. The Mode-A diff is 0 lines, and there are
  700 externs.
- Debug build: 0 warnings, 0 errors, 6 outputs. The grpc-server builds with 0 warnings and 0 errors.
- The touched class passed 9/9 on net10.0 and on net8.0.
- The full net10.0 suite passed 2400/2400 (2397 + 3), with no aborts.
- `dotnet format --verify-no-changes`: clean.

## CP3 re-review — fixup 3d35f27f

### 85.5 — nit — `DescribeTopicsResult`'s aggregate is built with `Dictionary.Add`, so a user-built result whose map holds two ordinally-equal names faults `AllTopicNames()` where Java's `all()` succeeds

- **File:line:**
  - `bindings/dotnet/src/Confluent.Kafka/Admin/DescribeTopicsResult.cs:219` has `descriptions.Add(entry.Key, entry.Value.Result)`. It is reached through the public ctor at `:95-100` via `AllTopicNames()` at `:156` and `AllTopicIds()` at `:168`.
  - The remark at `:72-75` says the aggregate has "Java's own key semantics for a `String` / `Uuid` `HashMap`".
- **Evidence:** I ran a probe against the HEAD Debug `Confluent.Kafka.dll`:

      var m = new Dictionary<string, Task<TopicDescription>>(ReferenceEqualityComparer.Instance);
      m[new string('t', 1)] = Task.FromResult(d1);
      m[new string('t', 1)] = Task.FromResult(d2);
      await new DescribeTopicsResult(null, m).AllTopicNames();
      // → faulted: ArgumentException "An item with the same key has already been added. Key: t"
      //   (TopicNameValues.Count == 2)

- **Why it is wrong:**
  - Java's `all(Map)` (`DescribeTopicsResult.java:97-114`) fills `new HashMap<>(futures.size())` with `descriptions.put(entry.getKey(), entry.getValue().get())` (`:105`). `put` replaces on an equal key and never throws.
  - So a Java test that builds the result through the `// VisibleForTesting` protected ctor (`:36-37`, the route D5 publishes) with an `IdentityHashMap` holding two equal-content names gets a successful `allTopicNames()` with one entry. .NET turns that success into a fault.
  - This is not the `Values` case above. There Java also fails, because `toMap` throws. Here Java has no `toMap`.
  - It is newly reachable in this phase. Before `3f802a2c` the only callers were `OfTopicIds` / `OfTopicNames`, whose aggregate comparer is the map's own bridge comparer, so no two keys could collide and `Add` behaved exactly like the indexer. `3f802a2c` pairs a caller map with any comparer with a fixed `StringComparer.Ordinal` aggregate, and that makes the collision reachable. It needs a comparer finer than ordinal: `ReferenceEqualityComparer` is in the BCL on net5+, and a two-line custom comparer works on net462. For `Uuid`, a `readonly struct`, only a broken comparer reaches it, so the `string` path is the real one.
  - The `:72-75` claim is half true. The key equality matches Java's `HashMap`; its handling of an equal key does not.
- **Expected fix:**
  - At `:219`, write `descriptions[entry.Key] = entry.Value.Result;`. That is `HashMap.put` semantics: the last-enumerated value wins. Add a one-line comment citing `DescribeTopicsResult.java:105`.
  - The client-built path is unchanged, because the bridge comparer admits no duplicates.
  - I proved it in the throwaway worktree. With only that line changed, the probe's `AllTopicNames()` completes with `Count == 1`, and the Describe plus topic-result filter stays green at 223/223.
  - Add a case to `PublicAdminTopicResultConstructionTests`. Build a map with a reference-equality `IEqualityComparer<string>` that holds two distinct `"t"` instances, and assert that `AllTopicNames()` completes with exactly one entry. Use a private comparer class built on `ReferenceEquals` and `RuntimeHelpers.GetHashCode`, so the test compiles on the net462 leg.
  - Leave `CreateTopicsResult.Values` as it is. Java throws there as well.
  - *Sibling, pre-existing, not this phase:* `DescribeConsumerGroupsResult.cs:119` and `DescribeClassicGroupsResult.cs:119` use the same `Add` behind public ctors that predate P13.2 (`0204437a`). Their Java `all()` methods also use `put` (`DescribeConsumerGroupsResult.java:54`, `DescribeClassicGroupsResult.java:54`). Apply the same one-token change there too if convenient, but do not gate CP3 on them.
- **Original commit:** 3f802a2c.

**Resolution (dotnet-actor 85, fixup `b0c8e637` of `3f802a2c`):**
- `DescribeTopicsResult.cs` `Gather`: `descriptions.Add(entry.Key, entry.Value.Result)` is now
  `descriptions[entry.Key] = entry.Value.Result;`, with a comment citing
  `DescribeTopicsResult.java:105` (`HashMap.put`: an equal key is replaced, the last-enumerated
  value wins). One loop serves both `AllTopicNames()` and `AllTopicIds()`, so both aggregates are
  fixed.
- The public-ctor remark no longer stops at "Java's own key semantics". It now says the
  collision handling is Java's too: where the supplied comparer is finer than the aggregate's,
  the entries collapse to one and the last-enumerated value wins rather than faulting. The
  client-built path is unchanged, since its bridge comparer admits no duplicates.
- New test `PublicAdminTopicResultConstructionTests.DescribeTopicsResult_TwoOrdinallyEqualNames_AggregateKeepsTheLastValue`:
  a private `ReferenceComparer : IEqualityComparer<string>` (`ReferenceEquals` +
  `RuntimeHelpers.GetHashCode`, so it compiles on net462) holding two distinct `new string('t', 1)`
  instances. `AllTopicNames()` completes with exactly one entry, key `"t"`, whose value is the
  last-enumerated entry's value.
- Mutation: with `Add` restored the test goes red with `System.ArgumentException : An item with
  the same key has already been added. Key: t` (1 failed / 9 passed in the class). Restored from a
  snapshot.
- Gates: header sha `a7560aa3…` unchanged; Debug solution build 0W/0E; the class plus the
  DescribeTopics filter passed 27/27 on net10.0, and the class passed 10/10 on net8.0;
  `dotnet format --verify-no-changes` clean on both files.
- Not touched, as directed: `DescribeConsumerGroupsResult.cs:119` and
  `DescribeClassicGroupsResult.cs:119` keep `Add` (pre-existing, not this phase).

## CP4 — TopicPartition, commits b0c8e637 + 9b3c80f0

### 85.6 (minor) — false remark: a throwing key reader is said to "leave the key's task pending … rather than a hang"

- **File:** `tests/Confluent.Kafka.UnitTests/PublicAdminNegativePartitionTests.cs:34-38`
- **Evidence:** The class remark says a result reader "that still rejected a negative partition would throw inside the no-throw callback boundary and leave the key's task pending — which the bounded wait here turns into a failure rather than a hang." That is not what the code does.
  - `CompletePerKeyVoid` in `AdminCallbacks.cs` swallows the reader's throw and still calls `ReleaseOne`.
  - At zero, `FailUncompleted` (`AdminOperation.cs:340-354`) faults every unanswered key. The fault is a code-0 `KafkaException`: "The alterPartitionReassignments result contained no entry for '…'."
  - Measured with M1 (the reader rejecting a negative partition): both `AlterPartitionReassignments` tests go red in 3-5 ms with `Expected: 3 / 17, Actual: 0`. The key's task never stays pending, and the 30 s deadline plays no part.
- **Why it matters:** The remark names the wrong witness. What catches the regression is the **Code and message assertions**, not the bounded wait. A test tightened to rely only on `Assert.ThrowsAsync<KafkaException>` would pass the M1 mutant: the countdown fault is itself a `KafkaException`. A maintainer who trusts the remark could drop the assertions that do the work.
- **Fix:** Rewrite the remark. Say that the countdown's `FailUncompleted` faults an unanswered key with a code-0 "result contained no entry" `KafkaException`, and that the `Code` and message assertions are what tell that fault apart from the core's per-key error.
- **Original commit:** `9b3c80f0`

**Resolution (dotnet-actor 85, fixup `5cc92f66` of `9b3c80f0`):**
- `PublicAdminNegativePartitionTests.cs:36-44` (class remark): the "leave the key's task pending … rather than a hang" sentence is replaced. It now says the per-callback no-throw boundary swallows the reader's throw and still releases the countdown, and that at zero `AdminOperation.FailUncompleted` faults the unanswered key at once, with a code-0 `KafkaException` "The alterPartitionReassignments result contained no entry for '…'.". It names the `Code` and message assertions on the negative key, not the bounded wait, as what tells that fault apart from the core's per-key answer.
- Assertion audit, all three tests in the file:
  - `Mock_AlterPartitionReassignments_…`: the negative key already asserted `Code` 3 and the exact message. No change.
  - `RealClient_ListPartitionReassignments_…`: the whole-request fault already asserted `Code` 17 and the exact message. No change.
  - `RealClient_AlterPartitionReassignments_…`: the per-key fault already asserted `Code` 17 and the message, but the `All()` leg was a bare `Assert.ThrowsAsync<KafkaException>`, which the countdown's code-0 fault would also satisfy. It now captures the exception and asserts `Code` 17 and the exact message too (`:132-137`). This is a test strengthening, not a behaviour change.
- Gates: 3/3 in the class on net10.0; the full suite passed 2434/2434 on net10.0 and on net8.0.

### 85.7 (nit) — the ConsumerHandle witness gives the wrong reason for the core failure

- **Files:** `tests/Confluent.Kafka.UnitTests/ConsumerNegativePartitionGuardTests.cs:41-44` (class remark) and `:161-162` (in-test comment)
- **Evidence:** Both say that without the guard these handle ops would fail in the core with a `KafkaException` "for a partition this consumer does not own". The handle comes from a `MockConsumer` (`NewMock()`, `:158-159`).
  - On a MockConsumer handle, `seek_with_offset`, `seek_with_offset_and_metadata`, `position` and `position_with_timeout` all go through `async_state()` (`src/consumer/async_kafka_consumer.rs` ~399-408).
  - That returns `unsupported_version`, "ConsumerHandle async operations are not supported on a MockConsumer handle; drive the MockConsumer directly.", **whatever the partition**. ffi §B5 documents this as core behaviour.
  - Measured with `ValidatePartition` disabled: all four ops throw that `UnsupportedVersion` `KafkaException`.
- **Why it matters:** The witness itself is sound: without the guard, `ArgumentOutOfRangeException` is not thrown, so the test goes red. Only the stated reason is wrong. It does misdescribe the core's contract for mock handles, which ffi §B5 asks tests to assert rather than paper over.
- **Fix:** Reword both places. Say the op would reach the core and fail there with `UnsupportedVersion` (every async op on a mock-derived handle does), so the `ArgumentOutOfRangeException` proves the guard ran first.
- **Original commit:** `9b3c80f0`

**Resolution (dotnet-actor 85, fixup `5cc92f66` of `9b3c80f0`):**
- `ConsumerNegativePartitionGuardTests.cs:37-47` (class remark) and `:164-166` (in-test comment): the handle-op reason now reads that without the guard the ops would reach the core, which answers them on a `MockConsumer`-derived handle with an `UnsupportedVersion` `KafkaException` whatever the partition (core behaviour, ffi §B5). So an `ArgumentOutOfRangeException` from a handle op proves the guard ran first. The witness itself is unchanged.
- Same paragraph, found while rewriting it: "`ThrowIfClosed`, the gate in front of every native call" was false (`NativeConsumer.Wakeup` checks `_closed` itself and returns, and `CloseSync` gates on its own `TryBeginClose` latch instead). It now reads "each guard exercised here on a disposed consumer runs before `ThrowIfClosed`, the closed-consumer gate those operations pass before their native call", which is the fact the disposed-consumer tests witness.
- Gates: 7/7 in the class on net10.0; full suite as under 85.6.

### 85.8 (nit) — false "only thing between … and undefined behaviour" quantifier in the new SnapshotPartitions remark

- **File:** `src/Confluent.Kafka/Internal/NativeConsumer.cs:3877-3879`
- **Evidence:** The remark says "The null-topic check is the only thing between such a value and undefined behaviour in the ABI (`read_topic_partitions` calls `CStr::from_ptr` on every topic; M15/P13.2 D1)." That is false.
  - Every caller of `SnapshotPartitions` marshals through `WithPinnedTopics`, then `Utf8Marshal.Pin`, which calls `Encoding.UTF8.GetBytes(null)`. That throws `ArgumentNullException` (Parameter `s`) before any P/Invoke, so no NULL pointer can reach `CStr::from_ptr`.
  - PLAN §3.10 records exactly this second barrier.
  - Measured with M5 (the null-topic check disabled): the five sync ops that take partition lists throw `ArgumentNullException` "(Parameter 's')", with no abort.
- **Why it matters:** This is the uniqueness-quantifier class that ffi §A6's round-5 amendment warns about. The claim that the check is load-bearing for memory safety is wrong. What the check actually buys is the **documented** exception, with its own parameter name and message, instead of an undocumented `ArgumentNullException` named `s` from inside the pin helper. Anyone later reasoning about removing or reordering the check gets a false safety argument.
- **Fix:** Drop the "only thing" claim. State that the check raises the documented `ArgumentException` before any pin, that the pin would otherwise throw an undocumented `ArgumentNullException` (Parameter `s`), and that neither path lets a NULL reach `read_topic_partitions`.
- **Original commit:** `9b3c80f0`

**Resolution (dotnet-actor 85, fixup `5cc92f66` of `9b3c80f0`):**
- `NativeConsumer.cs:3878-3883` (`SnapshotPartitions` remark): the "only thing between such a value and undefined behaviour" claim is removed. The remark now states the local fact:
  - the null-topic check raises the documented `ArgumentException` (parameter `partitions`) before any topic is pinned;
  - without it, `Utf8Marshal.Pin` would throw an undocumented `ArgumentNullException` (parameter `s`) instead;
  - on neither path does a null topic cross the ABI, where `read_topic_partitions` would hand the NULL pointer to `CStr::from_ptr`.
  Checked before writing: all seven `SnapshotPartitions` call sites (`NativeConsumer.cs:973/1092/2093/3360/3539`, `NativeConsumerHandle.cs:388/406`) pin through `WithPinnedTopics`, which calls `Utf8Marshal.Pin(topicAt(i))`, and `PinnedUtf8String` calls `Encoding.UTF8.GetBytes(value)` first.
- Sweep of `git show 9b3c80f0`'s added lines for uniqueness and exclusivity words ("only", "sole", "single", "alone", "nothing", "the one", "every", "no other", "unique"):
  - **Fixed:** `AdminConstructedNullTopicTests.cs:37` "the constructor no longer stands in front of the guard, so the guard alone decides". It has the same shape as 85.8, since `Utf8Marshal.Pin` stands behind each admin guard as a second barrier. It now reads "the constructor no longer throws ahead of the guard, so what each test observes is the guard's own exception".
  - **Fixed** (under 85.7): `ConsumerNegativePartitionGuardTests.cs` "the gate in front of every native call".
  - **Kept, verified true:** `ffi-marshalling.md:333-339` "`DeliveryRegistration.Fire` … the single firing site". `_callback.OnCompletion` has one caller (`DeliveryRegistration.cs:138`), and the nine producer fault and complete sites all call `Fire`.
  - **Kept, not a uniqueness claim:**
    - `grpc-server/CallbackLog.cs:342/348/387` (history, and what the guard shapes);
    - `TopicPartition.cs:37/54` and `PublicTopicPartitionTests.cs:111` (universal claims, recorded as D1/D10 and accepted in the CP4 review);
    - `PublicTopicPartitionTests.cs:32` ("the single authoritative test", restating the pre-G3-4 doc, a historical claim);
    - `PublicAdminMockSeedingTests.cs:186` (the test's own witness);
    - the seven "one of the two ways to present a null element topic" comments (a default value versus a constructor-stored one);
    - `AdminConstructedNullTopicTests.cs:26` ("Every admin null-topic guard family", the Critic-verified coverage claim).
- Gates:
  - header sha `a7560aa32cbe15a9401f4442926700aa8af579cb` is unchanged;
  - Mode A holds: the `7e550b95..HEAD` diff over `src/`, `cbindgen.toml`, `generator/`, `bindings/python` and `bindings/c` is empty, and there are 700 `internal static extern` declarations;
  - Debug solution build: 0W/0E, six outputs. grpc-server: 0W/0E;
  - `AdminConstructedNullTopicTests`: 16/16 on net10.0;
  - full suite: 2434/2434 on net10.0 and on net8.0, with no aborts;
  - `dotnet format --verify-no-changes` is clean.

## Review record (Manager, at phase close — 2026-09-30)

| Checkpoint | Commit(s) | Critic 85 rounds | Findings |
|---|---|---|---|
| CP1 — configs (F1, G2-1, G2-4) | `2070667f` + `fixup!` `d067deba`, `4f222126` | round 1: 85.1, 85.2; round 2: 85.3 (nit) | 85.1 and 85.2 were fixed in `d067deba`. 85.3 was fixed in `4f222126`, as CP2's step 0. |
| CP2 — ACLs, quotas, SCRAM (G4-1, G4-2, G4-4) | `61097faa` | round 1: CLEAN (it also verified `4f222126`) | none |
| CP3 — topic value types and results (G1-4, G1-6, G1-7) | `3f802a2c` + `fixup!` `3d35f27f`, `b0c8e637` | round 1: 85.4; round 2: 85.5 (nit) | 85.4 was fixed in `3d35f27f`. 85.5 was fixed in `b0c8e637`, as CP4's step 0. |
| CP4 — `TopicPartition` (G3-4) | `9b3c80f0` + `fixup!` `5cc92f66` | round 1: 85.6, 85.7, 85.8; round 2: CLEAN | 85.6–85.8 were fixed in `5cc92f66`. They were remark fixes, and one `All()` assertion was strengthened. |

Each `fixup!` targets its own checkpoint's feature commit. No finding in any round would have changed a ruled decision (D1–D13).

Final gates on `5cc92f66`:

- **Mode A.** The diff over `src/ cbindgen.toml generator/ bindings/python bindings/c` from `7e550b95` is empty. Header sha `a7560aa3…` is unchanged, and `internal static extern` is 700 → 700.
- **Build and format.** The build is 0W/0E on six outputs and on grpc-server. `dotnet format` is clean on the sln and on grpc-server.
- **Unit tests.** 2434/2434 on both net10.0 and net8.0 (baseline 2345), with `Test Run Aborted` 0.
- **Local Docker gate.** A fresh linux/amd64 `.so` (700/700 entry points exported) and a fresh sync .NET gRPC image. The seven PLAN §6 gate-9 admin families passed 41/41 on `__grpc_dotnet`: configs 7, ACLs 5, quotas 3, SCRAM 2, topics 9, partitions/records 7, elections 8. The plan's "48" is off by one in every family; these counts come from `--list`.
  - The other admin families' `__grpc_dotnet` arms passed 38/38.
  - The non-admin sync `__grpc_dotnet` arms passed 34/34, as a CP4 regression.
  - The three `producer_transactions_test` arms were not run: they fail with a known, pre-existing `Unimplemented`.
