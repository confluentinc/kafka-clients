# COMMENTS.DONE.14 — M5/P5 "Consumer partition-metadata queries" (Category E2) (Actor N=14 → Critic N=14)

Closed record for the **M5/P5 — Consumer partition-metadata queries (Category E2)** phase (the
tracked archive under the phase directory, per CLAUDE.md §8.4; the binding-root
`COMMENTS.14.md` / `COMMENTS.DONE.14.md` are local working files and stay untracked). Approved
plan: `design/history/M5/P5-consumer-partition-metadata/PLAN.md`. **This completes Category E**
(the consumer query family). No pre-existing `COMMENTS.14.md` items — fresh phase.

**Mode A** (no Rust authored). Scope: two async members on `IAsyncConsumer` (`PartitionsFor` /
`ListTopics`) + two nested public value types (`Node`, `PartitionInfo`), via the E1 owned-handle
completion bridge.

## Decisions & deviations made during execution

1. **`ToString` only, NO `IEquatable`** on `Node` / `PartitionInfo` (PLAN §8.1, user-locked).
   Query-result values placed into list/dictionary-value slots, never dict keys — the E1
   precedent. Java overrides `equals`/`hashCode`; adding `IEquatable` + `GetHashCode` later is
   non-breaking. Not added (no test needs value equality; `ListTopics` is keyed by `string`).

2. **Operational-failure faulted-`Task` end-to-end assertion → documented reachability limit,
   NOT a shipped test.** There is **no clean broker-free operational-failure path** for these two:
   the mock's `partitions_for` always returns a valid list (empty for an unregistered topic,
   populated after `update_partitions`) and `list_topics` always returns a valid (possibly empty)
   map — neither can fault broker-free (`src/consumer/mock_consumer.rs`); a real
   `AsyncKafkaConsumer` against `localhost:0` does not fault fast (metadata retries past the 30 s
   `TestTimeout` → `TimeoutException`, confirmed). Removed that test; replaced with a doc block.
   The faulted-`Task` **mechanism** (trampoline `Complete(error)` → `FromHandle`, error handle +
   `GCHandle` freed once, faulted `Task` not a synchronous throw) is identical to the E1
   offset-map bridges and already proven end-to-end there
   (`BeginningOffsets`/`EndOffsets` unset-partition faults + `OffsetsForTimes` unsupported-version
   fault). Concurrent-op fault is the D-Q4 non-blockable-mock ceiling. A documented slice, not a
   skipped requirement.

3. **`PartitionsFor("")` forwarded, not rejected** (PLAN §8.2, user-locked). The binding guards
   only `null` (`ArgumentNullException`, FFI panic-safety); an empty/whitespace topic is passed
   straight to the core (Python does zero topic validation). Tested via
   `PartitionsFor_EmptyTopic_ForwardedNotRejected_ReturnsEmptyList`.

4. **`SubmitOwnedHandleOperation<T>` reused verbatim** — no new submit helper. `PartitionsFor`
   pins its one topic call-scoped via a scoped `Utf8Marshal.Pin` (not the array-shaped
   `WithPinnedTopics`); `ListTopics` has no input. Proven poll/void/scalar/E1 submit paths
   byte-for-byte untouched (diff-verified zero deletions to `ConsumerCallbacks` / `NativeConsumer`
   / `NativeMethods`).

5. **Each ABI callback typedef its own delegate type** (`PartitionInfoListCallback` /
   `TopicPartitionInfoMapCallback`), matching the E1 convention.

6. **`PartitionInfo_destroy` deliberately NOT declared** in `NativeMethods` (and `Node_destroy`
   does not exist in the ABI). Every `PartitionInfo` / `Node` / nested list from a container is a
   borrowed Category-4 view; not declaring `PartitionInfo_destroy` makes a borrowed-element free
   structurally impossible (only the two root `_destroy`s are declared).

## Reachability outcomes

| Field | Data-tested broker-free | Documented-empty (mock limit) |
|---|---|---|
| `PartitionInfo.Topic` / `.Partition` | ✅ | |
| `PartitionInfo.Leader` (id/host/port) | ✅ | |
| `PartitionInfo.Replicas[0]` / `.InSyncReplicas[0]` | ✅ (== Leader) | |
| `PartitionInfo.OfflineReplicas` | | ⚠ always empty (mock never sets offline) |
| `Node.Rack` | | ⚠ always null (mock uses 3-arg `Node::new`) |

The documented-empty fields' marshaller paths ARE exercised structurally (empty
`IReadOnlyList<Node>` / null rack); the null-rack / null-leader value-level mapping is asserted
directly in `PublicConsumerPartitionMetadataValueTypeTests`. Non-empty offline replicas and a
non-null rack are a broker/integration concern, not a silent gap.

## Commits (branch `prashah_dev_public_consumer_remaining`)
- `34f70ea` archive approved plan
- `b133ec7` library: `Node`/`PartitionInfo` value types + 4 layered marshallers + 2 trampolines + submit paths + mock wire + public wiring
- `06e322f` tests (3 files)
- `6fd9ff2` doc-sync (STATUS M5/P5 entry + `IAsyncConsumer` remarks + CLAUDE.md §3 sketch)

## DoD gates (all green — Actor)
- `cargo build --features ffi` — no ABI change; `dotnet build` 0/0 across all 6 TFM legs,
  TreatWarningsAsErrors + CS1591 on the 2 members + 2 value types.
- `dotnet test` (net10.0) — **214 → 241** (+27), green 5/5 full serial runs (D8.8 stable).
- `dotnet format --verify-no-changes` — clean.

## Memory-safety audit (self-review)
- Each ROOT container `_destroy`d exactly once in the trampoline `finally`, AFTER full copy-out
  (null-safe → no-op on failure). No `PartitionInfo`/`Node`/nested list ever freed (borrowed
  Category-4; `PartitionInfo_destroy` not declared, no `Node_destroy`). Length-delimited
  `Node.host`/`rack` via `PtrToString(ptr, len)` (no NUL-scan); NUL-terminated
  `PartitionInfo.topic`/map topic keys via `PtrToString(ptr)`. No native-backed value escapes;
  per-op `GCHandle` freed once; `RunContinuationsAsynchronously` inherited; proven
  poll/void/scalar/E1 bridges byte-for-byte unchanged.

---

## Critic N=14 — review outcome (closed)

**Review (`34f70ea..6fd9ff2`): CLEAN, 0 genuine findings, phase PASSES — completes Category E.**
Independently verified against the C ABI header + the Kafka Java public-API shape + the approved PLAN:

- **Nested copy-out memory safety (central axis):** the whole tree is copied into owned managed
  values before the single root `_destroy` (try-body before `finally`); the two roots
  (`PartitionInfoList_t` / `TopicPartitionInfoMap_t`) `_destroy`d exactly once each; every
  `PartitionInfo`/`Node`/nested list is a borrowed Category-4 view **never** freed — structurally
  guaranteed (`PartitionInfo_destroy`/`Node_destroy` neither declared nor called). No native-backed
  value escapes.
- **String forms (the E1 shape difference):** `Node.host`/`.rack` use length-delimited
  `PtrToString(ptr, len)` (never NUL-scan — the over-read trap); topic strings use NUL-terminated
  `PtrToString(ptr)`; `rack (null,-1)` → `Node.Rack == null`. Both forms coexist correctly.
- **Bridge reuse:** `OperationCompletionSource.cs` untouched; `SubmitOwnedHandleOperation<T>`
  reused verbatim; proven poll/void/scalar/E1 paths byte-for-byte unchanged (zero deletions across
  the phase); two clean `OnPoll`-clone trampolines.
- **Value types / preconditions / mock wire / shape / doc-sync:** all faithful to Java + the plan
  (incl. `PartitionsFor("")` forwarded, not rejected).
- **Reachability honesty:** the removed operational-fault test is honest — `mock_consumer.rs`
  `partitions_for`/`list_topics` can only fail via an unreachable `ensure_not_closed()`, no mock
  error hook exists, so a broker-free fault is genuinely unreachable; the faulted mechanism is
  already proven by the identical E1 bridges. No coverage lost.
- **DoD independently observed:** `cargo build --features ffi` success; `dotnet build` **0/0**
  across all six TFM legs; `dotnet test -f net10.0` **241 passed / 0 failed / 0 skipped**, green
  **6/6** consecutive runs (no host-crash under the nested marshalling); `dotnet format
  --verify-no-changes` clean. No `COMMENTS.14.md` findings written.

**Loop closed:** Actor N=14 → Critic N=14 (CLEAN, 0 findings). No fix cycle required; no
outstanding review comments. **Category E complete.**
