# COMMENTS.DONE.13 — M5/P4 "Consumer offset-map query siblings" (Category E1) (Actor N=13 → Critic N=13)

Closed record for the **M5/P4 — Consumer offset-map query siblings (Category E1)** phase (the
tracked archive under the phase directory, per CLAUDE.md §8.4; the binding-root
`COMMENTS.13.md` / `COMMENTS.DONE.13.md` are local working files and stay untracked). Approved
plan: `design/history/M5/P4-consumer-offset-queries/PLAN.md`.

**Mode A** (no Rust authored). Scope: four async offset-map query members on `IAsyncConsumer`
(`Committed` / `OffsetsForTimes` / `BeginningOffsets` / `EndOffsets`) + two public value types
(`OffsetAndMetadata`, `OffsetAndTimestamp`), via the proven owned-handle completion bridge. No
pre-existing `COMMENTS.13.md` items — fresh phase.

## Decisions / deviations

1. **`SubmitOwnedHandleOperation<T>` as a parallel submit helper (PLAN §4.2 "clone" option),
   NOT a generalization of `SubmitOperation<T>`.** The new helper passes only
   `(consumer, userData)` and each op closes over its own strongly-typed rooted callback at the
   call site. Leaves `SubmitOperation<T>` (poll), `SubmitScalarOperation<T>` (position), and
   `SubmitVoidOperation` byte-for-byte untouched — diff-verified (zero deletions to those
   methods or to `OnPoll` / `ConsumerCallbacks.Poll` / `.Position`). Lowest-risk proven-path
   guarantee.

2. **Three distinct callback delegate types, one per ABI typedef** (`OffsetMapCallback` /
   `OffsetAndTimestampMapCallback` / `LongOffsetMapCallback`) rather than one shared
   `(IntPtr, IntPtr, IntPtr)` delegate — each `_async` DllImport binds a strongly-typed,
   self-documenting parameter (no behavioral difference; all Cdecl, same layout).
   `OnLongOffsets` is a single shared trampoline for `beginning`/`end` (shared
   `long_offsets_callback_t`).

3. **The two value types carry `ToString()` but no `IEquatable`.** They are dictionary
   *values*, not keys, so no test needs value equality; kept minimal (the `ConsumerGroupMetadata`
   precedent). `sealed class`, getter-props, immutable.

4. **`LeaderEpoch` presence-flag decode factored into `OffsetMapMarshalShared.ReadLeaderEpoch`**
   (present ⇒ epoch, absent ⇒ null), shared by both value marshallers and directly unit-tested
   (the `bool` return is honored, not hardcoded).

5. **Reachability outcomes (PLAN §6 — documented, not silently skipped):**
   - `BeginningOffsets` / `EndOffsets`: **fully data-tested** broker-free (non-empty round-trip
     incl. a non-ASCII topic through the key marshalling; unset-TP `illegal_state` faulted with
     the asserted message).
   - `Committed`: **empty-only** broker-free (the mock's committed map is populated only by the
     not-yet-wired commit-with-offsets family). Tested: empty-collection → empty map;
     uncommitted-TP → empty map; faulted/precondition/lifecycle. **Non-empty end-to-end data
     test DEFERRED to the commit-family phase** (which reuses `OffsetAndMetadata`).
   - `OffsetsForTimes`: **faulted-only** — the mock returns `unsupported_version` unconditionally
     (mirrors Java's not-implemented `MockConsumer`). Tested: faulted `Task` + message + code 35
     (`UnsupportedVersion`); negative-timestamp accepted (then faults); empty-map still faults
     (the FFI does NOT short-circuit empty before the mock call — verified).
   - **Non-empty `OffsetMap_t` / `OffsetAndTimestampMap_t` copy-out** (incl. the value-type
     `LeaderEpoch` presence flag through a borrowed element): **NOT reachable broker-free** (no
     ABI container constructor; mock `committed` empty; `offsets_for_times` always errors).
     **Deferred**; the pieces ARE covered another way: the presence-flag decode unit-tested
     directly; the value types' `int?` mapping at the value level; and the valid non-empty
     `LongOffsetMap_t` copy-out end-to-end via `BeginningOffsets`/`EndOffsets`.

6. **Finding — the container `_count`/`_get` accessors are NOT null-safe** (only `_destroy` is;
   `src/ffi/consumer.rs`). The **production path is correct**: the trampolines call `CopyOut`
   only on the SUCCESS branch (map guaranteed non-null); the failure branch (`map == null`)
   calls only the null-safe `*Destroy` in the `finally`. A first cut of the marshaller unit tests
   passed `IntPtr.Zero` to `CopyOut` and crashed the test host; removed those contract-violating
   cases; the empty (valid, non-null) container path is covered end-to-end instead. Documented in
   `OffsetMapMarshalTests` + STATUS.

7. **`SubmitPartitionOp` refactored to reuse `SnapshotPartitions`/`ExtractPartitions`** (the
   shared validate+snapshot, so null/null-topic/negative-partition validation is not
   copy-pasted). Behavior-identical (same exception types/messages/order); the 5 shipped
   partition ops still green.

8. **`EmptyReadOnlyDictionary<K,V>`** added under `Internal/` as the `Array.Empty` analog for the
   empty-map result path (ns2.0 has no built-in empty read-only dictionary) — avoids a per-empty
   allocation (the `ConsumerRecordsMarshal` `Array.Empty` precedent).

## Commits (branch `prashah_dev_public_consumer_remaining`)
- `dc9939e` archive approved plan
- `32f864f` value types + interop scaffolding (marshallers, trampolines, NativeMethods, EmptyReadOnlyDictionary)
- `ea3ffd2` the four public members + forwards
- `62efcdb` tests
- `a3c9b3d` doc-sync (STATUS + IAsyncConsumer remarks + CLAUDE.md §3 sketch)

## DoD (all green — Actor)
- `cargo build --features ffi` (no ABI change) → `dotnet build` 0/0 across all library TFMs
  (ns2.0/net8.0/net10.0) + all test TFMs (net462/net8.0/net10.0), TreatWarningsAsErrors + CS1591
  on the 4 members + 2 value types.
- `dotnet test -f net10.0`: **175 → 214**, green 10/10 full runs (D8.8 serial gate stable).
- `dotnet format --verify-no-changes` clean.
- Receive-path copy-out audit: each container `_destroy`d exactly once after copy-out in a
  `finally`; map key/value ELEMENTS never `_destroy`d (borrowed views); per-op `GCHandle` freed
  once; no native-backed value escapes; proven poll/void/scalar bridges unchanged.

---

## Critic N=13 — review outcome (closed)

**Review (`dc9939e..a3c9b3d`): CLEAN, 0 genuine findings, phase PASSES.** Independently verified
against the C ABI header + the Kafka Java public-API shape + the approved PLAN:

- **Receive-path copy-out / borrow-root (§B2/§B4):** each container root `_destroy`d exactly once
  in the trampoline `finally` after `CopyOut`; map elements (`OffsetAndMetadata_t`/
  `OffsetAndTimestamp_t` values, `TopicPartition_t` keys) are borrowed Category-4 views — no
  element `_destroy` declared in `NativeMethods` or called anywhere; strings copied via
  `PtrToString` before destroy; no native-backed value escapes.
- **Null-container safety (the flagged finding):** all three trampolines use
  `if (error != Zero) Complete(error); else CopyOut(container)` — the non-null-safe `_count`/`_get`
  are reached only on the success (non-null) branch, matching the header callback contract and the
  `OnPoll` precedent; the host-crashing `CopyOut(Zero)` unit tests are gone.
- **Free-exactly-once:** `FreeGcHandle` caller set = 6 trampoline `finally` + `AbandonBeforeSubmit`
  (grows one per new trampoline); no teardown/Dispose-path free.
- **Proven paths untouched:** `OperationCompletionSource.cs` empty diff; `OnPoll` /
  `SubmitOperation` / `SubmitVoidOperation` / `SubmitScalarOperation` unchanged;
  `SubmitOwnedHandleOperation<T>` a faithful callback-agnostic clone; the `NativeConsumer.cs`
  deletions are a behavior-preserving extraction of `SubmitPartitionOp`'s validation.
- **Value types / input marshalling / preconditions / doc-sync / test reachability** all confirmed
  (sealed classes, non-null `Metadata`, `int?` `LeaderEpoch` honoring the `[MarshalAs(I1)]`
  presence flag; `WithPinnedTopics` reuse + `WithPinnedTopicsAndTimestamps`; preconditions before
  P/Invoke; the non-empty `OffsetMap`/`OffsetAndTimestampMap` gap covered by unit tests + documented,
  not silent).
- **DoD independently observed:** `cargo build --features ffi` exit 0; `dotnet build` **0/0** across
  all six TFM legs; `dotnet test -f net10.0` **214 passed / 0 failed**, **looped 80× — no native
  host-crash**; `dotnet format --verify-no-changes` clean. No `COMMENTS.13.md` findings written.

**Non-finding recorded (NOT this phase):** a ~1-in-40 **managed** flake in the **M5/P2**
`PublicConsumerPositionTests.Position_PerOpAllocation_IsBounded` (`2260 B > 2048 B` budget) — a
pre-existing test (commit `312f1ab`) that M5/P4 does not touch; the known process-wide
`GC.GetTotalAllocatedBytes` measurement fragility, not a memory-safety defect. The new M5/P4 alloc
test uses a 2× (4096 B) budget and did not flake. Candidate for a trivial budget-loosening tidy on
the M5/P2 test.

**Loop closed:** Actor N=13 → Critic N=13 (CLEAN, 0 findings). No fix cycle required; no
outstanding review comments.
