# Critic 84 — M15/P13.1

## CP2 round 1 — review of `a95bb632` (`git diff e5d0aac0..a95bb632`)

Both items moved here by Actor 84 after fixup `c1b6223c` (`fixup! feat(dotnet): M15/P13.1 CP2 — …`, targeting `a95bb632`; doc-only, no production change).

---

### C84-2-1 — Low — Stage6 N>1 test claims to guard a leftover per-member countdown, but measures green under it

- **File:** `bindings/dotnet/tests/Confluent.Kafka.UnitTests/Interop/AdminP9PerKeyStage6Tests.cs:226-230`
  (remarks of `RemoveMembers_NonRemoveAllMode_AgainstTheMock_ManyMembersFireOneCallbackAndRelease`).
- **Originating commit:** `a95bb632`.
- **Claim:** "...on the single-callback ABI there is nothing to count down, and a leftover
  per-member arming would leave the handle open below (no second callback ever arrives)."
- **Evidence (the claim is false):**
  - On the single-callback shape, `SingleAdminOperation`'s completion runs `FreeGcHandle` in the
    trampoline's `finally` (`CompleteRootValueRpc`: `finally { destroyResult(result);
    context?.FailUncompleted(); context?.FreeGcHandle(); }`). That free does not depend on any
    countdown.
  - A leftover `SetPendingCallbacks(n)` + `ReleaseSubmitToken()` is armed at 2 or more here:
    removeAll gives 1+1, and member-list mode gives keys.Count ≥ 1 because the options reject an
    empty set. One submit release never reaches zero, so it never frees early. The one callback
    then frees exactly once, because `FreeGcHandle` is `Interlocked`-guarded. The handle closes
    and `IsClosed` is true, so this test stays green.
  - **Repro (measured):** in a throwaway worktree at `a95bb632`, add
    `operation.SetPendingCallbacks(removeAll ? 1 : keys!.Count);` before the submit P/Invoke in
    `NativeAdminClient.RemoveMembersFromConsumerGroup` and `operation.ReleaseSubmitToken();` after
    it. Then run
    `dotnet test -c Debug -f net10.0 --filter FullyQualifiedName~RemoveMembers`.
    Result: **32/32 GREEN**, this test included.
  - The same commit states the opposite, and it is right: the P13 theory remark
    (`AdminP13GroupOffsetsSingleCallbackTests.cs:386-394`) says "**neither** row here is reachable
    by a replay of the pre-CP2 `SetPendingCallbacks(pendingCallbacks)` + `ReleaseSubmitToken()`
    lines ... that countdown is armed at two or more and one submit release never reaches zero".
    So the two remarks in one commit contradict each other about the same mutant.
- **Why it matters:** this is a guard-claim that does not guard (DoD §12 spirit). A future
  reviewer who trusts it will think the pre-CP2 countdown replay is covered by this test when
  nothing covers it. Nothing needs to cover it, because it is an equivalent mutant: no leak, no
  double free, nothing observable.
- **Expected fix:** delete the clause "and a leftover per-member arming would leave the handle open
  below (no second callback ever arrives)". State only what the test does discriminate, which is
  the fire count (one callback resolves every accessor) and a trampoline that does not free (the
  handle stays open). Point to the P13 theory remark for which submit-side mutations are and are
  not reachable. Optionally note that `SetPendingCallbacks(0)` + `ReleaseSubmitToken()` makes this
  test fail (measured: 6 failures in the same filter, this test among them). Do not re-scope the
  false clause into a new comparative claim.

- **Resolution (`c1b6223c`):** deleted the false clause (and the pre-CP2 countdown sentence it hung off); the remark now states only what the test discriminates — one callback resolves `All()` and every member accessor, exactly one fire after the settle window, and a non-freeing trampoline leaves `IsClosed` false — and points at the P13 `RemoveMembers_UntilTheCallbackFires_TheOperationStaysPendingAndRooted` remarks for submit-side reachability. The optional `SetPendingCallbacks(0)` + `ReleaseSubmitToken()` note was re-measured before being written: 3/3 red on net10.0 (`All()` does not resolve within the deadline); production restored byte-identical (`cmp`). No new comparative quantifier.

---

### C84-2-2 — Low (doc) — P13 class remark overstates the sync ABI: a successful removeAll *does* produce a root

- **File:** `bindings/dotnet/tests/Confluent.Kafka.UnitTests/Interop/AdminP13GroupOffsetsSingleCallbackTests.cs:68-73`
  (class `<remarks>`, the ⚠ paragraph).
- **Originating commit:** `a95bb632`.
- **Claim:** "`removeMembersFromConsumerGroup`'s sync entry point produces a root only in
  member-list mode. In `removeAll` mode it returns the error instead (...), so no in-process root
  carries a `removeAll` result".
- **Evidence (header, `target/include/confluent_kafka.h`):**
  - `:8300-8303`, for `kafka_admin_AdminClient_remove_members_from_consumer_group`, says "On
    success writes a [`kafka_admin_RemoveMembersFromConsumerGroupResult_t`] to `*out_result` ...
    and returns null". This applies in both modes.
  - `:8310-8313` says "A non-null return means the request could not be submitted at all — or that
    `remove_all` was true and `all()` failed". So the error return in removeAll mode happens only
    when `all()` **failed**. A removeAll that succeeds writes a root with `_count` = 0 and a null
    `_all`.
  - The remark's conclusion (no in-process removeAll root) holds only because the in-process
    mock refuses every group RPC, so `all()` always fails there. It is not a property of the
    entry point, which the remark attributes it to.
- **Why it matters:** the remark sets the reason the removeAll `_all` read is pinned by
  `AdminP4ReaderWiringTests` rather than driven on native memory. The reason given is a
  misstatement of the ABI contract. A future phase with a mock or harness where removeAll succeeds
  would read it as "impossible" and not add the native-memory row.
- **Expected fix:** limit the sentence to the failure path and the mock. For example: "in
  `removeAll` mode a **failed** `all()` is returned as the error with no root (header quote), and
  against the mock `all()` always fails, so no in-process root carries a `removeAll` result". The
  rest of the paragraph can stay.

- **Resolution (`c1b6223c`):** the class remark now says only that in `removeAll` mode a **failed** `all()` is returned as the error with no root (header quote kept), and that against the mock `all()` always fails — hence no in-process `removeAll` root. The two parallel copies of the overstatement were corrected with it: `RemoveMembersSync_RemoveAllMode_ReturnsTheErrorAndWritesNoRoot`'s summary now scopes "no root" to the mock, and `SyncNativeMethods.RemoveMembersFromConsumerGroup`'s doc now says the root is written "on success" (either mode) instead of "in member-list mode". A grep for the clause's distinctive words across `src/` and `tests/` found no further copy.

---

## Review record (Manager, at phase close — 2026-09-29)

| Checkpoint | Commit(s) | Critic 84 rounds | Findings |
|---|---|---|---|
| CP1 — alter/delete group offsets | `e5d0aac0` | round 1: CLEAN | none |
| CP2 — removeMembersFromConsumerGroup | `a95bb632` + `fixup!` `c1b6223c` | round 1: 2 LOW; round 2: CLEAN | C84-2-1, C84-2-2 (above), both doc-only, fixed in `c1b6223c` |
| CP3 — retire fan-in / shape 4c, docs | `63782c71` | round 1: CLEAN | none |

Nothing any round found would have changed a ruled decision (D1–D8).

Final gates on `63782c71`:

- Mode-A diff over `src/ cbindgen.toml generator/ bindings/python bindings/c` from `54ed917c` was empty.
- `internal static extern` went 697 → 699 → 700 → 700.
- Build was 0W/0E on six outputs, and `dotnet format` was clean on the sln and on grpc-server.
- Unit tests passed 2345/2345 on both net10.0 and net8.0, with `Test Run Aborted` 0.
- Local Docker gate: the six §5 scenarios passed 6/6 on `__grpc_dotnet`, on the `__rust` oracle and on the `__grpc_python` oracle.
