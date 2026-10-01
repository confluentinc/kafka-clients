# Critic 80 — M15/P10 (SCRAM three-view ABI, closes D44)

**Reviewed:** `4077787a` (merge) · `3ad0277f` CP1 · `f110ed74` CP2 · `7c76ae24` CP3 ·
`7ea64083` CP4 · `60dd4307` CP5.
**Ground truth:** `target/include/confluent_kafka.h` (lines 1427-1448, 10194-10372),
`src/ffi/admin.rs` §19130-19355 where the C doc is ambiguous, `DescribeUserScramCredentialsResult.java`.
**Gates re-run locally:** `dotnet build` net10.0 → 0W/0E; `dotnet test -f net10.0` → **2308/2308 pass**.

## Verdict: CLEAN PASS — no findings. Nothing blocks.

I looked for a defect on each axis the phase is actually at risk on and found none. Recorded
below so the next reviewer does not re-derive the walks.

### 1 · Ownership, both directions — correct

| Site | ABI contract | Code | Verdict |
|---|---|---|---|
| `all_error` | `kafka_common_Error_t*`, **owned**, freed with `kafka_common_Error_destroy` (h:10261-10262) | `KafkaException.FromHandle` → `NativeMethods.ErrorDestroy` in a `finally` | ✅ freed once, incl. a throw inside construction |
| `all_get_description(i)` | `const …Description_t*`, **borrowed**, "Do NOT pass it to `…Description_destroy`" (h:10301-10304) | `UserScramCredentialMarshal.Read` (non-destroying twin) | ✅ no destroy anywhere on this path |
| `description(user)` out-param | **owned** description, caller destroys (h:10338-10340) | `UserScramCredentialMarshal.ReadAndDestroy` → `accessors.Destroy` in a `finally` | ✅ once, incl. the throwing read |
| `description(user)` return | **owned** `Error*`, `*out` left untouched on fault (h:10341-10342) | `FromHandle`, thrown before `description` is read | ✅ no leak, no read of the untouched slot |
| submit `error` param | owned by the callback (h:1439-1444) | `FromHandle` | ✅ |

Both `Error*` sites route through `kafka_common_Error_destroy` (not `KafkaError_destroy`) — the
right destroyer for this ABI's admin error type.

### 2 · The callback baton — all five paths destroy the root exactly once

`OnDescribeUserScramCredentials`, `rootBaton = result` at entry, zeroed only after `Adopt`:

1. **submit error** (`error != Zero`) → `SetException`, `return`, `finally` destroys `rootBaton`.
   Per h:1439 `result` is null here, and the destroy is null-safe; if the core ever delivered
   both, the callback owns the root and destroying it is still correct. ✅
2. **throw in copy-out** (`FromHandle` / `all_count` / `ReadStringKey` / `Read` / the `Adopt`
   allocation) → baton still holds it → `finally` destroys. ✅
3. **throw inside `Adopt`** — the allocation is the only fallible step and `SafeHandle.SetHandle`
   cannot throw, so either the handle owns it or the baton does, never both. ✅
4. **bad `userData`** (`GCHandle.FromIntPtr` throws) → `context` stays null, `catch` absorbs,
   `finally` destroys the root. ✅ (the `GCHandle` is not freed on this path — the pre-existing
   shared-trampoline behaviour, not introduced here.)
5. **success** → baton zeroed, `finally` destroys `IntPtr.Zero` (no-op); the `SafeHandle` is the
   sole owner. **No double destroy after adoption.** ✅

Residual, benign: a throw from `SetResult` / the `DescribeUserScramCredentialsViews` allocation
*after* adoption leaves release to the critical finalizer. Not a leak.

### 3 · `SafeDescribeUserScramCredentialsResultHandle` — no race, no objection to finalizer-only

- `ReleaseHandle → …Result_destroy` once; `SafeHandleZeroIsInvalid` gates it on `IsInvalid`, so the
  test fixture's `Adopt(IntPtr.Zero)` is inert rather than a null destroy.
- `Description()` cannot race release: `_description` is declared with the **`SafeHandle` as the
  P/Invoke parameter**, so the marshaller holds a call-scoped `DangerousAddRef` for the whole native
  call (ffi §A2's sync convention — the decl is stronger than PLAN §3.1's `IntPtr` sketch, correctly
  so), and the handle is reachable from the async state machine's `views` local besides.
- **No objection to finalizer-only release** (PLAN §6 risk 2): the root is a small metadata
  snapshot, `…Result_destroy` is a plain `Box::from_raw` drop (verified at `admin.rs:19349-19355`),
  and Java's result is not closeable. `IDisposable` stays a non-breaking future option.
- Concurrent `Description()` is safe: `kafka_admin_…_description` takes `*const` / `&self` and reads
  an already-resolved future via `block_on_ready` (`admin.rs:19318-19339`) — PLAN §2.1's three
  facts hold, including independence from the `AdminClient` (the inner owns `core` by value).

### 4 · Java fidelity of the three views (PLAN §1.2) — matches

- `All()` → `views.AllError is null ? views.All : throw views.AllError`; the fault branch builds an
  **empty** map, so the fault zeroes the whole view. ✅
- `Users()` faults only if the operation `Task` faulted — never on a user-level error; `users_count`
  / `users_get` are read **unconditionally**, outside the `allError is null` branch, which is what
  keeps `all()`-faulting from suppressing `users()`. ✅
- `Description(u)` is evaluated per call against the retained root, so RNF / "No such user" come from
  the core verbatim. `All()` faulting does not disable it (the fault lives in the payload, not the
  `Task`). ✅
- `All()`'s map keyed with `StringComparer.Ordinal`; `UserScramCredentialsDescription` copies its
  credential list defensively. ✅

### 5 · Marshalling — correct

- The credential walk is bounded by `credential_count()` **of that description**; the outer count is
  now *structurally unavailable* to the reader (the signature takes only a description pointer), which
  is a stronger guarantee than the old convention comment. ✅
- `ScramMechanisms.FromType` byte-range guard preserved **verbatim** from `4077787a`
  (`code >= byte.MinValue && code <= byte.MaxValue`, else `ScramMechanism.Unknown`) — `byte.MinValue`
  is 0, so a negative out-of-range code also lands on `Unknown`. ✅
- `all_get_user` / `users_get` / `name()` are NUL-terminated `const char*` → `ReadStringKey` (NUL-scan)
  is the right §B3 form; each is copied out before the root can die. ✅
- P/Invoke delta is exactly −6 / +12 `internal static extern`, every one with an explicit `EntryPoint`
  and `Cdecl`. `int32_t → int`, opaque `*_t → IntPtr`, no mirrored structs. ✅

### 6 · D44 is closed in behaviour, not only in prose

The managed three-view reimplementation (`BuildAll` / `BuildUsers` / `BuildDescription`) and
`UserScramCredentialEntry` are gone; the views are now thin pass-throughs of the C-side discriminant,
and `Description(rnfUser)` reaches the ABI rather than being answered from an enumeration that
cannot contain an RNF user. No `D44` reference survives in source (remaining grep hits are
`bin/**/Confluent.Kafka.xml` build artifacts).

### 7 · Consistency / layout

`DescribeUserScramCredentialsViews` (`Internal/`) and `SafeDescribeUserScramCredentialsResultHandle`
(`Internal/Interop/`) are both `internal sealed`; the public result stays non-disposable and is
asserted so. `SingleAdminOperation`'s TCS is `RunContinuationsAsynchronously`
(`AdminOperation.cs:478`) — which matters more now that the continuation after `await _views`
performs a P/Invoke.

## Note (informational — not a finding, does not block)

`PublicAdminP7Tests.DescribeUserScramCredentials_ResourceNotFoundUser_IsInAllButNotUsers` is labelled
"the D44-closing assertion", but every fact it asserts is one `ResultOver` supplied: it proves
`All() → views.All` and `Users() → views.Users` pass-through, not the RNF discriminant, which lives in
the core's view computation and the trampoline's copy-out. Transposing the trampoline's two loops
would leave it green. Given the documented constraint (no populated result root is constructible from
managed code), the test cannot do better and the pass-through wiring it *does* cover is real — so
this is a scope-of-claim observation only, raised so nobody later mistakes it for coverage of the
trampoline.
