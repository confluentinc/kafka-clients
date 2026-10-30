# M15/P10 — .NET Admin: `describeUserScramCredentials` three-view ABI (closes D44)

**Status:** DRAFT — awaiting approval. No code touched.
**Agent number:** N=80 (Actor + Critic).
**Branch:** `prashah_dev_dotnet_binding` (tip `1800bc68` = M15/P9, not pushed).
**Mode:** A (the ABI already exists on `pr-201-latest`; C#-only, header-down).
**Actor/Critic focus:** code change + behaviour correctness. Not docstrings, not prose.

---

## 0 · Prerequisite (step 0, Actor's first action)

`git merge pr-201-latest` into `prashah_dev_dotnet_binding`.

- Base `a32c335a` → tip `e6f0a80d`, 4 commits; verified touching only `bindings/c/`,
  `bindings/python/`, `cbindgen.toml`, `multilanguage-test-server/`, `src/ffi/admin.rs`,
  `tests/` — **zero** `bindings/dotnet/` overlap, so conflict-free.
- Then rebuild the native: `cargo build --features ffi` and confirm the 15 new symbols
  land in `target/include/confluent_kafka.h` (they are the only ABI delta this phase
  consumes). A stale `.so` surfaces as `EntryPointNotFoundException` at test time.
- **The Rust mock is unchanged** by those 4 commits (`git diff --stat a32c335a..e6f0a80d -- src/`
  = `src/ffi/admin.rs` only). So `MockAdminClient` still has no way to produce a populated
  result for this RPC → the injectable-accessor test pattern in
  `UserScramCredentialMarshal` **stays** (see §4).

---

## 1 · The ABI delta

**Unchanged:** `kafka_admin_AdminClient_describe_user_scram_credentials_callback_t` —
still `(result*, error*, user_data)`, one callback per submit. Shape 1 (single
aggregate), not a per-key fan-out. `..._Result_destroy` unchanged.

**Removed** (the 6 flattened-row accessors the .NET side consumes today):
`_count`, `_get_user(i)`, `_get_error(i)` *(borrowed)*, `_get_credential_count(i)`,
`_get_credential_mechanism(i,j)`, `_get_credential_iterations(i,j)`.

**Added** — three views over one retained core result
(`DescribeUserScramCredentialsResultInner { all_view, users_view, core }`, computed once
at handle construction except `description`):

| Symbol | Returns | Ownership |
|---|---|---|
| `..._Result_all_error(result)` | `Error*` or null | **OWNED** — caller frees |
| `..._Result_all_count(result)` | `i32` (0 when `all()` faults) | — |
| `..._Result_all_get_user(result, i)` | `const char*` | borrowed |
| `..._Result_all_get_description(result, i)` | `const UserScramCredentialsDescription_t*` | **borrowed** — never destroy |
| `..._Result_users_count(result)` | `i32` | — |
| `..._Result_users_get(result, i)` | `const char*` | borrowed |
| `..._Result_description(result, user, out_description)` | `Error*` or null; writes `*out` on success | **OWNED** description — caller destroys |
| `kafka_admin_UserScramCredentialsDescription_{name,credential_count,credential_mechanism(i),credential_iterations(i),destroy}` | — | dual (see below) |

### 1.1 The two things that will be got wrong if not stated

1. **`all_error` is OWNED**, unlike today's borrowed `_get_error(i)`. Use
   `KafkaException.FromHandle`, **not** `FromBorrowedHandle`. A `FromBorrowedHandle`
   here leaks one `Error` per faulting `All()`.
2. **`UserScramCredentialsDescription_t` is dual-ownership** — borrowed from
   `all_get_description`, owned from `description`. Exactly `ffi-marshalling.md` §A2's
   "the same type can be owned in one call and borrowed in another — read the
   signature, not the prefix". Destroying an `all_get_description` pointer is a
   double-free; not destroying a `description` pointer is a leak per call.

### 1.2 The Java semantics the three views now encode natively

Verified against the Rust doc comments + `DescribeUserScramCredentialsResult.java`:

| View | Faults when | RNF user | Hard-error user | Order |
|---|---|---|---|---|
| `all()` | first user error whose code ∉ {NONE, RESOURCE_NOT_FOUND} → **whole view** faults (`count`→0, getters→null) | **included**, empty credentials | (is the fault) | sorted by name (C-side, for stable indexing) |
| `users()` | never on a user-level error (only a top-level future failure, which the submit already surfaced) | **excluded** | **included** | response order |
| `description(u)` | `RESOURCE_NOT_FOUND` for an RNF user (broker's own message); `"No such user: <u>"` for an absent one | faults | faults with its own error | n/a |

This is the discriminant D44 said was missing. `Description()`'s RNF-vs-genuinely-empty
ambiguity, and `Users()`'s over-inclusion of RNF users, both close natively.

---

## 2 · The one design decision: eager vs lazy `Description(user)`

`all()` and `users()` are precomputed on the Rust side and are cheap eager copy-outs
either way — no decision there. `description(user)` is **on-demand against the retained
core result** (`block_on_ready(inner.core.description(&user).get())` on **every** call).

### Option E — eager, keep today's lifetime shape

Inside the completion callback, pre-materialize a `Dictionary<string, Result>` by calling
`_description(u)` for every `u` in `all()`-rows ∪ `users()`, destroying each owned
description immediately, then destroy the native root — exactly as today. No `SafeHandle`,
no unmanaged resource on the public type, no lifetime change.

**Why it does not work — this is decisive, not a preference.** When `all()` faults,
`all_count` is 0 and `all_get_*` return null, so the only enumerable user set is
`users()` — which **excludes RNF users**. RNF users are then unenumerable from any view,
so `Description(rnfUser)` would answer `"No such user: X"` where Java (and the new ABI)
faults `RESOURCE_NOT_FOUND`. That is **D44 re-created in a narrower window** — this phase
exists to close it. Option E is therefore rejected on correctness, not cost.

### Option L — lazy, retain the native root (RECOMMENDED)

The callback **transfers ownership** of the native root into a new
`SafeDescribeUserScramCredentialsResultHandle : SafeHandle` instead of destroying it;
the public `DescribeUserScramCredentialsResult` holds it for its own lifetime and
`Description(user)` P/Invokes `_description` on demand each call. `All()` / `Users()`
stay eager copy-outs taken in the callback (cheap, and it keeps the faulting `all_error`
handling in one place).

**Lifetime / disposal:** rely on `SafeHandle`'s own critical finalizer — do **not** add
public `IDisposable` to the result type. Java's `DescribeUserScramCredentialsResult` is
not closeable, and `..._Result_destroy` is a plain `Box::from_raw` drop (no runtime
teardown, non-blocking), so the `ffi-marshalling.md` §A2 "prefer `Dispose` over the
finalizer" reasoning — which is about the *producer's blocking* destroy — does not apply.
Adding `IDisposable` later is non-breaking if a deterministic-release need appears.
This is the `ffi-marshalling.md` Part B §B2 Category 3 borrow-root shape, held past the
call rather than freed in it.

**Recommendation: Option L.** Record Option E's rejection reason at the site so a future
reviewer does not "simplify" back into the D44 hole.

### 2.1 Facts to confirm before CP3 (cheap, but load-bearing for L)

- `_description` is safe to call from arbitrary managed threads, and concurrently
  (result inner is `&self` over a resolved `KafkaFuture`; confirm no `&mut` path).
- The retained root stays valid after the `AdminClient` is disposed (the inner owns
  `core: DescribeUserScramCredentialsResult` **by value**, so it should be independent —
  assert it with a test, per the M15 "result is Arc-backed / independent" precedent).
- `_description` is deterministic across repeated calls (the data future is resolved).

---

## 3 · File-by-file changes

All paths under `bindings/dotnet/src/Confluent.Kafka/`.

### 3.1 `Internal/Interop/NativeMethods.Admin.cs` (~3620-3648)
- **Remove** the 6 old `[DllImport]`s.
- **Add** 7 result accessors + 5 `UserScramCredentialsDescription_*` accessors (12 new).
  `EntryPoint` = the full ABI symbol for every one (§0.1 of ffi-marshalling).
  `_description` signature: `(IntPtr result, IntPtr user, out IntPtr outDescription) -> IntPtr`.
- P/Invoke count gate: count `internal static extern`, not `grep -c DllImport`
  (the latter over-counts).

### 3.2 `Internal/Interop/UserScramCredentialMarshal.cs` — rewrite
- Replace `ReadEntry(result, index[, accessors])` with a reader over **one**
  `UserScramCredentialsDescription_t*` → `UserScramCredentialsDescription`, shared by
  the `all()` path (borrowed pointer) and the `description()` path (owned pointer).
- **Keep the injected-`Accessors` pattern** (the mock still cannot populate a real
  result — §0), but reshape it to the new accessor set: `name`, `credential_count`,
  `credential_mechanism(i)`, `credential_iterations(i)`. Keep the
  "inner walk is bounded by `credential_count`, never an outer count" invariant.
- Add a `ReadAndDestroy` twin for the owned (`description`) call site, destroying in a
  `finally` — the `OffsetMapMarshal.CopyOutAndDestroy` precedent (ffi §B2): ship the
  destroying variant next to the non-destroying one so the two call sites cannot be
  confused. Plain read (no destroy) for the borrowed `all_get_description` site.
- Preserve the existing `ScramMechanisms.FromType` byte-range guard →
  `ScramMechanism.Unknown` for an out-of-range code.

### 3.3 `Internal/Interop/AdminCallbacks.cs` (~1041-1230)
- Callback typedef decl: **unchanged**.
- Delete the `DescribeUserScramCredentialsEntry` reader delegate,
  `s_describeUserScramCredentialsCount`, and the `CompleteListRpc` routing for this RPC.
- `OnDescribeUserScramCredentials` becomes its own trampoline (it no longer fits the
  shared flattening helper):
  1. non-null submit `error*` → fault, destroy, return (unchanged path);
  2. read `all_error` → if non-null, capture via `FromHandle` (**owned**);
  3. else copy out `all_count` rows (`all_get_user` + borrowed `all_get_description`);
  4. copy out `users_count` / `users_get`;
  5. **adopt** the root into the new `SafeHandle` (Option L) — with an ownership
     **baton** zeroed at adoption and a null-safe `_destroy` of the baton in the
     trampoline's own `finally`, so a throw anywhere in 2-4 destroys the root exactly
     once and a successful adoption never double-destroys (ffi §B6 third Rule's baton
     shape);
  6. keep `s_destroyDescribeUserScramCredentialsResult` — it is now only the baton's and
     the `SafeHandle`'s release path, not the callback's happy path.
- The `GCHandle` free site is unchanged (hookless one-shot; the callback owns it).

### 3.4 `Internal/NativeAdminClient.cs` (~702-710 delegate decl; ~3950-4018 method)
- Change the operation's payload type from
  `SingleAdminOperation<IReadOnlyCollection<UserScramCredentialEntry>>` to a payload
  carrying `(all-or-error, users, SafeHandle)` — the three pieces the public type needs.
- Keep the existing user-list de-duplication / null-users ("describe every user") submit
  behaviour unchanged.

### 3.5 `Admin/DescribeUserScramCredentialsResult.cs` — public type
- **Delete** `BuildAll` / `BuildUsers` / `BuildDescription` — the managed
  reimplementation of Java's three-view filtering. Thin-wrap the native views instead:
  - `All()` → the captured `all_error` (fault) or the copied-out map;
  - `Users()` → the copied-out list;
  - `Description(user)` → `ArgumentNullException` guard (keep), then P/Invoke
    `_description` through the retained handle; non-null `Error*` → throw via
    `FromHandle`; else read + destroy the owned description.
- **Delete the D44 divergence paragraph** (~lines 30-47). Do not carry it forward and do
  not reword it — the divergence is closed. Per M14/P1's lesson, delete the claim rather
  than re-scope it.
- Keep the existing `Task`-returning signatures (`Task<IReadOnlyDictionary<…>> All()`,
  `Task<IReadOnlyList<string>> Users()`, `Task<UserScramCredentialsDescription> Description(string)`)
  so the public shape is unchanged.

### 3.6 `Internal/UserScramCredentialEntry.cs` — **delete**
Its only purpose was bridging the flattened row shape. Its D44 paragraph (~24-39) goes
with it.

### 3.7 `Admin/UserScramCredentialsDescription.cs` — verify only
Expected unchanged (`(user, IReadOnlyList<ScramCredentialInfo>)`); now populated from the
new accessors. No public shape change expected — flag it if one is needed.

### 3.8 No change expected
`Admin/IAdmin.cs`, `Admin/KafkaAdminClient.cs`, `Admin/MockAdminClient.cs` (both clients
just delegate to `_native.DescribeUserScramCredentials`),
`Admin/DescribeUserScramCredentialsOptions.cs`.

---

## 4 · Tests

Files already touching this RPC: `PublicAdminP7Tests.cs`,
`PublicAdminP7ShapeParityTests.cs`, `Interop/AdminP7SubmitArgumentTests.cs`,
`Interop/AdminP4ReaderWiringTests.cs`, `Interop/AdminP7ResultMarshalTests.cs`.
Update all five; add to the P7Result/Reader interop files rather than a new P10 file
(the RPC's tests already live there).

Required coverage (each must fail against today's code):

1. **Three views, three RNF treatments**, via injected accessors:
   - `all()` faulting → `All()` faults with that exact code **and** message; `Users()`
     still succeeds; `Description(u)` still works per user.
   - `all()` clean, one RNF user → present in `All()` with zero credentials, **absent**
     from `Users()`, and `Description(rnfUser)` **faults** `RESOURCE_NOT_FOUND`
     (the D44-closing assertion).
   - `Description(absentUser)` → `"No such user: <u>"`.
2. **Ownership**, the two directions:
   - the borrowed `all_get_description` pointer is **never** destroyed (count destroys);
   - each `_description` call destroys its owned description **exactly once**, including
     on the throwing path.
3. **`all_error` is freed exactly once** (the owned-vs-borrowed trap, §1.1).
4. **Root lifetime (Option L):** `Description()` still works after the `AdminClient` is
   disposed; a leak-injection check that the root is destroyed exactly once
   (finalizer-driven — force a GC + `WaitForPendingFinalizers`).
5. **Callback baton:** a throw during the copy-out phase destroys the root exactly once
   and does not double-destroy after a successful adoption.
6. **Concurrency:** concurrent `Description()` calls on one result do not corrupt or
   double-free.
7. **Nested-walk bound** preserved (inner count, never an outer one).
8. Existing shape-parity assertions updated, not deleted.

Gates: `cargo build --features ffi` → `dotnet build` (TFM matrix, 0W/0E) →
`dotnet test -f net10.0` → `dotnet format --verify-no-changes`. Run
`cargo xtask format-check` **from the repo root** (it false-fails elsewhere).
`dotnet` is not on `PATH` (`~/.dotnet/dotnet`).

---

## 5 · Checkpoints (one commit each; resumable)

| CP | Content |
|---|---|
| CP0 | merge `pr-201-latest`; rebuild native; confirm the 15 symbols |
| CP1 | `NativeMethods.Admin.cs`: -6 / +12 `[DllImport]`s |
| CP2 | `UserScramCredentialMarshal` rewrite (+ owned/borrowed twin) |
| CP3 | Option-L `SafeHandle` + `AdminCallbacks` trampoline + `NativeAdminClient` payload (confirm §2.1 first) |
| CP4 | public `DescribeUserScramCredentialsResult` rewrite; delete `UserScramCredentialEntry.cs`; delete both D44 paragraphs |
| CP5 | tests (§4) + gates green |

Critic 80 runs **once at the end** unless the Actor reports a design deviation — the
M15/P5-P9 cadence.

---

## 6 · Risks

- **Silent ownership inversion** (§1.1): a `FromBorrowedHandle` on `all_error`, or a
  `_destroy` on an `all_get_description` pointer. Neither has a managed symptom; only
  tests 2-3 catch them.
- **Option L's finalizer** is the only release path. If the Critic objects to
  non-deterministic native release on a public type, the fallback is adding
  `IDisposable` (non-breaking) — not reverting to Option E, which is incorrect (§2).
- **Stale native** → `EntryPointNotFoundException` at first test; rebuild before blaming
  the C#.
- **D44 text resurfacing**: grep the whole binding for `D44` after CP4; both known
  paragraphs must be gone, and nothing may be reworded in their place.
