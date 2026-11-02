# M15/P11 — .NET Admin: `LogDirDescription.IsCordoned` (closes D15 / §15 Gap 1)

**Status: DONE / CLOSED (2026-09-24).** Commits `3e051bb0` (CP1) + `7c4fd002` (CP2). Critic 81
PASS on the first pass — 1 LOW finding, non-blocking and **pre-existing outside the diff**;
archived at `COMMENTS.DONE.81.md`. That finding was fixed separately as **N=82**
(`1101bea3` + citation correction `e8952685`), archived alongside at `COMMENTS.DONE.82.md`.
**Two plan defects were found during execution and are corrected in §3.3 / §5 below** — see
the CLOSE-OUT note at the end.
**Agent number:** N=81 (Actor + Critic). Confirmed free: highest used is 80 (M15/P10), no `COMMENTS.81.md` anywhere in the repo.
**Branch:** `prashah_dev_dotnet_binding` (tip `60dd4307` = M15/P10).
**Mode: A** (.NET-only — see §1; no Rust-core work, no ABI change).
**Actor/Critic focus:** code logic and behaviour correctness. Not docstrings, not prose.

---

## 1 · The finding that decides the mode: the documented premise is now FALSE

`bindings/dotnet/src/Confluent.Kafka/Admin/LogDirDescription.cs:38-44` states, as the
justification for omitting the member:

> `IsCordoned` is DELIBERATELY ABSENT (M15/P3 decision D15, §15 Gap 1) … the C ABI
> exports no accessor for it — verified: `grep -ci "cordoned"` over the header is **0**

**That is no longer true.** The accessor exists:

```c
bool kafka_admin_LogDirDescription_is_cordoned(const kafka_admin_LogDirDescription_t *description);
```

- Header: `target/include/confluent_kafka.h:4286` (`grep -ci cordoned` now returns **3**, not 0).
- Rust: `src/ffi/admin.rs:4951`, body `unsafe { log_dir_ref(description) }.is_cordoned`.
- Rust core has had it all along: `src/admin/log_dir_description.rs:119`, populated from the
  wire at `src/admin/kafka_admin_client.rs:2242-2247`.
- **Added 2026-09-23 by `4fea817a`** — "feat(ffi/admin): expose LogDirDescription.isCordoned()
  over the C ABI (finding 2)", i.e. a Rust-side critic finding, one day before this phase.
  D15 was correct when written and went stale a day ago. This is the
  [[dotnet_admin_perkey_abi_m17p1]] pattern again: a premise recorded as verified,
  re-verified here rather than trusted.

So this is **Mode A**: one `[DllImport]`, one property, one marshal read, and the removal of a
deliberate absence. **No Rust-core dependency, nothing outside `bindings/dotnet/`.**

## 2 · Java contract

`kafka/clients/src/main/java/org/apache/kafka/clients/admin/LogDirDescription.java`

- `:94` `public boolean isCordoned()` — plain getter over the `:36` field.
- `:46` the 5-arg constructor takes it; the `:38` / `:42` constructors default it to `false`.
- `:105` `toString()` renders `", isCordoned=" + isCordoned` as the **last** field, after
  `usableBytes`.
- Producer of the value: `KafkaAdminClient.java:3070`.

## 3 · Changes — 5 files, all under `bindings/dotnet/`

### 3.1 `src/Confluent.Kafka/Internal/Interop/NativeMethods.Admin.cs`
Add 1 `[DllImport]` for `kafka_admin_LogDirDescription_is_cordoned`
(`(IntPtr description) -> bool`, explicit `EntryPoint` + `Cdecl`, matching the sibling
`_total_bytes` / `_usable_bytes` declarations). `internal static extern` count **473 → 474**.

### 3.2 `src/Confluent.Kafka/Admin/LogDirDescription.cs`
- Add `public bool IsCordoned { get; }` (Java `:94` is a plain `boolean`, **not** nullable —
  do not model it as `bool?`; `TotalBytes`/`UsableBytes` are `long?` only because the ABI
  uses `-1` as an unknown sentinel, which has no analogue here).
- Add the parameter to the `internal` constructor.
- `ToString()`: append `, isCordoned={4}` after `usableBytes`, matching Java `:105` field order.
- **Delete** the D15 "deliberately absent" doc blocks at ~`:38-44`, and the absence clauses
  at ~`:58`, ~`:73`, ~`:138`. Delete outright — do **not** reword into "formerly absent"
  or "now closed" (M14/P1's lesson: a claim not made cannot go stale).

**⚠ The `ToString()` trap.** Java prints `true` / `false`; C# `bool.ToString()` prints
`True` / `False`. Render lowercase explicitly. A `string.Format` of the raw `bool` compiles,
reads naturally, and is wrong — and the parity test must assert the lowercase form or it
will pin the bug.

### 3.3 `src/Confluent.Kafka/Internal/NativeAdminClient.cs`
Read the new accessor at the log-dir marshal site and pass it to the constructor. Delete the
`:2515` comment asserting the type is "free of a faked `IsCordoned`".

### 3.4 `tests/.../PublicAdminLogDirsShapeParityTests.cs` (~`:333`)
`LogDirDescription_HasNoCordonedMember_AndTheSweepThatProvesItFindsTheOthers` is a
**deliberate tripwire** — its own doc says "this test is what keeps it absent". It goes red
the moment §3.2 lands. That is the tripwire working, not a regression.

**Invert it, do not delete it:** assert `IsCordoned` **is** present, and **keep its positive
control** — the same member walk must still find `TotalBytes`, because an assertion over a
reflection walk is vacuously true if the walk returns nothing. Rename to match. Also update
the constructor-visibility paragraph: the `internal` constructor was justified by "a public
one would take a parameter the type cannot store", which no longer holds. **Keeping the
constructor `internal` is still correct** (only the result marshaller builds one, consistent
with the sibling admin result types) — but the *stated reason* must change or be dropped.
Do not make it public in this phase.

### 3.5 `tests/.../PublicAdminLogDirsTests.cs` (~`:235`)
The `ToString()` test documents the `, isCordoned=…` omission as intentional. Update it to
expect the field, in Java's position, **lowercase**.

## 4 · Coverage

Reachable and required:
1. `IsCordoned` round-trips `true` and `false` through the marshal path.
2. `ToString()` matches Java's field order **and** lowercase rendering, both values.
3. The inverted shape-parity sweep passes **and** its positive control still fires.

Unlike M15/P10, there is no mock-reachability problem here: `DescribeLogDirs` is exercised by
the existing `PublicAdminLogDirsTests` fixtures, so all three are genuinely testable. If the
Actor finds otherwise, that is a finding to report, not to work around.

## 5 · Checkpoints (one commit each; hard stop after each)

| CP | Content |
|---|---|
| CP1 | §3.1 + §3.2 + §3.3 — the `[DllImport]`, the property, ctor, `ToString()`, marshal read, and every D15 absence block deleted. Build green; the §3.4 tripwire goes **red by design**. |
| CP2 | §3.4 + §3.5 test updates + §4 coverage, then the full gate. |

Two checkpoints, not five — the work is one accessor and the removal of a defended absence.

**Gate (CP2):** `cargo build --features ffi` (repo root, expected a no-op — no Rust change) →
`dotnet build` TFM matrix 0W/0E → `dotnet test -f net10.0` → `dotnet format --verify-no-changes`
→ `cargo xtask format-check` **from the repo root**. `dotnet` is not on `PATH` (`~/.dotnet/dotnet`).

## 6 · Risks

- **The `True`/`False` casing trap** (§3.2) — the one real correctness risk in the phase.
- **Deleting the tripwire instead of inverting it**, or inverting it while dropping its
  positive control, leaving a vacuously-green sweep.
- **D15 text resurfacing**: after CP1, `grep -rni "cordon" bindings/dotnet/src bindings/dotnet/tests`
  (excluding `bin/`, `obj/`) must show only live code and the new assertions — no surviving
  "deliberately absent" claim, and no reworded successor. Stale `bin/**/Confluent.Kafka.xml`
  artifacts are build output; ignore them.

---

## CLOSE-OUT (2026-09-24) — two defects in this plan, recorded not silently fixed

1. **§3.3 named the wrong file.** `NativeAdminClient.cs` has no `LogDirDescription`
   construction site. The only one in `src` is `LogDirMarshal.cs:157` (`CopyOutDescription`),
   which is where the accessor read went; `NativeAdminClient.cs` was touched only to delete a
   stale comment. Critic 81 confirmed the read is correctly placed.

2. **§5 understated CP1's blast radius.** It said the §3.4 tripwire "goes red by design". In
   fact adding a *required* ctor parameter broke the **whole test project** (`CS7036`) at three
   4-arg call sites — `AdminLogDirsMarshalTests.cs:259` (a file this plan never listed),
   `PublicAdminLogDirsTests.cs:247` and `:256`. CP2's real scope was 4 test files, not 2.
   Generalizable: *adding a required constructor parameter is a source-breaking change to every
   caller, not just the one test that asserts on the member.*

## Follow-on: N=82, the casing bug this phase's rule caught elsewhere

PLAN §3.2 flagged the `True`/`False` trap and P11 avoided it. Critic 81 then found the same bug
in **pre-existing** types. Fixed in `1101bea3`:

- Critic 81 named **2** outliers from a LogDirs-scoped review. An instruction to **re-sweep
  rather than trust that count** found **5 rendered bools across 4 types** (`ReplicaInfo`,
  `TopicListing`, `TopicDescription`, `ConfigEntry`×2). Fixing only the 2 named would have left
  3 behind green tests. **A count observed inside a narrower review is an observation, never an
  exhaustive sweep.**
- Critic 82 independently re-derived the sweep (no 6th outlier) and ruled out the two ways one
  could hide: no `record` type exists in `src/` (a compiler-generated `ToString` emits
  `True`/`False`), and no interpolated/non-override `ToString` renders a bool.
- Two near-misses correctly left alone: `MemberDescription.Upgraded` (already a ternary,
  matching Java's `orElse(null)`) and `ConfigEntry`'s `IsSensitive ? "Redacted" : Value`
  (a string-valued ternary, not a rendered bool).
- Critic 82's only finding — two past-EOF Java citations — fixed in `e8952685`.
