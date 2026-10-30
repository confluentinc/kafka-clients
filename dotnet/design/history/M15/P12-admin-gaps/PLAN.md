# M15/P12 — .NET Admin: close the three residual gaps

**N = 83.** Base **`2d9f5325`**, branch `prashah_dev_dotnet_binding`.

⚠ **Corrected after CP1.** This plan was drafted against `1bea0efc`, but
`2d9f5325` ("fix(ffi/admin): factor seed_scram_result's row type…") landed
before CP1 began and **touches `src/ffi/admin.rs`** — so the §0 Mode-A command
run against `1bea0efc` reports a core diff that no checkpoint of this phase
caused. Verified: `git diff 2d9f5325..HEAD -- src/ src/ffi/ cbindgen.toml generator/`
is empty; `2d9f5325` is an ancestor of HEAD. **CP2–CP7 use `2d9f5325`.**
Generally: if the branch advances mid-phase, the Mode-A base is the actual
parent of that checkpoint's first commit — state which SHA was used, rather
than carrying a hardcoded one that has gone stale.

Derived by unfiltered `find -name 'COMMENTS*.md'` across the whole repo (not by
counting phases). Highest used anywhere = **82**
(`bindings/dotnet/design/history/M15/P11-logdir-is-cordoned/COMMENTS.DONE.82.md`);
root repo's own sequence tops out at 79. Next free = **83**. Confirmed by the
user.

**Plan location** follows the M15 phase convention
(`design/history/M15/P<n>-<slug>/PLAN.md`, as P1…P11 all do).
`design/current/PLAN-M15-admin-client.md` is the *roadmap*, not a phase plan.

**Seven checkpoints**, seven hard stops. One Critic pass at the very end.

---

## 0. Standing constraints for this phase

Relay verbatim into every downstream Actor/Critic brief:

1. **The user is extremely short on tokens. Focus solely on code logic and
   behaviour correctness.** No long prose, no XML-doc essays, no elaborate
   comment blocks. Terse, correct code. This is a deliberate, user-granted
   relaxation of this repo's usual heavy-prose convention, **for this phase
   only**, and it binds the **Actor and the Critic both**. It does NOT relax
   correctness, Mode-A proof, or test coverage.
2. **Hard stop after EVERY checkpoint**, awaiting the user's explicit
   go-ahead. (Standing M15 requirement — the P10 cadence.)
3. **The Critic runs exactly ONCE, at the very end of CP7, and ONLY after the
   user explicitly grants permission.** Never mid-phase, never per-checkpoint.
4. One checkpoint = one commit = one session's work. Resumable.

### Environment traps (verbatim into the Actor brief)

- A clobbered `PATH` can hide an installed `cargo` — verify before declaring a
  toolchain missing. `dotnet` is not on `PATH`: use `~/.dotnet/dotnet`.
- `grep` is `ugrep`: empty regex alternations like `(a|)` error out.
- There is no `sed`.
- `cat` may be `bat`.
- zsh does not word-split unquoted vars.
- **A zero-match libtest run still prints a pass.** Confirm a filter matches a
  known-present test before trusting a green.
- `cargo xtask format-check` false-fails unless run from the repo root.
- net8.0 IS executable locally (`~/.dotnet` has 8.0.30 + 10.0.11); only net462
  is build-only.
- The .NET gRPC local Docker gate recipe is in
  `.claude/agent-memory/project-manager/project_m8p1_dotnet_grpc_local_gate_recipe.md`
  — CP7 depends on it. Summary: cross-build a **linux/amd64** `.so`
  (`docker run --rm --platform linux/amd64 -v "$PWD":/work -w /work rust:1-bookworm cargo build --features ffi --release --target-dir target-linux-amd64`),
  stage it at `target/release/libconfluent_kafka.so`, set
  `DOCKER_DEFAULT_PLATFORM=linux/amd64` for `make -C bindings/dotnet grpc-image`,
  then **unset** it for the `cargo test` run so the broker stays native arm64.
  Always `docker info` first — do not declare a Docker gate CI-only without it.
  Only the **sync** image is needed (see D1 in §7).

### Mode-A proof obligation

Every checkpoint must show:

```
git diff <base>..HEAD -- src/ src/ffi/ cbindgen.toml generator/      # EMPTY
```

- **CP1 and CP2 are C#-only**; that command is the whole proof.
- **CP3 necessarily touches `tests/common/*.rs`** (D4 Option 2 moved the
  harness glue there from CP7). That is *harness-test* Rust, not core/ABI Rust
  — the Mode-A command above does not cover `tests/` and must **still come back
  empty**, so **CP3 is NOT a Mode-B escalation**. Precedent: M12/P1 and M8/P1
  both landed `tests/common/` glue under Mode A. From CP3 on, add a second,
  bounding command:

```
git diff --stat <base>..HEAD -- tests/
# must list ONLY these four:
#   tests/common/multilanguage_admin_test_macro.rs   (the arm)
#   tests/common/backend_factory.rs                  (the factory impl)
#   tests/common/backend_pool.rs                     (doc-only: BackendKind::Dotnet)
#   tests/common/admin_backend.rs                    (doc-only: "four backends" -> five)
```

⚠ **Corrected at CP3 — this list said two files and was wrong.** §5.1 of this
same plan orders the `backend_pool.rs` and `admin_backend.rs` doc edits, so the
two-file bound contradicted it and could not hold. Both extras are **doc-only**
and that is the thing to verify: `git diff` filtered of comment lines must
yield **zero** lines for those two. The *glue* stays confined to the first two
files. A logic change in either doc-only file is an escalation.

- New `[DllImport]`s: **CP1 adds exactly 5. Every other checkpoint adds 0.**
  Count with `grep -c 'internal static extern'`, not `grep -c 'DllImport'`
  (the latter over-counts).

---

## 1. Checkpoint list

Ordering is **Gap 3 → Gap 2 → Gap 1**: trivial, then cheap, then large — so
the early work lands fast and the user can bail before the expensive part.

| CP | Gap | Scope | RPCs | Owner |
|----|-----|-------|------|-------|
| **1** | 3 + 2 | Stale `IAdmin.cs` cross-ref + P10 sweep; bind 5 `MockAdminClient` seeding ABI fns | — | dotnet-actor |
| **2** | 1 | C# scaffolding + G1 topics & partitions | 8 | dotnet-actor |
| **3** | 1 | **Rust harness glue (arm goes live)** + G2 cluster, configs, log dirs | 8 | dotnet-actor (**recorded exception**, §3) |
| **4** | 1 | G3 elections/reassignments/offsets **+** G6 producers/transactions | 4+6 | dotnet-actor |
| **5** | 1 | G4 groups | 9 | dotnet-actor |
| **6** | 1 | G5 acls, quotas, scram, tokens, features | 13 | dotnet-actor |
| **7** | 1 | Full Docker gate + pre-existing `BackendKind::DotnetAsync` doc one-liner | — | dotnet-actor |

48 RPCs total, covering all 80 admin scenarios. CP2/CP3/CP5/CP6 stay
unpaired — each is already at or past the size that crashed M15/P5 four times.

---

## 2. CP1 — Gap 3 + Gap 2

Two unrelated, independently small items, paired into one session on the
user's ruling. They share no files.

### 2a. Gap 3 — stale public-API cross-reference

`bindings/dotnet/src/Confluent.Kafka/Admin/IAdmin.cs` lines 1124-1126, inside
`DescribeUserScramCredentials`'s `<returns>`:

```
/// The three derived accessors Java publishes. See
/// <see cref="DescribeUserScramCredentialsResult"/>, including its recorded
/// <c>RESOURCE_NOT_FOUND</c> divergence.
```

M15/P10 resolved D44 and deleted that content; `DescribeUserScramCredentialsResult.cs`
now has zero occurrences of `RESOURCE_NOT_FOUND` / `RNF` / `divergence`. The
cross-reference points at nothing.

**Fix: delete the stale clause outright.** Do NOT reword or re-scope it in
place — that is the M14/P1 lesson (`ffi §A6` form C): a re-worded stale claim
produces the next round's false claim; deletion is strictly shrinking and
cannot introduce a new one.

**Also sweep** for sibling references left dangling by P10's deletion:
`grep -rn 'RESOURCE_NOT_FOUND\|RNF\|divergence' bindings/dotnet/src/` and
`grep -rn 'DescribeUserScramCredentialsResult' bindings/dotnet/src/`, and check
each surviving hit still describes something that exists. ⚠ **A count observed
inside a narrower review is an observation, never an exhaustive sweep**
(the N=82 lesson: 2 named outliers were really 5). Re-derive; do not treat the
three strings above as the complete list.

### 2b. Gap 2 — make `MockAdminClient` seedable

Five ABI functions are exported and unbound in .NET
(`kafka_admin_MockAdminClient_{set_feature_levels, timeout_next_request,
update_beginning_offsets, update_end_offsets, update_consumer_group_offsets}`).
Java's `MockAdminClient` has all five; Python binds all five; C's tests use
them. .NET binds only `_new` — `MockAdminClient.cs` has exactly one public
non-`IAdmin` member (its constructor), so the mock is inert.

**This is Mode A** — the ABI is already there;
`PLAN-M15-admin-client.md §2`'s own evidence table cites "+ 5 seeding
functions". Never scoped, never deferred: simply missed.

**Add**, as inherent methods on the concrete `MockAdminClient` (not on
`IAdmin` — mock-configuration methods are inherent, per `admin-client.md §9`),
named per Java + Python parity:

| Java | .NET |
|---|---|
| `timeoutNextRequest(int)` | `TimeoutNextRequest(int numTimeouts)` |
| `updateBeginningOffsets(Map)` | `UpdateBeginningOffsets(IDictionary<TopicPartition,long>)` |
| `updateEndOffsets(Map)` | `UpdateEndOffsets(IDictionary<TopicPartition,long>)` |
| `updateConsumerGroupOffsets(Map)` | `UpdateConsumerGroupOffsets(IDictionary<TopicPartition,long>)` |
| `setFeatureLevels(Map)` | `SetFeatureLevels(IReadOnlyDictionary<string,(short Level, short MinSupported, short MaxSupported)>)` |

Exact signatures are to be derived from `target/include/confluent_kafka.h`
and cross-checked against Java + `bindings/python/admin.py` — the header is the
contract. Follow the established per-key marshalling and SafeHandle-param
convention already used by the admin binding; do not invent a new one.

**Settled at CP1** (the header won, as instructed — recorded so the CP7 Critic
does not read these as unapproved deviations):
- `SetFeatureLevels` takes a **3-tuple per feature**, not a bare `short`. The
  header takes four parallel arrays (`features`, `levels`, `min_levels`,
  `max_levels`), collapsing Java's three separate `MockAdminClient.Builder`
  setters onto one key set; the Rust mock's `describe_features` reads all
  three (`mock_admin_client.rs:1944-46`), so a single level would report
  `SupportedVersionRange(0,0)` beside a non-zero finalized level. Matches
  `bindings/python/admin.py:3591`.
- `IReadOnlyDictionary`, not `IDictionary` — the admin binding's existing input
  convention (`CreatePartitions`, `DeleteRecords`).
- One doc line uses `<c>ListConsumerGroupOffsets</c>` rather than a `cref`: the
  overload pair makes the cref CS0419, which is an error here.

**Doc:** `MockAdminClient.cs`'s class doc acknowledges a *different* clipping
(Java's explicit broker list / controller) but not this one. **One line** of
correction, no more.

**Out of scope, one line:** the Rust core's `set_broker_log_dirs`, `add_topic`,
`mark_topic_for_deletion` are **not** at the C ABI — Mode B, excluded.

### CP1 gate

`dotnet build` 0W/0E both TFMs; `dotnet test -f net10.0` green (2309 existing +
the new mock tests); `dotnet format --verify-no-changes`; Mode-A diff empty;
**exactly 5** new `internal static extern`.

**Verified:** each of the 5 seeding calls changes what the subsequent admin RPC
returns. A binding that P/Invokes but whose effect is unobserved is not tested.
And no dangling `<see cref="...">` or prose claim in the admin public surface
points at deleted P10 content.

---

## 3. Gap 1 — ownership ruling (CP2–CP7)

`bindings/dotnet/CLAUDE.md §8.1` says `dotnet-actor` builds only C#, header-down,
and does not author Rust. Gap 1 spans both sides:

- **C# (unambiguously `dotnet-actor`'s):** `AdminServiceImpl.cs`, the
  `.csproj` `<Protobuf>` entry for `admin_service.proto`, admin directions in
  `Translate.cs`, `Program.cs` registration.
- **Rust (nominally root `actor-executor`'s):** one `#[cfg]`-gated dotnet arm
  in `tests/common/multilanguage_admin_test_macro.rs`, plus
  `impl AdminBackendFactory for DotnetGrpcFactory` in
  `tests/common/backend_factory.rs`.

**Ruling: `dotnet-actor` authors the Rust as an explicit, recorded exception —
not silently.**

⚠ **This exception moved from CP7 to CP3** when D4 was settled as Option 2
(2026-09-24). Same two files, same scope, same justification — only the
checkpoint changed. The end-of-phase Critic should read the Rust in CP3 as
sanctioned, not as an Actor exceeding its role.

Justification, stated so a reviewer can check it:

1. **It is not translation Rust.** `AdminBackend`'s 106 `async fn`s have exactly
   one gRPC implementation, `MultilanguageAdmin`, already shared verbatim by the
   python / python_async / c factories — the per-backend `impl
   AdminBackendFactory` block differs **only in a label string**. The .NET block
   is a mechanical copy of the existing `CGrpcFactory` block with `"c"` →
   `"dotnet"`. Likewise the macro arm is a copy of the `__grpc_c` arm with the
   `BackendKind` and factory swapped. Total: roughly 30 lines, zero new logic,
   zero Java translation.
2. **Direct precedent, twice.** M8/P1 and M12/P1 both had the `dotnet-actor`
   author exactly these two files for the consumer and producer matrices, each
   as an approved exception. CP7 is the third instance of the same edit in the
   same two files.
3. **Routing it elsewhere costs more than it buys.** `actor-executor` would
   have to load the whole .NET harness/image/flavor context to write 30 lines
   it cannot verify without the C# servicer from CP2–CP6 — against a hard token
   constraint.

**Bound:** the Rust is confined to those two files, enforced by the
`git diff --stat -- tests/` check in §0. Any Rust edit outside them is an
escalation to the user, not a judgement call. `src/`, `src/ffi/`,
`cbindgen.toml` and `generator/` must stay empty in every checkpoint's diff.

---

## 4. CP2–CP6 — Gap 1, the C# servicer

### Why staged, not all-at-once

The proto defines **48 RPCs** (46 admin + `CreateAdmin` + `Close`; the "49"
figure counts one extra). Python's sync `AdminService` is ~850 lines; the C#
equivalent with translation helpers will be substantially larger. M15/P5 hit
**four crashes** attempting a 9-RPC phase in one session, forcing resumable
one-RPC-per-session checkpoints. 48 in one go will not survive.

Staging follows the proto's **own section grouping**, which already mirrors the
integration-test file grouping — so each checkpoint maps to a nameable slice of
the 80 scenarios:

| CP | Slice | RPCs | n | Test files it unblocks |
|----|-------|------|---|---|
| 2 | G1 topics & partitions **+ scaffolding** | CreateAdmin, Close, CreateTopics, DeleteTopics, ListTopics, DescribeTopics, CreatePartitions, DeleteRecords | 8 | `admin_topics_test.rs` (9), `admin_partitions_records_test.rs` (7), `multilanguage_admin_test.rs` (1) |
| 3 | G2 cluster, configs, log dirs | DescribeCluster, DescribeConfigs, IncrementalAlterConfigs, ListConfigResources, ListClientMetricsResources, DescribeLogDirs, AlterReplicaLogDirs, DescribeReplicaLogDirs | 8 | `admin_cluster_configs_test.rs` (7), `admin_log_dirs_test.rs` (4) |
| 4 | G3 elections, reassignments, offsets | ElectLeaders, AlterPartitionReassignments, ListPartitionReassignments, ListOffsets | 4 | `admin_elections_reassignments_offsets_test.rs` (8) |
| 4 | G6 producers & transactions | DescribeProducers, DescribeTransactions, AbortTransaction, ForceTerminateTransaction, ListTransactions, FenceProducers | 6 | `admin_transactions_test.rs` (11) |
| 5 | G4 groups | ListGroups, ListConsumerGroups, DescribeConsumerGroups, DescribeClassicGroups, ListConsumerGroupOffsets, AlterConsumerGroupOffsets, DeleteConsumerGroupOffsets, DeleteConsumerGroups, RemoveMembersFromConsumerGroup | 9 | `admin_groups_test.rs` (12), `admin_group_offsets_test.rs` (5) |
| 6 | G5 acls, quotas, scram, tokens, features | CreateAcls, DescribeAcls, DeleteAcls, DescribeClientQuotas, AlterClientQuotas, DescribeUserScramCredentials, AlterUserScramCredentials, CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, DescribeDelegationToken, DescribeFeatures, UpdateFeatures | 13 | `admin_acls_test.rs` (5), `admin_quotas_test.rs` (3), `admin_scram_test.rs` (2), `admin_delegation_tokens_test.rs` (2), `admin_features_test.rs` (4) |

G6 is **pulled forward** out of proto order to sit with G3 in CP4. That is
safe — see §4.1.

### 4.1 Why pairing G3 with G6 is safe

Checked, not assumed:

- **No shared files beyond the two every Gap-1 checkpoint touches.** Both add
  servicer overrides to `AdminServiceImpl.cs` and translation helpers to
  `Translate.cs`. Neither touches `.csproj`, `Program.cs`, `NativeMethods`, or
  any `src/Confluent.Kafka/Admin/*.cs` — the `.csproj` proto entry and the
  `Program.cs` registration are both done once, in CP2. Since a checkpoint is
  one session and one commit, two groups editing the same two files is a single
  coherent edit, not a merge.
- **No ordering dependency.** Every RPC is an independent override on the
  generated servicer base; the only shared mutable state is the id map, created
  in CP2. Nothing in G6 reads anything G3 writes, or vice versa. So pulling G6
  forward past G4/G5 changes nothing.
- **Combined weight is comparable to the unpaired CP5, not larger.** Raw RPC
  count (10) is misleading — what costs is the number of *rich* response
  translations, since `VoidKeyedResponse` / `StatusResponse` handlers are a few
  lines each. Verified against the proto:
  - G3: `ElectLeaders`→Void, `AlterPartitionReassignments`→Void (2 trivial);
    `ListPartitionReassignments`, `ListOffsets` (2 rich).
  - G6: `AbortTransaction`→Status, `ForceTerminateTransaction`→Status
    (2 trivial); `DescribeProducers`, `DescribeTransactions`,
    `ListTransactions`, `FenceProducers` (4 rich).
  - **CP4 total: 4 trivial + 6 rich, none heavy.**
  - CP5 (G4, unpaired): 4 trivial (`AlterConsumerGroupOffsets`,
    `DeleteConsumerGroupOffsets`, `DeleteConsumerGroups`,
    `RemoveMembersFromConsumerGroup` — all Void) + 5 rich, **two of which are
    the heaviest in the whole proto** (`DescribeConsumerGroups` /
    `DescribeClassicGroups`: member lists, assignments, `ConsumerProtocol`-decoded
    classic assignments).

  So CP4 carries 6 rich translations of ordinary difficulty against CP5's 5
  including two heavy ones. Pairing G3+G6 does **not** create the largest
  checkpoint, which is why it is the right pair despite the higher RPC count.
  No substitute pairing is needed.

### 4.2 CP2 carries the scaffolding

- `.csproj`: add
  `<Protobuf Include="$(ProtoRoot)/admin_service.proto" ProtoRoot="$(ProtoRoot)" GrpcServices="Server" />`.
  Build-config only; proto **content** is untouched (same shape as M12/P1's
  producer flip).
- `AdminServiceImpl.cs`: singleton servicer, `ConcurrentDictionary<int, IAdmin>`
  id map + `Interlocked` counter (producer precedent — no per-op lock; admin is
  thread-safe).
- `Program.cs`: `AddSingleton<AdminServiceImpl>()` + `MapGrpcService`, in the
  **sync branch only** (see D1 in §7).
- `Translate.cs` (or a new `TranslateAdmin.cs` if it grows unwieldy): admin
  directions. Python factors these into shared `_admin_*` helpers
  (`_admin_selects_mock`, `_admin_timeout`, `_admin_void_response`,
  `_resolve_admin_futures`, …) — mirror that factoring so later checkpoints
  are additive.
- **`CreateAdmin` mock selection MUST mirror the normative predicate.** The
  proto's `CreateAdminRequest` comment calls it "the normative mock-selection
  rule for all three servers"; Python implements it as `_admin_selects_mock`.
  Port that predicate exactly — a divergence here silently routes whole
  scenarios to the wrong client.
- Per-key vs whole-call error placement: a raised error is a whole-call
  failure → response top-level `error`, `entries` empty; per-key failures never
  raise and arrive in the keyed map. Same contract as Python; do not invent a
  third convention.

### 4.2b The servicer is structurally untestable in-process — found at CP2

Verified at CP2, not assumed: **grpc-server cannot be reached from any existing
test project.** `grep -c GrpcServer Confluent.Kafka.sln` → **0**; grpc-server
has no `InternalsVisibleTo`; and `Confluent.Kafka.UnitTests` multi-targets
`net462;net8.0;net10.0`, whose net462 leg cannot `ProjectReference` a net8.0
`Microsoft.NET.Sdk.Web` project. Both routes to reach `TranslateAdmin` /
`AdminServiceImpl` from a unit test would widen the build and `dotnet format`
graph that the phase docs deliberately keep grpc-server out of.

**Consequence — the real risk of this phase.** CP2–CP6 accumulate the entire
C# servicer (825 lines at CP2 alone, likely ~2500 by CP6) with **zero automated
verification**, and it all gets verified at once by CP7's Docker gate. Five
checkpoints of unverified code followed by one big-bang gate is the phase's
main exposure; the per-checkpoint "tests green" line in §4.3 proves only
no-regression, never that the new servicer code is correct.

**Mitigation actually used at CP2** (keep doing this): a scratchpad-only
net10.0 console app that reflects into the internal servicer and drives each
RPC group against `MockAdminClient`, plus a TFM-swap boot smoke to prove the DI
registration and `MapGrpcService` wiring. Never staged, never committed. This
is genuine evidence but it is not a regression gate — it does not run again.

**RESOLVED — D4 Option 2 (2026-09-24):** the Rust arm is wired at **CP3**, so
the real 80-scenario gate runs from CP3 onward. Big-bang verification is
replaced by incremental. See §4.3.

### 4.3 Per-checkpoint gate for CP2–CP6

**CP2 only** (the arm was not yet live): build 0W/0E both TFMs, `dotnet test -f
net10.0` green, `dotnet format --verify-no-changes`, Mode-A diff empty, 0 new
`extern`. The servicer was inert, so CP2 could neither regress the suite nor be
behaviourally verified.

**CP3 onward — D4 Option 2. The real 80-scenario gate runs every checkpoint.**
In addition to the CP2 checks:

```
cargo test --features integration-tests,multilanguage-tests __grpc_dotnet
```

**A red matrix mid-phase is expected and is NOT a failure signal by itself.**
Scenarios whose RPC group is not yet implemented will fail. What makes the gate
meaningful is the bookkeeping:

- **The Actor MUST record the expected-red list each round** — which scenarios
  are red, and that each is red *only* because its RPC group is not yet
  implemented (name the group).
- **A red that is not on that list is a real defect and stops the checkpoint.**
- The red set must **shrink monotonically**: a scenario green in an earlier
  checkpoint and red now is a regression, never a known-red.

Without that list the red board is unreadable and the gate stops meaning
anything — which is why Option 2 is acceptable rather than reckless.

**Three corrections from CP3's first live run — fold these into every later round:**

1. **The filter's scope is WIDER than this phase.** `__grpc_dotnet` matches **152**
   tests, only **80** of which are admin. CP3 saw 90 reds: 51 admin (the real
   expected-red set) plus **39 that belong to neither this phase nor this image** —
   36 `__grpc_dotnet_async` arms (only the **sync** image is built per D1, so the
   container never starts: `pull access denied`) and 3
   `producer_transactions_test::*__grpc_dotnet` (**pre-existing** — the .NET producer
   has no transactional surface, so the generated servicer base answers
   `Unimplemented`; `grep -c InitTransactions ProducerServiceImpl.cs` → 0). An
   expected-red list derived from RPC groups **alone** leaves those 39 unexplained,
   which is exactly how a real red hides. Partition the reds into
   *admin-group-unimplemented* / *image-not-built* / *pre-existing* and account for
   all three.
2. **Reconcile the GREEN side too, not just the red.** Assert
   *implemented-group arm count == passing-admin arm count*. CP3's counts were 28 vs
   **29**, and that one-arm gap is the only thing that surfaced an **accidental
   pass**: `admin_groups_test::test_ml_admin_remove_members_rejects_an_explicitly_empty_selection__grpc_dotnet`
   passes with `RemoveMembersFromConsumerGroup` unimplemented, because its `Err` arm
   only asserts `err.message() != "Invalid empty members has been provided"` — which
   `Unimplemented`→`LocalIllegalState` satisfies. It proves nothing about the .NET
   servicer. **✅ RESOLVED at CP5** — now passes genuinely. Three independent
   witnesses, none of them "it is still green": (a) the **sibling** arm
   `remove_all_members_from_consumer_group`, which `unwrap`s and so *cannot* pass
   under `Unimplemented`, was RED in CP4's log and is GREEN at CP5 — one red, one
   green, one handler is the accidental-pass signature; (b) green-count
   reconciliation — CP4's surplus of exactly 1 over the legitimate 47, and newly
   green at CP5 = 16 = G4's 17 − that 1; (c) a reflection probe confirming
   `DeclaringType == AdminServiceImpl`, making the generated base's
   `Unimplemented` structurally unreachable.
   **The reusable technique: to prove a tolerant assertion is really exercised,
   find a sibling scenario over the same RPC whose assertion is strict, and show
   it flipped red→green.** Re-running the tolerant arm can never establish this.
3. **A red set that shrinks is necessary but not sufficient** — a scenario can move
   from red to green without its RPC being implemented (item 2). Shrinkage plus the
   green-side reconciliation is the real gate.

---

## 5. The Rust harness glue (**lands at CP3**) and CP7's residual scope

⚠ **D4 Option 2 moved this glue from CP7 to CP3** (2026-09-24). It is listed
here as one unit for readability; the work happens at **CP3**, and **no part of
it remains at CP7**.

### 5.1 The glue — CP3

- `tests/common/multilanguage_admin_test_macro.rs`: **one** additive
  `#[cfg(feature = "multilanguage-tests")]` arm, `__grpc_dotnet`, copied from
  the `__grpc_c` arm with `BackendKind::Dotnet` and the matching factory. The
  macro goes from four arms to five.
- `tests/common/backend_factory.rs`: `impl AdminBackendFactory for
  DotnetGrpcFactory`, `type Admin = MultilanguageAdmin`, label `"dotnet"`,
  `needs_container_bootstrap() -> true`. Copy of the `CGrpcFactory` block.
- Refresh the `BackendKind::Dotnet` doc comment, which says **"consumer-only"**
  — this phase makes it stale. One line, no essay.
- `admin_backend.rs`'s doc comments say "all four backends" in several places.
  Update the count to five; do not expand the prose.
- `Dockerfile.grpc` and `backend_pool.rs` (port 50053) already exist from the
  producer/consumer work — **expected to need no change**. If one does, that is
  a finding to report, not to absorb silently.

### 5.2 CP7's remaining scope

Only two things:

1. The **full** Docker gate — the whole 80-scenario admin matrix green on
   `__grpc_dotnet`, plus the free producer/consumer `__grpc_dotnet`
   no-regression sweep the same filter picks up.
2. ⚠ `BackendKind::DotnetAsync`'s "consumer-only" doc, which is **already
   stale** w.r.t. M12/P1 (it serves producer too) and which this phase does not
   change the behaviour of. One line, labelled **pre-existing** in the commit
   message so the Critic does not read it as scope creep.

### What the local gate actually proves

Only **3 of 6** backends run in this environment. The Python and C images do
not build on Apple-Silicon macOS, and the Python C extension cannot build
natively here at all.

**Runnable locally — and these are exactly the ones that matter for this phase:**

- `__rust` — 80 scenarios, already green; the oracle.
- `__grpc_dotnet` — 80 scenarios, **new**.

(`__grpc_dotnet_async` is not created — D1, §7.)

**The local gate therefore proves the substantive claim of Gap 1:** every one
of the 48 admin RPCs round-trips correctly through the .NET servicer against a
real broker, asserted by the same test bodies that assert against the native
Rust admin client. This is a genuine, sufficient proof of the new work — not a
smoke test.

**The local gate does NOT prove**, and only CI can:

- `__grpc_python`, `__grpc_python_async`, `__grpc_c` are still green
  (no-regression). Structurally near-zero risk: every CP7 edit is additive and
  `#[cfg]`-gated, and the shared `MultilanguageAdmin` client is untouched — but
  "near-zero" is an argument, not a measurement.
- The full **320 → 400** instance matrix under CI's amd64 Linux runner.

State both halves in the close-out. Do not report the phase as CI-verified on
the strength of the local run, and do not report the local run as a smoke test
on the strength of the missing backends.

### CP7 gate

`docker info` first. Then the §0 recipe, then:

```
cargo test --features integration-tests,multilanguage-tests __grpc_dotnet
```

⚠ Confirm the filter matches a known-present test before trusting the result —
a zero-match libtest run prints a pass. Note this filter also re-runs the
existing producer and consumer `__grpc_dotnet` arms, which is a free
no-regression sweep; the admin arms are the new ones. Plus `cargo xtask
format-check` and `cargo xtask lint` from the **repo root**, and the
`git diff --stat -- tests/` bound from §0.

**Then stop.** The Critic (N=83) runs over the whole phase only after the user
explicitly grants permission.

---

## 6. Out of scope (one line each)

- Mode-B Rust-core mock methods `set_broker_log_dirs` / `add_topic` /
  `mark_topic_for_deletion` — not exported at the C ABI.
- An `IAsyncAdmin` / `AsyncAdminClient` surface for .NET — the core's Admin is
  sync-returning-futures (`admin-client.md §1`); there is no async twin to bind.
- A `__grpc_dotnet_async` admin arm — D1, §7.
- Any change to `admin_service.proto` content, or new RPCs.
- Any change to the Python or C backends, or to `MultilanguageAdmin`.
- New admin integration scenarios — the 80 existing ones are the target.
- Pushing anything; the user manages pushes.

---

## 7. Settled decisions

**D1 — sync arm only. SETTLED: no `__grpc_dotnet_async` admin arm.**
Python has a real `AsyncAdminClient` / `AsyncMockAdminClient`, so its async
admin arm exercises genuinely different code. **.NET has no async admin
surface** (verified: zero hits for `IAsyncAdmin` / `AsyncAdminClient` under
`bindings/dotnet/src/`), so an async arm would drive the *same*
`AdminServiceImpl` over the *same* single `IAdmin` — byte-identical managed
code. That is ~80 extra Docker test instances for **zero** additional coverage.

Consequences, applied throughout this plan: the macro gains exactly **one** new
arm; `Program.cs` registers `AdminServiceImpl` in the **sync branch only**;
only the sync image is rebuilt for the CP7 gate; the matrix grows 320 → 400.

Precedent for an asymmetric row exists — `BackendKind`'s own docs already carve
out per-service asymmetry for the dotnet kinds. **Flipping the async arm on
later is a one-line macro change** plus registering the same singleton in the
async branch; nothing in this plan forecloses it.

**D2 — SETTLED: N=83**, plan at
`bindings/dotnet/design/history/M15/P12-admin-gaps/PLAN.md` per the P1–P11
convention.

**D4 — SETTLED (2026-09-24): Option 2. The Rust arm is wired at CP3.**
Raised at CP2, once §4.2b established that the C# servicer is structurally
unreachable from any test project, leaving CP2–CP6 unverified by anything that
re-runs. Option 1 (wire at CP7) kept a clean green matrix but rode all servicer
correctness on one gate at the end, making attribution hard if defects spanned
four checkpoints.

Consequences, applied throughout: the real 80-scenario gate runs from CP3
onward (§4.3), with unimplemented groups **known-red and shrinking** and a
mandatory per-round expected-red list; the Gap-1 ownership exception moves to
CP3 (§3); the Mode-A proof gains `tests/` in the diff from CP3 on, bounded to
two files (§0); CP7 shrinks to the full Docker gate plus one pre-existing doc
line (§5.2).

**D3 — SETTLED: seven checkpoints.** CP1 pairs Gap 3 + Gap 2; CP4 pairs G3 + G6
(safety argued in §4.1). CP2, CP3, CP5, CP6 stay unpaired — each is already at
or past the size that crashed M15/P5.
