# COMMENTS.DONE.64 — M14/P2 closed review record

**Phase:** Milestone 14 / Phase 2 — "producer callback log + `GetCallbackLog` on both .NET gRPC servicers"
**Agent number:** N=64 · **Status:** CLOSED, zero findings outstanding · **Closed:** 2026-08-30

## Resolution summary

The Critic ran **two passes** and found **no code defect** in either.

- **Pass 1 — PASS-WITH-FINDINGS: 2 × LOW.** Both were record-accuracy items against the
  `design/current/STATUS.md` entry, i.e. **Manager-owned** — the same call as P1's finding 3
  (STATUS is written in the Manager's phase close-out, per the M12/P1 precedent `7fc34a8e`).
  The Actor was told explicitly to leave them, and did. **Both are resolved in the M14/P2
  close-out commit**, and deliberately resolved by *dropping the quantifier* rather than
  re-measuring it:

  - **Finding 1** — the entry claimed the renamed `clientId` parameter had "all four" call
    sites; it has **nine** (`Append` × 5 including the one this phase added, `Response` × 4).
    "Four" was true of `Append`'s callers at the base commit. **Fix:** the count is deleted.
    The substantive property — every call site positional — is compiler-enforced anyway
    (a named argument would be CS1739, and `grpc-server` builds 0W/0E), so the count was
    carrying no weight it could not lose.
  - **Finding 2** — the entry cited `CallbackLog.cs:45-54` for the "entries survive Close"
    ownership contract, but this phase's own class-doc rewrite inserted two `<para>`s above
    it, so the range had already moved. **Fix:** the line numbers are deleted; the entry now
    cites "`CallbackLog`'s own remarks", which is the drift-proof form the shipped servicer
    field docs already use. Note the Critic's suggested replacement range (`:67-78`) had
    **itself gone stale by pass 2** (`fe9c993c` moved it to `:62-74`) — which is the argument
    for the numberless form rather than a re-measurement.

- **Pass 2 — CLEAN, no findings.** Verified the lock-rationale fix (below), including that the
  deletion lost nothing load-bearing: every thread that can enter `Append` is still named, and
  no site points at the deleted text.

## One unfiled observation, fixed anyway

The Critic left a third item **unfiled** as "inexact, not false": three sites justified
`CallbackLog`'s lock with *"one delivery-callback instance entered on several caller threads"*,
when both servicers allocate a fresh `LoggingDeliveryCallback` per `Send` — so no instance is
shared, and what the lock actually contends on is **several distinct instances appending to one
shared log**.

The Manager directed it fixed regardless: a known-inexact justification sitting in a decision
record is precisely how P1's five-cycle prose tail began. It was fixed by **deleting** the
paragraph and folding its one durable fact into the paragraph that already owns the topology,
plus two pointers — not by re-wording, per P1's structural lesson. The Actor also backed out a
first draft asserting *"a fresh instance per `Send`"*, on the sound ground that this is a
cross-file claim `CallbackLog` cannot enforce and that would go stale if a servicer ever cached
an instance, while the lock would remain correct; *"what the lock contends on"* holds under any
allocation strategy. Two `fixup!` commits, `fe9c993c` and `b4dcd9d5`.

## Operational finding recorded here because it outlives the phase

A **full-branch `--autosquash` does not complete**, and it is **pre-existing**. Scoped
autosquash is verified working (from `3f959b46` for P1's 15 fixups, from `c5d19d9e` for P2's 2 —
each `Successfully rebased`, zero `fixup!` remaining, empty squashed-vs-current diff over the
whole tree). From the branch's merge-base it fails at step 15 with
`CONFLICT (content): Merge conflict in Makefile` while replaying **M10/P1 `01a6b2c8`**, where the
M10-era fixup `fdbca58e` squashes in — reproduced identically with M14's commits absent. The
eventual squash must therefore be **scoped per phase**, with `3f959b46` as the floor (it is a
separate fix to `tests/common/backend_factory.rs` and must not be folded into M14).

## Everything below is the Critic's original file, preserved verbatim

Findings 1 and 2 appear below as filed; their resolution is recorded above.

---

# COMMENTS.64 — .NET Critic (N=64) — M14/P2 review

Scope: `5b178bf4` (source 2.1–2.6) · `182c2447` (doc-sync) · `a7033979` (self-review) on
`prashah_dev_dotnet_binding_producer`, base `c5d19d9e`. Reference: the C ABI header, the Kafka
Java public API, `multilanguage-test-server/proto/producer_service.proto`, and
`PLAN-M14-producer-delivery-callback-parity.md` §5 Phase 2 / §6 / §7.

**Verdict: PASS-WITH-FINDINGS.** No code defect. `LoggingDeliveryCallback`, the two `Send`
registrations, `override GetCallbackLog` on both servicers, the log-ownership contract, the
`clientId` rename and the ffi §A6 prohibition narrowing all check out against the proto, Python
and P1's shipped API — including the self-review fix, whose guard I re-derived independently and
found both necessary and complete. The two findings below are **record accuracy in
`design/current/STATUS.md`**, which the phase authored and which will be archived as the decision
record; both are one-line edits and neither reflects a problem in the shipped code.

Independent gate (run here, not taken from the commit messages) is at the end.

---

## 1 — LOW — `STATUS.md:17` undercounts the renamed parameter's call sites: "four" vs nine

**File:** `bindings/dotnet/design/current/STATUS.md:17` — the same sentence also appears in
`5b178bf4`'s commit-message body.

**What the record claims:**

> **Deviation from the plan — one parameter rename.** §5's 2.6 is labelled doc-only, but
> `CallbackLog.Append`/`Response`'s `consumerId` parameter is renamed `clientId`. Arity, types and
> order are unchanged (no signature **shape** change, which is what §6 protects), the type is
> `internal`, and **all four call sites are positional**.

**What the code has — nine call sites, all positional:**

| Method | Call sites |
|---|---|
| `Append` | `grpc-server/CallbackLog.cs:232`, `:236`, `:240` (rebalance), `:290` (commit), `:403` (**delivery — added by this phase**) |
| `Response` | `ConsumerServiceImpl.cs:661`, `AsyncConsumerServiceImpl.cs:736`, `ProducerServiceImpl.cs:337` (**new**), `AsyncProducerServiceImpl.cs:349` (**new**) |

"Four" was true of `Append`'s call sites at `c5d19d9e` (3 rebalance + 1 commit). This phase adds
the fifth `Append` caller and two `Response` callers, so the count was already stale in the commit
that introduced it.

**Why it matters.** The *substantive* property holds and I verified it two independent ways: a
repo-wide grep for named arguments (`consumerId:` / `clientId:` / `producerId:`) returns zero hits,
and a clean `dotnet build -c Debug --no-incremental` of `grpc-server` is 0W/0E — a named argument
anywhere would be `CS1739`, so "positional everywhere" is compiler-enforced, not asserted. So this
is not a defect in the rename, and the deviation itself is well-argued and behaviour-neutral. But
the STATUS bullet exists precisely so a later reader can re-audit a deviation from §5's "doc-only"
label cheaply, and the count is the only number in it that a re-audit would check. Suggest "all
nine call sites (five `Append`, four `Response`) are positional".

---

## 2 — LOW — `STATUS.md:14`'s `CallbackLog.cs:45-54` citation was invalidated by this phase's own class-doc rewrite

**File:** `bindings/dotnet/design/current/STATUS.md:14`.

**What the record claims:**

> A `CallbackLog` field on **each** producer servicer, owned by the **servicer** (not the id-map
> entry `Close` removes — the `CallbackLog.cs:45-54` "entries survive Close" contract).

**What the line range now points at.** `45-54` was exactly right *before* this phase — at
`c5d19d9e`, `CallbackLog.cs:45-54` is the `<b>Ownership: this lives on the SERVICER…</b>`
paragraph, and the words "entries survive Close" are on `:48` (verified with
`git show c5d19d9e:bindings/dotnet/grpc-server/CallbackLog.cs`). `5b178bf4`'s reframe inserted two
paragraphs above it, so in the shipped file:

- `CallbackLog.cs:45-54` is now the tail of the thread-topology paragraph plus the ⚠
  concurrent-`Send` paragraph — neither of which mentions ownership or `Close`;
- the ownership paragraph is `CallbackLog.cs:67-78`, with "entries survive Close" on `:71`.

**Why it matters.** The citation is inherited verbatim from `PLAN-M14…md:317`, where it was correct
at drafting time and should stay frozen — the plan is the contract, not a living document. The
issue is only that the *new* STATUS prose re-uses a line range that the very same commit moved, so
the archived record points a reader at the wrong paragraph of the file it is describing. Note the
**code got this right**: both producer servicers' `_callbackLog` field docs cite
"(`CallbackLog` remarks)" with no line numbers (`ProducerServiceImpl.cs:74-80`,
`AsyncProducerServiceImpl.cs:76-82`), which is why nothing in the source went stale. Suggest
`CallbackLog.cs:67-78`, or drop the numbers as the code does.

---

## Verified sound — recorded so it is not re-litigated

Grouped by the review axis; each was checked against the named reference, not against the commit
message.

**A · the placeholder-metadata decision and the self-review fix.** The `TopicPartition` throw is
real and reachable exactly as described: `TopicPartition.cs:47-61` throws
`ArgumentOutOfRangeException(nameof(partition), partition, "Partition must not be negative.")`, and
the failure-path placeholder is `new RecordMetadata(_topic, _partition, -1L, -1L)`
(`DeliveryRegistration.cs:128`) whose `_partition` is `record.Partition ?? -1` at all four Send
sites (`KafkaProducer.cs:164`, `MockProducer.cs:166`, `AsyncKafkaProducer.cs:175`,
`AsyncMockProducer.cs:174`). `DeliveryRegistration.Fire` wraps the whole invocation in one
`try`/`catch` → `TraceSwallowed` (`:125-144`), so the pre-fix code would indeed have produced a
**silently absent** entry on every producer-chosen-partition failure, invisible to the in-scope
test (which only sends successfully) — the severity claim is accurate.
The fix is **complete**: `LoggingDeliveryCallback.OnCompletion` is the only new site that builds a
`TopicPartition`, it is guarded by `metadata.Partition >= 0`, and `CallbackLogPartitionToProto`
(`Translate.cs:102-107`) is reached only through the guarded list. I looked for a second
silently-swallowed path in the new code and found none: the other candidate throw source is a null
`metadata.Topic` (protobuf's string setter and the `TopicPartition` ctor both reject null), and
that is closed upstream — `RecordMetadataMarshal.CopyOut:43` coalesces the marshalled topic with
`?? string.Empty`, and the placeholder path uses the record's validated topic. The three claimed
cases hold: success → resolved partition + offset (identical to Python); failure with a
producer-chosen partition → `partitions` empty / `offsets` empty, `error` only, which is
byte-identical on the wire to Python's `partitions=()` / `offsets=None`
(`grpc_translate.py:401-406`); failure with an explicit partition → that partition with offset
`-1`. The decision, its rationale and the "guard is load-bearing, do not delete" warning are all at
the site (`CallbackLog.cs:314-353`, `:384-389`).

**B · both arms report a real callback.** Neither servicer synthesizes an entry: the only
`.Append(` call sites in the whole assembly are the three logging-callback classes in
`CallbackLog.cs`, so no servicer can fabricate one from a resolved `Task`/`RecordMetadata`
(rejected Option A). `request.WithCallback` gates registration on both
(`ProducerServiceImpl.cs:189-191`, `AsyncProducerServiceImpl.cs:180-183`), mirroring
`grpc_server.py:135-139`. Both *"hint only … Nothing to do here"* comments are gone; the only
surviving occurrences are the two deliberate "this comment used to read …" notes. The two images
host different servicers (`Program.cs:71-89`), so `__grpc_dotnet` genuinely exercises the sync
`IProducer` path and `__grpc_dotnet_async` the `IAsyncProducer` one — D9 is really covered, not
nominally.

**C · entry encoding vs the wire contract.** `Translate.KindDelivery = "delivery"` exactly
(`Translate.cs:80`); the proto table is `producer_service.proto:214-222` and `"delivery"` is on
`:220`; `partitions` is the single `(topic, partition)`; `offsets` uses
`Translate.OffsetKey` = `$"{topic}-{partition}"` (`:89`), the same key the harness rebuilds
(`tests/common/callback_log.rs`); `error` is `exception?.Message` normalized to `string.Empty` by
`Append` (`CallbackLog.cs:131`), i.e. empty-string-not-absent, which is what
`delivery.error.is_empty()` (`producer_test.rs:951-956`) requires. Same `exception?.Message`
convention as the already-green `LoggingCommitCallback`. `ProducerCallbackLogRequest` (not the
consumer's `CallbackLogRequest`) is the right request type on both overrides.

**D · `GetCallbackLog` + log ownership.** Both overrides are
`Task.FromResult(_callbackLog.Response(request.ProducerId))`
(`ProducerServiceImpl.cs:337`, `AsyncProducerServiceImpl.cs:349`), mirroring
`ConsumerServiceImpl.cs:660-661` including its `<summary>`/`<remarks>`/`<inheritdoc/>` shape — an
established pattern, not a new one. No lock is taken and there is none to take (the producer
servicers have no per-id gate; ffi §A1). "Entries survive `Close`" is proved statically by
**absence of a removal site**: `_entries` is touched only by the two `TryGetValue`s and the one
insert (`CallbackLog.cs:155-158`, `:180`) — no `Remove`, no `Clear`, no eviction — while
`Close` `TryRemove`s only `_producers` (`ProducerServiceImpl.cs:282`) and the shutdown sweep
`Dispose` iterates only `_producers` (`:95-115`); both servicers are DI singletons
(`Program.cs:71-77` + `MapGrpcService`), so the log is process-lifetime. An unknown or closed id
yields an empty response, matching the consumer's load-bearing D3 and `grpc_server.py`'s
`GetCallbackLog`.

**E · thread-safety.** No second lock was added; `LoggingDeliveryCallback` holds only two
`readonly` fields, builds both collections before calling `Append`, and `Append` builds the whole
entry outside `_gate` and holds it only for the `list.Add` — the established pattern. Nothing in
the body can throw on the pump/caller thread once the sentinel guard is in place (see A).

**F · the `CallbackLog` reframe and its one deviation.** The rename is behaviour-neutral (arity,
types, order unchanged; type `internal`; all call sites positional and compiler-enforced — see
finding 1 for the count only). The consumer path is untouched: `LoggingRebalanceListener` and
`LoggingCommitCallback` keep their `_consumerId` fields (correct — those *are* consumer ids), and
`git diff c5d19d9e..HEAD` over both consumer servicers is empty. The reframed class doc asserts
nothing false about either service: I checked the id-collision claim (each servicer has its own
instance and its own `Interlocked.Increment` counter — `ProducerServiceImpl.cs:141`,
`AsyncProducerServiceImpl.cs:150`, `ConsumerServiceImpl.cs:143`,
`AsyncConsumerServiceImpl.cs:189`), the two new thread claims (pump thread before `TrySetResult` —
`SendCompletionPump.cs:396-397`; inline before the sync return — `NativeProducer.cs:645`), and the
`Close`-ordering claim ("the producer servicers before closing at all" — `ProducerServiceImpl.cs:282`
`TryRemove` precedes `producer.Close()`).
One wording note, **not filed** because it is inexact rather than false and its conclusion is
sound: the ⚠ paragraph at `CallbackLog.cs:51-56` (echoed at `:360-364` and
`ProducerServiceImpl.cs:78-80`) justifies the lock by "one delivery-callback instance can be
entered on several caller threads at once". Both servicers allocate a **fresh**
`LoggingDeliveryCallback` per `Send`, so that shape does not arise in this harness; what the lock
actually covers here is several *distinct* instances appending to one shared log, plus a concurrent
`GetCallbackLog` — which the preceding paragraph (`:40-48`) already states correctly and
completely.

**G · consumer no-regression.** No consumer callback family edited (§6); the four sibling callback
arms are green in my own run (below).

**H · the carried ffi §A6 item.** The repair is a strict narrowing of the prohibition's *closing
sentence* only (`ffi-marshalling.md:775-782`), adding the carve-out "**and this rule's own walk
narrative above**" plus its reason; the two TRUE sentences survive verbatim and unedited at `:712`
("two distinct sites…") and `:727` ("one wholesale-fault site…"). The prohibition is still
meaningful and still grep-verifiable. I re-ran P1's terminating-condition probes over the whole
binding (uniqueness quantifiers `the only`/`the one`/`sole`/`every other`, definite-article
exclusivity, and numeric counts, intersected with `residual`/`no-callback`/`throw source`/
`at-most-once`): the only hits are inside `IDeliveryCallback`'s own remarks — the canonical
enumeration — at `:189-190` and `:250`, the prohibition's own text at `:772`, and
`NativeConsumer.cs:2791`, which is the consumer's M9/P4 deferred-destroy residual (a different
family, correctly identified as out of scope in `182c2447`). `IDeliveryCallback.cs` is byte-unchanged
by P2, and **P2 introduced no new prohibited quantifier in its own prose**.

**I · plan §9 item 12 (not done) — justification holds.** Every claim checks out:
`Confluent.Kafka.sln` contains exactly two projects (`Confluent.Kafka`,
`Confluent.Kafka.UnitTests`) and not `grpc-server`; `Confluent.Kafka.UnitTests.csproj:17` is
`net462;net8.0;net10.0` and references only the library; `Confluent.Kafka.GrpcServer.csproj` is
`Sdk="Microsoft.NET.Sdk.Web"` / `net8.0`-only, with no `InternalsVisibleTo` anywhere; and
`LoggingDeliveryCallback` / `Translate.KindDelivery` / `Translate.OffsetKey` are all `internal` to
it. I also checked for the cheap route the brief asks about: there is **no** third `.csproj` under
`bindings/dotnet/` (only the three above), so no existing grpc-server-side test project exists to
host such a test, and grpc-server carries no test-framework package — every route touches a
`.csproj`, which §6 puts out of scope. Raising it as a maintainer decision is the right call, and
the entry shape is in fact asserted end-to-end by the two conformance arms, which read all four
fields.

**J · Mode A and scope.** `git diff c5d19d9e..HEAD` over `src/`, `src/ffi/`, `cbindgen.toml`,
`target/include/confluent_kafka.h`, `tests/`, `generator/`, `multilanguage-test-server/`,
`Makefile`, `.semaphore/` is **empty**; the changed set is 7 files (5 of them this phase's, plus my
own `dotnet-critic.md` from `cca71897`). `internal static extern` in `NativeMethods.cs` = **218**,
unchanged. `Producer_send_async` appears only inside generated `Confluent.Kafka.xml` doc output
(from a source comment) — never as a `[DllImport]`. Header sha256 =
`7d8ad0afd4d1af2e108373a22f5ec76af8d970dcbe84191c473de4a830e3ccd7`, byte-identical before and
after both cargo profiles. No `.proto` / `.csproj` / Dockerfile / Makefile / `.semaphore` change.
ffi §A6 form C respected: no `GCHandle`, no rooted delegate, no `UnmanagedFunctionPointer`, nothing
crossing the ABI — correct by design for this callback family, not an omission.

**K · the STATUS entry.** Factually accurate apart from findings 1 and 2. Spot-checked: the
placeholder-decision bullet was correctly *replaced* by `a7033979` (the superseded "carried
through, not suppressed" text is gone, not left alongside); the arm counts are right (my run shows
exactly 28 consumer `__grpc_dotnet[_async]` arms and 38 producer arms = 66); the header hash,
`extern` count and `cargo test --lib` 3693/0/3 all reproduce; and the "semantic merge conflict"
root cause holds under the landing-date reading — `7eb83969` has committer date **2026-08-20**
(author date 2026-08-06) while M12/P1's servicers are `dccda630` 2026-08-17 / `ea1f2d74`
2026-08-19.

---

## Independent gate (observed here, darwin/arm64, Docker 29.6.2)

Conformance leg — images **rebuilt from HEAD** before the run
(`DOCKER_DEFAULT_PLATFORM=linux/amd64 make -C bindings/dotnet grpc-image grpc-image-async`; every
layer including `COPY bindings/dotnet/grpc-server` and `RUN dotnet publish` reported `CACHED`,
which is itself the proof that the baked sources hash-match the current tree), then
`DOCKER_DEFAULT_PLATFORM` unset for the run:

```
test producer_test::test_delivery_callback_logs_metadata__grpc_dotnet ... ok
test producer_test::test_delivery_callback_logs_metadata__grpc_dotnet_async ... ok
test multilanguage_consumer_test::test_ml_commit_async_callback_logs_offsets__grpc_dotnet ... ok
test multilanguage_consumer_test::test_ml_commit_async_callback_logs_offsets__grpc_dotnet_async ... ok
test multilanguage_consumer_test::test_ml_rebalance_listener_logs_assigned_and_revoked__grpc_dotnet ... ok
test multilanguage_consumer_test::test_ml_rebalance_listener_logs_assigned_and_revoked__grpc_dotnet_async ... ok

test result: FAILED. 65 passed; 1 failed; 0 ignored; finished in 144.07s
```

The single failure is **not** this phase's: `test_ml_assign_and_consume__grpc_dotnet_async`
panicked at `multilanguage_consumer_test.rs:161` with
`close: IllegalState("dotnet_async gRPC backend transport error (Cancelled): Timeout expired")` —
a consumer arm, on a path P2 does not touch (`git diff` over both consumer servicers is empty). It
**passes in isolation on re-run** (`1 passed; 0 failed` in 4.46 s), i.e. the known intermittent
full-leg flake (plan §10 risk 2). Both target arms and all four sibling callback arms are green.

Standing gates, all clean:

| Gate | Result |
|---|---|
| `cargo build --features ffi` (debug **and** release) | exit 0, header hash unchanged |
| `dotnet build -c Release --no-incremental` (solution) | **0 Warning(s), 0 Error(s)**, all 6 TFM outputs |
| `dotnet build -c Debug --no-incremental` (`grpc-server`) | **0W/0E** — proves the `override GetCallbackLog` binds to a real base member, i.e. codegen now emits 8 RPCs (plan §10 risk 1) |
| `dotnet test -c Release -f net10.0` | **818 passed / 0 failed / 0 skipped** |
| `dotnet test -c Release -f net8.0` | **818 passed / 0 failed / 0 skipped** |
| `dotnet format --verify-no-changes` (solution) | exit 0 |
| `dotnet format --verify-no-changes` (`grpc-server`) | exit 0 |
| `cargo xtask format-check` (repo root) | ✅ clean |
| `cargo xtask lint` | ✅ doc-comment hygiene + clippy clean |
| `cargo test --lib` | **3693 passed / 0 failed / 3 ignored** |
| `TODO`/`FIXME` in the changed files | none |
