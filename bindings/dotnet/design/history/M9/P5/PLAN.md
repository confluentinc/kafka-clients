# M9/P5 — multilanguage-harness compile fix (`create_with_callback_log`)

**Status: DONE (2026-08-29). N=58.**

---

## ⚠ Why this file is a stub

**No standalone plan was ever written for M9/P5, and none is missing.** This phase
was scoped inline as **§5.1 of the milestone roadmap**,
`bindings/dotnet/design/current/PLAN-M9-consumer-callback-parity.md`, because it was
a two-method compile fix that the roadmap's gap analysis had already specified in
full — including the exact method bodies. Writing a separate `PLAN.md` would have
duplicated §5.1 rather than adding anything.

This stub exists so the directory is not misread as having lost a document. **The
authoritative plan of record for this phase is roadmap §5.1**; read it there.

Every other archived phase in this milestone (P6, P7, P8, P9) carries a real
`PLAN.md`, because each was dispatched from its own Actor brief.

---

## Scope as executed

Two `ConsumerBackendFactory` trait-method implementations in
`tests/common/backend_factory.rs`, each delegating to the existing private
`consumer_with_log` helper:

- `DotnetGrpcFactory::create_with_callback_log` → `consumer_with_log(…, "dotnet")`
- `DotnetAsyncGrpcFactory::create_with_callback_log` → `consumer_with_log(…, "dotnet_async")`

Plus a doc-comment refresh on both factory types (added during the review cycle —
see Findings).

**Why it was needed.** Master's PR #143 (`7161aae9`, "ffi-callback-bridging") added
`create_with_callback_log` as a **required** trait method with no default body and
implemented it for the Python and C factories. This branch had independently added
the two .NET factories. Git merged cleanly at `a7efb0d5` — no textual overlap — but
the .NET impls never picked up the new requirement: a **semantic merge conflict**,
producing two `error[E0046]`.

**What it broke, precisely.** `make verify-rust` on **both** CI jobs (Linux amd64
and macOS arm64) → `cargo build/test --all-features`, and `--all-features` turns on
`multilanguage-tests`, which compiles `backend_factory.rs`. `--skip __grpc` skips
*running*, not *compiling*. There is no `verify-dotnet` job and no
`test-integration-dotnet` target on this branch, so this was a **compile-only**
break in the **Rust** verify jobs — not a .NET test failure.

---

## Outcome

| | |
|---|---|
| **Commits** | `e72fc809` (the two impls, +14/−0) · `1a6c13f8` (`fixup!` — the dropped doc refresh, 14 lines, doc comments only) |
| **Findings** | **1 Minor**, closed. The plan's in-scope doc-comment refresh on `DotnetGrpcFactory` / `DotnetAsyncGrpcFactory` was silently dropped from the first commit. Every sentence of the existing prose stayed *literally* true, which is exactly why it became misleading — it read as "callback tests do not apply to .NET" at the moment the commit put .NET fully into the consumer callback matrix. |
| **Gates** | All four green, reproduced independently by the Critic. Autosquash pairing verified. |
| **Mode** | Rust harness glue under `tests/` — authored by the `dotnet-actor` under the standing **M12/P1 exception** for harness glue (`bindings/dotnet/CLAUDE.md` §8.1 otherwise scopes that persona to C#). |

**Archived review record:** `COMMENTS.DONE.58.md`, in this directory.

---

## What this phase set in motion

Its single finding — *a documented-scope item silently dropped* — turned out to be
the **first of three recurrences** of the same species. P6 then shipped code whose
contract docs contradicted it, and P7 the same. After P6 the Critic filed the **PLAN
template** as the defect rather than any Actor, and the fix became roadmap **§5.6**:
`ffi-marshalling.md` section-sync is now an *itemized, per-phase deliverable*, the
way `CLAUDE.md` doc-sync already was. P8 later added part **(c)** to that gate
(*does the shipped code satisfy the sentence you just wrote?*).

P9 — the first phase dispatched under the full §5.6 template — closed with **zero
findings**.
