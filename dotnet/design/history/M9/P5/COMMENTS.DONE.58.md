# COMMENTS.DONE.58 — M9/P5 (.NET consumer callback parity)

Resolved items from `COMMENTS.58.md` (Critic review of `e72fc809`).

---

## 1 · [Minor — decision hygiene] The plan's in-scope doc-comment refresh was dropped without a recorded deviation — **RESOLVED**

**Finding:** PLAN §5.1 listed a doc-comment refresh on `DotnetGrpcFactory`
(`tests/common/backend_factory.rs:403-412`) and its twin `DotnetAsyncGrpcFactory`
(`:449-459`) as in scope for M9/P5. Commit `e72fc809` shipped only the two method
bodies. Every sentence of the existing prose stayed literally true, which is why it
became misleading: it is the only prose on these two types and it reads as "callback
tests do not apply to .NET" at the exact moment the commit puts .NET fully into the
*consumer* callback matrix with four known-failing arms. Per `bindings/dotnet/CLAUDE.md`
§4 (no silent divergence), a reader could not tell "deliberately deferred" from
"missed".

**Option taken: (a) — add the clarifying prose now.** The alternative, (b) defer the
refresh to P9 with a recorded rationale, was defensible but leaves the misleading
paragraph live on the branch for four more phases. The cost of (a) is a few doc lines
with no code change; the cost of (b) is that every P6/P7/P8 reader passing through
this file gets the wrong impression in the meantime. The finding's own framing — that
this is "the one place a P6/P7 reader would look to learn that .NET is now on the
consumer-callback hook" — argues for putting the information there rather than in a
commit message that a reader of the file will never see.

**Fix:** added a paragraph to each doc comment.

- `DotnetGrpcFactory` (the primary): states that the producer exemption above says
  nothing about consumer callbacks; that as of M9/P5 the factory implements
  `create_with_callback_log`, so `multilanguage_consumer_test!` generates .NET arms
  for the rebalance-listener and commit-callback tests; that those arms fail at
  runtime today because the .NET gRPC server does not yet implement `CommitAsync` /
  `GetCallbackLog` (gRPC `UNIMPLEMENTED`); and that this is the bounded Q4(a) window,
  closing in M9/P9, with no CI job running them meanwhile.
- `DotnetAsyncGrpcFactory` (the twin): a shorter back-reference to the caveat on
  `DotnetGrpcFactory`, noting its own two arms and the same M9/P9 close, matching the
  existing "async twin of […] — identical wiring" style of that doc block.

Deliberately kept out of the prose: the `--all-targets` gate mapping and the
`Makefile` line numbers. Those are commit-message and plan material, not type
documentation, and they would go stale in a file that has no reason to track gate
definitions.

**Verification after the doc edit:** `cargo xtask format-check` clean;
`cargo xtask lint` clean (doc-comment hygiene pass **and** both clippy passes — the
relevant one here, since the edit adds intra-doc links); `cargo build --all-features
--all-targets` clean (confirms the added `[`DotnetGrpcFactory`]` link and the
backticked identifiers do not break the doc pass or compilation).

**Fixup commit:** `fixup! e72fc809` — see `git log`.

---

## Noted, no action taken by the Actor

The Critic's **Observation for the Manager** — that PLAN §5.1's DoD note is factually
stale about `cargo xtask lint` (it claims the integration binaries are unlinted;
untrue since `4fd1435f` / #139) — is being corrected by the PM in the plan document.
The Actor was explicitly scoped out of touching
`bindings/dotnet/design/current/PLAN-M9-consumer-callback-parity.md` in this round.

The Critic's suggested rule addition (record the gate map for harness-only changes,
so no future phase cites a bare `cargo build --all-features` as evidence) is likewise
a Manager/rules-file decision, not an Actor edit.
