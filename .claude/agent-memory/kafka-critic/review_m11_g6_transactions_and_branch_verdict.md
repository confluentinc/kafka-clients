---
name: review-m11-g6-transactions-and-branch-verdict
description: M11 G6 (producers/transactions) review — tautology re-introduced after removal, single-node fixtures can't discriminate a dropped present-flag, plan-vs-commit-message drift, and how to verify the branch's headline numbers cheaply
metadata:
  type: project
---

Round 17 reviewed the final slice of `dev/admin-multilanguage` (46/46 admin RPCs
over four gRPC backends) and issued the branch merge verdict. Patterns worth
carrying, beyond what the code itself shows.

## Review patterns that paid off

- **A "redundant-looking" assertion must be checked by expanding its
  definition.** `producer.is_valid()` sitting two lines under
  `assert!(producer.producer_id >= 0)` looks like belt-and-braces; `is_valid()` is
  `NO_PRODUCER_ID < producer_id` i.e. `producer_id > -1`, so it is the *same*
  predicate. This was the round-15 tautology class re-introduced two commits after
  being removed, and named in the ledger as a strengthening. Grep the accessor's
  body, never assume.
- **On a single-node fixture, "unset routes to the leader" and "set to the only
  node" are the same route.** So a scenario that sets an optional routing field
  and asserts the two answers agree catches a *substituted default* but not a
  *dropped field*. Any "fully observable" claim about a present-flag on a
  one-broker cluster deserves this check; ask which of (a) dropped and (b)
  defaulted is caught, separately.
- **Derived test inputs can silently collapse.** `id.split('_').next()` on an id
  built from a sanitized *thread name* yields the first path component
  (`"admin"`), not the unique prefix — the pattern anchored "on this id's own
  prefix" matched every id the whole suite creates. When a test derives a filter
  from a generated identifier, print/derive the actual value.
- **`check-generated` exits 1 both when a generated file is misformatted and when
  `rustfmt` is simply absent** (same `Error: No such file or directory`). The
  pinned toolchain declares `clippy`/`rustfmt` and ships neither; the nix store
  has `rustfmt-1.97.1`, so `PATH=/nix/store/*-rustfmt-1.97.1/bin:$PATH cargo xtask
  check-generated` is how to settle it. It passes cleanly (199 files) — the caveat
  five slices carried was genuinely stale.
- **Cheap reproductions for headline counts** (no Docker needed): `cargo test
  --features integration-tests --test integration -- --list` and the same with
  `,multilanguage-tests`. Both compile and list without containers, so
  "N `__rust` / M `__grpc`" claims are always verifiable. `cargo test --lib` runs
  in ~5 s.
- **`git diff <branch-base>..HEAD -- src/` is the single highest-value one-liner**
  for a harness-only branch; pair it with `-- bindings/ ':!bindings/*/grpc_server*'`
  to separate shipped-binding changes from harness servers.
- **An AST pass beats grep for "is the return inside the try".** Parsing the
  servicer class and comparing `Return` nodes reachable from `Try.body` gave an
  exact 46-inside / 0-outside count and correctly excluded encoder-less handlers
  (`CreateAdmin`, `Close`) and helpers.

## Documentation-as-record failure modes specific to a long branch

- **The plan and the later commit messages drift.** §5.8 of the plan (written in
  commit N) still disclosed `check-generated` as failing while commit N+1 said it
  passes. When a plan section is the PR description's source, re-grep it against
  the *last* commit's claims.
- **An errata table's own arithmetic.** §5.9 said "five entries were wrong" over a
  six-row table, and omitted a seventh ledger defect that had been fixed in code
  but not in the (immutable) commit message — which is exactly what an errata is
  for. Count the rows.
- **An "unreachable" row with an empty citation column is not a record**, even in
  a table whose preamble demands citations. The one such row deferred to another
  RPC's correction (`describeLogDirs` fan-out → the reassignment 3-broker
  correction), which does not discharge it: all four log-dirs scenarios run
  single-broker.
- **A defect ledger for the PR must include the already-`DEFERRED` items.** §5.6
  listed DEFERRED 3 but not DEFERRED 1 (lookup-stage busy spin, a self-inflicted
  broker DoS, re-measured at G6 on a fourth trigger) or DEFERRED 2
  (`enable.idempotence` defaults true, unimplemented — the reason G6 needs
  `docker exec kafka-console-producer.sh` at all). A new "production observation"
  that duplicates an existing DEFERRED entry *understates* it.

## The oracle error-decoding change (worth remembering as a template)

`prefer_transported_code` in `tests/common/multilanguage_producer.rs` overrides a
guessed variant with the transported broker code iff the guess is one of the five
`KafkaError` cases with no `Errors` slot AND the code is real. It is sound because
the two cases cannot overlap: those five report `Errors::UnknownServerError`, so
the C boundary transports `-1` for them by construction. Auditing it meant
(1) proving the "-1 always" premise at `src/common/kafka_error.rs` +
`src/ffi/common.rs`, (2) checking every manufactured server-side error stamps
`code=-1`, (3) enumerating every scenario matching on one of the five variants.
Worth repeating verbatim if that function changes again.
