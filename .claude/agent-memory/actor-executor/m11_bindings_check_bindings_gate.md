---
name: m11-bindings-check-bindings-gate
description: cargo xtask check-bindings — the CPython format-arity gate, what it does and does not catch, and the two workspace gaps it exposed
metadata:
  type: project
---

`cargo xtask check-bindings` (added after Critic round 8) statically checks
every `Py_BuildValue` and `PyArg_ParseTuple` / `PyArg_ParseTupleAndKeywords`
call in `bindings/python/_confluentkafka.c` for format-unit-versus-argument
arity. Wired into `make verify` and `make verify-sandbox`.

**Why it exists:** these are variadic; a format one unit short compiles with no
diagnostic and reads a garbage pointer. B4 shipped exactly that
(`consumer_group_description_to_py`, 10 units / 11 args) and **no test could
have caught it** — for every admin RPC Java's `MockAdminClient` leaves
unsupported, the success path of the matching `*_drain` is dead code in the
suite. Six of B4's nine RPCs, and all five of B5a's, are in that position.

## What it does and does not catch

  - **Catches:** arity, on both the build and the parse side. Also reports a
    **non-literal format** as a failure rather than skipping it — an
    unverifiable site is a hole in the gate, not a pass. (This fired on B5a's
    own `PyArg_ParseTuple(item, nullable ? "izizzii" : "isissii", ...)`; the
    fix is two literal-format calls.)
  - **Does not catch:** argument *order*. A transposed pair of same-typed
    fields is statically undetectable here and still needs a test or review.
    Say so when citing the gate as coverage.

## Proving it works

Run it against `761da3b2^` — `git show 761da3b2^:bindings/python/_confluentkafka.c
> scratch/prefix.c && cargo xtask check-bindings scratch/prefix.c`. It reports
exactly that revision's one defect (`prefix.c:4140 fmt='(sONsssNNNN)'`) and
nothing else, and exits 1. The optional path argument exists for precisely this
— pointing the checker at an older revision without mutating the tree.

Baseline counts at the end of B5a: **46 `Py_BuildValue` and 160
`PyArg_Parse*` sites, 0 mismatches.** (42 / 144 before B5a.) A count that drops
unexpectedly means an insertion landed somewhere it should not have.

## Two workspace gaps it exposed

`cargo test` and `cargo clippy` at the workspace root only cover the **root
package**, so the whole `xtask` crate — the build tooling itself — was
unexercised and unlinted.

  - `cargo xtask lint` / `lint-fix` now include `-p xtask`.
  - The `check-bindings` Make target runs `cargo test -p xtask` first, since
    nothing else runs the scanner's 21 unit tests and a gate is only worth as
    much as the parser behind it.

**How to apply:** when adding anything to `xtask`, `generator`, or any other
workspace member, remember plain `cargo test` does not test it. This is a
**repo-wide** gap, not an xtask one — every non-root workspace member
(`generator`, `consumer-perf`, `multilanguage-test-server`) is in the same
position, and `cargo xtask lint` only reaches `generator` because it names that
manifest explicitly. Raised to the Manager after B5a.
