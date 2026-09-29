---
name: workflow-teeth-check-mtime
description: Teeth-check traps — touch a mv-restored source or cargo runs the stale binary; under deny(warnings) an unused-import mutation fails to compile with no error[E] code
metadata:
  type: feedback
---

When verifying a test has "teeth" by temporarily corrupting a source arm
(`cp backup && perl -i corrupt && cargo test && mv backup back`), the restored
file's mtime is OLDER than the artifact cargo just compiled from the corrupted
version. Cargo's fingerprint sees no change and re-runs the STALE corrupted
binary, so the next `cargo test` shows a spurious failure on correct source.

**Why:** cargo change-detection keys on mtime; `mv` of a `cp`-made backup gives
the restored file an mtime earlier than the last compile.

**How to apply:** after restoring, run `touch <file>` (or edit it via the Edit
tool, which bumps mtime) before re-running `cargo test`. Better: do the
teeth-check with the Edit tool (corrupt → test → Edit-revert) instead of shell
`cp`/`mv`, so mtime is always fresh. Confirmed while resolving the M11 Tier 3
Phase 1 ACL exhaustive-test fix — one wasted cycle chasing a stale-binary
"failure" that the source had already fixed.

## The Python twin of the same trap (found in M11 bindings B6)

A pure column *swap* mutation keeps the source file the **same size**, and
Python's `.pyc` validity check is `(mtime, size)` at **second** resolution. A
rewrite landing in the same second as the cached pyc's recorded mtime is treated
as unchanged, so the mutation silently does nothing and the teeth run reports
"all passed". `shutil.copy` does not preserve mtime and still is not enough.
**Clear `__pycache__` before every run** in any Python teeth harness. See
[[m11_bindings_b6_notes]].

## `deny(warnings)` turns a mutation into a silent compile failure (M15 fix cycle)

`src/lib.rs:16` is `#![deny(warnings)]`. A mutation that stops using an import
(e.g. swapping a `BTreeSet` for a `BTreeMap`) fails to COMPILE with
`error: unused import` — a lint error with **no `error[E…]` code**. A harness that
detects compile failure by grepping `error[` reports "compiled, test not red",
which reads as a toothless test. **How to apply:** detect `could not compile`,
keep every import used inside the mutation (`let _: BTreeSet<&str> = BTreeSet::new();`),
and print the panic line of each red run to prove it hit the intended assertion.
See [[m15-password-redaction]].
