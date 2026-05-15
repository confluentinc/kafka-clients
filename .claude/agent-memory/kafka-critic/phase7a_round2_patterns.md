---
name: Phase-7a Round-2 patterns
description: Verified-good fix shapes for ProducerConfig DOC parity, validator rename safety, and reachable-subset test scoping
type: project
---

# Phase 7a Round 2 — verified-good fix shapes

Round 2 of Phase 7a closed cleanly: 0 Blocking, 0 Suggestion, 0 Nit. All
six review items (Suggestions 1-4 + Nits 2/3) were resolved per the
Round-1 dispositions. Patterns worth carrying forward:

## 1. Reachable-subset test scoping (Suggestion 1 fix shape)

When a Java test bundles multiple invariants (e.g. 2pc + idempotence +
transactional.id) and Milestone-1 rejects some of them upstream, the
correct fix is to:
- Identify the subset of the Java invariant reachable in this milestone.
- Translate a Rust-adapted test that exercises *only* that subset.
- Rewrite the skip-comment for the verbatim port to scope the deferral
  to the *unreachable subset only* — not the entire Java test.

Anti-pattern: leaving the verbatim test fully skipped because "Java
sets `enable.idempotence=true`". The Milestone-1-reachable invariant
(2pc + explicit timeout = mutual exclusion) is then untested even
though the production block exists.

This pattern echoes Phase-6e/Phase-6d skip-rationale-too-broad, but
Round-2 added a concrete fix-shape: the Rust-adapted test's asserted
error message must be **byte-exact** with the production format
string — `assert_eq!`, not `contains(...)` — so a future format-string
drift breaks the test.

## 2. Public DOC constants need byte-exact Java parity, file-private DOCs don't

Java's `public static final String *_DOC` constants are part of the
public API surface (IDE tooltips, HTML doc generator output). Any
deviation drifts the contract. CLAUDE.md Rule #4 applies.

Verified-good shape for milestone deviations: keep Java's verbatim
text intact, then append a `<p>` break and the deviation note.
**Do not interleave** the deviation note into Java's prose — it must
be additive at the end so the Java contract is intact for the parts
that still apply. Example from `ENABLE_IDEMPOTENCE_DOC`: Java text
through "...ConfigException is thrown.", then `<p>`, then the
Milestone-1 deviation paragraph pointing at PLAN.md.

File-private DOCs (`PARTITIONER_CLASS_DOC`, `BATCH_SIZE_DOC`, etc.)
can be shortened — they're not on the contract surface.

## 3. Apache-Kafka-source pinning (4.2 vs older versions)

The Java reference is whatever's bundled at `kafka/` in the repo —
**not** the latest upstream Apache Kafka. Older Kafka versions had
"Java 11 or newer" wording in `SSL_PROTOCOL_DOC` /
`SSL_ENABLED_PROTOCOLS_DOC` that was removed in 4.2. When verifying
DOC parity, always read the version checked into `kafka/`, not
upstream HEAD.

Verification technique: `grep -n SSL_PROTOCOL_DOC kafka/.../SslConfigs.java`
and read ±5 lines, then byte-compare against the Rust `concat!(...)`.
The diff revealed Round-1 had stale upstream text from a pre-4.2
Kafka version.

## 4. Validator rename safety checklist

When renaming a validator binding (`zero_or_more_send_buffer` →
`at_least_send_buffer_lower_bound`), verify three things:

1. **Name matches the actual bound.** `Range::at_least(-1)` is not
   "zero or more" — the false claim was the original review issue.
2. **All call sites updated.** `grep -n` for both old and new names
   and confirm no orphans.
3. **Inline comment documents the bound's semantics.** "Both lower
   bounds are `-1` (Kafka semantics: 'use OS default')" — without
   this, the reader has to follow the constant to discover the actual
   constraint.

The 4-line comment Actor added (mirroring Java's `atLeast(...)` call
sites) is a good template for future validator-name fixes.

## 5. Anchor-import cleanup verifies trim-down depth

When an `_unused_anchor: use foo as _foo_anchor;` block is removed,
the bare `use ... { foo, bar };` that the anchor was keeping alive
should also be trimmed if no other site references the bare module
name. Verification: after removing anchors, `grep -n "foo::"` should
either return only full-path references (`crate::common::config::foo::*`)
or none — bare `foo::*` references in the file's own module would have
broken under the trim.

For the Phase 7a Actor: confirmed by `grep -n "sasl_configs\|ssl_configs"`
showing only 4 full-path sites at lines 1156, 1427, 1431, 1439. Clean
removal.

## 6. New-defect scan: `*_DOC` rewrite must be doc-only

Regression vector: a "doc fix" PR accidentally edits a key's
*registration* (default, validator, importance) instead of just the
docstring. Quick check:

```
git diff <commit>~1 <commit> -- <file> | grep -E "^[+-]" | \
  grep -v "^[+-]{3}" | grep -vE "^[+-]\s*\"|^[+-]$"
```

If only `--- a/...` and `+++ b/...` lines come back (no actual
non-string-content edits), the rewrite is doc-only. Round 2's SSL
fix passed this filter cleanly.

## 7. Memory commit location verification

Memory commits should touch `.claude/agent-memory/<role>/` only —
**not** CLAUDE.md, **not** `.claude/rules/`. `git show <sha> --stat`
should show only files under `.claude/agent-memory/`.

The Actor's Round-1 memory commit (`fa089ef`) passed this check; this
is the second consecutive Phase where the Actor got the path right
out of the box (Phase 6e Round 2 was the same shape). Pattern is
established and reliable.
