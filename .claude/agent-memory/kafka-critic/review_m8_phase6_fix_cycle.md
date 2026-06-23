---
name: review-m8-phase6-fix-cycle
description: Re-review patterns from Phase 6 fix cycle — KafkaError variants vs Errors-based dispatch, format-pass commits, inline-mod-tests private field access
metadata:
  type: feedback
---

When a one-line change touches a centralized predicate like
`KafkaError::is_retriable()`, audit all call sites for the type of the
receiver — not just textual matches. In Phase 6 there were 8+ textual
matches but most were on `Errors::is_retriable()` (a different impl) or
on `ConsumerError::is_retriable()`, and only ONE production call site
in the consumer + the FFI re-export touched `KafkaError::is_retriable()`.

**Why:** Side-effect audits that count textual matches without
disambiguating the receiver type miss the actual risk surface — and
inflate false positives. Conversely, a real risk in a sibling type
(e.g. `Errors::is_retriable()`) won't surface from grepping the changed
method name.

**How to apply:** For any predicate change, list call sites by receiver
type before reasoning about regressions. `PartitionResponse.error: Errors`
calls `Errors::is_retriable()`, not `KafkaError::is_retriable()` —
distinct impls, distinct behaviors.

**Bonus pattern observed:** Rust's inline `mod tests { ... }` in the
same file as a `pub(crate)` struct can read private fields directly. No
visibility relaxation is required when the test lives next to the
struct. Useful when translating tests that depended on private-state
assertions in Java (where Java tests typically use package-private
access via the test's same package).

**Format-pass commits** following a tests-only commit are expected; a
`git show --stat` showing a small line count and no symbol diff confirms
they're pure rustfmt. Don't waste review cycles on these.
