---
name: SASL Authenticator translation patterns
description: Java try-catch scope vs Rust error propagation, integer wrapping in correlation ID management — both confirmed and fixed
type: project
---

Two bug patterns found and confirmed in SASL authenticator translation (both fixed in commit 1882a7f):

1. **Java try-catch scope mismatch**: Java `try { read(); parse(); } catch (SchemaException e) { setState(FAILED); }` wraps both read AND parse. Rust translation only catches read errors via `match`, letting parse errors propagate via `?` without state transitions. Fix: use `map_err` on the parse call to set state to Failed before returning the error.

**Why:** The Rust `?` operator makes it easy to forget that Java catch blocks often handle errors from multiple statements, not just one.
**How to apply:** When reviewing Rust translations of Java methods with try-catch, verify that ALL statements inside the try block have their errors handled consistently, not just the first one.

2. **Java wrapping integer arithmetic vs Rust overflow panic**: Java integer arithmetic wraps silently on overflow. Rust panics in debug. The `nextCorrelationId` method increments near `i32::MAX` and relies on wrapping to reset. Fix: use `wrapping_add` in Rust.

**Why:** This is a subtle semantic difference between Java and Rust that affects code near integer boundaries.
**How to apply:** Whenever Java code uses post-increment (`i++`, `++i`) or addition near known boundary values (`Integer.MAX_VALUE`, `Integer.MIN_VALUE`), verify the Rust translation uses `wrapping_add` or `wrapping_sub`.
