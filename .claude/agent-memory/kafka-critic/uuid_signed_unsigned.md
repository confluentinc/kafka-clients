---
name: Uuid signed vs unsigned comparison mismatch
description: Java Uuid.compareTo uses signed long comparison but Rust Uuid::cmp uses unsigned u64 comparison — ordering differs for ~50% of random UUIDs
type: project
---

Java's Uuid stores bits as `long` (signed) and `compareTo` uses signed comparison operators.
Rust's Uuid stores bits as `u64` (unsigned) and derives/implements `Ord` with unsigned comparison.

This produces different ordering for any UUID pair where the high bit is set in the most-significant or least-significant 64 bits.

**Why:** The translation converted Java `long` to Rust `u64` without considering that comparison semantics differ. The existing Java test (testCompareUuids) only uses small positive values that don't expose the bug.

**How to apply:** When reviewing Uuid or any type that stores Java `long` values and has comparison semantics, verify whether the comparison should be signed or unsigned. For wire protocol fields that represent signed Java longs, the Rust type should use `i64` or the comparison should cast to `i64`.
