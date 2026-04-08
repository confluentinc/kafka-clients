
## Issue 10 (Round 4): FALSE POSITIVE — testDefaultValues, testNonIgnorableFieldWithDefaultNull, testWriteNullForNonNullableFieldRaisesException

These three tests were already documented as not translatable in COMMENTS.DONE.0.md Round 2, Issue 2:

- **testDefaultValues / testNonIgnorableFieldWithDefaultNull**: Require per-field version validation (UVE for non-default values at unsupported versions) which the Rust generator validates at entry level instead. The Java generator produces "Attempted to write a non-default X at version Y" errors, but our generator silently ignores out-of-range fields. This is a generator-level feature gap, not a missing test.

- **testWriteNullForNonNullableFieldRaisesException**: Tests that setting a non-nullable field to null raises NullPointerException in Java. In Rust, non-nullable fields use `Vec<T>` (not `Option<Vec<T>>`), so the type system prevents null/None at compile time. No runtime test is needed or possible.

The Critic re-raised this as Issue 10 without checking COMMENTS.DONE.0.md for prior resolution.
