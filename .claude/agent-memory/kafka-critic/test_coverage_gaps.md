---
name: Test coverage gap patterns in Java-to-Rust translation
description: Common patterns where Java tests are not fully translated — dedicated test files, error message assertions, iteration tests, utility tests, byte-level protocol verification, and version validation weakening
type: feedback
---

Common test coverage gaps found in the translation:

1. **Dedicated test files completely missing**: Java has separate test files per generated message type (SimpleExampleMessageTest, NullableStructMessageTest, SimpleArraysMessageTest, ApiMessageTypeTest, RecordsSerdeTest). These are NOT part of MessageTest.java and are easily overlooked. The Actor tends to translate MessageTest.java but misses these dedicated test files.

2. **Error message assertions omitted**: Java tests often assert specific error message strings. Rust tests tend to only check `is_err()` without verifying the message content. The error messages should be verified since they are part of the behavioral contract.

3. **Repeated/iteration tests dropped**: Java `@RepeatedTest` annotations (like testRandomUuid with 100 iterations) are not translated. These should become loops in Rust tests.

4. **Utility class tests under-translated**: MessageUtilTest has 6 Java tests but only 3 Rust tests (remaining depend on unimplemented functions).

5. **Test values differ from Java originals**: Some Rust tests verify similar behavior but use different input values than the Java originals. Per DoD rule 3, tests should be direct translations where possible.

6. **Round-trip-only tests miss wire incompatibility**: Varint/UUID tests only verify encode-then-decode round-trips, not specific byte representations. A self-consistently wrong encoding passes round-trip tests but is wire-incompatible with Java. Always verify byte-level encoding against Java test vectors.

7. **Test helper behavioral differences**: verify_write_raises_uve in Rust unwraps size() but Java's assertThrows wraps both size() and write(). This masks errors that occur during sizing.

8. **Exhaustive tests weakened to spot-checks**: test_message_versions only checks 5 of 70+ API keys instead of all of them (Java tests all). Missing ApiMessageType is acknowledged but treated as acceptable rather than a gap.

9. **Trivially-passing type-mismatch tests**: When Java tests check that sensitive data (e.g., byte arrays) is not present in toString() output, the Rust translation may assert the absence of the ASCII string. But if the Rust type is Vec<u8> (rendered as numeric Debug), the assertion passes trivially whether or not the redaction code exists. Must assert the positive condition (empty bytes present) not just the negative (ASCII absent).

10. **RequestResponseTest.java tests missed**: RequestResponseTest.java contains per-API-key tests (corrupt parse, tagged fields, error response construction) that are separate from dedicated test classes. These are easily missed because they are bundled in one large file covering all request types.

**Why:** The Actor tends to write tests that verify the Rust code works rather than faithfully translating Java tests. This can miss edge cases that the Java tests were specifically designed to catch. Pattern #1 is the most common — entire test files are missed because they aren't in the obvious MessageTest.java.

**How to apply:** When reviewing test coverage, always compare test method names AND test values/assertions against the Java originals. Check for missing negative tests (error cases), verify error message content matches, and critically examine round-trip-only tests for wire protocol types — they need byte-level verification too. Also check for dedicated per-message-type test files beyond MessageTest.java.
