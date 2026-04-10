# Review Comments for Commit aa0d7b0 - Layer 2: Message and ApiMessage traits

## Issue 1: MessageTest.java tests not translated (DoD #3 and #4 violation)

**File**: `kafka/clients/src/test/java/org/apache/kafka/common/message/MessageTest.java`

Java `MessageTest.java` contains 22 test methods that exercise the `Message` trait through generated message types (round-trip serialization, duplication, version handling, unknown tagged fields, default values, null handling). None of these tests are translated.

Per DoD rule #3: "Are all tests using those classes translated?" — the `Message` trait is the class under test in `MessageTest.java`, and none of its 22 tests are present.

Per DoD rule #4: "Are there blockers for doing that? In case implement the needed classes as well." — the blocker is that generated message types don't implement the `Message` trait yet (no `read()`, `write()`, `size()`, `addSize()` on generated structs). Per rule #4, this blocker should have been resolved as part of this commit rather than deferred.

The tests require:
1. Generated message structs implementing the `Message` trait (`read`, `write`, `size`, `addSize`)
2. `PartialEq`, `Hash`, `Display` on generated types
3. `duplicate()` working on generated types
4. `MessageUtil::toByteBufferAccessor` helper
5. JSON round-trip converters (for `testJsonRoundTrip`)

**Fix**: The `SchemaGenerator` needs to generate `Message` trait implementations for the 197 generated message structs, and the `MessageTest.java` tests need to be translated and passing.