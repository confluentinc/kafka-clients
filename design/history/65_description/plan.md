# PR #65: Migrate `verificationAllTransactionComplete` from EOS system test to smoke test

## AK Commit

`a162393861628ffc85c62dd94e218a872c770e95`

**Title:** MINOR: Migrate `verificationAllTransactionComplete` from EOS system test to smoke test (#20718)

**Author:** Jinhe Zhang

**Summary:** Moves `VerificationResult` inner class from `SmokeTestDriver` to `SmokeTestUtil`,
making it public and accessible to more test utilities. Refactors the monolithic `verify()` method
into three helper methods (`preVerifyTransactions`, `pollAndCollect`, `reportAndFinalize`), and
adds a new `verifyAllTransactionFinished()` method that verifies all Kafka Streams EOS transactions
have committed by comparing read-committed vs. raw end offsets on output topics.

## Changed Files

| File | Change |
|------|--------|
| `streams/src/test/java/org/apache/kafka/streams/tests/SmokeTestDriver.java` | Refactored `verify()`, moved `VerificationResult`, added `verifyAllTransactionFinished()`, `PollResult`, `preVerifyTransactions()`, `pollAndCollect()`, `reportAndFinalize()` |
| `streams/src/test/java/org/apache/kafka/streams/tests/SmokeTestUtil.java` | Added `VerificationResult` inner class (public) |

## Relevance to Rust Translation

**This commit is a no-op for the Rust translation project.**

### Reasons

1. **Kafka Streams scope**: Both changed files are in
   `streams/src/test/java/org/apache/kafka/streams/tests/`. They belong to the Apache Kafka
   Streams module, which is a stream-processing framework built on top of the Kafka client. The
   Rust project translates only the Kafka **client** library (`clients/`, `common/`). Kafka Streams
   itself is out of scope.

2. **Test-only code**: Both files live under `src/test/`, making them test utilities only. Even
   if Kafka Streams were in scope, these classes would only be needed for integration/smoke tests,
   not for the production library.

3. **No client-layer API changes**: The commit does not modify any class reachable from the Kafka
   client public API (`KafkaProducer`, `KafkaConsumer`, `AdminClient`, network stack, protocol
   layer, etc.). The only consumer-facing types used internally (`IsolationLevel`,
   `ByteArrayDeserializer`, `KafkaConsumer`) are already translated or planned.

4. **Pure refactoring within Streams test code**: The change moves and restructures test helper
   logic. There is nothing new to translate for the client.

## Decision

No changes are required in the Rust repository for this commit.
