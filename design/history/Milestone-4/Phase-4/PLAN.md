# Phase 4: MockProducer Tests

## Goal

Translate 8 tests from `MockProducerTest.java` (the non-transactional, non-callback,
non-partitioner, non-serializer subset).

## Java Source

`org.apache.kafka.clients.producer.MockProducerTest` (751 lines, 54 tests total)

## Test File

`tests/clients/producer/mock_producer_test.rs` with entry point `tests/clients/producer/main.rs`

## Tests to Translate

### 1. `test_auto_complete_mock` (Java: `testAutoCompleteMock`, line 73)

```java
// Java: sends 2 records with autoComplete=true, verifies futures resolve immediately
MockProducer<byte[], byte[]> producer = new MockProducer<>(
    cluster, true, new RoundRobinPartitioner(), byteArraySerializer, byteArraySerializer);
Future<RecordMetadata> future1 = producer.send(record1);
assertFalse(future1.isDone());  // NOTE: Java impl IS done for autoComplete
// Actually in Java autoComplete makes it done immediately
assertTrue(future1.isDone());
assertEquals(0L, future1.get().offset());
```

Rust: Create MockProducer with auto_complete=true. Send two records. Verify futures
resolve immediately with sequential offsets (0, 1). Verify `history()` has both records.

### 2. `test_manual_completion` (Java: `testManualCompletion`, line 107)

```java
// Java: sends 2 records with autoComplete=false, manually completes one success + one error
MockProducer<byte[], byte[]> producer = new MockProducer<>(..., false, ...);
Future<RecordMetadata> future1 = producer.send(record1);
assertFalse(future1.isDone());
producer.completeNext();
assertTrue(future1.isDone());

Future<RecordMetadata> future2 = producer.send(record2);
producer.errorNext(new RuntimeException());
try { future2.get(); fail(); } catch (ExecutionException e) { ... }
```

Rust: Create MockProducer with auto_complete=false. Send record, verify future is not done.
Call complete_next(), verify future resolves with Ok. Send second record, call
error_next(error), verify future resolves with Err.

### 3. `test_should_be_flushed_if_no_buffered_records` (Java: `shouldBeFlushedIfNoBufferedRecords`, line 696)

```java
MockProducer<byte[], byte[]> producer = new MockProducer<>(cluster, true, ...);
assertTrue(producer.flushed());
```

Rust: Create MockProducer with auto_complete=true. Verify `flushed()` is true before
any sends.

### 4. `test_should_be_flushed_with_auto_complete_if_buffered_records` (Java: `shouldBeFlushedWithAutoCompleteIfBufferedRecords`, line 702)

```java
MockProducer<byte[], byte[]> producer = new MockProducer<>(cluster, true, ...);
producer.send(new ProducerRecord<>("topic", "key", "value"));
assertTrue(producer.flushed());
```

Rust: Create MockProducer with auto_complete=true. Send a record. Verify `flushed()` is
still true (auto-complete resolves immediately, no pending completions).

### 5. `test_should_not_be_flushed_with_no_auto_complete_if_buffered_records` (Java: `shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords`, line 709)

```java
MockProducer<byte[], byte[]> producer = new MockProducer<>(cluster, false, ...);
producer.send(new ProducerRecord<>("topic", "key", "value"));
assertFalse(producer.flushed());
```

Rust: Create MockProducer with auto_complete=false. Send a record. Verify `flushed()` is
false (there is a pending completion).

### 6. `test_should_not_be_flushed_after_flush` (Java: `shouldNotBeFlushedAfterFlush`, line 716)

```java
MockProducer<byte[], byte[]> producer = new MockProducer<>(cluster, false, ...);
producer.send(new ProducerRecord<>("topic", "key", "value"));
assertFalse(producer.flushed());
producer.flush();
assertTrue(producer.flushed());
```

Rust: Create MockProducer with auto_complete=false. Send a record. Verify not flushed.
Call flush(). Verify flushed.

### 7. `test_should_throw_on_send_if_producer_is_closed` (Java: `shouldThrowOnSendIfProducerIsClosed`, line 624)

```java
MockProducer<byte[], byte[]> producer = new MockProducer<>();
producer.close();
assertThrows(IllegalStateException.class, () -> producer.send(record1));
```

Rust: Create MockProducer. Close it. Call send(). Verify it returns
`Err(KafkaError)` with `Errors::IllegalState`.

### 8. `test_should_throw_on_flush_if_producer_is_closed` (Java: `shouldThrowOnFlushProducerIfProducerIsClosed`, line 673)

```java
MockProducer<byte[], byte[]> producer = new MockProducer<>();
producer.close();
assertThrows(IllegalStateException.class, () -> producer.flush());
```

Rust: Create MockProducer. Close it. Call flush(). Verify it returns
`Err(KafkaError)` with `Errors::IllegalState`.

## Tests Excluded (46 tests)

| Category | Count | Tests |
|----------|-------|-------|
| Transactional lifecycle | 10 | shouldInitTransactions, shouldBeginTransactions, shouldCommitEmptyTransaction, shouldAbortEmptyTransaction, shouldCountCommittedTransaction, shouldNotCountAbortedTransaction, shouldThrowOnInit*, shouldThrowOnBegin*, shouldThrowOnCommit*, shouldThrowOnAbort* (precondition checks) |
| Transactional send behavior | 5 | shouldPublishMessagesOnlyAfterCommit*, shouldFlushOnCommit*, shouldDropMessagesOnAbort*, shouldThrowOnAbortForNonAutoComplete*, shouldPreserveCommittedMessages* |
| Consumer group offsets | 10 | shouldPublishConsumerGroupOffsets*, shouldThrowOnNull*, shouldIgnoreEmpty*, shouldAddOffsets*, shouldResetSentOffsets*, shouldPublishLatestAndCumulative*, shouldDropConsumerGroupOffsets*, shouldPreserveOffsets* (x2) |
| Fencing | 8 | shouldThrowFenceProducerIfTransactionsNotInitialized, shouldThrowOnBeginIfFenced, shouldThrowOnSendIfFenced, shouldThrowOnSendOffsetsIfFenced (x2), shouldThrowOnCommitIfFenced, shouldThrowOnAbortIfFenced, shouldNotThrowOnFlushIfFenced |
| Closed+transactional | 7 | shouldThrowOnInitIfClosed, shouldThrowOnBeginIfClosed, shouldThrowSendOffsetsIfClosed (x2), shouldThrowOnCommitIfClosed, shouldThrowOnAbortIfClosed, shouldThrowOnFenceIfClosed |
| Callback | 1 | testMetadataOnException |
| Partitioner | 1 | testPartitioner |
| Serializer | 1 | shouldThrowClassCastException |
| **Subtotal excluded** | **43** | |
| Fenced+flush | 1 | shouldNotThrowOnFlushProducerIfProducerIsFenced |
| sendOffsets preconditions | 2 | shouldThrowOnSendOffsetsToTransaction* (x2) |
| **Total excluded** | **46** | |

## Test Infrastructure

Entry point at `tests/clients/producer/main.rs`:
```rust
mod mock_producer_test;
```

Each test creates its own `MockProducer` instance — no shared state, no setup/teardown needed.

## Verification

1. `cargo build`
2. `cargo test`
3. `cargo xtask format-check`
4. `cargo xtask lint`
5. All 8 new tests pass + all existing tests still pass
