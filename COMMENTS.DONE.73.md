# Critic 73 — resolved comments (commit `733931bd`, P2: per-partition lookup in `with_in_flight_batch_pool`)

## Finding 1 — `TxnPartitionMap.java:118` was mislabeled as `adjustSequencesDueToFailedBatch`

- **Severity**: LOW (documentation / citation accuracy — no behavioural effect)
- **Where**: `src/producer/internals/record_accumulator.rs` — the `SenderInFlightRestore`
  rustdoc (`TxnPartitionMap.java:36/43/118`) and the inline comment above the
  `remove` loop in `with_in_flight_batch_pool` (``:118` `adjustSequencesDueToFailedBatch``);
  also the commit message of `733931bd`.
- **What was wrong**: line 118 of
  `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/TxnPartitionMap.java`
  is inside `maybeUpdateLastAckedSequence` (declared `:117`, body `:118-121`).
  `adjustSequencesDueToFailedBatch` is declared at `:106` and does its direct
  `HashMap` lookup at `:114` (`get(batch.topicPartition)...`, routed through `get()`
  at `:42-43`). The claim the citation supports — Java looks partitions up directly
  and never scans — was and is correct.
- **Resolution**: both Rust cites changed from `:118` to `:114` in the fixup commit
  "fixup of 733931bd: cite TxnPartitionMap.java:114, not :118, for
  adjustSequencesDueToFailedBatch (COMMENTS.73.md Finding 1)". The original commit
  message is not amended; its `:118` should be read as `:114`. Verified against the
  Java source by the Manager before the fix (Java `:104-122` read directly).
