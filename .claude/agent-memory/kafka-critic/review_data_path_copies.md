---
name: Data path copy analysis
description: Complete trace of key/value byte copies from Python FFI through batch buffer to TCP socket — only 2 actual data copies in the zero-copy path
type: project
---

The key/value zero-copy path (KafkaProducer<Vec<u8>, Vec<u8>>::send) achieves only 2 actual memcpy operations:
1. Key/value bytes -> batch buffer via DefaultRecord::write_to (write_all on Vec<u8>) -- necessary for wire format serialization
2. Batch buffer -> kernel socket buffer via writev syscall -- unavoidable kernel boundary

**Why:** The scatter-gather SendBuilder design (write_records pushes Vec<u8> into completed_buffers without copying) and ownership-transfer chain (std::mem::take, Option::take, into_buffer) eliminate intermediate copies.

**How to apply:** When reviewing changes to the producer pipeline, verify that no new copies are introduced in the move chain: MemoryRecordsBuilder.take_batch_data -> MemoryRecords.into_buffer -> RequestBatchInfo.records_data.take -> PartitionProduceData.records.take -> SendBuilder.write_records -> ByteBufferSend.new -> IoSlice references -> writev. Any step that introduces clone(), to_vec(), or extend_from_slice on the batch buffer is a regression.

Key fragility: take_batch_data() uses std::mem::take when initial_position==0 (zero-cost), but falls back to .to_vec() copy when initial_position>0. The producer path always uses 0, but this invariant isn't enforced by types.
