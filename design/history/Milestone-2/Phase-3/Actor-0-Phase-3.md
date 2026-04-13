# Actor 0 Session - Phase 3: Wire Up Producer to NetworkClient

**Date:** 2026-04-10
**Branch:** `producer-attempt-Apr1`
**Role:** Actor (per agent-roles.md)

## Task

Rewrite ProducerBatch to use MemoryRecordsBuilder for proper Kafka RecordBatch wire format. Update Sender and RecordAccumulator accordingly.

## Commits

### 1. `114f0a3` - Rewrite ProducerBatch to use MemoryRecordsBuilder for proper Kafka wire format

Core rewrite of `src/clients/producer/batch.rs`:
- Replaced custom `[key_len:4][key]...` format with `MemoryRecordsBuilder`
- `new()` creates MemoryRecordsBuilder (magic v2, no compression)
- `try_append()` delegates to `MemoryRecordsBuilder::append()`
- `finalized_bytes()` closes builder, writes CRC, returns RecordBatch bytes
- Updated sender.rs and accumulator.rs for new API

### 2. `1520d0d` - Add end-to-end validation tests for ProducerBatch RecordBatch format

5 new tests verifying actual wire format: batch parsing, record round-trip, header preservation, CRC validation, ProduceRequest compatibility.

### 3. `da66c50` - fixup! Fix memory permit leak and silent error swallowing

Fixed 2 Critic issues:
| Issue | Fix |
|-------|-----|
| Memory permit leak (N-1)*61 bytes per batch | Removed header overhead from per-record estimate, added permits tracking |
| Append errors swallowed as None | Changed try_append to return Result<Option<SendFuture>, KafkaError> |

## Final State
- 560 tests passing (9 new in Phase 3)
- ProducerBatch now produces valid Kafka RecordBatch bytes
- NetworkProduceClient deferred to Phase 4
- All Critic issues resolved
