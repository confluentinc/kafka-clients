---
name: Producer pipeline refactoring plan
description: Milestone 2 Phases 5-7 eliminate ProduceClient/KafkaProduceClient Mutex bottleneck by reusing existing NetworkClient and KafkaClient trait
type: project
---

The producer pipeline has a Mutex bottleneck because `KafkaProduceClient` wraps a `Selector` behind `tokio::Mutex` and the `Sender` holds it for full network round trips. The fix is 3 phases:

- Phase 5: Make `KafkaClient` trait async (replace `block_on()` in `NetworkClient` with `.await`)
- Phase 6: Rewrite `Sender` to own `KafkaClient` directly, delete `ProduceClient` trait and `KafkaProduceClient`, implement `send_producer_data()` + `client.poll()` loop with `RequestCompletionHandler` callbacks
- Phase 7: Add `ProducerMetadata` (wrapping `Metadata`), update `KafkaProducer.wait_on_metadata()`, update integration/perf tests

**Why:** Performance is ~85k msg/s due to sequential send-wait-respond. Java achieves parallelism via non-blocking `client.send()` + single `client.poll()` driving all I/O with callbacks.

**How to apply:** Phases must be done in order (5 before 6, 6 before 7). Agent number for this work is N=0 per user request. Comment files go in `design/history/Milestone-2/Phase-N/`.
