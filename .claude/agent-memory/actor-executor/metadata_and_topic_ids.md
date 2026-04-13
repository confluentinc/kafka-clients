---
name: Metadata lookup and topic ID architecture
description: KafkaProducer.wait_on_metadata() fetches metadata before accumulator append; topic IDs cached in KafkaProduceClient for v13+
type: project
---

Metadata lookup now happens in KafkaProducer.send() via wait_on_metadata() BEFORE appending to the accumulator, matching Java's KafkaProducer.doSend() -> waitOnMetadata() flow.

**Why:** Java's architecture has metadata fetched in the producer layer, not the network client layer. The old implementation had the metadata retry loop inside KafkaProduceClient.send_produce() which was architecturally wrong.

**How to apply:**
- KafkaProducer.wait_on_metadata() retries with backoff until max_block_ms is exceeded
- KafkaProduceClient.fetch_partitions() caches topic IDs from MetadataResponse in topic_ids HashMap
- build_produce_request_data() sets both topicId and name on TopicProduceData for v13 support
- v13 response parsing uses reverse topic_names map to resolve topicId -> name, matching Java's Sender.handleProduceResponse()
- ProduceClient trait has topic_ids() with default empty implementation
