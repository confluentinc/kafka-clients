---
name: phase7c-design-notes
description: "Milestone-8 Phase 7c: TopicMetadataRequestManager translation patterns — Vec inflight with u64 ids replacing Java this-identity, MetadataRequestBuilder reuse, partitionless-topic cluster.topics() behaviour"
metadata:
  type: project
---

# Phase 7c — `TopicMetadataRequestManager` (Milestone 8)

**Why:** Translation of Apache Kafka's `TopicMetadataRequestManager`
(283 LOC) for the consumer's `list_topics()` and `partitions_for(topic)`
paths. The smallest of three parallel 7b/7c/7d sub-phases.

**How to apply:** When extending or debugging the topic-metadata
request path, or when translating analogous Java inner-class request
managers.

## Lessons / patterns

1. **`u64 request_id` replaces Java `this`-reference identity.** Java's
   `inflightRequests.remove(this)` relies on object identity inside an
   inner class. Rust can't safely match by `&` pointer (state may move
   during `Vec::remove`); use a monotonically-increasing `next_request_id`
   on the manager and a `find_inflight_index(request_id)` lookup. The
   counter is `u64` with `wrapping_add(1)` — overflow takes 2^64 calls.

2. **`MetadataRequestBuilder` is shared with the producer.** The Phase 6
   plan-§ instructs reuse from `src/common/requests/metadata_request.rs`
   — the consumer's metadata-request shape is identical. The producer's
   `MetadataRequestBuilder::new(topics: Option<&[&str]>, allow_auto: bool)`
   takes a slice of `&str`. For a single-topic request, use
   `Some(&[topic_str])`.

3. **`MetadataRequestBuilder::all_topics()` matches Java's
   `MetadataRequest.Builder.allTopics()`** — sets the topic list to
   `None` and `allowAutoTopicCreation=true` (the latter to satisfy
   V2-and-older serialization symmetry).

4. **`cluster.topics()` skips partitionless topics.** Java's
   `MetadataResponse.buildCluster()` only adds a topic to its
   `partitionsByTopic` map if the topic-metadata's partition list is
   non-empty AND `error_code == NONE`. Rust's `Cluster::topics()`
   delegates to `partitions_by_topic.keys()`. A regression test that
   reports a topic with `error_code==NONE` and `Collections.emptyList()`
   for partitions WILL NOT find that topic in the result map. To pin
   the success path, include at least one `MetadataResponsePartition` in
   the synthesised response.

5. **`Errors::UnknownTopicOrPartition` is "topic absent", not an
   error.** Java's `continue` branch in `handleTopicMetadataResponse`
   omits the topic from the result map and proceeds. Don't classify
   this as a fatal error.

6. **`Errors::InvalidTopicException`'s Rust analog is
   `KafkaError::invalid_topics(HashSet<String>)`** — the
   `InvalidTopic` variant carries a `HashSet`; populate it with the
   one offending topic name from the response's `errors()` map.

7. **`MetadataResponse::errors()` panics if any topic has a `None`
   name.** Acceptable here because the consumer always requests by
   topic name (not topic ID). If a future path uses topic IDs,
   switch to `errors_by_topic_id()`.

8. **`ConsumerConfig::allow_auto_create_topics` is `pub(crate)`.**
   No public builder method exposed; tests set the field directly.
   When building the manager, read `config.allow_auto_create_topics`
   (not a method call).

9. **`closing` flag DOES gate `poll`.** Java's `signalClose` default
   is a no-op for this manager. Phase 6's `CoordinatorRequestManager`
   adds a `closing: bool` and `if self.closing { return PollResult::empty() }`
   guard; we adopted the same shape here for crate-wide uniformity. If
   the critic flags this as a Java-behaviour deviation, the answer is
   that it matches Phase 6 precedent — gate by manager-set rule, not
   per-class.

10. **`RequestManagers::entries()` uses `Self { coordinator,
    topic_metadata, closed: _ } = self` to satisfy the borrow checker
    when iterating multiple `Option<...>` fields.** Direct
    `self.coordinator.as_mut(); self.topic_metadata.as_mut();` fails
    with "cannot borrow `self.topic_metadata` after partial move from
    `self.coordinator`".

## Commit / phase shape

- Single commit suggested by plan as two commits (prod, then tests).
  Tests are inline `#[cfg(test)] mod tests` per the same-file
  pub(crate) convention. Splitting would be artificial — bundled
  commit acceptable when the plan says "Suggested".

## Out of scope

- `TopicMetadataFetcher.java` (used only by `ClassicKafkaConsumer`)
- `TopicMetadataFetcherTest.java`
- Metrics / Sensor / ClientTelemetry parameters

See [[phase6_design_notes]] for the upstream `RequestManager` /
`UnsentRequest` / `FutureCompletionHandler` scaffolding this builds on.
See [[phase7a_design_notes]] for sibling fetch-path foundations.
