---
name: ProducerMetadata composition patterns
description: Bugs from Java inheritance-to-Rust-composition translation in ProducerMetadata -- update listener vs method override gaps
type: feedback
---

When Java uses inheritance (ProducerMetadata extends Metadata), the Rust translation uses composition with callback functions. Key pitfalls:

1. **Override methods that set state must ALL be represented by callbacks**: Java's ProducerMetadata.update() both clears newTopics AND sets errors. The Rust translation used an update_listener_fn for newTopics but stored errors in a separate wrapper method that isn't called in the production path (DefaultMetadataUpdater calls Metadata.update() directly).

2. **Missing sender wakeup**: When the producer and sender are decoupled (sender moved into spawned task), there's no mechanism to interrupt the sender's poll. Java holds a sender reference and calls sender.wakeup(). The Rust version needs a shared wakeup handle.

3. **Loop-invariant topic expiry refresh**: Java's waitOnMetadata refreshes topic expiry on each iteration with metadata.add(topic, nowMs + elapsed). Translators tend to hoist this to a single call before the loop, causing topic expiry drift.

**Why:** These patterns arise from the structural difference between Java's dynamic dispatch (synchronized methods, object monitors) and Rust's callback-based composition. The key insight is that ALL state mutations in a Java override method must be captured by the corresponding Rust callback -- not just the ones that seem most important.

**How to apply:** When reviewing Java-extends-Metadata patterns, check that every line of the Java override has a Rust equivalent executed through the same code path (callback, not wrapper method).
