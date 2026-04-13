---
name: KafkaProduceClient adapter patterns
description: Common issues in direct-Selector usage bypassing NetworkClient — missing caching, reconnection bugs, double clones
type: feedback
---

When translating the produce path with a direct Selector adapter (bypassing NetworkClient), watch for:

1. **Per-request handshakes**: Code that calls ApiVersions on every request instead of caching per-node. Java caches in NodeApiVersions; any adapter must replicate this.

2. **Reconnection without channel cleanup**: Selector.connect() fails with AlreadyExists if channel still registered. Must close_channel + poll before reconnecting. The connected_nodes map and selector.channels can get out of sync.

3. **Double data clones in request building**: When grouping batches by topic into a HashMap, the data gets cloned into the HashMap and then cloned again when iterating by reference. Use into_iter() or take ownership to avoid.

**Why:** These patterns emerge when bypassing NetworkClient's connection state management. NetworkClient handles API version caching, connection lifecycle, and reconnection internally.

**How to apply:** When reviewing any code that uses Selector directly (rather than through NetworkClient), check for these three patterns specifically.
