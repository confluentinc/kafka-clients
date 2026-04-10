---
name: Layer 6 NetworkClient review patterns
description: Common issues found in Layer 6 Java-to-Rust translation of NetworkClient, Metadata, and connection management
type: project
---

Key patterns found in Layer 6 review:

1. **Async/sync boundary issues**: When Java methods are synchronous but call methods that are async in Rust (like selector.connect()), the Actor may skip the call entirely with a comment about "next poll cycle" rather than using block_on(). This creates bugs where connections are never established.

2. **Java exception hierarchy used for isinstance checks**: Java metadata code uses `instanceof InvalidMetadataException` to determine if errors should trigger re-fetches. The Rust translation must carefully map this to the correct set of Errors enum variants. InvalidTopicException and TopicAuthorizationException are NOT subclasses of InvalidMetadataException despite appearing near metadata-related code.

3. **Time.milliseconds() vs passed-in timestamps**: Java's NetworkClient calls `time.milliseconds()` after selector.poll() to get a fresh timestamp. Rust passes the original `now` parameter through, which skips the time update. This pattern could recur wherever Java uses `time.milliseconds()` mid-method.

4. **IllegalStateException mapping**: Not all IllegalStateExceptions should be panics. When Java's `leastLoadedNode` throws ISE for empty nodes, the Actor chose to return None instead, which changes the contract.

**Why:** These patterns cause subtle runtime bugs that are hard to catch in tests (especially with MockSelector).
**How to apply:** When reviewing future NetworkClient-adjacent code, specifically check for these four pattern categories.
