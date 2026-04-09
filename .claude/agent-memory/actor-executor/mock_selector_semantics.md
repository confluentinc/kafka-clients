---
name: MockSelector Java vs Rust semantics
description: Key behavioral differences between Java MockSelector and Rust MockSelector that cause test failures
type: feedback
---

Three critical Java-to-Rust MockSelector/NetworkClient translation pitfalls:

1. **Java `connected()` returns snapshot-and-clear; Rust returns `&[String]`**. In Java, each call to `connected()` returns accumulated connections and clears the list. Since our `Selectable` trait's `connected()` returns a reference, we replicate one-shot semantics by clearing `connected` at the start of `poll()` and moving `pending_connected` in. Without this, `handle_connections()` re-processes old connections every poll.

2. **Java shares the same `Send` object between InFlightRequest and selector; Rust uses separate copies**. In Java, `send.completed()` on the InFlightRequest reflects actual I/O completion. In Rust, the InFlightRequest's send is never written, so `completed()` is always false. Fixed via a `send_completed` flag set by `handle_completed_sends()`.

3. **Java `disconnect()` uses `time.milliseconds()` (MockTime); Rust had `SystemTime::now()`**. Tests use synthetic timestamps (starting at 0), so wall-clock time breaks backoff calculations. Fixed by storing `last_poll_time_ms` from `poll()`/`ready()` calls.

**Why:** These are fundamental Java/Rust ownership and API differences that aren't obvious from reading Java code.
**How to apply:** When translating test infrastructure that relies on shared mutable state (Java references), check whether Rust's ownership model requires explicit synchronization (flags, stored timestamps, etc.).
