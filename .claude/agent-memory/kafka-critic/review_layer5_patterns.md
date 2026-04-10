---
name: Layer 5 Channel & Selection review patterns
description: Translation pitfalls in Selector/KafkaChannel — format string typos, test coverage gaps, poll loop design, idle expiry LRU semantics
type: feedback
---

Format string bugs in Rust translations are a subtle category. In `listener_name.rs`, the actor used `\"` in a format string, which in Rust produces a literal `"` (double-quote), not `\"` as escaped character. Unlike Java where `\"` in a string literal always produces a double-quote, in Rust format strings the backslash-quote just produces the quote. The intent was to produce a trailing `.` (period) but copy-paste or auto-complete introduced the wrong character.

**Why:** These bugs silently produce incorrect strings that break configuration prefix lookups downstream.

**How to apply:** When reviewing format strings, verify each literal character against the Java source output. Pay special attention to punctuation characters at boundaries of format placeholders.

The Selector poll loop design replaces Java NIO's `Selector.select(timeout)` with a busy-poll loop using `tokio::time::timeout(Duration::ZERO)` for non-blocking reads combined with 1ms sleep intervals. This is architecturally different but functionally correct for the PLAINTEXT path. The `Duration::ZERO` timeout ensures `stream.readable().await` inside `PlaintextTransportLayer::read()` is cancelled immediately if no data is ready, making reads truly non-blocking from the Selector's perspective.

Test coverage for graceful close, LRU idle expiry, and edge cases (zero-byte-send completion) tend to be under-translated because they require more complex test infrastructure (multiple pending receives, time manipulation, mock transport layers).

A key semantic difference between Java NIO and the Rust poll loop: Java only iterates channels with ready NIO selection keys (implicit I/O readiness filter), while Rust iterates ALL channels. This means any unconditional action in Java's `pollSelectionKeys` (like the idle expiry LRU update) needs an explicit activity check in Rust. The activity check must account for ALL forms of I/O progress (partial reads/writes transferring bytes, connection establishment, completed sends/receives), not just completed I/O operations.

**Why:** Only checking completed sends/receives misses channels doing large/slow transfers where individual reads don't complete a full NetworkReceive. Issue 27 identified this.

**How to apply:** When reviewing `poll_channel` or equivalent per-channel processing, verify that any Java behavior conditional on NIO key selection has an equivalent readiness/activity guard in Rust that captures all the same cases.
