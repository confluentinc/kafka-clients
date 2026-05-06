---
name: Phase-5c-2 Round-2 patterns
description: Verified-good fix shapes for Selector — io-progress flag, clear() split, TcpSocket pre-connect ordering, BTreeSet LRU
type: project
---

# Phase 5c-2 Round-2 verified patterns

Round-2 verification of fixup `fff9501` against Round-1 review of `c02ca85`.
All five comments fixed; 24/24 selector tests pass. Captured the
verified-good fix shapes for future review reuse.

## Verified-good fix patterns

### Java per-key idle.update → Rust io-active flag
- Java `pollSelectionKeys:525-526` calls `idleExpiryManager.update`
  unconditionally for every key returned ready by `nioSelector.select`.
- Rust analogue: track `made_progress` per channel inside
  `drive_channel_io` (read bytes>0 OR completed-receive OR
  bytes_written>0 OR completed-send) and update LRU only for io-active
  channels in `poll`'s tail.
- The fix correctly added write-bytes tracking — the original code
  only observed Err vs Ok on `channel.write()`, ignoring write
  progress.
- Regression test `busy_poll_does_not_reset_idle_clock`: 100ms
  idle budget, 300ms tight `poll(0)` loop, no I/O, assert disconnect
  state=Expired. This is the right shape for "busy-poll-no-IO doesn't
  reset clock."

### Java clear() ordering → Rust split
- Java `clear():842-863`: vec clears (843-846) → process closing
  channels (849-859, consumes failedSends entries via `.remove`) →
  drain remaining failedSends into disconnected (861-863).
- Rust split: `clear_per_poll_outputs` (vec clears) →
  `process_closing_channels` (swap_removes id from failed_sends) →
  `drain_failed_sends`. This is the correct ordering.
- The pre-fix bug was that `failed_sends` was drained BEFORE
  `process_closing_channels` ran, so the closing-channel sendFailed
  short-circuit (which depends on observing a non-empty failed_sends
  list) never fired.

### Java configureSocketChannel order → Rust TcpSocket pre-connect
- Java sets SO_KEEPALIVE, SO_SNDBUF, SO_RCVBUF on the unconnected
  SocketChannel BEFORE `socketChannel.connect()`. Required for
  TCP-window auto-tuning to pick up the buffer sizes.
- Tokio: `TcpStream::connect` skips all three. Use
  `TcpSocket::new_v4/v6` → `set_keepalive` →
  `set_send_buffer_size`/`set_recv_buffer_size` → `connect`.
- The `USE_DEFAULT_BUFFER_SIZE` (-1) sentinel preserves Java's
  Selectable contract: skip the setter to fall back to OS default.

### Java LinkedHashMap accessOrder → Rust BTreeSet+HashMap
- O(1) per touch in Java via doubly-linked nodes.
- O(log n) per touch in Rust: BTreeSet<(timestamp, id)> for ordered
  iteration, HashMap<id, timestamp> for previous-timestamp lookup.
- `update` removes old (prev, id), inserts (new, id). `remove` does
  the same in reverse. `poll_expired_connection` reads lowest via
  `iter().next()`.
- Tie-break difference: BTreeSet ties on equal timestamps go to
  smaller id; LinkedHashMap accessOrder ties go to insertion order.
  Practically irrelevant since `time.nanoseconds()` advances per
  call.

## Test-quality patterns observed (not blocking)

### Conditional assertion = conditional regression
Test pattern that hides regression value:

```rust
if selector.closing_channel(0).is_some() {
    // assertions here
}
// If the channel goes straight to disconnected without entering
// closing_channels, the test passes without exercising the bug.
```

If the assertion you care about is in a conditional branch and the
branch isn't always taken, the test passes trivially in the missed
case. Not a code bug — but the test name overstates its regression
value. Either:
- Add `else { panic!("setup failed: channel did not enter closing path") }`
- Force the path deterministically (e.g. inject a partial receive
  before close, instead of racing TCP RST timing)

### "Doesn't break" != "applied correctly"
Test pattern: connect with non-default settings, round-trip a
payload, conclude the settings are applied. This only verifies the
code path doesn't crash. To verify the settings actually stuck, need
`getsockopt` (or platform-specific equivalents). Java's own tests
don't do this either, so the bar is acceptable, but the regression
value is weaker than the test name suggests.

## Round-1 → Round-2 takeaways

1. When Java does X-per-ready-key, the Rust translation needs a
   per-channel signal that captures "would the OS have returned this
   key as ready?" — usually io-progress works, but check whether
   handshake-only readiness should also count. (Phase 5c-2 chose not
   to count handshake; acceptable since other paths handle stuck
   handshakes.)
2. When the Java `clear()` is a single private method but the Rust
   translation needs to split it (e.g. to insert
   `process_closing_channels` between phases), preserve the Java
   line-by-line ordering — getting it backwards is a silent
   behavior bug.
3. `TcpSocket` (not `TcpStream::connect`) is the correct primitive
   when you need to mirror Java's pre-connect socket option setup.
4. BTreeSet<(timestamp, id)> + HashMap<id, timestamp> is the right
   O(log n) translation of LinkedHashMap accessOrder=true. Both
   maps must be modified together inside the same `&mut self` method
   for atomicity.
