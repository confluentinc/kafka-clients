---
name: tokio-watch-send-vs-send-replace
description: tokio::sync::watch::Sender::send only stores when receivers exist; use send_replace when subscribers may not yet be alive
metadata:
  type: feedback
---

When using `tokio::sync::watch::Sender` to publish state that subscribers
may not yet be observing, use `send_replace(value)` not `send(value)`.

**Why:** `watch::Sender::send` returns `Err(SendError)` when there are zero
live receivers AND DOES NOT STORE THE NEW VALUE in that case. The next
`subscribe()` / `borrow()` sees the previous value. `send_replace` always
stores and returns the previous value.

**How to apply:** Used in `WakeupTrigger::rotate()` — the bg task may not
have called `subscribe()` by the time the first wakeup rotates the token.
With plain `send`, the rotation is silently dropped, leaving a cancelled
token in place. The unit test `rotate_replaces_the_token` caught this.
Pattern applies to any "current value" channel where the producer
publishes before consumers attach.
