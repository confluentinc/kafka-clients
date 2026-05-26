---
name: Phase 8a.0 first-broker wire debug
description: First real-broker exchange surfaced a scheduling bug, not a wire-bytes bug — Tokio Selector::poll missing OP_READ + has_send() arms
type: project
---

First real-broker wire encounter (apache/kafka:4.2.0 Testcontainer + producer_smoke_test).
Wire-byte hypothesis was wrong; the bug was scheduling-layer.

**Why:** When the producer crate is translated all-at-once before any
integration test, you can ship a Selector that passes every unit test
(because tests poke channels synchronously) yet silently times out
against a real broker because the I/O loop never wakes on socket
readiness. The wire bytes were correct on the very first attempt —
Kafka 4.2.0 broker decoded the v4 ApiVersionsRequest and replied
with a well-formed v4 response. The producer sat idle for the full
`default.request.timeout.ms` (30s) anyway.

**How to apply:** When a producer translates cleanly but `Producer::send`
hangs against a real broker for ~30s and then `Timeout`s:
- Step 1: capture bytes on both sides (request + response). If broker
  responds at all, the wire format is correct. Pin the bytes as
  hex-fixture regression tests on both `Request::write` and
  `Response::parse` — Phase 2 never had a live broker to do this with.
- Step 2: investigate the I/O scheduler before the wire encoder. The
  Tokio rewrite of Java's `nio.Selector` must arm a future per
  registered socket that resolves on read-readiness (e.g.
  `TcpStream::poll_peek` against a 1-byte scratch — non-destructive,
  matches `OP_READ` semantics). Without that arm, `Selector::poll(t)`
  sleeps the full `t` ms before the post-select `drive_channel_io`
  notices buffered bytes.
- Step 3: `has_immediate_work` in `Selector::poll` must include
  `has_send()` — a freshly-queued send is OP_WRITE-equivalent work
  that should short-circuit the sleep, otherwise the request waits
  the full timeout window before the next-tick `drive_channel_io` writes it.

**Trait surface:** Adding `poll_read_ready(&self, &mut Context) -> Poll<()>` to
`TransportLayer` with a `Poll::Ready(())` default impl is the right
pattern. Transports without OS-level readiness (mock, in-memory) fall
back to timeout-driven polling; real-socket transports
(`PlaintextTransportLayer`, `SslTransportLayer`) override.

**Sync bound gotcha:** Holding `&dyn TransportLayer` across an `.await`
point inside `Selector::poll` requires `T: Sync` (because `&T: Send` iff
`T: Sync`). `BoxedTransport` alias must include `+ Sync` even though
both production impls already are.
