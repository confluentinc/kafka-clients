---
name: Phase 5c-2 Selector (Tokio rewrite)
description: Single-task Selector design choices and Tokio↔Java NIO architectural mappings — load-bearing for Phase 5d NetworkClient
type: project
---

Phase 5c-2 landed `common::network::Selector` — the Tokio rewrite of
`org.apache.kafka.common.network.Selector`. ~660 LOC of source +
~570 LOC of tests in a single file (`src/common/network/selector.rs`).

**Why single-task** (not per-channel read/write tasks): the brief
suggested per-channel tasks but two pragmatic blockers exist:
1. SSL handshake (rustls `ClientConnection`) needs both halves of the
   TCP stream, so splitting via `into_split()` would force redesigning
   `SslTransportLayer` from Phase 5b-2.
2. Java semantics expect "this class is not thread safe" + a
   per-poll completed-sends drain — long-lived per-channel write
   tasks would race the drain.

The chosen design preserves Java semantics exactly: `&mut self` on
every method, single-task ownership of `KafkaChannel`s, non-blocking
syscalls (`try_read` / `try_write_vectored`) inside a synchronous I/O
loop. The only Tokio task spawned is the **short-lived connect task**
per `connect()` call, which mirrors Java's `OP_CONNECT` notification.

## Key design choices Phase 5d / 9 must respect

1. **Connect-task pattern**: `Selector::connect(id, addr, ...)`
   spawns a `TcpStream::connect(addr).await` task that pushes either
   `ConnectEvent::Connected{id, stream}` or `Failed{id, err}` onto
   an internal `mpsc::UnboundedSender<ConnectEvent>`. The Selector's
   `poll()` drains this on every tick + races it against the timeout
   sleep via `tokio::select! { biased; ... }`. The connect task is
   deliberately not held across cancellation — the JoinHandle is
   stored only so `close()` / `close_connection()` can `abort()`.

2. **`tokio::select!` with `biased; ` keyword**: only two arms (mpsc
   recv + sleep). Both are cancellation-safe. Side effects only
   occur inside `dispatch_connect_event`, called *after* the recv
   resolves. No MutexGuard across `.await` because the Selector
   holds no Mutex — `&mut self` everywhere. CLAUDE.md 9.6 honored.

3. **`wakeup()` is a no-op**. Java's wakeup aborts a blocking
   `nioSelector.select(timeout)` from another thread. Tokio tasks
   wake naturally; the Selector is single-task. NetworkClient (5d)
   should call it as a no-op stub to preserve the trait shape.

4. **Connection ids are `i32`** end-to-end. `NetworkSend::destination_id()`
   is parsed via `dest.parse::<i32>()`. The upstream invariant that
   destination_id is always `Integer.toString(node.id())` means the
   parse never fails in practice; a non-numeric value is a
   contract violation that panics (CLAUDE.md 10.1). This matches
   Java's `IllegalStateException` from `openOrClosingChannelOrFail`.

5. **`failed_sends.push(id)` + `close_internal` + panic in
   `Selector::send`**: when `KafkaChannel::set_send` returns
   `IllegalState` (caller queued a send while one was in flight),
   we push to `failed_sends`, close with `DiscardNoNotify`, AND
   panic. Java rethrows the `IllegalStateException`; we mirror with
   a panic. The `disconnected()` map will surface `FAILED_SEND` on
   the next poll, matching Java semantics from
   `testCantSendWithInProgress`.

6. **`close_internal` graceful path**: on `CloseMode::Graceful` with
   a partially-read receive, the channel moves to `closing_channels`
   for one more poll tick of best-effort drain (mirrors Java's
   `maybeReadFromClosingChannel`). The drain happens in
   `process_closing_channels` at the top of the next `poll()`.

7. **Idle-expiry**: `IdleExpiryManager` uses a `HashMap<id, ns>` +
   `VecDeque<id>` for LRU order (Java uses `LinkedHashMap` with
   `accessOrder=true`). On `update`, the id is removed from its
   prior position and pushed to the back. `poll_expired_connection`
   inspects only the front. Algorithm preserved verbatim.

8. **Per-poll output collections** (`completed_sends`,
   `completed_receives`, `connected`, `disconnected`): `Vec`/`HashMap`,
   cleared at the top of each `poll()`. `failed_sends` is a side
   list that drains into `disconnected` during the clear.

9. **`completed_receives` is `Vec`, not `LinkedHashMap`**. The
   Java guarantee "at most one entry per channel per poll" is
   enforced via `completed_receive_ids: HashSet<i32>`.

10. **EchoServer test fixture**: a localhost `TcpListener` accepts
    streams and echoes back size-prefixed frames. `close_connections`
    uses a shared `tokio::sync::Notify` to signal all spawned
    handlers to break out. **Do not** spawn one accept task per
    connection — use a single accept loop racing the shutdown
    `Notify`.

## Skipped vs. Java (per Phase 5 NOTES.md and PLAN.md)

- SASL re-authentication (Phase 9): `pollResponseReceivedDuringReauthentication`,
  `successfulAuth*` counters, `DelayedAuthenticationFailureClose`,
  `failedAuthenticationDelayMs`.
- `MemoryPool` mute-on-OOM: NetworkReceive allocates eagerly.
- `SelectorMetrics`: replaced with `// metric stub` no-op comments.
- `register(String, SocketChannel)`: server-side accept path.
- `lowestPriorityChannel()`: server-side `max.connections` helper.

## Tests written (20 selector tests, all <1s total)

Translations of `SelectorTest.java`:
- `connect_send_receive_round_trip` ← `testNormalOperation` (single-channel)
- `multi_connection_normal_operation` ← `testNormalOperation` (5 conns × 50 reqs)
- `server_disconnect_surfaces_in_disconnected` ← `testServerDisconnect`
- `double_send_panics_and_is_failed` ← `testCantSendWithInProgress`
- `send_without_connecting_panics` ← `testSendWithoutConnecting`
- `connect_to_unbound_port_surfaces_as_disconnect` ← `testConnectionRefused`
- `idle_connection_is_expired` ← `testCloseOldestConnection` + `testIdleExpiryWithoutReadyKeys`
- `duplicate_connect_id_returns_illegal_state` ← `testExistingConnectionId`
- `mute_suppresses_reads_until_unmute` ← `testMute`
- `empty_request_round_trips` ← `testEmptyRequest`
- `clear_completed_sends_and_receives` ← `testClearCompletedSendsAndReceives`
- `large_request_round_trips` ← `testSendLargeRequest`
- `local_close_does_not_notify_disconnect` ← graceful local close
- `build_channel_failure_surfaces_as_disconnect` ← Java's `registerFailure`/`testCloseAllChannels` via builder error
- `zero_byte_write_completes_send` ← `testWriteCompletesSendWithNoBytesWritten` (end-to-end)
- `close_during_iteration_does_not_panic` ← `testChannelCloseWhileProcessingReceives`
- `wakeup_is_noop` — pinning the no-op contract
- `pending_connects_clears_on_completion` — connect-task lifecycle
- `idle_expiry_manager_polls_oldest_first` — IdleExpiryManager unit test
- `close_aborts_in_flight_connect_tasks` — close path lifecycle

The tests use `SystemTime::instance()` (real clock) + `tokio::time::sleep`
to drive idle expiry, not `MockTime`. Reason: `MockTime::sleep_internal`
advances a fake clock but doesn't wake a `tokio::time::sleep` future — to
fake-out clock+sleep we'd need to drive `tokio::time::pause()` /
`advance()`. Real-clock tests are robust enough at the scales used
(50–150 ms idle thresholds).
