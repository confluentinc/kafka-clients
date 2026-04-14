# Phase 5 -- Make KafkaClient Trait Async-Compatible for Producer Sender

## Goal

Solve the async/sync impedance mismatch that prevents `NetworkClient` from being
used inside a Tokio task. Currently, `NetworkClient` calls `block_on()` for every
`Selector` operation, which panics if the future is not immediately ready.  The
real `Selector` returns `Pending` futures, so `NetworkClient` cannot be used from
the `Sender` Tokio task today.

This phase makes `KafkaClient` methods properly async so that `Sender` (Phase 6)
can call `ready()`, `send()`, and `poll()` inside an async context.

## Java Reference

Java's `KafkaClient` and `NetworkClient` are synchronous (single-threaded event
loop), matching Java NIO's non-blocking selector model.  In Rust with Tokio, the
equivalent is `async fn` -- the Selector is async, and `NetworkClient` must
propagate that.  This is the CLAUDE.md rule 8 ("non-blocking IO with Tokio")
adaptation.

## Design Decision: Async KafkaClient Trait

The `KafkaClient` trait methods that touch the `Selector` need to become async:

| Method | Currently | Change |
|--------|-----------|--------|
| `ready()` | sync, calls `block_on(initiate_connect)` | `async fn` |
| `poll()` | sync, calls `block_on(selector.poll)` | `async fn` |
| `disconnect()` | sync, calls `block_on(selector.close_channel)` | `async fn` |
| `close_connection()` | sync, calls `block_on(selector.close_channel)` | `async fn` |
| `close()` | sync, calls `block_on(selector.close)` | `async fn` |

Methods that do NOT touch the selector remain sync:
`is_ready()`, `connection_delay()`, `poll_delay_ms()`, `connection_failed()`,
`authentication_error()`, `send()` (just queues), `least_loaded_node()`,
`in_flight_request_count()`, `has_in_flight_requests()`, `has_ready_nodes()`,
`wakeup()`, `new_client_request()`, `new_client_request_with_timeout()`,
`initiate_close()`, `active()`.

### Why async trait, not spawning

Making the trait async (using `async_trait` or native async trait) is the minimal
change.  The alternative -- keeping the trait sync and spawning a background
runtime -- would diverge from Java's architecture where `Sender` directly owns
the client with no additional threads.

### Mock boundary

`MockSelector` already returns immediately-ready futures.  With async trait
methods, `NetworkClient<MockSelector, _>` implements `KafkaClient` and can be
used in `#[tokio::test]` directly.  No change to `MockSelector` is needed.

## Files to Modify

| File | Change |
|------|--------|
| `src/clients/kafka_client.rs` | Add `async` to `ready`, `poll`, `disconnect`, `close_connection`, `close` |
| `src/clients/network_client.rs` | Change `impl KafkaClient` to use async methods; remove `block_on()` calls, replace with `.await` |
| `src/clients/network_client_utils.rs` | Update to call async KafkaClient methods |

## Files NOT Changed

- `src/clients/producer/` -- no producer changes in this phase
- `src/clients/client_request.rs`, `client_response.rs` -- unchanged
- `src/clients/in_flight_requests.rs` -- unchanged

## Tests to Update

All existing `NetworkClient` unit tests in `src/clients/network_client.rs` must
remain passing.  They use `MockSelector` which returns ready futures, so making
the trait async and adding `.await` in tests is mechanical.

## Scope Limits

- Only the `KafkaClient` trait signature and `NetworkClient` implementation change.
- No producer code changes.
- No new classes.
- Existing behavior is preserved -- this is a pure async-ification refactor.

## Definition of Done

1. `KafkaClient` trait has async methods for `ready`, `poll`, `disconnect`, `close_connection`, `close`
2. `NetworkClient` implementation uses `.await` instead of `block_on()`
3. `block_on()` and `noop_waker()` helper functions are removed
4. All existing `NetworkClient` tests pass under `#[tokio::test]`
5. `cargo build` succeeds
6. `cargo test` passes
7. `cargo xtask format-check` passes
8. `cargo xtask lint` passes
