# Critic 23 — Phase 23 (non-allocating persistent readiness wait) — RESOLVED

Both DoD-gate issues raised by Critic 23 have been fixed in the working tree
and verified. Moved here from `COMMENTS.23.md`.

## Issue 1: `cargo xtask format-check` fails — DoD gate not met (RESOLVED)
- **File**: `src/common/network/ssl_transport_layer.rs:391,403`,
  `src/common/network/plaintext_transport_layer.rs:205,215`
- **Severity**: Missing Requirement (DoD)
- **Description**: The four new `poll_readable`/`poll_writable` impls were not
  rustfmt-clean (rustfmt wanted the `SslState::Closed =>` / plaintext `None =>`
  error arms reflowed into single-line blocks).
- **Resolution**: Ran `cargo xtask format`. The four arms were reflowed to the
  rustfmt-preferred shape. `cargo xtask format-check` is now green
  (`✅ All code is properly formatted!`). No logic change.

## Issue 2: Required new selector unit test is missing (RESOLVED)
- **File**: `src/common/network/selector.rs` (test module)
- **Severity**: Missing Requirement (DoD)
- **Description**: PLAN.md §Tests requires a new selector unit test exercising
  the rewritten wait path with three assertions: (a) a readable channel wakes
  the parked `poll`; (b) `wakeup()` returns the parked `poll` promptly; (c) an
  idle muted channel does NOT spin (poll returns on the deadline, not
  immediately).
- **Resolution**: Added `test_readiness_wait_path` to the
  `common::network::selector::tests` module, using the existing real TCP
  loopback `EchoServer` scaffolding (no mock transport — drives the production
  socket-readiness path). All three sub-assertions implemented:
    - **(a)** Send a request, then `poll(5_000)` parks on read-readiness; when
      the echo response arrives the registered waker un-parks the poll and the
      response is delivered well before the 5s deadline.
    - **(b)** With the channel idle, grab `wakeup_notify()`, spawn a task that
      fires `notify_one()` after 50ms, then `poll(10_000)`; the `notify.notified()`
      `biased;` arm wins and the poll returns inside a 2s hard timeout — far
      short of the 10s deadline.
    - **(c)** Mute the only channel (`channel_interest` → `(false,false)`,
      `has_interested_channel()` → false), then `poll(200)`; assert the elapsed
      time is `>= ~80%` of the requested timeout, i.e. the poll parked to the
      deadline rather than busy-spinning (a spurious `Ready` would return in
      ~0ms).
  Each sub-assertion is wrapped in `tokio::time::timeout` so a hung wait path
  fails the test instead of blocking the suite (deterministic and bounded).
- **Sanity-check (test tests something real)**: Verified via temporary
  mutations that the test fails when the wait path is broken:
    - Dropping the read-readiness waker registration in
      `poll_channel_readiness` → sub-assertion (a) fails (parked poll never
      wakes, times out at 5s).
    - Returning a spurious `Poll::Ready` (busy-spin) from
      `poll_channel_readiness` → also caught (surfaces as (a) failing because
      the poll spins without ever performing the read).
  Mutations were reverted; the production logic is unchanged.

## Verification (after fixes)
- `cargo build` — OK
- `cargo test --lib` — 1759 passed, 0 failed (1758 prior + 1 new test)
- `cargo test --lib common::network` — 71 passed (70 prior + new test)
- `cargo test --lib network_client` — 62 passed
- `cargo xtask lint` — clean
- `cargo xtask format-check` — green
