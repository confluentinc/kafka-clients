# Critic 8 review — RESOLVED (fixed in commit 70bcb5ee)

All three Minor findings addressed. No Blocker/Major were raised; the core of
all five fix commits was confirmed correct by the Critic (firing-count
invariant, empty-update_features raise, one-shot materialization, soak
fast-fail, gRPC removeAll response semantics).

## Issue 1 — JAAS parser over-matches `key=` inside a quoted value — FIXED
`parse_jaas_option` (`src/common/config/sasl_configs.rs`) rewritten to walk the
string once, **skip quoted regions wholesale**, and only match a key at an
option start (start-of-string or after whitespace) — mirroring Java's JAAS
lexer. The Critic's concrete case
`PlainLoginModule required password="username=x" username="right";` now resolves
`username` → `right` (previously returned garbage `x"`). Regression test added:
`test_jaas_config_key_inside_a_quoted_value_does_not_match`. The existing
serviceName / namespace / substring guards still pass (14 JAAS tests green).

## Issue 2 — stale `admin_remove_members_trampoline` comment — FIXED
`bindings/python/_confluentkafka.c` comment corrected: in removeAll mode the
callback fires exactly once with a NULL group instance id (→ Python `None`)
carrying the whole-op `all()` outcome, instead of "never fires at all".

## Issue 3 — `when_complete` doc / `#[allow(dead_code)]` — FIXED (with a correction to the finding)
The Critic reported that `KafkaFuture::when_complete` (kafka_future.rs line
~271, the one downgraded to `pub(crate)`) is also called from core at
`kafka_admin_client.rs:4325`, so `#[allow(dead_code)]` is unnecessary.

That call site is on a **`KafkaFutureImpl`** (`admin_api_future.rs:119`
`handle()` returns `KafkaFutureImpl<V>`), so it invokes the *separate*
`KafkaFutureImpl::when_complete` (line ~425), not the downgraded
`KafkaFuture::when_complete`. Verified by grep: `KafkaFuture::when_complete`
(line 271) has **zero** callers in any build configuration, so
`#[allow(dead_code)]` is required (dropping it would trip `#![deny(warnings)]`).

Resolution: kept `#[allow(dead_code)]` and rewrote the doc to state accurately
that both the FFI per-key path and `kafka_admin_client.rs` register on the
`KafkaFutureImpl` handle's `when_complete`, so this public-view method (mandated
by `admin-client.md` §4 as a `KafkaFuture.whenComplete` mirror) currently has no
in-tree caller and is retained as the public-mirror hook.

## Verification (in worktree, via --manifest-path)
- `cargo test --features ffi --lib`: 4233 passed, 0 failed.
- JAAS tests: 14 passed (incl. new regression test).
- non-ffi `cargo build`: clean (confirms `#[allow(dead_code)]` still needed/correct).
- `cargo xtask format-check`: clean.
- Python (pre-comment-change extension, comment change is inert): 199 admin unit
  + 154 soak + 11 targeted fix tests passed.
- CI-gated only: Docker multilang integration
  (`test_ml_admin_remove_all_members_from_consumer_group`, update_features
  integration) — not runnable in sandbox.
