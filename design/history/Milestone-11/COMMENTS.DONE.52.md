# Critic 52 — resolved comments

Both findings filed in `COMMENTS.52.md` are resolved. Verified after the fixes:
`cargo build`, `cargo test --lib` (**3518 passed, 0 failed, 3 ignored** — up two
from the 3516/3 baseline, the two net-new regression tests), `cargo xtask
format-check`, `cargo xtask lint` all clean.

---

## Issue 1 — RESOLVED (fixup `fc022821`)

`maybe_fail_with_error` never stamped `into_fatal()` on its fatal `else` branch,
so the app-facing txn API (`begin_*` / `send` / `commit` / `abort` /
`send_offsets_to_transaction`) and `send()` still returned `is_fatal() == false`
after a fatal fence/cluster-auth/poison failure — leaving the "retry a dead
producer forever" defect live on the dominant path.

**Fix (code was already applied uncommitted by the prior Actor session; verified
sound and committed):**

- `transaction_manager.rs::maybe_fail_with_error` fatal `else` branch now returns
  `error.into_fatal()`, disjoint from the abortable branch's
  `error.with_txn_requires_abort()`. The misleading "deliberately not stamped"
  comment was rewritten to state the disjoint fatal/abortable enrichment.
- `KafkaError::into_fatal` now **promotes** the payload-only `IllegalState`
  variant (which has no `KafkaGenericError` base to hold the flag) to a fatal
  wire-code `Generic(UnknownServerError)` error, message preserved. This is
  required because the KAFKA-14831 poison path (`transition_to`) stores an
  `IllegalState` `last_error` while moving to `State::FatalError`, which
  `maybe_fail_with_error` then surfaces from a fatal state. `with_message`
  produces a `Generic` variant, so the recursive `into_fatal` terminates at the
  `Generic` arm — no infinite recursion.

**Tests added/updated (the missing coverage this finding called for):**

- `kafka_producer.rs::test_send_after_fatal_error_returns_error` — added
  `assert!(error.is_fatal())` on the returned error. This is the production-path
  regression; it fails without the `into_fatal()` stamp.
- `transaction_manager.rs::test_maybe_fail_with_error_stamps_fatal_and_abortable_disjointly`
  — new direct test of `maybe_fail_with_error`'s output pinning disjointness on
  three surfaces: fatal state (`is_fatal()==true`, `!txn_requires_abort()`),
  abortable state (`txn_requires_abort()==true`, `!is_fatal()`), and the
  poison/`IllegalState`-in-fatal-state path (`is_fatal()==true`,
  `!txn_requires_abort()`). Reuses the `Caller::Sender` invalid-transition setup
  from `test_invalid_transition_poisons_only_on_the_sender_side`.
- `kafka_error.rs::into_fatal_stamps_and_promotes` — updated to assert the
  `IllegalState` promotion (`is_fatal()==true`, message preserved,
  `!txn_requires_abort()`) instead of the former pass-through-unchanged
  assertion.

All three new/updated assertions are discriminating: they fail if the one-line
`into_fatal()` in `maybe_fail_with_error` (and, for the poison case, the
`into_fatal` promotion arm) is reverted.

---

## Issue 2 — RESOLVED (fixup `87c17d6a`)

`await_ready` (`network_client_utils.rs`) returned `io::Result<(bool, Vec<ClientResponse>)>`,
so its two error early-returns (`connection_failed` → `ConnectionRefused`,
`authentication_error` → `PermissionDenied`) dropped the accumulated `responses`
`Vec`. A response for an unrelated in-flight request collected on a readiness
poll before the error was lost, hanging that batch to `delivery.timeout.ms`.
Java's `client.poll()` self-dispatches before it throws, so it loses nothing.

**Fix:**

- `await_ready` now returns `(Vec<ClientResponse>, io::Result<bool>)` — the `Vec`
  rides *outside* the `Result` on every exit (success, timeout, connection-failed,
  auth-failed). `is_ready` keeps its `(bool, Vec)` shape.
- `sender.rs::await_node_ready` dispatches the returned `Vec` through
  `self.handle_client_responses(&responses, now)` **before** the `?` propagates
  any error, mirroring Java's self-dispatch-before-throw. No `TransactionManager`
  guard is held across the await or the dispatch (producer-transactions.md §4).
- Callers audited: the only production caller of the free `await_ready` is
  `await_node_ready` (updated); the free `is_ready` is only called inside
  `await_ready` (shape unchanged); `network_client.rs:1884`'s 2-arg `await_ready`
  is a *separate* standalone test helper that does not wrap the free fn, so it
  and its ~18 call sites are unaffected.

**Test added:**

- `sender.rs::test_await_node_ready_dispatches_responses_polled_before_error_return`
  — puts an unrelated produce response in flight for node 0, backs off a
  *different* awaited node (`client_mut().backoff(..)`) so `await_ready` takes its
  `connection_failed` error return, and asserts the produce response was still
  routed (future resolves with offset 0, `pending_produce_responses` and
  `in_flight_batches` both drain) even though `await_node_ready` returns `Err`.
  Discriminating: without the fix the response is polled during readiness and then
  dropped on the error path, so the future hangs and both drain assertions fail.

**Auth-error return not directly tested (CLAUDE.md §5 disclosure):**
`MockClient::authentication_error` always returns `None`, so the harness cannot
drive the `PermissionDenied` early-return. The connection-failed return exercises
the identical shape — both now carry the `Vec` alongside the `io::Result` and are
dispatched by the same `await_node_ready` code before the `?` — so the auth path
is covered by construction. Noted in the test's doc comment.
