---
name: critic3-wakeup-and-logstate-round
description: Critic-3 fix round on ffi-callback-bridging — the two-primitive wakeup trap generalized, why a fixture must not hand-roll a production closure, and the detached-FFI-dispatcher lifetime rule for C test servers
metadata:
  type: project
---

Actor 3 fixed all six Critic-3 comments on `ffi-callback-bridging`
(`af60796` for the four core wakeup issues, `bdb8b77` for the harness ones).
Three lessons generalize past this round.

**1. "Wake the bg loop" has TWO primitives and the wrong one is silently inert,
not merely noisy.** The already-recorded half (see
[[multilanguage-suite-on-macos]]) was that firing the `WakeupTrigger` internally
leaks a spurious `KafkaError::Wakeup` into the app's own `poll()`. The other half
is worse and was live on the close path: `close_internal` step 1 calls
`wakeup_trigger.disable()`, after which `WakeupTrigger::wakeup()` returns early
without cancelling the token — so *every* trigger-based wake after that point is
a no-op. `close()` therefore waited out the in-flight network poll every time.
Measured: the 72 native `plaintext_consumer*` integration tests run in 12.59 s
fixed vs 20.32 s with the pre-fix wake, because `close()` is in every teardown.
Rule of thumb: an internal wake is the application-event `Notify`; the
`WakeupTrigger` belongs to `Consumer::wakeup()` and to nothing else.

**2. A test fixture must not hand-roll a production closure — extract a shared
builder instead.** This is the actual reason the bug survived. `spawn_dedicated_bg`
built its own `wakeup_fn` that poked the loop's `Notify` while production fired
the disabled trigger, so the test proved a wake path production did not have.
The fix was `build_network_thread_close_fns(running, event_notify)` called by
BOTH the ctor and the fixture (the instrumented fixtures *wrap* it rather than
replace it), which makes the divergence unrepresentable. Two further traps found
while doing it:
  - A fixture whose loop is `while running { park }` can observe
    `running == false` on its *first* `while` check if the test calls
    `signal_close()` before the thread has parked — so it passes with a
    completely inert wake. Any promptness test must first wait for a
    "loop has parked" flag.
  - The only client whose `poll()` actually blocks is
    `CountingClient` (`consumer_network_thread.rs` tests, `poll_block` +
    `wakeup_handle()` returning the very `Notify` the poll awaits). `MockClient`'s
    poll returns immediately, so a promptness test built on it proves nothing.

**3. C/C++ test servers must treat FFI callback `user_data` as
session-lifetime.** `kafka_{producer,consumer}_*_destroy` **detach** the
dispatcher thread rather than joining it, and the Rust-side callback only
*enqueues* the C callback as a dispatcher job. So "destroy returned, therefore no
callback can reference my state" is false, and `server.cc` freeing its `LogState`
in `Close` was a use-after-free (latent only because every test read the log
before closing). Corollary for anything read back after `close`: a post-close
`GetCallbackLog` on the `c` backend is *eventually* consistent, while the two
Python servers append synchronously in-process — assert with a polling helper,
never a single read.

**Test-strength pattern worth reusing:** `wait_for_kind` (return as soon as one
matching entry exists) can never support an `== 1` assertion — the snapshot is
taken at the earliest moment one entry exists, so a double-fire is sampled
between the two appends. Added
`ProducerCallbackLog::wait_for_kind_settled(kind, deadline, grace)`; any
count assertion needs the settle window.

Environment delta from [[ffi-callback-bridging-phase7]]: **Docker IS available
now**, so `make test-multilanguage` runs. The macOS recipe in
[[multilanguage-suite-on-macos]] still applies verbatim, with one simplification
observed this round — the container's `target/docker-linux` build did **not**
rewrite `target/include/confluent_kafka.h` as root, and the cross-built files
came out owned by the host user, so no `chown` was needed. Still back up the
Mach-O `target/release/libconfluent_kafka.a` before copying the ELF one over it,
and restore it afterwards (the host C suites link against it).

See [[ffi-callback-bridging-phase1]], [[multilanguage-suite-on-macos]],
[[phase41-consumer-handle]].
