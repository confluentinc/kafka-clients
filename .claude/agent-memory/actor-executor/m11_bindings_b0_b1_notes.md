---
name: m11-bindings-b0-b1-notes
description: M11 admin bindings B0+B1 — no access guard, finish_sync opaque-type trap, cbindgen skips module docs, ffi-feature archive clobbering, Python ext unbuildable on macOS, deadline-clamp omissions and clock-advancing test client
metadata:
  type: project
---

Admin C FFI + Python bindings, slices B0 (foundation) + B1 (topic RPCs), landed
on `dev/admin-bindings`. Non-obvious findings worth carrying forward.

**Why:** the remaining slices B2–B6 repeat this exact shape 40+ more times, and
several of these traps are silent (a leak, a panic across FFI) rather than
compile errors.

**How to apply:** read before starting any further admin binding slice.

## Admin FFI is deliberately *simpler* than the consumer FFI

`Admin` is `Send + Sync` with `&self` methods, so `src/ffi/admin.rs` has no
`UnsafeCell`, no `owner: AtomicU64` guard, no `acquire`/`release`, no `wakeup()`
abort path — and needs **no** `unsafe impl Send/Sync for AdminHandle`, because
the spawned task captures a `&'static dyn Admin` (`Send`, since `Admin: Sync`)
rather than the handle. Do not copy the consumer's guard machinery.

## The trap that cost real debugging: freeing an opaque handle generically

A generic helper that takes `*mut R` where `R` is the `#[repr(C)] { _private:
[u8;0] }` marker type **cannot** free it — `Box::from_raw(handle)` drops a ZST,
leaking the inner state and deallocating with the wrong layout. The fix was to
never build the handle when the caller passes a NULL out-param. Any future
"free it if the caller doesn't want it" branch needs the concrete `*_destroy`
fn passed in, or must not allocate at all. A test that just passes NULL and
checks the return value will pass either way — the bug is silent.

## Rust-core panics reachable from the FFI

`MockAdminClient::create(0)` succeeds but its `create_topics` then panics on
`state.brokers[0]`. Java throws from `Builder.build()` instead. Before exposing
any `MockAdminClient` inherent method through the FFI, check it for
`panic!`/`assert!`/indexing — `add_topic` and `mark_topic_for_deletion` both
panic (duplicate / missing topic) and were deliberately left unexposed for that
reason. Translate Java's throw into a NULL/error return; never let a panic
unwind into C.

## Per-key results across C

`KafkaFuture::join_map` short-circuits, so `join_map_results` was added.
`CreateTopicsResult` needed a `pub(crate) futures()` accessor — its public
`values()` maps the per-key future to `KafkaFuture<Void>` and cannot carry
`TopicMetadataAndConfig`. Expect the same for any `*Result` whose public
accessors erase the payload. Feature-gate such accessors with
`#[cfg_attr(not(feature = "ffi"), allow(dead_code))]`: the crate is
`#![deny(warnings)]` and plain `cargo build` / `cargo test` run without `ffi`.

Borrowed per-key errors (`*const kafka_common_KafkaError_t` into the result's
`Vec<Option<KafkaErrorInner>>`) die with the parent handle. The Python drain
must copy code/message/retriable/fatal out **before** destroying it — hence
`KafkaError._from_parts` in `producer.py`; `_from_c` would double-free because
it destroys the handle it is given.

Result entries are sorted by key so C's index addressing is reproducible.

## cbindgen does NOT copy module-level rustdoc into the header

Only per-item rustdoc reaches `target/include/confluent_kafka.h`. So a C-visible
doc comment must never say "see the module-level *X* section" — that reference
dangles for the only audience that reads the header. State the whole contract on
every entry point, even at the cost of repeating it 9 times. Verify with
`grep -c "<the sentence>" target/include/confluent_kafka.h` after `cargo build
--features ffi`.

## Environment gotchas

- `cargo xtask lint` does **not** enable the `ffi` feature, so the FFI modules
  escape the standard lint gate. Run
  `cargo clippy --all-targets --features ffi -- -D warnings` separately.
  `clippy`/`rustfmt` need nix: `nix shell nixpkgs#clippy nixpkgs#rustc
  nixpkgs#cargo --command ...` / `nix shell nixpkgs#rustfmt --command ...`.
- **Plain `cargo build` / `cargo test` overwrite `target/debug/
  libconfluent_kafka.a` without the FFI symbols**, and `bindings/c/build`'s
  CMake cache pins that exact path. The C link then fails with a wall of
  "Undefined symbols for architecture arm64" naming every `kafka_*` function —
  which looks like a broken FFI but is a stale archive. Always re-run
  `cargo build --features ffi` immediately before `cmake --build`.
- `cargo xtask check-generated` fails on a **clean** tree (unformatted files
  under `target/debug/build/*/out/generated`), so it is not a usable gate; the
  four real gates are build (both feature settings), test, `format-check`,
  clippy-with-ffi. Confirmed by stashing all changes and re-running.
- `_confluentkafka.c` includes `<threads.h>`, which the macOS SDK does not have,
  so **the Python extension cannot be built on this host at all**. Run the suite
  in Linux with this recipe (verified working in B2; an earlier
  copy-into-the-image variant wedged Docker Desktop, so prefer this one):

      docker run --rm -v "$PWD":/work -w /work -e CARGO_TARGET_DIR=/tmp/tl \
        ckr-pytest:dev bash -c '
          cd /work && cargo build --features ffi --release &&
          CONFLUENT_KAFKA_LIB_DIR=/tmp/tl/release sh -c "cd bindings/python &&
          /venv/bin/python -m pip install --quiet . &&
          /venv/bin/python -m pytest test/unit/test_admin.py -q --no-header \
            -p no:cacheprovider"'

  `CARGO_TARGET_DIR` **must** point outside the repo, or ~600 MB of Linux
  artifacts land in the working tree. Do not `docker pull` — the credential
  helper is broken; `ckr-pytest:dev` is already local and carries `/venv`
  (pytest 9.1.1) plus python3-dev.
- Host `pip` points at an authenticated CodeArtifact index that 401s; use
  `--index-url https://pypi.org/simple` or build inside Docker.
- `cmake` is not on PATH; use `nix shell nixpkgs#cmake --command cmake ...`.
  `bindings/c/tests/unity` needs `git submodule update --init` first.
- cbindgen emitted the new admin types even before they were added to
  `cbindgen.toml`'s `[export] include` (they are reachable from exported
  functions), but they were added anyway per the plan's checklist.

## Broker-less production-client tests earn their keep

The mock-only admin suites cannot see anything about the real client's I/O task.
Adding `test_kafka_admin.c` (no broker needed — construction only parses config,
and every RPC carries a short explicit timeout) immediately exposed a shutdown
hang in `AdminClientRunnable::should_exit`. When a C test hangs, `sample <pid>`
on macOS names the exact blocked frame and is much faster than bisecting.

Java gates shutdown on `hasActiveExternalCalls()`, which **skips `Call`s with
`internal == true`** — a filter that is easy to drop when translating, and whose
absence only shows up against an unreachable broker. Suspect the same class of
omission in any predicate that decides "is there still work outstanding".

## A deadline needs translating twice: into the exit predicate *and* into the wait

`processRequests` clamps `pollTimeout` to `curHardShutdownTimeMs - now`
(`KafkaAdminClient.java:1500-1502`) *and* consults the deadline in
`threadShouldExit`. Only the predicate had been translated, so the loop reached
its exit check no sooner than the poll returned — `close(100ms)` blocked up to
`request.timeout.ms`. When a Java loop mentions a deadline in more than one
place, translate every one; a shutdown/timeout budget honoured only by the
predicate is honoured only after the blocking wait it was supposed to bound.

## Making "how long would the loop have blocked" assertable under a mock clock

`MockClient::poll` ignores its timeout, so no assertion on elapsed time is
possible and predicate-level tests cannot see a missing clamp. The fix is a
test-only `KafkaClient` wrapper (`WaitingClient` in `kafka_admin_client.rs`,
same shape as the consumer's `CountingClient`) that records each poll timeout
and, behind an `AtomicBool` armed after setup, advances the `MockTime` by it —
a poll that finds an idle socket and waits its whole budget. The wait then shows
up on the mock clock: deterministic, instant, no real sleeping, and it fails
loudly with the offending timeout in the message. Arm the flag only after the
setup pumps, or the setup's own polls skew the clock. Recording the argument is
the Rust stand-in for Mockito's `verify(client).poll(captor.capture(), ...)` and
is the DoD #7 justification for the wrapper.

## Behaviour verified against Java, not assumed

`MockAdminClient.createTopics` (MockAdminClient.java:363-422 at kafka
`a18251bae0b8`) **ignores its `options` argument entirely and ignores
`replicasAssignments`**, substituting `defaultPartitions` /
`defaultReplicationFactor` for -1. Two tests initially asserted the intuitive
behaviour and failed; the Rust mock was right. Fetch the Java file before
asserting mock behaviour.
