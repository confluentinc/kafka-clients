---
name: multilanguage-suite-on-macos
description: How to actually run make test-multilanguage on this arm64 macOS host (Linux artifacts, .dockerignore, buildkit syntax-frontend), and the WakeupTrigger prod bug the suite caught
metadata:
  type: project
---

The multilanguage suite (`make test-multilanguage`, 124 backend-fanned test
instances) was designed on a Linux host and had never been executed here. It
runs green now; this is what it takes and what it found.

**Host prerequisites this repo's Makefile cannot satisfy on macOS.** The three
Dockerfiles `COPY target/release/libconfluent_kafka.{so,a}` from the *host*, and
`make build-grpc-images` depends on `make build` (needs cmake, absent). On
macOS cargo produces Mach-O `.a`/`.dylib` and no `.so` at all, so the images
cannot be built from host artifacts. Working recipe:

1. Cross-build in a container, own target dir so the host tree is untouched:
   `docker run --rm -v "$PWD":/work -w /work -e CARGO_TARGET_DIR=/work/target/docker-linux rust:trixie bash -c 'cargo build --features ffi --release'`
   Use `bash -c`, **not** `bash -lc` — a login shell resets PATH and cargo
   disappears. Run it detached (`docker run -d --name ...`) so a session hiccup
   doesn't kill it. ~2 min warm, ~6 min cold (rustup fetches the pinned 1.95.0).
   `chown -R 501:20` the touched dirs afterwards: `build.rs` writes
   `target/include/confluent_kafka.h` at a fixed path regardless of
   `CARGO_TARGET_DIR`, so the container also rewrites the host header as root.
2. Copy the ELF `.so` and `.a` over `target/release/`, build the images, then
   **restore the Mach-O `.a`** (`cp` it aside first). Leaving the Linux `.a`
   there breaks host C-binding builds.
3. `# syntax=docker/dockerfile:1.7` cannot resolve on this host — Docker
   Desktop's Hub mirror serves `library/*` only, and a direct
   registry-1.docker.io lookup gets `no such host`. Build from a scratch copy of
   each Dockerfile with just that line stripped (`grep -v '^# syntax='`),
   `-f <scratch> <repo root>`; everything else stays byte-identical, and the
   repo-root `.dockerignore` still applies.

**The suite is fast once the images exist**: ~10 s for the 12 callback tests,
~20 s for all 124, ~50 s for the whole 227-test integration binary (broker
containers come from the pool; 6 brokers + 3 backends run concurrently).

**What it caught — a real prod bug no local test could see.** Phase 41b's
listener-ack poke called `network_thread_close.wakeup()`, which fires the
`WakeupTrigger` — the *user-facing* `Consumer::wakeup()` token — so every
rebalance callback left a wakeup pending and the caller's own `poll()` returned
`KafkaError::Wakeup("WakeupTrigger fired")`. It broke the new gRPC listener
tests *and* the entire pre-existing `plaintext_consumer_callback_test` suite.
Fix: `ApplicationEventHandler::wake_background_task()` (the application-event
`Notify`, which `run_once`'s `select!` also treats as a poll wake source, but
which the app never observes). Two lessons worth generalizing:

  - **"Wakes the bg loop" is not a single primitive.** There are two `Notify`
    paths into the network poll; only one is invisible to the app. When a rule
    says "poke the bg wakeup", check *which*.
  - The guarding unit test asserted only that *some* wakeup fn ran, so it passed
    either way. A test that asserts a side effect happened must also assert the
    side effect that must *not* happen — here "no wakeup is pending afterwards".
    Making the fixture's `wakeup_fn` production-faithful (it fires the trigger)
    is what turned that into a real guard; mutation-check confirmed it.

**Stale `#[ignore]` spotted, not fixed** (out of scope):
`plaintext_consumer_poll_test::test_async_consumer_max_poll_interval_ms_delay_in_revocation`
is ignored citing "Issue 8 … structurally unsupported in Rust without a
listener-side handle" — Phase 41 shipped `ConsumerHandle`, so that reason is
obsolete and the test should be revisited.

See [[ffi-callback-bridging-phase7]], [[integration-test-infra]],
[[phase41-consumer-handle]].
