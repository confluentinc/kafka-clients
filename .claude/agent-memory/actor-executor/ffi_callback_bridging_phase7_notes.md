---
name: ffi-callback-bridging-phase7
description: FFI callback-bridging Phase 7 — multilanguage-tests feature had silently rotted, C++ server code can be runtime-verified without grpc++ via extract-and-link, xtask lint never sees the integration test binaries
metadata:
  type: project
---

Phase 7 (final) of the FFI/Python callback-bridging plan
(`~/.claude/plans/wiggly-bouncing-babbage.md`) landed as `8de4dce` on
`ffi-callback-bridging`, plus a lint-cleanup follow-up `76b4d10`.

**How to apply** — the process findings matter more than the feature:

1. **`--features multilanguage-tests` had not compiled for several milestones
   and nothing noticed.** `tests/common/multilanguage_consumer.rs` still
   implemented the pre-Phase-41 `Consumer` trait: it imported the deleted
   `WakeupHandle`, implemented the removed `wakeup_handle`, and was missing the
   added `metrics` / `handle`. Neither `cargo xtask lint`, `format-check`, nor
   `cargo test` (no features) touches that file. **Compile every feature combo
   you intend to touch at the START of a phase** — this is the Phase-1 lesson
   recurring, and here it also *masked* 24 clippy errors, because clippy can't
   lint a crate that doesn't type-check.

2. **`cargo xtask lint` runs clippy WITHOUT `--features`**, so `src/ffi/`
   (known since Phase 1) *and both integration test binaries* are unlinted. Run
   all three separately for any change in those areas:
   `cargo clippy --all-targets --features ffi -- -D warnings`
   `cargo clippy --all-targets --features integration-tests,multilanguage-tests -- -D warnings`

3. **A C++ file needing grpc++ CAN still be runtime-verified here.** cmake,
   protoc and grpc++ are all absent and must not be half-installed, but the
   *risky* part of `server.cc` is its FFI usage, not its protobuf usage. Working
   recipe: extract the new blocks **verbatim from the real file with a script**
   (never retype — the checked text must be the shipped text), `#include` them
   into a scratch TU alongside ~60 lines of value-storing protobuf stand-ins
   whose method names match the generated API, and link against the real
   `target/release/libconfluent_kafka.a`. That gave a running mirror of the
   Python smoke test — real MockProducer/MockConsumer, real listener + commit +
   delivery trampolines — and **clean under `-fsanitize=address`**, which is the
   only way to actually prove the "callee owns and destroys every delivered
   handle" discipline. Add `-Wno-extern-c-compat`; the header emits ~13
   zero-sized-struct warnings that are pre-existing noise.

4. **Driving a Python gRPC servicer directly (no transport, no Docker) is a
   complete functional test of a handler.** `ProducerService().Send(request,
   None)` works — the `context` argument is unused by these handlers, and an
   empty-config `CreateX` selects the Mock client. That verified all six new
   handlers in both servers, including negative cases (`with_callback=False`
   logs nothing; a listener-less re-`Subscribe` releases the registration).
   Generate the stubs into the scratchpad, not `bindings/python/`, and put that
   dir on `PYTHONPATH` — the servers `sys.path.insert(0, __file__'s dir)`, so
   both are found.

5. **Proto layout constraint worth remembering:** `consumer_service.proto`
   imports `producer_service.proto`, so shared messages must live in the
   *producer* file (the base) or there is an import cycle. But `TopicPartition`
   is declared in the consumer file, so a shared entry type cannot reference it
   — hence a small duplicate `CallbackLogPartition`. Moving `TopicPartition`
   down would relocate it between the two generated **Python** stub modules
   (`cpb.TopicPartition` -> `pb.TopicPartition`) and break every existing
   server reference, for zero wire-format gain. Rust is immune (prost puts a
   whole package in one module), so the constraint is Python-only.

6. **`Box<dyn Consumer>` erases everything a per-backend side-channel needs.**
   The consumer factory returns a boxed dyn, so there is nowhere to hang the
   native backend's `Arc<Mutex<log>>` or the gRPC backend's `(channel,
   consumer_id)`. Solution that fit cleanly: a second constructor
   `create_with_callback_log(config) -> (Box<dyn Consumer>, ConsumerCallbackLog)`
   returning both, with the log an enum whose gRPC arm issues its *own* RPCs
   against the same server-side id. The `Consumer` trait's callback methods
   stay `unsupported` on the gRPC backend — correctly, since an in-process
   `dyn` object cannot cross a process boundary.

7. **`unsubscribe()` fires `on_partitions_lost`, not `on_partitions_revoked`**
   (§31). To test a *revoke* end-to-end, change the subscription
   (`subscribe([a])` -> `subscribe([b])`) and re-register the listener, because
   a replacing `subscribe*` releases the previous registration (Phase 5/6
   finding) and it is the listener registered *at revocation time* that runs.

8. `clippy::doc_lazy_continuation` on a plain paragraph usually means a wrapped
   line began with `+ ` (or `- `/`* `): Markdown reads it as a list marker and
   every following line becomes a lazy continuation. Reflow so the operator is
   not at line start — do NOT "fix" it by indenting the following lines.

Environment recap (all still true): no cmake / grpc++ / protoc; compile the
Unity C suites directly against `target/release/libconfluent_kafka.a` (87 tests
across 5 files, all green); pytest works via a scratchpad venv with
`--index-url https://pypi.org/simple` **and** the local `threads.h` shim for
building the extension (the prebuilt `_confluentkafka.cpython-314-darwin.so`
from Phase 6 was still usable, so no rebuild was needed); Docker is
**unavailable**, so `make test-multilanguage` could not run — the three new
tests compile but have never executed.

See [[ffi-callback-bridging-phase1]], [[ffi-callback-bridging-phase5]],
[[ffi-callback-bridging-phase6]].
