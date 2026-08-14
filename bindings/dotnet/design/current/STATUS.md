# .NET binding — living status

Binding-local status for `bindings/dotnet/`. The .NET binding keeps its own
milestone/phase numbering, independent of the repo-root Rust `design/`.

## Current milestone/phase

Newest first.

- **Milestone 11 / Phase 3 — ".NET producer SEND path (Phase C)": DONE (2026-08-14). N=30. Mode A** (no `src/**`/`src/ffi/**`/`confluent_kafka.h`/`cbindgen.toml`/Rust-core change — verified: the P3 commits touch only `bindings/dotnet/` (`src`/`tests`/`.claude/rules`/`design`), and `cargo build --features ffi` shows no header delta). "Group C" of the producer roadmap — the **first public `Send`** (A foundation → B peripherals → **C send** → D sync producer). Plan: `design/history/M11/P3-producer-send/PLAN.md`. Branch: **`prashah_dev_producer_send`** (stacks on `prashah_dev_producer_async_peripherals`). Commits `23c3ec66` (feature) · `2560bdc7` (**`fixup!`** — Critic round-1 fixes) · `14d83dc8` (**`fixup!`** — async-flush teardown refinement) · a 4th **`fixup!`** (user-approved sync→`SafeHandle`-param convention adoption — see the post-close refinement below); all fixups autosquash into `23c3ec66` at PR finalize. Delivered:
  - **Completion model — Option C (inline pull-pump), decided in the PLAN §3 A/B/C analysis.** `Producer_send` singular, called **inline** on the caller with a **call-scoped `fixed` pin** (the core copies key/value synchronously during the call, `src/ffi/producer.rs` L262-281 `rt.block_on(producer.send(...))` — so B's borrow-until-callback long pin is avoided); a **single pump thread** (`SendCompletionPump`) draining a batched `FutureRecordMetadata_get_all`; an **unbounded `ConcurrentQueue<(future, TCS)>` + signal** completion queue (in-box, no `System.Threading.Channels` dep), practically bounded by the core's `buffer.memory` backpressure; every TCS `RunContinuationsAsynchronously`; `destroy_all` after reading. No `ProducerRecord_t` mirror struct (that was Option A / `send_batch`), no managed accumulator bound.
  - **Public surface (additive to P2, clipped to today's ABI):** `IAsyncProducer.Send(ProducerRecord, CancellationToken = default) → Task<RecordMetadata>` (grown additively like `IConsumer` did P8a→P8b); `ProducerRecord` value type (`Topic`/`Partition?`/`Timestamp?`/`Key`/`Value` as `ReadOnlyMemory<byte>?`, ctor `(topic, value, key=null, partition=null, timestamp=null)` — **no `Headers`**); `RecordMetadata` value type (`Topic`/`Partition`/`Offset`/`Timestamp` — **no** serialized-size/`has*`); `AsyncMockProducer` send-control helpers **inherent on the concrete type** (`bool CompleteNext()`, `bool ErrorNext(int, string?)`, `int HistoryCount`, `void Clear()`). Bytes-only interim; typed generic producer still deferred.
  - **Internal:** `NativeProducer.Send` (the worker — **named `Send`, not `SendWithCallback`**: Option C has no callback, PLAN §6.2), passing the `SafeProducerHandle` straight to the `Producer_send` P/Invoke so the marshaler auto-refs it call-scoped (destroy-vs-in-flight-send UAF safety — the sync→`SafeHandle`-param convention, see the post-close refinement below); `SendCompletionPump` (the one pump + MPSC queue); `Internal/Interop/ProducerSendMarshal.cs` (the only new `unsafe` — call-scoped `fixed` pin + §A4 sentinels: absent → `IntPtr.Zero`/`-1`, empty → non-null stack sentinel/`0`, present → ptr/len); `RecordMetadataMarshal.cs` (per-field copy-out); `NativeMethods` send/`get_all`/`destroy_all`/`RecordMetadata_*`/`MockProducer_*` + sync `ProducerFlush` DllImports (all wired — no dead imports; `is_done`/`RecordMetadata_copy` deliberately omitted as they'd be unused).
  - **Teardown (flush-before-join, folded into the P2.1 single-layer shape):** the pump's blocking `get_all` is unblocked by a **flush** (not `close` — the read-only core check found `MockProducer::close()` only sets `closed=true`, but `flush()` drains+completes pending sends sharing the future's `Arc<ProduceRequestResult>`). `StopPump` split into a shared `PumpToStop` reader + sync `StopPump()` (`Dispose` → sync `Producer_flush`) + `StopPumpAsync()` (`DisposeAsync`/`CloseWithCallback` → `await Producer_flush_async` via a new latch-free `FlushInternal`, twin of `CloseWithCallbackInternal`, no new DllImport). Flush resolves pending sends → `get_all` returns → the pump `_thread.Join()` can't hang; join + `Producer_destroy` stay blocking by design (making them awaitable is out of scope).
  - **Recorded deviations (Critic-validated sound):** (1) null-topic/negative-partition preconditions live in the **`ProducerRecord` ctor** (Java-faithful; record is immutable so the guard holds through `Send`); (2) teardown **completes** a `MockProducer(autoComplete:false)` uncompleted in-flight send (pull-pump-forced — the pump must resolve every future or the join hangs; Java-faithful for the *real* producer, whose `close()` flushes) — cross-referenced in the archived `PLAN.md` §2 parity anchor for Phase-D discoverability.
  - **Critic N=30:** round 1 — 1 MEDIUM (teardown join-before-flush → manual-mock `Dispose` hang) + 1 LOW (`Send` missing span-the-op ref → destroy-vs-send UAF), both RESOLVED in `2560bdc7`; round 2 — **CLEAN**; round 3 (async-flush refinement `14d83dc8`) — **CLEAN**. Closed record: `design/history/M11/P3-producer-send/COMMENTS.DONE.30.md`.
  - **Deferred/noted (not a P3 defect):** a round-2 low-confidence observation — on the `Enqueue`-races-`Stop` path a stalled send could call `FutureRecordMetadata_destroy_all` after `Producer_destroy`; whether that is a UAF depends on Rust-core `FutureRecordMetadata` lifetime (outside dotnet-critic scope; the churn test passes empirically). Pre-existing, not a fixup regression; note-and-deferred (not routed to kafka-critic).
  - **Post-close refinement (user-approved, round-3 review clean; the 4th `fixup!`):** adopted the **sync-native-call → `SafeHandle`-param** convention, starting with `Send`. `NativeMethods.ProducerSend`'s `producer` param changed `IntPtr` → `SafeProducerHandle` (single caller, confirmed), so the P/Invoke marshaler auto-`DangerousAddRef`/`Release`s it around the synchronous `Producer_send` — replacing the manual span-the-call `DangerousAddRef` from `2560bdc7`. The manual ref in `NativeProducer.Send` is dropped; `ProducerSendMarshal.Send` threads the `SafeProducerHandle` through (the `fixed` key/value pins stay inside that `unsafe` helper). Post-`Dispose` `Send` still throws `ObjectDisposedException` (via `ThrowIfClosed` + SafeHandle-marshal-on-closed) and the concurrent-`Send`-vs-`Dispose` churn test still passes. The principled split is documented in `ffi-marshalling.md §A2`: **sync = `SafeHandle`-param (auto ref, call-scoped) / async `*_async` = manual `DangerousAddRef` held submit→callback** (the auto ref releases before an async completion callback fires, so async can never use it). **Follow-up (tracked):** migrate the consumer **sync-op** DllImports (`ConsumerSeek` / `ConsumerSeekWithMetadata` / `ConsumerCurrentLag` / `ConsumerEnforceRebalance` / `ConsumerCommitAsync` / the sync state reads `ConsumerAssignment`/`Subscription`/`Paused` + the sync poll/commit/query family) to a `SafeConsumerHandle` parameter to match, leaving the consumer `*_async` ops on manual `DangerousAddRef`.
  - **Verification:** `dotnet build` 0W/0E across net462/net8.0/net10.0; `dotnet test -f net10.0` **507 passed / 0 failed** (net8.0/net462 build-verified); `dotnet format --verify-no-changes` clean; Mode A `git diff --stat` clean over ABI/ffi/header/cbindgen, no header-hash change.

- **Milestone 11 / Phase 2.1 — "Collapse producer teardown into `NativeProducer` + drop `Close(TimeSpan)`": DONE (2026-08-12). N=29. Mode A** (no `src/**`/`src/ffi/**`/`confluent_kafka.h`/Rust-core change — verified `git diff --stat prashah_dev_producer_foundation..HEAD` empty over those paths). A behavior-preserving teardown refactor **plus** a deliberate small public-API reduction. Plan: `design/history/M11/P2.1-collapse-producer-teardown/PLAN.md`. Branch: **`prashah_dev_producer_async_peripherals`** (stacks on the M11/P2 commits; the pending `fixup! 2d97172e` left untouched). Commit `84647361`. Delivered:
  - **(A) Teardown collapse (behavior-preserving):** moved the 3 kept teardown flavors into `NativeProducer` (mirror `NativeConsumer`), merging the wrappers' `_closed`/`TryBeginClose` latch + P1's `_disposed` guard into ONE `NativeConsumer`-style latch: `Dispose()` (sync `Producer_close`→destroy, swallow), `DisposeAsync()` (`Producer_close_async`→destroy, swallow, primary), `Close(ct)` (`Producer_close_async`→destroy, **surface**). Kept the span-the-op `SafeHandle` ref (destroy-while-close-in-flight UAF safety). Deleted `Internal/ProducerTeardown.cs`; `AsyncKafkaProducer`/`AsyncMockProducer` are now thin forwarders (`_native.Close(ct)`/`.Dispose()`/`.DisposeAsync()`) — the consumer's one-layer shape. **This is the sanctioned evolution of the M11/P1 `NativeProducer.Dispose` pin** (the user lifted the "don't touch the pin" constraint).
  - **(B) Removed `Close(TimeSpan)` (Python-producer parity):** Python's producer close has no timeout param (unlike the consumer's), so the timed-close overload + its entire `.NET`-side timer machinery (`Task.WhenAny`/`Task.Delay`/linked CTS) were removed. Kept `Close(CancellationToken = default)`. `IAsyncProducer` = `Flush`/`Close(ct)`/`PartitionsFor` (still no `Send` — phase C). Safe reduction: `Close(TimeSpan)` only shipped in M11/P2 on this unmerged branch.
  - **Micro-deviations (Critic-verified behavior-neutral):** dropped `ThrowIfClosed`/`RegisterCancellation(None)` from the relocated async-close bridge (necessary + no-op under the merged latch); `Close(ct)` throws on a pre-canceled token BEFORE taking the latch (so a later real close still destroys). Sanctioned consequence: a concurrent op during an in-flight close now throws `ObjectDisposedException` (matches the `NativeConsumer` precedent; unreachable under single-owner; more-correct).
  - **Critic N=29: CLEAN — 0 issues** (3 kept flavors byte-for-byte parity vs the pre-collapse bodies; merged latch idempotency + `ObjectDisposedException`; `Close(TimeSpan)`/timer fully gone; only the 4 `Close(TimeSpan)` tests deleted, all other teardown tests unchanged; consumer suite unaffected; Mode A).
  - **Verification:** `dotnet build` 0W/0E across net462/net8.0/net10.0; `dotnet test -f net10.0` 466 passed / 0 failed (470 − 4 deleted); `dotnet format` clean.

- **Milestone 11 / Phase 2 — ".NET producer async PERIPHERALS (`Flush` / `Close` / `PartitionsFor`)": DONE (2026-08-12). N=28. Mode A** (no `src/**`/`src/ffi/**`/`confluent_kafka.h`/Rust-core change — verified `git diff --stat prashah_dev_producer_foundation..HEAD` empty over those paths; 11 files, all under `bindings/dotnet/src/**` + `tests/**`). "Group B" of the producer roadmap — the **first public producer API**, but peripherals ONLY (`Send` deferred to phase C; `IAsyncProducer` grows additively like `IConsumer` did M5/P8a→P8b). All over the **push** `_async`→`OperationCompletionSource` bridge, reusing the consumer's §B7 machinery + `PartitionInfoListMarshal`/`PartitionInfo`/`Node` (no new bridge/value types). Plan: `design/history/M11/P2-producer-async-peripherals/PLAN.md`. Branch: **`prashah_dev_producer_async_peripherals`** (off `prashah_dev_producer_foundation`). Commits `b122a423` (Flush/Close/PartitionsFor) · `50b1ea98` (broker-free tests) · `2d97172e` (**`fixup!` for `50b1ea98`** — precondition-assertion strengthening; autosquash at PR finalize). Delivered:
  - **Public trio** `IAsyncProducer` (partial — `Flush`/`Close`/`Close(TimeSpan)`/`PartitionsFor` + `IDisposable`/`IAsyncDisposable`, **no `Send`**), `AsyncKafkaProducer`, `AsyncMockProducer`. Interop: producer async DllImports + `ProducerCallbacks` delegates; `NativeProducer.FlushWithCallback`/`CloseWithCallback`/`PartitionsForWithCallback`.
  - **Dispose upgrade (Decision 1, LOCKED YES):** layered in the public `AsyncKafkaProducer` via `ProducerTeardown` — `DisposeAsync` → graceful `Producer_close_async`→then destroy; `Dispose` → sync `Producer_close`→then destroy (a close error never skips the destroy; span-the-op ref makes destroy-while-close-in-flight UAF-safe; `Producer_destroy`-on-dispatcher is deadlock-safe because the core detaches the dispatcher). **P1's `NativeProducer` `Producer_destroy`-only teardown stays literally unchanged** (byte-for-byte) — the graceful close sits above it (consumer-wrapper shape).
  - **Shared-bridge change is purely additive:** `OperationCompletionSource.cs` gains only a new `internal void CancelAwaiter()` (no existing line removed/modified); the **consumer never calls it** (still wires cancellation to native `Wakeup()`) — full consumer suite stayed green.
  - **Honest caveats (documented, not papered over):** `PartitionsFor` on the FFI mock = **success + EMPTY list** (the mock ctor builds an empty cluster; a populated list is integration-only — no mock-seeding FFI ctor added, that's Mode B); cancellation cancels only the **.NET-side wait** (producer has **no `wakeup()`** → no native abort; the native op runs to completion and frees its own rooting); `Close(TimeSpan)` = **.NET-side deadline** (no `Producer_close_with_timeout` ABI).
  - **DEFERRED (phase C):** `Send`, `ProducerRecord`/`RecordMetadata`, the `ProducerRecord_t` mirror, the send pull-pump, the mock send-control helpers (`CompleteNext`/`ErrorNext`/`HistoryCount`/`Clear`), and the **ffi §A7 pull-vs-push decision** (P2 is entirely push).
  - **Critic N=28: 1 LOW test-coverage issue** (precondition tests asserted only exception type, not `ParamName`/message) → fixed in `2d97172e` (assert `ParamName=="timeout"`+"Timeout must not be negative." and `ParamName=="topic"`; +1 guard-ordering test); post-dispose tests left type-only (matches the consumer norm — zero `ObjectName` asserts repo-wide). Re-review CLEAN.
  - **Verification:** `dotnet build` 0W/0E across net462/net8.0/net10.0; `dotnet test -f net10.0` 470 passed / 0 failed (incl. full consumer suite green — the additive bridge change is consumer-safe); `dotnet format` clean; Mode A `git diff --stat` clean.

- **Milestone 11 / Phase 1 — ".NET Producer FOUNDATION (interop + lifecycle scaffolding only)": DONE (2026-08-12). N=27. Mode A** (no `src/**`/`src/ffi/**`/`confluent_kafka.h`/Rust-core change — verified `git diff --stat prashah_dev_dotnet_semaphore..HEAD` empty over those paths; the diff is 7 files, all under `bindings/dotnet/src/**` + `tests/**`). First phase of the **producer** milestone — internal scaffolding ONLY, **no public producer API** (mirrors the consumer's M1 interop + M2 SafeHandle/error scaffolding). The producer C ABI is fully present (106 `kafka_producer_*` symbols); later phases are additive Mode-A ports. Plan: `design/history/M11/P1-producer-foundation/PLAN.md`. Branch: **`prashah_dev_producer_foundation`** (off `prashah_dev_dotnet_semaphore`). Commits `2cb05dad` (interop + lifecycle) · `55a26428` (broker-free tests). Delivered:
  - **Interop scaffolding** — `SafeProducerHandle` + `SafeProducerPropertiesHandle` (`: SafeHandleZeroIsInvalid`, private ctor, `ReleaseHandle → Producer_destroy` / `ProducerProperties_destroy`, mirroring the consumer SafeHandles); `NativeMethods` producer section (the construct+lifecycle DllImport subset — `KafkaProducer_new` returning the SafeHandle + `out IntPtr outError`, `MockProducer_new(bool autoComplete)` with `[MarshalAs(I1)]`, `ProducerProperties_new`/`_put`/`_destroy`, `Producer_destroy`; construct fns return the SafeHandle directly per the M2/P2 no-gap pattern). Config `IReadOnlyDictionary<string,string>` → props `new`→`put`(UTF-8)→ `KafkaProducer_new` → **props freed after `new`** (ffi §A2 caller-frees-separately, in a `finally` — freed once on success AND every throw path, no double-free since the typed SafeHandle param is only AddRef/Release'd).
  - **`internal NativeProducer` lifecycle** — real (`KafkaProducer_new`, `out_error`→flat `KafkaException` §A5) + mock (`MockProducer_new`) construction; **`Dispose`/`DisposeAsync` = pinned §3.2 sequence: `Producer_destroy` ONLY** via the SafeHandle (no graceful `Producer_close`, no flush, no pump-join — all deferred to the send/flush phases; `Producer_destroy` blocks+joins the Sender per §A2, the minimal-correct subset). Idempotent (atomic latch), use-after-dispose → `ObjectDisposedException`, no finalizer reliance.
  - **DEFERRED (explicitly not in P1):** the send/peripheral DllImports (`Producer_send`, `FutureRecordMetadata_*`, `RecordMetadata_*`, `Producer_flush_async`/`_close_async`/`_partitions_for_async`), the `ProducerRecord_t` mirror, `ProducerRecord`/`RecordMetadata` value types, any public API, and the **ffi §A7 pull-vs-push send-completion decision** (quarantined to the later send phase). Build order: A foundation (this) → B async peripherals (Flush/Close/PartitionsFor over the push §B7 bridge) → C send (pull-pump; §A7 decided there) → D sync producer.
  - **Decisions/findings:** D1 — atomic `Interlocked` disposed latch (matches `NativeConsumer`; safer than a plain `bool`; teardown is exactly the pinned `Producer_destroy`-only subset). D2 — added two broker-free operational-failure tests (message-asserted per §A5/DoD §3) + the finding that a real `KafkaProducer_new` does NOT construct broker-free from an empty config (needs resolvable `bootstrap.servers`), unlike the consumer.
  - **Critic N=27: CLEAN — 0 issues** (props-ownership-on-throw-path, SafeHandle no-gap, marshalling, pinned Dispose, hard scope, Mode A all verified).
  - **Verification:** `dotnet build` 0W/0E across netstandard2.0/net8.0/net10.0/net462; `dotnet test -f net10.0` 438 passed / 0 failed (17 new producer tests; net8.0/net462 build-verified, run in CI); `dotnet format` clean; Mode A `git diff --stat` clean.

- **Milestone 10 / Phase 1 — "Wire the .NET binding into CI verification": DONE (2026-08-12). N=26. CI-wiring milestone (NOT Mode A — root infra in scope).** The .NET binding now has a CI gate mirroring the *shape* of `verify-python`: build → unit → format → `__grpc_dotnet(+_async)` integration. **Scope note (important, not a breach):** this milestone legitimately edits repo-root infra — root `Makefile`, `.semaphore/semaphore.yml`, new `.semaphore/install-dotnet.sh`, `bindings/dotnet/Makefile` — so the usual "diff stays under `bindings/dotnet/`" invariant does NOT apply. **The hard line that DID hold:** no `src/**`/`src/ffi/**`/`confluent_kafka.h`/Rust-core change (verified `git diff --stat prashah_dev_dotnet_binding_consumer..HEAD` empty over those paths). Plan: `design/history/M10/P1-ci-verify-dotnet/PLAN.md`. Branch: stacked **`prashah_dev_dotnet_semaphore`** off `prashah_dev_dotnet_binding_consumer` (`afbd79c3`). Commits `01a6b2c8` (Make wiring) · `701ab53a` (CI block + SDK install) · `fdbca58e` (**`fixup!` for `701ab53a`** — autosquash at PR finalize). Delivered:
  - **New amd64 CI block `"Verify .NET binding"`** (`s1-prod-ubuntu24-04-amd64-2`, `dependencies: []`, one `verify-dotnet` job → `make verify-dotnet`), separate from the existing arm64 "Verify language bindings" block (verify-c/verify-python **untouched**). **amd64 (Decision 3, per CKD precedent)** dissolves the arm64 `Grpc.Tools` protoc risk entirely — native `.so`, both gRPC images, and the testcontainers broker all build/run amd64-native (no QEMU/buildx). Semaphore sets machine type per **task**, not per **job** — hence the own-block.
  - **`.semaphore/install-dotnet.sh`** (new, job-scoped): `dotnet-install.sh --channel 10.0` (SDK — builds all TFMs, runs net10.0) + `--channel 8.0 --runtime dotnet` (lean net8 base runtime — runs net8.0 tests; a net8 test does not roll-forward to net10). Install-only; the block prologue sets `DOTNET_ROOT`/`PATH`/`DOTNET_MULTILEVEL_LOOKUP=0` as **top-level `export` commands** so the env reaches `make verify-dotnet` (the Critic-caught bug — see below).
  - **Make targets** (root + `bindings/dotnet`, mirroring the python/c split): `build-dotnet` (native-first `cargo build --features ffi --release` → `dotnet build -c Release` matrix), `test-dotnet` (build + `dotnet format --verify-no-changes` + `dotnet test -f net8.0` + `-f net10.0`), `build-grpc-images-dotnet`, `test-integration-dotnet` (`cargo test … -- __grpc_dotnet`, both sync+async arms), `verify-dotnet: test-dotnet` + `test-integration-dotnet`. **No perf stage** (Decision 4 — the one shape difference from `verify-python`; alloc-budget lives in unit). Image targets kept platform-agnostic (no hardcoded `--platform`).
  - **net462 (Decision 2a):** build-verified via the test project's pre-existing `Microsoft.NETFramework.ReferenceAssemblies` (no csproj edit needed); tests RUN on net8.0 + net10.0 only (net462 can't run on Linux). This is ≥ CKD, which runs no net462 tests.
  - **Critic N=26: 1 HIGH issue found & fixed** — `install-dotnet.sh`'s in-script `export`s ran in a child subshell (and the `~/.bash_profile` append was a no-op under Semaphore's single-session model), so `dotnet` wouldn't resolve for `make verify-dotnet` → the job would die at the first `dotnet` call. Fixed in `fdbca58e` by moving the exports to top-level prologue commands (matching CKD `semaphore.yml:46-48`); re-review CLEAN.
  - **Verification:** the authoritative run is the CI amd64 block (local `make verify-dotnet` needs the net8 runtime installed and builds the images under `DOCKER_DEFAULT_PLATFORM=linux/amd64` emulation — a local-dev concern; CI amd64 is native and authoritative). Mode A `git diff --stat` clean over ABI/ffi/core.
  - **Follow-up:** the `fixup!` commit `fdbca58e` should be autosquashed into `701ab53a` when the PR is finalized.

- **Milestone 9 / Phase 1 — "Complete the .NET `ConsumerRecord<TKey,TValue>` accessor surface": DONE (2026-08-12). N=25. Mode A** (no `confluent_kafka.h`/`src/ffi`/Rust-core change — verified `git diff --stat afbd79c3..d1ef… ` scoped to ABI/ffi/core is empty; the diff is 5 files, all under `bindings/dotnet/**`). First phase of a new milestone (M8, the multilanguage harness, is closed). Adds the three `ConsumerRecord` accessors that Java 4.2 has and the C ABI already exposed but the .NET record lacked — closing the **M8 `leader_epoch` divergence** (the gRPC backend's `Translate.RecordToProto` had omitted the proto's `leader_epoch` for want of a .NET accessor). Plan: `design/history/M9/P1-consumer-record-accessors/PLAN.md`. Branch: stacked **`prashah_dev_dotnet_consumer_record_accessors`** off `prashah_dev_dotnet_binding_consumer` (`afbd79c3`, PR #150 head). Commits `988ab38d` (accessors + tests) · `1610d0a3` (harness `leader_epoch` forwarding). Delivered:
  - **Three accessors:** `public int? LeaderEpoch` (Java `Optional<Integer>`→nullable), `public int SerializedKeySize`, `public int SerializedValueSize` (Java `int`, −1 if null) on `ConsumerRecord<TKey,TValue>`, with Java-mirrored XML docs; the internal poll-output-only ctor extended (no public ctor).
  - **NativeMethods:** two plain `int32_t` DllImports + a presence-style `leader_epoch` (`bool`+`out int`, `[return: MarshalAs(UnmanagedType.I1)]`, mirroring the `OffsetAndMetadata_leader_epoch` precedent). **Receive path:** `ConsumerRecordsMarshal.CopyRecord` reads the three scalars before building the record — scalar reads only, the §B4 copy-out/zero-copy contract untouched (allocation-budget test still green).
  - **Harness:** one line in `Translate.RecordToProto` forwards `leader_epoch` into the proto (`optional int32 leader_epoch`), serving both the sync + async servicers — Python-parity (`grpc_translate.py:196`). Serialized sizes not forwarded (the consumer proto has no such fields).
  - **`DeliveryCount` EXCLUDED (maintainer decision):** criterion = "include only if the Python sibling exposes it," and Python's `ConsumerRecord` C-extension getters (`bindings/python/_confluentkafka.c:1251-1262`) expose the three but **not** `delivery_count`. (For the record, `ConsumerRecord.deliveryCount()` IS public on Java 4.2 and the C ABI does expose it — a future phase could add `short? DeliveryCount` non-breakingly; it is simply out of scope here.)
  - **Finding — mock is size/epoch-blind:** the Rust core mock (`src/consumer/consumer_record.rs`, via `MockConsumer_add_record`) hard-codes both serialized sizes to `-1` and `leader_epoch` to `None` regardless of key/value, so mock-added records always report `-1`/`-1`/`null`. Unit tests assert the reachable contract (null→−1; `LeaderEpoch==null`); the positive-size / present-epoch paths are integration-only. **No `MockConsumer.AddRecord` overload** was added (injecting an epoch needs an extended mock ABI = Mode B, out of scope).
  - **Critic N=25: CLEAN — 0 issues.**
  - **Verification (all green):** `dotnet build` 0W/0E across netstandard2.0/net8.0/net10.0 (library) + net462/net8.0/net10.0 (tests) + grpc-server (net8.0); `dotnet test -f net10.0` 421 passed / 0 failed (4 new accessor tests; receive-path allocation-budget test still green); `dotnet format --verify-no-changes` clean; `git diff --stat` zero churn to `confluent_kafka.h`/`src/ffi`/Rust core.

- **Milestone 8 / Phase 2 — ".NET async-consumer gRPC backend (`dotnet_async`) for the multilanguage harness": DONE (2026-08-11). N=24. Mode A** (no `src`/production change, no `confluent_kafka.h`/`src/ffi` delta — verified `git diff --stat 815be651..d1ef8906` empty over those paths). The async twin of M8/P1: a **6th, consumer-only** backend (`"dotnet_async"`) drives the .NET **`AsyncKafkaConsumer<byte[],byte[]>`** (+ `AsyncMockConsumer` for empty config) so the same 11 `multilanguage_consumer_test!` scenarios also run against the .NET **async** consumer against a real broker — mirroring `python`/`python_async`. **Value beyond P1:** this exercises the .NET completion bridge end-to-end (`TaskCompletionSource`, the foreign callback-dispatcher thread, `RunContinuationsAsynchronously` — the machinery the committed UAF fix `0ff4c9ed` touches), which the sync path never covers. Plan: `design/history/M8/P2-async-consumer-grpc-backend/PLAN.md`. Commits `28a7d5a3` (async servicer + selector) · `a8fe7b4e` (two-image Docker/Make) · `d1ef8906` (harness 6th arm); 9 files, all additive/new except the one-line `Dockerfile.grpc` edit. Delivered:
  - **Shape: TWO images / Python-parity (maintainer decision, superseding the initial one-image draft).** New **standalone** `bindings/dotnet/Dockerfile.grpc.async` (own `FROM`, `sdk:10.0`→`aspnet:8.0`, `ENV CONSUMER_FLAVOR=async`, tags `confluent-kafka-rust/dotnet-async-grpc-server:dev`, `EXPOSE 50053`); `bindings/dotnet/Dockerfile.grpc` gains only a behavior-preserving `ENV CONSUMER_FLAVOR=sync`. Rationale: true isolation (a P2 change can't touch the shipped sync image; the load-bearing default footgun disappears) + cross-binding consistency (every `BackendKind` → one image, one behavior; `python`/`python_async` ↔ `dotnet`/`dotnet_async` parallel pairs). New `grpc-image-async` Make target + one top-level `build-grpc-images` line.
  - **`grpc-server/AsyncConsumerServiceImpl.cs`** — the 23-RPC async servicer over `AsyncKafkaConsumer`/`AsyncMockConsumer<byte[],byte[]>` (`Serdes.ByteArray`); `Program.cs` gains a `CONSUMER_FLAVOR` selector (`async`→async servicer, else sync). `Translate.cs` reused verbatim (unmodified). Rust harness 6th arm: `BackendKind::DotnetAsync` (image `dotnet-async-grpc-server`, port **50053** — Python-parity, separate container; no env injection), `DotnetAsyncGrpcFactory` (consumer-only), `__dotnet_async` macro arm. `multilanguage_test!` / `ProducerBackendFactory` untouched.
  - **Decisions / deviations (all PLAN-sanctioned):** async per-id gate = `SemaphoreSlim(1,1)` (`await WaitAsync()` + `finally Release()`, never a `lock` across `await`), **`Wakeup` gate-exempt** (must interrupt a blocked awaited `Poll`); **`Close` ignores `timeout_ms`** — `AsyncKafkaConsumer.Close` has only `Close(CancellationToken)` (no timed async close, CLAUDE.md §1), so `await Close()`; **`Seek` (both overloads) called sync** (they're sync `void` on the async consumer, on `IConsumerCommon`); no `Task.Run`/sync-over-async (`await` the binding `Task` directly, `.ConfigureAwait(false)`); the sync `Seek`/`Wakeup` `RunStatus` lambdas are non-async and `return Task.CompletedTask` (dodges CS1998 under warnings-as-errors).
  - **Critic N=24: CLEAN — 0 issues.**
  - **Verification (Actor run, coordinator-verified):** both dotnet images build (linux/amd64); `cargo test --features integration-tests,multilanguage-tests` → **22/22** multilanguage green (11 `…__dotnet_async` + the 11 sync `…__dotnet` isolation regression); C# `dotnet build` 0/0 + `dotnet format --verify-no-changes` clean; Rust `cargo xtask format-check`/`lint` clean; `git diff --stat` zero production/ABI churn. **Emulation note:** on Apple-Silicon the image build + async run need the linux/amd64 emulated path (arm64 Grpc.Tools protoc SIGSEGVs) and the async close/bridge amplifies the emulation close-timeout flake — a local timing artifact, not a code defect; native x86_64 is authoritative.
  - **M8 complete:** the .NET binding now participates in the multilanguage harness with both sync (`dotnet`) and async (`dotnet_async`) consumer backends.

- **Milestone 8 / Phase 1 — ".NET sync-consumer gRPC backend for the multilanguage harness": DONE (2026-08-11). N=23. Mode A** (no `src`/production change, no `confluent_kafka.h`/`src/ffi` delta — verified `git diff --stat 0ff4c9ed..f910e7b3` empty over those paths). The .NET binding now joins the Rust multilanguage integration-test harness as a **5th, consumer-only** backend (`"dotnet"`), so the repo's consumer integration tests (`tests/integration/multilanguage_consumer_test.rs`, via `multilanguage_consumer_test!`) also exercise the .NET binding's **synchronous `KafkaConsumer<byte[],byte[]>`** against a real broker — alongside `rust`/`python`/`python_async`/`c`. Consumer-only is clean because those tests seed data with a native in-process Rust producer, not the backend. Plan: `design/history/M8/P1-sync-consumer-grpc-backend/PLAN.md`. Delivered (10 files; all additive/new):
  - **(A/B/C) `bindings/dotnet/grpc-server/`** — a new test-harness server project (`Confluent.Kafka.GrpcServer.csproj`, net8.0, `Microsoft.NET.Sdk.Web`, `ProjectReference`→`../src/Confluent.Kafka`; **not** added to `Confluent.Kafka.sln`). `Program.cs` hosts Kestrel serving **h2c** (HTTP/2 cleartext, no TLS — the Rust client dials `http://`) and prints `listening on 0.0.0.0:50053` to **stderr** post-`Start()` (the harness `WaitFor::message_on_stderr("listening")` trigger). `ConsumerServiceImpl.cs` implements **ConsumerService only** (all 23 RPCs 1:1 over the shipped sync `IConsumer<byte[],byte[]>`; empty-config→`MockConsumer`, else `KafkaConsumer`, `Serdes.ByteArray`). `Translate.cs` is a faithful port of `bindings/python/grpc_translate.py` incl. the ordered first-match-wins `GuessVariant` (the flat `KafkaException` carries no variant, so it is sniffed from the message) + `KafkaException→KafkaError` + the unknown-consumer-id illegal-state builder.
  - **(D/E)** `bindings/dotnet/Dockerfile.grpc` (multi-stage `sdk:8.0`→`aspnet:8.0`, context=repo root, bundles `libconfluent_kafka.so` beside the app + `LD_LIBRARY_PATH=/app`, `EXPOSE 50053`); new `bindings/dotnet/Makefile` `grpc-image` target (c/python precedent) + exactly one appended `dotnet` line in the top-level `Makefile` `build-grpc-images`. Producer macro/factory untouched.
  - **(F)** Rust harness 5th arm (additive): `BackendKind::Dotnet` (repo `confluent-kafka-rust/dotnet-grpc-server`, internal port **50053** — no collision with python 50051 / c 50052, label `"dotnet"`); `DotnetGrpcFactory` implementing **only** `ConsumerBackendFactory`; a `__dotnet` macro arm mirroring `__c`.
  - **Decisions (see `design/history/M8/P1-sync-consumer-grpc-backend/COMMENTS.DONE.23.md`):** DI-singleton servicer (default per-request activation would empty the id→consumer map — a real bug the local smoke test caught); per-`consumer_id` op serialization with **`Wakeup` exempt** (it must interrupt a blocked `Poll` cross-thread — gating would deadlock); server not added to the `.sln`; `CA1031` relaxed on the server project only (the Python-parity broad catch that funnels every failure into a proto `KafkaError`).
  - **Critic N=23: CLEAN — 0 issues.** Two non-blocking verification caveats (net10.0 leg under the `sdk:8.0` builder resolves via net8.0 TFM negotiation; generated stubs under `TreatWarningsAsErrors` rely on the auto-generated-code exemption) — both confirmable only at image-build time.
  - **Verification (local, all green):** `cargo build --features ffi --release` (native + header regenerated); server `dotnet build -c Release` clean (0/0 under `Directory.Build.props`); `dotnet format --verify-no-changes` clean; end-to-end **h2c round-trip** verified (CreateConsumer→Assign→Assignment→Poll→Subscription→unknown-id→Close, both oneof arms); `cargo test --features integration-tests,multilanguage-tests --no-run` compiles the `…__dotnet` binary (0 warnings). **PENDING (Docker/CI-only — could not run locally, environment gap, not a failure):** `make build-grpc-images` (the `dotnet-grpc-server:dev` image build), the green `…__dotnet` test variants, and `cargo xtask format-check`/`lint` (rustfmt/clippy absent locally). The Dockerfile mirrors the Python image's native staging; the Rust edits mirror the clippy-clean siblings.
  - **Deferred to M8/P2 (out of scope):** a `dotnet_async` twin driving `AsyncKafkaConsumer` (mirrors `python_async`).

- **Milestone 7 / Phase 2a — "Consumer test-redundancy cleanup": DONE (2026-08-11).**
  **Test-only, Mode A** (no `src` / production change; `cargo build --features ffi` shows **no
  header delta**, `e5b06413…c4b0d` unchanged). Removed four source-verified redundancies across the
  consumer test corpus with **zero coverage loss** — every deletion's exact assertion is covered by
  a named retained test. **Count: 437 → 417 (net −20** on net10.0, the runtime installed here;
  net8.0/net462 legs compile-verified). The PLAN's ~−21 estimate assumed A1.4 = −12; actual A1.4 =
  −11 because CommitAsync's ViaInterface singleton is **uniquely-covering** and was retained (see
  A1.4 below) — a deliberate coverage-preservation call the PLAN sanctions. Delivered:
  - **A1.1 (net −7):** collapsed the **8** `new TopicPartition(_, -1)` ctor-guard copies (each a
    pure value-type check with zero consumer interaction, scattered across
    ApiTests / CommitTests / OffsetQueryTests / PositionTests / PartitionOpsTests / SeekLagTests /
    SyncPreconditionTests / SyncQueryTests) into one new
    `PublicTopicPartitionTests.NegativePartition_Throws` asserting the **superset** (`ParamName ==
    "partition"` **and** message `"Partition must not be negative."`, verified
    `TopicPartition.cs:55-56`). ⚠ The **9th** negative-partition test —
    `Interop/ConsumerUnsubscribeSeekGroupMetadataTests.Seek_NegativePartition_ThrowsArgumentOutOfRange`
    — is a **DIFFERENT layer** (the interop `NativeConsumer.Seek(partition:-1)` guard) and was
    **RETAINED** (deleted by exact file:method, never name-grep). `ReadyForPosition` in PositionTests
    was **kept** (still used by 9 retained tests — the PLAN's "now-unused" note did not hold against
    the current file; a recorded deviation).
  - **A1.2 (net −1):** deleted the byte-identical
    `PublicConsumerPartitionOpsTests.Assign_ThenAssignment_ReflectsExactlyTheAssignedPartitions`; the
    keeper `PublicConsumerSyncReadTests.Assignment_ReflectsAssign_ExactlyTheAssignedPartitions` (owns
    `Assignment()`) byte-covers it. Removed the now-orphaned `using System.Collections.Generic`
    (its only uses were in the deleted twin).
  - **A1.4 (net −11):** consolidated the **15** ViaInterface upcast tests (no explicit interface
    impl → a ViaInterface test only pins "(member, interface) is reachable"). **4 retained smokes**,
    11 deleted, every deleted pair still reached by a retained test:
    - `SyncReads_ReachableViaIAsyncConsumerInterface` → Subscription/Assignment/Paused/EnforceRebalance
      via IAsyncConsumer.
    - `SeekAndCurrentLag_ViaIConsumerCommon_Work` → Seek(long)/Seek(OaM)/CurrentLag via IConsumerCommon.
    - `SyncMockConsumer_ViaIConsumerInterface_RoundTrips` (**EXTENDED/fold**) → Poll + **Committed +
      PartitionsFor + ListTopics** via IConsumer (folds the 3 sync-query singletons).
    - `CommitAsync_ViaIConsumerCommonInterface_ReturnsWithoutThrowing` (**KEPT**) → CommitAsync via
      IConsumerCommon is reached **only** here (no incidental upcast, no other batched keeper).
    - **Deleted 11**, each covered by a retained **interface-typed test helper**: async Commit/Assign/
      BeginningOffsets/Poll/Position/PartitionsFor/ListTopics via the `*Of(IAsyncConsumer …)` helpers
      (CommitOf/CommitOffsetsOf/AssignOf/BeginningOffsetsOf/Poll(IAsyncConsumer)/PositionOf/
      PartitionsForOf/ListTopicsOf, used by retained tests); sync Poll via the `Poll(IConsumer …)`
      helper; sync Committed/PartitionsFor/ListTopics via the extended #7. Removed the now-orphaned
      RoundTripTests helpers `TestTimeoutResult` + `Poll(Task<…>)` (only the deleted async-Poll
      singleton used them).
  - **A1.6b (net −1):** deleted the strict-subset
    `PublicSyncConsumerRoundTripTests.Commit_WithOffsets_BrokerFree_Succeeds`; the superset keeper
    `PublicSyncConsumerQueryTests.Committed_AfterCommit_RoundTripsOffsetMetadataAndEpoch` adds the
    `Committed(...)` read-back. Siblings `Commit_NoOffsets_…` / `Commit_EmptyOffsets_…` (distinct
    paths) retained.
  - **DoD:** `dotnet build` 0 warnings / 0 errors on all TFM legs (net462/net8.0/net10.0 tests;
    library unchanged — no `src` touched); full suite green (437 → 417); the new
    `PublicTopicPartitionTests.NegativePartition_Throws` discovered + passing; `dotnet format
    --verify-no-changes` clean; `cargo build --features ffi` no header delta. (Test-run verified on
    net10.0, the only runtime installed here; net8.0/net462 legs compile-verified, executed in CI.)
  - ⚠ **Working-tree note (PR #144, Option 1):** the SafeHandle fix +
    `DisableTestParallelization=false` flip stay **uncommitted** in three files
    (`NativeConsumer.cs`, `OperationCompletionSource.cs`, `AssemblyInfo.cs`); M7/P2a's commits carry
    **only** `tests/…` edits (per-path staging; the fix files never staged/touched, remain ` M`).

- **Milestone 7 / Phase 1 — "Allocation-budget test hardening": DONE (2026-08-11).**
  **Test-only, Mode A** (no `src` / production change; `cargo build --features ffi` shows **no
  header delta**, `e5b06413…` unchanged). Made the receive-path / query allocation-budget tests
  robust under **parallel** execution and removed the CI `--filter !~Allocation` wart. The
  allocation-budget suite measured a tiny per-record signal with the **process-wide**
  `GC.GetTotalAllocatedBytes(precise:true)`, which is contaminated by concurrently-running tests
  under parallel execution (and mis-measured the async path, whose copy-out runs on the foreign
  dispatcher thread). Delivered:
  - **Hardened (process-wide → per-thread counter):** the two retained budgets —
    `PublicSyncConsumerAllocationBudgetTests.Poll_PerRecordAllocation_WithinCopyOutBudget` and the
    `PublicSyncConsumerQueryAllocationBudgetTests` begin-offsets / partitionsFor per-op budgets —
    now use **`GC.GetAllocatedBytesForCurrentThread()`**. The sync path runs the **shared**
    marshaller (`ConsumerRecordsMarshal.CopyOut<K,V>` / `OffsetMapMarshal` /
    `PartitionInfoListMarshal`) on the **caller thread**, so a per-thread count measures exactly
    this op's allocation and is **immune to concurrent tests** — no assembly-wide serialization
    needed. Marginal (large − small) subtraction, warmup, thresholds, and the net462 guard/skip
    unchanged.
  - **Converted (async consumer → sync typed consumer + per-thread counter):**
    `PublicConsumerTypedAllocationBudgetTests.TypedPoll_LargeValue_AddsNoValueSizedIntermediateAllocation`
    now drives the **sync** typed `MockConsumer<byte[], int>` (64 KiB value → small decoded `int`
    via `SpanLengthDeserializer`), reaching the identical `CopyOut<K,V>` on-thread — still proving
    **no value-sized intermediate `byte[]`** on the typed key/value path.
  - **Removed (redundant per-record, or §11-amortized per-op; all process-wide/flaky):** the async
    poll budget (`PublicConsumerAllocationBudgetTests.cs`) and its interop twin
    (`Interop/ConsumerPollAllocationBudgetTests.cs`) — whole files; and the per-op budgets
    `BeginningOffsets_PerOpAllocation` / `PartitionsFor_PerOpAllocation` / `Position_PerOpAllocation`
    / `Pause_RepeatedOp_MarshallingAllocationIsBounded` / `Assignment_RepeatedRead_…` /
    `SeekAndCurrentLag_PerOpAllocation` — the method + its private helper, in their shared files.
    Pre-delete no-unique-coverage check: each is either an async duplicate of a **retained** sync
    per-record budget (the marshaller is shared) or a per-op/per-RPC surface CLAUDE.md §11 deems
    amortized (not a per-record marshaller path) — no unique per-record coverage lost.
  - **Because the marshaller is shared between sync and async**, the per-thread sync measurement
    fully covers the async **per-record** budget; the async round-trip adds only **per-op**
    Task/GCHandle/state-machine overhead.
  - **Consciously-accepted gap (§4):** after this, **no test budgets the async per-op overhead**.
    Justified — CLAUDE.md §11 classifies per-RPC/per-op cost as amortized/negligible, and the
    marginal subtraction already cancelled it by construction (it was never budgeted). Documented
    decision, nothing real lost.
  - **DoD:** `dotnet build` 0 warnings / 0 errors on all TFM legs (net462/net8.0/net10.0 tests;
    library unchanged), full suite green with **no `--filter`**, `dotnet format` clean. **Headline
    gate:** the full suite ran ≥20× under `DisableTestParallelization=false` with **no**
    `--filter !~Allocation` (alloc tests included) with **zero** alloc-test failures — the manual
    `--filter !~Allocation` is **no longer needed**. (Verified locally on net10.0, the only
    runtime installed here; net8.0/net462 legs compile-verified, executed in CI.)
  - ⚠ **Working-tree note (PR #144, Option 1):** the SafeHandle fix +
    `DisableTestParallelization=false` flip live **uncommitted** in three files
    (`NativeConsumer.cs`, `OperationCompletionSource.cs`, `AssemblyInfo.cs`); M7/P1's commits carry
    **only** the alloc-budget test files. The parallel headline gate ran against that working-tree
    configuration.

- **Milestone 6 / Phase 1b — "Typed consumers": DONE (2026-08-10).** Second (final) phase of
  M6: the **generic-only conversion** of the shipped consumer family + the **zero-copy typed
  poll**, consuming P1a's serde foundation. **Mode A (no Rust authored):** genericness is a thin
  managed skin over the bytes-only `NativeConsumer`; `cargo build --features ffi` shows **no
  header delta** (diffed before/after, `e5b06413…` unchanged). Delivered:
  - **Generic record types** — `ConsumerRecord<TKey, TValue>` (Key→`TKey`, Value→`TValue`;
    Topic / Partition / Offset / Timestamp / TimestampType / **materialized `Headers`** unchanged;
    poll-output-only internal ctor) and `ConsumerRecords<TKey, TValue> :
    IReadOnlyCollection<ConsumerRecord<TKey, TValue>>`.
  - **Generic-only conversion (decision B)** — all six shipped client types converted to
    `<TKey, TValue>` and the **non-generic types removed** (no bytes sibling, no shadow types):
    sync `IConsumer<K,V>` / `KafkaConsumer<K,V>` / `MockConsumer<K,V>` and async
    `IAsyncConsumer<K,V>` / `AsyncKafkaConsumer<K,V>` / `AsyncMockConsumer<K,V>`. `IConsumerCommon`
    stays **non-generic** (all members K/V-free); both generic interfaces inherit it unchanged and
    **only `Poll` retypes**. Real ctors are 3-param `(config, keyDeserializer, valueDeserializer)`
    (Java `KafkaConsumer.java:601`). Bytes users write `<byte[], byte[]>` + `Serdes.ByteArray`.
  - **Zero-copy typed poll (the crux, ffi §B4)** — `ConsumerRecordsMarshal.CopyOut<K,V>` deserializes
    each key/value from an **unsafe `ReadOnlySpan<byte>` over the native batch** (contained to
    `Internal/Interop/`; the `ref struct` provably can't escape) — **no intermediate per-record
    `byte[]`**. Sync `NativeConsumer.PollTyped<K,V>` deserializes on the **caller's** thread; async
    `PollWithCallback<K,V>` runs `CopyOut<K,V>` on the core's **foreign dispatcher thread** (inside
    the typed poll trampoline `TypedPollCallbacks<K,V>`) **before** `ConsumerRecords_destroy`, then
    completes the TCS (`RunContinuationsAsynchronously`); the serdes travel in the per-op `GCHandle`
    context (`TypedPollCompletionSource<K,V>`). The dead non-generic poll path was removed.
  - **Null / tombstone → `default(T)` three-state (decision C)** — `len < 0` (or ptr==Zero): absent
    → `default(T)`, **deserializer NOT invoked**; `len == 0`: present-empty → a 0-length span;
    `len > 0`: present → the span. Documented deviation from Java's `deserialize(topic, null)`;
    `long?` distinguishes a tombstone from a genuine `0`.
  - **Mandatory `SerializationException` wrap (decision E)** — the typed-poll marshaller catches any
    user-deserializer throw and wraps it (inner + topic/partition/offset). Mandatory because the
    async deserialize runs on the foreign dispatcher thread — a managed exception escaping into
    native is UB; sync surfaces it as a synchronous throw, async faults the `Task` (never unwinds
    into native).
  - **MockConsumer deviation (§7)** — `MockConsumer<K,V>` / `AsyncMockConsumer<K,V>` ctors **take
    the two deserializers** (Java's mock doesn't — ours must, its `Poll` decodes native bytes like
    the real consumer); `AddRecord(topic, partition, offset, byte[]? key, byte[]? value)` stays
    **bytes-in** (Java's is typed-in) — tests the deserialize path in isolation + forced by the
    bytes-only core ABI. Both documented in doc-comments.
  - **Corpus-wide test migration** — every shipped consumer test migrated at construction
    (`new MockConsumer()` → `new MockConsumer<byte[],byte[]>(Serdes.ByteArray, Serdes.ByteArray)`,
    etc.) + the `ConsumerRecord(s)` / interface type refs to `<byte[],byte[]>`; under the identity
    `Serdes.ByteArray` `.Key`/`.Value` stay `byte[]?` so **assertions are unchanged**. The Interop
    poll tests additionally pass `Serdes.ByteArray` to the now-typed `NativeConsumer` poll (the one
    spot exceeding construction-only, forced by the typed poll path).
  - **New P1b tests** — typed round-trip on `<string,long>` (sync + async) + `<byte[],byte[]>`
    equivalence; the three-state null model (absent skips the deserializer, asserted via a counting
    AND a throwing deserializer; present-empty 0-length span; `long?` tombstone-vs-0); serde-throws
    → `SerializationException` (sync throws, async faults, a churn loop is the dispatcher-thread
    no-unwind regression); thread-of-deserialize (sync = caller, async = dispatcher); a per-op
    allocation budget proving a 64 KiB value decodes with ~0 marginal allocation (no intermediate
    `byte[]`); a typed TFM smoke. **429 → 445 tests** (16 new), green on net10.0; the
    threaded/serde tests stable over repeated runs.
  - **DoD:** `cargo build --features ffi` (no header delta, diffed) → `dotnet build` 0 warn/0 err
    across all library (ns2.0/net8.0/net10.0) + test (net462/net8.0/net10.0) TFMs → net10.0 tests
    green → `dotnet format --verify-no-changes` clean. DoD §6: the non-generic consumer types are
    **removed, not shadowed** (grep-confirmed). No TODO/FIXME; Apache-2.0 header on every new file;
    the new `unsafe` span-over-`IntPtr` lives only in `Internal/Interop/ConsumerRecordsMarshal.cs`.
  - Approved plan: `design/history/M6/P1b-typed-consumers/PLAN.md`. Commits on
    `prashah_dev_public_consumer_serdes_poc`. N=20. Closes M6.

- **Milestone 6 / Phase 1a — "Serde foundation": DONE (2026-08-10).** First phase of a new
  milestone (serde is a new subsystem; clean boundary from M5's consumer clients). The
  bidirectional (de)serialization foundation — the Java `Serializer<T>` / `Deserializer<T>` /
  `Serde<T>` shape in idiomatic C# — on top of the bytes-only ABI. **Pure managed, Mode A (no
  Rust authored):** (de)serialization is a binding-/user-layer concern (CLAUDE.md §4);
  `cargo build --features ffi` shows **no header delta** (diffed before/after, `e5b06413…`
  unchanged). **Scope = the foundation only** — NO records, typed poll marshaller, or typed
  clients (those are P1b, N=20). Delivered:
  - **`ISerializer<T>`** — `byte[]? Serialize(string topic, T data)` (Java `Serializer<T>` shape).
  - **`IDeserializer<T>`** — `T Deserialize(string topic, ReadOnlySpan<byte> data)`: **sync,
    span-based** (the §6.4/§27 zero-copy lock — a `ref struct` span borrows the native fetch slice
    in place and provably can't outlive the batch P1b borrows it from; sync because a span can't
    cross `await` and serde is CPU-bound). Header-less form only.
  - **`ISerde<T>` : `ISerializer<T>`, `IDeserializer<T>`** — the Java `Serde<T>` shape returned by
    the `Serdes` factory (composes the two directional interfaces).
  - **`Serdes` static factory** with 7 built-in `ISerde<T>` singletons, **byte-for-byte Java
    wire-format parity** (verified against `org.apache.kafka.common.serialization.*`, Apache Kafka
    4.2): `String` (UTF-8), `ByteArray` (identity; deserialize copies the span to an owned
    `byte[]`), `Int32` (4 bytes big-endian, `IntegerSerializer`), `Int64` (8 bytes big-endian,
    `LongSerializer`), `Double` (8 bytes big-endian `doubleToLongBits`, NaN canonicalized,
    `DoubleSerializer`), `Guid` (⚠ `UUID.toString()`→UTF-8, the **string** form — NOT the 16 raw
    bytes; sidesteps the Guid/UUID field-endianness mismatch, `UUIDSerializer`), `Null`
    (`VoidSerializer` — serialize `null`, deserialize default).
  - **`SerializationException : KafkaException`** — a flat Java-parity subclass (ffi §A5); the
    built-in serdes throw it on malformed input with **Java's exact messages** (e.g. `"Size of
    data received by IntegerDeserializer is not 4"`; `Double` uses Java's byte[]-overload quirk
    `"...received by Deserializer..."`). Catchable as `KafkaException`.
  - **Deliberate deviations, recorded (CLAUDE.md §3/§4, code doc-comments):** (1) deserializer
    **span** vs Java's `byte[]` — the zero-copy lock (ffi §B4); (2) **headers overload deferred** —
    addable non-breakingly as a C# default-interface-method; (3) **async serde deferred** — a NOTE
    only, no async interface; (4) `Serialize` returns **`byte[]?`** (nullable) — Java serializers
    return `null` for `null` input / `VoidSerializer` always `null` / tombstone semantics; (5)
    **`ISerde<T>` added** = Java `Serde<T>` (what `Serdes` returns) — composes the two shipped
    directional interfaces; concrete impls kept `internal` under `Internal/Serialization/`.
  - **Tests (broker-free, pure managed — no consumer needed):** `SerdesTests` — round-trips
    (String incl. non-ASCII/surrogate, ByteArray, Int32/Int64/Double incl. NaN/inf, Guid, Null),
    **byte-level Java-wire-parity vectors** (big-endian layout — `256`→`{00,00,01,00}` pins
    endianness; `Double` `1.0`→`{3F,F0,00,…}`; `Guid` = 36-byte canonical lowercase UUID string,
    asserted NOT 16 bytes; String UTF-8), malformed→`SerializationException` (exact type + message),
    base-type catch as `KafkaException`. **389 → 429 tests** (40 new), green on net10.0.
  - **DoD:** `cargo build --features ffi` (no header delta, diffed) → `dotnet build` 0 warn/0 err
    across all library (ns2.0/net8.0/net10.0) + test (net462/net8.0/net10.0) TFMs → net10.0 tests
    green → `dotnet format --verify-no-changes` clean. No TODO/FIXME; Apache-2.0 header on every
    new file. No `unsafe` added outside `Internal/Interop/` (the span→string decode lives in
    `Utf8Marshal`).
  - Approved plan: `design/history/M6/P1a-serde-foundation/PLAN.md`. Commits on
    `prashah_dev_public_consumer_serdes_poc`. N=19. P1b (typed consumers, N=20) starts after P1a
    closes.

- **Milestone 5 / Phase 8b — "Synchronous consumer — query family": DONE (2026-08-07).**
  The six blocking **query** members added **additively** to the shipped sync `IConsumer` (P8a
  shipped the core loop; P8b completes the surface) — the sync mirror of the async query family.
  **Mode A (no Rust authored):** every op has a sync C-ABI variant already in the header;
  `cargo build --features ffi` shows **no header delta** (diffed before/after,
  `e5b06413…`). Delivered:
  - **Six members on `IConsumer`** (and on `KafkaConsumer` + `MockConsumer` as thin forwarders):
    `Committed(IReadOnlyCollection<TopicPartition>)` → `IReadOnlyDictionary<TopicPartition,
    OffsetAndMetadata>`, `OffsetsForTimes(IReadOnlyDictionary<TopicPartition, long>)` →
    `IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>`, `BeginningOffsets` / `EndOffsets`
    (`IReadOnlyCollection<TopicPartition>`) → `IReadOnlyDictionary<TopicPartition, long>`,
    `PartitionsFor(string)` → `IReadOnlyList<PartitionInfo>`, `ListTopics()` →
    `IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>`. Each returns its owned result
    directly (no `Task`, **no `CancellationToken`** — the P8a sync shape); a failure is a
    **synchronous** `KafkaException` throw. The async counterparts on `IAsyncConsumer` are
    untouched.
  - **The load-bearing rule (held; Critic-checked):** every sync query calls the **sync C ABI
    directly** (`Consumer_committed` / `_offsets_for_times` / `_beginning_offsets` /
    `_end_offsets` / `_partitions_for` / `_list_topics`) — the core's `block_on` runs inside the
    Rust multi-thread runtime, so the caller parks deadlock-free (the P8a sync core-loop
    precedent). **No `…Async(...).GetAwaiter().GetResult()` / `.Result` / `.Wait()`, no
    `Task.Run`, no managed `block_on` façade over `AsyncKafkaConsumer`** anywhere in the sync
    query path (grep-verified). The sync and async families are siblings over the one
    `NativeConsumer`, not one wrapping the other.
  - **Per-op wrapper discipline:** preconditions (§B5) BEFORE any pin / P-Invoke → `ThrowIfClosed`
    → call-scoped input pin → P/Invoke → **copy-out-then-destroy** via the **existing** marshaller
    (`OffsetMapMarshal` / `OffsetAndTimestampMapMarshal` / `LongOffsetMapMarshal` /
    `PartitionInfoListMarshal` / `TopicPartitionInfoMapMarshal`), root destroyed in a `finally`
    (§6.4). NO completion bridge, NO `GCHandle`, NO callback (cleaner than the async ones).
  - **⚠ Out-param pre-init (correctness, verified against `src/ffi/consumer.rs`):** the sync query
    FFI writes `*out_handle` **only on success** and **leaves it untouched on failure** (the error
    arm does `return box_error(e)` without touching the out-param) — unlike `Consumer_poll`, which
    writes `out_error` on **both** paths. A blittable `out IntPtr` is pinned-in-place over the
    managed local, so every wrapper **pre-initializes** its out-local to `IntPtr.Zero`; the failure
    path then yields a null handle and the null-safe container `_destroy` is a no-op. (Recorded as
    a tricky-interop learning in local agent memory.)
  - **`NativeMethods`:** 6 new sync `[DllImport]`s, each `(…, out IntPtr outHandle) → IntPtr`
    (`KafkaError*`), full ABI `EntryPoint`s, `Cdecl`, parallel-array input shapes **identical** to
    the async query DllImports.
  - **`NativeConsumer`:** 6 new sync wrappers. **Reuse, no duplication (DoD §6):**
    `SnapshotPartitions` / `ExtractPartitions` / `WithPinnedTopics` /
    `WithPinnedTopicsAndTimestamps` and all five copy-out marshallers reused; a new shared generic
    `RunContainerQuerySync<TResult>` + `ThrowOrCopyOutAndDestroy<TResult>` back the three
    collection-input queries (the async `SubmitOwnedHandleOperation<TResult>` generic-over-result
    analog); a new shared `SnapshotTimestamps` (map-input validation) now backs **both** the sync
    `OffsetsForTimes` **and** the refactored async `OffsetsForTimesWithCallback` (the shared
    `SnapshotCommitOffsets` / `WithPinnedTopicsOnly` precedent — behavior preserved byte-for-byte).
  - **Mock reachability (honesty, verified against `src/consumer/mock_consumer.rs`):**
    - **`Committed` — now a FULL 3-field round-trip** (M5/P6 unblocked it): `Assign([tp]) →
      Commit({tp: new OffsetAndMetadata(42, "meta-x", 7)}) → Committed([tp])` reads back **offset
      42, metadata "meta-x", AND leader epoch 7** (the mock returns the stored value only for an
      **assigned** TP — `subscriptions.is_assigned`; unassigned/uncommitted → omitted). Plus the
      null-metadata/null-epoch variant (`"" ` / `null`).
    - **`OffsetsForTimes` — unsupported (honesty):** the mock returns `unsupported_version`
      unconditionally (Java's not-implemented `MockConsumer`), so the sync call **THROWS**
      `KafkaException` (code 35, exact message asserted) — even for an empty map. **No** success
      round-trip is claimed (unreachable on the mock); the copy-out path is proven by the two
      sibling offset-map marshallers.
    - **`BeginningOffsets`/`EndOffsets`:** data-testable via `UpdateBeginningOffset` /
      `UpdateEndOffset`; a TP with no offset → `KafkaException` (`illegal_state`, exact message).
    - **`PartitionsFor`/`ListTopics`:** data-testable via `UpdatePartitions` (owned copies); empty
      list for an unregistered topic, empty topic **forwarded** (not rejected), empty map when no
      topics. Offline-replicas-empty + null-rack are the documented mock slice.
  - **Tests (broker-free, all `MockConsumer`):** `PublicSyncConsumerQueryTests` (the round-trips,
    the honesty throw, unset-partition throws, non-ASCII key + length-delimited host, empty-input,
    all preconditions + **exact messages** before any native call incl. even-when-closed,
    post-dispose on all six, reusable-after-throw), `PublicSyncConsumerQueryAllocationBudgetTests`
    (per-op budget for `BeginningOffsets` + `PartitionsFor`, net8.0+), plus a sync query-family leg
    in `PublicConsumerTfmSmokeTests`. **346 → 389 tests**, green on net10.0, **stable 4/4** full
    runs.
  - **Doc-sync (DoD §1):** `bindings/dotnet/CLAUDE.md` §3 — the six query members added to the
    `IConsumer` sketch, the note updated (core loop P8a + query family P8b, both shipped).
  - **DoD:** `cargo build --features ffi` (no header delta, diffed) → `dotnet build` 0/0 across all
    library (ns2.0/net8.0/net10.0) + test (net462/net8.0/net10.0) TFMs → net10.0 tests green
    (net8.0 *run* + net462 are CI/Windows-only; all three *build* legs pass locally) →
    `dotnet format --verify-no-changes` clean. No TODO/FIXME; Apache-2.0 header on every new file.
  - Approved plan: `design/history/M5/P8-sync-consumer/PLAN.md` (P8b is the query family, §10
    phasing). Commits on `prashah_dev_public_consumer_remaining_sync` (off M5/P8a HEAD `8490cc42`),
    same PR family as P8a. N=18.

- **Milestone 5 / Phase 8a — "Synchronous consumer — surface + core loop": DONE
  (2026-08-07).** The **synchronous** consumer trio — `IConsumer` / `KafkaConsumer` /
  `MockConsumer` — the blocking mirror of the async trio and the most Java-faithful shape
  (Java's `Consumer` is synchronous). Un-defers the documented "async-only, no sync facade"
  stance (governance amendment, approved). **Mode A (no Rust authored):** every op has a sync
  C-ABI variant already in the header; `cargo build --features ffi` shows **no header delta**
  (diffed before/after). **P8a = surface + core loop; the query family is P8b** (the interface
  grows additively). Delivered:
  - **`IConsumer : IConsumerCommon, IDisposable`** — the P8a core-loop members: `Poll(TimeSpan)`
    → `ConsumerRecords`, `Subscribe`, `Unsubscribe`, `Assign`, `Pause`, `Resume`,
    `SeekToBeginning`, `SeekToEnd`, `Position` → `long`, `Commit()` / `Commit(offsets)`,
    `Close()` / `Close(TimeSpan)`. `Seek`×2 / `CurrentLag` / `Wakeup` / `Assignment` /
    `Subscription` / `Paused` / `GroupMetadata` / `EnforceRebalance` / `CommitAsync` come from
    `IConsumerCommon` for free. **No `CancellationToken`** on any blocking method — interruption
    is `Wakeup()` only (Java-faithful; locked decision 3). **`Close(TimeSpan)`** over the sync
    `close_with_timeout` ABI (no Rust dep): negative `TimeSpan` → `ArgumentOutOfRangeException`
    before any P/Invoke (even when closed), `TimeSpan.Zero` valid (locked decision 2).
  - **`KafkaConsumer`** (real KIP-848) and **`MockConsumer`** (broker-free, with the inherent
    mock-only helpers `AddRecord` / `SetPollError` / `UpdateBeginningOffset` / `UpdateEndOffset`
    / `UpdatePartitions`) — thin forwarders over `NativeConsumer`'s sync wrappers. Both are
    **siblings** of `AsyncKafkaConsumer` / `AsyncMockConsumer` over the **same** `NativeConsumer`;
    neither wraps the async API (sync names, no `Async` prefix).
  - **The load-bearing rule (held; Critic-checked):** every sync method calls the **sync C ABI
    directly** — the core's `block_on` runs inside the Rust multi-thread runtime, so the caller's
    thread parks deadlock-free (the shipped `Seek` / `CurrentLag` / `EnforceRebalance` sync-op
    precedent). **No `…Async(...).GetAwaiter().GetResult()` / `.Result` / `.Wait()`, no
    `Task.Run` wrapping, no managed `block_on` façade over `AsyncKafkaConsumer`** anywhere in the
    sync path (grep-verified).
  - **MUST-VERIFY blocker — RESOLVED + PROVEN.** The sync `Consumer_poll`'s `block_on` observes
    `Consumer_wakeup`: sync `Consumer_poll` (FFI L568) does `block_on(consumer_mut(h).poll(...))`
    — the **same** `poll()` future the async path awaits — and `Consumer_wakeup` (FFI L528) fires
    the same rotating token, so `poll()` returns `Err(Wakeup)` when it cancels, surfaced as a
    `KafkaException`. **Proven** by the required Wakeup one-shot regression (deterministic
    single-threaded + a cross-thread test), green + stable across **5/5** suite runs.
    ⚠ **Documented mock-poll determinism ceiling:** the mock `poll`
    (`src/consumer/mock_consumer.rs:525`) runs to completion **synchronously** — it records the
    timeout, drains one poll task, then checks-and-clears the wakeup flag (Step 4), then drains
    records; it never awaits, so a `Poll(30s)` does **not** actually block for 30 s. A genuinely
    mid-flight interrupt is therefore **not reachable** broker-free (the same ceiling the async
    M5/P2–P3 phases recorded). What IS deterministic and asserted: the wakeup flag is **sticky**
    until a poll observes-and-clears it, so a `Wakeup()` from another thread is caught by an
    actively-polling consumer (a bounded loop, `TestTimeout`-guarded) and one-shot then clears.
  - **`NativeConsumer`**: new sync wrappers — `Poll(TimeSpan)` (copy-out-then-destroy via
    `ConsumerRecordsMarshal.CopyOut` + `ConsumerRecordsDestroy` in a `finally`, §6.4;
    negative-timeout precondition matching `PollWithCallback`), `Subscribe` / `Unsubscribe` /
    `Assign(IReadOnlyCollection<TopicPartition>)` / `Pause` / `Resume` / `SeekToBeginning` /
    `SeekToEnd`, `Position` → `long` (out param), `CommitSync()` / `CommitSyncOffsets(offsets)`,
    `CloseSync()` / `CloseSyncWithTimeout(ms)` (share the `TryBeginClose` latch +
    `finally`-destroy — idempotent with `Dispose`/`DisposeAsync` — and **surface** the close
    error, unlike `Dispose`). **Reuse, no duplication (DoD §6):** `SnapshotPartitions` /
    `ExtractPartitions` / `WithPinnedTopics` / `WithPinnedCommitOffsets` / `SnapshotCommitOffsets`
    / `ConsumerRecordsMarshal` reused; new shared helpers `RunPartitionOpSync` +
    `InvokePartitionOpSync` (the sync partition-op tail, now also backing the tuple-form `Assign`
    driver) and `WithPinnedTopicsOnly` (the topics-only pin path, now shared by the sync
    `Subscribe` and the refactored async `SubscribeWithCallback`).
  - **`NativeMethods`**: 10 new sync `[DllImport]`s (`Consumer_poll` with `out IntPtr outError`,
    `_subscribe` / `_unsubscribe` / `_pause` / `_resume` / `_seek_to_beginning` / `_seek_to_end`,
    `_position` with `out long`, `_commit_sync` / `_commit_sync_offsets`), full ABI `EntryPoint`s,
    `Cdecl`, parallel-array shapes identical to the async DllImports. `Consumer_assign` (sync),
    `_close`, `_close_with_timeout` already declared — reused.
  - **Single-owner / concurrent-use:** a concurrent op from another thread → a synchronous
    `KafkaException` (ConcurrentModification), delivered by the core's access guard (no managed
    guard); **documented as a non-deterministic mock limit** (the mock poll holds the guard only
    for an instant — the async-phase D-Q4 ceiling), verified by inspection, no flaky test shipped.
  - **Tests (broker-free, all `MockConsumer`):** `PublicSyncConsumerRoundTripTests` (poll
    round-trip incl. non-ASCII / tombstone / empty / multiple; Seek→Position; the commit family;
    subscribe/unsubscribe; pause→paused→resume; seekTo* observed via poll),
    `PublicSyncConsumerWakeupTests` (the one-shot blocker proof — single-threaded + cross-thread,
    `TestTimeout`-bounded), `PublicSyncConsumerPreconditionTests` (all preconditions + **exact
    messages**, before any native call even when closed; unassigned Position/Seek/Pause →
    synchronous `KafkaException`), `PublicSyncConsumerTeardownTests` (Close/Close(TimeSpan)/Dispose
    idempotence + use-after-close + 100× churn), `PublicSyncConsumerAllocationBudgetTests` (Poll
    receive-path budget, net8.0+), plus two sync legs in `PublicConsumerTfmSmokeTests`. **282 →
    346 tests**, green on net10.0, **stable 5/5** full runs. (Throwing polls call the sync ABI
    directly — `TestTimeout.Run(Action)` surfaces a fault as `AggregateException`; the mock poll
    is synchronous so there is no hang to guard on the throwing path.)
  - **Governance + doc-sync (DoD §1):** repo-root `.claude/rules/consumer-threading.md` §1.1 —
    a new amendment recording that the sync facade is now shipped **in the .NET binding** (Java's
    `Consumer` is synchronous) via **direct sync-C-ABI calls** (`block_on` inside the Rust core's
    runtime), **distinct from** the forbidden managed `block_on`/`Task.Run`/`GetResult` façade,
    with the Rust public API **remaining async-only**; `bindings/dotnet/CLAUDE.md` §3 (the
    `IConsumer`/`KafkaConsumer`/`MockConsumer` sketch added beside the async trio) and §4
    (interface-naming row: sync `IConsumer` **shipped**, `IProducer` still deferred; the
    sync/async split carried by interface+type, no `Async` suffix on methods).
  - **DoD:** `cargo build --features ffi` (no header delta, diffed) → `dotnet build` 0/0 across
    all library (ns2.0/net8.0/net10.0) + test (net462/net8.0/net10.0) TFMs → net10.0 tests green
    (net8.0 *run* + net462 are CI/Windows-only; all three *build* legs pass locally) →
    `dotnet format --verify-no-changes` clean. No TODO/FIXME; Apache-2.0 header on every new file.
  - Approved plan: `design/history/M5/P8-sync-consumer/PLAN.md`. Commits on
    `prashah_dev_public_consumer_remaining_sync` (off M5/P7 HEAD `0bce1866`), as a new PR for
    M5/P8a. N=17. P8b (the query family, N=18) starts after P8a closes.

- **Milestone 5 / Phase 7 — "Consumer sync seek + current-lag": DONE (2026-08-07).**
  Two Python-aligned **synchronous** members added to `IConsumerCommon` (the shared
  non-blocking base), a **breaking** async→sync + interface relocation. **Mode A (no Rust
  authored):** all three ABI fns (`Consumer_seek` / `_seek_with_metadata` / `_current_lag`)
  verified present in the header; `cargo build --features ffi` shows **no header delta**
  (diffed before/after). Delivered:
  - **`void Seek(TopicPartition, long)` on `IConsumerCommon`** (Java `seek(tp, long)`) —
    **breaking:** the shipped `Task Seek(TopicPartition, long, CancellationToken)` on
    `IAsyncConsumer` is **removed** (async→sync, and moves down onto `IConsumerCommon`).
    Calls the sync ABI `Consumer_seek` **directly** (not the async bridge).
  - **`void Seek(TopicPartition, OffsetAndMetadata)` on `IConsumerCommon`** — **NEW** overload
    (Java `seek(tp, OffsetAndMetadata)`); calls the sync ABI `Consumer_seek_with_metadata`.
    `leaderEpoch = LeaderEpoch ?? -1` (the ABI's "no epoch" sentinel); `Metadata` is never-null
    (ctor-coerced to `""`), always pinned + passed (Python's `offset.metadata or ""`).
  - **`long? CurrentLag(TopicPartition)` on `IConsumerCommon`** (Java `currentLag(tp)` →
    `OptionalLong`) — a genuine non-blocking local read; the ABI returns `bool` + `out int64`,
    and **`false` → `null`** (unknown lag OR concurrent-guard-rejection, **both**; Python
    parity — **no** `InvalidOperationException` concurrent-read split, unlike the sync state
    reads).
  - **Q1 = KEEP** the Java-fidelity negative-offset guard on `Seek(tp, long)`:
    `ArgumentOutOfRangeException` with the exact message `"seek offset must not be a negative
    number"`, thrown **before** the P/Invoke even when the consumer is closed (the argument
    check precedes `ThrowIfClosed`). The one place .NET is deliberately stricter than Python
    (whose sync seek does no offset validation). `Seek(tp, OffsetAndMetadata)` needs no offset
    guard — the `OffsetAndMetadata` ctor already rejects negative offset (`"Invalid negative
    offset"`).
  - **Q2 = REMOVE** the now-dead `NativeConsumer.SeekWithCallback` and the `ConsumerSeekAsync`
    `[DllImport]`. The Rust `Consumer_seek_async` symbol stays in the header (Rust-owned;
    Mode A = no Rust change); C# simply stops declaring it.
  - **`NativeConsumer`**: new sync `Seek(topic, partition, offset)` /
    `SeekWithMetadata(topic, partition, OffsetAndMetadata)` / `CurrentLag(topic, partition)`,
    each following the shipped `EnforceRebalance` / `CommitAsync` / `UpdateOffset` sync-op
    discipline (preconditions → `ThrowIfClosed()` → call-scoped `Utf8Marshal.Pin` → P/Invoke →
    `KafkaException.FromHandle` throw-iff-non-null; no `GCHandle`, no bridge, no
    `CancellationToken`). **`NativeMethods`**: `ConsumerSeek` / `ConsumerSeekWithMetadata`
    (both return `IntPtr` = `KafkaError*`) + `ConsumerCurrentLag` (`[return: MarshalAs(I1)]
    bool`, `out long`). Both client classes forward all three; `IAsyncConsumer` drops the async
    `Seek`.
  - **§4 divergence (documented — CLAUDE.md §8 / PLAN §8):** `seek` **blocks** in Java, so §4
    would map it to a `Task`; shipped **sync** anyway because (a) Python exposes `seek`
    synchronously, (b) `seek_with_metadata` has no `_async` ABI variant, and (c) a sync method
    calling the sync ABI **directly** (no `Task.Run`) is legitimate — not the sync-over-async
    footgun. The caller parks in the core's `block_on` (deadlock-free, ffi §B1), exactly as the
    shipped `EnforceRebalance` / `CommitAsync` sync-op paths.
  - **Tests:** migrated every async `.Seek(...)` / `SeekWithCallback(...)` caller to the sync
    form (public + interop; the interop `MockReadyToPoll` helpers keep their awaiting callers
    via `Task.FromResult`); the seek-unassigned case re-expressed as a **synchronous**
    `KafkaException` (the void bridge stays proven by subscribe/unsubscribe SUCCESS + the
    error-path mechanism by poll/position); removed the Seek pre-canceled-token interop test
    (sync Seek has no `CancellationToken`; the path stays covered by the surviving Subscribe
    one). **New `PublicConsumerSeekLagTests.cs`**: `Seek(tp,long)` offset round-trip via
    `Position`; `Seek(tp,OffsetAndMetadata)` offset round-trip + metadata/leader-epoch
    **marshalling** coverage (epoch 7 / null→-1, non-ASCII, empty `""`) — **the mock's
    `seek_with_metadata` discards metadata + leader_epoch** (`src/consumer/mock_consumer.rs:746-755`),
    so their values are **not observable broker-free**; the test asserts the offset + that
    marshalling succeeds (the M5/P4 `Committed` value read-back precedent); `CurrentLag` real
    value 90 (`Assign → UpdateEndOffset(100) → Seek(10)`) + assigned-no-end 0 + unassigned
    `null`; all preconditions + exact messages + post-dispose; unassigned seek → synchronous
    `KafkaException` (both overloads); `IConsumerCommon`-reference reachability; a per-op
    allocation budget. **261 → 282** tests, green on net10.0, stable across 10 runs.
  - **Doc-sync (DoD §1):** CLAUDE.md §1 status (`current_lag`/`seek_with_metadata` now
    **shipped sync**, only `close_with_timeout` remains a gap); §3 sketch (both `Seek` overloads
    + `CurrentLag` moved into the `IConsumerCommon` block, dropped from `IAsyncConsumer`,
    "Already wired" prose updated); §4 idiom-map row (`seek`/`currentLag` removed from the
    blocking-async trigger); §4 "Stays sync — exactly these" (both `Seek` + `CurrentLag` added)
    + a new **§4 divergence** note.
  - **DoD:** `cargo build --features ffi` (no header delta, diffed) → `dotnet build` 0/0 across
    all library (ns2.0/net8.0/net10.0) + test (net462/net8.0/net10.0) TFMs → net10.0 tests green
    (net8.0 *run* needs the .NET 8 runtime not installed locally + net462 are CI/Windows-only;
    all three *build* legs pass locally) → `dotnet format --verify-no-changes` clean. No
    TODO/FIXME; Apache-2.0 header on the new test file.
  - Approved plan: `design/history/M5/P7-consumer-sync-seek-lag/PLAN.md`. Commits on
    `prashah_dev_public_consumer_remaining` (the M5 branch), as a new PR for M5/P7. N=16.

- **Milestone 5 / Phase 6 — "Consumer commit" (Category D): DONE (2026-08-06).**
  The commit family — the three public members plus one public constructor. **Mode A (no
  Rust authored):** all three ABI fns (`commit_sync_async` / `commit_sync_offsets_async` /
  `commit_async`) verified present in the header; no ABI change. **Python-parity scope: NO
  `OffsetCommitCallback` variant, NO `TimeSpan` overload** (both Python out-of-scope).
  Delivered:
  - **Two confirming `Task Commit(...)` overloads on `IAsyncConsumer`** (Java `commitSync` /
    `commitSync(Map)`; Python `commit()`): `Commit(CancellationToken)` →
    `commit_sync_async`, and `Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>,
    CancellationToken)` → `commit_sync_offsets_async`. Both are **async-bridged over the void
    `op_callback_t`**, reusing `SubmitVoidOperation` + `ConsumerCallbacks.Operation`
    **verbatim** (the subscribe/seek precedent — **NO new bridge / callback**; `ConsumerCallbacks.cs`
    byte-for-byte unchanged, diff-verified).
  - **`void CommitAsync()` on `IConsumerCommon`** (user-resolved placement, PLAN §5; Java
    `commitAsync`; Python `commit_async()`): the fire-and-forget commit is **flavor-independent**
    (always sync `void`), so it lives on the shared non-blocking base a future sync `IConsumer`
    inherits for free. **Sync-returning** over `commit_async` (`KafkaError*`), **structurally
    identical to the shipped `EnforceRebalance`** — `ThrowIfClosed()` then
    `KafkaException.FromHandle(...)` throw-iff-non-null; no pin, no `GCHandle`, no bridge, **no
    `CancellationToken`** (nothing to cancel). `IConsumerCommon`'s first data-plane member (a
    documented, accepted mild widening of its charter).
  - **`OffsetAndMetadata` public constructor** `(long offset, string? metadata = null, int?
    leaderEpoch = null)` — mirrors Java's canonical ctor validation exactly: **negative offset
    → `ArgumentOutOfRangeException` with Java's message `"Invalid negative offset"`** (asserted,
    DoD §3), null metadata coerced to `""` (Java `NO_METADATA`), `leaderEpoch` passed through.
    Param order matches Java's 2-arg `(offset, metadata)` + Python's `(offset, metadata="",
    leader_epoch=None)`.
  - **Offsets-input marshaller `WithPinnedCommitOffsets`** (the new work): the 5-parallel-array
    shape (`topics` / `partitions` / `offsets` / `leader_epochs` / `metadata` + `count`) with
    **two** string arrays (topics + metadata) — both pinned **call-scoped**, all pins released
    in **one `finally`** (no leak); the three numeric arrays blittable, passed straight through;
    **no per-element copy beyond the UTF-8 encode**. A **new parallel helper** (the "clone, don't
    generalize" discipline) so the shipped M5/P3–P5 `WithPinnedTopics` pin paths stay untouched.
    Fed by **`SnapshotCommitOffsets`** (validate + snapshot before any pin/P-Invoke): null map →
    `ArgumentNullException`; null element topic → `ArgumentException`; negative partition →
    `ArgumentOutOfRangeException`; null value → `ArgumentException`; null `LeaderEpoch` → the
    `-1` "no epoch" sentinel (Python `_commit_spec` convention); metadata never-null.
  - **`NativeConsumer`**: `CommitWithCallback(CT)`, `CommitWithCallback(offsets, CT)`,
    `CommitAsync()` + the two helpers. **`NativeMethods`**: three new `[DllImport]`s
    (`Consumer_commit_sync_async` / `_commit_sync_offsets_async` / `_commit_async`, full ABI
    `EntryPoint`s). Both client classes forward all three.
  - **The marquee test — the non-empty `Committed` round-trip** (unblocks E1's deferred
    assertion): `Assign([tp]) → Commit({tp: new OffsetAndMetadata(42, "meta-x", 7)}) →
    Committed({tp})` reads offset 42, metadata `"meta-x"`, **and leader epoch 7** back — the
    epoch round-trips **faithfully** (verified against `src/ffi/consumer.rs` `read_offset_map`
    building `with_leader_epoch(offset, Some(7), meta)` + `src/consumer/mock_consumer.rs` storing
    and cloning it back for an assigned TP). Not a documented limit — a true 7-in-7-out. Plus:
    the null-metadata/null-epoch variant (`""` / `null` read-back); `Commit()`/`Commit(empty)` /
    `CommitAsync()` broker-free; preconditions (null offsets, `default(TopicPartition)` null
    topic, negative partition via the `TopicPartition` ctor, null value); the ctor (negative
    offset message, null-metadata coercion, all-set, zero boundary); post-dispose on all three;
    pre-canceled → `OperationCanceledException` on both `Commit` overloads.
    **`PublicConsumerCommitTests.cs`** (20 tests, **241 → 261**), all green on net10.0.
  - **Doc-sync (PLAN §7):** CLAUDE.md §4 "Exception — Java sync/async pairs" note **rewritten**
    to the new mapping (`Task Commit(...)` = confirming/async-bridged; `void CommitAsync()` =
    fire-and-forget/sync `void`; the idiom-map reads Java's *blocking behavior*, not its name —
    `commitSync` blocks → `Task`, `commitAsync` non-blocking → sync `void`; exact Python parity;
    the old `CommitSync` blocking-façade member removed); §3 sketch updated (the two `Commit`
    overloads on `IAsyncConsumer`, `CommitAsync` on `IConsumerCommon`) + the commit family moved
    from "Still to come" to "Already wired"; the shipped `IAsyncConsumer.cs` not-yet-wired
    doc-comment dropped the commit family (pattern-subscribe + rebalance-listener remain).
  - **Deviations (recorded, COMMENTS.DONE.15):** (a) `OffsetAndMetadata` **collapsed to ONE
    public ctor** — the planned public `(long, string?, int?)` + shipped internal `(long,
    string, int?)` collide under CS0111 (`string`/`string?` are the same overload type); the
    public ctor is the single field-assignment site and the receive-path marshaller now calls it
    (safe — it always passes non-null metadata + non-negative offset, for which validate/coerce
    is a no-op); (b) an **operational `KafkaException` commit fault is NOT reachable broker-free**
    on the mock (`commit_async_impl` only fails on a closed consumer, intercepted as
    `ObjectDisposedException` before native) — a documented D-Q4 reachability limit, the
    faulted-`Task` mechanism already proven by E1; (c) negative-partition precondition asserted
    via the `TopicPartition` ctor guard (a negative partition cannot reach `SnapshotCommitOffsets`
    through a constructed struct — the shipped offset-query precedent).
  - **No-new-bridge audit:** `ConsumerCallbacks.cs` and the shipped void/owned/scalar/E1/E2
    bridges **byte-for-byte unchanged** (diff-verified); both commit-offsets string arrays pinned
    call-scoped + released in `finally`; the sync `CommitAsync` frees the error handle exactly
    once via `FromHandle`; no per-element byte copy beyond the UTF-8 encode.
  - **DoD:** `cargo build --features ffi` (no ABI change) → `dotnet build` 0/0 across all library
    (ns2.0/net8.0/net10.0) + test (net462/net8.0/net10.0) TFMs → net10.0 tests green (net8.0 *run*
    + net462 are CI/Windows-only; all three *build* legs pass locally) → `dotnet format
    --verify-no-changes` clean. CS1591 on the 2 `Commit` overloads + `CommitAsync` + the ctor;
    Apache-2.0 header on the new test file; no TODO/FIXME.
  - Approved plan + closed record: `design/history/M5/P6-consumer-commit/`. Commits on
    `prashah_dev_public_consumer_remaining` (the M5 branch), as a new PR for M5/P6. N=15.

- **Milestone 5 / Phase 5 — "Consumer partition-metadata queries" (Category E2): DONE
  (2026-08-06).** The two async partition-metadata queries on `IAsyncConsumer` —
  `PartitionsFor(string)` (`IReadOnlyList<PartitionInfo>`) and `ListTopics()`
  (`IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>`, no input) — plus the binding's
  **first NESTED public value types** `PartitionInfo` / `Node`. **Completes Category E** (the
  consumer query family). **Mode A (no Rust authored):** the two `_async` fns, both container
  types (`PartitionInfoList_t` / `TopicPartitionInfoMap_t`) + accessors, the `PartitionInfo_t`
  accessors, the `Node_t` (`kafka_common_Node`) accessors, the two callbacks, and the
  `MockConsumer_update_partitions` mock helper all already ship; no ABI change. Reused the E1
  owned-handle bridge; the only new dimension is the **depth** of the copy-out (a 2-to-3-level
  tree vs E1's flat map). Delivered:
  - **Two NESTED public value types** (`Confluent.Kafka` root, `public sealed class`,
    getter-props, full XML docs, the `OffsetAndTimestamp` precedent): `Node
    { int Id; string Host; int Port; string? Rack }` (Java `org.apache.kafka.common.Node`,
    `ToString` = `"host:port (id: N rack: R)"`, absent rack → `"null"`) and `PartitionInfo
    { string Topic; int Partition; Node? Leader; IReadOnlyList<Node> Replicas / InSyncReplicas
    / OfflineReplicas }` (Java `org.apache.kafka.common.PartitionInfo`, `ToString` mirrors
    Java's `Partition(topic=…, partition=…, leader=…, replicas=[ids], isr=[ids],
    offlineReplicas=[ids])`). **`ToString` only, NO `IEquatable`** (E1 value-type precedent —
    query-result values, not dict keys; recorded deviation). `Leader` nullable (ABI `_leader`
    may be null); `Rack` nullable (ABI `_rack` returns `(null, -1)` when absent).
    `PartitionInfo` is the binding's **first public type composed of another public type**.
  - **Four layered copy-out marshallers** (`Internal/Interop/`): shared **`NodeMarshal`**
    (`Node_t` → `Node`) → **`PartitionInfoMarshal`** (reuses `NodeMarshal` for the leader + 3
    replica lists) → reused by **`PartitionInfoListMarshal`** (→ `IReadOnlyList<PartitionInfo>`)
    and **`TopicPartitionInfoMapMarshal`** (→ `IReadOnlyDictionary<string,
    IReadOnlyList<PartitionInfo>>`, reusing `PartitionInfoListMarshal` per entry — the nested
    borrowed list copied out too). The whole tree is copied into owned managed values on the
    dispatcher thread BEFORE the single root `_destroy`; empty results →
    `Array.Empty<PartitionInfo>()` (lists) / the E1 `EmptyReadOnlyDictionary` singleton (map).
  - **Borrow discipline (§B2 Cat-3/4 — the central memory-safety risk):** every `PartitionInfo`
    / `Node` / nested list is a **borrowed Category-4 view** and is **NEVER freed**; only the
    ROOT container is `_destroy`d, exactly once, in the trampoline `finally`. Structurally
    enforced: **no `Node_destroy` exists**, and `PartitionInfo_destroy` is **deliberately NOT
    declared** in `NativeMethods` — so a borrowed-element free is not even expressible.
  - **String forms (§B3 — the one shape difference from E1):** `Node.host` / `Node.rack` are
    **LENGTH-DELIMITED** (`const char*` + `out int32_t len`) → `Utf8Marshal.PtrToString(ptr,
    len)`, **never NUL-scan** (the over-read trap); `rack` absent `(null, -1)` → `Node.Rack ==
    null`. `PartitionInfo.topic` and `TopicPartitionInfoMap_get_topic` are **NUL-terminated** →
    `Utf8Marshal.PtrToString(ptr)`. Both forms coexist in one tree — the matching overload per
    accessor.
  - **Two `OnPoll`-clone trampolines** in `ConsumerCallbacks` (`OnPartitionsFor` /
    `OnListTopics`) — differing from `OnCommitted` only in the result type, the copy-out
    marshaller, and which root `_destroy` runs in the `finally`. Every `OnPoll` invariant
    verbatim (no-throw boundary, copy-out on the dispatcher thread BEFORE `_destroy`, root
    `_destroy` null-safe in the `finally`, error via `Complete` freeing the error handle, per-op
    `GCHandle` freed once, `RunContinuationsAsync`). Each ABI callback typedef gets its own
    delegate type (self-documenting DllImports).
  - **`NativeConsumer`**: `PartitionsForWithCallback(string, CT)` (pins its one topic
    call-scoped via `Utf8Marshal.Pin` — not the array-shaped `WithPinnedTopics`) and
    `ListTopicsWithCallback(CT)` (no input), both over the E1 `SubmitOwnedHandleOperation<T>`
    helper (the proven poll / void / scalar / E1 submit paths left **byte-for-byte untouched** —
    diff-verified zero deletions to `ConsumerCallbacks` / `NativeConsumer` / `NativeMethods`).
    Plus the `UpdatePartitions` mock forwarder.
  - **Mock wire (`MockConsumer_update_partitions`, the last Python-parity mock gap):** a
    `NativeMethods` DllImport + a `NativeConsumer.UpdatePartitions` + an inherent
    `AsyncMockConsumer.UpdatePartitions(topic, partitionCount, leaderId, leaderHost,
    leaderPort)` (the M5/P3 `Update*Offset` pattern) so `PartitionsFor` / `ListTopics` return
    data broker-free.
  - **`IAsyncConsumer`**: the two members with full XML docs (CS1591); both `AsyncKafkaConsumer`
    + `AsyncMockConsumer` forward. Doc-sync: dropped `partitionsFor` / `listTopics` from the
    additive-growth "not-yet-wired" remark (only the commit family + pattern subscribe + the
    rebalance listener remain); added `UpdatePartitions` to the `AsyncMockConsumer`
    mock-only-helpers remark; CLAUDE.md §3 sketch updated (these + the two value types now
    wired).
  - **API shape (PLAN §1/§3, user-locked):** names mirror Java (no `Async` suffix); Java `List`
    → `IReadOnlyList`, `Map<String,List>` → `IReadOnlyDictionary<string,
    IReadOnlyList<PartitionInfo>>`. **One method each, NO `TimeSpan` overload** (the async ABI
    has no timeout — the `Position`/`Close` precedent); the `CancellationToken` is **user
    cancellation → `wakeup()`, NOT a deadline**; pre-canceled → `OperationCanceledException`
    synchronously. Preconditions BEFORE any pin/P-Invoke (§B5): `PartitionsFor(null)` →
    `ArgumentNullException`; **`PartitionsFor("")` is FORWARDED to the core, NOT rejected**
    (Java/Python-faithful — Python does zero topic validation; the binding guards only `null`
    for FFI panic-safety, PLAN §8.2); `UpdatePartitions` null topic/host →
    `ArgumentNullException`, negative count → `ArgumentOutOfRangeException`.
  - **Reachability (PLAN §6, documented not silently skipped):** with `update_partitions` wired,
    the mock builds each partition with a single leader `Node` that is also its sole replica +
    in-sync replica (`offline=[]`, no rack). **Data-testable broker-free:** `Topic`,
    `Partition`, `Leader` (id/host/port), `Replicas[0]`, `InSyncReplicas[0]`. **Documented-empty
    (mock limit, not a silent gap):** `OfflineReplicas` (always empty), `Node.Rack` (always
    null) — their marshaller paths still exercised structurally (empty list / null). **No clean
    broker-free operational-failure path** (the mock's queries always return a valid list/map; a
    real consumer against an unreachable broker retries past the 30 s hang guard — not
    deterministic), so the faulted-`Task` assertion for E2 is a **documented reachability
    limit**: the faulted-`Task` MECHANISM is identical to the E1 offset-map bridges and already
    proven there (`BeginningOffsets`/`EndOffsets` unset-partition + `OffsetsForTimes`
    unsupported-version faults). Concurrent-op fault is the D-Q4 non-blockable-mock ceiling.
  - **Tests:** `PublicConsumerPartitionMetadataTests.cs` (public surface: reachable data direct
    + via interface; empty list / empty map; empty-topic-forwarded; non-ASCII topic via the
    NUL-scan form + non-ASCII leader host via the length-delimited form on both leader and
    replica; preconditions; post-dispose; pre-canceled; wakeup-usable; reusable-after-op;
    per-op alloc sanity net8+ via process-wide `GetTotalAllocatedBytes` + marginal measurement)
    + `PublicConsumerPartitionMetadataValueTypeTests.cs` (field storage, nullable Rack/Leader at
    the value level, Java-mirroring `ToString` incl. absent-rack `"null"` / null-leader
    `"none"`) + a `PartitionsFor`/`ListTopics` leg folded into `PublicConsumerTfmSmokeTests`.
    **214 → 241 tests**, all green on net10.0 across **5/5** full serial runs (D8.8 gate
    stable); the alloc-budget test stable 6/6 in isolation. All 6 TFM build legs (library
    ns2.0/net8.0/net10.0 + tests net462/net8.0/net10.0) clean, 0 warnings; `dotnet format
    --verify-no-changes` clean.
  - **Deviations (recorded, COMMENTS.DONE.14):** (a) the value types carry `ToString` but no
    `IEquatable` (query-result values, not keys — PLAN §8.1); (b) the operational-failure
    faulted-`Task` end-to-end assertion is a documented reachability limit (no broker-free fault
    path; mechanism proven by E1) — the planned real-consumer fault test was removed because it
    hangs past the 30 s guard against an unreachable broker; (c) `PartitionsFor("")` forwarded,
    not rejected (PLAN §8.2, user-locked); (d) `SubmitOwnedHandleOperation<T>` reused verbatim
    (no new submit helper this phase); (e) each callback typedef its own delegate type
    (self-documenting), as in E1.
  - **DoD:** `cargo build --features ffi` (no ABI change) → `dotnet build` 0/0 across all
    library + test TFMs → net10.0 tests green (net8.0 *run* + net462 are CI/Windows-only; all
    three *build* legs pass locally) → `dotnet format --verify-no-changes` clean. CS1591 on the
    2 members + 2 value types; Apache-2.0 header on every new file; no TODO/FIXME.
  - Approved plan + closed record: `design/history/M5/P5-consumer-partition-metadata/`. Commits
    on `prashah_dev_public_consumer_remaining` (the M5 branch), as a new PR for M5/P5. N=14.

- **Milestone 5 / Phase 4 — "Consumer offset-map query siblings" (Category E1): DONE
  (2026-08-06).** The four async offset-map queries on `IAsyncConsumer` — `Committed`
  (`IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>`), `OffsetsForTimes`
  (`…, OffsetAndTimestamp>`, the one **map-input** query), `BeginningOffsets` / `EndOffsets`
  (`…, long>`) — plus the two new public value types `OffsetAndMetadata` /
  `OffsetAndTimestamp`. **Mode A (no Rust authored):** all four `_async` fns + their three
  container types (`OffsetMap_t` / `OffsetAndTimestampMap_t` / `LongOffsetMap_t`) + accessors
  + the two value types + the three callbacks already ship; no ABI change. The owned-handle
  **map** copy-out bridge (the `OnPoll` template, cloned once per container type). Delivered:
  - **Two public value types** (`Confluent.Kafka` root, `sealed class`, getter-props, full
    XML docs, the `ConsumerGroupMetadata` precedent): `OffsetAndMetadata { long Offset; string
    Metadata (non-null, "" = unset); int? LeaderEpoch }` and `OffsetAndTimestamp { long Offset;
    long Timestamp; int? LeaderEpoch }`. **`LeaderEpoch` maps the ABI presence-flag** (an `I1`
    `bool` return + `out int32`) → `int?` (false ⇒ null, true ⇒ epoch — honored, not
    hardcoded). Both carry `ToString()`; no `IEquatable` (they are dictionary *values*, not
    keys) — recorded deviation.
  - **`NativeMethods`**: 4 `_async` void DllImports (`committed` / `offsets_for_times` /
    `beginning_offsets` / `end_offsets`); 3 container types × (`count` / `get_key` / `get_value`
    / `destroy`); 2 value-type accessors × (`offset` / `metadata|timestamp` / `leader_epoch`
    with `[MarshalAs(I1)] bool` + `out int`). Reused the shipped `TopicPartition_topic` /
    `_partition` for the map keys. The value-type `_destroy` accessors are **deliberately NOT
    declared** — they are borrowed map elements (Category 4), never freed by the binding
    (structurally prevents a borrowed-element free).
  - **3 owned-handle trampolines in `ConsumerCallbacks`** (`OnCommitted` / `OnOffsetsForTimes`
    / `OnLongOffsets`) — `OnPoll` clones differing only in the result type, the copy-out
    marshaller, and which container `_destroy` runs in the `finally`. Every `OnPoll` invariant
    verbatim: no-throw boundary, copy-out on the dispatcher thread BEFORE `_destroy`, map root
    `_destroy` null-safe in the `finally` (no-op on the failure/null path), error via
    `Complete` (frees the error handle), per-op `GCHandle` freed once, `RunContinuationsAsync`.
    `OnLongOffsets` is **shared** by `beginning`/`end` (they share `long_offsets_callback_t`).
    Each callback typedef gets its own delegate type (distinct C fn-pointer types, self-
    documenting DllImport parameters).
  - **3 copy-out marshallers** (`OffsetMapMarshal` / `OffsetAndTimestampMapMarshal` /
    `LongOffsetMapMarshal`) + a shared `OffsetMapMarshalShared` (the `TopicPartition_t` key
    copy-out + the leader-epoch presence-flag decode, reused by all three) +
    `EmptyReadOnlyDictionary<K,V>` (the `Array.Empty` analog for the empty-map path — ns2.0 has
    no built-in). Borrow discipline (§B2 Category 4): the map key/value **elements** are
    borrowed and never freed; only the map root is destroyed (by the trampoline, after
    copy-out). Metadata/topic strings via the **NUL-terminated** `Utf8Marshal.PtrToString`
    (§B3), copied out before the root destroy.
  - **`NativeConsumer`**: `CommittedWithCallback` / `OffsetsForTimesWithCallback` /
    `BeginningOffsetsWithCallback` / `EndOffsetsWithCallback` over a new
    `SubmitOwnedHandleOperation<T>` parallel submit helper (a clone of `SubmitOperation<T>`
    passing only `(consumer, userData)`; each op closes over its own strongly-typed rooted
    callback at the call site) — **the proven poll / void / scalar submit paths are left
    byte-for-byte untouched** (PLAN §4.2; diff-verified: zero deletions to `SubmitOperation` /
    `SubmitScalarOperation` / `SubmitVoidOperation` / `OnPoll` / `ConsumerCallbacks.Poll`).
    `WithPinnedTopicsAndTimestamps` variant for the one map-input query (adds a blittable
    `long[]` timestamps). Shared `SnapshotPartitions` / `ExtractPartitions` validate+snapshot
    the TP collection once (§B5) — `SubmitPartitionOp` refactored to reuse them, so the
    validation is not copy-pasted (PLAN §3), behavior-identical.
  - **`IAsyncConsumer`**: the four members with full XML docs (CS1591); dropped `committed` /
    `beginningOffsets` / `endOffsets` / `offsetsForTimes` from the additive-growth
    "not-yet-wired" remark (commit family / `partitionsFor`·`listTopics` / pattern subscribe
    remain). Both `AsyncKafkaConsumer` + `AsyncMockConsumer` forward each. Doc-sync: CLAUDE.md
    §3 sketch prose updated (these + the two value types are now wired).
  - **API shape (PLAN §2/§3/§5, all user-locked):** names mirror Java (no `Async` suffix);
    Java `Map` → `IReadOnlyDictionary`, `Long` values → `long`, `Set`/`Collection` inputs →
    `IReadOnlyCollection<TopicPartition>`; the `Map<TP,Long>` input →
    `IReadOnlyDictionary<TopicPartition,long>`. **One method each, NO `TimeSpan` overload**
    (the async ABI has no timeout — the `Position`/`Close` precedent). The `CancellationToken`
    is **user cancellation → `wakeup()`, NOT a deadline**; pre-canceled →
    `OperationCanceledException` synchronously. Preconditions BEFORE any pin/P-Invoke (§B5):
    null collection/map → `ArgumentNullException`; null element/key topic → `ArgumentException`;
    negative partition → `ArgumentOutOfRangeException` (the `TopicPartition` ctor guard). A
    **negative timestamp** in `OffsetsForTimes` is a Kafka-valid sentinel — **accepted**, not
    rejected. **Empty input** is a valid pass-through for the collection queries;
    `OffsetsForTimes({})` still faults on the mock (the FFI does not short-circuit empty —
    verified in `src/ffi/consumer.rs`).
  - **Reachability (PLAN §6, documented not silently skipped):**
    `BeginningOffsets`/`EndOffsets` **fully data-testable** broker-free (shipped
    `UpdateBeginningOffset`/`UpdateEndOffset`) — non-empty round-trip + unset-TP `illegal_state`
    faulted (message asserted). `Committed` **empty-only** broker-free (the mock's committed
    map is populated only by the not-yet-wired commit-with-offsets family) — empty + faulted
    paths tested; the **non-empty end-to-end data test is deferred to the commit-family phase**
    (which reuses `OffsetAndMetadata`). `OffsetsForTimes` **faulted-only** — the mock returns
    `unsupported_version` unconditionally (mirrors Java's not-implemented `MockConsumer`);
    tested the faulted `Task` + message + code 35. The **non-empty `OffsetMap_t` /
    `OffsetAndTimestampMap_t` copy-out** (incl. the value-type `LeaderEpoch` presence flag
    through a borrowed element) is **not reachable broker-free** (no ABI container constructor;
    mock `committed` empty; `offsets_for_times` errors) — **deferred, documented**; the
    presence-flag decode is unit-tested directly (`OffsetMapMarshalShared.ReadLeaderEpoch`), the
    value types' `int?` mapping at the value level, and the valid non-empty `LongOffsetMap_t`
    copy-out end-to-end via `Beginning`/`EndOffsets`.
  - **Finding — the container `_count`/`_get` accessors are NOT null-safe** (only `_destroy`
    is): the ABI safety contract says "`map` must be a valid handle". The production path is
    correct (the trampolines call `CopyOut` only on the success branch, where the map is
    guaranteed non-null; the failure branch has `map == null` and calls only the null-safe
    `*Destroy`). A first cut of the marshaller unit tests passed `IntPtr.Zero` to `CopyOut` and
    crashed the host (`box_offset_map` null deref); removed those contract-violating cases (the
    empty/non-empty valid-container paths are covered end-to-end instead).
  - **Tests:** `PublicConsumerOffsetQueryTests.cs` (test root) + `PublicConsumerOffsetValueTypeTests.cs`
    (value types) + `Interop/OffsetMapMarshalTests.cs` (presence-flag decode +
    `EmptyReadOnlyDictionary`) + the four members folded into `PublicConsumerTfmSmokeTests`.
    **175 → 214 tests**, all green on net10.0 across **10/10** full runs (D8.8 serial gate
    stable). All 6 TFM build legs (library ns2.0/net8.0/net10.0 + tests net462/net8.0/net10.0)
    clean, 0 warnings; `dotnet format --verify-no-changes` clean.
  - **Deviations (recorded, COMMENTS.DONE.13):** (a) `SubmitOwnedHandleOperation<T>` added as a
    parallel submit helper (the "clone" option) rather than generalizing `SubmitOperation<T>`'s
    signature — leaves the proven poll path byte-for-byte untouched (PLAN §4.2 permitted
    either); (b) three distinct callback delegate types (one per ABI typedef) rather than one
    shared `(IntPtr,IntPtr,IntPtr)` delegate — self-documenting DllImport parameters, no
    behavioral difference; (c) the value types carry `ToString` but no `IEquatable` (dictionary
    *values*, not keys — PLAN §1 left this to the Actor); (d) the non-empty
    `OffsetMap`/`OffsetAndTimestampMap` copy-out + `Committed` non-empty data + `OffsetsForTimes`
    data are not reachable broker-free (documented reachable slices, not skipped); (e) the
    container-accessor null-safety finding (production correct; unit tests corrected).
  - **DoD:** `cargo build --features ffi` (no ABI change) → `dotnet build` 0/0 across all
    library + test TFMs → net10.0 tests green (net8.0 *run* + net462 are CI/Windows-only; all
    three *build* legs pass locally) → `dotnet format --verify-no-changes` clean. CS1591 on the
    4 members + 2 value types; Apache-2.0 header on every new file; no TODO/FIXME.
  - Approved plan + closed record: `design/history/M5/P4-consumer-offset-queries/`. Commits on
    `prashah_dev_public_consumer_remaining` (the M5 branch), as a new PR for M5/P4. N=13.

- **Milestone 5 / Phase 3 — "Consumer partition ops": DONE (2026-08-06).** Five void-async
  members on `IAsyncConsumer`, all `Task <Op>(IReadOnlyCollection<TopicPartition>, CancellationToken)`:
  `Assign` / `Pause` / `Resume` / `SeekToBeginning` / `SeekToEnd`. **Mode A (no Rust
  authored):** all five `_async` fns + the shared void `op_callback_t` + the two
  `MockConsumer_update_*_offsets` helpers already ship; no ABI change. **NO new completion
  bridge this phase** (unlike M5/P2's scalar bridge) — all five reuse the proven **void**
  bridge (`SubmitVoidOperation` + `ConsumerCallbacks.Operation`) **verbatim**; the only new
  managed work is marshalling a `TopicPartition` collection into the ABI's parallel arrays.
  Delivered:
  - **`NativeConsumer`**: five thin `<Op>WithCallback(IReadOnlyCollection<TopicPartition>,
    CT)` methods, each `=> SubmitPartitionOp(partitions, ct, NativeMethods.Consumer<Op>Async)`.
    One shared `SubmitPartitionOp` validates the collection preconditions (§B5) BEFORE any
    pin/P-Invoke, snapshots the `(topic, partition)` pairs, then runs the **unchanged**
    `SubmitVoidOperation` with a submit lambda that marshals via the shared
    `WithPinnedTopics`. One `NativePartitionOpSubmit` delegate binds a method-group ref to
    each `_async` DllImport.
  - **Shared collection→parallel-array marshaller (`WithPinnedTopics`)**: extracted from the
    shipped sync `Assign`'s pinning; used by **all five async ops AND the retained internal
    sync `Assign`** (no duplicated pinning). Pins `count` topics **call-scoped** (freed at
    submit return — the core copies during the call, verified for both `Consumer_assign` and
    each `_async`'s `read_topic_partitions`; never held across the `Task`), fills the
    `IntPtr[]` topics + passes the blittable `int[]` partitions straight through — **no
    per-element copy beyond the UTF-8 encode** (§A3/§A4).
  - **`NativeMethods`**: five `_async` void DllImports (each `(IntPtr[] topics, int[]
    partitions, int count, OperationCallback, IntPtr userData)`, reusing the shipped
    `op_callback_t` = `OperationCallback` — no new delegate) + the two per-`(topic, partition,
    offset)` mock offset helpers (`MockConsumer_update_beginning/end_offsets`).
  - **Assign reconciliation (PLAN §1, user-locked):** promoted `Assign` to the **public async**
    member on `IAsyncConsumer` (via `assign_async`, broker-free on the mock); **REMOVED** the
    public inherent sync `AsyncMockConsumer.Assign(IReadOnlyList<TopicPartition>)` — no
    sync/async `Assign` overload footgun, one public `Assign` (Task). Migrated the **10**
    public-root test-setup sites from `consumer.Assign(...)` to `await consumer.Assign(...)`
    (every existing assertion kept; sync `void` test methods that used it became `async Task`).
    The **internal** sync `NativeConsumer.Assign((string,int)[])` + the `Consumer_assign`
    DllImport + the **4 `Interop/` tests** that use them are **untouched** (75 Interop tests
    still green).
  - **Mock offset helpers wired (§6.6):** `UpdateBeginningOffset` / `UpdateEndOffset` inherent
    forwarders on `AsyncMockConsumer` so `SeekToBeginning`/`SeekToEnd` are observed end-to-end
    via a follow-up poll (position reset to the beginning/end offset).
  - **`IAsyncConsumer`**: five new members with full XML docs (CS1591); `AsyncKafkaConsumer` +
    `AsyncMockConsumer` forward each to `_native.<Op>WithCallback`. Doc-sync: dropped
    `assign`/`pause`/`resume`/`seekTo*` from the `IAsyncConsumer` additive-growth
    "not-yet-wired" remark; updated the `AsyncMockConsumer` mock-only-helpers remark; lifted
    the M5/P1 `Paused()` "non-empty not reachable until a public Pause lands" note (now
    reachable); confirmed/updated the CLAUDE.md §3 sketch prose (these are now wired).
  - **Error / precondition mapping (§B5):** null collection → `ArgumentNullException`;
    per-element null topic → `ArgumentException`; negative partition →
    `ArgumentOutOfRangeException` (unreachable through a constructed `TopicPartition`, whose
    ctor rejects it — the `Seek`/`Position` precedent — but kept as defense-in-depth in
    `SubmitPartitionOp`). **Empty collection = valid pass-through** (`assign([])` clears,
    the others no-op) — never a spurious throw. Operational failure → faulted `Task` with
    `KafkaException`; concurrent → faulted (core-delivered `ConcurrentModification`);
    post-dispose → `ObjectDisposedException` (`ThrowIfClosed` before submit); pre-canceled
    token → `OperationCanceledException` synchronously (mirrors `SubscribeWithCallback`).
  - **Tests:** new `PublicConsumerPartitionOpsTests.cs` at the **test root** (public-surface,
    broker-free via `AsyncMockConsumer`): Assign→Assignment reflects it; Assign([]) clears;
    **Pause→Paused returns the paused set (closes the M5/P1 non-empty `Paused()` gap)**;
    Resume clears; SeekToBeginning/SeekToEnd resolve broker-free AND observed via poll (the
    new offset helpers); empty-collection no-op; a **deterministic** operational failure
    (`Pause` of an unassigned partition) asserting the `KafkaException` **message** content
    ("No current assignment for partition …", DoD §3) + consumer-reusable-after-fault;
    preconditions (null collection / per-element null topic / negative-partition-ctor-guard);
    post-dispose; pre-canceled token; wakeup-leaves-usable; per-op marshalling allocation
    sanity (net8+). **153 → 175 tests**, all green on net10.0 across **multiple** full runs
    (D8.8 serial gate stable). All 6 TFM build legs (library ns2.0/net8.0/net10.0 + tests
    net462/net8.0/net10.0) clean, 0 warnings; `dotnet format --verify-no-changes` clean.
  - **Deviations (recorded, COMMENTS.DONE.12):** (a) the deterministic failure path is
    `Pause` of an **unassigned** partition (a clean broker-free operational failure asserting
    the message), chosen over the non-deterministic concurrent-op path the PLAN left as a
    fallback; (b) the negative-partition precondition is asserted via the `TopicPartition`
    ctor guard (a negative value cannot reach the op through a constructed struct — the
    shipped `Position` precedent), with the binding's own `SubmitPartitionOp` check kept as
    defense-in-depth; (c) `WithPinnedTopics` takes a `Func<int, string>` topic accessor so
    the one helper serves both the tuple-form sync `Assign` and the `TopicPartition`-form
    async ops without a per-element copy.
  - Approved plan + closed record: `design/history/M5/P3-consumer-partition-ops/`. Commits on
    `prashah_dev_public_consumer_remaining` (the M5 branch), as a new PR for M5/P3. N=12.

- **Milestone 5 / Phase 2 — "Consumer `Position`": DONE (2026-08-06).** The single async
  member `Task<long> Position(TopicPartition, CancellationToken)` on `IAsyncConsumer` —
  the CLAUDE.md §3-sketch-committed shape (no shape change). Its real work is the **third
  completion-bridge shape: the scalar callback** `(int64_t, error*, ud)` — a result carried
  directly in the callback, with **no owned result handle** to marshal or free (distinct
  from the shipped void `op` bridge and the owned-handle `poll` bridge). **Mode A (no Rust
  authored):** `position_async` + `position_callback_t` already ship; no ABI change.
  Delivered:
  - **`ConsumerCallbacks`**: a new `PositionCallback` delegate + `OnPosition` trampoline,
    cloned from `OnPoll`, rooted in a `static readonly` field (§B6 keep-alive), no-throw
    foreign-thread boundary. **The one structural difference from `OnPoll`:** the `finally`
    frees **only** the per-op `GCHandle` (`FreeGcHandle()`) — **no `*Destroy`** call,
    because the scalar owns no result handle. The error handle (failure path) is still
    freed exactly once via `Complete → KafkaException.FromHandle`.
  - **`OperationCompletionSource<long>` reused verbatim** — NO new context type, NO edit to
    `OperationCompletionSource.cs` (it is already result-type-agnostic:
    `CompleteWithResult(position)` on success, `Complete(error)` on failure). The scalar is
    blittable, so "marshalling" is trivial — no copy-out, no native read.
  - **`NativeConsumer`** gains a parallel **`SubmitScalarOperation<T>` + `NativeScalarSubmit`**
    delegate type (a line-for-line clone of `SubmitOperation<TResult>` with `Poll →
    Position`) — added rather than generalizing the proven `SubmitOperation` (poll) /
    `SubmitVoidOperation` paths, which stay byte-for-byte untouched — plus
    `PositionWithCallback(TopicPartition, CancellationToken)` (preconditions before any
    native call; call-scoped topic pin via `Utf8Marshal.Pin`).
  - **`NativeMethods`**: one `[DllImport]` (`Consumer_position_async`, `IntPtr topic` = a
    pinned NUL-terminated UTF-8 buffer). The **sync `Consumer_position` is deliberately NOT
    declared** (async-only; wrapping it in `Task.Run` would be the forbidden
    sync-over-async, §B7).
  - **`IAsyncConsumer`** gains `Position` (full XML docs, CS1591); `AsyncKafkaConsumer` +
    `AsyncMockConsumer` forward to `_native.PositionWithCallback`. `position` removed from
    the `IAsyncConsumer` "not-yet-wired" remarks (doc-sync). The CLAUDE.md §3 sketch already
    showed `Position` — confirmed, no sketch change.
  - **API shape (decisions locked in PLAN §2/§3):** **one method, NO `TimeSpan` overload**
    — Java's timed `position(tp, Duration)` is deferred until a timed `position_async` ABI
    exists (the shipped `Close` precedent). The **`CancellationToken` is user-initiated
    cancellation only, NOT a timeout/deadline** — it maps to `wakeup()` (best-effort),
    mirroring `PollWithCallback`; a pre-canceled token throws `OperationCanceledException`
    synchronously. Error mapping (§B5): operational failure → faulted `Task<long>` with
    `KafkaException`; concurrent → faulted (`ConcurrentModification`, core-delivered);
    post-dispose → `ObjectDisposedException` (`ThrowIfClosed()` before submit); null topic
    → `ArgumentNullException`, negative partition → `ArgumentOutOfRangeException` (the
    `Seek`/`Assign` precedent), both before any pin/P-Invoke.
  - **Deviations (recorded, COMMENTS.DONE.11):** (a) reuse `OperationCompletionSource<long>`
    verbatim for the scalar bridge (no new context type, no bridge-file change); (b)
    `SubmitScalarOperation<T>` added as a parallel helper rather than generalizing
    `SubmitOperation` (to leave the proven poll path untouched); (c) one `Position` method,
    no `TimeSpan` overload (the `Close` precedent) — the `CancellationToken` is
    cancellation, not a timeout; (d) the sync `Consumer_position` deliberately not declared;
    (e) the D-Q4-style non-deterministic-concurrency + no-timed-ABI + non-check-and-clear-
    wakeup-on-position reachability limits (recorded, not silently skipped).
  - **Tests:** new `PublicConsumerPositionTests.cs` at the **test root** (public-surface, not
    under `Interop/`) — the full §6 list broker-free via `AsyncMockConsumer`: happy path
    (assign → seek → `Position` returns offset, direct + interface + non-ASCII topic pin),
    unassigned-partition **faulted `Task` with the asserted `KafkaException` message**
    (DoD §3) + reusable-after, pre-canceled token → `OperationCanceledException`, wakeup
    non-corruption (reachable seam), null topic → `ArgumentNullException`, negative
    partition → `ArgumentOutOfRangeException`, post-dispose → `ObjectDisposedException`,
    concurrency reachable-seam + inspection (D-Q4), per-op allocation sanity (net8.0+,
    per-RPC not zero-alloc). **141 → 153 tests** (12 new), all green across ≥4 full net10.0
    runs; serial execution (D8.8) unchanged; every awaited op under a `TestTimeout` guard.
  - **Free-exactly-once audit (§7.7):** the per-op `GCHandle` freed exactly once on every
    path (success / operational failure / inline core-rejection / no-throw catch /
    submit-threw via `AbandonBeforeSubmit`); the error handle freed once on failure (via
    `FromHandle`); **no result-handle destroy** in `OnPosition`'s `finally` (the one
    structural difference from `OnPoll`) — verified by inspection.
  - **DoD:** `cargo build --features ffi` (no ABI change) → `dotnet build` 0/0 across all
    library TFMs (netstandard2.0 / net8.0 / net10.0) + all test TFMs (net462 / net8.0 /
    net10.0) → net10.0 tests green (net8.0 *run* + net462 are CI/Windows-only; all three
    *build* legs pass locally) → `dotnet format --verify-no-changes` clean. CS1591 on the new
    public member; Apache-2.0 header on the one new file; no TODO/FIXME.
  - Approved plan + closed record: `design/history/M5/P2-consumer-position/`. Commits on
    `prashah_dev_public_consumer_remaining` (the M5 branch; M5/P1 already shipped there), as
    a new PR for M5/P2. N=11.
- **Milestone 5 / Phase 1 — "Consumer sync read surface (`Assignment` / `Subscription` /
  `Paused` / `EnforceRebalance`)": DONE (2026-08-06).** The four sync members CLAUDE.md §4
  names in its "stays sync" list that were still unshipped — the Category A sync state
  getters + Category H `enforce_rebalance`. **Mode A (no Rust authored):** all four ABI
  functions and both list types (`TopicPartitionList_t` / `StringList_t`) already ship in
  the generated header; no ABI change, no new op semantics. M5 opened as a **new
  milestone** for completing the consumer surface (the earlier tentative "M5 = sync
  `IConsumer` facade" reservation moves to a later milestone; this milestone grows the
  **async** surface via `IConsumerCommon`). Delivered:
  - **`NativeMethods`**: four `[DllImport]`s (`Consumer_assignment` / `_subscription` /
    `_paused` / `_enforce_rebalance`) + the `TopicPartitionList_t` / `StringList_t`
    accessors (`_count` / `_get` / `_destroy`) + `TopicPartition_topic` / `_partition`
    (borrowed elements, never freed — only the list root is destroyed). No new callback
    delegates, no `[MarshalAs]` (all sync, no `bool` returns).
  - **Two copy-out marshallers** (`Internal/Interop/TopicPartitionListMarshal`,
    `StringListMarshal`), mirroring the shipped `ConsumerGroupMetadataMarshal`: read every
    element out (NUL-terminated `Utf8Marshal.PtrToString(ptr)`, §B3 — **not** the
    length-delimited receive-path form), then `_destroy` the root in a `finally`. Both are
    Category-3 owned borrow-roots whose elements are Category-4 borrowed views (copy-out
    before destroy, §B2). **No new `SafeHandle`** — transient, caller-thread, fully
    consumed in one sync call (the `ConsumerGroupMetadataMarshal` read-and-free pattern,
    not the long-lived handle pattern). No `unsafe`.
  - **`NativeConsumer`** gains `Assignment()` / `Subscription()` / `Paused()` (concurrent
    null-handle → `InvalidOperationException` via a **shared `ThrowIfConcurrentNull`**
    helper — `GetGroupMetadataHandleOrThrow` was refactored to reuse it, so all five sync
    reads share exactly one concurrency contract) and `EnforceRebalance(string?)` (pins
    `reason` call-scoped or `IntPtr.Zero`; `FromHandle` throw-iff-non-null discipline).
  - **`IConsumerCommon`** gains the four members; `AsyncKafkaConsumer` +
    `AsyncMockConsumer` forward. The three getters are plain `()` **methods** returning
    `IReadOnlyCollection<T>`, and `EnforceRebalance` is a method with `string? reason =
    null`.
  - **`enforceRebalance` is a KIP-848 logged no-op that returns success** (SOURCE-VERIFIED:
    Java `AsyncKafkaConsumer.enforceRebalance` throws nothing; Rust core
    `enforce_rebalance` returns `Ok(())`; the FFI `sync_void_op` returns a null error
    handle). `EnforceRebalance` therefore never throws a `KafkaException` on that path —
    the still-present `FromHandle` check is the uniform sync-op discipline reserving a real
    error for a future classic-protocol arm. ⚠ The header's `enforce_rebalance` doc
    ("returns an unsupported-version error") is **stale** — flagged as a separate
    **Rust-core doc-fix dependency**, not acted on in this C#-only phase; the mapping
    follows the actual behavior.
  - **Deviations (recorded, COMMENTS.DONE.10):** (a) the four members on `IConsumerCommon`
    rather than the literal §3-sketch `IAsyncConsumer` placement (consistent with M4/P4b's
    `Wakeup`/`GroupMetadata` move); (b) the three getters as **methods, not properties**
    (reverses the §3 sketch's property form, on FDG "throws / does work /
    fresh-collection-per-call → method" + the shipped `GroupMetadata()` precedent +
    Java/Python parity); (c) `EnforceRebalance` as one method collapsing Java's two
    overloads; (d) the stale ABI doc raised as a Rust-core dependency; (e) the D-Q4-style
    non-deterministic-concurrency + non-empty-`Paused` reachability limits (both recorded,
    not silently skipped).
  - **Tests:** new `Interop/ConsumerSyncReadTests.cs` — the full §7 list broker-free via
    `AsyncMockConsumer` (getter round-trips as **sets**, non-ASCII through both
    marshallers, post-dispose `ObjectDisposedException` on all four, `EnforceRebalance`
    no-throw incl. non-ASCII reason, allocation sanity net8.0+). Concurrent-null → IOE
    verified by the reachable free-guard seam + code inspection of `ThrowIfConcurrentNull`
    (D-Q4 non-determinism ceiling). **122 → 141 tests**, all green across ≥4 full net10.0
    runs; serial execution (D8.8) unchanged; every awaited op under a `TestTimeout` guard.
    Discovery: the core rejects `Assign` + `Subscribe` together (mutually exclusive).
  - **DoD:** `cargo build --features ffi` (no ABI change) → `dotnet build` 0/0 across all
    library TFMs (netstandard2.0 / net8.0 / net10.0) + all test TFMs (net462 / net8.0 /
    net10.0) → net10.0 tests green (net8.0 *run* + net462 are CI/Windows-only; all three
    *build* legs pass locally) → `dotnet format --verify-no-changes` clean. CS1591 on every
    new public member; Apache-2.0 header on the two new files; no TODO/FIXME.
  - Approved plan + closed record: `design/history/M5/P1-consumer-sync-read-surface/`.
    Commits on the new branch `prashah_dev_public_consumer_remaining` (post-M4 consumer
    work), as a new PR for M5/P1. N=10.
- **Milestone 4 / Phase 4b — "Async-surface rename (`IAsyncConsumer`, drop the `Async`
  suffix, `WithCallback`)": DONE (2026-08-05).** A pure C# rename of M4/P4a's async
  surface into its final shape — **no ABI change (Mode A), no Rust authored, no new
  `DllImport`, no behavior change; only identifiers, file names, and one new small
  interface**. Commits to the two-interface consumer direction: `IAsyncConsumer` is the
  **async** surface (blocking-in-Java → `Task`), reserving `IConsumer` / `KafkaConsumer`
  for the future **sync** surface (M5). Method names carry **no `Async` suffix** — the
  sync-vs-async distinction is carried by the interface/type, matching Java's method
  names and the Python sibling. Delivered:
  - **Public rename** (`src/Confluent.Kafka/`): `interface IConsumer` →
    `interface IAsyncConsumer : IConsumerCommon, IAsyncDisposable, IDisposable`; **new**
    `interface IConsumerCommon { void Wakeup(); ConsumerGroupMetadata GroupMetadata(); }`
    (the two non-blocking members move off the async interface onto a shared base);
    `class KafkaConsumer` → `AsyncKafkaConsumer`, `class MockConsumer` →
    `AsyncMockConsumer`; drop the `Async` suffix (methods still return `Task`) —
    `PollAsync`→`Poll`, `SubscribeAsync`→`Subscribe`, `UnsubscribeAsync`→`Unsubscribe`,
    `SeekAsync`→`Seek`, `CloseAsync`→`Close`. `Dispose`/`DisposeAsync` unchanged
    (framework contract). File renames: `IConsumer.cs`→`IAsyncConsumer.cs`
    (+ new `IConsumerCommon.cs`), `KafkaConsumer.cs`→`AsyncKafkaConsumer.cs`,
    `MockConsumer.cs`→`AsyncMockConsumer.cs`. `AsyncMockConsumer`'s inherent mock helpers
    (`Assign`/`AddRecord`/`SetPollError`) keep their names. Carried rationale docstrings
    moved onto the renamed members (`Seek`-is-async blocking-`addAndGet` + Python
    divergence; `byte[]` key/value/header; single-owner/not-thread-safe caveat; `Close()`
    surfaces the error unlike `DisposeAsync`; `Wakeup()` cross-thread caveat); CS1591
    intact.
  - **Internal rename** (`Internal/NativeConsumer.cs`): the bridge methods `…Async` →
    `…WithCallback` (their names reflect the callback bridge, not the .NET async
    convention) — `PollWithCallback`, `SubscribeWithCallback`, `UnsubscribeWithCallback`,
    `SeekWithCallback`, `CloseWithCallback`, and `CloseAsyncInternal` →
    `CloseWithCallbackInternal`. `Dispose`/`DisposeAsync`/`SubmitOperation`/
    `SubmitVoidOperation`/`Wakeup`/`GroupMetadata`/`GroupId` unchanged.
  - **Two guardrails held:** (1) the `NativeMethods` P/Invoke declarations and their
    `EntryPoint` strings are untouched — the extern names (`ConsumerPollAsync`, …) mirror
    the C ABI's own `_async` suffix (`kafka_consumer_Consumer_poll_async`, …), reflecting
    the C ABI, not our public naming; `ConsumerCallbacks`, the marshallers, value types,
    `OperationCompletionSource`, the `SafeHandle`s, and `KafkaException` are unchanged.
    (2) The archived M4/P4a docs (`design/history/M4/P4a-public-consumer/`) are NOT
    retro-edited — only this current STATUS moves to the new names.
  - **NOT in scope:** the sync surface (sync `IConsumer`/`KafkaConsumer`/`MockConsumer`,
    sync `Consumer_*` DllImports, sync `NativeConsumer` methods) — later milestone M5; no
    behavior change, no new op, no ABI change; `VSTHRD200` confirmed absent (no analyzer
    enforces the `Async` suffix), so dropping it builds clean under
    `TreatWarningsAsErrors`.
  - **Tests:** all consumer test references renamed (public + internal), **every
    assertion kept** — 122 tests, same count. Parallelism stays disabled (D8.8).
  - **Deviation (recorded):** PLAN sub-steps 1 (interface) and 2 (impl classes) landed as
    **one green commit** — the interface doc crefs the impl-class names and the impls
    implement the renamed interface, so they are the minimal compiling unit for the public
    rename (the library cannot be green with only one half). Sub-steps 3 (internal), 4
    (tests), 5 (docs) are separate commits as planned.
  - **Governance — N=9 is this rename.** Earlier entries (M4/P4a, M3/P3) pre-labeled the
    `Wakeup()`/`GroupId` handle-TOCTOU `DangerousAddRef` hardening a "candidate N=9
    follow-up (unscheduled)". That prediction is superseded: N=9 is the P4b rename, and the
    cross-thread hardening remains **accepted-by-design + unscheduled** (no review number
    assigned) — it is untouched here (pure rename, no behavior change). The three M3/P2
    accepted residuals also remain accepted, unchanged.
  - Approved plan + closed record: `design/history/M4/P4b-async-surface-rename/`. Additive
    commits on `prashah_dev_public_consumer_scaffolding` (the existing M4/P4a stacked PR;
    the PR description is updated to the final `IAsyncConsumer` naming before merge). N=9.
- **Milestone 4 / Phase 4a — "Public Consumer Client (the first usable public cut)":
  DONE (2026-08-04).** The first PUBLIC client surface — promotes the proven internal
  machinery to a Java-shaped, XML-documented public API: subscribe → poll → seek →
  group metadata → close, usable end-to-end broker-free via `MockConsumer` and against
  a real broker with auto-commit. Mode A (every ABI function already ships; no Rust
  authored). Delivered:
  - **Public value types** (namespace `Confluent.Kafka`, library root): promoted
    `ConsumerRecord` / `ConsumerRecords` (internal → `public sealed`); new
    `Header` / `Headers` (read-only view), the `TimestampType` enum, the
    `TopicPartition` `readonly struct` (value equality + Java `"topic-partition"`
    `ToString`), and `ConsumerGroupMetadata` (full four-field set). **`Key`/`Value` +
    `Header.Value` unified on `byte[]?`** (micro-decision A — a deliberate deviation
    from the CLAUDE.md §3 `ReadOnlyMemory` sketch; the copy-out marshaller drops the
    `ReadOnlyMemory` wrap it used, so `unsafe` left `ConsumerRecordsMarshal` too — a net
    simplification, no new copy). The internal `RecordHeader` struct is deleted.
  - **Public interface + clients**: `IConsumer : IAsyncDisposable, IDisposable`
    (minimal, additive-growth, **non-generic** — micro-decision D), and
    `KafkaConsumer` / `MockConsumer` (`public sealed`, both `impl IConsumer`). Both
    **compose** the internal `NativeConsumer` and forward (§2.5 compose-over-absorb) —
    the ~40 internal interop tests + the `unsafe`/`GCHandle` quarantine stay intact.
    `MockConsumer`'s mock helpers (`Assign` / `AddRecord` component-tuple /
    `SetPollError`) are inherent, NOT on `IConsumer` (consumer-threading §2).
  - **`NativeConsumer` edits**: `UnsubscribeAsync` (the ONE new void wire over
    `Consumer_unsubscribe_async`), the `SeekAsync` `offset < 0` precondition
    (`ArgumentOutOfRangeException`, exact Java message `"seek offset must not be a
    negative number"` — the only Java-fidelity behavior fix, PLAN dec. 11),
    `GroupMetadata()` (full four-field read via a new `ConsumerGroupMetadataMarshal` +
    the three group-metadata DllImports), and `CloseAsync(CancellationToken)` (dec. 6
    wiring: one-shot latch → `close_async` → destroy, **surfaces** the close error
    unlike `DisposeAsync`; NO timeout — the ABI has none).
  - **Async/sync split** (from the Java impl, CLAUDE.md §4): `PollAsync` /
    `SubscribeAsync` / `UnsubscribeAsync` / `SeekAsync` / `CloseAsync` async;
    `Wakeup()` / `GroupMetadata()` sync. **`SeekAsync`-is-async** is the load-bearing
    call (Java `seek()` blocks on `addAndGet`) — a deliberate divergence from Python's
    sync `seek`, documented in the docstring.
  - **Tests** (public-surface, PLAN §5): round-trip (incl. non-ASCII out_len path,
    sentinels, churn, byte[] header), async/sync split, SeekAsync exact-message,
    GroupMetadata full-field (real pre-join defaults + non-ASCII + Mock sentinels),
    teardown (Dispose/DisposeAsync/CloseAsync idempotence + unawaited-op residual +
    use-after-dispose), allocation budget, TFM smoke. net462 added to the test TFMs
    (builds cross-platform via `Microsoft.NETFramework.ReferenceAssemblies`; run
    Windows/CI-only). **122 tests, 20/20 full-suite runs green.**
  - **FINDING — pre-existing intermittent host crash under xUnit parallel execution**
    (COMMENTS.DONE.8 D8.8). Bisected to the tracked HEAD *before* any P4a test: the
    accepted single-owner residual (an unawaited-op straggler dispatcher callback after
    the fire-and-forget `Consumer_destroy`) races GC across parallel test collections.
    Fixed with `[assembly: CollectionBehavior(DisableTestParallelization = true)]` — the
    standard setting for a not-thread-safe native-resource suite (no assertion
    weakened; the within-test ops are already serialized by the core guard). A real fix
    of the residual (a Rust-core dispatcher-join on destroy) is out of scope.
  - **Governance — `Wakeup()` is now genuinely public / cross-thread for the first
    time** (the one item P4a changes). Per locked decision 5 = **option (a)**: the
    handle-TOCTOU residual stays accepted-by-design and is documented on the public
    `KafkaConsumer` / `IConsumer` as a not-thread-safe caveat (Python/CKD parity); NO
    per-call `DangerousAddRef` hardening this phase — flagged as a candidate **N=9**
    follow-up (not scheduled). The three M3/P2 accepted residuals remain accepted;
    composition inherits them unchanged.
  - **Deferred (later additive phases, unchanged public shape):** the commit family
    (`Consumer_commit_async` naming minefield), `position` (scalar callback),
    `Assignment`/`Subscription`/`Paused` (owned-list sync), the owned-handle query
    siblings (`committed`/`offsetsForTimes`/`beginning|endOffsets`/`partitionsFor`/
    `listTopics`), `subscribe(pattern)`/`assign`/`pause`/`resume`,
    `ConsumerRebalanceListener`/`OffsetCommitCallback`, serializers + generic
    `IConsumer<TKey,TValue>`, typed `KafkaException` subclasses, `CloseAsync(TimeSpan)`.
  - **⚠ CLAUDE.md §4 package-id pre-publish gate stays OPEN** (M0/P1). P4a adds public
    *types* but does not publish; `IsPackable=false` holds it shut. Decide (own SR
    integration, or diverge the id) before any publish — out of scope here.
  - Approved plan + closed record: `design/history/M4/P4a-public-consumer/`. Additive on
    `prashah_dev_public_consumer_scaffolding` (a NEW PR stacked on the M3/P3 PR).
- **Milestone 3 / Phase 3 — "Poll + the receive path (owned-handle completion
  bridge)": DONE (2026-08-03).** Proves the **result-returning** completion shape
  end-to-end via `poll` — the owned-handle bridge that five sibling query ops reuse
  later (`committed` / `offsetsForTimes` / `beginning|endOffsets` / `partitionsFor` /
  `listTopics`). Delivered: `OperationCompletionSource<TResult>` (the void bridge kept
  as the thin `OperationCompletionSource : <bool>` subclass, all 5 invariants intact);
  the `poll_callback_t` trampoline on `ConsumerCallbacks` (`OnPoll`, free-exactly-once
  in a `finally` on every path — batch destroy after copy-out, `KafkaError` via
  `FromHandle`, per-op `GCHandle`); the **on-dispatcher copy-out** `ConsumerRecordsMarshal`
  (§6.4 default — the native batch is created / copied-out / destroyed entirely inside
  the callback, so no `SafeConsumerRecordsHandle`, no native-backed `ReadOnlyMemory`,
  no leak-on-abandoned-`Task`); internal `ConsumerRecord` / `ConsumerRecords` /
  `RecordHeader` (owned copies; **internal**, under `Internal/`); the receive-path
  DllImports (`poll_async`, the `ConsumerRecords_t` / `ConsumerRecord_t` accessor set
  incl. headers, `MockConsumer_add_record` / `_set_poll_error`, `Consumer_assign`); the
  length-delimited `Utf8Marshal.PtrToString(ptr, len)` (§B3, never NUL-scan, un-defers
  M1/P1 D4); `NativeConsumer.PollAsync`. Mode A (no Rust / header change). NO sync
  `poll` DllImport (decision 5); NO public client type; `ConsumerRecord(s)` stay
  **internal**. Additive on `prashah_dev_asyncbridge_poll_scaffolding` (new PR stacked
  on #135). Approved plan + closed record:
  `design/history/M3/P3-poll-receive-path/`.
  - **Un-deferred → DONE this phase:** the **M3/P1 D1 wakeup-fault one-shot**. `poll` is
    the only op that observes `wakeup()` broker-free, so `Wakeup()` → next `PollAsync`
    faults with a Wakeup `KafkaException` **once**, then a subsequent poll succeeds
    (`ConsumerPollWakeupCancelTests.Wakeup_ThenPoll_FaultsOnce_ThenReusable`) — Java's
    one-shot `WakeupException` semantics, now **deterministic** (was D1-deferred as
    "not reachable without a wakeup-observing op").
  - **Remaining residuals (still deferred — each needs a Rust-core dependency, not a
    .NET change):** (1) the **in-flight `CancellationToken` cancel** (token fires after
    submit but while the poll is mid-flight) — the pre-canceled path is deterministic
    and tested, but the mock poll runs to completion synchronously and exposes no block
    hook, so the in-flight overlap is a genuine race; (2) the **M3/P2 D-Q4 concurrency
    matrix** (a controllable-duration guard-holding op to force a submit→callback
    overlap) — same non-blockable-mock ceiling; (3) a **full end-to-end header
    round-trip** — `MockConsumer_add_record` carries no headers, so only the
    empty-headers case is reachable (the §B3 length-delimited header-key *primitive* is
    directly tested via the record topic + `Utf8MarshalLengthDelimitedTests`). All three
    close when an FFI-exposed blockable mock poll (a `schedule_poll_task` / block hook)
    or a header-carrying `add_record` lands — a Rust-core dependency requested from the
    root `actor-executor`, reviewed by `kafka-critic`. Documented in
    `COMMENTS.DONE.7.md` (the D-Q4 precedent).
  - **Governance (N≥7 → N≥8 renumber):** M3/P3 **takes N=7**, so the STATUS /
    `NativeConsumer` cross-thread **hardening** labels previously pre-labeled "N≥7"
    (the `Wakeup()`/`GroupId()` handle TOCTOU vs teardown; the submit-vs-`destroy`
    handle race) are renumbered to **N≥8**. `poll` makes the wakeup *behavior* testable
    but does NOT make the cross-thread *races* reachable — the binding is still
    internal-only, single-owner, with no public cross-thread `Wakeup()` caller — so
    those items **stay deferred**, now "N≥8, whenever a public client makes `Wakeup()`
    genuinely cross-thread." No dangling "N≥7" label remains in tracked source/STATUS.
- **Milestone 3 / Phase 2 — "Single-owner alignment (drop the managed guard +
  in-flight tracking; keep the completion bridge)": DONE (2026-08-03).** Aligns the
  M3/P1 completion-bridge/teardown machinery to the in-repo Python sibling's
  "single-owner, not thread-safe" contract (`bindings/python/consumer.py`). The Rust
  **core's own** access guard is the serializer; the .NET-only managed mirror
  (`ConsumerAccessGuard`) and single-slot in-flight tracking (`_inFlightContext` /
  `_inFlightOperation`) are removed, as is `OperationCompletionSource.FaultTaskOnly`
  and the `Dispose` snapshot+fault / `DisposeAsync` drain blocks. `Dispose` →
  `close_with_timeout → destroy`; `DisposeAsync` → `close_async → destroy` (no
  separate-op drain — under single-owner the awaiter of an op is its disposer).
  Concurrency now surfaces the core's way: a concurrent **async op** → a faulted
  `Task` (`KafkaException` / ConcurrentModification, delivered by the core inline);
  a concurrent **sync state read** (`GroupId`) → `InvalidOperationException` from the
  core's null-handle path (was `return null`). KEPT (the 5 invariants + bridge core):
  the per-op self-rooting `GCHandle`; the completion callback = sole owner of the
  `GCHandle` free; the atomic `_closed` teardown gate; `SafeHandle` + TCS
  thread-safety; `RunContinuationsAsynchronously`; the whole `ConsumerCallbacks`
  trampoline, all 4 async DllImports, and `Wakeup`'s `if (_closed) return;` check (a
  deliberate divergence safer than Python). **This eliminates the M3/P1 op-submit-
  vs-teardown publish window** (no tracking fields → no window). Mode A (no Rust /
  header change); a localized, reversible simplification. Extends PR #135 as
  additive commits on `prashah_dev_asyncbridge_scaffolding` (no history rewrite).
  Approved plan + closed record:
  `design/history/M3/P2-single-owner-alignment/`.
- **Milestone 3 / Phase 1 — "Completion bridge + first async op (consumer,
  proof-of-plumbing)": DONE (2026-07-27).** The foreign-thread completion callback
  → `Task` bridge — the riskiest new machinery — de-risked BEFORE poll / the
  receive path. Activates the `_async`/callback ABI for the first time (Mode A, no
  Rust authored). Delivered: the void-result bridge (`OperationCompletionSource`,
  `TaskCompletionSource` with `RunContinuationsAsynchronously`, GCHandle keep-alive
  submit→fire, no-throw callback boundary, free-exactly-once); the managed
  one-op-in-flight `ConsumerAccessGuard` (mirrors — does not replace — the core
  guard); two thin proof ops on one bridge — `SubscribeAsync` (SUCCESS) and
  `SeekAsync` unassigned (FAILURE); `Wakeup()` + `CancellationToken` mapping;
  async-aware teardown (`IAsyncDisposable.DisposeAsync` drain→`close_async`→destroy,
  un-defers M2/P1 D3) with the N=5-deferred teardown-thread-safety hardening folded
  in (thread-safe closed flag). NO poll / receive path (Category 3/4 handles,
  `ConsumerRecord(s)`, length-delimited `out_len` strings, copy-out), NO other async
  ops, NO public client type (`KafkaException` remains the only public type) — all
  deferred. Approved plan + closed record:
  `design/history/M3/P1-completion-bridge/`.
- **Milestone 2 / Phase 2 — "SafeHandle marshaller-return hardening": DONE
  (2026-07-22).** The three owned-handle constructors
  (`ConsumerProperties_new` / `KafkaConsumer_new` / `MockConsumer_new`) now
  return their `SafeHandle` subtype **directly** instead of a raw `IntPtr`, so the
  interop marshaller creates-and-sets the handle atomically inside a constrained
  region — closing the M2/P1 `new + SetHandle` allocation-gap window (an async
  abort on net462, or OOM, between obtaining the pointer and `SetHandle` would
  leak the native handle). A **hardening** change, not a bug fix (M2/P1 is correct
  on net8/net10). Mode A (no ABI/Rust change; SafeHandle-return is a classic
  `[DllImport]` feature supported on the netstandard2.0 floor incl. net462).
  `SafeConsumerHandle.FromRaw` removed; `SafeConsumerPropertiesHandle.Create`
  collapses to the marshaller return. NO new public API, NO completion bridge, NO
  poll/subscribe/commit — unchanged from M2/P1 scope. Approved plan + closed
  record: `design/history/M2/P2-safehandle-return-hardening/`.
- **Milestone 2 / Phase 1 — "Error model + first SafeHandle (consumer client
  lifecycle)": DONE (2026-07-21).** The first PUBLIC type (`KafkaException`) plus
  the Category-1 owned-handle consumer lifecycle (create → close → destroy), kept
  INTERNAL (`NativeConsumer`, tested via `InternalsVisibleTo`). Activated the five
  `kafka_common_KafkaError_*` DllImports (declared in M1/P1) as live callers via
  `KafkaException.FromHandle`. Mode A (consumer C ABI already landed — no Rust
  authoring). NO completion bridge, NO poll/subscribe/commit, NO producer, NO
  Category 3/4 receive-path handles, NO public client type yet — all deferred.
  Approved plan + closed record: `design/history/M2/P1-error-model-safehandle/`.
- **Milestone 1 / Phase 1 — "Interop scaffolding + native-load probe": DONE
  (2026-07-20).** The client-agnostic interop FOUNDATION: the `NativeMethods` P/Invoke
  class (8 shared-foundation declarations), the `Utf8Marshal` marshalling helpers, the
  native-copy MSBuild target (un-defers M0/P0 decision D2), and a consumer-namespaced
  native-load probe. Mode A (C ABI already landed — no Rust authoring). NO public
  managed API, NO `SafeHandle`, NO completion bridge, NO Kafka logic yet — all
  deferred to later phases by scope.
- **Milestone 0 / Phase 1 — "Rename binding identity": DONE (2026-07-29).**
  The binding identity is `Confluent.Kafka` (was
  `Confluent.Kafka.ShareConsumer`): solution, strong-name key, both project
  directories and their csprojs, `<RootNamespace>`/`<AssemblyName>`/`<Product>`,
  `<AssemblyOriginatorKeyFile>`, the `InternalsVisibleTo` grant, the
  `ProjectReference`, and the test file-scoped namespace.

  **Compliance work, not preference** — CLAUDE.md §2's file map and §4's
  *Namespace / package id* row already read `Confluent.Kafka`, so the M0/P0
  artifacts were the drift, not the rulebook. Adds **no capability**; same
  milestone because it only corrects M0/P0's output. The old name described a
  KIP-932 feature that `.claude/rules/consumer-threading.md` §20 puts explicitly
  out of scope, so it was actively misleading.

  The strong-name key was **not** regenerated — the `.snk` is byte-identical and
  the public-key token stays `a6a493010a30d243`, so the assembly identity is
  unchanged (only `InternalsVisibleTo Include=` moved; its `Key=` blob is
  verbatim).

  ⚠ **CLAUDE.md §4's package-id pre-publish gate remains OPEN.** The binding now
  shares the `Confluent.Kafka` id with confluent-kafka-dotnet, meaning a project
  can hold ckd 2.x **or** this client, never both (so ckd's Schema-Registry /
  OAuthBearer packages can't be mixed in). This phase makes the *name* collide,
  so the gate is now held shut **structurally** rather than by prose:
  `Microsoft.NET.Sdk` defaults `IsPackable` to **true** for a library and
  `PackageId` to **`$(AssemblyName)`**, so writing neither is the *packable*
  state — a bare `dotnet pack` would emit a package id byte-equal to ckd's. The
  library csproj therefore sets `<IsPackable>false</IsPackable>` explicitly (and
  nothing else packaging-related). Check the **evaluated** property, never the
  absence of an element: `dotnet msbuild <library>.csproj -getProperty:IsPackable`
  → `false`. "No `Pack*` metadata" was never the right
  test either — `Directory.Build.props`'s `<Authors>`/`<Company>`/`<Product>`/
  `<Copyright>` flow into a nuspec on their own. There is also no publish
  automation in the repo (no `.github/workflows`, no `dotnet pack` / `nuget push`
  target anywhere), so nothing can trip the gate today. The decision — own SR
  integration, or diverge the id — is still owed **before any publish**, and
  un-defers by flipping that one line.
- **Milestone 0 / Phase 0 — "Project scaffolding": DONE (2026-07-20).** Pure
  structural skeleton (see `design/history/M0/P0-scaffolding/`).

## What exists now (structure)

```
bindings/dotnet/
├─ Confluent.Kafka.sln                       ← classic .sln, both projects + a "build" solution folder
├─ Directory.Build.props                     ← #nullable enable, LangVersion=latest,
│                                              EnforceCodeStyleInBuild, TreatWarningsAsErrors,
│                                              strong-name signing (shared .snk, both projects)
│                                              — NO AllowUnsafeBlocks here (library-only, M1/P1 D2)
├─ .editorconfig · .gitignore
├─ src/
│  └─ Confluent.Kafka/
│     ├─ Confluent.Kafka.csproj                ← TFMs netstandard2.0;net8.0;net10.0
│     │                                           (net462 via ns2.0), System.Memory on the ns2.0
│     │                                           leg only, GenerateDocumentationFile,
│     │                                           IsPackable=false (M0/P1 — holds §4's id gate shut),
│     │                                           InternalsVisibleTo → UnitTests (public key);
│     │                                           M1/P1: + <AllowUnsafeBlocks> (library only),
│     │                                           + native-copy MSBuild target (per-OS filename via
│     │                                           IsOSPlatform, profile from $(Configuration),
│     │                                           repo root 4 levels up, <Content> transitive,
│     │                                           + <Error> guard if native absent)
│     ├─ KafkaException.cs                      ← M2/P1: FIRST public type. sealed KafkaException :
│     │                                            Exception, flat Code/IsRetriable/IsFatal + Message;
│     │                                            internal FromHandle(IntPtr) (msg before free, copy
│     │                                            out, destroy in finally); flat-now/typed-later
│     └─ Internal/
│        ├─ NativeConsumer.cs                   ← M2/P1: internal lifecycle wrapper (unsafe-free, D4):
│        │                                         config -> ConsumerProperties_put -> KafkaConsumer_new
│        │                                         (FromHandle on out_error) -> SafeConsumerHandle;
│        │                                         graceful Dispose (close_with_timeout -> destroy);
│        │                                         preconditions -> ArgumentNullException/ArgumentException.
│        │                                         M2/P2: consumes the SafeHandle returns (dispose the
│        │                                         IsInvalid handle on error; defensive IsInvalid guard).
│        │                                         M3/P1: proof async ops (SubscribeAsync/SeekAsync via a
│        │                                         shared SubmitVoidOperation), Wakeup(), guarded GroupId()
│        │                                         state read; thread-safe closed flag (folds N=5 deferred);
│        │                                         IAsyncDisposable.DisposeAsync (drain->close_async->destroy).
│        │                                         M3/P2: single-owner alignment — drop the managed guard +
│        │                                         in-flight tracking (_guard/_inFlightContext/_inFlightOp);
│        │                                         Dispose=close_with_timeout->destroy, DisposeAsync=close_async
│        │                                         ->destroy (no separate-op drain); GroupId throws
│        │                                         InvalidOperationException on the null/concurrent-rejection path
│        ├─ OperationCompletionSource.cs        ← M3/P1: per-op callback->TCS context; TCS built with
│        │                                         RunContinuationsAsynchronously; KafkaError->KafkaException
│        │                                         (FromHandle); CancellationToken->wakeup +
│        │                                         OperationCanceledException; idempotent GCHandle free
│        │                                         (Complete/AbandonBeforeSubmit).
│        │                                         M3/P2: guard param/field + FaultTaskOnly removed; callback =
│        │                                         sole owner of the GCHandle free (only other path is
│        │                                         AbandonBeforeSubmit, when native never ran)
│        └─ Interop/                            ← the P/Invoke boundary — `unsafe` lives ONLY here
│           ├─ NativeMethods.cs                        ← internal static class NativeMethods: M1/P1 (8
│           │                                      shared decls) + M2/P1 consumer lifecycle
│           │                                      (KafkaConsumer_new/MockConsumer_new/close/
│           │                                      close_with_timeout/destroy) + group-metadata trio.
│           │                                      M2/P2: the three constructors return their SafeHandle
│           │                                      subtype directly (marshaller create-and-set).
│           │                                      M3/P1: 4 async decls — subscribe_async (topics as
│           │                                      IntPtr[] = const char* const*) / seek_async / wakeup /
│           │                                      close_async (op-callback as a kept-alive Cdecl delegate)
│           ├─ ConsumerCallbacks.cs                   ← M3/P1: [UnmanagedFunctionPointer(Cdecl)]
│           │                                      OperationCallback delegate type + one static readonly
│           │                                      rooted instance + the no-throw callback body
│           ├─ SafeHandleZeroIsInvalid.cs             ← M2/P1: shared base, IsInvalid => handle==Zero (D2)
│           ├─ SafeConsumerPropertiesHandle.cs        ← M2/P1: config handle (-> ConsumerProperties_destroy);
│           │                                            M2/P2: Create collapses to the marshaller return
│           ├─ SafeConsumerHandle.cs                  ← M2/P1: client handle (-> Consumer_destroy, bare
│           │                                            last-resort; graceful close is in NativeConsumer).
│           │                                            M2/P2: FromRaw removed (arrives marshaller-wrapped)
│           └─ Utf8Marshal.cs                          ← internal static class Utf8Marshal: Pin (disposable
│                                                  call-scoped pinned buffer) + PtrToString
│                                                  (NUL-terminated form; null for IntPtr.Zero)
└─ tests/
   └─ Confluent.Kafka.UnitTests/               ← TFMs net8.0;net10.0, unsafe-free
      ├─ TfmSentinelTests.cs                    ← M0/P0 TFM-sentinel smoke test (root: harness-level)
      ├─ KafkaExceptionTests.cs                 ← M2/P1: public-type test (root): classic -> Code 35
      │                                            + I1 both-false + msg; café msg echo; FromHandle(Zero)
      │                                            (M3/P2: ConsumerAccessGuardTests removed with the guard)
      ├─ TestTimeout.cs                         ← M2/P1: fail-fast deadline helper (hang -> test failure).
      │                                            M3/P1: + async Run(Func<Task>) overload (bridge/drain guard)
      └─ Interop/                               ← mirrors the library interop area (public test
         │                                         classes; "Interop" not "Internal/Interop" — the
         │                                         Internal visibility marker is library-only, §2)
         ├─ NativeLoadProbeTests.cs             ← M1/P1: 2 tests, both invoke a native [DllImport]
         │                                         (smoke new/put/destroy; non-ASCII put no-crash).
         │                                         M2/P2: props via `using SafeConsumerPropertiesHandle`
         │                                         (Dispose frees; asserts !IsInvalid)
         ├─ Utf8MarshalTests.cs                 ← M1/P1: managed Utf8Marshal codec round-trip
         │                                           + PtrToString(Zero)==null (no native call)
         ├─ SafeConsumerHandleTests.cs          ← M2/P1: lifecycle (mock + real), double-Dispose,
         │                                           use-after-Dispose, create/dispose many.
         │                                           M2/P2: KEY regression — classic-protocol
         │                                           KafkaConsumer_new -> IsInvalid handle + non-null
         │                                           out_error; Dispose skips ReleaseHandle (no spurious
         │                                           destroy); error round-trips via FromHandle
         ├─ ConsumerConfigMarshalTests.cs       ← M2/P1: config success + preconditions (null dict /
         │                                           null value / post-Dispose)
         ├─ Utf8RoundTripTests.cs               ← M2/P1 (D5 CLOSED): non-ASCII group.id -> group_metadata
         │                                         -> group_id readback == input (broker-free)
         ├─ ConsumerCompletionBridgeTests.cs    ← M3/P1: SUCCESS (subscribe, churned) + FAILURE (seek
         │                                         unassigned -> KafkaException Code -1/flags/Message,
         │                                         churned); no-throw boundary; GCHandle keep-alive under
         │                                         GC; RunContinuationsAsynchronously (bridge driven
         │                                         directly, off the completing thread); chained ops.
         │                                         M3/P2: the four FaultTaskOnly_* component tests removed
         │                                         (the sync-Dispose fault machinery is gone)
         ├─ ConsumerAsyncOperationTests.cs      ← M3/P1: wakeup (safe/reusable/during-op); cancellation
         │                                         (pre-canceled -> OperationCanceledException); GroupId
         │                                         read round-trip (incl. non-ASCII).
         │                                         M3/P2: GroupId now unguarded (single-owner); concurrent
         │                                         -rejection -> InvalidOperationException verified by
         │                                         inspection (not deterministic broker-free, D-Q4)
         └─ ConsumerAsyncTeardownTests.cs       ← M3/P1: DisposeAsync + Dispose with op in flight RETURN;
                                                   double/mixed/concurrent teardown safe; use-after-dispose
                                                   -> ObjectDisposedException.
                                                   M3/P2: op-in-flight teardown cases repurposed to assert
                                                   return-without-hang only (no separate-op drain; the
                                                   unawaited-op strand+leak is an accepted residual)
```

## Verification state (M4/P4b DoD — Actor, all green)

- `cargo build --features ffi` — native cdylib + generated header present (run FIRST,
  CLAUDE.md §7.1). **No ABI change this phase (Mode A).**
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs (netstandard2.0,
  net8.0, net10.0) and all test TFMs (net462, net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
  active. **CS1591 satisfied on every renamed public member** (`IAsyncConsumer`, the new
  `IConsumerCommon`, `AsyncKafkaConsumer`, `AsyncMockConsumer`, and the un-suffixed
  methods). Apache-2.0 header on the new `IConsumerCommon.cs`; no TODO/FIXME. No analyzer
  suppressions needed — **VSTHRD200 confirmed absent**
  (`Microsoft.VisualStudio.Threading.Analyzers` not referenced), so dropping the `Async`
  suffix builds clean.
- `dotnet test -f net10.0` — **122 passed, 0 failed** (same count as M4/P4a — a pure
  rename, no test weakened, no coverage lost); **20/20 full-suite runs green, 0 crashes /
  0 failures** (the stability gate). Serial execution
  (`[assembly: CollectionBehavior(DisableTestParallelization = true)]`) stays enabled
  (D8.8 — not re-enabled).
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** the local runtime is .NET 10; the net8.0 test *run* and
  net462 are CI/Windows-only. **All three test *build* legs (net462 / net8.0 / net10.0)
  pass locally**, and the library's three TFMs build.
- **Docs match code:** this STATUS is updated to the new names + N=9; the archived M4/P4a
  docs are left intact as the historical record (guardrail 2). The `NativeMethods` extern
  names / ABI `EntryPoint` strings are untouched (guardrail 1).

## Verification state (M4/P4a DoD — Actor, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1). **No ABI change this phase (Mode A).**
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and all test TFMs (**net462**, net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
  active. **CS1591 satisfied on every new public member** — the first new public
  surface since M2/P1 (`IConsumer`, `KafkaConsumer`, `MockConsumer`, `ConsumerRecord`,
  `ConsumerRecords`, `Header`, `Headers`, `TimestampType`, `TopicPartition`,
  `ConsumerGroupMetadata`). No analyzer suppressions needed (CA1815 satisfied by
  `TopicPartition`'s `==`/`!=` + value equality). Apache-2.0 header on every new file;
  no TODO/FIXME.
- `dotnet test -f net10.0` — **122 passed, 0 failed**; **20/20 full-suite runs green,
  0 crashes / 0 failures** (the §5.8 stability gate). Every awaited op / teardown under
  a `TestTimeout` hang guard. Serial execution
  (`[assembly: CollectionBehavior(DisableTestParallelization = true)]`) fixes a
  pre-existing intermittent host crash (D8.8) that only manifested under xUnit's
  default cross-collection parallelism.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** the local runtime is .NET 10; the net8.0 test *run* and
  net462 (via ns2.0 for the library; a direct net462 test TFM) are CI/Windows-only.
  **All three test *build* legs (net462 / net8.0 / net10.0) pass locally**, and the
  library's three TFMs build.
- **Docs match code:** `ffi-marshalling.md` §B (single-owner, copy-out, the 5
  invariants) and CLAUDE.md §3/§4 (the idiom map, `byte[]` key/value deviation,
  async/sync split) describe the landed public surface; the byte[] deviation +
  CloseAsync wiring + GroupMetadata reachability + the D8.8 finding are recorded in
  `COMMENTS.DONE.8.md`.

## Verification state (M3/P2 DoD — Actor, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1). No ABI change this phase (Mode A).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + CS1591 active. No new public
  type → no new CS1591 surface. No dangling references to `ConsumerAccessGuard` /
  `FaultTaskOnly` / `_inFlight*` (verified by grep; the only remaining mentions are
  intentional prose in the `NativeConsumer` docstrings recording what was removed).
- `dotnet test -f net10.0` — **43 passed, 0 failed** (~1 s); the M3/P1 set minus the
  5 `ConsumerAccessGuardTests` + the 4 `FaultTaskOnly_*` bridge tests (52 → 43).
  Every awaited op / teardown under a `TestTimeout` hang guard, so a bridge/drain
  hang fails fast.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** the local runtime here is .NET 10 (SDK 10.0.300,
  runtime 10.0.8); the net8.0 test *run* and net462 (via netstandard2.0) are
  CI-only. Both *build* legs pass.
- **Docs match code:** `ffi-marshalling.md` §B1/§B5/§B7 describe the single-owner /
  no-managed-guard model the landed `NativeConsumer` / `OperationCompletionSource`
  code implements (core-delivered concurrency, `InvalidOperationException` state
  read, `close_(with_timeout|async) → destroy` teardown with no separate-op drain).

## Decisions in force (M3/P2)

- **D-Q1 — `DisposeAsync` no longer drains a separately-submitted in-flight op.**
  Teardown is `close_async → destroy` (`Dispose`: `close_with_timeout → destroy`);
  under single-owner the awaiter of an op is its disposer, so there is no concurrent
  submitter to drain. Matches Python's `close()`. Accepted residual: an *unawaited*
  in-flight op + teardown may strand + leak once (`DisposeAsync` on the awaiting task
  is leak-free).
- **D-Q2 — `GroupId`: `return null` → `throw InvalidOperationException` on the
  core's concurrent-rejection (null-handle) path.** Internal-only (no public
  contract break); mirrors Python `_concurrent_error()` (`None → RuntimeError`) and
  the CLAUDE.md §3 idiom-map row (concurrent sync state read →
  `InvalidOperationException`).
- **D-Q3 — extends PR #135 on `prashah_dev_asyncbridge_scaffolding`** as additive
  commits (no new branch, no separate PR, no history rewrite of M3/P1's commits).
  #135 grows to contain the full "M3/P1 adds the guard + tracking, then M3/P2
  removes it" build-then-simplify arc.
- **D-Q4 — no flaky forced-overlap test for the `GroupId` concurrent →
  `InvalidOperationException` mapping.** Broker-free `MockConsumer` ops resolve
  instantly (the core guard is held only microseconds) and the one guard-holding op
  with a controllable duration is `poll` (out of scope), so the overlap is not
  deterministically reproducible this phase. Tested the reachable seam (the `GroupId`
  round-trip incl. non-ASCII); the null-handle → `InvalidOperationException` mapping
  is verified by code inspection and documented in `COMMENTS.DONE.6.md`, mirroring
  how M3/P1 documented D1/D2. **Honest cost:** removing the managed guard also
  removed M3/P1's deterministic component-level `ConsumerAccessGuardTests`, so this
  one concurrency behavior regresses from deterministic (component-level) to
  non-deterministic — accepted and documented.
- **The 5 invariants + bridge core are KEPT unchanged:** per-op self-rooting
  `GCHandle`; callback = sole owner of the `GCHandle` free; atomic `_closed`
  teardown gate (`TryBeginClose` / `ThrowIfClosed`); `SafeHandle` + TCS
  thread-safety; `RunContinuationsAsynchronously`. The whole `ConsumerCallbacks`
  trampoline, all 4 async DllImports, and `Wakeup`'s `if (_closed) return;` check
  (a deliberate divergence safer than Python) are unchanged.

## Verification state (M3/P1 DoD — Actor, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + CS1591 active. No new public
  type → no new CS1591 surface. The ns2.0 leg resolves `IAsyncDisposable` /
  `ValueTask` via `Microsoft.Bcl.AsyncInterfaces` (M3/P1 D3).
- `dotnet test -f net10.0` — **52 passed, 0 failed** (M3/P1 set incl. the N=5
  Finding-1/Finding-3 fixups: 5 access guard, 12 completion bridge — the four
  `FaultTaskOnly_*` among them, 6 async op, teardown, and the carried M0–M2 tests);
  ~370 ms — every awaited op / teardown under a `TestTimeout` hang guard, so a
  bridge/drain hang would fail fast.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** only the .NET 10 runtime is installed locally; the
  net8.0 test *run* and net462 (via netstandard2.0) are CI-only. Both *build* legs
  pass.

## Decisions in force (M3/P1)

- **D1 — wakeup-fault on the in-flight proof op is NOT reachable this phase
  (source-verified deviation from the PLAN's literal wakeup test).** A
  `MockConsumer` observes `wakeup()` **only** inside `poll()`
  (`src/consumer/mock_consumer.rs` poll Step 4); `subscribe`/`seek` never check the
  flag, and `acquire()` (`src/ffi/consumer.rs`) does not either — so the "in-flight
  op faults with a Wakeup `KafkaException` once" assertion needs `poll` (out of
  scope). The full wakeup + cancellation machinery is implemented (correct once poll
  lands); the tested slices are the reachable ones: `Wakeup()` is safe / leaves the
  consumer reusable, and a **pre-canceled** token maps to
  `OperationCanceledException` deterministically. The in-flight-cancel → wakeup →
  `OperationCanceledException` translation is wired (`RegisterCancellation`) but
  only deterministically exercisable once a wakeup-observing op exists.
- **D2 — the concurrency exception-type matrix is tested at the `ConsumerAccessGuard`
  component level (deterministic), not via a forced native op overlap.** Instant
  Mock ops make a genuine submit→callback overlap non-deterministic; the guard is a
  pure managed mirror, so its rejection types (async op → `KafkaException`; state
  read → `InvalidOperationException`) are fully proven as a component. The guard's
  wiring into `NativeConsumer` is exercised by the op / group-metadata tests
  (released between ops; a guarded `GroupId()` round-trips).
- **D3 — `Microsoft.Bcl.AsyncInterfaces` (8.0.0) added for the ns2.0 leg only.** It
  supplies `IAsyncDisposable` + the `ValueTask` async builder absent on the
  netstandard2.0 floor (built-in on net8.0+) — the enabling dependency for the
  primary `DisposeAsync`. A standard facade, conditioned exactly like `System.Memory`
  (ns2.0-only); no NuGet packaging of the binding itself (ffi §0.2 unchanged).
- **D4 — sync `Dispose` kept M2-shape (no drain) + a post-destroy Task-fault.**
  `Dispose` stays `close_with_timeout` → destroy (thread-safe closed flag added),
  NOT sync-over-async; the drain-first path is `DisposeAsync` (primary, ffi §B7).
  **Post-Critic (N=5) fix (Findings 1 + 3):** `close_with_timeout` is a *guarded* sync
  op, so while an async op genuinely holds the core guard the close is rejected
  (ConcurrentModification) and does **not** drain — the following `Consumer_destroy`
  then cancels the op's callback, which (before the fix) stranded the op `Task`
  (Finding 1). `Dispose` now, **after** destroy, faults any pending op's `Task`
  (`OperationCompletionSource.FaultTaskOnly` → `ObjectDisposedException`) so a
  fire-and-forget awaiter cannot strand — via idempotent primitives (`TrySetException`
  no-ops if completed), race-safe against a callback that fired before destroy, and NOT
  sync-over-async (it never waits on the op `Task`). Crucially it does **not** free the
  `GCHandle`: the completion callback is the **sole owner** of that free (Finding 3),
  aligning with the in-repo Python (`Py_DECREF` in the op trampoline; close drains then
  bare `_destroy`) and confluent-kafka-dotnet (`gch.Free()` in the delivery-report
  callback; `Dispose` drains via `callbackTask.Wait()` then destroys). Freeing it from
  `Dispose` was the case-B use-after-free — a completion job queued before destroy fires
  *after* it (the ABI drains queued dispatcher jobs without joining) and must recover a
  live handle. **Accepted residual (case A):** if destroy cancels the op before its
  callback is queued, that one op's `GCHandle` leaks — a rare, one-time, teardown-only
  leak in a misuse case (unawaited in-flight op + sync `Dispose`); the Python/CKD
  siblings accept the same residual, and `DisposeAsync` drains so it has no leak. So an
  op-in-flight sync `Dispose` no longer strands the `Task`; users wanting no leak use
  `DisposeAsync`.
- **N=5 deferred hardening — DONE.** The non-atomic `_disposed` bool is replaced by a
  thread-safe closed flag (`Interlocked`, `TryBeginClose`) + the §B5 access guard, so
  double / concurrent / mixed `Dispose`/`DisposeAsync` are safe. Per the deferred
  note, teardown is guarded the CKD way (thread-safe closed check + access guard) —
  NO per-call `SafeHandle` AddRef, NO close/destroy-as-SafeHandle-param.

## Verification state (M2/P2 DoD — Actor + Critic, all green)

- `cargo build --features ffi` — native cdylib + header present (run FIRST).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + CS1591 active. Removing the
  now-unused `using System;` from the two SafeHandle files and `NativeLoadProbeTests`
  kept IDE0005 from failing the build.
- `dotnet test -f net10.0` — **20 passed, 0 failed** (19 M2/P1 carried + 1 new
  M2/P2 failure-path regression); ~250 ms.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** only the .NET 10 runtime is installed locally; the
  net8.0 test *run* and net462 (via netstandard2.0) are CI-only. Both *build* legs
  pass.

## Decisions in force (M2/P2)

- **SafeHandle-return over `new + SetHandle`** — the three owned-handle
  constructors return their `SafeHandle` subtype directly; the marshaller invokes
  the private parameterless ctor and sets the handle atomically. Classic
  `[DllImport]` feature (no `[LibraryImport]`), supported on the netstandard2.0
  floor incl. net462 — the TFM where the async-abort window actually exists.
- **`FromRaw` removed entirely** — the handle now arrives marshaller-wrapped; no
  call site needs a thin non-marshalling helper, so none was kept (PLAN §2).
- **Defensive `IsInvalid`-without-error guard throws `KafkaException`** — the ABI
  contract says a null `out_error` implies a non-null handle, so a `(null handle,
  null error)` return is a core contract violation (not a caller programmer error),
  surfaced on the operational `KafkaException` surface with a descriptive message.
  Can't-happen per the header; the guard exists so an IsInvalid handle is never
  stored (a later `Handle` read would hand back a null pointer).
- **Unchanged from M2/P1** — `ReleaseHandle` bodies, `NativeConsumer.Dispose`
  graceful close→destroy, the `put` loop, D6 (props stays a SafeHandle in-param),
  the `KafkaError` five decls, `KafkaException.FromHandle`.

## Verification state (M2/P1 DoD — Actor + Critic, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0), with
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
  active. CS1591 is enforced on the public `KafkaException`; no analyzer
  suppressions were needed (the Java-style standard exception constructors satisfy
  CA1032).
- `dotnet test -f net10.0` — **19 passed, 0 failed** (4 M1/P1 carried + 15 new:
  6 error/precondition, 5 lifecycle, 3 config-marshal, 1 D5 round-trip); ~266 ms
  total (broker-less close is near-instant, so the fail-fast timeout guard never
  trips).
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** only the .NET 10 runtime is installed locally; the
  net8.0 test *run* and net462 (via netstandard2.0) are CI-only. Both *build* legs
  pass.

## Decisions in force (M2/P1)

- **D2** — `SafeHandleZeroIsInvalid` base (`IsInvalid => handle == Zero`), not
  `SafeHandleZeroOrMinusOneIsInvalid` (−1 is not our contract; all `_destroy` are
  null-safe).
- **D3 (deviation from CLAUDE.md §4)** — synchronous `IDisposable.Dispose()` only
  this phase; `IAsyncDisposable.DisposeAsync()` deferred with the completion bridge
  (the only close primitive in scope is the synchronous
  `Consumer_close_with_timeout`; wiring `DisposeAsync` now would be
  sync-over-async or depend on the deferred bridge). Recorded in
  `design/history/M2/P1-error-model-safehandle/COMMENTS.DONE.3.md`.
- **D4** — the lifecycle wrapper (`NativeConsumer`) lives under `Internal/` (not
  `Internal/Interop/`): `unsafe`-free (safe `Utf8Marshal.Pin` + `SafeHandle`),
  keeping `unsafe` quarantined to `Internal/Interop/`.
- **D5 — CLOSED (verified empirically).** A configured non-ASCII `group.id`
  surfaces broker-free / pre-join (the core stubs `group_metadata()` from the
  configured id before join), so the UTF-8 config-value round-trip
  (`Consumer_group_metadata` → `group_id` → `PtrToString` == input) is kept, not
  deferred. Adds the three group-metadata DllImports + an owned Category-3 handle
  marshal-then-destroy in the test.
- **D6** — `props` passed to `KafkaConsumer_new` as the SafeHandle type (marshaller
  does DangerousAddRef/Release); disposed in a `finally` after the call (header:
  caller retains props ownership).
- **Dispose close-error handling** — `Dispose()` consumes the close error via
  `FromHandle` (freed exactly once) but does NOT rethrow (Dispose must not throw;
  surfacing close errors is the future `CloseAsync(TimeSpan)`'s job).
- **Decision reversal (post-M2/P2, PR #134 review) — `KafkaException` un-sealed.**
  `KafkaException` is now `public class` (not `public sealed class`), aligning with
  CLAUDE.md §3's sketch (which already shows `public class KafkaException`) + the
  flat-now/typed-later intent (§4 / ffi §A5) — reversing the M2/P1 PLAN's `sealed`
  choice, per user direction during the PR #134 review. Non-breaking (source +
  binary compatible). The archived M2/P1 PLAN + `COMMENTS.DONE.3` are left intact
  as the historical record; this reversal lives here in current STATUS only.

## Verification state (M1/P1 DoD — Actor AND Critic ran independently, all green)
## Verification state (M1/P1 DoD — Actor AND Critic ran independently, all green; re-verified after the M0/P1 rename merge)

- `cargo build --features ffi` — cdylib `target/debug/libconfluent_kafka.dylib`
  + generated header `target/include/confluent_kafka.h` produced (run FIRST,
  CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0), with
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active. `/unsafe+` on the
  library triggered **no** analyzer warnings (CA5392 is opt-in; SYSLIB1054 is
  Info-severity) — so **no suppressions were needed** (M1/P1 decision D5).
- `dotnet test -f net10.0` — **4 passed, 0 failed** (2 native-load probe + 1
  `Utf8Marshal` codec + the M0/P0 sentinel). The native loaded, the first
  `[DllImport]` round-tripped, and UTF-8 marshalled into native correctly.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking this phase):** only the .NET 10 runtime is installed
  locally (`dotnet --list-runtimes` shows only Microsoft.NETCore.App 10.0.x).
  The net8.0 test *run* needs the .NET 8 runtime and net462 needs Windows — both
  are **CI-only**. Both *build* legs succeed; only the *runs* are deferred.
- **Handled by the two-stage pipeline (CLAUDE.md §7.1):** the native-copy target
  uses `<Content>` (not `<None>`) so the cdylib flows transitively to the
  referencing TEST project's output dir, where the probe resolves it via default
  `[DllImport]` probing.

Additional gates specific to M0/P1 (the rename), all green:

- The pre-rename `src/` and `tests/` project directories are fully gone from
  disk — stale `obj/` and `bin/` were destroyed *before* the moves, since
  `git mv` relocates only tracked files and untracked build output would
  otherwise have kept the old directories alive with a stale assembly and a
  cached `project.assets.json` naming the old `AssemblyName`.
- No tracked path carries the old identity, and no build or code file mentions
  it. The exact invariant is **scoped to build and code**, and both halves are
  checkable:
  `grep -rIn "ShareConsumer" . --exclude-dir=design --exclude='COMMENTS*.md'` →
  empty, and `git ls-files | grep -i shareconsumer` → empty. It is scoped rather
  than absolute because **four** documentation surfaces under `design/` name the
  old identity deliberately — the archived M0/P0 `PLAN.md` (6 occurrences,
  including its dated supersession note), the archived M0/P0
  `COMMENTS.DONE.1.md` (2), this phase's own
  `design/history/M0/P1-rename-identity/PLAN.md` (18 — a rename plan must name
  what it renames), and this file itself — its transition narrative above and
  its *Governance pointers* section below. That section links the first three;
  the fourth is this file. The M0/P1 review record `COMMENTS.DONE.1.md` quotes
  them too, hence the second exclusion.
- `.snk` byte-identical across the move (SHA-256 `d33f5c98…8eb197`); the
  `InternalsVisibleTo` `Key=` blob still equals `sn -tp` on the key file, and
  `Include=` matches the test project's `<AssemblyName>`.
- All 7 path changes recorded by git as **renames**, not delete+create.
- `Confluent.Kafka.sln` — a surgical 2-line edit (the two project entries): the
  7 GUIDs, `ProjectConfigurationPlatforms`, `NestedProjects`, the `build`
  folder's `SolutionItems`, and the UTF-8 BOM are all unchanged. The solution
  was **not** regenerated (SDK 10 would emit `.slnx`; classic `.sln` is a
  standing M0/P0 deviation).

## Decisions in force (M1/P1)

- **D1 (un-defers M0/P0 D2)** — native-copy MSBuild target landed; per-OS
  filename via MSBuild, profile from `$(Configuration)`, repo root 4 levels up,
  `<Content>` transitive, never a hardcoded path/filename (ffi §0.2 — the
  **pre-publish** half of the two-phase delivery model).
- **D2** — `<AllowUnsafeBlocks>` on the LIBRARY csproj only; test project stays
  unsafe-free; `unsafe` confined to `Internal/Interop/`.
- **D3** — classic `[DllImport]`, uniform across all TFMs (netstandard2.0 floor
  forbids `[LibraryImport]`/`PtrToStringUTF8`/`LPUTF8Str`).
- **D4** — `Utf8Marshal.Pin` = disposable call-scoped pin (`using`); `Utf8Marshal.PtrToString`
  = NUL-terminated form only (length-delimited receive-path form deferred).
- **D5** — analyzer suppressions contingent; none fired, none added.

## Decisions in force (M0/P0)

- **D1** — Library TFMs `netstandard2.0;net8.0;net10.0` (net462 via ns2.0).
- **D2** — Native-copy MSBuild target + Rust build deferred to the first
  implementation phase — **un-deferred in M1/P1 D1 above**.
- **D3** — Empty folders via `.gitkeep`, no placeholder types.
- **D4** — Test framework = xUnit.

Deviations recorded during execution (see the archived review record under
`design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md`):
- XML-comment MSB4025 fix in the csproj comment (literal `--` illegal in XML
  comments — reworded).
- `.gitkeep` deletion grouped into the csproj commit (commit-grouping only).

## Watch-item for future phases — RESOLVED post-close (`9ae31fa`)

- `PinnedUtf8String` was a `readonly struct` holding a `GCHandle`; `Dispose()`
  freed a compiler defensive copy. Correct under M1/P1's single-`using`
  ownership, but a later phase that **stores or copies** a `PinnedUtf8String`
  would have hit the false "idempotent for a single owner" claim (double-`Dispose`
  / disposed by-value copy would double-free the runtime handle). Recorded in
  `.claude/agent-memory/dotnet-critic/interop_review_patterns.md`.
- **Resolved in `9ae31fa`:** `PinnedUtf8String` is now a `sealed class`, so
  `Dispose()` mutates the real `GCHandle` field (no defensive copy) — the unpin
  is genuinely idempotent and the value-copy double-free hazard is gone. Verified:
  `dotnet build` 0/0 all TFMs, `dotnet test -f net10.0` 4/4, format clean. The
  archived `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md` is left
  unchanged as the phase-close snapshot.

## Review outcome (M3/P1)

Critic (N=5) review of `285b04c`/`d6f3022`/`3d0245f`/`1243350`/`259d098`: all four
DoD gates independently re-verified green; deviations D1–D4 verified sound; the core
bridge (free-once, GCHandle keep-alive, `RunContinuationsAsynchronously`, no-throw
boundary, `DisposeAsync` drain, marshalling, error classification, scope) had **no
defects**. Two findings, both on the sync-teardown / handle-lifetime edges (not the
async bridge):
- **Finding 1 [MEDIUM] — FIXED (its GCHandle-free part corrected by Finding 3).** Sync
  `Dispose` with an async op in flight stranded the op `Task` (and, originally, leaked
  the per-op `GCHandle`): the guarded `close_with_timeout` is rejected (no drain) while
  the op holds the core guard, then `Consumer_destroy` cancels the callback. Fixed by a
  post-destroy Task-fault (`OperationCompletionSource.FaultTaskOnly`; see D4); the
  masking `Dispose_WithOpInFlight` test now observes the op `Task` to a terminal state
  (+ a churn/GC variant). Not sync-over-async, race-safe.
- **Finding 3 [LOW/latent, memory-safety] — RESOLVED (re-review of the Finding-1 fixup).**
  The Finding-1 fix originally freed the `GCHandle` from `Dispose` (`FaultAndReclaim`);
  that is a case-B use-after-free — a completion job queued before `Consumer_destroy`
  fires *after* it (the ABI drains queued dispatcher jobs without joining) and
  dereferences the freed/recycled handle via `GCHandle.FromIntPtr(userData).Target`.
  Resolved by making the completion callback the **sole owner** of the `GCHandle` free
  (`FaultTaskOnly` faults the `Task` only), matching the in-repo Python +
  confluent-kafka-dotnet callback-frees / teardown-drains-not-reclaims pattern. Accepted
  case-A residual: a one-time teardown-only leak if destroy cancels the op before its
  callback is queued (both siblings accept the same); `DisposeAsync` drains and has no
  leak. New OCS component tests drive the straggler callback through the real
  `GCHandle.FromIntPtr` recovery path (proving no UAF). See COMMENTS.DONE.5.
- **Finding 2 [LOW/latent] — ACCEPTED (documented, no code change).**
  Cross-thread `Wakeup()`/`GroupId()` TOCTOU vs teardown; plan-consistent
  (per-call AddRef deliberately declined) and not reachable while internal-only.
  **M3/P2 re-contextualized this as an accepted-by-design residual of the
  single-owner model** (no longer a pending "N=6" fix; future hardening is N≥8,
  since M3/P3 took N=7) — see "STATUS reconciliation (M3/P2)" below.

## Review outcome (M2/P2)

Critic (N=4) review of commits `3359b70`, `6aa2f92` (via `git log`/`git show`):
**0 genuine findings** — clean. Verified the hardening contract exactly: the three
owned-handle constructors return their `SafeHandle` subtype directly (atomic
marshaller create-and-set — the `new + SetHandle` / `FromRaw` two-step is gone,
repo-wide sweep confirms no stale raw-`IntPtr`-return call site); both SafeHandle
subtypes retain the private parameterless ctor (no `MissingMethodException`);
`ownsHandle:true` + `IsInvalid => Zero` + both `ReleaseHandle` bodies unchanged;
the fallible path disposes the IsInvalid handle (ReleaseHandle skipped — no
spurious `Consumer_destroy`) then throws `FromHandle(outError)`; D6 + the graceful
`Dispose` preserved; the new failure-path regression is sound (drives the real
null-native-return 50× and asserts IsInvalid + non-null `out_error` + the
`FromHandle` round-trip). The Critic **independently re-verified** the DoD on this
machine (read-only): `dotnet build` 0/0 across all TFMs, `dotnet test -f net10.0`
20 passed/0 failed. No fix cycle required (one Actor pass → one Critic pass →
close).

## Review outcome (M2/P1)

Critic (N=3) review of commits `558de6a`..`c45f914` (via `git log`/`git show`,
not `cargo xtask await-commit`): **0 genuine findings** — clean. Verified the
full boundary: every `[DllImport]` matches the header (Cdecl, `int64_t`→`long`,
`[MarshalAs(I1)]` on the bool getters, hand-marshalled UTF-8, `out IntPtr` for
`KafkaError_t**`); `FromHandle` (null=success, message-before-free, copy-out,
`_destroy` in `finally`, freed exactly once); `SafeHandle` lifecycle
(`IsInvalid => Zero`, graceful `close_with_timeout` → release, props as the
SafeHandle D6, D5 group-metadata handle read-then-destroy once); preconditions →
`Argument*`/`ObjectDisposedException` (never `KafkaException`); `KafkaException`
the only new public type; D2/D3/D5/D6 recorded; no persona/agent-memory files
committed. No fix cycle required (one Actor pass → one Critic pass → close, as
M1/P1).

## Post-plan additions (M0/P0, interactive — 2026-07-20)

Made after the Critic (N=1) close, in an interactive review pass — these are
NOT part of the approved plan and were NOT put through a separate Critic cycle:
- **Solution items** — a `build` solution folder embedding
  `Directory.Build.props` + `.editorconfig` (ckd parity).
- **`GenerateDocumentationFile=true`** — added to the *library* csproj (NOT the
  shared props, which would make CS1591 break the test build under
  `TreatWarningsAsErrors`). Under TWAE this forces every public API member to
  carry an XML doc — faithful to CLAUDE.md §4 (javadoc → C# XML docs). This
  reverses the initial "dropped" deviation, better-scoped.
- **Strong-naming** — one shared key `Confluent.Kafka.snk` (the file was renamed
  in M0/P1; the key itself is byte-identical and was never regenerated),
  `SignAssembly` wired in `Directory.Build.props` (both projects), and the
  `InternalsVisibleTo` public key on the library csproj. Decided early on
  purpose: adding a strong name after the first published package is a
  binary-breaking change. The `.snk` is committed (identity, not a secret;
  publisher trust is NuGet/Authenticode signing at publish).
- **Review record renamed** — `COMMENTS.1.closed.md` → `COMMENTS.DONE.1.md`
  (the old name was swallowed by the repo-root `COMMENTS\.[0-9]*\.md` gitignore;
  the `DONE` name is tracked and matches the documented mechanics).

## Review outcome (earlier phases)

- **M1/P1** — Critic (N=2) review of commits `9441a4c`, `149e327`, `9423fa4`:
  **0 genuine findings** — clean, independently build/test/format-verified. No fix
  cycle required (one Actor pass → one Critic pass → close).
- **M0/P1** — Critic (N=1): 3 review cycles, 4 items, **all closed**
  (`COMMENTS.1.md` empty). The substantive one was the `IsPackable` default
  inversion recorded above; items 2–4 were STATUS.md documentation-accuracy
  defects.
- **M0/P0** — Critic (N=1) review of commits `f1fb7fc`, `f93a4ef`, `c910fca`:
  **0 genuine findings** — clean skeleton, verified by an independent
  build/test/format run.

## Governance pointers

Current phase (**M4/P4a** — public consumer client):

- Approved plan: `design/history/M4/P4a-public-consumer/PLAN.md` (current). Prior:
  `design/history/M3/P3-poll-receive-path/PLAN.md`,
  `design/history/M3/P2-single-owner-alignment/PLAN.md`,
  `design/history/M3/P1-completion-bridge/PLAN.md`,
  `design/history/M2/P2-safehandle-return-hardening/PLAN.md`,
  `design/history/M2/P1-error-model-safehandle/PLAN.md`,
  `design/history/M1/P1-interop-scaffolding/PLAN.md`.
- Closed review records:
  `design/history/M3/P3-poll-receive-path/COMMENTS.DONE.7.md`,
  `design/history/M3/P1-completion-bridge/COMMENTS.DONE.5.md`,
  `design/history/M2/P2-safehandle-return-hardening/COMMENTS.DONE.4.md`,
  `design/history/M2/P1-error-model-safehandle/COMMENTS.DONE.3.md`,
  `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md` (M3/P2's closed record
  is archived alongside its plan). M4/P4a: no Critic review yet — the archived record
  will be `design/history/M4/P4a-public-consumer/COMMENTS.DONE.8.md`.
- Personas: `dotnet-actor` (Actor **N=8**), `dotnet-critic` (Critic **N=8**). NEVER
  the Rust `actor-executor` / `kafka-critic`. The working `COMMENTS.8.md` is gitignored;
  the execution record `COMMENTS.DONE.8.md` is tracked but never `git add`ed into a code
  commit. Lands on `prashah_dev_public_consumer_scaffolding` (a NEW PR stacked on the
  M3/P3 PR) — additive commits, no history rewrite.
- **N-counter reconciliation:** M4/P4a **takes N=8**. The STATUS / `NativeConsumer`
  cross-thread **hardening** items previously labeled "N≥8, whenever a public client
  makes `Wakeup()` genuinely cross-thread" (the `Wakeup()`/`GroupMetadata()` handle
  TOCTOU vs teardown; the submit-vs-`destroy` handle race) are now **reachable in
  principle** — P4a is that public client. Per locked decision 5 = option (a) they stay
  **accepted-by-design, documented** on the public client (not scheduled); a future
  per-call `DangerousAddRef` hardening (and/or the D8.8 dispatcher-join) is renumbered
  **N=9** (candidate follow-up, unscheduled). No dangling "N≥8" label remains.

Previous phase (**M0/P1** — rename identity):

- Approved plan: `design/history/M0/P1-rename-identity/PLAN.md`.
- Closed review record: `design/history/M0/P1-rename-identity/COMMENTS.DONE.1.md`.

Previous phase (**M0/P0** — scaffolding):

- Approved plan: `design/history/M0/P0-scaffolding/PLAN.md`. Carries a dated
  supersession note: the phase shipped under the
  `Confluent.Kafka.ShareConsumer` identity, and its body is preserved verbatim
  as the record of what was approved and verified at the time.
- Closed review record: `design/history/M0/P0-scaffolding/COMMENTS.DONE.1.md` —
  left **verbatim** on purpose. It records the Critic's *verified* finding about
  the then-current `InternalsVisibleTo` name, so editing it would make a
  historical verification claim describe an assembly name that did not exist
  when the check ran.

## Next up (not started)

The **additive consumer op families** that grow the P4a public surface without
changing its shape (still Mode A unless a new ABI function is needed). Candidates, each
its own later phase:
- **Commit family** (`CommitSync` / `CommitAsync`) — needs a naming decision (the ABI
  `Consumer_commit_async` is Java's fire-and-forget *sync* `commitAsync`; the *push*
  variant of `commitSync` is `Consumer_commit_sync_async`).
- **`position`** — the scalar-callback (Category B) completion shape, not yet proven.
- **`Assignment` / `Subscription` / `Paused`** — owned-list sync marshalling
  (`TopicPartitionList_t` / `StringList_t`).
- **Owned-handle query siblings** — `committed` / `offsetsForTimes` /
  `beginning|endOffsets` / `partitionsFor` / `listTopics` (each a new result container).
- **`subscribe(pattern)` / `assign` (public) / `pause` / `resume` /
  `seekToBeginning`/`seekToEnd`**, `ConsumerRebalanceListener` /
  `OffsetCommitCallback`, serializers + generic `IConsumer<TKey,TValue>`, typed
  `KafkaException` subclasses, `CloseAsync(TimeSpan)` (needs a Rust-core
  `close_async_with_timeout` — Mode B).

Candidate N=9 hardening (unscheduled): the per-call `SafeHandle.DangerousAddRef` /
`DangerousRelease` around `Wakeup()` / `GroupMetadata()` now that `Wakeup()` is a public
cross-thread API; and/or a Rust-core dispatcher-join on `Consumer_destroy` (would also
close the parallel-test host-crash residual — D8.8 — and let the suite re-enable
parallelization). Sequence and exact scope to be set in the next PLAN (Manager, with
approval).

### Deferred hardening (N=5) — teardown thread-safety: **DONE (M3/P1, 2026-07-27)**

Delivered this phase (see "Decisions in force (M3/P1)"): the non-atomic `_disposed`
bool is replaced by a thread-safe closed flag (`Interlocked` + `TryBeginClose`) plus
the §B5 access guard, so double / concurrent / mixed `Dispose`/`DisposeAsync` are
safe and use-after-dispose throws. Guarded the CKD way (thread-safe closed check +
access guard) — NO per-call `SafeHandle` AddRef, NO close/destroy-as-SafeHandle
param, matching the deferred note's guidance. `DisposeAsync` is the primary
drain-first path (drain in-flight → `close_async` → destroy); `Dispose` stays the
M2-shape blocking fallback — **now with a post-destroy Task-fault** (Critic N=5
Findings 1 + 3, see D4) so an op-in-flight sync `Dispose` faults the op `Task` (never
freeing the `GCHandle` — the completion callback is the sole owner) instead of
stranding, with an accepted one-time case-A teardown residual (`DisposeAsync` has no
leak). Ops remain single-threaded-with-rejection; only `wakeup()` is cross-thread.

### STATUS reconciliation (M3/P2) — the two pre-labeled "N=6 deferred" items resolved

M3/P2 **takes review counter N=6**, so the two items `STATUS.md` previously
pre-labeled "N=6 deferred" for a *future* review collided with this phase. They are
now resolved (no dangling or contradictory "N=6 deferred" label remains — the only
N=6 is M3/P2 itself):

- **Item 1 — `Wakeup()`/`GroupId()` handle TOCTOU vs teardown (Critic N=5 Finding
  2): re-contextualized as an accepted-by-design residual of the single-owner
  model.** It is now one of the three enumerated accepted residuals (see the
  `NativeConsumer` class doc): under the not-thread-safe contract the closed-flag
  check and the `DangerousGetHandle()` deref are deliberately not atomic, so a
  concurrent teardown between them is a use-after-free reachable only under
  cross-thread misuse. Python has the same, more exposed (its `wakeup` has no closed
  check at all). It is **not** a pending N=6 fix. If a *future* hardening is ever
  wanted (per-call `SafeHandle.DangerousAddRef`/`DangerousRelease` around the native
  call, or a documented no-concurrent-teardown precondition), it renumbers to **N≥8**
  (M3/P3 took N=7) — whenever the public client makes `Wakeup()` genuinely cross-thread.
- **Item 2 — op-submit vs concurrent teardown window: ELIMINATED by M3/P2.** The
  window existed because `SubmitVoidOperation` published `_inFlightContext` /
  `_inFlightOperation` *after* the native `submit(...)`, leaving a gap where a
  concurrent teardown saw `null` and could neither drain nor fault the op. M3/P2
  removes those tracking fields entirely, so **there is nothing to publish and no
  window** — the race is structurally removed, not deferred. Any residual
  submit-vs-`destroy` *handle* race folds into the third accepted-by-design residual
  (`DangerousGetHandle()` in `SubmitVoidOperation` vs a concurrent `Consumer_destroy`,
  cross-thread misuse only); any future hardening is **N≥8** (M3/P3 took N=7).

**Accepted residuals (M3/P2, enumerated in the `NativeConsumer` class doc).** All
three are explicitly accepted, misuse-only, not-reachable-while-internal (Python
parity) under the single-owner not-thread-safe contract:
1. teardown-with-unawaited-in-flight-op → strand + one-time `GCHandle`/context leak
   (the Finding-1 `FaultTaskOnly` machinery is intentionally NOT re-added);
   `DisposeAsync` on the awaiting task is the clean, leak-free path;
2. `Wakeup()` / `GroupId()` handle TOCTOU vs teardown → UAF under cross-thread misuse
   (Item 1 above);
3. submit-vs-`destroy` handle race → UAF under cross-thread misuse (Item 2's residual
   handle race).
