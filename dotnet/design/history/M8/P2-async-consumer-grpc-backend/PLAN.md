# M8/P2 — ".NET async-consumer gRPC backend (`dotnet_async`) for the multilanguage harness"

Status: **APPROVED (maintainer 2026-08-11).** N=24.
Branch: `prashah_dev_public_consumer_grpc_setup` (M8/P1 complete + validated: sync `dotnet` backend, `815be651`).

---

## 0 · Identity

- **Binding:** `.NET` (`bindings/dotnet/`), plus additive Rust-harness wiring outside the binding.
- **Milestone / Phase:** **M8 / P2** — the async twin of M8/P1. P1 shipped the sync `dotnet` backend; **P2 = the `dotnet_async` backend** driving the .NET **`AsyncKafkaConsumer<byte[],byte[]>`** (+ `AsyncMockConsumer` for empty config), mirroring `python` vs `python_async`.
- **Assigned N (monotonic):** **24**.
- **Branch:** `prashah_dev_public_consumer_grpc_setup`.
- **Mode:** **Mode A** for the binding — **no new C# production code, no `confluent_kafka.h` / `src/ffi` change** (verified §2: every RPC maps 1:1 to a shipped async method). New code = a second servicer class + a `Program.cs` flavor selector + Rust-harness wiring — all test-harness artifacts, no Kafka logic, nothing shipped in `Confluent.Kafka`.
- **Value beyond P1 (the point of this phase):** the sync backend calls the sync C ABI (`block_on` inside the core). The **async** backend exercises the .NET **completion bridge end-to-end** — `TaskCompletionSource`, the foreign callback-dispatcher thread, `RunContinuationsAsynchronously` (`ffi-marshalling.md §B6/§B7`) — the exact machinery the committed UAF fix `0ff4c9ed` touches, which the sync path never covers.

## 1 · Objective & context

Add a **6th backend** (`dotnet_async`) so the same **11** `multilanguage_consumer_test!` scenarios (`tests/integration/multilanguage_consumer_test.rs:350-360`) ALSO run against the .NET **async** consumer against a real broker — exactly as they now run against `dotnet` (sync), `rust`, `python`, `python_async`, and `c`. Consumer-only stays clean because those tests seed data with a native in-process Rust producer (`multilanguage_consumer_test.rs:69-87`), not the backend. Harness architecture: `design/history/MILESTONE-6/DESIGN-multilanguage-tests.md`. This phase reuses the M8/P1 server wholesale (`bindings/dotnet/design/history/M8/P1-sync-consumer-grpc-backend/`).

## 2 · Grounded findings — VERIFIED against current code

Citations are current (`file:line`). Divergences from the pre-pass are called out — **none changes scope / breaks Mode A.**

1. **Mode A holds — every RPC maps 1:1 to a shipped async method.** `AsyncKafkaConsumer<TKey,TValue>` (`bindings/dotnet/src/Confluent.Kafka/AsyncKafkaConsumer.cs`) exposes, as **`Task`** (async): `Poll` (:94), `Subscribe` (:98), `Unsubscribe` (:102), `Assign` (:118), `Pause` (:122), `Resume` (:126), `SeekToBeginning` (:130), `SeekToEnd` (:134), `Position` (:138), `Commit()` (:142) / `Commit(offsets)` (:146), `Committed` (:151), `OffsetsForTimes` (:156), `BeginningOffsets` (:161), `EndOffsets` (:166), `PartitionsFor` (:171), `ListTopics` (:175), `Close` (:179); and as **sync** (`void`/scalar, on `IConsumerCommon`): `Seek(tp,long)` (:106), `Seek(tp,OffsetAndMetadata)` (:110), `Wakeup` (:183), `Assignment` (:189), `Subscription` (:192), `Paused` (:195). `AsyncMockConsumer` (`AsyncMockConsumer.cs:54`) mirrors the surface + `AddRecord` (:261). ✅ **No new binding/production code required.** If the Actor finds any gap → STOP + flag (would be Mode B).

   ⚠ **Divergence from grounded finding #1 ("Seek/SeekToBeginning/SeekToEnd are async").** Reality: **`Seek` (both overloads) is SYNC `void`** (`AsyncKafkaConsumer.cs:106,110`) — it is on `IConsumerCommon`, shipped sync for Python parity (the CLAUDE.md §4 divergence, identical to the sync consumer). Only **`SeekToBeginning`/`SeekToEnd` are async `Task`** (:130,:134). So in the async servicer the `Seek` RPC calls `Seek(...)` **synchronously** (no `await`), while `SeekToBeginning`/`SeekToEnd` are awaited. No Mode A impact — 1:1 mapping preserved.

   ⚠ **Divergence — `AsyncKafkaConsumer.Close` has NO `TimeSpan` overload** (only `Close(CancellationToken)`, `AsyncKafkaConsumer.cs:179`; grep confirms no second `Close`). The sync backend honors the proto's `optional int64 timeout_ms` via `Close(TimeSpan.FromMilliseconds(...))` (`ConsumerServiceImpl.cs:399-406`); the async binding cannot (no timed async close — CLAUDE.md §1 "the timed *consumer* close is deferred ... a **gap**"). ⇒ the async `Close` RPC handler **ignores `timeout_ms`** and calls `await Close()`. Documented divergence, **behaviorally invisible** to the 11 scenarios (none assert close-timeout).

2. **P1 is the template — reused wholesale.** `ConsumerServiceImpl.cs` (the 23-RPC sync servicer), `Translate.cs`, `Program.cs`, the `.csproj` (`grpc-server/Confluent.Kafka.GrpcServer.csproj`), `Dockerfile.grpc`, `bindings/dotnet/Makefile`, and the Rust harness plug points. The async servicer differs from `ConsumerServiceImpl.cs` ONLY in: (a) construct `AsyncKafkaConsumer`/`AsyncMockConsumer` (b) `await` the `Task`-returning ops (c) an **async** per-id gate (§4). ✅ Verified.

3. **`Translate.cs` is flavor-agnostic — reused VERBATIM.** Its surface (`GuessVariant`, `ToProto`, `UnknownConsumer`, `Tp`/`TpToProto`, `OamToProto`, `OatToProto`, `NodeToProto`, `PartitionInfoToProto`, `RecordToProto`) operates only on shared types (`ConsumerRecord<byte[],byte[]>`, `TopicPartition`, `OffsetAndMetadata`, `OffsetAndTimestamp`, `PartitionInfo`, `Node`) — grep confirms **zero** references to `IConsumer`/`KafkaConsumer`/`MockConsumer`/`AsyncKafkaConsumer` (`grpc-server/Translate.cs`). ✅ No change.

4. **The server is ONE project → a second servicer is just another `.cs` file; NO `.csproj` change.** The `.csproj` already `ProjectReference`s `Confluent.Kafka` (which ships BOTH `AsyncKafkaConsumer` and `KafkaConsumer` in one DLL) and does the proto codegen (consumer=Server, producer=None). Adding `AsyncConsumerServiceImpl.cs` needs no project edit. ✅ Verified.

5. **Harness plug points (additive; mirror the `python`/`python_async` image pair).**
   - `tests/common/backend_pool.rs` — `BackendKind` is `{ Python, PythonAsync, C, Dotnet }` (`:45-53`), with `Dotnet` → repo `confluent-kafka-rust/dotnet-grpc-server` (:62), port 50053 (:75), label `"dotnet"` (:84); `Python`/`PythonAsync` BOTH bind internal 50051 in separate containers (`:66-72`). Add a `DotnetAsync` variant → repo `confluent-kafka-rust/dotnet-async-grpc-server`, **internal port 50053** (mirror Python — separate container, so re-using 50053 is safe), label `"dotnet_async"`. **No `with_env_var`/`env_vars()` wiring** — each image bakes its own flavor (§4).
   - `tests/common/backend_factory.rs` — add `DotnetAsyncGrpcFactory` implementing **ONLY `ConsumerBackendFactory`** (`MultilanguageConsumer::new(channel, config, "dotnet_async")`, `needs_container_bootstrap()==true`, `name()=="dotnet_async"`) + add to the `grpc_backends` re-export. No `ProducerBackendFactory`.
   - `tests/common/multilanguage_consumer_test_macro.rs` — the `__dotnet` arm exists (`:91-94`, `BackendKind::Dotnet`); add a `__dotnet_async` arm mirroring it. Leave `multilanguage_test!` (producer) untouched.
   - `tests/common/multilanguage_consumer.rs` — backend-agnostic (carries a label string); reuse verbatim.

6. **Carry-forward P1 build learnings (now known-good).** `Dockerfile.grpc` builder is `mcr.microsoft.com/dotnet/sdk:10.0` (NETSDK1045 fix, `815be651`), runtime `aspnet:8.0` (`Dockerfile.grpc:30,63`). Local Apple-Silicon runs need the **linux/amd64 emulated** path (arm64 Grpc.Tools protoc SIGSEGVs); x86_64 CI is native. The new async Dockerfile is a near-copy, so these are inherited.

7. **Two-image Makefile shape is already the house style — mirror it.** The top-level `build-grpc-images` (`Makefile:87-91`) runs python `grpc-image` + python **`grpc-image-async`** + c `grpc-image` + dotnet `grpc-image`; `bindings/python/Makefile` carries both a `grpc-image` and a `grpc-image-async` target (the async one tags `python-async-grpc-server:dev` from `Dockerfile.grpc.async`); Python's `Dockerfile.grpc.async` is **standalone** — its own `FROM`, not `FROM …python-grpc-server:dev` (`bindings/python/Dockerfile.grpc.async:15-18`). `bindings/dotnet/Makefile` currently has ONLY `grpc-image` (tags `dotnet-grpc-server:dev`). ✅ Verified — the dotnet async target/image/line slot in exactly parallel to Python.

## 3 · In scope / Out of scope

**In scope (P2):**
- `AsyncConsumerServiceImpl.cs` — the async servicer over `AsyncKafkaConsumer`/`AsyncMockConsumer<byte[],byte[]>`.
- `Program.cs` — the `CONSUMER_FLAVOR` selector (§4) registering the sync **or** async servicer.
- **New `bindings/dotnet/Dockerfile.grpc.async`** — a standalone near-copy of `Dockerfile.grpc` hosting the async flavor, tagging `dotnet-async-grpc-server:dev`.
- **Edit `bindings/dotnet/Dockerfile.grpc`** — add `ENV CONSUMER_FLAVOR=sync` (behavior-preserving; §4).
- **New `grpc-image-async` target** in `bindings/dotnet/Makefile` + **one new line** in the top-level `Makefile` `build-grpc-images`.
- Rust harness 6th arm: `backend_pool` `DotnetAsync` (own image, port 50053, no env injection), `backend_factory` `DotnetAsyncGrpcFactory` (consumer-only), macro `__dotnet_async` arm.

**Out of scope (explicitly):**
- **NO `ProducerService`** (consumer-only; `multilanguage_test!` untouched).
- **NO** `ConsumerRebalanceListener` / `OffsetCommitCallback` / regex-pattern-subscribe RPCs (proto omits them; those tests stay native-Rust-only).
- **The sync `dotnet` backend behaves exactly as in P1** — the only edit to its `Dockerfile.grpc` is a behavior-preserving `ENV CONSUMER_FLAVOR=sync` (the default already resolves to `sync`); the sync image is otherwise its own untouched artifact (§4 isolation).
- **NO** new C# production code in `src/Confluent.Kafka/**`; **NO** `confluent_kafka.h` / `src/ffi` change (Mode A).

## 4 · Design shape — TWO images / Python-parity (maintainer decision)

**Decision (maintainer, 2026-08-11): two separate image artifacts, mirroring `python` / `python_async`.** A new standalone `Dockerfile.grpc.async` tags `confluent-kafka-rust/dotnet-async-grpc-server:dev` and hosts the async flavor; the sync `dotnet` backend keeps its own `dotnet-grpc-server:dev`. This supersedes the earlier draft's one-image + harness-injected-`CONSUMER_FLAVOR` approach.

**Rationale (why two images, per the maintainer):**
1. **True isolation.** Two artifacts mean a P2 change cannot touch the shipped P1 sync `dotnet` image. The one-image plan's "sync-still-green regression guard" and its **load-bearing `CONSUMER_FLAVOR=sync` default footgun** both disappear — the sync image is a separate, essentially-unchanged artifact.
2. **Cross-binding consistency** (`bindings/CLAUDE.md` mirror-the-siblings). Every `BackendKind` maps to **one image with one behavior**; `python`/`python_async` ↔ `dotnet`/`dotnet_async` then read as parallel image pairs. The harness needs no runtime env injection and no special-case per-kind env wiring.

*(Trade-off noted for the record: because .NET compiles to one `Confluent.Kafka.dll` that carries BOTH consumers and `Translate.cs` is flavor-agnostic (§2.3/§2.4), the two images share an essentially identical app binary — the split is achieved by each image pinning its flavor via a baked `ENV`, not by different code. The maintainer accepts this: "two scripts" in Python maps to "two images that each pin one servicer" in .NET. The cost is a second Dockerfile + a near-duplicate publish; the benefit is the isolation + sibling-parity above.)*

**Mechanism — flavor pinned per image at BUILD time (no harness injection):**
- Keep the single `Program.cs` selector: read `CONSUMER_FLAVOR`; `async` → `AddSingleton<AsyncConsumerServiceImpl>()` + `MapGrpcService<AsyncConsumerServiceImpl>()`; otherwise the sync pair. `GRPC_PORT` stays env-configurable (`Program.cs:73-82`, default 50053).
- **`Dockerfile.grpc.async`** sets `ENV CONSUMER_FLAVOR=async`; **`Dockerfile.grpc`** gains `ENV CONSUMER_FLAVOR=sync`. Each image is then **self-describing and single-behavior**, and the selector's implicit default is no longer load-bearing.
  - *Adopted: pin both explicitly* (the maintainer's preferred option). Adding `ENV CONSUMER_FLAVOR=sync` to `Dockerfile.grpc` is a one-line, **behavior-preserving** change (the default already resolves to `sync`), so the existing `…__dotnet` variants stay green. *(Alternative the maintainer allowed: leave `Dockerfile.grpc` byte-for-byte unchanged and rely on the `sync` default — rejected here only because pinning both is more self-describing and is what delivers the isolation the maintainer asked for.)*
- **`backend_pool.rs` `BackendKind::DotnetAsync`:** image repo `confluent-kafka-rust/dotnet-async-grpc-server`, **internal port 50053** (mirror Python — `python`/`python_async` both bind 50051 in separate containers, so both dotnet images binding 50053 in separate containers is safe), label `"dotnet_async"`. **No `env_vars()` / `with_env_var`** and **no 50054** — dropped from the one-image draft; the image bakes its own flavor.

**Work items — Docker / Make (new in this revision):**
- **(new) `bindings/dotnet/Dockerfile.grpc.async`** — a **standalone** near-copy of `Dockerfile.grpc` (builder `mcr.microsoft.com/dotnet/sdk:10.0`, runtime `mcr.microsoft.com/dotnet/aspnet:8.0`, same repo-root build context, same native-lib staging + `LD_LIBRARY_PATH=/app`, same `dotnet publish` of `Confluent.Kafka.GrpcServer.csproj`), differing only by `ENV CONSUMER_FLAVOR=async`, `EXPOSE 50053`, and a header comment. **Standalone** (its own `FROM`, NOT `FROM …dotnet-grpc-server:dev`) — mirrors `bindings/python/Dockerfile.grpc.async:15-18`.
- **(edit) `bindings/dotnet/Dockerfile.grpc`** — add `ENV CONSUMER_FLAVOR=sync` (behavior-preserving).
- **(new) `grpc-image-async` target** in `bindings/dotnet/Makefile` — mirrors the existing `grpc-image` (`docker build -t confluent-kafka-rust/dotnet-async-grpc-server:dev -f …/bindings/dotnet/Dockerfile.grpc.async <repo root>`); add it to `.PHONY` and give it its own `GRPC_ASYNC_IMAGE_TAG` var (Python precedent).
- **(edit) top-level `Makefile` `build-grpc-images` (`:87-91`)** — add exactly one line after the dotnet `grpc-image` line: `$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image-async`.

**The async servicer (`AsyncConsumerServiceImpl.cs`) — a copy of `ConsumerServiceImpl.cs` with exactly these differences:**
- Stores `IAsyncConsumer<byte[],byte[]>`; `CreateConsumer` builds `new AsyncKafkaConsumer<byte[],byte[]>(config, Serdes.ByteArray, Serdes.ByteArray)` (empty config → `new AsyncMockConsumer<byte[],byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest")`).
- Handlers `await` the `Task`-returning ops (Poll/Subscribe/Unsubscribe/Assign/Pause/Resume/SeekToBeginning/SeekToEnd/Position/CommitSync→`Commit`/Committed/OffsetsForTimes/BeginningOffsets/EndOffsets/PartitionsFor/ListTopics). Handlers call the **sync** members directly (no `await`): `Seek` (both overloads), `Wakeup`, `Assignment`, `Subscription`, `Paused`.
- **Async per-id gate:** replace P1's `lock (entry.Gate)` (`ConsumerServiceImpl.cs:105` etc.) with a per-id **`SemaphoreSlim(1,1)`** — `await gate.WaitAsync(); try { ... } finally { gate.Release(); }` — because a monitor `lock` **cannot** be held across `await`. **`Wakeup` stays gate-exempt** (same rationale as P1: it must interrupt an in-flight awaited `Poll`; gating deadlocks).
- **`Close` RPC:** `await Close()` — **ignores `timeout_ms`** (no timed async close, §2.1). Idempotent on unknown id (Python parity).
- Reuses `Translate.cs`, `Get`/`IsEmptyConfig`/`ToTopicPartitions`/`UnknownConsumer` patterns verbatim (async-adapted).
- **NO `Task.Run` / sync-over-async** — handlers are already `Task`-returning, so `await` the binding's `Task` directly (`ffi-marshalling.md §B7`).

## 5 · Risks / notes

- **Async completion bridge under emulation (the P2-specific one).** The async path drives the foreign callback-dispatcher thread + `TaskCompletionSource` + `RunContinuationsAsynchronously` — the UAF-fix machinery (`0ff4c9ed`). Under **linux/amd64 emulation on Apple Silicon** the P1 close-timeout flake is **amplified** on the async close/bridge path (stretched timing can make close/dispose look like a hang/timeout). This is a **local-emulation timing artifact, NOT a code defect** — native x86_64 CI is authoritative. Do not chase an emulated close-timeout flake as a P2 bug; validate green on native CI.
- **Async gate must be `SemaphoreSlim`, not `lock`** (can't hold a monitor across `await`); `Wakeup` exempt.
- **No sync-over-async** (`Task.Run` wrapping) — forbidden (`ffi §B7`); await directly.
- **Flavor is baked per image (`ENV CONSUMER_FLAVOR`), not injected by the harness** — so the selector's implicit default is no longer load-bearing and the two images are isolated artifacts. The one edit to the sync `Dockerfile.grpc` (`ENV CONSUMER_FLAVOR=sync`) is behavior-preserving; if the async servicer has a bug it can only affect the `dotnet-async-grpc-server:dev` image, never the sync `dotnet-grpc-server:dev` one.
- **Two near-duplicate Dockerfiles must not drift** — keep `Dockerfile.grpc.async` a faithful copy of `Dockerfile.grpc` (only `ENV`/comment differ), the same discipline Python's two Dockerfiles follow.
- **arm64 protoc SIGSEGV** (P1 learning) — inherited by the async image (same builder); the image build must run under linux/amd64. Native CI is fine.
- **Close ignores `timeout_ms`** on the async backend (§2.1) — documented, behaviorally invisible.

## 6 · Definition of Done (per `.claude/rules/definition-of-done.md` + `bindings/CLAUDE.md`)

- `make build-grpc-images` builds **BOTH** dotnet images (`dotnet-grpc-server:dev` + the new `dotnet-async-grpc-server:dev`) with no error.
- `cargo test --features integration-tests,multilanguage-tests` — **all 11 `…__dotnet_async` variants green** (parity with `…__dotnet` / `…__rust` / `…__python` / `…__c`), AND the existing `…__dotnet` (sync) variants **still green** — now trivially isolated (a separate, essentially-unchanged image).
- **No change** to `confluent_kafka.h`, `src/ffi/**`, or `src/Confluent.Kafka/**` (Mode A; confirm `git diff --stat`). Review ground truth = the C ABI header + the Java public API (shape, not logic).
- The producer macro `multilanguage_test!` and `ProducerBackendFactory` untouched; the sync `dotnet` backend untouched.
- The server project builds clean under `Directory.Build.props` analyzers; `dotnet format --verify-no-changes` clean.
- Rust harness edits pass `cargo build`, `cargo xtask format-check`, `cargo xtask lint`.

## 7 · Validation steps

1. `cargo build --features ffi --release` (native + header).
2. `make build-grpc-images` — confirm BOTH dotnet images build (`dotnet-grpc-server:dev` + `dotnet-async-grpc-server:dev`).
3. `docker run … confluent-kafka-rust/dotnet-async-grpc-server:dev` — no env needed (flavor is baked); confirm `listening` on stderr + a plaintext-h2c port.
4. `cargo test --features integration-tests,multilanguage-tests <scenario>__dotnet_async` for each of the 11; then the full consumer suite (also re-run `…__dotnet` to prove the sync image is unaffected).
5. `git diff --stat` — zero production/ABI/header churn; producer macro/factory untouched.
6. `cargo xtask format-check && cargo xtask lint`; `dotnet format --verify-no-changes` (server project).
7. Prefer a **native x86_64** runner for the test run (emulation amplifies the async close-timeout flake, §5).

## 8 · Phase checklist

- [ ] (server) `AsyncConsumerServiceImpl.cs` — `IAsyncConsumer<byte[],byte[]>`, await Task-ops, sync Seek/Wakeup/Assignment/Subscription/Paused, `SemaphoreSlim` per-id gate (Wakeup exempt), Close ignores timeout_ms, `Translate` reused
- [ ] (server) `Program.cs` — `CONSUMER_FLAVOR` selector wiring the async servicer (no csproj change)
- [ ] (docker) new standalone `Dockerfile.grpc.async` (`ENV CONSUMER_FLAVOR=async`, tags `dotnet-async-grpc-server:dev`, EXPOSE 50053) + `Dockerfile.grpc` gains `ENV CONSUMER_FLAVOR=sync`
- [ ] (make) new `grpc-image-async` target in `bindings/dotnet/Makefile` + one new line in top-level `build-grpc-images`
- [ ] (harness) `backend_pool.rs` `DotnetAsync` (repo `dotnet-async-grpc-server`, port 50053, label `dotnet_async`; NO env injection)
- [ ] (harness) `backend_factory.rs` `DotnetAsyncGrpcFactory` (Consumer-only) + re-export
- [ ] (harness) macro `__dotnet_async` arm mirroring `__dotnet`
- [ ] DoD §6 green (both dotnet images build; 11 `…__dotnet_async` + sync `…__dotnet` still green); Mode A `git diff --stat` clean; `COMMENTS.DONE.24` records the two-image + async-gate + Close-timeout decisions

## 9 · Comment workflow & handoff (Manager, N=24)

`dotnet-actor N=24` (server async variant + harness arm; per-path staging — never `git add` the root `.claude/agents/dotnet-*.md` discovery copies or the `COMMENTS.*24.md` working files, `bindings/dotnet/CLAUDE.md §8.4`) → DoD §6 → `dotnet-critic N=24` (async-gate correctness / no sync-over-async / `CONSUMER_FLAVOR=sync` default preserved / consumer-only invariant / Close-timeout divergence / no production-ABI churn) → fix cycle until `COMMENTS.24.md` empty + DoD passes → archive `COMMENTS.DONE.24.md` under `design/history/M8/P2-async-consumer-grpc-backend/`, update `design/current/STATUS.md` (M8/P2 DONE), reset `COMMENTS.24.md`. **Loop does not start until the maintainer approves this PLAN.**
