# M8/P1 — ".NET sync-consumer gRPC backend for the multilanguage harness"

Status: **APPROVED (maintainer 2026-08-11).** N=23.
Branch: `prashah_dev_public_consumer_grpc_setup` (on top of UAF fix `0ff4c9ed`, on top of finished M7/P2a).

---

## 0 · Identity

- **Binding:** `.NET` (`bindings/dotnet/`), plus additive Rust-harness + proto-codegen wiring outside the binding.
- **Milestone / Phase:** **M8 / P1**. M8 = ".NET binding joins the Rust multilanguage integration-test harness." **P1 = sync consumer backend only.**
- **Assigned N (monotonic):** **23**.
- **Branch:** `prashah_dev_public_consumer_grpc_setup`.
- **Mode:** **Mode A** for the binding (§6.2 CLAUDE.md) — **no new C# production code, no C ABI / `confluent_kafka.h` change** (verified: every RPC maps 1:1 to a shipped method, §2 below). The *new* code is a **test-harness server project** + Docker/Makefile/Rust-harness wiring — none of it is Kafka logic and none ships in the `Confluent.Kafka` package.
- **Deferred to M8/P2 (OUT OF SCOPE here):** a `dotnet_async` twin driving `AsyncKafkaConsumer` (the async sibling), mirroring `python_async`. Not planned in this document.

## 1 · Objective & context

Add a **.NET consumer-only gRPC backend** to the existing Rust multilanguage integration-test harness so the repo's consumer integration tests (`tests/integration/multilanguage_consumer_test.rs`, fanned out by `multilanguage_consumer_test!`) also run against the **.NET binding's synchronous `KafkaConsumer<byte[],byte[]>`** against a real broker — exactly as they already run against `rust`, `python`, `python_async`, and `c`. The harness architecture is documented in `design/history/MILESTONE-6/DESIGN-multilanguage-tests.md`.

This is the consumer-side twin of the existing Python/C producer+consumer backends, but **consumer-only** — and that is *clean* because consumer test bodies seed their data with a **native in-process Rust producer**, not the backend's producer (`tests/integration/multilanguage_consumer_test.rs:67-87`, the `produce()` helper). So the .NET backend never needs a `ProducerService`.

## 2 · Grounded findings — VERIFIED against current code

Each grounded finding was re-checked; citations are current (`file:line`). Divergences from the pre-pass are called out.

1. **Consumer-only is clean.** `produce()` builds a native `KafkaProducer<Vec<u8>,Vec<u8>>` on the host-loopback bootstrap and produces setup records itself (`multilanguage_consumer_test.rs:69-87`); the backend under test is only ever the consumer (`collect()` at `:91-105`). ⇒ the .NET backend implements **`ConsumerService` ONLY, never `ProducerService`**. ✅ Verified.

2. **No binding/production gap — every proto RPC maps 1:1 to a shipped sync method.** The sync `KafkaConsumer<TKey,TValue>` (`bindings/dotnet/src/Confluent.Kafka/KafkaConsumer.cs`) exposes: `Poll` (:90), `Subscribe` (:94), `Unsubscribe` (:97), `Assign` (:100), `Pause` (:103), `Resume` (:106), `SeekToBeginning` (:109), `SeekToEnd` (:112), `Position` (:115), `Commit()` (:118) / `Commit(offsets)` (:121), `Committed` (:125), `OffsetsForTimes` (:129), `BeginningOffsets` (:134), `EndOffsets` (:138), `PartitionsFor` (:142), `ListTopics` (:145), `Close()` (:148) / `Close(TimeSpan)` (:151), `Seek(tp,long)` (:165) / `Seek(tp,OffsetAndMetadata)` (:169), `Wakeup` (:177), `Assignment` (:183), `Subscription` (:186), `Paused` (:189). `MockConsumer<TKey,TValue>` exists with `AddRecord` (`MockConsumer.cs:263`) for the empty-config path. ✅ **No new binding/production code required** — this is the scope-defining check; if the Actor discovers any gap it MUST stop and flag it (would change scope to Mode B).

   ⚠ **Divergence from pre-pass ("24 RPCs"):** the proto declares **23 RPCs** (`CreateConsumer` + 22), not 24 (`consumer_service.proto:44-84`). The single `Seek` RPC covers **both** .NET `Seek` overloads via optional `leader_epoch`/`metadata` fields (`consumer_service.proto:237-244`); the single `CommitSync` RPC covers **both** `Commit()` / `Commit(offsets)` overloads (`:207-211`). No functional impact — just a precise count.

3. **Wire contract & shared messages.** `multilanguage-test-server/proto/consumer_service.proto` holds the 23 RPCs; it `import "producer_service.proto"` (`:39`) for the shared messages `KafkaError, PartitionInfo, Node, Header, StatusResponse, PartitionsForResponse` (`:23-25`, `:298`). ⇒ C# codegen must include **BOTH** `.proto` files with the proto dir on the import path, but the server implements **ConsumerService only**. ✅ Verified.

4. **Templates.** `bindings/python/grpc_server.py` is the closest analog (sync thread-pool servicer, id→consumer dict + lock, empty-config→MockConsumer) and `bindings/python/grpc_translate.py` holds the proto↔value converters + the `_guess_variant` error-message heuristic. ✅ Verified.
   ⚠ **Divergence:** `grpc_server.py` implements **BOTH** `ProducerService` and `ConsumerService` (`grpc_server.py:475-476`). The .NET server ports **only the consumer half** — still consumer-only as intended; the analog simply isn't itself consumer-only.

5. **Harness plug points (all additive).**
   - `tests/common/backend_pool.rs` — `BackendKind` is `{ Python, PythonAsync, C }` (`:44-51`); ports python=50051 / python_async=50051 / c=50052 (`:66-72`); image repo pattern `confluent-kafka-rust/<lang>-grpc-server` (`:54-60`); label per kind (`:74-80`); readiness `WaitFor::message_on_stderr("listening")` (`:160`). Add a `Dotnet` variant: repo `confluent-kafka-rust/dotnet-grpc-server`, **internal port 50053** (fresh — 50051/50052 taken), label `"dotnet"`.
   - `tests/common/backend_factory.rs` — the existing gRPC factories implement **both** `ProducerBackendFactory` **and** `ConsumerBackendFactory` (`:159-291`). Add a `DotnetGrpcFactory` implementing **ONLY `ConsumerBackendFactory`** (`create` wraps `MultilanguageConsumer::new(channel, config, "dotnet")`, `needs_container_bootstrap()==true`, `name()=="dotnet"`); export it from the `grpc_backends` mod re-export (`:295`). No `ProducerBackendFactory` impl — this is what keeps .NET consumer-only.
   - `tests/common/multilanguage_consumer_test_macro.rs` — expands to `__rust/__python/__python_async/__c` arms (`:41-87`). Add a `__dotnet` arm (`BackendKind::Dotnet` + `DotnetGrpcFactory`). **Leave `multilanguage_test!` (the producer macro) untouched** — that is what keeps .NET consumer-only.
   - `tests/common/multilanguage_consumer.rs` — the Rust `MultilanguageConsumer` gRPC client is backend-agnostic: it carries a `backend: &'static str` label (`:59,:69`) used only in log/error messages (`:74,:89-90`). ⇒ **reuse verbatim, no change expected.** ✅ Verified.

6. **Native lib resolution.** .NET P/Invokes `confluent_kafka` (`NativeMethods.cs:65` — `private const string DllName = "confluent_kafka";`). In the image, `libconfluent_kafka.so` must be on the loader path — copy it beside the published app **and** set `LD_LIBRARY_PATH`, mirroring the Python image (`bindings/python/Dockerfile.grpc:78,:92`). ✅ Verified.

7. **Readiness sentinel.** The server MUST print the literal substring **`listening`** to **stderr** after it starts (`backend_pool.rs:160`; Python prints `listening on 0.0.0.0:{port}` to stderr at `grpc_server.py:481`). ⚠ ASP.NET Core / Kestrel logs "Now listening on…" to **stdout** via `ILogger` by default; the harness watches **stderr only**, so we must emit our own explicit `Console.Error.WriteLine("listening on 0.0.0.0:50053")` after startup. Keep it stable.

8. **.NET is not in the top-level `Makefile`.** Confirmed: no `dotnet` target anywhere in `Makefile`; `build-grpc-images` (`Makefile:87-90`) calls python `grpc-image` / `grpc-image-async` / c `grpc-image` only. And **no `bindings/dotnet/Makefile` exists** (verified). ⇒ create `bindings/dotnet/Makefile` with a `grpc-image` target (c/python precedent) and add one line to `build-grpc-images`.
   ⚠ **Observation (out of scope):** `Makefile` has a duplicated/overlapping block (two `.PHONY`, two `build:`/`build-rust:` etc., `:17-47` vs `:48-90`); `build-grpc-images` is unambiguously at `:87-90`. Do **not** refactor the Makefile — only append the dotnet line.

9. **Prereq.** `cargo build --features ffi --release` produces `target/release/libconfluent_kafka.so` and `target/include/confluent_kafka.h` (top-level `build-rust`, `Makefile:52-53`; same inputs the python/c images copy). `build-grpc-images: build` already depends on it (`:87`). ✅ Verified.

### 2.1 Additional verified findings (marshalling ground truth)

- **The flat `KafkaException` carries no `variant`** (only `Code`/`IsRetriable`/`IsFatal`/`Message`, `ffi-marshalling.md §B5`), but the proto `KafkaError` has a `variant` enum. ⇒ the C# translate helper **must sniff `variant` from the message string**, a faithful port of `_guess_variant` (`grpc_translate.py:50-77`). This is the one genuinely non-trivial work item.
- **`ConsumerRecord<TKey,TValue>` exposes** Topic/Partition/Offset/Timestamp/`TimestampType`(enum)/Key/Value/Headers (`ConsumerRecord.cs:91-123`) — **no `LeaderEpoch`.** ⚠ The proto `ConsumerRecord.leader_epoch` (`:168`) is `optional`; the .NET server **omits it (absent)**, unlike Python which sets it. The Rust test only reads key/value (`collect()` `:100-102`), so this is behaviorally invisible — but note it, don't fake a value.
- **Sync ctors are 3-param / deserializer-taking** (unlike Python's `KafkaConsumer(config)`): the server constructs `new KafkaConsumer<byte[],byte[]>(config, Serdes.ByteArray, Serdes.ByteArray)` and `new MockConsumer<byte[],byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest")`. `Serdes.ByteArray` is shipped (`Serdes.cs:44`, `ISerde<byte[]>`).
- **`OffsetAndMetadata`** = Offset/Metadata/`LeaderEpoch`(int?) (`OffsetAndMetadata.cs:97,104,110`); **`OffsetAndTimestamp`** = Offset/Timestamp/`LeaderEpoch`(int?) (`OffsetAndTimestamp.cs:52,57,63`) — both map cleanly to their proto twins with proto3 optional-presence for `leader_epoch`.
- **Solution layout:** `bindings/dotnet/Confluent.Kafka.sln` + `Directory.Build.props` at the binding root. `Directory.Build.props` applies to every project under `bindings/dotnet/` — so a new server project **inherits** its analyzers / `#nullable` / any warnings-as-errors; the generated proto stubs + server code must be clean under it, or the server `.csproj` must locally relax the offending analyzers.

## 3 · In scope / Out of scope

**In scope (P1):**
- Sync **`KafkaConsumer<byte[],byte[]>`** (and `MockConsumer<byte[],byte[]>` for empty config) only.
- **`ConsumerService` only** — the 23 RPCs of `consumer_service.proto`.
- The .NET gRPC server project, its C# translate helper (incl. `_guess_variant` port), its `Dockerfile.grpc`, a new `bindings/dotnet/Makefile`, the top-level `build-grpc-images` line, and the additive Rust-harness 5th-backend wiring.

**Out of scope (explicitly):**
- **NO `ProducerService`** on the .NET backend (keeps it consumer-only; leave `multilanguage_test!` untouched).
- **NO** `ConsumerRebalanceListener` / `OffsetCommitCallback` / regex-pattern-subscribe RPCs — the proto already omits these (`consumer_service.proto:27-30`); those tests stay native-Rust-only.
- **NO** RPCs for `CommitAsync` / `CurrentLag` / `EnforceRebalance` / `GroupMetadata` / `Metrics` — not in the proto; not exercised.
- **NO** async `dotnet_async` twin — **DEFERRED to M8/P2.**
- **NO** C ABI / `confluent_kafka.h` change; **NO** new C# production code in `src/Confluent.Kafka` (Mode A). Any discovered need for either **stops the phase for re-scoping.**

## 4 · Work items

### (A) C# proto codegen
- Reference **`Grpc.Tools`** (transitively via `Grpc.AspNetCore`, item B) and add `<Protobuf>` items to the server `.csproj`:
  - `consumer_service.proto` → `GrpcServices="Server"`.
  - `producer_service.proto` → `GrpcServices="None"` (messages only — we need `KafkaError`/`PartitionInfo`/`Node`/`Header`/`StatusResponse`/`PartitionsForResponse`, not the producer service base).
  - Set the import root so `import "producer_service.proto"` resolves (`ProtoRoot` / `AdditionalImportDirs` = the proto dir).
- Single source of truth = `multilanguage-test-server/proto/*.proto` (copied into the image build context; do not fork the schema).

### (B) The .NET gRPC server project
- **Location:** `bindings/dotnet/grpc-server/` (own `.csproj`; `ProjectReference` → `../src/Confluent.Kafka/Confluent.Kafka.csproj`). A test-harness artifact — **do not** add it to the shipped package; optionally add to the `.sln`.
- **Framework choice — recommend `Grpc.AspNetCore` (Kestrel), `net8.0`:**
  - `Grpc.Core` (the C-core library) is **deprecated / end-of-life** (support ended 2022) and drags in its own native gRPC — reject it.
  - `Grpc.AspNetCore` is the supported, pure-managed HTTP/2 server path; the container only ever runs Linux `net8.0` (LTS). The Confluent.Kafka library multi-targets (incl. net8.0), so the ProjectReference restores its net8.0 asset.
  - ⚠ **Serve h2c (HTTP/2 cleartext, no TLS):** the Rust client dials `http://127.0.0.1:<port>` (`backend_pool.rs:181`, `Endpoint::from_shared`). Configure Kestrel `ListenAnyIP(50053, o => o.Protocols = HttpProtocols.Http2)` — the common Grpc.AspNetCore-defaults-to-TLS pitfall.
- **Servicer shape (port `ConsumerService` half of `grpc_server.py`):**
  - `ConcurrentDictionary<ulong, KafkaConsumer/MockConsumer>` (or dict + lock) keyed by a monotonically-increasing `consumer_id` (`grpc_server.py:206-212,228-229`).
  - `CreateConsumer`: empty-or-all-blank config → `MockConsumer<byte[],byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest")` (Python parity `grpc_server.py:220-221`); else `KafkaConsumer<byte[],byte[]>(config, Serdes.ByteArray, Serdes.ByteArray)`. Ctor throw → `CreateConsumerResponse{ error = ToProto(ex) }`.
  - Each RPC: resolve id (unknown id → the Python-parity hand-built `KafkaError{ variant=ILLEGAL_STATE, code=-1, message="unknown consumer_id N", is_retriable=false, is_fatal=true }`, `grpc_server.py:261-264`), call the sync binding method, map the result into the proto (oneof success vs `error`), and translate any `KafkaException` via item C. `ms→TimeSpan` for `Poll` (`grpc_server.py:266`). `CommitSync` with empty offsets → `Commit()`; else `Commit(map)` (`:275-287`). `Seek` with `metadata`/`leader_epoch` present → `Seek(tp,OffsetAndMetadata)`, else `Seek(tp,long)` (`:315-326`). `Close`/`Wakeup` idempotent on unknown id (`:449-459`).
  - **Blocking calls run directly on the gRPC handler thread** (Python's model — blocking `poll` on a `ThreadPoolExecutor` worker, `grpc_server.py:30-33,474`). Do **not** wrap in `Task.Run` (needless sync-over-async).
- **Readiness:** after `app.Start()`, `Console.Error.WriteLine("listening on 0.0.0.0:50053")` (item 7). Bind `GRPC_PORT`-overridable, default 50053.

### (C) The C# translate helper (port of `grpc_translate.py`)
- Value↔proto converters: `RecordToProto` (ConsumerRecord→proto, omit absent `LeaderEpoch`; `TimestampType` enum→int), `TpToProto`/`Tp`, `OamToProto`, `OffsetAndTimestamp`→proto, `PartitionInfo`/`Node`→proto, and the map builders (`OffsetMap`/`LongOffsetMap`/`OffsetAndTimestampMap`/`TopicListing`). proto3 **optional-presence** for `leader_epoch`/`metadata`/`key`/`value`.
- **`_guess_variant` — port EXACTLY** (`grpc_translate.py:50-77`), first-match-wins, on `message.ToLowerInvariant()`:
  1. empty message → `GENERIC(0)`
  2. `"max.request.size"` | `"is larger than"` | `"too large"` → `RECORD_TOO_LARGE(8)`
  3. `"buffer is full"` | `"buffer.memory"` → `BUFFER_EXHAUSTED(4)`
  4. `"timed out"` | `"expired"` | `"not present in metadata"` → `TIMEOUT(7)`
  5. `"topic authorization"` → `TOPIC_AUTHORIZATION(1)`
  6. `"invalid topic"` → `INVALID_TOPIC(2)`
  7. `"group authorization"` → `GROUP_AUTHORIZATION(3)`
  8. `"illegal state"` | `"already been closed"` → `ILLEGAL_STATE(6)`
  9. `"serialization"` | `"failed to serialize"` → `SERIALIZATION(9)`
  10. else → `GENERIC(0)`  *(`ILLEGAL_ARGUMENT(5)` is defined but never emitted — keep it that way.)*
- `KafkaException → KafkaError`: `variant = GuessVariant(msg)`, `code/is_retriable/is_fatal` straight from the exception; non-Kafka `Exception` → `{ variant=ILLEGAL_STATE, code=-1, message="dotnet server: <Type>: <msg>", is_retriable=false, is_fatal=true }` (mirrors `grpc_translate.py:97-103`). The Rust client asserts on `variant`, so this mapping must round-trip faithfully.

### (D) `bindings/dotnet/Dockerfile.grpc` (multi-stage)
- Build context = **repo root** (like c/python), so it can copy `target/release/libconfluent_kafka.so`, `target/include/confluent_kafka.h` (if the csproj copy-target needs it), the `bindings/dotnet/` sources, and `multilanguage-test-server/proto/*.proto`.
- **Stage 1 (builder):** `mcr.microsoft.com/dotnet/sdk:8.0`; stage the repo layout the Confluent.Kafka `.csproj` native-copy MSBuild target expects (mirror `python/Dockerfile.grpc:36-46` — put the `.so` at the `target/release/` path the csproj probes); `dotnet publish -c Release` the grpc-server project (proto codegen runs here via Grpc.Tools).
- **Stage 2 (runtime):** `mcr.microsoft.com/dotnet/aspnet:8.0` (ASP.NET Core runtime, needed by Grpc.AspNetCore); copy the publish output + `libconfluent_kafka.so` into `/app`; `ENV LD_LIBRARY_PATH=/app`; `EXPOSE 50053`; `CMD ["dotnet","/app/<server>.dll"]`. Keep the readiness line stable (item 7).

### (E) Makefile wiring
- New `bindings/dotnet/Makefile` (c/python precedent): `GRPC_IMAGE_TAG ?= confluent-kafka-rust/dotnet-grpc-server:dev`, `.PHONY: grpc-image`, `grpc-image:` → `docker build -t $(GRPC_IMAGE_TAG) -f $(RUST_PROJECT_ROOT)/bindings/dotnet/Dockerfile.grpc $(RUST_PROJECT_ROOT)`.
- Top-level `Makefile:90` — append `$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-image` under `build-grpc-images`. Do not otherwise touch the Makefile.

### (F) Rust harness — 5th backend arm (additive)
- `backend_pool.rs`: `BackendKind::Dotnet` → `image_repository`="confluent-kafka-rust/dotnet-grpc-server", `internal_port`=**50053**, `label`="dotnet".
- `backend_factory.rs`: `DotnetGrpcFactory { channel }` implementing **only** `ConsumerBackendFactory` (`create` → `MultilanguageConsumer::new(channel, config, "dotnet")`; `needs_container_bootstrap`=true; `name`="dotnet"); add to the `grpc_backends` re-export (`:295`).
- `multilanguage_consumer_test_macro.rs`: add the `[<$name __ dotnet>]` arm (mirror the `__c` arm `:75-87`, swapping `BackendKind::Dotnet` + `DotnetGrpcFactory`).

### (G) Behavior-parity checks
- **KIP-848:** `consumer_config()` sets `group.protocol=consumer` (`multilanguage_consumer_test.rs:51`); the config dict flows straight through `ConsumerProperties_put` — nothing special to do, just don't strip/rewrite keys.
- **Container bootstrap:** `needs_container_bootstrap()==true` routes tests to `container_bootstrap_servers()` (`multilanguage_consumer_test.rs:59-65`).
- **Error-variant heuristic:** item C (the Rust tests `matches!` on `KafkaError` variants translated from the message).
- **Three-state nullability:** proto3 optional-presence for `key`/`value` (absent vs empty), `leader_epoch` (omit on records — .NET has none; pass through on offsets), `metadata`.

## 5 · Risks / notes

- **Single-owner native consumer under a gRPC thread pool.** The native consumer is single-owner / not thread-safe (`bindings/dotnet/tests/Confluent.Kafka.UnitTests/AssemblyInfo.cs` single-owner rationale; `ffi-marshalling.md §B1`); a concurrent op faults/throws. Python is safe because the harness drives **one consumer sequentially, one op in flight** (`grpc_server.py` uses a single dict-lock, not per-op serialization). **Recommendation:** the .NET server should **serialize ops per `consumer_id`** (a per-id gate) as cheap defense-in-depth, even though the harness drives sequentially — a stray concurrent RPC then queues instead of faulting. Decide during implementation; document the choice in `COMMENTS.DONE.23`.
- **h2c pitfall:** Grpc.AspNetCore must be configured for cleartext HTTP/2 (item B) or the Rust `http://` client fails to connect.
- **Stderr sentinel vs Kestrel logging:** Kestrel's "Now listening" goes to stdout; emit our own `listening` line to stderr (item 7). Extra stderr noise is fine as long as our substring appears post-startup.
- **`Directory.Build.props` inheritance:** the server project inherits the binding's analyzers/warnings-as-errors; generated proto + server code must satisfy them or the server `.csproj` must locally relax them (item B).
- **MockConsumer empty-config path is likely dead** for this harness (every current test passes a real broker config), but implement it for Python parity; it is **not** seeded by the server layer (Python doesn't either, `grpc_server.py` has no `update_*`/`add_record` calls).
- **`LeaderEpoch` on records:** omit the absent proto field — do not fabricate a value.

## 6 · Definition of Done (per `.claude/rules/definition-of-done.md` + `bindings/CLAUDE.md`)

- `make build-grpc-images` builds the `confluent-kafka-rust/dotnet-grpc-server:dev` image (alongside the existing four) with no error.
- `cargo test --features integration-tests,multilanguage-tests` runs and **every consumer test's `…__dotnet` variant is green** (parity with `…__rust` / `…__python` / `…__c`).
- **No change** to `confluent_kafka.h`, `src/ffi/*.rs`, or `src/Confluent.Kafka/**` production code (Mode A; confirm with `git diff --stat`). Review ground truth = the C ABI header + the Kafka Java public API (`bindings/CLAUDE.md §8.2`) — **shape, not logic**; the server adds no Kafka behavior.
- The producer macro `multilanguage_test!` and `ProducerBackendFactory` are untouched (consumer-only invariant).
- The .NET server project builds clean under `Directory.Build.props` analyzers; `dotnet format --verify-no-changes` clean on the new project.
- Rust harness edits pass `cargo build`, `cargo xtask format-check`, `cargo xtask lint`.
- Error-variant translation round-trips (item C) for the variants the consumer tests assert on.

## 7 · Validation steps

1. `cargo build --features ffi --release` (produces the `.so` + header).
2. `make build-grpc-images` — confirm the dotnet image builds and tags `:dev`.
3. `docker run` the image manually; confirm `listening` on stderr and a plaintext-h2c gRPC port.
4. `cargo test --features integration-tests,multilanguage-tests <consumer_test>__dotnet` for each consumer test; then the full consumer suite.
5. `git diff --stat` — confirm zero production/ABI/header churn; confirm producer macro/factory untouched.
6. `cargo xtask format-check && cargo xtask lint`; `dotnet format --verify-no-changes` (server project).

## 8 · Phase checklist

- [ ] (A) proto codegen wired (consumer=Server, producer=None, import dir set)
- [ ] (B) `grpc-server/` project — Grpc.AspNetCore/net8.0, h2c, servicer, id-map, empty-config→Mock, per-id serialization decision, stderr sentinel
- [ ] (C) C# translate helper incl. faithful `_guess_variant` + `ToProto(KafkaException)`
- [ ] (D) `Dockerfile.grpc` multi-stage, bundles `.so`, `LD_LIBRARY_PATH`, EXPOSE 50053
- [ ] (E) `bindings/dotnet/Makefile` `grpc-image` + top-level `build-grpc-images` line
- [ ] (F) `backend_pool.rs` `Dotnet` + `backend_factory.rs` `DotnetGrpcFactory` (Consumer-only) + macro `__dotnet` arm
- [ ] (G) parity checks pass; DoD §6 green; `COMMENTS.DONE.23` records the per-id-serialization + framework decisions

## 9 · Comment workflow & handoff (Manager, N=23)

`dotnet-actor N=23` (items A–G; per-path staging — never `git add` the root `.claude/agents/dotnet-*.md` discovery copies, `bindings/dotnet/CLAUDE.md §8.4`) → DoD §6 → `dotnet-critic N=23` (h2c, `_guess_variant` fidelity, consumer-only invariant, single-owner serialization, no production/ABI churn, native-lib bundling) → fix cycle until `COMMENTS.23.md` empty + DoD passes → archive `COMMENTS.DONE.23.md` under `design/history/M8/P1-sync-consumer-grpc-backend/`, update `design/current/STATUS.md` (M8/P1 DONE), reset `COMMENTS.23.md`. **Loop does not start until the maintainer approves this PLAN.**
