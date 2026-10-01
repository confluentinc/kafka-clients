# M9/P3 — `Metrics` RPC in the .NET gRPC conformance test server

**Status:** APPROVED 2026-08-19 (all recommendations D1–D3 accepted by the coordinator). Agent number **N=38**.
**Branch:** `prashah_dev_dotnet_binding_consumer`.
**Depends on:** M9/P2 (N=37) — the binding's consumer `Metrics()` → `IReadOnlyDictionary<MetricName, IMetric>` on `IConsumerCommon` (commits `cde17a10` impl, `4cdfc8f3` tests, `bfcf6085` archived plan).

## Goal (deliberately narrow)

Implement the `Metrics` RPC handler in the .NET gRPC conformance test server (both the sync and async servicers) so the existing cross-language metrics conformance test (`test_ml_metrics`) covers the `__grpc_dotnet` and `__grpc_dotnet_async` backends. **`ClientId` is explicitly OUT of scope.**

## The gap (verified against repo)

- Metrics conformance test **exists**: `tests/integration/multilanguage_consumer_test.rs` — `metrics_reports_backend_registry` (L363), registered as `test_ml_metrics` (L438). Asserts `metrics()` non-empty; has `consumer-fetch-manager-metrics` (L390) + `consumer-metrics` (L396) groups; `records-lag-max` present (L405) and is a `Double` (L426–431); a topic/partition-tagged metric survives.
- The macro `tests/common/multilanguage_consumer_test_macro.rs` fans every consumer test to six backends including `__grpc_dotnet` and `__grpc_dotnet_async` — so `test_ml_metrics__grpc_dotnet[_async]` already exist as test functions.
- Harness bridge `tests/common/multilanguage_consumer.rs` `metrics()` (L332) calls the `Metrics` RPC and does `.expect("metrics RPC failed")` (L352), matching `metrics_response::Result::Metrics(list)` (L355) — an UNIMPLEMENTED RPC fails the test.
- Proto **has** the RPC: `multilanguage-test-server/proto/consumer_service.proto:83` — `rpc Metrics(ConsumerIdRequest) returns (MetricsResponse)`. `Metric` (L343): `name`/`group`/`description`/`tags` + oneof `value` — `double_value=5`, `string_value=6`, `long_value=7`, `int_value=8`. `MetricList` (L358), `MetricsResponse` (L362, oneof `metrics`/`error`).
- **C++ and both Python gRPC servers already implement `Metrics`** (commit `7a5425a2`): `bindings/c/grpc_server/server.cc` (Metrics at L795), `bindings/python/grpc_server.py`, `grpc_server_async.py`, `grpc_translate.py`. Mirror their translation shape.
- **The .NET consumer gRPC server does NOT implement `Metrics`.** `ConsumerServiceImpl.cs` (23 overrides) and `AsyncConsumerServiceImpl.cs` (23 overrides) override every consumer RPC except `Metrics`. There is **no `ClientId` RPC in the proto at all** — so ClientId isn't even a missing override; it has no conformance infra.
- Harness factories `DotnetGrpcFactory` / `DotnetAsyncGrpcFactory` (`tests/common/backend_factory.rs`) already exist — no harness/proto change needed.

## Mode determination — Mode A analog (test-infra only)

The gRPC server is C# test infrastructure under `bindings/dotnet/grpc-server/`. It consumes the already-shipped binding `Metrics()` (M9/P2, on `IConsumerCommon`, inherited by both `IConsumer<byte[],byte[]>` and `IAsyncConsumer<byte[],byte[]>`) and the already-generated proto stub. **No new `[DllImport]`, no `src/**`, no proto change, no Rust-harness change.** Within `dotnet-actor` scope ("the C# side, header-down", CLAUDE.md §8.1).

**Scope hard line:** the diff must be confined to `bindings/dotnet/grpc-server/**` (the two servicers + `Translate.cs`) plus this archived PLAN. Zero changes to `src/**`, `multilanguage-test-server/proto/**`, `tests/**` (the `test_ml_metrics` test already exists — do NOT modify it), or the Rust harness. If any is needed, STOP and flag it.

## Deliverables

1. **`bindings/dotnet/grpc-server/Translate.cs`** — add a **per-entry** converter `internal static Proto.Metric MetricToProto(MetricName name, IMetric metric)` (D3), mirroring the existing `*ToProto` helpers and the C++/Python shape:
   - Set `Name`/`Group`/`Description` from `MetricName`; populate the `Tags` map.
   - Set the `value` oneof by dispatching on the **boxed CLR runtime type** of `IMetric.Value`: `double` → `DoubleValue`, `string` → `StringValue`, `long` → `LongValue`, `int` → `IntValue`.
   - **Long vs int:** C# preserves the distinction in the boxed type (M9/P2 recorded "the boxed type is the kind"; no `Kind` member), unlike Python's single `int` + explicit `kind`.
   - **D2 — unmatched type → explicit failure:** if `IMetric.Value` is none of `double`/`string`/`long`/`int`, throw (→ caught in the servicer → proto `KafkaError`). Do NOT silently default to `double_value` (which could mask a future core `MetricValue` variant divergence; `test_ml_metrics` is structural and would not catch it).

2. **`bindings/dotnet/grpc-server/ConsumerServiceImpl.cs`** (sync) — add `public override Task<Proto.MetricsResponse> Metrics(Proto.ConsumerIdRequest request, ServerCallContext context)`, mirroring the `Subscription` override: `Get(request.ConsumerId)` → `Translate.UnknownConsumer` on null; `try { lock (entry.Gate) { build MetricList via MetricToProto over entry.Consumer.Metrics() } return Task.FromResult(new MetricsResponse { Metrics = list }); } catch (Exception ex) { return ...{ Error = Translate.ToProto(ex) }; }`. `Metrics()` is sync → `Task.FromResult`, no `await`.

3. **`bindings/dotnet/grpc-server/AsyncConsumerServiceImpl.cs`** (async) — the symmetric override. `ConsumerEntry.Consumer` is `IAsyncConsumer<byte[],byte[]>`, which also inherits `IConsumerCommon.Metrics()` (sync) — identical body shape (still `Task.FromResult`, no `await`).

   Concurrent-access note: the binding's `Metrics()` throws `InvalidOperationException` on the core's concurrent-null; the `catch` routes it through `Translate.ToProto` → `IllegalState` proto error — the C#-idiomatic analog of the C++ server's explicit `map == nullptr` concurrent-access branch.

## Verification / DoD

- **Locally runnable (must be green):** `cargo build --features ffi` (native) → `dotnet build` grpc-server (0W/0E) → harness `--no-run` compile (`cargo test --features integration-tests,multilanguage-tests --test integration --no-run`). `dotnet` is at `~/.dotnet/dotnet` / `/usr/local/share/dotnet/dotnet` (not on bare PATH). net10.0 is the execution gate; net8.0 build-only.
- **D1 — Docker gate:** run `docker info`. If up: `make build-grpc-images` (aggregate; builds the two dotnet images) **or** `make -C bindings/dotnet grpc-image grpc-image-async`, then `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet` and confirm `test_ml_metrics__grpc_dotnet[_async]` PASS **and other backends stay green**. If Docker down: record CI-pending. (Note: there is no `build-grpc-images-dotnet` / `test-integration-dotnet` Makefile target — use the aggregate or the per-binding `grpc-image[-async]` targets.)
- **DoD adjustments:** hot-path allocation audit N/A (test infra). Java-shape fidelity is against the C++/Python server translations + the proto (test infra, not a Kafka public-API surface). No new Java class translated → `marked_classes.txt` unchanged.

## Out of scope

`ClientId` (no proto RPC / no harness test); producer; other RPCs; any `src/**` / proto / `tests/**` / Rust-harness change.

## Execution loop

Actor/Critic **N=38** (`dotnet-actor` / `dotnet-critic`). Per-path `git add` only; never stage `.claude/agents/dotnet-*.md`, `COMMENTS.*`, `agent-memory/**`, `obj/`/`bin/`, built binaries, `target/`. Commits `--no-gpg-sign` + `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Commit locally; do NOT push (user manages pushes). Critic files to `COMMENTS.38.md`; resolved → `COMMENTS.DONE.38.md`; loop until clean.
