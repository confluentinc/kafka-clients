# COMMENTS.DONE.34 — M12/P1 ".NET producer gRPC conformance backend (sync + async)"

Decisions & deviations recorded during execution (per agent-roles Actor loop +
CLAUDE.md §4 decision-point latitude). The binding-root working file is never
committed; this is the Manager's archived copy under
`design/history/M12/P1-producer-grpc-backend/`.

**Critic N=34 outcome: CLEAN — 0 issues, round 1.** No `COMMENTS.34.md` was ever
created; the Actor/Critic loop closed on the first pass with no fix cycle. This
file records the approved decisions and the deviations made during execution, per
the PLAN §11 checklist item.

## The 5 approved decisions (implemented exactly)

1. **Label = M12/P1.** New milestone, by the consumer-gRPC precedent: M8 was its
   own milestone separate from the consumer *core* (M6); this producer-gRPC
   backend is the analog, separate from the producer *core* (M11).

2. **`CloseTimeout` ignores `timeout_ms` → `Close()` / `await Close()`.** Faithful
   port of the Python server (`grpc_server.py:189-192`), consistent with the
   M8/P2 async-consumer `Close`-ignores-`timeout_ms` precedent, and safe because
   **no `multilanguage_test!` scenario calls `close_timeout`** — the one
   `close_timeout(0)` test is Rust-native-only and documented non-multilanguageable.
   Behaviorally invisible to the conformance suite; a deliberate, recorded divergence.

3. **ONE phase** delivering BOTH servicers — `ProducerServiceImpl` (sync, over
   `IProducer`/`KafkaProducer`/`MockProducer`) + `AsyncProducerServiceImpl` (async,
   over `IAsyncProducer`/`AsyncKafkaProducer`/`AsyncMockProducer`). Unlike the
   consumer's M8/P1+P2 split (each needed its own new image + Docker + Make + CI
   built from scratch), all that infra already exists and is shared, the surface is
   small (6 RPCs), and the two servicers are near-identical.

4. **Reuse the two existing dotnet images** — each now hosts BOTH `ProducerService`
   + `ConsumerService`, flavor-selected by the existing `CONSUMER_FLAVOR` env
   (Python one-server-per-flavor shape). **No** new image, `BackendKind`,
   Dockerfile, Makefile target, or `.semaphore` change (M8 built that infra;
   `backend_pool.rs` `Dotnet`/`DotnetAsync` variants + the `__grpc_dotnet` CI filter
   are reused unchanged). **Isolation trade-off (recorded):** the proto flip + the
   added producer service now ride the shipped consumer images. The change is purely
   additive (ConsumerService codegen + servicers untouched), so the risk is low, but
   the DoD mandates the consumer `…__grpc_dotnet[_async]` arms be re-run to prove no
   regression (see the CI-gate note below).

5. **The dotnet-actor authored the 2 additive Rust harness-glue files itself** —
   `tests/common/backend_factory.rs` (`ProducerBackendFactory` impls) +
   `tests/common/multilanguage_test_macro.rs` (`__grpc_dotnet[_async]` producer
   arms). User-approved exception to the "dotnet-actor is C#-only" rule (M8
   precedent, where the dotnet-actor wrote the identical class of harness glue).
   These are mechanical mirrors of the existing python/c arms — not a new scenario,
   not a new client method.

## Deviations from the consumer precedent (recorded)

- **No per-op gate** (divergence from the consumer servicer's per-id
  `lock`/`SemaphoreSlim`). The consumer is single-owner / not thread-safe, so it
  gates per id. The **producer is thread-safe** — the core's internal `Mutex`
  serializes concurrent `Send` (`ffi-marshalling.md §A1`: "concurrent `Send` is
  safe — don't add your own lock"), so the producer servicer uses **only** a
  thread-safe `ConcurrentDictionary<ulong,…>` id→producer map + an `Interlocked`
  id counter to guard `CreateProducer`/`Close` races. Verified: no per-op lock.

- **Incoming proto headers dropped**; **`serialized_*_size = -1`** in
  `RecordMetadata`→proto. Both are Python-server parity — the .NET `ProducerRecord`
  has no headers, and the shipped `RecordMetadata` does not expose serialized sizes
  (clipped to today's ABI).

## Mode A invariant

Held. `git diff --stat 1bcedc5d..HEAD` is **empty** over `src/**`,
`target/include/confluent_kafka.h`, and `cbindgen.toml`; no new C# under
`src/Confluent.Kafka/**`; the shared `.proto` content is byte-untouched (only the
`.csproj` `GrpcServices` attribute changed). The **only** Rust delta is the two
*additive* `tests/common/*.rs` harness-glue files — not under `src/`, not ABI/core.
Every producer RPC maps 1:1 to an already-shipped generic producer member through
`<byte[],byte[]>` + `Serdes.ByteArray`; no ABI gap, no STOP.

## Verification notes (local environment)

- `cargo build --features ffi` exit 0 (no header delta); `dotnet build -c Release`
  (grpc-server, net8.0) **0W/0E** — the proto flip emitted `ProducerServiceBase`,
  both servicers compile, both `MapGrpcService<T>` type-check; Rust glue
  `cargo test … --test integration --no-run` compiles clean and lists both dotnet
  producer arms (28 scenarios × 2 = 56); server **boots + emits `listening`** for
  both flavors (ephemeral, reverted net10.0 TFM-swap DI smoke); `dotnet format
  --verify-no-changes` clean; `cargo xtask format-check` + `cargo xtask lint` clean.
- Commit hygiene: both commits unsigned (`%G? = N`) + carry the `Co-Authored-By`
  trailer; per-path staging; exactly 7 work-surface files committed; nothing
  forbidden staged (`COMMENTS.*`, `agent-memory/**`, root `.claude/agents/dotnet-*`,
  `target-linux*`, built binaries, `obj/`/`bin/`).

## CI/Docker-only gate — PENDING (expected in this env, not a defect)

The Docker-backed conformance gate could **not** run locally (`docker info` fails —
the sandbox has no Docker daemon), consistent with the M8/P1+P2 harness phases.
Not runnable here, must be confirmed green on a Docker-capable amd64 Linux runner
(CI):

- `make build-grpc-images-dotnet` — both dotnet images rebuild with the producer
  service compiled + registered.
- `cargo test --features integration-tests,multilanguage-tests --test integration
  -- __grpc_dotnet` — the producer `…__grpc_dotnet[_async]` variants green
  (Mock empty-config + broker-backed) **and** the consumer `…__grpc_dotnet[_async]`
  no-regression re-run green (decision-#4 isolation trade-off).

Recipe (from the recorded local-gate memory): cross-built `.so` at
`target-linux-amd64/release/libconfluent_kafka.so`, `DOCKER_DEFAULT_PLATFORM=linux/amd64`,
`sdk:10.0`, amd64-emulation for the arm64 Grpc.Tools protoc SIGSEGV. The existing
`.semaphore` "Verify .NET binding" `__grpc_dotnet` filter already auto-covers the
new producer arms (no `.semaphore` edit needed).

## marked_classes.txt

Not updated — this phase translated **no Java classes** (it is harness/binding
wiring: a .NET gRPC servicer + additive Rust test glue). `marked_classes.txt`
tracks Rust-core Java-class translations only.
