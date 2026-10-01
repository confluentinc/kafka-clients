# COMMENTS.DONE.24 — M8/P2 ".NET async-consumer gRPC backend (`dotnet_async`)"

Closed record for the phase (Manager-authored — the Actor completed without a
binding-root decisions file; captured here as the tracked archive). N=24.
Critic N=24 verdict: **CLEAN — 0 issues.** Mode A held.

## Commits (on `prashah_dev_public_consumer_grpc_setup`, atop `815be651`)

- `28a7d5a3` — async gRPC consumer servicer + `CONSUMER_FLAVOR` selector
- `a8fe7b4e` — two-image Docker + Make wiring for `dotnet-async-grpc-server`
- `d1ef8906` — Rust harness 6th (`dotnet_async`) consumer backend arm

## Design shape (maintainer decision — supersedes the initial one-image draft)

TWO images / Python-parity. New **standalone** `Dockerfile.grpc.async` (own `FROM`,
`sdk:10.0`→`aspnet:8.0`, `ENV CONSUMER_FLAVOR=async`, tags
`confluent-kafka-rust/dotnet-async-grpc-server:dev`, `EXPOSE 50053`); `Dockerfile.grpc`
gains only a behavior-preserving `ENV CONSUMER_FLAVOR=sync`. Rationale: true isolation
(a P2 change cannot touch the shipped sync image; the load-bearing `CONSUMER_FLAVOR=sync`
default footgun disappears) + cross-binding consistency (every `BackendKind` → one image,
one behavior; `python`/`python_async` ↔ `dotnet`/`dotnet_async` parallel pairs). Trade-off
on record: the two images share an essentially identical app binary (both consumers ship
in one `Confluent.Kafka.dll`, `Translate.cs` flavor-agnostic) — the split is achieved by
each image baking `ENV CONSUMER_FLAVOR`, accepted by the maintainer ("two scripts" in
Python → "two images each pinning one servicer" in .NET).

## Decisions & deviations (all PLAN-sanctioned)

1. **Async per-id gate = `SemaphoreSlim(1,1)`** — `await Gate.WaitAsync().ConfigureAwait(false)`
   + release in `finally`, wrapping the awaited op. Never a `lock`/Monitor across an
   `await` (would be a CS-level error / wrong-thread release). `SemaphoreSlim.Release()` is
   not thread-affine, so post-await release on another pool thread is correct.
2. **`Wakeup` is gate-exempt** — it is the one cross-thread member (interrupts a blocked
   awaited `Poll`); gating it would deadlock behind the very poll it must wake. Idempotent
   on unknown id.
3. **`Close` ignores `timeout_ms`** — `AsyncKafkaConsumer.Close` has only
   `Close(CancellationToken)` (no timed async close — CLAUDE.md §1 deferred gap), so the
   async `Close` RPC does `await Close()` (idempotent on unknown id). Behaviorally invisible
   to the 11 scenarios (none assert close-timeout).
4. **`Seek` (both overloads) called sync** — they are sync `void` on `IConsumerCommon` even
   on the async consumer (Python-parity §4 divergence), so no `await`. `SeekToBeginning`/
   `SeekToEnd` are async `Task` and are awaited.
5. **No sync-over-async** — handlers `await` the binding `Task` directly; no `Task.Run` /
   `.Result` / `.Wait()` / `.GetAwaiter().GetResult()`. `.ConfigureAwait(false)` is an impl
   choice, not a defect.
6. **`CS1998` dodge** — the sync `Seek`/`Wakeup` `RunStatus` lambdas are non-async and
   `return Task.CompletedTask;` (no `await`), correct under warnings-as-errors.
7. **`Translate.cs` reused VERBATIM** (flavor-agnostic; not in the diff).

## Verification (Actor run; independently git-verified by the Manager/coordinator)

- `git diff --stat 815be651..d1ef8906` over `bindings/dotnet/src/**`,
  `target/include/confluent_kafka.h`, `src/ffi/**`, `cbindgen.toml` → **empty** (Mode A held).
- 9 changed files; `Dockerfile.grpc` change is only the `ENV CONSUMER_FLAVOR=sync` line.
- `make build-grpc-images` → both dotnet images build (linux/amd64).
- `cargo test --features integration-tests,multilanguage-tests` → **22/22** multilanguage
  green (11 `…__dotnet_async` + 11 sync `…__dotnet` isolation regression).
- C# `dotnet build` 0/0 + `dotnet format --verify-no-changes` clean; Rust
  `cargo xtask format-check`/`lint` clean.
- **Emulation caveat:** on Apple-Silicon the image build + async run need the linux/amd64
  emulated path (arm64 Grpc.Tools protoc SIGSEGVs); the async close/bridge amplifies the
  emulation close-timeout flake — a local timing artifact, NOT a code defect. Native x86_64
  is authoritative.

## Mode A invariant

Held. Zero changes under `src/Confluent.Kafka/**`, `confluent_kafka.h`, `src/ffi/**`, or
any Rust core. Every RPC maps 1:1 to an already-shipped `AsyncKafkaConsumer` /
`AsyncMockConsumer` (or sync `IConsumerCommon`) member; no new binding/ABI code needed.
