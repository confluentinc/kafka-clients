# COMMENTS.DONE.23 — M8/P1 ".NET sync-consumer gRPC backend"

Decisions & deviations recorded during execution (per agent-roles Actor loop +
CLAUDE.md §4 decision-point latitude). This is a **local working file — never
committed**; the Manager archives it under
`design/history/M8/P1-sync-consumer-grpc-backend/`.

## Decisions

1. **Framework — Grpc.AspNetCore (Kestrel), net8.0, h2c (PLAN item B).** Taken as
   recommended. Grpc.Core is EOL. Kestrel endpoint pinned to
   `HttpProtocols.Http2` **without** HTTPS so the Rust client's plaintext
   `http://` dial connects (the common Grpc.AspNetCore-defaults-to-TLS pitfall).
   Verified locally: `Now listening on: http://…` (cleartext) + a full h2c
   round-trip from a throwaway client.

2. **Per-id op serialization (PLAN §5 recommendation) — ADOPTED, with Wakeup
   exempt.** Each op that touches a consumer takes that consumer's per-id
   `lock (entry.Gate)`, so a stray concurrent RPC queues instead of faulting the
   single-owner native consumer (cheap defense-in-depth; the harness drives one
   op in flight sequentially anyway). **`Wakeup` deliberately does NOT take the
   gate** — it is the one cross-thread member (it interrupts a blocked `Poll` on
   another thread); gating it would deadlock behind the very poll it must wake.
   `Close` pops the id from the map first (idempotent) then closes under the gate.

3. **Servicer is a DI singleton — required, not optional.** ASP.NET Core gRPC
   activates the servicer **per request** by default, so the id→consumer map would
   be empty on every RPC after `CreateConsumer` (a real bug caught by the local
   h2c smoke test: `assign err=unknown consumer_id 1`). Fixed with
   `builder.Services.AddSingleton<ConsumerServiceImpl>()` (Python registers one
   servicer instance). Post-fix the round-trip is correct.

4. **`Close()` alone tears down the native handle.** The binding's `IConsumer.Close()`
   (via `NativeConsumer.CloseSync`) gracefully closes **and** releases the
   `SafeConsumerHandle` → `Consumer_destroy` in a `finally`. So the `Close` RPC
   calls only `Close()`/`Close(TimeSpan)` — no extra `Dispose()` (Python parity;
   no handle leak).

5. **`_guess_variant` ported EXACTLY over `ToLowerInvariant()` (PLAN item C).**
   `ToLowerInvariant` (not `ToUpperInvariant`) is mandated by the PLAN to mirror
   Python's `.lower()` and the lowercase substring table; the substrings are
   ASCII so it is locale-safe. `ILLEGAL_ARGUMENT` is defined by the proto but
   never emitted (kept that way — Python never emits it either).

6. **`ConsumerRecord.leader_epoch` omitted (PLAN §2.1 / G).** The .NET
   `ConsumerRecord` has no leader epoch, so the proto optional field is left
   absent — not fabricated. `key`/`value`/`metadata`/offset `leader_epoch` use
   proto3 optional-presence (absent vs empty vs present).

7. **Server project NOT added to `Confluent.Kafka.sln` (PLAN item B "optionally").**
   Left out to avoid `.sln` churn/GUID risk; `Directory.Build.props` applies by
   directory regardless, so the server still inherits the analyzers. Built/formatted
   by explicit project path and via `make grpc-image` (Docker). Non-breaking to add
   later.

8. **`CA1031` relaxed on the server project only.** The servicer catches
   `System.Exception` broadly on purpose — a faithful port of `grpc_server.py`'s
   `except Exception` that funnels every failure into a proto `KafkaError`
   (`Translate.ToProto`). This is the harness contract, so `CA1031` is added to the
   server `.csproj` `NoWarn` (PLAN item B allows relaxing offending analyzers). All
   other Directory.Build.props analyzers pass on hand-written code; the generated
   protobuf/gRPC stubs are auto-generated code and are skipped by the analyzers.

## Verification notes (local environment)

- Rust prereq built: `cargo build --features ffi --release` → `target/release/`
  native (`libconfluent_kafka.dylib` on this macOS host; the image needs the
  **Linux `.so`**, produced in CI) + regenerated `target/include/confluent_kafka.h`.
- Server: `dotnet build -c Release` clean (0 warnings / 0 errors under
  Directory.Build.props); `dotnet format --verify-no-changes` clean; end-to-end
  h2c round-trip verified (CreateConsumer→Assign→Assignment→Poll→Subscription→
  unknown-id→Close), including the oneof success + error arms and the
  `UnknownConsumer` illegal-state path.
- **Docker unavailable in this environment** → `make build-grpc-images` and
  `cargo test --features integration-tests,multilanguage-tests` (the `…__dotnet`
  variants) could NOT be run here. The Dockerfile mirrors the Python image's
  native staging and the Makefile mirrors the c/python `grpc-image` precedent;
  the image build + the green `…__dotnet` variants must be confirmed in a
  Docker-capable environment (CI / Linux).
- SDK note: only the .NET 10 SDK (10.0.302) + ASP.NET Core 10 runtime are
  installed locally; the net8.0 server cross-builds fine, and the smoke run used
  `DOTNET_ROLL_FORWARD=LatestMajor`. The image itself uses `sdk:8.0` / `aspnet:8.0`.

## Mode A invariant

Held. Zero changes under `src/Confluent.Kafka/**`, `confluent_kafka.h`, or
`src/ffi/**`. Every RPC maps 1:1 to an already-shipped sync `KafkaConsumer` /
`MockConsumer` member; no new binding/ABI code was needed.
