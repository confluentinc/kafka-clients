# Milestone 11 — Admin multilanguage gRPC harness

Producer and consumer scenarios are written once in Rust and executed against
every language binding over gRPC (`RustNative` / `PythonGrpc` /
`PythonAsyncGrpc` / `CGrpc`). Producer has 70 such test entries and consumer
44. **Admin has none**, so the Rust core, the C FFI and the Python binding
have never been compared against each other. This plan closes that.

The gap is not theoretical: `ConsumerGroupDescription.coordinator()` returned a
fabricated `Node` (`host=""`, `port=-1`) and passed 3389 Rust, 249 C and 261
Python tests, because every one of those suites checks a single language
against its own expectations. A differential harness catches that class of
defect without anyone having to guess the right answer in advance — three
backends agree and one does not.

## 0. Scope

In scope: all 46 in-scope Admin RPCs (the same set enumerated in
`PLAN-bindings.md`), exposed over a new `AdminService`, driven from Rust
scenarios against four backends.

Out of scope: new Admin RPCs, changes to `src/admin` behaviour, and changes to
the production `Admin` trait or the `*Result` types. If a scenario reveals a
core defect, it is reported, not silently fixed inside this work.

## 1. Existing architecture this must follow

| Piece | Producer/consumer precedent |
|---|---|
| Proto | `multilanguage-test-server/proto/{producer,consumer}_service.proto`, compiled by `build.rs` via `tonic_build` with `protoc_bin_vendored` |
| Rust client | `tests/common/multilanguage_{producer,consumer}.rs` |
| Factory | `tests/common/backend_factory.rs` — `ProducerBackendFactory` / `ConsumerBackendFactory`, gRPC impls behind `#[cfg(feature = "multilanguage-tests")]` |
| Backend pool | `tests/common/backend_pool.rs` — `BackendKind`, one container per `(kind, broker_network)`, testcontainers-mapped ports |
| Macro | `tests/common/multilanguage_{test,consumer_test}_macro.rs` — one scenario name expands to four `#[tokio::test]`s with a `__grpc_` infix |
| Python server | `bindings/python/grpc_server.py` (sync) + `grpc_server_async.py` (asyncio), sharing `grpc_translate.py` |
| C++ server | `bindings/c/grpc_server/server.cc`, driving the **bare sync** C entry points |

Two properties of the precedent are load-bearing and must be preserved:

- **Blocking methods are awaited server-side.** The unary response *is* the
  resolved result; no futures or streaming cross the wire. Stated explicitly at
  `consumer_service.proto:33`.
- **The shared `__grpc_` infix** is what lets `make test-rust-all-features`
  exclude every container-backed arm with one `--skip __grpc`.

## 2. Decisions

### D1. The harness gets its own `AdminBackend` trait — the production `Admin` trait is not implementable from `tests/`

`MultilanguageConsumer` implements the production `Consumer` trait, so consumer
scenarios are generic over the real trait. **Admin cannot do this**, for two
independent reasons found by inspection:

- 33 of the 46 `*Result` types declare `pub(crate) fn new` (e.g.
  `list_topics_result.rs:34`, `list_offsets_result.rs:33`). Integration tests
  in `tests/` are a separate crate and cannot construct them.
- Completing a future later requires `KafkaFutureImpl`
  (`src/common/kafka_future.rs:377`), also `pub(crate)`. The only public
  constructor is `KafkaFuture::completed`.

Both visibilities are **correct** and must not be widened: Java's
`CreateTopicsResult` constructor is package-private and `KafkaFutureImpl` lives
in `org.apache.kafka.common.internals`, which CLAUDE.md maps to `pub(crate)`.
Widening them to make a test helper compile would diverge from Java.

So define a harness-local trait in `tests/common/admin_backend.rs`:

```rust
#[allow(async_fn_in_trait)]
pub trait AdminBackend {
    async fn create_topics(&self, topics: Vec<NewTopicSpec>, opts: CreateTopicsOpts)
        -> Result<HashMap<String, Result<(), KafkaError>>, KafkaError>;
    // ... one method per RPC, returning already-resolved plain data ...
    async fn close(&self, timeout: Option<Duration>) -> Result<(), KafkaError>;
    fn name(&self) -> &'static str;
}
```

Two implementations: `RustNativeAdmin` (owns a `Box<dyn Admin>`, calls the real
sync method then awaits the returned `KafkaFuture`s) and `MultilanguageAdmin`
(one gRPC round-trip per call).

**DoD #7 justification** (structs not present in Java): `AdminBackend` is test
scaffolding, exactly as `ConsumerBackendFactory` and `ProducerBackendFactory`
already are — neither exists in Java either. It models what actually crosses a
language boundary: both bindings already collapse per-key futures before
returning (Python `_run_sync` hands back a resolved dict; the C `_async` entry
points fire their callback with a fully-built result struct), so resolved plain
data is the honest wire shape, not a simplification. Per-key granularity is
still asserted — it is carried as a `HashMap<K, Result<V, KafkaError>>` rather
than as futures.

**Consequence to accept knowingly:** the harness cannot assert that an Admin
method *returns before* its future resolves. That property is untestable
through any binding (both are eager at the boundary) and stays covered by the
Rust unit tests in `src/admin`, which use the real trait.

### D2. Slicing mirrors the bindings slices, but a thin vertical goes first

The RPC grouping from `PLAN-bindings.md` (B0–B6) is reused verbatim as G0–G6 so
each slice is traceable to the binding slice it exercises. Ordering deviates in
one way: **G1 carries one scenario end-to-end through all four backends before
any further RPCs are added.** The proto shape for per-key results is the one
decision that is expensive to revisit once 46 RPCs depend on it, so it gets
validated against three independently-written servers first.

| Slice | Content |
|---|---|
| G0 | `admin_service.proto` skeleton + shared messages, `build.rs` wiring, `AdminService` stubs in both servers, `MultilanguageAdmin`, `AdminBackendFactory`, `multilanguage_admin_test!`, `BackendKind` reuse, `create`/`close` only |
| G1 | Topics & partitions (6): `createTopics`, `deleteTopics`, `listTopics`, `describeTopics` (names **and** ids), `createPartitions`, `deleteRecords` — **plus proving one scenario green on all four backends** |
| G2 | Cluster, configs, log dirs (8) |
| G3 | Elections, reassignments, offsets (4) |
| G4 | Groups & offsets (9) |
| G5 | ACLs, quotas, SCRAM, tokens, features (13) |
| G6 | Producers & transactions (6) + scenario sweep and the README/plan updates |

### D3. Scenario inventory is converted, not invented

The **primary** source is the 38 already-committed real-broker admin
integration tests (`tests/integration/admin_*_test.rs`, 3,235 lines across 12
files). They are Rust, they already use `TestContext`, and they currently run
against `RustNative` only. Converting each into a scenario body generic over
`AdminBackend` and registering it via `multilanguage_admin_test!` turns 38
single-backend tests into ~152 test entries — which is exactly how producer
reached 70 and consumer 44.

Conversion is mechanical because these tests only ever *read* `*Result` types:

```rust
// today
let admin = admin_for(ctx.bootstrap_servers());
let names = admin.list_topics(ListTopicsOptions::new()).names().get().await?;

// converted
let admin = factory.create(admin_config(&bootstrap_for(factory, ctx))).await?;
let names = admin.list_topics().await?;
```

This is also the empirical proof of D1: those 38 tests read results through
public accessors, and none of them constructs a `*Result` — construction is the
part that is `pub(crate)`, and it is exactly what a gRPC proxy would need.

The **secondary** source is `~/Desktop/ckr-apitest` (836 C checks, 718 Python
checks, all 46 RPCs), which covers edge cases the committed tests do not:
partial-failure batches, `validate_only` proven non-mutating, duplicate keys,
unicode byte-exactness through values *and* error messages, `None` vs `""` vs
absent, empty batches, boundary numerics. Those are ad-hoc and uncommitted;
this harness is where the ones worth keeping become permanent.

Where a committed test and an ad-hoc probe disagree, the committed test wins
unless the Java source says otherwise.

Cases that a single PLAINTEXT node cannot reach are recorded, not silently
skipped — the four delegation-token RPCs are error-path only, ACL denial is
unreachable (`User:ANONYMOUS` is a super user), and `electLeaders` reaches only
`ELECTION_NOT_NEEDED(84)`.

### D4. Each server drives the same surface its own binding exposes

- Python sync server → `admin.py` `AdminClient` / `MockAdminClient`.
- Python async server → `AsyncAdminClient` / `AsyncMockAdminClient`.
- C++ server → the **bare sync** C entry points, as `server.cc` already does
  for the consumer. The `_async` variants are covered by the committed C unit
  tests; re-testing them here would trade differential coverage for redundancy.

## 3. Environment notes

- `docker pull` on the dev machine fails with a credential-helper timeout.
  Bypass with a scratch `DOCKER_CONFIG` (`{"auths":{}}`, no `currentContext`)
  **plus** an explicit `DOCKER_HOST=unix:///Users/pratyush/.docker/run/docker.sock`.
  Both base images (`python:3.11-slim`, `debian:trixie-slim`) are now cached.
- Point `CARGO_TARGET_DIR` outside the repo for container builds, or Linux
  artifacts land in the working tree.
- `cargo test` accepts only one positional filter.
- Host has no `cmake`, so `make build-c` / `test-c` / `verify` and the
  `.githooks/pre-commit` hook cannot pass locally; commit with `--no-verify`
  and leave `cargo xtask lint` to a Linux runner.

### Finding: the gRPC images are not buildable on macOS as committed

All three Dockerfiles consume **host-built** Rust artifacts —
`bindings/c/Dockerfile.grpc:43` copies `target/release/libconfluent_kafka.a`
and `bindings/python/Dockerfile.grpc:36` copies
`target/release/libconfluent_kafka.so`. On macOS the archive is Mach-O, so the
in-container GNU `ld` fails with `archive has no index; run ranlib to add one`,
and the `.so` does not exist at all because cargo emits
`libconfluent_kafka.dylib`. Architecture is **not** the cause: host and
container are both arm64/aarch64. CI is Linux, where the host artifact is
already ELF, which is why this has never surfaced.

Worked around locally by building the Rust artifacts for Linux in a container
and assembling a scratch build context (the repo tree is only read, so it is
safe to run alongside an editing agent). Making the committed Dockerfiles
host-independent — e.g. a builder stage that runs cargo in-container — is a
deliberate infra change and is **out of scope here**; it is recorded as a
finding for a separate decision.

## 4. Definition of Done

Per slice: `cargo build`, the new tests green on **all four** backends,
`cargo xtask format-check`, `cargo xtask lint`, `cargo xtask check-bindings`,
and no regression in the existing producer/consumer multilanguage arms. A slice
is not done while any backend is skipped for convenience — a backend that
cannot run must be reported as a blocker, not quietly dropped.
