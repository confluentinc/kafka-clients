# Multilanguage Integration Test Harness

## Context

Today the Rust Kafka client has three test surfaces that don't share scenarios:
- Rust integration tests (`tests/integration/`) drive `KafkaProducer` against a real broker spun up by testcontainers.
- C tests (`bindings/c/tests/`) use Unity and only call `kafka_producer_*` against `localhost:9092` (hardcoded).
- Python tests (`bindings/python/test/unit/`) use pytest, currently only with `MockProducer`.

Result: each binding's behavior is verified with a different (and shallower) set of scenarios than the native Rust client, so a regression in `producer.py` or in C error mapping can pass `make verify`. We want the **same** integration test bodies to run end-to-end against the native producer **and** through the Python and C bindings.

The chosen approach: introduce a `MultilanguageProducer` that implements the existing `Producer` trait by tunneling each call over **gRPC** to a small server written in the target language. The Python server uses `bindings/python/producer.py`; the C server links the `confluent_kafka` static library and calls the public C FFI directly. Both servers connect to the same testcontainer-managed broker. Test bodies are parameterized over a backend factory so each scenario runs three times (rust / python / c).

Why gRPC rather than embedding interpreters: the harness only needs a request/response boundary; gRPC gives us schema-defined messages, polyglot servers, and language-idiomatic process isolation. There is **no** runtime requirement on gRPC in the production client — it is gated behind a `multilanguage-tests` cargo feature and only pulled in as a dev-dependency.

The intended outcome is that `cargo test --features integration-tests,multilanguage-tests` runs every existing producer integration scenario against all three backends, and a single failure in any binding fails CI.

## Architecture

```
                          tests/integration/producer_test.rs
                                       │
                                       │  factory.create(config) -> Box<dyn Producer<Vec<u8>, Vec<u8>>>
                ┌──────────────────────┼──────────────────────┐
                ▼                      ▼                      ▼
       RustNativeFactory       PythonGrpcFactory       CGrpcFactory
                │                      │                      │
                ▼                      ▼                      ▼
       KafkaProducer<…>      MultilanguageProducer    MultilanguageProducer
                                       │                      │
                                       │  tonic unary RPCs     │
                                       ▼                      ▼
                            Python gRPC server         C++ gRPC server
                            (grpcio, :50051)           (grpc-cpp, :50052)
                                       │                      │
                                       ▼                      ▼
                            bindings/python/producer.py    direct calls
                                       │                  to kafka_producer_*
                                       ▼                      │
                            _confluentkafka.so  ◄─────────────┘
                                       │  (cdylib / staticlib)
                                       ▼
                            Apache Kafka 4.2.0 (testcontainers)
```

A single broker is shared across all backends in a test run (existing `cluster_pool.rs` already does this). The gRPC servers run as **Docker containers**, one image per language, lazily started on first use via the existing `testcontainers` dev-dependency (`Cargo.toml:40`). Each container exposes its gRPC port; testcontainers maps it to a random host port, which the backend pool hands to the factory. Container teardown is handled by the existing `atexit` Docker cleanup hook in `tests/common/cluster_pool.rs:83` — no separate child-process bookkeeping.

The images (`confluent-kafka-rust/python-grpc-server:dev`, `confluent-kafka-rust/c-grpc-server:dev`) are built once by `make build-grpc-images` (a new Makefile target that runs after the existing `build` target produces `libconfluent_kafka.so` and the bindings). The images are reusable: the Rust test process pulls them locally, doesn't rebuild between runs, and they're cached in CI by image tag/digest.

## Wire protocol (`proto/producer_service.proto`)

The proto mirrors the `Producer` trait one-to-one. Three deliberate simplifications:

1. **No `KafkaFuture` on the wire.** The server `await`s the producer's future internally; the gRPC unary response *is* the resolved future. This eliminates a stateful "future handle" protocol.
2. **No callback round-trip.** `send_with_callback`'s closure stays on the Rust client side. The client awaits the unary RPC, then synchronously runs `cb(Some(&metadata), None)` or `cb(None, Some(&error))` from the response. The proto only carries a `with_callback: bool` flag so the server can emit equivalent log lines if desired.
3. **K, V are `bytes`.** `MultilanguageProducer` only implements `Producer<Vec<u8>, Vec<u8>>`. Existing tests that use `String` keys/values switch to `into_bytes()` — a one-line change per assertion.

Sketch of the service:

```proto
syntax = "proto3";
package confluent.kafka.test;

service ProducerService {
  rpc CreateProducer(CreateProducerRequest) returns (CreateProducerResponse);
  rpc Send(SendRequest) returns (SendResponse);              // server awaits; response = metadata or error
  rpc Flush(FlushRequest) returns (StatusResponse);
  rpc PartitionsFor(PartitionsForRequest) returns (PartitionsForResponse);
  rpc Close(CloseRequest) returns (StatusResponse);
  rpc CloseTimeout(CloseTimeoutRequest) returns (StatusResponse);  // takes timeout_ms
}

message ProducerRecord {
  string topic = 1;
  optional int32 partition = 2;
  optional int64 timestamp = 3;
  optional bytes key = 4;
  optional bytes value = 5;
  repeated Header headers = 6;
}
message Header { string key = 1; bytes value = 2; }

message RecordMetadata {
  int64 offset = 1;
  int64 timestamp = 2;
  int32 serialized_key_size = 3;
  int32 serialized_value_size = 4;
  string topic = 5;
  int32 partition = 6;
}

message KafkaError {
  enum Variant { GENERIC = 0; TOPIC_AUTHORIZATION = 1; INVALID_TOPIC = 2;
                 GROUP_AUTHORIZATION = 3; BUFFER_EXHAUSTED = 4; ILLEGAL_ARGUMENT = 5;
                 ILLEGAL_STATE = 6; TIMEOUT = 7; RECORD_TOO_LARGE = 8; SERIALIZATION = 9; }
  Variant variant = 1;
  int32 code = 2;
  string message = 3;
  bool is_retriable = 4;
  bool is_fatal = 5;
  // Variant-specific payloads
  repeated string unauthorized_topics = 6;
  repeated string invalid_topics = 7;
  optional string group_id = 8;
}
```

Variant mapping uses the existing `KafkaError` enum at `src/common/kafka_error.rs:228-262`.

## Critical files & changes

### Production crate (small, tightly scoped)

- **`src/common/kafka_future.rs`** — Add `pub fn KafkaFuture::completed(result: Result<T, KafkaError>) -> KafkaFuture<T>`. Required because `MultilanguageProducer::send` already knows the result by the time it returns a `KafkaFuture`, and the existing `KafkaFuture::new` is `pub(crate)` so external test code cannot construct one. This is also a generally useful public API (analogous to `std::future::ready`).

### New crate: `multilanguage-test-server` (workspace member)

- **`Cargo.toml`** — Add `multilanguage-test-server` to `[workspace] members` in the root `Cargo.toml:2`. This crate owns the `.proto` file, runs the `tonic-build` codegen (via its own `build.rs`), and exposes the generated client/server stubs as a library used by both the Rust test code and the Python/C servers (the latter two regenerate their own stubs from the same `.proto`).
- **`multilanguage-test-server/proto/producer_service.proto`** — Service definition above.
- **`multilanguage-test-server/build.rs`** — `tonic_build::compile_protos(...)`.
- **`multilanguage-test-server/src/lib.rs`** — Re-export generated `pub mod proto`.
- **`multilanguage-test-server/src/bin/rust_server.rs`** *(optional, low priority)* — A reference Rust gRPC server that calls `KafkaProducer` directly. Useful for debugging the proto, not strictly required for the harness.

### Test harness (under `multilanguage-tests` feature)

- **`Cargo.toml`** — Add a new feature `multilanguage-tests = ["integration-tests"]` and dev-dependencies `tonic = "0.12"`, `prost = "0.13"`, `tokio-stream = "0.1"`, `multilanguage-test-server = { path = "multilanguage-test-server" }`. All gated under `#[cfg(feature = "multilanguage-tests")]` in test code so the production binary is unaffected.

- **`tests/common/multilanguage_producer.rs`** *(new)* — `MultilanguageProducer { producer_id: u64, client: ProducerServiceClient<Channel>, runtime: Handle }` implementing `Producer<Vec<u8>, Vec<u8>>`. Each method:
  - Builds the proto request, awaits the unary RPC, decodes response.
  - For `send` / `send_with_callback`, wraps the resolved result via `KafkaFuture::completed(...)`.
  - For `send_with_callback`, after decoding, synchronously invokes the local `Callback` closure with the decoded `RecordMetadata` or `KafkaError` reference.

- **`tests/common/backend_factory.rs`** *(new)* — `pub trait ProducerBackendFactory` with one method `async fn create(&self, config: ProducerConfig) -> Box<dyn Producer<Vec<u8>, Vec<u8>> + Send + Sync>` and `fn name(&self) -> &'static str`. Three impls: `RustNativeFactory`, `PythonGrpcFactory { endpoint }`, `CGrpcFactory { endpoint }`.

- **`tests/common/backend_pool.rs`** *(new)* — Mirrors `tests/common/cluster_pool.rs:83`: a `LazyLock<Mutex<HashMap<BackendKind, OnceCell<BackendHandle>>>>` that lazily starts a `testcontainers::GenericImage` for the requested backend on first use. The container exposes the fixed internal gRPC port (e.g. `50051`); testcontainers reports back the random host port via `container.get_host_port_ipv4(50051)`. The pool hands out `tonic::transport::Channel`s pointing at `http://127.0.0.1:<random_port>`. Cleanup reuses the existing `atexit` Docker hook in `cluster_pool.rs` (factor it into a small helper if not already shared).

- **`tests/common/multilanguage_test_macro.rs`** *(new)* — `multilanguage_test!(name, body_fn)` declarative macro that emits three `#[tokio::test(flavor = "multi_thread")]` wrappers (`name__rust`, `name__python`, `name__c`). Each wrapper acquires the appropriate backend handle from the pool and calls `body_fn(&factory).await`.

- **`tests/integration/producer_test.rs`** *(refactor)* — Convert each existing test (`test_produce_single_record`, `test_produce_with_key`, `test_produce_multiple_records_ordering`, etc.) into:
  ```rust
  async fn produce_single_record_inner<F: ProducerBackendFactory>(factory: &F) {
      let mut ctx = TestContext::new(ClusterConfig::default()).await;
      let topic = ctx.topic("single_record");
      let producer = factory.create(make_config(ctx.bootstrap_servers())).await;
      let record = ProducerRecord::with_key(topic.clone(),
          Some(b"test-key".to_vec()), Some(b"test-value".to_vec()));
      let metadata = producer.send(record).await.unwrap()
          .get_timeout(Duration::from_secs(30)).await.unwrap();
      assert!(metadata.offset() >= 0);
      assert_eq!(metadata.topic(), topic);
      producer.close().await.unwrap();
  }
  multilanguage_test!(test_produce_single_record, produce_single_record_inner);
  ```
  Reuse `make_config` from the same file (line 41) — already factored. `KafkaProducer<Vec<u8>, Vec<u8>>` is supported (see `src/producer/kafka_producer.rs:866`), so `RustNativeFactory` only needs `Box::new(ByteArraySerializer)` instead of `StringSerializer`.

### Python server (Docker image: `confluent-kafka-rust/python-grpc-server:dev`)

- **`bindings/python/grpc_server.py`** *(new)* — `grpcio` server bound to `0.0.0.0:50051`. Holds `producers: dict[int, KafkaProducer]`. Each RPC handler converts the proto request to a `producer.py` call and translates the response back. `Send` calls `producer.send(...)` then `future.result(timeout=...)` and packs the `RecordMetadata` fields into the response proto. `KafkaError` → proto `KafkaError` via the existing `code`/`message`/`is_retriable`/`is_fatal` properties on `bindings/python/producer.py:29-43`.
- **`bindings/python/Dockerfile.grpc`** *(new)* — Multi-stage:
  - **Stage 1** (`builder`): copy the prebuilt `libconfluent_kafka.so` from the host's `target/release/` into the image, build `_confluentkafka.so` against it via the existing `setup.py`.
  - **Stage 2** (runtime): slim Python base image; install `grpcio`; copy `producer.py`, `_confluentkafka.so`, generated proto stubs, and `grpc_server.py`. `EXPOSE 50051`. `CMD ["python", "grpc_server.py"]`.
- **`bindings/python/Makefile`** — Add `grpc-image` target that runs the codegen (`python -m grpc_tools.protoc ...`) then `docker build -t confluent-kafka-rust/python-grpc-server:dev -f Dockerfile.grpc .`. Idempotent — Docker layer caching makes repeated builds fast.

### C server (Docker image: `confluent-kafka-rust/c-grpc-server:dev`)

- **`bindings/c/grpc_server/server.cc`** *(new)* — gRPC C++ server (`grpc++`) since pure-C gRPC is awkward. Generates stubs from the same `.proto` via `protoc --grpc_out`. Includes `target/include/confluent_kafka.h` and links against `libconfluent_kafka.a` (already produced — see `Cargo.toml:15` `crate-type = ["lib", "staticlib", "cdylib"]`).
- **`bindings/c/CMakeLists.txt`** — Add a `kafka_grpc_server` executable target alongside the existing test executables (line 54+). Depends on system `grpc++` and `protobuf` packages.
- **`bindings/c/Dockerfile.grpc`** *(new)* — Multi-stage:
  - **Stage 1** (`builder`): base image with `cmake`, `g++`, `grpc++`-dev, `protobuf-compiler-grpc`. Copy `libconfluent_kafka.a` and `confluent_kafka.h` from the host build, plus the `grpc_server/` source and `.proto`. Build `kafka_grpc_server` static binary.
  - **Stage 2** (runtime): minimal base image (e.g. `debian:slim` with `libgrpc++` runtime). Copy in the binary. `EXPOSE 50052`. `CMD ["/usr/local/bin/kafka_grpc_server"]`.
- **`bindings/c/Makefile`** — Add `grpc-image` target invoking `docker build -t confluent-kafka-rust/c-grpc-server:dev -f Dockerfile.grpc .`.
- Server internals: holds a `std::unordered_map<uint64_t, kafka_producer_Producer_t*>`. `Send` calls `kafka_producer_Producer_send`, then `kafka_producer_FutureRecordMetadata_get`, then unpacks the `kafka_producer_RecordMetadata_t` fields into the response proto. Errors → proto `KafkaError` via `kafka_common_KafkaError_code` / `_message` / `_is_retriable` / `_is_fatal`.

### Build / orchestration

- **`Makefile`** — New targets that compose with the existing flow:
  - `build-grpc-images`: depends on `build` (which already produces `libconfluent_kafka.so`, `_confluentkafka.so`, and the C library — see `Makefile:7-10`), then `make -C bindings/python grpc-image` and `make -C bindings/c grpc-image`.
  - `test-multilanguage`: depends on `build-grpc-images`, then runs `cargo test --features integration-tests,multilanguage-tests`. The Rust test process is responsible for `docker run`-ing the prebuilt images via testcontainers — the Makefile only ensures the images exist locally.
  - Don't add to `verify` initially — keep `test-multilanguage` opt-in until the harness is stable, then promote.

- **`xtask/src/main.rs`** — New subcommand `test-multilanguage` matching the Makefile target; thin wrapper invoking `make test-multilanguage`. Mirrors the existing `coverage_all` pattern (`xtask/src/main.rs:213`).

## Container-to-broker networking

The existing `KafkaCluster` (`tests/common/kafka_cluster.rs:312`) exposes brokers on **host ports** like `127.0.0.1:32781`. Native Rust tests reach the broker via that address. But the gRPC client containers run in their own network namespace, where `127.0.0.1` is the container itself.

Resolution (in order of preference, choose one in implementation):

1. **Host-gateway alias (simplest, Linux + recent Docker Desktop)**: start the gRPC client containers with `--add-host=host.docker.internal:host-gateway` (testcontainers exposes this via `with_host("host.docker.internal", Host::HostGateway)`). The Rust test rewrites the bootstrap servers it sends in `CreateProducer` from `127.0.0.1:<port>` → `host.docker.internal:<port>`. No changes to `KafkaCluster`.

2. **Shared Docker network**: have `KafkaCluster` create a named user-defined bridge network and attach both the broker container and the gRPC client containers to it. The broker advertises an additional internal listener (e.g. `INTERNAL://kafka:9092`) and the test passes `kafka:9092` as bootstrap servers to the gRPC server. Cleaner topology but requires extending `kafka_cluster.rs` to add the internal listener.

The plan adopts **(1)** initially because it requires no changes to `KafkaCluster` and works on any Docker host with `host-gateway` support. If CI runs on an older Docker that doesn't support `host-gateway`, fall back to **(2)**.

## Verification

End-to-end checks, in order:

1. **Codegen builds**: `cargo build -p multilanguage-test-server` succeeds; generated `proto.rs` is reachable from `tests/common/multilanguage_producer.rs`.
2. **Images build**: `make build-grpc-images` produces `confluent-kafka-rust/python-grpc-server:dev` and `confluent-kafka-rust/c-grpc-server:dev`. Verify with `docker images`.
3. **Image standalone smoke test**: `docker run --rm -p 50051:50051 confluent-kafka-rust/python-grpc-server:dev` starts and responds to a manual `grpcurl` `CreateProducer` call. Same for C image on `:50052`.
4. **Backend pool isolation**: `cargo test --features multilanguage-tests test_produce_single_record__rust` must pass without `docker run`-ing either gRPC image (lazy initialization — verify by checking `docker ps` during the test).
5. **Python end-to-end**: `cargo test --features multilanguage-tests test_produce_single_record__python` lazily spins up the Python image, runs the test, container is cleaned up at process exit. Verify with `docker logs <container>` that the Python server saw `Producer.send`.
6. **C end-to-end**: same flow for `test_produce_single_record__c`.
7. **Full sweep**: `make test-multilanguage` runs the full producer integration suite three times (rust + python + c), all green. Random ports must not collide across parallel test threads (testcontainers handles this).
8. **Negative-path coverage**: existing `Timeout` and `RecordTooLarge` scenarios in `tests/integration/producer_test.rs` produce the same `KafkaError` variant from all three backends after refactor.
9. **Production binary unchanged**: `cargo build --release` (no features) produces a `libconfluent_kafka.so` that does **not** link against tonic/prost — gRPC must not leak into the release artifact. `nm -D target/release/libconfluent_kafka.so | grep -i tonic` returns nothing.

## Out of scope (call out, defer)

- Transactional producer methods — the trait doesn't include them yet (see `src/producer/producer_trait.rs:34-36`).
- `MockProducer`-only methods (`complete_next`, `error_next`, `history_count`) — not on the `Producer` trait, so they don't run multilanguage.
- Consumer-side parameterization — same pattern will apply when the Consumer trait lands, but is a follow-up.
- Promoting `test-multilanguage` into `make verify` — wait until the harness has been stable in CI for one cycle.
