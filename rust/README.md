# Rust Kafka Client

The `confluent_kafka` crate: a high-fidelity, architecture-preserving Rust
translation of the Apache Kafka Java client (client only), translated from
`org.apache.kafka.clients` and related Java packages (Apache Kafka 4.4.0, whose
sources are the `kafka/` submodule at the repository root).

- **Architecture:** Mirrors the Java client structure, namespaces, and logic, adapted to Rust module and naming conventions
- **Schema:** All Kafka message schemas are generated from the official JSON definitions
- **Wire Protocol:** Full support for Kafka's binary protocol, including flexible versions and tagged fields
- **C FFI: (experimental)** Behind the `ffi` feature, the crate exports the C API that the
  bindings in `../c/` and `../python/` are built on; the build writes its header
  to `target/include/confluent_kafka.h` for C users or to `target/include/confluent_kafka_bindings.h` for usage by other bindings. The FFI layer is experimental right now, it means it's not supported and its API is not stable yet.

## Status

**Generally Available:** this is the first GA release of the Rust client, version 1.0.0. We welcome your feedback, see [RFCs and feedback](../README.md#rfcs-and-feedback).

See [Known limitations](#known-limitations) for what this release does not support yet.

## Directory Structure

```
src/                    # The confluent_kafka crate
├── lib.rs              # Crate root
├── *.rs                # Client layer (org.apache.kafka.clients), flat at the crate root
├── common/             # org.apache.kafka.common
├── producer/           # org.apache.kafka.clients.producer
├── consumer/           # org.apache.kafka.clients.consumer
├── admin/              # org.apache.kafka.clients.admin
└── ffi/                # C FFI (feature `ffi`)
generator/              # Wire-protocol code generator
├── src/                # Generator sources, compiled into build.rs
├── messages/           # Kafka JSON RPC definitions
├── test-messages/      # Test-only message specs
└── crate/              # Manifest for the generator's tests and CLI
build.rs                # Runs the generator; writes the C header
tests/                  # Integration and multilanguage tests
examples/
xtask/                  # Build automation (`cargo xtask ...`)
multilanguage-test-server/  # gRPC protos + server for the multilanguage tests
consumer-perf/          # Consumer benchmark harness
tools/verifiable-clients/
Makefile                # Targets the repository root's Makefile delegates to
```

`../design/current/structure.md` describes every module in detail.

## Install development tools

```bash
cargo install cargo-llvm-cov grcov && rustup component add llvm-tools
```

- **cargo-llvm-cov**: LLVM source-based code coverage instrumentation
- **grcov**: HTML coverage report generator
- **llvm-tools**: LLVM binaries required by cargo-llvm-cov

## Building

Rust automation is handled via [xtask](https://github.com/matklad/cargo-xtask) commands.

**Note:** The project is configured with `#![deny(warnings)]` to treat all compiler warnings as errors, ensuring code quality and maintainability.

### 1. Build the Library

```bash
cargo build
```
- Runs the code generator, formats all generated code, and builds the library.
  Add `--features ffi` for the C API.

### 2. Format All Code

```bash
cargo xtask format
```
- Formats all source and generated Rust code using `rustfmt`.

### 3. Check Formatting (CI-friendly)

```bash
cargo xtask format-check
```
- Checks formatting of all code (source + generated). Fails if any file is not properly formatted.

### 4. Check Generated Code

```bash
cargo xtask check-generated
```
- Checks, without modifying anything, that the message specs in `generator/messages/` match the `kafka` submodule's, that the generated error-code tables are current, and the formatting of generated files. Useful for CI.

### 5. Lint

```bash
cargo xtask lint          # clippy with warnings as errors
cargo xtask lint-custom   # the project's translation-rule checks
```

## Running Tests

```bash
cargo test --workspace
```
- Runs the unit tests and the tests that need no broker, including round-trip
  wire protocol and generated message tests.

The broker-backed suites are behind cargo features and need Docker:

```bash
cargo test --features integration-tests --test integration
cargo test --all-features -- --skip __grpc   # everything, minus the gRPC arms
```

The `__grpc_python` / `__grpc_c` arms of the multilanguage suite need the
bindings' gRPC images; `make -C python test-integration` and
`make -C c test-integration`, from the repository root, build those first.

## Make targets

`Makefile` wraps the commands above with the flags CI uses. Run it here, or as
`make -C rust <target>` from the repository root:

```bash
make build                # release build, feature ffi
make test                 # cargo test --workspace, then with feature ffi
make test-all-features    # all features, skipping the gRPC arms
make test-integration     # functional integration suite (Docker)
make test-integration-perf
make format-check lint doc-check
make verify               # what CI runs on Linux; verify-macos on macOS
make clean
```

## Code Coverage

```bash
cargo xtask coverage          # Unit tests, HTML report at coverage/html/index.html
cargo xtask coverage-lcov     # Unit tests, lcov output at coverage/lcov.info
cargo xtask coverage-all      # Unit + integration tests (requires Docker)
```

## Regenerating Code

Generated code is automatically updated on build. To force regeneration:

```bash
cargo clean && cargo build
```

## Cargo features

Every optional dependency is a default feature, so a default build supports
every compression type, TLS and SCRAM credential salting:

| Feature | Default | What it enables |
|---|---|---|
| `gzip` | yes | `compression.type=gzip`, and reading gzip batches (`flate2`) |
| `snappy` | yes | `compression.type=snappy`, and reading snappy batches (`snap`) |
| `lz4` | yes | `compression.type=lz4`, and reading lz4 batches (`lz4_flex`) |
| `zstd` | yes | `compression.type=zstd`, and reading zstd batches (`zstd`) |
| `aws-lc-rs` | yes | PBKDF2 for `Admin::alter_user_scram_credentials` upsertions, and the crypto provider of `rustls` |
| `rustls` | yes | `security.protocol` `SSL` and `SASL_SSL`; implies `aws-lc-rs` |
| `webpki-roots` | yes | Mozilla's root certificates, trusted without `ssl.truststore.location` or `ssl.truststore.certificates`; implies `rustls` |
| `ffi` | no | the C API |
| `integration-tests` | no | the broker-backed test suites; implies every codec and `webpki-roots` |

With a feature off, the client fails with an explicit error where it would
need it:

- a disabled `compression.type` fails the producer's configuration with a
  `ConfigError`, and a fetched batch in a disabled codec fails with an
  `InvalidRecordError`;
- `SSL` and `SASL_SSL` fail the client's construction with a `ConfigError`;
- a SCRAM upsertion without `aws-lc-rs` fails its user's future with a
  `LocalUnsupportedOperationError`, while deletions still work.

`rustls` needs a default trust store, and `webpki-roots` is the only one so
far, so TLS is enabled through `webpki-roots`. For example, a client with lz4
and TLS only:

```toml
[dependencies]
confluent-kafka = { version = "1.0.0", default-features = false, features = ["lz4", "webpki-roots"] }
```

## Examples

See [./examples/README.md](./examples/README.md).

## Share consumer (KIP-932)

`KafkaShareConsumer` consumes from a share group, translated from Java 4.4.0's
`KafkaShareConsumer`. Records are acquired by one consumer at a time and
acknowledged one by one, instead of committing offsets per partition. The
integration tests run it against Apache Kafka 4.2.0 and 4.4.0 brokers; some
broker features, such as the dead-letter queue, need 4.4.

```rust,no_run
use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::StringDeserializer;
use confluent_kafka::consumer::{AcknowledgeType, KafkaShareConsumer, ShareConsumer};

async fn run() -> Result<(), Error> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
        ("group.id".to_string(), "my-share-group".to_string()),
        ("share.acknowledgement.mode".to_string(), "explicit".to_string()),
    ]);
    let mut consumer = KafkaShareConsumer::new(&props, StringDeserializer::new(), StringDeserializer::new())?;
    consumer.subscribe(vec!["orders".to_string()]).await?;
    loop {
        let records = consumer.poll(Duration::from_millis(1000)).await?;
        for record in &records {
            // Accept, Release (deliver again) or Reject (archive) each record.
            consumer.acknowledge_with_record_type(record, AcknowledgeType::Accept)?;
        }
        consumer.commit_sync().await?;
    }
}
```

In the default `implicit` acknowledgement mode, the next `poll`,
`commit_sync` or `commit_async` accepts every record the previous `poll`
returned, so no `acknowledge_*` call is needed.

- **API:** the `ShareConsumer` trait, with `KafkaShareConsumer` and
  `MockShareConsumer` (for tests) implementing it, and `DynShareConsumer` to
  hold different implementations behind one type. `ShareConsumerHandle`
  (`ShareConsumer::handle`) calls `wakeup` from another task while `poll`
  holds the consumer. `AcknowledgementCommitCallback` reports the outcome of
  each acknowledgement, on the task that calls `poll`, `commit_*` or `close`.
- **Admin:** `Admin::describe_share_groups`, `list_share_group_offsets`,
  `alter_share_group_offsets`, `delete_share_group_offsets` and
  `delete_share_groups`, each with its `_with_options` form.
- **Differences from Java:** the rustdoc of `KafkaShareConsumer` and
  `ShareConsumer` lists them under *Translation notes*. The notable one: a
  record acknowledged with `AcknowledgeType::Renew` comes back from a later
  `poll` as a new `ConsumerRecord`, deserialized again.
- **Not included yet:** the C FFI and the Python, C and .NET bindings of the
  share consumer and of the Admin share-group methods; the KIP-714 telemetry
  methods (`clientInstanceId`, `registerMetricForSubscription`,
  `unregisterMetricFromSubscription`), which `Consumer` does not have either.

## Known limitations

The crate does not include the following Java client features:

- **Consumer groups:** only the KIP-848 group protocol
  (`group.protocol=consumer`). The classic protocol (`group.protocol=classic`)
  and client-side partition assignors are not supported, and
  `KafkaConsumer::new` returns `LocalUnsupportedOperationError` for them.
- **Share groups:** the 1.0 release has no share consumer. The share
  consumer added since (see [Share consumer (KIP-932)](#share-consumer-kip-932),
  not yet released) is Rust only: it has no C FFI and no Python, C or .NET
  binding yet.
- **SASL:** only `PLAIN`, over `SASL_PLAINTEXT` and `SASL_SSL`. SCRAM, GSSAPI
  (Kerberos) and OAUTHBEARER are not supported, and any other `sasl.mechanism`
  fails the configuration.
- **TLS stores:** PEM only, as files or inline in the configuration. JKS and
  PKCS12 keystores and truststores are not supported, so convert them to PEM.
  Encrypted private keys are rejected.
- **Client telemetry:** KIP-714 is not implemented, so `enable.metrics.push`
  is not a defined configuration.
- **Interceptors:** producer and consumer interceptors cannot be registered.
- **Windows:** it's not supported yet.

## License

Apache License 2.0. See [LICENSE](../LICENSE) for details.
