# Phase 6: Integration Tests — SSL & SASL against Real Kafka

## Context

Phases 1-5 are complete. All channel builders, the SASL authenticator state machine, and the factory function are implemented and unit tested. Phase 6 verifies everything end-to-end against a real Kafka 4.2.0 broker running in Docker with SASL and SSL listeners.

## Scope

- Add test certificate generation utility
- Extend test infrastructure for SASL/SSL Docker containers
- Add 5 integration tests covering SSL, SASL_PLAINTEXT, SASL_SSL, auth failure, and unsupported mechanism
- Feature-gated under `integration-tests` like existing tests

## Test Matrix

| Test | Protocol | Mechanism | What it verifies |
|---|---|---|---|
| `test_ssl_connection` | SSL | — | TLS handshake, ApiVersions over TLS |
| `test_sasl_plaintext_connection` | SASL_PLAINTEXT | PLAIN | SASL handshake, auth, metadata fetch |
| `test_sasl_ssl_connection` | SASL_SSL | PLAIN | TLS + SASL combined |
| `test_sasl_wrong_credentials` | SASL_PLAINTEXT | PLAIN | Authentication failure handling |
| `test_sasl_unsupported_mechanism` | SASL_PLAINTEXT | — | Error when requesting unsupported mechanism |

---

## Step 1: Add `rcgen` dev-dependency

**File:** `Cargo.toml`

```toml
[dev-dependencies]
rcgen = "0.13"
```

The `rcgen` crate generates self-signed X.509 certificates programmatically, avoiding checked-in certificate files and external CLI tool dependencies.

---

## Step 2: Create `tests/common/test_certs.rs`

Certificate generation utility for SSL integration tests.

### Public API

```rust
/// Holds PEM-encoded certificates and key generated for testing.
pub struct TestCertificates {
    /// CA certificate in PEM format.
    pub ca_cert_pem: String,
    /// Broker certificate in PEM format (signed by the CA).
    pub broker_cert_pem: String,
    /// Broker private key in PEM format.
    pub broker_key_pem: String,
}

/// Generates a self-signed CA and a broker certificate for testing.
///
/// The broker certificate includes SANs for `hostname`, `localhost`,
/// and `127.0.0.1` to cover all test connection scenarios.
pub fn generate_test_certificates(hostname: &str) -> TestCertificates
```

### Implementation

1. Use `rcgen::CertificateParams` with `is_ca: IsCa::Ca(BasicConstraints::Unconstrained)` to create a CA keypair
2. Create broker `CertificateParams` with:
   - `subject_alt_names`: `hostname`, `localhost`, `127.0.0.1` (as DNS and IP SANs)
   - `is_ca: IsCa::ExplicitNoCa`
3. Sign the broker cert with the CA key
4. Export all as PEM strings via `serialize_pem()` / `serialize_private_key_pem()`

---

## Step 3: Add `SecurityMode` to `tests/common/cluster_config.rs`

### New enum

```rust
/// Security mode for the Kafka test cluster.
///
/// Determines which listeners and authentication are configured
/// on the Docker container.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum SecurityMode {
    /// Plaintext (no encryption, no authentication).
    Plaintext,
    /// SASL PLAIN over plaintext TCP.
    SaslPlaintext { username: String, password: String },
    /// SSL/TLS encryption (no SASL authentication).
    Ssl,
    /// SASL PLAIN over SSL/TLS.
    SaslSsl { username: String, password: String },
}

impl Default for SecurityMode {
    fn default() -> Self {
        SecurityMode::Plaintext
    }
}
```

### Modify `ClusterConfig`

Add `security_mode` field:
```rust
pub struct ClusterConfig {
    pub brokers: u16,
    pub server_properties: BTreeMap<String, String>,
    pub security_mode: SecurityMode,
}
```

`Default` impl includes `security_mode: SecurityMode::default()` — backward compatible.

---

## Step 4: Create custom `SecureKafka` image in `tests/common/kafka_cluster.rs`

### Why a custom image?

The standard `testcontainers_modules::kafka::apache::Kafka` only supports PLAINTEXT:
- It configures a single PLAINTEXT listener on port 9092
- `exec_after_start()` only writes `KAFKA_ADVERTISED_LISTENERS` for PLAINTEXT
- It defaults to the GraalVM native image which may not support SASL

### SecureKafka design

A new struct implementing `testcontainers::Image`:

```rust
struct SecureKafka {
    env_vars: HashMap<String, String>,
    copy_to_sources: Vec<(String, Vec<u8>)>,  // (container_path, content)
    exposed_ports: Vec<ContainerPort>,
}
```

**Image name:** `apache/kafka` (JVM, not native — SASL requires full JVM)
**Tag:** `4.2.0`

**Port allocation:**
| Protocol | Container port | Purpose |
|---|---|---|
| PLAINTEXT | 9092 | Always present (health, existing tests) |
| BROKER | 9093 | Inter-broker |
| CONTROLLER | 9094 | KRaft controller |
| SASL_PLAINTEXT | 9095 | SASL without encryption |
| SSL | 9096 | TLS only |
| SASL_SSL | 9097 | TLS + SASL |

Only the required secure port is exposed (based on `SecurityMode`), plus 9092 always.

### Listener configuration (env vars)

**SASL_PLAINTEXT example:**
```
KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,SASL_PLAINTEXT://0.0.0.0:9095,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT
KAFKA_SASL_ENABLED_MECHANISMS=PLAIN
KAFKA_OPTS=-Djava.security.auth.login.config=/opt/kafka/config/kafka_server_jaas.conf
```

**SSL example:**
```
KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9096,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,SSL:SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT
KAFKA_SSL_KEYSTORE_TYPE=PEM
KAFKA_SSL_KEYSTORE_KEY=<broker_key_pem>
KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN=<broker_cert_pem>
KAFKA_SSL_TRUSTSTORE_TYPE=PEM
KAFKA_SSL_TRUSTSTORE_CERTIFICATES=<ca_cert_pem>
```

**SASL_SSL:** Combines both SASL and SSL env vars.

### JAAS config

Mounted to `/opt/kafka/config/kafka_server_jaas.conf` via `with_copy_to()`:

```
KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin"
    password="admin-secret"
    user_admin="admin-secret";
};
```

### SSL certificates

For SSL and SASL_SSL modes, certificates are generated using `test_certs::generate_test_certificates("localhost")` and:
- Broker key/cert configured via inline PEM env vars (`KAFKA_SSL_KEYSTORE_TYPE=PEM`, etc.)
- CA cert stored for client-side `SslConfig::truststore_certificates`
- Using PEM format avoids JKS/PKCS12 keystore complexity

### `exec_after_start()` implementation

```rust
fn exec_after_start(&self, cs: ContainerState) -> Result<Vec<ExecCommand>, TestcontainersError> {
    // Build KAFKA_ADVERTISED_LISTENERS with host-mapped ports
    // e.g., "PLAINTEXT://127.0.0.1:{plaintext_port},SASL_PLAINTEXT://127.0.0.1:{sasl_port},BROKER://localhost:9093"
    let script = format!(
        "#!/usr/bin/env bash\n\
         export KAFKA_ADVERTISED_LISTENERS={advertised_listeners}\n\
         /etc/kafka/docker/run\n",
        advertised_listeners = /* dynamic based on security mode */
    );
    // Write script to START_SCRIPT path, wait for "Kafka Server started"
}
```

### KafkaCluster modifications

**New fields:**
```rust
pub struct KafkaCluster {
    _container: ContainerAsync</* either Kafka or SecureKafka */>,
    container_id: String,
    bootstrap_servers: String,
    /// Bootstrap servers for the secure listener (SASL/SSL port).
    secure_bootstrap_servers: Option<String>,
    /// CA certificate PEM for SSL tests.
    ca_cert_pem: Option<String>,
    config: ClusterConfig,
}
```

**Modified `start_with_config()`:**
- `SecurityMode::Plaintext` — uses existing `apache::Kafka` image (unchanged)
- All other modes — uses `SecureKafka` image, retrieves secure port, stores CA cert if SSL

**New accessors:**
```rust
pub fn secure_bootstrap_servers(&self) -> Option<&str>
pub fn ca_cert_pem(&self) -> Option<&str>
```

### Container type challenge

`KafkaCluster` currently stores `ContainerAsync<Kafka>`. With `SecureKafka`, it needs to store either. Options:
1. Use `enum` wrapper for the container
2. Store as `Box<dyn Any>` (loses type info)
3. Always use `SecureKafka` (even for PLAINTEXT)

**Recommendation:** Option 1 — an internal enum `KafkaContainer` with variants for `Kafka` and `SecureKafka`.

---

## Step 5: Update `tests/common/test_context.rs`

Add delegation methods:

```rust
impl TestContext {
    /// Bootstrap servers for the secure listener (SASL/SSL port).
    /// Returns `None` for PLAINTEXT clusters.
    pub fn secure_bootstrap_servers(&self) -> Option<&str> {
        self.cluster.secure_bootstrap_servers()
    }

    /// CA certificate PEM for SSL tests.
    /// Returns `None` for non-SSL clusters.
    pub fn ca_cert_pem(&self) -> Option<&str> {
        self.cluster.ca_cert_pem()
    }
}
```

---

## Step 6: Update `tests/common/mod.rs`

Add:
```rust
#[cfg(feature = "integration-tests")]
#[allow(dead_code)]
pub mod test_certs;
```

---

## Step 7: Create `tests/integration/ssl_sasl_test.rs`

### Test helpers

```rust
/// Create a Selector with SslChannelBuilder.
fn create_ssl_selector(ca_cert_pem: &str) -> Selector {
    let ssl_config = SslConfig {
        truststore_certificates: Some(ca_cert_pem.to_string()),
        endpoint_identification_algorithm: String::new(), // disable hostname verification for tests
        ..SslConfig::default()
    };
    let ssl_factory = SslFactory::new(&ssl_config).unwrap();
    let channel_builder = Box::new(SslChannelBuilder::new(ssl_factory, None));
    Selector::with_defaults(NO_IDLE_TIMEOUT_MS, channel_builder)
}

/// Create a Selector with SaslChannelBuilder for SASL_PLAINTEXT.
fn create_sasl_plaintext_selector(username: &str, password: &str) -> Selector {
    let sasl_config = SaslConfig {
        mechanism: "PLAIN".to_string(),
        username: Some(username.to_string()),
        password: Some(password.to_string()),
        ..SaslConfig::default()
    };
    let channel_builder = SaslChannelBuilder::new(
        SecurityProtocol::SaslPlaintext, sasl_config, None, None, "integration-test",
    ).unwrap();
    Selector::with_defaults(NO_IDLE_TIMEOUT_MS, Box::new(channel_builder))
}

/// Create a Selector with SaslChannelBuilder for SASL_SSL.
fn create_sasl_ssl_selector(username: &str, password: &str, ca_cert_pem: &str) -> Selector {
    let ssl_config = SslConfig {
        truststore_certificates: Some(ca_cert_pem.to_string()),
        endpoint_identification_algorithm: String::new(),
        ..SslConfig::default()
    };
    let ssl_factory = SslFactory::new(&ssl_config).unwrap();
    let sasl_config = SaslConfig {
        mechanism: "PLAIN".to_string(),
        username: Some(username.to_string()),
        password: Some(password.to_string()),
        ..SaslConfig::default()
    };
    let channel_builder = SaslChannelBuilder::new(
        SecurityProtocol::SaslSsl, sasl_config, Some(ssl_factory), None, "integration-test",
    ).unwrap();
    Selector::with_defaults(NO_IDLE_TIMEOUT_MS, Box::new(channel_builder))
}

/// Poll until disconnection (for error tests).
async fn poll_until_disconnected(selector: &mut Selector) { ... }
```

Reuse `parse_bootstrap_addr`, `build_request_send`, `parse_response`, `poll_until_connected`, `poll_until_receive` from `connection_test.rs` (extract to shared helper or duplicate — keep simple).

### Test 1: `test_ssl_connection`

```rust
#[tokio::test]
async fn test_ssl_connection() {
    let ctx = TestContext::new(ClusterConfig { security_mode: SecurityMode::Ssl, ..Default::default() }).await;
    let ca_cert_pem = ctx.ca_cert_pem().expect("SSL cluster should have CA cert");
    let mut selector = create_ssl_selector(ca_cert_pem);
    let addr = parse_bootstrap_addr(ctx.secure_bootstrap_servers().unwrap());

    selector.connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await.expect("Failed to connect");
    poll_until_connected(&mut selector).await;

    // Send ApiVersions to verify data flows over TLS
    let builder = ApiVersionsRequestBuilder::new();
    let (send, header) = build_request_send(&builder, "ssl-test", 1, NODE_ID);
    selector.send(send).unwrap();
    poll_until_receive(&mut selector).await;

    let receives = selector.completed_receives();
    assert_eq!(1, receives.len());
    let response = parse_response(receives[0].payload().unwrap(), &header);
    let ConcreteResponse::ApiVersions(avr) = &response else { panic!("Expected ApiVersions") };
    assert_eq!(avr.data().error_code, Errors::None.code());

    selector.close().await;
}
```

### Test 2: `test_sasl_plaintext_connection`

```rust
#[tokio::test]
async fn test_sasl_plaintext_connection() {
    let config = ClusterConfig {
        security_mode: SecurityMode::SaslPlaintext {
            username: "admin".to_string(),
            password: "admin-secret".to_string(),
        },
        ..Default::default()
    };
    let ctx = TestContext::new(config).await;
    let mut selector = create_sasl_plaintext_selector("admin", "admin-secret");
    let addr = parse_bootstrap_addr(ctx.secure_bootstrap_servers().unwrap());

    selector.connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await.expect("Failed to connect");
    poll_until_connected(&mut selector).await;

    // Channel ready means SASL auth completed successfully
    assert!(selector.is_channel_ready(NODE_ID));

    // Verify with a MetadataRequest
    let metadata_builder = MetadataRequestBuilder::new_with_version(None, true, 0);
    let (send, header) = build_request_send(&metadata_builder, "sasl-test", 1, NODE_ID);
    selector.send(send).unwrap();
    poll_until_receive(&mut selector).await;

    let receives = selector.completed_receives();
    assert_eq!(1, receives.len());
    let response = parse_response(receives[0].payload().unwrap(), &header);
    let ConcreteResponse::Metadata(mr) = &response else { panic!("Expected Metadata") };
    assert!(!mr.data().brokers.is_empty());

    selector.close().await;
}
```

### Test 3: `test_sasl_ssl_connection`

Same as test 2 but with `SecurityMode::SaslSsl` and `create_sasl_ssl_selector`. Verifies TLS + SASL combined.

### Test 4: `test_sasl_wrong_credentials`

```rust
#[tokio::test]
async fn test_sasl_wrong_credentials() {
    let config = ClusterConfig {
        security_mode: SecurityMode::SaslPlaintext {
            username: "admin".to_string(),
            password: "admin-secret".to_string(),
        },
        ..Default::default()
    };
    let ctx = TestContext::new(config).await;
    // Wrong password
    let mut selector = create_sasl_plaintext_selector("admin", "wrong-password");
    let addr = parse_bootstrap_addr(ctx.secure_bootstrap_servers().unwrap());

    selector.connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await.expect("Connect should succeed (TCP level)");

    // Poll until disconnect — broker rejects auth
    poll_until_disconnected(&mut selector).await;
    assert!(!selector.is_channel_ready(NODE_ID));
}
```

### Test 5: `test_sasl_unsupported_mechanism`

```rust
#[tokio::test]
async fn test_sasl_unsupported_mechanism() {
    let config = ClusterConfig {
        security_mode: SecurityMode::SaslPlaintext {
            username: "admin".to_string(),
            password: "admin-secret".to_string(),
        },
        ..Default::default()
    };
    let ctx = TestContext::new(config).await;
    // Use SCRAM-SHA-256 mechanism which the broker doesn't have enabled
    let sasl_config = SaslConfig {
        mechanism: "SCRAM-SHA-256".to_string(),
        username: Some("admin".to_string()),
        password: Some("admin-secret".to_string()),
        ..SaslConfig::default()
    };
    let channel_builder = SaslChannelBuilder::new(
        SecurityProtocol::SaslPlaintext, sasl_config, None, None, "integration-test",
    ).unwrap();
    let mut selector = Selector::with_defaults(NO_IDLE_TIMEOUT_MS, Box::new(channel_builder));
    let addr = parse_bootstrap_addr(ctx.secure_bootstrap_servers().unwrap());

    selector.connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await.expect("Connect should succeed (TCP level)");

    // Poll until disconnect — broker rejects unsupported mechanism
    poll_until_disconnected(&mut selector).await;
    assert!(!selector.is_channel_ready(NODE_ID));
}
```

---

## Step 8: Update `tests/integration/main.rs`

Add:
```rust
mod ssl_sasl_test;
```

---

## Implementation Order

### Commit 1: Test infrastructure
1. Add `rcgen = "0.13"` to `[dev-dependencies]` in `Cargo.toml`
2. Create `tests/common/test_certs.rs`
3. Add `SecurityMode` enum + field to `tests/common/cluster_config.rs`
4. Add `pub mod test_certs` to `tests/common/mod.rs`
5. `cargo build`, `cargo test`
6. **Commit**: `"Add test certificate generation and SecurityMode to cluster config (Phase 6 infra)"`

### Commit 2: SecureKafka image + KafkaCluster
1. Implement `SecureKafka` struct with `Image` trait in `tests/common/kafka_cluster.rs`
2. Modify `KafkaCluster` to dispatch on `SecurityMode`
3. Add `secure_bootstrap_servers()`, `ca_cert_pem()` to `KafkaCluster` and `TestContext`
4. `cargo build`, `cargo test`
5. **Commit**: `"Add SecureKafka image and extend KafkaCluster for SASL/SSL"`

### Commit 3: Integration tests
1. Create `tests/integration/ssl_sasl_test.rs` with 5 tests
2. Add `mod ssl_sasl_test` to `tests/integration/main.rs`
3. `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint`
4. `cargo test --features integration-tests` (requires Docker)
5. **Commit**: `"Add SSL and SASL PLAIN integration tests (Phase 6)"`

---

## Verification

1. `cargo build` — compiles without errors
2. `cargo test` — all unit tests pass (no Docker needed)
3. `cargo xtask format-check` — properly formatted
4. `cargo xtask lint` — no clippy warnings
5. `cargo test --features integration-tests` — all 16 integration tests pass (11 existing + 5 new)

---

## Risks and Mitigations

### 1. GraalVM native image lacks SASL
The `apache/kafka-native` image may not include full SASL support.
**Mitigation:** Use `apache/kafka` (JVM) image for all secure tests. Keep native image for PLAINTEXT.

### 2. Certificate hostname verification
Tests connect to `127.0.0.1` but cert may need `localhost` SANs.
**Mitigation:** Generate certs with SANs for both `localhost` and `127.0.0.1`. Also disable hostname verification in test `SslConfig` (`endpoint_identification_algorithm: ""`).

### 3. JAAS config mounting
SASL requires a JAAS config file inside the container.
**Mitigation:** Use testcontainers `with_copy_to()` API (confirmed available in v0.27) to write JAAS file before container start.

### 4. SSL keystore format
Kafka traditionally uses JKS/PKCS12 keystores.
**Mitigation:** Use PEM format (`ssl.keystore.type=PEM`) with inline PEM env vars. Kafka 4.2 supports this natively, avoiding keystore complexity.

### 5. Dynamic port mapping
Multiple listeners need correct host-mapped ports in `KAFKA_ADVERTISED_LISTENERS`.
**Mitigation:** `exec_after_start()` receives `ContainerState` with `host_port_ipv4()` for all exposed ports. Write all listener addresses into the start script dynamically.

### 6. Container startup time
SSL/SASL containers (JVM) are slower than native PLAINTEXT.
**Mitigation:** Use `ClusterPool` (keyed by `ClusterConfig` including `SecurityMode`) to share containers across tests with the same security mode.

---

## Files Changed

| File | Action |
|---|---|
| `Cargo.toml` | Modify (add rcgen dev-dep) |
| `tests/common/test_certs.rs` | **Create** |
| `tests/common/cluster_config.rs` | Modify (add SecurityMode) |
| `tests/common/mod.rs` | Modify (add test_certs module) |
| `tests/common/kafka_cluster.rs` | Modify (major: SecureKafka image) |
| `tests/common/test_context.rs` | Modify (add accessors) |
| `tests/integration/ssl_sasl_test.rs` | **Create** |
| `tests/integration/main.rs` | Modify (add module) |

## Dependencies

| Dependency | Version | Section | Purpose |
|---|---|---|---|
| `rcgen` | `0.13` | `[dev-dependencies]` | Self-signed certificate generation |

No changes to `[dependencies]` — all production code is already in place from Phases 1-5.

## Java Source Reference

- `kafka/clients/src/test/java/org/apache/kafka/common/network/SaslChannelBuilderTest.java`
- Integration test patterns from `kafka/clients/src/test/java/org/apache/kafka/common/network/SslTransportLayerTest.java`
