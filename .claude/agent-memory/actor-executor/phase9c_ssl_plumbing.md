---
name: phase9c-ssl-plumbing
description: Phase 9c producer-side SSL / SASL_SSL enablement — config plumbing, SNI propagation, gate lift, integration test patterns
metadata:
  type: project
---

# Phase 9c — producer-side SSL / SASL_SSL closure (Milestone-1)

**Status:** Closed Round 1, awaiting Critic 9.

## What landed

1. **`src/common/security/ssl/mod.rs`** — `pub(crate) fn build_client_config_from_producer_config(&ProducerConfig) -> Result<Arc<rustls::ClientConfig>, KafkaError>`. Reads `ssl.truststore.location|.type` (PEM only, JKS/PKCS12 rejected), `ssl.endpoint.identification.algorithm` ("https" default; "" disables via `NoHostnameVerifier` wrapping `WebPkiServerVerifier`), optional mTLS keystore trio.

2. **SNI plumbing through `Selectable::connect(host: &str, ...)`** — Selector stashes the host in `connection_hosts: HashMap<ConnectionId, String>`, cleaned up on all 5 disposal paths. `ChannelBuilder` trait grew `build_channel_with_server_name(server_name: Option<ServerName<'static>>)` with default that ignores; SSL/SASL_SSL builders override.

3. **Producer-side gate lift** in `KafkaProducer::build_production_network_client` — `security_protocol.uses_ssl()` triggers SSL config build; works for `Ssl` and `SaslSsl`.

4. **Integration test** `producer_smoke_ssl_1000_records` in `tests/integration/producer_smoke_test.rs` — mirrors PLAINTEXT 1000-records, swaps in `ctx.ssl_bootstrap_servers()` + truststore tempfile from `ctx.ca_cert_pem()`.

## Key design calls (carry-forward for 9d/9e)

- **`NoHostnameVerifier` chain validation**: wraps `WebPkiServerVerifier` with a static placeholder hostname `"invalid.example"`; only `CertificateError::NotValidForName{,Context}` errors are translated to success. All other chain-validation errors propagate.
- **Trait-method default vs typed entry point**: `build_channel_with_server_name` is a NEW method with default impl that ignores `server_name`; SSL builder override REQUIRES `Some` (else `IllegalState`); SASL builder dispatches by inner protocol. Pre-existing `build_ssl_channel` / `build_sasl_ssl_channel` typed entry points kept and called by the override.
- **SNI host stashed in separate HashMap**, not on `ConnectTask`: the host is needed AFTER the connect task is removed (at `build_and_register_channel` time).
- **Raw-IP hosts**: `ServerName::try_from("127.0.0.1")` returns `Ok(IpAddress)`, not `Err`. The SSL builders reject `None`, so the only loud-rejection case is unparseable hosts.

## Test-side patterns

- **Self-signed truststore in tests**: use `rcgen::CertificateParams::default()` + `KeyPair::generate()` + `params.self_signed(&key).pem()`. Write to `tempfile::NamedTempFile` and **keep the binding alive** for the entire test scope (Drop closes the file).
- **Capturing test builder for SNI propagation**: stub `ChannelBuilder` that overrides `build_channel_with_server_name` to record `Option<ServerName>`, AND overrides `build_channel` to panic if reached — proves the selector ALWAYS calls the SNI variant.
- **Tagged-field parser-preservation**: re-parse framed bytes standalone via `ResponseHeader::parse(accessor, header_v)` + `*ResponseData::read(accessor, api_v)`, then assert `unknown_tagged_fields[i].tag()` and `.data()`. The in-band authenticator test path doesn't expose its parsed response, so standalone re-parse is the only way to pin parser preservation.

## Deferrals (carried into 9d+)

- SASL_PLAINTEXT integration test → 9d
- SASL_SSL integration test → 9e
- Auth-failure / unsupported-mechanism integration tests → 9f / 9g
- Flakiness gate (3-run loop over full matrix) → 9h
- CCloud env-var-gated smoke → 9i (optional)
- Re-authentication → out of Milestone 1
