# Phase 9 — SASL/PLAIN + SASL_SSL (Milestone-1 closer)

**Goal:** Enable `security.protocol = SASL_SSL` with `sasl.mechanism = PLAIN` so the producer can connect to CCloud and real brokers requiring authentication. Scope is intentionally narrow: PLAIN mechanism only, no SCRAM, no Kerberos, no OAUTHBEARER. Also closes Phase 8's deferred 8e (TLS happy path) and 8f (3-consecutive-run flakiness gate) by folding them into this phase's test matrix.

**Plan reference:** `design/history/Milestone-1/PLAN.md:353-393`.

**Primary code reference:** `master` branch — has a working implementation of exactly this scope. Read it via `git show master:<path>` and copy patterns where Java parity allows. **Do not git-checkout master files wholesale** — `fresh-impl` has diverged in architecture (309 commits ahead), so a literal copy will compile-fail.

**Master files to reference (most useful):**
- `tests/integration/ssl_sasl_test.rs` — the 5-case test suite
- `src/common/network/sasl_channel_builder.rs`
- `src/common/security/authenticator/sasl_client_authenticator.rs`
- `src/common/requests/sasl_{handshake,authenticate}_{request,response}.rs`
- `generator/messages/Sasl{Handshake,Authenticate}{Request,Response}.json`
- `.claude/agent-memory/actor-executor/sasl_plain_auth_flow.md`
- `.claude/agent-memory/kafka-critic/review_sasl_authenticator_patterns.md`
- `design/history/Milestone-3/SSL_SASL_PLAN.md`

## Java classes to translate

Per PLAN.md:359-362:
- `common/network/SaslChannelBuilder.java` — creates a `KafkaChannel` with either plaintext or TLS transport + `SaslClientAuthenticator`
- `common/security/authenticator/SaslClientAuthenticator.java` — PLAIN-only state machine: `SendApiVersionsRequest → ReceiveApiVersionsResponse → SendHandshakeRequest → ReceiveHandshakeResponse → SendPlainToken → ReceiveResponse → Complete`
- `common/security/ssl/SslFactory.java` / `DefaultSslEngineFactory.java` — already partially covered by `SslChannelBuilder` in Phase 5; extend to load CA cert from `ssl.ca.location` env-style config

Plus the four message types (`SaslHandshakeRequest/Response`, `SaslAuthenticateRequest/Response`) generated from existing JSON specs in `generator/messages/`.

## Skip explicitly

Per PLAN.md:364-367:
- SCRAM, OAUTHBEARER, Kerberos/GSSAPI — reject with `ConfigError("Unsupported SASL mechanism: …")`
- Server-side: `KafkaPrincipal`, `KafkaPrincipalBuilder`, `LoginManager`, JAAS server contexts
- Re-authentication (methods exist on the `Authenticator` trait as no-ops; leave as-is)

## Config changes (PLAN.md:373-376)

- `ProducerConfig` / `KafkaProducer::new` must accept `SASL_PLAINTEXT` and `SASL_SSL` in addition to `PLAINTEXT` and `SSL` (remove the Phase 7 rejection for SASL_*)
- Accept `sasl.mechanism` (default `PLAIN`), `sasl.jaas.config` or separate `sasl.username` / `sasl.password` keys
- `SecurityProtocol` enum extended with `SaslPlaintext` and `SaslSsl` variants

## Sub-phase ladder

| # | Scope | Test entry point |
|---|---|---|
| 9.0 | **Generator + wire-protocol prerequisite.** Generate `SaslHandshake{Request,Response}` and `SaslAuthenticate{Request,Response}` from `generator/messages/*.json`. Verify per-field `flexibleVersions` handling against `master`'s generated output and Java client byte fixtures (CLAUDE.md "wire-protocol byte-vector divergence" risk #1). | lib-level round-trip + Java hex fixture |
| 9a | **`SaslChannelBuilder` + `SaslClientAuthenticator` PLAIN state machine** (Java parity, no integration test yet). Plug into existing `channel_builders.rs` registry alongside `PlaintextChannelBuilder` and `SslChannelBuilder`. Unit tests for the state machine using mocked transport. | lib tests |
| 9b | **Config: accept `SASL_PLAINTEXT` / `SASL_SSL` in `SecurityProtocol`**, parse `sasl.mechanism` / `sasl.jaas.config` / `sasl.username` / `sasl.password`. Reject SCRAM / OAUTHBEARER / Kerberos at config-validation time with the same error message Java emits. | unit tests for `ProducerConfig` validation |
| 9c | **Integration test 1: SSL connection (TLS-only, self-signed cert via `rcgen`).** This is the deferred Phase 8e work. Same producer-smoke flow as PLAINTEXT, but over the broker's 9096 SSL listener. Uses `tests/common/test_certs.rs`. | `tests/integration/ssl_sasl_test.rs` (or extend `producer_smoke_test.rs` — pick one consistently) |
| 9d | **Integration test 2: SASL_PLAINTEXT + PLAIN credentials.** Producer-side smoke flow over SASL_PLAINTEXT listener (typically 9094 in the test container). Asserts ack-count + shape (same as 8a). | extends 9c |
| 9e | **Integration test 3: SASL_SSL (TLS + PLAIN).** Combined transport. Producer-side smoke flow over SASL_SSL listener (typically 9095). | extends 9d |
| 9f | **Integration test 4: auth failure with wrong credentials → `KafkaError::Authentication`.** Asserts the error message matches Java's. | extends 9e |
| 9g | **Integration test 5: unsupported mechanism → `UnsupportedSaslMechanismException` equivalent.** Asserts validator rejects `sasl.mechanism = SCRAM-SHA-512` (or similar) at construction time. | unit test against `ProducerConfig` |
| 9h | **Flakiness gate (folded-in Phase 8f).** Run the **full integration matrix** (PLAINTEXT 5-test suite from Phase 8a-c + cases 1-5 from 9c-9g) **3 consecutive times** under `cargo test --features integration-tests`. Address flakes (cold-start, slow leader-election, cert-load latency). Strictly more rigorous than 8f's PLAINTEXT+SSL gate. | CI loop check |
| 9i | **(Optional) CCloud smoke test.** Wire `tests/integration/performance_test.rs` (already env-var-aware from `dev/milestone-5`) to accept `SECURITY_PROTOCOL`, `SASL_MECHANISM`, `SASL_USERNAME`, `SASL_PASSWORD`, `SSL_CA_LOCATION`. Test skips if `SASL_USERNAME` unset — runs only when EC2/CCloud env is configured. | env-var-gated; non-blocking for Milestone-1 close |

Each sub-phase ends green on:
```
cargo build --features integration-tests
cargo test                                                              # lib + unit
cargo test --features integration-tests                                 # integration (needs Docker)
cargo xtask format-check
cargo xtask lint
```

## DoD additions on top of `definition-of-done.md` (PLAN.md:389-392)

1. Auth failure surfaces as `KafkaError::Authentication` with a message matching Java's error string.
2. SASL handshake sends correct mechanism name in `SaslHandshakeRequest`; token format matches RFC 4616 (`\0username\0password`).
3. All 5 integration test cases green against a Testcontainers broker with SASL configured.
4. **Folded-in 8f gate:** the full PLAINTEXT-through-SASL_SSL test matrix passes **3 consecutive times**.
5. **Folded-in 8d audit (lib-level only):** confirm Phase 3's codec round-trip + hex-fixture tests are still green after Phase 9 lands (no codec regression). End-to-end compression matrix is explicitly deferred to a future milestone.

## Approved phase-level decisions (Manager + user, 2026-05-21)

1. **Skip Phase 8d standalone** — compression matrix deferred. Lib-level codec tests in Phase 3 are sole coverage.
2. **Fold Phase 8e into 9c** — TLS happy path produced as part of Phase 9's case 1 instead of standalone.
3. **Fold Phase 8f into 9h** — flakiness gate runs the full matrix (PLAINTEXT + SSL + SASL_PLAINTEXT + SASL_SSL + failure cases) instead of just PLAINTEXT + SSL.
4. **Master-branch reference is primary** alongside Java source — see "Primary code reference" above. `fresh-impl` divergence rules out wholesale checkout; copy patterns where they fit.
5. **Test container topology:** assume a single broker exposing 4 listeners (PLAINTEXT 9092, SSL 9096, SASL_PLAINTEXT 9094, SASL_SSL 9095). Confirm against `tests/common/kafka_cluster.rs` / `cluster_config.rs` before 9c. If the existing scaffolding doesn't support multi-listener, extending it is in-scope for 9c — see master's `kafka_cluster.rs` for the pattern.

## Skip list (rejected or deferred)

- **SCRAM, OAUTHBEARER, Kerberos/GSSAPI** — reject at config validation (per PLAN.md:365)
- **Re-authentication** — Authenticator trait no-op methods retained (per PLAN.md:367)
- **Server-side principal classes** — out of client scope (per PLAN.md:366)
- **`KafkaPrincipal` / `LoginManager`** — out of scope (per PLAN.md:366)
- **End-to-end compression matrix integration test** — deferred to future milestone (Phase 8d carryforward)
- **`MockProducer`, `KafkaConsumer`** — out of Milestone-1 entirely

## Phase-7 / Phase-8 carry-overs retired here

- **Phase 8e (TLS happy path)** — folded into sub-phase 9c.
- **Phase 8f (flakiness gate + `performance_test.rs` compile-only gate)** — flakiness gate folded into sub-phase 9h; `performance_test.rs` compile-only gate already resolved in Phase 8a.1 (commit `eb0f890`), so the only remaining work is the 3-run loop in 9h.

## Open Phase-8 followups bundled here (from `COMMENTS.8.md`)

- **Phase 8c Round 1 Suggestion 1** — drop `Arc::try_unwrap` ceremony in `tests/integration/producer_smoke_test.rs:~1280-1288`. Bundle with 9c (the next touch on the integration test file).
- **Phase 8c Round 1 Nit 1** — extend the `consume_records` helper rustdoc to document `\n` (0x0A) collision risk alongside `\x1F` separator collision. Bundle with 9c.
- **Phase 8c Round 1 Nit 2** — commit `bc0508d` message attributes the doc-grouping fix to a clippy lint that did not fire. No source-code change; no fixup commit needed unless the Actor's captured output names a different lint. Leave as-is; archived for record.

## Comment files

- Open: `design/history/Milestone-1/Phase-9/COMMENTS.9.md` (will be created by Critic 9 on first review — gitignored working file)
- Resolved: `design/history/Milestone-1/Phase-9/COMMENTS.DONE.9.md` (will be created by Manager on first close — tracked archive)

## Workflow (Manager loop)

1. Plan approved (user, 2026-05-21 — pre-approved at PLAN.md draft time + sub-phase ladder above).
2. This NOTES.md created.
3. Spawn **Actor 9** for sub-phase 9.0 (generator + wire-protocol prerequisite).
4. Spawn **Critic 9** to review 9.0 commits → comments to `COMMENTS.9.md`.
5. Loop fixup-and-review until `COMMENTS.9.md` is empty for 9.0, then proceed to 9a.
6. Repeat steps 3-5 for 9a through 9h. 9i is optional (env-var-gated, non-blocking).
7. Phase 9 closes (= Milestone-1 closes) when 9h's 3-consecutive-run gate is green and all comment files are resolved.

Agent number for this phase: **N = 9**.

## Risks specific to this phase

| Risk | Mitigation |
|---|---|
| Wire-protocol byte-vector divergence in SASL frames (PLAN.md risk #1) | Capture hex fixtures from Java client for `SaslHandshakeRequest/Response` and `SaslAuthenticateRequest/Response`. Assert bytes literally in 9.0. Do not rely on round-trip alone. |
| RFC 4616 token format off-by-one | `\0username\0password` is one literal NUL byte before username, one between, no trailing NUL. Master has the working pattern — verify against it. |
| Multi-listener Testcontainer setup brittleness | Reuse master's `kafka_cluster.rs` listener config if `fresh-impl`'s current scaffolding doesn't support it. Add a cluster-pool variant for SASL-enabled brokers rather than mutating the PLAINTEXT pool (which Phase 8 tests rely on). |
| Auth-failure error message divergence | Pin the exact Java error string in a test assertion. PLAN.md DoD #1. |
| SSL_SSL handshake ordering: TLS handshake must complete *before* SASL handshake begins | Java's `SslTransportLayer.handshake()` → then `SaslClientAuthenticator.authenticate()`. Order matters; verify via wireshark or master's flow. |
| `sasl.jaas.config` parsing edge cases (semicolons, escaped quotes) | Use a constrained parser — username/password key=value pairs only. Reject malformed JAAS configs at validation time rather than at authenticate time. |
