# Phase 9 — SASL/PLAIN + SASL_SSL (Milestone-1 closer)

**Goal:** Enable `security.protocol = SASL_SSL` with `sasl.mechanism = PLAIN` so the producer can connect to CCloud and real brokers requiring authentication. Scope is intentionally narrow: PLAIN mechanism only, no SCRAM, no Kerberos, no OAUTHBEARER. Also closes Phase 8's deferred 8e (TLS happy path) and 8f (3-consecutive-run flakiness gate) by folding them into this phase's test matrix.

**Plan reference:** `design/history/Milestone-1/PLAN.md:353-393`.

**Code reference policy (user directive, 2026-05-21):** translate **fresh, from Java source only**. **Do NOT reference the `master` branch's prior implementation.** This explicitly overrides PLAN.md:357's "use master as primary reference alongside Java source" — user wants Java-parity issues caught fresh without inheriting master's potential bugs.

Allowed reading scope for Actor 9 and Critic 9:
- Java source in `kafka/` (Apache Kafka 4.2, read-only)
- The current `fresh-impl` tree (existing translated classes, generator infrastructure, channel builder registry, etc.)
- JSON message specs in `generator/messages/` on this branch

Forbidden:
- `git show master:…`, `git log master`, `git diff master…fresh-impl`, browsing master files in any tool
- Master-branch agent memory files (`.claude/agent-memory/*/sasl_*.md` if any were ever ported in — must not be consulted)
- Any "this is how master does it" hint in this NOTES.md (none present — see directive above)

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
| 9.0 | **Generator + wire-protocol prerequisite.** Generate `SaslHandshake{Request,Response}` and `SaslAuthenticate{Request,Response}` from `generator/messages/*.json`. Verify per-field `flexibleVersions` handling against Java client byte fixtures captured from running the Java client (CLAUDE.md "wire-protocol byte-vector divergence" risk #1). | lib-level round-trip + Java hex fixture |
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
4. **Java-only translation, no master-branch reuse** — see "Code reference policy" above. Overrides PLAN.md:357.
5. **Test container topology:** assume a single broker exposing 4 listeners (PLAINTEXT 9092, SSL 9096, SASL_PLAINTEXT 9094, SASL_SSL 9095). Confirm against `tests/common/kafka_cluster.rs` / `cluster_config.rs` before 9c. If the existing scaffolding on this branch doesn't support multi-listener, extending it is in-scope for 9c — translate the listener-config pattern from `kafka/core/src/test/scala/integration/kafka/server/IntegrationTestHarness.scala` and `kafka/clients/src/test/java/org/apache/kafka/common/network/NetworkTestUtils.java` (Java sources only).

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
| RFC 4616 token format off-by-one | `\0username\0password` is one literal NUL byte before username, one between, no trailing NUL. Verify against RFC 4616 directly + Java `PlainSaslClient.evaluateChallenge()` source. |
| Multi-listener Testcontainer setup brittleness | If `fresh-impl`'s current scaffolding doesn't support multi-listener, extend it by translating Java's `IntegrationTestHarness.scala` / `NetworkTestUtils.java` listener-config pattern. Add a cluster-pool variant for SASL-enabled brokers rather than mutating the PLAINTEXT pool (which Phase 8 tests rely on). |
| Auth-failure error message divergence | Pin the exact Java error string in a test assertion. PLAN.md DoD #1. |
| SSL_SSL handshake ordering: TLS handshake must complete *before* SASL handshake begins | Java's `SslTransportLayer.handshake()` → then `SaslClientAuthenticator.authenticate()`. Order matters; verify via wireshark capture against a running Java client + reading the Java source flow. |
| `sasl.jaas.config` parsing edge cases (semicolons, escaped quotes) | Use a constrained parser — username/password key=value pairs only. Reject malformed JAAS configs at validation time rather than at authenticate time. |

## Sub-phase 9.0 — closed (Round 1)

**Closed pending Critic 9 Round 1.** Four commits land the
generator + wire-protocol prerequisite from the sub-phase ladder
above. The generator was already producing the 4 SASL `*_data.rs`
files from `generator/messages/*.json` via `build.rs`; the Actor
discovered no generator changes were needed — only hand-written
wrappers + tests.

- `9bcf845` — **Phase 9.0 (1/N):
  `SaslHandshake/SaslAuthenticate` request+response wrappers.**
  Adds 4 wrapper files in `src/common/requests/` patterned after
  the existing `ApiVersionsRequest/Response` wrappers:
  `AbstractRequest` / `AbstractResponse` trait impls,
  `OnceLock<&'static ApiKey>`-cached `api_key()`,
  `AbstractRequestBuilder` impls (`Handshake`, `Authenticate`),
  convenience accessors (`mechanism()`, `auth_bytes()`,
  `error()`, etc.), and Debug masking for the credential-carrying
  `SaslAuthenticateRequest/Response` types (translation of Java's
  `toString()` override). Also wires the 4 generated `*_data`
  modules into `common::message::mod.rs` and extends
  `abstract_response::parse_response_body()` to dispatch api keys
  17 and 36 so future NetworkClient SASL traffic decodes through
  the existing path.
- `73d9ff4` — **Phase 9.0 (2/N): SASL wire-type round-trip lib
  tests.** Adds 28 lib-level round-trip tests across the 4 SASL
  wrappers covering each supported wire version plus structural
  invariants (every version 0/1/2, flex-boundary on both sides,
  empty payloads, tagged-field round-trip at v2, tagged-field
  rejection at v0/v1, Debug-masking parity from Java
  `testSaslAuthenticateRequestResponseToStringMasksSensitiveData`,
  multi-mechanism response arrays). Test count: 1233 → 1261.
- `884c8c1` — **Phase 9.0 (3/N): SASL wire-type Java hex-fixture
  byte-vector tests.** Adds 14 literal-byte fixtures covering each
  version of all 4 SASL wrappers — pins CLAUDE.md Risk #1
  (wire-protocol byte-vector divergence). Per-fixture rustdoc
  carries a byte-by-byte layout breakdown. Flex-boundary fixtures
  (v2 on both `SaslAuthenticate{Request,Response}`) are the
  strongest defense against silent generator drift at the
  per-field `flexibleVersions: "2+"` override. Test count:
  1261 → 1275.
- HEAD (this commit) — **Phase 9.0 (4/N): sub-phase 9.0 close
  stanza in NOTES.md.**

**Fixture provenance note (for Critic 9 review).** All 14 hex
fixtures in commit 3 are **hand-derived** from the JSON specs
against the documented Kafka wire-protocol encoding rules (i16/i32
length-prefix at non-flex; varint compact-bytes + varint
tagged-field trailer at flex). The rustdoc on each fixture
includes an `awaiting Java-runtime byte capture` note — once a
real Java 4.2 client is wired (likely 9c, when Testcontainers is
up with a SASL listener), the fixtures should be cross-verified
by tcpdump/wireshark capture or by piping a `KafkaProducer`
configured for SASL through a recording proxy. The hand-derived
encodings did successfully round-trip through the generator's
read+write paths and matched the generator's `serialize()` output
byte-for-byte, which validates the generator-vs-spec alignment at
minimum; the residual risk is that *both* the generator and the
hand-derivation share a common misinterpretation of the spec.
Mitigation: cross-verify in 9c (PLAN.md Risk #1 carry-over).

**Decisions made by Actor 9 inside the brief:**
1. Did **not** introduce a `KafkaError::UnsupportedSaslMechanism`
   variant (suggested by the brief). Per CLAUDE.md rule 10.3, the
   existing `KafkaError::Authentication(String)` already maps to
   wire code 58 (`SASL_AUTHENTICATION_FAILED`) and is the broader
   Java-`AuthenticationException`-equivalent which covers the
   mechanism-rejection error path (broker returns code 33,
   `UNSUPPORTED_SASL_MECHANISM`, but the client surfaces it as
   `AuthenticationException` to user callbacks). Keeping the
   error mapping at wire-code precision (`Errors::Unsupported
   SaslMechanism = 33` in `protocol/errors.rs`) without a
   dedicated `KafkaError` variant is consistent with how other
   broker-side error codes (e.g. `IllegalSaslState = 34`) are
   handled. If 9b's `ProducerConfig` validator needs a more
   specific variant, that can be added then.
2. Extended `parse_response_body()` for api keys 17 and 36 in this
   sub-phase (not deferred to 9a) — the change is mechanical and
   keeps 9a focused on `SaslChannelBuilder` / state machine work.
3. Added a `SaslHandshakeRequestBuilder` and a
   `SaslAuthenticateRequestBuilder` as Java parity (Java has both
   `public static class Builder`). Both implement
   `AbstractRequestBuilder` with `oldest_allowed_version` /
   `latest_allowed_version` from the `ApiKey` registry.

**Deferred (in scope for 9a+):**
- `SaslChannelBuilder` + `SaslClientAuthenticator` PLAIN state
  machine (sub-phase 9a).
- Java-runtime hex-fixture cross-verification (carry-over to 9c
  when Testcontainers is up with SASL listeners).
- `KafkaError::UnsupportedSaslMechanism` variant if 9b config
  validation requires it.

**Java tests intentionally not translated** (DoD #3 — Critic 9
Nit 2):
- `RequestResponseTest.testInvalidSaslHandShakeRequest`
  (`RequestResponseTest.java:3898-3908`)
- `RequestResponseTest.testInvalidSaslAuthenticateRequest`
  (`RequestResponseTest.java:3911-3930`)
- `RequestResponseTest.testInvalidTaggedFieldsWithSaslAuthenticateRequest`
  (`RequestResponseTest.java:3961-3984`)

All three exercise the underlying `Readable`/`ByteBufferAccessor`
corruption-error path, *using* SaslHandshake/SaslAuthenticate
purely as transport. Equivalent Rust coverage exists at the
codec level in `src/common/protocol/byte_buffer_accessor.rs:380-410`
(short-read, malformed varint, truncated payload) and
`src/common/protocol/types/type.rs:433-525` (per-type read
boundary checks). Re-doing this test through the SASL types
would not exercise any SASL-specific code path beyond what the
existing round-trip and hex-fixture tests already cover.

**Status at close:** `cargo build` OK, `cargo xtask format-check`
OK, `cargo xtask lint` OK, `cargo test --lib` 1275 passed (+42
versus the Phase 8 close baseline of 1233:
28 round-trip + 14 hex-fixture).
`cargo test --features integration-tests` not required for 9.0
(no integration tests added; per the brief).

## Sub-phase 9a — closed (Round 1)

**Closed pending Critic 9 Round 1.** Four commits land
`SaslChannelBuilder` + `SaslClientAuthenticator` PLAIN state machine
plus the four Phase 9.0 Round-1 followups.

- `a8bd3f8` — **Phase 9a (1/N): pre-impl gate — redact credential
  bytes in generator-level Debug/Display.** Resolves the lead-priority
  Phase 9.0 Round 1 Suggestion 1 (generator-level credential-leak
  vector). Adds `is_credential_field_name`, `has_credential_field`,
  `generate_redacted_debug_impl` to the generator: when a struct has
  any field named `AuthBytes` / `auth_bytes`, the generator skips
  `Debug` from the derive list and emits a hand-written `impl Debug`
  that renders the field as `<redacted>` instead of the raw bytes.
  `Display` continues to delegate to `Debug` and inherits the
  redaction. 4 new generator unit tests pin the behaviour; 2 new lib
  tests in `src/common/requests/sasl_authenticate_*.rs` exercise the
  data-class formatting end-to-end. Suggestion 2 bundled: the
  `debug_masks_auth_bytes` test panic messages no longer echo the
  full Debug output on failure. Test count: 1275 → 1277.
- `73efc50` — **Phase 9a (2/N): bundle 9.0 round-1 followups
  (Suggestions 3, 4 + Nits 1, 2).** Suggestion 3:
  `success_response_error_counts_includes_none` on both
  `SaslHandshakeResponse` and `SaslAuthenticateResponse` test
  modules (translation of Java's
  `testErrorCountsIncludesNone`). Nit 1: tighten the SaslHandshake
  `error_counts_via_get_error_response_v1` from `>= 1` to `== 1` plus
  the explicit `SaslAuthenticationFailed` value check. Suggestion 4:
  rewrite `parse_response_body` error string at `abstract_response.rs`
  as a static list of supported API keys, no phase-number references.
  Nit 2: enumerate the 3 intentionally skipped Java tests
  (`testInvalid*SaslHandshakeRequest`, `testInvalid*SaslAuthenticateRequest`,
  `testInvalidTaggedFieldsWithSaslAuthenticateRequest` from
  `RequestResponseTest.java:3898-3984`) in the Phase 9.0 close stanza
  above. Test count: 1277 → 1279.
- `a596c36` — **Phase 9a (3/N): SaslClientAuthenticator PLAIN state
  machine** (Java parity, unit tests with mocked transport). Core
  deliverable. New module `src/common/security/authenticator/` —
  mirrors Java's
  `org.apache.kafka.common.security.authenticator`. The
  `SaslClientAuthenticator` is a sync, non-blocking state machine
  (CLAUDE.md rule 9.1) driven by repeated `authenticate()` calls
  from the upper layer's poll loop, matching Java's pattern. States:
  `SendApiVersionsRequest → ReceiveApiVersionsResponse →
  SendHandshakeRequest → ReceiveHandshakeResponse → SendInitialToken
  → ReceiveAuthenticateResponse → Complete | Failed`. Re-authentication
  states from Java are deferred to a future milestone (PLAN.md:367).
  `PlainCredentials` struct replaces Java's `Subject` + JAAS
  indirection. `MIN/MAX_RESERVED_CORRELATION_ID` + `is_reserved()`
  translated verbatim. Legacy `DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER`
  branch preserved for pre-1.0 brokers. Authentication failures
  surface as `KafkaError::Authentication` (fatal, non-retriable —
  matches Java semantics). Hand-emitted Debug on both
  `PlainCredentials` and `SaslClientAuthenticator` masks the
  password and the in-flight send buffer. 10 unit tests covering:
  full PLAIN happy path, unsupported mechanism / auth-failure error
  paths with Java-parity error strings, EOF mid-handshake,
  construction-time mechanism rejection, correlation-id reserved
  range, RFC 4616 byte shape, tagged-field v2 round-trip, credential
  Debug redaction. Test count: 1279 → 1289.
- `aff279a` — **Phase 9a (4/N): SaslChannelBuilder +
  SecurityProtocol::SaslPlaintext/SaslSsl variants.** Plugs the
  SASL channel builder into `channel_builders.rs` dispatch
  alongside `PlaintextChannelBuilder` and `SslChannelBuilder`.
  Extends `SecurityProtocol` with the SASL variants (id 2 / 3) +
  `is_sasl()` / `uses_ssl()` predicates. New
  `src/common/network/sasl_channel_builder.rs` validates
  construction (must be SASL protocol, must be PLAIN mechanism,
  `ssl_config` required for `SaslSsl`). `build_channel()` currently
  returns `KafkaError::UnsupportedOperation` — the Phase 5b-3
  `Authenticator` trait does not thread a transport reference
  through `authenticate()`, and reshaping it to host the SASL
  authenticator is intentionally deferred to Phase 9b. Per
  CLAUDE.md rule 5: fail with `KafkaError`, not silent skip. 4 new
  channel-builders dispatch tests + 5 new SaslChannelBuilder
  construction tests. The producer-side gate at
  `kafka_producer.rs:660` still rejects non-PLAINTEXT — user-visible
  behaviour for `KafkaProducer::new` is unchanged. Test count:
  1289 → 1299.

**Phase 9a deferrals (carried into 9b+):**
- **Channel-side SASL wiring.** `SaslChannelBuilder::build_channel`
  returns `UnsupportedOperation`. Phase 9b must extend the Phase
  5b-3 `Authenticator` trait (or introduce a SASL-specific
  authenticator wrapper) to thread the transport reference
  through `authenticate()`. The `SaslClientAuthenticator` state
  machine itself is complete + unit-tested.
- **`ProducerConfig` acceptance of `SASL_PLAINTEXT` / `SASL_SSL`
  strings + `sasl.mechanism` / `sasl.username` / `sasl.password`
  parsing.** Phase 9b. Currently `kafka_producer.rs:660` still
  rejects non-PLAINTEXT via the existing
  `KafkaError::UnsupportedOperation` gate.
- **JAAS config parsing.** Phase 9b. The current
  `PlainCredentials::new(username, password)` accepts the typed
  fields directly; the typed `ProducerConfig`-to-`PlainCredentials`
  bridge lands in 9b.
- **Re-authentication.** Out of Milestone 1 (PLAN.md:367). The
  `Authenticator` trait keeps the no-op `reauthenticate` method
  from Phase 5b-3.
- **Java-runtime hex-fixture cross-verification.** Still 9c
  (when Testcontainers is up with SASL listeners).

**Java tests intentionally not translated** (DoD #3):
- `SaslAuthenticatorTest.*` — Java's full server+client integration
  suite. Out of scope for 9a (which mocks transport at the
  read/write boundary). The Phase 9c-g integration tests
  cover real broker exchange.
- `SaslAuthenticatorFailure{Delay,PositiveDelay,NoDelay}Test.java`
  — broker-side timed-failure delay tests. Server-side concern;
  client cannot observe the timing semantics directly.
- `ClientAuthenticationFailureTest.java` — server-side test
  fixture wrapping `NetworkClient`. The 9a unit tests for
  `authenticate_failure_preserves_broker_message` cover the
  client-side surface.
- `LoginManagerTest.java`, `TestJaasConfig.java`,
  `TestDigestLoginModule.java` — JAAS / DIGEST-MD5 plumbing,
  out of Phase 9a scope (PLAIN only).
- `SaslServerAuthenticatorTest.java` — server-side,
  out of Milestone 1 client scope.

**Decisions made by Actor 9 inside the brief:**
1. Implemented option (a) from Suggestion 1 — generator-level
   redaction. Not invasive: 3 new helpers totalling ~80 lines in
   `generator/src/lib.rs`, 3 single-line wirings into the existing
   struct-emit call sites, and a small change to the existing
   `generate_struct_derives_and_impls` to conditionally drop
   `Debug` from the derive list. Cleanest separation of concerns
   — the generator owns both the wire format and the safe
   Debug/Display.
2. Implemented the state machine as a sync (non-blocking) loop
   matching Java's structure, rather than rewriting to async.
   Rationale: the upper layer (`KafkaChannel::prepare`) drives
   `Authenticator::authenticate()` from a sync trait already; an
   async-fn in the trait would touch every call site. The Phase
   5b transport's `read()` returns `Ok(0)` on `WouldBlock`, so
   the sync state machine composes correctly with the existing
   poll-loop pattern.
3. Did **not** introduce `KafkaError::UnsupportedSaslMechanism` —
   continued to use `KafkaError::Authentication` per the Phase
   9.0 decision (memory `phase9_0_sasl_wire_types.md`).
   `KafkaError::Config` covers the construction-time validation
   path (different surface from runtime auth failure).
4. `SaslChannelBuilder::build_channel` returns
   `UnsupportedOperation` rather than partially wiring through
   a fake authenticator. The `SaslClientAuthenticator` is fully
   testable directly via `SaslClientAuthenticator::new` (the unit
   tests do exactly that). Phase 9b will finish the channel-side
   integration once the `Authenticator` trait is reshaped.
5. Added `is_sasl()` and `uses_ssl()` predicates on
   `SecurityProtocol` — they mirror Java's idiom
   (`securityProtocol == SASL_PLAINTEXT || == SASL_SSL`) used
   in multiple call sites in Java. Anticipates 9b/9c usage.

**Status at close:** `cargo build` OK, `cargo xtask format-check`
OK, `cargo xtask lint` OK, `cargo test --lib` 1299 passed (+24
versus the Phase 9.0 close baseline of 1275: 2 generator-level
redaction lib tests + 2 followup test assertions + 10 SASL
authenticator state-machine tests + 6 SecurityProtocol expansion
tests + 4 channel-builders dispatch tests).
`cargo test --features integration-tests` not required for 9a
(no integration tests added; integration begins at 9c per the
brief).
