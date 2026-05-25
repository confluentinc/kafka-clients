# Critic 9 — Phase 9 archived reviews

Resolved Phase 9 review entries land here as Manager closes each round.

---

# Critic 9 — Phase 9.0 review

Review window: commits `9bcf845..2c19ffe` on branch `fresh-impl`.

Java references consulted (all under `kafka/clients/src/main/java/org/apache/kafka/common/`):
- `requests/SaslHandshakeRequest.java`
- `requests/SaslHandshakeResponse.java`
- `requests/SaslAuthenticateRequest.java`
- `requests/SaslAuthenticateResponse.java`
- `requests/RequestResponseTest.java` (lines 976-977, 1002, 1021, 1097, 1116, 2563-2585, 3897-4006)
- Java-generated `message/SaslAuthenticateRequestData.java` + `SaslAuthenticateResponseData.java` (build/generated tree, to confirm Java's own data-class `toString()` behaviour)
- JSON specs in `generator/messages/Sasl{Handshake,Authenticate}{Request,Response}.json`

Master-branch consultation: **none**, per the user's 2026-05-21 Java-only directive. All wire-format verification is against the JSON specs + Apache Kafka protocol-guide encoding rules read directly from the Java reader/writer in the build-generated `*Data.java`.

Gate status (verified, not from Actor report):
- `cargo build` — clean
- `cargo test --lib` — 1275 passed, 0 failed
- `cargo xtask format-check` — clean
- `cargo xtask lint` — clean (`No lint issues found!`)
- `cargo build --features integration-tests` — clean

Verdict: **0 Blocking, 4 Suggestion, 2 Nit.**

## Per-area verifications

| Area | Result | Notes |
|---|---|---|
| Java parity: 4 wrapper classes | OK | All Java methods present with correct signatures (see below) |
| Builder pattern | OK | Both builders implement `AbstractRequestBuilder`; version ranges sourced from `ApiKey` registry which sources from generated `ApiMessageType` (JSON-derived) |
| `parse_response_body` dispatch | OK | arms 17→`SaslHandshakeResponse`, 36→`SaslAuthenticateResponse` — no swap, no off-by-one |
| Hex fixture wire-byte verification | OK (14/14 spot-checked) | All 14 hex fixtures verified by hand against JSON specs + Kafka protocol guide; flex-boundary v2 fixtures are the strongest test |
| Per-field `flexibleVersions` override compliance | OK | JSON specs have only message-level `"none"` (Handshake) and `"2+"` (Authenticate) — no per-field overrides to worry about |
| Nullable defaults (CLAUDE.md) | OK | `error_message` defaults to `Some(String::new())` (not `None`) per CLAUDE.md rule; generated code at line 63 of `sasl_authenticate_response_data.rs` confirms |
| Long fields as `i64` (CLAUDE.md) | OK | `session_lifetime_ms: i64` |
| Test coverage breakdown | OK | 28 round-trip + 14 hex fixtures = 42 new tests; covers all 6 distinct versions (Handshake v0/v1, Authenticate v0/v1/v2) on both req+resp sides, empty/default/max values, RFC 4616 PLAIN token shape |
| Credential redaction on wrappers | OK | Manual `impl fmt::Debug` for `SaslAuthenticateRequest` and `SaslAuthenticateResponse` masks `auth_bytes` as `<redacted>` |
| Credential leak via generated data classes | Suggestion 1 — generator-level | See Finding S1 |
| Hot-path allocation audit | N/A | SASL is not on the producer send path; skipped per DoD #10 |
| Error handling | OK | `parse_response_body` returns `Result`, no panics; `KafkaError::Authentication` is fatal and maps to wire code 58 |
| License headers | OK | Apache 2.0, Confluent Inc copyright |
| `unwrap`/`expect` use in production code | OK | `ApiKeys::for_id(17/36).expect(...)` only — these lookup a const registry guaranteed to contain those ids; cannot fail |
| Skipped Java tests | Acceptable + 1 small note | See Finding N2 |
| Round-trip byte-stability test | OK | `flexible_versions_none_no_tagged_trailer_v1` proves no extra trailer at v1 |
| Tagged-field reject on non-flex versions | OK | `tagged_fields_rejected_pre_flexible` asserts error path + message-content match |
| NOTES.md close stanza | OK | Accurate, flags hand-derived fixture provenance, defers Java-runtime cross-verification to 9c |

### Java method/accessor parity checklist

| Java method | Rust equivalent | Status |
|---|---|---|
| `SaslHandshakeRequest.Builder(SaslHandshakeRequestData)` | `SaslHandshakeRequestBuilder::new(data)` | OK |
| `SaslHandshakeRequest.data()` | `request_data()` | OK |
| `SaslHandshakeRequest.parse(Readable, short)` | `SaslHandshakeRequest::parse(accessor, version)` | OK |
| `SaslHandshakeRequest.getErrorResponse(int, Throwable)` | `get_error_response(throttle_time_ms, error)` | OK |
| `SaslHandshakeResponse.error() → Errors` | `error() -> Errors` | OK |
| `SaslHandshakeResponse.enabledMechanisms() → List<String>` | `enabled_mechanisms() -> &[String]` | OK |
| `SaslHandshakeResponse.throttleTimeMs() → 0 (DEFAULT_THROTTLE_TIME)` | `throttle_time_ms() -> DEFAULT_THROTTLE_TIME` | OK |
| `SaslHandshakeResponse.maybeSetThrottleTimeMs(int)` (no-op) | `maybe_set_throttle_time_ms(_)` (no-op) | OK |
| `SaslHandshakeResponse.parse(Readable, short)` | `SaslHandshakeResponse::parse(...)` | OK |
| `SaslAuthenticateRequest.Builder(SaslAuthenticateRequestData)` | `SaslAuthenticateRequestBuilder::new(data)` | OK |
| `SaslAuthenticateRequest.toString()` masks authBytes | `impl fmt::Debug` masks `auth_bytes` as `<redacted>` | OK |
| `SaslAuthenticateRequest.parse(Readable, short)` | `SaslAuthenticateRequest::parse(...)` | OK |
| `SaslAuthenticateRequest.getErrorResponse(int, Throwable)` | `get_error_response(throttle_time_ms, error)` | OK |
| `SaslAuthenticateResponse.error() → Errors` | `error() -> Errors` | OK |
| `SaslAuthenticateResponse.errorMessage() → String` | `error_message() -> Option<&str>` | OK |
| `SaslAuthenticateResponse.sessionLifetimeMs() → long` | `session_lifetime_ms() -> i64` | OK |
| `SaslAuthenticateResponse.saslAuthBytes() → byte[]` | `sasl_auth_bytes() -> &[u8]` | OK |
| `SaslAuthenticateResponse.toString()` masks authBytes | `impl fmt::Debug` masks `auth_bytes` as `<redacted>` | OK |
| `SaslAuthenticateResponse.parse(Readable, short)` | `SaslAuthenticateResponse::parse(...)` | OK |

### Hex-fixture byte-by-byte verification (all 14)

All 14 fixtures independently re-derived from JSON spec + protocol-guide encoding rules; bytes match.

| Fixture | Version | Encoding details verified |
|---|---|---|
| `hex_fixture_v0_mechanism_plain` (Handshake req) | v0, non-flex | `00 05` i16 + "PLAIN" — OK |
| `hex_fixture_v1_mechanism_plain` (Handshake req) | v1, non-flex | identical to v0 (`flexibleVersions: "none"`) — OK |
| `hex_fixture_v1_mechanism_scram_sha_512` (Handshake req) | v1, non-flex | `00 0D` i16 + 13 chars — OK |
| `hex_fixture_v0_gssapi_only` (Handshake resp) | v0, non-flex | error_code `00 00` + array_len i32 `00 00 00 01` + str_len `00 06` + "GSSAPI" — OK |
| `hex_fixture_v1_plain_only` (Handshake resp) | v1, non-flex | identical shape to v0 — OK |
| `hex_fixture_v1_error_empty_mechanisms` (Handshake resp) | v1, non-flex | `00 21` (33) + i32(0) empty array — OK |
| `hex_fixture_v0_plain_token` (Authenticate req) | v0, non-flex | i32(10) length + `\0user\0pass` — OK |
| `hex_fixture_v1_plain_token` (Authenticate req) | v1, non-flex | identical to v0 — OK |
| `hex_fixture_v2_plain_token` (Authenticate req) **flex boundary** | v2, flex | uvarint(11) `0B` compact-bytes + 10 bytes + uvarint(0) tagged trailer — OK |
| `hex_fixture_v2_empty_auth_bytes` (Authenticate req) | v2, flex | uvarint(1) `01` + uvarint(0) `00` — OK |
| `hex_fixture_v0_success_null_message` (Authenticate resp) | v0, non-flex | error_code `00 00` + nullable-string-null `FF FF` + bytes-len i32(0) + (no session_lifetime_ms at v0) — OK |
| `hex_fixture_v1_success_long_max_session` (Authenticate resp) | v1, non-flex | v0 fields (8 bytes) + i64 `7F FF FF FF FF FF FF FF` — OK |
| `hex_fixture_v2_success_null_message_empty_auth` (Authenticate resp) **flex boundary** | v2, flex | error_code + uvarint(0) null-string + uvarint(1) empty-bytes + i64(0) session + uvarint(0) tagged trailer = 13 bytes — OK |
| `hex_fixture_v2_auth_failed_with_message` (Authenticate resp) | v2, flex | error_code `00 3A` (58) + uvarint(5) + "fail" + uvarint(1) empty bytes + i64(0) + uvarint(0) tagged trailer = 17 bytes — OK |

Residual risk: same generator-vs-spec common-mode misreading risk Actor 9 already documented in NOTES.md. Cross-verification with a running Java 4.2 client is the only complete defense; carry-over to 9c is appropriate.

## Findings

### Suggestion 1: `impl fmt::Display for SaslAuthenticateRequestData` / `SaslAuthenticateResponseData` bypasses wrapper-level redaction
- **Files**:
  - generator template emitting `impl fmt::Display { write!(f, "{:?}", self) }` for the `*Data` types — concretely, the SASL request/response data classes at end-of-file (lines 213-217 and 327-331 of the generated `sasl_authenticate_{request,response}_data.rs` under `target/debug/build/.../out/generated/`).
  - Custom-Debug wrappers: `src/common/requests/sasl_authenticate_request.rs:134-144`, `src/common/requests/sasl_authenticate_response.rs:113-125`.
- **Severity**: Suggestion (credential-leak-adjacent)
- **Java reference**: `SaslAuthenticateRequest.java:80-86` (override of `toString()` clears authBytes via `data.duplicate()`); but Java's own generated `SaslAuthenticateRequestData.toString()` *does* print `authBytes=Arrays.toString(authBytes)`, which leaks if called directly. The Rust port mirrors this Java pattern by only redacting at the wrapper level — so this is a Java-parity behaviour. **The new gap Rust introduces** is `impl fmt::Display` on the data class that calls through the derived `Debug`. Java's `SaslAuthenticateRequestData` does not have a separate `Display`-equivalent path.
- **Description**: The wrapper-level `Debug` masking protects `format!("{:?}", req)` / `format!("{:?}", resp)` but not `format!("{:?}", req.request_data())`, nor `format!("{}", req.request_data())`. The latter goes through the generated `impl fmt::Display` (line 213 of the response data, line 327 of the request data) which writes `{:?}` of `self` — using the **derived** Debug that prints `auth_bytes: [0x73, 0x65, ...]`. A `tracing::debug!("data = {}", req.request_data())` anywhere in the SASL state machine (9a+) would silently log the secret.
- **Expected**: Either (a) suppress `auth_bytes` in the generator-emitted Debug/Display for any message whose field is named `auth_bytes` (and credential-bearing in general — there are only the two SASL Authenticate types), or (b) add a hand-written `impl fmt::Debug` for `SaslAuthenticateRequestData` and `SaslAuthenticateResponseData` *next to the generated module* (the generated module supports re-import, so a sibling file impl is feasible), or (c) document a "never log `request_data()` / `response_data()` directly" rule and add a clippy lint or grep gate as part of 9a/9b.
- **Actual**: Calling `{:?}` or `{}` on the underlying `*Data` types leaks credentials. The wrapper-level Debug only protects the wrapper type.
- **Recommendation**: Carry-over to 9a (when the state machine actually starts logging). Not blocking for 9.0 — but please pin a follow-up checklist item before 9a's `SaslClientAuthenticator` lands so the first logging call doesn't introduce the regression.

### Suggestion 2: `debug_masks_auth_bytes` test panic message would leak the secret on failure
- **Files**:
  - `src/common/requests/sasl_authenticate_request.rs:267-269`
  - `src/common/requests/sasl_authenticate_response.rs:243-244`
- **Severity**: Suggestion (test-only, very small leak surface)
- **Description**: When the assertion `assert!(!dbg.contains("sensitive"), "Debug output leaked secret: {dbg}")` fires, Rust prints the full `dbg` string in the panic message — which is precisely the unredacted Debug output. In CI logs this could surface the leaked credentials. The assertion is necessary to *detect* the leak, but the panic message itself re-leaks.
- **Expected**: The format string should reference a sentinel (e.g. `"<test-fixture-secret>"`) but **not** echo `dbg` verbatim. E.g. `assert!(!dbg.contains(SENTINEL), "Debug output contains the sentinel — leaked! len={}", dbg.len());`. Or assert only on a sentinel substring and use a non-credential test fixture (the literal `"sensitive-auth-token-123"` is harmless but reinforces the pattern).
- **Actual**: On assertion failure, the full leaked Debug output appears in test runner output and any CI log.
- **Note**: This only fires if the redaction itself is broken — so the practical risk is minimal. Flagged as Suggestion because the test is otherwise the right shape.

### Suggestion 3: One Java RequestResponseTest is genuinely SASL-specific and could be translated cheaply
- **File**: `src/common/requests/sasl_authenticate_response.rs` (tests module)
- **Severity**: Suggestion
- **Java reference**: `kafka/clients/src/test/java/org/apache/kafka/common/requests/RequestResponseTest.java:976-977`:
  ```
  assertEquals(1, createSaslAuthenticateResponse().errorCounts().get(Errors.NONE));
  assertEquals(1, createSaslHandshakeResponse().errorCounts().get(Errors.NONE));
  ```
- **Description**: These two lines from Java's `testErrorCountsIncludesNone` are SASL-specific contract assertions (every response surfaces `Errors.NONE: 1` for a success-shaped response). The Rust SaslHandshakeResponse test `round_trip_unsupported_sasl_mechanism_error` asserts `error_counts().get(Errors::UnsupportedSaslMechanism) == Some(&1)` — a similar shape but never asserts the success-path `Errors::None` entry. The Rust SaslAuthenticateResponse side has `sasl_authentication_failed_error_counts` but again no positive-success assertion. Tiny addition (two assert_eq lines per type) and pins the most-common branch.
- **Expected**: Add a `success_response_error_counts_includes_none` assertion on both response wrapper test modules so the success-path `Errors::None` entry is byte-pinned at 1.
- **Actual**: Current tests cover the error-path entries but the success-path `Errors::None` entry presence is asserted only indirectly (via the `error()` accessor in round-trip tests, not via `error_counts()`).

### Suggestion 4: `parse_response_body` error message still says "Phase 2e wires only Produce, Metadata, ApiVersions, and (since Phase 9.0) ..."
- **File**: `src/common/requests/abstract_response.rs:142-145`
- **Severity**: Suggestion (rustdoc/error-message hygiene)
- **Description**: The fallback error string is informative and historically accurate, but starts with "Phase 2e wires only" which now contradicts its tail. The two phases (2e and 9.0) are referenced in the same error string. Future readers won't necessarily know what Phase 2e or Phase 9.0 mean.
- **Expected**: Rewrite as a static list of supported keys without phase references — the error path is user-facing on broker bring-up failure and should not lean on internal phase nomenclature.
- **Actual**: Error message mixes user-facing API listing with internal phase numbers. Functional impact: none; documentation/error-message hygiene only.

### Nit 1: `error_counts_via_get_error_response_v1` test asserts only `total >= 1`, not `== 1`
- **File**: `src/common/requests/sasl_handshake_request.rs:198-210`
- **Severity**: Nit
- **Description**: The test runs `req.get_error_response(0, &Authentication("bad mechanism"))` and asserts that the response's `error_counts()` `values().sum() >= 1`. Java's `SaslHandshakeResponse.errorCounts()` is implemented as `errorCounts(Errors.forCode(data.errorCode()))` which produces a single-entry map (count = 1) — so the sum is precisely 1. The `>=` assertion is correct but weaker than Java semantics — a future bug that produced `{None: 1, Authentication: 1}` (both branches firing) would silently pass.
- **Expected**: `assert_eq!(counts.values().sum::<i32>(), 1);` and `assert_eq!(counts.get(&Errors::SaslAuthenticationFailed), Some(&1));`.
- **Actual**: The weaker `>= 1` slip lets a duplicate-entry regression pass.

### Nit 2: Three skipped Java tests (`testInvalid*SaslAuthenticateRequest`, `testInvalidTaggedFieldsWithSaslAuthenticateRequest`) — explanation worth pinning in the NOTES.md close stanza
- **File**: `design/history/Milestone-1/Phase-9/NOTES.md` (close stanza)
- **Severity**: Nit (process / audit trail)
- **Java reference**:
  - `RequestResponseTest.java:3898-3908` (`testInvalidSaslHandShakeRequest`)
  - `RequestResponseTest.java:3911-3930` (`testInvalidSaslAuthenticateRequest`)
  - `RequestResponseTest.java:3961-3984` (`testInvalidTaggedFieldsWithSaslAuthenticateRequest`)
- **Description**: All three Java tests test the underlying `Readable`/`ByteBufferAccessor` corruption-error path, *using* SaslHandshake/SaslAuthenticate as transport. Equivalent Rust coverage exists at the codec level (`src/common/protocol/byte_buffer_accessor.rs:380-410`, `src/common/protocol/types/type.rs:433-525`). So the skip is reasonable — DoD #3 allows skipping Java tests that are "not relevant to the Rust codebase" when explained. **But the NOTES.md close stanza does not enumerate these three skipped Java tests or the rationale.** Future audits will not be able to tell whether the skips are intentional or accidental.
- **Expected**: A "Java tests intentionally not translated" note in the 9.0 close stanza listing the three tests above + the rationale ("equivalent codec-level corruption coverage at `byte_buffer_accessor.rs:380-410`").
- **Actual**: Close stanza describes what was translated but doesn't explicitly enumerate the (acceptable) skips.

## Round 1 verdict

**Accept-with-followups.** Phase 9.0's core deliverable — generator-driven SASL message types with wire-byte-pinned tests at every supported version including both flex boundaries — is in good shape:
1. Java-API parity is complete across both request types, both response types, both builders, and both `parse_response_body` arms.
2. All 14 hex fixtures verified by hand against the JSON specs + protocol-guide encoding rules; bytes are correct.
3. Credential redaction on the **wrapper level** is implemented for both Authenticate types.
4. Gates green (build, lib tests 1275/1275, format-check, lint, integration-test build).

The 4 Suggestions and 2 Nits are all non-blocking. Suggestion 1 (data-class Display/Debug leak) is the most important — it's a real leak vector that 9a's logging would likely surface, but it can be addressed in 9a alongside the first logging callsite. Suggestion 3 (one missing positive-path `Errors::None` count assertion per response) and Nit 1 (weak `>= 1` sum) are paper cuts that would tighten the test contract.

## Next steps for Manager

- Close 9.0 as accept-with-followups; either bundle the 4 Suggestions + 2 Nits into a small Round-2 cleanup pass before 9a, or carry Suggestion 1 specifically into 9a's pre-implementation checklist (since 9a is where the actual logging callsites that would expose the data-class leak will land).
- Carry-over the Java-runtime hex-fixture cross-verification into 9c per Actor 9's own note in the NOTES.md close stanza. The current hand-derived fixtures are internally consistent (generator and hand-derivation agree) but a wireshark/proxy capture against an Apache Kafka 4.2 client remains the only complete defense against generator+spec common-mode misreading.
- No changes needed to `CLAUDE.md` or `.claude/rules/*` from this review. The existing CLAUDE.md rules on nullable defaults, hot-path allocations, naming, and credential-handling were all followed where applicable.

Phase 9.0 Round 1 closes. Manager advances to Phase 9a (SaslChannelBuilder + SaslClientAuthenticator state machine).

---

## Phase 9a Round 1 — Critic review

Review window: commits `a8bd3f8..07ad0d4` on branch `fresh-impl` (5 Actor commits; housekeeping `2d3db4d` excluded).

Java references consulted (all under `kafka/clients/src/main/java/org/apache/kafka/common/`):
- `security/authenticator/SaslClientAuthenticator.java` (full file, ~712 LOC)
- `security/plain/PlainLoginModule.java`, `security/plain/PlainAuthenticateCallback.java` (PLAIN-callback shape — note: Apache Kafka 4.2 *client* side uses JCA `com.sun.security.sasl.PlainClient`, not a bundled class; the RFC-4616 token format is therefore derived from RFC 4616 directly)
- `security/auth/SecurityProtocol.java` (SASL variant ids)
- `network/SaslChannelBuilder.java` (constructor parameter shape, dispatch)
- `security/authenticator/SaslAuthenticatorTest.java` — only `testCorrelationId` referenced (the rest is server-integration, out of 9a per skip-list)
- `requests/SaslHandshakeResponse.java::enabledMechanisms()` and Java `List<String>.toString()` semantics for error-message parity
- RFC 4616 §2.1 (PLAIN token format)
- `generator/messages/{ApiVersionsRequest,SaslHandshake,SaslAuthenticate}*.json`

Master-branch consultation: **none**, per the user's 2026-05-21 Java-only directive.

Gate status (verified independently, not from Actor report):
- `cargo build` — clean
- `cargo test --lib` — **1299 passed**, 0 failed
- `cargo xtask format-check` — clean (`All code is properly formatted!`)
- `cargo xtask lint` — clean (`No lint issues found!`)
- `cargo test -p generator` — 78 passed, 0 failed

Verdict: **0 Blocking, 3 Suggestion, 2 Nit.**

## Per-area verifications

| Area | Result | Notes |
|---|---|---|
| Suggestion 1 (generator credential redaction) | OK | Generator emits hand-written `impl fmt::Debug` for `SaslAuthenticate{Request,Response}Data`; `Display` delegates to `Debug` (verified in generated `sasl_authenticate_request_data.rs:213-226` and `sasl_authenticate_response_data.rs:327-345`); `auth_bytes` rendered as `<redacted>`. 4 generator tests + 2 lib tests pin the contract end-to-end (both `{:?}` AND `{}` checked). Audited all `generator/messages/*.json` — no other JSON message has a field named `AuthBytes`/`auth_bytes`, so no false-positive redaction. |
| Suggestion 2 (test panic message) | OK | `src/common/requests/sasl_authenticate_request.rs:266-283` rewritten — asserts contain `dbg.len()`, not the raw `dbg` string. Sibling assertion in `sasl_authenticate_response.rs` matches. |
| Suggestion 3 (success-path error_counts) | OK | Both `success_response_error_counts_includes_none` tests present; `== 1` assertion on sum + explicit `Errors::None` entry. |
| Suggestion 4 (error string hygiene) | OK | `abstract_response.rs:142-145` rewritten to static API-key list; no phase-numbers; verified user-facing string is now deterministic on broker bring-up. |
| Nit 1 (tighten `>= 1` to `== 1`) | OK | `sasl_handshake_request.rs:200-217` tightened with explicit `SaslAuthenticationFailed` value check. |
| Nit 2 (NOTES.md skipped Java tests) | OK | NOTES.md:222-238 now enumerates the 3 skipped tests with codec-level rationale. |
| Java state-machine parity (PLAIN happy path) | OK | Rust collapses `INITIAL → INTERMEDIATE` into `SendInitialToken → ReceiveAuthenticateResponse → Complete`. For PLAIN, this is correct: Java's `INTERMEDIATE` checks `saslClient.isComplete()` (true after one round-trip for PLAIN) AND `noResponsesPending` (true since SASL_AUTHENTICATE response is non-null) → goes to `COMPLETE` not `CLIENT_COMPLETE`. The Rust collapse is faithful for PLAIN-only scope. |
| Java state-machine parity (negotiate version) | OK | `set_sasl_versions_from_api_versions` does `min(broker_max, ApiKey.latest_version)` exactly like Java; preserves `DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER` sentinel when broker omits SASL_AUTHENTICATE. |
| RFC 4616 token format | **OK byte-exact** | `build_plain_token()` produces `\0<username>\0<password>` with one leading NUL, one separator NUL, no trailing NUL. Pinned by `plain_token_rfc_4616_byte_shape` test at line 1162-1163: `assert_eq!(token, b"\0alice\0supersecret")`. |
| Tagged-field handling on v2 SASL_AUTHENTICATE | OK | `tagged_field_round_trip_on_authenticate_v2_response` verifies; delegates parsing to generator's writer/reader, which Phase 9.0 hex fixtures pinned to spec. |
| Correlation-id reserved range | OK | `MIN/MAX_RESERVED` constants match Java exactly (`i32::MAX - 7 ..= i32::MAX`). Wrap-around tested via `next_correlation_id_stays_in_reserved_range`. |
| `pending_send` partial-write handling | OK with caveat | `PendingSend { inner: ByteBufferSend, correlation_header: Option<RequestHeader> }` deferred correlation-header latching until flush completes. Note: state advances eagerly in Rust (vs Java's `pendingSaslState` deferral) — see Finding S1 for divergence analysis; not a bug for the producer use case. |
| Mocked transport tests | OK | `MockTransport` faithful to Java NIO semantics: `Ok(0)` for WouldBlock, `Err(UnexpectedEof)` for closed peer. Test fixtures reuse generator's own `serialize()` output, not hand-derived hex — robust against generator drift. |
| `Debug` redaction on `PlainCredentials` / `SaslClientAuthenticator` | OK | Hand-emitted at lines 125-132 and 236-253; password masked; in-flight send buffer masked. `plain_credentials_debug_masks_password` test pins. |
| `SaslChannelBuilder` construction | OK | All 5 construction tests pass; rejects non-SASL protocol, non-PLAIN mechanism, SASL_SSL without ssl_config. |
| `SecurityProtocol::SaslPlaintext`/`SaslSsl` variants | OK | Ids 2 and 3 match Java `SecurityProtocol.java`. `is_sasl()` / `uses_ssl()` predicates correct. |
| Producer-side gate at `kafka_producer.rs:660` | OK | Still rejects non-PLAINTEXT; user-visible producer behaviour unchanged. SASL only reachable via direct `SaslClientAuthenticator::new` (test path) or future Phase 9b `client_channel_builder` call. |
| Removal of pre-Phase-9 constants | OK | `grep -r "reject_sasl_until_phase_9\|SASL_RESERVED" src/` returns empty. |
| Skipped Java tests | OK | NOTES.md enumerates 6 skipped Java tests + rationale per DoD #3; all rationales concrete (server-side, integration-only, JAAS plumbing for non-PLAIN). |
| Hot-path allocation audit | N/A | SASL is not on the producer send path; per DoD #10 skipped. |
| License headers | OK | Apache 2.0, Confluent Inc copyright on both new files. |

## Findings

### Suggestion S1 — Java's `List<String>.toString()` parity in `handle_sasl_handshake_response` error message

**File:** `src/common/security/authenticator/sasl_client_authenticator.rs:553`
**Severity:** Suggestion (Java-parity / DoD #3 "Error message content is asserted")
**Java reference:** `SaslClientAuthenticator.java:609-616` uses `response.enabledMechanisms()` which returns `List<String>`. Java's `List<String>.toString()` produces `[m1, m2]` — bracketed with comma-space separator, **no quotes around individual strings**.

**Current Rust:**
```rust
let enabled = format!("{:?}", response.response_data().mechanisms);
```
Rust's `{:?}` on `Vec<String>` produces `["SCRAM-SHA-512"]` — bracketed but **with quotes** around each string.

**Concrete divergence:**
- Java emits: `enabled mechanisms are [SCRAM-SHA-512]`
- Rust emits: `enabled mechanisms are ["SCRAM-SHA-512"]`

The unit test (`handshake_unsupported_mechanism_fails_with_java_message:1031-1034`) only checks `msg.contains("SCRAM-SHA-512")`, so it passes either way. But Phase 9 DoD #1 says auth failure must surface "with a message matching Java's error string" — the quote difference is a real divergence that a future cross-broker test (or a customer reading the error message) would catch.

**Suggested fix:** render with `enabled.join(", ")` (or use the `enabled_mechanisms()` accessor and format as `[{}]`):
```rust
let enabled = format!("[{}]", response.enabled_mechanisms().join(", "));
```
Same for the `Errors::IllegalSaslState` and default arms of the match.

Same issue applies anywhere `format!("{:?}", ...)` is used on a `Vec<String>` for user-facing error messages. Worth a brief grep when fixing.

### Suggestion S2 — `eof_mid_handshake_surfaces_as_unexpected_eof` doesn't transition to `Failed`

**File:** `src/common/security/authenticator/sasl_client_authenticator.rs:1090-1108`
**Severity:** Suggestion (state-machine consistency)
**Java reference:** `SaslClientAuthenticator.java` — in Java, EOF during a read inside `authenticate()` raises `EOFException` from `channel.read()` returning -1 (mapped via `NetworkReceive`). The Selector handles this at the outer level by closing the channel; the authenticator's state remains at whatever it was — Java also doesn't set `FAILED` on EOF. So this is **consistent with Java** at first read.

However: in Rust, after the EOF, `authenticate()` returns `Err(UnexpectedEof)`. The Rust state remains at `ReceiveApiVersionsResponse` (test asserts this) AND `failure` remains `None`. If a future caller (e.g. a buggy retry layer) invokes `authenticate(transport)` again on a now-closed transport, the loop top-guard `if self.pending_send.is_some() && !self.flush_pending_send(transport)?` short-circuits because `pending_send` is None (already flushed in step 1). It then re-enters `ReceiveApiVersionsResponse` and would try to `read()` again from a closed transport — typically returning `Ok(0)` (WouldBlock) and looping. **Effect:** authenticator is in a wedged-but-not-Failed state.

This is acceptable for Phase 9a because the upper layer (Phase 9b channel wiring) is expected to detect EOF and close the channel. **But** there's no Phase-9b-test-pinning that the channel will do this. A safer translation would set `state = Failed` + `failure = Some(KafkaError::Authentication("EOF during SASL handshake"))` when EOF is propagated. Java avoids the issue by relying on the JVM exception propagating up; Rust's `io::Result` requires explicit state housekeeping.

**Suggested fix:** wrap the call chain in `authenticate()` with a match on errors of kind `UnexpectedEof` / `ConnectionReset` etc. that sets `state = Failed` before propagating. Or document the wedged-state caveat in the `authenticate()` rustdoc so 9b knows to add a transport-closed check.

### Suggestion S3 — Eager state transition on partial-write may surface confusing errors under simulated load

**File:** `src/common/security/authenticator/sasl_client_authenticator.rs:391, 428, 465`
**Severity:** Suggestion (Java-parity / robustness)
**Java reference:** `SaslClientAuthenticator.java:406-425` (`setSaslState`) — Java defers state transitions while `netOutBuffer != null && !netOutBuffer.completed()`, latching `pendingSaslState` and applying it inside `flushNetOutBufferAndUpdateInterestOps`. Rust transitions state eagerly after each `queue_request` (lines 391, 428, 465 of `sasl_client_authenticator.rs`).

In practice this works for the happy path because the receive path returns `Ok(None)` when no bytes are available. But under simulated partial-write + delayed-flush conditions (which the Phase 9a mocks do NOT exercise but Phase 9b/9c will), there's a subtle invariant break:

After partial-write in `queue_request`, state has already advanced to `ReceiveApiVersionsResponse` but `current_request_header` is `None` (set inside `flush_pending_send` only on completion). If a buggy broker happened to respond before we finished sending (impossible on TCP but possible in a faulty mock), `receive_response` would hit `let header = self.current_request_header.take().ok_or_else(...)` and return an `Authentication` error — but actually the request hadn't even finished being sent.

This is an "edge case that can't happen on a sane wire" rather than a real bug. But the eager-vs-deferred divergence is worth a comment near the state transition or a test that explicitly simulates partial-write to lock in the contract. **Phase 9a does not have a partial-write test** (Test mock comment at line 738-740 explicitly defers this to 9b/9c).

**Suggested fix:** add a regression test using a partial-write mock (cap `write_vectored` accepted bytes at e.g. 4) to verify the state machine correctly resumes when called again. If the test fails, refactor to defer state transition until flush completes (matching Java's `pendingSaslState`). Bundle into 9b's channel-wiring work.

### Nit N1 — Tagged-field round-trip test would benefit from raw-byte assertion

**File:** `src/common/security/authenticator/sasl_client_authenticator.rs:1169-1205`
**Severity:** Nit
The test `tagged_field_round_trip_on_authenticate_v2_response` proves the state machine reaches `Complete` after a tagged-field-bearing response. But it does NOT assert that the *parsed* `unknown_tagged_fields` were preserved, nor that the byte representation on the wire matches Phase 9.0's hex fixtures. The current assertion (`auth.state() == Complete`) would also pass if the parser silently dropped tagged fields. The Phase 9.0 hex fixtures already cover byte-level parity; this test is more about "state machine doesn't crash on tagged-fields" which is a weaker contract.

**Suggested fix:** after reaching Complete, drain the inbound mock and re-parse the response to confirm `RawTaggedField::new(7, vec![0xAB, 0xCD])` survived; or write a tiny assertion that the wire bytes used for the response contain the tagged-trailer bytes (`07 02 AB CD`). Low-impact since 9.0's hex fixtures cover this at the message-type layer.

### Nit N2 — `next_correlation_id_stays_in_reserved_range` test loop iteration count comment is misleading

**File:** `src/common/security/authenticator/sasl_client_authenticator.rs:1126-1142`
**Severity:** Nit
The test comment says "(MAX - MIN) * 2" which evaluates to 14 (since MAX - MIN = 7). The test inserts 14 ids into a HashSet, then asserts `seen.len() == (MAX - MIN + 1) = 8`. The math is correct (14 calls produce 8 distinct ids: MIN..=MAX once + MIN..MIN+5 after wrap), but the comment "Java: (MAX - MIN) * 2 iterations to exhaust + wrap" is opaque.

**Suggested fix:** rephrase comment as "exercise full range + 6 additional calls past wrap to prove reset to MIN; expected distinct ids = MAX-MIN+1 = 8". Or just `let iterations = 2 * (MAX_RESERVED_CORRELATION_ID - MIN_RESERVED_CORRELATION_ID + 1) as usize;` with a one-line comment. Cosmetic only.

## `build_channel() = UnsupportedOperation` deviation review

**Acceptable as-is.** Rationale:
- The Phase 9a brief explicitly states "no integration test yet" for 9a.
- The deferral is documented inline (lines 159-176 of `sasl_channel_builder.rs`) with the exact reason (Phase 5b-3 `Authenticator` trait doesn't thread transport reference) AND the path forward.
- The `SaslClientAuthenticator` is fully unit-tested directly via `::new`, exercising the production state-machine code path without channel scaffolding.
- The producer-side gate at `kafka_producer.rs:660` still rejects non-PLAINTEXT, so no user-visible regression.
- A pinning test (`build_channel_returns_unsupported_operation_in_phase_9a`) locks the deferral so a 9b contributor cannot silently break the contract.

Note for 9b: the `Authenticator` trait reshape needed for 9b touches `PlaintextAuthenticator` and `SslAuthenticator` (which also implement the trait). Suggestion: introduce a separate `SaslAuthenticator` trait or a `ChannelAuthenticator` wrapper that carries a transport reference, rather than reshaping the shared `Authenticator` trait — the latter would touch all existing call sites.

## Round 1 verdict: accept-with-followups

All Phase 9.0 Round 1 followups (Suggestions 1-4 + Nits 1-2) are resolved. The PLAIN state machine is byte-exact RFC-4616 conformant and Java-parity faithful for the producer scope (PLAIN-only, no re-auth). The `UnsupportedOperation` channel-builder deferral is acceptable per the brief.

3 Suggestions + 2 Nits are non-blocking and can either be bundled into 9b (where partial-write coverage and EOF→Failed handling naturally land alongside channel wiring) or fixed opportunistically. **S1 (Java List<String>.toString() parity)** is the most user-visible — a customer reading the error message in production would see Rust's quote-wrapped form differ from any Java-client-issued reference docs.

## Next steps for Manager

1. Decide on S1 (Java List<String>.toString() parity): fix now or bundle into 9b error-message hardening alongside DoD #1 ("message matching Java's error string"). Recommend fix now since it's a 2-line change with public-error-string impact.
2. Decide on S2 and S3 (EOF→Failed transition and partial-write test): both naturally bundle into 9b channel wiring. Mark in 9b brief.
3. N1 and N2 are cosmetic; can be opportunistically bundled with any future touch of the test file.
4. Proceed to 9b once Actor 9 (or Critic 9 ack) acknowledges S1.

Phase 9a Round 1 closes. Manager advances to Phase 9b (config: SecurityProtocol::SaslPlaintext/SaslSsl, sasl.mechanism / sasl.jaas.config parsing, validator rejections).

---

## Phase 9b Round 1 — Critic review

Review window: commits `731072a..fafd523` on branch `fresh-impl` (8 Actor commits + 1 Manager housekeeping `e6b6491`).

Java references consulted (all `kafka/clients/src/main/java/org/apache/kafka/`):
- `clients/CommonClientConfigs.java:297-306` (`postValidateSaslMechanismConfig`)
- `common/network/SaslChannelBuilder.java:215-272` (`buildChannel`, `buildTransportLayer`)
- `common/security/authenticator/SaslClientAuthenticator.java:485-489` (`principal()`)
- `common/security/authenticator/SaslClientAuthenticator.java:603-616` (error format strings)

Master-branch consultation: **none**, per the user's 2026-05-21 Java-only directive.

Gate status (verified, not from Actor report):
- `cargo build` — clean
- `cargo test --lib` — **1327 passed, 0 failed** (matches Actor's claim 1299 → 1327, delta +28)
- `cargo xtask format-check` — clean
- `cargo xtask lint` — clean
- `cargo test --package generator --lib` — 78 passed

Verdict: **0 Blocking, 4 Suggestion, 3 Nit.**

## Per-area verifications

| Area | Result | Notes |
|---|---|---|
| **S1: Java `List<String>.toString()` parity** | OK | Format swapped from `{:?}` to `[{}]/join(", ")`. Test now uses exact `assert_eq!` against full Java string. Grep audit confirmed no other `format!("{:?}", Vec<String>)` in user-facing error paths. |
| **S2: EOF→Failed transition** | OK | `authenticate()` now wraps `authenticate_inner()` and on `UnexpectedEof`/`ConnectionReset` sets `state = Failed` + `failure = Some(KafkaError::Authentication("EOF during SASL handshake"))`. Re-invocation surfaces the captured error rather than wedging. Test pins all 4 properties. |
| **S3: partial-write resumption** | OK | New `partial_writes_resume_correctly_to_complete` test caps `write_vectored` at 4 bytes/call, drives the full PLAIN handshake. Walkthrough below confirms resumption path. No refactor needed. |
| **N1: tagged-field test pinning** | Partial | Test now asserts encoder emits the trailer bytes `01 07 02 AB CD` — catches encoder regression. Does NOT assert parser preserves `unknown_tagged_fields` in the parsed struct (response is dropped inside the state machine). Improvement over the previous "Complete-only" assertion but still doesn't match the Nit's literal phrasing. See N4 below. |
| **N2: correlation-id comment** | OK | Rewritten as "exercise full range + 6 additional calls past wrap"; iteration math labelled `range_size = MAX - MIN + 1 = 8`. |
| **Architectural: SaslAuthenticator trait + ChannelAuthenticator enum** | OK (justified) | See "Architectural decision audit" in findings. The dual shape is the right shape for Rust here — alternative collapse-to-one-trait would force every `PlaintextAuthenticator`/`SslAuthenticator` call site to plumb a useless `&mut dyn TransportLayer`. |
| **PlaintextAuthenticator/SslAuthenticator untouched** | OK | Confirmed via `git diff 58fc7ec..6f426eb`: only doc text changed; impl blocks unchanged. |
| **`build_channel()` Java parity** | Acceptable deviation | Java's single `buildChannel(id, key, maxReceiveSize, ...)` handles both via the `SelectionKey`'s SocketChannel. Rust splits into `build_channel(stream, ...)` + `build_sasl_ssl_channel(stream, server_name, ...)` because the trait method can't carry SNI hostname. Documented. The 9a deferral-pinning test was removed; 3 new tests cover the post-deferral surface. |
| **JAAS parser PLAIN-only choice** | OK | Option (b) — ~270 LOC + 15 unit tests. Module rustdoc clearly states rationale. Rejection messages name the offending LoginModule. |
| **`sasl.username`/`sasl.password` shortcut** | OK (with caveat) | Documented as fresh-impl extension in module doc + per-const rustdoc. JAAS-wins precedence matches Java's canonical-source semantics. See S2 below for a minor schema-test gap. |
| **Validator error messages** | Acceptable | Java's `postValidateSaslMechanismConfig` only checks null/empty; Rust adds a **new** Milestone-1 narrowing layer that names the offending mechanism. Not a Java parity claim — clearly labelled `reject_milestone_1_unsupported_sasl_mechanism`. Acceptable for the narrowing layer's purpose. |
| **Producer-side gate lift** | OK | `security.protocol = SSL` / `SASL_SSL` still rejected with a clearer "SSL plumbing not yet wired" message. `SASL_PLAINTEXT` reaches `build_production_network_client`. 3 new construction tests. |
| **SASL_SSL deferral scope** | OK | The gate at `kafka_producer.rs:657` is correct in scope: there's no `ssl.ca.location`/`ssl.truststore.location` → `rustls::ClientConfig` bridge through `ProducerConfig` yet. `SslTransportLayer`/`SslChannelBuilder` exist from Phase 5b-3, but the producer-config-to-`Arc<ClientConfig>` plumbing is a Phase 9c (folded-in 8e) task. The `build_sasl_ssl_channel(...)` typed entry point is implemented but unreachable from `KafkaProducer::new` until that plumbing lands — correct deferral shape. |
| **Test count delta** | OK | 1299 + 15 JAAS + 4 producer-config SASL + 3 ChannelAuthenticator dispatch + 3 SaslChannelBuilder build_channel + 1 partial-write + 3 KafkaProducer SASL − 1 obsoleted = 1327. Matches verified count. |
| **Skipped Java tests** | Acceptable | `SaslConfigsTest`, `JaasConfigTest`, `JaasContextTest`, `SaslAuthenticatorTest.testCorrelationId` rationales documented in NOTES.md close stanza. JAAS parser's own 15 tests cover the equivalent narrow surface. |
| **NOTES.md close stanza** | OK | Accurate. Lists 8 commits, captures the architectural decision, JAAS choice, S2 outcome, deferrals, skipped tests, 5 Actor decisions, final test count. |

## S2 partial-write walkthrough (verified, not from Actor report)

I traced the resumption path the Actor claims works without refactor. Walkthrough:

1. **First `authenticate()` call.** `pending_send == None`, top-guard skipped. Enters loop, hits `SendApiVersionsRequest` arm → calls `queue_request()`.
2. **Inside `queue_request()`.** Encodes the request into a `ByteBufferSend`, sets `pending_send = Some(PendingSend { inner, correlation_header: Some(header) })`. Then calls `flush_pending_send()`.
3. **Inside `flush_pending_send()` (first iteration).** `pending.inner.write_to(transport)` — partial write returns `Ok(4)` (4 bytes), `pending.inner.completed()` is `false`. Returns `Ok(false)`. **Critically: `current_request_header` is NOT set yet (it's only set inside the `if pending.inner.completed()` branch).** `pending_send` remains `Some`.
4. **Back in `queue_request()`.** `let _ = self.flush_pending_send(...)` — return value ignored. Returns `Ok(())`.
5. **Back in the state-machine loop.** `self.state = SaslState::ReceiveApiVersionsResponse` (eager transition). `started_state` was `SendApiVersionsRequest`, current state is `ReceiveApiVersionsResponse` → state changed → continue loop.
6. **Next iteration: state == `ReceiveApiVersionsResponse`.** Hits `receive_response()`. Inside `receive_response()`, `self.current_request_header.take()` returns `None` → would error with "received SASL response with no pending request header".

**Wait — this looks like a real bug.** Let me re-read more carefully…

Re-reading `authenticate_inner()` top: `if self.pending_send.is_some() && !self.flush_pending_send(transport)? { return Ok(()) }`. So **after** `queue_request` completes and the loop iterates, when we enter `receive_response()` on the *next* call to `authenticate()`, the top-guard fires first.

But within the **same** `authenticate()` call, after `queue_request` returns successfully, we immediately advance state and the loop tries to `receive_response()` in the same iteration. **However**, before `receive_response` parses anything, it calls `receive_raw_token(transport)?` first, which returns `Ok(None)` when no bytes are buffered (test's `MockTransport.read()` returns `Ok(0)` on empty queue). `None` → `receive_response` returns `Ok(None)` → state-machine arm returns `Ok(())` from `authenticate_inner`. **The `current_request_header.take()` line is never reached during this round-trip** because the early return on `Ok(None)` from `receive_raw_token` short-circuits.

7. **Next `authenticate()` call.** Top guard: `pending_send.is_some()` (true), enter `flush_pending_send`. This time the kernel accepts the remaining bytes; `pending.inner.completed()` → true. Inside the `if completed()` branch, `current_request_header = Some(header)` is set, OP_WRITE removed. Returns `Ok(true)`. The `!` makes it `false` so we proceed past the guard.
8. **Loop iteration. state == `ReceiveApiVersionsResponse`.** `receive_response()` → if the response is now on the wire (test pushes it after the partial-write loop completes), it parses successfully. If not, returns `Ok(None)`.

**OK, the path is correct.** The early return on `receive_raw_token == None` is what saves us — it ensures we don't reach `current_request_header.take()` until after the flush completes (which sets the header). Actor 9's claim that no refactor is needed holds up.

The subtle invariant: `receive_raw_token` must always return `Ok(None)` first when no payload is buffered, before `current_request_header.take()` is reached. The current `receive_raw_token` implementation honors this by trying to read the 4-byte length prefix first; a `read == 0` returns `Ok(None)`. Good.

## Findings

### Suggestion 1 — `SaslClientAuthenticator::principal()` returns anonymous, Java returns username

- **File**: `src/common/security/authenticator/sasl_client_authenticator.rs:755-765`
- **Severity**: Suggestion
- **Java Reference**: `kafka/clients/.../SaslClientAuthenticator.java:487-489`
- **Description**: Java's `SaslClientAuthenticator.principal()` returns `new KafkaPrincipal(KafkaPrincipal.USER_TYPE, clientPrincipalName)` — the SASL-authenticated username. The Rust impl returns `KafkaPrincipal::anonymous()` with the rustdoc noting "principals carry no semantic value on the client side outside of logging / metrics". This is a documented divergence and a reasonable Milestone-1 deferral (the client-only producer never uses the principal for ACL), but it **does** affect log lines that include the principal name. Phase 9c integration tests against a real broker may surface this as a diagnostic gap (you'd see `User:ANONYMOUS` in log lines for an authenticated SASL session). Consider returning `KafkaPrincipal::new("User", &self.credentials.username)` instead — it's a one-line change and preserves the Java surface for the one thing principals are used for on the client (log identity).

### Suggestion 2 — JAAS-only schema test does not include the new SASL_USERNAME / SASL_PASSWORD keys

- **File**: `src/common/config/sasl_configs.rs:642-657`
- **Severity**: Suggestion
- **Java Reference**: N/A (fresh-impl extension)
- **Description**: The `add_client_sasl_support_registers_core_keys` test in `sasl_configs.rs` checks that `SASL_MECHANISM`, `SASL_JAAS_CONFIG`, etc. are registered, but does NOT include the new `SASL_USERNAME` / `SASL_PASSWORD` keys that Commit 5 added in the same `add_client_sasl_support()` body. The schema coverage at `producer_config.rs:1483-1484` (`schema_includes_sasl_keys`) does check them, so the keys are tested *somewhere* — but the per-module test that's specifically named "registers_core_keys" should mention the keys the module registers. Add `SASL_USERNAME` and `SASL_PASSWORD` to the iteration list in the `sasl_configs.rs` test for symmetry.

### Suggestion 3 — Stale doc reference to a non-existent method name in `producer_config.rs`

- **File**: `src/producer/producer_config.rs:415`
- **Severity**: Nit (cosmetic — wait, calling it Suggestion because broken intra-doc link affects rustdoc generation)
- **Description**: The comment near the security-protocol validator references `[`Self::post_validate_sasl_mechanism_config_with_milestone_narrowing`]` — but the actual method name added in Commit 5 is `reject_milestone_1_unsupported_sasl_mechanism`. Broken intra-doc link. Will produce a rustdoc warning on next `cargo doc` run.

### Suggestion 4 — Stale "Phase 9a scope" rustdoc in `sasl_channel_builder.rs`

- **File**: `src/common/network/sasl_channel_builder.rs:34, 47, 49, 96, 132`
- **Severity**: Nit (doc-only)
- **Description**: Several rustdoc comments still say "Phase 9a scope" / "out-of-scope for Phase 9a:" / "(Phase 9a scope)" — these were accurate when the builder was first introduced in Phase 9a but the deferred items (JAAS parsing, `PlainCredentials` resolution, `build_channel` body) are now landed in Phase 9b. Reword to "Phase 9b scope" or remove the phase tag entirely; the rustdoc inaccurately says these are still deferred.

### Nit 1 — N1's tagged-field test pins the encoder, not the parser

- **File**: `src/common/security/authenticator/sasl_client_authenticator.rs:1411-1465` (`tagged_field_round_trip_on_authenticate_v2_response`)
- **Severity**: Nit (test-name vs assertion mismatch)
- **Description**: The original Nit N1 wording was "Does NOT assert that `unknown_tagged_fields` survived parsing." The fix adds an assertion that the literal trailer bytes appear in the framed bytes pushed to the mock transport — i.e., it proves the *encoder* writes the trailer. It does NOT prove the *parser* preserves `unknown_tagged_fields` in the parsed struct (the response is consumed inside `handle_sasl_authenticate_response`, where the field is unused). The Phase 9.0 hex fixtures + `Readable::read_tagged_field` cover the parser surface separately, so the gap is theoretical — but the test name "round_trip_on_authenticate_v2_response" implies a parser preservation claim it doesn't make. Either rename to `..._encodes_tagged_trailer` or extend to capture the response from the authenticator and assert `parsed.unknown_tagged_fields == vec![RawTaggedField::new(7, vec![0xAB, 0xCD])]`.

### Nit 2 — `password()` accessor's rustdoc references `build_plain_token` as a hint, but it's a private method

- **File**: `src/common/security/authenticator/sasl_client_authenticator.rs:121-130`
- **Severity**: Nit (doc-only)
- **Description**: The rustdoc on `PlainCredentials::password()` says "Callers that need the actual token use `SaslClientAuthenticator::build_plain_token` internally". `build_plain_token` is a private method (no `pub`), so external callers cannot use it. The intent is clear (the SASL state machine internally builds the token), but the cross-reference is dead. Reword as "internal: see private `SaslClientAuthenticator::build_plain_token`" or drop the hint.

### Nit 3 — `resolve_plain_credentials` match arm has a redundant pattern

- **File**: `src/producer/kafka_producer.rs:764`
- **Severity**: Nit
- **Description**: The second arm `(Some(u), None) | (Some(u), Some(_)) if !u.is_empty() => ...` matches when username is non-empty and either password is None OR present. Combined with the first arm's `(Some(u), Some(p)) if !u.is_empty() && !p.is_empty()`, the second arm with `Some(_)` only fires when password is `Some("")`. The error message says "missing or empty" — accurate. The pattern is technically correct but mildly hard to read; could be split into two arms (one for None, one for Some-but-empty) or replaced with a guard `match` on cleaner branches. Not a behavioral issue.

## Round 1 verdict

**Accept with followups.**

All Phase 9b mandate items landed correctly: S1/S2/S3 followups from 9a all resolved, JAAS parser + config plumbing implemented, producer-side SASL_PLAINTEXT gate lift verified, architectural decision (trait + enum) is justified. SASL_SSL deferral scope is correctly waiting on Phase 9c's SSL config plumbing. Gates green.

The findings are all Suggestion / Nit severity — none of them block 9b from closing. Suggestions 1 (anonymous principal) and 2 (schema test symmetry) are worth bundling into 9c when next touching those files. Suggestions 3-4 (stale doc references) are 1-line fixes. Nits 1-3 are quality improvements that can wait.

## Next steps for Manager

1. Close 9b as accept-with-followups; archive this section to `COMMENTS.DONE.9.md`.
2. Carry these followups into 9c work:
   - **S1** (principal returns anonymous) — fits naturally with the integration-test round when a real broker is talking SASL, so wire log identity to the SASL username then.
   - **S2** (schema test symmetry) — bundle when next touching `sasl_configs.rs`.
   - **S3** (stale doc reference) and **S4** (stale "Phase 9a scope") — 1-line fixes; opportunistic.
   - **N1-N3** — test/doc polish; bundle opportunistically.
3. Spawn Actor 9 for sub-phase 9c (Integration test 1: SSL connection + producer-side `Arc<ClientConfig>` plumbing). The 9b deferrals (SASL_SSL gate, SSL config plumbing) all collapse into 9c's scope.

Phase 9b Round 1 closes. Manager advances to Phase 9c (Integration test 1: SSL connection + producer-side rustls ClientConfig plumbing — folded-in Phase 8e).

---

# Phase 9c Round 1 — Phase 9b R1 + Phase 8c R1 followups resolved

Resolutions for the 7 Phase 9b Round 1 followups + 2 Phase 8c R1
followups that were carried into Phase 9c. All seven 9b followups
bundled into commit `0dfee58`; the two 8c followups bundled with the
SSL integration test into commit `ee3ea59`.

## Phase 9b Round 1 — resolved in Phase 9c

- **S1 — `SaslClientAuthenticator::principal()` returned `KafkaPrincipal::anonymous()`.** **Resolved (`0dfee58`)**: returns `KafkaPrincipal::new(USER_TYPE, &self.credentials.username)` per Java parity. Field accessor `username` is `pub(crate)` (same module) so direct access is preferred over the `.username()` getter.
- **S2 — Schema-coverage test in `sasl_configs.rs` did not iterate `SASL_USERNAME` / `SASL_PASSWORD`.** **Resolved (`0dfee58`)**: the test now iterates them alongside the canonical SASL keys.
- **S3 — Broken intra-doc link `post_validate_sasl_mechanism_config_with_milestone_narrowing`.** **Resolved (`0dfee58`)**: corrected to `reject_milestone_1_unsupported_sasl_mechanism` (actual method name).
- **S4 — Stale "Phase 9a scope" references in `sasl_channel_builder.rs`.** **Resolved (`0dfee58`)**: five references reworded to "Milestone-1 scope" or phase-neutral language.
- **N1 — Tagged-field test name implied parser-preservation but only asserted encoder output.** **Resolved (`0dfee58`)**: extended with a standalone re-parse step via `ResponseHeader::parse` + `SaslAuthenticateResponseData::read`, asserting `unknown_tagged_fields` survives with the correct tag id and payload.
- **N2 — `PlainCredentials::password()` rustdoc referenced private `build_plain_token`.** **Resolved (`0dfee58`)**: reworded to "internal callers ... build it inside the SASL authenticator's RFC 4616 token assembler (not exposed as a public API)".
- **N3 — `resolve_plain_credentials` match-arm pattern hard to read.** **Resolved (`0dfee58`)**: empty-string normalized to `None` upfront via `.filter(|s| !s.is_empty())`, leaving a flat 4-arm match on `(Option<&str>, Option<&str>)`.

## Phase 8c Round 1 — resolved in Phase 9c (touch point: producer-smoke test file)

- **Phase 8c R1 Suggestion 1 — `Arc::try_unwrap` clone-drop dance.** **Resolved (`ee3ea59`)**: replaced with `Arc::into_inner` returning `Some(T)` since the test holds the sole strong ref. One line, no `map_err` discard.
- **Phase 8c R1 Nit 1 — `consume_records` rustdoc missing `\n` collision discussion.** **Resolved (`ee3ea59`)**: extended the rustdoc to enumerate both `\x1F` (within-line) and `\n` (across-line) collision risks for non-ASCII payloads + suggested extension paths.

Phase 9c Round 1 closes pending Critic 9 review.

---

# Phase 9c Round 1 — Critic 9 followups resolved

Resolutions for the 4 Suggestions + 3 Nits raised by Critic 9 against
the Phase 9c Round 1 commit ladder (`2a3dc83..03a507c`).

## Phase 9c Round 1 — resolved

- **S1 — Raw-IPv4 SNI behaviour doc was wrong in 4 source-tree locations + NOTES.md.** **Resolved**: rewrote (a) `build_and_register_channel` body comment to describe rustls' actual three-way `try_from` outcomes (DNS / IP literal / parse failure), (b) test rustdoc + Case-2 inline comment to drop the "rustls rejects raw IPs as hostnames" claim, (c) the assertion-adjacent comment to note that SSL builders accept IP literals and that the handshake omits SNI per RFC 6066 §3 while verifying via IP-SAN match, and (d) the 9c.2 close-stanza in NOTES.md to clarify that only the `None` (unparseable) case is rejected loudly. The actual code is unchanged because it was already correct.
- **N3 — Integration test cert-SAN comment misleading.** **Resolved**: rewrote `tests/integration/producer_smoke_test.rs:683-685` to note that the cert carries SANs for `localhost`, the container hostname, AND IP `127.0.0.1`, and that since `ssl_bootstrap_servers` is `127.0.0.1:<port>` the verification path is IP-SAN match (not hostname-SAN match against `localhost`).
- **S2 — `NoHostnameVerifier` chain-of-trust path had no direct test coverage.** **Resolved**: added two unit tests in `src/common/security/ssl/mod.rs` that construct real cert chains via `rcgen` and exercise the verifier directly. `no_hostname_verifier_rejects_untrusted_ca_chain` pins that a cert rooted at a CA NOT in the truststore is rejected (chain validation runs even with hostname check disabled). `no_hostname_verifier_accepts_chain_with_san_mismatch` pins that a chain-valid cert with a SAN that does not match the wrapper's placeholder hostname is still accepted (the two `NotValidForName` / `NotValidForNameContext` error variants are translated into success). Both tests build a `WebPkiServerVerifier` against a `RootCertStore` and invoke `NoHostnameVerifier::verify_server_cert` directly — `NoHostnameVerifier` is reachable from the test module as a sibling private item, no API surface change required. Test count: 1339 → 1341 (+2).
- **S3 — `SaslClientAuthenticator::principal()` had no regression test + the "Java parity" comment was misleading.** **Resolved**: (a) added `sasl_authenticator_principal_returns_configured_username` regression test in `sasl_client_authenticator.rs` tests module that pins both the `USER_TYPE` tag and the literal username `alice` — an inadvertent revert to `KafkaPrincipal::anonymous()` (Phase 9a placeholder) would now fail compile-and-run; (b) reworded the inline comment to describe Java's actual behaviour (`clientPrincipalName = null` for PLAIN → latent NPE on `requireNonNull(name)` in the `KafkaPrincipal` ctor) and explain why the Rust impl is a documented *deviation*, not parity. Test count: 1341 → 1342 (+1).
- **S4 — SSL "missing truststore" rejection test used a substring check; symmetric SASL test does the same.** **Resolved (chose lighter option, no behavioural code change)**: added a rustdoc paragraph on `public_new_rejects_ssl_without_truststore_location` documenting the substring-assertion policy and pinning the full error message (`"ssl.truststore.location is required when security.protocol uses SSL"`) inline. Rationale captured: substring is sufficient because the missing-key name is the load-bearing diagnostic and is resilient to harmless suffix additions (e.g. a remediation hint); if a refactor changes the key name itself, the test surfaces it. Symmetric SASL test (`public_new_rejects_sasl_plaintext_without_credentials`) left as substring for balance — Critic explicitly marked this acceptable.
- **N1 — Dev-notes-style comment block in `NoHostnameVerifier::verify_server_cert`.** **Resolved**: replaced the 3-candidate "Easier path … Cleanest approach …" walkthrough at `src/common/security/ssl/mod.rs:354-367` with a single-paragraph description of what the code does (delegate chain validation to `WebPkiServerVerifier` with a static placeholder hostname, translate the two name-mismatch error variants into success, propagate every other error). The Phase 9c.1 design-decision rationale is preserved in NOTES.md.
- **N2 — SSL integration test used the old `Arc::try_unwrap` ceremony; byte-fidelity test in same commit used `Arc::into_inner`.** **Resolved**: replaced `Arc::try_unwrap(producer).map_err(|_| ()).expect(...)` at `tests/integration/producer_smoke_test.rs:777-779` with `Arc::into_inner(producer).expect(...)` — one-line consistency fix. Three older pre-existing sites (lines 609, 968, 1157) left untouched (out of Phase 9c R1 scope; can be folded into a future cleanup pass).

---

## Critic 9 — Phase 9d Rounds 1+2 review — Round 3 resolution

Resolutions for the 1 Suggestion + 2 Nits raised by Critic 9 (2026-05-22) against the Phase 9d Rounds 1+2 commit ladder (`111dcad..d7a4bf4`).

### Original Critic 9 review summary

Reviewed six commits:

| Commit | Subject |
|---|---|
| `111dcad` | Phase 9d (1/N): producer-smoke SASL_PLAINTEXT integration test + Java-runtime cross-verification of SASL wire types |
| `2073c26` | Phase 9d (2/N): retire "awaiting Java-runtime" rustdoc on SASL hex fixtures |
| `89f90c1` | Phase 9d (final/N): sub-phase 9d close stanza in NOTES.md |
| `440e1bb` | Phase 9d Round 2 fixup — selector readability filter Java parity (fixup! 111dcad) |
| `2ad2760` | Phase 9d Round 2 — live verification: producer_smoke_sasl_plaintext_1000_records passes against real broker |
| `d7a4bf4` | Phase 9d Round 2 close — Selector readability filter fix verified live |

Critic 9 confirmed: (a) `selector.rs:1176` (production) and `selector.rs:463` (debug accessor) are textually identical filters — the regression-pin test exercises the same predicate the production poll loop uses; (b) Java parity claims against `PlaintextTransportLayer.java:48-53`, `Selector.java:525-548`, `KafkaChannel.java:252-271` are faithful (OP_READ is set at `finishConnect()` and removed only by `mute()`); (c) the `wait_any_transport_readable_includes_mid_handshake_channels` unit test is well-constructed with discriminating power against both the bug-fix invariant and the Java mute parity; (d) live-run evidence (8.35 s, cluster ID `5L6g3nShT-eMCtK--X86sw`) is from a real broker and consistent with PLAINTEXT (10.37 s) and SSL (15.68 s) sibling tests. Recommendation: **Accept-with-followups.**

### Phase 9d Rounds 1+2 — resolved

- **S1 — Response-fixture rustdoc overclaims what Phase 9d empirically verifies** (Suggestion, NOT BLOCKING). **Resolved (`1806ad0`)**: tightened the provenance-status rustdoc on the two response hex fixtures (`hex_fixture_v0_gssapi_only` in `sasl_handshake_response.rs:208-218` and `hex_fixture_v0_success_null_message` in `sasl_authenticate_response.rs:334-348`) to acknowledge the request/response encoding/decoding asymmetry. Production code never encodes `SaslHandshakeResponse` / `SaslAuthenticateResponse` — the broker is the encoder and Rust is the decoder on the response side. The integration test reaching 1000 acks confirms (a) Rust's request encoder bytes match Java's expected wire input (broker decodes them), and (b) Rust's response decoder correctly parses Java's wire output. It does NOT exercise Rust's response encoder. The fixtures' asserted byte-strings therefore remain hand-derived against spec rules and pinned by round-trip with the decoder, not anchored to Java's encoder output. Also updated `NOTES.md` close-stanza wording in three places (`111dcad` commit summary, Decision 2 hex-fixture cross-verification narrative, `2073c26` retire-pass scope) to call out the same asymmetry: request retirement is broker-anchored, response retirement is decoder-anchored. Request-side hex fixtures (`sasl_handshake_request.rs:238`, `sasl_authenticate_request.rs:330`) left UNCHANGED — Critic explicitly approved them as "genuinely empirically validated."
- **N1 — Round 1 commit message claim "Live integration run not exercised in this environment" contradicts the close-stanza claim "ran live with Docker available but FAILED"** (Nit, NOT BLOCKING). **No action — historical artifact.** Critic 9 explicitly classified N1 as no-action: "No action required for Phase 9d (committed; archived as historical artifact). For future phases, when the commit-N status changes between commit-N and commit-N+M, a one-line correction note in commit-N+M's body ... preempts this kind of reader-stumble." The git history of commits `111dcad` and `89f90c1` is immutable; the inconsistency is a between-commit stale status line, not a defect in the present working tree. Acknowledged as a process improvement for future phases.
- **N2 — `debug_insert_channel` and `debug_readable_watch_transport_ids` are `#[cfg(test)]` but warrant a one-line "test seam only" comment block** (Nit, NOT BLOCKING). **Resolved (`1806ad0`)**: added single-line separator comments wrapping the two `#[cfg(test)]` test seams at `src/common/network/selector.rs:453-481`: `// --- Phase 9d Round 2: test-only seams below (#[cfg(test)] gated; absent in published crate) ---` above and `// --- end Phase 9d Round 2 test-only seams ---` below. The seams were NOT moved to the test module because they need to be on `impl Selector` to access private state (`self.channels`); the `#[cfg(test)]` attribute plus separator is the right shape for "sibling-of-production-helpers but absent at runtime."

Phase 9d Round 3 closes; Phase 9d ready for final manager close.

---

## Critic 9 — Phase 9e Round 1 review (2026-05-23) — zero findings

Critic 9 review of the Phase 9e commit ladder (`a2de19d..607dc68`) returned **0 Suggestions + 0 Nits**.

### Commits reviewed

| Commit | Subject |
|---|---|
| `a2de19d` | Phase 9e (1/N): producer-smoke SASL_SSL integration test — TLS + PLAIN combined transport |
| `607dc68` | Phase 9e (final/N): sub-phase 9e close stanza in NOTES.md |

### Verification performed (per Critic 9 review entry)

- **Side-by-side diff with siblings** (SSL `producer_smoke_ssl_1000_records` and SASL_PLAINTEXT `producer_smoke_sasl_plaintext_1000_records`). The new `producer_smoke_sasl_ssl_1000_records` is the structural union of the two with the expected delta: combined truststore + JAAS plumbing, `security.protocol = "SASL_SSL"`, `ctx.sasl_ssl_bootstrap_servers()` bootstrap, distinct `client.id`. The five assertions (ack count, RecordMetadata shape + partition consistency, monotonic offsets, multi-partition coverage) are byte-for-byte identical to the SSL/SASL_PLAINTEXT siblings; assertion comment markers `(1)..(5)` match.
- **Truststore lifecycle.** `truststore_file` binding kept alive to the explicit `drop(truststore_file)` after `close_with_timeout` — matches the SSL test shape. No premature-unlink risk.
- **`Arc::into_inner` consistency.** Matches the SSL test (line 777) and SASL_PLAINTEXT test (line 963) — the two immediate sibling 1000-record tests.
- **Production path exercised.** Reaching ack #1000 over `security.protocol = SASL_SSL` requires `SaslChannelBuilder::build_sasl_ssl_channel` (`src/common/network/sasl_channel_builder.rs:164`), `KafkaChannel::prepare` (`src/common/network/kafka_channel.rs:257-273` enforces `transport.handshake()` FIRST then `authenticator.authenticate(transport)`, matching Java parity at `Selector.java:529-548`), and the post-9d-Round-2 readability filter (`src/common/network/selector.rs:1180`: `c.transport_layer_ref().is_open() && !c.is_muted()`). A one-leg-only test would either fail TLS (wrong listener) or fail SASL (no creds) — the test does exercise the combined transport.
- **Rustdoc accuracy.** All claims (combined TLS+SASL, references to 9c/9d, post-9d-Round-2 readability filter, IP-SAN matching, `sasl.jaas.config` parity, Phase 9b unit-pinning of the username/password shortcut) cross-verified against the codebase.
- **Close-stanza completeness.** Both commits listed; decisions documented (single hostname-check variant, single credential path, no wiring changes); deferrals (9f auth-failure, 9g unsupported-mechanism, 9h flakiness, 9i CCloud) carried from 9d's `9e+` list with only 9e removed — clean carryover, no silent drops; Java tests intentionally not translated documented (`SaslAuthenticatorTest.test*Ssl*`, `SslSelectorTest.test*Sasl*`); status reported (1343 lib tests; integration test passed in 7.84 s; cluster `5L6g3nShT-eMCtK--X86sw`).
- **Local re-verification by Critic.** `cargo build --tests --features integration-tests` OK. `cargo xtask format-check` OK. `cargo xtask lint` clean. `cargo test --lib` 1343 passed. `cargo test --lib producer::internals::buffer_pool` — all 14 passed on Critic's run; Actor's reported transient flake not reproducible, and would not be a 9e bug regardless (no production code changes).
- **Java parity edge cases.** `KafkaChannel::prepare` enforces `transport.ready() → authenticator.complete()` ordering exactly as Java's `Selector.poll → channel.prepare()` does. Selector readability filter watches mid-SASL-over-TLS channels (covered by parity per 9d Round 2; empirically confirmed by 9e's first-try pass).

### Phase 9e — no findings

The phase is correct, internally consistent, conservatively scoped (single hostname variant, single credential path — well-justified), and integration-test verified end-to-end against Apache Kafka 4.2. The first-try pass against a real broker is strong evidence that the parity argument from 9d Round 2 generalises to two sequential mid-channel handshake phases.

**Recommendation: close 9e immediately — accept-with-no-followups. No Round 2 needed.**

Phase 9e closes.

---

## Critic 9 — Phase 9f Round 1 review (2026-05-25) — zero findings

Critic 9 review of the Phase 9f commit ladder (`e4fd8ec..621ce55`) returned **0 Suggestions + 0 Nits**.

### Commits reviewed

| Commit | Subject |
|---|---|
| `e4fd8ec` | Phase 9f (1/N): producer-smoke SASL auth-failure integration tests (PLAINTEXT + SSL) |
| `621ce55` | Phase 9f (final/N): sub-phase 9f close stanza in NOTES.md |

`git diff 1d0f5a7..621ce55 -- 'src/'` is empty — pure test-add phase, no production code changes.

### Verification performed (per Critic 9 review entry)

- **Java parity literal cross-verified.** Read `kafka/clients/src/main/java/org/apache/kafka/common/security/plain/internals/PlainSaslServer.java:106` — the broker emits exactly `"Authentication failed: Invalid username or password"` (literal match). Read `SaslAuthenticatorTest.java:278` — Java's own canonical test asserts the same literal, and `testInvalidUsernameSaslPlain:295` asserts the **identical** string, confirming Actor's claim that wrong-password and unknown-user collapse onto a single broker message — so a single per-listener test suffices to pin both Java assertions at integration level. Read `SaslServerAuthenticator.java:476-479` — `e.getMessage()` is packed verbatim into `SaslAuthenticateResponse.errorMessage`. The propagation chain Actor documented is correct end-to-end.
- **`KafkaError::Display` prefix wrinkle verified.** Confirmed `src/common/network/kafka_channel.rs:291` uses `e.to_string()` to capture the wrapped `io::Error::other(KafkaError::Authentication(...))`. Confirmed `src/common/errors.rs:502-510` renders `KafkaError` as `"<java_class_name>: <message>"` and line 403 maps `Authentication(_) → "AuthenticationException"`. So the final wire message is `"AuthenticationException: Authentication failed: Invalid username or password"`, with the broker substring intact. `.contains()` substring matching is the correct compromise; the documented cleanup follow-up at `kafka_channel.rs:291` is real, and the deferral to 9g+ is acceptable for a test-only phase.
- **Malformed-JAAS deferral coverage verified.** Read `src/common/security/jaas_config.rs:267-381` — counted **10** negative-path unit cases (Actor's claim of "8 explicit malformed cases" is conservative). Integration-level malformed-JAAS retest would not add wire-level evidence. Deferral justified.
- **Test fails fast — not via timeout-masking.** Structural analysis: with `max.block.ms = 15000`, a broken auth-failure notify path would return `Err(KafkaError::Timeout)` (not `Authentication`) after ~15 s — the test's `match` arm `other => panic!("expected KafkaError::Authentication, got {other:?}")` correctly distinguishes "broker rejected" from "client timed out waiting". The `elapsed < 30s` assertion catches the partial-degradation case (returns `Authentication` slowly). Live timings (~313 ms PLAINTEXT, ~388 ms SSL) are well under both ceilings — proves fast-fail is intrinsic to the producer's metadata fatal-error notify path, not a timeout-masking artifact.
- **Wire-level propagation chain spot-checked.** `PlainSaslServer.java:106` → `SaslServerAuthenticator.java:476-479` → `SaslClientAuthenticator::handle_sasl_authenticate_response` → `KafkaChannel::prepare` → `NetworkClient::process_disconnection` → `DefaultMetadataUpdater::handle_server_disconnect` → `metadata.fatal_error` → `await_update`'s `Notify::notify_waiters` → `wait_on_metadata` → `do_send_inner` → `send()`. The outer `.send().await` returns `Err(Authentication)` directly because `wait_on_metadata` propagates the fatal error from the metadata layer before any record-future is constructed — no need to also call `.get().await`. Sufficient test shape.
- **Test shape consistent with 9d/9e siblings.** Same helpers (`TestContext`, `cluster_pool::get_or_create`, `create_topic`, `PLAIN_LOGIN_MODULE`, `SASL_USERNAME`), same `Arc::into_inner` + `close_with_timeout(30s)` cleanup, same inline `HashMap<String, String>` props composition, same physical location in `producer_smoke_test.rs` (right after `producer_smoke_sasl_ssl_1000_records`). No structural drift.
- **No new structs/traits.** `grep -n "^struct\|^enum\|^trait" tests/integration/producer_smoke_test.rs` returns only the pre-existing `StderrLogger` from Phase 8a. CLAUDE.md rule 7 satisfied.
- **`cargo check --features integration-tests --tests` clean by Critic.** No warnings, no errors.
- **Close-stanza format consistent with 9e.** Commit ladder ✓, decisions made ✓ (4 decisions, all named and justified), deferrals carried into 9g+ ✓ (including the new Display-prefix cleanup follow-up), Java tests intentionally not translated ✓ (`testInvalidUsernameSaslPlain` collapses with `testInvalidPasswordSaslPlain` at the broker, `testMissingUsernameSaslPlain` is JAAS-config-validation level, SCRAM tests out of Milestone-1, re-auth permanently skipped), all-gates-green status ✓ with live timings + cluster ID (`5L6g3nShT-eMCtK--X86sw`).

### Phase 9f — no findings

The phase is correct, internally consistent, conservatively scoped (one wrong-password test per listener — well-justified by Java's single-message collapse at `PlainSaslServer.java:106`), and integration-test verified end-to-end against Apache Kafka 4.2. The deferred Display-prefix cleanup at `kafka_channel.rs:291` is correctly flagged as a production-code refinement out of 9f test-only scope; the `.contains()` substring assertion is robust under either rendering (prefixed or stripped), so there is no test-fragility risk from accepting the deferral.

The fatal-error wakeup path in `process_disconnection` → `handle_server_disconnect` → `metadata.fatal_error` → `notify_waiters` exists in the live codebase, and the empirical sub-400 ms surface time on both listeners confirms the notify path is wired. The test is not just structurally sound — it's an empirical regression pin against `handle_server_disconnect` ever silently swallowing the captured exception.

**Recommendation: close 9f immediately — accept-with-no-followups. No Round 2 needed.**

Phase 9f closes.

---

## Critic 9 — Phase 9g Round 1 review (2026-05-25) — zero findings

Critic 9 review of the single Phase 9g commit `c5d485a` (zero-code-change scope-resolved close) returned **0 Suggestions + 0 Nits**.

### Commit reviewed

| Commit | Subject |
|---|---|
| `c5d485a` | Phase 9g: scope-resolved to Option A (zero-code-change); sub-phase 9g close stanza in NOTES.md |

`git diff 85e137c..c5d485a -- 'src/'` is empty — pure NOTES.md + memory-file commit, no production code drift.

### Scope-ambiguity resolution

The Phase 9 ladder line 52 read 9g as a client-side validator unit test; the Phase 9e/9f close stanzas reframed it as a broker-handshake integration pin. Actor 9 was given three options (A: zero-code-change if both scopes already pinned; B: scenario-1 unit-test gap; C: scenario-2 integration-test gap) and chose **A** with citations. Critic verified both load-bearing claims independently.

### Claim 1 verification — Scenario (1) client-side validator rejection, pinned by Phase 9b

Read each cited test in `src/producer/producer_config.rs`:

- `test_sasl_scram_mechanism_rejected` (line 1916) — exists, asserts `matches!(err, KafkaError::Config(_))` + `.contains("Unsupported SASL mechanism: SCRAM-SHA-512")`. Exact mechanism named in ladder line 52.
- `test_sasl_oauthbearer_mechanism_rejected` (line 1937) — exists, identical shape, `.contains("Unsupported SASL mechanism: OAUTHBEARER")`. Covers the "(or similar)" of ladder line 52.
- `test_sasl_ssl_default_mechanism_rejected_in_milestone_1` (line 1865) — exists, `.contains("Unsupported SASL mechanism: GSSAPI")`. Covers the GSSAPI/Kerberos axis.
- `test_sasl_plaintext_plain_mechanism_accepted` (line 1885) — positive control, `PLAIN` accepted.
- `test_non_sasl_protocol_ignores_sasl_mechanism` (line 1961) — negative control, `PLAINTEXT` + `SCRAM-SHA-256` accepted (security-protocol gating).

All five tests use `KafkaError::Config(_)` variant matching AND specific substring assertions — **DoD #3 met on every rejection test**. The validator path at `producer_config.rs:1252-1272` (`reject_milestone_1_unsupported_sasl_mechanism`) is invoked from `post_process_parsed_config:1167` synchronously inside `ProducerConfig::new(...)` before any network IO — "at construction time" verified at the call-graph level. Mechanism-set audit: skip list (`NOTES.md:83`: SCRAM, OAUTHBEARER, Kerberos/GSSAPI) is fully covered.

### Claim 2 verification — Scenario (2) broker-handshake rejection, pinned by Phase 9a unit test

Read `src/common/security/authenticator/sasl_client_authenticator.rs:1122-1164` — `handshake_unsupported_mechanism_fails_with_java_message` exists. Drives the handshake state machine past `ApiVersions` via `MockTransport`, broker responds with `Errors::UnsupportedSaslMechanism` (wire code 33) + `enabled_mechanisms = ["SCRAM-SHA-512"]`. Asserts `matches!(kafka_err, KafkaError::Authentication(_))` + `is_fatal()` + `!is_retriable()` + `state == SaslState::Failed` + **`assert_eq!`** (not `.contains()`) on:

```
"Client SASL mechanism 'PLAIN' not enabled in the server, enabled mechanisms are [SCRAM-SHA-512]"
```

**Java fixture cross-check** — Read `kafka/clients/src/main/java/org/apache/kafka/common/security/authenticator/SaslClientAuthenticator.java:609-610`:

```java
throw new UnsupportedSaslMechanismException(String.format(
    "Client SASL mechanism '%s' not enabled in the server, enabled mechanisms are %s",
    mechanism, response.enabledMechanisms()));
```

Where `response.enabledMechanisms()` returns `List<String>` whose `toString()` is `[item1, item2]` (no inner quotes). Substituting `mechanism="PLAIN"` and `enabledMechanisms()=["SCRAM-SHA-512"]` yields a byte-exact match with the Rust test fixture. The `assert_eq!` against a verified fixture is **strictly stronger** than what an integration test could pin (which would degrade to `.contains(...)` because the broker may add contextual text). Actor's strength claim holds.

### Side claim — Test broker hard-coded to PLAIN

`tests/common/kafka_cluster.rs:202` is `env_vars.insert("KAFKA_SASL_ENABLED_MECHANISMS".into(), "PLAIN".into());`. No override path exists. Adding scenario-(2) live coverage would require a second cluster image or runtime broker reconfig + SCRAM/OAUTHBEARER server-side credential plumbing — out of Milestone-1 scope per `PLAN.md:365`. Actor's "ruling out cheap scenario-(2) live testing" justification is sound.

### Other checks

- `cargo test --lib` count = **1343** (unchanged from 9d/9e/9f baseline).
- No new structs / traits (NOTES.md + memory files only).
- Close-stanza format consistent with 9e/9f: explicit option-and-justification, commit ladder, scope-resolution evidence (Scenario 1 + Scenario 2 sub-sections), why-not-Options-B/C exhaustive argument, Java parity check (`SaslAuthenticatorTest.testInvalidMechanism` documented as Java-SPI-lookup contract-parity-not-string-parity), 3 numbered decisions, deferrals carried forward (9h flakiness gate, 9i CCloud env-var test, `kafka_channel.rs:291` Display-prefix cleanup), Java tests intentionally not translated with rationale, all-gates-green status.
- No CLAUDE.md / agent-roles.md rule mandates a new test for 9g. The DoD's rule 3 is met by virtue of the 9b/9a tests already pinning the contracts. Actor is not papering over.
- Secondary citations spot-checked: `KafkaError::Authentication(String)` at `errors.rs:157`, `is_fatal()` true at `errors.rs:300`, non-retriable at `errors.rs:242`, `UnsupportedSaslMechanism` wire code 33 at `errors.rs:75`.

### Phase 9g — no findings

Both load-bearing claims independently verified by reading the named files. The Option A close is defensible: ladder-line-52 scope is fully pinned by 5 Phase 9b unit tests with `KafkaError::Config(_)` variant matching + substring message assertions; the 9e/9f-reframed scope-2 is pinned by a Phase 9a unit test with `assert_eq!` against a fixture byte-verified against Java's `String.format` output at `SaslClientAuthenticator.java:609`. The 1343 lib-test baseline is preserved. Zero risk to the codebase.

**Recommendation: close 9g immediately — accept-with-no-followups. No Round 2 needed.**

Phase 9g closes.
