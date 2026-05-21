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
