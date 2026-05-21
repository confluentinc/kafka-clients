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
