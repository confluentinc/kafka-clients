## Review: Commit 0d141bd — SASL handshake and authenticate request/response types (Phase 3)

All four Java classes are fully translated with correct method coverage and correct dispatch in ConcreteRequest/ConcreteResponse. Throttle behavior, error codes, and `should_client_throttle` all match Java semantics. The network_client.rs wildcard match is a reasonable defensive measure. Two issues found:

---

## Issue: Missing Java tests from RequestResponseTest.java

- **File**: `src/common/requests/sasl_handshake_request.rs`, `src/common/requests/sasl_authenticate_request.rs`
- **Severity**: Missing Requirement
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/common/requests/RequestResponseTest.java:3898-3984`
- **Description**: Four SASL-related tests in `RequestResponseTest.java` are not translated:
  1. `testInvalidSaslHandShakeRequest` (line 3898) -- serializes a SaslHandshakeRequest with mechanism "PLAIN", corrupts the mechanism string length to `Short.MAX_VALUE`, asserts `parse_request` fails with the expected error message about insufficient bytes.
  2. `testInvalidSaslAuthenticateRequest` (line 3911) -- serializes a SaslAuthenticateRequest at version 1, corrupts the auth_bytes array length to `Integer.MAX_VALUE`, asserts parse fails with expected error message.
  3. `testValidTaggedFieldsWithSaslAuthenticateRequest` (line 3933) -- manually constructs a byte buffer with a SASL_AUTHENTICATE request at the latest version including a valid tagged field, parses it, verifies authBytes and unknown_tagged_fields are preserved.
  4. `testInvalidTaggedFieldsWithSaslAuthenticateRequest` (line 3961) -- same as above but with a corrupted tagged field size (`Short.MAX_VALUE`), verifies parse fails with expected error.
- **Expected**: All four tests should be translated per the Definition of Done (rule 3: "Are all tests using those classes translated?")
- **Actual**: Only basic roundtrip, builder, error response, and display tests exist. The corruption and tagged-field parse tests are missing.

---

## Issue: Redaction Display tests pass trivially without the redaction code

- **File**: `src/common/requests/sasl_authenticate_request.rs:193-203`, `src/common/requests/sasl_authenticate_response.rs:192-202`
- **Severity**: Missing Requirement
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/common/requests/RequestResponseTest.java:3987-4006`
- **Description**: The `test_display_redacted` tests check `!display.contains("secret-password")` / `!display.contains("server-secret-token")`. Since `auth_bytes` is `Vec<u8>` and the generated `Display` delegates to `Debug`, byte arrays render as numeric lists (e.g., `[115, 101, 99, ...]`), never as the ASCII string. These tests would pass even if the redaction code were removed entirely. The Java test (`testSaslAuthenticateRequestResponseToStringMasksSensitiveData`) asserts the **positive** condition `assertTrue(requestString.contains("authBytes=[]"))`, verifying the bytes are replaced with an empty array. The Rust tests should similarly assert the positive condition -- that `auth_bytes` in the Display output is empty (e.g., `display.contains("auth_bytes: []")`), not merely that an ASCII rendering of the secret is absent.
- **Expected**: Tests that verify the redaction code is actually working by asserting the output contains empty auth_bytes, matching the Java test.
- **Actual**: Tests that pass trivially regardless of whether redaction code is present.

---

## Review: Milestone 3, Phase 4 — SASL Client Authenticator

**Commits reviewed:** ebe89b1, 8791295
**Reviewer:** Critic 0

---

## Issue (RESOLVED): `receive_kafka_response` does not set state to Failed on parse errors

- **File**: `src/common/security/authenticator/sasl_client_authenticator.rs`
- **Severity**: Bug
- **Java Reference**: `SaslClientAuthenticator.java:568-599`
- **Resolution**: Added `map_err` on `ConcreteResponse::parse_response()` that sets state to `SaslState::Failed` before returning the error. Added `test_parse_error_sets_state_to_failed` test.
- **Fixed in**: 1882a7f

---

## Issue (RESOLVED): `next_correlation_id` integer overflow panic in debug builds

- **File**: `src/common/security/authenticator/sasl_client_authenticator.rs`
- **Severity**: Bug
- **Java Reference**: `SaslClientAuthenticator.java:367-371`
- **Resolution**: Changed `self.correlation_id += 1` to `self.correlation_id = self.correlation_id.wrapping_add(1)` to match Java wrapping semantics. Added `test_correlation_id_wraps_on_overflow` test.
- **Fixed in**: 1882a7f
