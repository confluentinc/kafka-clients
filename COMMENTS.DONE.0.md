# Resolved Comments -- Critic 0 Review Phase 2

## RESOLVED: has_bytes_buffered() always returns false for SSL

- **Fix**: Changed `has_bytes_buffered()` to check `!conn.wants_read()` on the `rustls::ClientConnection`. The `wants_read()` method returns `false` when the internal `received_plaintext` buffer is non-empty, which is the correct indicator of buffered plaintext data.
- **Commit**: fixup of 36d740b

## NOT AN ISSUE: aws-lc-rs + cmake pulled as transitive dependency

- **Reason**: User decision to use `aws-lc-rs` (FIPS compliant). Switched from `ring` to `aws-lc-rs` as the crypto backend intentionally.

## RESOLVED: NoHostnameVerifier does not catch NotValidForNameContext variant

- **File**: `src/common/security/ssl/ssl_factory.rs`
- **Description**: The `NoHostnameVerifier::verify_server_cert()` method only caught `CertificateError::NotValidForName`, but rustls 0.23.37 WebPkiServerVerifier produces `CertificateError::NotValidForNameContext { expected, presented }` for hostname mismatches. This meant setting `endpoint_identification_algorithm=""` did NOT actually disable hostname verification.
- **Fix**: Added match arm for `NotValidForNameContext { .. }` alongside `NotValidForName`. Both variants are now caught and suppressed when hostname verification is disabled.
- **Tests added**:
  1. `test_no_hostname_verifier_accepts_mismatched_hostname` — verifies hostname mismatch is suppressed
  2. `test_no_hostname_verifier_accepts_matching_hostname` — verifies matching hostname still works
  3. `test_no_hostname_verifier_rejects_untrusted_cert` — verifies non-hostname errors are NOT suppressed
