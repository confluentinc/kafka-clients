# Resolved Comments -- Critic 0 Review Phase 2

## RESOLVED: has_bytes_buffered() always returns false for SSL

- **Fix**: Changed `has_bytes_buffered()` to check `!conn.wants_read()` on the `rustls::ClientConnection`. The `wants_read()` method returns `false` when the internal `received_plaintext` buffer is non-empty, which is the correct indicator of buffered plaintext data.
- **Commit**: fixup of 36d740b

## NOT AN ISSUE: aws-lc-rs + cmake pulled as transitive dependency

- **Reason**: User decision to use `aws-lc-rs` (FIPS compliant). Switched from `ring` to `aws-lc-rs` as the crypto backend intentionally.
