# Critic 2 Review -- Commit 4b91b6c -- RESOLVED

All 3 issues resolved in commit a2bf7d7.

## Issue 1: Selector created with NO_IDLE_TIMEOUT_MS instead of connections_max_idle_ms -- RESOLVED
- **Fix**: Changed `Selector::with_defaults(NO_IDLE_TIMEOUT_MS, channel_builder)` to `Selector::with_defaults(config.connections_max_idle_ms, channel_builder)` in `from_config_with_compression`.
- **Verification**: Removed unused `NO_IDLE_TIMEOUT_MS` import. Build passes.

## Issue 2: Missing configureDeliveryTimeout validation in from_config -- RESOLVED
- **Fix**: Added `configure_delivery_timeout()` method that validates `delivery_timeout_ms >= linger_ms + request_timeout_ms`. Returns `Err(IllegalArgument)` when inconsistent (matching Java ConfigException for explicitly set values). Default values (120000 >= 5 + 30000) always pass the check.
- **Tests**: Updated `test_delivery_timeout_and_linger_ms_config` to actually test the validation. Added `test_delivery_timeout_valid_config` for the happy path.
- **Verification**: All 33 kafka_producer tests pass.

## Issue 3: Missing ClientUtilsTest translations -- RESOLVED
- **Fix**: Refactored `parse_and_validate_addresses` to properly parse host:port (including IPv6 bracket notation) and validate:
  - Embedded whitespace (newlines, spaces, tabs) after trimming causes immediate error
  - Missing port causes immediate error
  - Invalid port (>65535) causes immediate error
- **Tests translated**: 10 new test cases:
  1. `test_parse_and_validate_addresses_ipv6` -- [::1]:8000
  2. `test_parse_and_validate_addresses_ipv6_and_hostname` -- mixed IPv6 + hostname
  3. `test_parse_and_validate_addresses_hostname_port_preserved` -- port preservation (hostname difference documented)
  4. `test_no_port` -- 127.0.0.1 without port rejected
  5. `test_invalid_port` -- port 70000 rejected
  6. `test_invalid_broker_address_embedded_newlines` -- newlines in single string rejected
  7. `test_invalid_broker_address_leading_space` -- Rust trims leading whitespace (documented difference)
  8. `test_invalid_broker_address_space_separated` -- space-separated in single string rejected
  9. `test_valid_broker_address` -- valid list accepted
  10. Added `parse_host_port` helper function
- **Behavioral difference documented**: Rust trims leading/trailing whitespace (more lenient than Java). Java preserves hostnames in InetSocketAddress; Rust resolves to IP in SocketAddr.
- **Verification**: All 18 client_utils tests pass.
