# Critic 4 review — Phase P1 (producer-metrics core) — RESOLVED

## Issue 1: `metrics.recording.level` validator is case-insensitive; Java rejects any non-uppercase value

- **File**: `src/producer/producer_config.rs` (METRICS_RECORDING_LEVEL_CONFIG arm in `from_properties`)
- **Severity**: Behavior Mismatch (minor — over-permissive validation)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java:461-464` —
  `in("INFO", "DEBUG", "TRACE")`; `ConfigDef.ValidString.in(...).ensureValid` is an exact, case-sensitive
  membership check (`ConfigDef.java:1103` throws `ConfigException(name, o, "String must be one of: " + ...)`).

### Disposition — FIXED

- Replaced `let uc = value.to_ascii_uppercase();` + `uc != "INFO" && ...` with an exact case-sensitive
  comparison of the raw `value` against `"INFO" / "DEBUG" / "TRACE"`. Lowercase/mixed-case values
  (e.g. `debug`, `Info`) are now rejected, matching Java.
- Aligned the rejection error text with Java's `ConfigException` wording (consistent with the
  `num.samples` / `sample.window.ms` arms): the message now ends with
  `Invalid value <v> for configuration metrics.recording.level: String must be one of: INFO, DEBUG, TRACE`
  (the `KafkaError` Display prepends `IllegalArgumentError: `).
- Updated `test_metrics_recording_level_validator`: asserts all three uppercase enum values are accepted,
  that lowercase `debug` is rejected, and asserts the exact Java-style error wording (DoD #3) for both
  `debug` and `bogus`.

### Note — ConsumerConfig has the SAME bug (out of scope for this loop)

`src/consumer/consumer_config.rs:806-816` (`METRICS_RECORDING_LEVEL_CONFIG` arm) has the identical
`value.to_ascii_uppercase()` case-insensitivity plus the old non-Java error wording
(`Invalid value for '<key>': <value>`). NOT changed here — consumer code is out of scope for this
producer-metrics loop. Flagged for the Manager to decide whether to open a follow-up.

Verified: `cargo build`, `cargo test` (all pass), `cargo xtask format`, `cargo xtask lint` (clean).

---

# Critic 4 review — Phase P2 (sender metrics) — RESOLVED

## Issue 2: Duplicated `#[allow(clippy::too_many_arguments)]` attribute (cleanliness)

- **File**: `src/producer/internals/sender.rs:403-404`
- **Severity**: Cleanliness (non-blocking; no functional impact)
- **Introduced by**: `d2d3f54a` (added the `metrics: SenderMetricsRegistry` parameter).

### Disposition — FIXED

- Removed the duplicated second `#[allow(clippy::too_many_arguments)]` line on
  `Sender::new`, leaving a single attribute. No functional change; lint stays green.
