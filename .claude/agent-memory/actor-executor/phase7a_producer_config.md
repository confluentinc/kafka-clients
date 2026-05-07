---
name: Phase 7a ProducerConfig translation
description: ProducerConfig schema + constructor + post-processing landed; Milestone-1 deviations and post-process ordering choices
type: project
---

ProducerConfig translation. Lessons & patterns:

**Architecture: post-processed value overlay.** Java mutates the parsed `Map<String, Object>` in `postProcessParsedConfig`. Rust mirror: `ProducerConfig { inner: AbstractConfig, post_processed: HashMap<String, ConfigValue> }`. All `get_*` accessors check `post_processed` first, then fall back to `inner`. Saves us from rebuilding `AbstractConfig` after post-processing and avoids duplicating the parsed map.

**Range bound trait: `i64: Into<f64>` doesn't exist.** `Range::at_least` / `Range::between` accept `N: Into<f64>`. `i32` works (`From<i32> for f64` exists). `i64` does NOT (would lose precision). For `Long` config keys with bounds, use `Range::at_least(0_f64)`. Same for `Range::between(0, i32::MAX)` — i32::MAX is fine because it's `i32`.

**Post-process ordering matters for error-message parity.** Java's `postProcessParsedConfig` order is fixed; deviating affects which error fires first. Specific ordering choices in our translation:
1. SASL mechanism validate
2. Backoff warning
3. Reconnect backoff override
4. Milestone-1 transactional.id rejection (BEFORE step 4 because step 4's "transactional.id without idempotence" message would fire first under our default `enable.idempotence=false`)
5. `post_process_and_validate_idempotence_configs` (Java step 4)
6. Milestone-1 enable.idempotence=true rejection (AFTER step 5 so `testUpperboundCheckOfEnableIdempotence`'s exact error message wins when both `enable.idempotence=true` AND `max.in.flight=6` are set)
7. `maybe_override_client_id` (Java step 5; runs after rejections so `PRODUCER_CLIENT_ID_SEQUENCE` doesn't burn ids on rejected configurations)

**Test-fixture path for properties files**: use `CARGO_MANIFEST_DIR + "/tests/data/producer.properties"`. Java reads `System.getProperty("user.dir") + "/../config/producer.properties"`. We can't share the kafka/config copy directly because Milestone-1 deviations require commenting out `enable.idempotence=true`.

**`append_serializer_to_config` Rust signature**: takes `Option<&str>` (FQCN strings) instead of Java's `Serializer<?>` instances. Reason: Rust does not perform reflective class loading. Internal map type is `HashMap<String, Option<String>>` — `None` represents "value explicitly null in map" (Java's `configs.put(KEY, null)`); absent keys are "key not in map" (Java's `configs.containsKey(KEY) == false`).

**Java testCaseInsensitiveSecurityProtocol deviation**: Java uses `SASL_SSL.toLowerCase()`. SASL is rejected in Milestone-1, so the test substitutes `Ssl` (mixed-case PLAINTEXT/SSL) to exercise the same case-insensitive validator behaviour. Re-translate to use `SASL_SSL` lowercase form when Phase 9 lands.

**Java testTwoPhaseCommitIncompatibleWithTransactionTimeout SKIPPED**: requires both `enable.idempotence=true` AND `transactional.id`, both rejected in Milestone-1. TODO Phase 9.

**Skipped Java symbols**:
- `static main(String[] args)` — generates HTML doc; Phase 7a does not translate the documentation generator.
- `configDef()` — public Rust accessor exists as `ProducerConfig::config_def() -> &'static ConfigDef`.
