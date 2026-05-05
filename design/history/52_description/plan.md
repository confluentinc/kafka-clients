# Translation Design: KAFKA-19875 — Duplicated topic config prevents broker start

**AK commit:** `e8459d3864e5387f656e1f9ddb711a61d26ec942`
**AK branch:** trunk
**PR:** #52
**Rust branch:** `kafka-translate/e8459d3864e5387f656e1f9ddb711a61d26ec942`

---

## Summary of the Java Commit

KAFKA-19875 fixes a regression where a duplicated entry in a LIST-type
broker configuration (e.g. `ssl.cipher.suites=TLS_AES_256_GCM_SHA384,TLS_AES_256_GCM_SHA384`)
caused the broker to refuse to start with a `ConfigException`.

The fix is in two parts:

### 1. `ConfigDef.parse()` — silent deduplication with warning

In `ConfigDef.java`, the `parse()` method is updated so that when a
`List`-type config uses a `ValidList` validator, duplicate entries are
silently removed from the parsed value and a `WARN`-level log message is
emitted instead of throwing. This maintains backward compatibility: a
configuration that was previously valid (before strict duplicate
rejection was added) is again accepted, with a warning nudging the
operator to fix it.

```java
if (key.validator instanceof ValidList && parsedValue instanceof List) {
    List<?> original = (List<?>) parsedValue;
    parsedValue = original.stream().distinct().collect(Collectors.toList());
    if (original.size() \!= ((List<?>) parsedValue).size()) {
        LOGGER.warn("Configuration key \"{}\" contains duplicate values...", key.name, ...);
    }
}
```

### 2. Validator additions on specific LIST configs

Several existing LIST-type config definitions in
`BrokerSecurityConfigs`, `DefaultConfigPropertyFilter`,
`DefaultTopicFilter`, and `AllowlistConnectorClientConfigOverridePolicy`
are updated to explicitly use `ConfigDef.ValidList.anyNonDuplicateValues(true, false)`:

| Config key | Class |
|---|---|
| `ssl.cipher.suites` | BrokerSecurityConfigs |
| `sasl.enabled.mechanisms` | BrokerSecurityConfigs |
| `sasl.kerberos.principal.to.local.rules` | BrokerSecurityConfigs |
| `sasl.oauthbearer.expected.audience` | BrokerSecurityConfigs |
| `config.properties.exclude` | DefaultConfigPropertyFilter (Mirror) |
| `topics` / `topics.exclude` | DefaultTopicFilter (Mirror) |
| `allowlist` | AllowlistConnectorClientConfigOverridePolicy |

### 3. Minor: `ValidList.toString()` fix

`ValidList.toString()` is fixed to return an empty string when
`validStrings` is empty (previously it printed the null-/empty-allowed
suffix even when there were no valid strings to display).

### 4. Test updates

- `ConfigDefTest`: new test `testParsedValueWillRemoveDuplicatesInValidList`
  proves the dedup happens during `parse()`, not just at validation time.
  Existing `testListValidatorAnyNonDuplicateValues` is expanded to cover
  additional edge cases (mixed-list with an empty element, `false/false` combo).
- `KafkaConfigTest`: two tests that expected a `ConfigException` for
  duplicate listener configs are removed — duplicates are now silently
  dropped.
- `LogConfigTest`: `cleanup.policy=delete,delete,delete` is now valid
  (duplicates dropped), replacing the previous `delete` single-value test.

---

## Applicability to the Rust Client Library

### Out-of-scope Java changes

The configs receiving the new `ValidList` validator are all **broker-side**
or **Kafka Connect** plugin configs:

- `BrokerSecurityConfigs` — broker security configs (not implemented in
  the Rust client library)
- `DefaultConfigPropertyFilter` / `DefaultTopicFilter` — Mirror Maker 2
  Connect plugins (not in scope)
- `AllowlistConnectorClientConfigOverridePolicy` — Kafka Connect worker
  policy (not in scope)

These classes have no Rust counterparts and require no translation.

### `ConfigDef` framework — no Rust equivalent

The Rust library does **not** implement the `ConfigDef` dynamic-config
framework. Instead, configuration is represented as typed Rust structs
(`ProducerConfig`, `SslConfig`, `SaslConfig`, etc.) with a
`from_properties(HashMap<String, String>)` parsing method.

The `ValidList` validator class, `ConfigDef.parse()`, and the
`ValidList.toString()` fix are therefore not directly applicable.

### In-scope spirit: dedup behavior for client list configs

Although no specific client LIST config is required by the Java commit
to gain the dedup treatment, the **intent** — silently accepting
duplicate list entries with a warning for backward compatibility — is
sound to apply uniformly. The Rust library currently parses two
list-typed configs from string form:

| Rust field | Config key | Parsing location |
|---|---|---|
| `ProducerConfig::bootstrap_servers` | `bootstrap.servers` | `producer_config.rs::from_properties()` |
| `SslConfig::enabled_protocols` | `ssl.enabled.protocols` | `producer_config.rs::parse_ssl_config()` |

Both currently use the pattern:

```rust
value.split(',').map(|s| s.trim().to_string()).collect()
```

This does not deduplicate. To match the Java change's spirit, both sites
should be updated to deduplicate (preserving insertion order) and emit a
`warn\!` log if any duplicates were removed.

---

## Rust Implementation Plan

### Files to change

#### `src/producer/producer_config.rs`

1. **Add a private helper function** `parse_list_dedup(key, value) -> Vec<String>`:
   - Split `value` on `,` and trim whitespace from each token
   - Deduplicate while preserving first-occurrence order (iterate, insert
     into a seen `HashSet`, keep token only if not already seen)
   - If the deduplicated length differs from the original, emit:
     `warn\!("Configuration key \"{}\" contains duplicate values. Duplicates will be removed. The original value is: {:?}, the updated value is: {:?}", key, original, deduped)`
   - Return the deduplicated `Vec<String>`

2. **Update `BOOTSTRAP_SERVERS_CONFIG` parsing** to use `parse_list_dedup`.

3. **Update `SSL_ENABLED_PROTOCOLS_CONFIG` parsing** in `parse_ssl_config`
   to use `parse_list_dedup`.

### Files NOT changed

| Java file | Reason not translated |
|---|---|
| `ConfigDef.java` | No `ConfigDef` framework in Rust |
| `BrokerSecurityConfigs.java` | Broker-only, out of scope |
| `DefaultConfigPropertyFilter.java` | Connect plugin, out of scope |
| `DefaultTopicFilter.java` | Connect plugin, out of scope |
| `AllowlistConnectorClientConfigOverridePolicy.java` | Connect plugin, out of scope |
| `ConfigDefTest.java` | `ValidList`/`ConfigDef` not translated |
| `KafkaConfigTest.scala` | Broker tests, out of scope |
| `LogConfigTest.scala` | Broker tests, out of scope |

---

## Test Plan

Add tests in `src/producer/producer_config.rs` (`#[cfg(test)]` module):

1. **`test_bootstrap_servers_dedup`**: parse
   `bootstrap.servers=host1:9092,host2:9093,host1:9092` and assert the
   resulting `bootstrap_servers` is `["host1:9092", "host2:9093"]`
   (length 2, not 3).

2. **`test_ssl_enabled_protocols_dedup`**: parse
   `ssl.enabled.protocols=TLSv1.3,TLSv1.2,TLSv1.3` and assert
   `enabled_protocols` is `["TLSv1.3", "TLSv1.2"]`.

3. **`test_parse_list_dedup_no_duplicates`**: verify that a list with no
   duplicates passes through unchanged and no warning is emitted (can be
   verified by confirming the returned vec length matches the split count).
