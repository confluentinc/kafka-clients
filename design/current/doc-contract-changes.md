# Documented contract changes

Every Rust or C item whose doc carries a `WARNING:` (a place where the translation
does not keep the Java contract) is listed here. The format and the acceptance flow
are in `.claude/rules/documentation.md` §6. `cargo xtask lint-custom`
(`check-api-docs`) parses this file.

- An entry with an empty `Accepted` is proposed. Agents add proposed entries; only the
  user fills `Accepted`.
- The lint excuses a `WARNING` only for an accepted entry.

Entry format (a numbered list; the four sub-items are required, in this order):

```markdown
1. `crate::module::Type::method`
   - Java member: `org.apache.kafka.….Class#method(Type,Type)`
   - Contract change: what differs from the Java contract
   - Why: the rationale
   - Accepted: YYYY-MM-DD Name
```

## Entries
1. `crate::producer::ProducerConfig::new`
   - Java member: `org.apache.kafka.clients.producer.ProducerConfig#ProducerConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` makes the constructor return a `ConfigError`; Java accepts any mechanism name and fails only at authentication.
   - Why: this client implements only SASL/PLAIN, so failing at construction reports the problem where it is configured instead of at the first connection.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::producer::Partitioner`
   - Java member: `org.apache.kafka.clients.producer.Partitioner`
   - Contract change: Java's partitioner may implement `Monitorable` to register metrics, tagged with `config` and `class`; the Rust partitioner cannot, as `Monitorable` and the `Plugin` wrapper are not translated.
   - Why: the plugin-metrics SPI (KIP-877) is not translated in this client.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::producer::Partitioner::partition`
   - Java member: `org.apache.kafka.clients.producer.Partitioner#partition(String,Object,byte[],Object,byte[],Cluster)`
   - Contract change: on the borrowed send path of `KafkaProducer<Vec<u8>, Vec<u8>>` (records of `&[u8]`), `key` and `value` are `None` even when the record has them; only `key_bytes` / `value_bytes` carry them. Java passes the key and value objects whenever the record has them.
   - Why: building a typed `&Vec<u8>` from the borrowed bytes would allocate per record, against the zero-copy rule (CLAUDE.md §14); the bytes are the same data.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `kafka_producer_ProducerConfig_new`
   - Java member: `org.apache.kafka.clients.producer.ProducerConfig#ProducerConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` fails the constructor with an error answering `kafka_common_Error_is_config_error`; Java accepts any mechanism name and fails only at authentication.
   - Why: the C constructor wraps `crate::producer::ProducerConfig::new` (see its entry).
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::consumer::ConsumerConfig::new`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerConfig#ConsumerConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` makes the constructor return a `ConfigError`; Java accepts any mechanism name and fails only at authentication.
   - Why: this client implements only SASL/PLAIN, so failing at construction reports the problem where it is configured instead of at the first connection (as `crate::producer::ProducerConfig::new`).
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `kafka_consumer_ConsumerConfig_new`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerConfig#ConsumerConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` fails the constructor with an error answering `kafka_common_Error_is_config_error`; Java accepts any mechanism name and fails only at authentication.
   - Why: the C constructor wraps `crate::consumer::ConsumerConfig::new` (see its entry).
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::consumer::ConsumerConfig::DEFAULT_ISOLATION_LEVEL`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerConfig#DEFAULT_ISOLATION_LEVEL`
   - Contract change: the default `isolation.level` is `read_committed`, so a consumer that does not set it returns only committed transactional records; Java's default is `read_uncommitted`, which also returns records of aborted and open transactions.
   - Why: `read_committed` is librdkafka's default, which users of the other Confluent clients expect; `isolation.level=read_uncommitted` restores Java's behaviour.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::admin::AdminClientConfig::new`
   - Java member: `org.apache.kafka.clients.admin.AdminClientConfig#AdminClientConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` makes the constructor return a `ConfigError`; Java accepts any mechanism name and fails only at authentication.
   - Why: this client implements only SASL/PLAIN, so failing at construction reports the problem where it is configured instead of at the first connection (as `crate::producer::ProducerConfig::new`).
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `kafka_admin_AdminClientConfig_new`
   - Java member: `org.apache.kafka.clients.admin.AdminClientConfig#AdminClientConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` fails the constructor with an error answering `kafka_common_Error_is_config_error`; Java accepts any mechanism name and fails only at authentication.
   - Why: the C constructor wraps `crate::admin::AdminClientConfig::new` (see its entry).
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::common::metrics::MetricsReporter`
   - Java member: `org.apache.kafka.common.metrics.MetricsReporter`
   - Contract change: a reporter implementing `ClusterResourceListener` receives no cluster metadata; Java's clients register their configured reporters as cluster resource listeners. No Rust client takes a reporter, since `metric.reporters` is class-valued, and a reporter added through `Metrics::add_reporter` is not registered as a listener.
   - Why: CLAUDE.md §2 forbids class-valued configuration, and Rust has no `instanceof` to discover the listener on a reporter trait object.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::common::serialization::StringSerializer`
   - Java member: `org.apache.kafka.common.serialization.StringSerializer`
   - Contract change: the encoding is always UTF-8; `key.serializer.encoding`, `value.serializer.encoding` and `serializer.encoding` are ignored, and an unknown charset is not rejected.
   - Why: a Rust string is UTF-8 by definition, and its bytes are borrowed as they are, with no copy.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::common::serialization::StringDeserializer`
   - Java member: `org.apache.kafka.common.serialization.StringDeserializer`
   - Contract change: the encoding is always UTF-8 (malformed input becomes U+FFFD); `key.deserializer.encoding`, `value.deserializer.encoding` and `deserializer.encoding` are ignored, and an unknown charset is not rejected.
   - Why: the result is a Rust `String`, which is UTF-8 by definition; other charsets would need a transcoding dependency.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `kafka_common_serialization_StringSerializer_new`
   - Java member: `org.apache.kafka.common.serialization.StringSerializer#StringSerializer()`
   - Contract change: the encoding is always UTF-8; `key.serializer.encoding`, `value.serializer.encoding` and `serializer.encoding` are ignored.
   - Why: the handle wraps `crate::common::serialization::StringSerializer`, which carries the same change.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `kafka_common_serialization_StringDeserializer_new`
   - Java member: `org.apache.kafka.common.serialization.StringDeserializer#StringDeserializer()`
   - Contract change: the encoding is always UTF-8 (malformed input becomes U+FFFD); `key.deserializer.encoding`, `value.deserializer.encoding` and `deserializer.encoding` are ignored.
   - Why: the handle wraps `crate::common::serialization::StringDeserializer`, which carries the same change.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::common::ClusterResourceListener`
   - Java member: `org.apache.kafka.common.ClusterResourceListener`
   - Contract change: `on_update` is never called: no client registers a user's listener, while Java calls `onUpdate` on every interceptor, serializer, deserializer and metrics reporter implementing the interface after each metadata response.
   - Why: the clients take serializers, deserializers and reporters as trait objects and have no `instanceof` check to find the listener; the registration is not translated.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::common::serialization::Serializer`
   - Java member: `org.apache.kafka.common.serialization.Serializer`
   - Contract change: combining a serializer with `ClusterResourceListener` or `Monitorable` has no effect: the client never calls `on_update` on it and `Monitorable` is not translated.
   - Why: see `crate::common::ClusterResourceListener`; Java finds both interfaces with `instanceof`, which a Rust trait object cannot do.
   - Accepted: 2026-10-26 Emanuele Sabellico
1. `crate::common::serialization::Deserializer`
   - Java member: `org.apache.kafka.common.serialization.Deserializer`
   - Contract change: combining a deserializer with `ClusterResourceListener` or `Monitorable` has no effect: the client never calls `on_update` on it and `Monitorable` is not translated.
   - Why: see `crate::common::ClusterResourceListener`; Java finds both interfaces with `instanceof`, which a Rust trait object cannot do.
   - Accepted: 2026-10-26 Emanuele Sabellico
