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
1. `crate::producer::Producer::metrics`
   - Java member: `org.apache.kafka.clients.producer.Producer#metrics()`
   - Contract change: the returned map is a snapshot of the registered metrics, not Java's live unmodifiable view: a metric registered or removed after the call is not reflected in it. Each value is the registry's shared `KafkaMetric`, so its value stays current.
   - Why: the registry lives behind a lock that cannot be lent to the caller; a snapshot of `Arc` handles is the borrow-free equivalent.
   - Accepted:
1. `crate::producer::ProducerConfig::new`
   - Java member: `org.apache.kafka.clients.producer.ProducerConfig#ProducerConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` makes the constructor return a `ConfigError`; Java accepts any mechanism name and fails only at authentication.
   - Why: this client implements only SASL/PLAIN, so failing at construction reports the problem where it is configured instead of at the first connection.
   - Accepted:
1. `crate::producer::Partitioner`
   - Java member: `org.apache.kafka.clients.producer.Partitioner`
   - Contract change: Java's partitioner may implement `Monitorable` to register metrics, tagged with `config` and `class`; the Rust partitioner cannot, as `Monitorable` and the `Plugin` wrapper are not translated.
   - Why: the plugin-metrics SPI (KIP-877) is not translated in this client.
   - Accepted:
1. `crate::producer::Partitioner::partition`
   - Java member: `org.apache.kafka.clients.producer.Partitioner#partition(String,Object,byte[],Object,byte[],Cluster)`
   - Contract change: on the borrowed send path of `KafkaProducer<Vec<u8>, Vec<u8>>` (records of `&[u8]`), `key` and `value` are `None` even when the record has them; only `key_bytes` / `value_bytes` carry them. Java passes the key and value objects whenever the record has them.
   - Why: building a typed `&Vec<u8>` from the borrowed bytes would allocate per record, against the zero-copy rule (CLAUDE.md §14); the bytes are the same data.
   - Accepted:
1. `kafka_producer_Producer_metrics`
   - Java member: `org.apache.kafka.clients.producer.Producer#metrics()`
   - Contract change: the returned map is a snapshot of the registered metrics, not Java's live unmodifiable view: a metric registered or removed after the call is not reflected in it.
   - Why: the C map is an owned copy of the Rust `Producer::metrics` snapshot (see the entry for `crate::producer::Producer::metrics`).
   - Accepted:
1. `kafka_producer_ProducerConfig_new`
   - Java member: `org.apache.kafka.clients.producer.ProducerConfig#ProducerConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` fails the constructor with an error answering `kafka_common_Error_is_config_error`; Java accepts any mechanism name and fails only at authentication.
   - Why: the C constructor wraps `crate::producer::ProducerConfig::new` (see its entry).
   - Accepted:
1. `crate::consumer::ConsumerConfig::new`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerConfig#ConsumerConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` makes the constructor return a `ConfigError`; Java accepts any mechanism name and fails only at authentication.
   - Why: this client implements only SASL/PLAIN, so failing at construction reports the problem where it is configured instead of at the first connection (as `crate::producer::ProducerConfig::new`).
   - Accepted:
1. `crate::consumer::Consumer::metrics`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#metrics()`
   - Contract change: the returned map is a snapshot of the registered metrics, not Java's live unmodifiable view: a metric registered or removed after the call is not reflected in it. Each value is the registry's shared `KafkaMetric`, so its value stays current.
   - Why: the registry lives behind a lock that cannot be lent to the caller; a snapshot of `Arc` handles is the borrow-free equivalent (as `crate::producer::Producer::metrics`).
   - Accepted:
1. `kafka_consumer_Consumer_metrics`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#metrics()`
   - Contract change: the returned map is a snapshot of the registered metrics, not Java's live unmodifiable view: a metric registered or removed after the call is not reflected in it.
   - Why: the C map is an owned copy of the Rust `Consumer::metrics` snapshot (see the entry for `crate::consumer::Consumer::metrics`).
   - Accepted:
1. `kafka_consumer_ConsumerConfig_new`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerConfig#ConsumerConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` fails the constructor with an error answering `kafka_common_Error_is_config_error`; Java accepts any mechanism name and fails only at authentication.
   - Why: the C constructor wraps `crate::consumer::ConsumerConfig::new` (see its entry).
   - Accepted:
1. `crate::consumer::ConsumerConfig::DEFAULT_ISOLATION_LEVEL`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerConfig#DEFAULT_ISOLATION_LEVEL`
   - Contract change: the default `isolation.level` is `read_committed`, so a consumer that does not set it returns only committed transactional records; Java's default is `read_uncommitted`, which also returns records of aborted and open transactions.
   - Why: `read_committed` is librdkafka's default, which users of the other Confluent clients expect; `isolation.level=read_uncommitted` restores Java's behaviour.
   - Accepted:
1. `crate::admin::AdminClientConfig::new`
   - Java member: `org.apache.kafka.clients.admin.AdminClientConfig#AdminClientConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` makes the constructor return a `ConfigError`; Java accepts any mechanism name and fails only at authentication.
   - Why: this client implements only SASL/PLAIN, so failing at construction reports the problem where it is configured instead of at the first connection (as `crate::producer::ProducerConfig::new`).
   - Accepted:
1. `kafka_admin_AdminClientConfig_new`
   - Java member: `org.apache.kafka.clients.admin.AdminClientConfig#AdminClientConfig(Map)`
   - Contract change: under a SASL `security.protocol`, a `sasl.mechanism` other than `PLAIN` fails the constructor with an error answering `kafka_common_Error_is_config_error`; Java accepts any mechanism name and fails only at authentication.
   - Why: the C constructor wraps `crate::admin::AdminClientConfig::new` (see its entry).
   - Accepted:
1. `crate::common::metrics::MetricsReporter`
   - Java member: `org.apache.kafka.common.metrics.MetricsReporter`
   - Contract change: a reporter implementing `ClusterResourceListener` receives no cluster metadata; Java's clients register their configured reporters as cluster resource listeners. No Rust client takes a reporter, since `metric.reporters` is class-valued, and a reporter added through `Metrics::add_reporter` is not registered as a listener.
   - Why: CLAUDE.md §2 forbids class-valued configuration, and Rust has no `instanceof` to discover the listener on a reporter trait object.
   - Accepted:
1. `crate::common::metrics::Sensor::record_with_value`
   - Java member: `org.apache.kafka.common.metrics.Sensor#record(double)`
   - Contract change: recording checks no quota, so a value moving a metric beyond its quota bound is recorded without reporting the violation; Java throws `QuotaViolationException` from `record`. The caller checks with `Sensor::check_quotas`.
   - Why: `record` returns `()`; returning the violation needs a `Result` reaching every recording call site, its own piece of work, and a per-metric scan on the per-record path.
   - Accepted:
1. `crate::common::metrics::Sensor::record_with_value_time_ms`
   - Java member: `org.apache.kafka.common.metrics.Sensor#record(double,long)`
   - Contract change: as `crate::common::metrics::Sensor::record_with_value`: no quota is checked and no violation is reported.
   - Why: as `crate::common::metrics::Sensor::record_with_value`.
   - Accepted:
1. `crate::common::errors::GroupAuthorizationError::group_id`
   - Java member: `org.apache.kafka.common.errors.GroupAuthorizationException#groupId`
   - Contract change: returns an empty string when the group ID is not known; Java returns `null`.
   - Why: the getter returns `&str`, and `with_default_message` (the error built from the broker's code) stores an empty string; returning `Option<&str>` is a signature change for its own piece of work.
   - Accepted:
1. `crate::common::KafkaFuture::then_apply`
   - Java member: `org.apache.kafka.common.KafkaFuture#thenApply(BaseFunction)`
   - Contract change: the function runs on the task awaiting the returned future, once per `get` / `get_with_timeout` call, and not at all if the returned future is never awaited; Java runs it once, on the thread that calls `thenApply` or the one that completes this future.
   - Why: the Rust `KafkaFuture` is lazy: the dependant computes its value when awaited, with no completion callback on the source.
   - Accepted:
1. `crate::common::serialization::Deserializer::deserialize_with_headers`
   - Java member: `org.apache.kafka.common.serialization.Deserializer#deserialize(String,Headers,byte[])`
   - Contract change: the headers are passed as `&RecordHeaders`, so the deserializer cannot modify them; Java allows it to add headers as a side effect of deserialization.
   - Why: the consumer shares the record's headers immutably while it deserializes key and value; the C binding hands them over read-only too.
   - Accepted:
1. `crate::common::serialization::Deserializer::deserialize_with_headers_bytes`
   - Java member: `org.apache.kafka.common.serialization.Deserializer#deserialize(String,Headers,ByteBuffer)`
   - Contract change: the headers are passed as `&RecordHeaders`, so the deserializer cannot modify them; Java allows it to add headers as a side effect of deserialization.
   - Why: the consumer shares the record's headers immutably while it deserializes key and value; the C binding hands them over read-only too.
   - Accepted:
1. `crate::common::serialization::StringSerializer`
   - Java member: `org.apache.kafka.common.serialization.StringSerializer`
   - Contract change: the encoding is always UTF-8; `key.serializer.encoding`, `value.serializer.encoding` and `serializer.encoding` are ignored, and an unknown charset is not rejected.
   - Why: a Rust string is UTF-8 by definition, and its bytes are borrowed as they are, with no copy.
   - Accepted:
1. `crate::common::serialization::StringDeserializer`
   - Java member: `org.apache.kafka.common.serialization.StringDeserializer`
   - Contract change: the encoding is always UTF-8 (malformed input becomes U+FFFD); `key.deserializer.encoding`, `value.deserializer.encoding` and `deserializer.encoding` are ignored, and an unknown charset is not rejected.
   - Why: the result is a Rust `String`, which is UTF-8 by definition; other charsets would need a transcoding dependency.
   - Accepted:
1. `kafka_common_KafkaFuture_then_apply`
   - Java member: `org.apache.kafka.common.KafkaFuture#thenApply(BaseFunction)`
   - Contract change: `function` runs once, on the thread that first resolves the returned future (the caller of its `get`, or a runtime task for a `_cb`), and not at all if it is never resolved; Java runs it on the thread that calls `thenApply` or the one that completes this future.
   - Why: the export wraps the lazy `crate::common::KafkaFuture::then_apply`; it keeps the first result, so a C function with side effects runs once, as Java's does.
   - Accepted:
1. `kafka_common_serialization_Deserializer_deserialize_with_headers`
   - Java member: `org.apache.kafka.common.serialization.Deserializer#deserialize(String,Headers,byte[])`
   - Contract change: the headers are a `const` record-headers handle, so the implementation cannot modify them; Java allows it to add headers as a side effect of deserialization.
   - Why: the export passes on the immutable borrow of `crate::common::serialization::Deserializer::deserialize_with_headers`.
   - Accepted:
1. `kafka_common_serialization_Deserializer_deserialize_with_headers_cb`
   - Java member: `org.apache.kafka.common.serialization.Deserializer#deserialize(String,Headers,byte[])`
   - Contract change: as `kafka_common_serialization_Deserializer_deserialize_with_headers`: the headers are read-only.
   - Why: the non-blocking twin of `kafka_common_serialization_Deserializer_deserialize_with_headers`, over the same borrow.
   - Accepted:
1. `kafka_common_serialization_Deserializer_deserialize_with_headers_bytes`
   - Java member: `org.apache.kafka.common.serialization.Deserializer#deserialize(String,Headers,ByteBuffer)`
   - Contract change: as `kafka_common_serialization_Deserializer_deserialize_with_headers`: the headers are read-only.
   - Why: the export passes on the immutable borrow of `crate::common::serialization::Deserializer::deserialize_with_headers_bytes`.
   - Accepted:
1. `kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_cb`
   - Java member: `org.apache.kafka.common.serialization.Deserializer#deserialize(String,Headers,ByteBuffer)`
   - Contract change: as `kafka_common_serialization_Deserializer_deserialize_with_headers`: the headers are read-only.
   - Why: the non-blocking twin of `kafka_common_serialization_Deserializer_deserialize_with_headers_bytes`, over the same borrow.
   - Accepted:
1. `kafka_common_serialization_StringSerializer_new`
   - Java member: `org.apache.kafka.common.serialization.StringSerializer#StringSerializer()`
   - Contract change: the encoding is always UTF-8; `key.serializer.encoding`, `value.serializer.encoding` and `serializer.encoding` are ignored.
   - Why: the handle wraps `crate::common::serialization::StringSerializer`, which carries the same change.
   - Accepted:
1. `kafka_common_serialization_StringDeserializer_new`
   - Java member: `org.apache.kafka.common.serialization.StringDeserializer#StringDeserializer()`
   - Contract change: the encoding is always UTF-8 (malformed input becomes U+FFFD); `key.deserializer.encoding`, `value.deserializer.encoding` and `deserializer.encoding` are ignored.
   - Why: the handle wraps `crate::common::serialization::StringDeserializer`, which carries the same change.
   - Accepted:
1. `kafka_common_metrics_Sensor_record_with_value`
   - Java member: `org.apache.kafka.common.metrics.Sensor#record(double)`
   - Contract change: as `crate::common::metrics::Sensor::record_with_value`: recording checks no quota, so a value moving a metric beyond its quota bound is recorded without reporting the violation; the caller checks with `kafka_common_metrics_Sensor_check_quotas`.
   - Why: the export forwards to `crate::common::metrics::Sensor::record_with_value`, and its `void` signature has no `out_err` to report a violation through.
   - Accepted:
1. `kafka_common_metrics_Sensor_record_with_value_time_ms`
   - Java member: `org.apache.kafka.common.metrics.Sensor#record(double,long)`
   - Contract change: as `kafka_common_metrics_Sensor_record_with_value`: no quota is checked and no violation is reported.
   - Why: the export forwards to `crate::common::metrics::Sensor::record_with_value_time_ms`.
   - Accepted:
1. `kafka_common_GroupAuthorizationError_group_id`
   - Java member: `org.apache.kafka.common.errors.GroupAuthorizationException#groupId`
   - Contract change: returns an empty string when the group ID is not known; Java returns `null`.
   - Why: the export reads `crate::common::errors::GroupAuthorizationError::group_id`, which carries the same change.
   - Accepted:
1. `crate::consumer::Consumer::offsets_for_times`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#offsetsForTimes(Map)`
   - Contract change: a partition with no offset for the timestamp is absent from the returned map; Java maps it to `null`, so every queried partition is a key of its result.
   - Why: `OffsetAndTimestamp` is not nullable in Rust and the map value type has no `Option`; `crate::consumer::internals::AsyncKafkaConsumer` drops the `null` entries `org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#offsetsForTimes(Map,Duration)` keeps.
   - Accepted:
1. `crate::consumer::Consumer::offsets_for_times_with_timeout`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#offsetsForTimes(Map,Duration)`
   - Contract change: a partition with no offset for the timestamp is absent from the returned map; Java maps it to `null`, so every queried partition is a key of its result.
   - Why: `OffsetAndTimestamp` is not nullable in Rust and the map value type has no `Option`; `crate::consumer::internals::AsyncKafkaConsumer` drops the `null` entries `org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#offsetsForTimes(Map,Duration)` keeps.
   - Accepted:
1. `kafka_consumer_Consumer_offsets_for_times`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#offsetsForTimes(Map)`
   - Contract change: a partition with no offset for the timestamp is absent from the returned `kafka_Map_t`; Java maps it to `null`.
   - Why: the export returns the map of `crate::consumer::Consumer::offsets_for_times`, which carries the same change.
   - Accepted:
1. `kafka_consumer_Consumer_offsets_for_times_cb`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#offsetsForTimes(Map)`
   - Contract change: a partition with no offset for the timestamp is absent from the returned `kafka_Map_t`; Java maps it to `null`.
   - Why: the export returns the map of `crate::consumer::Consumer::offsets_for_times`, which carries the same change.
   - Accepted:
1. `kafka_consumer_Consumer_offsets_for_times_with_timeout`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#offsetsForTimes(Map,Duration)`
   - Contract change: a partition with no offset for the timestamp is absent from the returned `kafka_Map_t`; Java maps it to `null`.
   - Why: the export returns the map of `crate::consumer::Consumer::offsets_for_times_with_timeout`, which carries the same change.
   - Accepted:
1. `kafka_consumer_Consumer_offsets_for_times_with_timeout_cb`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#offsetsForTimes(Map,Duration)`
   - Contract change: a partition with no offset for the timestamp is absent from the returned `kafka_Map_t`; Java maps it to `null`.
   - Why: the export returns the map of `crate::consumer::Consumer::offsets_for_times_with_timeout`, which carries the same change.
   - Accepted:
1. `crate::consumer::Consumer::committed`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#committed(Set)`
   - Contract change: a partition with no committed offset is absent from the returned map; Java maps it to `null`, so every queried partition is a key of its result.
   - Why: `OffsetAndMetadata` is not nullable in Rust and the map value type has no `Option`; `crate::consumer::internals::events::ApplicationEventProcessor` drops the `null` entries of `org.apache.kafka.clients.consumer.internals.CommitRequestManager$OffsetFetchResult#toOffsetMapWithNulls`, which Java's `org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#committed(Set,Duration)` returns as they are.
   - Accepted:
1. `crate::consumer::Consumer::committed_with_timeout`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#committed(Set,Duration)`
   - Contract change: a partition with no committed offset is absent from the returned map; Java maps it to `null`, so every queried partition is a key of its result.
   - Why: `OffsetAndMetadata` is not nullable in Rust and the map value type has no `Option`; `crate::consumer::internals::events::ApplicationEventProcessor` drops the `null` entries of `org.apache.kafka.clients.consumer.internals.CommitRequestManager$OffsetFetchResult#toOffsetMapWithNulls`, which Java's `org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#committed(Set,Duration)` returns as they are.
   - Accepted:
1. `kafka_consumer_Consumer_committed`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#committed(Set)`
   - Contract change: a partition with no committed offset is absent from the returned `kafka_Map_t`; Java maps it to `null`.
   - Why: the export returns the map of `crate::consumer::Consumer::committed`, which carries the same change.
   - Accepted:
1. `kafka_consumer_Consumer_committed_cb`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#committed(Set)`
   - Contract change: a partition with no committed offset is absent from the returned `kafka_Map_t`; Java maps it to `null`.
   - Why: the export returns the map of `crate::consumer::Consumer::committed`, which carries the same change.
   - Accepted:
1. `kafka_consumer_Consumer_committed_with_timeout`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#committed(Set,Duration)`
   - Contract change: a partition with no committed offset is absent from the returned `kafka_Map_t`; Java maps it to `null`.
   - Why: the export returns the map of `crate::consumer::Consumer::committed_with_timeout`, which carries the same change.
   - Accepted:
1. `kafka_consumer_Consumer_committed_with_timeout_cb`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#committed(Set,Duration)`
   - Contract change: a partition with no committed offset is absent from the returned `kafka_Map_t`; Java maps it to `null`.
   - Why: the export returns the map of `crate::consumer::Consumer::committed_with_timeout`, which carries the same change.
   - Accepted:
1. `crate::common::ClusterResourceListener`
   - Java member: `org.apache.kafka.common.ClusterResourceListener`
   - Contract change: `on_update` is never called: no client registers a user's listener, while Java calls `onUpdate` on every interceptor, serializer, deserializer and metrics reporter implementing the interface after each metadata response.
   - Why: the clients take serializers, deserializers and reporters as trait objects and have no `instanceof` check to find the listener; the registration is not translated.
   - Accepted:
1. `crate::common::serialization::Serializer`
   - Java member: `org.apache.kafka.common.serialization.Serializer`
   - Contract change: combining a serializer with `ClusterResourceListener` or `Monitorable` has no effect: the client never calls `on_update` on it and `Monitorable` is not translated.
   - Why: see `crate::common::ClusterResourceListener`; Java finds both interfaces with `instanceof`, which a Rust trait object cannot do.
   - Accepted:
1. `crate::common::serialization::Deserializer`
   - Java member: `org.apache.kafka.common.serialization.Deserializer`
   - Contract change: combining a deserializer with `ClusterResourceListener` or `Monitorable` has no effect: the client never calls `on_update` on it and `Monitorable` is not translated.
   - Why: see `crate::common::ClusterResourceListener`; Java finds both interfaces with `instanceof`, which a Rust trait object cannot do.
   - Accepted:
