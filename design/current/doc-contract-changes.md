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
1. `crate::consumer::Consumer::poll`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#poll(Duration)`
   - Contract change: a `timeout` greater than `i64::MAX` milliseconds counts as `i64::MAX` milliseconds, where Java's `Duration.toMillis()` throws `ArithmeticException`; `poll(Duration::MAX)` waits without limit.
   - Why: `Duration::MAX` is Rust's usual way to wait without limit, and Rust's `Duration` reaches about 5.8 × 10¹¹ years, past `i64::MAX` milliseconds; only timeouts over about 292 million years differ.
   - Accepted: 2026-10-27 Emanuele Sabellico
1. `kafka_consumer_Consumer_poll`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#poll(Duration)`
   - Contract change: never fails with `ArithmeticException`: `timeout` is an `int64_t` of milliseconds, so it cannot exceed `Long.MAX_VALUE` milliseconds.
   - Why: the C timeout is the millisecond count Java's `Duration.toMillis()` produces, so the overflow cannot be expressed.
   - Accepted: 2026-10-27 Emanuele Sabellico
1. `kafka_consumer_Consumer_poll_cb`
   - Java member: `org.apache.kafka.clients.consumer.Consumer#poll(Duration)`
   - Contract change: never fails with `ArithmeticException`: `timeout` is an `int64_t` of milliseconds, so it cannot exceed `Long.MAX_VALUE` milliseconds.
   - Why: as `kafka_consumer_Consumer_poll`.
   - Accepted: 2026-10-27 Emanuele Sabellico
1. `kafka_common_TopicPartition_new`
   - Java member: `org.apache.kafka.common.TopicPartition#TopicPartition`
   - Contract change: a `NULL` `topic` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_common_Node_new`
   - Java member: `org.apache.kafka.common.Node#Node(int,String,int)`
   - Contract change: a `NULL` `host` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_common_Node_with_rack`
   - Java member: `org.apache.kafka.common.Node#Node(int,String,int,String)`
   - Contract change: a `NULL` `host` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_common_Node_with_rack_is_fenced`
   - Java member: `org.apache.kafka.common.Node#Node(int,String,int,String,boolean)`
   - Contract change: a `NULL` `host` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_common_PartitionInfo_new`
   - Java member: `org.apache.kafka.common.PartitionInfo#PartitionInfo`
   - Contract change: a `NULL` `topic`, `replicas`, `in_sync_replicas` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_common_PartitionInfo_with_offline_replicas`
   - Java member: `org.apache.kafka.common.PartitionInfo#PartitionInfo`
   - Contract change: a `NULL` `topic`, `replicas`, `in_sync_replicas`, `offline_replicas` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico 
1. `kafka_admin_TopicListing_new`
   - Java member: `org.apache.kafka.clients.admin.TopicListing#TopicListing`
   - Contract change: a `NULL` `name`, `topic_id` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_TopicDescription_new`
   - Java member: `org.apache.kafka.clients.admin.TopicDescription#TopicDescription`
   - Contract change: a `NULL` `name`, `partitions` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_TopicDescription_with_authorized_operations`
   - Java member: `org.apache.kafka.clients.admin.TopicDescription#TopicDescription`
   - Contract change: a `NULL` `name`, `partitions` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_TopicDescription_with_authorized_operations_topic_id`
   - Java member: `org.apache.kafka.clients.admin.TopicDescription#TopicDescription`
   - Contract change: a `NULL` `name`, `partitions`, `topic_id` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_NewTopic_with_num_partitions_replication_factor`
   - Java member: `org.apache.kafka.clients.admin.NewTopic#NewTopic`
   - Contract change: a `NULL` `name` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_NewTopic_with_replicas_assignments`
   - Java member: `org.apache.kafka.clients.admin.NewTopic#NewTopic`
   - Contract change: a `NULL` `name` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_MemberToRemove_new`
   - Java member: `org.apache.kafka.clients.admin.MemberToRemove#MemberToRemove`
   - Contract change: a `NULL` `group_instance_id` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_GroupListing_new`
   - Java member: `org.apache.kafka.clients.admin.GroupListing#GroupListing`
   - Contract change: a `NULL` `group_id`, `protocol` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_ConsumerGroupDescription_new`
   - Java member: `org.apache.kafka.clients.admin.ConsumerGroupDescription#ConsumerGroupDescription(String,boolean,Collection,String,GroupType,GroupState,Node,Set,Optional,Optional)`
   - Contract change: a `NULL` `group_type`, `group_state` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_ClassicGroupDescription_new`
   - Java member: `org.apache.kafka.clients.admin.ClassicGroupDescription#ClassicGroupDescription(String,String,String,Collection,ClassicGroupState,Node)`
   - Contract change: a `NULL` `protocol`, `state` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_ClassicGroupDescription_with_authorized_operations`
   - Java member: `org.apache.kafka.clients.admin.ClassicGroupDescription#ClassicGroupDescription(String,String,String,Collection,ClassicGroupState,Node,Set)`
   - Contract change: a `NULL` `protocol`, `state` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_TransactionListing_new`
   - Java member: `org.apache.kafka.clients.admin.TransactionListing#TransactionListing`
   - Contract change: a `NULL` `transactional_id`, `transaction_state` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_common_security_token_delegation_TokenInformation_new`
   - Java member: `org.apache.kafka.common.security.token.delegation.TokenInformation#TokenInformation`
   - Contract change: a `NULL` `token_id`, `owner`, `renewers` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_common_security_token_delegation_TokenInformation_with_token_requester`
   - Java member: `org.apache.kafka.common.security.token.delegation.TokenInformation#TokenInformation`
   - Contract change: a `NULL` `token_id`, `owner`, `token_requester`, `renewers` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_consumer_SubscriptionPattern_new`
   - Java member: `org.apache.kafka.clients.consumer.SubscriptionPattern#SubscriptionPattern`
   - Contract change: a `NULL` `pattern` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_common_quota_ClientQuotaAlteration_Op_new`
   - Java member: `org.apache.kafka.common.quota.ClientQuotaAlteration$Op#Op`
   - Contract change: a `NULL` `key` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_consumer_ConsumerPartitionAssignor_Assignment_new`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Assignment#Assignment`
   - Contract change: a `NULL` `partitions` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_consumer_ConsumerPartitionAssignor_Assignment_with_user_data`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Assignment#Assignment`
   - Contract change: a `NULL` `partitions` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be stored; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted: 2026-11-03 Emanuele Sabellico
1. `kafka_admin_LogDirDescription_new`
   - Java member: `org.apache.kafka.clients.admin.LogDirDescription#LogDirDescription(ApiException,Map)`
   - Contract change: a `NULL` `replica_infos` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_LogDirDescription_with_total_bytes_usable_bytes`
   - Java member: `org.apache.kafka.clients.admin.LogDirDescription#LogDirDescription(ApiException,Map,long,long)`
   - Contract change: a `NULL` `replica_infos` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_LogDirDescription_with_total_bytes_usable_bytes_is_cordoned`
   - Java member: `org.apache.kafka.clients.admin.LogDirDescription#LogDirDescription(ApiException,Map,long,long,boolean)`
   - Contract change: a `NULL` `replica_infos` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_TransactionDescription_new`
   - Java member: `org.apache.kafka.clients.admin.TransactionDescription#TransactionDescription`
   - Contract change: a `NULL` `topic_partitions` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_common_quota_ClientQuotaAlteration_new`
   - Java member: `org.apache.kafka.common.quota.ClientQuotaAlteration#ClientQuotaAlteration`
   - Contract change: a `NULL` `ops` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_common_quota_ClientQuotaEntity_new`
   - Java member: `org.apache.kafka.common.quota.ClientQuotaEntity#ClientQuotaEntity`
   - Contract change: a `NULL` `entries` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_consumer_ConsumerPartitionAssignor_Subscription_new`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#Subscription(List)`
   - Contract change: a `NULL` `topics` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_consumer_ConsumerPartitionAssignor_Subscription_with_user_data`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#Subscription(List,ByteBuffer)`
   - Contract change: a `NULL` `topics` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_consumer_ConsumerPartitionAssignor_Subscription_with_user_data_owned_partitions`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#Subscription(List,ByteBuffer,List)`
   - Contract change: a `NULL` `topics`, `owned_partitions` panic with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_consumer_ConsumerRecords_with_next_offsets`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerRecords#ConsumerRecords(Map,Map)`
   - Contract change: a `NULL` `records` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_DescribeProducersResult_PartitionProducerState_new`
   - Java member: `org.apache.kafka.clients.admin.DescribeProducersResult$PartitionProducerState#PartitionProducerState`
   - Contract change: a `NULL` `active_producers` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_AlterClientQuotasResult_new`
   - Java member: `org.apache.kafka.clients.admin.AlterClientQuotasResult#AlterClientQuotasResult`
   - Contract change: a `NULL` `futures` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_DeleteRecordsResult_new`
   - Java member: `org.apache.kafka.clients.admin.DeleteRecordsResult#DeleteRecordsResult`
   - Contract change: a `NULL` `futures` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_DescribeClassicGroupsResult_new`
   - Java member: `org.apache.kafka.clients.admin.DescribeClassicGroupsResult#DescribeClassicGroupsResult`
   - Contract change: a `NULL` `futures` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_DescribeConsumerGroupsResult_new`
   - Java member: `org.apache.kafka.clients.admin.DescribeConsumerGroupsResult#DescribeConsumerGroupsResult`
   - Contract change: a `NULL` `futures` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_ListOffsetsResult_new`
   - Java member: `org.apache.kafka.clients.admin.ListOffsetsResult#ListOffsetsResult`
   - Contract change: a `NULL` `futures` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_common_quota_ClientQuotaFilter_contains`
   - Java member: `org.apache.kafka.common.quota.ClientQuotaFilter#contains`
   - Contract change: a `NULL` `components` panics with `LocalNullPointerError` (`<param> must not be null`), where Java builds a filter holding `null`.
   - Why: the Rust method takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_common_quota_ClientQuotaFilter_contains_only`
   - Java member: `org.apache.kafka.common.quota.ClientQuotaFilter#containsOnly`
   - Contract change: a `NULL` `components` panics with `LocalNullPointerError` (`<param> must not be null`), where Java builds a filter holding `null`.
   - Why: the Rust method takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_ConfigEntryOptionsBuilder_set_synonyms`
   - Java member: `org.apache.kafka.clients.admin.ConfigEntry#ConfigEntry(String,String,ConfigSource,boolean,boolean,List,ConfigType,String)`
   - Contract change: a `NULL` `synonyms` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_common_metrics_MetricConfig_set_tags`
   - Java member: `org.apache.kafka.common.metrics.MetricConfig#tags(Map)`
   - Contract change: a `NULL` `tags` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's setter stores `null`.
   - Why: the Rust method takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_CreateDelegationTokenOptions_set_renewers`
   - Java member: `org.apache.kafka.clients.admin.CreateDelegationTokenOptions#renewers`
   - Contract change: a `NULL` `renewers` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's setter stores `null`.
   - Why: the Rust method takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_consumer_SubscriptionOptionsBuilder_set_topics`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#Subscription(List,ByteBuffer,List,int,Optional)`
   - Contract change: a `NULL` `topics` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_consumer_SubscriptionOptionsBuilder_set_owned_partitions`
   - Java member: `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Subscription#Subscription(List,ByteBuffer,List,int,Optional)`
   - Contract change: a `NULL` `owned_partitions` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's constructor stores `null`.
   - Why: the Rust constructor takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_MockAdminClient_Builder_set_cluster_id`
   - Java member: `org.apache.kafka.clients.admin.MockAdminClient$Builder#clusterId`
   - Contract change: a `NULL` `cluster_id` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's setter stores `null`.
   - Why: the Rust method takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_MockAdminClient_Builder_set_broker_log_dirs`
   - Java member: `org.apache.kafka.clients.admin.MockAdminClient$Builder#brokerLogDirs`
   - Contract change: a `NULL` `broker_log_dirs` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's setter stores `null`.
   - Why: the Rust method takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_MockAdminClient_Builder_set_default_group_configs`
   - Java member: `org.apache.kafka.clients.admin.MockAdminClient$Builder#defaultGroupConfigs`
   - Contract change: a `NULL` `default_group_configs` panics with `LocalNullPointerError` (`<param> must not be null`), where Java's setter stores `null`.
   - Why: the Rust method takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_admin_MockAdminClient_add_topic`
   - Java member: `org.apache.kafka.clients.admin.MockAdminClient#addTopic(boolean,String,List,Map,boolean)`
   - Contract change: a `NULL` `name` panics with `LocalNullPointerError` (`<param> must not be null`), where Java adds a topic named `null`.
   - Why: the Rust method takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
1. `kafka_consumer_MockConsumer_update_partitions`
   - Java member: `org.apache.kafka.clients.consumer.MockConsumer#updatePartitions`
   - Contract change: a `NULL` `topic` panics with `LocalNullPointerError` (`<param> must not be null`), where Java stores the partitions under a `null` topic.
   - Why: the Rust method takes the value, not an `Option`, so `null` cannot be passed on; reading `NULL` as empty would hide the caller's bug, and CLAUDE.md §4 makes a `NULL` mandatory parameter panic.
   - Accepted:
