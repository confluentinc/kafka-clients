# Changelog

All notable changes to the `confluent-kafka` crate are documented in this file.

Before 1.0 the public API is not stable, and any minor release may contain
breaking changes.

## [Unreleased]

### Producer

- **Breaking:** `Serializer::serialize_with_headers` (and its `DynSerializer`
  counterpart) takes the record's headers as `&mut RecordHeaders`, so a
  serializer can add or remove headers, as Java's can. The record carries what
  the headers hold once the value serializer returns; they become read-only
  after serialization. Update the parameter type in your implementations.
- `Serializer` gains Java's `configure(configs, is_key)` and `close()`, as
  async default methods that do nothing, so existing implementations compile
  unchanged.
- `KafkaProducer::close` / `close_with_timeout` now close the key serializer,
  then the value serializer, once each; a failure is returned as
  "Failed to close kafka producer". A `KafkaProducer` whose constructor fails
  closes the serializers it was given.
- A closed `KafkaProducer` is freed: reference cycles in the metrics registry
  kept it alive after `close`.

### Consumer

- A `KafkaConsumer` whose constructor fails closes the deserializers it was
  given.
- `poll` moves the consumed positions only after the last deserializer call of
  the batch has finished. A cancelled `poll` leaves the positions where they
  were, and auto-commit can no longer commit records that were never returned.
- `StringDeserializer` decodes the fetched buffer in place.
- A closed consumer is freed: the rebalance listener (which can capture a
  `ConsumerHandle`) is released on `close`, `MockConsumer` releases it on drop,
  and the metrics registry no longer keeps the consumer alive.
- **Breaking:** `ConsumerConfig::get_string` answers every `STRING` key,
  including `security.protocol`, `client.dns.lookup`, `sasl.mechanism` and the
  `ssl.*` location and type keys. A known key of another type now returns an
  `IllegalArgumentError` ("Configuration '<key>' is of type <T>, not STRING")
  instead of a value; an unknown key is still a `ConfigError`.
- **Breaking:** `ConsumerHandle::group_metadata` now panics like
  `KafkaConsumer::group_metadata` on a consumer without `group.id` instead of
  returning a group-less value.
- **Breaking:** `isolation.level` defaults to `read_committed`, as in
  librdkafka, instead of Java's `read_uncommitted`: a consumer no longer returns
  records of aborted or still open transactions, and reads only up to the last
  stable offset. The new `ConsumerConfig::DEFAULT_ISOLATION_LEVEL` holds the
  default. Set `isolation.level=read_uncommitted` for the previous behaviour.

### Common

- **Breaking:** `ProducerConfig`, `ConsumerConfig` and `AdminClientConfig` no
  longer implement `Default`. Build them with `::new(&configs)` from a
  `HashMap<String, String>` (an empty map gives the defaults).
- New `CommonClientConfigs::DEFAULT_SECURITY_PROTOCOL`.
- New error class `LocalUnsupportedOperationError` (Java's
  `UnsupportedOperationException`), with `Error::local_unsupported_operation`
  and the `is_local_unsupported_operation_error` predicate. In C it is
  `kafka_common_Error_LOCAL_UNSUPPORTED_OPERATION` (-31), built with
  `kafka_common_Error_local_unsupported_operation`. In Python it is the root
  class `confluent_kafka.UnsupportedOperationError`, a `NotImplementedError`.
- **Breaking (Rust, C API, Python):** the calls that fail where Java throws
  `UnsupportedOperationException`, or that this client does not support, now
  fail with `LocalUnsupportedOperationError` instead of `UnsupportedVersionError`
  (code 35):
  - `KafkaConsumer::new` with `group.protocol=classic`, the default, so every
    consumer that does not set the key;
  - `AdminClientConfig::new` with `bootstrap.controllers` and no
    `bootstrap.servers`;
  - `KafkaProducer::new` with the incremental `buffer.memory` allocation
    strategy and a compression type;
  - the `MockAdminClient` methods Java's mock leaves "Not implemented yet", and
    `incremental_alter_configs` for a resource type the mock does not handle;
  - `MockConsumer::offsets_for_times`, and the async operations of a
    `ConsumerHandle` taken from a `MockConsumer`;
  - in Python also the pure-Python `MockAdminClient` / `AsyncMockAdminClient`
    and `MockConsumer`, completing a `KafkaFuture.to_completion_stage()` future
    from user code, and `send_offsets_to_transaction` with a mock consumer's
    group metadata.

  For these, `is_unsupported_version_error`, `is_api_error` and
  `is_kafka_error` now answer `false`; match them with
  `is_local_unsupported_operation_error()`. In C `kafka_common_Error__enum`
  returns `kafka_common_Error_LOCAL_UNSUPPORTED_OPERATION` (-31) instead of
  `kafka_common_Error_UNSUPPORTED_VERSION`. In Python the class is not a
  `KafkaError`, so `except UnsupportedVersionError`, `except ApiError` and
  `except KafkaError` no longer catch them: use
  `except UnsupportedOperationError` or `except NotImplementedError`.
  `unregister_controller` / `unregister_broker` on the mock, where Java throws
  `UnsupportedVersionException`, and every failure caused by the broker's
  version keep `UnsupportedVersionError`.
- **Breaking:** `InlineFuture::new` is no longer public. `InlineFuture` stays
  public as the future the sealed `DynSerializer` / `DynDeserializer` methods
  return; only their blanket implementations build one.

### C API

- **Breaking (C API):** serializers and deserializers are registered only
  through `kafka_common_serialization_Serializer_new_async` /
  `kafka_common_serialization_Deserializer_new_async`. The synchronous
  `Serializer_new` / `Deserializer_new` constructors, their `_err` twins and
  the synchronous `_fn_t` typedefs are removed.
- **Breaking (C API):** the parameters of the published constructor
  `kafka_common_serialization_Serializer_new_async` changed: it takes two more
  nullable function pointers, `configure` and `close`, after the serialize
  pointers (`NULL` keeps Java's no-op default). Existing calls must pass them.
- **Breaking (C API):** a C serializer receives the record's own headers as a
  mutable `kafka_common_header_internals_RecordHeaders_t *` and may change
  them: `Serializer_serialize_with_headers`, its `_cb` twin and the
  `serialize_with_headers` function-pointer type drop `const`. A deserializer
  still gets them read-only, without a copy. `RecordHeaders__as_Headers` now
  takes a `const` handle, and `ByteArraySerializer__as_Serializer` /
  `StringSerializer__as_Serializer` return a non-`const` view.
- `kafka_common_serialization_Serializer__is_async` /
  `Deserializer__is_async` answer 1 for a C implementation and 0 for a
  built-in class's view.
- **Breaking (C API):** `current_lag` returns Java's `OptionalLong` as a
  presence flag: `kafka_consumer_Consumer_current_lag` and
  `kafka_consumer_ConsumerHandle_current_lag` return `int8_t` (1 when a lag is
  known) and write the lag to an `int64_t *out_lag`; the `_cb` completion type
  gains a leading `int8_t present` argument.
- `kafka_producer_Producer_send_with_callback` documents that the callback
  handle is consumed by the call.

## [0.2.0-beta.1] - 2026-10-19

Second preview release. The client now follows Apache Kafka 4.4, the C API was
rewritten to one uniform set of conventions, and serializers and deserializers
became async traits. There are many breaking changes; the entries below say
what to change.

### Producer

- **Breaking:** `Serializer` is an async trait (its methods return
  `impl Future + Send`) and has `Send + Sync` supertraits, so it is no longer
  dyn-compatible. Hold serializers as `Box<dyn DynSerializer<K>>`;
  `Box::new(StringSerializer::new())` coerces to it. `serialize` returns
  `Option<Cow<'_, [u8]>>`, so a serializer can hand back bytes it already owns.
  `serialize_headers` and `serialize_owned_headers` are removed (use
  `serialize_with_headers` / `serialize_owned_with_headers`). Calls through
  `DynSerializer` store the future inline (256 bytes) instead of boxing it.
- **Breaking:** the producer `Callback` is a trait instead of a boxed closure
  type; build one from a closure with `callback::new`.
- **Breaking:** constants that are not Java's on these classes were removed
  from `ProducerConfig`, `ConsumerConfig` and `AdminClientConfig`
  (`SECURITY_PROTOCOL_CONFIG`, `SASL_MECHANISM_CONFIG`, `SASL_JAAS_CONFIG`,
  `METADATA_MAX_AGE_MS_CONFIG`, `METADATA_RECOVERY_*`, ...); use the keys from
  `CommonClientConfigs` and `SaslConfigs`, as in Java.
  `RecordMetadata::INVALID_OFFSET` is no longer public.
- **Breaking:** `ProducerRecord::headers_mut` / `into_parts`,
  `RecordMetadata::topic_partition` and `MockProducer::committed_offset` are
  removed. `MockProducer` is no longer `UnwindSafe` / `RefUnwindSafe`.
- Kafka 4.4: rack-aware partitioning (KIP-1123, `partitioner.rack.aware` with
  the producer's `client.rack`), and `TxnOffsetCommit` v6 with topic ids
  (KIP-1319) when the broker supports it.
- Transaction latency metrics are recorded; `acks` and `transactional.id` are
  validated as in Java; `group.instance.id` is sent in transactional offset
  commits; boolean configuration values are case-insensitive.

### Consumer

- **Breaking:** `KafkaConsumer` is now a concrete type, `KafkaConsumer<K, V>`,
  and `KafkaConsumer::new` returns it instead of a `Box<dyn Consumer<K, V>>`.
  Calling a `Consumer` method on it, or through generic code taking
  `C: Consumer<K, V>`, no longer boxes the returned future. `KafkaConsumer` is
  `Send` but not `Sync`, matching Java's single-threaded consumer; to reach it
  from another task (for example to call `wakeup`) use `consumer.handle()`.
- **Breaking:** the asynchronous methods of the `Consumer` trait return
  `impl Future + Send` instead of using `#[async_trait]`, so the trait is no
  longer dyn-compatible. Implementations can still be written with `async fn`.
- New `DynConsumer` trait for dynamic dispatch (`Box<dyn DynConsumer<K, V>>`),
  implemented automatically for every `Consumer`, as `DynProducer` is for
  `Producer`. Replace `Box<dyn Consumer<K, V>>` with
  `Box<dyn DynConsumer<K, V>>`; with only `Consumer` imported, method calls on
  it keep working unchanged.
- **Breaking:** `assignment`, `subscription`, `paused`, `group_metadata` and
  `current_lag` take `&mut self`, following Java's guard on these methods.
  `client_id` is removed from the trait (it is not on Java's `Consumer`).
- **Breaking:** `ConsumerHandle` keeps only the methods of KIP-1306's
  callback view: `subscription`, `assign` and `commit_async_offsets` are
  removed. Its calls are accepted only from inside a rebalance listener or
  commit callback; elsewhere they fail with a concurrent-modification error.
- **Breaking:** `Deserializer` is an async trait, held as
  `Box<dyn DynDeserializer<V>>`. `deserialize_with_headers` takes
  `&RecordHeaders`; `deserialize_headers`, `deserialize_from_shared` and
  `deserialize_from_shared_headers` are replaced by KIP-863's
  `deserialize_with_headers_bytes(topic, headers, &Bytes)`, which receives the
  record's slice of the fetch buffer. `configure` and `close` are async. The
  consumer closes its deserializers in `close` (failures reported as "Failed to
  close deserializers"); dropping it no longer does.
- **Breaking:** `ConsumerConfig` lost its typed getters and setters
  (`bootstrap_servers`, `set_*`, ...); read values by key, for example with
  `get_string`. `CloseOptions::DEFAULT_CLOSE_TIMEOUT_MS` is removed.
- **Breaking:** renames to Java's names under the overload rules:
  `CloseOptions::new_timeout` → `new_with_timeout`,
  `CloseOptions::new_group_membership_operation` → `with_operation`,
  `ConsumerNoOffsetForPartitionError::for_partitions` → `with_partitions`,
  `ConsumerRecords::records_partition` / `records_topic` →
  `records_with_partition` / `records_with_topic`.
- Kafka 4.4: `MockConsumer::lose_partitions`, and every consumer sensor is
  removed on close.
- `wakeup` follows Java's `WakeupTrigger`: a result that is already ready wins
  over a later wakeup, and a wakeup is checked even when no time is left. An
  `OffsetCommitCallback` error propagates as in Java.
- The fetch path is cancel-safe. Offset retries wake the background task,
  `close` passes the remaining time on, and `beginning_offsets` /
  `end_offsets` / `offsets_for_times` no longer wait about 5 seconds.
- Fixed: `client.rack` was never sent in KIP-848 heartbeats; offset fetches
  used the member epoch from request creation; a `REBOOTSTRAP_REQUIRED`
  disconnect could panic `poll`; a zero-timeout `poll` never yielded; a
  closed consumer was never freed.

### Admin client

- **Breaking:** `DescribeTopicsResult` and `DeleteTopicsResult` are structs
  instead of enums, keyed by topic name or topic id.
- **Breaking:** `AdminClientConfig` is no longer `Clone` and lost its typed
  getters (`bootstrap_servers`, `client_id`, `ssl_config`, ...).
- **Breaking:** `MockAdminClient::create()` returns a `MockAdminClient.Builder`
  as in Java (`MockAdminClient::new()`, `with_brokers_controller`,
  `set_controller`, `set_fetches_remaining_until_visible` were added;
  `set_feature_levels` and `set_broker_log_dirs` are removed).
- **Breaking:** renames to Java's names: `group_type`, `config_type` and
  `resource_type` getters → `r#type`, `ClientQuotaFilterComponent::match_spec`
  → `r#match`, `ListConsumerGroupOffsetsResult::partitions_to_offset_and_metadata_for_group`
  → `partitions_to_offset_and_metadata_with_group_id`,
  `RecordsToDelete::before_offset_with_offset` → `with_offset`,
  `NewPartitions::increase_to_new_assignments` →
  `increase_to_with_new_assignments`. `RemoveMembersFromConsumerGroupOptions::new`
  takes no arguments (`with_members` takes the members).
  `DelegationToken::token_info_mut` is removed and `GroupOffsets` is no longer
  public.
- New `Admin::unregister_controller` (Kafka 4.4), with its options and result.
- `describe_topics` by name uses `DescribeTopicPartitions`, as Java does.
  `TopicPartitionInfo::elr` / `last_known_elr` return lists.
- Fixed: calls made before `close` complete; calls after it fail with "Cannot
  accept new calls when AdminClient is closing."; a zero topic id no longer
  panics; lookup retries honour `retries`; `DeleteTopics` throttling starts at
  v2.

### Common

- Up to Apache Kafka 4.4: KIP-909 optional asynchronous bootstrap DNS
  resolution (`bootstrap.resolve.timeout.ms`, default 0 keeps the previous
  behaviour), KIP-1242 cluster-id checks (`metadata.cluster.check.enable`,
  default true), error codes 134–136, `BootstrapResolutionError`, and
  `UnsupportedProtocolField` with the `is_unsupported_version_error`
  predicate.
- Every Java exception class has its own predicate on `Error`
  (`is_<class>_error`, meaning Java's `instanceof`); classify errors with them
  rather than by matching variants.
- **Breaking:** functional interfaces are traits with factory functions:
  `KafkaFuture`'s `BaseFunction` (`kafka_future::BaseFunction_new`) and
  `MetricValueProvider` (an enum before, now a trait with `new` / `with_*`
  factories). `Gauge` extends `MetricValueProvider` and lost `value`;
  `ClosureGauge` is removed.
- **Breaking:** `Metrics`, `Sensor` and `MetricConfig` keep only Java's public
  methods: the `metric_name_*`, `sensor_*`, `add_metric_*` and
  `metric_instance_*` variants were renamed or removed, as were
  `Sensor::record_value*`, `expire_sensors` and
  `MetricConfig::set_time_window_ms`.
- **Breaking:** `Headers::add_header` / `add_key_value` →
  `add_with_header` / `add_with_key_value`.
- **Breaking:** `Uuid::zero()` → `Uuid::ZERO_UUID`;
  `most_sig_bits` / `least_sig_bits` → `most_significant_bits` /
  `least_significant_bits`; `to_base64_string` is removed (use `Display`).
  `KafkaFuture::completed` → `completed_future`; `join_map`,
  `join_map_results` and `then_apply_try` are removed.
  `TopicPartition::topic_arc` and `SaslConfigs::resolve_username` /
  `resolve_password` are removed; `PatternType::VALUES` and
  `ResourceType::VALUES` are removed.
- Public enum variants that carry data wrap a crate type (for example
  `TopicCollection`, `OffsetSpec`), so the payload can grow without a breaking
  change.
- Password configuration values are a `Password` type shown as `[hidden]`;
  `ProducerConfig`'s `Debug` prints keys only.
- Hardening against malformed or hostile input: bounded allocations when
  decoding arrays, strings, tagged fields and compressed data; compressed
  batches are decompressed one record at a time; a record above 2147483639
  bytes is refused; a snappy block is capped at 512 MiB; a negative string
  length inside an array is rejected.
- `metadata.recovery.strategy` is honoured by the producer and the consumer.
- Fixed: a read readiness could be stranded for about 30 seconds in the
  selector.

### C API

- **Breaking (C API):** the C API was rewritten to one set of conventions.
  Code written against 0.1.1 needs porting; the main changes are:
  - **Headers.** `confluent_kafka.h` is the full API; language bindings
    include `confluent_kafka_bindings.h`, where every non-fallible function
    appears as its `_err` twin. Every export catches Rust panics and reports
    them as an `IllegalStateError`.
  - **Errors.** Fallible functions take a trailing `kafka_common_Error_t
    **out_err`. `kafka_common_Error_code` and `kafka_common_Error_new` are
    removed: `kafka_common_Error__enum` gives the class id, each class has its
    own constructor (`kafka_common_Error_local_illegal_argument`,
    `kafka_common_Error_local_callback`, ...), payload views expose extra
    fields, and the predicates (`kafka_common_Error_is_retriable_error`, ...)
    return `int8_t`.
  - **Configuration and clients.** The `*Properties` builders
    (`kafka_producer_ProducerProperties_*`,
    `kafka_consumer_ConsumerProperties_*`,
    `kafka_admin_AdminClientProperties_*`) are replaced by
    `kafka_producer_ProducerConfig_new`, `kafka_consumer_ConsumerConfig_new`
    and `kafka_admin_AdminClientConfig_new`. `kafka_producer_KafkaProducer_new`
    and `kafka_consumer_KafkaConsumer_new` return owned class handles, freed
    with their `_destroy`; the `kafka_producer_Producer_*` /
    `kafka_consumer_Consumer_*` operations take the borrowed
    `__as_Producer` / `__as_Consumer` view. `kafka_producer_Producer_destroy`
    and `kafka_consumer_Consumer_destroy` are removed.
  - **Asynchronous calls.** The `_async` functions and their `*_callback_t`
    typedefs are replaced by `_cb` twins. Completions are queued on the client
    and run by `<Client>__execute_callbacks`; `<Client>__set_callbacks_notify`
    says when there is work. There is no dispatcher thread, and `_destroy` runs
    what is still pending. A record's future completes only after its delivery
    callback has run.
  - **Interfaces.** Traits (serializers, listeners, callbacks) are built with
    `<Interface>_new(self, fn pointers...)`, a `NULL` pointer meaning Java's
    default; classes give a borrowed `<Class>__as_<Interface>` view. An async
    method reports through `<Client>__set_callback_result`.
  - **Admin.** `kafka_admin_AdminClient_*` becomes `kafka_admin_Admin_*`
    (created with `kafka_admin_AdminClient_create`); each RPC returns a
    `<Rpc>Result_t` holding one `kafka_common_KafkaFuture_t` per key, and
    takes a `<Rpc>Options_t` in its `_with_options` form.
    `kafka_admin_MockAdminClient_create` returns a builder.
  - **Collections.** The per-type list and map helpers (`StringList`,
    `TopicPartitionList`, `OffsetAndMetadataMap`, `MetricMap`, ...) and the
    indexed accessors (`*_count`, `TopicPartitionInfo_replica`,
    `ConsumerRecord_header_*`) are replaced by `kafka_List_t` / `kafka_Map_t`
    returning accessors (`TopicPartitionInfo_replicas`, `_isr`, `_elr`).
  - **Types.** Booleans are `int8_t`; enums are handles with a C `_e` enum
    and `__enum()` (for example `ConfigEntry_source` returns a
    `kafka_admin_ConfigEntry_ConfigSource_t *`); optional values such as leader
    and group epochs are returned directly with `-1` for none instead of
    through an out-parameter; `kafka_consumer_ConsumerRecord_value` returns
    the deserialized `void *`.

## [0.1.1] - 2026-10-01

First preview release of the Rust Kafka client: a translation of the Apache
Kafka Java client (4.3.1) that keeps its architecture, names and behavior,
adapted to Rust conventions and to async Rust on Tokio.

> **Preview:** not yet recommended for production use. The public API is not
> stable before the 1.0 GA release and may change. We welcome your feedback
> while the design is still open.

### Producer

- `KafkaProducer` behind the `Producer` trait, plus `DynProducer` for
  dynamic dispatch (`Box<dyn DynProducer<K, V>>`).
- `send` and `send_with_callback`, returning a future that resolves to the
  record's `RecordMetadata`; `flush`, `partitions_for`, `metrics`, `close` and
  `close_with_timeout`.
- Idempotent producer, with the Java client's sequence tracking, epoch bump and
  recovery.
- Transactions: `init_transactions`, `begin_transaction`,
  `send_offsets_to_transaction`, `commit_transaction` and `abort_transaction`.
- Built-in sticky partitioner, `RoundRobinPartitioner`, and custom partitioners
  through the `Partitioner` trait.
- Producer interceptors.
- Batching, linger, retries with exponential backoff, delivery timeout and
  in-flight request limits, as in the Java client.
- Zero-copy send path: keys, values and headers are written straight into the
  batch buffer, and requests go out with vectored I/O.
- `MockProducer` and `MockPartitioner` for tests.

### Consumer

- `KafkaConsumer::new` returns a `Box<dyn Consumer<K, V>>` backed by the
  KIP-848 consumer group protocol (`group.protocol=consumer`).
- Subscription by topic list or by `SubscriptionPattern` (a regular expression
  evaluated by the broker), with or without a `ConsumerRebalanceListener`;
  manual assignment with `assign`; `unsubscribe`.
- `poll`, returning `ConsumerRecords` together with the next offset to consume for each partition.
- Offset management: `commit_sync` and `commit_async` (with or without explicit
  offsets and an `OffsetCommitCallback`), `committed`, `position`, `seek_*`,
  `seek_to_beginning` and `seek_to_end`.
- Offset lookups: `beginning_offsets`, `end_offsets`, `offsets_for_times` and
  `current_lag`.
- `pause` / `resume`, `partitions_for`, `list_topics`, `group_metadata`,
  `metrics`, `enforce_rebalance`.
- Rebalance listeners run on the caller's task during `poll`, and can call back
  into the consumer through a `ConsumerHandle` (for example to `commit_sync`
  on revocation).
- `wakeup` to interrupt a blocking call, and `close` / `close_with_options`
  using `CloseOptions`.
- `read_committed` and `read_uncommitted` isolation levels.
- Consumer interceptors.
- Zero-copy receive path: deserializers borrow from the fetched buffer.
- `MockConsumer` for tests.

### Admin client

- `AdminClient::create` returns a `Box<dyn Admin>`. Every operation returns
  immediately with a result holding one `KafkaFuture` per key, as in Java, and
  has a `_with_options` variant.
- Topics and partitions: `create_topics`, `delete_topics`, `list_topics`,
  `describe_topics_*`, `create_partitions`, `delete_records`, `list_offsets`,
  `elect_leaders`, `alter_partition_reassignments`,
  `list_partition_reassignments`.
- Cluster and configuration: `describe_cluster`, `describe_configs`,
  `incremental_alter_configs`, `list_config_resources`, `describe_features`,
  `update_features`.
- Log directories: `describe_log_dirs`, `alter_replica_log_dirs`,
  `describe_replica_log_dirs`.
- Groups: `list_groups`, `describe_consumer_groups`, `describe_classic_groups`,
  `list_consumer_group_offsets_*`, `alter_consumer_group_offsets`,
  `delete_consumer_group_offsets`, `delete_consumer_groups`.
- Transactions and producers: `describe_producers`, `describe_transactions`,
  `list_transactions`, `abort_transaction`, `fence_producers`,
  `force_terminate_transaction`.
- Security: `create_acls`, `describe_acls`, `delete_acls`,
  `describe_client_quotas`, `alter_client_quotas`,
  `describe_user_scram_credentials`, `alter_user_scram_credentials`, and
  delegation tokens (`create`, `renew`, `expire`, `describe`).
- `MockAdminClient` for tests, mirroring the Java mock.

### Common

- Networking on Tokio non-blocking I/O, with one selector multiplexing all
  broker connections, as in the Java client.
- Wire protocol generated from the official Kafka JSON message definitions,
  including flexible versions and tagged fields.
- TLS through rustls with the aws-lc-rs crypto provider, including mutual TLS
  and hostname verification. Keys and certificates are PEM, either as files or
  inline in the configuration.
- SASL/PLAIN authentication over `SASL_PLAINTEXT` and `SASL_SSL`.
- Compression: gzip, snappy, lz4 and zstd.
- Serializers and deserializers for strings and byte arrays, a deserializer for
  `Bytes`, plus
  custom ones through the `Serializer` / `Deserializer` traits.
- A single `Error` type for every fallible API, with predicates that rebuild
  the Java exception hierarchy (`is_retriable_error`, `is_api_error`,
  `is_authorization_error`, ...).
- Client metrics and configuration keys use the Java client's names.
- Every client logs a preview warning at startup through the `log` crate.

### Known limitations

- Classic consumer group protocol (`group.protocol=classic`) and client-side
  partition assignors are not supported.
- Share groups (KIP-932) and the share consumer are not included.
- Of the SASL mechanisms, only `PLAIN` is supported (no SCRAM, GSSAPI or
  OAUTHBEARER).
- JKS and PKCS12 keystores and truststores are not supported; convert them to
  PEM.