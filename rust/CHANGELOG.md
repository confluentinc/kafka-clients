# Changelog

All notable changes to the `confluent-kafka` crate are documented in this file.

Before 1.0 the public API is not stable, and any minor release may contain
breaking changes.

## [Unreleased]

### General availability

- First generally available (GA) release of the Rust client, version 1.0.0.
- The FFI layer (the `ffi` feature) is experimental right now.
- Windows is not supported yet.

### Common

- **Breaking:** Mozilla's root certificates, trusted without
  `ssl.truststore.location` or `ssl.truststore.certificates`, now come from the
  default feature `webpki-roots`. With default features on nothing changes. A
  build with `default-features = false` must add `features = ["webpki-roots"]`;
  without it the build stops with "confluent-kafka needs a default trust
  store", since no other default trust store exists yet. A later one is then a
  feature of its own, and `default-features = false` selects it without the
  bundle.
- **Breaking:** `Uuid::from_string` applies Java's `Base64.getUrlDecoder()` rules to `=`
  padding: a `=` that starts a 4-character unit, a single `=` after two
  characters, or anything after the padding is an `IllegalArgumentError` with
  Java's message, where it was accepted or reported as a wrong decoded length.
  A one-character input and a last unit of one character also report Java's
  messages (#269).

## [1.0.0-rc.1] - 2026-10-28

### Consumer

- **Breaking:** `Consumer::committed` / `committed_with_timeout` return
  `HashMap<TopicPartition, Option<OffsetAndMetadata>>`: every queried
  partition is a key, `None` for one with no committed offset, as Java maps it
  to `null`. It was absent from the map before.
- **Breaking:** `Consumer::offsets_for_times` /
  `offsets_for_times_with_timeout` return
  `HashMap<TopicPartition, Option<OffsetAndTimestamp>>`: every queried
  partition is a key, `None` for one without an offset for its timestamp, as
  in Java. A zero timeout maps every queried partition to `None`, as Java's
  `emptyResults()`. The same holds for `ConsumerHandle`.
- **Breaking:** `Deserializer::deserialize_with_headers` and
  `deserialize_with_headers_bytes` take the record's headers as
  `&mut RecordHeaders`, so a deserializer can change them, as Java's can.
  Update the parameter type in your implementations.
- `ConsumerHandle` refuses a closed consumer as the consumer does: once
  `close()` has returned, its async operations return `IllegalStateError`
  ("This consumer has already been closed.") instead of
  `ConcurrentModificationError`, and `assignment`, `paused` and
  `group_metadata` panic with that message instead of returning stale data.
  A listener that `close()` runs still calls back in; `wakeup` stays callable.

### Admin client

- **Behaviour change:** a retriable error during `close(timeout)` no longer
  fails the call at once: until the deadline the call is retried, as Java's
  `close` does, and `close` returns once it completes. New calls are still
  rejected as soon as `close` is called.
- **Breaking:** an explicit `default.api.timeout.ms` below
  `request.timeout.ms` fails client creation with Java's `The specified value
  of default.api.timeout.ms must be no smaller than the value of
  request.timeout.ms.`, wrapped in `Failed to create new KafkaAdminClient`.
  When `default.api.timeout.ms` is not set and `request.timeout.ms` is above
  the API timeout's 60000 ms default, the API timeout is raised to the request
  timeout, with a WARN, as Java's `configureDefaultApiTimeoutMs` does.
- A negative per-call `timeout_ms` is treated as `0`, as Java's
  `calcDeadlineMs` does: the call is sent once instead of timing out before it
  was sent.
- `Admin::list_transactions()` without options sends no duration filter
  (`-1`), as Java's does; `ListTransactionsOptions::default()` now equals
  `ListTransactionsOptions::new()`.
- The admin client's `NetworkClient` allows one in-flight request per
  connection and has Java's one-hour default request timeout, as
  `KafkaAdminClient.createInternal` builds it. A broker that answers the
  `ApiVersions` handshake after `request.timeout.ms` is waited for instead of
  being disconnected, and a node with a request in flight is no longer picked
  as the least-loaded node.
- New public `EndpointType`, Java's `org.apache.kafka.clients.admin.EndpointType`
  (KIP-919): `Unknown`, `Broker` and `Controller`, with `id()` and
  `from_id(i8)`, which answers `Unknown` for an id it does not know.
- `bootstrap.controllers` (KIP-919) is defined again (Rust, C API, Python), and
  `AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG` is back: a `LIST`, empty
  by default, validated as Java's `AdminClientConfig` validates it, and read
  by the client whichever list it bootstraps from, so it is no longer listed
  among the unused configurations. A client configured with it alone
  bootstraps from the KRaft controller quorum, as Java's does: it refreshes
  its metadata with `DescribeCluster` for the CONTROLLER endpoint, sends the
  calls the controllers serve (describing the cluster, the configs and the
  features, altering configs, ACLs, partition reassignments, updating the
  features, unregistering a controller) to the active controller or the node
  they name, and fails every other call at once with
  `UnsupportedEndpointTypeError`.
- **Breaking:** setting neither `bootstrap.servers` nor `bootstrap.controllers`
  fails client creation, still wrapped in `Failed to create new
  KafkaAdminClient`, with Java's `KafkaAdminClient.determineBootstrapType`
  `ConfigError` `You must set either bootstrap.servers or
  bootstrap.controllers`, where it was `ClientUtils`'s `No resolvable
  bootstrap urls given in bootstrap.servers`; setting both fails with `You
  cannot set both bootstrap.servers and bootstrap.controllers`.
- **Breaking (Rust, Python):** a config entry can have no type, as Java's
  `ConfigEntry.type()` can be `null`. `ConfigEntry::r#type()` returns
  `Option<ConfigType>` and `ConfigEntryOptionsBuilder::set_config_type` takes
  one. The entries of a `createTopics` result have no type
  (`KafkaAdminClient.java:1992-2002`): they reported `ConfigType::Unknown` and
  printed `type=UNKNOWN`, and now report `None`, print `type=null` and differ
  from an entry whose type is `Unknown`. In Python `ConfigEntry.type()` returns
  `None` for them.
- **Breaking:** `GroupState::name()`, `ClassicGroupState::name()` and
  `GroupType::name()` return Java's `Enum.name()`, the constant name
  (`PREPARING_REBALANCE`, `CONSUMER`), instead of the `toString()` text
  (`PreparingRebalance`, `Consumer`), which `Display` still prints and `parse`
  still reads.
- **Breaking:** `Admin::delete_topics` and `Admin::delete_topics_with_options`
  are renamed `delete_topics_with_topic_collection` and
  `delete_topics_with_topic_collection_options`, and Java's two overloads
  taking topic names are added as `delete_topics_with_string_slice` and
  `delete_topics_with_string_slice_options`. Java names the parameter of all
  four `topics`, so each is named after its parameter's type (CLAUDE.md §2).
- **Breaking:** `ClassicGroupDescription::new` is Java's six-argument
  constructor, which gives the description an empty set of authorized
  operations; the seven-argument constructor is now
  `ClassicGroupDescription::with_authorized_operations`.
- **Breaking:** `CreateDelegationTokenOptions::set_owner` and
  `RemoveMembersFromConsumerGroupOptions::set_reason` take an `Option`, so
  `None` clears a value set earlier, as Java's `owner(null)` and
  `reason(null)` do.
- **Breaking:** `NewTopic::set_configs` takes an
  `Option<BTreeMap<String, String>>`: `None` clears configs set earlier, as
  Java's `configs(null)` does. A config value cannot be null: the broker
  rejects a null config value of a new topic with `INVALID_CONFIG`. In Python,
  `create_topics` raises `TypeError("new_topics must not contain a NewTopic
  with a None config value")` for one.
- `NewTopic`'s `Display` prints Java's `toString()`, the maps as `{k=v}` and a
  missing map or value as `null`, instead of Rust's debug syntax.
- `AclOperation::values()` and `AclPermissionType::values()` return the
  constants in declaration order, as Java's `values()` does.
- `MockAdminClient` stores a config value of null as null, as Java's mock
  does: a `NewTopic` config given to `createTopics` with a `None` value, or a
  `SET` with a `None` value through `incrementalAlterConfigs`, reads back from
  `describe_configs` as `None` instead of `Some("")`. A group config set to
  null reads back as its default when it has one, as Java's `putIfAbsent`
  overlay replaces a null value; the Python `MockAdminClient` now does the
  same.

### Common

- **Breaking:** `Sensor::record`, `record_with_value` and
  `record_with_value_time_ms` return `Result<(), Error>` and check the
  sensor's quotas, as Java's do: a violated quota returns
  `QuotaViolationError` and stops the parent sensors from recording. New
  `record_with_value_time_ms_check_quotas` translates Java's
  `record(double, long, boolean)`.
- **Breaking:** `GroupAuthorizationError::group_id` returns `Option<&str>`,
  `None` where Java's `groupId()` is `null`. `GroupAuthorizationError::new`
  takes the message only, as Java's `(String message)`;
  `with_group_id(group_id, message)` takes the pair.
- **Breaking:** `Serializer::configure` / `close` and `Deserializer::configure`
  / `close` return a future of `Result<(), Error>`, so a serde can report a
  failure. The producer and the consumer close both serdes and return the
  first failure as the cause of `Failed to close kafka producer` /
  `Failed to close kafka consumer`, as Java's `closeQuietly`. Return `Ok(())`
  from your implementations.
- `KafkaFuture::then_apply` runs its function once, when this future
  completes (inside `then_apply` when it already has), and completes the
  returned future with its result, as Java does. It ran on every `get` of the
  returned future before, and never when nobody awaited it. `KafkaFuture::all_of`
  completes once every future has, with the error of the first failed one in
  the list, as Java's `allOf`; it returned the first failure in list order
  without waiting for the others before.
- **Breaking:** `Producer::metrics`, `Consumer::metrics` and `Metrics::metrics`
  return a `MetricsView`, a live read-only view
  of the registry, as Java's `Collections.unmodifiableMap(metrics.metrics())`:
  a metric registered or removed after the call shows in it. It returned a
  snapshot `HashMap` before. `MetricsView::snapshot` gives the whole map as it
  is now, without copying it; `MetricsView::from(HashMap)` builds a standalone
  view for a test double.
- The minimum rustls version is now 0.23.46, which fixes
  [RUSTSEC-2026-0346](https://rustsec.org/advisories/RUSTSEC-2026-0346): the
  `Debug` output of rustls's client session values exposed TLS session secrets.
  The client never formats those values, but the C and Python bindings were
  built with the affected 0.23.45.
- The minimum webpki-roots version is now 1.0.10, which stops trusting the
  emSign (eMudhra) intermediate CAs whose private keys leaked
  ([RUSTSEC-2026-0381](https://rustsec.org/advisories/RUSTSEC-2026-0381)).
  A Rust application should also update rustls-webpki to 0.103.16
  ([RUSTSEC-2026-0382](https://rustsec.org/advisories/RUSTSEC-2026-0382)) in
  its lockfile.

### C API

- **Breaking:** `kafka_consumer_Consumer_committed*` and
  `kafka_consumer_Consumer_offsets_for_times*` (and the `ConsumerHandle`
  ones) keep every queried partition in the returned map, with a `NULL` value
  for one with no offset.
- **Breaking:** `kafka_common_metrics_Sensor_record`,
  `_record_with_value` and `_record_with_value_time_ms` take a trailing
  `out_err`; new `kafka_common_metrics_Sensor_record_with_value_time_ms_check_quotas`.
- **Breaking:** `kafka_common_GroupAuthorizationError_new` takes the message
  only, `kafka_common_GroupAuthorizationError_with_group_id` the pair, and
  `kafka_common_GroupAuthorizationError_group_id` returns `NULL` when the
  group is unknown.
- **Breaking:** a C deserializer receives the record's own headers as a
  mutable `kafka_common_header_internals_RecordHeaders_t *` and may change
  them, as Java's may: `Deserializer_deserialize_with_headers`,
  `Deserializer_deserialize_with_headers_bytes`, their `_cb` twins and the
  `deserialize_with_headers_async_fn_t` /
  `deserialize_with_headers_bytes_async_fn_t` function-pointer types drop
  `const`.
- `kafka_common_KafkaFuture_then_apply` runs the C function once, when the
  source future completes, and completes the returned future with its result:
  inside the call when the source is already complete, else from the client's
  `__execute_callbacks`, where it is queued on completion. It ran on the first
  `get` of the returned future before.
- **Breaking:** `kafka_common_serialization_{Serializer,Deserializer}_configure`
  and `_close` take a trailing `kafka_common_Error_t **out_err`, and their
  `_configure_cb_t` / `_close_cb_t` completions receive a
  `kafka_common_Error_t *` (`NULL` on success) before `opaque`. An error a C
  serde reports through `__set_callback_result` reaches the caller instead of
  being logged.
- **Breaking:** `kafka_producer_Producer_metrics`, `kafka_consumer_Consumer_metrics`
  and `kafka_common_metrics_Metrics_metrics` return a live
  `kafka_common_metrics_MetricsView_t *` (`_len`, `_is_empty`, `_contains_key`,
  `_get`, `_iterator`, `_destroy`) instead of a snapshot `kafka_Map_t *`.
- New `kafka_Iterator_t` (`kafka_Iterator_has_next`, `_next`, `_destroy`), Java's
  `java.util.Iterator`, and `kafka_Map_Entry_t` (`kafka_Map_Entry_key`,
  `_value`), Java's `Map.Entry`, its elements when iterating a map.
- `kafka_admin_EndpointType_t`, Java's `EndpointType`: the singletons
  `kafka_admin_EndpointType_unknown`, `_broker` and `_controller`, the C enum
  `kafka_admin_EndpointType_e` read with `kafka_admin_EndpointType__enum`, and
  `_id`, `_from_id` and `_to_string`.
- **Breaking (C API):** `kafka_admin_ConfigEntry_type` returns `NULL` for an
  entry without a type, such as the entries of
  `kafka_admin_CreateTopicsResult_config`, so a caller must check it before
  `kafka_admin_ConfigEntry_ConfigType__enum`, and
  `kafka_admin_ConfigEntryOptionsBuilder_set_config_type` takes `NULL` for
  Java's `null` type.
- **Breaking (C API):** `kafka_common_GroupState_name`,
  `kafka_common_ClassicGroupState_name` and `kafka_common_GroupType_name`
  return the constant name (`PREPARING_REBALANCE`), as Java's `Enum.name()`
  does. The text `_parse` reads (`PreparingRebalance`) is what the
  `_to_string` functions return.
- **Breaking (C API):** `kafka_admin_Admin_delete_topics` and
  `kafka_admin_Admin_delete_topics_with_options` are renamed
  `kafka_admin_Admin_delete_topics_with_topic_collection` and
  `kafka_admin_Admin_delete_topics_with_topic_collection_options`. The new
  `kafka_admin_Admin_delete_topics_with_string_slice` and
  `kafka_admin_Admin_delete_topics_with_string_slice_options` take the topic
  names as a `kafka_List_t` of `const char *`.
- **Breaking (C API):** `kafka_admin_ClassicGroupDescription_new` takes the
  six arguments of Java's short constructor; the seven-argument form is
  `kafka_admin_ClassicGroupDescription_with_authorized_operations`.
- **Breaking (C API):** `kafka_admin_RemoveMembersFromConsumerGroupOptions_set_reason`
  with `NULL` clears the reason instead of setting an empty one, and
  `kafka_admin_CreateDelegationTokenOptions_set_owner` with `NULL` clears the
  owner, as Java's `reason(null)` and `owner(null)` do.
- **Breaking (C API):** a config value passed to
  `kafka_admin_NewTopic_set_configs` must not be `NULL`, a value the broker
  rejects for a new topic; it used to be sent as an empty string. A `NULL`
  value panics and leaves the configs unchanged, which
  `kafka_admin_NewTopic_set_configs_err` reports as an `IllegalStateError`. A
  `NULL` map clears the configs, as Java's `configs(null)` does, where it used
  to set an empty map.
- `kafka_common_acl_AclOperation_values` and
  `kafka_common_acl_AclPermissionType_values` return an owned list of the
  borrowed singletons in declaration order.

## [0.3.0-beta.1] - 2026-10-23

### Producer

- **Breaking:** `KafkaProducer::new` takes its serializers as
  `impl Serializer<K> + 'static` / `impl Serializer<V> + 'static` and boxes
  them itself: pass `StringSerializer::new()` where `Box::new(StringSerializer::new())`
  was passed. `MockProducerOptionsBuilder::set_key_serializer` /
  `set_value_serializer` likewise take `impl Serializer<K> + 'static` instead
  of `Option<Box<dyn DynSerializer<K>>>`; leaving a setter uncalled is Java's
  `null`, which the mock's no-argument constructor passes.
- **Breaking:** `Serializer::serialize_with_headers` takes the record's headers
  as `&mut RecordHeaders`, so a serializer can add or remove headers, as Java's
  can. The record carries what the headers hold once the value serializer
  returns; they become read-only after serialization. Update the parameter
  type in your implementations.
- `Serializer` gains Java's `configure(configs, is_key)` and `close()`, as
  async default methods that do nothing, so existing implementations compile
  unchanged.
- `KafkaProducer::close` / `close_with_timeout` now close the key serializer,
  then the value serializer, once each; a failure is returned as
  "Failed to close kafka producer". A `KafkaProducer` whose constructor fails
  closes the serializers it was given.
- A closed `KafkaProducer` is freed: reference cycles in the metrics registry
  kept it alive after `close`.
- **Breaking:** `ProducerConfig::new` parses and validates every key with
  Java's `ConfigDef` (`ProducerConfig.java`), so a value Java rejects fails
  with Java's `ConfigError` message, `Invalid value <v> for configuration
  <key>: <reason>`, and a value of the wrong type with Java's `parseType`
  message. `partitioner.type` keeps its message for an unsupported value, and
  its value is now trimmed, as every `STRING` key is.
- `compression.gzip.level`, `compression.lz4.level` and
  `compression.zstd.level` are honoured, with Java's ranges and defaults: the
  producer used to compress every codec at its default level.
- `ProducerConfig` answers Java's `AbstractConfig` getters: `get_short`,
  `get_int`, `get_long`, `get_double`, `get_boolean`, `get_string`,
  `get_list` (each `Result<Option<T>, Error>`, `None` for Java's `null`),
  `unused`, `ignore`, `log_unused`, and the static `config_names`. Its
  `PartialEq` / `Hash` compare the properties it was built from, as Java's
  `equals` / `hashCode` do. `get_string("acks")` answers the number
  `parseAcks` made of it (`-1` for `all`).
- New `ProducerConfig` constants for keys Java's `ProducerConfig` names:
  `COMPRESSION_GZIP_LEVEL_CONFIG`, `COMPRESSION_LZ4_LEVEL_CONFIG`,
  `COMPRESSION_ZSTD_LEVEL_CONFIG`,
  `SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG` and
  `SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG`.
- The producer logs its configuration values at INFO when it is built, as
  Java's does, and then, in one INFO line, the supplied keys it did not use:
  `These configurations '[a, b]' were supplied but are not used yet.` This
  replaces the warning logged per unknown key and the `ssl.*` warnings logged
  when `security.protocol` does not use them; the `ssl.*` / `sasl.*` keys are
  listed in the line instead. `key.serializer`, `value.serializer`,
  `partitioner.class`, `interceptor.classes`, `metric.reporters` and
  `security.providers` are not producer configs here (the config takes no
  class), so they are listed too.
- `KafkaProducer`, `MockProducer`, `MockProducerOptions`,
  `MockProducerOptionsBuilder`, `ProducerRecordOptionsBuilder` and
  `RoundRobinPartitioner` implement `Debug`. `KafkaProducer` prints its client
  id only, never its configuration; `MockProducer` its flags and how many
  records it sent; the mock's options whether a partitioner and serializers
  are set.

### Consumer

- **Breaking:** `ConsumerConfig` is no longer `Clone`: a configuration holder
  belongs to the client built from it, as Java's `KafkaConsumer` builds its
  own `ConsumerConfig` from the properties. Build another one with
  `ConsumerConfig::new(&props)` for a second consumer.
- **Breaking:** `ConsumerRecord`, `ConsumerRecords`, `CloseOptions` and
  `ConsumerPartitionAssignor`'s `Subscription` and `Assignment` no longer
  implement `PartialEq` and `Eq`: Java's classes do not override `equals`, so
  a field added to them later cannot break a promise of equality. Compare the
  getters instead.
- **Breaking:** `KafkaConsumer::new` takes its deserializers as
  `impl Deserializer<K>` / `impl Deserializer<V>` and boxes them itself: pass
  `StringDeserializer::new()` where `Box::new(StringDeserializer::new())` was
  passed.
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
- **Breaking:** `ConsumerConfig::SECURITY_PROVIDERS_CONFIG` is removed.
  `security.providers` and `partition.assignment.strategy` are no longer
  consumer configs: nothing read them (JSSE providers and client-side assignors
  have no counterpart here). Setting either now lists it in the unused-config
  INFO line, and `get_string` on them returns a `ConfigError`.
- **Breaking:** `ConsumerConfig::new` parses and validates every key with
  Java's `ConfigDef` (`ConsumerConfig.java`), so values Java rejects now fail
  with Java's `ConfigError` message, `Invalid value <v> for configuration
  <key>: <reason>`: a negative `auto.commit.interval.ms`, `fetch.min.bytes`,
  `fetch.max.bytes`, `fetch.max.wait.ms`, `max.partition.fetch.bytes`,
  `metadata.max.age.ms`, `request.timeout.ms`, `default.api.timeout.ms`, the
  `reconnect.backoff.*` / `retry.backoff.*` keys or `metrics.sample.window.ms`;
  `max.poll.interval.ms`, `max.poll.records` or `metrics.num.samples` below
  `1`; `send.buffer.bytes` / `receive.buffer.bytes` below `-1`; and an
  `isolation.level` other than `read_committed` / `read_uncommitted` (the check
  is case-sensitive, as Java's), which used to be accepted as given.
- **Breaking:** as in Java, `enable.auto.commit=true` without `group.id` fails
  with an `InvalidConfigurationError` ("enable.auto.commit cannot be set to
  true when default group id (null) is used."), and `enable.auto.commit`
  defaults to `false` without a `group.id`.
- **Breaking:** as in Java, the keys the chosen `group.protocol` does not
  support are rejected with a `ConfigError`: `group.remote.assignor` with
  `classic`, `session.timeout.ms` and `heartbeat.interval.ms` with `consumer`
  (and `share.acknowledgement.mode` / `share.acquire.mode` with either), e.g.
  "session.timeout.ms cannot be set when group.protocol=CONSUMER". An invalid
  `group.protocol` reports Java's message, `String must be one of
  (case insensitive): CONSUMER, CLASSIC`.
- **Breaking:** as in Java, a blank or unknown `share.acquire.mode` /
  `share.acknowledgement.mode` fails with the message of Java's
  `ShareAcquireMode.Validator` / `ShareAcknowledgementMode.Validator`, e.g.
  "Invalid value `` for configuration share.acquire.mode. The value must
  either be 'batch_optimized' or 'record_limit'."; a blank value used to be
  accepted. `share.acquire.mode` defaults to Java's `BATCH_OPTIMIZED`, which
  `get_string` returns, instead of `batch_optimized`.
- `ConsumerConfig` answers Java's `AbstractConfig` getters: `get_short`,
  `get_int`, `get_long`, `get_double`, `get_boolean`, `get_string`,
  `get_list` (each `Result<Option<T>, Error>`, `None` for Java's `null`),
  `unused`, `ignore`, `log_unused`, and the static
  `config_names`. Its `PartialEq` / `Hash` compare the properties it was built
  from, as Java's `equals` / `hashCode` do.
- The consumer logs its configuration values at INFO when it is built, as
  Java's does, and then, in one INFO line, the supplied keys it did not use:
  `These configurations '[a, b]' were supplied but are not used yet.` This
  replaces the warning logged per unknown key.
- **Breaking (Rust, C API, Python):** a consumer whose channel builder cannot
  be created (for example an unreadable `ssl.truststore.location`) now fails
  `KafkaConsumer::new` with a `ConfigError` cause, Java's `ConfigException`,
  as the producer and the admin client already did, instead of
  `LocalIllegalArgumentError`; the outer "Failed to construct kafka consumer"
  error is unchanged.
- `MockConsumer` is `Send` but not `Sync` by declaration, as `KafkaConsumer`
  is, rather than through one of its fields: a future field cannot make it
  `Sync` by accident.
- `GroupProtocol` and `CloseOptions`' `GroupMembershipOperation` implement
  `PartialOrd` and `Ord`, ordering their values as Java's `Enum.compareTo`,
  by declaration.
- `GroupMembershipOperation` implements `Display`, printing the Java
  constant's name (`LEAVE_GROUP`), as Java's `Enum.toString()` does.
- `ConsumerPartitionAssignor`'s `Subscription` and `Assignment` implement
  `Display` in the format of Java's `toString()`: lists print as `[a, b]`,
  an absent group instance id or rack id as `null`, and the user data, when
  set (even empty), only as its size.
- `KafkaConsumer`, `MockConsumer`, `ConsumerHandle`, `ConsumerRecordOptions`,
  `ConsumerRecordOptionsBuilder` and `SubscriptionOptionsBuilder` implement
  `Debug`. `KafkaConsumer` prints its client id and group id only, never its
  configuration; `MockConsumer` whether it is closed.

### Admin client

- **Breaking:** `DeletedRecords`, `RecordsToDelete`, `FeatureUpdate`,
  `FinalizedVersionRange`, `SupportedVersionRange`, `OffsetSpec` and its
  `TimestampSpec` are no longer `Copy`: a public struct is never `Copy`, so a
  field added to it later (a `String`, a collection) cannot break the promise.
  They stay `Clone`; write `.clone()` where a value was copied implicitly.
- **Breaking:** the admin `*Options` types, `DeletedRecords`,
  `ListOffsetsResultInfo`, `NewPartitionReassignment`, `NewPartitions`,
  `OffsetSpec` and its `TimestampSpec`, `PartitionProducerState`,
  `PartitionReassignment`, `ReplicaInfo`, `ReplicaLogDirInfo` and
  `TopicListing` no longer implement `PartialEq` and `Eq`: Java's classes do
  not override `equals`. Compare the getters instead.
- **Breaking:** `AdminClientConfig::new` parses and validates every key with
  Java's `ConfigDef` (`AdminClientConfig.java`), so values Java rejects now
  fail with Java's `ConfigError` message, `Invalid value <v> for configuration
  <key>: <reason>`: a negative `metadata.max.age.ms`, `reconnect.backoff.ms`,
  `reconnect.backoff.max.ms`, `retry.backoff.ms`, `retry.backoff.max.ms`,
  `request.timeout.ms`, `retries`, `default.api.timeout.ms`,
  `metrics.sample.window.ms` or `metadata.recovery.rebootstrap.trigger.ms`;
  `send.buffer.bytes` / `receive.buffer.bytes` below `-1`;
  `metrics.num.samples` below `1`; and a `metrics.recording.level` or
  `metadata.recovery.strategy` outside Java's values.
- **Breaking (Rust, C API, Python):** `bootstrap.controllers` is no longer
  defined, and `AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG` is removed.
  Bootstrapping through the controllers (KIP-919) is not implemented, so the
  key stays undefined until it is: a value set for it is listed among the
  unused configurations, where `AdminClientConfig::new` used to fail. Without
  `bootstrap.servers` the configuration now builds, as Java's does, and the
  client's constructor fails, whatever `bootstrap.resolve.timeout.ms` is, with
  `Failed to create new KafkaAdminClient` caused by `ClientUtils`'s
  `ConfigError` `No resolvable bootstrap urls given in bootstrap.servers`, the
  producer's and the consumer's message, where
  `AdminClientConfig::new` used to fail with Java's
  `KafkaAdminClient.determineBootstrapType` message `You must set either
  bootstrap.servers or bootstrap.controllers`.
- **Breaking (Rust, C API, Python):** `enable.metrics.push` is no longer
  defined on `ProducerConfig`, `ConsumerConfig` or `AdminClientConfig`, and
  `ConsumerConfig::ENABLE_METRICS_PUSH_CONFIG`, released since 0.1.0, is
  removed (`ProducerConfig::ENABLE_METRICS_PUSH_CONFIG` was never released).
  Client telemetry (KIP-714) is not implemented, so the key stays undefined
  until it is: a value set for it is listed among the unused configurations
  instead of being parsed and ignored, its typed getters answer `Unknown
  configuration 'enable.metrics.push'`, and a value that is not a boolean no
  longer fails the configuration.
- `AdminClientConfig` answers Java's `AbstractConfig` getters: `get_short`,
  `get_int`, `get_long`, `get_double`, `get_boolean`, `get_string`,
  `get_list` (each `Result<Option<T>, Error>`, `None` for Java's `null`),
  `unused`, `ignore`, `log_unused`, and the static
  `config_names`. Its `PartialEq` / `Hash` compare the
  properties it was built from, as Java's `equals` / `hashCode` do.
- The admin client logs its configuration values at INFO when it is built, as
  Java's `Admin.create` does, and then, in one INFO line, the supplied keys it
  did not use: `These configurations '[a, b]' were supplied but are not used
  yet.` It used to log neither. The line also lists keys Java reads and this
  client does not yet (the `metrics.*` keys), and the `ssl.*` / `sasl.*` keys
  when `security.protocol` does not use them.
- **Behaviour change:** the admin client follows `metadata.recovery.strategy`,
  whose default is Java's `rebootstrap`, where it always ran as `none`. Once
  bootstrapped, it goes back to the `bootstrap.servers` addresses when no
  known broker is available or ready, when a metadata update has gone
  unanswered for `metadata.recovery.rebootstrap.trigger.ms` (300000 ms by
  default), and at once on a `REBOOTSTRAP_REQUIRED` metadata response. Set
  `metadata.recovery.strategy=none` for the previous behaviour.
- `send.buffer.bytes` (default 128 KiB) and `receive.buffer.bytes` (64 KiB)
  size the admin client's sockets; it used the operating system's defaults.
  These two keys and the `metadata.recovery.*` keys are no longer listed among
  the unused configurations.
- `socket.connection.setup.timeout.max.ms` is honoured; the admin client used
  `socket.connection.setup.timeout.ms` for both bounds.
- The admin types whose Java class overrides `hashCode()` implement `Hash`, so
  they can key a `HashMap` or fill a `HashSet` as in Java:
  `AbortTransactionSpec`, `AlterConfigOp`, `ClassicGroupDescription`, `Config`,
  `ConfigEntry`, `ConfigSynonym`, `ConsumerGroupDescription`, `FeatureMetadata`,
  `ListConsumerGroupOffsetsSpec`, `MemberAssignment`, `MemberDescription`,
  `NewTopic`, `DescribeProducersOptions`, `ListTopicsOptions`,
  `ListTransactionsOptions`, `TopicDescription` (without its topic id, as its
  equality), `TransactionDescription` and `UserScramCredentialsDescription`.
  A set or map field hashes independently of its order, as Java's
  `Set.hashCode()` and `Map.hashCode()` do.
- The admin enums `OpType`, `ConfigSource`, `ConfigType`, `UpgradeType`,
  `ScramMechanism` and `TransactionState` implement `PartialOrd` and `Ord`,
  ordering their values as Java's `Enum.compareTo`, by declaration.
- `FeatureUpdate`'s `UpgradeType` and `ScramMechanism` implement `Display`,
  printing the Java constant's name (`SAFE_DOWNGRADE`, `SCRAM_SHA_256`), as
  Java's `Enum.toString()` does.
- `ConsumerGroupDescription`, `ClassicGroupDescription`,
  `AbortTransactionOptions`, `DescribeProducersOptions`,
  `DescribeTransactionsOptions`, `FenceProducersOptions`,
  `TerminateTransactionOptions` and `ListTransactionsOptions` implement
  `Display` in the format of Java's `toString()`. An absent value prints as
  `null`, and a set as `[a, b]`. `ListTransactionsOptions` prints its states
  in declaration order and its producer ids in ascending order.
- `KafkaAdminClient`, `AlterConfigsResult`, `AlterReplicaLogDirsResult`,
  `DescribeClusterResult`, `DescribeConfigsResult`, `DescribeLogDirsResult`,
  `DescribeReplicaLogDirsResult`, `ListConfigResourcesResult` and
  `ConfigEntryOptionsBuilder` implement `Debug`. `KafkaAdminClient` prints its
  client id only, never its configuration.

### Common

- **Breaking:** `SslConfigs` and `SaslConfigs` are no longer `Clone`, like the
  client configurations that hold them.
- **Breaking:** `Quota` is no longer `Copy`, and `MetricConfig::quota()`
  returns `Option<&Quota>` instead of a copy. `Quota` stays `Clone`; write
  `config.quota().cloned()` where an owned quota is needed.
- **Breaking:** `TopicCollection` and its `TopicIdCollection` and
  `TopicNameCollection`, `ClientQuotaAlteration`, `SslConfigs` and
  `SaslConfigs` no longer implement `PartialEq` (and `Eq`): Java's classes do
  not override `equals`. `ClientQuotaAlteration`'s `Op` keeps its equality,
  as Java's `Op.equals`. Compare the getters instead.
- **Breaking:** `ProducerConfig`, `ConsumerConfig` and `AdminClientConfig` no
  longer implement `Default`. Build them with `::new(&configs)` from a
  `HashMap<String, String>` (an empty map gives the defaults).
- New `CommonClientConfigs::DEFAULT_SECURITY_PROTOCOL`.
- `SslConfigs::DEFAULT_SSL_KEYSTORE_TYPE` and `DEFAULT_SSL_TRUSTSTORE_TYPE`
  are `"PEM"`, the default the clients already used, instead of Java's `"JKS"`,
  which the SSL factory rejects.
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
- Java's `ConfigDef` (types, defaults, the `parse` checks and messages, and
  the `Range`, `ValidList`, `ValidString`, `CaseInsensitiveValidString`,
  `NonEmptyString`, `NonNullValidator`, `CompositeValidator` and `ListSize`
  validators) and the `ssl.*` / `sasl.*` key definitions are translated as
  crate-internal code, for the client configs to be built on. Deferred to
  later work, because each needs a new public type: a public `ConfigDef`,
  its validators, `ConfigValue` and `LambdaValidator`; `get_password` (a
  public `Password`); `type_of` (a public `ConfigDef.Type`); `originals*()` /
  `values*()` (Java returns a `RecordingMap`, and a plain `HashMap` now would
  fix the wrong return type); `documentation_of` (the keys' documentation
  strings are not kept, as only it and Java's documentation renderers read
  them); how Java's `Object` config values are typed; and, in C, an
  `__as_AbstractConfig` view. The class-valued keys
  (`ssl.engine.factory.class`, the `sasl.*.class` keys) stay undefined, as
  configurations never take a class. `config.providers` stays undefined too
  until config providers are translated, so a value set for it is listed as
  unused instead of being accepted and ignored.
- **Breaking:** `DynSerializer`, `DynDeserializer` and `InlineFuture` are no
  longer public. They are the crate's own way of holding a serde behind one
  type without a heap allocation per record; since the clients now take serdes
  as `impl Serializer<K>` / `impl Deserializer<K>` and never hand one out, no
  caller needs to name them, and keeping them private leaves the dispatch free
  to change. Code that held a `Box<dyn DynSerializer<K>>` passes the serializer
  itself to the constructor; code that implemented `Serializer` or
  `Deserializer` is unaffected.
- **Breaking (Rust, C API, Python):** under a SASL `security.protocol`
  (`SASL_PLAINTEXT`, `SASL_SSL`), a `sasl.mechanism` other than `PLAIN`, the
  one mechanism this client implements, now fails `ProducerConfig::new`,
  `ConsumerConfig::new` and `AdminClientConfig::new` (and so the clients'
  constructors) with `ConfigError`
  `Invalid value <m> for configuration sasl.mechanism: only PLAIN is supported by this client`,
  instead of failing at authentication. For SCRAM over `SASL_PLAINTEXT` the old
  path sent the password in cleartext as a PLAIN token before the broker
  rejected it. `sasl.mechanism` keeps Java's `GSSAPI` default, so a SASL
  configuration that leaves it unset now fails at construction with this
  clear error, naming `GSSAPI`, instead of a misleading "requires a username"
  or a broker-side handshake failure: set `sasl.mechanism=PLAIN`. An empty
  mechanism keeps Java's own `ConfigError` message.
- **Breaking (Rust, C API, Python):** `ssl.keystore.password`,
  `ssl.truststore.password` and `ssl.key.password` are no longer defined, and
  `SslConfigs::SSL_KEYSTORE_PASSWORD_CONFIG`, `SSL_TRUSTSTORE_PASSWORD_CONFIG`
  and `SSL_KEY_PASSWORD_CONFIG` are removed. None of them can work in this
  client: PEM is the only store format, and Java rejects a store password with
  PEM stores; and encrypted PEM private keys, which Java decrypts with the key
  password, are not supported. A value set for one of them is now listed among
  the unused configurations instead of being accepted and ignored, and reading
  one through `get_string` fails with `Unknown configuration`. An encrypted
  PEM private key (`ENCRYPTED PRIVATE KEY`, or a legacy block with
  `Proc-Type: 4,ENCRYPTED`) now fails the clients' constructors with a
  `ConfigError` cause, `Failed to create channel builder: Encrypted PEM private
  keys are not supported by this client; provide an unencrypted PKCS#8 key`,
  where it used to fail with `No private key found in inline keystore key`,
  `No private key found in keystore file '<path>'` or a PEM parse error; the
  outer error ("Failed to construct kafka producer", "Failed to construct kafka
  consumer", "Failed to create new KafkaAdminClient") is unchanged.
- **Breaking (Rust, C API, Python):** `ssl.cipher.suites` is honoured. The
  clients offer the configured suites, named as in Java and IANA
  (`TLS_AES_128_GCM_SHA256`, `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`), in the
  configured order; an empty list keeps the TLS provider's defaults, as
  `ssl.protocol` restricts them (below). Every suite of the aws-lc-rs
  provider is accepted. The key used to be read only to
  log a warning and then ignored. An unknown name fails the clients'
  constructors with a `ConfigError` cause, `Failed to create channel builder:
  Unsupported CipherSuite: <name>`, where Java fails the first connection with
  the same message.
- **Breaking (Rust, C API, Python):** `ssl.protocol` is honoured as JSSE
  honours it. It used to be defined and ignored. It selects Java's
  `SSLContext.getInstance(protocol)`, which supplies only the default lists:
  its default protocols, used when `ssl.enabled.protocols` is empty, and its
  default cipher suites (the provider's suites usable with those protocols),
  used when `ssl.cipher.suites` is empty; a protocol is used only when an
  enabled suite can be used with it. So with `TLSv1.2` and the default suites
  the clients do not use TLS 1.3 even when it is enabled, while a TLS 1.3
  suite in `ssl.cipher.suites` allows it, as in Java. `TLSv1.3` (the default),
  `TLS` and `SSL` default to TLS 1.3 and TLS 1.2. `TLSv1.1`, `TLSv1` and
  `SSLv3` default to versions this client does not implement (nor does the
  JDK enable them), so with an empty `ssl.cipher.suites` they fail the clients'
  constructors (`No cipher suite is enabled: ssl.cipher.suites is empty and
  none of the TLS provider's cipher suites can be used with the protocols of
  ssl.protocol = TLSv1.1 (this client implements TLS 1.2 and TLS 1.3 only)`),
  and with a list they use it. Names match ignoring case. Another value
  (`Default`, a DTLS context, an unknown name) fails the constructors
  (`Unsupported TLS protocol: '<value>'. Supported protocols: TLSv1.3,
  TLSv1.2, TLSv1.1, TLSv1, SSLv3, TLS, SSL`), and so do enabled protocols none
  of which an enabled suite can be used with (`No TLS protocol of
  ssl.enabled.protocols = [TLSv1.3] can be used with the enabled cipher suites
  (ssl.protocol = TLSv1.2, ssl.cipher.suites = [])`). Java reports these two
  at the handshake instead (`No appropriate protocol`).
- `client.dns.lookup=resolve_canonical_bootstrap_servers_only` now resolves
  each bootstrap address to its canonical host name as Java's
  `InetAddress.getCanonicalHostName()` does: a reverse (PTR) lookup through the
  system resolver, confirmed by a forward lookup that must return the address,
  with the textual IP as the fallback (new dependency `dns-lookup`). Where PTR
  records exist the bootstrap node's host is the FQDN, so TLS hostname
  verification checks it as in Java, where it used to be the textual IP; an
  empty canonical name becomes `localhost`, Java's node host for it. The
  warning these configurations logged for this mode with TLS is removed.
- `TopicPartitionInfo`, `MetricNameTemplate`, `ClientQuotaFilter`, `Quota`,
  `SslClientAuth`, `RecordingLevel` and `DeserializationErrorOrigin` implement
  `Hash`, as their Java classes override `hashCode()`.
- `ClientQuotaAlteration`'s `Op` implements `Eq` and `Hash`. Its value is
  compared as Java's `Double.equals` compares it: every `NaN` equals every
  other, and `0.0` differs from `-0.0`.
- `Quota` compares its bound the same way. Java compares it with the
  primitive `==`, under which `NaN` differs from itself, which an `Eq` cannot
  express. A `NaN` quota now equals itself, and an upper bound of `0.0` no
  longer equals one of `-0.0`; every other bound compares as before.
- Every enum translating a Java enum implements `PartialOrd` and `Ord`,
  ordering its values as Java's `Enum.compareTo`, by declaration. New here:
  `ClassicGroupState`, `ConfigResource`'s `Type`, `SslClientAuth`,
  `ElectionType`, `DeserializationErrorOrigin`, `GroupState`, `GroupType`,
  `IsolationLevel`, `RecordingLevel`, `TimestampType` and `SecurityProtocol`.
- `ConfigResource`'s `Type`, `ElectionType`, `RecordingLevel` and
  `DeserializationErrorOrigin` implement `Display`, printing the Java
  constant's name (`BROKER_LOGGER`, `UNCLEAN`), as Java's `Enum.toString()`
  does.
- The metric stats implement `Display` in the format of Java's `toString()`,
  doubles printed as Java's `Double.toString`. `SampledStat` and its
  `Sample` print their own format, and `Avg`, `Max`, `Min`, `WindowedCount`
  and `WindowedSum` print the `SampledStat` they inherit it from, as in
  Java. `CumulativeCount` prints as `CumulativeSum`, `SimpleRate` as `Rate`,
  and `Meter` prints its rate, total and the two metric names.
- `Metrics`, `Sensor`, `KafkaMetric`, `NamedMeasurable`, `ClosureMeasurable`,
  `SensorOptions`, `SensorOptionsBuilder` and the metric stats (`SampledStat`,
  `Avg`, `Max`, `Min`, `WindowedCount`, `WindowedSum`, `Rate`, `SimpleRate`,
  `Meter`) implement `Debug`. `Metrics` prints how many sensors and metrics it
  holds, `Sensor` its name, and `KafkaMetric` what its `Display` prints.

### C API

- **Breaking (C API):** `kafka_common_metrics_MetricConfig_quota` returns a
  borrowed `const kafka_common_metrics_Quota_t *`, valid until the config
  handle is destroyed or one of its setters runs, instead of an owned copy:
  do not pass it to `kafka_common_metrics_Quota_destroy`.
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
- `kafka_admin_AdminClientConfig_t` answers `AbstractConfig`'s getters:
  `kafka_admin_AdminClientConfig_get_short`, `_get_int`, `_get_long`,
  `_get_double` and `_get_boolean` return `1` with the value in `*out_value`
  and `0` for Java's `null`; `_get_string` returns a string borrowed from the
  handle; `_get_list`, `_unused` and `_config_names`
  return an owned `kafka_List_t`; plus `_ignore` and `_log_unused`. A client
  created from the handle keeps its own record of the keys it used.
- `kafka_consumer_ConsumerConfig_t` answers the same getters:
  `kafka_consumer_ConsumerConfig_get_short`, `_get_int`, `_get_long`,
  `_get_double`, `_get_boolean`, `_get_list`, `_unused`,
  `_ignore`, `_log_unused` and `_config_names`, beside the existing
  `_get_string`. A consumer created from the handle keeps its own record of
  the keys it used, and runs under the handle's `client.id`, the one
  `_get_string` answers: the consumers of one handle share it, as Java's
  consumers given one `ConsumerConfig` do.
- `kafka_producer_ProducerConfig_t` answers the same getters:
  `kafka_producer_ProducerConfig_get_short`, `_get_int`, `_get_long`,
  `_get_double`, `_get_boolean`, `_get_string`, `_get_list`, `_unused`,
  `_ignore`, `_log_unused` and `_config_names`. A producer created from the
  handle keeps its own record of the keys it used, and runs under the
  handle's `client.id`, the one `_get_string` answers: the producers of one
  handle share it, as Java's producers given one `ProducerConfig` do. A
  generated id used to take a second number from the `producer-<n>`
  sequence, so the handle's `producer-1` built a `producer-2`.
- Each configuration warning of the C config handles
  (`kafka_consumer_ConsumerConfig_t`, `kafka_producer_ProducerConfig_t`,
  `kafka_admin_AdminClientConfig_t`) is logged once per handle, when
  `_new` builds it, not again for every client created from it, as Java's
  one config object per client logs it once. `_new` sets up the default
  stderr logger first, as creating a client does, so the warnings are not
  lost when the handle is created before any client.
- New `_to_string` exports, each returning an owned string freed with
  `kafka_string_destroy`, for the enums that gained `Display`:
  `kafka_admin_FeatureUpdate_UpgradeType_to_string`,
  `kafka_admin_ScramMechanism_to_string`,
  `kafka_common_ElectionType_to_string`,
  `kafka_common_config_ConfigResource_Type_to_string`,
  `kafka_common_metrics_Sensor_RecordingLevel_to_string`,
  `kafka_common_RecordDeserializationError_DeserializationErrorOrigin_to_string`
  and `kafka_consumer_CloseOptions_GroupMembershipOperation_to_string`.
- New `_to_string` exports for the admin types that gained `Display`:
  `kafka_admin_ConsumerGroupDescription_to_string`,
  `kafka_admin_ClassicGroupDescription_to_string`,
  `kafka_admin_AbortTransactionOptions_to_string`,
  `kafka_admin_DescribeProducersOptions_to_string`,
  `kafka_admin_DescribeTransactionsOptions_to_string`,
  `kafka_admin_FenceProducersOptions_to_string`,
  `kafka_admin_TerminateTransactionOptions_to_string` and
  `kafka_admin_ListTransactionsOptions_to_string`. Each returns an owned
  string freed with `kafka_string_destroy`.
- New `_to_string` exports for the stats and assignor types that gained
  `Display`: `kafka_common_metrics_stats_SampledStat_to_string`,
  `kafka_common_metrics_stats_SampledStat_Sample_to_string`,
  `kafka_common_metrics_stats_Avg_to_string`,
  `kafka_common_metrics_stats_Max_to_string`,
  `kafka_common_metrics_stats_Min_to_string`,
  `kafka_common_metrics_stats_WindowedCount_to_string`,
  `kafka_common_metrics_stats_WindowedSum_to_string`,
  `kafka_common_metrics_stats_CumulativeSum_to_string`,
  `kafka_common_metrics_stats_CumulativeCount_to_string`,
  `kafka_common_metrics_stats_Rate_to_string`,
  `kafka_common_metrics_stats_SimpleRate_to_string`,
  `kafka_common_metrics_stats_Meter_to_string`,
  `kafka_consumer_ConsumerPartitionAssignor_Subscription_to_string` and
  `kafka_consumer_ConsumerPartitionAssignor_Assignment_to_string`. Each
  returns an owned string freed with `kafka_string_destroy`.

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