# FFI entry-point list

This list is a temporary input to generation (CLAUDE.md, "Python Binding Conventions",
Implementation over the FFI). Generation looks up the entry-point name that §2 (Rust name) and §4
(C prefix) derive for each Java overload of an FFI-backed class; when the C header
`rust/target/include/confluent_kafka.h` lacks that name, it uses the entry point this list gives for
the overload, and an overload with no row here and no derived name in the header is not generated.
The list exists only because the admin FFI and seven consumer entry points do not use the derived
names. It is deleted when the FFI uses the derived
names, and the three rule sentences that refer to it go back to their text before the list.

## How to read it

- **Java overload**: the class, the method and its parameter types, with its line in the Java source
  (Apache Kafka 4.3.1). Rows follow the Java declaration order of that file.
- **Derived name**: the C prefix of the Java interface (`kafka_consumer_Consumer_`,
  `kafka_admin_Admin_`) plus the Rust method that translates the overload (`rust/src/consumer/mod.rs`,
  `rust/src/admin/mod.rs`); `(_async)` is the completion form. Two Java overloads that the Rust core
  folds into one method share its name (`deleteTopics(Collection<String>)` and
  `deleteTopics(TopicCollection)` → `delete_topics`). None of these names is in the header.
- **Entry point used**: the header function the generated method calls. When it has an `_async`
  completion form, that is the form listed: the sync class waits on it and the async class awaits it
  (Class family). Every admin RPC entry point has a plain form beside it; the generated code does not
  use the plain forms.
- **Arguments**: how Java's values become the entry point's arguments. The C extension
  `_confluentkafka` does the marshaling (CLAUDE.md, the table of conversions); this column names the
  C parameters it fills. The admin handle, the out-result pointer, the callback and `user_data` are
  left out.

Conventions used in the Arguments column:

- `timeout_ms` is `AbstractOptions.timeoutMs()`; `null` becomes `-1`, which the FFI reads as unset
  (the client's `default.api.timeout.ms`).
  A negative `timeoutMs()` does not cross as Java means it: the FFI reads every negative value as
  unset, where Java's `calcDeadlineMs` (`KafkaAdminClient.java:496`) makes the call expire at once.
- "defaults" means the overload passes a fresh Options object (`new CreateTopicsOptions()`): every
  option argument takes that Options class's starting value, `timeout_ms` `-1`.
- A Java collection becomes a C array and a count, in iteration order. A `TopicPartition` element
  becomes `topics[i]`, `partitions[i]`.
- A Java `null` string becomes `NULL`.
- An enum becomes the code named in the row: `ConfigResource.Type.id()`, `AlterConfigOp.OpType.id()`,
  `ElectionType.value`, `IsolationLevel.id()`, `ResourceType.code()`, `PatternType.code()`,
  `AclOperation.code()`, `AclPermissionType.code()`, `ScramMechanism.type()`,
  `FeatureUpdate.UpgradeType.code()`, `CloseOptions.GroupMembershipOperation` (`0` default, `1`
  leave group, `2` remain in group); `GroupState`, `GroupType` and `TransactionState` become their
  `toString()` names.

## Consumer

| Java overload | Derived name | Entry point used | Arguments |
|---|---|---|---|
| `Consumer.subscribe(Collection<String>)` :54 | `kafka_consumer_Consumer_subscribe_with_topics(_async)` | `kafka_consumer_Consumer_subscribe_async` | `topics` → `topics`, `count` |
| `Consumer.subscribe(Collection<String>, ConsumerRebalanceListener)` :59 | `kafka_consumer_Consumer_subscribe_with_topics_listener(_async)` | `kafka_consumer_Consumer_subscribe_caller_thread_listener_async` | `topics` → `topics`, `count`; the listener runs on the caller's thread, from the pending callbacks the waiting call drains |
| `Consumer.subscribe(SubscriptionPattern, ConsumerRebalanceListener)` :79 | `kafka_consumer_Consumer_subscribe_with_pattern_listener(_async)` | `kafka_consumer_Consumer_subscribe_pattern_caller_thread_listener_async` | `pattern` → `pattern` (`SubscriptionPattern.pattern()`); listener as above |
| `Consumer.subscribe(SubscriptionPattern)` :84 | `kafka_consumer_Consumer_subscribe_with_pattern(_async)` | `kafka_consumer_Consumer_subscribe_pattern_async` (no plain form) | `pattern` → `pattern` (`SubscriptionPattern.pattern()`) |
| `Consumer.commitAsync(Map<TopicPartition, OffsetAndMetadata>, OffsetCommitCallback)` :128 | `kafka_consumer_Consumer_commit_async_with_offsets_callback` | `kafka_consumer_Consumer_commit_async_offsets_with_callback` (no `_async` form: it does not wait) | `offsets` → `topics`, `partitions`, `offsets`, `leader_epochs` (`-1` when absent), `metadata`, `count`; `callback` → the commit callback, run on the caller's thread |
| `Consumer.seek(TopicPartition, long)` :142 | `kafka_consumer_Consumer_seek_with_offset(_async)` | `kafka_consumer_Consumer_seek_async` | `partition` → `topic`, `partition`; `offset` → `offset` |
| `Consumer.close(CloseOptions)` :288 | `kafka_consumer_Consumer_close_with_options(_async)` | `kafka_consumer_Consumer_close_with_option_async` | `option.timeout()` → `timeout_ms` (absent: `-1`); `option.groupMembershipOperation()` → `operation_code` |

## Admin

`Admin` is the Java interface (`clients/admin/Admin.java`); `KafkaAdminClient` implements every row
and adds no public method of its own. Java's `AdminClient` is not generated, so its
`create` overloads have no rows.

| Java overload | Derived name | Entry point used | Arguments |
|---|---|---|---|
| `Admin.create(Properties)` :133 | `kafka_admin_Admin_create` | `kafka_admin_AdminClient_new` (no `_async` form) | each entry → `kafka_admin_AdminClientProperties_put(props, key, value)` on `kafka_admin_AdminClientProperties_new()`, then `props`; the properties handle is freed after the call |
| `Admin.create(Map<String, Object>)` :143 | `kafka_admin_Admin_create` | `kafka_admin_AdminClient_new` (no `_async` form) | as `create(Properties)` |
| `Admin.close()` :153 | `kafka_admin_Admin_close(_async)` | `kafka_admin_AdminClient_close_async` | `timeout_ms` = `-1` (Java's `Duration.ofMillis(Long.MAX_VALUE)`) |
| `Admin.close(Duration)` :167 | `kafka_admin_Admin_close_with_timeout(_async)` | `kafka_admin_AdminClient_close_async` | `timeout` → `timeout_ms` (`int64`) |
| `Admin.createTopics(Collection<NewTopic>)` :180 | `kafka_admin_Admin_create_topics(_async)` | `kafka_admin_AdminClient_create_topics_async` | as the next row, with defaults |
| `Admin.createTopics(Collection<NewTopic>, CreateTopicsOptions)` :201 | `kafka_admin_Admin_create_topics_with_options(_async)` | `kafka_admin_AdminClient_create_topics_async` | each `NewTopic` → `kafka_admin_NewTopic_new(name, numPartitions or -1, replicationFactor or -1)`, plus `_set_replicas_assignment` per assigned partition and `_put_config` per config; → `topics`, `count`; `options` → `timeout_ms`, `validate_only`, `retry_on_quota_violation` |
| `Admin.deleteTopics(Collection<String>)` :212 | `kafka_admin_Admin_delete_topics(_async)` | `kafka_admin_AdminClient_delete_topics_async` | `topics` → `names`, `count`; defaults |
| `Admin.deleteTopics(Collection<String>, DeleteTopicsOptions)` :226 | `kafka_admin_Admin_delete_topics_with_options(_async)` | `kafka_admin_AdminClient_delete_topics_async` | `topics` → `names`, `count`; `options` → `timeout_ms`, `retry_on_quota_violation` |
| `Admin.deleteTopics(TopicCollection)` :240 | `kafka_admin_Admin_delete_topics(_async)` | `TopicNameCollection`: `kafka_admin_AdminClient_delete_topics_async`; `TopicIdCollection`: `kafka_admin_AdminClient_delete_topics_by_ids_async` | names → `names`, or ids (`Uuid.toString()`) → `topic_ids`; `count`; defaults |
| `Admin.deleteTopics(TopicCollection, DeleteTopicsOptions)` :264 | `kafka_admin_Admin_delete_topics_with_options(_async)` | as the row above, by subclass | as the row above; `options` → `timeout_ms`, `retry_on_quota_violation` |
| `Admin.listTopics()` :274 | `kafka_admin_Admin_list_topics(_async)` | `kafka_admin_AdminClient_list_topics_async` | defaults |
| `Admin.listTopics(ListTopicsOptions)` :284 | `kafka_admin_Admin_list_topics_with_options(_async)` | `kafka_admin_AdminClient_list_topics_async` | `options` → `timeout_ms`, `list_internal` |
| `Admin.describeTopics(Collection<String>)` :295 | `kafka_admin_Admin_describe_topics_with_topic_names(_async)` | `kafka_admin_AdminClient_describe_topics_async` | `topicNames` → `names`, `count`; defaults |
| `Admin.describeTopics(Collection<String>, DescribeTopicsOptions)` :306 | `kafka_admin_Admin_describe_topics_with_topic_names_options(_async)` | `kafka_admin_AdminClient_describe_topics_async` | `topicNames` → `names`, `count`; `options` → `timeout_ms`, `include_authorized_operations`, `partition_size_limit_per_response` |
| `Admin.describeTopics(TopicCollection)` :319 | `kafka_admin_Admin_describe_topics_with_topics(_async)` | `TopicNameCollection`: `kafka_admin_AdminClient_describe_topics_async`; `TopicIdCollection`: `kafka_admin_AdminClient_describe_topics_by_ids_async` | names → `names`, or ids (`Uuid.toString()`) → `topic_ids`; `count`; defaults |
| `Admin.describeTopics(TopicCollection, DescribeTopicsOptions)` :332 | `kafka_admin_Admin_describe_topics_with_topics_options(_async)` | as the row above, by subclass | as the row above; `options` → `timeout_ms`, `include_authorized_operations`, `partition_size_limit_per_response` |
| `Admin.describeCluster()` :342 | `kafka_admin_Admin_describe_cluster(_async)` | `kafka_admin_AdminClient_describe_cluster_async` | defaults |
| `Admin.describeCluster(DescribeClusterOptions)` :355 | `kafka_admin_Admin_describe_cluster_with_options(_async)` | `kafka_admin_AdminClient_describe_cluster_async` | `options` → `timeout_ms`, `include_authorized_operations`, `include_fenced_brokers` |
| `Admin.describeAcls(AclBindingFilter)` :366 | `kafka_admin_Admin_describe_acls(_async)` | `kafka_admin_AdminClient_describe_acls_async` | `filter` → `resource_type`, `resource_name`, `pattern_type`, `principal`, `host`, `operation`, `permission_type` (codes; a `null` string is `NULL`, "any"); defaults |
| `Admin.describeAcls(AclBindingFilter, DescribeAclsOptions)` :382 | `kafka_admin_Admin_describe_acls_with_options(_async)` | `kafka_admin_AdminClient_describe_acls_async` | as the row above; `options` → `timeout_ms` |
| `Admin.createAcls(Collection<AclBinding>)` :393 | `kafka_admin_Admin_create_acls(_async)` | `kafka_admin_AdminClient_create_acls_async` | each `AclBinding` → `resource_types[i]`, `resource_names[i]`, `pattern_types[i]`, `principals[i]`, `hosts[i]`, `operations[i]`, `permission_types[i]`; `count`; defaults |
| `Admin.createAcls(Collection<AclBinding>, CreateAclsOptions)` :411 | `kafka_admin_Admin_create_acls_with_options(_async)` | `kafka_admin_AdminClient_create_acls_async` | as the row above; `options` → `timeout_ms` |
| `Admin.deleteAcls(Collection<AclBindingFilter>)` :422 | `kafka_admin_Admin_delete_acls(_async)` | `kafka_admin_AdminClient_delete_acls_async` | each filter → the seven arrays of `createAcls` (a `null` string is `NULL`); `count`; defaults |
| `Admin.deleteAcls(Collection<AclBindingFilter>, DeleteAclsOptions)` :437 | `kafka_admin_Admin_delete_acls_with_options(_async)` | `kafka_admin_AdminClient_delete_acls_async` | as the row above; `options` → `timeout_ms` |
| `Admin.describeConfigs(Collection<ConfigResource>)` :451 | `kafka_admin_Admin_describe_configs(_async)` | `kafka_admin_AdminClient_describe_configs_async` | each resource → `resource_types[i]` (`Type.id()`), `resource_names[i]`; `count`; defaults |
| `Admin.describeConfigs(Collection<ConfigResource>, DescribeConfigsOptions)` :488 | `kafka_admin_Admin_describe_configs_with_options(_async)` | `kafka_admin_AdminClient_describe_configs_async` | as the row above; `options` → `timeout_ms`, `include_synonyms`, `include_documentation` |
| `Admin.incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>)` :501 | `kafka_admin_Admin_incremental_alter_configs(_async)` | `kafka_admin_AdminClient_incremental_alter_configs_async` | one row per op: `resource_types[i]`, `resource_names[i]`, `config_names[i]` (`configEntry().name()`), `config_values[i]` (`value()`, `NULL` when null), `op_types[i]` (`opType().id()`); `count`; defaults |
| `Admin.incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>, AlterConfigsOptions)` :530 | `kafka_admin_Admin_incremental_alter_configs_with_options(_async)` | `kafka_admin_AdminClient_incremental_alter_configs_async` | as the row above; `options` → `timeout_ms`, `validate_only` (`shouldValidateOnly()`) |
| `Admin.alterReplicaLogDirs(Map<TopicPartitionReplica, String>)` :549 | `kafka_admin_Admin_alter_replica_log_dirs(_async)` | `kafka_admin_AdminClient_alter_replica_log_dirs_async` | each entry → `topics[i]`, `partitions[i]`, `broker_ids[i]`, `log_dirs[i]`; `count`; defaults |
| `Admin.alterReplicaLogDirs(Map<TopicPartitionReplica, String>, AlterReplicaLogDirsOptions)` :567 | `kafka_admin_Admin_alter_replica_log_dirs_with_options(_async)` | `kafka_admin_AdminClient_alter_replica_log_dirs_async` | as the row above; `options` → `timeout_ms` |
| `Admin.describeLogDirs(Collection<Integer>)` :581 | `kafka_admin_Admin_describe_log_dirs(_async)` | `kafka_admin_AdminClient_describe_log_dirs_async` | `brokers` → `brokers`, `count`; defaults |
| `Admin.describeLogDirs(Collection<Integer>, DescribeLogDirsOptions)` :594 | `kafka_admin_Admin_describe_log_dirs_with_options(_async)` | `kafka_admin_AdminClient_describe_log_dirs_async` | as the row above; `options` → `timeout_ms` |
| `Admin.describeReplicaLogDirs(Collection<TopicPartitionReplica>)` :607 | `kafka_admin_Admin_describe_replica_log_dirs(_async)` | `kafka_admin_AdminClient_describe_replica_log_dirs_async` | each replica → `topics[i]`, `partitions[i]`, `broker_ids[i]`; `count`; defaults |
| `Admin.describeReplicaLogDirs(Collection<TopicPartitionReplica>, DescribeReplicaLogDirsOptions)` :620 | `kafka_admin_Admin_describe_replica_log_dirs_with_options(_async)` | `kafka_admin_AdminClient_describe_replica_log_dirs_async` | as the row above; `options` → `timeout_ms` |
| `Admin.createPartitions(Map<String, NewPartitions>)` :634 | `kafka_admin_Admin_create_partitions(_async)` | `kafka_admin_AdminClient_create_partitions_async` | each entry → `topics[i]` and `new_partitions[i]` = `kafka_admin_NewPartitions_new(totalCount(), assignments() != null)` plus `_add_assignment` per assignment; `count`; defaults |
| `Admin.createPartitions(Map<String, NewPartitions>, CreatePartitionsOptions)` :677 | `kafka_admin_Admin_create_partitions_with_options(_async)` | `kafka_admin_AdminClient_create_partitions_async` | as the row above; `options` → `timeout_ms`, `validate_only`, `retry_on_quota_violation` |
| `Admin.deleteRecords(Map<TopicPartition, RecordsToDelete>)` :691 | `kafka_admin_Admin_delete_records(_async)` | `kafka_admin_AdminClient_delete_records_async` | each entry → `topics[i]`, `partitions[i]`, `before_offsets[i]` (`beforeOffset()`); `count`; defaults |
| `Admin.deleteRecords(Map<TopicPartition, RecordsToDelete>, DeleteRecordsOptions)` :704 | `kafka_admin_Admin_delete_records_with_options(_async)` | `kafka_admin_AdminClient_delete_records_async` | as the row above; `options` → `timeout_ms` |
| `Admin.createDelegationToken()` :715 | `kafka_admin_Admin_create_delegation_token(_async)` | `kafka_admin_AdminClient_create_delegation_token_async` | defaults: no renewers, `owner_principal_type` and `owner_name` `NULL`, `max_lifetime_ms` `-1` |
| `Admin.createDelegationToken(CreateDelegationTokenOptions)` :741 | `kafka_admin_Admin_create_delegation_token_with_options(_async)` | `kafka_admin_AdminClient_create_delegation_token_async` | `renewers()` → `renewer_principal_types`, `renewer_names`, `renewer_count`; `owner()` → `owner_principal_type`, `owner_name` (`NULL` when empty); `maxLifetimeMs()` → `max_lifetime_ms`; `timeout_ms` |
| `Admin.renewDelegationToken(byte[])` :753 | `kafka_admin_Admin_renew_delegation_token(_async)` | `kafka_admin_AdminClient_renew_delegation_token_async` | `hmac` → `hmac`, `hmac_len`; defaults (`renew_time_period_ms` `-1`) |
| `Admin.renewDelegationToken(byte[], RenewDelegationTokenOptions)` :783 | `kafka_admin_Admin_renew_delegation_token_with_options(_async)` | `kafka_admin_AdminClient_renew_delegation_token_async` | `hmac` → `hmac`, `hmac_len`; `options` → `renew_time_period_ms`, `timeout_ms` |
| `Admin.expireDelegationToken(byte[])` :794 | `kafka_admin_Admin_expire_delegation_token(_async)` | `kafka_admin_AdminClient_expire_delegation_token_async` | `hmac` → `hmac`, `hmac_len`; defaults (`expiry_time_period_ms` `-1`) |
| `Admin.expireDelegationToken(byte[], ExpireDelegationTokenOptions)` :824 | `kafka_admin_Admin_expire_delegation_token_with_options(_async)` | `kafka_admin_AdminClient_expire_delegation_token_async` | `hmac` → `hmac`, `hmac_len`; `options` → `expiry_time_period_ms`, `timeout_ms` |
| `Admin.describeDelegationToken()` :834 | `kafka_admin_Admin_describe_delegation_token(_async)` | `kafka_admin_AdminClient_describe_delegation_token_async` | defaults (`has_owners_filter` `false`) |
| `Admin.describeDelegationToken(DescribeDelegationTokenOptions)` :857 | `kafka_admin_Admin_describe_delegation_token_with_options(_async)` | `kafka_admin_AdminClient_describe_delegation_token_async` | `owners()` → `has_owners_filter` (`owners() != null`), `owner_principal_types`, `owner_names`, `owner_count`; `timeout_ms` |
| `Admin.describeConsumerGroups(Collection<String>, DescribeConsumerGroupsOptions)` :866 | `kafka_admin_Admin_describe_consumer_groups_with_options(_async)` | `kafka_admin_AdminClient_describe_consumer_groups_async` | `groupIds` → `group_ids`, `count`; `options` → `timeout_ms`, `include_authorized_operations` |
| `Admin.describeConsumerGroups(Collection<String>)` :878 | `kafka_admin_Admin_describe_consumer_groups(_async)` | `kafka_admin_AdminClient_describe_consumer_groups_async` | as the row above, with defaults |
| `Admin.listConsumerGroupOffsets(String, ListConsumerGroupOffsetsOptions)` :912 | `kafka_admin_Admin_list_consumer_group_offsets_with_group_id_options(_async)` | `kafka_admin_AdminClient_list_consumer_group_offsets_async` | one group: `group_ids` = `[groupId]`, `all_partitions` = `[true]` (Java's `new ListConsumerGroupOffsetsSpec()`), `group_count` `1`; `options` → `timeout_ms`, `require_stable` |
| `Admin.listConsumerGroupOffsets(String)` :928 | `kafka_admin_Admin_list_consumer_group_offsets_with_group_id(_async)` | `kafka_admin_AdminClient_list_consumer_group_offsets_async` | as the row above, with defaults |
| `Admin.listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>, ListConsumerGroupOffsetsOptions)` :940 | `kafka_admin_Admin_list_consumer_group_offsets_with_group_specs_options(_async)` | `kafka_admin_AdminClient_list_consumer_group_offsets_async` | each entry → `group_ids[g]`, `all_partitions[g]` (`topicPartitions() == null`), `topics[g]`, `partitions[g]`, `partition_counts[g]`; `group_count`; `options` → `timeout_ms`, `require_stable` |
| `Admin.listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>)` :951 | `kafka_admin_Admin_list_consumer_group_offsets_with_group_specs(_async)` | `kafka_admin_AdminClient_list_consumer_group_offsets_async` | as the row above, with defaults |
| `Admin.deleteConsumerGroups(Collection<String>, DeleteConsumerGroupsOptions)` :986 | `kafka_admin_Admin_delete_consumer_groups_with_options(_async)` | `kafka_admin_AdminClient_delete_consumer_groups_async` | `groupIds` → `group_ids`, `count`; `options` → `timeout_ms` |
| `Admin.deleteConsumerGroups(Collection<String>)` :993 | `kafka_admin_Admin_delete_consumer_groups(_async)` | `kafka_admin_AdminClient_delete_consumer_groups_async` | as the row above, with defaults |
| `Admin.deleteConsumerGroupOffsets(String, Set<TopicPartition>, DeleteConsumerGroupOffsetsOptions)` :1024 | `kafka_admin_Admin_delete_consumer_group_offsets_with_options(_async)` | `kafka_admin_AdminClient_delete_consumer_group_offsets_async` | `groupId` → `group_id`; `partitions` → `topics`, `partitions`, `count`; `options` → `timeout_ms` |
| `Admin.deleteConsumerGroupOffsets(String, Set<TopicPartition>)` :1035 | `kafka_admin_Admin_delete_consumer_group_offsets(_async)` | `kafka_admin_AdminClient_delete_consumer_group_offsets_async` | as the row above, with defaults |
| `Admin.listGroups()` :1072 | `kafka_admin_Admin_list_groups(_async)` | `kafka_admin_AdminClient_list_groups_async` | defaults: every filter empty (`NULL`, `0`) |
| `Admin.listGroups(ListGroupsOptions)` :1082 | `kafka_admin_Admin_list_groups_with_options(_async)` | `kafka_admin_AdminClient_list_groups_async` | `groupStates()` → `group_states` (names), `group_state_count`; `protocolTypes()` → `protocol_types`, `protocol_type_count`; `types()` → `types` (names), `type_count`; `timeout_ms` |
| `Admin.electLeaders(ElectionType, Set<TopicPartition>)` :1094 | `kafka_admin_Admin_elect_leaders(_async)` | `kafka_admin_AdminClient_elect_leaders_async` | `electionType` → `election_type` (`value`); `partitions` `null` → `all_partitions` `true`, else `topics`, `partitions`, `count`; defaults |
| `Admin.electLeaders(ElectionType, Set<TopicPartition>, ElectLeadersOptions)` :1134 | `kafka_admin_Admin_elect_leaders_with_options(_async)` | `kafka_admin_AdminClient_elect_leaders_async` | as the row above; `options` → `timeout_ms` |
| `Admin.alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>>)` :1147 | `kafka_admin_Admin_alter_partition_reassignments(_async)` | `kafka_admin_AdminClient_alter_partition_reassignments_async` | each entry → `topics[i]`, `partitions[i]`, `cancel[i]` (`Optional.empty()`), `target_replicas[i]` and `target_replica_counts[i]` (`targetReplicas()`); `count`; defaults |
| `Admin.alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>>, AlterPartitionReassignmentsOptions)` :1182 | `kafka_admin_Admin_alter_partition_reassignments_with_options(_async)` | `kafka_admin_AdminClient_alter_partition_reassignments_async` | as the row above; `options` → `timeout_ms`, `allow_replication_factor_change` |
| `Admin.listPartitionReassignments()` :1193 | `kafka_admin_Admin_list_partition_reassignments(_async)` | `kafka_admin_AdminClient_list_partition_reassignments_async` | `all_partitions` `true`; defaults |
| `Admin.listPartitionReassignments(Set<TopicPartition>)` :1203 | `kafka_admin_Admin_list_partition_reassignments_with_partitions(_async)` | `kafka_admin_AdminClient_list_partition_reassignments_async` | `all_partitions` `false`; `partitions` → `topics`, `partitions`, `count`; defaults |
| `Admin.listPartitionReassignments(Set<TopicPartition>, ListPartitionReassignmentsOptions)` :1225 | `kafka_admin_Admin_list_partition_reassignments_with_partitions_options(_async)` | `kafka_admin_AdminClient_list_partition_reassignments_async` | `all_partitions` `false`; `partitions` → `topics`, `partitions`, `count`; `options` → `timeout_ms` |
| `Admin.listPartitionReassignments(ListPartitionReassignmentsOptions)` :1248 | `kafka_admin_Admin_list_partition_reassignments_with_options(_async)` | `kafka_admin_AdminClient_list_partition_reassignments_async` | `all_partitions` `true`; `options` → `timeout_ms` |
| `Admin.listPartitionReassignments(Optional<Set<TopicPartition>>, ListPartitionReassignmentsOptions)` :1257 | `kafka_admin_Admin_list_partition_reassignments_with_partitions_options(_async)` | `kafka_admin_AdminClient_list_partition_reassignments_async` | `Optional.empty()` → `all_partitions` `true`, else `false` and `topics`, `partitions`, `count`; `options` → `timeout_ms` |
| `Admin.removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)` :1269 | `kafka_admin_Admin_remove_members_from_consumer_group_with_options(_async)` | `kafka_admin_AdminClient_remove_members_from_consumer_group_async` | `groupId` → `group_id`; `options.removeAll()` → `remove_all`; `members()` → `group_instance_ids` (each `MemberToRemove.groupInstanceId()`), `member_count`; `reason()` → `reason` (`NULL` when null); `timeout_ms` |
| `Admin.alterConsumerGroupOffsets(String, Map<TopicPartition, OffsetAndMetadata>)` :1281 | `kafka_admin_Admin_alter_consumer_group_offsets(_async)` | `kafka_admin_AdminClient_alter_consumer_group_offsets_async` | `groupId` → `group_id`; each entry → `topics[i]`, `partitions[i]`, `offsets[i]`, `metadata[i]`, `leader_epochs[i]` and `has_leader_epoch[i]` (`leaderEpoch()`); `count`; defaults |
| `Admin.alterConsumerGroupOffsets(String, Map<TopicPartition, OffsetAndMetadata>, AlterConsumerGroupOffsetsOptions)` :1295 | `kafka_admin_Admin_alter_consumer_group_offsets_with_options(_async)` | `kafka_admin_AdminClient_alter_consumer_group_offsets_async` | as the row above; `options` → `timeout_ms` |
| `Admin.listOffsets(Map<TopicPartition, OffsetSpec>)` :1334 | `kafka_admin_Admin_list_offsets(_async)` | `kafka_admin_AdminClient_list_offsets_async` | each entry → `topics[i]`, `partitions[i]`, `is_timestamp[i]` and `spec_timestamps[i]` (`forTimestamp(t)`: `true`, `t`; otherwise `false` and the spec's code, `-1` latest … `-6` earliestPendingUpload); `count`; defaults (`isolation_level` `0`) |
| `Admin.listOffsets(Map<TopicPartition, OffsetSpec>, ListOffsetsOptions)` :1346 | `kafka_admin_Admin_list_offsets_with_options(_async)` | `kafka_admin_AdminClient_list_offsets_async` | as the row above; `options` → `timeout_ms`, `isolation_level` (`isolationLevel().id()`) |
| `Admin.describeClientQuotas(ClientQuotaFilter)` :1360 | `kafka_admin_Admin_describe_client_quotas(_async)` | `kafka_admin_AdminClient_describe_client_quotas_async` | each component → `entity_types[i]`, `match_types[i]` (`0` exact, `1` default, `2` specified), `match_names[i]`; `count`; `strict()` → `strict`; defaults |
| `Admin.describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)` :1385 | `kafka_admin_Admin_describe_client_quotas_with_options(_async)` | `kafka_admin_AdminClient_describe_client_quotas_async` | as the row above; `options` → `timeout_ms` |
| `Admin.alterClientQuotas(Collection<ClientQuotaAlteration>)` :1398 | `kafka_admin_Admin_alter_client_quotas(_async)` | `kafka_admin_AdminClient_alter_client_quotas_async` | each alteration → `entity().entries()` as `entity_types[i]`, `entity_names[i]` (`NULL` for the default entity), `entity_counts[i]`; `ops()` as `op_keys[i]`, `op_values[i]`, `op_has_values[i]` (`value() != null`), `op_counts[i]`; `count`; defaults |
| `Admin.alterClientQuotas(Collection<ClientQuotaAlteration>, AlterClientQuotasOptions)` :1425 | `kafka_admin_Admin_alter_client_quotas_with_options(_async)` | `kafka_admin_AdminClient_alter_client_quotas_async` | as the row above; `options` → `timeout_ms`, `validate_only` |
| `Admin.describeUserScramCredentials()` :1434 | `kafka_admin_Admin_describe_user_scram_credentials(_async)` | `kafka_admin_AdminClient_describe_user_scram_credentials_async` | `users` `NULL`, `count` `0` (Java's `null`: every user); defaults |
| `Admin.describeUserScramCredentials(List<String>)` :1447 | `kafka_admin_Admin_describe_user_scram_credentials_with_users(_async)` | `kafka_admin_AdminClient_describe_user_scram_credentials_async` | `users` → `users`, `count` (`null` or empty: every user); defaults |
| `Admin.describeUserScramCredentials(List<String>, DescribeUserScramCredentialsOptions)` :1474 | `kafka_admin_Admin_describe_user_scram_credentials_with_users_options(_async)` | `kafka_admin_AdminClient_describe_user_scram_credentials_async` | as the row above; `options` → `timeout_ms` |
| `Admin.alterUserScramCredentials(List<UserScramCredentialAlteration>)` :1484 | `kafka_admin_Admin_alter_user_scram_credentials(_async)` | `kafka_admin_AdminClient_alter_user_scram_credentials_async` | each alteration → `users[i]`, `is_deletions[i]` (`UserScramCredentialDeletion`), `mechanisms[i]`, `iterations[i]`, `passwords[i]` and `password_lens[i]`, `salts[i]`, `salt_lens[i]` and `has_salts[i]`; `count`; defaults |
| `Admin.alterUserScramCredentials(List<UserScramCredentialAlteration>, AlterUserScramCredentialsOptions)` :1515 | `kafka_admin_Admin_alter_user_scram_credentials_with_options(_async)` | `kafka_admin_AdminClient_alter_user_scram_credentials_async` | as the row above; `options` → `timeout_ms` |
| `Admin.describeFeatures()` :1525 | `kafka_admin_Admin_describe_features(_async)` | `kafka_admin_AdminClient_describe_features_async` | defaults (`has_node_id` `false`) |
| `Admin.describeFeatures(DescribeFeaturesOptions)` :1544 | `kafka_admin_Admin_describe_features_with_options(_async)` | `kafka_admin_AdminClient_describe_features_async` | `nodeId()` → `has_node_id`, `node_id`; `timeout_ms` |
| `Admin.updateFeatures(Map<String, FeatureUpdate>)` :1555 | `kafka_admin_Admin_update_features(_async)` | `kafka_admin_AdminClient_update_features_async` | each entry → `features[i]`, `max_version_levels[i]` (`maxVersionLevel()`), `upgrade_types[i]` (`upgradeType().code()`); `count`; defaults |
| `Admin.updateFeatures(Map<String, FeatureUpdate>, UpdateFeaturesOptions)` :1613 | `kafka_admin_Admin_update_features_with_options(_async)` | `kafka_admin_AdminClient_update_features_async` | as the row above; `options` → `timeout_ms`, `validate_only` |
| `Admin.describeProducers(Collection<TopicPartition>)` :1690 | `kafka_admin_Admin_describe_producers(_async)` | `kafka_admin_AdminClient_describe_producers_async` | `partitions` → `topics`, `partitions`, `count`; defaults (`has_broker_id` `false`) |
| `Admin.describeProducers(Collection<TopicPartition>, DescribeProducersOptions)` :1703 | `kafka_admin_Admin_describe_producers_with_options(_async)` | `kafka_admin_AdminClient_describe_producers_async` | as the row above; `brokerId()` → `has_broker_id`, `broker_id`; `timeout_ms` |
| `Admin.describeTransactions(Collection<String>)` :1712 | `kafka_admin_Admin_describe_transactions(_async)` | `kafka_admin_AdminClient_describe_transactions_async` | `transactionalIds` → `transactional_ids`, `count`; defaults |
| `Admin.describeTransactions(Collection<String>, DescribeTransactionsOptions)` :1724 | `kafka_admin_Admin_describe_transactions_with_options(_async)` | `kafka_admin_AdminClient_describe_transactions_async` | as the row above; `options` → `timeout_ms` |
| `Admin.abortTransaction(AbortTransactionSpec)` :1733 | `kafka_admin_Admin_abort_transaction(_async)` | `kafka_admin_AdminClient_abort_transaction_async` | `spec` → `topic`, `partition` (`topicPartition()`), `producer_id`, `producer_epoch`, `coordinator_epoch`; defaults |
| `Admin.abortTransaction(AbortTransactionSpec, AbortTransactionOptions)` :1746 | `kafka_admin_Admin_abort_transaction_with_options(_async)` | `kafka_admin_AdminClient_abort_transaction_async` | as the row above; `options` → `timeout_ms` |
| `Admin.listTransactions()` :1754 | `kafka_admin_Admin_list_transactions(_async)` | `kafka_admin_AdminClient_list_transactions_async` | defaults: no states, no producer ids, `duration_ms` `-1`, `transactional_id_pattern` `NULL` |
| `Admin.listTransactions(ListTransactionsOptions)` :1769 | `kafka_admin_Admin_list_transactions_with_options(_async)` | `kafka_admin_AdminClient_list_transactions_async` | `filteredStates()` → `states` (names), `state_count`; `filteredProducerIds()` → `producer_ids`, `producer_id_count`; `filteredDuration()` → `duration_ms`; `filteredTransactionalIdPattern()` → `transactional_id_pattern` (`NULL` when null); `timeout_ms` |
| `Admin.fenceProducers(Collection<String>)` :1780 | `kafka_admin_Admin_fence_producers(_async)` | `kafka_admin_AdminClient_fence_producers_async` | `transactionalIds` → `transactional_ids`, `count`; defaults |
| `Admin.fenceProducers(Collection<String>, FenceProducersOptions)` :1791 | `kafka_admin_Admin_fence_producers_with_options(_async)` | `kafka_admin_AdminClient_fence_producers_async` | as the row above; `options` → `timeout_ms` |
| `Admin.listConfigResources(Set<ConfigResource.Type>, ListConfigResourcesOptions)` :1802 | `kafka_admin_Admin_list_config_resources_with_options(_async)` | `kafka_admin_AdminClient_list_config_resources_async` | `configResourceTypes` → `resource_types` (`Type.id()`), `count` (empty: every type); `options` → `timeout_ms` |
| `Admin.listConfigResources()` :1812 | `kafka_admin_Admin_list_config_resources(_async)` | `kafka_admin_AdminClient_list_config_resources_async` | `resource_types` `NULL`, `count` `0` (Java's `Set.of()`); defaults |
| `Admin.describeClassicGroups(Collection<String>, DescribeClassicGroupsOptions)` :2086 | `kafka_admin_Admin_describe_classic_groups_with_options(_async)` | `kafka_admin_AdminClient_describe_classic_groups_async` | `groupIds` → `group_ids`, `count`; `options` → `timeout_ms`, `include_authorized_operations` |
| `Admin.describeClassicGroups(Collection<String>)` :2098 | `kafka_admin_Admin_describe_classic_groups(_async)` | `kafka_admin_AdminClient_describe_classic_groups_async` | as the row above, with defaults |
| `Admin.forceTerminateTransaction(String)` :2150 | `kafka_admin_Admin_force_terminate_transaction(_async)` | `kafka_admin_AdminClient_force_terminate_transaction_async` | `transactionalId` → `transactional_id`; defaults |
| `Admin.forceTerminateTransaction(String, TerminateTransactionOptions)` :2164 | `kafka_admin_Admin_force_terminate_transaction_with_options(_async)` | `kafka_admin_AdminClient_force_terminate_transaction_async` | as the row above; `options` → `timeout_ms` |

## Overloads with no entry point

Neither the derived name nor any header function serves these overloads, so they are not generated
(Implementation over the FFI). The derived name is the name generation looked for; for methods the
Rust core does not translate, it is the name §2 gives the Java method.

| Java overload | Name looked for |
|---|---|
| `Admin.listStreamsGroupOffsets(Map<String, ListStreamsGroupOffsetsSpec>, ListStreamsGroupOffsetsOptions)` :965 | `kafka_admin_Admin_list_streams_group_offsets_with_options(_async)` |
| `Admin.listStreamsGroupOffsets(Map<String, ListStreamsGroupOffsetsSpec>)` :976 | `kafka_admin_Admin_list_streams_group_offsets(_async)` |
| `Admin.deleteStreamsGroups(Collection<String>, DeleteStreamsGroupsOptions)` :1005 | `kafka_admin_Admin_delete_streams_groups_with_options(_async)` |
| `Admin.deleteStreamsGroups(Collection<String>)` :1012 | `kafka_admin_Admin_delete_streams_groups(_async)` |
| `Admin.deleteStreamsGroupOffsets(String, Set<TopicPartition>, DeleteStreamsGroupOffsetsOptions)` :1049 | `kafka_admin_Admin_delete_streams_group_offsets_with_options(_async)` |
| `Admin.deleteStreamsGroupOffsets(String, Set<TopicPartition>)` :1060 | `kafka_admin_Admin_delete_streams_group_offsets(_async)` |
| `Admin.alterStreamsGroupOffsets(String, Map<TopicPartition, OffsetAndMetadata>)` :1307 | `kafka_admin_Admin_alter_streams_group_offsets(_async)` |
| `Admin.alterStreamsGroupOffsets(String, Map<TopicPartition, OffsetAndMetadata>, AlterStreamsGroupOffsetsOptions)` :1323 | `kafka_admin_Admin_alter_streams_group_offsets_with_options(_async)` |
| `Admin.describeMetadataQuorum()` :1623 | `kafka_admin_Admin_describe_metadata_quorum(_async)` |
| `Admin.describeMetadataQuorum(DescribeMetadataQuorumOptions)` :1642 | `kafka_admin_Admin_describe_metadata_quorum_with_options(_async)` |
| `Admin.unregisterBroker(int)` :1656 | `kafka_admin_Admin_unregister_broker(_async)` |
| `Admin.unregisterBroker(int, UnregisterBrokerOptions)` :1681 | `kafka_admin_Admin_unregister_broker_with_options(_async)` |
| `Admin.clientInstanceId(Duration)` :1865 | `kafka_admin_Admin_client_instance_id(_async)` |
| `Admin.addRaftVoter(int, Uuid, Set<RaftVoterEndpoint>)` :1874 | `kafka_admin_Admin_add_raft_voter(_async)` |
| `Admin.addRaftVoter(int, Uuid, Set<RaftVoterEndpoint>, AddRaftVoterOptions)` :1897 | `kafka_admin_Admin_add_raft_voter_with_options(_async)` |
| `Admin.removeRaftVoter(int, Uuid)` :1910 | `kafka_admin_Admin_remove_raft_voter(_async)` |
| `Admin.removeRaftVoter(int, Uuid, RemoveRaftVoterOptions)` :1935 | `kafka_admin_Admin_remove_raft_voter_with_options(_async)` |
| `Admin.describeShareGroups(Collection<String>, DescribeShareGroupsOptions)` :1948 | `kafka_admin_Admin_describe_share_groups_with_options(_async)` |
| `Admin.describeShareGroups(Collection<String>)` :1960 | `kafka_admin_Admin_describe_share_groups(_async)` |
| `Admin.alterShareGroupOffsets(String, Map<TopicPartition, Long>, AlterShareGroupOffsetsOptions)` :1974 | `kafka_admin_Admin_alter_share_group_offsets_with_options(_async)` |
| `Admin.alterShareGroupOffsets(String, Map<TopicPartition, Long>)` :1986 | `kafka_admin_Admin_alter_share_group_offsets(_async)` |
| `Admin.listShareGroupOffsets(Map<String, ListShareGroupOffsetsSpec>, ListShareGroupOffsetsOptions)` :1997 | `kafka_admin_Admin_list_share_group_offsets_with_options(_async)` |
| `Admin.listShareGroupOffsets(Map<String, ListShareGroupOffsetsSpec>)` :2008 | `kafka_admin_Admin_list_share_group_offsets(_async)` |
| `Admin.deleteShareGroupOffsets(String, Set<String>, DeleteShareGroupOffsetsOptions)` :2020 | `kafka_admin_Admin_delete_share_group_offsets_with_options(_async)` |
| `Admin.deleteShareGroupOffsets(String, Set<String>)` :2033 | `kafka_admin_Admin_delete_share_group_offsets(_async)` |
| `Admin.deleteShareGroups(Collection<String>, DeleteShareGroupsOptions)` :2044 | `kafka_admin_Admin_delete_share_groups_with_options(_async)` |
| `Admin.deleteShareGroups(Collection<String>)` :2052 | `kafka_admin_Admin_delete_share_groups(_async)` |
| `Admin.describeStreamsGroups(Collection<String>, DescribeStreamsGroupsOptions)` :2063 | `kafka_admin_Admin_describe_streams_groups_with_options(_async)` |
| `Admin.describeStreamsGroups(Collection<String>)` :2075 | `kafka_admin_Admin_describe_streams_groups(_async)` |
| `Admin.registerMetricForSubscription(KafkaMetric)` :2123 | `kafka_admin_Admin_register_metric_for_subscription` |
| `Admin.unregisterMetricFromSubscription(KafkaMetric)` :2134 | `kafka_admin_Admin_unregister_metric_from_subscription` |
| `Admin.metrics()` :2139 | `kafka_admin_Admin_metrics` |

The consumer's overloads with no entry point (the timed forms, `clientInstanceId`, the KIP-1076
metric methods) and the share-consumer family are listed in the docstrings of the modules that leave
them out, as the rule asks; none of them has a row here.

## Not in this list for another reason

- `Admin.listConsumerGroups()` :902, `Admin.listConsumerGroups(ListConsumerGroupsOptions)` :890,
  `Admin.listClientMetricsResources()` :1836 and
  `Admin.listClientMetricsResources(ListClientMetricsResourcesOptions)` :1824 are deprecated
  (`design/current/java-deprecated.txt`), so they are not generated whatever the header has.
- Header functions that serve no Java overload: `kafka_admin_AdminClient_destroy` frees the handle
  after `close()`; `kafka_admin_AdminClientProperties_*`, `kafka_admin_NewTopic_*` and
  `kafka_admin_NewPartitions_*` build arguments for the rows above; the `kafka_admin_MockAdminClient_*`
  functions serve the C binding, because `MockAdminClient` is a pure-Python translation (Implementation
  over the FFI).
