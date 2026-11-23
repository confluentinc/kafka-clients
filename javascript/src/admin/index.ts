// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * The public classes of Java's `org.apache.kafka.clients.admin` this client
 * has.
 *
 * Left out because the C FFI has no entry point for them: `Admin`'s
 * `addRaftVoter`, `removeRaftVoter`, `describeMetadataQuorum`,
 * `unregisterBroker`, `metrics`, `clientInstanceId`,
 * `registerMetricForSubscription`, `unregisterMetricFromSubscription`, the
 * share-group calls and the streams-group calls; `MockAdminClient`'s
 * `brokers()`, `broker(int)`, `injectTimeoutException`,
 * `handleDescribeTopicsUsingIds`, `setMockMetrics`, `addedMetrics`,
 * `disableTelemetry`, `setClientInstanceId`, `advanceTimeOnClientInstanceId`,
 * `addStreamsGroupDescription`, and the five-parameter form of `addTopic`
 * (`addTopic(internal, name, partitions, configs, usesTopicId)`), with the
 * options, Result and value classes only those calls use (the share-group,
 * streams-group, Raft voter, metadata quorum and `unregisterBroker` ones,
 * `QuorumInfo`, `RaftVoterEndpoint` and `EndpointType`).
 *
 * Not translated: `AdminClient`, Java's abstract base class of
 * `KafkaAdminClient`; and Java's deprecated API, `ForwardingAdmin`,
 * `listConsumerGroups` with `ConsumerGroupListing`,
 * `ListConsumerGroupsOptions` and `ListConsumerGroupsResult`, and
 * `listClientMetricsResources` with its classes (use `listGroups` and
 * `listConfigResources`).
 *
 * `AdminClientConfig` holds Java's constants only, as its `ConfigDef`
 * machinery is not translated: `AdminClientConfig(Map)`, `configNames()`,
 * `configDef()` and `main(String[])` are left out, and constructing it throws
 * `TypeError`. Its constants include those Java's class inherits from
 * `AbstractConfig`, such as `CONFIG_PROVIDERS_CONFIG`.
 *
 * Invented *(js-only)*: `MemberToRemoveSet`, Java's `Set<MemberToRemove>`:
 * JavaScript has no value equality for set members.
 *
 * `MockAdminClient` wraps the C FFI's mock, as the .NET client's does; Java
 * ships it in its test fixtures, and it is public here, as in the Rust core.
 *
 * @remarks *(deviation)* A null Java only stores, and reads later or never,
 * is a `TypeError` naming the parameter at the call, where Java's caller sees
 * the failure at `get()`, or none:
 *
 * - options given as `null` (the FFI's "options must not be null"), also where
 *   Java reads them only once there is something to send, or never (an empty
 *   collection), and on `MockAdminClient` where Java's mock answers without
 *   reading them. Java's mock does read them in the four delegation-token
 *   calls, where a null throws there too, and in `updateFeatures` when there
 *   are updates and every one passes; the calls it does not implement throw
 *   before reading them
 *   (below);
 * - `listConfigResources`: a null set of types, or a null type in it, which
 *   Java reads when it builds the request;
 * - `incrementalAlterConfigs`: a null list of operations, a null operation, or
 *   an `AlterConfigOp` with a null `configEntry` or `opType`, which Java's
 *   request builder reads;
 * - a `TopicPartition` with a null topic: in `alterPartitionReassignments`
 *   (a key) and `listPartitionReassignments`, where Java fails the future with
 *   `InvalidTopicException` "The given topic name 'null' cannot be represented
 *   in a request."; in `electLeaders`, `listOffsets` (a key),
 *   `alterConsumerGroupOffsets` (a key), `deleteConsumerGroupOffsets`, a
 *   `ListConsumerGroupOffsetsSpec`'s partitions, `describeProducers` and an
 *   `AbortTransactionSpec`'s partition, which Java reads when it builds the
 *   request;
 * - a null element of a partition set, in `electLeaders`,
 *   `deleteConsumerGroupOffsets` and a `ListConsumerGroupOffsetsSpec`'s
 *   partitions, and a null key of `alterConsumerGroupOffsets`' offsets, which
 *   Java reads when it builds the request;
 * - `electLeaders`: a null election type, which Java reads when it builds the
 *   request;
 * - `listOffsets`: options whose isolation level is null
 *   (`new ListOffsetsOptions(null)`), which Java reads when it builds a request
 *   (so an empty map completes there), and Java's `MockAdminClient` never;
 * - `MockAdminClient.updateBeginningOffsets`, `updateEndOffsets` and
 *   `updateConsumerGroupOffsets`: a null key, a key with a null topic, or a
 *   null offset, which Java's mock stores with `putAll`;
 * - the seven group calls that take group ids: a null group id, or a null id
 *   in `groupIds`, which Java's `CoordinatorKey.byGroupId` stores, the future
 *   failing later;
 * - `listConsumerGroupOffsets`: a null `ListConsumerGroupOffsetsSpec`;
 *   `alterConsumerGroupOffsets`: a null `offsets` map, or a null
 *   `OffsetAndMetadata` in it;
 *   `deleteConsumerGroupOffsets`: null partitions. Java's handlers store them
 *   and read them when they build the request;
 * - `removeMembersFromConsumerGroup`: a `MemberToRemove` whose group instance
 *   id is null, which Java sends as a null member identity;
 * - `AlterConsumerGroupOffsetsResult.partitionResult`: a null partition, or
 *   one with a null topic, where Java's future fails with the request's error
 *   or with "Alter offset for partition "null" was not attempted".
 * - `describeClientQuotas`: a null filter, a filter with null components, or
 *   a null component, which Java's request builder reads;
 * - `alterClientQuotas`: a `ClientQuotaAlteration` with a null entity, null
 *   ops or a null op, an entity with null entries, or an op with a null key,
 *   which Java's request builder reads;
 * - `renewDelegationToken` and `expireDelegationToken`: a null HMAC, which
 *   Java writes when it sends the request, and which Java's `MockAdminClient`
 *   matches to no token, failing the future with
 *   `DelegationTokenNotFoundException("")`;
 * - `describeDelegationToken`: a `DescribeDelegationTokenOptions` whose
 *   owners hold null, which Java's request builder reads, and which Java's
 *   `MockAdminClient` matches to no token;
 * - `describeTransactions` and `fenceProducers`: a null id in
 *   `transactionalIds`, and `forceTerminateTransaction`: a null
 *   `transactionalId`, which Java's `CoordinatorKey.byTransactionalId`
 *   stores, the future failing later;
 * - `listTransactions`: a `ListTransactionsOptions` whose filtered states or
 *   producer ids hold null, which Java's request builder reads;
 * - `updateFeatures`: a null `FeatureUpdate`, or one whose upgrade type is
 *   null, which Java's request builder reads.
 *
 * Why: the C API cannot carry these nulls (a list, a handle, a C string or a
 * singleton cannot be `NULL` there). Goes when: the FFI takes them and the
 * core fails the future as Java does.
 *
 * *(deviation)* `alterUserScramCredentials` takes only the two
 * `UserScramCredentialAlteration` subclasses Java has, and refuses another
 * subclass with a `TypeError` at the call, where Java leaves its user out of
 * the request and fails its future after the response with "The broker
 * response did not contain a result for user <user>", or with the call's
 * error. Why: the C API carries an alteration as one of the two; a subclass
 * Java adds is a new form. Permanent.
 *
 * *(deviation)* `alterClientQuotas` with an op whose value is `NaN` clears the
 * quota, as a null value does, where Java sends `NaN`. Why: the FFI reads
 * `NaN` as Java's null. Goes when: the FFI carries the null separately.
 *
 * *(deviation)* `alterReplicaLogDirs` with a null log directory sends the empty
 * string, where Java's request carries a null path, which fails it. Why: the
 * FFI reads a `NULL` log directory as `""`. Goes when: the FFI carries the
 * null.
 *
 * *(deviation)* `updateFeatures` with `options` given as `null` refuses the
 * options first ("options must not be null"), a null `FeatureUpdate` first
 * ("featureUpdates must not contain null"), and a `FeatureUpdate` whose
 * upgrade type is null first ("featureUpdates must not contain a
 * FeatureUpdate with a null upgradeType"), where Java's
 * `KafkaAdminClient` first refuses an empty map ("Feature updates can not be
 * null or empty.") and a blank or null feature name ("Provided feature can
 * not be empty."). Why: the FFI reads the options and each update before the
 * core's checks, and the binding repeats none of them. Goes when: the FFI
 * leaves the options and the updates to the core, after its checks.
 *
 * *(deviation)* `MockAdminClient.updateFeatures` with a null feature name
 * keys its Result, and words its error, by `""` ("Invalid update version 2
 * for feature . Can't upgrade above 0"), where Java's mock uses null
 * ("for feature null."). On `KafkaAdminClient` the `""` is Java's answer: the
 * core refuses it as Java's `Utils.isBlank(null)` does. Why: the C map takes
 * no `NULL` key, so the FFI reads a null name as `""`. Goes when: the FFI
 * carries a null feature name.
 *
 * *(deviation)* `MockAdminClient` answers through the Result where Java's
 * mock throws `UnsupportedOperationException` from the call: in the calls
 * Java's mock does not implement, `describeConsumerGroups`,
 * `describeClassicGroups`, `deleteConsumerGroups`,
 * `deleteConsumerGroupOffsets`, `alterConsumerGroupOffsets`,
 * `removeMembersFromConsumerGroup`, `electLeaders`, `createAcls`,
 * `describeAcls`, `deleteAcls`, `describeClientQuotas`, `alterClientQuotas`,
 * `describeUserScramCredentials`, `alterUserScramCredentials`,
 * `describeProducers`, `describeTransactions`, `abortTransaction`,
 * `forceTerminateTransaction`, `listTransactions`, `fenceProducers` and
 * `createPartitions`, and in the cases Java's mock leaves out of the others:
 * `deleteRecords` with records to delete, `listConsumerGroupOffsets` with
 * other than one group, and `listOffsets` with an `OffsetSpec.forTimestamp`.
 * The call returns its Result, whose futures fail with
 * `UnsupportedOperationError` and Java's message ("Not implemented yet", or
 * "Not implement yet" where Java's mock spells it so). Given an empty
 * collection, `describeConsumerGroups`, `describeClassicGroups`,
 * `deleteConsumerGroups`, `createPartitions`, `createAcls`, `deleteAcls`,
 * `alterClientQuotas`, `alterUserScramCredentials`, `describeProducers`,
 * `describeTransactions`, `fenceProducers` and `listConsumerGroupOffsets`
 * (an empty `groupSpecs`) answer. A null argument is the
 * binding's `TypeError`, whatever Java's mock would throw. Why: the FFI's mock answers through the
 * Result, and an RPC's entry point reports no error at the call but a caught
 * panic. Goes when: the FFI's mock throws from the call as Java's does.
 *
 * Two more differences of `MockAdminClient` are marked *(deviation)* on its
 * class, with their Why and Goes when: its futures show their outcome on a
 * later turn of the event loop, and `createDelegationToken` with no renewer
 * fails its future where Java's mock throws.
 *
 * *(deviation)* `MockAdminClient` answers otherwise where Java's mock throws:
 *
 * - `createTopics` on `new MockAdminClient()` (whose broker is
 *   `Node.noNode()`) fails its futures with `IllegalArgumentError` "Index -1
 *   out of bounds for length 1", where Java's mock throws
 *   `IndexOutOfBoundsException` from the call;
 * - `Builder.controller(index)` out of range, and `Builder.numBrokers`
 *   shrinking below the log-dir list, throw `IllegalArgumentError`
 *   (`controller(3)` with three brokers: "Index 3 out of bounds for length 3";
 *   the shrinking `numBrokers`: "toIndex = 2"), where Java throws
 *   `IndexOutOfBoundsException`;
 * - `setFetchesRemainingUntilVisible` on an unknown topic throws
 *   `IllegalArgumentError`, where Java's mock throws a bare
 *   `RuntimeException`;
 * - `controller(null)`, and the constructor's `{ brokers, controller: null }`,
 *   throw `TypeError` "controller must not be null", where Java's mock throws
 *   `IllegalArgumentException` "The controller node must be in the list of
 *   brokers";
 * - `addTopic` with a null name throws `TypeError` "name must not be null",
 *   where Java's mock adds a topic named null; `Builder.clusterId(null)` and
 *   `Builder.brokerLogDirs(null)` throw `TypeError`, where Java's builder
 *   stores the null and builds a mock with it;
 * - `alterReplicaLogDirs` with a broker id beyond the log dirs,
 *   `listPartitionReassignments` after the topic is deleted or shrinks, and a
 *   `ConfigResource.Type.BROKER` resource whose name is not a number (`abc`)
 *   in `describeConfigs` and `incrementalAlterConfigs` fail their futures,
 *   where Java's mock throws `IndexOutOfBoundsException`, a bare
 *   `RuntimeException` or `NumberFormatException` from the call;
 * - `listOffsets` on a partition never seeded answers offset -1,
 *   `listConsumerGroupOffsets(groupId)` with seeded offsets answers every
 *   offset, and `describeDelegationToken()` with the default options lists
 *   every token, where Java's mock throws `NullPointerException`;
 * - `alterPartitionReassignments` on an unknown partition fails its future
 *   with `UnknownTopicOrPartitionError` "This server does not host this
 *   topic-partition.", where Java's mock's error has no message;
 * - where Java's mock returns null for a field it does not track, the core's
 *   mock returns an empty list or `UNKNOWN`.
 *
 * Why: the FFI's mock answers so, and its argument checks are the FFI's.
 * Goes when: the core's mock throws and answers as Java's does.
 *
 * *(deviation)* `deleteTopics` and `describeTopics` with a null topic name, and
 * `createTopics` with a `NewTopic` whose name is null, throw `TypeError` at the
 * call ("topics must not contain null", "topic_names must not contain null",
 * "name must not be null", the FFI's words), where Java fails that topic's
 * future with `InvalidTopicError` ("The given topic name 'null' cannot be
 * represented in a request." for a `NewTopic`). Why: the C API cannot carry a
 * null name. Goes when: the FFI takes the null and the core fails the future
 * as Java's does.
 *
 * *(deviation)* `describeConfigs` and `incrementalAlterConfigs` with a
 * `ConfigResource.Type.BROKER` or `BROKER_LOGGER` resource whose name is not
 * a number return their Result: the core sends the resource to any broker,
 * which refuses it, and its future fails with the broker's
 * `InvalidRequestError` ("Broker id must be an integer, but it is: abc", or
 * "Node id must be an integer …"), where Java's `KafkaAdminClient` throws
 * `NumberFormatException` from the call. Why: the core reads such a name as
 * any broker. Goes when: the core refuses it as Java's does.
 *
 * *(deviation)* `RemoveMembersFromConsumerGroupResult.memberResult` for a
 * member not in a client's request fails with the core's message, "Member
 * <id> was not included in the original request", where Java's names the
 * member by its `Object.toString()`; a Result built through the constructor
 * uses Java's text. Why: the core builds the client's Result. Goes when: the
 * core's message is Java's.
 *
 * *(deviation, as in Python)* `Admin.create` takes its configuration as
 * `configs`, bare or as `{ configs }`, where Java names it `props` and `conf`.
 * Why: one name for every client's configuration. Permanent.
 *
 * @packageDocumentation
 */

export { AbortTransactionOptions } from './abort_transaction_options';
export { AbortTransactionResult } from './abort_transaction_result';
export { AbortTransactionSpec } from './abort_transaction_spec';
export { AbstractOptions } from './abstract_options';
export { Admin } from './admin';
export type { AdminClientConfigs } from './admin_client_configs';
export { AlterClientQuotasOptions } from './alter_client_quotas_options';
export { AlterClientQuotasResult } from './alter_client_quotas_result';
export { AlterConfigOp } from './alter_config_op';
export { AlterConfigsOptions } from './alter_configs_options';
export { AlterConfigsResult } from './alter_configs_result';
export { AlterConsumerGroupOffsetsOptions } from './alter_consumer_group_offsets_options';
export { AlterConsumerGroupOffsetsResult } from './alter_consumer_group_offsets_result';
export { AlterPartitionReassignmentsOptions } from './alter_partition_reassignments_options';
export { AlterPartitionReassignmentsResult } from './alter_partition_reassignments_result';
export { AlterReplicaLogDirsOptions } from './alter_replica_log_dirs_options';
export { AlterReplicaLogDirsResult } from './alter_replica_log_dirs_result';
export { AlterUserScramCredentialsOptions } from './alter_user_scram_credentials_options';
export { AlterUserScramCredentialsResult } from './alter_user_scram_credentials_result';
export { ClassicGroupDescription } from './classic_group_description';
export type { CloseArgs } from './kafka_admin_client';
export { Config } from './config';
export { ConfigEntry } from './config_entry';
export { ConsumerGroupDescription } from './consumer_group_description';
export { CreateAclsOptions } from './create_acls_options';
export { CreateAclsResult } from './create_acls_result';
export { CreateDelegationTokenOptions } from './create_delegation_token_options';
export { CreateDelegationTokenResult } from './create_delegation_token_result';
export { CreatePartitionsOptions } from './create_partitions_options';
export { CreatePartitionsResult } from './create_partitions_result';
export { CreateTopicsOptions } from './create_topics_options';
export { CreateTopicsResult } from './create_topics_result';
export { DeleteAclsOptions } from './delete_acls_options';
export { DeleteAclsResult } from './delete_acls_result';
export { DeleteConsumerGroupOffsetsOptions } from './delete_consumer_group_offsets_options';
export { DeleteConsumerGroupOffsetsResult } from './delete_consumer_group_offsets_result';
export { DeleteConsumerGroupsOptions } from './delete_consumer_groups_options';
export { DeleteConsumerGroupsResult } from './delete_consumer_groups_result';
export { DeleteRecordsOptions } from './delete_records_options';
export { DeleteRecordsResult } from './delete_records_result';
export { DeleteTopicsOptions } from './delete_topics_options';
export { DeleteTopicsResult } from './delete_topics_result';
export { DeletedRecords } from './deleted_records';
export { DescribeAclsOptions } from './describe_acls_options';
export { DescribeAclsResult } from './describe_acls_result';
export { DescribeClassicGroupsOptions } from './describe_classic_groups_options';
export { DescribeClassicGroupsResult } from './describe_classic_groups_result';
export { DescribeClientQuotasOptions } from './describe_client_quotas_options';
export { DescribeClientQuotasResult } from './describe_client_quotas_result';
export { DescribeClusterOptions } from './describe_cluster_options';
export { DescribeClusterResult } from './describe_cluster_result';
export { DescribeConfigsOptions } from './describe_configs_options';
export { DescribeConfigsResult } from './describe_configs_result';
export { DescribeConsumerGroupsOptions } from './describe_consumer_groups_options';
export { DescribeConsumerGroupsResult } from './describe_consumer_groups_result';
export { DescribeDelegationTokenOptions } from './describe_delegation_token_options';
export { DescribeDelegationTokenResult } from './describe_delegation_token_result';
export { DescribeFeaturesOptions } from './describe_features_options';
export { DescribeFeaturesResult } from './describe_features_result';
export { DescribeLogDirsOptions } from './describe_log_dirs_options';
export { DescribeLogDirsResult } from './describe_log_dirs_result';
export { DescribeProducersOptions } from './describe_producers_options';
export { DescribeProducersResult } from './describe_producers_result';
export { DescribeReplicaLogDirsOptions } from './describe_replica_log_dirs_options';
export { DescribeReplicaLogDirsResult } from './describe_replica_log_dirs_result';
export { DescribeTopicsOptions } from './describe_topics_options';
export { DescribeTopicsResult } from './describe_topics_result';
export { DescribeTransactionsOptions } from './describe_transactions_options';
export { DescribeTransactionsResult } from './describe_transactions_result';
export { DescribeUserScramCredentialsOptions } from './describe_user_scram_credentials_options';
export { DescribeUserScramCredentialsResult } from './describe_user_scram_credentials_result';
export { ElectLeadersOptions } from './elect_leaders_options';
export { ElectLeadersResult } from './elect_leaders_result';
export { ExpireDelegationTokenOptions } from './expire_delegation_token_options';
export { ExpireDelegationTokenResult } from './expire_delegation_token_result';
export { FeatureMetadata } from './feature_metadata';
export { FeatureUpdate } from './feature_update';
export { FenceProducersOptions } from './fence_producers_options';
export { FenceProducersResult } from './fence_producers_result';
export { FinalizedVersionRange } from './finalized_version_range';
export { GroupListing } from './group_listing';
export { KafkaAdminClient } from './kafka_admin_client';
export { ListConfigResourcesOptions } from './list_config_resources_options';
export { ListConfigResourcesResult } from './list_config_resources_result';
export { ListConsumerGroupOffsetsOptions } from './list_consumer_group_offsets_options';
export { ListConsumerGroupOffsetsResult } from './list_consumer_group_offsets_result';
export { ListConsumerGroupOffsetsSpec } from './list_consumer_group_offsets_spec';
export { ListGroupsOptions } from './list_groups_options';
export { ListGroupsResult } from './list_groups_result';
export { ListOffsetsOptions } from './list_offsets_options';
export { ListOffsetsResult } from './list_offsets_result';
export { ListPartitionReassignmentsOptions } from './list_partition_reassignments_options';
export { ListPartitionReassignmentsResult } from './list_partition_reassignments_result';
export { ListTopicsOptions } from './list_topics_options';
export { ListTopicsResult } from './list_topics_result';
export { ListTransactionsOptions } from './list_transactions_options';
export { ListTransactionsResult } from './list_transactions_result';
export { LogDirDescription } from './log_dir_description';
export { MemberAssignment } from './member_assignment';
export { MemberDescription } from './member_description';
export { MemberToRemove } from './member_to_remove';
export { MemberToRemoveSet } from './member_to_remove_set';
export { MockAdminClient } from './mock_admin_client';
export { NewPartitionReassignment } from './new_partition_reassignment';
export { NewPartitions } from './new_partitions';
export { NewTopic } from './new_topic';
export { OffsetSpec } from './offset_spec';
export { PartitionReassignment } from './partition_reassignment';
export { ProducerState } from './producer_state';
export { RecordsToDelete } from './records_to_delete';
export { RemoveMembersFromConsumerGroupOptions } from './remove_members_from_consumer_group_options';
export { RemoveMembersFromConsumerGroupResult } from './remove_members_from_consumer_group_result';
export { RenewDelegationTokenOptions } from './renew_delegation_token_options';
export { RenewDelegationTokenResult } from './renew_delegation_token_result';
export { ReplicaInfo } from './replica_info';
export { ScramCredentialInfo } from './scram_credential_info';
export { ScramMechanism } from './scram_mechanism';
export { SupportedVersionRange } from './supported_version_range';
export { TerminateTransactionOptions } from './terminate_transaction_options';
export { TerminateTransactionResult } from './terminate_transaction_result';
export { TopicDescription } from './topic_description';
export { TopicListing } from './topic_listing';
export { TransactionDescription } from './transaction_description';
export { TransactionListing } from './transaction_listing';
export { TransactionState } from './transaction_state';
export { UnregisterControllerOptions } from './unregister_controller_options';
export { UnregisterControllerResult } from './unregister_controller_result';
export { UpdateFeaturesOptions } from './update_features_options';
export { UpdateFeaturesResult } from './update_features_result';
export { UserScramCredentialAlteration } from './user_scram_credential_alteration';
export { UserScramCredentialDeletion } from './user_scram_credential_deletion';
export { UserScramCredentialUpsertion } from './user_scram_credential_upsertion';
export { UserScramCredentialsDescription } from './user_scram_credentials_description';

// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { AdminClientConfig } from './admin_client_config';
// END GENERATED EXPORTS

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;
