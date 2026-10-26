# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Java ports for the admin family: inventory and the tests the other admin test files lack.

Inventory of every Java test of the translated admin-family classes (Apache Kafka
4.4.0). Status: ported before (named file), ported now (this file), skipped (reason).
A test needing a Java test double the binding cannot inject (`MockClient`,
`MockTime`, mocked metadata) is skipped here; its translation is in the
core, `rust/src/admin/kafka_admin_client.rs`, under the name given. Where Java uses
the double only to make a call fail, the port runs against an unreachable bootstrap.

Per class (`clients/admin`, `common.*`):

  AdminClientConfigTest | 1 ported now, 1 skipped | testInvalidMetadataRecoveryStrategy here; testDefaultMetadataRecoveryStrategy skipped (AdminClientConfig is not generated, the value cannot be read)
  AdminClientTestUtils / AdminClientUnitTestEnv / FakeForwardingAdmin | no tests | Java test doubles
  MockAdminClientTest (4.4 test jar) | skipped | all four test describeStreamsGroups (Streams, not generated); the mock itself is covered by test_mock_admin_client.py
  RaftVoterEndpointTest, 15 tests | skipped | RaftVoterEndpoint belongs to the Raft quorum RPCs, not generated
  ConfigTest | ported before | test_admin_value_types.py
  ConsumerGroupDescriptionTest | ported before | test_admin_value_types.py
  ConsumerGroupListingTest | skipped | deprecated class, not generated
  DeleteConsumerGroupOffsetsResultTest | ported before | test_admin_reference_set.py::TestDeleteConsumerGroupOffsetsResult
  DeleteTopicsResultTest | ported before | test_admin_reference_set.py
  DescribeTopicsResultTest | ported before | test_admin_results_hand.py
  DescribeUserScramCredentialsResultTest | ported before | test_admin_results_hand.py
  EndpointTypeTest | skipped | EndpointType is used only inside KafkaAdminClient, not reachable from the public API, not generated
  GroupListingTest | ported before | test_admin_value_types.py
  ListConsumerGroupsOptionsTest | skipped | deprecated class, not generated
  ListGroupsOptionsTest | ported before | test_admin_reference_set.py
  ListTransactionsResultTest | ported before | test_admin_reference_set.py::TestListTransactionsResult
  MemberDescriptionTest | ported before | test_admin_value_types.py
  NewTopicTest | ported before (8 of 10) | test_admin_value_types.py; the two convertToCreatableTopic tests are package-private, not generated
  RemoveMembersFromConsumerGroupOptionsTest | ported before | test_admin_reference_set.py
  RemoveMembersFromConsumerGroupResultTest | ported before | test_admin_results_hand.py
  ScramMechanismTest | ported before | test_admin_value_types.py
  TopicCollectionTest | ported before | test_admin_common_types.py
  common.acl: AclBindingTest, AclOperationTest, AclPermissionTypeTest, ResourcePatternFilterTest, ResourcePatternTest | ported before | test_admin_common_types.py
  common.resource.ResourceTypeTest; common.config.ConfigResourceTest | ported before | test_admin_common_types.py
  common.config: AbstractConfigTest, ConfigDefTest, ConfigTransformerTest, SaslConfigsTest, provider/* | skipped | config classes are not translated (CLAUDE.md, Not translated); not added by the admin family
  common.security: JaasContextTest, SaslExtensionsTest, auth/*, authenticator/*, kerberos/*, oauthbearer/*, plain/*, scram/*, ssl/* | skipped | not classes the admin family added; admin has no SASL (decision log)
  (no common.quota test directory) | none | ClientQuota* have no Java test; smoke tests in test_admin_common_types.py
  admin/internals/*Test (AbortTransactionHandler, AdminApiDriver, AdminBootstrapAddresses, AdminFetchMetricsManager, AdminMetadataManager, AllBrokersStrategy(+Integration), AlterConsumerGroupOffsetsHandler, CoordinatorStrategy, DeleteConsumerGroupOffsetsHandler, DeleteConsumerGroupsHandler, DeleteGroupsHandler, DeleteRecordsHandler, DeleteShareGroupsHandler, DescribeConsumerGroupsHandler, DescribeProducersHandler, DescribeTransactionsHandler, FenceProducersHandler, ListConsumerGroupOffsetsHandler, ListOffsetsHandler, ListTransactionsHandler, PartitionLeaderStrategy(+Integration), RemoveMembersFromConsumerGroupHandler) | skipped | package `internals`: not public, not generated, driven only through the core; translated in rust/src/admin/internals/*.rs

The seven 4.4.0 files, 283 tests in all (Apache Kafka 4.4.0 split 4.3.1's KafkaAdminClientTest by area): 6 before, 8 internal, 8 telemetry, 184 MockClient, 13 now, 63 not generated, 1 JVM-only.

KafkaAdminClientTest, 73 tests: 5 before, 8 internal, 8 telemetry, 31 MockClient, 6 now, 14 not generated, 1 JVM-only.

  testDefaultApiTimeoutAndRequestTimeoutConflicts | ported before | test_admin_client.py::test_default_api_timeout_and_request_timeout_conflicts
  testParseDescribeClusterResponseWithError | skipped | package-private static helper, not generated; core: none
  testParseDescribeClusterResponseWithUnexpectedEndpointType | skipped | package-private static helper, not generated; core: none
  testParseSuccessfulDescribeClusterResponse | skipped | package-private static helper, not generated; core: none
  testGetOrCreateListValue | skipped | package-private static helper, not generated; core: none
  testCalcTimeoutMsRemainingAsInt | skipped | package-private static helper, not generated; core: none
  testPrettyPrintException | skipped | package-private static helper, not generated; core: none
  testGenerateClientId | skipped | package-private static helper, not generated; core: test_generate_client_id
  testMetricsReporterAutoGeneratedClientId | skipped | metric reporters are class-valued config and KIP-714 clientInstanceId is omitted
  testDisableJmxReporter | skipped | metric reporters are class-valued config and KIP-714 clientInstanceId is omitted
  testExplicitlyEnableJmxReporter | skipped | metric reporters are class-valued config and KIP-714 clientInstanceId is omitted
  testExplicitlyEnableTelemetryReporter | skipped | metric reporters are class-valued config and KIP-714 clientInstanceId is omitted
  testTelemetryReporterIsDisabledByDefault | skipped | metric reporters are class-valued config and KIP-714 clientInstanceId is omitted
  testCloseAdminClient | ported before | test_admin_client.py::test_close_admin_client
  testCloseAdminClientInCallback | ported before | test_admin_client.py::test_close_admin_client_in_callback
  testAdminClientFailureWhenClosed | ported before | test_admin_client.py::test_admin_client_failure_when_closed
  testTimeoutWithoutMetadata | ported before | test_admin_client.py::test_timeout_without_metadata
  testConnectionFailureOnMetadataUpdate | skipped | drives MockClient; core: no same-named core test
  testUnreachableBootstrapServer | skipped | drives MockClient; core: test_unreachable_bootstrap_server
  testUnreachableBootstrapServerNoRebootstrap | skipped | drives MockClient; core: test_unreachable_bootstrap_server_no_rebootstrap
  testPropagatedMetadataFetchException | skipped | drives MockClient; core: test_propagated_metadata_fetch_error
  testAdminClientApisAuthenticationFailure | skipped | drives MockClient; core: test_admin_client_apis_authentication_failure
  testDescribeCluster | skipped | drives MockClient; core: test_describe_cluster
  testDescribeClusterHandleError | skipped | drives MockClient; core: test_describe_cluster_handle_error
  testDescribeClusterFailBack | skipped | drives MockClient; core: test_describe_cluster_fail_back
  testDescribeClusterHandleUnsupportedVersionForIncludingFencedBrokers | skipped | drives MockClient; core: test_describe_cluster_unsupported_version_for_fenced_brokers
  testUpdateFeaturesDuringSuccess | skipped | drives MockClient; core: test_update_features_during_success
  testUpdateFeaturesTopLevelError | skipped | drives MockClient; core: test_update_features_top_level_error
  testUpdateFeaturesHandleNotControllerException | skipped | drives MockClient; core: test_update_features_handle_not_controller_error
  testUpdateFeaturesShouldFailRequestForEmptyUpdates | ported now | test_update_features_should_fail_request_for_empty_updates
  testUpdateFeaturesShouldFailRequestForInvalidFeatureName | ported now | test_update_features_should_fail_request_for_invalid_feature_name
  testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion | ported now | test_update_features_should_fail_request_in_client_when_downgrade_flag_is_not_set_during_deletion
  testDescribeFeaturesSuccess | skipped | drives MockClient; core: test_describe_features_success
  testDescribeFeaturesFailure | skipped | drives MockClient; core: test_describe_features_failure
  testDescribeFeaturesWithNodeSuccess | skipped | drives MockClient; core: test_describe_features_with_node_success
  testDescribeFeaturesWithNodeFailure | skipped | drives MockClient; core: test_describe_features_with_node_failure
  testDescribeMetadataQuorumSuccess | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDescribeMetadataQuorumRetriableError | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDescribeMetadataQuorumFailure | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testGetSubLevelError | skipped | package-private static helper, not generated; core: none
  testSuccessfulRetryAfterRequestTimeout | skipped | drives MockClient; core: no same-named core test
  testDefaultApiTimeout | skipped | MockTime / MockClient; core: test_default_api_timeout
  testDefaultApiTimeoutOverride | skipped | MockTime / MockClient; core: test_default_api_timeout_override
  testRequestTimeoutExceedingDefaultApiTimeout | skipped | drives MockClient; core: test_request_timeout_exceeding_default_api_timeout
  testUnregisterBrokerSuccess | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testUnregisterBrokerFailure | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testUnregisterBrokerTimeoutAndSuccessRetry | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testUnregisterBrokerTimeoutAndFailureRetry | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testUnregisterBrokerTimeoutMaxRetry | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testUnregisterBrokerTimeoutMaxWait | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testUnregisterControllerSuccess | skipped | drives MockClient; core: test_unregister_controller_success
  testUnregisterControllerFailure | skipped | drives MockClient; core: test_unregister_controller_failure
  testUnregisterControllerTimeoutAndSuccessRetry | skipped | drives MockClient; core: test_unregister_controller_timeout_and_success_retry
  testUnregisterControllerTimeoutAndFailureRetry | skipped | drives MockClient; core: test_unregister_controller_timeout_and_failure_retry
  testUnregisterControllerTimeoutMaxRetry | skipped | drives MockClient; core: test_unregister_controller_timeout_max_retry
  testUnregisterControllerTimeoutMaxWait | ported now | test_unregister_controller_timeout_max_wait (unreachable bootstrap, timeout_ms=10); core: test_unregister_controller_timeout_max_wait
  testClientSideTimeoutAfterFailureToSend | skipped | drives MockClient; core: no same-named core test
  testClientSideTimeoutAfterFailureToReceiveResponse | skipped | drives MockClient; core: no same-named core test
  testClientInstanceId | skipped | metric reporters are class-valued config and KIP-714 clientInstanceId is omitted
  testClientInstanceIdInvalidTimeout | skipped | metric reporters are class-valued config and KIP-714 clientInstanceId is omitted
  testClientInstanceIdNoTelemetryReporterRegistered | skipped | metric reporters are class-valued config and KIP-714 clientInstanceId is omitted
  testListClientMetricsResources | skipped | listClientMetricsResources is deprecated (4.1), not generated
  testListClientMetricsResourcesEmpty | skipped | listClientMetricsResources is deprecated (4.1), not generated
  testListClientMetricsResourcesNotSupported | skipped | listClientMetricsResources is deprecated (4.1), not generated
  testListConfigResources | skipped | drives MockClient; core: test_list_config_resources
  testListConfigResourcesEmpty | skipped | drives MockClient; core: test_list_config_resources_empty
  testListConfigResourcesNotSupported | skipped | drives MockClient; core: test_list_config_resources_not_supported
  testCallFailWithUnsupportedVersionExceptionDoesNotHaveConcurrentModificationException | skipped | drives MockClient; core: no same-named core test
  testAddRaftVoterRequest | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testRemoveRaftVoterRequest | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testOutOfMemoryErrorPropagation | skipped | JVM-only: a Mockito-mocked MetadataResponse throws java.lang.OutOfMemoryError; Rust has no catchable out-of-memory error (allocation failure aborts, CLAUDE.md 12.1), so nothing is translated; core: none
  testAdminBootstrapResolutionExceptionPropagated | ported now | test_admin_bootstrap_resolution_error_propagated (bootstrap.resolve.timeout.ms=3000, no MockClient needed)
  testAdminConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero | ported now | test_admin_constructor_fails_with_config_error_on_unresolvable_bootstrap_when_timeout_zero

KafkaAdminClientConfigTest, 19 tests: 18 MockClient, 1 now.

  testDescribeAcls | skipped | drives MockClient; core: test_describe_acls
  testCreateAclsToController | skipped | drives MockClient; core: test_create_acls_to_controller
  testDeleteAclsToController | skipped | drives MockClient; core: test_delete_acls_to_controller
  testCreateAcls | skipped | drives MockClient; core: test_create_acls
  testDeleteAcls | skipped | drives MockClient; core: test_delete_acls
  testDescribeBrokerConfigs | skipped | drives MockClient; core: test_describe_broker_configs
  testDescribeBrokerAndLogConfigs | skipped | drives MockClient; core: test_describe_broker_and_log_configs
  testDescribeConfigsPartialResponse | skipped | drives MockClient; core: test_describe_configs_partial_response
  testDescribeConfigsUnrequested | skipped | drives MockClient; core: test_describe_configs_unrequested
  testDescribeClientMetricsConfigs | skipped | drives MockClient; core: test_describe_client_metrics_configs
  testDescribeConsumerGroupConfigs | skipped | drives MockClient; core: no same-named core test
  testIncrementalAlterConfigs | skipped | drives MockClient; core: test_incremental_alter_configs
  testIncrementalAlterConfigsToController | skipped | drives MockClient; core: no same-named core test
  testDescribeClientQuotas | skipped | drives MockClient; core: test_describe_client_quotas
  testEqualsOfClientQuotaFilterComponent | ported now | test_equals_of_client_quota_filter_component
  testAlterClientQuotas | skipped | drives MockClient; core: test_alter_client_quotas
  testDescribeUserScramCredentials | skipped | drives MockClient; core: test_describe_user_scram_credentials
  testAlterUserScramCredentialsUnknownMechanism | skipped | drives MockClient; core: test_alter_user_scram_credentials_unknown_mechanism
  testAlterUserScramCredentials | skipped | drives MockClient; core: test_alter_user_scram_credentials

KafkaAdminClientConsumerGroupTest, 68 tests: 60 MockClient, 2 now, 6 not generated.

  testListGroups | skipped | drives MockClient; core: test_list_groups
  testListGroupsMetadataFailure | ported now | test_list_groups_metadata_failure
  testListGroupsEmptyProtocol | skipped | drives MockClient; core: no same-named core test
  testListGroupsEmptyGroupType | skipped | drives MockClient; core: no same-named core test
  testListGroupsWithProtocolTypes | skipped | drives MockClient; core: no same-named core test
  testListGroupsWithTypes | skipped | drives MockClient; core: test_list_groups_with_types
  testListGroupsWithTypesOlderBrokerVersion | skipped | drives MockClient; core: test_list_groups_with_types_older_broker_version
  testListConsumerGroups | skipped | drives MockClient; core: test_list_consumer_groups
  testListConsumerGroupsMetadataFailure | ported now | test_list_consumer_groups_metadata_failure
  testListConsumerGroupsWithStates | skipped | drives MockClient; core: test_list_consumer_groups_with_states
  testListConsumerGroupsWithProtocolTypes | skipped | drives MockClient; core: no same-named core test
  testListConsumerGroupsWithTypes | skipped | drives MockClient; core: no same-named core test
  testListConsumerGroupsWithStatesOlderBrokerVersion | skipped | drives MockClient; core: no same-named core test
  testListConsumerGroupsWithTypesOlderBrokerVersion | skipped | drives MockClient; core: test_list_consumer_groups_with_types_older_broker_version
  testListConsumerGroupsDeprecated | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListConsumerGroupsDeprecatedMetadataFailure | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListConsumerGroupsDeprecatedWithStates | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListConsumerGroupsDeprecatedWithTypes | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListConsumerGroupsDeprecatedWithStatesOlderBrokerVersion | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListConsumerGroupsDeprecatedWithTypesOlderBrokerVersion | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testOffsetCommitNumRetries | skipped | drives MockClient; core: no same-named core test
  testOffsetCommitWithMultipleErrors | skipped | drives MockClient; core: test_offset_commit_with_multiple_errors
  testOffsetCommitRetryBackoff | skipped | drives MockClient; core: no same-named core test
  testDescribeConsumerGroupNumRetries | skipped | drives MockClient; core: no same-named core test
  testDescribeConsumerGroupRetryBackoff | skipped | drives MockClient; core: no same-named core test
  testDescribeConsumerGroups | skipped | drives MockClient; core: test_describe_consumer_groups
  testDescribeMultipleConsumerGroups | skipped | drives MockClient; core: no same-named core test
  testDescribeConsumerGroupsGroupIdNotFound | skipped | drives MockClient; core: test_describe_consumer_groups_group_id_not_found
  testDescribeConsumerGroupsWithAuthorizedOperationsOmitted | skipped | drives MockClient; core: no same-named core test
  testDescribeNonConsumerGroups | skipped | drives MockClient; core: no same-named core test
  testDescribeGroupsWithBothUnsupportedApis | skipped | drives MockClient; core: test_describe_groups_with_both_unsupported_apis
  testDescribeOldAndNewConsumerGroups | skipped | drives MockClient; core: no same-named core test
  testListConsumerGroupOffsetsOptionsWithBatchedApi | skipped | drives MockClient; core: test_list_consumer_group_offsets_options_with_batched_api
  testListConsumerGroupOffsetsNumRetries | skipped | drives MockClient; core: no same-named core test
  testListConsumerGroupOffsetsRetryBackoff | skipped | drives MockClient; core: no same-named core test
  testListConsumerGroupOffsetsRetriableErrors | skipped | drives MockClient; core: no same-named core test
  testListConsumerGroupOffsetsNonRetriableErrors | skipped | drives MockClient; core: test_list_consumer_group_offsets_non_retriable_errors
  testListConsumerGroupOffsets | skipped | drives MockClient; core: test_list_consumer_group_offsets
  testBatchedListConsumerGroupOffsets | skipped | drives MockClient; core: test_batched_list_consumer_group_offsets
  testBatchedListConsumerGroupOffsetsWithNoFindCoordinatorBatching | skipped | drives MockClient; core: test_batched_list_consumer_group_offsets_with_no_find_coordinator_batching
  testBatchedListConsumerGroupOffsetsWithNoOffsetFetchBatching | skipped | drives MockClient; core: test_batched_list_consumer_group_offsets_with_no_offset_fetch_batching
  testDeleteConsumerGroupsNumRetries | skipped | drives MockClient; core: test_delete_consumer_groups_num_retries
  testDeleteConsumerGroupsRetryBackoff | skipped | drives MockClient; core: no same-named core test
  testDeleteConsumerGroupsWithOlderBroker | skipped | drives MockClient; core: test_delete_consumer_groups_with_older_broker
  testDeleteMultipleConsumerGroupsWithOlderBroker | skipped | drives MockClient; core: no same-named core test
  testDeleteConsumerGroupOffsetsNumRetries | skipped | drives MockClient; core: no same-named core test
  testDeleteConsumerGroupOffsetsRetryBackoff | skipped | drives MockClient; core: no same-named core test
  testDeleteConsumerGroupOffsets | skipped | drives MockClient; core: test_delete_consumer_group_offsets
  testDeleteConsumerGroupOffsetsRetriableErrors | skipped | drives MockClient; core: test_delete_consumer_group_offsets_retriable_errors
  testDeleteConsumerGroupOffsetsNonRetriableErrors | skipped | drives MockClient; core: test_delete_consumer_group_offsets_non_retriable_errors
  testDeleteConsumerGroupOffsetsFindCoordinatorRetriableErrors | skipped | drives MockClient; core: no same-named core test
  testDeleteConsumerGroupOffsetsFindCoordinatorNonRetriableErrors | skipped | drives MockClient; core: test_delete_consumer_group_offsets_find_coordinator_non_retriable_errors
  testDescribeClassicGroups | skipped | drives MockClient; core: test_describe_classic_groups
  testDescribeClassicGroupsWithAuthorizedOperationsOmitted | skipped | drives MockClient; core: test_describe_classic_groups_with_authorized_operations_omitted
  testDescribeMultipleClassicGroups | skipped | drives MockClient; core: test_describe_multiple_classic_groups
  testRemoveMembersFromGroupNumRetries | skipped | drives MockClient; core: test_remove_members_from_group_num_retries
  testRemoveMembersFromGroupRetryBackoff | skipped | drives MockClient; core: no same-named core test
  testRemoveMembersFromGroupRetriableErrors | skipped | drives MockClient; core: test_remove_members_from_group_retriable_errors
  testRemoveMembersFromGroupNonRetriableErrors | skipped | drives MockClient; core: test_remove_members_from_group_non_retriable_errors
  testRemoveMembersFromGroup | skipped | drives MockClient; core: test_remove_members_from_group
  testRemoveMembersFromGroupReason | skipped | drives MockClient; core: test_remove_members_from_group_reason
  testRemoveMembersFromGroupTruncatesReason | skipped | drives MockClient; core: test_remove_members_from_group_truncates_reason
  testRemoveMembersFromGroupDefaultReason | skipped | drives MockClient; core: test_remove_members_from_group_default_reason
  testAlterConsumerGroupOffsets | skipped | drives MockClient; core: test_alter_consumer_group_offsets
  testAlterConsumerGroupOffsetsRetriableErrors | skipped | drives MockClient; core: no same-named core test
  testAlterConsumerGroupOffsetsNonRetriableErrors | skipped | drives MockClient; core: test_alter_consumer_group_offsets_non_retriable_errors
  testAlterConsumerGroupOffsetsFindCoordinatorRetriableErrors | skipped | drives MockClient; core: no same-named core test
  testAlterConsumerGroupOffsetsFindCoordinatorNonRetriableErrors | skipped | drives MockClient; core: test_alter_consumer_group_offsets_find_coordinator_non_retriable_errors

KafkaAdminClientShareGroupTest, 22 tests: 18 not generated, 3 MockClient, 1 now.

  testDescribeShareGroups | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDescribeShareGroupsGroupIdNotFound | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDescribeShareGroupsWithAuthorizedOperationsOmitted | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDescribeMultipleShareGroups | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListShareGroups | skipped | drives MockClient; listGroups with for_streams_groups()/for_share_groups() is generated; core: none
  testListShareGroupsMetadataFailure | ported now | test_list_share_groups_metadata_failure
  testListShareGroupsWithStates | skipped | drives MockClient; listGroups with for_streams_groups()/for_share_groups() is generated; core: none
  testListShareGroupsWithStatesOlderBrokerVersion | skipped | drives MockClient; listGroups with for_streams_groups()/for_share_groups() is generated; core: none
  testListShareGroupOffsetsOptionsWithBatchedApi | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListShareGroupOffsets | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListShareGroupOffsetsMultipleGroups | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListShareGroupOffsetsEmpty | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListShareGroupOffsetsWithErrorInOnePartition | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testAlterShareGroupOffsets | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testAlterShareGroupOffsetsWithTopLevelError | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testAlterShareGroupOffsetsWithErrorInOnePartition | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteShareGroupOffsetsOptionsWithBatchedApi | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteShareGroupOffsets | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteShareGroupOffsetsEmpty | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteShareGroupOffsetsWithErrorInGroup | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteShareGroupOffsetsWithErrorInOneTopic | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteShareGroupOffsetsWithPartitionNotPresentInResult | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)

KafkaAdminClientStreamsGroupTest, 29 tests: 25 not generated, 3 MockClient, 1 now.

  testStreamsOffsetCommitNumRetries | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testStreamsOffsetCommitWithMultipleErrors | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testStreamsOffsetCommitRetryBackoff | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testBatchedListStreamsGroupOffsets | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testBatchedListStreamsGroupOffsetsWithNoFindCoordinatorBatching | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testBatchedListStreamsGroupOffsetsWithNoOffsetFetchBatching | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupsNumRetries | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupsRetryBackoff | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupsWithOlderBroker | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteMultipleStreamsGroupsWithOlderBroker | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupOffsetsNumRetries | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupOffsetsRetryBackoff | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupOffsets | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupOffsetsRetriableErrors | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupOffsetsNonRetriableErrors | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupOffsetsFindCoordinatorRetriableErrors | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDeleteStreamsGroupOffsetsFindCoordinatorNonRetriableErrors | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDescribeStreamsGroups | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDescribeStreamsGroupsWithAuthorizedOperationsOmitted | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testDescribeMultipleStreamsGroups | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testListStreamsGroups | skipped | drives MockClient; listGroups with for_streams_groups()/for_share_groups() is generated; core: none
  testListStreamsGroupsMetadataFailure | ported now | test_list_streams_groups_metadata_failure
  testListStreamsGroupsWithStates | skipped | drives MockClient; listGroups with for_streams_groups()/for_share_groups() is generated; core: none
  testListStreamsGroupsWithStatesOlderBrokerVersion | skipped | drives MockClient; listGroups with for_streams_groups()/for_share_groups() is generated; core: none
  testAlterStreamsGroupOffsets | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testAlterStreamsGroupOffsetsRetriableErrors | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testAlterStreamsGroupOffsetsNonRetriableErrors | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testAlterStreamsGroupOffsetsFindCoordinatorRetriableErrors | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)
  testAlterStreamsGroupOffsetsFindCoordinatorNonRetriableErrors | skipped | subject not generated (Streams/Share/Raft/Quorum/UnregisterBroker, or a deprecated API)

KafkaAdminClientTopicTest, 60 tests: 57 MockClient, 1 before, 2 now.

  testCreateTopics | skipped | drives MockClient; core: no same-named core test
  testCreateTopicsPartialResponse | skipped | drives MockClient; core: no same-named core test
  testCreateTopicsRetryBackoff | skipped | drives MockClient; core: test_create_topics_retry_backoff
  testCreateTopicsHandleNotControllerException | skipped | drives MockClient; core: test_create_topics_handle_not_controller_error
  testCreateTopicsRetryThrottlingExceptionWhenEnabled | skipped | drives MockClient; core: test_create_topics_retry_throttling_error_when_enabled
  testCreateTopicsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut | skipped | drives MockClient; core: test_create_topics_retry_throttling_error_when_enabled_until_request_timeout
  testCreateTopicsDontRetryThrottlingExceptionWhenDisabled | skipped | drives MockClient; core: test_create_topics_dont_retry_throttling_error_when_disabled
  testDeleteTopics | skipped | drives MockClient; core: no same-named core test
  testDeleteTopicsPartialResponse | skipped | drives MockClient; core: test_delete_topics_partial_response
  testDeleteTopicsRetryThrottlingExceptionWhenEnabled | skipped | drives MockClient; core: test_delete_topics_retry_throttling_error_when_enabled
  testDeleteTopicsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut | skipped | drives MockClient; core: test_delete_topics_retry_throttling_error_when_enabled_until_request_timeout
  testDeleteTopicsDontRetryThrottlingExceptionWhenDisabled | skipped | drives MockClient; core: test_delete_topics_dont_retry_throttling_error_when_disabled
  testInvalidTopicNames | ported before | test_admin_client.py::test_invalid_topic_names (createTopics); here (deleteTopics, describeTopics)
  testDescribeTopicsWithDescribeTopicPartitionsApiBasic | skipped | drives MockClient; core: no same-named core test
  testDescribeTopicPartitionsApiWithAuthorizedOps | skipped | drives MockClient; core: no same-named core test
  testDescribeTopicPartitionsApiWithoutAuthorizedOps | skipped | drives MockClient; core: no same-named core test
  testDescribeTopicsWithDescribeTopicPartitionsApiEdgeCase | skipped | drives MockClient; core: no same-named core test
  testDescribeTopicsWithDescribeTopicPartitionsApiErrorHandling | skipped | drives MockClient; core: no same-named core test
  testElectLeaders | skipped | drives MockClient; core: test_elect_leaders
  testDescribeLogDirs | skipped | drives MockClient; core: test_describe_log_dirs
  testDescribeLogDirsWithVolumeBytes | skipped | drives MockClient; core: test_describe_log_dirs_with_volume_bytes
  testDescribeLogDirsOfflineDir | skipped | drives MockClient; core: test_describe_log_dirs_offline_dir
  testDescribeLogDirsWithCordonedDir | ported now (isCordoned assertions) | test_describe_log_dirs_with_cordoned_dir over the extension's per-directory rows; the MockClient response is the core's, core: test_describe_log_dirs_with_cordoned_dir
  testDescribeReplicaLogDirs | skipped | drives MockClient; core: test_describe_replica_log_dirs
  testDescribeReplicaLogDirsUnexpected | skipped | drives MockClient; core: test_describe_replica_log_dirs_unexpected
  testCreatePartitions | skipped | drives MockClient; core: test_create_partitions
  testCreatePartitionsRetryThrottlingExceptionWhenEnabled | skipped | drives MockClient; core: test_create_partitions_retry_throttling_error_when_enabled
  testCreatePartitionsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut | skipped | drives MockClient; core: test_create_partitions_retry_throttling_error_when_enabled_until_request_timeout
  testCreatePartitionsDontRetryThrottlingExceptionWhenDisabled | skipped | drives MockClient; core: test_create_partitions_dont_retry_throttling_error_when_disabled
  testDeleteRecordsTopicAuthorizationError | skipped | drives MockClient; core: test_delete_records_topic_authorization_error
  testDeleteRecordsMultipleSends | skipped | drives MockClient; core: test_delete_records_multiple_sends
  testDeleteRecords | skipped | drives MockClient; core: test_delete_records
  testDescribeTopicsByIds | skipped | drives MockClient; core: test_describe_topics_by_ids
  testAlterPartitionReassignments | skipped | drives MockClient; core: no same-named core test
  testListPartitionReassignments | skipped | drives MockClient; core: test_list_partition_reassignments
  testListOffsets | skipped | drives MockClient; core: test_list_offsets
  testListOffsetsRetriesLookupWhenCachedLeaderLeavesCluster | skipped | drives MockClient; core: test_list_offsets_retries_lookup_when_cached_leader_leaves_cluster
  testListOffsetsRetriableErrors | skipped | drives MockClient; core: test_list_offsets_retriable_errors
  testListOffsetsNonRetriableErrors | skipped | drives MockClient; core: test_list_offsets_non_retriable_errors
  testListOffsetsMaxTimestampUnsupportedSingleOffsetSpec | skipped | drives MockClient; core: test_list_offsets_max_timestamp_unsupported_single_offset_spec
  testListOffsetsMaxTimestampUnsupportedMultipleOffsetSpec | skipped | drives MockClient; core: test_list_offsets_max_timestamp_unsupported_multiple_offset_spec
  testListOffsetsHandlesFulfillmentTimeouts | skipped | drives MockClient; core: no same-named core test
  testListOffsetsUnsupportedNonMaxTimestamp | skipped | drives MockClient; core: no same-named core test
  testListOffsetsNonMaxTimestampDowngradedImmediately | skipped | drives MockClient; core: no same-named core test
  testListOffsetsEarliestLocalSpecMinVersion | skipped | drives MockClient; core: no same-named core test
  testListOffsetsLatestTierSpecSpecMinVersion | skipped | drives MockClient; core: no same-named core test
  testListOffsetsEarliestPendingUploadSpecSpecMinVersion | skipped | drives MockClient; core: no same-named core test
  testListOffsetsMetadataRetriableErrors | skipped | drives MockClient; core: no same-named core test
  testListOffsetsWithMultiplePartitionsLeaderChange | skipped | drives MockClient; core: no same-named core test
  testListOffsetsWithLeaderChange | skipped | drives MockClient; core: no same-named core test
  testListOffsetsMetadataNonRetriableErrors | skipped | drives MockClient; core: no same-named core test
  testListOffsetsPartialResponse | skipped | drives MockClient; core: test_list_offsets_partial_response
  testAlterReplicaLogDirsSuccess | skipped | drives MockClient; core: test_alter_replica_log_dirs_success
  testAlterReplicaLogDirsLogDirNotFound | skipped | drives MockClient; core: test_alter_replica_log_dirs_log_dir_not_found
  testAlterReplicaLogDirsUnrequested | skipped | drives MockClient; core: test_alter_replica_log_dirs_unrequested
  testAlterReplicaLogDirsPartialResponse | skipped | drives MockClient; core: test_alter_replica_log_dirs_partial_response
  testAlterReplicaLogDirsPartialFailure | skipped | drives MockClient; core: test_alter_replica_log_dirs_partial_failure
  testDescribeLogDirsPartialFailure | skipped | drives MockClient; core: test_describe_log_dirs_partial_failure
  testDescribeReplicaLogDirsWithNonExistReplica | skipped | drives MockClient; core: test_describe_replica_log_dirs_with_non_exist_replica
  testDescribeTopicsTimeoutWhenNoBrokerResponds | ported now | test_describe_topics_timeout_when_no_broker_responds

KafkaAdminClientTransactionTest, 12 tests: 12 MockClient.

  testDescribeProducers | skipped | drives MockClient; core: test_describe_producers
  testDescribeProducersTimeout | skipped | drives MockClient; core: test_describe_producers_timeout
  testDescribeProducersRetryAfterDisconnect | skipped | drives MockClient; core: test_describe_producers_retry_after_disconnect
  testDescribeTransactions | skipped | drives MockClient; core: test_describe_transactions
  testRetryDescribeTransactionsAfterNotCoordinatorError | skipped | drives MockClient; core: test_retry_describe_transactions_after_not_coordinator_error
  testAbortTransaction | skipped | drives MockClient; core: test_abort_transaction
  testAbortTransactionFindLeaderAfterDisconnect | skipped | drives MockClient; core: test_abort_transaction_find_leader_after_disconnect
  testForceTerminateTransaction | skipped | drives MockClient; core: test_force_terminate_transaction
  testForceTerminateTransactionWithError | skipped | drives MockClient; core: test_force_terminate_transaction_with_error
  testForceTerminateTransactionWithCustomTimeout | skipped | drives MockClient; core: test_force_terminate_transaction_with_custom_timeout
  testListTransactions | skipped | drives MockClient; core: test_list_transactions
  testFenceProducers | skipped | drives MockClient; core: test_fence_producers

"""

from __future__ import annotations

import time
from typing import Any

import pytest

from confluent_kafka import ExecutionError, IllegalArgumentError
from confluent_kafka.admin import (
    Admin,
    DescribeTopicsOptions,
    UnregisterControllerOptions,
    FeatureUpdate,
    ListGroupsOptions,
)
from confluent_kafka.common import ClientQuotaEntity, ClientQuotaFilterComponent, KafkaError
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common import TopicPartition
from confluent_kafka.common.errors import BootstrapResolutionError, InvalidTopicError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError

import _confluentkafka as _lib
from confluent_kafka.admin import _conversions
from confluent_kafka.admin._futures import Sync

from .admin_futures import assert_future_throws

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
SHORT = 300
LIST_GROUPS_FAILED = "Failed to find brokers to send ListGroups"


def _admin(**extra: Any) -> Admin:
    return Admin.create(configs={**UNREACHABLE, **extra})


# ---- AdminClientConfigTest ----------------------------------------------------


@pytest.mark.parametrize("strategy", ["rebootstrap", "none"])
def test_valid_metadata_recovery_strategy(strategy: str) -> None:
    """``testDefaultMetadataRecoveryStrategy``'s accepted side: the default and the
    two names construct (the value itself cannot be read back; see the skip)."""
    _admin().close()
    _admin(**{"metadata.recovery.strategy": strategy}).close()


def test_invalid_metadata_recovery_strategy() -> None:
    """``testInvalidMetadataRecoveryStrategy``: ``ConfigException`` whose message
    names the key."""
    with pytest.raises(ConfigError) as info:
        _admin(**{"metadata.recovery.strategy": "abc"})
    assert "metadata.recovery.strategy" in str(info.value)


# ---- KafkaAdminClientTest -----------------------------------------------------


def test_invalid_topic_names_delete_topics() -> None:
    """``testInvalidTopicNames``, its ``deleteTopics`` part (``createTopics`` is in
    ``test_admin_client.py``). Java's ``null`` name is not a Java-typed call here."""
    with _admin() as admin:
        futures = admin.delete_topics(topics=[""]).topic_name_values() or {}
        assert_future_throws(futures[""], InvalidTopicError)


def test_invalid_topic_names_describe_topics() -> None:
    """``testInvalidTopicNames``, its ``describeTopics`` part."""
    with _admin() as admin:
        futures = admin.describe_topics(topic_names=[""]).topic_name_values() or {}
        assert_future_throws(futures[""], InvalidTopicError)


def test_describe_topics_timeout_when_no_broker_responds() -> None:
    """``testDescribeTopicsTimeoutWhenNoBrokerResponds``: ``retries=0``, a request
    timeout of 30 s and a call timeout of 200 ms; the future fails with a timeout
    after at least ~150 ms and well before 30 s. The unreachable bootstrap address
    stands for Java's ``MockClient`` that never responds."""
    with _admin(retries="0", **{"request.timeout.ms": "30000"}) as admin:
        start = time.monotonic()
        result = admin.describe_topics(
            topic_names=["test-topic"], options=DescribeTopicsOptions(timeout_ms=200))
        future = (result.topic_name_values() or {})["test-topic"]
        assert_future_throws(future, KafkaTimeoutError, timeout=30)
        duration_ms = (time.monotonic() - start) * 1000
        assert 150 <= duration_ms < 30000


def test_update_features_should_fail_request_for_empty_updates() -> None:
    """``testUpdateFeaturesShouldFailRequestForEmptyUpdates``."""
    with _admin() as admin:
        with pytest.raises(IllegalArgumentError) as info:
            admin.update_features(feature_updates={})
        assert str(info.value) == "Feature updates can not be null or empty."


def test_update_features_should_fail_request_for_invalid_feature_name() -> None:
    """``testUpdateFeaturesShouldFailRequestForInvalidFeatureName``."""
    upgrade = FeatureUpdate.UpgradeType.UPGRADE
    with _admin() as admin:
        with pytest.raises(IllegalArgumentError) as info:
            admin.update_features(feature_updates={
                "feature": FeatureUpdate(max_version_level=2, upgrade_type=upgrade),
                "": FeatureUpdate(max_version_level=2, upgrade_type=upgrade)})
        assert str(info.value) == "Provided feature can not be empty."


def test_update_features_should_fail_request_in_client_when_downgrade_flag_is_not_set_during_deletion() -> None:
    """``testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion``."""
    with pytest.raises(IllegalArgumentError) as info:
        FeatureUpdate(max_version_level=0, upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)
    assert str(info.value) == (
        "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when "
        "the provided maxVersionLevel:0 is < 1.")


@pytest.mark.parametrize("options", [
    ListGroupsOptions(timeout_ms=SHORT),
    ListGroupsOptions.for_consumer_groups().timeout_ms(timeout_ms=SHORT),
    ListGroupsOptions.for_streams_groups().timeout_ms(timeout_ms=SHORT),
    ListGroupsOptions.for_share_groups().timeout_ms(timeout_ms=SHORT)],
    ids=["list_groups", "list_consumer_groups", "list_streams_groups", "list_share_groups"])
def test_list_groups_metadata_failure(options: Any) -> None:
    """``testListGroupsMetadataFailure``, ``testListConsumerGroupsMetadataFailure``,
    ``testListStreamsGroupsMetadataFailure`` and ``testListShareGroupsMetadataFailure``:
    Java scripts an empty Metadata response so no broker is found, and asserts the
    ``all()`` future fails with ``KafkaException``; an unreachable bootstrap makes the
    same call fail (Java's message, ``KafkaAdminClient.listGroups``), through
    ``get()``'s ``ExecutionError``."""
    from confluent_kafka import ExecutionError

    with _admin(retries="0") as admin:
        with pytest.raises(ExecutionError) as info:
            admin.list_groups(options=options).all().get()
        assert type(info.value.__cause__) is KafkaError
        assert str(info.value.__cause__) == LIST_GROUPS_FAILED


def test_equals_of_client_quota_filter_component() -> None:
    """``testEqualsOfClientQuotaFilterComponent`` (all six assertions)."""
    user = ClientQuotaEntity.USER
    default = ClientQuotaFilterComponent.of_default_entity(entity_type=user)
    any_name = ClientQuotaFilterComponent.of_entity_type(entity_type=user)
    exact = ClientQuotaFilterComponent.of_entity(entity_type=user, entity_name="user")
    assert default == ClientQuotaFilterComponent.of_default_entity(entity_type=user)
    assert any_name == ClientQuotaFilterComponent.of_entity_type(entity_type=user)
    assert default != any_name  # match = null is different from match = Empty
    assert exact == ClientQuotaFilterComponent.of_entity(entity_type=user, entity_name="user")
    assert exact != default
    assert exact != any_name


# ---- KafkaAdminClientTest, Apache Kafka 4.4.0 ------------------------------------


def test_unregister_controller_timeout_max_wait() -> None:
    """``testUnregisterControllerTimeoutMaxWait``: Java prepares no response and sets
    ``timeoutMs(10)``; the future fails with ``TimeoutException``. The unreachable
    bootstrap address stands for the ``MockClient`` that never responds."""
    with _admin() as admin:
        result = admin.unregister_controller(
            controller_id=1, options=UnregisterControllerOptions(timeout_ms=10))
        assert_future_throws(result.all(), KafkaTimeoutError, timeout=30)


def test_admin_bootstrap_resolution_error_propagated() -> None:
    """``testAdminBootstrapResolutionExceptionPropagated`` (KIP-909): with a
    positive ``bootstrap.resolve.timeout.ms`` and an unresolvable host the client
    is created; calls fail with ``BootstrapResolutionError`` once the budget runs
    out, and every later call fails with it too."""
    configs = {"bootstrap.servers": "unresolvable.invalid:9092",
               "bootstrap.resolve.timeout.ms": "3000"}
    with Admin.create(configs=configs) as admin:
        start = time.monotonic()
        while True:
            assert time.monotonic() - start < 15, (
                "Expected BootstrapResolutionError to be thrown within 15000ms")
            try:
                admin.list_topics().names().get()
            except ExecutionError as e:
                if isinstance(e.__cause__, BootstrapResolutionError):
                    cause = e.__cause__
                    break
        assert str(cause) == (
            "Failed to resolve bootstrap servers after 3000ms. "
            "Please check your bootstrap.servers configuration and DNS settings.")

        # After the first failure, any further API call must also surface the bootstrap error.
        with pytest.raises(ExecutionError) as info:
            admin.list_topics().names().get()
        assert isinstance(info.value.__cause__, BootstrapResolutionError)


def test_admin_constructor_fails_with_config_error_on_unresolvable_bootstrap_when_timeout_zero() -> None:
    """``testAdminConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero``:
    the default ``bootstrap.resolve.timeout.ms=0`` resolves DNS in the constructor, so
    an unresolvable host fails the creation with a ``ConfigError`` wrapped in the
    constructor's ``KafkaError``."""
    with pytest.raises(KafkaError) as info:
        Admin.create(configs={"bootstrap.servers": "unresolvable.invalid:9092"})
    assert str(info.value) == "Failed to create new KafkaAdminClient"
    assert type(info.value.__cause__) is ConfigError
    assert str(info.value.__cause__) == "No resolvable bootstrap urls given in bootstrap.servers"


# ---- KafkaAdminClientTopicTest, the isCordoned assertions ---------------------------


class _LogDirsNative:
    """An extension ``AdminResult`` stand-in: broker 0's ``descriptions()`` future is
    completed with the rows the core hands the conversion, as
    ``(error, total, usable, replicas, is_cordoned)`` per directory."""

    def __init__(self, rows: dict[str, Any]) -> None:
        self._rows = rows

    def futures(self, name: str, post: Any = None) -> dict[Any, Any]:
        assert name == "descriptions"
        return {0: _lib.KafkaFuture.completed_future(post(self._rows))}


def test_describe_log_dirs_with_cordoned_dir() -> None:
    """``testDescribeLogDirsWithCordonedDir`` (KIP-1066): a log directory the broker
    reports as cordoned says so through ``is_cordoned()`` in ``descriptions()``
    (``KafkaAdminClientTopicTest:1121``) and in ``all_descriptions()`` (``:1129``).
    The extension hands each broker's directories to the conversion as
    ``(error, total, usable, replicas, is_cordoned)`` rows; the ``MockClient``
    response of Java is the core's (``test_describe_log_dirs_with_cordoned_dir``)."""
    tp = TopicPartition(topic="test", partition=0)
    raw = {"/data": (None, -1, -1, [("test", 0, 123, -1, False)], True)}
    result = _conversions.describe_log_dirs_result(_LogDirsNative(raw), Sync, None)

    futures = result.descriptions()
    assert set(futures) == {0}
    described = futures[0].get(timeout=5)
    assert set(described) == {"/data"}
    assert described["/data"].is_cordoned() is True
    assert set(described["/data"].replica_infos()) == {tp}

    everything = result.all_descriptions().get(timeout=5)
    assert set(everything) == {0}
    assert set(everything[0]) == {"/data"}
    assert everything[0]["/data"].is_cordoned() is True
    assert everything[0]["/data"].total_bytes() is None
    assert everything[0]["/data"].usable_bytes() is None


def test_describe_log_dirs_assert_description_contains_is_not_cordoned() -> None:
    """``assertDescriptionContains``' ``assertFalse(isCordoned())`` (KafkaAdminClientTopicTest,
    shared by every describeLogDirs test): a directory the broker does not cordon."""
    raw = {"/data": (None, 1000, 500, [("test", 0, 123, 4, False)], False)}
    description = _conversions._log_dir_descriptions(raw)["/data"]
    assert description.error() is None
    assert description.total_bytes() == 1000 and description.usable_bytes() == 500
    assert description.is_cordoned() is False
