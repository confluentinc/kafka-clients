
# Critic 0 Review -- Layer 6 (commits 5884a10..096643f)

## Issue 34: NetworkClientTest missing 17 test translations
- **File**: `src/clients/network_client.rs`
- **Severity**: Missing Requirement
- **Java Reference**: `NetworkClientTest.java`
- **Description**: Java NetworkClientTest has 41 test methods but only 29 are translated to Rust. The following tests are missing (excluding testTelemetryRequest which is deferred per plan):

  1. testDnsLookupFailure
  2. testRebootstrap
  3. testInflightRequestsDuringRebootstrap
  4. testUnsupportedApiVersionsRequestWithVersionProvidedByTheBroker
  5. testUnsupportedApiVersionsRequestWithoutVersionProvidedByTheBroker
  6. testRequestTimeout
  7. testDefaultRequestTimeout
  8. testConnectionSetupTimeout
  9. testConnectionTimeoutAfterThrottling
  10. testConnectionThrottling
  11. testAuthenticationFailureWithInFlightMetadataRequest
  12. testConnectionDelayDisconnectedWithNoExponentialBackoff
  13. testReconnectAfterAddressChange
  14. testFailedConnectionToFirstAddress
  15. testFailedConnectionToFirstAddressAfterReconnect
  16. testCloseConnectingNode
  17. testConnectionDoesNotRemainStuckInCheckingApiVersionsStateIfChannelNeverBecomesReady

  Notable: timeout tests (6-10), rebootstrap tests (2-3), and address change tests (13-15) test important functionality.

- **Expected**: All Java tests should be translated per Definition of Done.
- **Actual**: 17 tests are not translated.

## Issue 35: MetadataTest missing 9 test translations
- **File**: `src/clients/metadata.rs`
- **Severity**: Missing Requirement
- **Java Reference**: `MetadataTest.java`
- **Description**: Java MetadataTest has 29 test methods but only 20 are translated to Rust. Missing tests:

  1. testIgnoreLeaderEpochInOlderMetadataResponse
  2. testStaleMetadata
  3. testPartialMetadataUpdate
  4. testNodeIfOnlineWhenNotInReplicaSet
  5. testNodeIfOnlineNonExistentTopicPartition
  6. testLeaderMetadataInconsistentWithBrokerMetadata
  7. testMetadataMerge
  8. testMetadataMergeOnIdDowngrade
  9. testConcurrentUpdateAndFetchForSnapshotAndCluster

  Notable: testPartialMetadataUpdate (3) tests an important code path, and testMetadataMerge/testMetadataMergeOnIdDowngrade (7-8) test the merge_with logic.

- **Expected**: All Java tests should be translated per Definition of Done.
- **Actual**: 9 tests are not translated.

## Issue 36: ClusterConnectionStatesTest missing 5 test translations
- **File**: `src/clients/cluster_connection_states.rs`
- **Severity**: Missing Requirement
- **Java Reference**: `ClusterConnectionStatesTest.java`
- **Description**: Java ClusterConnectionStatesTest has 15 test methods but only 10 are translated to Rust. The 5 missing tests need to be identified and translated.
- **Expected**: All Java tests should be translated per Definition of Done.
- **Actual**: 5 tests are not translated.
