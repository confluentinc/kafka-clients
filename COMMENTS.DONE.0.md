# Review Comments for Commit 62be2dd - Layer 1: Core Protocol Types

## Issue 1: Cluster - Missing PartialEq/Eq/Hash implementations

**File**: src/common/cluster.rs

Java Cluster implements equals() (line 377) and hashCode() (line 393), comparing isBootstrapConfigured, nodes, unauthorizedTopics, invalidTopics, internalTopics, controller, partitionsByTopicPartition, clusterResource, and topicIds.

The Rust Cluster struct does not derive or implement PartialEq, Eq, or Hash. This means clusters cannot be compared for equality, which blocks translating testEquals and testNotEquals from ClusterTest.java.

**Fix**: Implement PartialEq, Eq, and Hash for Cluster matching the Java fields used in equals/hashCode.


## Issue 2: Cluster - Missing node shuffling in constructor

**File**: src/common/cluster.rs, line 87

Java Cluster constructor (line 122-124) creates a shuffled copy of the nodes list. The comment says "make a randomized, unmodifiable copy of the nodes" - this is intentional for load balancing across brokers. The Rust version stores nodes in the order received without shuffling.

**Fix**: Shuffle the nodes vector before storing it (e.g., using rand::seq::SliceRandom).

## Issue 3: Cluster - Missing Java test translations

**File**: src/common/cluster.rs tests module

Java ClusterTest.java has 3 test methods not translated to Rust:
- testReturnUnmodifiableCollections() (line 65) - Rust borrow checker enforces this, can be skipped.
- testEquals() (line 143) - Cannot be translated until Issue 1 is fixed.
- testNotEquals() (line 95) - Cannot be translated until Issue 1 is fixed.

Per CLAUDE.md rule 3: "Keep the same tests, after translating a class, also translate and run all its corresponding tests."

**Fix**: Translate testEquals and testNotEquals after fixing Issue 1. testReturnUnmodifiableCollections can be skipped as Rust's type system enforces this at compile time.

## Issue 4: ApiKeys - Multiple missing public methods

**File**: src/common/protocol/api_keys.rs

The following public methods from Java ApiKeys.java are not implemented in Rust:

- allVersions() (line 230) - Returns list of all supported versions
- toApiVersion(boolean) (line 279) - Converts to ApiVersionsResponseData.ApiVersion
- toApiVersionForApiResponse(boolean, ListenerType) (line 274) - API version for response with listener scope
- inScope(ListenerType) (line 309) - Checks if API is in scope for a listener type
- brokerApis() (line 354) - Returns APIs for broker listener
- controllerApis() (line 358) - Returns APIs for controller listener
- clientApis() (line 362) - Returns APIs for client use
- apisForListener(ListenerType) (line 366) - General listener filtering

These methods are needed by the Kafka client for API version negotiation during connection establishment. This is a **blocker** for implementing the network layer.

**Fix**: Implement the missing methods. Note that toApiVersion and toApiVersionForApiResponse depend on ApiVersionsResponseData (a generated type), so the generated message structs may need read/write support first.


## Issue 5: ApiKeys - Missing Java test translations

**File**: src/common/protocol/api_keys.rs tests module

Java ApiKeysTest.java has the following untranslated tests:
- testForIdWithInvalidIdLow() - Tests that invalid low ID is handled
- testForIdWithInvalidIdHigh() - Tests that invalid high ID is handled
- testAlterPartitionIsClusterAction() - Tests cluster action flag
- testResponseThrottleTime() - Tests throttle time in response schema
- testApiScope() - Tests API scope per listener type (blocked by missing inScope())
- testHasValidVersions() - Only partially covered by existing test_version_range()

Per CLAUDE.md rule 3.

**Fix**: Translate the non-blocked tests (testForIdWithInvalidIdLow, testForIdWithInvalidIdHigh, testAlterPartitionIsClusterAction, testHasValidVersions). The scope and throttle time tests can be added after the missing methods are implemented.



## Issue 6: Errors - Truncated error messages

**File**: src/common/protocol/errors.rs

Multiple error messages are shorter than their Java equivalents, missing important context:

- InvalidSessionTimeout (26): Missing "(as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)"
- InvalidTransactionTimeout (50): Missing "(as configured by transaction.max.timeout.ms)"
- InvalidRequest (42): Missing "See the broker logs for more details."
- UnknownProducerId (59): Missing detail about retention and producer metadata removal
- OperationNotAttempted (55): Missing detail about batched RPCs
- FencedMemberEpoch (110): Missing "The member must abandon all its partitions and rejoin."
- StaleMemberEpoch (112): Missing retry guidance via ConsumerGroupHeartbeat API

These messages are user-facing and help with debugging. Per CLAUDE.md rule 4: "Keep similar comments as the Java source."

**Fix**: Complete the error messages to match the Java source strings.
