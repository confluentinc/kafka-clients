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

//! Generate the Python exception hierarchy for the `confluent_kafka` binding.
//!
//! Reads the Java exception sources (Apache Kafka 4.3.1, under `kafka/`) and
//! emits one Python class per Java exception class the FFI enum
//! `kafka_common_ErrorCode_t` names, plus Java's abstract bases, the base
//! `KafkaException` and the Java built-in exceptions of the Types table, each as
//! a `.py` plus `.pyi` file in the module of its Java package (CLAUDE.md, Python
//! Binding Conventions, Modules and Errors). Every class keeps Java's `extends`
//! chain and Java's constructors, collapsed by the Signatures rules: the
//! constructors are evaluated statically through their `this(...)` /
//! `super(...)` chains, which gives each overload's message, cause and fields,
//! the Java-given defaults a shorter overload passes to a longer one, and the
//! `java_forms` overload table.
//!
//! The build fails when a Kafka class has no FFI id, an FFI id has no class, the
//! `extends` chain differs, or a Java construct outside the small slice the
//! exception sources use appears (see `java_parse`).
//!
//! The one input that is not derivable from the Java source or the FFI enum alone
//! is the correspondence between an FFI id constant (an error-*code* name such as
//! `REQUEST_TIMED_OUT`) and the Java *class* it identifies (`TimeoutException`):
//! there is no mechanical relation between the two vocabularies. That
//! correspondence is the reviewed [`BRIDGE`] table below, derived from the Rust
//! core's own `Error`-variant → id mapping (`error_code_of` in
//! `src/ffi/common.rs`). The build validates that every bridge entry names a real
//! Java class and a real FFI id, so a drift in either direction is caught.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::java_parse::{self, CallKind, Ctor, Expr, GetterBody, JavaClass, Param, Visibility};

/// Root of the Java source tree (Apache Kafka 4.3.1), relative to the repo root.
const JAVA_ROOT: &str = "kafka/clients/src/main/java";

/// The FFI enum whose id constants the Python classes carry as `_ffi_id`.
const FFI_ENUM_SOURCE: &str = "src/ffi/common.rs";

/// The Python binding's source root (relative to the repo root).
const PY_ROOT: &str = "bindings/python";

/// The Java **abstract** exception classes on this surface. They are catch-only
/// grouping bases (constructing one raises `TypeError`) and carry no FFI id, so —
/// unlike the concrete classes — the FFI-enum bijection cannot catch a missing
/// one. Listed so the generator can pull them into the graph, and **validated
/// against the Java `abstract` modifier**: the build fails if the exception-class
/// scan finds an in-scope `abstract` exception this list omits, or an entry here
/// that Java does not mark `abstract`.
const ABSTRACT_CLASSES: &[&str] = &[
    "org.apache.kafka.common.errors.RetriableException",
    "org.apache.kafka.common.errors.RefreshRetriableException",
    "org.apache.kafka.common.errors.InvalidMetadataException",
    "org.apache.kafka.common.errors.ApplicationRecoverableException",
    "org.apache.kafka.clients.consumer.InvalidOffsetException",
];

/// `*Exception` (and the two suffix-less exception) classes under `common/` and
/// `clients/` that are deliberately **out of scope** — the client error surface
/// this binding raises is only the public broker/JDK errors, not internal helpers,
/// share-consumer internals, or the security-mechanism plugins. The exception-class
/// scan ([`scan_exception_classes`]) requires every scanned class to be in the
/// bridge, an abstract base, an intermediate/base class, or listed here; a new Java
/// exception that is none of those fails the build, so it can no longer be silently
/// invisible (Critic 64 F1). Each entry cites why it is excluded.
const EXCLUSIONS: &[&str] = &[
    // Internal client-side signalling class, never surfaced to the user; no FFI id.
    "org.apache.kafka.clients.StaleMetadataException",
    // Consumer background-thread internals (package `...consumer.internals`).
    "org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException",
    // Share-consumer internals (KIP-932) — out of scope (consumer-threading.md §20).
    "org.apache.kafka.clients.consumer.internals.ShareFetchException",
    "org.apache.kafka.clients.consumer.internals.ShareInFlightBatchException",
    // Internal network-layer signalling, not a public client error.
    "org.apache.kafka.common.network.DelayedResponseAuthenticationException",
    // OAuth Bearer security-mechanism plugin errors — not the public client surface.
    "org.apache.kafka.common.security.oauthbearer.JwtRetrieverException",
    "org.apache.kafka.common.security.oauthbearer.JwtValidatorException",
    "org.apache.kafka.common.security.oauthbearer.internals.secured.UnretryableException",
    "org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerConfigException",
    "org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException",
];

/// `(ffi_id_constant, java_class_fqn)` — one row per concrete (non-abstract) Java
/// exception class, i.e. one row per FFI id (all 162 enum ids except `NONE`, which
/// is the no-error sentinel; `UNKNOWN_SERVER_ERROR` is `UnknownServerException`).
/// This is the reviewed correspondence between the two vocabularies (see the
/// module doc); everything else the generator derives from the Java source.
const BRIDGE: &[(&str, &str)] = &[
    ("API", "org.apache.kafka.common.errors.ApiException"),
    ("AUTHENTICATION", "org.apache.kafka.common.errors.AuthenticationException"),
    ("AUTHORIZATION", "org.apache.kafka.common.errors.AuthorizationException"),
    (
        "AUTHORIZER_NOT_READY",
        "org.apache.kafka.common.errors.AuthorizerNotReadyException",
    ),
    (
        "BROKER_ID_NOT_REGISTERED",
        "org.apache.kafka.common.errors.BrokerIdNotRegisteredException",
    ),
    (
        "BROKER_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.BrokerNotAvailableException",
    ),
    (
        "CLUSTER_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.ClusterAuthorizationException",
    ),
    (
        "CONCURRENT_TRANSACTIONS",
        "org.apache.kafka.common.errors.ConcurrentTransactionsException",
    ),
    ("CONFIG", "org.apache.kafka.common.config.ConfigException"),
    (
        "CONSUMER_COMMIT_FAILED",
        "org.apache.kafka.clients.consumer.CommitFailedException",
    ),
    (
        "CONSUMER_LOG_TRUNCATION",
        "org.apache.kafka.clients.consumer.LogTruncationException",
    ),
    (
        "CONSUMER_NO_OFFSET_FOR_PARTITION",
        "org.apache.kafka.clients.consumer.NoOffsetForPartitionException",
    ),
    (
        "CONSUMER_OFFSET_OUT_OF_RANGE",
        "org.apache.kafka.clients.consumer.OffsetOutOfRangeException",
    ),
    (
        "CONSUMER_RETRIABLE_COMMIT_FAILED",
        "org.apache.kafka.clients.consumer.RetriableCommitFailedException",
    ),
    (
        "COORDINATOR_LOAD_IN_PROGRESS",
        "org.apache.kafka.common.errors.CoordinatorLoadInProgressException",
    ),
    (
        "COORDINATOR_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.CoordinatorNotAvailableException",
    ),
    (
        "CORRELATION_ID_MISMATCH",
        "org.apache.kafka.common.requests.CorrelationIdMismatchException",
    ),
    ("CORRUPT_MESSAGE", "org.apache.kafka.common.errors.CorruptRecordException"),
    (
        "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.DelegationTokenAuthorizationException",
    ),
    (
        "DELEGATION_TOKEN_AUTH_DISABLED",
        "org.apache.kafka.common.errors.DelegationTokenDisabledException",
    ),
    (
        "DELEGATION_TOKEN_EXPIRED",
        "org.apache.kafka.common.errors.DelegationTokenExpiredException",
    ),
    (
        "DELEGATION_TOKEN_NOT_FOUND",
        "org.apache.kafka.common.errors.DelegationTokenNotFoundException",
    ),
    (
        "DELEGATION_TOKEN_OWNER_MISMATCH",
        "org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException",
    ),
    (
        "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
        "org.apache.kafka.common.errors.UnsupportedByAuthenticationException",
    ),
    ("DISCONNECT", "org.apache.kafka.common.errors.DisconnectException"),
    (
        "DUPLICATE_BROKER_REGISTRATION",
        "org.apache.kafka.common.errors.DuplicateBrokerRegistrationException",
    ),
    (
        "DUPLICATE_RESOURCE",
        "org.apache.kafka.common.errors.DuplicateResourceException",
    ),
    (
        "DUPLICATE_SEQUENCE_NUMBER",
        "org.apache.kafka.common.errors.DuplicateSequenceException",
    ),
    ("DUPLICATE_VOTER", "org.apache.kafka.common.errors.DuplicateVoterException"),
    (
        "ELECTION_NOT_NEEDED",
        "org.apache.kafka.common.errors.ElectionNotNeededException",
    ),
    (
        "ELIGIBLE_LEADERS_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.EligibleLeadersNotAvailableException",
    ),
    (
        "FEATURE_UPDATE_FAILED",
        "org.apache.kafka.common.errors.FeatureUpdateFailedException",
    ),
    ("FENCED_INSTANCE_ID", "org.apache.kafka.common.errors.FencedInstanceIdException"),
    (
        "FENCED_LEADER_EPOCH",
        "org.apache.kafka.common.errors.FencedLeaderEpochException",
    ),
    (
        "FENCED_MEMBER_EPOCH",
        "org.apache.kafka.common.errors.FencedMemberEpochException",
    ),
    ("FENCED_STATE_EPOCH", "org.apache.kafka.common.errors.FencedStateEpochException"),
    (
        "FETCH_SESSION_ID_NOT_FOUND",
        "org.apache.kafka.common.errors.FetchSessionIdNotFoundException",
    ),
    (
        "FETCH_SESSION_TOPIC_ID_ERROR",
        "org.apache.kafka.common.errors.FetchSessionTopicIdException",
    ),
    (
        "GROUP_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.GroupAuthorizationException",
    ),
    ("GROUP_ID_NOT_FOUND", "org.apache.kafka.common.errors.GroupIdNotFoundException"),
    (
        "GROUP_MAX_SIZE_REACHED",
        "org.apache.kafka.common.errors.GroupMaxSizeReachedException",
    ),
    (
        "GROUP_SUBSCRIBED_TO_TOPIC",
        "org.apache.kafka.common.errors.GroupSubscribedToTopicException",
    ),
    (
        "ILLEGAL_GENERATION",
        "org.apache.kafka.common.errors.IllegalGenerationException",
    ),
    ("ILLEGAL_SASL_STATE", "org.apache.kafka.common.errors.IllegalSaslStateException"),
    (
        "INCONSISTENT_CLUSTER_ID",
        "org.apache.kafka.common.errors.InconsistentClusterIdException",
    ),
    (
        "INCONSISTENT_GROUP_PROTOCOL",
        "org.apache.kafka.common.errors.InconsistentGroupProtocolException",
    ),
    (
        "INCONSISTENT_TOPIC_ID",
        "org.apache.kafka.common.errors.InconsistentTopicIdException",
    ),
    (
        "INCONSISTENT_VOTER_SET",
        "org.apache.kafka.common.errors.InconsistentVoterSetException",
    ),
    (
        "INELIGIBLE_REPLICA",
        "org.apache.kafka.common.errors.IneligibleReplicaException",
    ),
    ("INTERRUPT", "org.apache.kafka.common.errors.InterruptException"),
    (
        "INVALID_COMMIT_OFFSET_SIZE",
        "org.apache.kafka.common.errors.InvalidCommitOffsetSizeException",
    ),
    ("INVALID_CONFIG", "org.apache.kafka.common.errors.InvalidConfigurationException"),
    (
        "INVALID_FETCH_SESSION_EPOCH",
        "org.apache.kafka.common.errors.InvalidFetchSessionEpochException",
    ),
    ("INVALID_FETCH_SIZE", "org.apache.kafka.common.errors.InvalidFetchSizeException"),
    ("INVALID_GROUP_ID", "org.apache.kafka.common.errors.InvalidGroupIdException"),
    ("INVALID_OFFSET", "org.apache.kafka.common.errors.InvalidOffsetException"),
    (
        "INVALID_PARTITIONS",
        "org.apache.kafka.common.errors.InvalidPartitionsException",
    ),
    (
        "INVALID_PRINCIPAL_TYPE",
        "org.apache.kafka.common.errors.InvalidPrincipalTypeException",
    ),
    (
        "INVALID_PRODUCER_EPOCH",
        "org.apache.kafka.common.errors.InvalidProducerEpochException",
    ),
    (
        "INVALID_PRODUCER_ID_MAPPING",
        "org.apache.kafka.common.errors.InvalidPidMappingException",
    ),
    ("INVALID_RECEIVE", "org.apache.kafka.common.network.InvalidReceiveException"),
    (
        "INVALID_RECORD_STATE",
        "org.apache.kafka.common.errors.InvalidRecordStateException",
    ),
    ("INVALID_RECORD", "org.apache.kafka.common.InvalidRecordException"),
    (
        "INVALID_REGISTRATION",
        "org.apache.kafka.common.errors.InvalidRegistrationException",
    ),
    (
        "INVALID_REGULAR_EXPRESSION",
        "org.apache.kafka.common.errors.InvalidRegularExpression",
    ),
    (
        "INVALID_REPLICATION_FACTOR",
        "org.apache.kafka.common.errors.InvalidReplicationFactorException",
    ),
    (
        "INVALID_REPLICA_ASSIGNMENT",
        "org.apache.kafka.common.errors.InvalidReplicaAssignmentException",
    ),
    ("INVALID_REQUEST", "org.apache.kafka.common.errors.InvalidRequestException"),
    (
        "INVALID_REQUIRED_ACKS",
        "org.apache.kafka.common.errors.InvalidRequiredAcksException",
    ),
    (
        "INVALID_SESSION_TIMEOUT",
        "org.apache.kafka.common.errors.InvalidSessionTimeoutException",
    ),
    (
        "INVALID_SHARE_SESSION_EPOCH",
        "org.apache.kafka.common.errors.InvalidShareSessionEpochException",
    ),
    ("INVALID_TIMESTAMP", "org.apache.kafka.common.errors.InvalidTimestampException"),
    ("INVALID_TOPIC_ERROR", "org.apache.kafka.common.errors.InvalidTopicException"),
    (
        "INVALID_TRANSACTION_TIMEOUT",
        "org.apache.kafka.common.errors.InvalidTxnTimeoutException",
    ),
    ("INVALID_TXN_STATE", "org.apache.kafka.common.errors.InvalidTxnStateException"),
    (
        "INVALID_UPDATE_VERSION",
        "org.apache.kafka.common.errors.InvalidUpdateVersionException",
    ),
    ("INVALID_VOTER_KEY", "org.apache.kafka.common.errors.InvalidVoterKeyException"),
    ("KAFKA_STORAGE_ERROR", "org.apache.kafka.common.errors.KafkaStorageException"),
    (
        "LEADER_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.LeaderNotAvailableException",
    ),
    ("LISTENER_NOT_FOUND", "org.apache.kafka.common.errors.ListenerNotFoundException"),
    ("LOCAL_CONCURRENT_MODIFICATION", "java.util.ConcurrentModificationException"),
    ("LOCAL_ILLEGAL_ARGUMENT", "java.lang.IllegalArgumentException"),
    ("LOCAL_ILLEGAL_STATE", "java.lang.IllegalStateException"),
    ("LOCAL_TIMEOUT", "java.util.concurrent.TimeoutException"),
    ("LOG_DIR_NOT_FOUND", "org.apache.kafka.common.errors.LogDirNotFoundException"),
    ("MEMBER_ID_REQUIRED", "org.apache.kafka.common.errors.MemberIdRequiredException"),
    ("MESSAGE_TOO_LARGE", "org.apache.kafka.common.errors.RecordTooLargeException"),
    (
        "MISMATCHED_ENDPOINT_TYPE",
        "org.apache.kafka.common.errors.MismatchedEndpointTypeException",
    ),
    ("NETWORK_ERROR", "org.apache.kafka.common.errors.NetworkException"),
    ("NEW_LEADER_ELECTED", "org.apache.kafka.common.errors.NewLeaderElectedException"),
    ("NON_EMPTY_GROUP", "org.apache.kafka.common.errors.GroupNotEmptyException"),
    ("NOT_CONTROLLER", "org.apache.kafka.common.errors.NotControllerException"),
    ("NOT_COORDINATOR", "org.apache.kafka.common.errors.NotCoordinatorException"),
    (
        "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        "org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException",
    ),
    (
        "NOT_ENOUGH_REPLICAS",
        "org.apache.kafka.common.errors.NotEnoughReplicasException",
    ),
    (
        "NOT_LEADER_OR_FOLLOWER",
        "org.apache.kafka.common.errors.NotLeaderOrFollowerException",
    ),
    (
        "NO_REASSIGNMENT_IN_PROGRESS",
        "org.apache.kafka.common.errors.NoReassignmentInProgressException",
    ),
    (
        "OFFSET_METADATA_TOO_LARGE",
        "org.apache.kafka.common.errors.OffsetMetadataTooLarge",
    ),
    (
        "OFFSET_MOVED_TO_TIERED_STORAGE",
        "org.apache.kafka.common.errors.OffsetMovedToTieredStorageException",
    ),
    (
        "OFFSET_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.OffsetNotAvailableException",
    ),
    (
        "OFFSET_OUT_OF_RANGE",
        "org.apache.kafka.common.errors.OffsetOutOfRangeException",
    ),
    (
        "OPERATION_NOT_ATTEMPTED",
        "org.apache.kafka.common.errors.OperationNotAttemptedException",
    ),
    (
        "OUT_OF_ORDER_SEQUENCE_NUMBER",
        "org.apache.kafka.common.errors.OutOfOrderSequenceException",
    ),
    ("POLICY_VIOLATION", "org.apache.kafka.common.errors.PolicyViolationException"),
    (
        "POSITION_OUT_OF_RANGE",
        "org.apache.kafka.common.errors.PositionOutOfRangeException",
    ),
    (
        "PREFERRED_LEADER_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.PreferredLeaderNotAvailableException",
    ),
    (
        "PRINCIPAL_DESERIALIZATION_FAILURE",
        "org.apache.kafka.common.errors.PrincipalDeserializationException",
    ),
    (
        "PRODUCER_BUFFER_EXHAUSTED",
        "org.apache.kafka.clients.producer.BufferExhaustedException",
    ),
    ("PRODUCER_FENCED", "org.apache.kafka.common.errors.ProducerFencedException"),
    ("QUOTA_VIOLATION", "org.apache.kafka.common.metrics.QuotaViolationException"),
    (
        "REASSIGNMENT_IN_PROGRESS",
        "org.apache.kafka.common.errors.ReassignmentInProgressException",
    ),
    (
        "REBALANCE_IN_PROGRESS",
        "org.apache.kafka.common.errors.RebalanceInProgressException",
    ),
    (
        "REBOOTSTRAP_REQUIRED",
        "org.apache.kafka.common.errors.RebootstrapRequiredException",
    ),
    (
        "RECORD_DESERIALIZATION",
        "org.apache.kafka.common.errors.RecordDeserializationException",
    ),
    (
        "RECORD_LIST_TOO_LARGE",
        "org.apache.kafka.common.errors.RecordBatchTooLargeException",
    ),
    (
        "REPLICA_NOT_AVAILABLE",
        "org.apache.kafka.common.errors.ReplicaNotAvailableException",
    ),
    ("REQUEST_TIMED_OUT", "org.apache.kafka.common.errors.TimeoutException"),
    ("RESOURCE_NOT_FOUND", "org.apache.kafka.common.errors.ResourceNotFoundException"),
    (
        "SASL_AUTHENTICATION_FAILED",
        "org.apache.kafka.common.errors.SaslAuthenticationException",
    ),
    ("SCHEMA", "org.apache.kafka.common.protocol.types.SchemaException"),
    ("SECURITY_DISABLED", "org.apache.kafka.common.errors.SecurityDisabledException"),
    ("SERIALIZATION", "org.apache.kafka.common.errors.SerializationException"),
    (
        "SHARE_SESSION_LIMIT_REACHED",
        "org.apache.kafka.common.errors.ShareSessionLimitReachedException",
    ),
    (
        "SHARE_SESSION_NOT_FOUND",
        "org.apache.kafka.common.errors.ShareSessionNotFoundException",
    ),
    ("SNAPSHOT_NOT_FOUND", "org.apache.kafka.common.errors.SnapshotNotFoundException"),
    (
        "SSL_AUTHENTICATION",
        "org.apache.kafka.common.errors.SslAuthenticationException",
    ),
    ("STALE_BROKER_EPOCH", "org.apache.kafka.common.errors.StaleBrokerEpochException"),
    (
        "STALE_CONTROLLER_EPOCH",
        "org.apache.kafka.common.errors.ControllerMovedException",
    ),
    ("STALE_MEMBER_EPOCH", "org.apache.kafka.common.errors.StaleMemberEpochException"),
    (
        "STREAMS_INVALID_TOPOLOGY_EPOCH",
        "org.apache.kafka.common.errors.StreamsInvalidTopologyEpochException",
    ),
    (
        "STREAMS_INVALID_TOPOLOGY",
        "org.apache.kafka.common.errors.StreamsInvalidTopologyException",
    ),
    (
        "STREAMS_TOPOLOGY_FENCED",
        "org.apache.kafka.common.errors.StreamsTopologyFencedException",
    ),
    (
        "TELEMETRY_TOO_LARGE",
        "org.apache.kafka.common.errors.TelemetryTooLargeException",
    ),
    (
        "THROTTLING_QUOTA_EXCEEDED",
        "org.apache.kafka.common.errors.ThrottlingQuotaExceededException",
    ),
    ("TOPIC_ALREADY_EXISTS", "org.apache.kafka.common.errors.TopicExistsException"),
    (
        "TOPIC_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.TopicAuthorizationException",
    ),
    (
        "TOPIC_DELETION_DISABLED",
        "org.apache.kafka.common.errors.TopicDeletionDisabledException",
    ),
    (
        "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        "org.apache.kafka.common.errors.TransactionalIdAuthorizationException",
    ),
    (
        "TRANSACTIONAL_ID_NOT_FOUND",
        "org.apache.kafka.common.errors.TransactionalIdNotFoundException",
    ),
    (
        "TRANSACTION_ABORTABLE",
        "org.apache.kafka.common.errors.TransactionAbortableException",
    ),
    (
        "TRANSACTION_ABORTED",
        "org.apache.kafka.common.errors.TransactionAbortedException",
    ),
    (
        "TRANSACTION_COORDINATOR_FENCED",
        "org.apache.kafka.common.errors.TransactionCoordinatorFencedException",
    ),
    (
        "UNACCEPTABLE_CREDENTIAL",
        "org.apache.kafka.common.errors.UnacceptableCredentialException",
    ),
    (
        "UNKNOWN_CONTROLLER_ID",
        "org.apache.kafka.common.errors.UnknownControllerIdException",
    ),
    (
        "UNKNOWN_LEADER_EPOCH",
        "org.apache.kafka.common.errors.UnknownLeaderEpochException",
    ),
    ("UNKNOWN_MEMBER_ID", "org.apache.kafka.common.errors.UnknownMemberIdException"),
    (
        "UNKNOWN_PRODUCER_ID",
        "org.apache.kafka.common.errors.UnknownProducerIdException",
    ),
    ("UNKNOWN_SERVER_ERROR", "org.apache.kafka.common.errors.UnknownServerException"),
    (
        "UNKNOWN_SUBSCRIPTION_ID",
        "org.apache.kafka.common.errors.UnknownSubscriptionIdException",
    ),
    ("UNKNOWN_TOPIC_ID", "org.apache.kafka.common.errors.UnknownTopicIdException"),
    (
        "UNKNOWN_TOPIC_OR_PARTITION",
        "org.apache.kafka.common.errors.UnknownTopicOrPartitionException",
    ),
    (
        "UNRELEASED_INSTANCE_ID",
        "org.apache.kafka.common.errors.UnreleasedInstanceIdException",
    ),
    (
        "UNSTABLE_OFFSET_COMMIT",
        "org.apache.kafka.common.errors.UnstableOffsetCommitException",
    ),
    (
        "UNSUPPORTED_ASSIGNOR",
        "org.apache.kafka.common.errors.UnsupportedAssignorException",
    ),
    (
        "UNSUPPORTED_COMPRESSION_TYPE",
        "org.apache.kafka.common.errors.UnsupportedCompressionTypeException",
    ),
    (
        "UNSUPPORTED_ENDPOINT_TYPE",
        "org.apache.kafka.common.errors.UnsupportedEndpointTypeException",
    ),
    (
        "UNSUPPORTED_FOR_MESSAGE_FORMAT",
        "org.apache.kafka.common.errors.UnsupportedForMessageFormatException",
    ),
    (
        "UNSUPPORTED_SASL_MECHANISM",
        "org.apache.kafka.common.errors.UnsupportedSaslMechanismException",
    ),
    (
        "UNSUPPORTED_VERSION",
        "org.apache.kafka.common.errors.UnsupportedVersionException",
    ),
    ("VOTER_NOT_FOUND", "org.apache.kafka.common.errors.VoterNotFoundException"),
    ("WAKEUP", "org.apache.kafka.common.errors.WakeupException"),
];

/// Java's `KafkaException`: the base of the Kafka side, `KafkaError` in Python.
const KAFKA_EXCEPTION_FQN: &str = "org.apache.kafka.common.KafkaException";

/// The Java built-in exceptions of the Types table (CLAUDE.md, Python Binding
/// Conventions, Types): FQN, parent FQN, FFI id constant (`""` when the core
/// models none) and the constructors as `(type, name)` lists. The JDK source is
/// not in the tree; the constructors are Java 11's, the release Kafka's clients
/// compile for (`kafka/build.gradle`: `minClientJavaVersion = 11`).
type JdkCtor = &'static [(&'static str, &'static str)];
const JDK_FOUR: &[JdkCtor] = &[
    &[],
    &[("String", "s")],
    &[("String", "message"), ("Throwable", "cause")],
    &[("Throwable", "cause")],
];
const JDK_BUILTINS: &[(&str, &str, &str, &[JdkCtor])] = &[
    (
        "java.lang.IllegalStateException",
        "java.lang.RuntimeException",
        "LOCAL_ILLEGAL_STATE",
        JDK_FOUR,
    ),
    (
        "java.lang.IllegalArgumentException",
        "java.lang.RuntimeException",
        "LOCAL_ILLEGAL_ARGUMENT",
        JDK_FOUR,
    ),
    (
        "java.util.ConcurrentModificationException",
        "java.lang.RuntimeException",
        "LOCAL_CONCURRENT_MODIFICATION",
        &[
            &[],
            &[("String", "message")],
            &[("Throwable", "cause")],
            &[("String", "message"), ("Throwable", "cause")],
        ],
    ),
    (
        "java.util.concurrent.TimeoutException",
        "java.lang.Exception",
        "LOCAL_TIMEOUT",
        &[&[], &[("String", "message")]],
    ),
    (
        "java.util.NoSuchElementException",
        "java.lang.RuntimeException",
        "",
        &[&[], &[("String", "s")]],
    ),
    (
        "java.lang.NullPointerException",
        "java.lang.RuntimeException",
        "",
        &[&[], &[("String", "s")]],
    ),
];

/// JDK classes at the top of every chain: `Throwable`'s constructors decide the
/// message and the cause.
const TERMINAL_BASES: &[&str] = &[
    "java.lang.RuntimeException",
    "java.lang.Exception",
    "java.lang.Throwable",
];

/// Python packages that also hold hand-written types: their `__init__.py` is
/// hand-written, and the generator owns only the marked block of error imports.
const MIXED_PACKAGES: &[&str] = &[
    "confluent_kafka",
    "confluent_kafka.common",
    "confluent_kafka.common.config",
    "confluent_kafka.consumer",
    "confluent_kafka.producer",
];

const BLOCK_BEGIN: &str = "# BEGIN GENERATED ERRORS (cargo xtask generate-error-codes; do not edit)";
const BLOCK_END: &str = "# END GENERATED ERRORS";

// ---------------------------------------------------------------------------
// The class graph
// ---------------------------------------------------------------------------

/// One class in the generated hierarchy.
#[derive(Clone, Debug)]
struct ClassInfo {
    java_fqn: String,
    py_name: String,
    /// `(constant name, value)`; `None` for the abstract classes and the two
    /// built-ins the core does not model.
    ffi_id: Option<(String, i32)>,
    is_abstract: bool,
    parent_fqn: String,
    /// The Python module the class lives in (`confluent_kafka.common.errors`).
    py_module: String,
    java: JavaClass,
    is_jdk: bool,
}

impl ClassInfo {
    fn file_stem(&self) -> String {
        snake(&self.py_name)
    }

    fn module_file(&self) -> String {
        format!("{}.{}", self.py_module, self.file_stem())
    }
}

/// `CamelCase` -> `snake_case` (`TopicAuthorizationError` ->
/// `topic_authorization_error`, `throttleTimeMs` -> `throttle_time_ms`).
fn snake(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            let prev_lower = i > 0 && (chars[i - 1].is_lowercase() || chars[i - 1].is_ascii_digit());
            let next_lower = i + 1 < chars.len() && chars[i + 1].is_lowercase();
            let prev_upper = i > 0 && chars[i - 1].is_uppercase();
            if i > 0 && (prev_lower || (prev_upper && next_lower)) {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// The Python class name for a Java simple class name: "a class name's
/// `Exception` suffix becomes `Error`, nothing else changes" (CLAUDE.md,
/// Python Binding Conventions, Idiom translations), so a name without the
/// suffix is kept (`InvalidRegularExpression`, `OffsetMetadataTooLarge`).
fn python_name(java_simple: &str) -> String {
    match java_simple.strip_suffix("Exception") {
        Some(stem) => format!("{stem}Error"),
        None => java_simple.to_string(),
    }
}

/// The Python module of a Java package: `clients` dropped, `java.*` at the root.
fn py_module_for(package: &str) -> String {
    if package.starts_with("java.") {
        return "confluent_kafka".to_string();
    }
    let rest = package.strip_prefix("org.apache.kafka.").unwrap_or(package);
    let rest = rest.strip_prefix("clients.").unwrap_or(rest);
    format!("confluent_kafka.{rest}")
}

fn java_path(repo_root: &Path, fqn: &str) -> PathBuf {
    let mut p = repo_root.join(JAVA_ROOT);
    for seg in fqn.split('.') {
        p.push(seg);
    }
    p.set_extension("java");
    p
}

fn resolve_parent(jc: &JavaClass) -> String {
    if let Some(fqn) = jc.imports.get(&jc.parent_simple) {
        return fqn.clone();
    }
    if matches!(
        jc.parent_simple.as_str(),
        "RuntimeException" | "Exception" | "Throwable" | "IllegalStateException" | "IllegalArgumentException"
    ) {
        return format!("java.lang.{}", jc.parent_simple);
    }
    format!("{}.{}", jc.package, jc.parent_simple)
}

fn jdk_class(fqn: &str, parent: &str, ctors: &[JdkCtor]) -> JavaClass {
    let (package, simple) = fqn.rsplit_once('.').unwrap();
    JavaClass {
        package: package.to_string(),
        simple: simple.to_string(),
        is_abstract: false,
        parent_simple: parent.rsplit('.').next().unwrap().to_string(),
        imports: BTreeMap::new(),
        javadoc: String::new(),
        ctors: ctors
            .iter()
            .map(|params| Ctor {
                visibility: Visibility::Public,
                params: params
                    .iter()
                    .map(|(ty, name)| Param { ty: (*ty).to_string(), name: (*name).to_string() })
                    .collect(),
                call: Some((
                    CallKind::Super,
                    params.iter().map(|(_, name)| Expr::Name(vec![(*name).to_string()])).collect(),
                )),
                assigns: Vec::new(),
                deprecated: None,
            })
            .collect(),
        fields: Vec::new(),
        getters: Vec::new(),
        enums: Vec::new(),
        singletons: Vec::new(),
    }
}

/// Build the complete class graph and cross-check it against the FFI enum.
fn build_graph(repo_root: &Path) -> anyhow::Result<BTreeMap<String, ClassInfo>> {
    // 1. Cross-check the bridge against the FFI enum: every bridge id is a real
    //    enum constant, and every non-`NONE` enum constant is named by exactly
    //    one bridge row.
    let enum_id_values = parse_ffi_enum_ids(repo_root)?;
    let bridge_ids: BTreeSet<&str> = BRIDGE.iter().map(|(id, _)| *id).collect();
    if bridge_ids.len() != BRIDGE.len() {
        anyhow::bail!("error hierarchy: the BRIDGE table has a duplicate FFI id");
    }
    for (id, fqn) in BRIDGE {
        if !enum_id_values.contains_key(*id) {
            anyhow::bail!(
                "error hierarchy: BRIDGE names FFI id `{id}` (for `{fqn}`) that is not in \
                 kafka_common_ErrorCode_t ({FFI_ENUM_SOURCE})"
            );
        }
    }
    for id in enum_id_values.keys() {
        if id != "NONE" && !bridge_ids.contains(id.as_str()) {
            anyhow::bail!(
                "error hierarchy: FFI id `{id}` has no class in the BRIDGE table — every \
                 kafka_common_ErrorCode_t id except NONE must map to one Java class"
            );
        }
    }
    let mut fqn_to_id: BTreeMap<String, String> = BTreeMap::new();
    for (id, fqn) in BRIDGE {
        if fqn_to_id.insert((*fqn).to_string(), (*id).to_string()).is_some() {
            anyhow::bail!("error hierarchy: two FFI ids map to the same Java class `{fqn}`");
        }
    }

    let mut classes: BTreeMap<String, ClassInfo> = BTreeMap::new();

    // 2. The Java built-ins (root module), from the JDK constructor table.
    for (fqn, parent, id, ctors) in JDK_BUILTINS {
        let bridge_id = fqn_to_id.get(*fqn).cloned();
        let expected = (!id.is_empty()).then(|| (*id).to_string());
        if bridge_id != expected {
            anyhow::bail!(
                "error hierarchy: `{fqn}` has FFI id {bridge_id:?} in BRIDGE, JDK_BUILTINS says {expected:?}"
            );
        }
        let simple = fqn.rsplit('.').next().unwrap();
        classes.insert(
            (*fqn).to_string(),
            ClassInfo {
                java_fqn: (*fqn).to_string(),
                py_name: python_name(simple),
                ffi_id: expected.map(|i| {
                    let v = enum_id_values[&i];
                    (i, v)
                }),
                is_abstract: false,
                parent_fqn: (*parent).to_string(),
                py_module: "confluent_kafka".to_string(),
                java: jdk_class(fqn, parent, ctors),
                is_jdk: true,
            },
        );
    }
    for (_, fqn) in BRIDGE {
        if fqn.starts_with("java.") && !classes.contains_key(*fqn) {
            anyhow::bail!("error hierarchy: BRIDGE names `{fqn}`, which JDK_BUILTINS does not describe");
        }
    }

    // 3. Every Kafka class: the bridge classes, the abstract bases, KafkaException.
    let mut to_parse: BTreeSet<String> = BRIDGE
        .iter()
        .map(|(_, f)| (*f).to_string())
        .filter(|f| !f.starts_with("java."))
        .collect();
    for a in ABSTRACT_CLASSES {
        to_parse.insert((*a).to_string());
    }
    to_parse.insert(KAFKA_EXCEPTION_FQN.to_string());

    for fqn in &to_parse {
        let path = java_path(repo_root, fqn);
        let raw = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let mut jc = java_parse::parse_class(&raw).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        if jc.ctors.is_empty() {
            // Java's implicit public no-argument constructor.
            jc.ctors.push(Ctor {
                visibility: Visibility::Public,
                params: Vec::new(),
                call: None,
                assigns: Vec::new(),
                deprecated: None,
            });
        }
        let id = if fqn == KAFKA_EXCEPTION_FQN {
            // A bare KafkaException is reported as UNKNOWN_SERVER_ERROR, which
            // UnknownServerError owns in the id table.
            Some(("UNKNOWN_SERVER_ERROR".to_string(), enum_id_values["UNKNOWN_SERVER_ERROR"]))
        } else {
            fqn_to_id.get(fqn).map(|i| (i.clone(), enum_id_values[i]))
        };
        if jc.is_abstract && id.is_some() {
            anyhow::bail!(
                "error hierarchy: `{fqn}` is Java-abstract yet the BRIDGE gives it an FFI id — abstract \
                 classes are catch-only and carry no id"
            );
        }
        if !jc.is_abstract && id.is_none() {
            anyhow::bail!(
                "error hierarchy: `{fqn}` is a concrete Java class with no FFI id — every concrete \
                 exception class must appear in the BRIDGE"
            );
        }
        let simple = fqn.rsplit('.').next().unwrap();
        classes.insert(
            fqn.clone(),
            ClassInfo {
                java_fqn: fqn.clone(),
                py_name: python_name(simple),
                ffi_id: id,
                is_abstract: jc.is_abstract,
                parent_fqn: resolve_parent(&jc),
                py_module: py_module_for(&jc.package),
                java: jc,
                is_jdk: false,
            },
        );
    }

    // 4. Every parent is a generated class or a terminal JDK base.
    for info in classes.values() {
        if !classes.contains_key(&info.parent_fqn) && !TERMINAL_BASES.contains(&info.parent_fqn.as_str()) {
            anyhow::bail!(
                "error hierarchy: `{}` extends `{}`, which is neither generated nor a JDK base — extend the \
                 generator",
                info.java_fqn,
                info.parent_fqn
            );
        }
    }

    // 5. Fail-closed universe check: every in-scope Java exception class is
    //    accounted for, and the abstract set is Java's `abstract` modifier.
    validate_exception_scan(repo_root, &classes)?;
    Ok(classes)
}

// ---------------------------------------------------------------------------
// Static evaluation of the constructor chains
// ---------------------------------------------------------------------------

/// A Python value produced by translating a Java expression: its source text,
/// its Java type (`None` for `null`), and — when it is a constant — the Python
/// text of the Java-given default it stands for (`Collections.emptySet()` is
/// the value `set()` and the default `()`).
#[derive(Clone, Debug, PartialEq)]
struct PyVal {
    expr: String,
    ty: Option<String>,
    konst: Option<String>,
}

impl PyVal {
    fn param(expr: &str, ty: &str) -> PyVal {
        PyVal { expr: expr.to_string(), ty: Some(ty.to_string()), konst: None }
    }

    fn constant(expr: &str, ty: Option<&str>, default: &str) -> PyVal {
        PyVal {
            expr: expr.to_string(),
            ty: ty.map(str::to_string),
            konst: Some(default.to_string()),
        }
    }
}

/// What a constructor does: the message and cause it gives `Throwable`, and the
/// fields it sets (Python attribute name -> value), over the whole chain.
#[derive(Clone, Debug, PartialEq)]
struct Effect {
    message: String,
    cause: String,
    fields: BTreeMap<String, PyVal>,
}

type Env = BTreeMap<String, PyVal>;

fn attr_name(field: &str) -> String {
    format!("_{}", snake(field))
}

fn base_type(ty: &str) -> &str {
    ty.split('<').next().unwrap().trim()
}

fn is_primitive(ty: &str) -> bool {
    matches!(ty, "int" | "long" | "short" | "byte" | "double" | "float" | "boolean" | "char")
}

fn throwable_like(ty: &str) -> bool {
    let b = base_type(ty);
    matches!(b, "Throwable" | "Exception" | "RuntimeException") || b.ends_with("Exception")
}

fn header_type(ty: &str) -> bool {
    matches!(base_type(ty), "Headers") || ty == "Iterable<Header>"
}

fn compatible(arg: &Option<String>, param_ty: &str) -> bool {
    let p = base_type(param_ty);
    let Some(a) = arg else {
        return !is_primitive(p);
    };
    let a = base_type(a);
    a == p
        || p == "Object"
        || (p == "Throwable" && throwable_like(a))
        || (matches!(p, "Collection" | "Iterable") && matches!(a, "Set" | "List" | "Collection"))
        || (p == "long" && a == "int")
        || (p == "double" && matches!(a, "int" | "long"))
}

fn py_str_literal(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Qualified Java constants the exception sources use, as Python.
fn java_constant(name: &[String]) -> Option<PyVal> {
    match name.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["TimestampType", "NO_TIMESTAMP_TYPE"] => Some(PyVal::constant(
            "TimestampType.NO_TIMESTAMP_TYPE",
            Some("TimestampType"),
            "TimestampType.NO_TIMESTAMP_TYPE",
        )),
        // ConsumerRecord.NO_TIMESTAMP = RecordBatch.NO_TIMESTAMP = -1L.
        ["ConsumerRecord", "NO_TIMESTAMP"] | ["RecordBatch", "NO_TIMESTAMP"] => {
            Some(PyVal::constant("-1", Some("long"), "-1"))
        },
        _ => None,
    }
}

struct Graph<'a> {
    classes: &'a BTreeMap<String, ClassInfo>,
}

impl Graph<'_> {
    fn class(&self, fqn: &str) -> anyhow::Result<&ClassInfo> {
        self.classes
            .get(fqn)
            .ok_or_else(|| anyhow::anyhow!("error hierarchy: no class `{fqn}` in the graph"))
    }

    /// The declared type of a field of `fqn` or one of its ancestors.
    fn field_type(&self, fqn: &str, field: &str) -> Option<String> {
        let mut cur = fqn.to_string();
        while let Some(info) = self.classes.get(&cur) {
            if let Some(f) = info.java.fields.iter().find(|f| f.name == field && !f.is_static) {
                return Some(f.ty.clone());
            }
            cur = info.parent_fqn.clone();
        }
        None
    }

    /// Every instance field of `fqn`'s chain: attribute -> Java type, ancestors
    /// first.
    fn chain_fields(&self, fqn: &str) -> Vec<(String, String)> {
        let mut chain = Vec::new();
        let mut cur = fqn.to_string();
        while let Some(info) = self.classes.get(&cur) {
            chain.push(info);
            cur = info.parent_fqn.clone();
        }
        let mut out: Vec<(String, String)> = Vec::new();
        for info in chain.iter().rev() {
            for f in info.java.fields.iter().filter(|f| !f.is_static) {
                let a = attr_name(&f.name);
                if !out.iter().any(|(x, _)| *x == a) {
                    out.push((a, f.ty.clone()));
                }
            }
        }
        out
    }

    fn translate(&self, e: &Expr, env: &Env) -> anyhow::Result<PyVal> {
        Ok(match e {
            Expr::Null => PyVal::constant("None", None, "None"),
            Expr::Bool(b) => {
                let t = if *b { "True" } else { "False" };
                PyVal::constant(t, Some("boolean"), t)
            },
            Expr::Int(n) => PyVal::constant(&n.to_string(), Some("int"), &n.to_string()),
            Expr::Str(s) => {
                let lit = py_str_literal(s);
                PyVal::constant(&lit, Some("String"), &lit)
            },
            Expr::Name(name) => {
                let name: Vec<String> = if name.first().map(String::as_str) == Some("this") {
                    name[1..].to_vec()
                } else {
                    name.clone()
                };
                if name.len() == 1 {
                    if let Some(v) = env.get(&name[0]) {
                        return Ok(v.clone());
                    }
                }
                java_constant(&name).ok_or_else(|| anyhow::anyhow!("unknown name `{}`", name.join(".")))?
            },
            Expr::Call(name, args) => {
                let vals: Vec<PyVal> = args.iter().map(|a| self.translate(a, env)).collect::<anyhow::Result<_>>()?;
                let n: Vec<&str> = name.iter().map(String::as_str).collect();
                match (n.as_slice(), vals.as_slice()) {
                    (["Collections", "emptySet"], []) => PyVal::constant("set()", Some("Set"), "()"),
                    (["Collections", "emptyList"], []) => PyVal::constant("[]", Some("List"), "()"),
                    (["Collections", "emptyMap"], []) => PyVal::constant("{}", Some("Map"), "None"),
                    (["Collections", "singleton"], [x]) => {
                        PyVal { expr: format!("{{{}}}", x.expr), ty: Some("Set".into()), konst: None }
                    },
                    (["Set", "copyOf"] | ["Collections", "unmodifiableSet"], [x]) => PyVal {
                        expr: format!("_throwable.copy_set({})", x.expr),
                        ty: Some("Set".into()),
                        konst: None,
                    },
                    (["Collections", "unmodifiableMap"] | ["Map", "copyOf"], [x]) => PyVal {
                        expr: format!("_throwable.copy_dict({})", x.expr),
                        ty: Some("Map".into()),
                        konst: None,
                    },
                    _ => anyhow::bail!("unsupported call `{}`", name.join(".")),
                }
            },
            Expr::New(ty, args) => match (ty.as_str(), args.as_slice()) {
                ("HashSet" | "LinkedHashSet", []) => PyVal::constant("set()", Some("Set"), "()"),
                ("HashMap", []) => PyVal::constant("{}", Some("Map"), "None"),
                // `new InterruptedException()`: Python has no such class and no
                // thread interruption; the cause is left empty.
                ("InterruptedException", []) => {
                    PyVal { expr: "None".into(), ty: Some("InterruptedException".into()), konst: None }
                },
                _ => anyhow::bail!("unsupported `new {ty}(...)`"),
            },
            Expr::Add(..) => {
                let mut parts = Vec::new();
                flatten_add(e, &mut parts);
                if !parts.iter().any(|p| matches!(p, Expr::Str(_))) {
                    anyhow::bail!("unsupported arithmetic `+`");
                }
                let mut out: Vec<String> = Vec::new();
                for p in parts {
                    let v = self.translate(p, env)?;
                    let text = match p {
                        Expr::Str(_) => v.expr,
                        Expr::Ternary(..) if v.ty.as_deref() == Some("String") => v.expr,
                        _ => format!("java_str({})", v.expr),
                    };
                    out.push(text);
                }
                PyVal { expr: out.join(" + "), ty: Some("String".into()), konst: None }
            },
            Expr::Eq(a, b) => {
                let (Expr::Name(_), Expr::Null) = (a.as_ref(), b.as_ref()) else {
                    anyhow::bail!("unsupported `==`")
                };
                let x = self.translate(a, env)?;
                PyVal { expr: format!("{} is None", x.expr), ty: Some("boolean".into()), konst: None }
            },
            Expr::Ternary(c, a, b) => {
                let c = self.translate(c, env)?;
                let a = self.translate(a, env)?;
                let b = self.translate(b, env)?;
                PyVal {
                    expr: format!("({} if {} else {})", a.expr, c.expr, b.expr),
                    ty: a.ty.clone().or(b.ty),
                    konst: None,
                }
            },
        })
    }

    /// The constructor of `fqn` a `this(...)` / `super(...)` call with `vals`
    /// selects, by arity and argument types (Java's overload resolution for the
    /// shapes the sources use).
    fn resolve(&self, fqn: &str, vals: &[PyVal], from_subclass: bool) -> anyhow::Result<usize> {
        let info = self.class(fqn)?;
        let candidates: Vec<usize> = info
            .java
            .ctors
            .iter()
            .enumerate()
            .filter(|(_, c)| !(from_subclass && c.visibility == Visibility::Private))
            .filter(|(_, c)| c.params.len() == vals.len())
            .filter(|(_, c)| c.params.iter().zip(vals).all(|(p, v)| compatible(&v.ty, &p.ty)))
            .map(|(i, _)| i)
            .collect();
        match candidates.as_slice() {
            [one] => Ok(*one),
            [] => anyhow::bail!("{fqn}: no constructor matches {} argument(s)", vals.len()),
            _ => {
                // Prefer the candidate whose parameter types match exactly.
                let exact: Vec<usize> = candidates
                    .iter()
                    .copied()
                    .filter(|i| {
                        info.java.ctors[*i]
                            .params
                            .iter()
                            .zip(vals)
                            .all(|(p, v)| v.ty.as_deref().map(base_type) == Some(base_type(&p.ty)))
                    })
                    .collect();
                match exact.as_slice() {
                    [one] => Ok(*one),
                    _ => anyhow::bail!("{fqn}: ambiguous constructor call with {} argument(s)", vals.len()),
                }
            },
        }
    }

    fn bind(&self, fqn: &str, idx: usize, vals: Vec<PyVal>) -> anyhow::Result<Env> {
        let ctor = &self.class(fqn)?.java.ctors[idx];
        let mut env = Env::new();
        for (p, mut v) in ctor.params.iter().zip(vals) {
            if v.ty.is_none() {
                v.ty = Some(p.ty.clone());
            }
            env.insert(p.name.clone(), v);
        }
        Ok(env)
    }

    /// Evaluate constructor `idx` of `fqn` with the arguments in `env`.
    fn eval(&self, fqn: &str, idx: usize, env: &Env, depth: usize) -> anyhow::Result<Effect> {
        if depth > 16 {
            anyhow::bail!("{fqn}: constructor chain too deep");
        }
        let info = self.class(fqn)?;
        let ctor = &info.java.ctors[idx];
        let mut eff = match &ctor.call {
            Some((CallKind::This, args)) => {
                let vals: Vec<PyVal> = args.iter().map(|a| self.translate(a, env)).collect::<anyhow::Result<_>>()?;
                let target = self.resolve(fqn, &vals, false)?;
                let env2 = self.bind(fqn, target, vals)?;
                self.eval(fqn, target, &env2, depth + 1)?
            },
            other => {
                let args = other.as_ref().map(|(_, a)| a.clone()).unwrap_or_default();
                let vals: Vec<PyVal> = args.iter().map(|a| self.translate(a, env)).collect::<anyhow::Result<_>>()?;
                let parent = info.parent_fqn.as_str();
                let mut e = if TERMINAL_BASES.contains(&parent) {
                    throwable_effect(&vals)?
                } else {
                    let target = self.resolve(parent, &vals, true)?;
                    let env2 = self.bind(parent, target, vals)?;
                    self.eval(parent, target, &env2, depth + 1)?
                };
                // Instance initializers run after `super(...)`.
                for f in info.java.fields.iter().filter(|f| !f.is_static) {
                    if let Some(init) = &f.init {
                        let v = self.translate(init, &Env::new())?;
                        e.fields.insert(attr_name(&f.name), v);
                    }
                }
                e
            },
        };
        for (field, value) in &ctor.assigns {
            let v = self.translate(value, env)?;
            let ty = self
                .field_type(fqn, field)
                .ok_or_else(|| anyhow::anyhow!("{fqn}: assignment to unknown field `{field}`"))?;
            eff.fields.insert(attr_name(field), convert_for_field(&ty, v));
        }
        Ok(eff)
    }
}

/// Whether two effects are the same, comparing values by their Python text
/// (a parameter's declared Java type does not matter once it is stored).
fn same_effect(a: &Effect, b: &Effect) -> bool {
    a.message == b.message
        && a.cause == b.cause
        && a.fields.len() == b.fields.len()
        && a.fields
            .iter()
            .all(|(k, v)| b.fields.get(k).is_some_and(|w| w.expr == v.expr && w.konst == v.konst))
}

fn flatten_add<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    if let Expr::Add(a, b) = e {
        flatten_add(a, out);
        flatten_add(b, out);
    } else {
        out.push(e);
    }
}

/// `Throwable`'s four constructors: the message and the cause they record.
fn throwable_effect(vals: &[PyVal]) -> anyhow::Result<Effect> {
    let (message, cause) = match vals {
        [] => ("None".to_string(), "None".to_string()),
        [one] if one.ty.as_deref().is_some_and(throwable_like) => {
            // Throwable(Throwable cause): message = cause == null ? null : cause.toString()
            (format!("_throwable.cause_message({})", one.expr), one.expr.clone())
        },
        [one] => (one.expr.clone(), "None".to_string()),
        [m, c] => (m.expr.clone(), c.expr.clone()),
        _ => anyhow::bail!("Throwable has no constructor with {} arguments", vals.len()),
    };
    Ok(Effect { message, cause, fields: BTreeMap::new() })
}

/// Store a value in a field of Java type `ty`: collections and buffers are
/// copied into their output form (a `set`, a `dict`, a `memoryview`, header
/// tuples) unless the value is a constant.
fn convert_for_field(ty: &str, v: PyVal) -> PyVal {
    if v.konst.is_some() || v.expr.starts_with("_throwable.") || v.expr.starts_with('{') {
        return v;
    }
    let wrap = match base_type(ty) {
        "Set" | "Collection" => Some("copy_set"),
        "Map" => Some("copy_dict"),
        "ByteBuffer" => Some("view"),
        _ if header_type(ty) => Some("headers"),
        _ => None,
    };
    match wrap {
        Some(w) => PyVal { expr: format!("_throwable.{w}({})", v.expr), ..v },
        None => v,
    }
}

/// The parameter a field value stores, if the value is a (converted) parameter.
fn stored_param(expr: &str) -> &str {
    for w in ["copy_set", "copy_dict", "view", "headers"] {
        if let Some(inner) = expr.strip_prefix(&format!("_throwable.{w}(")).and_then(|s| s.strip_suffix(')')) {
            return inner;
        }
    }
    expr
}

// ---------------------------------------------------------------------------
// The Python model of a class's constructors (CLAUDE.md, Signatures, Errors)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Dflt {
    Required,
    NoneDefault,
    Unset,
    Const(String),
}

impl Dflt {
    fn python(&self) -> String {
        match self {
            Dflt::Required => String::new(),
            Dflt::NoneDefault => "None".into(),
            Dflt::Unset => "UNSET".into(),
            Dflt::Const(c) => c.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct PyParam {
    name: String,
    java_ty: String,
    default: Dflt,
    /// Java's null is one of its Java-given defaults, so the implementation
    /// signature types it `T | None` even when its default is `UNSET`.
    nullable: bool,
    /// Its Java-given default `null` is a field default (a shorter
    /// constructor assigns null to the field this parameter fills), so the
    /// stubs of the forms that require it take `T | None` too.
    field_nullable: bool,
}

#[derive(Clone, Debug)]
struct PyForm {
    /// Index into the class's Java constructors.
    ctor: usize,
    params: Vec<String>,
    /// The Java-given defaults `java_forms` fills: values a shorter
    /// constructor passes to this one through `this(...)`.
    defaults: BTreeMap<String, String>,
    /// Signature defaults only: constants a shorter constructor assigns to the
    /// fields this one fills from parameters. They give a parameter its
    /// default value but never let `java_forms` leave it out (CLAUDE.md,
    /// Python Binding Conventions, Signatures: "Only such values are O's
    /// Java-given defaults").
    field_defaults: BTreeMap<String, String>,
    deprecated: Option<String>,
    effect: Effect,
}

struct Selection {
    form: usize,
    fills: Vec<(String, String)>,
}

struct PyModel {
    params: Vec<PyParam>,
    forms: Vec<PyForm>,
    decorated: bool,
    /// Form indices a call can select, in priority order (most parameters,
    /// then declaration order).
    reachable: Vec<usize>,
    /// `@overload` stubs: (parameter names, optional names), in stub order.
    stubs: Vec<(Vec<String>, BTreeSet<String>)>,
}

fn surface_ctors(info: &ClassInfo) -> Vec<usize> {
    info.java
        .ctors
        .iter()
        .enumerate()
        .filter(|(_, c)| {
            c.visibility == Visibility::Public || (info.is_abstract && c.visibility == Visibility::Protected)
        })
        .map(|(i, _)| i)
        .collect()
}

fn placeholder(name: &str) -> String {
    format!("\u{a7}{name}\u{a7}")
}

/// Merge the forms' parameter orders: each keeps its own order; where that
/// leaves a choice, the parameter of the earliest-declared form goes first.
fn merge_order(forms: &[Vec<String>]) -> anyhow::Result<Vec<String>> {
    let mut all: Vec<String> = Vec::new();
    for f in forms {
        for p in f {
            if !all.contains(p) {
                all.push(p.clone());
            }
        }
    }
    let mut placed: Vec<String> = Vec::new();
    while placed.len() < all.len() {
        let mut best: Option<(usize, usize, String)> = None;
        for p in &all {
            if placed.contains(p) {
                continue;
            }
            // Every predecessor of p in every form is already placed.
            let ready = forms.iter().all(|f| match f.iter().position(|x| x == p) {
                Some(i) => f[..i].iter().all(|q| placed.contains(q)),
                None => true,
            });
            if !ready {
                continue;
            }
            let (fi, pi) = forms
                .iter()
                .enumerate()
                .find_map(|(fi, f)| f.iter().position(|x| x == p).map(|pi| (fi, pi)))
                .unwrap();
            if best.as_ref().map(|(bf, bp, _)| (fi, pi) < (*bf, *bp)).unwrap_or(true) {
                best = Some((fi, pi, p.clone()));
            }
        }
        let (_, _, p) = best.ok_or_else(|| anyhow::anyhow!("inconsistent parameter orders {forms:?}"))?;
        placed.push(p);
    }
    Ok(placed)
}

fn build_model(g: &Graph, info: &ClassInfo) -> anyhow::Result<PyModel> {
    let fqn = info.java_fqn.as_str();
    let ctors = surface_ctors(info);

    // Pass 1: find each constructor's message parameter — the String passed
    // unchanged to super(...) as the message, directly or through this(...).
    let mut message_java_names: BTreeSet<String> = BTreeSet::new();
    let mut message_param: BTreeMap<usize, String> = BTreeMap::new();
    for &ci in &ctors {
        let ctor = &info.java.ctors[ci];
        let env: Env = ctor
            .params
            .iter()
            .map(|p| (p.name.clone(), PyVal::param(&placeholder(&p.name), &p.ty)))
            .collect();
        let eff = g.eval(fqn, ci, &env, 0)?;
        for p in &ctor.params {
            if base_type(&p.ty) == "String" && eff.message == placeholder(&p.name) {
                message_java_names.insert(p.name.clone());
                message_param.insert(ci, p.name.clone());
            }
        }
    }

    // Python names per constructor parameter.
    let mut forms: Vec<PyForm> = Vec::new();
    let mut java_ty_of: BTreeMap<String, String> = BTreeMap::new();
    for &ci in &ctors {
        let ctor = &info.java.ctors[ci];
        let mut names = Vec::new();
        let mut env = Env::new();
        for p in &ctor.params {
            let py = if base_type(&p.ty) == "String"
                && (message_param.get(&ci) == Some(&p.name) || message_java_names.contains(&p.name))
            {
                "message".to_string()
            } else if throwable_like(&p.ty) {
                "cause".to_string()
            } else {
                snake(&p.name)
            };
            if names.contains(&py) {
                anyhow::bail!("{fqn}: two parameters of one constructor are both `{py}`");
            }
            let ty = if throwable_like(&p.ty) {
                "Throwable".to_string()
            } else {
                p.ty.clone()
            };
            if let Some(prev) = java_ty_of.get(&py) {
                if base_type(prev) != base_type(&ty) {
                    anyhow::bail!("{fqn}: parameter `{py}` has Java types `{prev}` and `{ty}`");
                }
            }
            java_ty_of.insert(py.clone(), ty.clone());
            env.insert(p.name.clone(), PyVal::param(&py, &p.ty));
            names.push(py);
        }
        let effect = g.eval(fqn, ci, &env, 0)?;
        forms.push(PyForm {
            ctor: ci,
            params: names,
            defaults: BTreeMap::new(),
            field_defaults: BTreeMap::new(),
            deprecated: ctor.deprecated.clone(),
            effect,
        });
    }

    // Java-given defaults (a): a shorter constructor passing constants to a
    // longer one through this(...).
    for fi in 0..forms.len() {
        let ci = forms[fi].ctor;
        let ctor = &info.java.ctors[ci];
        let Some((CallKind::This, args)) = &ctor.call else {
            continue;
        };
        let env: Env = ctor
            .params
            .iter()
            .zip(&forms[fi].params)
            .map(|(p, py)| (p.name.clone(), PyVal::param(py, &p.ty)))
            .collect();
        let vals: Vec<PyVal> = args.iter().map(|a| g.translate(a, &env)).collect::<anyhow::Result<_>>()?;
        let target = g.resolve(fqn, &vals, false)?;
        let Some(ti) = forms.iter().position(|f| f.ctor == target) else {
            continue;
        };
        let target_params = forms[ti].params.clone();
        let target_tys: Vec<String> = info.java.ctors[target].params.iter().map(|p| p.ty.clone()).collect();
        for ((py, v), jty) in target_params.iter().zip(vals).zip(target_tys) {
            if v.konst.is_none() && &v.expr == py {
                continue;
            }
            if let Some(k) = v.konst {
                let k = if header_type(&jty) && k == "None" {
                    "()".to_string()
                } else {
                    k
                };
                forms[ti].defaults.insert(py.clone(), k);
            }
        }
    }

    // Signature defaults (b): a shorter constructor that is the longer one with
    // constants assigned to the fields the longer one fills from parameters.
    // The default rule counts them ("or the constant it assigns to that
    // parameter's field"); matching does not, since the shorter constructor
    // passes nothing to the longer one.
    for si in 0..forms.len() {
        if matches!(info.java.ctors[forms[si].ctor].call, Some((CallKind::This, _))) {
            continue;
        }
        for ti in 0..forms.len() {
            if ti == si || matches!(info.java.ctors[forms[ti].ctor].call, Some((CallKind::This, _))) {
                continue;
            }
            let s_params: BTreeSet<&String> = forms[si].params.iter().collect();
            let t_params: BTreeSet<&String> = forms[ti].params.iter().collect();
            if !(s_params.is_subset(&t_params) && s_params.len() < t_params.len()) {
                continue;
            }
            let missing: Vec<String> = forms[ti].params.iter().filter(|p| !s_params.contains(p)).cloned().collect();
            let mut consts: BTreeMap<String, PyVal> = BTreeMap::new();
            for p in &missing {
                let found = forms[ti]
                    .effect
                    .fields
                    .iter()
                    .find(|(_, v)| v.konst.is_none() && stored_param(&v.expr) == p)
                    .map(|(attr, _)| attr.clone());
                let Some(attr) = found else { break };
                let Some(sv) = forms[si].effect.fields.get(&attr) else {
                    break;
                };
                if sv.konst.is_none() {
                    break;
                }
                consts.insert(p.clone(), sv.clone());
            }
            if consts.len() != missing.len() {
                continue;
            }
            let tctor = &info.java.ctors[forms[ti].ctor];
            let env: Env = tctor
                .params
                .iter()
                .zip(&forms[ti].params)
                .map(|(p, py)| {
                    let v = consts.get(py).cloned().unwrap_or_else(|| PyVal::param(py, &p.ty));
                    (p.name.clone(), v)
                })
                .collect();
            let eff = g.eval(fqn, forms[ti].ctor, &env, 0)?;
            if same_effect(&eff, &forms[si].effect) {
                for (p, v) in consts {
                    let jty = java_ty_of[&p].clone();
                    let k = v.konst.unwrap();
                    let k = if header_type(&jty) && k == "None" {
                        "()".to_string()
                    } else {
                        k
                    };
                    forms[ti].field_defaults.insert(p, k);
                }
            }
        }
    }

    // Union order and the forms of the @overload stubs.
    let orders: Vec<Vec<String>> = forms.iter().map(|f| f.params.clone()).collect();
    let order = merge_order(&orders)?;
    let n = forms.len();
    let is_prefix = |a: &Vec<String>, b: &Vec<String>| a.len() < b.len() && b[..a.len()] == a[..];
    let mut root: Vec<usize> = (0..n).collect();
    for (i, slot) in root.iter_mut().enumerate() {
        let mut cur = i;
        while let Some(j) = (0..n).find(|&j| j != cur && is_prefix(&forms[cur].params, &forms[j].params)) {
            cur = j;
        }
        *slot = cur;
    }
    // stub groups: root -> members
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (i, r) in root.iter().enumerate() {
        groups.entry(*r).or_default().push(i);
    }
    let mut stub_groups: Vec<(usize, Vec<usize>)> = groups.into_iter().collect();
    stub_groups.sort_by_key(|(r, members)| {
        let all_deprecated = members.iter().all(|m| forms[*m].deprecated.is_some());
        (all_deprecated, *members.iter().min().unwrap(), *r)
    });

    // Defaults of the implementation signature.
    let mut params: Vec<PyParam> = Vec::new();
    for p in &order {
        let jty = java_ty_of[p].clone();
        let in_every = forms.iter().all(|f| f.params.contains(p));
        let group_required = |members: &Vec<usize>| members.iter().all(|m| forms[*m].params.contains(p));
        let group_optional =
            |members: &Vec<usize>| members.iter().any(|m| forms[*m].params.contains(p)) && !group_required(members);
        let required_somewhere = stub_groups.iter().any(|(_, m)| group_required(m));
        let optional_somewhere = stub_groups.iter().any(|(_, m)| group_optional(m));
        let java_defaults: Vec<String> = forms
            .iter()
            .filter_map(|f| f.defaults.get(p).or_else(|| f.field_defaults.get(p)).cloned())
            .collect();
        let non_null_default = java_defaults.iter().any(|d| d != "None");
        let field_default = forms.iter().any(|f| f.field_defaults.contains_key(p));
        let required_by_one = forms.iter().any(|f| f.params.contains(p) && !f.defaults.contains_key(p));
        let default = if p == "cause" {
            Dflt::NoneDefault
        } else if in_every {
            Dflt::Required
        } else if (required_somewhere && optional_somewhere) || ((non_null_default || field_default) && required_by_one)
        {
            // A field default is a value, not an overload: the form that
            // requires the parameter must still see `None` as given.
            Dflt::Unset
        } else if let Some(first) = java_defaults.first() {
            if java_defaults.iter().any(|d| d != first) {
                anyhow::bail!("{fqn}: parameter `{p}` has different Java-given defaults {java_defaults:?}");
            }
            if first == "None" {
                Dflt::NoneDefault
            } else {
                Dflt::Const(first.clone())
            }
        } else if header_type(&jty) {
            Dflt::Const("()".into())
        } else {
            Dflt::NoneDefault
        };
        let nullable = java_defaults.iter().any(|d| d == "None");
        let field_nullable = forms
            .iter()
            .any(|f| f.field_defaults.get(p).map(String::as_str) == Some("None"));
        params.push(PyParam { name: p.clone(), java_ty: jty, default, nullable, field_nullable });
    }

    // The java_forms table, replicated: which form each set of given names
    // selects, and with which fills.
    let bit: BTreeMap<&str, u64> = order.iter().enumerate().map(|(i, p)| (p.as_str(), 1u64 << i)).collect();
    let mask_of = |names: &[String]| names.iter().fold(0u64, |m, p| m | bit[p.as_str()]);
    let mut table: BTreeMap<u64, Selection> = BTreeMap::new();
    for (fi, form) in forms.iter().enumerate() {
        let full = mask_of(&form.params);
        let defaultable: Vec<&String> = form.params.iter().filter(|p| form.defaults.contains_key(*p)).collect();
        for subset in 0..(1u64 << defaultable.len()) {
            let left_out: Vec<&String> = (0..defaultable.len())
                .filter(|i| subset & (1 << i) != 0)
                .map(|i| defaultable[i])
                .collect();
            let given = full & !left_out.iter().fold(0u64, |m, p| m | bit[p.as_str()]);
            if let Some(prev) = table.get(&given) {
                let prev_size = forms[prev.form].params.len();
                if form.params.len() < prev_size || (form.params.len() == prev_size && fi > prev.form) {
                    continue;
                }
            }
            table.insert(
                given,
                Selection {
                    form: fi,
                    fills: left_out.iter().map(|p| ((*p).clone(), form.defaults[*p].clone())).collect(),
                },
            );
        }
    }
    // A parameter in every overload is required: it always counts as given
    // (the errors' `cause` defaults to None and is still Java-required).
    let in_every = |p: &PyParam| {
        forms.iter().all(|f| f.params.contains(&p.name)) && !forms.iter().any(|f| f.defaults.contains_key(&p.name))
    };
    let required_mask = params
        .iter()
        .filter(|p| in_every(p))
        .fold(0u64, |m, p| m | bit[p.name.as_str()]);
    let optional: Vec<u64> = params.iter().filter(|p| !in_every(p)).map(|p| bit[p.name.as_str()]).collect();
    let mut decorated = false;
    let mut reachable_set: BTreeSet<usize> = BTreeSet::new();
    for combo in 0..(1u64 << optional.len()) {
        let mut given = required_mask;
        for (i, b) in optional.iter().enumerate() {
            if combo & (1 << i) != 0 {
                given |= b;
            }
        }
        match table.get(&given) {
            None => decorated = true,
            Some(sel) => {
                reachable_set.insert(sel.form);
                for (p, v) in &sel.fills {
                    let d = &params.iter().find(|x| &x.name == p).unwrap().default;
                    if &d.python() != v {
                        decorated = true;
                    }
                }
            },
        }
    }
    if params.iter().any(|p| p.default == Dflt::Unset) {
        decorated = true;
    }
    if decorated {
        // With the decorator every form a table entry selects is reachable.
        reachable_set = table.values().map(|s| s.form).collect();
    }
    let mut reachable: Vec<usize> = reachable_set.into_iter().collect();
    reachable.sort_by_key(|&f| (std::cmp::Reverse(forms[f].params.len()), f));

    let mut stubs: Vec<(Vec<String>, BTreeSet<String>)> = if decorated {
        stub_groups
            .iter()
            .map(|(r, members)| {
                let names = forms[*r].params.clone();
                let mut optional: BTreeSet<String> = names
                    .iter()
                    .filter(|p| members.iter().any(|m| !forms[*m].params.contains(p)))
                    .cloned()
                    .collect();
                // The errors' `cause` is always `= None` in a stub.
                if names.iter().any(|n| n == "cause") {
                    optional.insert("cause".to_string());
                }
                (names, optional)
            })
            .collect()
    } else {
        Vec::new()
    };
    // A stub an earlier one already accepts (every name of it in the earlier
    // stub, every other name of the earlier stub optional there) can never be
    // matched; the errors' `cause = None` makes `(cause)` such a stub.
    let mut k = 0;
    while k < stubs.len() {
        let subsumed = (0..k).any(|j| {
            let (earlier, earlier_optional) = &stubs[j];
            let (names, optional) = &stubs[k];
            names
                .iter()
                .all(|n| earlier.contains(n) && (earlier_optional.contains(n) || !optional.contains(n)))
                && earlier.iter().all(|n| names.contains(n) || earlier_optional.contains(n))
        });
        if subsumed {
            stubs.remove(k);
        } else {
            k += 1;
        }
    }

    Ok(PyModel { params, forms, decorated, reachable, stubs })
}

// ---------------------------------------------------------------------------
// Python types (CLAUDE.md, Python Binding Conventions, Types)
// ---------------------------------------------------------------------------

/// Where a Python name used in an annotation or a value comes from.
fn import_source(name: &str) -> Option<(&'static str, &'static str)> {
    Some(match name {
        "TopicPartition" => ("confluent_kafka.common.topic_partition", "TopicPartition"),
        "OffsetAndMetadata" => ("confluent_kafka.consumer.offset_and_metadata", "OffsetAndMetadata"),
        "KafkaMetric" => ("confluent_kafka.common.kafka_metric", "KafkaMetric"),
        "TimestampType" => ("confluent_kafka.common.timestamp_type", "TimestampType"),
        "Headers" => ("confluent_kafka.common.headers", "Headers"),
        "Iterable" => ("collections.abc", "Iterable"),
        "Mapping" => ("collections.abc", "Mapping"),
        "Sequence" => ("collections.abc", "Sequence"),
        "Any" => ("typing", "Any"),
        _ => return None,
    })
}

fn generic_args(ty: &str) -> Vec<String> {
    match (ty.find('<'), ty.rfind('>')) {
        (Some(a), Some(b)) if a < b => ty[a + 1..b].split(',').map(|s| s.trim().to_string()).collect(),
        _ => Vec::new(),
    }
}

/// The Python type of a Java type, as an input (a parameter) or an output (a
/// getter's return). Names needing an import are recorded in `used`.
fn py_type(ty: &str, input: bool, info: &ClassInfo, used: &mut BTreeSet<String>) -> anyhow::Result<String> {
    let base = base_type(ty);
    let args = generic_args(ty);
    let arg = |i: usize, used: &mut BTreeSet<String>| -> anyhow::Result<String> {
        let a = args.get(i).ok_or_else(|| anyhow::anyhow!("`{ty}` has no type argument {i}"))?;
        py_type(a, input, info, used)
    };
    let named = |n: &str, used: &mut BTreeSet<String>| {
        used.insert(n.to_string());
        n.to_string()
    };
    Ok(match base {
        "String" => "str".into(),
        "int" | "long" | "short" | "byte" | "Integer" | "Long" | "Short" | "Byte" => "int".into(),
        "double" | "float" | "Double" | "Float" => "float".into(),
        "boolean" | "Boolean" => "bool".into(),
        "Object" => named("Any", used),
        _ if throwable_like(base) => "BaseException".into(),
        "Set" | "Collection" | "Iterable" if input => format!("{}[{}]", named("Iterable", used), arg(0, used)?),
        "Set" => format!("set[{}]", arg(0, used)?),
        "List" if input => format!("{}[{}]", named("Sequence", used), arg(0, used)?),
        "List" | "Collection" | "Iterable" => format!("list[{}]", arg(0, used)?),
        "Map" if input => format!("{}[{}, {}]", named("Mapping", used), arg(0, used)?, arg(1, used)?),
        "Map" => format!("dict[{}, {}]", arg(0, used)?, arg(1, used)?),
        "ByteBuffer" | "byte[]" if input => "bytes".into(),
        "ByteBuffer" | "byte[]" => "memoryview".into(),
        _ if header_type(ty) && input => {
            format!("{}[tuple[str, bytes | bytearray | memoryview | None]]", named("Iterable", used))
        },
        _ if header_type(ty) => named("Headers", used),
        "TopicPartition" | "OffsetAndMetadata" | "KafkaMetric" | "TimestampType" => named(base, used),
        other if info.java.enums.iter().any(|e| e.name == other) => format!("{}.{}", info.py_name, other),
        other => anyhow::bail!("{}: no Python type for Java type `{other}`", info.java_fqn),
    })
}

fn java_default_value(ty: &str) -> &'static str {
    match base_type(ty) {
        "int" | "long" | "short" | "byte" => "0",
        "double" | "float" => "0.0",
        "boolean" => "False",
        _ => "None",
    }
}

fn getter_name(java: &str) -> String {
    let stripped = java
        .strip_prefix("get")
        .filter(|rest| rest.chars().next().is_some_and(char::is_uppercase))
        .unwrap_or(java);
    snake(stripped)
}

/// Whether a field may hold `None` after construction, over the forms a call
/// can select (CLAUDE.md, Signatures, Nullability).
fn field_nullable(model: &PyModel, attr: &str, ty: &str) -> bool {
    for &fi in &model.reachable {
        let form = &model.forms[fi];
        match form.effect.fields.get(attr) {
            None => {
                if java_default_value(ty) == "None" {
                    return true;
                }
            },
            Some(v) => {
                if v.expr == "None" {
                    return true;
                }
                // A parameter the form requires is given, so not None; one the
                // form defaults holds its Java-given default when left out.
                let p = stored_param(&v.expr);
                if form.defaults.get(p).map(String::as_str) == Some("None") {
                    return true;
                }
            },
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Emission
// ---------------------------------------------------------------------------

const PY_HEADER: &str = "# Copyright 2025 Confluent Inc.\n\
#\n\
# Licensed under the Apache License, Version 2.0 (the \"License\");\n\
# you may not use this file except in compliance with the License.\n\
# You may obtain a copy of the License at\n\
#\n\
#     http://www.apache.org/licenses/LICENSE-2.0\n\
#\n\
# Unless required by applicable law or agreed to in writing, software\n\
# distributed under the License is distributed on an \"AS IS\" BASIS,\n\
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
# See the License for the specific language governing permissions and\n\
# limitations under the License.\n";

const GENERATED_NOTE: &str = "GENERATED, DO NOT EDIT. Produced from the Java source by\n\
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,\n\
Errors) and validated for staleness by ``cargo xtask check-generated``.";

/// The Python base expression of a class.
fn py_base(g: &Graph, info: &ClassInfo) -> String {
    if TERMINAL_BASES.contains(&info.parent_fqn.as_str()) {
        if info.java_fqn == "java.util.concurrent.TimeoutException" {
            "builtins.TimeoutError".into()
        } else {
            "RuntimeError".into()
        }
    } else {
        g.classes[&info.parent_fqn].py_name.clone()
    }
}

fn is_root_class(info: &ClassInfo) -> bool {
    TERMINAL_BASES.contains(&info.parent_fqn.as_str())
}

fn wrap_doc(text: &str, indent: &str) -> String {
    let mut out = Vec::new();
    for para in text.split("\n\n") {
        let mut line = String::new();
        for word in para.split_whitespace() {
            if !line.is_empty() && indent.len() + line.len() + 1 + word.len() > 79 {
                out.push(format!("{indent}{line}"));
                line.clear();
            }
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }
        if !line.is_empty() {
            out.push(format!("{indent}{line}"));
        }
        out.push(String::new());
    }
    while out.last().is_some_and(String::is_empty) {
        out.pop();
    }
    out.join("\n").replace(" \n", "\n")
}

fn class_doc(info: &ClassInfo, model: &PyModel) -> String {
    let mut parts = Vec::new();
    if !info.java.javadoc.is_empty() {
        parts.push(info.java.javadoc.replace('\\', "\\\\"));
    }
    if info.is_jdk {
        parts.push(format!("Java's built-in ``{}``.", info.java_fqn));
    } else {
        parts.push(format!("Java: ``{}``.", info.java_fqn));
    }
    for form in &model.forms {
        if let Some(note) = &form.deprecated {
            parts.push(format!(
                "Deprecated: the constructor form ``({})``. {}",
                form.params.join(", "),
                note.replace('\\', "\\\\")
            ));
        }
    }
    if info.is_abstract {
        parts.push("A catch-only base: constructing it raises ``TypeError``.".into());
    }
    parts.join("\n\n")
}

fn given_expr(p: &PyParam) -> String {
    match &p.default {
        Dflt::Required => "True".into(),
        Dflt::NoneDefault => format!("{} is not None", p.name),
        Dflt::Unset => format!("{} is not UNSET", p.name),
        Dflt::Const(c) => format!("is_given({}, {})", p.name, c),
    }
}

fn not_given_expr(p: &PyParam) -> String {
    match &p.default {
        Dflt::Required => "False".into(),
        Dflt::NoneDefault => format!("{} is None", p.name),
        Dflt::Unset => format!("{} is UNSET", p.name),
        Dflt::Const(c) => format!("not is_given({}, {})", p.name, c),
    }
}

/// One constructor branch: `Throwable` init plus every field of the chain.
fn branch_code(g: &Graph, info: &ClassInfo, form: &PyForm, indent: &str) -> Vec<String> {
    let mut lines = vec![format!(
        "{indent}_throwable.init(self, {}, {})",
        form.effect.message, form.effect.cause
    )];
    for (attr, ty) in g.chain_fields(&info.java_fqn) {
        let value = form
            .effect
            .fields
            .get(&attr)
            .map(|v| v.expr.clone())
            .unwrap_or_else(|| java_default_value(&ty).to_string());
        lines.push(format!("{indent}self.{attr} = {value}"));
    }
    lines
}

/// One `__init__` branch: its condition's conjuncts (parameter, test) and its
/// code lines.
type Branch = (Vec<(String, String)>, Vec<String>);

/// Replace the identifier `word` (not a longer identifier containing it).
fn replace_word(line: &str, word: &str, with: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = line.chars().collect();
    let w: Vec<char> = word.chars().collect();
    let ident = |c: char| c.is_alphanumeric() || c == '_' || c == '.';
    let mut i = 0;
    while i < chars.len() {
        if chars[i..].starts_with(&w)
            && (i == 0 || !ident(chars[i - 1]))
            && chars.get(i + w.len()).is_none_or(|c| !(c.is_alphanumeric() || *c == '_'))
        {
            out.push_str(with);
            i += w.len();
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn param_decl(p: &PyParam, info: &ClassInfo, used: &mut BTreeSet<String>) -> anyhow::Result<String> {
    let ty = py_type(&p.java_ty, true, info, used)?;
    Ok(match &p.default {
        Dflt::Required => format!("{}: {ty}", p.name),
        Dflt::NoneDefault if ty == "Any" => format!("{}: Any = None", p.name),
        Dflt::NoneDefault => format!("{}: {ty} | None = None", p.name),
        Dflt::Unset if p.nullable && ty != "Any" => format!("{}: {ty} | None = UNSET", p.name),
        Dflt::Unset => format!("{}: {ty} = UNSET", p.name),
        Dflt::Const(c) => format!("{}: {ty} = {c}", p.name),
    })
}

/// A stub parameter: required in this form, or optional with Java's value.
fn stub_param(
    p: &PyParam,
    optional: bool,
    model: &PyModel,
    info: &ClassInfo,
    used: &mut BTreeSet<String>,
) -> anyhow::Result<String> {
    let ty = py_type(&p.java_ty, true, info, used)?;
    if p.name == "cause" {
        return Ok(format!("cause: {ty} | None = None"));
    }
    if !optional {
        if p.field_nullable && ty != "Any" {
            return Ok(format!("{}: {ty} | None", p.name));
        }
        return Ok(format!("{}: {ty}", p.name));
    }
    let java_value = model.forms.iter().find_map(|f| f.defaults.get(&p.name).cloned());
    Ok(match (&p.default, java_value) {
        (Dflt::NoneDefault, _) => format!("{}: {ty} | None = None", p.name),
        (_, Some(v)) if v == "None" => format!("{}: {ty} | None = None", p.name),
        (_, Some(v)) => format!("{}: {ty} = {v}", p.name),
        (Dflt::Const(c), None) => format!("{}: {ty} = {c}", p.name),
        _ => format!("{}: {ty} = ...", p.name),
    })
}

struct Rendered {
    py: String,
    pyi: String,
}

fn render_class(g: &Graph, info: &ClassInfo) -> anyhow::Result<Rendered> {
    let model = build_model(g, info)?;
    let py_name = &info.py_name;
    let base = py_base(g, info);
    let mut used: BTreeSet<String> = BTreeSet::new();

    // ---- the class body (.py) ----
    let mut body: Vec<String> = Vec::new();
    body.push(format!("class {py_name}({base}):"));
    let doc = wrap_doc(&class_doc(info, &model), "    ");
    if doc.contains('\n') {
        body.push(format!("    \"\"\"{}", doc.trim_start()));
        body.push("    \"\"\"".into());
    } else {
        body.push(format!("    \"\"\"{}\"\"\"", doc.trim_start()));
    }
    body.push(String::new());
    body.push(format!("    __module__ = \"{}\"", info.py_module));
    for s in &info.java.singletons {
        body.push(String::new());
        body.push(format!("    {}: ClassVar[{py_name}]", s.name));
    }
    if let Some((id, value)) = &info.ffi_id {
        body.push(String::new());
        if info.java_fqn == KAFKA_EXCEPTION_FQN {
            body.push("    # A bare KafkaException is reported as UNKNOWN_SERVER_ERROR; UnknownServerError".into());
            body.push("    # owns that id in the id -> class table.".into());
        }
        body.push(format!("    _ffi_id: ClassVar[int] = {value}  # kafka_common_ErrorCode_{id}"));
    }
    for e in &info.java.enums {
        body.push(String::new());
        body.push(format!("    class {}(enum.Enum):", e.name));
        body.push(format!(
            "        \"\"\"Java's nested enum ``{}.{}``.\"\"\"",
            info.java.simple, e.name
        ));
        body.push(String::new());
        for c in &e.constants {
            body.push(format!("        {c} = \"{c}\""));
        }
    }

    // __init__
    body.push(String::new());
    if model.decorated {
        body.push("    @java_forms(".into());
        for form in &model.forms {
            let mut args: Vec<String> = form.params.iter().map(|p| format!("\"{p}\"")).collect();
            if !form.defaults.is_empty() {
                let d: Vec<String> = form
                    .params
                    .iter()
                    .filter_map(|p| form.defaults.get(p).map(|v| format!("\"{p}\": {v}")))
                    .collect();
                args.push(format!("defaults={{{}}}", d.join(", ")));
            }
            if let Some(note) = &form.deprecated {
                args.push(format!("deprecated={}", py_str_literal(note)));
            }
            body.push(format!("        Form({}),", args.join(", ")));
        }
        body.push("    )".into());
    }
    let mut decls: Vec<String> = Vec::new();
    for p in &model.params {
        decls.push(param_decl(p, info, &mut used)?);
    }
    if model.decorated {
        decls.push("_java_form: int = -1".into());
    }
    if decls.is_empty() {
        body.push("    def __init__(self) -> None:".into());
    } else {
        body.push("    def __init__(".into());
        body.push("        self,".into());
        body.push("        *,".into());
        for d in &decls {
            body.push(format!("        {d},"));
        }
        body.push("    ) -> None:".into());
    }
    if info.is_abstract {
        body.push(format!("        if type(self) is {py_name}:"));
        body.push("            raise TypeError(".into());
        body.push(format!(
            "                \"{py_name} is an abstract catch-only base; it is never raised directly\""
        ));
        body.push("            )".into());
    }
    if !model.decorated {
        // Undecorated: a deprecated form is recognized by its given names.
        for form in model.forms.iter().filter(|f| f.deprecated.is_some()) {
            let conds: Vec<String> = model
                .params
                .iter()
                .filter(|p| !model.forms.iter().all(|f| f.params.contains(&p.name)))
                .map(|p| {
                    if form.params.contains(&p.name) {
                        given_expr(p)
                    } else {
                        not_given_expr(p)
                    }
                })
                .collect();
            let cond = if conds.is_empty() {
                "True".to_string()
            } else {
                conds.join(" and ")
            };
            body.push(format!("        if {cond}:"));
            let text = format!(
                "{py_name}({}) is deprecated. {}",
                form.params.join(", "),
                form.deprecated.as_deref().unwrap_or("")
            );
            body.push(format!(
                "            warnings.warn({}, DeprecationWarning, stacklevel=2)",
                py_str_literal(&text)
            ));
        }
    }
    let reachable: Vec<usize> = if model.decorated {
        let mut r = model.reachable.clone();
        r.sort_unstable();
        r
    } else {
        model.reachable.clone()
    };
    // Branches: (condition conjuncts keyed by parameter, code).
    let mut branches: Vec<Branch> = Vec::new();
    for &fi in &reachable {
        let form = &model.forms[fi];
        let conds: Vec<(String, String)> = if model.decorated {
            vec![(String::new(), format!("_java_form == {fi}"))]
        } else {
            model
                .params
                .iter()
                .filter(|p| !model.forms.iter().all(|f| f.params.contains(&p.name)))
                .filter_map(|p| {
                    if form.params.contains(&p.name) && !form.defaults.contains_key(&p.name) {
                        Some((p.name.clone(), given_expr(p)))
                    } else if !form.params.contains(&p.name) {
                        Some((p.name.clone(), not_given_expr(p)))
                    } else {
                        None
                    }
                })
                .collect()
        };
        branches.push((conds, branch_code(g, info, form, "            ")));
    }
    if !model.decorated {
        // A branch that differs from a longer one only by a None-default
        // parameter being None merges into it (`(message)` into
        // `(message, cause)`): the longer branch's code with that parameter
        // None is the shorter branch's code.
        let mut k = 0;
        while k < branches.len() {
            let mut merged = false;
            for j in 0..k {
                let (cj, codej) = (&branches[j].0, &branches[j].1);
                let (ck, codek) = (&branches[k].0, &branches[k].1);
                if cj.len() != ck.len() {
                    continue;
                }
                let differing: Vec<usize> = (0..cj.len()).filter(|i| cj[*i] != ck[*i]).collect();
                let [i] = differing.as_slice() else { continue };
                let name = &cj[*i].0;
                let Some(p) = model.params.iter().find(|p| &p.name == name) else {
                    continue;
                };
                if p.default != Dflt::NoneDefault || cj[*i].1 != given_expr(p) {
                    continue;
                }
                let replaced: Vec<String> = codej.iter().map(|l| replace_word(l, name, "None")).collect();
                if &replaced == codek {
                    branches[j].0.remove(*i);
                    branches.remove(k);
                    merged = true;
                    break;
                }
            }
            if !merged {
                k += 1;
            }
        }
    }
    if branches.len() == 1 {
        for line in &branches[0].1 {
            body.push(line.strip_prefix("    ").unwrap_or(line).to_string());
        }
    } else {
        let count = branches.len();
        for (k, (conds, code)) in branches.iter().enumerate() {
            let cond = if conds.is_empty() {
                "True".to_string()
            } else {
                conds.iter().map(|(_, c)| c.clone()).collect::<Vec<_>>().join(" and ")
            };
            if k + 1 == count {
                body.push("        else:".into());
            } else if k == 0 {
                body.push(format!("        if {cond}:"));
            } else {
                body.push(format!("        elif {cond}:"));
            }
            body.extend(code.iter().cloned());
        }
    }
    let kwargs: Vec<String> = model.params.iter().map(|p| format!("{0}={0}", p.name)).collect();
    body.push(format!("        self._java_kwargs = _throwable.kwargs({})", kwargs.join(", ")));

    // getters
    let mut getter_sigs: Vec<String> = Vec::new();
    for gt in &info.java.getters {
        let name = getter_name(&gt.name);
        let mut ret = py_type(&gt.ret, false, info, &mut used)?;
        let code = match &gt.body {
            GetterBody::Field(f) => {
                let attr = attr_name(f);
                let fty = g
                    .field_type(&info.java_fqn, f)
                    .ok_or_else(|| anyhow::anyhow!("{}: getter of unknown field `{f}`", info.java_fqn))?;
                if field_nullable(&model, &attr, &fty) {
                    ret = format!("{ret} | None");
                }
                format!("return self.{attr}")
            },
            GetterBody::KeySet(f) => format!("return set(self.{}.keys())", attr_name(f)),
            GetterBody::Abstract => "raise NotImplementedError".to_string(),
        };
        body.push(String::new());
        body.push(format!("    def {name}(self) -> {ret}:"));
        body.push(format!("        \"\"\"Java's ``{}()``.\"\"\"", gt.name));
        body.push(format!("        {code}"));
        getter_sigs.push(format!("    def {name}(self) -> {ret}: ..."));
    }
    if is_root_class(info) {
        body.push(String::new());
        body.push("    def __str__(self) -> str:".into());
        body.push("        \"\"\"Java's ``getMessage()``; ``\"\"`` when it is ``null``.\"\"\"".into());
        body.push("        return _throwable.message_text(self)".into());
        body.push(String::new());
        body.push("    def __reduce__(self) -> str | tuple[Any, ...]:".into());
        body.push("        \"\"\"Rebuild from the constructor arguments by name (``copy``, ``pickle``).\"\"\"".into());
        body.push("        return _throwable.reduce(self)".into());
        used.insert("Any".into());
    }
    for s in &info.java.singletons {
        let vals: Vec<PyVal> = s
            .args
            .iter()
            .map(|a| g.translate(a, &Env::new()))
            .collect::<anyhow::Result<_>>()?;
        let target = g.resolve(&info.java_fqn, &vals, false)?;
        let env = g.bind(&info.java_fqn, target, vals)?;
        let eff = g.eval(&info.java_fqn, target, &env, 0)?;
        body.push(String::new());
        body.push(String::new());
        body.push(format!(
            "{py_name}.{0} = _throwable.singleton({py_name}, \"{py_name}.{0}\", {1}, {2})",
            s.name, eff.message, eff.cause
        ));
    }
    let class_text = body.join("\n") + "\n";

    // ---- imports (.py) ----
    let mut runtime_std: Vec<String> = Vec::new();
    if base.starts_with("builtins.") {
        runtime_std.push("import builtins".into());
    }
    if !info.java.enums.is_empty() {
        runtime_std.push("import enum".into());
    }
    if class_text.contains("warnings.warn") {
        runtime_std.push("import warnings".into());
    }
    let mut typing_names: BTreeSet<&str> = BTreeSet::new();
    if class_text.contains("ClassVar[") {
        typing_names.insert("ClassVar");
    }
    let annotation_pkgs: Vec<(&str, &str)> = used
        .iter()
        .filter_map(|n| import_source(n))
        .filter(|(m, _)| m.starts_with("confluent_kafka"))
        .collect();
    if !annotation_pkgs.is_empty() {
        typing_names.insert("TYPE_CHECKING");
    }
    let mut abc: BTreeSet<&str> = BTreeSet::new();
    for n in &used {
        match import_source(n) {
            Some(("collections.abc", name)) => {
                abc.insert(name);
            },
            Some(("typing", name)) => {
                typing_names.insert(name);
            },
            _ => {},
        }
    }
    let mut lines: Vec<String> = Vec::new();
    lines.push(PY_HEADER.trim_end().to_string());
    lines.push(String::new());
    let first = info.java.javadoc.split("\n\n").next().unwrap_or("").to_string();
    let _ = first;
    lines.push(format!(
        "\"\"\"``{py_name}``: Java's ``{}``.\n\n{GENERATED_NOTE}\n\"\"\"",
        info.java_fqn
    ));
    lines.push(String::new());
    lines.push("from __future__ import annotations".into());
    lines.push(String::new());
    let mut std_block: Vec<String> = runtime_std.clone();
    if !abc.is_empty() {
        std_block.push(format!(
            "from collections.abc import {}",
            abc.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    if !typing_names.is_empty() {
        std_block.push(format!(
            "from typing import {}",
            typing_names.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    if !std_block.is_empty() {
        lines.extend(std_block);
        lines.push(String::new());
    }
    let mut pkg_block: Vec<String> = vec!["from confluent_kafka import _throwable".into()];
    let mut args_names: Vec<&str> = Vec::new();
    if class_text.contains("UNSET") {
        args_names.push("UNSET");
    }
    if model.decorated {
        args_names.push("Form");
    }
    if class_text.contains("is_given(") {
        args_names.push("is_given");
    }
    if model.decorated {
        args_names.push("java_forms");
    }
    if !args_names.is_empty() {
        pkg_block.push(format!("from confluent_kafka._args import {}", args_names.join(", ")));
    }
    if class_text.contains("java_str(") {
        pkg_block.push("from confluent_kafka._java import java_str".into());
    }
    let runtime_timestamp = class_text.lines().any(|l| {
        let l = l.trim();
        l.contains("TimestampType.") && !l.starts_with("def ") && !l.contains("-> ")
    });
    if runtime_timestamp {
        pkg_block.push("from confluent_kafka.common.timestamp_type import TimestampType".into());
    }
    if !is_root_class(info) {
        let parent = &g.classes[&info.parent_fqn];
        pkg_block.push(format!("from {} import {}", parent.module_file(), parent.py_name));
    }
    pkg_block.sort();
    lines.extend(pkg_block);
    let checking: Vec<String> = annotation_pkgs
        .iter()
        .filter(|(m, n)| !(runtime_timestamp && *n == "TimestampType" && m.ends_with("timestamp_type")))
        .map(|(m, n)| format!("    from {m} import {n}"))
        .collect();
    if !checking.is_empty() {
        lines.push(String::new());
        lines.push("if TYPE_CHECKING:".into());
        lines.extend(checking);
    }
    lines.push(String::new());
    lines.push(format!("__all__ = [\"{py_name}\"]"));
    lines.push(String::new());
    lines.push(String::new());
    let py = lines.join("\n") + "\n" + &class_text;

    // ---- the stub (.pyi) ----
    let mut stub_used: BTreeSet<String> = used.clone();
    let mut sbody: Vec<String> = vec![format!("class {py_name}({base}):")];
    for s in &info.java.singletons {
        sbody.push(format!("    {}: ClassVar[{py_name}]", s.name));
    }
    if info.ffi_id.is_some() {
        sbody.push("    _ffi_id: ClassVar[int]".into());
    }
    for e in &info.java.enums {
        sbody.push(format!("    class {}(enum.Enum):", e.name));
        for c in &e.constants {
            sbody.push(format!("        {c} = \"{c}\""));
        }
    }
    if model.stubs.len() > 1 {
        for (names, optional) in &model.stubs {
            let mut ps = Vec::new();
            for n in names {
                let p = model.params.iter().find(|p| &p.name == n).unwrap();
                ps.push(stub_param(p, optional.contains(n), &model, info, &mut stub_used)?);
            }
            sbody.push("    @overload".into());
            if ps.is_empty() {
                sbody.push("    def __init__(self) -> None: ...".into());
            } else {
                sbody.push(format!("    def __init__(self, *, {}) -> None: ...", ps.join(", ")));
            }
        }
    } else {
        let mut ps = Vec::new();
        for p in &model.params {
            let decl = param_decl(p, info, &mut stub_used)?;
            ps.push(if p.default == Dflt::Unset {
                decl.replace("= UNSET", "= ...")
            } else {
                decl
            });
        }
        if ps.is_empty() {
            sbody.push("    def __init__(self) -> None: ...".into());
        } else {
            sbody.push(format!("    def __init__(self, *, {}) -> None: ...", ps.join(", ")));
        }
    }
    sbody.extend(getter_sigs);
    if is_root_class(info) {
        sbody.push("    def __str__(self) -> str: ...".into());
        sbody.push("    def __reduce__(self) -> str | tuple[Any, ...]: ...".into());
    }
    let stext = sbody.join("\n") + "\n";
    let mut slines: Vec<String> = vec![PY_HEADER.trim_end().to_string(), String::new()];
    slines.push(format!("# GENERATED, DO NOT EDIT (stub for {}.{py_name}).", info.py_module));
    slines.push(String::new());
    let mut s_std: Vec<String> = Vec::new();
    if base.starts_with("builtins.") {
        s_std.push("import builtins".into());
    }
    if !info.java.enums.is_empty() {
        s_std.push("import enum".into());
    }
    let mut s_abc: BTreeSet<&str> = BTreeSet::new();
    let mut s_typing: BTreeSet<&str> = BTreeSet::new();
    if stext.contains("ClassVar[") {
        s_typing.insert("ClassVar");
    }
    if stext.contains("@overload") {
        s_typing.insert("overload");
    }
    let mut s_pkg: Vec<String> = Vec::new();
    for n in &stub_used {
        match import_source(n) {
            Some(("collections.abc", name)) => {
                s_abc.insert(name);
            },
            Some(("typing", name)) => {
                s_typing.insert(name);
            },
            Some((m, name)) => s_pkg.push(format!("from {m} import {name}")),
            None => {},
        }
    }
    if !s_abc.is_empty() {
        s_std.push(format!(
            "from collections.abc import {}",
            s_abc.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    if !s_typing.is_empty() {
        s_std.push(format!(
            "from typing import {}",
            s_typing.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    if !is_root_class(info) {
        let parent = &g.classes[&info.parent_fqn];
        s_pkg.push(format!("from {} import {}", parent.module_file(), parent.py_name));
    }
    s_pkg.sort();
    if !s_std.is_empty() {
        slines.extend(s_std);
        slines.push(String::new());
    }
    if !s_pkg.is_empty() {
        slines.extend(s_pkg);
        slines.push(String::new());
    }
    slines.push(format!("__all__ = [\"{py_name}\"]"));
    slines.push(String::new());
    let pyi = slines.join("\n") + "\n" + &stext;
    Ok(Rendered { py, pyi })
}

fn module_dir(repo_root: &Path, module: &str) -> PathBuf {
    let mut p = repo_root.join(PY_ROOT);
    for seg in module.split('.') {
        p.push(seg);
    }
    p
}

/// The re-export lines of a package's error classes, sorted by name.
fn reexports(classes: &[&ClassInfo]) -> (Vec<String>, Vec<String>) {
    let mut sorted: Vec<&&ClassInfo> = classes.iter().collect();
    sorted.sort_by(|a, b| a.py_name.cmp(&b.py_name));
    let imports = sorted
        .iter()
        .map(|c| format!("from .{} import {1} as {1}", c.file_stem(), c.py_name))
        .collect();
    let names = sorted.iter().map(|c| format!("    \"{}\",", c.py_name)).collect();
    (imports, names)
}

fn render_block(classes: &[&ClassInfo]) -> String {
    let (imports, names) = reexports(classes);
    let mut s = vec![BLOCK_BEGIN.to_string()];
    s.extend(imports);
    s.push("__all__ += [".into());
    s.extend(names);
    s.push("]".into());
    s.push(BLOCK_END.into());
    s.join("\n") + "\n"
}

fn splice_block(current: &str, block: &str) -> String {
    match (current.find(BLOCK_BEGIN), current.find(BLOCK_END)) {
        (Some(a), Some(b)) if a < b => {
            let end = b + BLOCK_END.len();
            let end = if current[end..].starts_with('\n') { end + 1 } else { end };
            format!("{}{}{}", &current[..a], block, &current[end..])
        },
        _ => format!("{}\n{}", current.trim_end().to_string() + "\n", block),
    }
}

/// The generated files, so `check-generated` can compare.
pub fn generated_outputs(repo_root: &Path) -> anyhow::Result<Vec<(PathBuf, String)>> {
    let classes = build_graph(repo_root)?;
    let g = Graph { classes: &classes };
    let mut outputs = Vec::new();
    let mut by_module: BTreeMap<String, Vec<&ClassInfo>> = BTreeMap::new();
    for info in classes.values() {
        let r = render_class(&g, info)?;
        let dir = module_dir(repo_root, &info.py_module);
        outputs.push((dir.join(format!("{}.py", info.file_stem())), r.py));
        outputs.push((dir.join(format!("{}.pyi", info.file_stem())), r.pyi));
        by_module.entry(info.py_module.clone()).or_default().push(info);
    }

    // Package __init__ files.
    let mut intermediate: BTreeSet<String> = BTreeSet::new();
    for (module, members) in &by_module {
        let dir = module_dir(repo_root, module);
        if MIXED_PACKAGES.contains(&module.as_str()) {
            let path = dir.join("__init__.py");
            let current = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
            outputs.push((path, splice_block(&current, &render_block(members))));
            continue;
        }
        let java_package = &members[0].java.package;
        let (imports, names) = reexports(members);
        let mut py = vec![PY_HEADER.trim_end().to_string(), String::new()];
        py.push(format!(
            "\"\"\"``{module}``: the errors of Java's ``{java_package}``.\n\n{GENERATED_NOTE}\n\"\"\""
        ));
        py.push(String::new());
        py.push("from __future__ import annotations".into());
        py.push(String::new());
        py.extend(imports.clone());
        py.push(String::new());
        py.push("__all__ = [".into());
        py.extend(names.clone());
        py.push("]".into());
        outputs.push((dir.join("__init__.py"), py.join("\n") + "\n"));
        let mut pyi = vec![PY_HEADER.trim_end().to_string(), String::new()];
        pyi.push(format!("# GENERATED, DO NOT EDIT (stub for the {module} package)."));
        pyi.push(String::new());
        pyi.extend(imports);
        pyi.push(String::new());
        pyi.push("__all__ = [".into());
        pyi.extend(names);
        pyi.push("]".into());
        outputs.push((dir.join("__init__.pyi"), pyi.join("\n") + "\n"));
        // Parent packages that hold no class of their own.
        let mut parent = module.rsplit_once('.').map(|(p, _)| p.to_string());
        while let Some(p) = parent {
            if !MIXED_PACKAGES.contains(&p.as_str()) && !by_module.contains_key(&p) {
                intermediate.insert(p.clone());
            }
            parent = p.rsplit_once('.').map(|(q, _)| q.to_string());
        }
    }
    for module in intermediate {
        let dir = module_dir(repo_root, &module);
        let py = format!(
            "{}\n\n\"\"\"``{module}``: a parent package of generated error modules.\n\n{GENERATED_NOTE}\n\"\"\"\n\nfrom __future__ import annotations\n\n__all__: list[str] = []\n",
            PY_HEADER.trim_end()
        );
        let pyi = format!(
            "{}\n\n# GENERATED, DO NOT EDIT (stub for the {module} package).\n\n__all__: list[str]\n",
            PY_HEADER.trim_end()
        );
        outputs.push((dir.join("__init__.py"), py));
        outputs.push((dir.join("__init__.pyi"), pyi));
    }

    // The registry of every generated class, for the id table and the tests.
    let mut rows: Vec<String> = classes
        .values()
        .map(|c| format!("    (\"{}\", \"{}\", \"{}\"),", c.module_file(), c.py_name, c.java_fqn))
        .collect();
    rows.sort();
    let registry = format!(
        "{}\n\n\"\"\"Every generated error class: (defining module, class name, Java class).\n\n{GENERATED_NOTE}\n\"\"\"\n\nfrom __future__ import annotations\n\nERRORS: tuple[tuple[str, str, str], ...] = (\n{}\n)\n",
        PY_HEADER.trim_end(),
        rows.join("\n")
    );
    outputs.push((repo_root.join(PY_ROOT).join("confluent_kafka/_error_registry.py"), registry));
    Ok(outputs)
}

/// Regenerate the Python exception hierarchy files.
pub fn generate(repo_root: &Path) -> anyhow::Result<()> {
    let outputs = generated_outputs(repo_root)?;
    for (path, content) in &outputs {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)?;
    }
    // A class Java renamed or dropped leaves its old output behind: remove it.
    let orphans = orphaned_outputs(repo_root, &outputs)?;
    for path in &orphans {
        fs::remove_file(path)?;
    }
    println!(
        "✅ Wrote {} generated error-hierarchy file(s), removed {} orphaned",
        outputs.len(),
        orphans.len()
    );
    let unlisted = unlisted_packages(repo_root, &outputs)?;
    if !unlisted.is_empty() {
        anyhow::bail!("generated packages missing from bindings/python/pyproject.toml: {unlisted:?}");
    }
    Ok(())
}

/// The marker every generated error module and stub carries.
const GENERATED_MARKER: &str = "GENERATED, DO NOT EDIT";

/// Generated files under the package that the Java sources no longer produce
/// (a renamed or dropped exception class): files carrying the generated
/// marker that are not among `outputs`.
fn orphaned_outputs(repo_root: &Path, outputs: &[(PathBuf, String)]) -> anyhow::Result<Vec<PathBuf>> {
    let expected: BTreeSet<&PathBuf> = outputs.iter().map(|(p, _)| p).collect();
    let mut orphans = Vec::new();
    let mut stack = vec![repo_root.join(PY_ROOT).join("confluent_kafka")];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n != "__pycache__") {
                    stack.push(path);
                }
                continue;
            }
            let is_source = path.extension().is_some_and(|e| e == "py" || e == "pyi");
            if !is_source || expected.contains(&path) {
                continue;
            }
            let text = fs::read_to_string(&path)?;
            // Hand-written package inits carry only a spliced block, not the
            // file marker.
            if text.lines().take(25).any(|l| l.contains(GENERATED_MARKER)) {
                orphans.push(path);
            }
        }
    }
    orphans.sort();
    Ok(orphans)
}

/// The generated packages (directories with a generated `__init__.py`) that
/// `pyproject.toml`'s hand-kept `packages` list lacks, so the wheel would
/// leave them out.
fn unlisted_packages(repo_root: &Path, outputs: &[(PathBuf, String)]) -> anyhow::Result<Vec<String>> {
    let pyproject = fs::read_to_string(repo_root.join(PY_ROOT).join("pyproject.toml"))?;
    let root = repo_root.join(PY_ROOT);
    let mut missing = Vec::new();
    for (path, _) in outputs {
        if path.file_name().is_none_or(|n| n != "__init__.py") {
            continue;
        }
        let Some(dir) = path.parent() else { continue };
        let Ok(rel) = dir.strip_prefix(&root) else { continue };
        let package = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join(".");
        if !pyproject.contains(&format!("\"{package}\"")) {
            missing.push(package);
        }
    }
    missing.sort();
    missing.dedup();
    Ok(missing)
}

/// Fail when a generated file no longer matches what the Java sources + FFI enum
/// would produce.
pub fn check_up_to_date(repo_root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let outputs = generated_outputs(repo_root)?;
    let mut stale = Vec::new();
    for (path, expected) in &outputs {
        match fs::read_to_string(path) {
            Ok(actual) if &actual == expected => {},
            _ => stale.push(path.clone()),
        }
    }
    // Output left behind for a renamed or dropped Java class is stale too.
    stale.extend(orphaned_outputs(repo_root, &outputs)?);
    // So is a generated package the wheel would leave out.
    stale.extend(
        unlisted_packages(repo_root, &outputs)?
            .into_iter()
            .map(|p| PathBuf::from(format!("{PY_ROOT}/pyproject.toml (package {p} not listed)"))),
    );
    Ok(stale)
}

/// Every `…Exception` (plus the two suffix-less exception) class declared under
/// `common/` and `clients/`, with its Java `abstract` flag. This is the "all Java
/// exceptions" universe the fail-closed check runs against.
fn scan_exception_classes(repo_root: &Path) -> anyhow::Result<Vec<(String, bool)>> {
    // The two Kafka exception classes whose name does not end in `Exception`.
    const SUFFIXLESS: &[&str] = &["InvalidRegularExpression", "OffsetMetadataTooLarge"];
    let mut out = Vec::new();
    for top in ["common", "clients"] {
        let root = repo_root.join(JAVA_ROOT).join("org/apache/kafka").join(top);
        scan_dir(&root, &mut out, SUFFIXLESS)?;
    }
    Ok(out)
}

/// Recursively collect exception classes from `.java` files under `dir`.
fn scan_dir(dir: &Path, out: &mut Vec<(String, bool)>, suffixless: &[&str]) -> anyhow::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            scan_dir(&path, out, suffixless)?;
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("java") {
            continue;
        }
        let txt = fs::read_to_string(&path)?;
        // `public [abstract] class <Name> extends <Parent>` — a top-level class
        // declaration. (`\n    public` inner classes are not matched: an exception
        // class is always top-level here.)
        let Some(caps) = find_class_decl(&txt) else {
            continue;
        };
        let (is_abstract, name) = caps;
        let is_exception = name.ends_with("Exception") || suffixless.contains(&name.as_str());
        if !is_exception {
            continue;
        }
        let package = txt
            .lines()
            .find_map(|l| l.trim().strip_prefix("package ").map(|p| p.trim_end_matches(';').trim()))
            .ok_or_else(|| anyhow::anyhow!("{}: no package declaration", path.display()))?;
        out.push((format!("{package}.{name}"), is_abstract));
    }
    Ok(())
}

/// Return `(is_abstract, class_name)` for a `public [abstract] class X extends …`
/// declaration, or `None`. Anchored on `public class` / `public abstract class`
/// / `public final class` so a stray ` class ` in a javadoc comment (e.g.
/// `KafkaException`'s "The base class of all other Kafka exceptions") is not
/// mistaken for the declaration.
fn find_class_decl(txt: &str) -> Option<(bool, String)> {
    // Try the abstract form first so `is_abstract` is set correctly.
    for (needle, is_abstract) in [
        ("public abstract class ", true),
        ("public final class ", false),
        ("public class ", false),
    ] {
        if let Some(idx) = txt.find(needle) {
            let after = &txt[idx + needle.len()..];
            // Every Kafka exception extends something; ignore declarations without it.
            if !after.contains(" extends ") {
                continue;
            }
            let name: String = after.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
            if !name.is_empty() {
                return Some((is_abstract, name));
            }
        }
    }
    None
}

/// The fail-closed universe check (Critic 64 F1): every scanned exception class is
/// accounted for, and the abstract set is derived from Java, not trusted.
fn validate_exception_scan(repo_root: &Path, classes: &BTreeMap<String, ClassInfo>) -> anyhow::Result<()> {
    let scanned = scan_exception_classes(repo_root)?;
    let scanned_fqns: BTreeSet<&str> = scanned.iter().map(|(f, _)| f.as_str()).collect();
    let exclusions: BTreeSet<&str> = EXCLUSIONS.iter().copied().collect();
    let abstract_list: BTreeSet<&str> = ABSTRACT_CLASSES.iter().copied().collect();

    // (a) No stale exclusion: every EXCLUSIONS entry must be a real scanned class.
    for ex in &exclusions {
        if !scanned_fqns.contains(ex) {
            anyhow::bail!(
                "error hierarchy: EXCLUSIONS lists `{ex}`, which the exception-class scan does not \
                 find — remove the stale exclusion"
            );
        }
    }

    // (b) Abstract set derived from Java: every scanned `abstract` in-scope class is
    //     in ABSTRACT_CLASSES, and every ABSTRACT_CLASSES entry is really `abstract`.
    let scanned_abstract: BTreeSet<&str> = scanned
        .iter()
        .filter(|(f, ab)| *ab && !exclusions.contains(f.as_str()))
        .map(|(f, _)| f.as_str())
        .collect();
    for a in &scanned_abstract {
        if !abstract_list.contains(a) {
            anyhow::bail!(
                "error hierarchy: `{a}` is a Java `abstract` exception in scope but is not in \
                 ABSTRACT_CLASSES — add it (it is a catch-only base with no FFI id)"
            );
        }
    }
    for a in &abstract_list {
        if !scanned_abstract.contains(a) {
            anyhow::bail!(
                "error hierarchy: ABSTRACT_CLASSES lists `{a}`, which the scan does not find as an \
                 in-scope Java `abstract` exception — it is not abstract, out of scope, or renamed"
            );
        }
    }

    // (c) Every scanned class is covered: in the built graph, an intermediate/base,
    //     or an explicit exclusion. This is what makes a new Java exception (or a
    //     concrete one the core forgot to give an FFI id) fail the build instead of
    //     being silently invisible.
    for (fqn, _is_abstract) in &scanned {
        let covered = classes.contains_key(fqn) || exclusions.contains(fqn.as_str());
        if !covered {
            anyhow::bail!(
                "error hierarchy: Java exception `{fqn}` is neither in the BRIDGE, an abstract \
                 base, KafkaException, nor an explicit EXCLUSION. If it is a public client \
                 error it needs an FFI id + BRIDGE row; otherwise add it to EXCLUSIONS with a \
                 reason."
            );
        }
    }

    Ok(())
}

/// Extract the enumerator `(name, value)` pairs of `kafka_common_ErrorCode_t`
/// (names without the `kafka_common_ErrorCode_` prefix) from the FFI source.
fn parse_ffi_enum_ids(repo_root: &Path) -> anyhow::Result<BTreeMap<String, i32>> {
    let src = fs::read_to_string(repo_root.join(FFI_ENUM_SOURCE))?;
    let body = src
        .split_once("pub enum kafka_common_ErrorCode_t {")
        .map(|(_, rest)| rest)
        .ok_or_else(|| anyhow::anyhow!("{FFI_ENUM_SOURCE}: kafka_common_ErrorCode_t not found"))?;
    let body = body.split_once("\n}").map(|(b, _)| b).unwrap_or(body);
    let mut ids = BTreeMap::new();
    for line in body.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("kafka_common_ErrorCode_") {
            if let Some((name, value)) = rest.split_once(" = ") {
                let value: i32 = value.trim_end_matches(',').trim().parse()?;
                ids.insert(name.to_string(), value);
            }
        }
    }
    if ids.is_empty() {
        anyhow::bail!("{FFI_ENUM_SOURCE}: kafka_common_ErrorCode_t has no enumerators");
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_root() -> PathBuf {
        // xtask/ is a workspace member; the repo root is its parent.
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
    }

    fn model_of(classes: &BTreeMap<String, ClassInfo>, fqn: &str) -> PyModel {
        let g = Graph { classes };
        build_model(&g, &classes[fqn]).unwrap()
    }

    fn param<'m>(m: &'m PyModel, name: &str) -> &'m PyParam {
        m.params.iter().find(|p| p.name == name).unwrap()
    }

    #[test]
    fn names_and_modules() {
        assert_eq!(python_name("TopicAuthorizationException"), "TopicAuthorizationError");
        assert_eq!(python_name("InvalidRegularExpression"), "InvalidRegularExpression");
        assert_eq!(python_name("OffsetMetadataTooLarge"), "OffsetMetadataTooLarge");
        assert_eq!(snake("TopicAuthorizationError"), "topic_authorization_error");
        assert_eq!(snake("throttleTimeMs"), "throttle_time_ms");
        assert_eq!(snake("SslAuthenticationError"), "ssl_authentication_error");
        assert_eq!(py_module_for("org.apache.kafka.common.errors"), "confluent_kafka.common.errors");
        assert_eq!(py_module_for("org.apache.kafka.clients.producer"), "confluent_kafka.producer");
        assert_eq!(py_module_for("org.apache.kafka.common"), "confluent_kafka.common");
        assert_eq!(py_module_for("java.lang"), "confluent_kafka");
        assert_eq!(getter_name("getMostSignificantBits"), "most_significant_bits");
        assert_eq!(getter_name("topicPartition"), "topic_partition");
    }

    #[test]
    fn merge_order_follows_the_rules_examples() {
        let v = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        // subscribe(topics), (topics, callback), (pattern, callback), (pattern)
        let forms = vec![
            v(&["topics"]),
            v(&["topics", "callback"]),
            v(&["pattern", "callback"]),
            v(&["pattern"]),
        ];
        assert_eq!(merge_order(&forms).unwrap(), v(&["topics", "pattern", "callback"]));
        // TopicIdPartition(topicId, topicPartition), (topicId, partition, topic)
        let forms = vec![
            v(&["topic_id", "topic_partition"]),
            v(&["topic_id", "partition", "topic"]),
        ];
        assert_eq!(
            merge_order(&forms).unwrap(),
            v(&["topic_id", "topic_partition", "partition", "topic"])
        );
    }

    #[test]
    fn graph_builds_and_cross_checks() {
        let classes = build_graph(&repo_root()).expect("graph must build and cross-check");
        // 161 FFI ids (one class each) + 5 abstract + KafkaException + 2 id-less built-ins.
        assert_eq!(classes.len(), 169);
        assert_eq!(classes.values().filter(|c| c.is_abstract).count(), 5);
        let with_id = classes.values().filter(|c| c.ffi_id.is_some()).count();
        assert_eq!(with_id, 162, "161 bridge classes plus the base's UNKNOWN_SERVER_ERROR");
        for c in classes.values() {
            assert_eq!(c.is_abstract, c.ffi_id.is_none() && !c.is_jdk, "{}", c.java_fqn);
        }
    }

    #[test]
    fn extends_chains_follow_java() {
        let classes = build_graph(&repo_root()).unwrap();
        let g = Graph { classes: &classes };
        let base = |fqn: &str| py_base(&g, &classes[fqn]);
        assert_eq!(
            base("org.apache.kafka.common.errors.TopicAuthorizationException"),
            "AuthorizationError"
        );
        assert_eq!(base("org.apache.kafka.clients.consumer.CommitFailedException"), "KafkaError");
        assert_eq!(base("org.apache.kafka.common.KafkaException"), "RuntimeError");
        assert_eq!(base("java.util.concurrent.TimeoutException"), "builtins.TimeoutError");
        assert_eq!(
            base("org.apache.kafka.common.requests.CorrelationIdMismatchException"),
            "IllegalStateError"
        );
    }

    #[test]
    fn topic_authorization_model() {
        let classes = build_graph(&repo_root()).unwrap();
        let m = model_of(&classes, "org.apache.kafka.common.errors.TopicAuthorizationException");
        assert!(m.decorated);
        assert_eq!(m.forms[0].params, vec!["message", "unauthorized_topics"]);
        // (String message) passes Collections.emptySet() to (message, topics).
        assert_eq!(m.forms[0].defaults.get("unauthorized_topics").map(String::as_str), Some("()"));
        assert_eq!(param(&m, "message").default, Dflt::NoneDefault);
        assert_eq!(param(&m, "unauthorized_topics").default, Dflt::Unset);
        assert_eq!(
            m.forms[1].effect.message,
            "\"Not authorized to access topics: \" + java_str(unauthorized_topics)"
        );
    }

    #[test]
    fn config_exception_message_parameter() {
        let classes = build_graph(&repo_root()).unwrap();
        let m = model_of(&classes, "org.apache.kafka.common.config.ConfigException");
        let names: Vec<&str> = m.params.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["name", "value", "message"]);
        assert_eq!(param(&m, "message").default, Dflt::Unset);
        assert_eq!(m.forms[2].defaults.get("message").map(String::as_str), Some("None"));
    }

    #[test]
    fn field_assigned_constants_are_signature_defaults_not_match_defaults() {
        let classes = build_graph(&repo_root()).unwrap();
        // (String message) assigns nothing and passes nothing to (int, String):
        // the field default 0 is no Java-given default for matching, so the
        // two forms are matched strictly and throttle_time_ms is UNSET.
        let m = model_of(&classes, "org.apache.kafka.common.errors.ThrottlingQuotaExceededException");
        assert_eq!(param(&m, "throttle_time_ms").default, Dflt::Unset);
        assert!(m.decorated);
        let m = model_of(&classes, "org.apache.kafka.common.errors.RecordDeserializationException");
        let full = &m.forms[1];
        // The deprecated constructor assigns the fields itself; it passes
        // nothing to the full one, so no parameter of the full one may be left
        // out (CLAUDE.md, Signatures: "Only such values are O's Java-given
        // defaults").
        assert!(full.defaults.is_empty());
        assert_eq!(full.field_defaults.get("origin").map(String::as_str), Some("None"));
        assert_eq!(full.field_defaults.get("timestamp").map(String::as_str), Some("-1"));
        assert_eq!(
            full.field_defaults.get("timestamp_type").map(String::as_str),
            Some("TimestampType.NO_TIMESTAMP_TYPE")
        );
        // A header parameter defaults to ().
        assert_eq!(full.field_defaults.get("headers").map(String::as_str), Some("()"));
        assert!(m.decorated);
        for p in [
            "origin",
            "timestamp",
            "timestamp_type",
            "key_buffer",
            "value_buffer",
            "headers",
        ] {
            assert_eq!(param(&m, p).default, Dflt::Unset, "{p}");
        }
        assert!(param(&m, "key_buffer").field_nullable);
        assert!(m.forms[0].deprecated.as_deref().unwrap().starts_with("Since 3.9."));
    }

    #[test]
    fn orphaned_outputs_and_unlisted_packages_are_found() {
        let root = std::env::temp_dir().join(format!("xtask-orphans-{}", std::process::id()));
        let errors = root.join(PY_ROOT).join("confluent_kafka/common/errors");
        fs::create_dir_all(&errors).unwrap();
        let kept = errors.join("kept_error.py");
        let gone = errors.join("gone_error.pyi");
        let hand = errors.join("_helper.py");
        fs::write(&kept, format!("\"\"\"x\n\n{GENERATED_MARKER}.\n\"\"\"\n")).unwrap();
        fs::write(&gone, format!("# {GENERATED_MARKER} (stub)\n")).unwrap();
        fs::write(&hand, "\"\"\"Hand-written.\"\"\"\n").unwrap();
        let init = errors.join("__init__.py");
        let outputs = vec![(kept.clone(), String::new()), (init, String::new())];
        assert_eq!(orphaned_outputs(&root, &outputs).unwrap(), vec![gone]);
        fs::write(root.join(PY_ROOT).join("pyproject.toml"), "packages = [\"confluent_kafka\"]\n").unwrap();
        assert_eq!(
            unlisted_packages(&root, &outputs).unwrap(),
            vec!["confluent_kafka.common.errors"]
        );
        fs::write(
            root.join(PY_ROOT).join("pyproject.toml"),
            "packages = [\"confluent_kafka\", \"confluent_kafka.common.errors\"]\n",
        )
        .unwrap();
        assert!(unlisted_packages(&root, &outputs).unwrap().is_empty());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn check_up_to_date_on_the_real_tree() {
        let stale = check_up_to_date(&repo_root()).unwrap();
        assert!(stale.is_empty(), "stale generated files: {stale:?}");
    }
}
