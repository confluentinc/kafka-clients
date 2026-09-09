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
//! Reads the Java exception sources (Apache Kafka 4.3.1, under `kafka/`), derives
//! each class's Python name, parent chain, abstractness and module placement, and
//! cross-checks them against the FFI `kafka_common_ErrorCode_t` enum
//! (`src/ffi/common.rs`). Emits static Python source plus `.pyi` stubs into the
//! module mirroring each Java package. Any Java class without an FFI id, FFI id
//! without a Java class, or a differing `extends` chain **fails the build** — Java
//! parity becomes a build-time guarantee (spec §5.5 / Design Decisions D1).
//!
//! The one input that is not derivable from the Java source or the FFI enum alone
//! is the correspondence between an FFI id constant (an error-*code* name such as
//! `REQUEST_TIMED_OUT`) and the Java *class* it identifies (`TimeoutException`):
//! there is no mechanical relation between the two vocabularies (Design Decisions
//! D1, "wire code vs FFI id"). That correspondence is the reviewed [`BRIDGE`]
//! table below, derived from the Rust core's own `Error`-variant → id mapping
//! (`error_code_of` in `src/ffi/common.rs`) and its per-class `//! Translated
//! from` provenance. The build validates that every bridge entry names a real
//! Java class and a real FFI id, so a drift in either direction is caught.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Root of the Java source tree (Apache Kafka 4.3.1), relative to the repo root.
const JAVA_ROOT: &str = "kafka/clients/src/main/java";

/// The FFI enum whose id constants the Python classes carry as `_ffi_id`.
const FFI_ENUM_SOURCE: &str = "src/ffi/common.rs";

/// Output roots (relative to the repo root).
const PKG_ROOT: &str = "bindings/python/confluent_kafka";

/// The five Java **abstract** exception classes on this surface. They are
/// catch-only grouping bases (constructing one raises `TypeError`) and carry no
/// FFI id (Design Decisions D1). Derived from the Java `abstract` modifier; listed
/// here only so the generator knows to pull them into the graph even though no
/// bridge entry (which maps *ids*) references them.
const ABSTRACT_CLASSES: &[&str] = &[
    "org.apache.kafka.common.errors.RetriableException",
    "org.apache.kafka.common.errors.RefreshRetriableException",
    "org.apache.kafka.common.errors.InvalidMetadataException",
    "org.apache.kafka.common.errors.ApplicationRecoverableException",
    "org.apache.kafka.clients.consumer.InvalidOffsetException",
];

/// Java's `KafkaException` — the base of the Kafka side. Not generated: the Python
/// base `KafkaError` is hand-written in `common/errors/_base.py` so the runtime
/// mapping and the generated classes can both import it without a cycle. Every
/// generated class that Java roots at `KafkaException` roots here.
const KAFKA_EXCEPTION_FQN: &str = "org.apache.kafka.common.KafkaException";

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

/// Which generated Python module a class lands in (spec §4 / §5.5).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Module {
    /// `confluent_kafka.common.errors` — the Kafka `common.*` hierarchy.
    CommonErrors,
    /// `confluent_kafka.common.config` — `ConfigException`.
    CommonConfig,
    /// `confluent_kafka.consumer` — the `clients.consumer` exceptions.
    Consumer,
    /// `confluent_kafka` (root) — the JDK-type analogs.
    Root,
}

/// One class in the generated hierarchy.
#[derive(Clone, Debug)]
struct ClassInfo {
    /// Java fully-qualified name (the identity used for graph edges).
    java_fqn: String,
    /// Python class name (`…Error`).
    py_name: String,
    /// FFI id constant name, or `None` for abstract classes.
    ffi_id: Option<String>,
    /// FFI id integer value, or `None` for abstract classes.
    ffi_id_value: Option<i32>,
    /// Whether Java marks the class `abstract`.
    is_abstract: bool,
    /// The parent's Java FQN, or `None` when the parent is a Python builtin
    /// (`RuntimeError`, `builtins.TimeoutError`) rather than a generated class.
    parent_fqn: Option<String>,
    /// The Python base expression to emit (`KafkaError`, `RetriableError`,
    /// `RuntimeError`, `builtins.TimeoutError`, …).
    py_base: String,
    /// The generated module the class lands in.
    module: Module,
}

/// A parsed Java exception source: simple name, package, `abstract` flag, and the
/// resolved parent FQN (via the file's `import`s / same-package rule).
struct JavaClass {
    package: String,
    is_abstract: bool,
    parent_fqn: String,
}

/// Parse one Java exception source file.
fn parse_java(path: &Path) -> anyhow::Result<JavaClass> {
    let txt = fs::read_to_string(path)?;
    let package = txt
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix("package ")
                .map(|p| p.trim_end_matches(';').trim().to_string())
        })
        .ok_or_else(|| anyhow::anyhow!("{}: no package declaration", path.display()))?;

    // `public [abstract] class <Name> extends <Parent>`
    let (is_abstract, parent_simple) = {
        // Find "class <X> extends <Y>" allowing an optional "abstract" before class.
        let idx = txt
            .find(" class ")
            .ok_or_else(|| anyhow::anyhow!("{}: no class declaration", path.display()))?;
        // Look back a little for "abstract".
        let prefix_start = idx.saturating_sub(20);
        let is_abstract = txt[prefix_start..idx].contains("abstract");
        // After " class ", read the tokens: <Name> [<...>] extends <Parent>
        let after = &txt[idx + " class ".len()..];
        let extends_at = after
            .find(" extends ")
            .ok_or_else(|| anyhow::anyhow!("{}: class has no `extends`", path.display()))?;
        // Skip any extra whitespace after `extends` (some sources double-space).
        let rest = after[extends_at + " extends ".len()..].trim_start();
        let parent_simple: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
        (is_abstract, parent_simple)
    };

    // Resolve the parent's FQN: an explicit import wins; otherwise a `java.lang`
    // builtin; otherwise same package.
    let parent_fqn = if let Some(imp) = txt.lines().find_map(|l| {
        let l = l.trim();
        l.strip_prefix("import ")
            .map(|i| i.trim_end_matches(';').trim())
            .filter(|i| i.rsplit('.').next() == Some(parent_simple.as_str()))
            .map(|i| i.to_string())
    }) {
        imp
    } else if matches!(
        parent_simple.as_str(),
        "RuntimeException" | "Exception" | "Throwable" | "IllegalStateException" | "IllegalArgumentException"
    ) {
        format!("java.lang.{parent_simple}")
    } else {
        format!("{package}.{parent_simple}")
    };

    Ok(JavaClass { package, is_abstract, parent_fqn })
}

/// The Python class name for a Java simple class name: strip a trailing
/// `Exception` and append `Error` (`TopicAuthorizationException` →
/// `TopicAuthorizationError`); a name without the suffix just gets `Error`
/// (`InvalidRegularExpression` → `InvalidRegularExpressionError`,
/// `OffsetMetadataTooLarge` → `OffsetMetadataTooLargeError`).
fn python_name(java_simple: &str) -> String {
    let stem = java_simple.strip_suffix("Exception").unwrap_or(java_simple);
    format!("{stem}Error")
}

/// The module a class lands in, from its Java package.
fn module_for(package: &str) -> Module {
    match package {
        "org.apache.kafka.common.config" => Module::CommonConfig,
        "org.apache.kafka.clients.consumer" => Module::Consumer,
        p if p.starts_with("java.") => Module::Root,
        // Every other Kafka class descends from `KafkaException`; the spec's
        // module table gives `common.errors` as the home for the Kafka common
        // hierarchy, so the handful of classes in sibling `common.*` subpackages
        // (and `clients.producer.BufferExhaustedException`) land there too. See
        // the P1 clarification.
        _ => Module::CommonErrors,
    }
}

/// A path under the Java source tree for a fully-qualified class name.
fn java_path(repo_root: &Path, fqn: &str) -> PathBuf {
    let mut p = repo_root.join(JAVA_ROOT);
    for seg in fqn.split('.') {
        p.push(seg);
    }
    p.set_extension("java");
    p
}

/// Build the complete class graph and cross-check it against the FFI enum.
fn build_graph(repo_root: &Path) -> anyhow::Result<Vec<ClassInfo>> {
    // 1. Cross-check the bridge against the FFI enum: every bridge id must be a
    //    real enum constant, and every non-`NONE` enum constant must be named by
    //    exactly one bridge row.
    let enum_id_values = parse_ffi_enum_ids(repo_root)?;
    let enum_ids: BTreeSet<String> = enum_id_values.keys().cloned().collect();
    let bridge_ids: BTreeSet<&str> = BRIDGE.iter().map(|(id, _)| *id).collect();
    if bridge_ids.len() != BRIDGE.len() {
        anyhow::bail!("error hierarchy: the BRIDGE table has a duplicate FFI id");
    }
    for (id, fqn) in BRIDGE {
        if !enum_ids.contains(*id) {
            anyhow::bail!(
                "error hierarchy: BRIDGE names FFI id `{id}` (for `{fqn}`) that is not in \
                 kafka_common_ErrorCode_t ({FFI_ENUM_SOURCE})"
            );
        }
    }
    for id in &enum_ids {
        if id == "NONE" {
            continue; // the no-error sentinel has no exception class
        }
        if !bridge_ids.contains(id.as_str()) {
            anyhow::bail!(
                "error hierarchy: FFI id `{id}` has no class in the BRIDGE table — every \
                 kafka_common_ErrorCode_t id except NONE must map to one Java class"
            );
        }
    }

    // 2. Collect every FQN we must parse: the bridge classes plus the abstract
    //    classes. Non-JDK classes get parsed for extends/abstract; JDK classes are
    //    described from the bridge alone (no Kafka source, deliberate JDK analogs).
    let mut fqn_to_id: BTreeMap<String, String> = BTreeMap::new();
    for (id, fqn) in BRIDGE {
        if fqn_to_id.insert((*fqn).to_string(), (*id).to_string()).is_some() {
            anyhow::bail!("error hierarchy: two FFI ids map to the same Java class `{fqn}`");
        }
    }

    let mut classes: BTreeMap<String, ClassInfo> = BTreeMap::new();

    // JDK analogs (root module). Their Python base is a builtin.
    for (id, fqn) in BRIDGE {
        if !fqn.starts_with("java.") {
            continue;
        }
        let java_simple = fqn.rsplit('.').next().unwrap().to_string();
        let py_name = python_name(&java_simple);
        let py_base = jdk_python_base(&java_simple);
        classes.insert(
            (*fqn).to_string(),
            ClassInfo {
                java_fqn: (*fqn).to_string(),
                py_name,
                ffi_id: Some((*id).to_string()),
                ffi_id_value: Some(enum_id_values[*id]),
                is_abstract: false,
                parent_fqn: None,
                py_base,
                module: Module::Root,
            },
        );
    }

    // Parse every non-JDK bridge class and every abstract class.
    let mut to_parse: BTreeSet<String> = BRIDGE
        .iter()
        .map(|(_, f)| (*f).to_string())
        .filter(|f| !f.starts_with("java."))
        .collect();
    for a in ABSTRACT_CLASSES {
        to_parse.insert((*a).to_string());
    }

    for fqn in &to_parse {
        let jc = parse_java(&java_path(repo_root, fqn))?;
        let java_simple = fqn.rsplit('.').next().unwrap().to_string();
        let ffi_id = fqn_to_id.get(fqn).cloned();

        // Cross-check abstractness against the FFI id: abstract ⇒ no id, and every
        // non-abstract bridge class ⇒ has an id (guaranteed by construction).
        if let (true, Some(id)) = (jc.is_abstract, ffi_id.as_ref()) {
            anyhow::bail!(
                "error hierarchy: `{fqn}` is Java-abstract yet the BRIDGE gives it FFI id \
                 `{id}` — abstract classes are catch-only and carry no id"
            );
        }
        if !jc.is_abstract && ffi_id.is_none() {
            anyhow::bail!(
                "error hierarchy: `{fqn}` is a concrete Java class with no FFI id — every \
                 concrete exception class must appear in the BRIDGE"
            );
        }

        classes.insert(
            fqn.clone(),
            ClassInfo {
                java_fqn: fqn.clone(),
                py_name: python_name(&java_simple),
                ffi_id_value: ffi_id.as_ref().map(|id| enum_id_values[id.as_str()]),
                ffi_id,
                is_abstract: jc.is_abstract,
                parent_fqn: Some(jc.parent_fqn.clone()),
                py_base: String::new(), // filled once all classes are known
                module: module_for(&jc.package),
            },
        );
    }

    // 3. Resolve each class's Python base expression from its parent FQN.
    let known: BTreeMap<String, ClassInfo> = classes.clone();
    for info in classes.values_mut() {
        if info.parent_fqn.is_none() {
            continue; // JDK analogs already have their builtin base
        }
        let parent = info.parent_fqn.as_ref().unwrap();
        info.py_base = if parent == KAFKA_EXCEPTION_FQN {
            // Java's `KafkaException` — the Python base `KafkaError`, hand-written
            // in `common/errors/_base.py`.
            "KafkaError".to_string()
        } else if let Some(pc) = known.get(parent) {
            // Any other parent — a generated Kafka class, or a JDK analog that is
            // itself a generated class (e.g. `CorrelationIdMismatchException
            // extends IllegalStateException` → the root `IllegalStateError`).
            pc.py_name.clone()
        } else {
            anyhow::bail!(
                "error hierarchy: `{}` extends `{}`, which is neither KafkaException nor a known \
                 generated class — extend the generator",
                info.java_fqn,
                parent
            );
        };
    }

    Ok(classes.into_values().collect())
}

/// The Python builtin base for a JDK analog (spec §5.5 API-misuse table).
fn jdk_python_base(java_simple: &str) -> String {
    match java_simple {
        "TimeoutException" => "builtins.TimeoutError".to_string(),
        // IllegalStateException, IllegalArgumentException,
        // ConcurrentModificationException all extend RuntimeException in Java →
        // Python's RuntimeError.
        _ => "RuntimeError".to_string(),
    }
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

// ----------------------------------------------------------------------------
// Emission
// ----------------------------------------------------------------------------

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

/// Emit one class's `.py` body (source form).
fn emit_class_py(info: &ClassInfo) -> String {
    let mut s = String::new();
    s.push_str(&format!("class {}({}):\n", info.py_name, info.py_base));
    s.push_str(&format!("    \"\"\"Mirrors Java's ``{}``.\"\"\"\n", info.java_fqn));
    if info.is_abstract {
        // Guard construction of the abstract base itself, but let concrete
        // subclasses (which inherit this ``__init__``) construct normally.
        s.push_str("\n    def __init__(self, *args: object) -> None:\n");
        s.push_str(&format!("        if type(self) is {}:\n", info.py_name));
        s.push_str(&format!(
            "            raise TypeError(\n                \"{} is an abstract catch-only base; it is never raised directly\"\n            )\n",
            info.py_name
        ));
        s.push_str("        super().__init__(*args)\n");
    } else {
        let id = info.ffi_id.as_ref().unwrap();
        let value = info.ffi_id_value.unwrap();
        s.push_str(&format!(
            "\n    _ffi_id: ClassVar[int] = {value}  # kafka_common_ErrorCode_{id}\n"
        ));
    }
    s
}

/// Emit one class's `.pyi` stub.
fn emit_class_pyi(info: &ClassInfo) -> String {
    let mut s = String::new();
    s.push_str(&format!("class {}({}):\n", info.py_name, info.py_base));
    if info.is_abstract {
        s.push_str("    def __init__(self, *args: object) -> None: ...\n");
    } else {
        s.push_str("    _ffi_id: ClassVar[int]\n");
    }
    s
}

/// Topologically order classes within a module so a subclass follows its base.
/// Classes whose base is not in the same module (a builtin or a cross-module
/// import) have no in-module dependency.
fn order_module<'a>(classes: &'a [&'a ClassInfo]) -> Vec<&'a ClassInfo> {
    let names: BTreeSet<&str> = classes.iter().map(|c| c.py_name.as_str()).collect();
    let mut placed: BTreeSet<&str> = BTreeSet::new();
    let mut ordered: Vec<&ClassInfo> = Vec::new();
    // Deterministic: repeatedly place every class whose in-module base is already
    // placed, scanning in name order.
    let mut remaining: Vec<&ClassInfo> = classes.to_vec();
    remaining.sort_by(|a, b| a.py_name.cmp(&b.py_name));
    while !remaining.is_empty() {
        let mut progressed = false;
        let mut next_remaining = Vec::new();
        for c in remaining {
            let base_in_module = names.contains(c.py_base.as_str());
            if !base_in_module || placed.contains(c.py_base.as_str()) {
                ordered.push(c);
                placed.insert(c.py_name.as_str());
                progressed = true;
            } else {
                next_remaining.push(c);
            }
        }
        remaining = next_remaining;
        if !progressed {
            // A cycle should be impossible (Java has no exception-class cycles);
            // emit the rest in name order rather than looping forever.
            ordered.append(&mut remaining);
        }
    }
    ordered
}

/// The import path of a generated module (where its classes are defined).
fn module_import_path(module: Module) -> &'static str {
    match module {
        Module::CommonErrors => "confluent_kafka.common.errors._generated",
        Module::CommonConfig => "confluent_kafka.common.config._generated_errors",
        Module::Consumer => "confluent_kafka.consumer._generated_errors",
        Module::Root => "confluent_kafka._generated_errors",
    }
}

/// Cross-module imports a generated module needs, as `(module_path, name)`.
///
/// `all_classes` is the whole graph, used to find which module a base class lives
/// in when it is not local to this file.
fn module_imports(classes: &[&ClassInfo], all_classes: &[ClassInfo]) -> Vec<(String, String)> {
    let local: BTreeSet<&str> = classes.iter().map(|c| c.py_name.as_str()).collect();
    let by_name: BTreeMap<&str, &ClassInfo> = all_classes.iter().map(|c| (c.py_name.as_str(), c)).collect();
    let mut imports: BTreeSet<(String, String)> = BTreeSet::new();
    for c in classes {
        let base = c.py_base.as_str();
        if local.contains(base) {
            continue; // defined in this file
        }
        match base {
            "RuntimeError" | "object" => {},
            "builtins.TimeoutError" => {}, // handled by an `import builtins`
            "KafkaError" => {
                // The hand-written base always lives in `common/errors/_base.py`.
                imports.insert(("confluent_kafka.common.errors._base".to_string(), "KafkaError".to_string()));
            },
            other => {
                // A generated class defined in another module. Route by its module.
                let base_module = by_name
                    .get(other)
                    .map(|c| c.module)
                    .expect("a non-local, non-builtin base must be a generated class");
                imports.insert((module_import_path(base_module).to_string(), other.to_string()));
            },
        }
    }
    imports.into_iter().collect()
}

/// Render one generated module's `.py` source.
fn render_module_py(module: Module, classes: &[&ClassInfo], all_classes: &[ClassInfo]) -> String {
    let ordered = order_module(classes);
    let mut s = String::new();
    s.push_str(PY_HEADER);
    s.push('\n');
    s.push_str(&format!("\"\"\"{}\n\n", module_docstring(module)));
    s.push_str(
        "GENERATED, DO NOT EDIT. Produced from the Java exception sources by\n\
         `cargo xtask generate-error-codes`, cross-checked against the FFI\n\
         `kafka_common_ErrorCode_t` enum, and validated for staleness by\n\
         `cargo xtask check-generated`.\n\"\"\"\n\n",
    );
    s.push_str("from __future__ import annotations\n\n");

    // Imports.
    let needs_builtins = ordered.iter().any(|c| c.py_base == "builtins.TimeoutError");
    if needs_builtins {
        s.push_str("import builtins\n");
    }
    let needs_classvar = ordered.iter().any(|c| !c.is_abstract);
    if needs_classvar {
        s.push_str("from typing import ClassVar\n");
    }
    if needs_builtins || needs_classvar {
        s.push('\n');
    }
    for (path, name) in module_imports(classes, all_classes) {
        s.push_str(&format!("from {path} import {name}\n"));
    }
    s.push('\n');

    // `__all__`.
    s.push_str("__all__ = [\n");
    let mut names: Vec<&str> = ordered.iter().map(|c| c.py_name.as_str()).collect();
    names.sort_unstable();
    for n in &names {
        s.push_str(&format!("    \"{n}\",\n"));
    }
    s.push_str("]\n\n\n");

    // Classes.
    for (i, c) in ordered.iter().enumerate() {
        if i > 0 {
            s.push_str("\n\n");
        }
        s.push_str(&emit_class_py(c));
    }
    s
}

/// Render one generated module's `.pyi` stub.
fn render_module_pyi(classes: &[&ClassInfo], all_classes: &[ClassInfo]) -> String {
    let ordered = order_module(classes);
    let mut s = String::new();
    s.push_str(PY_HEADER);
    s.push('\n');
    s.push_str("# GENERATED, DO NOT EDIT (stubs for the generated error hierarchy).\n\n");
    let needs_builtins = ordered.iter().any(|c| c.py_base == "builtins.TimeoutError");
    if needs_builtins {
        s.push_str("import builtins\n");
    }
    let needs_classvar = ordered.iter().any(|c| !c.is_abstract);
    if needs_classvar {
        s.push_str("from typing import ClassVar\n");
    }
    for (path, name) in module_imports(classes, all_classes) {
        s.push_str(&format!("from {path} import {name}\n"));
    }
    s.push('\n');

    // ``__all__`` mirrors the ``.py`` so wildcard re-exports type-check.
    s.push_str("__all__: list[str]\n\n");

    for c in &ordered {
        s.push_str(&emit_class_pyi(c));
        s.push('\n');
    }
    s
}

fn module_docstring(module: Module) -> &'static str {
    match module {
        Module::CommonErrors => "Generated Kafka error hierarchy for ``confluent_kafka.common.errors``.",
        Module::CommonConfig => "Generated config error(s) for ``confluent_kafka.common.config``.",
        Module::Consumer => "Generated consumer-package errors for ``confluent_kafka.consumer``.",
        Module::Root => "Generated JDK-analog errors for the ``confluent_kafka`` root.",
    }
}

/// The output `.py` path for a module.
fn module_py_path(repo_root: &Path, module: Module) -> PathBuf {
    let base = repo_root.join(PKG_ROOT);
    match module {
        Module::CommonErrors => base.join("common/errors/_generated.py"),
        Module::CommonConfig => base.join("common/config/_generated_errors.py"),
        Module::Consumer => base.join("consumer/_generated_errors.py"),
        Module::Root => base.join("_generated_errors.py"),
    }
}

/// The generated files this module owns, so `check-generated` can compare.
pub fn generated_outputs(repo_root: &Path) -> anyhow::Result<Vec<(PathBuf, String)>> {
    let classes = build_graph(repo_root)?;
    let mut outputs = Vec::new();
    for module in [
        Module::Root,
        Module::CommonErrors,
        Module::CommonConfig,
        Module::Consumer,
    ] {
        let in_module: Vec<&ClassInfo> = classes.iter().filter(|c| c.module == module).collect();
        let py_path = module_py_path(repo_root, module);
        let pyi_path = py_path.with_extension("pyi");
        outputs.push((py_path, render_module_py(module, &in_module, &classes)));
        outputs.push((pyi_path, render_module_pyi(&in_module, &classes)));
    }
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
    println!("✅ Wrote {} generated error-hierarchy file(s)", outputs.len());
    Ok(())
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
    Ok(stale)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_root() -> PathBuf {
        // xtask/ is a workspace member; the repo root is its parent.
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
    }

    #[test]
    fn python_name_transforms() {
        assert_eq!(python_name("TopicAuthorizationException"), "TopicAuthorizationError");
        assert_eq!(python_name("CommitFailedException"), "CommitFailedError");
        // Two Java classes lack the `Exception` suffix.
        assert_eq!(python_name("InvalidRegularExpression"), "InvalidRegularExpressionError");
        assert_eq!(python_name("OffsetMetadataTooLarge"), "OffsetMetadataTooLargeError");
    }

    #[test]
    fn module_placement() {
        assert_eq!(module_for("org.apache.kafka.common.errors"), Module::CommonErrors);
        assert_eq!(module_for("org.apache.kafka.common.config"), Module::CommonConfig);
        assert_eq!(module_for("org.apache.kafka.clients.consumer"), Module::Consumer);
        assert_eq!(module_for("java.lang"), Module::Root);
        // Sibling common subpackages fold into common.errors.
        assert_eq!(module_for("org.apache.kafka.common.metrics"), Module::CommonErrors);
        assert_eq!(module_for("org.apache.kafka.clients.producer"), Module::CommonErrors);
    }

    #[test]
    fn bridge_has_no_duplicates() {
        let ids: BTreeSet<&str> = BRIDGE.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids.len(), BRIDGE.len(), "duplicate FFI id in BRIDGE");
        let fqns: BTreeSet<&str> = BRIDGE.iter().map(|(_, f)| *f).collect();
        assert_eq!(fqns.len(), BRIDGE.len(), "duplicate Java class in BRIDGE");
    }

    #[test]
    fn parse_java_topic_authorization() {
        let jc = parse_java(&java_path(
            &repo_root(),
            "org.apache.kafka.common.errors.TopicAuthorizationException",
        ))
        .unwrap();
        assert!(!jc.is_abstract);
        assert_eq!(jc.parent_fqn, "org.apache.kafka.common.errors.AuthorizationException");
        assert_eq!(jc.package, "org.apache.kafka.common.errors");
    }

    #[test]
    fn parse_java_abstract_retriable() {
        let jc = parse_java(&java_path(&repo_root(), "org.apache.kafka.common.errors.RetriableException")).unwrap();
        assert!(jc.is_abstract, "RetriableException is abstract in Java");
        assert_eq!(jc.parent_fqn, "org.apache.kafka.common.errors.ApiException");
    }

    #[test]
    fn parse_java_resolves_jdk_parent_via_import() {
        // ConfigException extends KafkaException (imported).
        let jc = parse_java(&java_path(&repo_root(), "org.apache.kafka.common.config.ConfigException")).unwrap();
        assert_eq!(jc.parent_fqn, "org.apache.kafka.common.KafkaException");

        // CorrelationIdMismatchException extends IllegalStateException (a JDK class).
        let jc = parse_java(&java_path(
            &repo_root(),
            "org.apache.kafka.common.requests.CorrelationIdMismatchException",
        ))
        .unwrap();
        assert_eq!(jc.parent_fqn, "java.lang.IllegalStateException");
    }

    #[test]
    fn graph_builds_and_cross_checks() {
        let classes = build_graph(&repo_root()).expect("graph must build and cross-check");
        // 161 concrete (one per FFI id except NONE) + 5 abstract = 166.
        let concrete = classes.iter().filter(|c| !c.is_abstract).count();
        let abstract_ct = classes.iter().filter(|c| c.is_abstract).count();
        assert_eq!(concrete, 161, "one concrete class per FFI id (except NONE)");
        assert_eq!(abstract_ct, 5, "exactly five abstract catch-only bases");
    }

    #[test]
    fn every_concrete_class_has_ffi_id_and_abstract_none() {
        let classes = build_graph(&repo_root()).unwrap();
        for c in &classes {
            if c.is_abstract {
                assert!(c.ffi_id.is_none(), "{} is abstract but has an ffi id", c.py_name);
            } else {
                assert!(c.ffi_id.is_some(), "{} is concrete but has no ffi id", c.py_name);
            }
        }
    }

    #[test]
    fn known_extends_chains() {
        let classes = build_graph(&repo_root()).unwrap();
        let by_name: BTreeMap<&str, &ClassInfo> = classes.iter().map(|c| (c.py_name.as_str(), c)).collect();
        // TopicAuthorizationError -> AuthorizationError
        assert_eq!(by_name["TopicAuthorizationError"].py_base, "AuthorizationError");
        // NotLeaderOrFollowerError is retriable in Java.
        assert_eq!(by_name["NotLeaderOrFollowerError"].py_base, "InvalidMetadataError");
        // CommitFailedError -> KafkaError (base).
        assert_eq!(by_name["CommitFailedError"].py_base, "KafkaError");
        // RetriableCommitFailedError -> RetriableError (cross-module).
        assert_eq!(by_name["RetriableCommitFailedError"].py_base, "RetriableError");
        // CorrelationIdMismatchError -> the root JDK analog IllegalStateError
        // (which is itself a generated class, so the base is the plain name and a
        // cross-module import brings it in).
        assert_eq!(by_name["CorrelationIdMismatchError"].py_base, "IllegalStateError");
    }
}
