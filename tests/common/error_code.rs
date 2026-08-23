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

//! Error-code constants -- GENERATED, DO NOT EDIT.
//!
//! Generated from `kafka_common_ErrorCode_t` in `src/ffi/common.rs` by
//! `cargo xtask generate-error-codes`, and checked for staleness by
//! `cargo xtask check-generated`.
//!
//! The multilanguage harness decodes a proto `KafkaError` back into an
//! [`Error`](confluent_kafka::common::Error) by its code, and cannot use the
//! enum itself: `src/ffi` is behind the `ffi` feature, which the multilanguage
//! test targets do not enable.
//!
//! Values are the FFI error codes: Java's wire codes at Java's own values, plus
//! negatives for the classes only the client raises. They are injective over
//! the error classes, so the code alone identifies the class.

pub const UNKNOWN_SERVER_ERROR: i32 = -1;
pub const NONE: i32 = 0;
pub const OFFSET_OUT_OF_RANGE: i32 = 1;
pub const CORRUPT_MESSAGE: i32 = 2;
pub const UNKNOWN_TOPIC_OR_PARTITION: i32 = 3;
pub const INVALID_FETCH_SIZE: i32 = 4;
pub const LEADER_NOT_AVAILABLE: i32 = 5;
pub const NOT_LEADER_OR_FOLLOWER: i32 = 6;
pub const REQUEST_TIMED_OUT: i32 = 7;
pub const BROKER_NOT_AVAILABLE: i32 = 8;
pub const REPLICA_NOT_AVAILABLE: i32 = 9;
pub const MESSAGE_TOO_LARGE: i32 = 10;
pub const STALE_CONTROLLER_EPOCH: i32 = 11;
pub const OFFSET_METADATA_TOO_LARGE: i32 = 12;
pub const NETWORK_ERROR: i32 = 13;
pub const COORDINATOR_LOAD_IN_PROGRESS: i32 = 14;
pub const COORDINATOR_NOT_AVAILABLE: i32 = 15;
pub const NOT_COORDINATOR: i32 = 16;
pub const INVALID_TOPIC_ERROR: i32 = 17;
pub const RECORD_LIST_TOO_LARGE: i32 = 18;
pub const NOT_ENOUGH_REPLICAS: i32 = 19;
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i32 = 20;
pub const INVALID_REQUIRED_ACKS: i32 = 21;
pub const ILLEGAL_GENERATION: i32 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i32 = 23;
pub const INVALID_GROUP_ID: i32 = 24;
pub const UNKNOWN_MEMBER_ID: i32 = 25;
pub const INVALID_SESSION_TIMEOUT: i32 = 26;
pub const REBALANCE_IN_PROGRESS: i32 = 27;
pub const INVALID_COMMIT_OFFSET_SIZE: i32 = 28;
pub const TOPIC_AUTHORIZATION_FAILED: i32 = 29;
pub const GROUP_AUTHORIZATION_FAILED: i32 = 30;
pub const CLUSTER_AUTHORIZATION_FAILED: i32 = 31;
pub const INVALID_TIMESTAMP: i32 = 32;
pub const UNSUPPORTED_SASL_MECHANISM: i32 = 33;
pub const ILLEGAL_SASL_STATE: i32 = 34;
pub const UNSUPPORTED_VERSION: i32 = 35;
pub const TOPIC_ALREADY_EXISTS: i32 = 36;
pub const INVALID_PARTITIONS: i32 = 37;
pub const INVALID_REPLICATION_FACTOR: i32 = 38;
pub const INVALID_REPLICA_ASSIGNMENT: i32 = 39;
pub const INVALID_CONFIG: i32 = 40;
pub const NOT_CONTROLLER: i32 = 41;
pub const INVALID_REQUEST: i32 = 42;
pub const UNSUPPORTED_FOR_MESSAGE_FORMAT: i32 = 43;
pub const POLICY_VIOLATION: i32 = 44;
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i32 = 45;
pub const DUPLICATE_SEQUENCE_NUMBER: i32 = 46;
pub const INVALID_PRODUCER_EPOCH: i32 = 47;
pub const INVALID_TXN_STATE: i32 = 48;
pub const INVALID_PRODUCER_ID_MAPPING: i32 = 49;
pub const INVALID_TRANSACTION_TIMEOUT: i32 = 50;
pub const CONCURRENT_TRANSACTIONS: i32 = 51;
pub const TRANSACTION_COORDINATOR_FENCED: i32 = 52;
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i32 = 53;
pub const SECURITY_DISABLED: i32 = 54;
pub const OPERATION_NOT_ATTEMPTED: i32 = 55;
pub const KAFKA_STORAGE_ERROR: i32 = 56;
pub const LOG_DIR_NOT_FOUND: i32 = 57;
pub const SASL_AUTHENTICATION_FAILED: i32 = 58;
pub const UNKNOWN_PRODUCER_ID: i32 = 59;
pub const REASSIGNMENT_IN_PROGRESS: i32 = 60;
pub const DELEGATION_TOKEN_AUTH_DISABLED: i32 = 61;
pub const DELEGATION_TOKEN_NOT_FOUND: i32 = 62;
pub const DELEGATION_TOKEN_OWNER_MISMATCH: i32 = 63;
pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i32 = 64;
pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i32 = 65;
pub const DELEGATION_TOKEN_EXPIRED: i32 = 66;
pub const INVALID_PRINCIPAL_TYPE: i32 = 67;
pub const NON_EMPTY_GROUP: i32 = 68;
pub const GROUP_ID_NOT_FOUND: i32 = 69;
pub const FETCH_SESSION_ID_NOT_FOUND: i32 = 70;
pub const INVALID_FETCH_SESSION_EPOCH: i32 = 71;
pub const LISTENER_NOT_FOUND: i32 = 72;
pub const TOPIC_DELETION_DISABLED: i32 = 73;
pub const FENCED_LEADER_EPOCH: i32 = 74;
pub const UNKNOWN_LEADER_EPOCH: i32 = 75;
pub const UNSUPPORTED_COMPRESSION_TYPE: i32 = 76;
pub const STALE_BROKER_EPOCH: i32 = 77;
pub const OFFSET_NOT_AVAILABLE: i32 = 78;
pub const MEMBER_ID_REQUIRED: i32 = 79;
pub const PREFERRED_LEADER_NOT_AVAILABLE: i32 = 80;
pub const GROUP_MAX_SIZE_REACHED: i32 = 81;
pub const FENCED_INSTANCE_ID: i32 = 82;
pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: i32 = 83;
pub const ELECTION_NOT_NEEDED: i32 = 84;
pub const NO_REASSIGNMENT_IN_PROGRESS: i32 = 85;
pub const GROUP_SUBSCRIBED_TO_TOPIC: i32 = 86;
pub const INVALID_RECORD: i32 = 87;
pub const UNSTABLE_OFFSET_COMMIT: i32 = 88;
pub const THROTTLING_QUOTA_EXCEEDED: i32 = 89;
pub const PRODUCER_FENCED: i32 = 90;
pub const RESOURCE_NOT_FOUND: i32 = 91;
pub const DUPLICATE_RESOURCE: i32 = 92;
pub const UNACCEPTABLE_CREDENTIAL: i32 = 93;
pub const INCONSISTENT_VOTER_SET: i32 = 94;
pub const INVALID_UPDATE_VERSION: i32 = 95;
pub const FEATURE_UPDATE_FAILED: i32 = 96;
pub const PRINCIPAL_DESERIALIZATION_FAILURE: i32 = 97;
pub const SNAPSHOT_NOT_FOUND: i32 = 98;
pub const POSITION_OUT_OF_RANGE: i32 = 99;
pub const UNKNOWN_TOPIC_ID: i32 = 100;
pub const DUPLICATE_BROKER_REGISTRATION: i32 = 101;
pub const BROKER_ID_NOT_REGISTERED: i32 = 102;
pub const INCONSISTENT_TOPIC_ID: i32 = 103;
pub const INCONSISTENT_CLUSTER_ID: i32 = 104;
pub const TRANSACTIONAL_ID_NOT_FOUND: i32 = 105;
pub const FETCH_SESSION_TOPIC_ID_ERROR: i32 = 106;
pub const INELIGIBLE_REPLICA: i32 = 107;
pub const NEW_LEADER_ELECTED: i32 = 108;
pub const OFFSET_MOVED_TO_TIERED_STORAGE: i32 = 109;
pub const FENCED_MEMBER_EPOCH: i32 = 110;
pub const UNRELEASED_INSTANCE_ID: i32 = 111;
pub const UNSUPPORTED_ASSIGNOR: i32 = 112;
pub const STALE_MEMBER_EPOCH: i32 = 113;
pub const MISMATCHED_ENDPOINT_TYPE: i32 = 114;
pub const UNSUPPORTED_ENDPOINT_TYPE: i32 = 115;
pub const UNKNOWN_CONTROLLER_ID: i32 = 116;
pub const UNKNOWN_SUBSCRIPTION_ID: i32 = 117;
pub const TELEMETRY_TOO_LARGE: i32 = 118;
pub const INVALID_REGISTRATION: i32 = 119;
pub const TRANSACTION_ABORTABLE: i32 = 120;
pub const INVALID_RECORD_STATE: i32 = 121;
pub const SHARE_SESSION_NOT_FOUND: i32 = 122;
pub const INVALID_SHARE_SESSION_EPOCH: i32 = 123;
pub const FENCED_STATE_EPOCH: i32 = 124;
pub const INVALID_VOTER_KEY: i32 = 125;
pub const DUPLICATE_VOTER: i32 = 126;
pub const VOTER_NOT_FOUND: i32 = 127;
pub const INVALID_REGULAR_EXPRESSION: i32 = 128;
pub const REBOOTSTRAP_REQUIRED: i32 = 129;
pub const STREAMS_INVALID_TOPOLOGY: i32 = 130;
pub const STREAMS_INVALID_TOPOLOGY_EPOCH: i32 = 131;
pub const STREAMS_TOPOLOGY_FENCED: i32 = 132;
pub const SHARE_SESSION_LIMIT_REACHED: i32 = 133;
pub const LOCAL_CONCURRENT_MODIFICATION: i32 = -2;
pub const LOCAL_ILLEGAL_ARGUMENT: i32 = -3;
pub const LOCAL_ILLEGAL_STATE: i32 = -4;
pub const LOCAL_TIMEOUT: i32 = -5;
pub const API: i32 = -6;
pub const AUTHENTICATION: i32 = -7;
pub const AUTHORIZER_NOT_READY: i32 = -8;
pub const AUTHORIZATION: i32 = -9;
pub const CONFIG: i32 = -10;
pub const DISCONNECT: i32 = -11;
pub const INTERRUPT: i32 = -12;
pub const INVALID_OFFSET: i32 = -13;
pub const SCHEMA: i32 = -14;
pub const SERIALIZATION: i32 = -15;
pub const SSL_AUTHENTICATION: i32 = -16;
pub const TRANSACTION_ABORTED: i32 = -17;
pub const WAKEUP: i32 = -18;
pub const CONSUMER_COMMIT_FAILED: i32 = -19;
pub const CONSUMER_LOG_TRUNCATION: i32 = -20;
pub const CONSUMER_NO_OFFSET_FOR_PARTITION: i32 = -21;
pub const CONSUMER_OFFSET_OUT_OF_RANGE: i32 = -22;
pub const CONSUMER_RETRIABLE_COMMIT_FAILED: i32 = -23;
pub const CORRELATION_ID_MISMATCH: i32 = -24;
pub const INVALID_RECEIVE: i32 = -25;
pub const QUOTA_VIOLATION: i32 = -26;
pub const RECORD_DESERIALIZATION: i32 = -27;
pub const PRODUCER_BUFFER_EXHAUSTED: i32 = -28;
