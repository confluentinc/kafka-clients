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

//! C exports of `Error`'s predicates -- GENERATED, DO NOT EDIT.
//!
//! Generated from the predicates on `impl Error` in `src/common/error.rs` by
//! `cargo xtask generate-error-predicates`, and checked for staleness by
//! `cargo xtask check-generated`.
//!
//! One export per predicate, named after it behind the type prefix
//! (`is_retriable_error` -> `kafka_common_Error_is_retriable_error`,
//! CLAUDE.md §4). C cannot see the error's variants, so these predicates are
//! how a C caller classifies an error beyond its numeric code
//! (`kafka_common_Error_code`). Every one is `false` for a null handle.

use super::common::{error_ref, kafka_common_Error_t};

/// Whether this is a Kafka error rather than a generic programming error.
///
/// Mirrors `Error::is_kafka_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_kafka_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_kafka_error()
}

/// Whether this error corresponds to a Java `ApiException`.
///
/// Mirrors `Error::is_api_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_api_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_api_error()
}

/// Whether this error's Java class extends `RetriableException` — i.e. whether re-sending the
/// failed request can succeed.
///
/// Mirrors `Error::is_retriable_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_retriable_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_retriable_error()
}

/// Whether this error's Java class extends `RefreshRetriableException` (CLAUDE.md §12.4) —
/// retriable, and a metadata / coordinator refresh is what clears it.
///
/// Mirrors `Error::is_refresh_retriable_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_refresh_retriable_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_refresh_retriable_error()
}

/// Whether this error's Java class extends `TimeoutException` (CLAUDE.md §12.4).
///
/// Mirrors `Error::is_timeout_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_timeout_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_timeout_error()
}

/// Whether this error's Java class extends `InvalidMetadataException` (CLAUDE.md §12.4) — the
/// client's cached metadata may be stale.
///
/// Mirrors `Error::is_invalid_metadata_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_metadata_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_metadata_error()
}

/// Whether this error's Java class extends `InvalidConfigurationException` (CLAUDE.md §12.4).
///
/// Mirrors `Error::is_invalid_configuration_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_configuration_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_configuration_error()
}

/// Whether this error's Java class extends `ApplicationRecoverableException` (CLAUDE.md §12.4) —
/// the application can recover, but only by re-initialising its producer or rejoining its group;
/// the current epoch or session is gone.
///
/// Mirrors `Error::is_application_recoverable_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_application_recoverable_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_application_recoverable_error()
}

/// Whether this error's Java class extends `InvalidOffsetException` (CLAUDE.md §12.4).
///
/// Mirrors `Error::is_invalid_offset_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_offset_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_offset_error()
}

/// Whether this error's Java class extends `OutOfOrderSequenceException` (CLAUDE.md §12.4).
///
/// Mirrors `Error::is_out_of_order_sequence_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_out_of_order_sequence_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_out_of_order_sequence_error()
}

/// Whether this error's Java class extends
/// `org.apache.kafka.clients.consumer.InvalidOffsetException` (CLAUDE.md §12.4) — no offset is
/// usable for the partition.
///
/// Mirrors `Error::is_consumer_invalid_offset_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_invalid_offset_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_consumer_invalid_offset_error()
}

/// Whether this error's Java class extends
/// `org.apache.kafka.clients.consumer.OffsetOutOfRangeException` (CLAUDE.md §12.4).
///
/// Mirrors `Error::is_consumer_offset_out_of_range_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_offset_out_of_range_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_consumer_offset_out_of_range_error()
}

/// Whether this error's Java class extends `SerializationException` (CLAUDE.md §12.4).
///
/// Mirrors `Error::is_serialization_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_serialization_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_serialization_error()
}

/// Whether this error's Java class extends `AuthenticationException` (CLAUDE.md §12.4).
///
/// Mirrors `Error::is_authentication_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authentication_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_authentication_error()
}

/// Whether this error's Java class extends `AuthorizationException` (CLAUDE.md §12.4).
///
/// Mirrors `Error::is_authorization_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authorization_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_authorization_error()
}

/// Whether this error's Java class is, or extends, `java.lang.IllegalArgumentException`.
///
/// Mirrors `Error::is_local_illegal_argument_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_illegal_argument_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_local_illegal_argument_error()
}

/// Whether this error's Java class is, or extends, `java.lang.IllegalStateException`.
///
/// Mirrors `Error::is_local_illegal_state_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_illegal_state_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_local_illegal_state_error()
}

/// Whether this error's Java class is, or extends, `java.util.ConcurrentModificationException`.
///
/// Mirrors `Error::is_local_concurrent_modification_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_concurrent_modification_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_local_concurrent_modification_error()
}

/// Whether this error's Java class is, or extends, `java.util.concurrent.TimeoutException`.
///
/// Mirrors `Error::is_local_timeout_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_timeout_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_local_timeout_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.AuthorizerNotReadyException`.
///
/// Mirrors `Error::is_authorizer_not_ready_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authorizer_not_ready_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_authorizer_not_ready_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.BrokerIdNotRegisteredException`.
///
/// Mirrors `Error::is_broker_id_not_registered_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_broker_id_not_registered_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_broker_id_not_registered_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.BrokerNotAvailableException`.
///
/// Mirrors `Error::is_broker_not_available_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_broker_not_available_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_broker_not_available_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.producer.BufferExhaustedException`.
///
/// Mirrors `Error::is_producer_buffer_exhausted_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_producer_buffer_exhausted_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_producer_buffer_exhausted_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ClusterAuthorizationException`.
///
/// Mirrors `Error::is_cluster_authorization_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_cluster_authorization_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_cluster_authorization_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ConcurrentTransactionsException`.
///
/// Mirrors `Error::is_concurrent_transactions_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_concurrent_transactions_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_concurrent_transactions_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ControllerMovedException`.
///
/// Mirrors `Error::is_controller_moved_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_controller_moved_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_controller_moved_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.CoordinatorLoadInProgressException`.
///
/// Mirrors `Error::is_coordinator_load_in_progress_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_coordinator_load_in_progress_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_coordinator_load_in_progress_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.CoordinatorNotAvailableException`.
///
/// Mirrors `Error::is_coordinator_not_available_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_coordinator_not_available_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_coordinator_not_available_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.requests.CorrelationIdMismatchException`.
///
/// Mirrors `Error::is_correlation_id_mismatch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_correlation_id_mismatch_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_correlation_id_mismatch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.CorruptRecordException`.
///
/// Mirrors `Error::is_corrupt_record_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_corrupt_record_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_corrupt_record_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenAuthorizationException`.
///
/// Mirrors `Error::is_delegation_token_authorization_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_authorization_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_delegation_token_authorization_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenDisabledException`.
///
/// Mirrors `Error::is_delegation_token_disabled_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_disabled_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_delegation_token_disabled_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenExpiredException`.
///
/// Mirrors `Error::is_delegation_token_expired_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_expired_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_delegation_token_expired_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenNotFoundException`.
///
/// Mirrors `Error::is_delegation_token_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_not_found_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_delegation_token_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException`.
///
/// Mirrors `Error::is_delegation_token_owner_mismatch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_owner_mismatch_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_delegation_token_owner_mismatch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DisconnectException`.
///
/// Mirrors `Error::is_disconnect_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_disconnect_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_disconnect_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DuplicateBrokerRegistrationException`.
///
/// Mirrors `Error::is_duplicate_broker_registration_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_duplicate_broker_registration_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_duplicate_broker_registration_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DuplicateResourceException`.
///
/// Mirrors `Error::is_duplicate_resource_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_duplicate_resource_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_duplicate_resource_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DuplicateSequenceException`.
///
/// Mirrors `Error::is_duplicate_sequence_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_duplicate_sequence_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_duplicate_sequence_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DuplicateVoterException`.
///
/// Mirrors `Error::is_duplicate_voter_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_duplicate_voter_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_duplicate_voter_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ElectionNotNeededException`.
///
/// Mirrors `Error::is_election_not_needed_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_election_not_needed_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_election_not_needed_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.EligibleLeadersNotAvailableException`.
///
/// Mirrors `Error::is_eligible_leaders_not_available_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_eligible_leaders_not_available_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_eligible_leaders_not_available_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FeatureUpdateFailedException`.
///
/// Mirrors `Error::is_feature_update_failed_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_feature_update_failed_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_feature_update_failed_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FencedInstanceIdException`.
///
/// Mirrors `Error::is_fenced_instance_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fenced_instance_id_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_fenced_instance_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FencedLeaderEpochException`.
///
/// Mirrors `Error::is_fenced_leader_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fenced_leader_epoch_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_fenced_leader_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FencedMemberEpochException`.
///
/// Mirrors `Error::is_fenced_member_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fenced_member_epoch_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_fenced_member_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FencedStateEpochException`.
///
/// Mirrors `Error::is_fenced_state_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fenced_state_epoch_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_fenced_state_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FetchSessionIdNotFoundException`.
///
/// Mirrors `Error::is_fetch_session_id_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fetch_session_id_not_found_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_fetch_session_id_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FetchSessionTopicIdException`.
///
/// Mirrors `Error::is_fetch_session_topic_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fetch_session_topic_id_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_fetch_session_topic_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupAuthorizationException`.
///
/// Mirrors `Error::is_group_authorization_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_authorization_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_group_authorization_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupIdNotFoundException`.
///
/// Mirrors `Error::is_group_id_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_id_not_found_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_group_id_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupMaxSizeReachedException`.
///
/// Mirrors `Error::is_group_max_size_reached_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_max_size_reached_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_group_max_size_reached_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupNotEmptyException`.
///
/// Mirrors `Error::is_group_not_empty_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_not_empty_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_group_not_empty_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupSubscribedToTopicException`.
///
/// Mirrors `Error::is_group_subscribed_to_topic_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_subscribed_to_topic_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_group_subscribed_to_topic_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.IllegalGenerationException`.
///
/// Mirrors `Error::is_illegal_generation_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_illegal_generation_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_illegal_generation_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.IllegalSaslStateException`.
///
/// Mirrors `Error::is_illegal_sasl_state_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_illegal_sasl_state_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_illegal_sasl_state_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InconsistentClusterIdException`.
///
/// Mirrors `Error::is_inconsistent_cluster_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_inconsistent_cluster_id_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_inconsistent_cluster_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InconsistentGroupProtocolException`.
///
/// Mirrors `Error::is_inconsistent_group_protocol_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_inconsistent_group_protocol_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_inconsistent_group_protocol_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InconsistentTopicIdException`.
///
/// Mirrors `Error::is_inconsistent_topic_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_inconsistent_topic_id_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_inconsistent_topic_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InconsistentVoterSetException`.
///
/// Mirrors `Error::is_inconsistent_voter_set_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_inconsistent_voter_set_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_inconsistent_voter_set_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.IneligibleReplicaException`.
///
/// Mirrors `Error::is_ineligible_replica_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_ineligible_replica_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_ineligible_replica_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InterruptException`.
///
/// Mirrors `Error::is_interrupt_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_interrupt_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_interrupt_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidCommitOffsetSizeException`.
///
/// Mirrors `Error::is_invalid_commit_offset_size_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_commit_offset_size_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_commit_offset_size_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidFetchSessionEpochException`.
///
/// Mirrors `Error::is_invalid_fetch_session_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_fetch_session_epoch_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_fetch_session_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidFetchSizeException`.
///
/// Mirrors `Error::is_invalid_fetch_size_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_fetch_size_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_fetch_size_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidGroupIdException`.
///
/// Mirrors `Error::is_invalid_group_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_group_id_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_group_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidPartitionsException`.
///
/// Mirrors `Error::is_invalid_partitions_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_partitions_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_partitions_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidPidMappingException`.
///
/// Mirrors `Error::is_invalid_pid_mapping_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_pid_mapping_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_pid_mapping_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidPrincipalTypeException`.
///
/// Mirrors `Error::is_invalid_principal_type_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_principal_type_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_principal_type_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidProducerEpochException`.
///
/// Mirrors `Error::is_invalid_producer_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_producer_epoch_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_producer_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.InvalidRecordException`.
///
/// Mirrors `Error::is_invalid_record_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_record_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_record_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRecordStateException`.
///
/// Mirrors `Error::is_invalid_record_state_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_record_state_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_record_state_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRegistrationException`.
///
/// Mirrors `Error::is_invalid_registration_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_registration_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_registration_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRegularExpression`.
///
/// Mirrors `Error::is_invalid_regular_expression_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_regular_expression_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_regular_expression_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidReplicaAssignmentException`.
///
/// Mirrors `Error::is_invalid_replica_assignment_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_replica_assignment_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_replica_assignment_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidReplicationFactorException`.
///
/// Mirrors `Error::is_invalid_replication_factor_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_replication_factor_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_replication_factor_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRequestException`.
///
/// Mirrors `Error::is_invalid_request_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_request_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_request_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRequiredAcksException`.
///
/// Mirrors `Error::is_invalid_required_acks_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_required_acks_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_required_acks_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidSessionTimeoutException`.
///
/// Mirrors `Error::is_invalid_session_timeout_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_session_timeout_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_session_timeout_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidShareSessionEpochException`.
///
/// Mirrors `Error::is_invalid_share_session_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_share_session_epoch_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_share_session_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidTimestampException`.
///
/// Mirrors `Error::is_invalid_timestamp_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_timestamp_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_timestamp_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidTopicException`.
///
/// Mirrors `Error::is_invalid_topic_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_topic_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_topic_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidTxnStateException`.
///
/// Mirrors `Error::is_invalid_txn_state_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_txn_state_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_txn_state_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidTxnTimeoutException`.
///
/// Mirrors `Error::is_invalid_txn_timeout_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_txn_timeout_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_txn_timeout_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidUpdateVersionException`.
///
/// Mirrors `Error::is_invalid_update_version_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_update_version_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_update_version_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidVoterKeyException`.
///
/// Mirrors `Error::is_invalid_voter_key_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_voter_key_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_voter_key_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.KafkaStorageException`.
///
/// Mirrors `Error::is_kafka_storage_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_kafka_storage_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_kafka_storage_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.LeaderNotAvailableException`.
///
/// Mirrors `Error::is_leader_not_available_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_leader_not_available_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_leader_not_available_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ListenerNotFoundException`.
///
/// Mirrors `Error::is_listener_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_listener_not_found_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_listener_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.LogDirNotFoundException`.
///
/// Mirrors `Error::is_log_dir_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_log_dir_not_found_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_log_dir_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.MemberIdRequiredException`.
///
/// Mirrors `Error::is_member_id_required_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_member_id_required_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_member_id_required_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.MismatchedEndpointTypeException`.
///
/// Mirrors `Error::is_mismatched_endpoint_type_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_mismatched_endpoint_type_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_mismatched_endpoint_type_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NetworkException`.
///
/// Mirrors `Error::is_network_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_network_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_network_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NewLeaderElectedException`.
///
/// Mirrors `Error::is_new_leader_elected_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_new_leader_elected_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_new_leader_elected_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NoReassignmentInProgressException`.
///
/// Mirrors `Error::is_no_reassignment_in_progress_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_no_reassignment_in_progress_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_no_reassignment_in_progress_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotControllerException`.
///
/// Mirrors `Error::is_not_controller_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_controller_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_not_controller_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotCoordinatorException`.
///
/// Mirrors `Error::is_not_coordinator_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_coordinator_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_not_coordinator_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotEnoughReplicasException`.
///
/// Mirrors `Error::is_not_enough_replicas_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_enough_replicas_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_not_enough_replicas_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException`.
///
/// Mirrors `Error::is_not_enough_replicas_after_append_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_enough_replicas_after_append_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_not_enough_replicas_after_append_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotLeaderOrFollowerException`.
///
/// Mirrors `Error::is_not_leader_or_follower_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_leader_or_follower_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_not_leader_or_follower_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OffsetMetadataTooLarge`.
///
/// Mirrors `Error::is_offset_metadata_too_large_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_offset_metadata_too_large_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_offset_metadata_too_large_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OffsetMovedToTieredStorageException`.
///
/// Mirrors `Error::is_offset_moved_to_tiered_storage_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_offset_moved_to_tiered_storage_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_offset_moved_to_tiered_storage_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OffsetNotAvailableException`.
///
/// Mirrors `Error::is_offset_not_available_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_offset_not_available_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_offset_not_available_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OffsetOutOfRangeException`.
///
/// Mirrors `Error::is_offset_out_of_range_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_offset_out_of_range_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_offset_out_of_range_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OperationNotAttemptedException`.
///
/// Mirrors `Error::is_operation_not_attempted_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_operation_not_attempted_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_operation_not_attempted_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.PolicyViolationException`.
///
/// Mirrors `Error::is_policy_violation_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_policy_violation_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_policy_violation_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.PositionOutOfRangeException`.
///
/// Mirrors `Error::is_position_out_of_range_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_position_out_of_range_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_position_out_of_range_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.PreferredLeaderNotAvailableException`.
///
/// Mirrors `Error::is_preferred_leader_not_available_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_preferred_leader_not_available_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_preferred_leader_not_available_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.PrincipalDeserializationException`.
///
/// Mirrors `Error::is_principal_deserialization_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_principal_deserialization_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_principal_deserialization_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ProducerFencedException`.
///
/// Mirrors `Error::is_producer_fenced_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_producer_fenced_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_producer_fenced_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.metrics.QuotaViolationException`.
///
/// Mirrors `Error::is_quota_violation_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_quota_violation_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_quota_violation_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ReassignmentInProgressException`.
///
/// Mirrors `Error::is_reassignment_in_progress_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_reassignment_in_progress_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_reassignment_in_progress_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RebalanceInProgressException`.
///
/// Mirrors `Error::is_rebalance_in_progress_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_rebalance_in_progress_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_rebalance_in_progress_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RebootstrapRequiredException`.
///
/// Mirrors `Error::is_rebootstrap_required_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_rebootstrap_required_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_rebootstrap_required_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RecordBatchTooLargeException`.
///
/// Mirrors `Error::is_record_batch_too_large_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_record_batch_too_large_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_record_batch_too_large_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RecordDeserializationException`.
///
/// Mirrors `Error::is_record_deserialization_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_record_deserialization_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_record_deserialization_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RecordTooLargeException`.
///
/// Mirrors `Error::is_record_too_large_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_record_too_large_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_record_too_large_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.network.InvalidReceiveException`.
///
/// Mirrors `Error::is_invalid_receive_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_receive_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_receive_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.config.ConfigException`.
///
/// Mirrors `Error::is_config_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_config_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_config_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.consumer.RetriableCommitFailedException`.
///
/// Mirrors `Error::is_consumer_retriable_commit_failed_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_retriable_commit_failed_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_consumer_retriable_commit_failed_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.consumer.CommitFailedException`.
///
/// Mirrors `Error::is_consumer_commit_failed_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_commit_failed_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_consumer_commit_failed_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`.
///
/// Mirrors `Error::is_consumer_no_offset_for_partition_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_no_offset_for_partition_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_consumer_no_offset_for_partition_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.consumer.LogTruncationException`.
///
/// Mirrors `Error::is_consumer_log_truncation_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_log_truncation_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_consumer_log_truncation_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ReplicaNotAvailableException`.
///
/// Mirrors `Error::is_replica_not_available_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_replica_not_available_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_replica_not_available_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ResourceNotFoundException`.
///
/// Mirrors `Error::is_resource_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_resource_not_found_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_resource_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.SaslAuthenticationException`.
///
/// Mirrors `Error::is_sasl_authentication_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_sasl_authentication_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_sasl_authentication_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.protocol.types.SchemaException`.
///
/// Mirrors `Error::is_schema_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_schema_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_schema_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.SecurityDisabledException`.
///
/// Mirrors `Error::is_security_disabled_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_security_disabled_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_security_disabled_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ShareSessionLimitReachedException`.
///
/// Mirrors `Error::is_share_session_limit_reached_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_share_session_limit_reached_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_share_session_limit_reached_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ShareSessionNotFoundException`.
///
/// Mirrors `Error::is_share_session_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_share_session_not_found_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_share_session_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.SnapshotNotFoundException`.
///
/// Mirrors `Error::is_snapshot_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_snapshot_not_found_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_snapshot_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.SslAuthenticationException`.
///
/// Mirrors `Error::is_ssl_authentication_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_ssl_authentication_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_ssl_authentication_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StaleBrokerEpochException`.
///
/// Mirrors `Error::is_stale_broker_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_stale_broker_epoch_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_stale_broker_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StaleMemberEpochException`.
///
/// Mirrors `Error::is_stale_member_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_stale_member_epoch_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_stale_member_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StreamsInvalidTopologyException`.
///
/// Mirrors `Error::is_streams_invalid_topology_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_streams_invalid_topology_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_streams_invalid_topology_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StreamsInvalidTopologyEpochException`.
///
/// Mirrors `Error::is_streams_invalid_topology_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_streams_invalid_topology_epoch_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_streams_invalid_topology_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StreamsTopologyFencedException`.
///
/// Mirrors `Error::is_streams_topology_fenced_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_streams_topology_fenced_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_streams_topology_fenced_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TelemetryTooLargeException`.
///
/// Mirrors `Error::is_telemetry_too_large_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_telemetry_too_large_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_telemetry_too_large_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ThrottlingQuotaExceededException`.
///
/// Mirrors `Error::is_throttling_quota_exceeded_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_throttling_quota_exceeded_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_throttling_quota_exceeded_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TopicAuthorizationException`.
///
/// Mirrors `Error::is_topic_authorization_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_topic_authorization_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_topic_authorization_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TopicDeletionDisabledException`.
///
/// Mirrors `Error::is_topic_deletion_disabled_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_topic_deletion_disabled_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_topic_deletion_disabled_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TopicExistsException`.
///
/// Mirrors `Error::is_topic_exists_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_topic_exists_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_topic_exists_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionAbortableException`.
///
/// Mirrors `Error::is_transaction_abortable_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transaction_abortable_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_transaction_abortable_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionAbortedException`.
///
/// Mirrors `Error::is_transaction_aborted_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transaction_aborted_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_transaction_aborted_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionCoordinatorFencedException`.
///
/// Mirrors `Error::is_transaction_coordinator_fenced_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transaction_coordinator_fenced_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_transaction_coordinator_fenced_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionalIdAuthorizationException`.
///
/// Mirrors `Error::is_transactional_id_authorization_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transactional_id_authorization_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_transactional_id_authorization_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionalIdNotFoundException`.
///
/// Mirrors `Error::is_transactional_id_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transactional_id_not_found_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_transactional_id_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnacceptableCredentialException`.
///
/// Mirrors `Error::is_unacceptable_credential_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unacceptable_credential_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unacceptable_credential_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownControllerIdException`.
///
/// Mirrors `Error::is_unknown_controller_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_controller_id_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unknown_controller_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownLeaderEpochException`.
///
/// Mirrors `Error::is_unknown_leader_epoch_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_leader_epoch_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unknown_leader_epoch_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownMemberIdException`.
///
/// Mirrors `Error::is_unknown_member_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_member_id_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unknown_member_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownProducerIdException`.
///
/// Mirrors `Error::is_unknown_producer_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_producer_id_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unknown_producer_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownServerException`.
///
/// Mirrors `Error::is_unknown_server_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_server_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unknown_server_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownSubscriptionIdException`.
///
/// Mirrors `Error::is_unknown_subscription_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_subscription_id_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unknown_subscription_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownTopicIdException`.
///
/// Mirrors `Error::is_unknown_topic_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_topic_id_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unknown_topic_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownTopicOrPartitionException`.
///
/// Mirrors `Error::is_unknown_topic_or_partition_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_topic_or_partition_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unknown_topic_or_partition_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnreleasedInstanceIdException`.
///
/// Mirrors `Error::is_unreleased_instance_id_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unreleased_instance_id_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unreleased_instance_id_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnstableOffsetCommitException`.
///
/// Mirrors `Error::is_unstable_offset_commit_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unstable_offset_commit_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unstable_offset_commit_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedAssignorException`.
///
/// Mirrors `Error::is_unsupported_assignor_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_assignor_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unsupported_assignor_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedByAuthenticationException`.
///
/// Mirrors `Error::is_unsupported_by_authentication_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_by_authentication_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unsupported_by_authentication_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedCompressionTypeException`.
///
/// Mirrors `Error::is_unsupported_compression_type_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_compression_type_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unsupported_compression_type_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedEndpointTypeException`.
///
/// Mirrors `Error::is_unsupported_endpoint_type_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_endpoint_type_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unsupported_endpoint_type_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedForMessageFormatException`.
///
/// Mirrors `Error::is_unsupported_for_message_format_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_for_message_format_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unsupported_for_message_format_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedSaslMechanismException`.
///
/// Mirrors `Error::is_unsupported_sasl_mechanism_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_sasl_mechanism_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unsupported_sasl_mechanism_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedVersionException`.
///
/// Mirrors `Error::is_unsupported_version_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_version_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_unsupported_version_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.VoterNotFoundException`.
///
/// Mirrors `Error::is_voter_not_found_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_voter_not_found_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_voter_not_found_error()
}

/// Whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.WakeupException`.
///
/// Mirrors `Error::is_wakeup_error`.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_wakeup_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_wakeup_error()
}

/// Every export above, by the name of the predicate it mirrors, for the tests
/// that check each export against every error class.
#[cfg(test)]
pub(crate) const PREDICATES: &[(&str, unsafe extern "C" fn(*const kafka_common_Error_t) -> bool)] = &[
    ("is_kafka_error", kafka_common_Error_is_kafka_error),
    ("is_api_error", kafka_common_Error_is_api_error),
    ("is_retriable_error", kafka_common_Error_is_retriable_error),
    ("is_refresh_retriable_error", kafka_common_Error_is_refresh_retriable_error),
    ("is_timeout_error", kafka_common_Error_is_timeout_error),
    ("is_invalid_metadata_error", kafka_common_Error_is_invalid_metadata_error),
    (
        "is_invalid_configuration_error",
        kafka_common_Error_is_invalid_configuration_error,
    ),
    (
        "is_application_recoverable_error",
        kafka_common_Error_is_application_recoverable_error,
    ),
    ("is_invalid_offset_error", kafka_common_Error_is_invalid_offset_error),
    (
        "is_out_of_order_sequence_error",
        kafka_common_Error_is_out_of_order_sequence_error,
    ),
    (
        "is_consumer_invalid_offset_error",
        kafka_common_Error_is_consumer_invalid_offset_error,
    ),
    (
        "is_consumer_offset_out_of_range_error",
        kafka_common_Error_is_consumer_offset_out_of_range_error,
    ),
    ("is_serialization_error", kafka_common_Error_is_serialization_error),
    ("is_authentication_error", kafka_common_Error_is_authentication_error),
    ("is_authorization_error", kafka_common_Error_is_authorization_error),
    (
        "is_local_illegal_argument_error",
        kafka_common_Error_is_local_illegal_argument_error,
    ),
    ("is_local_illegal_state_error", kafka_common_Error_is_local_illegal_state_error),
    (
        "is_local_concurrent_modification_error",
        kafka_common_Error_is_local_concurrent_modification_error,
    ),
    ("is_local_timeout_error", kafka_common_Error_is_local_timeout_error),
    (
        "is_authorizer_not_ready_error",
        kafka_common_Error_is_authorizer_not_ready_error,
    ),
    (
        "is_broker_id_not_registered_error",
        kafka_common_Error_is_broker_id_not_registered_error,
    ),
    (
        "is_broker_not_available_error",
        kafka_common_Error_is_broker_not_available_error,
    ),
    (
        "is_producer_buffer_exhausted_error",
        kafka_common_Error_is_producer_buffer_exhausted_error,
    ),
    (
        "is_cluster_authorization_error",
        kafka_common_Error_is_cluster_authorization_error,
    ),
    (
        "is_concurrent_transactions_error",
        kafka_common_Error_is_concurrent_transactions_error,
    ),
    ("is_controller_moved_error", kafka_common_Error_is_controller_moved_error),
    (
        "is_coordinator_load_in_progress_error",
        kafka_common_Error_is_coordinator_load_in_progress_error,
    ),
    (
        "is_coordinator_not_available_error",
        kafka_common_Error_is_coordinator_not_available_error,
    ),
    (
        "is_correlation_id_mismatch_error",
        kafka_common_Error_is_correlation_id_mismatch_error,
    ),
    ("is_corrupt_record_error", kafka_common_Error_is_corrupt_record_error),
    (
        "is_delegation_token_authorization_error",
        kafka_common_Error_is_delegation_token_authorization_error,
    ),
    (
        "is_delegation_token_disabled_error",
        kafka_common_Error_is_delegation_token_disabled_error,
    ),
    (
        "is_delegation_token_expired_error",
        kafka_common_Error_is_delegation_token_expired_error,
    ),
    (
        "is_delegation_token_not_found_error",
        kafka_common_Error_is_delegation_token_not_found_error,
    ),
    (
        "is_delegation_token_owner_mismatch_error",
        kafka_common_Error_is_delegation_token_owner_mismatch_error,
    ),
    ("is_disconnect_error", kafka_common_Error_is_disconnect_error),
    (
        "is_duplicate_broker_registration_error",
        kafka_common_Error_is_duplicate_broker_registration_error,
    ),
    ("is_duplicate_resource_error", kafka_common_Error_is_duplicate_resource_error),
    ("is_duplicate_sequence_error", kafka_common_Error_is_duplicate_sequence_error),
    ("is_duplicate_voter_error", kafka_common_Error_is_duplicate_voter_error),
    ("is_election_not_needed_error", kafka_common_Error_is_election_not_needed_error),
    (
        "is_eligible_leaders_not_available_error",
        kafka_common_Error_is_eligible_leaders_not_available_error,
    ),
    (
        "is_feature_update_failed_error",
        kafka_common_Error_is_feature_update_failed_error,
    ),
    ("is_fenced_instance_id_error", kafka_common_Error_is_fenced_instance_id_error),
    ("is_fenced_leader_epoch_error", kafka_common_Error_is_fenced_leader_epoch_error),
    ("is_fenced_member_epoch_error", kafka_common_Error_is_fenced_member_epoch_error),
    ("is_fenced_state_epoch_error", kafka_common_Error_is_fenced_state_epoch_error),
    (
        "is_fetch_session_id_not_found_error",
        kafka_common_Error_is_fetch_session_id_not_found_error,
    ),
    (
        "is_fetch_session_topic_id_error",
        kafka_common_Error_is_fetch_session_topic_id_error,
    ),
    ("is_group_authorization_error", kafka_common_Error_is_group_authorization_error),
    ("is_group_id_not_found_error", kafka_common_Error_is_group_id_not_found_error),
    (
        "is_group_max_size_reached_error",
        kafka_common_Error_is_group_max_size_reached_error,
    ),
    ("is_group_not_empty_error", kafka_common_Error_is_group_not_empty_error),
    (
        "is_group_subscribed_to_topic_error",
        kafka_common_Error_is_group_subscribed_to_topic_error,
    ),
    ("is_illegal_generation_error", kafka_common_Error_is_illegal_generation_error),
    ("is_illegal_sasl_state_error", kafka_common_Error_is_illegal_sasl_state_error),
    (
        "is_inconsistent_cluster_id_error",
        kafka_common_Error_is_inconsistent_cluster_id_error,
    ),
    (
        "is_inconsistent_group_protocol_error",
        kafka_common_Error_is_inconsistent_group_protocol_error,
    ),
    (
        "is_inconsistent_topic_id_error",
        kafka_common_Error_is_inconsistent_topic_id_error,
    ),
    (
        "is_inconsistent_voter_set_error",
        kafka_common_Error_is_inconsistent_voter_set_error,
    ),
    ("is_ineligible_replica_error", kafka_common_Error_is_ineligible_replica_error),
    ("is_interrupt_error", kafka_common_Error_is_interrupt_error),
    (
        "is_invalid_commit_offset_size_error",
        kafka_common_Error_is_invalid_commit_offset_size_error,
    ),
    (
        "is_invalid_fetch_session_epoch_error",
        kafka_common_Error_is_invalid_fetch_session_epoch_error,
    ),
    ("is_invalid_fetch_size_error", kafka_common_Error_is_invalid_fetch_size_error),
    ("is_invalid_group_id_error", kafka_common_Error_is_invalid_group_id_error),
    ("is_invalid_partitions_error", kafka_common_Error_is_invalid_partitions_error),
    ("is_invalid_pid_mapping_error", kafka_common_Error_is_invalid_pid_mapping_error),
    (
        "is_invalid_principal_type_error",
        kafka_common_Error_is_invalid_principal_type_error,
    ),
    (
        "is_invalid_producer_epoch_error",
        kafka_common_Error_is_invalid_producer_epoch_error,
    ),
    ("is_invalid_record_error", kafka_common_Error_is_invalid_record_error),
    (
        "is_invalid_record_state_error",
        kafka_common_Error_is_invalid_record_state_error,
    ),
    (
        "is_invalid_registration_error",
        kafka_common_Error_is_invalid_registration_error,
    ),
    (
        "is_invalid_regular_expression_error",
        kafka_common_Error_is_invalid_regular_expression_error,
    ),
    (
        "is_invalid_replica_assignment_error",
        kafka_common_Error_is_invalid_replica_assignment_error,
    ),
    (
        "is_invalid_replication_factor_error",
        kafka_common_Error_is_invalid_replication_factor_error,
    ),
    ("is_invalid_request_error", kafka_common_Error_is_invalid_request_error),
    (
        "is_invalid_required_acks_error",
        kafka_common_Error_is_invalid_required_acks_error,
    ),
    (
        "is_invalid_session_timeout_error",
        kafka_common_Error_is_invalid_session_timeout_error,
    ),
    (
        "is_invalid_share_session_epoch_error",
        kafka_common_Error_is_invalid_share_session_epoch_error,
    ),
    ("is_invalid_timestamp_error", kafka_common_Error_is_invalid_timestamp_error),
    ("is_invalid_topic_error", kafka_common_Error_is_invalid_topic_error),
    ("is_invalid_txn_state_error", kafka_common_Error_is_invalid_txn_state_error),
    ("is_invalid_txn_timeout_error", kafka_common_Error_is_invalid_txn_timeout_error),
    (
        "is_invalid_update_version_error",
        kafka_common_Error_is_invalid_update_version_error,
    ),
    ("is_invalid_voter_key_error", kafka_common_Error_is_invalid_voter_key_error),
    ("is_kafka_storage_error", kafka_common_Error_is_kafka_storage_error),
    (
        "is_leader_not_available_error",
        kafka_common_Error_is_leader_not_available_error,
    ),
    ("is_listener_not_found_error", kafka_common_Error_is_listener_not_found_error),
    ("is_log_dir_not_found_error", kafka_common_Error_is_log_dir_not_found_error),
    ("is_member_id_required_error", kafka_common_Error_is_member_id_required_error),
    (
        "is_mismatched_endpoint_type_error",
        kafka_common_Error_is_mismatched_endpoint_type_error,
    ),
    ("is_network_error", kafka_common_Error_is_network_error),
    ("is_new_leader_elected_error", kafka_common_Error_is_new_leader_elected_error),
    (
        "is_no_reassignment_in_progress_error",
        kafka_common_Error_is_no_reassignment_in_progress_error,
    ),
    ("is_not_controller_error", kafka_common_Error_is_not_controller_error),
    ("is_not_coordinator_error", kafka_common_Error_is_not_coordinator_error),
    ("is_not_enough_replicas_error", kafka_common_Error_is_not_enough_replicas_error),
    (
        "is_not_enough_replicas_after_append_error",
        kafka_common_Error_is_not_enough_replicas_after_append_error,
    ),
    (
        "is_not_leader_or_follower_error",
        kafka_common_Error_is_not_leader_or_follower_error,
    ),
    (
        "is_offset_metadata_too_large_error",
        kafka_common_Error_is_offset_metadata_too_large_error,
    ),
    (
        "is_offset_moved_to_tiered_storage_error",
        kafka_common_Error_is_offset_moved_to_tiered_storage_error,
    ),
    (
        "is_offset_not_available_error",
        kafka_common_Error_is_offset_not_available_error,
    ),
    ("is_offset_out_of_range_error", kafka_common_Error_is_offset_out_of_range_error),
    (
        "is_operation_not_attempted_error",
        kafka_common_Error_is_operation_not_attempted_error,
    ),
    ("is_policy_violation_error", kafka_common_Error_is_policy_violation_error),
    (
        "is_position_out_of_range_error",
        kafka_common_Error_is_position_out_of_range_error,
    ),
    (
        "is_preferred_leader_not_available_error",
        kafka_common_Error_is_preferred_leader_not_available_error,
    ),
    (
        "is_principal_deserialization_error",
        kafka_common_Error_is_principal_deserialization_error,
    ),
    ("is_producer_fenced_error", kafka_common_Error_is_producer_fenced_error),
    ("is_quota_violation_error", kafka_common_Error_is_quota_violation_error),
    (
        "is_reassignment_in_progress_error",
        kafka_common_Error_is_reassignment_in_progress_error,
    ),
    (
        "is_rebalance_in_progress_error",
        kafka_common_Error_is_rebalance_in_progress_error,
    ),
    (
        "is_rebootstrap_required_error",
        kafka_common_Error_is_rebootstrap_required_error,
    ),
    (
        "is_record_batch_too_large_error",
        kafka_common_Error_is_record_batch_too_large_error,
    ),
    (
        "is_record_deserialization_error",
        kafka_common_Error_is_record_deserialization_error,
    ),
    ("is_record_too_large_error", kafka_common_Error_is_record_too_large_error),
    ("is_invalid_receive_error", kafka_common_Error_is_invalid_receive_error),
    ("is_config_error", kafka_common_Error_is_config_error),
    (
        "is_consumer_retriable_commit_failed_error",
        kafka_common_Error_is_consumer_retriable_commit_failed_error,
    ),
    (
        "is_consumer_commit_failed_error",
        kafka_common_Error_is_consumer_commit_failed_error,
    ),
    (
        "is_consumer_no_offset_for_partition_error",
        kafka_common_Error_is_consumer_no_offset_for_partition_error,
    ),
    (
        "is_consumer_log_truncation_error",
        kafka_common_Error_is_consumer_log_truncation_error,
    ),
    (
        "is_replica_not_available_error",
        kafka_common_Error_is_replica_not_available_error,
    ),
    ("is_resource_not_found_error", kafka_common_Error_is_resource_not_found_error),
    ("is_sasl_authentication_error", kafka_common_Error_is_sasl_authentication_error),
    ("is_schema_error", kafka_common_Error_is_schema_error),
    ("is_security_disabled_error", kafka_common_Error_is_security_disabled_error),
    (
        "is_share_session_limit_reached_error",
        kafka_common_Error_is_share_session_limit_reached_error,
    ),
    (
        "is_share_session_not_found_error",
        kafka_common_Error_is_share_session_not_found_error,
    ),
    ("is_snapshot_not_found_error", kafka_common_Error_is_snapshot_not_found_error),
    ("is_ssl_authentication_error", kafka_common_Error_is_ssl_authentication_error),
    ("is_stale_broker_epoch_error", kafka_common_Error_is_stale_broker_epoch_error),
    ("is_stale_member_epoch_error", kafka_common_Error_is_stale_member_epoch_error),
    (
        "is_streams_invalid_topology_error",
        kafka_common_Error_is_streams_invalid_topology_error,
    ),
    (
        "is_streams_invalid_topology_epoch_error",
        kafka_common_Error_is_streams_invalid_topology_epoch_error,
    ),
    (
        "is_streams_topology_fenced_error",
        kafka_common_Error_is_streams_topology_fenced_error,
    ),
    ("is_telemetry_too_large_error", kafka_common_Error_is_telemetry_too_large_error),
    (
        "is_throttling_quota_exceeded_error",
        kafka_common_Error_is_throttling_quota_exceeded_error,
    ),
    ("is_topic_authorization_error", kafka_common_Error_is_topic_authorization_error),
    (
        "is_topic_deletion_disabled_error",
        kafka_common_Error_is_topic_deletion_disabled_error,
    ),
    ("is_topic_exists_error", kafka_common_Error_is_topic_exists_error),
    (
        "is_transaction_abortable_error",
        kafka_common_Error_is_transaction_abortable_error,
    ),
    ("is_transaction_aborted_error", kafka_common_Error_is_transaction_aborted_error),
    (
        "is_transaction_coordinator_fenced_error",
        kafka_common_Error_is_transaction_coordinator_fenced_error,
    ),
    (
        "is_transactional_id_authorization_error",
        kafka_common_Error_is_transactional_id_authorization_error,
    ),
    (
        "is_transactional_id_not_found_error",
        kafka_common_Error_is_transactional_id_not_found_error,
    ),
    (
        "is_unacceptable_credential_error",
        kafka_common_Error_is_unacceptable_credential_error,
    ),
    (
        "is_unknown_controller_id_error",
        kafka_common_Error_is_unknown_controller_id_error,
    ),
    (
        "is_unknown_leader_epoch_error",
        kafka_common_Error_is_unknown_leader_epoch_error,
    ),
    ("is_unknown_member_id_error", kafka_common_Error_is_unknown_member_id_error),
    ("is_unknown_producer_id_error", kafka_common_Error_is_unknown_producer_id_error),
    ("is_unknown_server_error", kafka_common_Error_is_unknown_server_error),
    (
        "is_unknown_subscription_id_error",
        kafka_common_Error_is_unknown_subscription_id_error,
    ),
    ("is_unknown_topic_id_error", kafka_common_Error_is_unknown_topic_id_error),
    (
        "is_unknown_topic_or_partition_error",
        kafka_common_Error_is_unknown_topic_or_partition_error,
    ),
    (
        "is_unreleased_instance_id_error",
        kafka_common_Error_is_unreleased_instance_id_error,
    ),
    (
        "is_unstable_offset_commit_error",
        kafka_common_Error_is_unstable_offset_commit_error,
    ),
    (
        "is_unsupported_assignor_error",
        kafka_common_Error_is_unsupported_assignor_error,
    ),
    (
        "is_unsupported_by_authentication_error",
        kafka_common_Error_is_unsupported_by_authentication_error,
    ),
    (
        "is_unsupported_compression_type_error",
        kafka_common_Error_is_unsupported_compression_type_error,
    ),
    (
        "is_unsupported_endpoint_type_error",
        kafka_common_Error_is_unsupported_endpoint_type_error,
    ),
    (
        "is_unsupported_for_message_format_error",
        kafka_common_Error_is_unsupported_for_message_format_error,
    ),
    (
        "is_unsupported_sasl_mechanism_error",
        kafka_common_Error_is_unsupported_sasl_mechanism_error,
    ),
    ("is_unsupported_version_error", kafka_common_Error_is_unsupported_version_error),
    ("is_voter_not_found_error", kafka_common_Error_is_voter_not_found_error),
    ("is_wakeup_error", kafka_common_Error_is_wakeup_error),
];
