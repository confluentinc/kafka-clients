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

//! Helpers shared between `AsyncKafkaConsumer` and its in-package
//! collaborators.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerUtils`.
//!
//! # Scope
//!
//! Only the subset used by `AsyncKafkaConsumer` and its dependency closure
//! is translated:
//!
//!  - Public constants used by the consumer (`DEFAULT_CLOSE_TIMEOUT_MS`,
//!    JMX / metric-group prefixes, `CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION`).
//!  - `create_log_context` — builds the `[Consumer clientId=…]` prefix.
//!  - `configured_isolation_level` — parses the `isolation.level` config.
//!  - `create_subscription_state` — constructs a `SubscriptionState` from
//!    `auto.offset.reset`.
//!  - `refresh_committed_offsets` — seeds positions from committed offsets.
//!  - `maybe_wrap_as_kafka_error` / `maybe_wrap_as_kafka_error_with_msg` —
//!    Java's `KafkaException` cast-or-wrap.
//!
//! # Out of scope (skipped per PLAN.md deferral #7)
//!
//! - `createConsumerNetworkClient` — uses `ConsumerNetworkClient` which is
//!   classic-protocol-only per `consumer-threading.md` §20.
//! - `createMetrics`, `createFetchMetricsManager`,
//!   `createShareFetchMetricsManager` — the metrics framework is deferred
//!   across Milestone-8 (see Phase 11 PLAN.md "AsyncConsumerMetrics" note).
//! - `configuredConsumerInterceptors` — relies on Java's reflection-based
//!   class-name-to-instance machinery. Phase 2 made the decision to take
//!   interceptors as already-constructed `Vec<Box<dyn ConsumerInterceptor>>`
//!   from the user. The `interceptor.classes` config key is still accepted
//!   (no rejection) but yields an empty list.
//! - `getResult(Future<T>, …)` — Java's blocking `Future.get()` translates
//!   to `tokio::time::timeout(...).await` at the call site in Rust; the
//!   wrapper does not add value.

#![allow(dead_code)] // Phase 11 commit (1/N): helpers land before their callers (commits 2-7).

use std::sync::{Arc, Mutex};

use log::info;

use crate::common::protocol::Errors;
use crate::common::{Error, IsolationLevel, KafkaError, TopicPartition};
use crate::consumer::consumer_config::ConsumerConfig;
use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
use crate::consumer::internals::subscription_state::{FetchPosition, SubscriptionState};
use crate::consumer::offset_and_metadata::OffsetAndMetadata;

/// Java: `DEFAULT_CLOSE_TIMEOUT_MS` (30 seconds).
pub(crate) const DEFAULT_CLOSE_TIMEOUT_MS: i64 = 30 * 1000;

/// Java: `CONSUMER_JMX_PREFIX`.
pub(crate) const CONSUMER_JMX_PREFIX: &str = "kafka.consumer";

/// Java: `CONSUMER_METRIC_GROUP_PREFIX`.
pub(crate) const CONSUMER_METRIC_GROUP_PREFIX: &str = "consumer";

/// Java: `CONSUMER_SHARE_METRIC_GROUP_PREFIX`.
pub(crate) const CONSUMER_SHARE_METRIC_GROUP_PREFIX: &str = "consumer-share";

/// Java: `COORDINATOR_METRICS_SUFFIX`.
pub(crate) const COORDINATOR_METRICS_SUFFIX: &str = "-coordinator-metrics";

/// Java: `CONSUMER_METRICS_SUFFIX`.
pub(crate) const CONSUMER_METRICS_SUFFIX: &str = "-metrics";

/// Java: `CONSUMER_METRIC_GROUP`.
pub(crate) const CONSUMER_METRIC_GROUP: &str = "consumer-metrics";

/// Java: `CONSUMER_SHARE_METRIC_GROUP`.
pub(crate) const CONSUMER_SHARE_METRIC_GROUP: &str = "consumer-share-metrics";

/// Java: `CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION`.
///
/// Translates Java's per-connection cap on in-flight requests for the
/// consumer's `NetworkClient`. Mirrors the comment in
/// `ConsumerUtils.java:75` ("A fixed, large enough value will suffice for
/// max.").
pub(crate) const CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION: i32 = 100;

/// Java: `THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED`.
///
/// Note: this string is the **config key**, matching Java's identically-
/// named `package-private` field. The corresponding bool lives on
/// [`ConsumerConfig::throw_on_fetch_stable_offset_unsupported`].
pub(crate) const THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED: &str = "internal.throw.on.fetch.stable.offset.unsupported";

/// Java: `CONSUMER_CLIENT_ID_METRIC_TAG` (package-private).
pub(crate) const CONSUMER_CLIENT_ID_METRIC_TAG: &str = "client-id";

// SKIP: classic protocol out of scope per consumer-threading.md §20.
// `createConsumerNetworkClient(...)` builds a `ConsumerNetworkClient`
// which only the classic-protocol consumer uses. The KIP-848 path uses
// `NetworkClientDelegate` (Phase 6) directly.

/// Builds the log-prefix string used by Java's `LogContext`.
///
/// Translates `createLogContext(ConsumerConfig, GroupRebalanceConfig)`.
/// The Java return type is `LogContext`, a slf4j adapter; Rust uses the
/// `log` crate and threads the prefix as a plain `Arc<str>`. Callers
/// `format!("{prefix} …", prefix=log_prefix)` when emitting messages.
///
/// `group_id` and `group_instance_id` are taken directly because the
/// `GroupRebalanceConfig` Java helper is folded into `ConsumerConfig`
/// fields on this side of the translation.
pub(crate) fn create_log_context(client_id: &str, group_id: Option<&str>, group_instance_id: Option<&str>) -> Arc<str> {
    let group_id_str = group_id.unwrap_or("null");
    match group_instance_id {
        Some(instance_id) => {
            format!("[Consumer instanceId={instance_id}, clientId={client_id}, groupId={group_id_str}] ").into()
        },
        None => format!("[Consumer clientId={client_id}, groupId={group_id_str}] ").into(),
    }
}

/// Java: `configuredIsolationLevel(ConsumerConfig)`.
///
/// Parses the `isolation.level` config string. Java throws
/// `IllegalArgumentException` for unknown values via `Enum.valueOf`; Rust
/// returns [`Error::local_illegal_argument`].
pub(crate) fn configured_isolation_level(config: &ConsumerConfig) -> Result<IsolationLevel, Error> {
    isolation_level_from_str(&config.isolation_level)
}

/// Parses an isolation-level string. Accepts the Java enum's spelling
/// (case-insensitive, matching Java's `toUpperCase(Locale.ROOT)` +
/// `Enum.valueOf`).
fn isolation_level_from_str(s: &str) -> Result<IsolationLevel, Error> {
    let upper = s.to_ascii_uppercase();
    match upper.as_str() {
        "READ_UNCOMMITTED" => Ok(IsolationLevel::ReadUncommitted),
        "READ_COMMITTED" => Ok(IsolationLevel::ReadCommitted),
        other => Err(Error::local_illegal_argument(format!("Unknown isolation level {other}"))),
    }
}

/// Java: `createSubscriptionState(ConsumerConfig, LogContext)`.
///
/// Builds a [`SubscriptionState`] seeded with the parsed
/// [`AutoOffsetResetStrategy`] from `auto.offset.reset`.
pub(crate) fn create_subscription_state(config: &ConsumerConfig) -> Result<SubscriptionState, Error> {
    let strategy = AutoOffsetResetStrategy::from_string(config.auto_offset_reset())?;
    Ok(SubscriptionState::new(strategy))
}

/// Java: `maybeWrapAsKafkaException(Throwable)`
/// (`ConsumerUtils.java:249-254`).
///
/// ```java
/// public static KafkaException maybeWrapAsKafkaException(Throwable t) {
///     if (t instanceof KafkaException)
///         return (KafkaException) t;
///     else
///         return new KafkaException(t);
/// }
/// ```
///
/// CONDITIONAL behavior, exactly like the two-argument
/// [`maybe_wrap_as_kafka_error_with_msg`]: an error already inside the
/// `KafkaException` hierarchy ([`Error::is_kafka_error`] is `true`) is
/// returned unchanged; anything else — Java's `java.lang` runtime
/// exceptions, which are *siblings* of `KafkaException` rather than
/// subclasses, i.e. the Rust [`Error::LocalIllegalArgument`] /
/// [`Error::LocalIllegalState`] / [`Error::LocalConcurrentModification`] variants — is
/// wrapped so that `is_kafka_error()` answers `true`, carrying the original
/// as its [`Error::source`].
///
/// This is NOT a no-op. Per CLAUDE.md §10.3 the Rust `Error` enum is flat and
/// holds both `KafkaException`'s subclasses AND the generic runtime
/// exceptions beside it; `is_kafka_error()` is the only thing that recovers
/// the distinction, and several call sites branch on it. Returning the input
/// unchanged would let a generic error reach a caller that Java guarantees
/// receives a `KafkaException`.
pub(crate) fn maybe_wrap_as_kafka_error(err: Error) -> Error {
    if err.is_kafka_error() {
        // `t instanceof KafkaException` → return unchanged.
        err
    } else {
        // `new KafkaException(t)`. Java's `Throwable(Throwable cause)`
        // constructor sets `detailMessage = cause.toString()`, so the wrapper's
        // `getMessage()` is the cause rendered in full — NOT a generic
        // "unknown server error" string. `Display` on `Error` translates
        // `Throwable.toString()` (see `Error::message`'s doc), so
        // `err.to_string()` is exactly Java's `cause.toString()`. Using the
        // code's default message instead would silently discard the only
        // diagnostic the error carries.
        let message = err.to_string();
        Error::KafkaError(KafkaError::new_message_source(Errors::UnknownServerError, message, err))
    }
}

/// Java: `maybeWrapAsKafkaException(Throwable, String)`
/// (`ConsumerUtils.java:256`).
///
/// ```java
/// public static KafkaException maybeWrapAsKafkaException(Throwable t, String message) {
///     if (t instanceof KafkaException)
///         return (KafkaException) t;
///     else
///         return new KafkaException(message, t);
/// }
/// ```
///
/// CONDITIONAL behavior: if `err` is already a `KafkaException`
/// ([`Error::is_kafka_error`] is `true`) it is returned
/// unchanged — message and all. Only a generic error (Java's
/// `IllegalArgumentException` / `IllegalStateException` /
/// `ConcurrentModificationException`, i.e. the Rust
/// [`Error::LocalIllegalArgument`] / [`Error::LocalIllegalState`] /
/// [`Error::LocalConcurrentModification`] variants) is wrapped in a new `KafkaException` whose message is exactly
/// `message`, carrying the original as its [`Error::source`] (Java's
/// `getCause()`). This matches Java, where
/// `new KafkaException(message, t).getMessage()` returns `message` verbatim
/// while `getCause()` still yields `t`.
pub(crate) fn maybe_wrap_as_kafka_error_with_msg(err: Error, message: &str) -> Error {
    if err.is_kafka_error() {
        // `t instanceof KafkaException` → return unchanged.
        err
    } else {
        // `new KafkaException(message, t)`. The wrapped error's message is
        // exactly `message`; `t` becomes the cause, reachable through
        // `Error::source()` — not merely logged, which would leave the
        // original unreachable to a programmatic caller.
        // Java wraps into `KafkaException` here.
        log::debug!("Wrapping non-Kafka error into the Kafka error hierarchy: cause={err}");
        Error::KafkaError(KafkaError::new_message_source(
            Errors::UnknownServerError,
            message.to_string(),
            err,
        ))
    }
}

/// Java: `refreshCommittedOffsets(Map<TopicPartition, OffsetAndMetadata>,
/// ConsumerMetadata, SubscriptionState)`.
///
/// Update subscription state and metadata using the provided committed
/// offsets:
///  1. Update partition offsets with the committed offsets.
///  2. Update the metadata with any newer leader epoch discovered in the
///     committed offsets' metadata.
///
/// This will ignore any partition included in the `offsets_and_metadata`
/// parameter that may no longer be assigned.
///
/// # Lock discipline
///
/// `subscriptions` is taken behind `Arc<Mutex<...>>` because Java's
/// `synchronized` is reentrant and callers in Java hold the
/// `SubscriptionState` monitor. The Rust translation acquires the lock
/// inside this helper for each mutation; never holds it across `.await`
/// (the function is synchronous).
pub(crate) fn refresh_committed_offsets(
    offsets_and_metadata: &std::collections::HashMap<TopicPartition, OffsetAndMetadata>,
    metadata: &ConsumerMetadata,
    subscriptions: &Arc<Mutex<SubscriptionState>>,
) {
    for (tp, offset_and_metadata) in offsets_and_metadata.iter() {
        // first update the epoch if necessary
        if let Some(epoch) = offset_and_metadata.leader_epoch() {
            // Java's `void`-returning helper ignores any error; mirror that
            // here (negative-epoch guard cannot trigger because
            // `OffsetAndMetadata::leader_epoch()` filters negative values
            // to `None`).
            let _ = metadata.update_last_seen_epoch_if_newer(tp, epoch);
        }

        // it's possible that the partition is no longer assigned when the
        // response is received, so we need to ignore seeking if that's the
        // case
        let mut subs = subscriptions.lock().unwrap();
        if subs.is_assigned(tp) {
            let leader_and_epoch = metadata.current_leader(tp);
            let position = FetchPosition::with_leader(
                offset_and_metadata.offset(),
                offset_and_metadata.leader_epoch(),
                leader_and_epoch,
            );

            // `seek_unvalidated` mutates the assigned partition state;
            // ignore the result because Java's seekUnvalidated is `void`
            // and any error here is reported via subsequent fetch
            // attempts.
            let _ = subs.seek_unvalidated(tp, position.clone());
            drop(subs);
            info!("Setting offset for partition {tp} to the committed offset {position}");
        } else {
            drop(subs);
            info!("Ignoring the returned {offset_and_metadata:?} since its partition {tp} is no longer assigned");
        }
    }
}

#[cfg(test)]
mod tests {
    //! No Java `ConsumerUtilsTest.java` file exists — inline unit tests
    //! cover the surface that survives translation.

    use std::collections::HashMap;

    use crate::common::internals::ClusterResourceListeners;

    use super::*;

    #[test]
    fn constants_match_java() {
        assert_eq!(DEFAULT_CLOSE_TIMEOUT_MS, 30_000);
        assert_eq!(CONSUMER_JMX_PREFIX, "kafka.consumer");
        assert_eq!(CONSUMER_METRIC_GROUP_PREFIX, "consumer");
        assert_eq!(CONSUMER_METRIC_GROUP, "consumer-metrics");
        assert_eq!(CONSUMER_SHARE_METRIC_GROUP, "consumer-share-metrics");
        assert_eq!(CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION, 100);
        assert_eq!(
            THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED,
            "internal.throw.on.fetch.stable.offset.unsupported"
        );
    }

    #[test]
    fn log_context_without_group_instance_id() {
        let prefix = create_log_context("client-1", Some("group-A"), None);
        assert_eq!(&*prefix, "[Consumer clientId=client-1, groupId=group-A] ");
    }

    #[test]
    fn log_context_with_group_instance_id() {
        let prefix = create_log_context("client-1", Some("group-A"), Some("inst-7"));
        assert_eq!(&*prefix, "[Consumer instanceId=inst-7, clientId=client-1, groupId=group-A] ");
    }

    #[test]
    fn log_context_without_group_id() {
        // Matches Java's `groupId.orElse("null")`.
        let prefix = create_log_context("c", None, None);
        assert_eq!(&*prefix, "[Consumer clientId=c, groupId=null] ");
    }

    #[test]
    fn isolation_level_parses_case_insensitively() {
        assert_eq!(
            isolation_level_from_str("read_uncommitted").unwrap(),
            IsolationLevel::ReadUncommitted
        );
        assert_eq!(
            isolation_level_from_str("READ_UNCOMMITTED").unwrap(),
            IsolationLevel::ReadUncommitted
        );
        assert_eq!(
            isolation_level_from_str("read_committed").unwrap(),
            IsolationLevel::ReadCommitted
        );
        assert_eq!(
            isolation_level_from_str("Read_Committed").unwrap(),
            IsolationLevel::ReadCommitted
        );
    }

    #[test]
    fn isolation_level_rejects_unknown() {
        let err = isolation_level_from_str("read_serializable").expect_err("must err");
        assert!(matches!(err, Error::LocalIllegalArgument(_)));
    }

    #[test]
    fn configured_isolation_level_uses_config_value() {
        let mut cfg = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        cfg.isolation_level = "read_committed".to_string();
        assert_eq!(configured_isolation_level(&cfg).unwrap(), IsolationLevel::ReadCommitted);
    }

    #[test]
    fn create_subscription_state_accepts_valid_strategy() {
        let mut cfg = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        cfg.auto_offset_reset = "earliest".to_string();
        // Round-trip succeeds for valid strategies; the exact strategy
        // is asserted by the `auto_offset_reset_strategy` tests.
        assert!(create_subscription_state(&cfg).is_ok());

        cfg.auto_offset_reset = "garbage".to_string();
        assert!(create_subscription_state(&cfg).is_err());
    }

    /// Java `maybeWrapAsKafkaException(Throwable)` (`ConsumerUtils.java:249-254`)
    /// is CONDITIONAL, not an identity: a non-`KafkaException` is wrapped in
    /// `new KafkaException(t)` so that `is_kafka_error()` answers `true`, with
    /// `t` as the cause and `t.toString()` as the wrapper's message.
    #[test]
    fn maybe_wrap_as_kafka_error_wraps_a_generic_error() {
        let err = Error::local_illegal_state("boom");
        assert!(!err.is_kafka_error(), "precondition: IllegalState is not a Kafka error");
        let rendered = err.to_string();

        let wrapped = maybe_wrap_as_kafka_error(err);

        // Class flipped into the KafkaException hierarchy — this is the whole
        // point of the helper, and what the four Java call sites rely on.
        assert!(
            wrapped.is_kafka_error(),
            "the wrap must make is_kafka_error() true: {wrapped:?}"
        );
        assert!(
            !matches!(wrapped, Error::LocalIllegalState(_)),
            "must no longer be an IllegalState: {wrapped:?}"
        );
        // Java's `Throwable(Throwable cause)` sets `detailMessage =
        // cause.toString()`, so the diagnostic is preserved verbatim.
        assert_eq!(rendered, wrapped.message(), "the wrapper's message must be cause.toString()");
        // And the cause is reachable via `source()` (Java's `getCause()`).
        let source = std::error::Error::source(&wrapped).expect("the cause must be preserved");
        assert_eq!(rendered, source.to_string(), "source() must be the original error");
    }

    /// The other half of the conditional: an error already inside the
    /// `KafkaException` hierarchy is returned UNCHANGED — Java's
    /// `if (t instanceof KafkaException) return (KafkaException) t;`.
    #[test]
    fn maybe_wrap_as_kafka_error_passes_through_a_kafka_error() {
        let err = Error::with_message(Errors::InvalidTopicError, "bad topic");
        assert!(err.is_kafka_error(), "precondition: this is a Kafka error");
        let before = err.to_string();

        let wrapped = maybe_wrap_as_kafka_error(err);

        assert_eq!(before, wrapped.to_string(), "a Kafka error must pass through verbatim");
        assert!(
            std::error::Error::source(&wrapped).is_none(),
            "pass-through must not add a wrapper cause: {wrapped:?}"
        );
    }

    /// Java `maybeWrapAsKafkaException(t, message)` (`ConsumerUtils.java:256`):
    /// a non-`KafkaException` (`IllegalStateException` /
    /// `IllegalArgumentException`) is wrapped in a new `KafkaException`
    /// whose `getMessage()` is exactly `message` (the cause is preserved
    /// separately, not folded into the message).
    #[test]
    fn maybe_wrap_as_kafka_error_with_msg_replaces_message_for_non_kafka_error() {
        // IllegalState → not a KafkaException → wrapped with exact message.
        let err = Error::local_illegal_state("always failed");
        let wrapped = maybe_wrap_as_kafka_error_with_msg(err, "User rebalance callback throws an error");
        assert!(!matches!(wrapped, Error::LocalIllegalState(_)));
        assert_eq!(wrapped.message(), "User rebalance callback throws an error");
        // Java's `new KafkaException(message, t)` keeps `t` as the cause, so it
        // must be reachable via `source()` — not merely logged.
        let source = std::error::Error::source(&wrapped).expect("the cause must be preserved");
        assert!(
            source.to_string().contains("always failed"),
            "source() must be the original error, got: {source}"
        );

        // IllegalArgument → not a KafkaException → wrapped with exact message.
        let err = Error::local_illegal_argument("bad arg");
        let wrapped = maybe_wrap_as_kafka_error_with_msg(err, "User rebalance callback throws an error");
        assert!(!matches!(wrapped, Error::LocalIllegalArgument(_)));
        assert_eq!(wrapped.message(), "User rebalance callback throws an error");
    }

    /// Java: a `KafkaException` (here a `TimeoutException`, which extends
    /// `ApiException extends KafkaException`) passes through
    /// `maybeWrapAsKafkaException(t, message)` UNCHANGED — message and all.
    #[test]
    fn maybe_wrap_as_kafka_error_with_msg_passes_kafka_error_through() {
        // Timeout IS a KafkaException → returned unchanged.
        let err = Error::timeout("deadline");
        let wrapped = maybe_wrap_as_kafka_error_with_msg(err, "in commit");
        match wrapped {
            Error::Timeout(msg) => assert_eq!(msg.message(), "deadline"),
            other => panic!("expected Timeout unchanged, got {other:?}"),
        }

        // Serialization IS a KafkaException → returned unchanged.
        let err = Error::serialization("bad bytes".to_string());
        let wrapped = maybe_wrap_as_kafka_error_with_msg(err, "should be ignored");
        match wrapped {
            Error::Serialization(msg) => assert_eq!(msg.message(), "bad bytes"),
            other => panic!("expected Serialization unchanged, got {other:?}"),
        }

        // Wakeup IS a KafkaException → returned unchanged.
        let err = Error::wakeup("woken");
        let wrapped = maybe_wrap_as_kafka_error_with_msg(err, "should be ignored");
        assert!(matches!(wrapped, Error::Wakeup(ref m) if m.message() == "woken"));
    }

    #[test]
    fn refresh_committed_offsets_seeds_position_for_assigned_partition() {
        use std::collections::HashSet;

        let cfg = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let subs = Arc::new(Mutex::new(create_subscription_state(&cfg).unwrap()));
        let metadata = ConsumerMetadata::from_config(&cfg, Arc::clone(&subs), ClusterResourceListeners::new());

        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut guard = subs.lock().unwrap();
            guard
                .assign_from_user([tp.clone()].into_iter().collect::<HashSet<_>>())
                .unwrap();
        }

        let mut offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        offsets.insert(
            tp.clone(),
            OffsetAndMetadata::with_leader_epoch(42, Some(7), "metadata".to_string()).unwrap(),
        );

        refresh_committed_offsets(&offsets, &metadata, &subs);

        let guard = subs.lock().unwrap();
        let pos = guard.position(&tp).unwrap().expect("position set");
        assert_eq!(pos.offset, 42);
        assert_eq!(pos.offset_epoch, Some(7));
    }

    #[test]
    fn refresh_committed_offsets_skips_unassigned_partition() {
        let cfg = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let subs = Arc::new(Mutex::new(create_subscription_state(&cfg).unwrap()));
        let metadata = ConsumerMetadata::from_config(&cfg, Arc::clone(&subs), ClusterResourceListeners::new());

        // No assignment — partition is unknown.
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        offsets.insert(
            tp.clone(),
            OffsetAndMetadata::with_leader_epoch(99, Some(5), String::new()).unwrap(),
        );

        // Must not panic and must not seek anything.
        refresh_committed_offsets(&offsets, &metadata, &subs);

        let guard = subs.lock().unwrap();
        assert!(guard.position_or_null(&tp).is_none());
    }
}
