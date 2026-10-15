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

//! The per-request retry unit driven by the admin background task.
//!
//! Corresponds to the `Call` abstract class and the `NodeProvider` hierarchy in
//! `org.apache.kafka.clients.admin.KafkaAdminClient`. Per
//! `.claude/rules/admin-client.md` §2 these are plain structs/enums driven by
//! the background task — never `#[async_trait]` traits. The per-`Call`
//! `create_request` / `handle_response` / `handle_failure` /
//! `handle_unsupported_version` hooks (Java abstract methods on an anonymous
//! subclass) are modelled as boxed sync closures owned by the [`Call`].

use crate::KafkaClient;
use crate::MetadataRecoveryStrategy;
use crate::common::errors::TimeoutError;
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, RequestBuilder};
use crate::common::{Error, Node};

use super::AdminMetadataManager;

/// Builds the request body for a [`Call`] given the per-attempt timeout.
pub(crate) type CreateRequestFn = Box<dyn FnMut(i32) -> Result<Box<dyn RequestBuilder>, Error> + Send>;
/// Processes a successful response for a [`Call`].
///
/// The third argument is the node the request was actually sent to — Java's
/// `Call.curNode()`, which `KafkaAdminClient.newCall` hands to
/// `AdminApiDriver.onResponse` (and from there to `AdminApiHandler`, which may
/// store it in public API such as `ConsumerGroupDescription.coordinator()`).
/// Because a closure cannot reach `Call`'s own fields the way a Java anonymous
/// subclass reaches `curNode()`, the node is passed in explicitly. It is `None`
/// only before the node provider has assigned one, which cannot happen on the
/// response path.
pub(crate) type HandleResponseFn = Box<dyn FnMut(&ConcreteResponse, i64, Option<&Node>) -> HandleResult + Send>;
/// Terminal-failure hook for a [`Call`].
pub(crate) type HandleFailureFn = Box<dyn FnMut(&Error) + Send>;
/// Unsupported-version hook, given the `UnsupportedVersionException` the call
/// failed with; returns `true` iff the call should be retried after a protocol
/// downgrade (without spending a retry). Mirrors
/// `Call.handleUnsupportedVersionException(UnsupportedVersionException)`.
pub(crate) type HandleUnsupportedVersionFn = Box<dyn FnMut(&Error) -> bool + Send>;
/// Retry hook invoked from `Call.fail`'s retriable branch (mirrors Java's
/// `Call.maybeRetry`). Returns whether the runnable should re-queue this call or
/// the hook has taken over (e.g. the [`AdminApiDriver`] re-issued requests).
pub(crate) type MaybeRetryFn = Box<dyn FnMut(&Error, i64) -> MaybeRetryOutcome + Send>;
/// Hook invoked by the polling loop when no node could be assigned to a call
/// (mirrors Java's `Call.handleNodeUnavailable`). Returns `true` if the call
/// took corrective action and should be removed from the pending queue, `false`
/// to remain pending and retry node assignment on a later iteration.
pub(crate) type HandleNodeUnavailableFn = Box<dyn FnMut(&AdminMetadataManager, i64) -> bool + Send>;

/// The outcome of [`Call::maybe_retry`].
pub(crate) enum MaybeRetryOutcome {
    /// The runnable should re-queue this call into `pending_calls` (Java's
    /// default `maybeRetry`).
    Requeue,
    /// The hook handled the failure itself (the current call is finished).
    Handled,
}

/// The outcome of [`Call::handle_response`].
pub(crate) enum HandleResult {
    /// Every per-key future was completed (terminal). No further action.
    Done,
    /// The call is finished, and the runnable moves every call that is
    /// assigned to a node but not yet sent back to the pending calls, so that
    /// a new destination is chosen from the metadata just applied. Java's
    /// metadata calls end their `handleResponse` with
    /// `unassignUnsentCalls(node -> true)` (`KafkaAdminClient.java:1756`,
    /// `:1799`), a method of the runnable a Rust hook cannot call.
    UnassignUnsentCalls,
    /// Enqueue a fresh follow-up call (e.g. quota-exceeded retry topics). The
    /// current call is finished.
    NewCall(Box<Call>),
    /// Issue this same call again, keeping its deadline, tries and backoff
    /// state: Java's `runnable.call(this, now)` from inside `handleResponse`,
    /// which `describeTopicPartitions` uses to request the next page
    /// (`KafkaAdminClient.java:2414-2416`). The Rust hook cannot hand back
    /// `self`, so the runnable re-queues the call it just dispatched.
    CallAgain,
    /// The whole call must be retried; route the error through
    /// [`fail`](super::admin_client_runnable) (respecting backoff / retries).
    /// Mirrors an exception escaping Java's `handleResponse` (e.g.
    /// `NOT_CONTROLLER`).
    Retry(Error),
}

/// Strategy for selecting the target node of a [`Call`].
///
/// Corresponds to the `NodeProvider` implementations in `KafkaAdminClient`.
pub(crate) enum NodeProvider {
    /// Targets the cluster controller (createTopics / deleteTopics /
    /// createPartitions). Mirrors `ControllerNodeProvider`.
    Controller {
        /// Java's `ControllerNodeProvider(boolean supportsUseControllers)`
        /// argument: whether a `bootstrap.controllers` client may send the call
        /// (to the active controller). Java's no-argument constructor sets it to
        /// `false`.
        supports_use_controllers: bool,
    },
    /// Targets the least-loaded broker (listTopics / describeTopics, driver
    /// lookup requests).
    LeastLoaded,
    /// Targets the least-loaded broker, or the active controller when the
    /// client uses `bootstrap.controllers` (KIP-919). Mirrors
    /// `LeastLoadedBrokerOrActiveKController`.
    LeastLoadedBrokerOrActiveKController,
    /// Targets the least-loaded node for the internal metadata refresh call.
    MetadataUpdate,
    /// Targets a specific node id (driver fulfillment requests).
    ///
    /// Mirrors `ConstantNodeIdProvider`.
    ConstantNodeId {
        /// The id of the node to send the call to.
        node_id: i32,
        /// Java's `ConstantNodeIdProvider(int nodeId, boolean
        /// supportsUseControllers)` argument: whether a `bootstrap.controllers`
        /// client may send the call (to the controller with that id). Java's
        /// one-argument constructor sets it to `false`.
        supports_use_controllers: bool,
    },
}

impl NodeProvider {
    /// Whether the provider may run when the client uses `bootstrap.controllers`
    /// (KIP-919). Mirrors `NodeProvider.supportsUseControllers`: fixed for
    /// three providers, and the constructor argument of
    /// `ControllerNodeProvider` and `ConstantNodeIdProvider`.
    pub(crate) fn supports_use_controllers(&self) -> bool {
        match self {
            NodeProvider::Controller { supports_use_controllers } => *supports_use_controllers,
            NodeProvider::LeastLoaded => false,
            NodeProvider::LeastLoadedBrokerOrActiveKController => true,
            NodeProvider::MetadataUpdate => true,
            NodeProvider::ConstantNodeId { supports_use_controllers, .. } => *supports_use_controllers,
        }
    }

    /// Selects a node for the call, mirroring `NodeProvider.provide`.
    ///
    /// Returns `Ok(Some(node))` when a node is assigned, `Ok(None)` when the
    /// call must stay pending (metadata not ready / no available node), and
    /// `Err(_)` when the call should fail (a stored fatal metadata error).
    ///
    /// `metadata_recovery_strategy` is the `KafkaAdminClient` field
    /// `MetadataUpdateNodeIdProvider` reads.
    pub(crate) fn provide<C: KafkaClient>(
        &self,
        metadata_manager: &AdminMetadataManager,
        client: &C,
        metadata_recovery_strategy: MetadataRecoveryStrategy,
        now: i64,
    ) -> Result<Option<Node>, Error> {
        match self {
            NodeProvider::MetadataUpdate => {
                // Mirrors MetadataUpdateNodeIdProvider (`KafkaAdminClient.java:788-800`):
                // once a bootstrap cluster is installed (KIP-909 resolves it
                // asynchronously), with no node available or connection-ready, go
                // back to the bootstrap cluster, and still return whatever node
                // was found.
                let least_loaded_node = client.least_loaded_node(now);
                if metadata_recovery_strategy == MetadataRecoveryStrategy::Rebootstrap
                    && metadata_manager.is_bootstrapped()
                    && !least_loaded_node.has_node_available_or_connection_ready()
                {
                    metadata_manager.rebootstrap(now);
                }
                Ok(least_loaded_node.node().cloned())
            },
            NodeProvider::Controller { .. } => {
                if metadata_manager.is_ready()? {
                    match metadata_manager.controller() {
                        Some(node) => Ok(Some(node)),
                        None => {
                            metadata_manager.request_update();
                            Ok(None)
                        },
                    }
                } else {
                    metadata_manager.request_update();
                    Ok(None)
                }
            },
            NodeProvider::LeastLoaded => {
                if metadata_manager.is_ready()? {
                    Ok(client.least_loaded_node(now).node().cloned())
                } else {
                    metadata_manager.request_update();
                    Ok(None)
                }
            },
            NodeProvider::LeastLoadedBrokerOrActiveKController => {
                if metadata_manager.is_ready()? {
                    if metadata_manager.using_bootstrap_controllers() {
                        // `return metadataManager.controller();`
                        // (`KafkaAdminClient.java:901-902`): with no known
                        // controller the call stays pending, and unlike
                        // `ControllerNodeProvider` this provider does not
                        // request a metadata update for it.
                        Ok(metadata_manager.controller())
                    } else {
                        Ok(client.least_loaded_node(now).node().cloned())
                    }
                } else {
                    metadata_manager.request_update();
                    Ok(None)
                }
            },
            NodeProvider::ConstantNodeId { node_id, .. } => {
                // Mirrors ConstantNodeIdProvider: if we can't find the node with
                // the given id, schedule a metadata update and hope it appears.
                if metadata_manager.is_ready()?
                    && let Some(node) = metadata_manager.node_by_id(*node_id)
                {
                    return Ok(Some(node));
                }
                metadata_manager.request_update();
                Ok(None)
            },
        }
    }
}

/// A single administrative request, with retry bookkeeping.
///
/// Corresponds to the `Call` abstract class.
pub(crate) struct Call {
    /// Human-readable name (e.g. `"createTopics"`), used in log/timeout messages.
    pub(crate) call_name: String,
    /// Whether this is an internal (metadata) call vs. an external (user) call.
    pub(crate) internal: bool,
    /// Absolute deadline in epoch milliseconds.
    pub(crate) deadline_ms: i64,
    /// The earliest epoch-ms time this call may be retried (backoff gate).
    pub(crate) next_allowed_try_ms: i64,
    /// The number of attempts made so far.
    pub(crate) tries: i32,
    /// The node currently assigned to this call, if any.
    pub(crate) cur_node: Option<Node>,
    /// The node-selection strategy.
    pub(crate) node_provider: NodeProvider,
    create_request_fn: CreateRequestFn,
    handle_response_fn: HandleResponseFn,
    handle_failure_fn: HandleFailureFn,
    handle_unsupported_version_fn: HandleUnsupportedVersionFn,
    /// Optional override of `Call.maybeRetry` (used by the driver so a disconnect
    /// retries lookup rather than re-sending to a dead node). `None` mirrors
    /// Java's default `maybeRetry` (re-queue into pending calls).
    maybe_retry_fn: Option<MaybeRetryFn>,
    /// Optional override of `Call.handleNodeUnavailable` (used by the
    /// `AdminApiDriver` partition-leader calls so a fulfillment target that has
    /// left the cluster is sent back to the lookup stage). `None` mirrors Java's
    /// default `handleNodeUnavailable`, which returns `false`.
    handle_node_unavailable_fn: Option<HandleNodeUnavailableFn>,
}

impl std::fmt::Display for Call {
    /// `Call.toString()` (`KafkaAdminClient.java:1082-1085`):
    ///
    /// ```java
    /// return "Call(callName=" + callName + ", deadlineMs=" + deadlineMs +
    ///     ", tries=" + tries + ", nextAllowedTryMs=" + nextAllowedTryMs + ")";
    /// ```
    ///
    /// Java embeds this rendering in the `TimeoutException` message that
    /// `handleTimeoutFailure` builds, so it is part of the observable text.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Call(callName={}, deadlineMs={}, tries={}, nextAllowedTryMs={})",
            self.call_name, self.deadline_ms, self.tries, self.next_allowed_try_ms
        )
    }
}

impl Call {
    /// Creates an external (user-facing) call.
    pub(crate) fn new(
        call_name: impl Into<String>,
        deadline_ms: i64,
        node_provider: NodeProvider,
        create_request_fn: CreateRequestFn,
        handle_response_fn: HandleResponseFn,
        handle_failure_fn: HandleFailureFn,
        handle_unsupported_version_fn: HandleUnsupportedVersionFn,
    ) -> Self {
        Self {
            call_name: call_name.into(),
            internal: false,
            deadline_ms,
            next_allowed_try_ms: 0,
            tries: 0,
            cur_node: None,
            node_provider,
            create_request_fn,
            handle_response_fn,
            handle_failure_fn,
            handle_unsupported_version_fn,
            maybe_retry_fn: None,
            handle_node_unavailable_fn: None,
        }
    }

    /// Sets the `maybe_retry` override (mirrors overriding `Call.maybeRetry`).
    pub(crate) fn set_maybe_retry_fn(&mut self, f: MaybeRetryFn) {
        self.maybe_retry_fn = Some(f);
    }

    /// Sets the `handle_node_unavailable` override (mirrors overriding
    /// `Call.handleNodeUnavailable`).
    pub(crate) fn set_handle_node_unavailable_fn(&mut self, f: HandleNodeUnavailableFn) {
        self.handle_node_unavailable_fn = Some(f);
    }

    /// Invoked by the polling loop when no node could be assigned to this call.
    /// Returns `true` if the call took corrective action and should be removed
    /// from the pending queue, `false` to remain pending. Mirrors
    /// `Call.handleNodeUnavailable`; the default (no hook) returns `false`.
    pub(crate) fn handle_node_unavailable(&mut self, metadata_manager: &AdminMetadataManager, now: i64) -> bool {
        match self.handle_node_unavailable_fn.as_mut() {
            Some(f) => f(metadata_manager, now),
            None => false,
        }
    }

    /// Runs the retry hook from `fail`'s retriable branch, returning whether the
    /// runnable should re-queue this call. Mirrors `Call.maybeRetry`; the
    /// default (no hook) requeues.
    pub(crate) fn maybe_retry(&mut self, error: &Error, now: i64) -> MaybeRetryOutcome {
        match self.maybe_retry_fn.as_mut() {
            Some(f) => f(error, now),
            None => MaybeRetryOutcome::Requeue,
        }
    }

    /// Creates an internal (metadata) call: Java's `Call(true, ..)`.
    pub(crate) fn new_internal(
        call_name: impl Into<String>,
        deadline_ms: i64,
        node_provider: NodeProvider,
        create_request_fn: CreateRequestFn,
        handle_response_fn: HandleResponseFn,
        handle_failure_fn: HandleFailureFn,
        handle_unsupported_version_fn: HandleUnsupportedVersionFn,
    ) -> Self {
        let mut call = Self::new(
            call_name,
            deadline_ms,
            node_provider,
            create_request_fn,
            handle_response_fn,
            handle_failure_fn,
            handle_unsupported_version_fn,
        );
        call.internal = true;
        call
    }

    /// Builds the request body for this attempt.
    pub(crate) fn create_request(&mut self, timeout_ms: i32) -> Result<Box<dyn RequestBuilder>, Error> {
        (self.create_request_fn)(timeout_ms)
    }

    /// Processes a successful response.
    ///
    /// Passes `cur_node` to the hook, mirroring `driver.onResponse(..., this.curNode())`
    /// in `KafkaAdminClient.newCall`. The fields are destructured so the hook's
    /// `&mut` borrow and the node's shared borrow stay disjoint.
    pub(crate) fn handle_response(&mut self, response: &ConcreteResponse, now: i64) -> HandleResult {
        let Self { cur_node, handle_response_fn, .. } = self;
        (handle_response_fn)(response, now, cur_node.as_ref())
    }

    /// Runs the terminal-failure hook.
    pub(crate) fn handle_failure(&mut self, error: &Error) {
        (self.handle_failure_fn)(error);
    }

    /// Wraps a non-timeout cause as a timeout and fails the call terminally.
    ///
    /// Translated from `Call.handleTimeoutFailure`. A cause that already is a
    /// timeout (`cause instanceof TimeoutException`) is passed through
    /// unchanged; anything else is wrapped with the call's rendering.
    pub(crate) fn handle_timeout_failure(&mut self, now: i64, cause: Error) {
        let error = if cause.error() == Errors::RequestTimedOut {
            cause
        } else {
            // `new TimeoutException(this + " timed out at " + now + " after " + tries
            // + " attempt(s)", cause)` (`KafkaAdminClient.java:1043-1044`), where `this`
            // renders through `Call.toString()` (`:1082-1085`).
            //
            // The message used to gain an invented `"Aborted due to timeout: "`
            // prefix (a string that appears nowhere in the Kafka tree), drop the
            // `Call(...)` rendering, and append the cause as text — which left
            // `Error::source()` empty where Java's `getCause()` is populated.
            let message = format!("{} timed out at {} after {} attempt(s)", self, now, self.tries);
            Error::Timeout(TimeoutError::with_source(message, cause))
        };
        self.handle_failure(&error);
    }

    /// Runs the unsupported-version hook with the `UnsupportedVersionException`
    /// the call failed with; returns `true` iff the call should be retried after
    /// a protocol downgrade.
    pub(crate) fn handle_unsupported_version(&mut self, error: &Error) -> bool {
        (self.handle_unsupported_version_fn)(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `supportsUseControllers()` of each Java `NodeProvider`
    /// (`KafkaAdminClient.java:788-917`): `MetadataUpdateNodeIdProvider` and
    /// `LeastLoadedBrokerOrActiveKController` answer `true`,
    /// `LeastLoadedNodeProvider` `false`, and `ControllerNodeProvider` /
    /// `ConstantNodeIdProvider` answer what their constructor was given.
    #[test]
    fn supports_use_controllers_follows_the_java_providers() {
        assert!(NodeProvider::MetadataUpdate.supports_use_controllers());
        assert!(NodeProvider::LeastLoadedBrokerOrActiveKController.supports_use_controllers());
        assert!(!NodeProvider::LeastLoaded.supports_use_controllers());
        for flag in [false, true] {
            assert_eq!(
                NodeProvider::Controller { supports_use_controllers: flag }.supports_use_controllers(),
                flag
            );
            assert_eq!(
                NodeProvider::ConstantNodeId { node_id: 3, supports_use_controllers: flag }.supports_use_controllers(),
                flag
            );
        }
    }
}
