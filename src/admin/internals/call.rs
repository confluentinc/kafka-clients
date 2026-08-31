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

use crate::common::requests::{ConcreteResponse, RequestBuilder};
use crate::common::{KafkaError, Node};
use crate::kafka_client::KafkaClient;
use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;

use super::admin_metadata_manager::AdminMetadataManager;

/// Builds the request body for a [`Call`] given the per-attempt timeout.
pub(crate) type CreateRequestFn = Box<dyn FnMut(i32) -> Result<Box<dyn RequestBuilder>, KafkaError> + Send>;
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
pub(crate) type HandleFailureFn = Box<dyn FnMut(&KafkaError) + Send>;
/// Unsupported-version hook; returns `true` iff the call should be retried after
/// a protocol downgrade (without spending a retry).
pub(crate) type HandleUnsupportedVersionFn = Box<dyn FnMut() -> bool + Send>;
/// Retry hook invoked from `Call.fail`'s retriable branch (mirrors Java's
/// `Call.maybeRetry`). Returns whether the runnable should re-queue this call or
/// the hook has taken over (e.g. the [`AdminApiDriver`] re-issued requests).
pub(crate) type MaybeRetryFn = Box<dyn FnMut(&KafkaError, i64) -> MaybeRetryOutcome + Send>;

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
    /// Enqueue a fresh follow-up call (e.g. quota-exceeded retry topics). The
    /// current call is finished.
    NewCall(Box<Call>),
    /// The whole call must be retried; route the error through
    /// [`fail`](super::admin_client_runnable) (respecting backoff / retries).
    /// Mirrors an exception escaping Java's `handleResponse` (e.g.
    /// `NOT_CONTROLLER`).
    Retry(KafkaError),
}

/// Strategy for selecting the target node of a [`Call`].
///
/// Corresponds to the `NodeProvider` implementations in `KafkaAdminClient`.
pub(crate) enum NodeProvider {
    /// Targets the cluster controller (createTopics / deleteTopics /
    /// createPartitions).
    Controller,
    /// Targets the least-loaded broker (listTopics / describeTopics, driver
    /// lookup requests).
    LeastLoaded,
    /// Targets the least-loaded broker, or the active controller when the
    /// client uses `bootstrap.controllers` (KIP-919). Mirrors
    /// `LeastLoadedBrokerOrActiveKController`. Because `bootstrap.controllers`
    /// is unsupported here (`using_bootstrap_controllers()` is always false),
    /// its `provide` behaves like [`NodeProvider::LeastLoaded`]; it differs only
    /// in `supports_use_controllers`, used by `describeCluster` /
    /// `describeConfigs` / `incrementalAlterConfigs`.
    LeastLoadedBrokerOrActiveKController,
    /// Targets the least-loaded node for the internal metadata refresh call.
    MetadataUpdate,
    /// Targets a specific broker id (driver fulfillment requests).
    ///
    /// Mirrors `ConstantNodeIdProvider`.
    ConstantNodeId(i32),
}

impl NodeProvider {
    /// Whether the provider may run when the client uses `bootstrap.controllers`
    /// (KIP-919). Mirrors `NodeProvider.supportsUseControllers`.
    pub(crate) fn supports_use_controllers(&self) -> bool {
        match self {
            NodeProvider::Controller => false,
            NodeProvider::LeastLoaded => false,
            NodeProvider::LeastLoadedBrokerOrActiveKController => true,
            NodeProvider::MetadataUpdate => true,
            NodeProvider::ConstantNodeId(_) => false,
        }
    }

    /// Selects a node for the call, mirroring `NodeProvider.provide`.
    ///
    /// Returns `Ok(Some(node))` when a node is assigned, `Ok(None)` when the
    /// call must stay pending (metadata not ready / no available node), and
    /// `Err(_)` when the call should fail (a stored fatal metadata error).
    pub(crate) fn provide<C: KafkaClient>(
        &self,
        metadata_manager: &AdminMetadataManager,
        client: &C,
        now: i64,
    ) -> Result<Option<Node>, KafkaError> {
        match self {
            NodeProvider::MetadataUpdate => {
                // Mirrors MetadataUpdateNodeIdProvider (rebootstrap only applies
                // under MetadataRecoveryStrategy::Rebootstrap, deferred here).
                let _ = MetadataRecoveryStrategy::None;
                Ok(client.least_loaded_node(now).node().cloned())
            },
            NodeProvider::Controller => {
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
                        match metadata_manager.controller() {
                            Some(node) => Ok(Some(node)),
                            None => {
                                metadata_manager.request_update();
                                Ok(None)
                            },
                        }
                    } else {
                        Ok(client.least_loaded_node(now).node().cloned())
                    }
                } else {
                    metadata_manager.request_update();
                    Ok(None)
                }
            },
            NodeProvider::ConstantNodeId(node_id) => {
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
        }
    }

    /// Sets the `maybe_retry` override (mirrors overriding `Call.maybeRetry`).
    pub(crate) fn set_maybe_retry_fn(&mut self, f: MaybeRetryFn) {
        self.maybe_retry_fn = Some(f);
    }

    /// Runs the retry hook from `fail`'s retriable branch, returning whether the
    /// runnable should re-queue this call. Mirrors `Call.maybeRetry`; the
    /// default (no hook) requeues.
    pub(crate) fn maybe_retry(&mut self, error: &KafkaError, now: i64) -> MaybeRetryOutcome {
        match self.maybe_retry_fn.as_mut() {
            Some(f) => f(error, now),
            None => MaybeRetryOutcome::Requeue,
        }
    }

    /// Creates an internal (metadata) call.
    pub(crate) fn new_internal(
        call_name: impl Into<String>,
        deadline_ms: i64,
        node_provider: NodeProvider,
        create_request_fn: CreateRequestFn,
        handle_response_fn: HandleResponseFn,
        handle_failure_fn: HandleFailureFn,
    ) -> Self {
        let mut call = Self::new(
            call_name,
            deadline_ms,
            node_provider,
            create_request_fn,
            handle_response_fn,
            handle_failure_fn,
            Box::new(|| false),
        );
        call.internal = true;
        call
    }

    /// Builds the request body for this attempt.
    pub(crate) fn create_request(&mut self, timeout_ms: i32) -> Result<Box<dyn RequestBuilder>, KafkaError> {
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
    pub(crate) fn handle_failure(&mut self, error: &KafkaError) {
        (self.handle_failure_fn)(error);
    }

    /// Runs the unsupported-version hook; returns `true` iff the call should be
    /// retried after a protocol downgrade.
    pub(crate) fn handle_unsupported_version(&mut self) -> bool {
        (self.handle_unsupported_version_fn)()
    }
}
