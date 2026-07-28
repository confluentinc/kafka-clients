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

//! The scope used by [`AdminApiDriver`](super::admin_api_driver::AdminApiDriver)
//! to group key lookups and to bridge to the internal `NodeProvider`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.internals.ApiRequestScope`.
//! Java models this as an interface with per-strategy implementations
//! (`SINGLE_REQUEST_SCOPE`, `FulfillmentScope`, and per-key coordinator scopes).
//! In Rust we use a closed enum:
//! - [`SingleLookup`](ApiRequestScope::SingleLookup) is the shared "batch all
//!   keys into one lookup" scope used by `PartitionLeaderStrategy` and by
//!   `CoordinatorStrategy` in its default batched mode (mirrors Java's
//!   `CoordinatorStrategy.BATCH_REQUEST_SCOPE`).
//! - [`CoordinatorLookup`](ApiRequestScope::CoordinatorLookup) is the per-key
//!   scope used by `CoordinatorStrategy` after `disable_batch()` (mirrors Java's
//!   `CoordinatorStrategy.LookupRequestScope`), so each unbatchable coordinator
//!   key gets its own lookup request.

use super::coordinator_key::CoordinatorKey;

/// Indicates how lookup requests can be batched together and the target broker
/// (if any) for a request.
///
/// Corresponds to `ApiRequestScope`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum ApiRequestScope {
    /// A single shared lookup scope: all keys are batched into one lookup
    /// request (`PartitionLeaderStrategy`, batched `CoordinatorStrategy`). Has no
    /// destination broker, so lookup is required.
    SingleLookup,
    /// A per-key coordinator lookup scope, used by `CoordinatorStrategy` when
    /// batching is disabled. Has no destination broker, so lookup is required.
    ///
    /// Corresponds to `CoordinatorStrategy.LookupRequestScope`.
    #[allow(dead_code)] // wired by CoordinatorStrategy/driver later in this phase
    CoordinatorLookup(CoordinatorKey),
    /// A fulfillment scope keyed by destination broker id. Each destination
    /// broker in the fulfillment stage gets its own request scope.
    ///
    /// Corresponds to `AdminApiDriver.FulfillmentScope`.
    Fulfillment(i32),
}

impl ApiRequestScope {
    /// The target broker id that a request is intended for, or `None` if the
    /// request can be sent to any broker (i.e. lookup is required first).
    ///
    /// Mirrors `ApiRequestScope.destinationBrokerId`.
    pub(crate) fn destination_broker_id(&self) -> Option<i32> {
        match self {
            ApiRequestScope::SingleLookup => None,
            ApiRequestScope::CoordinatorLookup(_) => None,
            ApiRequestScope::Fulfillment(broker_id) => Some(*broker_id),
        }
    }
}
