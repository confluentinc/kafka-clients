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

//! The lookup strategy used when the destination broker id is already known.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.StaticBrokerStrategy`.

use std::collections::HashSet;
use std::marker::PhantomData;

use crate::common::requests::{ConcreteResponse, RequestBuilder};

use super::ApiRequestScope;
use super::{AdminApiLookupStrategy, LookupResult};

/// A lookup strategy for cases where the destination broker id is already known
/// and no explicit lookup is required.
///
/// Corresponds to `StaticBrokerStrategy<K>`. By returning a scope whose
/// [`destination_broker_id`](ApiRequestScope::destination_broker_id) is set, the
/// driver skips the lookup stage entirely, so
/// [`build_request`](AdminApiLookupStrategy::build_request) and
/// [`handle_response`](AdminApiLookupStrategy::handle_response) are never
/// invoked (they panic, mirroring Java's `UnsupportedOperationException`).
pub(crate) struct StaticBrokerStrategy<K> {
    broker_id: i32,
    _marker: PhantomData<fn() -> K>,
}

impl<K> StaticBrokerStrategy<K> {
    /// Creates a strategy targeting the given broker id.
    pub(crate) fn new(broker_id: i32) -> Self {
        Self { broker_id, _marker: PhantomData }
    }
}

impl<K: Send> AdminApiLookupStrategy<K> for StaticBrokerStrategy<K> {
    fn lookup_scope(&self, _key: &K) -> ApiRequestScope {
        ApiRequestScope::Fulfillment(self.broker_id)
    }

    fn build_request(&self, _keys: &HashSet<K>) -> Box<dyn RequestBuilder> {
        // Mirrors Java's `throw new UnsupportedOperationException()`: the driver
        // never calls this because `lookup_scope` returns a fulfillment scope
        // with a known destination broker, so no lookup phase occurs.
        panic!("StaticBrokerStrategy.build_request should never be called: lookup is skipped")
    }

    fn handle_response(&self, _keys: &HashSet<K>, _response: &ConcreteResponse) -> LookupResult<K> {
        panic!("StaticBrokerStrategy.handle_response should never be called: lookup is skipped")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::TopicPartition;

    #[test]
    fn lookup_scope_targets_the_static_broker() {
        let strategy: StaticBrokerStrategy<TopicPartition> = StaticBrokerStrategy::new(5);
        let scope = strategy.lookup_scope(&TopicPartition::new("t", 0));
        assert_eq!(scope.destination_broker_id(), Some(5));
    }

    #[test]
    #[should_panic(expected = "should never be called")]
    fn build_request_panics() {
        let strategy: StaticBrokerStrategy<TopicPartition> = StaticBrokerStrategy::new(5);
        let _ = strategy.build_request(&HashSet::new());
    }
}
