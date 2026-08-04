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

//! The result of `Admin::list_consumer_groups`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupsResult`
//! (deprecated since 4.1 in favor of `Admin::list_groups`).

#![allow(deprecated)]

use crate::admin::ConsumerGroupListing;
use crate::common::{KafkaError, KafkaFuture};

/// The result of `Admin::list_consumer_groups`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupsResult`
/// (deprecated since 4.1). See [`ListGroupsResult`](crate::admin::ListGroupsResult)
/// for the `Vec<Result<..>>` modeling of Java's mixed `Collection<Object>`.
#[deprecated(since = "4.1.0", note = "Use Admin::list_groups instead")]
#[derive(Clone, Debug)]
pub struct ListConsumerGroupsResult {
    source: KafkaFuture<Vec<Result<ConsumerGroupListing, KafkaError>>>,
}

impl ListConsumerGroupsResult {
    /// Creates a result from the combined per-broker listings-or-errors future.
    pub(crate) fn new(source: KafkaFuture<Vec<Result<ConsumerGroupListing, KafkaError>>>) -> Self {
        Self { source }
    }

    /// A future yielding either the first error, or the full set of listings.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<Vec<ConsumerGroupListing>> {
        self.source.then_apply_try(|results| {
            let mut valid = Vec::new();
            for result in results {
                match result {
                    Ok(listing) => valid.push(listing),
                    Err(error) => return Err(error),
                }
            }
            Ok(valid)
        })
    }

    /// A future yielding just the valid listings (never fails). Mirrors `valid()`.
    pub fn valid(&self) -> KafkaFuture<Vec<ConsumerGroupListing>> {
        self.source
            .then_apply(|results| results.into_iter().filter_map(Result::ok).collect())
    }

    /// A future yielding just the errors (never fails). Mirrors `errors()`.
    pub fn errors(&self) -> KafkaFuture<Vec<KafkaError>> {
        self.source
            .then_apply(|results| results.into_iter().filter_map(Result::err).collect())
    }
}
