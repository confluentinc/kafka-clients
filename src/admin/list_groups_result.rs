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

//! The result of `Admin::list_groups`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListGroupsResult`.

use crate::admin::GroupListing;
use crate::common::{Error, KafkaFuture};

/// The result of `Admin::list_groups`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListGroupsResult`.
///
/// Java's constructor takes a single `KafkaFuture<Collection<Object>>` mixing
/// `GroupListing`s and `Throwable`s and, via `thenApply`, splits it into the
/// `all`/`valid`/`errors` futures. The Rust port models the mixed collection as
/// `Vec<Result<GroupListing, Error>>` (a listing is `Ok`, an error is
/// `Err`) and derives the three views lazily with `then_apply`/`then_apply_try`
/// — semantically identical (`all` fails with the first error; `valid`/`errors`
/// never fail).
#[derive(Clone, Debug)]
pub struct ListGroupsResult {
    source: KafkaFuture<Vec<Result<GroupListing, Error>>>,
}

impl ListGroupsResult {
    /// Creates a result from the combined per-broker listings-or-errors future.
    pub(crate) fn new(source: KafkaFuture<Vec<Result<GroupListing, Error>>>) -> Self {
        Self { source }
    }

    /// A future yielding either the first error, or the full set of listings.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<Vec<GroupListing>> {
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
    pub fn valid(&self) -> KafkaFuture<Vec<GroupListing>> {
        self.source
            .then_apply(|results| results.into_iter().filter_map(Result::ok).collect())
    }

    /// A future yielding just the errors (never fails). Mirrors `errors()`.
    pub fn errors(&self) -> KafkaFuture<Vec<Error>> {
        self.source
            .then_apply(|results| results.into_iter().filter_map(Result::err).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::GroupType;

    fn listing(id: &str) -> GroupListing {
        GroupListing::new(id, Some(GroupType::Consumer), "consumer", None)
    }

    #[tokio::test]
    async fn valid_and_errors_split() {
        let source = KafkaFuture::completed(Ok(vec![
            Ok(listing("g1")),
            Err(Error::local_illegal_state("boom")),
            Ok(listing("g2")),
        ]));
        let result = ListGroupsResult::new(source);
        assert_eq!(result.valid().get().await.unwrap().len(), 2);
        assert_eq!(result.errors().get().await.unwrap().len(), 1);
        // all() fails with the first error.
        assert!(result.all().get().await.is_err());
    }

    #[tokio::test]
    async fn all_succeeds_when_no_errors() {
        let source = KafkaFuture::completed(Ok(vec![Ok(listing("g1"))]));
        let result = ListGroupsResult::new(source);
        assert_eq!(result.all().get().await.unwrap(), vec![listing("g1")]);
    }
}
