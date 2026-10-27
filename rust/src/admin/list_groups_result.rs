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

use crate::admin::GroupListing;
use crate::common::{Error, KafkaFuture};

/// The result of `Admin::list_groups`.
// Java's constructor takes a single `KafkaFuture<Collection<Object>>` mixing
// `GroupListing`s and `Throwable`s and, via `thenApply`, splits it into the
// `all`/`valid`/`errors` futures. The Rust port models the mixed collection as
// `Vec<Result<GroupListing, Error>>` (a listing is `Ok`, an error is
// `Err`) and derives the three views lazily with `then_apply`
// — semantically identical (`all` fails with the first error; `valid`/`errors`
// never fail).
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.ListGroupsResult")]
pub struct ListGroupsResult {
    source: KafkaFuture<Vec<Result<GroupListing, Error>>>,
}

impl ListGroupsResult {
    /// Creates a result from the combined per-broker listings-or-errors future.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsResult#ListGroupsResult")]
    pub(crate) fn new(source: KafkaFuture<Vec<Result<GroupListing, Error>>>) -> Self {
        Self { source }
    }

    /// Returns a future yielding either the first error, or the full set of listings.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsResult#all")]
    pub fn all(&self) -> KafkaFuture<Vec<GroupListing>> {
        self.source.then_apply(|results| {
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

    /// Returns a future which yields just the valid listings.
    ///
    /// This future never fails with an error, no matter what happens. Errors are completely
    /// ignored. If nothing can be fetched, an empty collection is yielded. If there is an error,
    /// but some results can be returned, this future will yield those partial results. When
    /// using this future, it is a good idea to also check the errors future so that errors can
    /// be displayed and handled.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsResult#valid")]
    pub fn valid(&self) -> KafkaFuture<Vec<GroupListing>> {
        self.source.then_apply(|results: Vec<Result<GroupListing, Error>>| {
            Ok(results.into_iter().filter_map(Result::ok).collect())
        })
    }

    /// Returns a future which yields just the errors which occurred.
    ///
    /// If this future yields a non-empty collection, it is very likely that elements are missing
    /// from the [`valid`](Self::valid) set.
    ///
    /// This future itself never fails with an error. In the event of an error, this future will
    /// successfully yield a collection containing at least one error.
    #[doc(alias = "org.apache.kafka.clients.admin.ListGroupsResult#errors")]
    pub fn errors(&self) -> KafkaFuture<Vec<Error>> {
        self.source.then_apply(|results: Vec<Result<GroupListing, Error>>| {
            Ok(results.into_iter().filter_map(Result::err).collect())
        })
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
        let source = KafkaFuture::completed_future(Ok(vec![
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
        let source = KafkaFuture::completed_future(Ok(vec![Ok(listing("g1"))]));
        let result = ListGroupsResult::new(source);
        assert_eq!(result.all().get().await.unwrap(), vec![listing("g1")]);
    }
}
