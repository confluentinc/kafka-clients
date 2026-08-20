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

//! The result of `Admin::delete_acls`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DeleteAclsResult`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::common::acl::{AclBinding, AclBindingFilter};
use crate::common::kafka_future::KafkaFutureOps;
use crate::common::{Error, KafkaFuture};

/// A class containing either the deleted ACL binding or an exception if the
/// delete failed.
///
/// Corresponds to `DeleteAclsResult.FilterResult`.
#[derive(Clone, Debug)]
pub struct FilterResult {
    binding: Option<AclBinding>,
    exception: Option<Error>,
}

impl FilterResult {
    /// Creates a filter result carrying the deleted binding and/or an error.
    pub fn new(binding: Option<AclBinding>, exception: Option<Error>) -> Self {
        Self { binding, exception }
    }

    /// Return the deleted ACL binding, or `None` if there was an error.
    pub fn binding(&self) -> Option<&AclBinding> {
        self.binding.as_ref()
    }

    /// Return an exception if the ACL delete was not successful, or `None` if it
    /// was.
    pub fn exception(&self) -> Option<&Error> {
        self.exception.as_ref()
    }
}

/// A class containing the results of the delete ACLs operation.
///
/// Corresponds to `DeleteAclsResult.FilterResults`.
#[derive(Clone, Debug)]
pub struct FilterResults {
    values: Vec<FilterResult>,
}

impl FilterResults {
    /// Creates a `FilterResults` from a list of per-ACL results.
    pub fn new(values: Vec<FilterResult>) -> Self {
        Self { values }
    }

    /// Return a list of delete ACLs results for a given filter.
    pub fn values(&self) -> &[FilterResult] {
        &self.values
    }
}

/// The result of the `Admin::delete_acls` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.DeleteAclsResult`.
#[derive(Clone, Debug)]
pub struct DeleteAclsResult {
    futures: HashMap<AclBindingFilter, KafkaFuture<FilterResults>>,
}

impl DeleteAclsResult {
    /// Creates a new result from the per-filter futures.
    pub fn new(futures: HashMap<AclBindingFilter, KafkaFuture<FilterResults>>) -> Self {
        Self { futures }
    }

    /// Return a map from ACL filters to futures which can be used to check the
    /// status of the deletions by each filter.
    pub fn values(&self) -> &HashMap<AclBindingFilter, KafkaFuture<FilterResults>> {
        &self.futures
    }

    /// Return a future which succeeds only if all the ACL deletions succeed, and
    /// which contains all the deleted ACLs. Note that if the filters don't match
    /// any ACLs, this is not considered an error.
    ///
    /// Mirrors `DeleteAclsResult.all` /
    /// `DeleteAclsResult.getAclBindings`.
    pub fn all(&self) -> KafkaFuture<Vec<AclBinding>> {
        KafkaFuture::new(Arc::new(AclBindingsFuture {
            futures: self.futures.values().cloned().collect(),
        }))
    }
}

/// Internal aggregate future backing [`DeleteAclsResult::all`].
///
/// Awaits every per-filter future and flattens the successful [`FilterResult`]s
/// into deleted [`AclBinding`]s, surfacing the first error encountered (a failed
/// filter future or a per-ACL deletion error). Mirrors Java's
/// `allOf(...).thenApply(getAclBindings)`.
struct AclBindingsFuture {
    futures: Vec<KafkaFuture<FilterResults>>,
}

impl AclBindingsFuture {
    async fn collect(&self) -> Result<Vec<AclBinding>, Error> {
        let mut acls = Vec::new();
        for value in &self.futures {
            let results = value.get().await?;
            for result in results.values() {
                if let Some(exception) = result.exception() {
                    return Err(exception.clone());
                }
                if let Some(binding) = result.binding() {
                    acls.push(binding.clone());
                }
            }
        }
        Ok(acls)
    }
}

impl KafkaFutureOps<Vec<AclBinding>> for AclBindingsFuture {
    fn get(&self) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<AclBinding>, Error>> + Send + '_>> {
        Box::pin(self.collect())
    }

    fn get_timeout(
        &self,
        _timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<AclBinding>, Error>> + Send + '_>> {
        // The per-filter futures resolve together via the background task; the
        // aggregate simply awaits them (mirroring Java's `allOf` + `get`).
        Box::pin(self.collect())
    }

    fn is_done(&self) -> bool {
        self.futures.iter().all(KafkaFuture::is_done)
    }
}
