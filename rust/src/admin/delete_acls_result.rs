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

use std::collections::HashMap;

use crate::common::acl::{AclBinding, AclBindingFilter};
use crate::common::{Error, KafkaFuture};

/// A class containing either the deleted ACL binding or an exception if the
/// delete failed.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult$FilterResult")]
pub struct FilterResult {
    binding: Option<AclBinding>,
    error: Option<Error>,
}

impl FilterResult {
    /// Creates a filter result carrying the deleted binding and/or an error.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult$FilterResult#FilterResult")]
    pub(crate) fn new(binding: Option<AclBinding>, error: Option<Error>) -> Self {
        Self { binding, error }
    }

    /// Returns the deleted ACL binding, or `None` if there was an error.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult$FilterResult#binding")]
    pub fn binding(&self) -> Option<&AclBinding> {
        self.binding.as_ref()
    }

    /// Returns an exception if the ACL delete was not successful, or `None` if it
    /// was.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult$FilterResult#exception")]
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }
}

/// A class containing the results of the delete ACLs operation.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult$FilterResults")]
pub struct FilterResults {
    values: Vec<FilterResult>,
}

impl FilterResults {
    /// Creates a `FilterResults` from a list of per-ACL results.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult$FilterResults#FilterResults")]
    pub(crate) fn new(values: Vec<FilterResult>) -> Self {
        Self { values }
    }

    /// Returns a list of delete ACLs results for a given filter.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult$FilterResults#values")]
    pub fn values(&self) -> &[FilterResult] {
        &self.values
    }
}

/// The result of the `Admin::delete_acls` call.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult")]
pub struct DeleteAclsResult {
    futures: HashMap<AclBindingFilter, KafkaFuture<FilterResults>>,
}

impl DeleteAclsResult {
    /// Creates a new result from the per-filter futures.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult#DeleteAclsResult")]
    pub(crate) fn new(futures: HashMap<AclBindingFilter, KafkaFuture<FilterResults>>) -> Self {
        Self { futures }
    }

    /// Returns a map from ACL filters to futures which can be used to check the
    /// status of the deletions by each filter.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult#values")]
    pub fn values(&self) -> &HashMap<AclBindingFilter, KafkaFuture<FilterResults>> {
        &self.futures
    }

    /// Returns a future which succeeds only if all the ACL deletions succeed, and
    /// which contains all the deleted ACLs. Note that if the filters don't match
    /// any ACLs, this is not considered an error.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult#all")]
    pub fn all(&self) -> KafkaFuture<Vec<AclBinding>> {
        let futures: Vec<KafkaFuture<FilterResults>> = self.futures.values().cloned().collect();
        KafkaFuture::all_of(futures.clone()).then_apply(move |()| get_acl_bindings(&futures))
    }
}

/// The deleted ACL bindings of `futures`, all complete, or the first per-ACL
/// deletion error.
#[doc(alias = "org.apache.kafka.clients.admin.DeleteAclsResult#getAclBindings")]
fn get_acl_bindings(futures: &[KafkaFuture<FilterResults>]) -> Result<Vec<AclBinding>, Error> {
    let mut acls = Vec::new();
    for value in futures {
        // `all_of` has completed every future, successfully, so this is the
        // unreachable branch Java reports as an internal error.
        let results = match value.result_if_done() {
            Some(Ok(results)) => results,
            Some(Err(error)) => return Err(Error::kafka_message_source("DeleteAclsResult#all: internal error", error)),
            None => return Err(Error::kafka_message("DeleteAclsResult#all: internal error")),
        };
        for result in results.values() {
            if let Some(error) = result.error() {
                return Err(error.clone());
            }
            if let Some(binding) = result.binding() {
                acls.push(binding.clone());
            }
        }
    }
    Ok(acls)
}
