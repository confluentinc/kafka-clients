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

//! AlterClientQuotas response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.AlterClientQuotasResponse`.

use std::collections::HashMap;
use std::io;

use crate::AlterClientQuotasResponseData;
use crate::alter_client_quotas_response_data::{EntityData, EntryData};
use crate::common::Error;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::quota::ClientQuotaEntity;

use super::AbstractResponse;

/// An AlterClientQuotas response.
///
/// Corresponds to `org.apache.kafka.common.requests.AlterClientQuotasResponse`.
#[derive(Debug, Clone)]
pub struct AlterClientQuotasResponse {
    data: AlterClientQuotasResponseData,
    #[allow(dead_code)]
    version: i16,
}

impl AlterClientQuotasResponse {
    /// Creates a new `AlterClientQuotasResponse` from data and version.
    pub fn new(data: AlterClientQuotasResponseData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_CLIENT_QUOTAS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AlterClientQuotasResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterClientQuotasResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Decodes the per-entity alteration results.
    ///
    /// Mirrors the iteration in `AlterClientQuotasResponse.complete`: each entry
    /// is mapped to its [`ClientQuotaEntity`] plus either `Ok(())` on success or
    /// an `Err(Error)` carrying the entry's error code/message. The caller
    /// completes the corresponding per-entity future (and, like Java, is
    /// responsible for rejecting an entity the request did not include).
    pub fn results(&self) -> Vec<(ClientQuotaEntity, Result<(), Error>)> {
        let mut results = Vec::with_capacity(self.data.entries.len());
        for entry_data in &self.data.entries {
            let mut entity_entries = HashMap::with_capacity(entry_data.entity.len());
            for entity_data in &entry_data.entity {
                // A wire-null name (`None`) is the built-in default entity;
                // preserve it verbatim rather than coercing to `""`.
                entity_entries.insert(entity_data.entity_type.clone(), entity_data.entity_name.clone());
            }
            let entity = ClientQuotaEntity::new(entity_entries);
            let error = Errors::for_code(entry_data.error_code);
            let outcome = if error == Errors::None {
                Ok(())
            } else {
                // Java: `error.exception(entryData.errorMessage())`
                // (`AlterClientQuotasResponse.java:60`). `Errors.exception(String)`
                // falls back to the code's default text only when the message is
                // **null** (`Errors.java:462-469`); a non-null empty string is passed
                // through. Treating `Some("")` as absent would substitute the default
                // where the broker deliberately sent none.
                Err(match &entry_data.error_message {
                    Some(m) => Error::with_message(error, m.clone()),
                    None => Error::new(error),
                })
            };
            results.push((entity, outcome));
        }
        results
    }

    /// Returns the error counts aggregated across all entries.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for entry in &self.data.entries {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(entry.error_code));
        }
        counts
    }

    /// Parses an `AlterClientQuotasResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterClientQuotasResponseData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Whether the client should throttle on this response.
    ///
    /// Mirrors `AlterClientQuotasResponse` which does not override
    /// `AbstractResponse.shouldClientThrottle` (always `false`).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Builds a response from a per-entity error result.
    ///
    /// Mirrors `AlterClientQuotasResponse.fromQuotaEntities`. Java takes a
    /// `Map<ClientQuotaEntity, ApiError>`; the Rust codebase has no `ApiError`
    /// type, so each result is decomposed into its `(Errors, message)` pair,
    /// matching the `api_error(code, message)` convention used elsewhere in the
    /// admin client.
    pub fn from_quota_entities(result: &[(ClientQuotaEntity, Errors, Option<String>)], throttle_time_ms: i32) -> Self {
        let mut entries = Vec::with_capacity(result.len());
        for (entity, error, message) in result {
            let mut entity_data = Vec::with_capacity(entity.entries().len());
            for (entity_type, entity_name) in entity.entries() {
                let mut ed = EntityData::new();
                // `None` (default entity) -> wire-null name; `Some(name)` -> the
                // (possibly empty) non-null string.
                ed.set_entity_type(entity_type.clone()).set_entity_name(entity_name.clone());
                entity_data.push(ed);
            }
            let mut entry = EntryData::new();
            entry
                .set_error_code(error.code())
                .set_error_message(message.clone())
                .set_entity(entity_data);
            entries.push(entry);
        }
        let mut data = AlterClientQuotasResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms).set_entries(entries);
        Self::new(data, ApiKeys::ALTER_CLIENT_QUOTAS.latest_version())
    }
}

impl std::fmt::Display for AlterClientQuotasResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlterClientQuotasResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entity(name: &str) -> ClientQuotaEntity {
        ClientQuotaEntity::new(HashMap::from([(ClientQuotaEntity::USER.to_string(), Some(name.to_string()))]))
    }

    /// The built-in default entity: a `None` (wire-null) name.
    fn default_entity() -> ClientQuotaEntity {
        ClientQuotaEntity::new(HashMap::from([(ClientQuotaEntity::USER.to_string(), None)]))
    }

    #[test]
    fn results_decode_success_and_error() {
        let response = AlterClientQuotasResponse::from_quota_entities(
            &[
                (entity("user-1"), Errors::None, None),
                (
                    entity("user-0"),
                    Errors::ClusterAuthorizationFailed,
                    Some("Authorization failed".to_string()),
                ),
            ],
            0,
        );
        let results = response.results();
        assert_eq!(results.len(), 2);
        let good = results.iter().find(|(e, _)| *e == entity("user-1")).unwrap();
        assert!(good.1.is_ok());
        let bad = results.iter().find(|(e, _)| *e == entity("user-0")).unwrap();
        assert_eq!(bad.1.as_ref().unwrap_err().error(), Errors::ClusterAuthorizationFailed);
    }

    #[test]
    fn results_preserve_default_entity_null_name() {
        // A default entity (`None` name) and an empty-named entity (`Some("")`)
        // must decode back distinctly: null stays `None`, "" stays `Some("")`.
        // The pre-fix `unwrap_or_default()` collapsed both to `""`.
        let response = AlterClientQuotasResponse::from_quota_entities(
            &[(default_entity(), Errors::None, None), (entity(""), Errors::None, None)],
            0,
        );
        let results = response.results();
        assert_eq!(results.len(), 2);
        let default_res = results.iter().find(|(e, _)| *e == default_entity()).unwrap();
        assert_eq!(default_res.0.entries().get(ClientQuotaEntity::USER), Some(&None));
        assert!(default_res.1.is_ok());
        let empty_res = results.iter().find(|(e, _)| *e == entity("")).unwrap();
        assert_eq!(empty_res.0.entries().get(ClientQuotaEntity::USER), Some(&Some(String::new())));
        assert_ne!(default_res.0, empty_res.0);
    }

    #[test]
    fn error_counts_aggregate_across_entries() {
        let response = AlterClientQuotasResponse::from_quota_entities(
            &[
                (entity("a"), Errors::ClusterAuthorizationFailed, None),
                (entity("b"), Errors::ClusterAuthorizationFailed, None),
                (entity("c"), Errors::InvalidRequest, None),
            ],
            0,
        );
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::ClusterAuthorizationFailed), Some(&2));
        assert_eq!(counts.get(&Errors::InvalidRequest), Some(&1));
    }

    #[test]
    fn does_not_throttle() {
        let response = AlterClientQuotasResponse::new(AlterClientQuotasResponseData::new(), 1);
        assert!(!response.should_client_throttle(1));
    }
}
