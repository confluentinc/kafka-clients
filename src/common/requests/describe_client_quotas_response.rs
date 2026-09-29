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

//! DescribeClientQuotas response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeClientQuotasResponse`.

use std::collections::HashMap;
use std::io;

use crate::DescribeClientQuotasResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::quota::ClientQuotaEntity;
use crate::describe_client_quotas_response_data::{EntityData, EntryData, ValueData};

use super::AbstractResponse;

/// A DescribeClientQuotas response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeClientQuotasResponse`.
#[derive(Debug, Clone)]
pub struct DescribeClientQuotasResponse {
    data: DescribeClientQuotasResponseData,
    #[allow(dead_code)]
    version: i16,
}

impl DescribeClientQuotasResponse {
    /// Creates a new `DescribeClientQuotasResponse` from data and version.
    pub fn new(data: DescribeClientQuotasResponseData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_CLIENT_QUOTAS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeClientQuotasResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeClientQuotasResponseData {
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

    /// Returns the response error code.
    pub fn error_code(&self) -> i16 {
        self.data.error_code
    }

    /// Returns the response error message, if any.
    pub fn error_message(&self) -> Option<&str> {
        self.data.error_message.as_deref()
    }

    /// Builds the entity-to-quota-values map from the response, mirroring the
    /// success branch of `DescribeClientQuotasResponse.complete`.
    ///
    /// The caller is responsible for checking [`Self::error_code`] first; on an
    /// error response this returns whatever entries the broker sent (empty for
    /// a well-formed error response, whose `entries` is null).
    pub fn entities(&self) -> HashMap<ClientQuotaEntity, HashMap<String, f64>> {
        let mut result = HashMap::new();
        if let Some(entries) = &self.data.entries {
            for entry in entries {
                let mut entity = HashMap::with_capacity(entry.entity.len());
                for entity_data in &entry.entity {
                    // A wire-null name (`None`) is the built-in default entity;
                    // preserve it verbatim rather than coercing to `""`.
                    entity.insert(entity_data.entity_type.clone(), entity_data.entity_name.clone());
                }
                let mut values = HashMap::with_capacity(entry.values.len());
                for value_data in &entry.values {
                    values.insert(value_data.key.clone(), value_data.value);
                }
                result.insert(ClientQuotaEntity::new(entity), values);
            }
        }
        result
    }

    /// Returns the error counts aggregated for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        counts
    }

    /// Parses a `DescribeClientQuotasResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeClientQuotasResponseData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Whether the client should throttle on this response.
    ///
    /// Mirrors `DescribeClientQuotasResponse` which does not override
    /// `AbstractResponse.shouldClientThrottle` (always `false`).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Builds a success response from a map of entities to quota values.
    ///
    /// Mirrors `DescribeClientQuotasResponse.fromQuotaEntities`.
    pub fn from_quota_entities(
        entities: &HashMap<ClientQuotaEntity, HashMap<String, f64>>,
        throttle_time_ms: i32,
    ) -> Self {
        let mut entries = Vec::with_capacity(entities.len());
        for (quota_entity, quota_values) in entities {
            let mut entity_data = Vec::with_capacity(quota_entity.entries().len());
            for (entity_type, entity_name) in quota_entity.entries() {
                let mut ed = EntityData::new();
                // `None` (default entity) -> wire-null name; `Some(name)` -> the
                // (possibly empty) non-null string.
                ed.set_entity_type(entity_type.clone()).set_entity_name(entity_name.clone());
                entity_data.push(ed);
            }
            let mut value_data = Vec::with_capacity(quota_values.len());
            for (key, value) in quota_values {
                let mut vd = ValueData::new();
                vd.set_key(key.clone()).set_value(*value);
                value_data.push(vd);
            }
            let mut entry = EntryData::new();
            entry.set_entity(entity_data).set_values(value_data);
            entries.push(entry);
        }
        let mut data = DescribeClientQuotasResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms)
            .set_error_code(0)
            .set_error_message(None)
            .set_entries(Some(entries));
        Self::new(data, ApiKeys::DESCRIBE_CLIENT_QUOTAS.latest_version())
    }
}

impl std::fmt::Display for DescribeClientQuotasResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeClientQuotasResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entity(pairs: &[(&str, &str)]) -> ClientQuotaEntity {
        ClientQuotaEntity::new(pairs.iter().map(|(k, v)| ((*k).to_string(), Some((*v).to_string()))).collect())
    }

    /// The built-in default entity: a `None` (wire-null) name.
    fn default_entity() -> ClientQuotaEntity {
        ClientQuotaEntity::new(HashMap::from([(ClientQuotaEntity::USER.to_string(), None)]))
    }

    #[test]
    fn from_quota_entities_round_trips_to_entities() {
        let e1 = entity(&[
            (ClientQuotaEntity::USER, "user-1"),
            (ClientQuotaEntity::CLIENT_ID, "value"),
        ]);
        let e2 = entity(&[
            (ClientQuotaEntity::USER, "user-2"),
            (ClientQuotaEntity::CLIENT_ID, "value"),
        ]);
        let mut data = HashMap::new();
        data.insert(e1.clone(), HashMap::from([("consumer_byte_rate".to_string(), 10000.0)]));
        data.insert(e2.clone(), HashMap::from([("producer_byte_rate".to_string(), 20000.0)]));

        let response = DescribeClientQuotasResponse::from_quota_entities(&data, 0);
        let entities = response.entities();
        assert_eq!(entities.len(), 2);
        assert_eq!(entities[&e1].get("consumer_byte_rate"), Some(&10000.0));
        assert_eq!(entities[&e2].get("producer_byte_rate"), Some(&20000.0));
    }

    #[test]
    fn entities_preserve_default_entity_null_name() {
        // A default entity (`None` name) and an empty-named entity (`Some("")`)
        // must decode back distinctly through describe: null stays `None`, ""
        // stays `Some("")`. The pre-fix `unwrap_or_default()` collapsed both to
        // `""`, merging them into one map key.
        let mut data = HashMap::new();
        data.insert(default_entity(), HashMap::from([("consumer_byte_rate".to_string(), 10000.0)]));
        data.insert(
            entity(&[(ClientQuotaEntity::USER, "")]),
            HashMap::from([("producer_byte_rate".to_string(), 20000.0)]),
        );

        let response = DescribeClientQuotasResponse::from_quota_entities(&data, 0);
        let entities = response.entities();
        assert_eq!(entities.len(), 2);
        assert_eq!(entities[&default_entity()].get("consumer_byte_rate"), Some(&10000.0));
        assert_eq!(
            entities[&entity(&[(ClientQuotaEntity::USER, "")])].get("producer_byte_rate"),
            Some(&20000.0)
        );
        // The default entity's name decodes to `None`, distinct from `Some("")`.
        assert!(entities.contains_key(&default_entity()));
        assert_ne!(default_entity(), entity(&[(ClientQuotaEntity::USER, "")]));
    }

    #[test]
    fn null_entries_yield_empty_map() {
        let mut data = DescribeClientQuotasResponseData::new();
        data.set_error_code(Errors::InvalidRequest.code());
        data.set_entries(None);
        let response = DescribeClientQuotasResponse::new(data, 1);
        assert!(response.entities().is_empty());
        assert_eq!(response.error_code(), Errors::InvalidRequest.code());
    }

    #[test]
    fn does_not_throttle() {
        let response = DescribeClientQuotasResponse::new(DescribeClientQuotasResponseData::new(), 1);
        assert!(!response.should_client_throttle(0));
        assert!(!response.should_client_throttle(1));
    }
}
