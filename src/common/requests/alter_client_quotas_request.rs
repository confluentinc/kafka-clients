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

//! AlterClientQuotas request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.AlterClientQuotasRequest`.

use std::collections::HashMap;
use std::io;

use crate::alter_client_quotas_request_data::{AlterClientQuotasRequestData, EntityData, EntryData, OpData};
use crate::alter_client_quotas_response_data::{
    AlterClientQuotasResponseData, EntityData as ResponseEntityData, EntryData as ResponseEntryData,
};
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::quota::client_quota_alteration::Op;
use crate::common::quota::{ClientQuotaAlteration, ClientQuotaEntity};

use super::{AlterClientQuotasResponse, ConcreteRequest, ConcreteResponse, RequestBuilder};

/// An AlterClientQuotas request.
///
/// Corresponds to `org.apache.kafka.common.requests.AlterClientQuotasRequest`.
#[derive(Debug, Clone)]
pub struct AlterClientQuotasRequest {
    data: AlterClientQuotasRequestData,
    version: i16,
}

impl AlterClientQuotasRequest {
    /// Creates a new `AlterClientQuotasRequest` from data and version.
    pub fn new(data: AlterClientQuotasRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AlterClientQuotasRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterClientQuotasRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_CLIENT_QUOTAS
    }

    /// Reconstructs the list of [`ClientQuotaAlteration`]s from the wire data.
    ///
    /// Mirrors `AlterClientQuotasRequest.entries()`. A `remove` op maps back to
    /// an [`Op`] with a `None` value (quota removal).
    pub fn entries(&self) -> Vec<ClientQuotaAlteration> {
        let mut entries = Vec::with_capacity(self.data.entries.len());
        for entry_data in &self.data.entries {
            let mut entity = HashMap::with_capacity(entry_data.entity.len());
            for entity_data in &entry_data.entity {
                // A wire-null name (`None`) is the built-in default entity;
                // preserve it verbatim rather than coercing to `""`.
                entity.insert(entity_data.entity_type.clone(), entity_data.entity_name.clone());
            }
            let mut ops = Vec::with_capacity(entry_data.ops.len());
            for op_data in &entry_data.ops {
                let value = if op_data.remove { None } else { Some(op_data.value) };
                ops.push(Op::new(op_data.key.clone(), value));
            }
            entries.push(ClientQuotaAlteration::new(ClientQuotaEntity::new(entity), ops));
        }
        entries
    }

    /// Returns whether this is a validate-only request.
    ///
    /// Mirrors `AlterClientQuotasRequest.validateOnly()`.
    pub fn validate_only(&self) -> bool {
        self.data.validate_only
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `AlterClientQuotasRequest.getErrorResponse`: every requested
    /// entity is echoed back with the given error code/message.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response_entries = Vec::with_capacity(self.data.entries.len());
        for entry_data in &self.data.entries {
            let mut response_entities = Vec::with_capacity(entry_data.entity.len());
            for entity_data in &entry_data.entity {
                let mut re = ResponseEntityData::new();
                re.set_entity_type(entity_data.entity_type.clone())
                    .set_entity_name(entity_data.entity_name.clone());
                response_entities.push(re);
            }
            let mut re = ResponseEntryData::new();
            re.set_entity(response_entities)
                .set_error_code(error.code())
                .set_error_message(Some(error.message().to_string()));
            response_entries.push(re);
        }
        let mut response_data = AlterClientQuotasResponseData::new();
        response_data
            .set_throttle_time_ms(throttle_time_ms)
            .set_entries(response_entries);
        ConcreteResponse::AlterClientQuotas(AlterClientQuotasResponse::new(response_data, self.version))
    }

    /// Parses an `AlterClientQuotasRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterClientQuotasRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for AlterClientQuotasRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlterClientQuotasRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`AlterClientQuotasRequest`].
///
/// Corresponds to `AlterClientQuotasRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct AlterClientQuotasRequestBuilder {
    data: AlterClientQuotasRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl AlterClientQuotasRequestBuilder {
    /// Creates a builder from a collection of alterations, mirroring
    /// `AlterClientQuotasRequest.Builder(Collection, boolean)`.
    ///
    /// An [`Op`] with a `None` value is encoded as `remove=true` with a
    /// placeholder `value=0.0` (Java's `op.value() == null` handling).
    pub fn new(entries: &[ClientQuotaAlteration], validate_only: bool) -> Self {
        let mut entry_data = Vec::with_capacity(entries.len());
        for entry in entries {
            let mut entity_data = Vec::with_capacity(entry.entity().entries().len());
            for (entity_type, entity_name) in entry.entity().entries() {
                let mut ed = EntityData::new();
                // `None` (the default entity) maps to a wire-null name; a
                // concrete `Some(name)` maps to the (possibly empty) non-null
                // string. Mirrors `AlterClientQuotasRequest.Builder` sending the
                // raw, possibly-null value via `setEntityName(...)`.
                ed.set_entity_type(entity_type.clone()).set_entity_name(entity_name.clone());
                entity_data.push(ed);
            }
            let mut op_data = Vec::with_capacity(entry.ops().len());
            for op in entry.ops() {
                let mut od = OpData::new();
                od.set_key(op.key().to_string())
                    .set_value(op.value().unwrap_or(0.0))
                    .set_remove(op.value().is_none());
                op_data.push(od);
            }
            let mut ed = EntryData::new();
            ed.set_entity(entity_data).set_ops(op_data);
            entry_data.push(ed);
        }
        let mut data = AlterClientQuotasRequestData::new();
        data.set_entries(entry_data).set_validate_only(validate_only);
        Self {
            data,
            oldest_allowed_version: ApiKeys::ALTER_CLIENT_QUOTAS.oldest_version(),
            latest_allowed_version: ApiKeys::ALTER_CLIENT_QUOTAS.latest_version(),
        }
    }
}

impl RequestBuilder for AlterClientQuotasRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_CLIENT_QUOTAS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::AlterClientQuotas(AlterClientQuotasRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::quota::client_quota_entity::USER;

    fn entity(name: &str) -> ClientQuotaEntity {
        ClientQuotaEntity::new(HashMap::from([(USER.to_string(), Some(name.to_string()))]))
    }

    /// The built-in default entity: a `None` (wire-null) name.
    fn default_entity() -> ClientQuotaEntity {
        ClientQuotaEntity::new(HashMap::from([(USER.to_string(), None)]))
    }

    #[test]
    fn builder_encodes_set_and_remove_ops() {
        let alterations = vec![ClientQuotaAlteration::new(
            entity("user-1"),
            vec![
                Op::new("consumer_byte_rate", Some(10000.0)),
                Op::new("producer_byte_rate", None),
            ],
        )];
        let mut builder = AlterClientQuotasRequestBuilder::new(&alterations, false);
        let ConcreteRequest::AlterClientQuotas(r) = builder.build().unwrap() else {
            panic!("expected AlterClientQuotas request");
        };
        let ops = &r.data().entries[0].ops;
        // Set op: remove=false, value carried.
        assert!(!ops[0].remove);
        assert_eq!(ops[0].value, 10000.0);
        // Remove op: remove=true, value placeholder 0.0.
        assert!(ops[1].remove);
        assert_eq!(ops[1].value, 0.0);
    }

    #[test]
    fn entries_round_trips_remove_as_none() {
        let alterations = vec![ClientQuotaAlteration::new(
            entity("user-1"),
            vec![
                Op::new("consumer_byte_rate", Some(10000.0)),
                Op::new("producer_byte_rate", None),
            ],
        )];
        let mut builder = AlterClientQuotasRequestBuilder::new(&alterations, true);
        let ConcreteRequest::AlterClientQuotas(r) = builder.build().unwrap() else {
            panic!("expected AlterClientQuotas request");
        };
        assert!(r.validate_only());
        let decoded = r.entries();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].ops()[0].value(), Some(10000.0));
        // A removal op decodes back to a `None` value.
        assert_eq!(decoded[0].ops()[1].value(), None);
    }

    #[test]
    fn serialize_parse_round_trip() {
        let alterations = vec![ClientQuotaAlteration::new(
            entity("user-1"),
            vec![Op::new("consumer_byte_rate", Some(10000.0))],
        )];
        let version = ApiKeys::ALTER_CLIENT_QUOTAS.latest_version();
        let mut builder = AlterClientQuotasRequestBuilder::new(&alterations, false);
        let mut request = builder.build().unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = AlterClientQuotasRequest::parse(&mut readable, version).unwrap();
        assert_eq!(parsed.entries(), alterations);
    }

    #[test]
    fn known_wire_vector_remove_flag() {
        // v1 (flexible): one entry, entity {user: "u1"}, one remove op
        // "producer_byte_rate", validate_only=false. Asserts the remove byte
        // (0x01) and placeholder value 0.0 on the wire.
        let alterations = vec![ClientQuotaAlteration::new(
            entity("u1"),
            vec![Op::new("producer_byte_rate", None)],
        )];
        let mut builder = AlterClientQuotasRequestBuilder::new(&alterations, false);
        let mut request = builder.build_version(1).unwrap();
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x02, // entries: compact array length (1 + 1)
            0x02, // entity: compact array length (1 + 1)
            0x05, b'u', b's', b'e', b'r', // entity_type = "user"
            0x03, b'u', b'1', // entity_name = "u1" (compact-nullable string)
            0x00, // entity tagged fields
            0x02, // ops: compact array length (1 + 1)
            0x13, b'p', b'r', b'o', b'd', b'u', b'c', b'e', b'r', b'_', b'b', b'y', b't', b'e', b'_', b'r', b'a', b't',
            b'e', // key = "producer_byte_rate" (len 18 + 1 = 0x13)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value = 0.0 (f64 big-endian)
            0x01, // remove = true
            0x00, // op tagged fields
            0x00, // entry tagged fields
            0x00, // validate_only = false
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }

    #[test]
    fn known_wire_vector_default_entity_null_name() {
        // v1 (flexible): one entry, DEFAULT entity {user: null} (i.e.
        // `--entity-type users --entity-default`), one remove op
        // "producer_byte_rate", validate_only=false. Asserts the entity name is
        // a wire-NULL compact-nullable string (0x00) — distinct from an
        // empty-string name, which would be a NON-null zero-length string
        // (0x01). Hand-computed known vector; has teeth against the old
        // `HashMap<String, String>` shape, which could not represent a null
        // name and always emitted 0x01 (empty non-null string) instead.
        let alterations = vec![ClientQuotaAlteration::new(
            default_entity(),
            vec![Op::new("producer_byte_rate", None)],
        )];
        let mut builder = AlterClientQuotasRequestBuilder::new(&alterations, false);
        let mut request = builder.build_version(1).unwrap();
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x02, // entries: compact array length (1 + 1)
            0x02, // entity: compact array length (1 + 1)
            0x05, b'u', b's', b'e', b'r', // entity_type = "user"
            0x00, // entity_name = null (compact-nullable string, null marker)
            0x00, // entity tagged fields
            0x02, // ops: compact array length (1 + 1)
            0x13, b'p', b'r', b'o', b'd', b'u', b'c', b'e', b'r', b'_', b'b', b'y', b't', b'e', b'_', b'r', b'a', b't',
            b'e', // key = "producer_byte_rate" (len 18 + 1 = 0x13)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value = 0.0 (f64 big-endian)
            0x01, // remove = true
            0x00, // op tagged fields
            0x00, // entry tagged fields
            0x00, // validate_only = false
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());

        // Contrast: an entity literally named "" encodes the name as a NON-null
        // zero-length compact string (0x01), NOT the null marker (0x00). This
        // is the byte position (index 7) that distinguishes default from "".
        let empty_named = vec![ClientQuotaAlteration::new(
            entity(""),
            vec![Op::new("producer_byte_rate", None)],
        )];
        let mut builder = AlterClientQuotasRequestBuilder::new(&empty_named, false);
        let mut request = builder.build_version(1).unwrap();
        let empty_bytes = request.serialize().unwrap().into_buffer();
        assert_eq!(bytes.as_slice()[7], 0x00, "default entity name must be wire-null");
        assert_eq!(
            empty_bytes.as_slice()[7],
            0x01,
            "empty-string name must be non-null zero-length"
        );
    }

    #[test]
    fn default_entity_and_empty_name_survive_round_trip_distinctly() {
        // A `None` (default) entity and a `Some("")` (empty-named) entity must
        // both survive encode -> serialize -> parse -> decode, and stay
        // distinct: `None` stays `None`, `Some("")` stays `Some("")`. The
        // pre-fix code coerced null -> "" on decode, collapsing the two.
        let alterations = vec![
            ClientQuotaAlteration::new(default_entity(), vec![Op::new("consumer_byte_rate", Some(1.0))]),
            ClientQuotaAlteration::new(entity(""), vec![Op::new("consumer_byte_rate", Some(2.0))]),
        ];
        let version = ApiKeys::ALTER_CLIENT_QUOTAS.latest_version();
        let mut builder = AlterClientQuotasRequestBuilder::new(&alterations, false);
        let mut request = builder.build().unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = AlterClientQuotasRequest::parse(&mut readable, version).unwrap();
        let decoded = parsed.entries();
        assert_eq!(decoded.len(), 2);

        let default_decoded = decoded.iter().find(|a| a.entity() == &default_entity()).unwrap();
        assert_eq!(default_decoded.entity().entries().get(USER), Some(&None));
        let empty_decoded = decoded.iter().find(|a| a.entity() == &entity("")).unwrap();
        assert_eq!(empty_decoded.entity().entries().get(USER), Some(&Some(String::new())));

        // The two entities must remain distinct after the round trip.
        assert_ne!(default_decoded.entity(), empty_decoded.entity());
    }
}
