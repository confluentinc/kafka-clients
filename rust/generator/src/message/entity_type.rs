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

use super::field_type::FieldType;
use serde::{Deserialize, Serialize};

/// Represents the entity type of a field (e.g., transactional ID, topic name).
///
/// Translated from org.apache.kafka.message.EntityType
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum EntityType {
    #[serde(rename = "unknown")]
    #[default]
    Unknown,
    #[serde(rename = "transactionalId")]
    TransactionalId,
    #[serde(rename = "producerId")]
    ProducerId,
    #[serde(rename = "groupId")]
    GroupId,
    #[serde(rename = "topicName")]
    TopicName,
    #[serde(rename = "brokerId")]
    BrokerId,
}

impl EntityType {
    /// Returns the base field type for this entity type.
    pub fn base_type(&self) -> Option<FieldType> {
        match self {
            EntityType::Unknown => None,
            EntityType::TransactionalId => Some(FieldType::String),
            EntityType::ProducerId => Some(FieldType::Int64),
            EntityType::GroupId => Some(FieldType::String),
            EntityType::TopicName => Some(FieldType::String),
            EntityType::BrokerId => Some(FieldType::Int32),
        }
    }

    /// Verifies that the given field type matches the expected base type for this entity.
    pub fn verify_type_matches(&self, field_name: &str, field_type: &FieldType) -> Result<(), String> {
        if *self == EntityType::Unknown {
            return Ok(());
        }

        let type_to_check = if let Some(element_type) = field_type.element_type() {
            element_type
        } else {
            field_type
        };

        if let Some(base_type) = self.base_type()
            && type_to_check != &base_type
        {
            return Err(format!(
                "Field {} has entity type {:?}, but field type {}, which does not match.",
                field_name, self, field_type
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_types() {
        assert_eq!(EntityType::Unknown.base_type(), None);
        assert_eq!(EntityType::TransactionalId.base_type(), Some(FieldType::String));
        assert_eq!(EntityType::ProducerId.base_type(), Some(FieldType::Int64));
        assert_eq!(EntityType::GroupId.base_type(), Some(FieldType::String));
        assert_eq!(EntityType::TopicName.base_type(), Some(FieldType::String));
        assert_eq!(EntityType::BrokerId.base_type(), Some(FieldType::Int32));
    }

    #[test]
    fn test_unknown_entity_type() {
        // Unknown entity type should accept any field type
        let field_types = vec![
            FieldType::String,
            FieldType::Int8,
            FieldType::Int16,
            FieldType::Int32,
            FieldType::Int64,
            FieldType::Array(Box::new(FieldType::String)),
        ];

        for field_type in field_types {
            assert!(EntityType::Unknown.verify_type_matches("unknown", &field_type).is_ok());
        }
    }

    #[test]
    fn test_verify_type_matches() {
        // Test all entity types with correct field types
        assert!(
            EntityType::TransactionalId
                .verify_type_matches("transactionalIdField", &FieldType::String)
                .is_ok()
        );
        assert!(
            EntityType::TransactionalId
                .verify_type_matches("transactionalIdField", &FieldType::Array(Box::new(FieldType::String)))
                .is_ok()
        );

        assert!(
            EntityType::ProducerId
                .verify_type_matches("producerIdField", &FieldType::Int64)
                .is_ok()
        );
        assert!(
            EntityType::ProducerId
                .verify_type_matches("producerIdField", &FieldType::Array(Box::new(FieldType::Int64)))
                .is_ok()
        );

        assert!(
            EntityType::GroupId
                .verify_type_matches("groupIdField", &FieldType::String)
                .is_ok()
        );
        assert!(
            EntityType::GroupId
                .verify_type_matches("groupIdField", &FieldType::Array(Box::new(FieldType::String)))
                .is_ok()
        );

        assert!(
            EntityType::TopicName
                .verify_type_matches("topicNameField", &FieldType::String)
                .is_ok()
        );
        assert!(
            EntityType::TopicName
                .verify_type_matches("topicNameField", &FieldType::Array(Box::new(FieldType::String)))
                .is_ok()
        );

        assert!(
            EntityType::BrokerId
                .verify_type_matches("brokerIdField", &FieldType::Int32)
                .is_ok()
        );
        assert!(
            EntityType::BrokerId
                .verify_type_matches("brokerIdField", &FieldType::Array(Box::new(FieldType::Int32)))
                .is_ok()
        );
    }

    #[test]
    fn test_verify_type_mismatches() {
        // Test that incorrect type combinations fail
        assert!(
            EntityType::TransactionalId
                .verify_type_matches("transactionalIdField", &FieldType::Int32)
                .is_err()
        );
        assert!(
            EntityType::ProducerId
                .verify_type_matches("producerIdField", &FieldType::String)
                .is_err()
        );
        assert!(
            EntityType::GroupId
                .verify_type_matches("groupIdField", &FieldType::Int8)
                .is_err()
        );
        assert!(
            EntityType::TopicName
                .verify_type_matches("topicNameField", &FieldType::Array(Box::new(FieldType::Int64)))
                .is_err()
        );
        assert!(
            EntityType::BrokerId
                .verify_type_matches("brokerIdField", &FieldType::Int64)
                .is_err()
        );
    }

    #[test]
    fn test_verify_array_type() {
        let array_type = FieldType::Array(Box::new(FieldType::String));
        assert!(EntityType::TopicName.verify_type_matches("topics", &array_type).is_ok());
    }
}
