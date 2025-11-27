/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::message::{FieldSpec, MessageSpecType, RequestListenerType, StructSpec, Versions};
use serde::{Deserialize, Deserializer};

/// Top-level message specification for Kafka messages
#[derive(Debug, Clone, PartialEq)]
pub struct MessageSpec {
    struct_spec: StructSpec,
    api_key: Option<i16>,
    msg_type: MessageSpecType,
    common_structs: Vec<StructSpec>,
    flexible_versions: Versions,
    listeners: Vec<RequestListenerType>,
    latest_version_unstable: bool,
}

impl MessageSpec {
    /// Create a new MessageSpec with validation
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        valid_versions: Option<&str>,
        deprecated_versions: Option<&str>,
        fields: Vec<FieldSpec>,
        api_key: Option<i16>,
        msg_type: MessageSpecType,
        common_structs: Vec<StructSpec>,
        flexible_versions_str: Option<&str>,
        listeners: Vec<RequestListenerType>,
        latest_version_unstable: bool,
    ) -> Result<Self, String> {
        let struct_spec = StructSpec::new(name.clone(), valid_versions, deprecated_versions, fields)?;

        // If the struct has no valid versions, configure the spec to be effectively empty
        if struct_spec.versions().empty() {
            return Ok(MessageSpec {
                struct_spec,
                api_key,
                msg_type,
                common_structs,
                flexible_versions: Versions::NONE,
                listeners: vec![],
                latest_version_unstable: false,
            });
        }

        // Validate flexible versions
        let flexible_versions_str = flexible_versions_str.ok_or_else(|| {
            "You must specify a value for flexibleVersions. Please use 0+ for all new messages.".to_string()
        })?;

        let flexible_versions = Versions::parse(Some(flexible_versions_str), Versions::NONE)?;

        if !flexible_versions.empty() && flexible_versions.highest() < i16::MAX {
            return Err(format!(
                "Field {} specifies flexibleVersions {}, which is not open-ended. \
                flexibleVersions must be either none, or an open-ended range (that ends with a plus sign).",
                name, flexible_versions
            ));
        }

        // Validate listeners
        if !listeners.is_empty() && msg_type != MessageSpecType::Request {
            return Err("The `requestScope` property is only valid for messages with type `request`".to_string());
        }

        // Validate latestVersionUnstable
        if latest_version_unstable && msg_type != MessageSpecType::Request {
            return Err(
                "The `latestVersionUnstable` property is only valid for messages with type `request`".to_string(),
            );
        }

        // Validate coordinator-key type
        if msg_type == MessageSpecType::CoordinatorKey {
            if api_key.is_none() {
                return Err(format!(
                    "The ApiKey must be set for messages {} with type `coordinator-key`",
                    name
                ));
            }
            if struct_spec.versions() != Versions::new(0, 0)? {
                return Err(format!(
                    "The Versions must be set to `0` for messages {} with type `coordinator-key`",
                    name
                ));
            }
            if !flexible_versions.empty() {
                return Err(format!(
                    "The FlexibleVersions are not supported for messages {} with type `coordinator-key`",
                    name
                ));
            }
        }

        // Validate coordinator-value type
        if msg_type == MessageSpecType::CoordinatorValue && api_key.is_none() {
            return Err("The ApiKey must be set for messages with type `coordinator-value`".to_string());
        }

        Ok(MessageSpec {
            struct_spec,
            api_key,
            msg_type,
            common_structs,
            flexible_versions,
            listeners,
            latest_version_unstable,
        })
    }

    pub fn struct_spec(&self) -> &StructSpec {
        &self.struct_spec
    }

    pub fn name(&self) -> &str {
        self.struct_spec.name()
    }

    pub fn has_valid_version(&self) -> bool {
        !self.struct_spec.versions().empty()
    }

    pub fn valid_versions(&self) -> Versions {
        self.struct_spec.versions()
    }

    pub fn valid_versions_string(&self) -> String {
        self.struct_spec.versions_string()
    }

    pub fn fields(&self) -> &[FieldSpec] {
        self.struct_spec.fields()
    }

    pub fn api_key(&self) -> Option<i16> {
        self.api_key
    }

    pub fn msg_type(&self) -> MessageSpecType {
        self.msg_type
    }

    pub fn common_structs(&self) -> &[StructSpec] {
        &self.common_structs
    }

    pub fn flexible_versions(&self) -> Versions {
        self.flexible_versions
    }

    pub fn flexible_versions_string(&self) -> String {
        self.flexible_versions.to_string()
    }

    pub fn listeners(&self) -> &[RequestListenerType] {
        &self.listeners
    }

    pub fn latest_version_unstable(&self) -> bool {
        self.latest_version_unstable
    }

    pub fn data_class_name(&self) -> String {
        match self.msg_type {
            MessageSpecType::Header | MessageSpecType::Request | MessageSpecType::Response => {
                // Append the Data suffix to request/response/header classes to avoid
                // collisions with existing objects
                format!("{}Data", self.name())
            },
            _ => self.name().to_string(),
        }
    }
}

// Custom deserialization to handle validation during JSON parsing
impl<'de> Deserialize<'de> for MessageSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MessageSpecHelper {
            name: String,
            #[serde(rename = "validVersions")]
            valid_versions: Option<String>,
            #[serde(rename = "deprecatedVersions")]
            deprecated_versions: Option<String>,
            fields: Option<Vec<FieldSpec>>,
            #[serde(rename = "apiKey")]
            api_key: Option<i16>,
            #[serde(rename = "type")]
            msg_type: MessageSpecType,
            #[serde(rename = "commonStructs")]
            common_structs: Option<Vec<StructSpec>>,
            #[serde(rename = "flexibleVersions")]
            flexible_versions: Option<String>,
            listeners: Option<Vec<RequestListenerType>>,
            #[serde(rename = "latestVersionUnstable", default)]
            latest_version_unstable: bool,
        }

        let helper = MessageSpecHelper::deserialize(deserializer)?;

        MessageSpec::new(
            helper.name,
            helper.valid_versions.as_deref(),
            helper.deprecated_versions.as_deref(),
            helper.fields.unwrap_or_default(),
            helper.api_key,
            helper.msg_type,
            helper.common_structs.unwrap_or_default(),
            helper.flexible_versions.as_deref(),
            helper.listeners.unwrap_or_default(),
            helper.latest_version_unstable,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_message_spec_creation() {
        let spec = MessageSpec::new(
            "TestMessage".to_string(),
            Some("0-5"),
            None,
            vec![],
            Some(1),
            MessageSpecType::Request,
            vec![],
            Some("0+"),
            vec![],
            false,
        )
        .unwrap();

        assert_eq!(spec.name(), "TestMessage");
        assert_eq!(spec.api_key(), Some(1));
        assert_eq!(spec.msg_type(), MessageSpecType::Request);
    }

    #[test]
    fn test_flexible_versions_required() {
        let result = MessageSpec::new(
            "TestMessage".to_string(),
            Some("0-5"),
            None,
            vec![],
            Some(1),
            MessageSpecType::Request,
            vec![],
            None, // Missing flexible_versions
            vec![],
            false,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("flexibleVersions"));
    }

    #[test]
    fn test_flexible_versions_must_be_open_ended() {
        let result = MessageSpec::new(
            "TestMessage".to_string(),
            Some("0-5"),
            None,
            vec![],
            Some(1),
            MessageSpecType::Request,
            vec![],
            Some("0-3"), // Not open-ended
            vec![],
            false,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("open-ended"));
    }

    #[test]
    fn test_listeners_only_for_requests() {
        let result = MessageSpec::new(
            "TestMessage".to_string(),
            Some("0-5"),
            None,
            vec![],
            Some(1),
            MessageSpecType::Response, // Not a request
            vec![],
            Some("0+"),
            vec![RequestListenerType::Broker],
            false,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requestScope"));
    }

    #[test]
    fn test_data_class_name_for_request() {
        let spec = MessageSpec::new(
            "Produce".to_string(),
            Some("0-5"),
            None,
            vec![],
            Some(0),
            MessageSpecType::Request,
            vec![],
            Some("0+"),
            vec![],
            false,
        )
        .unwrap();

        assert_eq!(spec.data_class_name(), "ProduceData");
    }

    #[test]
    fn test_data_class_name_for_metadata() {
        let spec = MessageSpec::new(
            "MetadataRecord".to_string(),
            Some("0-5"),
            None,
            vec![],
            None,
            MessageSpecType::Metadata,
            vec![],
            Some("0+"),
            vec![],
            false,
        )
        .unwrap();

        assert_eq!(spec.data_class_name(), "MetadataRecord");
    }

    #[test]
    fn test_deserialize_from_json() {
        let json = r#"{
            "name": "TestMessage",
            "validVersions": "0-3",
            "type": "request",
            "apiKey": 1,
            "flexibleVersions": "0+",
            "fields": []
        }"#;

        let spec: MessageSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.name(), "TestMessage");
        assert_eq!(spec.api_key(), Some(1));
        assert_eq!(spec.msg_type(), MessageSpecType::Request);
    }
}
