/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use serde::{Deserialize, Serialize};

/// Message specification types for Kafka messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageSpecType {
    /// Kafka request RPCs
    Request,

    /// Kafka response RPCs
    Response,

    /// Kafka RPC headers
    Header,

    /// KIP-631 controller records
    Metadata,

    /// Other message spec types
    Data,

    /// Coordinator key types
    #[serde(rename = "coordinator-key")]
    CoordinatorKey,

    /// Coordinator value types
    #[serde(rename = "coordinator-value")]
    CoordinatorValue,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_deserialize_request() {
        let json = r#""request""#;
        let msg_type: MessageSpecType = serde_json::from_str(json).unwrap();
        assert_eq!(msg_type, MessageSpecType::Request);
    }

    #[test]
    fn test_deserialize_coordinator_key() {
        let json = r#""coordinator-key""#;
        let msg_type: MessageSpecType = serde_json::from_str(json).unwrap();
        assert_eq!(msg_type, MessageSpecType::CoordinatorKey);
    }

    #[test]
    fn test_serialize_response() {
        let msg_type = MessageSpecType::Response;
        let json = serde_json::to_string(&msg_type).unwrap();
        assert_eq!(json, r#""response""#);
    }
}
