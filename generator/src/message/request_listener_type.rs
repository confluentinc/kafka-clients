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

/// Request listener types for Kafka requests
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestListenerType {
    /// Broker listener
    Broker,

    /// Controller listener
    Controller,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_deserialize_broker() {
        let json = r#""broker""#;
        let listener: RequestListenerType = serde_json::from_str(json).unwrap();
        assert_eq!(listener, RequestListenerType::Broker);
    }

    #[test]
    fn test_deserialize_controller() {
        let json = r#""controller""#;
        let listener: RequestListenerType = serde_json::from_str(json).unwrap();
        assert_eq!(listener, RequestListenerType::Controller);
    }
}
