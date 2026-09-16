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

//! Translated from `org.apache.kafka.common.errors.AuthorizationException`.

use crate::common::kafka_error::kafka_error_class;
use crate::common::protocol::Errors;

kafka_error_class! {
    /// The client is not authorized to perform the operation.
    ///
    /// Corresponds to Java's `AuthorizationException`, which reports error code
    /// [`Errors::InvalidConfig`] by inheritance.
    ///
    /// Java `extends` chain:
    ///    `AuthorizationException` -> `InvalidConfigurationException` ->
    ///   `ApiException` -> `KafkaException`
    ///
    /// `Errors.forException` walks the superclass chain
    /// (`protocol/Errors.java:520-531`), and `InvalidConfigurationException` **is**
    /// in the map — `INVALID_CONFIG(40, ..., InvalidConfigurationException::new)`
    /// (`Errors.java:264`). So Java answers **40** for this class even though the
    /// class itself has no entry of its own. `InvalidConfigurationError` remains
    /// the code's owner: `Errors::error(InvalidConfig)` names that class, not this
    /// one.
    AuthorizationError,
    code: Errors::InvalidConfig,
    extends: [
        is_kafka_error,
        is_api_error,
        is_invalid_configuration_error,
        is_authorization_error,
    ],
}
