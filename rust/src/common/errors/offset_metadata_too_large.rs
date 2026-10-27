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

//! Translated from `org.apache.kafka.common.errors.OffsetMetadataTooLarge`.

use crate::common::error::kafka_error_type;
use crate::common::protocol::Errors;

kafka_error_type! {
    /// The client has tried to save its offset with associated metadata larger than the maximum
    /// size allowed by the server.
    ///
    /// # Translation notes
    ///
    /// Error code `Errors::OffsetMetadataTooLarge`. Java `extends` chain: `OffsetMetadataTooLarge`
    /// -> `ApiException` -> `KafkaException`.
    #[doc(alias = "org.apache.kafka.common.errors.OffsetMetadataTooLarge")]
    OffsetMetadataTooLarge,
    code: Errors::OffsetMetadataTooLarge,
    extends: [
        is_kafka_error,
        is_api_error,
        is_offset_metadata_too_large_error,
    ],
}
