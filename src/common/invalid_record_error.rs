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

//! Translated from `org.apache.kafka.common.InvalidRecordException`.
//!
//! Note the package: this class sits in `org.apache.kafka.common`, beside
//! `KafkaException`, not in `common.errors` with the rest of the family.

use crate::common::kafka_error::kafka_error_class;
use crate::common::protocol::Errors;

kafka_error_class! {
    /// This record has failed the validation on broker and hence will be
    /// rejected.
    ///
    /// Corresponds to Java's `InvalidRecordException`, error code [`Errors::InvalidRecord`].
    ///
    /// Java `extends` chain:
    ///    `InvalidRecordException`
    InvalidRecordError,
    code: Errors::InvalidRecord,
    extends: [
        is_kafka_error,
        is_api_error,
        is_invalid_configuration_error,
    ],
}
