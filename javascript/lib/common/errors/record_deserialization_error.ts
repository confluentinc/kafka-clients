// Copyright 2026 Confluent Inc.
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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// the Java source of `org.apache.kafka.common.errors.RecordDeserializationException`;
// `cargo xtask check-generated` checks it is up to date.

import { setFfiId } from '../../_throwable';
import { SerializationError } from './serialization_error';

/**
 * This exception is raised for any error that occurs while deserializing
 * records received by the consumer using the configured
 * `org.apache.kafka.common.serialization.Deserializer`.
 *
 * Java: `org.apache.kafka.common.errors.RecordDeserializationException`.
 */
export class RecordDeserializationError extends SerializationError {}

setFfiId(RecordDeserializationError, -27); // kafka_common_Error_RECORD_DESERIALIZATION
