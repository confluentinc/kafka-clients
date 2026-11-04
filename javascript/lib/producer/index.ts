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

// The producer namespace: the public classes of Java's
// org.apache.kafka.clients.producer this client has.

export { KafkaProducer } from './kafka_producer';
export type { CloseOptions, ProducerConfigs } from './kafka_producer';
export type { Callback } from './callback';
export { ProducerRecord } from './producer_record';
export type { Data, Header, ProducerRecordOptions } from './producer_record';
export { RecordMetadata } from './record_metadata';
// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { BufferExhaustedError } from './buffer_exhausted_error';
export { ProducerConfig } from './producer_config';
// END GENERATED EXPORTS
