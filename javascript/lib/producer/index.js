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

'use strict';

// The producer namespace: the public classes of Java's
// org.apache.kafka.clients.producer this client has.

/** @typedef {import('./callback').Callback} Callback */
/** @typedef {import('./kafka_producer').ProducerConfigs} ProducerConfigs */
/** @typedef {import('./kafka_producer').CloseOptions} CloseOptions */
/** @typedef {import('./producer_record').Data} Data */
/** @typedef {import('./producer_record').Header} Header */
/** @typedef {import('./producer_record').ProducerRecordOptions} ProducerRecordOptions */

const { KafkaProducer } = require('./kafka_producer');
const { ProducerRecord } = require('./producer_record');
const { RecordMetadata } = require('./record_metadata');

module.exports = { KafkaProducer, ProducerRecord, RecordMetadata };
