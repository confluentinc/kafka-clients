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

// Public exports of the Node.js client: the producer slice of Java's API.

const native = require('./_native');
const { KafkaError } = require('./common/kafka_error');
const { KafkaFuture } = require('./common/kafka_future');
const { KafkaProducer } = require('./producer/kafka_producer');
const { ProducerRecord } = require('./producer/producer_record');
const { RecordMetadata } = require('./producer/record_metadata');

// The native layer builds its errors and metadata through these.
native.setFactories(
  (fields) => KafkaError._from(fields),
  (fields) => new RecordMetadata(fields),
);

module.exports = {
  KafkaError,
  KafkaFuture,
  KafkaProducer,
  ProducerRecord,
  RecordMetadata,
};
