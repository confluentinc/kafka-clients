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

// The common namespace: the public classes of Java's org.apache.kafka.common
// this client has, and its subpackages' namespaces (`common.errors` is
// org.apache.kafka.common.errors, `common.config` org.apache.kafka.common.config).

const { KafkaFuture } = require('./kafka_future');
const config = require('./config');
const security = require('./security');
// BEGIN GENERATED REQUIRES (cargo xtask generate-error-codes; do not edit)
const { InvalidRecordError } = require('./invalid_record_error');
const { KafkaError } = require('./kafka_error');
const errors = require('./errors');
const metrics = require('./metrics');
const network = require('./network');
const protocol = require('./protocol');
const requests = require('./requests');
// END GENERATED REQUIRES

module.exports = {
  KafkaFuture,
  config,
  security,
  // BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
  InvalidRecordError,
  KafkaError,
  errors,
  metrics,
  network,
  protocol,
  requests,
  // END GENERATED EXPORTS
};
