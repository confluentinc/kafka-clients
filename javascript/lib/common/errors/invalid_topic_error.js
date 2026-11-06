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
// the Java source of `org.apache.kafka.common.errors.InvalidTopicException`;
// `cargo xtask check-generated` checks it is up to date.

'use strict';

const _throwable = require('../../_throwable');
const { InvalidConfigurationError } = require('./invalid_configuration_error');

/**
 * The client has attempted to perform an operation on an invalid topic. For
 * example the topic name is too long, contains invalid characters etc. This
 * exception is not retriable because the operation won't suddenly become valid.
 *
 * Java: `org.apache.kafka.common.errors.InvalidTopicException`.
 */
class InvalidTopicError extends InvalidConfigurationError {}

_throwable.setFfiId(InvalidTopicError, 17); // kafka_common_Error_INVALID_TOPIC_ERROR

module.exports = { InvalidTopicError };
