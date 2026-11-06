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

// The package root: the module namespaces, Java's packages
// (`require('@confluentinc/kafka-javascript').producer` is
// `org.apache.kafka.clients.producer`, `.common` is `org.apache.kafka.common`).
// Each is also its own entry point (`require('@confluentinc/kafka-javascript/producer')`).
// The root also holds the Java built-in errors (`java.lang.IllegalStateException`
// is `IllegalStateError`, ...), as the Python client's root package does: Java's
// `java.*` packages map to the root.

const producer = require('./producer');
const common = require('./common');
// BEGIN GENERATED REQUIRES (cargo xtask generate-error-codes; do not edit)
const { ConcurrentModificationError } = require('./concurrent_modification_error');
const { ExecutionError } = require('./execution_error');
const { IllegalArgumentError } = require('./illegal_argument_error');
const { IllegalStateError } = require('./illegal_state_error');
const { IndexOutOfBoundsError } = require('./index_out_of_bounds_error');
const { NoSuchElementError } = require('./no_such_element_error');
const { TimeoutError } = require('./timeout_error');
const { UnsupportedOperationError } = require('./unsupported_operation_error');
const consumer = require('./consumer');
// END GENERATED REQUIRES

module.exports = {
  producer,
  common,
  // BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
  ConcurrentModificationError,
  ExecutionError,
  IllegalArgumentError,
  IllegalStateError,
  IndexOutOfBoundsError,
  NoSuchElementError,
  TimeoutError,
  UnsupportedOperationError,
  consumer,
  // END GENERATED EXPORTS
};
