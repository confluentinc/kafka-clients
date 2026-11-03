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

// The KafkaErrors of the binding's own checks (private): where Java throws
// IllegalArgumentException / IllegalStateException, with the core's code for
// that class.

const { KafkaError } = require('./common/kafka_error');

// `kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT` / `_LOCAL_ILLEGAL_STATE`: the
// core's codes for Java's IllegalArgumentException / IllegalStateException
// (test/static/header_conformance.test.js checks them against the header).
const LOCAL_ILLEGAL_ARGUMENT = -3;
const LOCAL_ILLEGAL_STATE = -4;

/** A binding-side check where Java throws `IllegalArgumentException`. */
function illegalArgument(message) {
  return KafkaError._from({ code: LOCAL_ILLEGAL_ARGUMENT, name: 'LOCAL_ILLEGAL_ARGUMENT', message });
}

/** A binding-side check where Java throws `IllegalStateException`. */
function illegalState(message) {
  return KafkaError._from({ code: LOCAL_ILLEGAL_STATE, name: 'LOCAL_ILLEGAL_STATE', message });
}

module.exports = { illegalArgument, illegalState };
