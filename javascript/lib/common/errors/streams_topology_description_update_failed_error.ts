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
// the Java source of `org.apache.kafka.common.errors.StreamsTopologyDescriptionUpdateFailedException`;
// `cargo xtask check-generated` checks it is up to date.

import { setFfiId } from '../../_throwable';
import { ApiError } from './api_error';

/**
 * Indicates that the streams group topology description plugin failed to
 * process a StreamsGroupTopologyDescriptionUpdate request. The accompanying
 * error message describes the underlying cause; the broker tracks the
 * transient-vs-permanent distinction internally and does not reflect it on the
 * wire.
 *
 * Java:
 * `org.apache.kafka.common.errors.StreamsTopologyDescriptionUpdateFailedException`.
 */
export class StreamsTopologyDescriptionUpdateFailedError extends ApiError {}

setFfiId(StreamsTopologyDescriptionUpdateFailedError, 135); // kafka_common_Error_STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED
