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
// the Java source of `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`;
// `cargo xtask check-generated` checks it is up to date.

import { setFfiId } from '../_throwable';
import { InvalidOffsetError } from './invalid_offset_error';

/**
 * Indicates that there is no stored offset for a partition and no defined
 * offset reset policy.
 *
 * Java: `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`.
 */
export class NoOffsetForPartitionError extends InvalidOffsetError {}

setFfiId(NoOffsetForPartitionError, -21); // kafka_common_Error_CONSUMER_NO_OFFSET_FOR_PARTITION
