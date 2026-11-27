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

// Java's LongDeserializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): 8 big-endian bytes as a bigint, Java's message for
// another size.

import { deserializerBytes } from '../../internal/builtin_serde';
import { SerializationError } from '../errors/serialization_error';
import type { Headers } from '../header/headers';
import type { Deserializer } from './deserializer';

/**
 * Converts 8 big-endian bytes to a bigint, Java's `Long`.
 */
export class LongDeserializer implements Deserializer<bigint> {
  /** Creates a LongDeserializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  deserialize(options: { topic: string; headers: Headers; data: Buffer | null }): bigint | null {
    const data = options.data;
    if (data === null) return null;
    const bytes = deserializerBytes('LongDeserializer', data);
    if (bytes.length !== 8) throw new SerializationError({ message: 'Size of data received by LongDeserializer is not 8' });
    return bytes.readBigInt64BE(0);
  }
}
