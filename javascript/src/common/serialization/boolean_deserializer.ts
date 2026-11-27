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

// Java's BooleanDeserializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): one byte as a boolean, Java's messages for
// another size and another byte, which names the byte as Java's signed byte.

import { deserializerBytes } from '../../internal/builtin_serde';
import { SerializationError } from '../errors/serialization_error';
import type { Headers } from '../header/headers';
import type { Deserializer } from './deserializer';

const TRUE = 0x01;
const FALSE = 0x00;

/**
 * Converts one byte, 1 for true and 0 for false, to a boolean.
 */
export class BooleanDeserializer implements Deserializer<boolean> {
  /** Creates a BooleanDeserializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  deserialize(options: { topic: string; headers: Headers; data: Buffer | null }): boolean | null {
    const data = options.data;
    if (data === null) return null;
    const bytes = deserializerBytes('BooleanDeserializer', data);
    if (bytes.length !== 1) throw new SerializationError({ message: 'Size of data received by BooleanDeserializer is not 1' });
    const b = bytes.readInt8(0);
    if (b === TRUE) return true;
    if (b === FALSE) return false;
    throw new SerializationError({ message: `Unexpected byte received by BooleanDeserializer: ${b}` });
  }
}
