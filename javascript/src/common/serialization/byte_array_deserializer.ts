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

// Java's ByteArrayDeserializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): the Buffer given, over the fetched bytes, not
// copied, as Java returns the array.

import { deserializerBytes } from '../../internal/builtin_serde';
import type { Headers } from '../header/headers';
import type { Deserializer } from './deserializer';

/**
 * Passes bytes through as they are: the `Buffer` given, over the fetched bytes.
 */
export class ByteArrayDeserializer implements Deserializer<Buffer> {
  /** Creates a ByteArrayDeserializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  deserialize(options: { topic: string; headers: Headers; data: Buffer | null }): Buffer | null {
    const data = options.data;
    if (data === null) return null;
    return deserializerBytes('ByteArrayDeserializer', data);
  }
}
