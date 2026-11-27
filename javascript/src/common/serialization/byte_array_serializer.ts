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

// Java's ByteArraySerializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): the bytes given, not copied, as Java returns the
// array.

import { wrongType } from '../../internal/builtin_serde';
import type { Headers } from '../header/headers';
import type { Serializer } from './serializer';

/**
 * Passes bytes through as they are.
 */
export class ByteArraySerializer implements Serializer<Uint8Array> {
  /** Creates a ByteArraySerializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  serialize(options: { topic: string; headers: Headers; data: Uint8Array | null }): Uint8Array | null {
    const data: unknown = options.data;
    if (data === null || data instanceof Uint8Array) return data;
    throw wrongType('ByteArraySerializer', 'a Uint8Array or null', data);
  }
}
