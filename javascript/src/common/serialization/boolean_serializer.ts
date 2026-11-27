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

// Java's BooleanSerializer (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): a boolean as one byte, 1 for true and 0 for false.

import { wrongType } from '../../internal/builtin_serde';
import type { Headers } from '../header/headers';
import type { Serializer } from './serializer';

const TRUE = 0x01;
const FALSE = 0x00;

/**
 * Converts a boolean to one byte, 1 for true and 0 for false.
 */
export class BooleanSerializer implements Serializer<boolean> {
  /** Creates a BooleanSerializer. */
  constructor() {
    // Java's implicit constructor: nothing to set.
  }

  serialize(options: { topic: string; headers: Headers; data: boolean | null }): Uint8Array | null {
    const data: unknown = options.data;
    if (data === null) return null;
    if (typeof data !== 'boolean') throw wrongType('BooleanSerializer', 'a boolean or null', data);
    return Buffer.of(data ? TRUE : FALSE);
  }
}
