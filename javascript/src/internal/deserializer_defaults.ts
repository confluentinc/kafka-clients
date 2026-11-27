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

// The default bodies of Java's Deserializer interface (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family): what the consumer runs
// for a member the user's deserializer leaves out.

import type { Deserializer } from '../common/serialization/deserializer';

/**
 * Java's `Deserializer` default methods, for a deserializer that lacks them: the
 * binding runs each with the user's deserializer as `this`
 * (`DeserializerDefaults.prototype.close.call(deserializer)`), as Java runs an
 * interface's default method on the object that implements it.
 */
export class DeserializerDefaults {
  /**
   * Close this deserializer: Java's default `Deserializer.close()`, which is
   * intentionally left blank.
   */
  close(this: Deserializer<unknown>): void {
    // Java's default body is empty.
  }
}
