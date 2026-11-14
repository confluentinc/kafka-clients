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

// The default bodies of Java's Serializer interface (root CLAUDE.md,
// JavaScript Binding Conventions, Class family, Q28): what the producer runs
// for a member the user's serializer leaves out.

import type { Serializer } from '../common/serialization/serializer';

/** Java's `Serializer` default methods, for a serializer that lacks them. */
export class SerializerDefaults {
  /**
   * Java's default `Serializer.close()`, which is intentionally left blank.
   *
   * @param serializer - the serializer being closed
   */
  static close(serializer: Serializer<unknown>): void {
    void serializer;
  }
}
