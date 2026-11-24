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

// `Header` returns Node's Buffer: the directive carries @types/node into the
// compiled header.d.ts (`preserve`, as tsc >= 5.5 drops it from declarations
// otherwise).
/// <reference types="node" preserve="true" />

/**
 * A header is a key-value pair.
 */
export interface Header {
  /**
   * Returns the key of the header.
   *
   * @returns the header's key; must not be null.
   */
  key(): string;

  /**
   * Returns the value of the header.
   *
   * @returns the header's value; may be null.
   */
  value(): Buffer | null;
}
