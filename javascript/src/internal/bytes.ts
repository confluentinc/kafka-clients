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

// Bytes as Java prints them (private), for the toString() of a record and its
// headers.

/** Java's `Arrays.toString(byte[])`: the signed bytes, `[1, -1]`, or `null`. */
export function bytesToString(value: Uint8Array | null): string {
  if (value === null) return 'null';
  let out = '';
  for (let i = 0; i < value.length; i++) {
    const b = String((value[i] << 24) >> 24);
    out = i === 0 ? b : `${out}, ${b}`;
  }
  return `[${out}]`;
}
