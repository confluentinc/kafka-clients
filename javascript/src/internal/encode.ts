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

// The one place a string becomes bytes (private; `check-string-encoding`
// refuses `Buffer.from(<string>)` and `TextEncoder` anywhere else in src/).
//
// A JavaScript string is UTF-16 and may hold a lone surrogate, which UTF-8
// cannot encode. Java's `String.getBytes(UTF_8)` (and so `StringSerializer`)
// writes `?` for each one; Node's `Buffer.from(s, 'utf8')`, and Node-API's
// `napi_get_value_string_utf8`, write U+FFFD (`EF BF BD`) instead. So a string
// becomes bytes here, the Java way, byte for byte as the Python client's
// `_encoding.py` writes them (root CLAUDE.md, JavaScript Binding Conventions,
// Types): a key or value given as a string, a header key, and the
// configuration's keys and values. Topic names skip it.

/**
 * The UTF-8 bytes of `s` as Java's `String.getBytes(UTF_8)` writes them: each
 * lone surrogate as `?`. A well-formed string (`isWellFormed()`, which V8
 * answers without scanning a one-byte string) is encoded as it is. The Buffer
 * is the client's own, so the addon sends it without a copy.
 *
 * @param s - the string
 * @returns its bytes
 */
export function encodeLikeJava(s: string): Buffer {
  return Buffer.from(s.isWellFormed() ? s : s.replace(/\p{Surrogate}/gu, '?'), 'utf8');
}
