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

// The package of Java's org.apache.kafka.common.serialization: the
// Serializer and Deserializer interfaces, and Java's built-in serializer and
// deserializer classes of String, ByteArray, Short, Integer, Long, Float,
// Double, Boolean, UUID and Void, built with `new`.
//
// Not translated (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): the ByteBuffer, Bytes and List serdes, and Serde and Serdes.

export type { Deserializer } from './deserializer';
export type { Serializer } from './serializer';
export { BooleanDeserializer } from './boolean_deserializer';
export { BooleanSerializer } from './boolean_serializer';
export { ByteArrayDeserializer } from './byte_array_deserializer';
export { ByteArraySerializer } from './byte_array_serializer';
export { DoubleDeserializer } from './double_deserializer';
export { DoubleSerializer } from './double_serializer';
export { FloatDeserializer } from './float_deserializer';
export { FloatSerializer } from './float_serializer';
export { IntegerDeserializer } from './integer_deserializer';
export { IntegerSerializer } from './integer_serializer';
export { LongDeserializer } from './long_deserializer';
export { LongSerializer } from './long_serializer';
export { ShortDeserializer } from './short_deserializer';
export { ShortSerializer } from './short_serializer';
export { StringDeserializer } from './string_deserializer';
export { StringSerializer } from './string_serializer';
export { UUIDDeserializer } from './uuid_deserializer';
export { UUIDSerializer } from './uuid_serializer';
export { VoidDeserializer } from './void_deserializer';
export { VoidSerializer } from './void_serializer';

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;
