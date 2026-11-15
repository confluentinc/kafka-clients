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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// the Java sources (the generated modules of src/common/requests/);
// `cargo xtask check-generated` checks it is up to date.

// `src/common/requests`: the generated modules of src/common/requests/.

/**
 * The classes of the Java package this client has, generated from the Java
 * sources.
 *
 * What they leave out:
 *
 * - `CorrelationIdMismatchError`: An error the core reports has no payload for
 *   `requestCorrelationId` (it is -1, a value Java's counter never assigns, as
 *   the Python client reads it) and `responseCorrelationId` (it is -1, a value
 *   Java's counter never assigns, as the Python client reads it): the FFI's
 *   view of the class has no accessor for it.
 *
 * @packageDocumentation
 */

export { CorrelationIdMismatchError } from './correlation_id_mismatch_error';

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined`.
export declare const __esModule: true;
