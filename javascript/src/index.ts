// Copyright 2025 Confluent Inc.
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

// The package root: the Java built-in errors (`java.lang.IllegalStateException`
// is `IllegalStateError`, ...) and the type of Java's `java.time.Duration`, as
// the Python client's root package holds them: Java's `java.*` packages map to
// the root. Each of Java's packages has its own import path
// (`@confluentinc/kafka-javascript/producer`,
// `@confluentinc/kafka-javascript/common/errors`), which the root does not
// re-export.

export type { Duration } from './duration';
// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { CancellationError } from './cancellation_error';
export { ConcurrentModificationError } from './concurrent_modification_error';
export { ExecutionError } from './execution_error';
export { IllegalArgumentError } from './illegal_argument_error';
export { IllegalStateError } from './illegal_state_error';
export { IndexOutOfBoundsError } from './index_out_of_bounds_error';
export { NoSuchElementError } from './no_such_element_error';
export { TimeoutError } from './timeout_error';
export { UnsupportedOperationError } from './unsupported_operation_error';
// END GENERATED EXPORTS

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;
