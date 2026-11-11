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
// the Java source of `org.apache.kafka.common.errors.ClusterAuthorizationException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../../internal/throwable';
import { AuthorizationError } from './authorization_error';

/**
 * Java: `org.apache.kafka.common.errors.ClusterAuthorizationException`.
 */
export class ClusterAuthorizationError extends AuthorizationError {
  /**
   * Makes `ClusterAuthorizationError` nominal: TypeScript checks it by name,
   * not by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_CLUSTER_AUTHORIZATION_FAILED`,
   * for the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 31;
}

setForms(ClusterAuthorizationError, [
  [['message'], 'message'],
  [['message', 'cause'], 'message'],
]);
