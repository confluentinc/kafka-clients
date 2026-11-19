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
// the Java source of `java.util.NoSuchElementException`;
// `cargo xtask check-generated` checks it is up to date.

import { errorArgs, init, setForms } from './internal/throwable';

/**
 * Java's built-in `java.util.NoSuchElementException`.
 */
export class NoSuchElementError extends Error {
  /**
   * The error's cause, Java's `getCause()`: set by `super()` (`Error`'s
   * `options.cause`), declared here for the targets whose `lib` predates
   * ES2022's `Error.cause`. A `declare` field, which emits nothing: a class
   * field would overwrite it.
   */
  declare readonly cause: unknown;

  /**
   * Makes `NoSuchElementError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's no-argument constructor. The message is empty, Java's null.
   */
  constructor();
  /**
   * Java's `(String message)` constructor.
   *
   * @param options - the message
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; name?: never });
  /**
   * Java's `(String message)` constructor. Given the message on its own, the
   * same as `{ message }`.
   *
   * @param message - the message; `null` reads as `''`
   */
  constructor(message: string | null);
  constructor(...args: unknown[]) {
    super(...errorArgs(new.target, args));
    init(this, new.target);
  }
}

setForms(NoSuchElementError, [
  [[], null],
  [['message'], 'message'],
]);
