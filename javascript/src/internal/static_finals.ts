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

// Java's public static final fields of a class that is not frozen whole (an
// enum or a config class freezes itself): each named static is made
// non-writable and non-configurable, so no code can reassign it. A static
// made on its first read keeps its getter and becomes non-configurable.

/**
 * Makes the statics `names` of `cls` final, as Java's `static final` fields
 * are: a data property non-writable and non-configurable, a getter
 * non-configurable.
 *
 * @param cls - the class
 * @param names - its static finals
 * @throws `TypeError` when `cls` has no own static of one of the names
 */
export function finalStatics<T extends object>(cls: T, ...names: ReadonlyArray<keyof T & string>): void {
  for (const name of names) {
    const descriptor = Object.getOwnPropertyDescriptor(cls, name);
    if (descriptor === undefined) throw new TypeError(`${name} is not a static of the class`);
    Object.defineProperty(cls, name, 'value' in descriptor ? { writable: false, configurable: false } : { configurable: false });
  }
}
