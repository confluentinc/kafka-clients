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

// Java's org.apache.kafka.clients.admin.Config: a map of entries by name, in
// the order they were given (Java's HashMap order is unspecified).

import { JavaForms, kind } from '../internal/args';
import { mapEquals, mapHashCode, sameClass } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { ConfigEntry } from './config_entry';

const FORMS = new JavaForms('Config', [['entries']], {
  bareTypes: { entries: (value) => value === null || (typeof value === 'object' && Symbol.iterator in value) },
});
const ENTRIES_FORMS = new JavaForms('entries', [[]], { reader: true });
const GET_FORMS = new JavaForms('get', [['name']], { bareTypes: { name: (value) => value === null || typeof value === 'string' } });
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * A configuration object containing the configuration entries for a
 * resource.
 */
export class Config {
  readonly #entries = new Map<string, ConfigEntry>();

  /**
   * Create a configuration instance with the provided entries.
   *
   * @param entries - the entries, any iterable of ConfigEntry, on its own
   * @throws `TypeError` when they are null (Java iterates them) or not ConfigEntry objects
   */
  constructor(entries: Iterable<ConfigEntry>);
  /**
   * Create a configuration instance with the provided entries, given by name.
   *
   * @param options - the entries
   * @throws `TypeError` when they are null (Java iterates them) or not ConfigEntry objects
   */
  constructor(options: {
    /** The entries, any iterable of ConfigEntry. */
    entries: Iterable<ConfigEntry>;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const entries = FORMS.value(options, 'entries');
    if (entries === null) throw new TypeError('entries must not be null');
    for (const entry of entries as Iterable<unknown>) {
      if (entry === null) throw new TypeError('entries must not contain null');
      if (!(entry instanceof ConfigEntry)) throw new TypeError(`entries must contain only ConfigEntry, not ${kind(entry)}`);
      this.#entries.set(entry.name(), entry);
    }
  }

  /**
   * Configuration entries for a resource.
   *
   * @returns the entries, a new array
   * @throws `TypeError` when it is given an argument: it only reads
   */
  entries(): ConfigEntry[] {
    if (arguments.length !== 0) ENTRIES_FORMS.match(undefined, arguments.length);
    return [...this.#entries.values()];
  }

  /**
   * Get the configuration entry with the provided name or null if there
   * isn't one.
   *
   * @param name - the entry's name, on its own
   * @returns the entry, or null
   */
  get(name: string | null): ConfigEntry | null;
  /**
   * The same as `get(name)`, the name given by name.
   *
   * @param options - the entry's name
   * @returns the entry, or null
   */
  get(options: {
    /** The entry's name. */
    name: string | null;
  }): ConfigEntry | null;
  get(name: unknown): ConfigEntry | null {
    GET_FORMS.match(name, arguments.length);
    const key = GET_FORMS.value(name, 'name');
    return typeof key === 'string' ? (this.#entries.get(key) ?? null) : null;
  }

  /**
   * Whether `o` is a Config of equal entries.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    return sameClass(this, other) && mapEquals(this.#entries, (other as Config).#entries);
  }

  /**
   * Java's `entries.hashCode()`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return mapHashCode(this.#entries);
  }

  /**
   * The configuration as JSON: its entries.
   *
   * @returns the entries
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return { entries: [...this.#entries.values()] };
  }

  /**
   * Java's `toString()`: `Config(entries=[<entries>])`.
   *
   * @returns the text
   */
  toString(): string {
    return `Config(entries=${javaString([...this.#entries.values()])})`;
  }
}
