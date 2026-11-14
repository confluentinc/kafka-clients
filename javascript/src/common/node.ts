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

// Java's Node, a data class (root CLAUDE.md, JavaScript Binding Conventions,
// Class family): Java's no-argument value methods are readonly fields set in
// the constructor (`node.id`, `node.host`, `node.isEmpty`), and Java's three
// public constructors are one constructor taking one object of their named
// parameters. The object is frozen when it is created. `equals` and
// `hashCode` are Java's bodies (Q29).
//
// Not translated: Java's protected constructor Node(id, host, port, rack,
// isFenced, idString), which only a subclass calls.

import { JavaForms } from '../internal/args';
import { objectHashCode, objectsEquals, sameClass } from '../internal/java_object';
import { checkInt } from '../internal/java_number';

// Java's public constructors, in Java declaration order (src/internal/args.ts).
const FORMS = new JavaForms('Node', [
  ['id', 'host', 'port'],
  ['id', 'host', 'port', 'rack'],
  ['id', 'host', 'port', 'rack', 'isFenced'],
]);
const NO_NODE_FORMS = new JavaForms('noNode', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['obj']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

function checkOptionalString(value: unknown, what: string): string | null {
  if (value !== null && typeof value !== 'string') throw new TypeError(`${what} must be a string or null`);
  return value;
}

/**
 * Information about a Kafka node
 */
export class Node {
  /** The node id of this node */
  readonly id: number;

  /**
   * String representation of the node id. Typically, the integer id is used
   * to serialize over the wire, the string representation is used as an
   * identifier with NetworkClient code
   */
  readonly idString: string;

  /** The host name for this node */
  readonly host: string | null;

  /** The port for this node */
  readonly port: number;

  /** True if this node has a defined rack */
  readonly hasRack: boolean;

  /** The rack for this node */
  readonly rack: string | null;

  /**
   * Whether this node is fenced.
   *
   * This applies to broker nodes only. For controller quorum nodes, this
   * field is not relevant and is defined to be `false`.
   */
  readonly isFenced: boolean;

  /**
   * Whether this node is empty, which may be the case if `noNode()` is used
   * as a placeholder in a response payload with an error: true if it is,
   * false otherwise
   */
  readonly isEmpty: boolean;

  /**
   * Makes `Node` nominal: TypeScript checks it by name, not by shape, so an
   * object literal with the same fields is no `Node`.
   */
  readonly #brand = true;

  static #noNode: Node | null = null;

  /**
   * Creates a node, Java's `Node(id, host, port, rack, isFenced)`.
   *
   * @param options - the node's id, host, port, rack and whether it is fenced
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{id, host, port}`, `{id, host, port, rack}` or
   *   `{id, host, port, rack, isFenced}`
   * @throws `TypeError` when an argument has the wrong type or is `undefined`
   * @throws `RangeError` when the id or the port is beyond Java's `int`
   *
   * @remarks Java's three constructors are one, each a signature of its own;
   * one a constructor leaves out is what it passes (no rack, not fenced).
   * The object is frozen.
   */
  constructor(options: {
    /** The node id. */
    id: number;
    /** The host name, or null. */
    host: string | null;
    /** The port. */
    port: number;
    /** The rack, or null. */
    rack: string | null;
    /** Whether the node is fenced. */
    isFenced: boolean;
  });
  /**
   * Creates a node, Java's `Node(id, host, port, rack)`: not fenced.
   *
   * @param options - the node's id, host, port and rack
   */
  constructor(options: {
    /** The node id. */
    id: number;
    /** The host name, or null. */
    host: string | null;
    /** The port. */
    port: number;
    /** The rack, or null. */
    rack: string | null;
    isFenced?: never;
  });
  /**
   * Creates a node, Java's `Node(id, host, port)`: no rack, not fenced.
   *
   * @param options - the node's id, host and port
   */
  constructor(options: {
    /** The node id. */
    id: number;
    /** The host name, or null. */
    host: string | null;
    /** The port. */
    port: number;
    rack?: never;
    isFenced?: never;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const id = checkInt(FORMS.value(options, 'id'), 'id');
    const host = checkOptionalString(FORMS.value(options, 'host'), 'host');
    const port = checkInt(FORMS.value(options, 'port'), 'port');
    const given = FORMS.value(options, 'rack');
    const rack = given === undefined ? null : checkOptionalString(given, 'rack');
    const fenced = FORMS.value(options, 'isFenced');
    if (fenced !== undefined && typeof fenced !== 'boolean') throw new TypeError('isFenced must be a boolean');
    this.id = id;
    this.idString = String(id);
    this.host = host;
    this.port = port;
    this.hasRack = rack !== null;
    this.rack = rack;
    this.isFenced = fenced === true;
    this.isEmpty = host === null || host === '' || port < 0;
    Object.freeze(this);
  }

  /**
   * The placeholder node, Java's `new Node(-1, "", -1)`: the same object
   * every call.
   *
   * @returns the placeholder node
   * @throws `TypeError` when it is given an argument
   */
  static noNode(): Node {
    NO_NODE_FORMS.match(undefined, arguments.length);
    Node.#noNode ??= new Node({ id: -1, host: '', port: -1 });
    return Node.#noNode;
  }

  /**
   * Java's hash code of the node: `31` plus the host's hash code (0 for
   * null), then `31 * result` plus the id, the port, the rack's hash code (0
   * for null) and `isFenced`'s (`Boolean.hashCode`).
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    let result = (31 + objectHashCode(this.host)) | 0;
    result = (Math.imul(31, result) + this.id) | 0;
    result = (Math.imul(31, result) + this.port) | 0;
    result = (Math.imul(31, result) + objectHashCode(this.rack)) | 0;
    result = (Math.imul(31, result) + objectHashCode(this.isFenced)) | 0;
    return result;
  }

  /**
   * Whether `obj` is a `Node` with the same id, port, host, rack and fenced
   * state, Java's `Node.equals`.
   *
   * @param options - the object to compare with
   * @returns true when `obj` is an equal node
   * @throws `IllegalArgumentError` when the given parameters are not `{obj}`
   */
  equals(options: {
    /** The object to compare with. */
    obj: unknown;
  }): boolean;
  /**
   * The same as `equals({obj})`, with the object on its own; a plain object
   * is read as the options object.
   *
   * @param obj - the object to compare with
   * @returns true when `obj` is an equal node
   */
  equals(obj: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const obj = EQUALS_FORMS.value(options, 'obj');
    if (this === obj) return true;
    if (obj === null || obj === undefined || !sameClass(this, obj)) return false;
    const other = obj as Node;
    return (
      this.id === other.id &&
      this.port === other.port &&
      objectsEquals(this.host, other.host) &&
      objectsEquals(this.rack, other.rack) &&
      objectsEquals(this.isFenced, other.isFenced)
    );
  }

  /**
   * The node as Java writes it:
   * `<host>:<port> (id: <idString> rack: <rack> isFenced: <isFenced>)`.
   *
   * @returns the node's text
   */
  toString(): string {
    return `${String(this.host)}:${this.port} (id: ${this.idString} rack: ${String(this.rack)} isFenced: ${this.isFenced})`;
  }
}
