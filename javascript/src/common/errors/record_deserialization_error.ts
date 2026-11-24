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
// the Java source of `org.apache.kafka.common.errors.RecordDeserializationException`;
// `cargo xtask check-generated` checks it is up to date.

import { JavaForms } from '../../internal/args';
import { illegalArgument } from '../../internal/errors';
import * as P from '../../internal/payload';
import { payloadOf, setForms } from '../../internal/throwable';
import type { Header } from '../header/header';
import type { Headers } from '../header/headers';
import { TimestampType } from '../record/timestamp_type';
import type { TopicPartition } from '../topic_partition';
import { SerializationError } from './serialization_error';

const ENUM_0_VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], {
  bareTypes: { name: (value) => value === null || typeof value === 'string' },
});

/**
 * This exception is raised for any error that occurs while deserializing
 * records received by the consumer using the configured
 * `org.apache.kafka.common.serialization.Deserializer`.
 *
 * Java: `org.apache.kafka.common.errors.RecordDeserializationException`.
 */
export class RecordDeserializationError extends SerializationError {
  /**
   * Makes `RecordDeserializationError` nominal: TypeScript checks it by name,
   * not by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `DeserializationExceptionOrigin origin`.
   */
  readonly #origin: RecordDeserializationError.DeserializationExceptionOrigin | null;

  /**
   * Java's field `TopicPartition partition`.
   */
  readonly #partition: TopicPartition | null;

  /**
   * Java's field `long offset`.
   */
  readonly #offset: bigint;

  /**
   * Java's field `TimestampType timestampType`.
   */
  readonly #timestampType: TimestampType | null;

  /**
   * Java's field `long timestamp`.
   */
  readonly #timestamp: bigint;

  /**
   * Java's field `ByteBuffer keyBuffer`.
   */
  readonly #keyBuffer: Buffer | null;

  /**
   * Java's field `ByteBuffer valueBuffer`.
   */
  readonly #valueBuffer: Buffer | null;

  /**
   * Java's field `Headers headers`.
   */
  readonly #headers: Headers | null;

  /**
   * The FFI id of the class, `kafka_common_Error_RECORD_DESERIALIZATION`, for
   * the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -27;

  /**
   * Java's `(DeserializationExceptionOrigin origin, TopicPartition partition,
   * long offset, long timestamp, TimestampType timestampType, ByteBuffer
   * keyBuffer, ByteBuffer valueBuffer, Headers headers, String message,
   * Throwable cause)` constructor.
   *
   * @param options - the `origin` and the `partition` and the `offset` and the
   *   `timestamp` and the `timestampType` and the `keyBuffer` and the
   *   `valueBuffer` and the `headers` and the message and the cause
   */
  constructor(options: { /** Java's `DeserializationExceptionOrigin origin`. */ origin: RecordDeserializationError.DeserializationExceptionOrigin; /** Java's `TopicPartition partition`. */ partition: TopicPartition; /** Java's `long offset`. */ offset: bigint | number; /** Java's `long timestamp`. */ timestamp: bigint | number; /** Java's `TimestampType timestampType`. */ timestampType: TimestampType; /** Java's `ByteBuffer keyBuffer`. */ keyBuffer: Uint8Array | null; /** Java's `ByteBuffer valueBuffer`. */ valueBuffer: Uint8Array | null; /** Java's `Headers headers`. */ headers: Headers | Iterable<Header | readonly [string, Uint8Array | null]>; /** The message; `null` reads as `''`. */ message: string | null; /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
    const fields = payloadOf(this);
    this.#origin = fields.origin as RecordDeserializationError.DeserializationExceptionOrigin | null;
    this.#partition = fields.partition as TopicPartition | null;
    this.#offset = fields.offset as bigint;
    this.#timestampType = fields.timestampType as TimestampType | null;
    this.#timestamp = fields.timestamp as bigint;
    this.#keyBuffer = fields.keyBuffer as Buffer | null;
    this.#valueBuffer = fields.valueBuffer as Buffer | null;
    this.#headers = fields.headers as Headers | null;
  }

  /**
   * Java's `origin()`.
   *
   * @returns Java's `origin`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  origin(): RecordDeserializationError.DeserializationExceptionOrigin {
    P.readOnly('origin', arguments.length);
    return this.#origin as RecordDeserializationError.DeserializationExceptionOrigin;
  }

  /**
   * Java's `topicPartition()`.
   *
   * @returns Java's `partition`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  topicPartition(): TopicPartition {
    P.readOnly('topicPartition', arguments.length);
    return this.#partition as TopicPartition;
  }

  /**
   * Java's `offset()`.
   *
   * @returns Java's `offset`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  offset(): bigint {
    P.readOnly('offset', arguments.length);
    return this.#offset;
  }

  /**
   * Java's `timestampType()`.
   *
   * @returns Java's `timestampType`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  timestampType(): TimestampType {
    P.readOnly('timestampType', arguments.length);
    return this.#timestampType as TimestampType;
  }

  /**
   * Java's `timestamp()`.
   *
   * @returns Java's `timestamp`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  timestamp(): bigint {
    P.readOnly('timestamp', arguments.length);
    return this.#timestamp;
  }

  /**
   * Java's `keyBuffer()`.
   *
   * @returns Java's `keyBuffer`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  keyBuffer(): Buffer | null {
    P.readOnly('keyBuffer', arguments.length);
    return this.#keyBuffer;
  }

  /**
   * Java's `valueBuffer()`.
   *
   * @returns Java's `valueBuffer`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  valueBuffer(): Buffer | null {
    P.readOnly('valueBuffer', arguments.length);
    return this.#valueBuffer;
  }

  /**
   * Java's `headers()`.
   *
   * @returns Java's `headers`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  headers(): Headers {
    P.readOnly('headers', arguments.length);
    return this.#headers as Headers;
  }
}

// Java's nested types, static members of the class (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Names).
// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested enums, reached through the class name as in Java
export namespace RecordDeserializationError {
  /**
   * Java's nested enum
   * `org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin`.
   */
  export class DeserializationExceptionOrigin {
    readonly #name: string;

    readonly #ordinal: number;

    /**
     * Made by this class only (the enum's constants).
     *
     * @param ordinal - the constant's position in Java's declaration, its
     *   `ordinal()`
     * @param name - the constant's name, its `name()`
     */
    private constructor(ordinal: number, name: string) {
      this.#name = name;
      this.#ordinal = ordinal;
      Object.freeze(this);
    }

    /**
     * Java's `KEY`.
     */
    static readonly KEY: DeserializationExceptionOrigin = new DeserializationExceptionOrigin(0, 'KEY');

    /**
     * Java's `VALUE`.
     */
    static readonly VALUE: DeserializationExceptionOrigin = new DeserializationExceptionOrigin(1, 'VALUE');

    /**
     * The constants in declaration order, Java's `values()`.
     *
     * @returns a new array of the constants
     * @throws `TypeError` when it is given an argument: it only reads
     */
    static values(): DeserializationExceptionOrigin[] {
      P.readOnly('values', arguments.length);
      return [DeserializationExceptionOrigin.KEY, DeserializationExceptionOrigin.VALUE];
    }

    /**
     * The constant named exactly `name`, Java's `valueOf(String)`, the name on
     * its own or named.
     *
     * @param name - the name of the constant to return
     * @returns the constant with the specified name
     * @throws `IllegalArgumentError` when no constant has that name, or the
     *   given parameters are not `{name}`
     * @throws `TypeError` when the name is null or not a string
     */
    static valueOf(name: string): DeserializationExceptionOrigin;
    /**
     * The constant named exactly `name`, the same as `valueOf(name)`.
     *
     * @param options - the constant's name
     * @returns the constant
     */
    static valueOf(options: {
      /** The name of the constant to return. */
      name: string;
    }): DeserializationExceptionOrigin;
    static valueOf(options: unknown): DeserializationExceptionOrigin {
      ENUM_0_VALUE_OF_FORMS.match(options, arguments.length);
      const name = ENUM_0_VALUE_OF_FORMS.value(options, 'name');
      // java.lang.Enum.valueOf: requireNonNull's "Name is null".
      if (name === null) throw new TypeError('Name is null');
      if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${typeof name}`);
      const found = DeserializationExceptionOrigin.values().find((c) => c.#name === name);
      if (found === undefined) throw illegalArgument(`No enum constant org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin.${name}`);
      return found;
    }

    /**
     * The name of this constant, exactly as declared, Java's `Enum.name()`.
     *
     * @returns the name
     * @throws `TypeError` when it is given an argument: it only reads
     */
    name(): string {
      P.readOnly('name', arguments.length);
      return this.#name;
    }

    /**
     * The position of this constant in its enum declaration, the first one
     * being 0, Java's `Enum.ordinal()`.
     *
     * @returns the ordinal
     * @throws `TypeError` when it is given an argument: it only reads
     */
    ordinal(): number {
      P.readOnly('ordinal', arguments.length);
      return this.#ordinal;
    }

    /**
     * The name of this constant, Java's `Enum.toString()`.
     *
     * @returns the name
     */
    toString(): string {
      return this.#name;
    }

    /**
     * The name, which `JSON.stringify` writes for the constant.
     *
     * @param key - the key `JSON.stringify` passes, which is ignored
     * @returns the name
     *
     * @remarks *(js-only)* JavaScript's JSON protocol.
     */
    toJSON(key?: string): string {
      void key;
      return this.#name;
    }
  }
}

setForms(RecordDeserializationError, [
  [['origin', 'partition', 'offset', 'timestamp', 'timestampType', 'keyBuffer', 'valueBuffer', 'headers', 'message', 'cause'], 'message', (v) => ({ origin: v.origin, partition: v.partition, offset: v.offset, timestampType: v.timestampType, timestamp: v.timestamp, keyBuffer: v.keyBuffer, valueBuffer: v.valueBuffer, headers: v.headers })],
], {
  order: ['origin', 'partition', 'offset', 'timestamp', 'timestampType', 'keyBuffer', 'valueBuffer', 'headers', 'message', 'cause'],
  params: { origin: P.enumOf(RecordDeserializationError.DeserializationExceptionOrigin), partition: P.topicPartition, offset: P.long, timestamp: P.long, timestampType: P.enumOf(TimestampType), keyBuffer: P.bytes, valueBuffer: P.bytes, headers: P.headers },
  fields: { origin: [P.enumOf(RecordDeserializationError.DeserializationExceptionOrigin), null], partition: [P.topicPartition, null], offset: [P.long, 0n], timestampType: [P.enumOf(TimestampType), null], timestamp: [P.long, 0n], keyBuffer: [P.bytes, null], valueBuffer: [P.bytes, null], headers: [P.headers, null] },
});
