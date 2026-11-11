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

// The typing test: compiled by test/typing/typing.test.js with
// `tsc --noEmit --strict` against the declarations tsc compiles into dist/,
// resolved through package.json's `exports` (the package's own name). Every
// `@ts-expect-error` line must be a type error, and the types the client
// returns are asserted with expect-type's `expectTypeOf`: `toEqualTypeOf`
// for an exact type, `toExtend` where a value is one of a wider type (an
// error of its parent class, a value of the caller's own type).

import { expectTypeOf } from 'expect-type';
import type { Duration } from '@confluentinc/kafka-javascript';
// @ts-expect-error the root exports no namespace: each package has its own import path
import { producer } from '@confluentinc/kafka-javascript';
import {
  type Callback,
  type CloseArgs,
  KafkaProducer,
  MockProducer,
  ProducerConfig,
  type ProducerConfigs,
  ProducerRecord,
  RecordMetadata,
} from '@confluentinc/kafka-javascript/producer';
import { KafkaError, KafkaFuture, Node, PartitionInfo, TopicPartition } from '@confluentinc/kafka-javascript/common';
import { CancellationError, ExecutionError, IllegalArgumentError, IllegalStateError } from '@confluentinc/kafka-javascript';
import { AuthenticationError, RetriableError, SaslAuthenticationError, TopicAuthorizationError } from '@confluentinc/kafka-javascript/common/errors';
import { QuotaViolationError } from '@confluentinc/kafka-javascript/common/metrics';
import { ConfigError, SaslConfigs, SslClientAuth, SslConfigs } from '@confluentinc/kafka-javascript/common/config';
import { SecurityProtocol } from '@confluentinc/kafka-javascript/common/security/auth';
import { Header, Headers } from '@confluentinc/kafka-javascript/common/header';
import type { Serializer } from '@confluentinc/kafka-javascript/common/serialization';

void producer;

const configs: ProducerConfigs = {
  'bootstrap.servers': 'localhost:9092',
  'linger.ms': 5,
  'enable.idempotence': true,
  'buffer.memory': 33554432n,
  'bootstrap.servers.list': ['a:1', 'b:2'],
  'client.id': null,
};
// @ts-expect-error a configuration is a plain object, not a Map
const fromMap: ProducerConfigs = new Map<string, unknown>([['bootstrap.servers', 'localhost:9092']]);
void fromMap;

const headers: Header[] = [new Header({ key: 'h', value: Buffer.from('v') }), new Header({ key: 'n', value: null })];
// A producer without serializers is a KafkaProducer<Uint8Array, Uint8Array>:
// its records hold bytes.
const key = Buffer.from('k');
const record = new ProducerRecord({ topic: 't', partition: 0, timestamp: null, key, value: new Uint8Array(1), headers });
expectTypeOf(record).toExtend<ProducerRecord<Uint8Array, Uint8Array>>();
// One signature per Java constructor.
new ProducerRecord({ topic: 't', partition: 0, timestamp: 5, key: null, value: 'v' });
new ProducerRecord({ topic: 't', partition: null, key, value: 'v', headers });
new ProducerRecord({ topic: 't', partition: null, key: null, value: null });
new ProducerRecord({ topic: 't', key, value: 'v' });
new ProducerRecord({ topic: 't', value: null });
const topic = record.topic;
expectTypeOf(topic).toEqualTypeOf<string>();
const partition = record.partition;
expectTypeOf(partition).toEqualTypeOf<number | null>();
const recordHeaders = record.headers;
expectTypeOf(recordHeaders).toEqualTypeOf<Headers>();
void [topic, partition, recordHeaders];
// Headers: Java's members, the headers given in any of the three forms.
const built = new Headers([new Header({ key: 'a', value: Buffer.from('1') })]).add(new Header({ key: 'b', value: null })).add({ key: 'c', value: null });
expectTypeOf(built).toEqualTypeOf<Headers>();
new Headers({ headers: built });
new Headers({ headers: null });
new Headers();
new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: built });
const last = built.lastHeader('a');
expectTypeOf(last).toEqualTypeOf<Header | null>();
const byKey = built.headers({ key: 'a' });
expectTypeOf(byKey).toEqualTypeOf<Iterable<Header>>();
const asArray = built.toArray();
expectTypeOf(asArray).toEqualTypeOf<Header[]>();
// Header keeps Java's methods; a value is returned as a Buffer.
const headerValue = asArray[0].value();
expectTypeOf(headerValue).toEqualTypeOf<Buffer | null>();
const headerKey = asArray[0].key();
expectTypeOf(headerKey).toEqualTypeOf<string>();
for (const h of built.remove('a')) void h.key();
// @ts-expect-error key() only reads: it takes no argument
asArray[0].key(1);
// @ts-expect-error a Header's key is a method, not a field
const notAKeyField: string = asArray[0].key;
void notAKeyField;
void [last, byKey, headerValue, headerKey];
// @ts-expect-error a header value is bytes: encode a string first
new Header({ key: 'k', value: 'v' });
// @ts-expect-error a header key is a string
new Header({ key: 1, value: null });
// @ts-expect-error a [key, value] pair is no Header
new Headers([['k', null]]);
// @ts-expect-error a {key, value} object is not a Header
built.add({ header: { key: 'k', value: null } });
// A data object: its fields spread into a plain object of the same types.
const { timestamp: recordTimestamp, key: recordKey, value: recordValue }: { timestamp: number | null; key: Buffer | null; value: Uint8Array | null } = { ...record };
void [recordTimestamp, recordKey, recordValue];

const callback: Callback = ({ metadata, exception: error }: { metadata: RecordMetadata; exception: Error | null }) => {
  const offset = metadata.offset;
  expectTypeOf(offset).toEqualTypeOf<number>();
  const hasOffset = metadata.hasOffset;
  expectTypeOf(hasOffset).toEqualTypeOf<boolean>();
  // Classified with instanceof (or `name`): an error has no code or retriable flag.
  if (error instanceof KafkaError) {
    const name = error.name;
    expectTypeOf(name).toEqualTypeOf<string>();
    const message = error.message;
    expectTypeOf(message).toEqualTypeOf<string>();
    const cause = error.cause;
    expectTypeOf(cause).toEqualTypeOf<unknown>();
    void [name, message, cause];
    // @ts-expect-error the FFI id is private
    void error.code;
    // @ts-expect-error retriable is instanceof RetriableError
    void error.retriable;
    // @ts-expect-error the enumerator name is private too
    void error.errorName;
    // @ts-expect-error apiError is instanceof ApiError
    void error.apiError;
  }
  void [offset, hasOffset];
};

async function produce(): Promise<void> {
  const kafkaProducer = new KafkaProducer({ configs });
  // The configs on their own, Java's one-parameter constructor.
  const bareProducer = new KafkaProducer(configs);
  // `await using` closes it: KafkaProducer is AsyncDisposable.
  const disposed = bareProducer[Symbol.asyncDispose]();
  expectTypeOf(disposed).toEqualTypeOf<Promise<void>>();
  void disposed;
  const future = await kafkaProducer.send({ record, callback });
  expectTypeOf(future).toEqualTypeOf<KafkaFuture<RecordMetadata>>();
  await kafkaProducer.send({ record });
  await kafkaProducer.send({ record, callback: null });
  // The record on its own, Java's send(record).
  await kafkaProducer.send(record);
  // @ts-expect-error send's record is required
  await kafkaProducer.send({ callback });
  // @ts-expect-error the callback is not a bare value: send(record, callback) is `{record, callback}`
  await kafkaProducer.send(record, callback);
  const metadata = await future.get();
  expectTypeOf(metadata).toEqualTypeOf<RecordMetadata>();
  // @ts-expect-error get's timeout, when given, is a Duration: undefined is no value
  await future.get(undefined);
  const done = future.isDone();
  expectTypeOf(done).toEqualTypeOf<boolean>();
  const failed = future.isCompletedExceptionally();
  expectTypeOf(failed).toEqualTypeOf<boolean>();
  const cancelled = future.cancel(true);
  expectTypeOf(cancelled).toEqualTypeOf<boolean>();
  void [metadata, done, failed, cancelled, future.isCancelled(), future.cancel({ mayInterruptIfRunning: false })];
  // Java's KafkaFuture members, each taking the bare value or its options object.
  const timed = await future.get(5000);
  expectTypeOf(timed).toEqualTypeOf<RecordMetadata>();
  const timedToo = await future.get({ timeout: Infinity });
  expectTypeOf(timedToo).toEqualTypeOf<RecordMetadata>();
  const now = future.getNow(null);
  expectTypeOf(now).toEqualTypeOf<RecordMetadata | null>();
  const nowToo = future.getNow({ valueIfAbsent: null });
  expectTypeOf(nowToo).toEqualTypeOf<RecordMetadata | null>();
  const offset: KafkaFuture<number> = future.thenApply((m) => m.offset);
  const offsetToo: KafkaFuture<number> = future.thenApply({ function: (m: RecordMetadata) => m.offset });
  const fn: KafkaFuture.BaseFunction<RecordMetadata, string> = (m) => m.topic;
  const topicFuture = future.thenApply(fn);
  expectTypeOf(topicFuture).toEqualTypeOf<KafkaFuture<string>>();
  const action: KafkaFuture.BiConsumer<RecordMetadata | null, unknown> = (m, e) => void [m, e];
  const same = future.whenComplete(action);
  expectTypeOf(same).toEqualTypeOf<KafkaFuture<RecordMetadata>>();
  const sameToo: KafkaFuture<RecordMetadata> = future.whenComplete({ action: (m, e) => void [m, e] });
  const stage = future.toCompletionStage();
  expectTypeOf(stage).toEqualTypeOf<Promise<RecordMetadata>>();
  const all = KafkaFuture.allOf([future, offset]);
  expectTypeOf(all).toEqualTypeOf<KafkaFuture<void>>();
  const allToo = KafkaFuture.allOf({ futures: [future] });
  expectTypeOf(allToo).toEqualTypeOf<KafkaFuture<void>>();
  const completed = KafkaFuture.completedFuture(1);
  expectTypeOf(completed).toEqualTypeOf<KafkaFuture<number>>();
  const completedToo = KafkaFuture.completedFuture({ value: 'v' });
  expectTypeOf(completedToo).toEqualTypeOf<KafkaFuture<string>>();
  const text = future.toString();
  expectTypeOf(text).toEqualTypeOf<string>();
  void [timed, timedToo, now, nowToo, offsetToo, topicFuture, same, sameToo, stage, all, allToo, completed, completedToo, text];
  // A plain object is always the options object: an object value goes by name.
  const objectValue = KafkaFuture.completedFuture({ value: { key: 'k', value: 'v' } });
  expectTypeOf(objectValue).toEqualTypeOf<KafkaFuture<{ key: string; value: string }>>();
  const objectNow = future.getNow({ valueIfAbsent: now });
  expectTypeOf(objectNow).toEqualTypeOf<RecordMetadata | null>();
  const arrayValue = KafkaFuture.completedFuture([1, 2]);
  expectTypeOf(arrayValue).toEqualTypeOf<KafkaFuture<number[]>>();
  void [objectValue, objectNow, arrayValue];
  // @ts-expect-error a bare plain object is the options object, not the value
  KafkaFuture.completedFuture({ key: 'k', value: 'v' });
  // @ts-expect-error generic code passes its value by name: a T may be a plain object
  const wrap = <V>(x: V): KafkaFuture<V> => KafkaFuture.completedFuture(x);
  void wrap;
  if (now !== null) {
    // @ts-expect-error an instance of the caller's class is passed to getNow by name in TypeScript
    future.getNow(now);
  }
  // A bare value infers its widened type, as Java's KafkaFuture<Integer>.
  let counter = KafkaFuture.completedFuture(0);
  counter = counter.thenApply((v) => v + 1);
  let flag = KafkaFuture.completedFuture(true);
  flag = KafkaFuture.completedFuture(false);
  let label = KafkaFuture.completedFuture('a');
  label = KafkaFuture.completedFuture('b');
  const fallbacks: [number | null, string | null, boolean | null] = [
    KafkaFuture.completedFuture(1).getNow(0),
    KafkaFuture.completedFuture('a').getNow({ valueIfAbsent: 'b' }),
    KafkaFuture.completedFuture(true).getNow(false),
  ];
  void [counter, flag, label, fallbacks];
  // The built-in classes no plain object can be are taken bare too.
  const map = KafkaFuture.completedFuture(new Map<string, number>());
  expectTypeOf(map).toEqualTypeOf<KafkaFuture<Map<string, number>>>();
  const set = KafkaFuture.completedFuture(new Set<string>());
  expectTypeOf(set).toEqualTypeOf<KafkaFuture<Set<string>>>();
  const date = KafkaFuture.completedFuture(new Date(0));
  expectTypeOf(date).toEqualTypeOf<KafkaFuture<Date>>();
  const buffer = KafkaFuture.completedFuture(Buffer.from('x'));
  expectTypeOf(buffer).toExtend<KafkaFuture<Buffer>>();
  const bytes = KafkaFuture.completedFuture(new Uint8Array(2));
  expectTypeOf(bytes).toExtend<KafkaFuture<Uint8Array>>();
  const raw = KafkaFuture.completedFuture(new ArrayBuffer(2));
  expectTypeOf(raw).toEqualTypeOf<KafkaFuture<ArrayBuffer>>();
  const fnValue: KafkaFuture<() => number> = KafkaFuture.completedFuture(() => 1);
  const bufferNow = buffer.getNow(Buffer.alloc(0));
  expectTypeOf(bufferNow).toExtend<Buffer | null>();
  const mapNow = map.getNow(new Map<string, number>());
  expectTypeOf(mapNow).toEqualTypeOf<Map<string, number> | null>();
  void [set, date, bytes, raw, fnValue, bufferNow, mapNow];
  // An instance of the caller's own class goes by name in TypeScript (plain JavaScript also takes it bare).
  class Box {
    constructor(readonly n: number) {}
  }
  const boxed = KafkaFuture.completedFuture({ value: new Box(1) });
  expectTypeOf(boxed).toEqualTypeOf<KafkaFuture<Box>>();
  void boxed;
  // @ts-expect-error a class instance is passed by name in TypeScript
  KafkaFuture.completedFuture(new Box(1));
  // @ts-expect-error undefined is no value
  KafkaFuture.completedFuture(undefined);
  // @ts-expect-error a plain object is the options object, a boolean valueOf notwithstanding
  KafkaFuture.completedFuture({ valueOf: () => true });
  // The value's own type is kept: a branded primitive, a literal union open to any string.
  type TopicId = string & { readonly brand: 'TopicId' };
  type Offset = number & { readonly brand: 'Offset' };
  type Compression = 'gzip' | 'snappy' | (string & {});
  const topicId = 'orders' as TopicId;
  const brandedOffset = 7 as Offset;
  const codec: Compression = 'zstd';
  const idFuture = KafkaFuture.completedFuture(topicId);
  expectTypeOf(idFuture).toEqualTypeOf<KafkaFuture<TopicId>>();
  const offsetFuture = KafkaFuture.completedFuture(brandedOffset);
  expectTypeOf(offsetFuture).toEqualTypeOf<KafkaFuture<Offset>>();
  const codecFuture = KafkaFuture.completedFuture(codec);
  expectTypeOf(codecFuture).toExtend<KafkaFuture<Compression>>();
  const codecLength: KafkaFuture<number> = codecFuture.thenApply((v) => v.length);
  const idNow = idFuture.getNow(topicId);
  expectTypeOf(idNow).toEqualTypeOf<TopicId | null>();
  const codecNow = codecFuture.getNow('gzip');
  expectTypeOf(codecNow).toExtend<Compression | null>();
  const numberFuture = KafkaFuture.completedFuture(brandedOffset as number);
  expectTypeOf(numberFuture).toEqualTypeOf<KafkaFuture<number>>();
  const booleanFuture = KafkaFuture.completedFuture(Math.random() > 0.5);
  expectTypeOf(booleanFuture).toEqualTypeOf<KafkaFuture<boolean>>();
  void [offsetFuture, codecLength, idNow, codecNow, numberFuture, booleanFuture];
  // @ts-expect-error cancel's mayInterruptIfRunning is required, as Java's
  future.cancel();
  // @ts-expect-error thenApply's function is called positionally with one value
  future.thenApply((a: RecordMetadata, b: number) => a.offset + b);
  // @ts-expect-error get's timeout is a Duration
  await future.get('5000');
  const unknown = RecordMetadata.UNKNOWN_PARTITION;
  expectTypeOf(unknown).toEqualTypeOf<-1>();
  void unknown;
  await kafkaProducer.flush();
  // @ts-expect-error flush takes no argument
  await kafkaProducer.flush(100);
  const closeArgs: CloseArgs = { timeout: 0 };
  await kafkaProducer.close(closeArgs);
  // @ts-expect-error undefined is no timeout: leave the argument out
  await kafkaProducer.close(undefined);
  // @ts-expect-error close(Duration)'s timeout is required in its options object
  await kafkaProducer.close({});
  const timeout: Duration = Infinity;
  await kafkaProducer.close(timeout);
  await kafkaProducer.close(5000);
  await kafkaProducer.close({ timeout: Infinity });
  await kafkaProducer.close();
  // @ts-expect-error the timeout is `timeout`, Java's name
  await kafkaProducer.close({ timeoutMs: 0 });
  // @ts-expect-error a timeout is milliseconds or a Temporal.Duration
  await kafkaProducer.close({ timeout: '5s' });
}
void produce;

// The errors: Java's extends chain, Java's message and cause constructor forms.
const sasl = new SaslAuthenticationError('bad credentials');
expectTypeOf(sasl).toExtend<AuthenticationError>();
const base = new KafkaError({ message: null, cause: sasl });
expectTypeOf(base).toEqualTypeOf<KafkaError>();
const fromCause = new KafkaError(sasl);
expectTypeOf(fromCause).toEqualTypeOf<KafkaError>();
const causeByName = new KafkaError({ cause: sasl });
expectTypeOf(causeByName).toEqualTypeOf<KafkaError>();
const empty = new KafkaError();
expectTypeOf(empty).toEqualTypeOf<KafkaError>();
const state = new IllegalStateError('closed');
expectTypeOf(state).toEqualTypeOf<IllegalStateError>();
// IllegalArgumentError is JavaScript's RangeError.
const range = new IllegalArgumentError({ message: 'bad' });
expectTypeOf(range).toExtend<RangeError>();
const execution = new ExecutionError({ message: 'failed', cause: null });
expectTypeOf(execution).toEqualTypeOf<ExecutionError>();
// Java's CancellationException extends IllegalStateException.
const cancellation = new CancellationError('cancelled');
expectTypeOf(cancellation).toExtend<IllegalStateError>();
void [cancellation, new CancellationError(), new CancellationError({ message: null })];
// A failed future's get() rejects with ExecutionError; the delivery error is its cause.
async function deliveryError(future: KafkaFuture<RecordMetadata>): Promise<KafkaError | null> {
  try {
    await future.get();
    return null;
  } catch (e) {
    if (e instanceof ExecutionError && e.cause instanceof KafkaError) return e.cause;
    throw e;
  }
}
void deliveryError;
const config = new ConfigError('Invalid value');
expectTypeOf(config).toEqualTypeOf<ConfigError>();
void [sasl, base, fromCause, causeByName, empty, state, range, execution, config];
// @ts-expect-error (message, options) is not a Java form: the cause is a named parameter
new KafkaError('outer', { cause: sasl });
// @ts-expect-error a bare cause is an Error
new KafkaError(5);
// @ts-expect-error TopicAuthorizationException has no (String, Throwable) constructor
new TopicAuthorizationError({ message: 'm', cause: sasl });
// @ts-expect-error ExecutionException has no (String) constructor
new ExecutionError('failed');
// @ts-expect-error QuotaViolationException's constructors take a payload, not translated yet
new QuotaViolationError('m');
// An Error is not an options object: Java's ConfigException(Throwable) does not exist.
const caught = new Error('caught');
// @ts-expect-error a ConfigError takes no cause
new ConfigError(caught);
// @ts-expect-error nor a client error, although it has a string message
new ConfigError(sasl);
// @ts-expect-error a TopicAuthorizationError takes no cause either
new TopicAuthorizationError(caught);
// A class with a (cause) form takes the Error as its cause.
void new KafkaError(caught);
class MyRetriable extends RetriableError {}
void new MyRetriable('m');
// @ts-expect-error Java's RetriableError is abstract: caught and subclassed, never constructed
new RetriableError('m');

// The config classes' constants and the security enums.
const mechanism = SaslConfigs.SASL_MECHANISM;
expectTypeOf(mechanism).toEqualTypeOf<string>();
const truststore = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
expectTypeOf(truststore).toEqualTypeOf<string>();
// A constant Set is a read-only set.
const reconfigurable = SslConfigs.RECONFIGURABLE_CONFIGS;
expectTypeOf(reconfigurable).toEqualTypeOf<ReadonlySet<string>>();
// @ts-expect-error a read-only set has no add
SslConfigs.RECONFIGURABLE_CONFIGS.add('x');
const auth = SslClientAuth.forConfig('required');
expectTypeOf(auth).toEqualTypeOf<SslClientAuth | null>();
const named = SslClientAuth.forConfig({ key: null });
expectTypeOf(named).toEqualTypeOf<SslClientAuth | null>();
const protocol = SecurityProtocol.forName('sasl_ssl');
expectTypeOf(protocol).toEqualTypeOf<SecurityProtocol>();
const byName = SecurityProtocol.forName({ name: 'ssl' });
expectTypeOf(byName).toEqualTypeOf<SecurityProtocol>();
const byId = SecurityProtocol.forId({ id: 1 });
expectTypeOf(byId).toEqualTypeOf<SecurityProtocol | null>();
void [named, byName, byId];
const protocolId = protocol.id;
expectTypeOf(protocolId).toEqualTypeOf<number>();
void [mechanism, truststore, reconfigurable, auth, protocolId];
// java.lang.Enum's members.
const protocolName = protocol.name();
expectTypeOf(protocolName).toEqualTypeOf<string>();
const protocolOrdinal = protocol.ordinal();
expectTypeOf(protocolOrdinal).toEqualTypeOf<number>();
const sasl2 = SecurityProtocol.valueOf('SASL_SSL');
expectTypeOf(sasl2).toEqualTypeOf<SecurityProtocol>();
const ssl2 = SecurityProtocol.valueOf({ name: 'SSL' });
expectTypeOf(ssl2).toEqualTypeOf<SecurityProtocol>();
const authName = SslClientAuth.NONE.name();
expectTypeOf(authName).toEqualTypeOf<string>();
const authOrdinal = SslClientAuth.NONE.ordinal();
expectTypeOf(authOrdinal).toEqualTypeOf<number>();
const authValues = SslClientAuth.values();
expectTypeOf(authValues).toEqualTypeOf<SslClientAuth[]>();
const none = SslClientAuth.valueOf('NONE');
expectTypeOf(none).toEqualTypeOf<SslClientAuth>();
const protocolJson = protocol.toJSON();
expectTypeOf(protocolJson).toEqualTypeOf<string>();
void [protocolName, protocolOrdinal, sasl2, ssl2, authName, authOrdinal, authValues, none, protocolJson];
// @ts-expect-error name is Enum's name(), a method
const notAField: string = protocol.name;
void notAField;
// @ts-expect-error a constant is read-only
SslConfigs.SSL_PROTOCOL_CONFIG = 'x';
// @ts-expect-error ProducerConfig holds Java's constants only: its constructor is private
new ProducerConfig();

// Made by the client only: the constructors are private, and the library's
// own factories and methods are left out of the declarations (stripInternal).
const fields = { topic: 't', partition: 0, hasOffset: true, offset: 0, hasTimestamp: true, timestamp: 0, serializedKeySize: -1, serializedValueSize: -1 };
// @ts-expect-error RecordMetadata's constructor is private
new RecordMetadata(fields);
// @ts-expect-error KafkaFuture's constructor is private
new KafkaFuture();
// @ts-expect-error SecurityProtocol's constructor is private
new SecurityProtocol(0, 0, 'PLAINTEXT');
// @ts-expect-error SslClientAuth's constructor is private
new SslClientAuth(0, 'NONE');
// @ts-expect-error RecordMetadata.internalCreate is the library's own, not declared
RecordMetadata.internalCreate(fields);
// @ts-expect-error KafkaFuture.internalCreate is the library's own, not declared
KafkaFuture.internalCreate(true);
// @ts-expect-error KafkaFuture's internalComplete is the library's own, not declared
(null as unknown as KafkaFuture<string>).internalComplete('v', null);
// @ts-expect-error ProducerRecord's internalToNative is the library's own, not declared
record.internalToNative();
// @ts-expect-error no underscore member is left either
record._toNative();

// The fields of a data object are read-only.
// @ts-expect-error topic is a readonly field
record.topic = 'other';
const delivered = null as unknown as RecordMetadata;
// @ts-expect-error offset is a readonly field
delivered.offset = 1;

// @ts-expect-error a ProducerRecord needs a topic
new ProducerRecord({ value: 'v' });
// @ts-expect-error a ProducerRecord needs a value, which every Java constructor takes
new ProducerRecord({ topic: 't' });
// @ts-expect-error no Java constructor takes a partition without a key
new ProducerRecord({ topic: 't', partition: 0, value: 'v' });
// @ts-expect-error no Java constructor takes a timestamp without a partition
new ProducerRecord({ topic: 't', timestamp: 5, key: null, value: 'v' });
// @ts-expect-error no Java constructor takes headers without a partition
new ProducerRecord({ topic: 't', key: null, value: 'v', headers });
// A variable is checked as well: a form's other keys are closed.
const loose = { topic: 't', partition: 0, value: 'v' };
// @ts-expect-error (topic, partition, value) is no Java constructor
new ProducerRecord(loose);
// @ts-expect-error ProducerRecord takes no bare value: Java has no one-parameter constructor
new ProducerRecord('t');
// A record's key and value are what the serializers take; an omitted one
// binds never, and a null one null (TypeScript infers null from a null
// argument), which every producer sends.
expectTypeOf(new ProducerRecord({ topic: 't', key: 1, value: 'v' })).toEqualTypeOf<ProducerRecord<number, string>>();
expectTypeOf(new ProducerRecord({ topic: 't', value: 'v' })).toEqualTypeOf<ProducerRecord<never, string>>();
expectTypeOf(new ProducerRecord({ topic: 't', key: null, value: null })).toEqualTypeOf<ProducerRecord<null, null>>();
// Covariant: a record of narrower types is a record of wider ones.
const wider: ProducerRecord<number | string, unknown> = new ProducerRecord({ topic: 't', key: 1, value: { a: 1 } });
void wider;
// @ts-expect-error a config value is a string, a number, a boolean, a bigint or an array of them
new KafkaProducer({ 'linger.ms': {} });
// @ts-expect-error a callback takes one object, ({ metadata, exception })
const badCallback: Callback = (metadata: string) => void metadata;
void badCallback;
// A callback may return a Promise, which the producer awaits.
const asyncCallback: Callback = async ({ exception }) => {
  if (exception !== null) await Promise.resolve(exception.message);
};
void asyncCallback;
// A callback may return any value, which the producer ignores (Java's void
// onCompletion takes an expression lambda such as (m, e) -> list.add(m)).
const seen: RecordMetadata[] = [];
const pushCallback: Callback = ({ metadata }) => seen.push(metadata);
const valueAsyncCallback: Callback = async () => 42;
void pushCallback;
void valueAsyncCallback;

// The private files are not entry points, and have no declarations.
// @ts-expect-error dist/internal/config.js is not exported
import('@confluentinc/kafka-javascript/dist/internal/config');
// @ts-expect-error dist/internal/errors.js is not exported
import('@confluentinc/kafka-javascript/dist/internal/errors');

// RecordMetadata and the error classes are checked by name, not by shape
// (root conventions, Stability): each has a private member of its own.
// @ts-expect-error an object literal with RecordMetadata's fields is no RecordMetadata
const literalMetadata: RecordMetadata = {
  topic: 't',
  partition: 0,
  offset: 0,
  hasOffset: true,
  timestamp: 0,
  hasTimestamp: true,
  serializedKeySize: -1,
  serializedValueSize: -1,
};
void literalMetadata;
// @ts-expect-error a plain Error with a KafkaError's members is no KafkaError
const literalError: KafkaError = Object.assign(new Error('x'), { cause: undefined });
void literalError;
// @ts-expect-error two error classes with the same public members are not interchangeable
const otherError: TopicAuthorizationError = new SaslAuthenticationError('x');
void otherError;

// The common value types: TopicPartition and Node are data classes (readonly
// fields), PartitionInfo keeps Java's methods and has no public constructor.
const tp = new TopicPartition({ topic: 't', partition: 0 });
expectTypeOf(tp.topic).toEqualTypeOf<string | null>();
expectTypeOf(tp.partition).toEqualTypeOf<number>();
expectTypeOf(tp.toJSON()).toEqualTypeOf<{ topic: string | null; partition: number }>();
// @ts-expect-error a TopicPartition's fields are readonly
tp.partition = 1;
// @ts-expect-error an object literal with TopicPartition's fields is no TopicPartition
const literalTp: TopicPartition = { topic: 't', partition: 0, toJSON: () => ({ topic: 't', partition: 0 }) };
void literalTp;
const node = new Node({ id: 1, host: 'h', port: 9092, rack: null });
expectTypeOf(node.rack).toEqualTypeOf<string | null>();
expectTypeOf(node.isEmpty).toEqualTypeOf<boolean>();
expectTypeOf(Node.noNode()).toEqualTypeOf<Node>();
// @ts-expect-error isFenced needs the rack, as Java's constructors do
void new Node({ id: 1, host: 'h', port: 9092, isFenced: true });
declare const info: PartitionInfo;
expectTypeOf(info.topic()).toEqualTypeOf<string>();
expectTypeOf(info.leader()).toEqualTypeOf<Node | null>();
// @ts-expect-error PartitionInfo has no public constructor yet
void new PartitionInfo({ topic: 't' });
expectTypeOf(KafkaProducer.NETWORK_THREAD_PREFIX).toEqualTypeOf<string>();
expectTypeOf(KafkaProducer.PRODUCER_METRIC_GROUP_NAME).toEqualTypeOf<string>();

// Serializers: the producer's type arguments are inferred from them, and an
// omitted (or null) serializer binds Uint8Array.
interface Order {
  id: number;
}
const stringSerializer: Serializer<string> = {
  serialize: ({ data }) => (data === null ? null : Buffer.from(data, 'utf8')),
};
const orderSerializer: Serializer<Order> = {
  // A serializer may return a Promise, which send() awaits.
  serialize: async ({ topic, headers, data }) => {
    expectTypeOf(topic).toEqualTypeOf<string>();
    expectTypeOf(headers).toEqualTypeOf<Headers>();
    expectTypeOf(data).toEqualTypeOf<Order | null>();
    return Promise.resolve(Buffer.from(JSON.stringify(data)));
  },
  close: () => undefined,
};
// @ts-expect-error a serializer returns bytes, not a string
const textSerializer: Serializer<string> = { serialize: ({ data }) => data };
void textSerializer;
async function serializers(): Promise<void> {
  const typed = new KafkaProducer({ configs, keySerializer: stringSerializer, valueSerializer: orderSerializer });
  expectTypeOf(typed).toEqualTypeOf<KafkaProducer<string, Order>>();
  await typed.send(new ProducerRecord({ topic: 't', key: 'k', value: { id: 1 } }));
  await typed.send({ record: new ProducerRecord({ topic: 't', key: null, value: null }), callback: null });
  // @ts-expect-error a number key for a string serializer
  await typed.send(new ProducerRecord({ topic: 't', key: 1, value: { id: 1 } }));
  const bytes = new KafkaProducer({ configs });
  expectTypeOf(bytes).toEqualTypeOf<KafkaProducer<Uint8Array, Uint8Array>>();
  expectTypeOf(new KafkaProducer(configs)).toEqualTypeOf<KafkaProducer<Uint8Array, Uint8Array>>();
  await bytes.send(new ProducerRecord({ topic: 't', key: Buffer.from('k'), value: null }));
  const half = new KafkaProducer({ configs, keySerializer: null, valueSerializer: stringSerializer });
  expectTypeOf(half).toEqualTypeOf<KafkaProducer<Uint8Array, string>>();
  // @ts-expect-error Java's forms take both serializers or neither
  void new KafkaProducer({ configs, keySerializer: stringSerializer });
  // @ts-expect-error a serializer is an object, not the function that makes one
  void new KafkaProducer({ configs, keySerializer: () => stringSerializer, valueSerializer: null });
  await Promise.all([typed.close(), bytes.close(), half.close()]);
}
void serializers;

// MockProducer: the producer methods return KafkaProducer's types, and its
// type arguments are inferred from the serializers, as KafkaProducer's.
async function mock(): Promise<void> {
  const typed = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: stringSerializer, valueSerializer: orderSerializer });
  expectTypeOf(typed).toEqualTypeOf<MockProducer<string, Order>>();
  expectTypeOf(new MockProducer()).toEqualTypeOf<MockProducer<Uint8Array, Uint8Array>>();
  const future = await typed.send(new ProducerRecord({ topic: 't', key: 'k', value: { id: 1 } }));
  expectTypeOf(future).toEqualTypeOf<KafkaFuture<RecordMetadata>>();
  // @ts-expect-error a number key for a string serializer
  await typed.send(new ProducerRecord({ topic: 't', key: 1, value: { id: 1 } }));
  expectTypeOf(typed.history()).toEqualTypeOf<ProducerRecord<string | null, Order | null>[]>();
  expectTypeOf(typed.completeNext()).toEqualTypeOf<boolean>();
  expectTypeOf(typed.errorNext(new Error('x'))).toEqualTypeOf<boolean>();
  expectTypeOf(typed.flushed()).toEqualTypeOf<boolean>();
  typed.sendException = new Error('injected');
  typed.sendException = null;
  // @ts-expect-error the constructor taking a Cluster is not translated
  void new MockProducer({ cluster: null, autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null });
  await typed.flush();
  await typed.close({ timeout: 0 });
}
void mock;
