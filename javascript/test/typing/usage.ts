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
  type Partitioner,
  type Producer,
  ProducerConfig,
  type ProducerConfigs,
  ProducerRecord,
  RecordMetadata,
} from '@confluentinc/kafka-javascript/producer';
import {
  KafkaError,
  KafkaFuture,
  type Metric,
  MetricName,
  MetricNameMap,
  Node,
  PartitionInfo,
  TopicPartition,
  TopicPartitionMap,
  TopicPartitionSet,
} from '@confluentinc/kafka-javascript/common';
import { KafkaMetric } from '@confluentinc/kafka-javascript/common/metrics';
import { CancellationError, ExecutionError, IllegalArgumentError, IllegalStateError } from '@confluentinc/kafka-javascript';
import {
  AuthenticationError,
  GroupAuthorizationError,
  RecordDeserializationError,
  RecordTooLargeError,
  RetriableError,
  SaslAuthenticationError,
  ThrottlingQuotaExceededError,
  TopicAuthorizationError,
} from '@confluentinc/kafka-javascript/common/errors';
import {
  LogTruncationError,
  NoOffsetForPartitionError,
  OffsetAndMetadata,
  OffsetOutOfRangeError,
} from '@confluentinc/kafka-javascript/consumer';
import { TimestampType } from '@confluentinc/kafka-javascript/common/record';
import { QuotaViolationError } from '@confluentinc/kafka-javascript/common/metrics';
import { ConfigError, SaslConfigs, SslClientAuth, SslConfigs } from '@confluentinc/kafka-javascript/common/config';
import { SecurityProtocol } from '@confluentinc/kafka-javascript/common/security/auth';
import { type Header, Headers, RecordHeader } from '@confluentinc/kafka-javascript/common/header';
import { type Serializer, stringSerializer } from '@confluentinc/kafka-javascript/common/serialization';

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
// The serde config keys take a synchronous function ({ configs, isKey }) => serializer.
const viaConfig: ProducerConfigs = {
  'bootstrap.servers': 'localhost:9092',
  'key.serializer': ({ configs: given, isKey }) => {
    expectTypeOf(given).toEqualTypeOf<Readonly<Record<string, unknown>>>();
    expectTypeOf(isKey).toEqualTypeOf<boolean>();
    return { serialize: () => null };
  },
  'value.serializer': null,
};
void viaConfig;
// @ts-expect-error a class name cannot be loaded in JavaScript
const byClassName: ProducerConfigs = { 'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer' };
void byClassName;
// @ts-expect-error the function returns the serializer synchronously, not a Promise
const asyncFactory: ProducerConfigs = { 'value.serializer': async () => ({ serialize: () => null }) };
void asyncFactory;
// @ts-expect-error a known key that is no serde key takes no function
const lingerFunction: ProducerConfigs = { 'linger.ms': () => ({ serialize: () => null }) };
void lingerFunction;

const headers: Header[] = [new RecordHeader({ key: 'h', value: Buffer.from('v') }), new RecordHeader({ key: 'n', value: null })];
// A producer without serializers is a KafkaProducer<Uint8Array, Uint8Array>:
// its records hold bytes.
const key = Buffer.from('k');
const record = new ProducerRecord({ topic: 't', partition: 0, timestamp: null, key, value: new Uint8Array(1), headers });
expectTypeOf(record).toExtend<ProducerRecord<Uint8Array, Uint8Array>>();
// One signature per Java constructor.
new ProducerRecord({ topic: 't', partition: 0, timestamp: 5, key: null, value: 'v' });
// A long takes a bigint too.
new ProducerRecord({ topic: 't', partition: 0, timestamp: 5n, key: null, value: 'v' });
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
const built = new Headers([new RecordHeader({ key: 'a', value: Buffer.from('1') })])
  .add(new RecordHeader({ key: 'b', value: null }))
  .add({ key: 'c', value: null });
expectTypeOf(built).toEqualTypeOf<Headers>();
new Headers({ headers: built });
new Headers({ headers: null });
new Headers();
new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: built });
// Where Java takes headers, a [key, value] pair is a header too, mixed with
// Header objects, a value bytes or null; a frozen pair included.
new Headers([['k', null], new RecordHeader({ key: 'h', value: null }), ['v', Buffer.from('1')], ['u', new Uint8Array(1)]]);
new Headers({ headers: [['k', null]] as const });
new ProducerRecord({
  topic: 't',
  partition: null,
  key: null,
  value: 'v',
  headers: [
    ['k', Buffer.from('1')],
    ['n', null],
  ],
});
new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: new Set([['k', null] as const]) });
// A Header the app implements is a Header, given by name to add(): a plain
// object given bare is the { key, value } options object.
const ownHeader: Header = { key: () => 'own', value: () => null };
new Headers([ownHeader]).add({ header: ownHeader });
new Headers().add({ header: { key: () => 'literal', value: () => null } });
// Given bare, only a RecordHeader: TypeScript cannot tell another Header from
// a plain object, which the run time reads as the options object.
new Headers().add(new RecordHeader({ key: 'r', value: null }));
// @ts-expect-error a plain object given bare is the { key, value } options object
new Headers().add({ key: () => 'literal', value: () => null });
// @ts-expect-error a Header of another class goes by name, add({ header })
new Headers().add(ownHeader);
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
// equals and hashCode stay methods (Java's), on the records and the headers.
expectTypeOf(record.equals(record)).toEqualTypeOf<boolean>();
expectTypeOf(record.equals({ o: null })).toEqualTypeOf<boolean>();
expectTypeOf(record.hashCode()).toEqualTypeOf<number>();
const recordHeader = new RecordHeader({ key: 'h', value: null });
expectTypeOf(recordHeader.equals(recordHeader)).toEqualTypeOf<boolean>();
expectTypeOf(recordHeader.hashCode()).toEqualTypeOf<number>();
expectTypeOf(recordHeader.value()).toEqualTypeOf<Buffer | null>();
expectTypeOf(recordHeader).toExtend<Header>();
expectTypeOf(built.hashCode()).toEqualTypeOf<number>();
// @ts-expect-error hashCode() only reads: it takes no argument
built.hashCode(1);
// @ts-expect-error key() only reads: it takes no argument
asArray[0].key(1);
// @ts-expect-error a Header's key is a method, not a field
const notAKeyField: string = asArray[0].key;
void notAKeyField;
void [last, byKey, headerValue, headerKey];
// @ts-expect-error a header value is bytes: encode a string first
new RecordHeader({ key: 'k', value: 'v' });
// @ts-expect-error a header key is a string
new RecordHeader({ key: 1, value: null });
// @ts-expect-error Header is an interface: RecordHeader builds a header
new Header({ key: 'k', value: null });
// @ts-expect-error a pair's value is bytes or null: encode a string first
new Headers([['k', 'v']]);
// @ts-expect-error a pair's key is a string
new Headers([[1, null]]);
// @ts-expect-error a pair has two elements
new Headers([['k', null, 'extra']]);
// @ts-expect-error add takes one Header, or the key and value by name, not a pair
built.add(['k', null]);
// @ts-expect-error a {key, value} object is not a Header
built.add({ header: { key: 'k', value: null } });
// A data object: its fields spread into a plain object of the same types.
const {
  timestamp: recordTimestamp,
  key: recordKey,
  value: recordValue,
}: { timestamp: bigint | null; key: Buffer | null; value: Uint8Array | null } = { ...record };
void [recordTimestamp, recordKey, recordValue];

const callback: Callback = ({ metadata, exception: error }: { metadata: RecordMetadata; exception: Error | null }) => {
  // Java's long: a bigint.
  const offset = metadata.offset;
  expectTypeOf(offset).toEqualTypeOf<bigint>();
  expectTypeOf(metadata.timestamp).toEqualTypeOf<bigint>();
  expectTypeOf(metadata.partition).toEqualTypeOf<number>();
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
  // Java's Producer interface, which KafkaProducer implements.
  const asInterface: Producer = kafkaProducer;
  expectTypeOf(asInterface.send).returns.toEqualTypeOf<Promise<KafkaFuture<RecordMetadata>>>();
  expectTypeOf(asInterface.flush).returns.toEqualTypeOf<Promise<void>>();
  // @ts-expect-error KafkaFuture is abstract, as Java's is
  void new KafkaFuture<number>();
  // KafkaFuture is checked by name, not by shape: an object with its
  // methods is no KafkaFuture.
  const real = KafkaFuture.completedFuture(1);
  const lookAlike = {
    toCompletionStage: () => real.toCompletionStage(),
    thenApply: real.thenApply.bind(real),
    whenComplete: real.whenComplete.bind(real),
    cancel: real.cancel.bind(real),
    get: real.get.bind(real),
    getNow: real.getNow.bind(real),
    isCancelled: () => real.isCancelled(),
    isCompletedExceptionally: () => real.isCompletedExceptionally(),
    isDone: () => real.isDone(),
    toString: () => real.toString(),
  };
  // @ts-expect-error a look-alike with KafkaFuture's methods is no KafkaFuture
  const fakeFuture: KafkaFuture<number> = lookAlike;
  void fakeFuture;
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
  const offset: KafkaFuture<bigint> = future.thenApply((m) => m.offset);
  const offsetToo: KafkaFuture<bigint> = future.thenApply({ function: (m: RecordMetadata) => m.offset });
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
  // By name, an unconstrained generic value type-checks.
  const wrapByName = <V>(x: V): KafkaFuture<V> => KafkaFuture.completedFuture({ value: x });
  expectTypeOf(wrapByName(1)).toEqualTypeOf<KafkaFuture<number>>();
  void [wrap, wrapByName];
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
  /** A class of the caller's own: its instance goes by name in TypeScript (plain JavaScript also takes it bare). */
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
  future.thenApply((a: RecordMetadata, b: bigint) => a.offset + b);
  // @ts-expect-error get's timeout is a Duration
  await future.get('5000');
  const unknown = RecordMetadata.UNKNOWN_PARTITION;
  expectTypeOf(unknown).toEqualTypeOf<-1>();
  void unknown;
  await kafkaProducer.flush();
  // @ts-expect-error flush takes no argument
  await kafkaProducer.flush(100);
  // partitionsFor: Java's List as an array, through a Promise.
  expectTypeOf(kafkaProducer.partitionsFor('t')).toEqualTypeOf<Promise<PartitionInfo[]>>();
  expectTypeOf(asInterface.partitionsFor({ topic: 't' })).toEqualTypeOf<Promise<PartitionInfo[]>>();
  // @ts-expect-error a topic is a string
  void kafkaProducer.partitionsFor(1);
  // metrics(): synchronous, Java's Map<MetricName, ? extends Metric>.
  const metrics = kafkaProducer.metrics();
  expectTypeOf(metrics).toEqualTypeOf<MetricNameMap<Metric>>();
  expectTypeOf(asInterface.metrics()).toEqualTypeOf<MetricNameMap<Metric>>();
  for (const [metricName, metric] of metrics) {
    expectTypeOf(metricName.tags()).toEqualTypeOf<Map<string, string>>();
    expectTypeOf(metric.metricValue()).toEqualTypeOf<unknown>();
  }
  const metricName = new MetricName({ name: 'n', group: 'g', description: '', tags: new Map([['client-id', 'c']]) });
  expectTypeOf(metrics.get(metricName)).toEqualTypeOf<Metric | null>();
  expectTypeOf(new MetricNameMap<number>()).toEqualTypeOf<MetricNameMap<number>>();
  const maybeEntries: Array<[MetricName, number]> | undefined = undefined as Array<[MetricName, number]> | undefined;
  // @ts-expect-error undefined is no entries: leave the argument out
  void new MetricNameMap<number>(maybeEntries);
  const kafkaMetric = new KafkaMetric({
    lock: null,
    metricName,
    valueProvider: { value: () => 1 },
    config: null,
    time: { milliseconds: () => 0 },
  });
  const asMetric: Metric = kafkaMetric;
  void asMetric;
  // @ts-expect-error metrics() takes no argument
  kafkaProducer.metrics(1);
  // TopicPartitionMap and TopicPartitionSet (js-only): keyed by value.
  const offsets = new TopicPartitionMap<bigint>([[new TopicPartition({ topic: 't', partition: 0 }), 5n]]);
  expectTypeOf(offsets.get(new TopicPartition({ topic: 't', partition: 0 }))).toEqualTypeOf<bigint | null>();
  expectTypeOf(offsets.set(new TopicPartition({ topic: 't', partition: 1 }), 6n)).toEqualTypeOf<TopicPartitionMap<bigint>>();
  for (const [tp, offset] of offsets) {
    expectTypeOf(tp).toEqualTypeOf<TopicPartition>();
    expectTypeOf(offset).toEqualTypeOf<bigint>();
  }
  // @ts-expect-error a TopicPartitionMap is keyed by TopicPartition only
  offsets.set('t-0', 1n);
  const partitions = new TopicPartitionSet([new TopicPartition({ topic: 't', partition: 0 })]);
  expectTypeOf(partitions.has(new TopicPartition({ topic: 't', partition: 0 }))).toEqualTypeOf<boolean>();
  expectTypeOf([...partitions]).toEqualTypeOf<TopicPartition[]>();
  // @ts-expect-error a TopicPartitionSet holds TopicPartitions only
  partitions.add({ topic: 't', partition: 0 });
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
// @ts-expect-error QuotaViolationException has no (String) constructor: its one form is { metric, value, bound }
new QuotaViolationError('m');
// metric() is a KafkaMetric, an error the core reports included: its name comes from the core.
declare const quota: QuotaViolationError;
expectTypeOf(quota.metric()).toEqualTypeOf<KafkaMetric>();
// The payload constructors: Java's other parameters by name, beside message and cause.
const unauthorized = new TopicAuthorizationError({ unauthorizedTopics: ['a', 'b'] });
expectTypeOf(unauthorized.unauthorizedTopics()).toEqualTypeOf<Set<string>>();
void new TopicAuthorizationError({ message: 'm', unauthorizedTopics: new Set(['a']) });
// @ts-expect-error a payload is given by name only, never on its own
new TopicAuthorizationError(['a']);
// @ts-expect-error unauthorizedTopics is an iterable of strings
new TopicAuthorizationError({ unauthorizedTopics: [1] });
// A long payload taken is a bigint or a number; returned, a TopicPartitionMap<bigint>.
const tooLarge = new RecordTooLargeError({
  message: 'too large',
  recordTooLargePartitions: new TopicPartitionMap<bigint | number>([[new TopicPartition({ topic: 't', partition: 0 }), 1]]),
});
expectTypeOf(tooLarge.recordTooLargePartitions()).toEqualTypeOf<TopicPartitionMap<bigint> | null>();
// @ts-expect-error only a TopicPartitionMap is taken for a Map<TopicPartition, Long>
new RecordTooLargeError({ message: 'm', recordTooLargePartitions: new Map<TopicPartition, bigint>() });
const outOfRange = new OffsetOutOfRangeError({ offsetOutOfRangePartitions: new TopicPartitionMap<bigint>() });
expectTypeOf(outOfRange.offsetOutOfRangePartitions()).toEqualTypeOf<TopicPartitionMap<bigint>>();
expectTypeOf(outOfRange.partitions()).toEqualTypeOf<TopicPartitionSet>();
expectTypeOf(
  new NoOffsetForPartitionError({ partition: new TopicPartition({ topic: 't', partition: 0 }) }).partitions(),
).toEqualTypeOf<TopicPartitionSet>();
expectTypeOf(GroupAuthorizationError.forGroupId('g').groupId()).toEqualTypeOf<string | null>();
expectTypeOf(GroupAuthorizationError.forGroupId({ groupId: 'g' })).toEqualTypeOf<GroupAuthorizationError>();
expectTypeOf(new ThrottlingQuotaExceededError({ throttleTimeMs: 5, message: 'm' }).throttleTimeMs()).toEqualTypeOf<number>();
void new ConfigError({ name: 'linger.ms', value: true });
void new ConfigError({ name: 'linger.ms', value: 'x', message: 'bad' });
// @ts-expect-error ConfigException has no (String name) constructor
new ConfigError({ name: 'linger.ms' });
// @ts-expect-error a getter only reads
unauthorized.unauthorizedTopics(1);
// @ts-expect-error LogTruncationException has no (String) constructor
new LogTruncationError({ message: 'm' });
const truncated = new LogTruncationError({
  fetchOffsets: new TopicPartitionMap<bigint>(),
  divergentOffsets: new TopicPartitionMap([[new TopicPartition({ topic: 't', partition: 0 }), new OffsetAndMetadata(5n)]]),
});
expectTypeOf(truncated.divergentOffsets()).toEqualTypeOf<TopicPartitionMap<OffsetAndMetadata>>();
const offsetAndMetadata = new OffsetAndMetadata({ offset: 1, leaderEpoch: null, metadata: 'm' });
expectTypeOf(offsetAndMetadata.offset).toEqualTypeOf<bigint>();
expectTypeOf(offsetAndMetadata.leaderEpoch).toEqualTypeOf<number | null>();
// @ts-expect-error OffsetAndMetadata has no (offset, leaderEpoch) constructor
new OffsetAndMetadata({ offset: 1, leaderEpoch: 2 });
const deserialization = new RecordDeserializationError({
  origin: RecordDeserializationError.DeserializationExceptionOrigin.VALUE,
  partition: new TopicPartition({ topic: 't', partition: 0 }),
  offset: 1n,
  timestamp: -1,
  timestampType: TimestampType.CREATE_TIME,
  keyBuffer: null,
  valueBuffer: Buffer.from('v'),
  headers: [],
  message: 'failed',
  cause: null,
});
expectTypeOf(deserialization.origin()).toEqualTypeOf<RecordDeserializationError.DeserializationExceptionOrigin>();
expectTypeOf(deserialization.timestampType()).toEqualTypeOf<TimestampType>();
expectTypeOf(deserialization.keyBuffer()).toEqualTypeOf<Buffer | null>();
expectTypeOf(deserialization.offset()).toEqualTypeOf<bigint>();
expectTypeOf(deserialization.headers()).toEqualTypeOf<Headers>();
// Its headers, where Java takes Headers: a Headers, kept as it is, or Header
// objects and [key, value] pairs, mixed.
const deserializationArgs = {
  origin: RecordDeserializationError.DeserializationExceptionOrigin.KEY,
  partition: new TopicPartition({ topic: 't', partition: 0 }),
  offset: 1n,
  timestamp: -1,
  timestampType: TimestampType.CREATE_TIME,
  keyBuffer: Buffer.from('k'),
  valueBuffer: null,
  message: 'failed',
  cause: null,
};
void new RecordDeserializationError({ ...deserializationArgs, headers: built });
void new RecordDeserializationError({
  ...deserializationArgs,
  headers: [['k', null], new RecordHeader({ key: 'h', value: null }), ['v', Buffer.from('1')]],
});
// @ts-expect-error a pair's value is bytes or null: encode a string first
void new RecordDeserializationError({ ...deserializationArgs, headers: [['k', 'v']] });
expectTypeOf(TimestampType.forName('CreateTime')).toEqualTypeOf<TimestampType>();
const nullPartition = new TopicPartitionSet([null]);
expectTypeOf(nullPartition.has(null)).toEqualTypeOf<boolean>();
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
/** A caller's subclass of an abstract error base. */
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
const fields = {
  topic: 't',
  partition: 0,
  hasOffset: true,
  offset: 0n,
  hasTimestamp: true,
  timestamp: 0n,
  serializedKeySize: -1,
  serializedValueSize: -1,
};
// @ts-expect-error RecordMetadata's public constructor is Java's, not the fields the client reads
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
// @ts-expect-error Headers' setReadOnly is the library's own, not declared
built.setReadOnly();
// @ts-expect-error Headers' isReadOnly is the library's own, not declared
built.isReadOnly();
// @ts-expect-error Headers' internalToNative is the library's own, not declared
built.internalToNative();
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
// A record's key and value are what the serializers take; an omitted or a
// null one binds never, through a signature per null combination, so every
// producer sends it.
expectTypeOf(new ProducerRecord({ topic: 't', key: 1, value: 'v' })).toEqualTypeOf<ProducerRecord<number, string>>();
expectTypeOf(new ProducerRecord({ topic: 't', value: 'v' })).toEqualTypeOf<ProducerRecord<never, string>>();
expectTypeOf(new ProducerRecord({ topic: 't', value: null })).toEqualTypeOf<ProducerRecord<never, never>>();
expectTypeOf(new ProducerRecord({ topic: 't', key: null, value: null })).toEqualTypeOf<ProducerRecord<never, never>>();
expectTypeOf(new ProducerRecord({ topic: 't', key: null, value: 'v' })).toEqualTypeOf<ProducerRecord<never, string>>();
expectTypeOf(new ProducerRecord({ topic: 't', key: 1, value: null })).toEqualTypeOf<ProducerRecord<number, never>>();
expectTypeOf(new ProducerRecord({ topic: 't', partition: 0, key: null, value: null })).toEqualTypeOf<ProducerRecord<never, never>>();
expectTypeOf(new ProducerRecord({ topic: 't', partition: 0, timestamp: 1, key: null, value: 'v' })).toEqualTypeOf<
  ProducerRecord<never, string>
>();
expectTypeOf(new ProducerRecord({ topic: 't', partition: 0, key: 1, value: null, headers: null })).toEqualTypeOf<
  ProducerRecord<number, never>
>();
expectTypeOf(new ProducerRecord({ topic: 't', partition: 0, timestamp: 1, key: null, value: null, headers: null })).toEqualTypeOf<
  ProducerRecord<never, never>
>();
// A key or value that may be null binds its type without null.
declare const maybeKey: string | null;
expectTypeOf(new ProducerRecord({ topic: 't', key: maybeKey, value: maybeKey })).toEqualTypeOf<ProducerRecord<string, string>>();
// send() takes the ProducerRecord<K, V> of the producer's serializers: a null
// key or value fits any producer.
async function nullRecords(typedProducer: KafkaProducer<string, string>): Promise<void> {
  await typedProducer.send(new ProducerRecord({ topic: 't', key: null, value: null }));
  await typedProducer.send({ record: new ProducerRecord({ topic: 't', key: null, value: 'v' }), callback: null });
  // @ts-expect-error a number key goes to no producer of string keys
  await typedProducer.send(new ProducerRecord({ topic: 't', key: 1, value: 'v' }));
}
void nullRecords;
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
// (JS conventions, Stability): each has a private member of its own.
// @ts-expect-error an object literal with RecordMetadata's fields is no RecordMetadata
const literalMetadata: RecordMetadata = {
  topic: 't',
  partition: 0,
  offset: 0n,
  hasOffset: true,
  timestamp: 0n,
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
// fields), PartitionInfo keeps Java's methods.
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
expectTypeOf(info.topic()).toEqualTypeOf<string | null>();
expectTypeOf(info.leader()).toEqualTypeOf<Node | null>();
// @ts-expect-error Java's constructors take the leader and the replicas too
void new PartitionInfo({ topic: 't' });
// A Java Node[] taken is any iterable of Node; one returned an array.
const builtInfo = new PartitionInfo({ topic: 't', partition: 0, leader: node, replicas: new Set([node]), inSyncReplicas: [node] });
expectTypeOf(builtInfo.replicas()).toEqualTypeOf<Node[] | null>();
expectTypeOf(builtInfo.equals(info)).toEqualTypeOf<boolean>();
expectTypeOf(builtInfo.hashCode()).toEqualTypeOf<number>();
expectTypeOf(tp.equals({ obj: tp })).toEqualTypeOf<boolean>();
expectTypeOf(node.hashCode()).toEqualTypeOf<number>();
// RecordMetadata's public constructor is Java's: a long takes a bigint or a number.
const builtMetadata = new RecordMetadata({
  topicPartition: tp,
  baseOffset: 15n,
  batchIndex: 3,
  timestamp: 1,
  serializedKeySize: -1,
  serializedValueSize: 5,
});
expectTypeOf(builtMetadata.offset).toEqualTypeOf<bigint>();
expectTypeOf(KafkaProducer.NETWORK_THREAD_PREFIX).toEqualTypeOf<string>();
expectTypeOf(KafkaProducer.PRODUCER_METRIC_GROUP_NAME).toEqualTypeOf<string>();

// Serializers: the producer's type arguments are inferred from them, and an
// omitted (or null) serializer binds Uint8Array.
/** A value of the caller's own type, which a serializer of its own writes. */
interface Order {
  id: number;
}
// The built-in factory: called as a constructor argument, or without () in a
// serde config key, its configure(configs, isKey) form.
expectTypeOf(stringSerializer()).toEqualTypeOf<Serializer<string>>();
expectTypeOf(stringSerializer({ configs: { 'serializer.encoding': 'UTF-8' }, isKey: true })).toEqualTypeOf<Serializer<string>>();
// @ts-expect-error configure takes the configuration and isKey together
stringSerializer({ configs: {} });
const viaFactory: ProducerConfigs = { 'key.serializer': stringSerializer, 'value.serializer': stringSerializer };
void viaFactory;
// A configuration typed as ProducerConfigs has lost its functions' types:
// the producer is typed as bytes.
expectTypeOf(new KafkaProducer(viaFactory)).toEqualTypeOf<KafkaProducer<Uint8Array, Uint8Array>>();
// The config keys bind the type their functions return, as arguments do
// (JS conventions, Class family): the README's "the same through the configuration".
expectTypeOf(new KafkaProducer({ ...configs, 'key.serializer': stringSerializer, 'value.serializer': stringSerializer })).toEqualTypeOf<
  KafkaProducer<string, string>
>();
expectTypeOf(new KafkaProducer({ configs: { ...configs, 'value.serializer': stringSerializer } })).toEqualTypeOf<
  KafkaProducer<Uint8Array, string>
>();
expectTypeOf(new KafkaProducer({ ...configs, 'key.serializer': null })).toEqualTypeOf<KafkaProducer<Uint8Array, Uint8Array>>();
expectTypeOf(
  new KafkaProducer({ configs: { ...configs, 'key.serializer': stringSerializer }, keySerializer: null, valueSerializer: null }),
).toEqualTypeOf<KafkaProducer<string, Uint8Array>>();
async function serializersFromTheConfiguration(): Promise<void> {
  const same = new KafkaProducer({ ...configs, 'key.serializer': stringSerializer, 'value.serializer': stringSerializer });
  await same.send(new ProducerRecord({ topic: 't', key: 'k', value: 'v' }));
  // @ts-expect-error a string serializer takes no number
  await same.send(new ProducerRecord({ topic: 't', key: 1, value: 'v' }));
  await same.close();
}
void serializersFromTheConfiguration;
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
  const typed = new KafkaProducer({ configs, keySerializer: stringSerializer(), valueSerializer: orderSerializer });
  expectTypeOf(typed).toEqualTypeOf<KafkaProducer<string, Order>>();
  await typed.send(new ProducerRecord({ topic: 't', key: 'k', value: { id: 1 } }));
  await typed.send({ record: new ProducerRecord({ topic: 't', key: null, value: null }), callback: null });
  // @ts-expect-error a number key for a string serializer
  await typed.send(new ProducerRecord({ topic: 't', key: 1, value: { id: 1 } }));
  const bytes = new KafkaProducer({ configs });
  expectTypeOf(bytes).toEqualTypeOf<KafkaProducer<Uint8Array, Uint8Array>>();
  expectTypeOf(new KafkaProducer(configs)).toEqualTypeOf<KafkaProducer<Uint8Array, Uint8Array>>();
  await bytes.send(new ProducerRecord({ topic: 't', key: Buffer.from('k'), value: null }));
  const half = new KafkaProducer({ configs, keySerializer: null, valueSerializer: stringSerializer() });
  expectTypeOf(half).toEqualTypeOf<KafkaProducer<Uint8Array, string>>();
  // @ts-expect-error Java's forms take both serializers or neither
  void new KafkaProducer({ configs, keySerializer: stringSerializer() });
  // @ts-expect-error a constructor argument is the serializer, not the factory (did you forget '()'?)
  void new KafkaProducer({ configs, keySerializer: stringSerializer, valueSerializer: null });
  // @ts-expect-error a serializer is an object, not the function that makes one
  void new KafkaProducer({ configs, keySerializer: () => stringSerializer(), valueSerializer: null });
  await Promise.all([typed.close(), bytes.close(), half.close()]);
}
void serializers;

// MockProducer: Java's Producer, over the FFI's mock. Its type arguments are
// inferred from its serializers; a mock built without them is the one place
// a type argument is written.
async function mocks(): Promise<void> {
  const typed = new MockProducer({
    autoComplete: true,
    partitioner: null,
    keySerializer: stringSerializer(),
    valueSerializer: orderSerializer,
  });
  expectTypeOf(typed).toEqualTypeOf<MockProducer<string, Order>>();
  const asInterface: Producer<string, Order> = typed;
  expectTypeOf(asInterface.send).returns.toEqualTypeOf<Promise<KafkaFuture<RecordMetadata>>>();
  await typed.send(new ProducerRecord({ topic: 't', key: 'k', value: { id: 1 } }));
  // @ts-expect-error a number key for a string serializer
  await typed.send(new ProducerRecord({ topic: 't', key: 1, value: { id: 1 } }));
  expectTypeOf(typed.history()).toEqualTypeOf<Array<ProducerRecord<string, Order>>>();
  expectTypeOf(typed.completeNext()).toEqualTypeOf<boolean>();
  expectTypeOf(typed.errorNext(new Error('e'))).toEqualTypeOf<boolean>();
  expectTypeOf(typed.errorNext({ e: null })).toEqualTypeOf<boolean>();
  expectTypeOf(typed.closed()).toEqualTypeOf<boolean>();
  expectTypeOf(typed.flushed()).toEqualTypeOf<boolean>();
  typed.setSendException(new KafkaError('send'));
  typed.setFlushException({ flushException: null });
  typed.setPartitionsForException(null);
  typed.setCloseException({ closeException: new Error('close') });
  // @ts-expect-error an error to throw is an Error
  typed.setSendException('boom');
  typed.clear();
  // Java's MockProducer() binds nothing: the type arguments are written.
  const untyped: MockProducer<string, string> = new MockProducer<string, string>();
  await untyped.send(new ProducerRecord({ topic: 't', key: 'k', value: 'v' }));
  // The partitioner is Java's untranslated Partitioner, a placeholder.
  const partitioner: Partitioner = { partition: () => 0 };
  void new MockProducer({ autoComplete: false, partitioner, keySerializer: null, valueSerializer: null });
  // @ts-expect-error Java's forms take all four parameters or none
  void new MockProducer({ autoComplete: true });
  // @ts-expect-error a mock's cluster is always empty: Java's Cluster form is left out
  void new MockProducer({ cluster: null, autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null });
  await using disposed = new MockProducer<Uint8Array, Uint8Array>();
  void disposed;
  await Promise.all([typed.close(), untyped.close()]);
}
void mocks;

// The admin client: Admin.create, the RPCs' Results and their futures, the
// option classes and the mock.
async function adminClient(): Promise<void> {
  const { Admin, DescribeClusterOptions, KafkaAdminClient, ListTopicsOptions, MockAdminClient } =
    await import('@confluentinc/kafka-javascript/admin');
  const { AclOperation } = await import('@confluentinc/kafka-javascript/common/acl');
  const { Node, Uuid } = await import('@confluentinc/kafka-javascript/common');
  type AdminType = import('@confluentinc/kafka-javascript/admin').Admin;
  type KafkaFutureOf<T> = import('@confluentinc/kafka-javascript/common').KafkaFuture<T>;
  type AclOperationType = import('@confluentinc/kafka-javascript/common/acl').AclOperation;
  const admin = Admin.create({ 'bootstrap.servers': 'localhost:9092', 'request.timeout.ms': 1000 });
  expectTypeOf(admin).toEqualTypeOf<AdminType>();
  void Admin.create({ configs: { 'bootstrap.servers': 'localhost:9092' } });
  const cluster = admin.describeCluster(new DescribeClusterOptions({ includeAuthorizedOperations: true }));
  expectTypeOf(cluster.nodes()).toEqualTypeOf<KafkaFutureOf<InstanceType<typeof Node>[]>>();
  expectTypeOf(cluster.controller()).toEqualTypeOf<KafkaFutureOf<InstanceType<typeof Node> | null>>();
  expectTypeOf(cluster.clusterId()).toEqualTypeOf<KafkaFutureOf<string>>();
  expectTypeOf(cluster.authorizedOperations()).toEqualTypeOf<KafkaFutureOf<Set<AclOperationType> | null>>();
  const topics = admin.listTopics({ options: new ListTopicsOptions().listInternal(true) });
  expectTypeOf(topics.names()).toEqualTypeOf<KafkaFutureOf<Set<string>>>();
  expectTypeOf(await topics.namesToListings().get()).toExtend<Map<string, unknown>>();
  // @ts-expect-error the options are an instance, not a plain object
  admin.listTopics({ options: { listInternal: true } });
  // @ts-expect-error an RPC takes the options of its own class
  admin.describeCluster(new ListTopicsOptions());
  const options = new DescribeClusterOptions({ timeoutMs: 100 });
  expectTypeOf(options.timeoutMs()).toEqualTypeOf<number | null>();
  expectTypeOf(options.timeoutMs(5)).toEqualTypeOf<InstanceType<typeof DescribeClusterOptions>>();
  expectTypeOf(options.includeFencedBrokers()).toEqualTypeOf<boolean>();
  // @ts-expect-error listInternal only sets; shouldListInternal reads
  new ListTopicsOptions().listInternal();
  // @ts-expect-error an unknown key of the keyed constructor
  void new DescribeClusterOptions({ validateOnly: true });
  // @ts-expect-error KafkaAdminClient's constructor is private: Admin.create builds it
  void new KafkaAdminClient();
  await admin.close({ timeout: 1000 });
  await admin.close();
  const mock = MockAdminClient.create().numBrokers(3).controller(1).clusterId('c').build();
  expectTypeOf(mock).toEqualTypeOf<InstanceType<typeof MockAdminClient>>();
  mock.controller(new Node({ id: 0, host: 'localhost', port: 1000 }));
  mock.timeoutNextRequest(1);
  void new MockAdminClient.Builder({ brokers: [new Node({ id: 0, host: 'h', port: 1 })], controller: 0 });
  await using disposed = new MockAdminClient();
  void disposed;
  const id = Uuid.randomUuid();
  expectTypeOf(id.getMostSignificantBits()).toEqualTypeOf<bigint>();
  expectTypeOf(Uuid.fromString(id.toString())).toEqualTypeOf<InstanceType<typeof Uuid>>();
  expectTypeOf(AclOperation.fromCode(3)).toEqualTypeOf<AclOperationType>();
  await mock.close();
}
void adminClient;

async function adminTopics(): Promise<void> {
  const { CreateTopicsOptions, DescribeTopicsOptions, MockAdminClient, NewPartitions, NewTopic, RecordsToDelete } =
    await import('@confluentinc/kafka-javascript/admin');
  const { TopicCollection, TopicPartition, TopicPartitionMap, Uuid, UuidMap } = await import('@confluentinc/kafka-javascript/common');
  type KafkaFutureOf<T> = import('@confluentinc/kafka-javascript/common').KafkaFuture<T>;
  type TopicDescriptionType = import('@confluentinc/kafka-javascript/admin').TopicDescription;
  type DeletedRecordsType = import('@confluentinc/kafka-javascript/admin').DeletedRecords;
  type UuidType = import('@confluentinc/kafka-javascript/common').Uuid;
  const admin = new MockAdminClient();
  const created = admin.createTopics({
    newTopics: [new NewTopic({ name: 't', numPartitions: 1, replicationFactor: 1 }).configs({ 'cleanup.policy': 'compact' })],
    options: new CreateTopicsOptions({ validateOnly: true }),
  });
  expectTypeOf(created.all()).toEqualTypeOf<KafkaFutureOf<void>>();
  expectTypeOf(created.values()).toEqualTypeOf<Map<string, KafkaFutureOf<void>>>();
  expectTypeOf(created.numPartitions('t')).toEqualTypeOf<KafkaFutureOf<number>>();
  void new NewTopic({ name: 't', numPartitions: null, replicationFactor: null });
  void new NewTopic({ name: 't', replicasAssignments: new Map([[0, [1, 2]]]) });
  // @ts-expect-error the two forms' keys mixed
  void new NewTopic({ name: 't', numPartitions: 1, replicationFactor: 1, replicasAssignments: new Map() });
  const byName = admin.describeTopics(['t']);
  expectTypeOf(byName.allTopicNames()).toEqualTypeOf<KafkaFutureOf<Map<string, TopicDescriptionType>> | null>();
  const byId = admin.describeTopics({ topics: TopicCollection.ofTopicIds([Uuid.randomUuid()]), options: new DescribeTopicsOptions() });
  expectTypeOf(byId.topicIdValues()).toEqualTypeOf<InstanceType<typeof UuidMap<KafkaFutureOf<TopicDescriptionType>>> | null>();
  // @ts-expect-error describeTopics takes the names or a TopicCollection, never both
  admin.describeTopics({ topicNames: ['t'], topics: TopicCollection.ofTopicNames(['t']) });
  void admin.deleteTopics(['t']);
  void admin.deleteTopics({ topics: TopicCollection.ofTopicIds([] as UuidType[]) });
  void admin.createPartitions({ newPartitions: { t: NewPartitions.increaseTo(2) } });
  void admin.createPartitions(new Map([['t', NewPartitions.increaseTo({ totalCount: 3, newAssignments: [[1], [2]] })]]));
  // @ts-expect-error a plain object given alone is the options object: the map goes under newPartitions
  admin.createPartitions({ t: NewPartitions.increaseTo(2) });
  const deleted = admin.deleteRecords(
    new TopicPartitionMap([[new TopicPartition({ topic: 't', partition: 0 }), RecordsToDelete.beforeOffset(5n)]]),
  );
  expectTypeOf(deleted.lowWatermarks()).toEqualTypeOf<InstanceType<typeof TopicPartitionMap<KafkaFutureOf<DeletedRecordsType>>>>();
  // @ts-expect-error deleteRecords takes a TopicPartitionMap
  admin.deleteRecords(new Map());
  await admin.close();
}
void adminTopics;

// The config, log-dir, leader, reassignment, offset and group calls: each
// Result accessor's type, and each RPC's forms by name.
async function adminConfigsOffsetsGroups(): Promise<void> {
  const admin = await import('@confluentinc/kafka-javascript/admin');
  const common = await import('@confluentinc/kafka-javascript/common');
  const { ConfigResource, ConfigResourceMap } = await import('@confluentinc/kafka-javascript/common/config');
  const { OffsetAndMetadata } = await import('@confluentinc/kafka-javascript/consumer');
  type KafkaFutureOf<T> = import('@confluentinc/kafka-javascript/common').KafkaFuture<T>;
  type TopicPartitionMapOf<V> = import('@confluentinc/kafka-javascript/common').TopicPartitionMap<V>;
  type TopicPartitionReplicaMapOf<V> = import('@confluentinc/kafka-javascript/common').TopicPartitionReplicaMap<V>;
  type ConfigResourceMapOf<V> = import('@confluentinc/kafka-javascript/common/config').ConfigResourceMap<V>;
  type ConfigType = import('@confluentinc/kafka-javascript/admin').Config;
  type ConfigResourceType = import('@confluentinc/kafka-javascript/common/config').ConfigResource;
  type LogDirDescriptionType = import('@confluentinc/kafka-javascript/admin').LogDirDescription;
  type ReplicaLogDirInfoType = import('@confluentinc/kafka-javascript/admin').DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
  type PartitionReassignmentType = import('@confluentinc/kafka-javascript/admin').PartitionReassignment;
  type ListOffsetsResultInfoType = import('@confluentinc/kafka-javascript/admin').ListOffsetsResult.ListOffsetsResultInfo;
  type GroupListingType = import('@confluentinc/kafka-javascript/admin').GroupListing;
  type ConsumerGroupDescriptionType = import('@confluentinc/kafka-javascript/admin').ConsumerGroupDescription;
  type ClassicGroupDescriptionType = import('@confluentinc/kafka-javascript/admin').ClassicGroupDescription;
  type OffsetAndMetadataType = import('@confluentinc/kafka-javascript/consumer').OffsetAndMetadata;
  const mock = new admin.MockAdminClient();
  const tp = new common.TopicPartition({ topic: 't', partition: 0 });
  const resource = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: 't' });
  const replica = new common.TopicPartitionReplica({ topic: 't', partition: 0, brokerId: 0 });

  // Configs and log dirs.
  const configs = mock.describeConfigs({ resources: [resource] });
  expectTypeOf(configs.values()).toEqualTypeOf<ConfigResourceMapOf<KafkaFutureOf<ConfigType>>>();
  expectTypeOf(configs.all()).toEqualTypeOf<KafkaFutureOf<ConfigResourceMapOf<ConfigType>>>();
  const ops = new ConfigResourceMap([
    [
      resource,
      [
        new admin.AlterConfigOp({
          configEntry: new admin.ConfigEntry({ name: 'a', value: 'b' }),
          operationType: admin.AlterConfigOp.OpType.SET,
        }),
      ],
    ],
  ]);
  const altered = mock.incrementalAlterConfigs({ configs: ops, options: new admin.AlterConfigsOptions({ validateOnly: true }) });
  expectTypeOf(altered.values()).toEqualTypeOf<ConfigResourceMapOf<KafkaFutureOf<void>>>();
  expectTypeOf(altered.all()).toEqualTypeOf<KafkaFutureOf<void>>();
  expectTypeOf(mock.listConfigResources().all()).toEqualTypeOf<KafkaFutureOf<ConfigResourceType[]>>();
  const logDirs = mock.describeLogDirs({ brokers: [0] });
  expectTypeOf(logDirs.descriptions()).toEqualTypeOf<Map<number, KafkaFutureOf<Map<string, LogDirDescriptionType>>>>();
  expectTypeOf(logDirs.allDescriptions()).toEqualTypeOf<KafkaFutureOf<Map<number, Map<string, LogDirDescriptionType>>>>();
  const moved = mock.alterReplicaLogDirs({ replicaAssignment: new common.TopicPartitionReplicaMap([[replica, '/tmp']]) });
  expectTypeOf(moved.values()).toEqualTypeOf<TopicPartitionReplicaMapOf<KafkaFutureOf<void>>>();
  const replicaDirs = mock.describeReplicaLogDirs({ replicas: [replica] });
  expectTypeOf(replicaDirs.values()).toEqualTypeOf<TopicPartitionReplicaMapOf<KafkaFutureOf<ReplicaLogDirInfoType>>>();
  expectTypeOf(replicaDirs.all()).toEqualTypeOf<KafkaFutureOf<TopicPartitionReplicaMapOf<ReplicaLogDirInfoType>>>();

  // Leaders, reassignments and offsets.
  const elected = mock.electLeaders({ electionType: common.ElectionType.PREFERRED, partitions: null });
  expectTypeOf(elected.partitions()).toEqualTypeOf<KafkaFutureOf<TopicPartitionMapOf<unknown>>>();
  expectTypeOf(elected.all()).toEqualTypeOf<KafkaFutureOf<void>>();
  const reassigned = mock.alterPartitionReassignments({
    reassignments: new common.TopicPartitionMap([[tp, new admin.NewPartitionReassignment([0])]]),
  });
  expectTypeOf(reassigned.values()).toEqualTypeOf<TopicPartitionMapOf<KafkaFutureOf<void>>>();
  const listed = mock.listPartitionReassignments({ options: new admin.ListPartitionReassignmentsOptions() });
  expectTypeOf(listed.reassignments()).toEqualTypeOf<KafkaFutureOf<TopicPartitionMapOf<PartitionReassignmentType>>>();
  void mock.listPartitionReassignments({ partitions: [tp] });
  void mock.listPartitionReassignments({ partitions: null, options: new admin.ListPartitionReassignmentsOptions() });
  const offsets = mock.listOffsets({ topicPartitionOffsets: new common.TopicPartitionMap([[tp, admin.OffsetSpec.latest()]]) });
  expectTypeOf(offsets.partitionResult(tp)).toEqualTypeOf<KafkaFutureOf<ListOffsetsResultInfoType>>();
  expectTypeOf(offsets.all()).toEqualTypeOf<KafkaFutureOf<TopicPartitionMapOf<ListOffsetsResultInfoType>>>();
  mock.updateBeginningOffsets({ newOffsets: new common.TopicPartitionMap([[tp, 0n]]) });

  // Groups.
  const groups = mock.listGroups({ options: admin.ListGroupsOptions.forConsumerGroups() });
  expectTypeOf(groups.all()).toEqualTypeOf<KafkaFutureOf<GroupListingType[]>>();
  expectTypeOf(groups.errors()).toEqualTypeOf<KafkaFutureOf<unknown[]>>();
  const described = mock.describeConsumerGroups({ groupIds: ['g'] });
  expectTypeOf(described.describedGroups()).toEqualTypeOf<Map<string, KafkaFutureOf<ConsumerGroupDescriptionType>>>();
  expectTypeOf(described.all()).toEqualTypeOf<KafkaFutureOf<Map<string, ConsumerGroupDescriptionType>>>();
  const classic = mock.describeClassicGroups({ groupIds: ['g'] });
  expectTypeOf(classic.all()).toEqualTypeOf<KafkaFutureOf<Map<string, ClassicGroupDescriptionType>>>();
  const groupOffsets = mock.listConsumerGroupOffsets({ groupId: 'g' });
  expectTypeOf(groupOffsets.partitionsToOffsetAndMetadata()).toEqualTypeOf<
    KafkaFutureOf<TopicPartitionMapOf<OffsetAndMetadataType | null>>
  >();
  expectTypeOf(groupOffsets.all()).toEqualTypeOf<KafkaFutureOf<Map<string, TopicPartitionMapOf<OffsetAndMetadataType | null>>>>();
  void mock.listConsumerGroupOffsets({ groupSpecs: { g: new admin.ListConsumerGroupOffsetsSpec() } });
  const committed = mock.alterConsumerGroupOffsets({
    groupId: 'g',
    offsets: new common.TopicPartitionMap([[tp, new OffsetAndMetadata(1n)]]),
  });
  expectTypeOf(committed.partitionResult(tp)).toEqualTypeOf<KafkaFutureOf<void>>();
  expectTypeOf(mock.deleteConsumerGroupOffsets({ groupId: 'g', partitions: [tp] }).all()).toEqualTypeOf<KafkaFutureOf<void>>();
  expectTypeOf(mock.deleteConsumerGroups({ groupIds: ['g'] }).deletedGroups()).toEqualTypeOf<Map<string, KafkaFutureOf<void>>>();
  const removed = mock.removeMembersFromConsumerGroup({
    groupId: 'g',
    options: new admin.RemoveMembersFromConsumerGroupOptions([new admin.MemberToRemove('i')]),
  });
  expectTypeOf(removed.memberResult(new admin.MemberToRemove('i'))).toEqualTypeOf<KafkaFutureOf<void>>();
  // @ts-expect-error removeMembersFromConsumerGroup has only Java's two-parameter form
  mock.removeMembersFromConsumerGroup({ groupId: 'g' });
  // @ts-expect-error listOffsets takes a TopicPartitionMap
  mock.listOffsets(new Map());
  await mock.close();
}
void adminConfigsOffsetsGroups;

// R3, the security calls: each Result accessor's type, and each RPC's forms
// by name and bare.
async function adminSecurity(): Promise<void> {
  const admin = await import('@confluentinc/kafka-javascript/admin');
  const acl = await import('@confluentinc/kafka-javascript/common/acl');
  const res = await import('@confluentinc/kafka-javascript/common/resource');
  type KafkaFutureOf<T> = import('@confluentinc/kafka-javascript/common').KafkaFuture<T>;
  type AclBindingType = import('@confluentinc/kafka-javascript/common/acl').AclBinding;
  type AclBindingMapOf<V> = import('@confluentinc/kafka-javascript/common/acl').AclBindingMap<V>;
  type AclBindingFilterMapOf<V> = import('@confluentinc/kafka-javascript/common/acl').AclBindingFilterMap<V>;
  type FilterResultsType = import('@confluentinc/kafka-javascript/admin').DeleteAclsResult.FilterResults;
  type FilterResultType = import('@confluentinc/kafka-javascript/admin').DeleteAclsResult.FilterResult;
  type ApiErrorType = import('@confluentinc/kafka-javascript/common/errors').ApiError;
  const mock = new admin.MockAdminClient();
  const binding = new acl.AclBinding({
    pattern: new res.ResourcePattern({ resourceType: res.ResourceType.TOPIC, name: 't', patternType: res.PatternType.LITERAL }),
    entry: new acl.AccessControlEntry({
      principal: 'User:a',
      host: '*',
      operation: acl.AclOperation.READ,
      permissionType: acl.AclPermissionType.ALLOW,
    }),
  });

  const created = mock.createAcls([binding]);
  expectTypeOf(created.values()).toEqualTypeOf<AclBindingMapOf<KafkaFutureOf<void>>>();
  expectTypeOf(created.all()).toEqualTypeOf<KafkaFutureOf<void>>();
  void mock.createAcls({ acls: new Set([binding]), options: new admin.CreateAclsOptions({ timeoutMs: 1 }) });
  void mock.createAcls({ acls: [binding] });
  const described = mock.describeAcls(acl.AclBindingFilter.ANY);
  expectTypeOf(described.values()).toEqualTypeOf<KafkaFutureOf<AclBindingType[]>>();
  void mock.describeAcls({ filter: binding.toFilter(), options: new admin.DescribeAclsOptions() });
  void mock.describeAcls({ filter: acl.AclBindingFilter.ANY });
  const deleted = mock.deleteAcls({ filters: [acl.AclBindingFilter.ANY] });
  expectTypeOf(deleted.values()).toEqualTypeOf<AclBindingFilterMapOf<KafkaFutureOf<FilterResultsType>>>();
  expectTypeOf(deleted.all()).toEqualTypeOf<KafkaFutureOf<AclBindingType[]>>();
  const results = await deleted.values().get(acl.AclBindingFilter.ANY)?.get();
  if (results !== undefined) {
    expectTypeOf(results.values()).toEqualTypeOf<FilterResultType[]>();
    expectTypeOf(results.values()[0].binding()).toEqualTypeOf<AclBindingType | null>();
    expectTypeOf(results.values()[0].exception()).toEqualTypeOf<ApiErrorType | null>();
  }
  expectTypeOf(acl.AccessControlEntryFilter.ANY.principal()).toEqualTypeOf<string | null>();
  expectTypeOf(res.ResourcePatternFilter.ANY.name()).toEqualTypeOf<string | null>();
  expectTypeOf(res.PatternType.fromString(null)).toEqualTypeOf<import('@confluentinc/kafka-javascript/common/resource').PatternType>();
  // @ts-expect-error describeAcls takes one filter, not a collection
  mock.describeAcls([acl.AclBindingFilter.ANY]);
  // @ts-expect-error createAcls takes bindings, not filters
  mock.createAcls([acl.AclBindingFilter.ANY]);
  // @ts-expect-error a ResourcePattern's name is not null
  new res.ResourcePattern({ resourceType: res.ResourceType.TOPIC, name: null, patternType: res.PatternType.LITERAL });

  // Quotas, SCRAM credentials and delegation tokens.
  const quota = await import('@confluentinc/kafka-javascript/common/quota');
  const auth = await import('@confluentinc/kafka-javascript/common/security/auth');
  type ClientQuotaEntityMapOf<V> = import('@confluentinc/kafka-javascript/common/quota').ClientQuotaEntityMap<V>;
  type DescriptionType = import('@confluentinc/kafka-javascript/admin').UserScramCredentialsDescription;
  type DelegationTokenType = import('@confluentinc/kafka-javascript/common/security/token/delegation').DelegationToken;
  const entity = new quota.ClientQuotaEntity(new Map([[quota.ClientQuotaEntity.USER, 'a']]));
  const quotas = mock.describeClientQuotas(quota.ClientQuotaFilter.all());
  expectTypeOf(quotas.entities()).toEqualTypeOf<KafkaFutureOf<ClientQuotaEntityMapOf<Map<string, number>>>>();
  void mock.describeClientQuotas({
    filter: quota.ClientQuotaFilter.containsOnly([quota.ClientQuotaFilterComponent.ofEntityType('user')]),
    options: new admin.DescribeClientQuotasOptions(),
  });
  void mock.describeClientQuotas({ filter: quota.ClientQuotaFilter.all() });
  const altered = mock.alterClientQuotas([
    new quota.ClientQuotaAlteration({ entity, ops: [new quota.ClientQuotaAlteration.Op({ key: 'k', value: null })] }),
  ]);
  expectTypeOf(altered.values()).toEqualTypeOf<ClientQuotaEntityMapOf<KafkaFutureOf<void>>>();
  expectTypeOf(altered.all()).toEqualTypeOf<KafkaFutureOf<void>>();
  void mock.alterClientQuotas({ entries: [], options: new admin.AlterClientQuotasOptions({ validateOnly: true }) });
  void mock.alterClientQuotas({ entries: [] });
  const scram = mock.describeUserScramCredentials();
  expectTypeOf(scram.all()).toEqualTypeOf<KafkaFutureOf<Map<string, DescriptionType>>>();
  expectTypeOf(scram.users()).toEqualTypeOf<KafkaFutureOf<string[]>>();
  expectTypeOf(scram.description('u')).toEqualTypeOf<KafkaFutureOf<DescriptionType>>();
  void mock.describeUserScramCredentials(['u']);
  void mock.describeUserScramCredentials({ users: ['u'] });
  void mock.describeUserScramCredentials({ users: null, options: new admin.DescribeUserScramCredentialsOptions() });
  const info = new admin.ScramCredentialInfo({ mechanism: admin.ScramMechanism.SCRAM_SHA_256, iterations: 4096 });
  const scramAltered = mock.alterUserScramCredentials([
    new admin.UserScramCredentialUpsertion({ user: 'u', credentialInfo: info, password: 'p' }),
    new admin.UserScramCredentialUpsertion({ user: 'u', credentialInfo: info, password: new Uint8Array(1), salt: new Uint8Array(1) }),
    new admin.UserScramCredentialDeletion({ user: 'u', mechanism: admin.ScramMechanism.SCRAM_SHA_512 }),
  ]);
  expectTypeOf(scramAltered.values()).toEqualTypeOf<Map<string, KafkaFutureOf<void>>>();
  expectTypeOf(scramAltered.all()).toEqualTypeOf<KafkaFutureOf<void>>();
  void mock.alterUserScramCredentials({
    alterations: [new admin.UserScramCredentialDeletion({ user: 'u', mechanism: admin.ScramMechanism.SCRAM_SHA_256 })],
  });
  const principal = new auth.KafkaPrincipal({ principalType: auth.KafkaPrincipal.USER_TYPE, name: 'a' });
  const created2 = mock.createDelegationToken(
    new admin.CreateDelegationTokenOptions({ renewers: [principal], owner: null, maxLifetimeMs: 1n }),
  );
  expectTypeOf(created2.delegationToken()).toEqualTypeOf<KafkaFutureOf<DelegationTokenType>>();
  void mock.createDelegationToken({ options: new admin.CreateDelegationTokenOptions({ renewers: [principal] }) });
  void mock.renewDelegationToken({ hmac: new Uint8Array(1) });
  void mock.expireDelegationToken({ hmac: new Uint8Array(1) });
  void mock.describeDelegationToken({ options: new admin.DescribeDelegationTokenOptions() });
  expectTypeOf(mock.renewDelegationToken(new Uint8Array(1)).expiryTimestamp()).toEqualTypeOf<KafkaFutureOf<bigint>>();
  expectTypeOf(
    mock.expireDelegationToken({ hmac: new Uint8Array(1), options: new admin.ExpireDelegationTokenOptions() }).expiryTimestamp(),
  ).toEqualTypeOf<KafkaFutureOf<bigint>>();
  expectTypeOf(mock.describeDelegationToken().delegationTokens()).toEqualTypeOf<KafkaFutureOf<DelegationTokenType[]>>();
  const token = await created2.delegationToken().get();
  expectTypeOf(token.hmac()).toEqualTypeOf<Buffer>();
  expectTypeOf(token.tokenInfo().expiryTimestamp()).toEqualTypeOf<bigint>();
  expectTypeOf(new admin.CreateDelegationTokenOptions().owner()).toEqualTypeOf<
    import('@confluentinc/kafka-javascript/common/security/auth').KafkaPrincipal | null
  >();
  // @ts-expect-error describeUserScramCredentials has no (options) form
  mock.describeUserScramCredentials({ options: new admin.DescribeUserScramCredentialsOptions() });
  // @ts-expect-error a ScramCredentialInfo is required
  new admin.UserScramCredentialUpsertion({ user: 'u', password: 'p' });
  new admin.UserScramCredentialUpsertion({ user: 'u', credentialInfo: info, password: new Uint8Array(1), salt: new Uint8Array(1) });
  // @ts-expect-error a salt only with a byte password: Java has no (String, ScramCredentialInfo, String, byte[]) form
  new admin.UserScramCredentialUpsertion({ user: 'u', credentialInfo: info, password: 'p', salt: new Uint8Array(1) });
  await mock.close();
}
void adminSecurity;

// The R3 transaction and producer calls: every Result accessor's type, and
// each RPC's forms bare and by name.
async function adminTransactions(): Promise<void> {
  const admin = await import('@confluentinc/kafka-javascript/admin');
  const common = await import('@confluentinc/kafka-javascript/common');
  type KafkaFutureOf<T> = import('@confluentinc/kafka-javascript/common').KafkaFuture<T>;
  type TopicPartitionMapOf<V> = import('@confluentinc/kafka-javascript/common').TopicPartitionMap<V>;
  type PartitionProducerStateType = import('@confluentinc/kafka-javascript/admin').DescribeProducersResult.PartitionProducerState;
  type ListingType = import('@confluentinc/kafka-javascript/admin').TransactionListing;
  type DescriptionType = import('@confluentinc/kafka-javascript/admin').TransactionDescription;
  const mock = new admin.MockAdminClient();
  const tp = new common.TopicPartition({ topic: 't', partition: 0 });

  const producers = mock.describeProducers([tp]);
  expectTypeOf(producers.partitionResult(tp)).toEqualTypeOf<KafkaFutureOf<PartitionProducerStateType>>();
  expectTypeOf(producers.all()).toEqualTypeOf<KafkaFutureOf<TopicPartitionMapOf<PartitionProducerStateType>>>();
  void mock.describeProducers({ partitions: new Set([tp]), options: new admin.DescribeProducersOptions({ brokerId: 1 }) });
  void mock.describeProducers({ partitions: [tp] });
  const state = await producers.partitionResult({ partition: tp }).get();
  expectTypeOf(state.activeProducers()).toEqualTypeOf<import('@confluentinc/kafka-javascript/admin').ProducerState[] | null>();
  const producer = (state.activeProducers() ?? [])[0];
  expectTypeOf(producer.producerId()).toEqualTypeOf<bigint>();
  expectTypeOf(producer.coordinatorEpoch()).toEqualTypeOf<number | null>();
  expectTypeOf(producer.currentTransactionStartOffset()).toEqualTypeOf<bigint | null>();

  const described = mock.describeTransactions(['a']);
  expectTypeOf(described.description('a')).toEqualTypeOf<KafkaFutureOf<DescriptionType>>();
  expectTypeOf(described.all()).toEqualTypeOf<KafkaFutureOf<Map<string, DescriptionType>>>();
  void mock.describeTransactions({ transactionalIds: ['a'], options: new admin.DescribeTransactionsOptions() });
  void mock.describeTransactions({ transactionalIds: ['a'] });
  const description = await described.description({ transactionalId: 'a' }).get();
  expectTypeOf(description.transactionStartTimeMs).toEqualTypeOf<bigint | null>();
  expectTypeOf(description.topicPartitions).toEqualTypeOf<import('@confluentinc/kafka-javascript/common').TopicPartitionSet | null>();

  const spec = new admin.AbortTransactionSpec({ topicPartition: tp, producerId: 1n, producerEpoch: 0, coordinatorEpoch: 0 });
  expectTypeOf(mock.abortTransaction(spec).all()).toEqualTypeOf<KafkaFutureOf<void>>();
  void mock.abortTransaction({ spec, options: new admin.AbortTransactionOptions() });
  void mock.abortTransaction({ spec });
  expectTypeOf(mock.forceTerminateTransaction('a').result()).toEqualTypeOf<KafkaFutureOf<void>>();
  void mock.forceTerminateTransaction({ transactionalId: 'a', options: new admin.TerminateTransactionOptions() });
  void mock.forceTerminateTransaction({ transactionalId: 'a' });

  const listed = mock.listTransactions(new admin.ListTransactionsOptions({ filteredStates: [admin.TransactionState.ONGOING] }));
  expectTypeOf(listed.all()).toEqualTypeOf<KafkaFutureOf<ListingType[]>>();
  expectTypeOf(listed.byBrokerId()).toEqualTypeOf<KafkaFutureOf<Map<number, KafkaFutureOf<ListingType[]>>>>();
  expectTypeOf(listed.allByBrokerId()).toEqualTypeOf<KafkaFutureOf<Map<number, ListingType[]>>>();
  void mock.listTransactions();
  void mock.listTransactions({ options: new admin.ListTransactionsOptions() });

  const fenced = mock.fenceProducers(['a']);
  expectTypeOf(fenced.fencedProducers()).toEqualTypeOf<Map<string, KafkaFutureOf<void>>>();
  expectTypeOf(fenced.producerId('a')).toEqualTypeOf<KafkaFutureOf<bigint>>();
  expectTypeOf(fenced.epochId({ transactionalId: 'a' })).toEqualTypeOf<KafkaFutureOf<number>>();
  expectTypeOf(fenced.all()).toEqualTypeOf<KafkaFutureOf<void>>();
  void mock.fenceProducers({ transactionalIds: ['a'], options: new admin.FenceProducersOptions() });
  void mock.fenceProducers({ transactionalIds: ['a'] });

  // @ts-expect-error a Result's constructor is private: instances come from the client
  void new admin.ListTransactionsResult();
  // @ts-expect-error describeTransactions takes the ids, not one id
  void mock.describeTransactions({ transactionalId: 'a' });
  await mock.close();
}
void adminTransactions;

// The R3 feature and controller calls: every Result accessor's type, and
// each RPC's forms bare and by name.
async function adminFeatures(): Promise<void> {
  const admin = await import('@confluentinc/kafka-javascript/admin');
  type KafkaFutureOf<T> = import('@confluentinc/kafka-javascript/common').KafkaFuture<T>;
  type FeatureMetadataType = import('@confluentinc/kafka-javascript/admin').FeatureMetadata;
  type FinalizedType = import('@confluentinc/kafka-javascript/admin').FinalizedVersionRange;
  type SupportedType = import('@confluentinc/kafka-javascript/admin').SupportedVersionRange;
  const mock = admin.MockAdminClient.create().usingRaftController(true).build();

  const described = mock.describeFeatures();
  expectTypeOf(described.featureMetadata()).toEqualTypeOf<KafkaFutureOf<FeatureMetadataType>>();
  void mock.describeFeatures(new admin.DescribeFeaturesOptions({ nodeId: 1 }));
  void mock.describeFeatures({ options: new admin.DescribeFeaturesOptions() });
  const metadata = await described.featureMetadata().get();
  expectTypeOf(metadata.finalizedFeatures()).toEqualTypeOf<Map<string, FinalizedType>>();
  expectTypeOf(metadata.finalizedFeaturesEpoch()).toEqualTypeOf<bigint | null>();
  expectTypeOf(metadata.supportedFeatures()).toEqualTypeOf<Map<string, SupportedType>>();
  expectTypeOf(new admin.DescribeFeaturesOptions().nodeId()).toEqualTypeOf<number | null>();

  const update = new admin.FeatureUpdate({ maxVersionLevel: 2, upgradeType: admin.FeatureUpdate.UpgradeType.UPGRADE });
  expectTypeOf(update.upgradeType()).toEqualTypeOf<import('@confluentinc/kafka-javascript/admin').FeatureUpdate.UpgradeType>();
  const updated = mock.updateFeatures(new Map([['f', update]]));
  expectTypeOf(updated.values()).toEqualTypeOf<Map<string, KafkaFutureOf<void>>>();
  expectTypeOf(updated.all()).toEqualTypeOf<KafkaFutureOf<void>>();
  void mock.updateFeatures({ featureUpdates: { f: update }, options: new admin.UpdateFeaturesOptions({ validateOnly: true }) });
  void mock.updateFeatures({ featureUpdates: new Map([['f', update]]) });
  expectTypeOf(new admin.UpdateFeaturesOptions().validateOnly()).toEqualTypeOf<boolean>();

  expectTypeOf(mock.unregisterController(1).all()).toEqualTypeOf<KafkaFutureOf<void>>();
  void mock.unregisterController({ controllerId: 1, options: new admin.UnregisterControllerOptions() });
  void mock.unregisterController({ controllerId: 1 });

  // @ts-expect-error a plain object of updates given alone is the options object
  void mock.updateFeatures({ f: update });
  // @ts-expect-error a Result's constructor is private: instances come from the client
  void new admin.UpdateFeaturesResult(new Map());
  // @ts-expect-error describeFeatures takes no node id of its own: it is an option
  void mock.describeFeatures({ nodeId: 1 });
  await mock.close();
}
void adminFeatures;
