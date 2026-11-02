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
// `tsc --noEmit --strict` against the committed declarations, resolved
// through package.json's `exports` (the package's own name). Every
// `@ts-expect-error` line must be a type error.

import { common, producer } from 'confluent-kafka-javascript-rust';
import {
  type Callback,
  type CloseOptions,
  type Data,
  type Header,
  KafkaProducer,
  type ProducerConfigs,
  ProducerRecord,
  type ProducerRecordOptions,
  RecordMetadata,
} from 'confluent-kafka-javascript-rust/producer';
import { KafkaError, KafkaFuture } from 'confluent-kafka-javascript-rust/common';

// The root exports the namespaces, the same classes as the entry points.
const SameProducer: typeof KafkaProducer = producer.KafkaProducer;
const SameError: typeof KafkaError = common.KafkaError;
void SameProducer;
void SameError;
const viaNamespace: producer.Callback = (metadata, error) => void [metadata, error];
void viaNamespace;

const configs: ProducerConfigs = {
  'bootstrap.servers': 'localhost:9092',
  'linger.ms': 5,
  'enable.idempotence': true,
  'buffer.memory': 33554432n,
  'bootstrap.servers.list': ['a:1', 'b:2'],
  'client.id': null,
};
const fromMap: ProducerConfigs = new Map<string, unknown>([['bootstrap.servers', 'localhost:9092']]);
void fromMap;

const headers: Header[] = [{ key: 'h', value: Buffer.from('v') }, { key: 'n', value: null }];
const key: Data = 'k';
const options: ProducerRecordOptions = { topic: 't', partition: 0, timestamp: null, key, value: new Uint8Array(1), headers };
const record = new ProducerRecord(options);
const topic: string = record.topic;
const partition: number | null = record.partition;
const recordHeaders: ReadonlyArray<Readonly<Header>> = record.headers;
void [topic, partition, recordHeaders];

const callback: Callback = (metadata: RecordMetadata, error: KafkaError | null) => {
  const offset: number = metadata.offset;
  const hasOffset: boolean = metadata.hasOffset;
  if (error !== null) {
    const code: number = error.code;
    const name: string = error.name;
    const retriable: boolean = error.retriable;
    const apiError: boolean = error.apiError;
    const cause: unknown = error.cause;
    void [code, name, retriable, apiError, cause];
  }
  void [offset, hasOffset];
};

async function produce(): Promise<void> {
  const kafkaProducer = new KafkaProducer(configs);
  const future: KafkaFuture<RecordMetadata> = await kafkaProducer.send(record, callback);
  await kafkaProducer.send(record);
  await kafkaProducer.send(record, null);
  const metadata: RecordMetadata = await future.get();
  const done: boolean = future.isDone();
  const failed: boolean = future.isCompletedExceptionally();
  const cancelled: false = future.cancel(true);
  void [metadata, done, failed, cancelled, future.isCancelled()];
  const unknown: -1 = RecordMetadata.UNKNOWN_PARTITION;
  void unknown;
  await kafkaProducer.flush();
  const closeOptions: CloseOptions = { timeoutMs: 0 };
  await kafkaProducer.close(closeOptions);
  await kafkaProducer.close();
}
void produce;

// Made by the client only.
// @ts-expect-error RecordMetadata's constructor is private
new RecordMetadata();
// @ts-expect-error KafkaFuture's constructor is private
new KafkaFuture();
// @ts-expect-error KafkaError's constructor is private
new KafkaError();
// @ts-expect-error the client's own members are private
KafkaError._from({});
// @ts-expect-error the client's own members are private
record._toNative();

// Getters are read-only.
// @ts-expect-error topic is a getter
record.topic = 'other';

// @ts-expect-error a ProducerRecord needs a topic
new ProducerRecord({ value: 'v' });
// @ts-expect-error a key is bytes or a string
new ProducerRecord({ topic: 't', key: 1 });
// @ts-expect-error a callback takes (metadata, error)
const badCallback: Callback = (metadata: string) => void metadata;
void badCallback;

// The private files are not entry points.
// @ts-expect-error lib/_config.js is not exported
import('confluent-kafka-javascript-rust/lib/_config');
