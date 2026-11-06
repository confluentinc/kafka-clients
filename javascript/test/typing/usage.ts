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
// `@ts-expect-error` line must be a type error.

import { common, producer } from '@confluentinc/kafka-javascript';
import {
  type Callback,
  type CloseOptions,
  type Data,
  type Header,
  KafkaProducer,
  ProducerConfig,
  type ProducerConfigs,
  ProducerRecord,
  type ProducerRecordOptions,
  RecordMetadata,
} from '@confluentinc/kafka-javascript/producer';
import { KafkaError, KafkaFuture } from '@confluentinc/kafka-javascript/common';
import { IllegalStateError } from '@confluentinc/kafka-javascript';
import { AuthenticationError, RetriableError, SaslAuthenticationError } from '@confluentinc/kafka-javascript/common/errors';
import { ConfigError, SaslConfigs, SslClientAuth, SslConfigs } from '@confluentinc/kafka-javascript/common/config';
import { SecurityProtocol } from '@confluentinc/kafka-javascript/common/security/auth';

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

const callback: Callback = (metadata: RecordMetadata, error: Error | null) => {
  const offset: number = metadata.offset;
  const hasOffset: boolean = metadata.hasOffset;
  if (error instanceof KafkaError) {
    const code: number = error.code;
    const name: string = error.name;
    const errorName: string | null = error.errorName;
    void errorName;
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

// The errors: Java's extends chain, Java's (message, cause) constructors.
const sasl: AuthenticationError = new SaslAuthenticationError('bad credentials');
const base: KafkaError = new KafkaError(null, { cause: sasl });
const state: IllegalStateError = new IllegalStateError('closed');
const stateCode: number = state.code;
const retriable: boolean = base.retriable;
const config: ConfigError = new ConfigError('Invalid value');
void [sasl, base, stateCode, retriable, config];
class MyRetriable extends RetriableError {}
void new MyRetriable('m');
// @ts-expect-error Java's RetriableError is abstract: caught and subclassed, never constructed
new RetriableError('m');
// @ts-expect-error code is read-only
base.code = 1;

// The config classes' constants and the security enums.
const mechanism: string = SaslConfigs.SASL_MECHANISM;
const truststore: string = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
const reconfigurable: ReadonlyArray<string> = SslConfigs.RECONFIGURABLE_CONFIGS;
const auth: SslClientAuth | null = SslClientAuth.forConfig('required');
const protocol: SecurityProtocol = SecurityProtocol.forName('sasl_ssl');
const protocolId: number = protocol.id;
void [mechanism, truststore, reconfigurable, auth, protocolId];
// @ts-expect-error a constant is read-only
SslConfigs.SSL_PROTOCOL_CONFIG = 'x';
// @ts-expect-error ProducerConfig holds Java's constants only: its constructor is private
new ProducerConfig();

// Made by the client only.
// @ts-expect-error RecordMetadata's constructor is private
new RecordMetadata();
// @ts-expect-error KafkaFuture's constructor is private
new KafkaFuture();
// @ts-expect-error SecurityProtocol's constructor is private
new SecurityProtocol();
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

// The private files are not entry points, and have no declarations.
// @ts-expect-error dist/_config.js is not exported
import('@confluentinc/kafka-javascript/dist/_config');
// @ts-expect-error dist/_errors.js is not exported
import('@confluentinc/kafka-javascript/dist/_errors');
