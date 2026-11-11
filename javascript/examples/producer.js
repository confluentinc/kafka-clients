#!/usr/bin/env node
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

'use strict';

// Produces one record and prints its RecordMetadata.
//
// Usage: node examples/producer.js <bootstrap-servers> <topic> [key] [value]

// The package's own name resolves inside the package (Node's self-reference
// through package.json `exports`), so this runs without an npm install.
const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { Header } = require('@confluentinc/kafka-javascript/common/header');
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');

async function main(argv) {
  const [bootstrap, topic, key = null, value = 'hello from Node.js'] = argv;
  if (!bootstrap || !topic) {
    console.error('usage: node examples/producer.js <bootstrap-servers> <topic> [key] [value]');
    return 2;
  }
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': bootstrap } });
  try {
    // Resolves once the producer has the record (Java's send() returning)...
    // A header value is bytes; a string key or value is encoded as UTF-8, as
    // Java's StringSerializer encodes it. A record with headers names its
    // partition (null: the producer chooses), as Java's constructors do.
    const headers = [new Header({ key: 'source', value: Buffer.from('examples/producer.js') })];
    const future = await producer.send({
      record: new ProducerRecord({ topic, partition: null, key, value, headers }),
      callback: ({ exception }) => {
        if (exception !== null) console.error(`delivery failed: ${exception.name}: ${exception.message}`);
      },
    });
    // ...and the future once the broker acknowledged it. A failed delivery
    // rejects get() with ExecutionError (Java's ExecutionException), whose
    // cause is the error the callback received.
    let metadata;
    try {
      metadata = await future.get();
    } catch (e) {
      throw e instanceof ExecutionError && e.cause instanceof Error ? e.cause : e;
    }
    console.log(
      `RecordMetadata topic=${metadata.topic} partition=${metadata.partition} ` +
        `offset=${metadata.offset} timestamp=${metadata.timestamp}`,
    );
    await producer.flush();
  } finally {
    await producer.close();
  }
  return 0;
}

main(process.argv.slice(2)).then(
  (code) => {
    process.exitCode = code;
  },
  (error) => {
    console.error(`${error.name}: ${error.message}`);
    process.exitCode = 1;
  },
);
