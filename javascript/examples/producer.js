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

const { KafkaProducer, ProducerRecord } = require('..');

async function main(argv) {
  const [bootstrap, topic, key = null, value = 'hello from Node.js'] = argv;
  if (!bootstrap || !topic) {
    console.error('usage: node examples/producer.js <bootstrap-servers> <topic> [key] [value]');
    return 2;
  }
  const producer = new KafkaProducer({ 'bootstrap.servers': bootstrap });
  try {
    // Resolves once the producer has the record (Java's send() returning)...
    const future = await producer.send(new ProducerRecord({ topic, key, value }), (m, error) => {
      if (error !== null) console.error(`delivery failed: ${error.name}: ${error.message}`);
    });
    // ...and the future once the broker acknowledged it.
    const metadata = await future.get();
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
