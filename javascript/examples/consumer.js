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

// Subscribes to a topic as a member of a group, polls until it has read
// max-records records (default 1) or is interrupted, prints each record, and
// commits what it read after each poll.
//
// Usage: node examples/consumer.js <bootstrap-servers> <topic> <group-id> [max-records]

// The package's own name resolves inside the package (Node's self-reference
// through package.json `exports`), so this runs without an npm install.
const { StringDeserializer } = require('@confluentinc/kafka-javascript/common/serialization');
const { WakeupError } = require('@confluentinc/kafka-javascript/common/errors');
const { KafkaConsumer } = require('@confluentinc/kafka-javascript/consumer');

async function main(argv) {
  const [bootstrap, topic, group, max = '1'] = argv;
  if (!bootstrap || !topic || !group) {
    console.error('usage: node examples/consumer.js <bootstrap-servers> <topic> <group-id> [max-records]');
    return 2;
  }
  // new StringDeserializer() reads the key and value as Java's
  // StringDeserializer does. Every consumer sets group.protocol to
  // 'consumer' (KIP-848), the only group protocol the client has.
  const consumer = new KafkaConsumer({
    configs: { 'bootstrap.servers': bootstrap, 'group.id': group, 'group.protocol': 'consumer', 'auto.offset.reset': 'earliest' },
    keyDeserializer: new StringDeserializer(),
    valueDeserializer: new StringDeserializer(),
  });
  // Ctrl+C wakes the poll that is waiting: it rejects with WakeupError, the
  // loop ends, and only then is the consumer closed (close() during a pending
  // poll() fails, as in Java).
  const stop = () => consumer.wakeup();
  process.once('SIGINT', stop);
  let read = 0;
  try {
    await consumer.subscribe([topic]);
    while (read < Number(max)) {
      // Waits up to a second; a record's key and value are strings here.
      const records = await consumer.poll(1000);
      for (const record of records) {
        console.log(
          `ConsumerRecord topic=${record.topic} partition=${record.partition} offset=${record.offset} ` +
            `key=${record.key} value=${record.value}`,
        );
        read++;
      }
      // Commits the offsets after the records this poll returned, and waits
      // for the broker's answer.
      if (!records.isEmpty()) await consumer.commitSync();
    }
  } catch (e) {
    if (!(e instanceof WakeupError)) throw e;
  } finally {
    process.removeListener('SIGINT', stop);
    await consumer.close();
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
