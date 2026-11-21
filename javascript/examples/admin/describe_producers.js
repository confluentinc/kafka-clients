#!/usr/bin/env node
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

'use strict';

// Describes the active producers of partitions.

const { DescribeProducersOptions } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');
const { fail, main, parseInt32, topicPartitions } = require('./common');

const USAGE = 'usage: node examples/admin/describe_producers.js [-X key=value]... [--broker=<id>] (<topic> <partition>)...';

main(USAGE, async (admin, args) => {
  const broker = args.value('broker');
  // A partition named twice is described once.
  const partitions = [...new TopicPartitionSet(topicPartitions(args.positional(), true))];
  // No broker: each partition's leader.
  const options =
    broker === null ? new DescribeProducersOptions() : new DescribeProducersOptions({ brokerId: parseInt32('broker id', broker) });
  const result = admin.describeProducers({ partitions, options });
  for (const partition of partitions) {
    try {
      const producers = (await result.partitionResult(partition).get()).activeProducers() ?? [];
      console.log(`Partition ${partition}: ${producers.length} active producers`);
      for (const producer of producers) {
        console.log(
          `  producer id ${producer.producerId()}, producer epoch ${producer.producerEpoch()}, ` +
            `coordinator epoch ${producer.coordinatorEpoch() ?? -1}, last sequence ${producer.lastSequence()}, ` +
            `last timestamp ${producer.lastTimestamp()}, current transaction start offset ${producer.currentTransactionStartOffset() ?? -1}`,
        );
      }
    } catch (error) {
      fail(`Failed to describe producers of ${partition}`, error);
    }
  }
});
