#!/usr/bin/env python
#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example high-level Kafka balanced consumer (KIP-848 group protocol).
# Prints every message it reads, and every change to its assignment.
#

import sys

from confluent_kafka.common.serialization import string_deserializer
from confluent_kafka.consumer import ConsumerRebalanceListener, KafkaConsumer


def partition_list(partitions):
    return ', '.join(str(p) for p in sorted(partitions, key=lambda p: (p.topic(), p.partition()))) or '(none)'


class PrintRebalance(ConsumerRebalanceListener):
    """Prints each change to the consumer's assignment. The methods run on the
    thread calling poll(), and the rebalance waits for them to return."""

    def __init__(self, *, consumer):
        self._consumer = consumer

    def on_partitions_assigned(self, partitions):
        # A listener may call back into its consumer.
        sys.stderr.write('%% Assigned: %s; assignment now: %s\n' %
                         (partition_list(partitions), partition_list(self._consumer.assignment())))

    def on_partitions_revoked(self, partitions):
        sys.stderr.write('%% Revoked: %s\n' % partition_list(partitions))

    def on_partitions_lost(self, partitions):
        sys.stderr.write('%% Lost: %s\n' % partition_list(partitions))


if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    group = sys.argv[2]
    topics = sys.argv[3:]

    # Consumer configuration.
    # The keys are the Java client's: https://kafka.apache.org/documentation/#consumerconfigs
    conf = {
        'bootstrap.servers': broker,
        'group.id': group,
        # This client speaks only the KIP-848 consumer group protocol.
        'group.protocol': 'consumer',
        'auto.offset.reset': 'earliest',
        # With auto-commit (the default), the consumer commits the offsets of
        # the records poll() returned every auto.commit.interval.ms (5 s),
        # before a rebalance takes partitions away, and on close(). Processing
        # each batch before the next poll() gives at-least-once delivery.
    }

    # Create the consumer. The deserializers decode keys and values as UTF-8.
    c = KafkaConsumer(configs=conf, key_deserializer=string_deserializer(),
                      value_deserializer=string_deserializer())

    # Subscribe to the topics.
    c.subscribe(topics=topics, callback=PrintRebalance(consumer=c))

    # Read messages from Kafka, print to stdout.
    try:
        while True:
            # poll() returns a batch of records, possibly empty. A failed
            # poll() raises a KafkaError subclass.
            records = c.poll(timeout=1.0)
            for msg in records:
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('% Aborted by user\n')

    finally:
        # Close the consumer: it commits the final offsets and leaves the group.
        c.close()
