#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example high-level Kafka Consumer (KIP-848 consumer group protocol).
#
# Adapted from confluent-kafka-python's examples/consumer.py for this
# client's bindings. The API differs from confluent-kafka-python in ways
# that matter here:
#   * poll() returns a batch (ConsumerRecords), not a single Message, and
#     raises KafkaError on failure instead of returning a message whose
#     .error() must be checked.
#   * ConsumerRecord fields (topic, partition, offset, key, value) are
#     plain properties, not methods.
#   * The rebalance listener is a Java-style ConsumerRebalanceListener object
#     (on_partitions_assigned / on_partitions_revoked), passed as
#     subscribe(topics, listener=...) — the analog of confluent-kafka-python's
#     on_assign=. Its methods run on the Rust dispatcher thread and the
#     rebalance does not complete until they return; to call back into the
#     consumer from inside a listener (e.g. commit_sync before partitions are
#     revoked), use consumer.handle(), not the consumer's own methods.
#   * There is no store_offsets / enable.auto.offset.store; this example
#     relies on the client's default auto-commit behavior.
#

import getopt
import sys

from consumer import KafkaConsumer
from producer import KafkaError


class RebalanceListener:
    """Prints assignment changes — the analog of the original example's
    on_assign=print_assignment. Runs on the Rust dispatcher thread."""

    def on_partitions_assigned(self, partitions):
        print('Assigned:', [(tp.topic, tp.partition) for tp in partitions])

    def on_partitions_revoked(self, partitions):
        print('Revoked:', [(tp.topic, tp.partition) for tp in partitions])


def print_usage_and_exit(program_name):
    sys.stderr.write(
        'Usage: %s <bootstrap-brokers> <group> <topic1> <topic2> ..\n'
        % program_name)
    sys.exit(1)


if __name__ == '__main__':
    _optlist, argv = getopt.getopt(sys.argv[1:], '')
    if len(argv) < 3:
        print_usage_and_exit(sys.argv[0])

    broker = argv[0]
    group = argv[1]
    topics = argv[2:]

    # Consumer configuration. See bindings/python/README.md for the config
    # keys this client currently accepts. Config values must be strings.
    #
    # group.protocol=consumer is REQUIRED: this client implements only the
    # KIP-848 ("consumer") group protocol; the classic protocol is rejected
    # at construction. (The confluent-kafka-python original does not need
    # this key.)
    conf = {
        'bootstrap.servers': broker,
        'group.id': group,
        'group.protocol': 'consumer',
        'session.timeout.ms': '6000',
        'auto.offset.reset': 'earliest',
    }

    # Create Consumer instance. The `with` block closes it (committing final
    # offsets) on exit.
    with KafkaConsumer(conf) as c:
        c.subscribe(topics, listener=RebalanceListener())

        # Read messages from Kafka, print to stdout
        try:
            while True:
                try:
                    records = c.poll(1.0)
                except KafkaError as e:
                    sys.stderr.write('%% Poll failed: %s\n' % e)
                    continue

                for record in records:
                    key = bytes(record.key) if record.key is not None else None
                    sys.stderr.write(
                        '%% %s [%d] at offset %d with key %s:\n'
                        % (record.topic, record.partition, record.offset, key)
                    )
                    print(bytes(record.value))

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')
