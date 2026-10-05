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
# Example Kafka Producer.
# Reads lines from stdin and sends them to Kafka.
#

import functools
import sys

from confluent_kafka.common.serialization import string_serializer
from confluent_kafka.producer import KafkaProducer, ProducerRecord


def delivery_callback(record, metadata, exception):
    """Per-message delivery callback, run on the producer's background
    completion thread once the message is delivered or has permanently failed
    (after retries). The client passes (metadata, exception); the record is
    bound in front by functools.partial, since the metadata carries no value."""
    if exception is not None:
        sys.stderr.write('%% Message failed delivery: %s: %s\n' % (record.value(), exception))
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d: %s\n' %
                         (metadata.topic(), metadata.partition(), metadata.offset(), record.value()))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Producer configuration.
    # The keys are the Java client's: https://kafka.apache.org/documentation/#producerconfigs
    conf = {'bootstrap.servers': broker}

    # Create the producer. The value serializer encodes each line as UTF-8.
    p = KafkaProducer(configs=conf, value_serializer=string_serializer())

    try:
        # Read lines from stdin and send each one (without its newline).
        # send() does not raise when the local buffer is full: it waits for
        # space, up to max.block.ms.
        for line in sys.stdin:
            record = ProducerRecord(topic=topic, value=line.rstrip())
            p.send(record=record, callback=functools.partial(delivery_callback, record))

        # Wait until every message has been delivered or has failed.
        sys.stderr.write('% Waiting for outstanding deliveries\n')
        p.flush()

    finally:
        p.close()
