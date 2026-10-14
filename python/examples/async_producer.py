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
# Example asyncio Kafka Producer.
# Reads lines from stdin and sends them to Kafka. Leaving the `async with`
# block flushes the producer, then closes it.
#

import asyncio
import functools
import sys

from confluent_kafka.common.serialization import string_serializer
from confluent_kafka.producer import AsyncKafkaProducer, ProducerRecord


def delivery_callback(record, metadata, exception):
    """Per-message delivery callback, run on the event loop once the message is
    delivered or has permanently failed (after retries). The client passes
    (metadata, exception); the record is bound in front by functools.partial,
    since the metadata carries no value."""
    if exception is not None:
        sys.stderr.write('%% Message failed delivery: %s: %s\n' % (record.value(), exception))
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d: %s\n' %
                         (metadata.topic(), metadata.partition(), metadata.offset(), record.value()))


async def main(broker, topic):
    # Producer configuration.
    # The keys are the Java client's: https://kafka.apache.org/documentation/#producerconfigs
    conf = {'bootstrap.servers': broker}

    # The value serializer encodes each line as UTF-8.
    async with AsyncKafkaProducer(configs=conf, value_serializer=string_serializer()) as p:
        while True:
            # Read stdin off the event loop, so delivery callbacks keep running.
            line = await asyncio.to_thread(sys.stdin.readline)
            if not line:
                break
            record = ProducerRecord(topic=topic, value=line.rstrip())
            # Awaiting send() waits for buffer space, and returns the future of
            # the delivery; `await (await p.send(...))` would give its metadata.
            await p.send(record=record, callback=functools.partial(delivery_callback, record))

        sys.stderr.write('% Waiting for outstanding deliveries\n')
    # Leaving the block waited for every delivery, then closed the producer.


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    asyncio.run(main(sys.argv[1], sys.argv[2]))
