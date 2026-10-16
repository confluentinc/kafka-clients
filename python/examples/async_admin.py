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
# Example asyncio Kafka Admin client.
# Creates a topic, then lists the topics. Leaving the `async with` block closes
# the client.
#

import asyncio
import sys

from confluent_kafka import ExecutionError
from confluent_kafka.admin import AsyncAdmin, NewTopic
from confluent_kafka.common.errors import TopicExistsError


async def main(broker, topic):
    # Admin configuration.
    # The keys are the Java client's: https://kafka.apache.org/documentation/#adminclientconfigs
    conf = {'bootstrap.servers': broker}

    async with AsyncAdmin.create(configs=conf) as admin:
        # Admin calls return their result at once; its AsyncKafkaFutures are
        # awaited through get().
        result = admin.create_topics(
            new_topics=[NewTopic(name=topic, num_partitions=3, replication_factor=1)])

        # One AsyncKafkaFuture per topic; get() raises ExecutionError for a
        # failed one, the error being its __cause__.
        for name, future in result.values().items():
            try:
                await future.get()
                print('Topic %s created' % name)
            except ExecutionError as e:
                if not isinstance(e.__cause__, TopicExistsError):
                    raise
                print('Topic %s already exists' % name)

        listing = admin.list_topics()
        print('Topics: %s' % sorted(await listing.names().get()))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    asyncio.run(main(sys.argv[1], sys.argv[2]))
