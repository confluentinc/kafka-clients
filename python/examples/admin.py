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
# Example Kafka Admin client, used as a context manager.
# Creates a topic, waits for the result of each topic, then lists the topics.
# Leaving the `with` block closes the client.
#

import sys

from confluent_kafka.admin import Admin, NewTopic
from confluent_kafka.common.errors import TopicExistsError

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Admin configuration.
    # The keys are the Java client's: https://kafka.apache.org/documentation/#adminclientconfigs
    conf = {'bootstrap.servers': broker}

    with Admin.create(configs=conf) as admin:
        # create_topics() returns once the core has answered every topic, so
        # each future in the result is already complete.
        result = admin.create_topics(
            new_topics=[NewTopic(name=topic, num_partitions=3, replication_factor=1)])

        # One future per topic; result() raises the topic's error, as a class.
        for name, future in result.values().items():
            try:
                future.result()
                print('Topic %s created' % name)
            except TopicExistsError:
                print('Topic %s already exists' % name)

        # list_topics() returns one future with every topic's name.
        print('Topics: %s' % sorted(admin.list_topics().names().result()))
