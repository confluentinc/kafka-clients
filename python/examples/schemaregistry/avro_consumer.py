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
# NOTE: For GA, Confluent will provide a supported Schema Registry client that
# works with this Python client. During the preview, use the third-party
# python-schema-registry-client package, as this example does.
#

#
# Consumes the Avro records avro_producer.py writes, decoding them with the
# schema fetched from Confluent Schema Registry through the
# python-schema-registry-client package.
#
# The registry deserializer is wrapped in a serde of this client,
# AvroDeserializer, so poll() returns records whose values are decoded User
# objects, like any other deserializer.
#
# The deserializer fetches the writer schema by the id embedded in each
# record. Only the first record of each schema reaches the registry: the
# registry client caches the schema by id, so every later call decodes
# without a network round trip.
#

import dataclasses
import os
import sys
import uuid

from schema_registry.client import SchemaRegistryClient
from schema_registry.serializers import AvroMessageSerializer

from confluent_kafka.common.errors import RetriableError, SerializationError
from confluent_kafka.common.serialization import string_deserializer
from confluent_kafka.consumer import ConsumerConfig, KafkaConsumer

TOPIC_NAME = 'avro-users'
GROUP_ID = 'avro-users-group'


@dataclasses.dataclass
class User:
    """The Python shape of a User record, filled from the decoded Avro value by
    field name; it matches the schema avro_producer.py registers."""
    name: str
    favorite_number: int
    favorite_color: str


class AvroDeserializer:
    """A deserializer that reads the Confluent wire format (magic byte, schema
    id, Avro binary payload) with the writer schema from the registry, and
    builds a `record_type` from the decoded fields."""

    def __init__(self, *, registry, record_type):
        self._inner = AvroMessageSerializer(registry)
        self._record_type = record_type

    def __call__(self, *, topic, data, headers=None, **kwargs):
        # The client does not call a deserializer for a null value; `data` is
        # a memoryview into the fetch batch, which the decoder reads as is.
        try:
            return self._record_type(**self._inner.decode_message(data))
        except Exception as e:
            raise SerializationError(
                message='cannot deserialize the Avro record from topic %s: %s' % (topic, e), cause=e)


def bootstrap_servers():
    return os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')


def schema_registry_url():
    return os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')


def consume_loop(consumer):
    while True:
        # The value deserializer decodes every record inside poll().
        try:
            records = consumer.poll(timeout=30.0)
        except RetriableError as e:
            print('transient poll error, retrying: %s' % e, file=sys.stderr)
            continue

        if records.is_empty():
            print('No records consumed in this poll interval.')
            return

        for record in records:
            user = record.value()
            if user is None:
                print('Consumed tombstone: key=%s' % record.key())
                continue
            print('Consumed record: key=%s, user %s likes %s and %d (partition %d, offset %d)' %
                  (record.key(), user.name, user.favorite_color, user.favorite_number,
                   record.partition(), record.offset()))
        consumer.commit()


def main():
    registry = SchemaRegistryClient(url=schema_registry_url())
    value_deserializer = AvroDeserializer(registry=registry, record_type=User)
    print('Schema Registry client created for %s.' % schema_registry_url())

    conf = {
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG: bootstrap_servers(),
        ConsumerConfig.GROUP_ID_CONFIG: GROUP_ID,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG: 'earliest',
        # This client speaks only the KIP-848 consumer group protocol.
        ConsumerConfig.GROUP_PROTOCOL_CONFIG: 'consumer',
        ConsumerConfig.CLIENT_ID_CONFIG: 'client-%s' % uuid.uuid4(),
    }

    # Leaving the block closes the consumer.
    with KafkaConsumer(configs=conf, key_deserializer=string_deserializer(),
                       value_deserializer=value_deserializer) as consumer:
        print('Kafka consumer created successfully.')
        consumer.subscribe(topics=[TOPIC_NAME])
        print('Subscribed to topic: %s' % TOPIC_NAME)
        try:
            consume_loop(consumer)
        except KeyboardInterrupt:
            print('Aborted by user', file=sys.stderr)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        sys.stderr.write('avro consumer failed: %s\n' % e)
        sys.exit(1)
