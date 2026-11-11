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
# NOTE: For GA, we have plans to provide a Confluent supported Schema Registry
# client that works with this Python client. During the preview, use the
# third-party python-schema-registry-client package, as this example does.
#

#
# Produces Avro records whose schema lives in Confluent Schema Registry, using
# the python-schema-registry-client package for the registry round trip.
#
# The registry serializer is wrapped in a serde of this client,
# AvroSerializer: a callable taking topic=, data= and headers=, so the
# producer is given User objects and serializes each value inside send(),
# like any other serializer.
#
# This client calls serdes synchronously, so the wrapper uses the registry
# package's synchronous client. Only the first record reaches the registry:
# the registry client caches the schema id, so every later call encodes
# without a network round trip.
#

import dataclasses
import os
import uuid

from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import AvroMessageSerializer

from confluent_kafka.common.errors import SerializationError
from confluent_kafka.common.serialization import string_serializer
from confluent_kafka.producer import KafkaProducer, ProducerConfig, ProducerRecord

TOPIC_NAME = 'avro-users'
NUM_RECORDS = 10
COLORS = ('red', 'green', 'blue')

# The Avro schema of the record values. The serializer registers it under the
# subject `avro-users-value` the first time it is used.
USER_SCHEMA = schema.AvroSchema({
    'type': 'record',
    'name': 'User',
    'namespace': 'io.confluent.examples',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'favorite_number', 'type': 'long'},
        {'name': 'favorite_color', 'type': 'string'},
    ],
})


@dataclasses.dataclass
class User:
    """The Python shape of a User record; its fields map onto the schema's."""
    name: str
    favorite_number: int
    favorite_color: str


class AvroSerializer:
    """A serializer that writes a dataclass as Avro in the Confluent wire
    format: a magic byte, the id the registry assigned to the schema, then the
    Avro binary payload."""

    def __init__(self, *, registry, avro_schema, is_key=False):
        self._inner = AvroMessageSerializer(registry)
        self._schema = avro_schema
        self._suffix = '-key' if is_key else '-value'

    def __call__(self, *, topic, data, headers=None, **kwargs):
        # A null value stays null, as Java's serializers recommend.
        if data is None:
            return None
        # The `<topic>-value` subject: the registry's conventional naming
        # (TopicNameStrategy). The schema id travels in the payload prefix, so
        # the record headers are left untouched.
        try:
            return self._inner.encode_record_with_schema(
                topic + self._suffix, self._schema, dataclasses.asdict(data))
        except Exception as e:
            raise SerializationError(
                message='cannot serialize the Avro record for topic %s: %s' % (topic, e), cause=e)


def bootstrap_servers():
    return os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')


def schema_registry_url():
    return os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')


def main():
    registry = SchemaRegistryClient(url=schema_registry_url())
    value_serializer = AvroSerializer(registry=registry, avro_schema=USER_SCHEMA)
    print('Schema Registry client created for %s.' % schema_registry_url())

    conf = {
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG: bootstrap_servers(),
        ProducerConfig.CLIENT_ID_CONFIG: 'client-%s' % uuid.uuid4(),
    }
    print('Kafka producer properties: %s' % conf)

    # Leaving the block flushes the producer, then closes it.
    with KafkaProducer(configs=conf, key_serializer=string_serializer(),
                       value_serializer=value_serializer) as producer:
        print('Kafka producer created successfully.')
        for i in range(NUM_RECORDS):
            user = User(name='user-%d' % i, favorite_number=i, favorite_color=COLORS[i % len(COLORS)])
            print('Producing record: key=%s, value=%s' % (user.name, user))
            # The value serializer encodes `user` inside send(). The first
            # record registers the schema; later ones find its id in the
            # client's cache.
            record = ProducerRecord(topic=TOPIC_NAME, key=user.name, value=user)

            # Synchronous produce: send() returns a future for the record's
            # delivery, and get() blocks until the broker has acknowledged it
            # (or the delivery has failed) before the next record is sent.
            metadata = producer.send(record=record).get()
            print('Delivered record %d to topic %s, partition %d at offset %d' %
                  (i, metadata.topic(), metadata.partition(), metadata.offset()))


if __name__ == '__main__':
    main()
