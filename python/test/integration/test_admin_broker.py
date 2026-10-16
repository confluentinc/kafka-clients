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

"""The admin client against a broker, where a value only a broker's response
carries reaches the Python value classes: ``createTopics``' per-topic config,
whose entries Java builds from the response (``KafkaAdminClient.configEntry``:
the broker's source, no synonyms, no type, no documentation). Skips without
Docker (``kafka_broker``)."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from confluent_kafka.admin import Admin, AsyncAdmin, Config, ConfigEntry, NewTopic

#: Read back from the created topic's config.
RETENTION_MS = "4242"


def _new_topic() -> NewTopic:
    return NewTopic(name=f"py-admin-{uuid.uuid4().hex[:12]}", num_partitions=1,
                    replication_factor=1).configs(configs={"retention.ms": RETENTION_MS})


def _assert_created_config(config: Config) -> None:
    entry = config.get(name="retention.ms")
    assert entry is not None and entry.value() == RETENTION_MS
    assert entry.source() is ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG
    assert entry.type() is ConfigEntry.ConfigType.UNKNOWN
    assert entry.synonyms() == [] and entry.documentation() is None


def test_create_topics_config(kafka_broker: Any) -> None:
    """``CreateTopicsResult.config(topic)`` of a created topic."""
    topic = _new_topic()
    with Admin.create(configs={"bootstrap.servers": kafka_broker.external_bootstrap}) as admin:
        result = admin.create_topics(new_topics=[topic])
        _assert_created_config(result.config(topic=topic.name()).get(timeout=30))
        assert result.num_partitions(topic=topic.name()).get(timeout=30) == 1


def test_async_create_topics_config(kafka_broker: Any) -> None:
    """The same on the asyncio client."""
    async def main() -> Config:
        topic = _new_topic()
        async with AsyncAdmin.create(
                configs={"bootstrap.servers": kafka_broker.external_bootstrap}) as admin:
            result = admin.create_topics(new_topics=[topic])
            config: Config = await result.config(topic=topic.name()).get(timeout=30)
            return config

    _assert_created_config(asyncio.run(main()))
