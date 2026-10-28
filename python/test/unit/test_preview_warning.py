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

"""The preview warning (CLAUDE.md, Python Binding Conventions, Behaviour with
no other home): each ``Kafka*`` client and its async peer logs it once, at
WARNING, on its ``confluent_kafka.<module>`` logger when constructed; the mocks
log nothing."""

from __future__ import annotations

import logging

import pytest

from confluent_kafka._preview import PREVIEW_WARNING
from confluent_kafka.admin import Admin, AsyncAdmin, AsyncMockAdminClient, MockAdminClient
from confluent_kafka.consumer import (
    AsyncKafkaConsumer, AsyncMockConsumer, CloseOptions, KafkaConsumer, MockConsumer,
)
from confluent_kafka.producer import (
    AsyncKafkaProducer, AsyncMockProducer, KafkaProducer, MockProducer,
)

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CONSUMER_CONFIGS = {**UNREACHABLE, "group.protocol": "consumer", "group.id": "g"}


def preview_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage() == PREVIEW_WARNING]


def assert_logged_once(caplog: pytest.LogCaptureFixture, logger: str) -> None:
    records = preview_records(caplog)
    assert [(r.name, r.levelno) for r in records] == [(logger, logging.WARNING)]


def test_preview_warning_text() -> None:
    assert PREVIEW_WARNING == (
        "The Python client is in preview and not yet recommended for production use. The "
        "public API is not stable before the GA release and may change. We welcome your "
        "feedback while the design is still open")


def test_kafka_producer_logs_preview_warning(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    KafkaProducer(configs=UNREACHABLE).close()
    assert_logged_once(caplog, "confluent_kafka.producer")


async def test_async_kafka_producer_logs_preview_warning(
        caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    await AsyncKafkaProducer(configs=UNREACHABLE).close()
    assert_logged_once(caplog, "confluent_kafka.producer")


def test_kafka_consumer_logs_preview_warning(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    KafkaConsumer(configs=CONSUMER_CONFIGS).close(option=CloseOptions.timeout(timeout=0))
    assert_logged_once(caplog, "confluent_kafka.consumer")


async def test_async_kafka_consumer_logs_preview_warning(
        caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    await AsyncKafkaConsumer(configs=CONSUMER_CONFIGS).close(
        option=CloseOptions.timeout(timeout=0))
    assert_logged_once(caplog, "confluent_kafka.consumer")


def test_kafka_admin_client_logs_preview_warning(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    Admin.create(configs=UNREACHABLE).close(timeout=0)
    assert_logged_once(caplog, "confluent_kafka.admin")


async def test_async_kafka_admin_client_logs_preview_warning(
        caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    await AsyncAdmin.create(configs=UNREACHABLE).close(timeout=0)
    assert_logged_once(caplog, "confluent_kafka.admin")


def test_each_client_logs_its_own_warning(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    KafkaProducer(configs=UNREACHABLE).close()
    KafkaProducer(configs=UNREACHABLE).close()
    assert len(preview_records(caplog)) == 2


async def test_mocks_log_no_preview_warning(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    MockProducer().close()
    await AsyncMockProducer().close()
    MockConsumer(offset_reset_strategy="earliest").close()
    await AsyncMockConsumer(offset_reset_strategy="earliest").close()
    MockAdminClient().close()
    await AsyncMockAdminClient().close()
    assert preview_records(caplog) == []
