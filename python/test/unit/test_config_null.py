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

"""An explicit ``None`` config value is Java's ``null``: ``ConfigDef.parseValue``
runs the key's validator on it, so a key whose validator rejects ``null``
raises Java's ``ConfigException``; any other ``None`` reaches the core as not
set (CLAUDE.md, Python Binding Conventions, Configuration). One test per
validator kind, with the message Java's validator builds."""

from __future__ import annotations

from typing import Any

import pytest

from confluent_kafka import _config
from confluent_kafka.admin import Admin
from confluent_kafka.common.config import ConfigError
from confluent_kafka.consumer import KafkaConsumer
from confluent_kafka.producer import KafkaProducer


def _refused(client: Any, key: str) -> str:
    with pytest.raises(ConfigError) as exc:
        _config.prepare({key: None}, client=client)
    return str(exc.value)


@pytest.mark.parametrize("client,key", [
    ("producer", "linger.ms"),                       # atLeast(0)
    ("producer", "max.in.flight.requests.per.connection"),  # atLeast(1)
    ("consumer", "max.poll.records"),                # atLeast(1)
    ("consumer", "request.timeout.ms"),              # atLeast(0)
    ("admin", "retries"),                            # between(0, Integer.MAX_VALUE)
    ("producer", "sasl.login.refresh.window.factor"),  # Range.between(0.5, 1.0)
    ("producer", "compression.gzip.level"),          # CompressionType.GZIP.levelValidator()
    ("producer", "compression.zstd.level"),          # CompressionType.ZSTD.levelValidator()
])
def test_range(client: Any, key: str) -> None:
    assert _refused(client, key) == (
        f"Invalid value None for configuration {key}: Value must be non-null")


@pytest.mark.parametrize("client,key,valid", [
    ("producer", "acks", "all, -1, 0, 1"),
    ("producer", "compression.type", "none, gzip, snappy, lz4, zstd"),
    ("consumer", "isolation.level", "read_committed, read_uncommitted"),
    ("admin", "client.dns.lookup", "use_all_dns_ips, resolve_canonical_bootstrap_servers_only"),
    ("consumer", "metrics.recording.level", "INFO, DEBUG, TRACE"),
])
def test_valid_string(client: Any, key: str, valid: str) -> None:
    assert _refused(client, key) == (
        f"Invalid value None for configuration {key}: String must be one of: {valid}")


@pytest.mark.parametrize("client,key,valid", [
    ("admin", "security.protocol", "SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT"),
    ("consumer", "group.protocol", "CONSUMER, CLASSIC"),
    ("producer", "metadata.recovery.strategy", "REBOOTSTRAP, NONE"),
    ("producer", "buffer.memory.allocation.strategy", "FULL, INCREMENTAL"),
    ("consumer", "sasl.oauthbearer.assertion.algorithm", "ES256, RS256"),
])
def test_case_insensitive_valid_string(client: Any, key: str, valid: str) -> None:
    # Java joins the upper-cased values of a HashSet (Collectors.toSet()), so in
    # its iteration order, the order the core's own message uses.
    assert _refused(client, key) == (
        f"Invalid value None for configuration {key}: "
        f"String must be one of (case insensitive): {valid}")


@pytest.mark.parametrize("client,key", [
    ("producer", "bootstrap.servers"),   # ValidList.anyNonDuplicateValues(false, false)
    ("consumer", "metric.reporters"),    # ValidList.anyNonDuplicateValues(true, false)
])
def test_valid_list_refusing_null(client: Any, key: str) -> None:
    assert _refused(client, key) == f"Configuration '{key}' values must not be null."


@pytest.mark.parametrize("client,key,tail", [
    ("consumer", "auto.offset.reset",
     "The value must be either 'earliest', 'latest', 'none' or of the format "
     "'by_duration:<PnDTnHnMn.nS.>'."),
    ("consumer", "share.acknowledgement.mode",
     "The value must either be 'implicit' or 'explicit'."),
    ("consumer", "share.acquire.mode",
     "The value must either be 'batch_optimized' or 'record_limit'."),
])
def test_custom_validator(client: Any, key: str, tail: str) -> None:
    # The validator's parser rejects null and its catch throws
    # ConfigException(name, value, "Invalid value `" + v + "` for configuration " + name + ...).
    assert _refused(client, key) == (
        f"Invalid value None for configuration {key}: "
        f"Invalid value `None` for configuration {key}. {tail}")


@pytest.mark.parametrize("client,key", [
    ("producer", "transactional.id"),   # NonEmptyString: allows null
    ("producer", "client.id"),          # no validator
    ("consumer", "group.id"),           # no validator
    ("admin", "sasl.jaas.config"),      # no validator
    ("producer", "key.serializer"),     # the role's serde key (Serialization)
    ("consumer", "value.deserializer"),
])
def test_none_allowed_is_not_set(client: Any, key: str) -> None:
    copy, native = _config.prepare({key: None}, client=client)
    assert copy == {key: None} and native == {}


def test_the_clients_refuse_a_null_their_validator_rejects() -> None:
    """Each client's constructor raises the error before the core sees the configs."""
    with pytest.raises(ConfigError) as p:
        KafkaProducer(configs={"bootstrap.servers": "localhost:1", "acks": None})
    assert str(p.value) == ("Invalid value None for configuration acks: "
                            "String must be one of: all, -1, 0, 1")
    with pytest.raises(ConfigError) as c:
        KafkaConsumer(configs={"bootstrap.servers": "localhost:1", "max.poll.records": None})
    assert str(c.value) == ("Invalid value None for configuration max.poll.records: "
                            "Value must be non-null")
    with pytest.raises(ConfigError) as a:
        Admin.create(configs={"bootstrap.servers": None})
    assert str(a.value) == "Configuration 'bootstrap.servers' values must not be null."
