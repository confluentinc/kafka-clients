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

"""Tests of the config classes translated for their constants (CLAUDE.md,
Python Binding Conventions, Scope) and of the config-value enums
``SslClientAuth``, ``SecurityProtocol`` and ``GroupProtocol``."""

from __future__ import annotations

import enum
import re
from pathlib import Path

import pytest

import confluent_kafka
from confluent_kafka import IllegalArgumentError
from confluent_kafka.admin import AdminClientConfig
from confluent_kafka.common.config import (
    LogLevelConfig,
    SaslConfigs,
    SecurityConfig,
    SslClientAuth,
    SslConfigs,
    TopicConfig,
)
from confluent_kafka.common.security.auth import SecurityProtocol
import confluent_kafka.consumer
from confluent_kafka.consumer import ConsumerConfig, GroupProtocol
from confluent_kafka.producer import ProducerConfig

_JAVA = Path(__file__).resolve().parents[3] / "kafka/clients/src/main/java/org/apache/kafka"
_DEPRECATED = Path(__file__).resolve().parents[3] / "design/current/java-deprecated.txt"

_CLASSES = [
    (ProducerConfig, "clients/producer/ProducerConfig"),
    (ConsumerConfig, "clients/consumer/ConsumerConfig"),
    (AdminClientConfig, "clients/admin/AdminClientConfig"),
    (TopicConfig, "common/config/TopicConfig"),
    (SaslConfigs, "common/config/SaslConfigs"),
    (SslConfigs, "common/config/SslConfigs"),
    (SecurityConfig, "common/config/SecurityConfig"),
    (LogLevelConfig, "common/config/LogLevelConfig"),
]

_LITERAL = re.compile(r'public static final String (\w+)\s*=\s*"((?:[^"\\]|\\.)*)";')


@pytest.mark.parametrize(("cls", "java"), _CLASSES, ids=[c.__name__ for c, _ in _CLASSES])
def test_every_literal_constant_matches_java(cls: type, java: str) -> None:
    """Every ``public static final String`` that Java initializes with one
    literal is a class attribute with that value; a deprecated one is absent."""
    source = (_JAVA / f"{java}.java").read_text()
    fqn = "org.apache.kafka." + java.replace("/", ".")
    deprecated = {line.split("#", 1)[1] for line in _DEPRECATED.read_text().splitlines()
                  if line.startswith(fqn + "#")}
    found = _LITERAL.findall(source)
    assert found
    for name, value in found:
        if name in deprecated:
            assert not hasattr(cls, name), name
        else:
            assert getattr(cls, name) == value, name


def test_evaluated_constants() -> None:
    """Constants whose Java initializer is an expression are evaluated as Java does."""
    assert ProducerConfig.BOOTSTRAP_SERVERS_CONFIG == "bootstrap.servers"
    assert ProducerConfig.CONFIG_PROVIDERS_CONFIG == "config.providers"
    assert ConsumerConfig.DEFAULT_FETCH_MAX_BYTES == 50 * 1024 * 1024
    assert ConsumerConfig.DEFAULT_GROUP_PROTOCOL == "classic"
    assert ConsumerConfig.DEFAULT_ISOLATION_LEVEL == "read_uncommitted"
    assert ConsumerConfig.DEFAULT_GROUP_REMOTE_ASSIGNOR is None
    assert ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS is True
    # A List constant is a tuple in Java's order; a Set constant a frozenset.
    assert ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS == (
        "range", "roundrobin", "sticky", "cooperative-sticky")
    assert ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS[3] == "cooperative-sticky"
    assert isinstance(SslConfigs.RECONFIGURABLE_CONFIGS, frozenset)
    assert AdminClientConfig.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS == 300_000
    # Values Java's classes take from CommonClientConfigs, which is not translated.
    assert AdminClientConfig.DEFAULT_METADATA_RECOVERY_STRATEGY == "rebootstrap"
    assert ProducerConfig.SECURITY_PROVIDERS_CONFIG == "security.providers"
    assert ConsumerConfig.GROUP_ID_CONFIG == "group.id"
    assert SaslConfigs.DEFAULT_SASL_MECHANISM == "GSSAPI"
    assert SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR == 0.8
    assert SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM == "SunX509"
    assert SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM == "PKIX"
    assert "ssl.keystore.type" in SslConfigs.RECONFIGURABLE_CONFIGS
    assert LogLevelConfig.VALID_LOG_LEVELS == frozenset(
        {"FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"})


def test_deprecated_constants_absent() -> None:
    assert not hasattr(ProducerConfig, "PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG")
    assert ProducerConfig.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG == (
        "partitioner.adaptive.partitioning.enable")
    assert not hasattr(TopicConfig, "MESSAGE_DOWNCONVERSION_ENABLE_CONFIG")
    assert not hasattr(TopicConfig, "MESSAGE_DOWNCONVERSION_ENABLE_DOC")


def test_common_client_configs_is_not_translated() -> None:
    # Not @InterfaceAudience.Public in Kafka 4.4 (left out of the public API).
    assert not hasattr(confluent_kafka, "CommonClientConfigs")
    assert "CommonClientConfigs" not in confluent_kafka.__all__


def test_share_consumer_config_is_not_translated() -> None:
    # Not translated while the share consumer is not.
    assert not hasattr(confluent_kafka.consumer, "ShareConsumerConfig")


@pytest.mark.parametrize("cls", [ProducerConfig, ConsumerConfig, AdminClientConfig])
def test_constants_only_classes_are_not_instantiable(cls: type) -> None:
    with pytest.raises(TypeError) as e:
        cls({"bootstrap.servers": "x"})
    assert str(e.value) == (f"{cls.__name__} is not instantiable: its ConfigDef machinery "
                            "is not translated; a configuration is a dict of its keys")


@pytest.mark.parametrize("cls", [TopicConfig, SaslConfigs, SslConfigs,
                                 SecurityConfig, LogLevelConfig])
def test_classes_with_java_default_constructor_construct(cls: type) -> None:
    assert isinstance(cls(), cls)


def test_modules() -> None:
    """Each class is in the module of its Java package."""
    assert ProducerConfig.__module__ == "confluent_kafka.producer.producer_config"
    assert SecurityProtocol.__module__ == "confluent_kafka.common.security.auth.security_protocol"
    assert GroupProtocol.__module__ == "confluent_kafka.consumer.group_protocol"


# ---- SslClientAuth ---------------------------------------------------------------


def test_ssl_client_auth() -> None:
    assert issubclass(SslClientAuth, enum.Enum)
    assert [a.name for a in SslClientAuth] == ["REQUIRED", "REQUESTED", "NONE"]
    # Java's List.of(values()): a tuple in declaration order.
    assert SslClientAuth.VALUES == (SslClientAuth.REQUIRED, SslClientAuth.REQUESTED, SslClientAuth.NONE)
    assert SslClientAuth.VALUES[0] is SslClientAuth.REQUIRED
    assert str(SslClientAuth.REQUESTED) == "requested"
    assert SslClientAuth.for_config(key=None) is SslClientAuth.NONE
    assert SslClientAuth.for_config(key="required") is SslClientAuth.REQUIRED
    assert SslClientAuth.for_config(key="ReQuEsTeD") is SslClientAuth.REQUESTED
    assert SslClientAuth.for_config(key="other") is None
    with pytest.raises(TypeError):
        SslClientAuth.for_config("required")  # type: ignore[misc]


# ---- SecurityProtocol ------------------------------------------------------------


def test_security_protocol() -> None:
    assert issubclass(SecurityProtocol, enum.IntEnum)
    assert [(p.name, int(p)) for p in SecurityProtocol] == [
        ("PLAINTEXT", 0), ("SSL", 1), ("SASL_PLAINTEXT", 2), ("SASL_SSL", 3)]
    assert SecurityProtocol.names() == ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
    assert SecurityProtocol.for_id(id=3) is SecurityProtocol.SASL_SSL
    assert SecurityProtocol.for_id(id=9) is None
    assert SecurityProtocol.for_name(name="sasl_plaintext") is SecurityProtocol.SASL_PLAINTEXT
    assert str(SecurityProtocol.SSL) == "SSL"
    with pytest.raises(IllegalArgumentError) as e:
        SecurityProtocol.for_name(name="tls")
    assert str(e.value) == "No enum constant org.apache.kafka.common.security.auth.SecurityProtocol.TLS"
    with pytest.raises(TypeError) as t:
        SecurityProtocol.for_name(name=None)  # type: ignore[arg-type]
    assert str(t.value) == "name must not be None"
    with pytest.raises(TypeError):
        SecurityProtocol.for_id(1)  # type: ignore[misc]


# ---- GroupProtocol ---------------------------------------------------------------


def test_group_protocol() -> None:
    assert issubclass(GroupProtocol, enum.Enum)
    assert [(p.name, p.value) for p in GroupProtocol] == [
        ("CLASSIC", "CLASSIC"), ("CONSUMER", "CONSUMER")]
    assert GroupProtocol.of(name="consumer") is GroupProtocol.CONSUMER
    assert str(GroupProtocol.CLASSIC) == "CLASSIC"
    with pytest.raises(IllegalArgumentError) as e:
        GroupProtocol.of(name="share")
    assert str(e.value) == "No enum constant org.apache.kafka.clients.consumer.GroupProtocol.SHARE"
    with pytest.raises(TypeError) as t:
        GroupProtocol.of(name=None)  # type: ignore[arg-type]
    assert str(t.value) == "name must not be None"
    with pytest.raises(TypeError):
        GroupProtocol.of("classic")  # type: ignore[misc]
