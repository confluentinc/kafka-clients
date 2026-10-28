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

"""Every config key of the generated ``_config_types`` table through
``_config.prepare``, the entry point the three clients use: a value of the key's
``ConfigDef`` type converts to the string the core parses, and a value of the wrong
Python type raises ``ConfigError`` with Java's ``parseType`` message (CLAUDE.md,
Python Binding Conventions, Configuration; Implementation over the FFI, exception
2). The matrix is derived from the table, so a key a Java release adds is covered
without a new test. The smoke tests build each real client with one typed key of
every type, SSL and SASL keys included, and only check that nothing is refused on
the Python side (the core accepts or warns)."""

from __future__ import annotations

from typing import Any

import pytest

from confluent_kafka import _config, _config_types
from confluent_kafka._java import java_str
from confluent_kafka.admin import Admin
from confluent_kafka.common.config import ConfigError
from confluent_kafka.consumer import CloseOptions, KafkaConsumer
from confluent_kafka.producer import KafkaProducer

# A value of each type, and the text the core parses for it.
GOOD: dict[str, tuple[Any, str]] = {
    "INT": (7, "7"),
    "LONG": (7, "7"),
    "SHORT": (7, "7"),
    "DOUBLE": (1.5, "1.5"),
    "STRING": ("x", "x"),
    "PASSWORD": ("secret", "secret"),
    "BOOLEAN": (True, "true"),
    "LIST": (["a", "b"], "a,b"),
    "CLASS": (str, "builtins.str"),
}

# A value of the wrong Python type for each type, with Java's message.
BAD: dict[str, tuple[Any, str]] = {
    "INT": (42.5, "Expected value to be a 32-bit integer, but it was a builtins.float"),
    "LONG": (object(), "Expected value to be a 64-bit integer (long), but it was a builtins.object"),
    "SHORT": (1.5, "Expected value to be a 16-bit integer (short), but it was a builtins.float"),
    "DOUBLE": (object(), "Expected value to be a double, but it was a builtins.object"),
    "STRING": (object(), "Expected value to be a string, but it was a builtins.object"),
    "PASSWORD": (5, "Expected value to be a string, but it was a builtins.int"),
    "BOOLEAN": (1, "Expected value to be either true or false"),
    "LIST": (53, "Expected a comma separated list."),
    "CLASS": (5, "Expected a Class instance or class name."),
}

CASES: list[tuple[str, str, str]] = [
    (client, key, type_name)
    for client, table in (("producer", _config_types.PRODUCER),
                          ("consumer", _config_types.CONSUMER),
                          ("admin", _config_types.ADMIN))
    for key, type_name in sorted(table.items())
]
IDS = [f"{client}:{key}" for client, key, _ in CASES]


def test_the_table_covers_every_type_here() -> None:
    types = {type_name for _, _, type_name in CASES}
    assert types <= set(GOOD) and set(GOOD) == set(BAD)
    assert len(CASES) > 300


@pytest.mark.parametrize("client,key,type_name", CASES, ids=IDS)
def test_a_value_of_the_keys_type_reaches_the_core(client: Any, key: str, type_name: str) -> None:
    value, text = GOOD[type_name]
    copy, native = _config.prepare({key: value}, client=client)
    assert copy == {key: value}
    if key in _config._SERDE_KEYS[client]:
        # The client's own serde keys stay with the binding.
        assert key not in native
    else:
        assert native == {key: text}


@pytest.mark.parametrize("client,key,type_name", CASES, ids=IDS)
def test_a_value_of_the_wrong_type_is_rejected(client: Any, key: str, type_name: str) -> None:
    value, message = BAD[type_name]
    with pytest.raises(ConfigError) as exc:
        _config.prepare({key: value}, client=client)
    assert str(exc.value) == f"Invalid value {java_str(value)} for configuration {key}: {message}"
    assert exc.value.__cause__ is None


@pytest.mark.parametrize("client,key,type_name", CASES, ids=IDS)
def test_none_follows_the_keys_validator(client: Any, key: str, type_name: str) -> None:
    # A None for a key whose validator rejects null raises Java's ConfigException
    # (ConfigDef.parseValue runs the validator on null); any other None reaches
    # the core as not set, so the core's default applies.
    refused = getattr(_config_types, f"{client.upper()}_NULL").get(key)
    if refused is None or key in _config._SERDE_KEYS[client]:
        copy, native = _config.prepare({key: None}, client=client)
        assert copy == {key: None} and native == {}
        return
    form, message = refused
    with pytest.raises(ConfigError) as exc:
        _config.prepare({key: None}, client=client)
    if form == "named":
        assert str(exc.value) == f"Invalid value None for configuration {key}: {message}"
    else:
        assert str(exc.value) == message


# One key of every type, SSL and SASL keys included; the values are in Java's
# validators' ranges.
TYPED_KEYS: dict[str, Any] = {
    "request.timeout.ms": 30000,                       # INT
    "metadata.max.age.ms": 300000,                     # LONG
    "sasl.login.refresh.min.period.seconds": 60,       # SHORT
    "sasl.login.refresh.window.factor": 0.8,           # DOUBLE
    "sasl.mechanism": "GSSAPI",                        # STRING
    "ssl.keystore.password": "keystore-secret",        # PASSWORD
    "ssl.enabled.protocols": ["TLSv1.2", "TLSv1.3"],   # LIST
    "ssl.engine.factory.class": str,                   # CLASS
    "client.id": "typed-keys",
}


def test_producer_takes_a_typed_key_of_every_type() -> None:
    producer: KafkaProducer[bytes, bytes] = KafkaProducer(configs={
        "bootstrap.servers": "localhost:1", "enable.idempotence": True, **TYPED_KEYS})
    producer.close()


def test_consumer_takes_a_typed_key_of_every_type() -> None:
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs={
        "bootstrap.servers": "localhost:1", "group.protocol": "consumer", "group.id": "typed",
        "enable.auto.commit": False, **TYPED_KEYS})
    consumer.close(option=CloseOptions.timeout(timeout=0))


def test_admin_takes_a_typed_key_of_every_type() -> None:
    admin = Admin.create(configs={"bootstrap.servers": "localhost:1", "retries": 0,
                                  "enable.metrics.push": False, **TYPED_KEYS})
    admin.close()
