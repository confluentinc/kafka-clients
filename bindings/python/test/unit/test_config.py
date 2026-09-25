# Copyright 2025 Confluent Inc.
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

"""Tests of ``confluent_kafka._config``: Java's ``ConfigDef`` / ``AbstractConfig``
parsing of ``configs``.

The config classes are not translated (CLAUDE.md, Python Binding Conventions,
Scope), so there is no Java test of this module; the ``ConfigDef.parseType`` /
``convertToString`` cases of Java's ``ConfigDefTest`` are translated
(``testBasicTypes``, ``testBadInputs``, ``testConvertValueToString*``), with
Java's ``ConfigException`` messages. The validator, documentation, dependents
and ``ConfigDef``-building tests of ``ConfigDefTest`` test the config classes
themselves and are not.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pytest

from confluent_kafka import IllegalArgumentError, _config, _config_types
from confluent_kafka._java import java_str
from confluent_kafka.common.config import ConfigError

_LOGGER = "confluent_kafka.common.config"

# --------------------------------------------------------------------------- #
# ConfigDefTest.testBasicTypes
# --------------------------------------------------------------------------- #


def test_basic_types() -> None:
    assert _config.coerce("a", "1   ", "INT") == 1
    assert _config.coerce("b", 2, "LONG") == 2
    assert _config.coerce("c", "hello", "STRING") == "hello"
    assert _config.coerce("d", " a , b, c", "LIST") == ["a", "b", "c"]
    assert _config.coerce("e", 42.5, "DOUBLE") == 42.5
    assert _config.coerce("f", str, "CLASS") is str
    assert _config.coerce("g", "true", "BOOLEAN") is True
    assert _config.coerce("h", "FalSE", "BOOLEAN") is False
    assert _config.coerce("i", "TRUE", "BOOLEAN") is True
    assert _config.coerce("j", "password", "PASSWORD") == "password"


def test_equivalent_forms() -> None:
    # "true" equals True, "1000" equals 1000 (CLAUDE.md, Configuration).
    assert _config.coerce("k", True, "BOOLEAN") is _config.coerce("k", "true", "BOOLEAN")
    assert _config.coerce("k", 1000, "INT") == _config.coerce("k", "1000", "INT") == 1000
    assert _config.coerce("k", "+7", "LONG") == 7
    assert _config.coerce("k", "32767", "SHORT") == 32767
    assert _config.coerce("k", 3, "DOUBLE") == 3.0
    assert _config.coerce("k", "1e3", "DOUBLE") == 1000.0
    assert _config.coerce("k", ("a", "b"), "LIST") == ["a", "b"]
    assert _config.coerce("k", "", "LIST") == []
    assert _config.coerce("k", "  x.Y ", "CLASS") == "x.Y"
    assert _config.coerce("k", None, "INT") is None


# --------------------------------------------------------------------------- #
# ConfigDefTest.testBadInputs, with Java's messages
# --------------------------------------------------------------------------- #

_BAD_INPUTS: list[tuple[str, object, str]] = [
    ("INT", "hello", "Not a number of type INT"),
    ("INT", "42.5", "Not a number of type INT"),
    ("INT", 42.5, "Expected value to be a 32-bit integer, but it was a builtins.float"),
    ("INT", 2**63 - 1, "Not a number of type INT"),
    ("INT", str(2**63 - 1), "Not a number of type INT"),
    ("INT", object(), "Expected value to be a 32-bit integer, but it was a builtins.object"),
    ("INT", True, "Expected value to be a 32-bit integer, but it was a builtins.bool"),
    ("INT", "1_000", "Not a number of type INT"),
    ("LONG", "hello", "Not a number of type LONG"),
    ("LONG", "42.5", "Not a number of type LONG"),
    ("LONG", str(2**63 - 1) + "00", "Not a number of type LONG"),
    ("LONG", object(), "Expected value to be a 64-bit integer (long), but it was a builtins.object"),
    ("SHORT", "32768", "Not a number of type SHORT"),
    ("SHORT", 1.5, "Expected value to be a 16-bit integer (short), but it was a builtins.float"),
    ("DOUBLE", "hello", "Not a number of type DOUBLE"),
    ("DOUBLE", "inf", "Not a number of type DOUBLE"),
    ("DOUBLE", object(), "Expected value to be a double, but it was a builtins.object"),
    ("DOUBLE", False, "Expected value to be a double, but it was a builtins.bool"),
    ("STRING", object(), "Expected value to be a string, but it was a builtins.object"),
    ("PASSWORD", 5, "Expected value to be a string, but it was a builtins.int"),
    ("LIST", 53, "Expected a comma separated list."),
    ("LIST", object(), "Expected a comma separated list."),
    ("BOOLEAN", "hello", "Expected value to be either true or false"),
    ("BOOLEAN", "truee", "Expected value to be either true or false"),
    ("BOOLEAN", "fals", "Expected value to be either true or false"),
    ("BOOLEAN", 1, "Expected value to be either true or false"),
    ("CLASS", 5, "Expected a Class instance or class name."),
]


@pytest.mark.parametrize("type_name,value,message", _BAD_INPUTS)
def test_bad_inputs(type_name: str, value: object, message: str) -> None:
    with pytest.raises(ConfigError) as exc:
        _config.coerce("name", value, type_name)
    assert str(exc.value) == f"Invalid value {java_str(value)} for configuration name: {message}"


def test_bad_class_name_is_rejected_where_the_class_is_loaded() -> None:
    # ConfigDefTest.testBadInputs(Type.CLASS, "ClassDoesNotExist"): a serde key
    # is loaded by the serde route, which raises ConfigDef's message.
    from confluent_kafka.common.serialization import bytes_serializer
    from confluent_kafka.common.serialization._supply import resolve_serde

    originals, _ = _config.prepare({"key.serializer": "ClassDoesNotExist"}, client="producer")
    with pytest.raises(ConfigError) as exc:
        resolve_serde(None, originals, "key.serializer", is_key=True, default=bytes_serializer())
    assert str(exc.value) == (
        "Invalid value ClassDoesNotExist for configuration key.serializer: "
        "Class ClassDoesNotExist could not be found.")


# --------------------------------------------------------------------------- #
# ConfigDefTest.testConvertValueToString*
# --------------------------------------------------------------------------- #


class _Nested:
    pass


def test_convert_value_to_string() -> None:
    assert _config.convert_to_string(True, "BOOLEAN") == "true"
    assert _config.convert_to_string(32767, "SHORT") == "32767"
    assert _config.convert_to_string(2147483647, "INT") == "2147483647"
    assert _config.convert_to_string(9223372036854775807, "LONG") == "9223372036854775807"
    assert _config.convert_to_string(3.125, "DOUBLE") == "3.125"
    assert _config.convert_to_string(1.7976931348623157e308, "DOUBLE") == "1.7976931348623157E308"
    assert _config.convert_to_string(102400000.0, "DOUBLE") == "1.024E8"
    assert _config.convert_to_string(-102400000.0, "DOUBLE") == "-1.024E8"
    assert _config.convert_to_string("foobar", "STRING") == "foobar"
    assert _config.convert_to_string("foobar", "PASSWORD") == "foobar"
    assert _config.convert_to_string(["a", "bc", "d"], "LIST") == "a,bc,d"
    assert _config.convert_to_string(_Nested, "CLASS") == f"{__name__}._Nested"
    assert _config.convert_to_string("foobar", None) == "foobar"
    for type_name in ("BOOLEAN", "SHORT", "INT", "LONG", "DOUBLE", "STRING", "PASSWORD", "LIST",
                      "CLASS", None):
        assert _config.convert_to_string(None, type_name) is None


# --------------------------------------------------------------------------- #
# prepare: the keys, the core's string map, the rejected keys
# --------------------------------------------------------------------------- #


def test_prepare_coerces_known_keys_for_the_core() -> None:
    originals, native = _config.prepare(
        {"bootstrap.servers": " a:9092 , b:9092", "linger.ms": 5, "enable.idempotence": "TRUE",
         "sasl.login.refresh.window.factor": 0.8, "acks": " all ", "unknown.key": 1,
         "key.serializer": "x.Y", "transactional.id": None}, client="producer")
    assert native == {"bootstrap.servers": "a:9092,b:9092", "linger.ms": "5",
                      "enable.idempotence": "true", "sasl.login.refresh.window.factor": "0.8",
                      "acks": "all", "unknown.key": "1"}
    # The user's configs, as given, for the serde route.
    assert originals["key.serializer"] == "x.Y" and originals["transactional.id"] is None


def test_prepare_uses_the_client_config_def() -> None:
    assert "max.poll.records" in _config_types.CONSUMER
    assert "max.poll.records" not in _config_types.PRODUCER
    with pytest.raises(ConfigError) as exc:
        _config.prepare({"max.poll.records": "many"}, client="consumer")
    assert str(exc.value) == (
        "Invalid value many for configuration max.poll.records: Not a number of type INT")
    # Not a producer key: accepted as given.
    assert _config.prepare({"max.poll.records": "many"}, client="producer")[1] == {
        "max.poll.records": "many"}


def test_key_must_be_a_string() -> None:
    with pytest.raises(ConfigError) as exc:
        _config.prepare({1: "x"}, client="producer")  # type: ignore[dict-item]
    assert str(exc.value) == "Invalid value x for configuration 1: Key must be a string."
    with pytest.raises(TypeError):
        _config.prepare([("a", "b")], client="producer")  # type: ignore[arg-type]


@pytest.mark.parametrize("client", ["producer", "consumer"])
def test_interceptor_classes_raise(client: _config.Client) -> None:
    with pytest.raises(ConfigError) as exc:
        _config.prepare({"interceptor.classes": "com.example.Interceptor"}, client=client)
    assert str(exc.value) == (
        "Invalid value com.example.Interceptor for configuration interceptor.classes: "
        "The client runs no interceptors.")
    # Java's default, the empty list, names no interceptor.
    assert _config.prepare({"interceptor.classes": ""}, client=client)[1] == {
        "interceptor.classes": ""}


def test_partitioner_class_raises_but_for_java_round_robin() -> None:
    with pytest.raises(ConfigError) as exc:
        _config.prepare({"partitioner.class": "com.example.MyPartitioner"}, client="producer")
    assert str(exc.value) == (
        "Invalid value com.example.MyPartitioner for configuration partitioner.class: "
        "The client runs no custom partitioner; "
        "org.apache.kafka.clients.producer.RoundRobinPartitioner is the only partitioner class "
        "it has.")
    with pytest.raises(ConfigError):
        _config.prepare({"partitioner.class": _Nested}, client="producer")
    round_robin = "org.apache.kafka.clients.producer.RoundRobinPartitioner"
    assert _config.prepare({"partitioner.class": round_robin}, client="producer")[1] == {
        "partitioner.class": round_robin}


def test_old_client_keys_are_accepted_and_logged_as_unused(
        caplog: pytest.LogCaptureFixture) -> None:
    configs = {"error_cb": print, "on_delivery": print, "logger": "x", "bootstrap.servers": "b"}
    originals, native = _config.prepare(configs, client="producer")
    assert set(native) == {"error_cb", "on_delivery", "logger", "bootstrap.servers"}
    with caplog.at_level(logging.INFO, logger=_LOGGER):
        _config.log_unused(originals, client="producer")
    records = [r for r in caplog.records if r.name == _LOGGER]
    assert len(records) == 1 and records[0].levelno == logging.INFO
    assert records[0].getMessage() == (
        "These configurations '[error_cb, logger, on_delivery]' were supplied but are not "
        "used yet.")


def test_keys_a_serde_reads_are_not_unused(caplog: pytest.LogCaptureFixture) -> None:
    originals, _ = _config.prepare(
        {"key.serializer.encoding": "UTF-16", "other.key": 1}, client="producer")
    originals.get("key.serializer.encoding")
    with caplog.at_level(logging.INFO, logger=_LOGGER):
        _config.log_unused(originals, client="producer")
    messages = [r.getMessage() for r in caplog.records if r.name == _LOGGER]
    assert messages == ["These configurations '[other.key]' were supplied but are not used yet."]


def test_nothing_unused_logs_nothing(caplog: pytest.LogCaptureFixture) -> None:
    originals, _ = _config.prepare({"bootstrap.servers": "b", "group.id": "g"}, client="consumer")
    with caplog.at_level(logging.INFO, logger=_LOGGER):
        _config.log_unused(originals, client="consumer")
    assert [r for r in caplog.records if r.name == _LOGGER] == []


def test_generated_key_types_are_java_s() -> None:
    assert _config_types.PRODUCER["acks"] == "STRING"
    assert _config_types.PRODUCER["buffer.memory"] == "LONG"
    assert _config_types.PRODUCER["ssl.keystore.password"] == "PASSWORD"
    assert _config_types.CONSUMER["group.id"] == "STRING"
    assert _config_types.CONSUMER["key.deserializer"] == "CLASS"
    assert _config_types.CONSUMER["isolation.level"] == "STRING"


# --------------------------------------------------------------------------- #
# duration_to_ms
# --------------------------------------------------------------------------- #


def test_duration_to_ms() -> None:
    assert _config.duration_to_ms(None, default_ms=60000) == 60000
    assert _config.duration_to_ms(1.5, default_ms=0) == 1500
    assert _config.duration_to_ms(timedelta(seconds=2), default_ms=0) == 2000
    assert _config.duration_to_ms(0, default_ms=7) == 0
    for negative in (-0.001, timedelta(milliseconds=-1)):
        with pytest.raises(IllegalArgumentError) as exc:
            _config.duration_to_ms(negative, default_ms=0)
        assert str(exc.value) == "The timeout cannot be negative."
