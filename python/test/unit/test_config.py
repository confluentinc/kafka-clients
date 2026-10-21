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

import math
from datetime import timedelta

import pytest

from confluent_kafka import IllegalArgumentError, _config, _config_types
from confluent_kafka._java import java_str
from confluent_kafka.common.config import ConfigError

# --------------------------------------------------------------------------- #
# ConfigDefTest.testBasicTypes
# --------------------------------------------------------------------------- #


def test_basic_types() -> None:
    # Java's testBasicTypes values by Python type; the Python-type check keeps
    # a value as given, the core parses and trims it.
    assert _config.check_type("a", "1   ", "INT") == "1   "
    assert _config.check_type("b", 2, "LONG") == 2
    assert _config.check_type("c", "hello", "STRING") == "hello"
    assert _config.check_type("d", [" a ", "b"], "LIST") == [" a ", "b"]
    assert _config.check_type("e", 42.5, "DOUBLE") == 42.5
    assert _config.check_type("f", str, "CLASS") is str
    assert _config.check_type("g", True, "BOOLEAN") is True
    assert _config.check_type("h", "FalSE", "BOOLEAN") == "FalSE"
    assert _config.check_type("j", "password", "PASSWORD") == "password"
    assert _config.check_type("k", None, "INT") is None
    assert _config.check_type("k", ("a", "b"), "LIST") == ["a", "b"]
    assert _config.check_type("k", 3, "DOUBLE") == 3


# --------------------------------------------------------------------------- #
# ConfigDefTest.testBadInputs: the wrong-Python-type cases, with Java's
# messages. The value checks (number text, ranges, boolean text, class names)
# are the core's (CLAUDE.md, Implementation over the FFI, exception 2), so a
# string such as "hello" for an INT passes through here.
# --------------------------------------------------------------------------- #

_BAD_INPUTS: list[tuple[str, object, str]] = [
    ("INT", 42.5, "Expected value to be a 32-bit integer, but it was a builtins.float"),
    ("INT", object(), "Expected value to be a 32-bit integer, but it was a builtins.object"),
    ("INT", True, "Expected value to be a 32-bit integer, but it was a builtins.bool"),
    ("LONG", object(), "Expected value to be a 64-bit integer (long), but it was a builtins.object"),
    ("SHORT", 1.5, "Expected value to be a 16-bit integer (short), but it was a builtins.float"),
    ("DOUBLE", object(), "Expected value to be a double, but it was a builtins.object"),
    ("DOUBLE", False, "Expected value to be a double, but it was a builtins.bool"),
    ("STRING", object(), "Expected value to be a string, but it was a builtins.object"),
    ("PASSWORD", 5, "Expected value to be a string, but it was a builtins.int"),
    ("LIST", 53, "Expected a comma separated list."),
    ("LIST", object(), "Expected a comma separated list."),
    ("BOOLEAN", 1, "Expected value to be either true or false"),
    ("CLASS", 5, "Expected a Class instance or class name."),
]


@pytest.mark.parametrize("type_name,value,message", _BAD_INPUTS)
def test_bad_inputs(type_name: str, value: object, message: str) -> None:
    with pytest.raises(ConfigError) as exc:
        _config.check_type("name", value, type_name)
    assert str(exc.value) == f"Invalid value {java_str(value)} for configuration name: {message}"
    # ConfigDef.parseType throws a new ConfigException: getCause() is null.
    assert exc.value.__cause__ is None


@pytest.mark.parametrize("type_name,value", [
    ("INT", "hello"), ("INT", "42.5"), ("INT", 2**63 - 1), ("LONG", "xxx"),
    ("SHORT", "32768"), ("DOUBLE", "inf"), ("BOOLEAN", "truee"), ("LIST", "a,b"),
    ("CLASS", "ClassDoesNotExist"),
])
def test_value_checks_are_the_cores(type_name: str, value: object) -> None:
    # Ranges, number and boolean text and class names are checked by the core
    # (Implementation over the FFI, exception 2): Python passes the value on
    # for the core's ConfigError. Only the Python type is Python's check, so a
    # str is accepted for every type and an out-of-range int for an INT key.
    assert _config.check_type("name", value, type_name) == value


def test_java_number_parsers_report_java_s_messages() -> None:
    from confluent_kafka._java import parse_int, parse_long, parse_long_string

    for parse, text, message in [
        (lambda s: parse_int(s), "x", 'For input string: "x"'),
        (lambda s: parse_int(s), "3000000000", 'For input string: "3000000000"'),
        (lambda s: parse_int(s, 16), "40000", 'Value out of range. Value:"40000" Radix:10'),
        (parse_long_string, "", 'For input string: ""'),
        (parse_long_string, "1_0", 'For input string: "1_0"'),
        # The CharSequence overload (UUID.fromString) reports the index.
        (lambda s: parse_long(s, 0, len(s), 16), "1g", 'Error at index 1 in: "1g"'),
    ]:
        with pytest.raises(IllegalArgumentError) as exc:
            parse(text)
        assert str(exc.value) == message
    assert parse_int("-32768", 16) == -32768 and parse_long_string("+9") == 9


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
    assert exc.value.__cause__ is None
    # Java loads the trimmed name and prints the value as given.
    originals, _ = _config.prepare({"key.serializer": " no.Such "}, client="producer")
    with pytest.raises(ConfigError) as exc:
        resolve_serde(None, originals, "key.serializer", is_key=True, default=bytes_serializer())
    assert str(exc.value) == (
        "Invalid value  no.Such  for configuration key.serializer: Class  no.Such  could not be "
        "found.")


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


def test_prepare_converts_known_keys_for_the_core() -> None:
    originals, native = _config.prepare(
        {"bootstrap.servers": " a:9092 , b:9092", "linger.ms": 5, "enable.idempotence": "TRUE",
         "sasl.login.refresh.window.factor": 0.8, "acks": " all ", "unknown.key": 1,
         "key.serializer": "x.Y", "transactional.id": None}, client="producer")
    # Strings are passed as given: the core trims and parses them.
    assert native == {"bootstrap.servers": " a:9092 , b:9092", "linger.ms": "5",
                      "enable.idempotence": "TRUE", "sasl.login.refresh.window.factor": "0.8",
                      "acks": " all ", "unknown.key": "1"}
    _, native = _config.prepare({"bootstrap.servers": ["a:9092", "b:9092"],
                                 "enable.idempotence": True}, client="producer")
    assert native == {"bootstrap.servers": "a:9092,b:9092", "enable.idempotence": "true"}
    # The user's configs, as given, for the serde route.
    assert originals["key.serializer"] == "x.Y" and originals["transactional.id"] is None


def test_a_given_serde_argument_replaces_its_config_key() -> None:
    # ProducerConfig.appendSerializerToConfig / ConsumerConfig.appendDeserializerToConfig:
    # the argument's class replaces the key before ConfigDef parses it.
    instance = object()
    for client, key in (("producer", "key.serializer"), ("producer", "value.serializer"),
                        ("consumer", "key.deserializer"), ("consumer", "value.deserializer")):
        with pytest.raises(ConfigError) as exc:
            _config.prepare({key: instance}, client=client)  # type: ignore[arg-type]
        assert str(exc.value) == (
            f"Invalid value {instance} for configuration {key}: "
            "Expected a Class instance or class name.")
        originals, native = _config.prepare({key: instance}, client=client,  # type: ignore[arg-type]
                                            given_serdes=[key])
        assert native == {} and originals[key] is instance


def test_bad_number_text_reaches_the_core() -> None:
    # The number text is the core's to check (Implementation over the FFI,
    # exception 2); a real client reaches it at construction, no broker needed.
    # The core omits Java's ": Not a number of type INT|LONG" suffix (ConfigDef
    # line 789), so only Java's prefix is asserted.
    from confluent_kafka.consumer import KafkaConsumer
    from confluent_kafka.producer import KafkaProducer

    with pytest.raises(ConfigError) as exc:
        KafkaConsumer(configs={"bootstrap.servers": "localhost:1", "group.id": "g",
                               "max.poll.records": "many"})
    assert str(exc.value).startswith("Invalid value many for configuration max.poll.records")
    with pytest.raises(ConfigError) as exc:
        KafkaProducer(configs={"bootstrap.servers": "localhost:1", "linger.ms": "abc"})
    assert str(exc.value).startswith("Invalid value abc for configuration linger.ms")


def test_unsupported_sasl_mechanism_is_rejected_by_the_core() -> None:
    # PLAIN is the only SASL mechanism the core implements: under a SASL
    # security.protocol any other one, Java's GSSAPI default included, fails
    # the client's construction with the core's LocalUnsupportedOperationError.
    from confluent_kafka import UnsupportedOperationError
    from confluent_kafka.producer import KafkaProducer

    with pytest.raises(UnsupportedOperationError) as exc:
        KafkaProducer(configs={"bootstrap.servers": "localhost:1",
                               "security.protocol": "SASL_PLAINTEXT",
                               "sasl.mechanism": "SCRAM-SHA-256"})
    assert str(exc.value) == (
        "SASL mechanism 'SCRAM-SHA-256' is not supported by this client; only PLAIN is")
    with pytest.raises(UnsupportedOperationError) as exc:
        KafkaProducer(configs={"bootstrap.servers": "localhost:1",
                               "security.protocol": "SASL_SSL"})
    assert str(exc.value) == (
        "SASL mechanism 'GSSAPI' is not supported by this client; only PLAIN is")


def test_prepare_uses_the_client_config_def() -> None:
    assert "max.poll.records" in _config_types.CONSUMER
    assert "max.poll.records" not in _config_types.PRODUCER
    # A wrong Python type for a consumer key is Python's ConfigError.
    with pytest.raises(ConfigError) as exc:
        _config.prepare({"max.poll.records": 1.5}, client="consumer")
    assert str(exc.value) == (
        "Invalid value 1.5 for configuration max.poll.records: "
        "Expected value to be a 32-bit integer, but it was a builtins.float")
    # A bad text is the core's to report: passed as given.
    assert _config.prepare({"max.poll.records": "many"}, client="consumer")[1] == {
        "max.poll.records": "many"}
    # Not a producer key: accepted as given.
    assert _config.prepare({"max.poll.records": 1.5}, client="producer")[1] == {
        "max.poll.records": "1.5"}


def test_key_must_be_a_string() -> None:
    with pytest.raises(ConfigError) as exc:
        _config.prepare({1: "x"}, client="producer")  # type: ignore[dict-item]
    assert str(exc.value) == "Invalid value x for configuration 1: Key must be a string."
    with pytest.raises(TypeError):
        _config.prepare([("a", "b")], client="producer")  # type: ignore[arg-type]


@pytest.mark.parametrize("client", ["producer", "consumer"])
def test_interceptor_and_partitioner_keys_reach_the_core(client: _config.Client) -> None:
    # Not core keys, so unknown keys, which only the core reports (CLAUDE.md,
    # Python Binding Conventions, Configuration); the core's own
    # partitioner.type is passed verbatim.
    configs = {"interceptor.classes": "com.example.Interceptor",
               "partitioner.class": "com.example.MyPartitioner",
               "partitioner.type": "RoundRobinPartitioner"}
    assert _config.prepare(configs, client=client)[1] == configs


def test_only_the_own_role_serde_keys_stay_with_the_binding() -> None:
    # Configuration: a producer keeps its serializer keys and their encodings,
    # a consumer the deserializer ones; every other key reaches the core, the
    # other role's serde keys included (as unknown keys the core reports).
    serializer_keys = {"key.serializer": "x.S", "value.serializer": "x.S",
                       "serializer.encoding": "UTF-16", "key.serializer.encoding": "ascii",
                       "value.serializer.encoding": "UTF-16BE"}
    deserializer_keys = {"key.deserializer": "x.D", "value.deserializer": "x.D",
                         "deserializer.encoding": "UTF-16", "key.deserializer.encoding": "ascii",
                         "value.deserializer.encoding": "UTF-16BE"}
    configs = {**serializer_keys, **deserializer_keys}
    originals, native = _config.prepare(configs, client="producer")
    assert native == deserializer_keys and dict(originals) == configs
    originals, native = _config.prepare(configs, client="consumer")
    assert native == serializer_keys and dict(originals) == configs


def test_a_serializer_encoding_reaches_configure_not_the_core() -> None:
    from confluent_kafka.common.serialization import bytes_serializer
    from confluent_kafka.common.serialization._supply import resolve_serde

    originals, native = _config.prepare(
        {"value.serializer": "confluent_kafka.common.serialization._string_serializer."
                             "StringSerializer",
         "value.serializer.encoding": "UTF-16BE"}, client="producer")
    assert native == {}
    serializer = resolve_serde(None, originals, "value.serializer", is_key=False,
                               default=bytes_serializer())
    assert serializer("t", "a") == "a".encode("utf-16-be")


def test_old_client_keys_are_accepted() -> None:
    configs = {"error_cb": print, "on_delivery": print, "logger": "x", "bootstrap.servers": "b"}
    _, native = _config.prepare(configs, client="producer")
    assert set(native) == {"error_cb", "on_delivery", "logger", "bootstrap.servers"}


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


def test_duration_to_ms_reads_a_timedelta_exactly() -> None:
    # Java's Duration.toMillis() is integer arithmetic. timedelta.total_seconds()
    # goes through a float, which makes timedelta.max one millisecond too long.
    td = timedelta.max
    exact = (td.days * 86_400 + td.seconds) * 1_000 + td.microseconds // 1_000
    assert exact == 86_399_999_999_999_999
    assert _config.duration_to_ms(td, default_ms=0) == exact
    # Rounded down to the millisecond, as toMillis().
    assert _config.duration_to_ms(timedelta(milliseconds=1), default_ms=0) == 1
    assert _config.duration_to_ms(timedelta(microseconds=1_999), default_ms=0) == 1
    assert _config.duration_to_ms(timedelta(microseconds=999), default_ms=0) == 0
    assert _config.duration_to_ms(0.0019999, default_ms=0) == 1
    assert _config.duration_to_ms(0.0009, default_ms=0) == 0


def test_duration_to_ms_rejects_what_does_not_fit_a_long() -> None:
    # Beyond Long.MAX_VALUE milliseconds Java's toMillis() throws
    # ArithmeticException; here OverflowError, infinity included, and NaN
    # ValueError, before the FFI (CLAUDE.md, Python Binding Conventions,
    # Signatures, Timeouts). A float number of seconds cannot hold
    # Long.MAX_VALUE ms: 9223372036854776.0 s is 2**63 ms, the first refused,
    # and the float below it the largest accepted.
    assert _config.duration_to_ms(9_223_372_036_854_774.0, default_ms=0) == (
        9_223_372_036_854_773_760)
    for too_long, millis in ((9_223_372_036_854_776.0, 2**63),
                             (1e300, math.floor(1e300 * 1000.0))):
        with pytest.raises(OverflowError) as exc:
            _config.duration_to_ms(too_long, default_ms=0)
        assert str(exc.value) == f"timeout of {millis} ms does not fit a signed 64-bit integer"
    for infinite in (float("inf"), float("-inf")):
        with pytest.raises(OverflowError) as exc:
            _config.duration_to_ms(infinite, default_ms=0)
        assert str(exc.value) == "cannot convert float infinity to integer"
    with pytest.raises(ValueError) as exc:
        _config.duration_to_ms(float("nan"), default_ms=0)
    assert str(exc.value) == "cannot convert float NaN to integer"
