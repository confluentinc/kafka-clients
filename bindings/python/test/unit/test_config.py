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

"""Config-helper tests (P3): ``confluent_kafka._config``.

Covers the ``ConfigDef.parseType`` coercion rules ported from
``kafka/clients/src/main/java/org/apache/kafka/common/config/ConfigDef.java``
(``"true"``/``True`` and ``"1000"``/``1000`` equivalence, Java's
``ConfigException`` messages), ``logUnused`` (accept-and-warn, never reject),
``duration_to_ms`` (negative -> ``IllegalArgumentError``) and the rejection of
old-client callback config keys naming the replacement. There is no single Java
test class for these; the messages are asserted against Java's ``ConfigException``
text (DoD #3).
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pytest

from confluent_kafka import IllegalArgumentError, _config
from confluent_kafka.common.config import ConfigError

T = _config.ConfigType


class TestBooleanCoercion:
    @pytest.mark.parametrize(
        "value,expected",
        [("true", True), ("TRUE", True), ("  false ", False), (True, True), (False, False)],
    )
    def test_valid(self, value: object, expected: bool) -> None:
        assert _config.coerce_config_value("k", value, T.BOOLEAN) is expected

    def test_invalid_string(self) -> None:
        with pytest.raises(ConfigError) as exc:
            _config.coerce_config_value("k", "yes", T.BOOLEAN)
        assert str(exc.value) == (
            "Invalid value yes for configuration k: "
            "Expected value to be either true or false"
        )

    def test_invalid_type(self) -> None:
        with pytest.raises(ConfigError):
            _config.coerce_config_value("k", 5, T.BOOLEAN)


class TestIntCoercion:
    @pytest.mark.parametrize("value,expected", [("1000", 1000), (1000, 1000), (" -5 ", -5)])
    def test_valid(self, value: object, expected: int) -> None:
        assert _config.coerce_config_value("k", value, T.INT) == expected

    def test_not_a_number(self) -> None:
        with pytest.raises(ConfigError) as exc:
            _config.coerce_config_value("k", "abc", T.INT)
        assert str(exc.value) == (
            "Invalid value abc for configuration k: Not a number of type INT"
        )

    def test_bool_rejected(self) -> None:
        with pytest.raises(ConfigError) as exc:
            _config.coerce_config_value("k", True, T.INT)
        assert "bool" in str(exc.value)

    def test_string_out_of_int_range(self) -> None:
        with pytest.raises(ConfigError) as exc:
            _config.coerce_config_value("k", str(2**40), T.INT)
        assert "Not a number of type INT" in str(exc.value)


class TestShortLongDouble:
    def test_short(self) -> None:
        assert _config.coerce_config_value("k", "32767", T.SHORT) == 32767

    def test_short_overflow_string(self) -> None:
        with pytest.raises(ConfigError) as exc:
            _config.coerce_config_value("k", "70000", T.SHORT)
        assert "Not a number of type SHORT" in str(exc.value)

    def test_long(self) -> None:
        assert _config.coerce_config_value("k", "9999999999", T.LONG) == 9999999999

    def test_double_from_string(self) -> None:
        assert _config.coerce_config_value("k", "3.5", T.DOUBLE) == 3.5

    def test_double_from_int(self) -> None:
        assert _config.coerce_config_value("k", 3, T.DOUBLE) == 3.0

    def test_double_not_a_number(self) -> None:
        with pytest.raises(ConfigError) as exc:
            _config.coerce_config_value("k", "x", T.DOUBLE)
        assert "Not a number of type DOUBLE" in str(exc.value)

    def test_double_bool_rejected(self) -> None:
        with pytest.raises(ConfigError):
            _config.coerce_config_value("k", True, T.DOUBLE)


class TestStringListClass:
    def test_string_trimmed(self) -> None:
        assert _config.coerce_config_value("k", "  x ", T.STRING) == "x"

    def test_string_non_str_rejected(self) -> None:
        with pytest.raises(ConfigError) as exc:
            _config.coerce_config_value("k", 5, T.STRING)
        assert "Expected value to be a string" in str(exc.value)

    def test_list_from_string(self) -> None:
        assert _config.coerce_config_value("k", "a, b ,c", T.LIST) == ["a", "b", "c"]

    def test_list_empty_string(self) -> None:
        assert _config.coerce_config_value("k", "", T.LIST) == []

    def test_list_passthrough(self) -> None:
        assert _config.coerce_config_value("k", ["a", "b"], T.LIST) == ["a", "b"]

    def test_class_string_passthrough(self) -> None:
        assert _config.coerce_config_value("k", "my.mod.Cls", T.CLASS) == "my.mod.Cls"

    def test_class_object_passthrough(self) -> None:
        assert _config.coerce_config_value("k", int, T.CLASS) is int

    def test_class_bad_value(self) -> None:
        with pytest.raises(ConfigError) as exc:
            _config.coerce_config_value("k", 5, T.CLASS)
        assert "Expected a Class instance or class name" in str(exc.value)

    def test_none_passes_through(self) -> None:
        assert _config.coerce_config_value("k", None, T.INT) is None


class TestDurationToMs:
    def test_none_uses_default(self) -> None:
        assert _config.duration_to_ms(None, default_ms=5000) == 5000

    def test_float_seconds(self) -> None:
        assert _config.duration_to_ms(1.5, default_ms=0) == 1500

    def test_timedelta(self) -> None:
        assert _config.duration_to_ms(timedelta(seconds=2), default_ms=0) == 2000

    def test_negative_float_raises(self) -> None:
        with pytest.raises(IllegalArgumentError) as exc:
            _config.duration_to_ms(-1.0, default_ms=0)
        # Java's exact text (KafkaProducer.java:1393, AsyncKafkaConsumer.java:1552).
        assert str(exc.value) == "The timeout cannot be negative."

    def test_negative_timedelta_raises(self) -> None:
        with pytest.raises(IllegalArgumentError):
            _config.duration_to_ms(timedelta(seconds=-1), default_ms=0)

    def test_zero_is_allowed(self) -> None:
        assert _config.duration_to_ms(0.0, default_ms=999) == 0


class TestLogUnused:
    def test_logs_unused_keys(self, caplog) -> None:  # noqa: ANN001
        with caplog.at_level(logging.INFO, logger="confluent_kafka"):
            _config.log_unused({"unknown.key"})
        assert any("not used yet" in r.getMessage() for r in caplog.records)

    def test_empty_set_no_log(self, caplog) -> None:  # noqa: ANN001
        with caplog.at_level(logging.INFO, logger="confluent_kafka"):
            _config.log_unused(set())
        assert not caplog.records


class TestRejectCallbackKeys:
    @pytest.mark.parametrize(
        "key",
        ["error_cb", "logger", "on_delivery", "on_commit", "stats_cb", "rebalance_cb"],
    )
    def test_callback_key_rejected(self, key: str) -> None:
        with pytest.raises(ConfigError) as exc:
            _config.reject_callback_config_keys({key: object()})
        assert key in str(exc.value)

    def test_group_id_not_rejected(self) -> None:
        # group.id is optional at construction (Java) — never rejected here.
        _config.reject_callback_config_keys({"group.id": "g"})

    def test_plain_config_not_rejected(self) -> None:
        _config.reject_callback_config_keys(
            {"bootstrap.servers": "localhost:9092", "client.id": "x"}
        )
