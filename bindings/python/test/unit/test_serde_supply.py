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

"""Serde supply-route and lifecycle tests (P3).

Covers ``resolve_serde`` (kwarg wins, class rejected with the redirect message,
config dotted-path / class-object resolution + ``configure(conf, is_key)``,
instance-in-config rejection, default fallthrough) and the lifecycle helpers
``configure_if_defined`` / ``close_if_defined`` (config-route configure,
``close`` exceptions logged not raised). There is no direct Java test — Java
resolves this in ``Deserializers`` / ``AbstractConfig``; these assert the
Java-parity behavior recorded in D6.
"""

from __future__ import annotations

import logging

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.serialization import (
    StringDeserializer,
    bytes_deserializer,
    close_if_defined,
    configure_if_defined,
    resolve_serde,
    string_deserializer,
)

KEY = "value.deserializer"


class _RecordingSerde:
    """A serde-shaped instance that records its configure/close calls."""

    def __init__(self) -> None:
        self.configured: tuple[dict[str, object], bool] | None = None
        self.closed = 0

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        self.configured = (configs, is_key)

    def close(self) -> None:
        self.closed += 1

    def __call__(self, topic, data, headers=None):  # noqa: ANN001
        return None if data is None else bytes(data)


class TestResolveSerde:
    def test_neither_returns_default(self) -> None:
        default = bytes_deserializer()
        got = resolve_serde(None, {}, KEY, is_key=False, default=default)
        assert got is default

    def test_kwarg_instance_used_as_is(self) -> None:
        default = bytes_deserializer()
        inst = string_deserializer()
        got = resolve_serde(inst, {}, KEY, is_key=False, default=default)
        assert got is inst

    def test_kwarg_wins_over_config(self) -> None:
        default = bytes_deserializer()
        inst = string_deserializer()
        got = resolve_serde(
            inst,
            {KEY: "confluent_kafka.common.serialization.StringDeserializer"},
            KEY,
            is_key=False,
            default=default,
        )
        assert got is inst

    def test_kwarg_function_accepted(self) -> None:
        default = bytes_deserializer()

        def fn(topic, data, headers=None):  # noqa: ANN001
            return data

        got = resolve_serde(fn, {}, KEY, is_key=False, default=default)
        assert got is fn

    def test_kwarg_class_rejected_with_redirect(self) -> None:
        default = bytes_deserializer()
        with pytest.raises(IllegalArgumentError) as exc:
            resolve_serde(StringDeserializer, {}, KEY, is_key=False, default=default)
        assert "pass an instance, not the class" in str(exc.value)

    def test_kwarg_non_callable_rejected(self) -> None:
        default = bytes_deserializer()
        with pytest.raises(IllegalArgumentError):
            resolve_serde(42, {}, KEY, is_key=False, default=default)

    def test_config_dotted_path_resolved_and_configured(self) -> None:
        default = bytes_deserializer()
        got = resolve_serde(
            None,
            {
                KEY: "confluent_kafka.common.serialization.StringDeserializer",
                "value.deserializer.encoding": "utf_16",
            },
            KEY,
            is_key=False,
            default=default,
        )
        assert isinstance(got, StringDeserializer)
        # configure() picked the per-slot key out of the client config.
        assert got._encoding == "utf_16"

    def test_config_is_key_flag_selects_key_slot(self) -> None:
        default = bytes_deserializer()
        got = resolve_serde(
            None,
            {
                "key.deserializer": (
                    "confluent_kafka.common.serialization.StringDeserializer"
                ),
                "key.deserializer.encoding": "ascii",
            },
            "key.deserializer",
            is_key=True,
            default=default,
        )
        assert isinstance(got, StringDeserializer)
        assert got._encoding == "ascii"

    def test_config_class_object_resolved(self) -> None:
        default = bytes_deserializer()
        got = resolve_serde(
            None, {KEY: StringDeserializer}, KEY, is_key=False, default=default
        )
        assert isinstance(got, StringDeserializer)

    def test_config_instance_rejected(self) -> None:
        default = bytes_deserializer()
        with pytest.raises(ConfigError) as exc:
            resolve_serde(
                None,
                {KEY: string_deserializer()},
                KEY,
                is_key=False,
                default=default,
            )
        assert "Class instance or class name" in str(exc.value)

    def test_config_unresolvable_path_raises_config_error(self) -> None:
        default = bytes_deserializer()
        with pytest.raises(ConfigError) as exc:
            resolve_serde(
                None, {KEY: "no.such.Module"}, KEY, is_key=False, default=default
            )
        assert "could not be found" in str(exc.value)


class TestLifecycleHelpers:
    def test_configure_if_defined_calls_configure(self) -> None:
        serde = _RecordingSerde()
        configs = {"value.deserializer.encoding": "utf_8"}
        configure_if_defined(serde, configs, False)
        assert serde.configured == (configs, False)

    def test_configure_if_defined_noop_on_bare_function(self) -> None:
        def fn(topic, data, headers=None):  # noqa: ANN001
            return data

        configure_if_defined(fn, {}, False)  # must not raise

    def test_close_if_defined_calls_close(self) -> None:
        serde = _RecordingSerde()
        close_if_defined(serde)
        assert serde.closed == 1

    def test_close_if_defined_logs_not_raises(self, caplog) -> None:  # noqa: ANN001
        class Boom:
            def close(self) -> None:
                raise RuntimeError("boom")

        with caplog.at_level(logging.WARNING, logger="confluent_kafka"):
            close_if_defined(Boom())  # must not raise
        assert any("boom" in r.getMessage() or r.exc_info for r in caplog.records)

    def test_close_if_defined_noop_on_bare_function(self) -> None:
        close_if_defined(lambda *a: None)  # must not raise
