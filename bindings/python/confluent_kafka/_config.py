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

"""Client configuration: the parts of Java's ``AbstractConfig`` / ``ConfigDef``
the clients run before the core sees ``configs``.

``configs`` is a ``dict`` of Java's dotted keys (CLAUDE.md, Python Binding
Conventions, Configuration):

- a key that is not a ``str`` raises ``ConfigError`` (``Utils.castToStringObjectMap``);
- a value of a key the client's ``ConfigDef`` defines is coerced to the key's
  type as ``ConfigDef.parseType`` does (``"true"`` equals ``True``,
  ``"1000"`` equals ``1000``), and a bad value raises ``ConfigError`` with
  Java's message;
- ``interceptor.classes`` naming an interceptor, and ``partitioner.class``
  naming a class other than Java's built-in ``RoundRobinPartitioner``, raise
  ``ConfigError``: the core runs no interceptors and no custom partitioner;
- every other key is accepted, and the keys neither the ``ConfigDef`` defines
  nor a serde's ``configure`` reads are logged once as unused
  (``AbstractConfig.logUnused()``).

The keys and types are generated from Java's ``ProducerConfig`` /
``ConsumerConfig`` into ``_config_types``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Mapping
from datetime import timedelta
from typing import Any, Literal

from confluent_kafka import _config_types
from confluent_kafka._java import java_str, java_trim, parse_double, parse_int, parse_long_string
from confluent_kafka.common.config.config_error import ConfigError
from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["RecordingConfigs", "coerce", "convert_to_string", "duration_to_ms", "log_unused",
           "prepare"]

_LOG = logging.getLogger("confluent_kafka.common.config")

Client = Literal["producer", "consumer"]

_TYPES: dict[str, dict[str, str]] = {
    "producer": _config_types.PRODUCER,
    "consumer": _config_types.CONSUMER,
}

# The serde keys: resolved by the binding (``_supply``), never passed to the core.
_SERDE_KEYS = frozenset({"key.serializer", "value.serializer",
                         "key.deserializer", "value.deserializer"})
_INTERCEPTOR_CLASSES = "interceptor.classes"
_PARTITIONER_CLASS = "partitioner.class"
_ROUND_ROBIN_PARTITIONER = "org.apache.kafka.clients.producer.RoundRobinPartitioner"
_INT_BITS = {"INT": 32, "SHORT": 16, "LONG": 64}
_INT_LABELS = {"INT": "a 32-bit integer", "SHORT": "a 16-bit integer (short)",
               "LONG": "a 64-bit integer (long)"}


class RecordingConfigs(dict[str, Any]):
    """The user's ``configs``, recording the keys a serde's ``configure``
    reads, as Java's ``AbstractConfig.RecordingMap`` does, so they are not
    logged as unused."""

    def __init__(self, configs: Mapping[str, Any]) -> None:
        super().__init__(configs)
        self.used: set[str] = set()

    def __getitem__(self, key: str) -> Any:
        self.used.add(key)
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        self.used.add(key)
        return super().get(key, default)


def _type_name(value: object) -> str:
    """The class name Java's ``value.getClass().getName()`` gives, for a
    Python value."""
    cls = type(value)
    return f"{cls.__module__}.{cls.__qualname__}"


def _parse_number(name: str, value: object, parse: Callable[[Any], Any], type_name: str) -> Any:
    """``parse`` the value, a ``NumberFormatException`` reported as
    ``ConfigDef.parseType`` reports it: a new ``ConfigException`` with no
    cause."""
    try:
        return parse(java_trim(value) if isinstance(value, str) else value)
    except IllegalArgumentError:
        raise ConfigError(name=name, value=value,
                          message=f"Not a number of type {type_name}") from None


def _fits(bits: int) -> Callable[[int], int]:
    def check(value: int) -> int:
        if not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
            raise IllegalArgumentError(message=f"{value} is out of range")
        return value
    return check


def coerce(name: str, value: object, type_name: str) -> object:
    """``ConfigDef.parseType(name, value, type)``: ``value`` as the key's
    ``ConfigDef`` type, or ``ConfigError`` with Java's message. ``None`` is
    Java's ``null``."""
    if value is None:
        return None
    if type_name == "BOOLEAN":
        if isinstance(value, str):
            trimmed = java_trim(value).lower()
            if trimmed in ("true", "false"):
                return trimmed == "true"
        elif isinstance(value, bool):
            return value
        raise ConfigError(name=name, value=value,
                          message="Expected value to be either true or false")
    if type_name in ("STRING", "PASSWORD"):
        if isinstance(value, str):
            return java_trim(value)
        raise ConfigError(name=name, value=value,
                          message=f"Expected value to be a string, but it was a {_type_name(value)}")
    if type_name in _INT_BITS:
        bits = _INT_BITS[type_name]
        if isinstance(value, int) and not isinstance(value, bool):
            return _parse_number(name, value, _fits(bits), type_name)
        if isinstance(value, str):
            if bits == 64:
                return _parse_number(name, value, parse_long_string, type_name)
            return _parse_number(name, value, lambda s: parse_int(s, bits), type_name)
        raise ConfigError(
            name=name, value=value,
            message=f"Expected value to be {_INT_LABELS[type_name]}, but it was a {_type_name(value)}")
    if type_name == "DOUBLE":
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, str):
            return _parse_number(name, value, parse_double, type_name)
        raise ConfigError(name=name, value=value,
                          message=f"Expected value to be a double, but it was a {_type_name(value)}")
    if type_name == "LIST":
        if isinstance(value, (list, tuple)):
            return list(value)
        if isinstance(value, str):
            trimmed = java_trim(value)
            return [] if not trimmed else [java_trim(part) for part in trimmed.split(",")]
        raise ConfigError(name=name, value=value, message="Expected a comma separated list.")
    if type_name == "CLASS":
        # A class is loaded where it is used (the serde keys, by ``_supply``);
        # the core resolves the Java class names of the other class keys.
        if isinstance(value, type):
            return value
        if isinstance(value, str):
            return java_trim(value)
        raise ConfigError(name=name, value=value,
                          message="Expected a Class instance or class name.")
    raise ConfigError(name=name, value=value, message=f"Unknown type {type_name}")


def convert_to_string(value: object, type_name: str | None) -> str | None:
    """``ConfigDef.convertToString(parsedValue, type)``: a parsed value as the
    text the core parses (a password as its value, since the core needs it)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if type_name == "LIST" and isinstance(value, list):
        return ",".join(java_str(v) for v in value)
    if isinstance(value, type):
        return f"{value.__module__}.{value.__qualname__}"
    return java_str(value)


def prepare(configs: Mapping[str, Any], *, client: Client,
            given_serdes: Collection[str] = ()) -> tuple[RecordingConfigs, dict[str, str]]:
    """Parse ``configs`` for a ``client``: the user's configs, recording what
    the serdes read, and the string map the core parses (the serde keys and
    ``None`` values left out).

    ``given_serdes`` names the serde keys (``key.serializer``, …) whose
    constructor argument is given: the argument wins and the key is not
    parsed, as Java's ``ProducerConfig.appendSerializerToConfig`` /
    ``ConsumerConfig.appendDeserializerToConfig`` replace it with the
    argument's class before ``ConfigDef`` parses the configs."""
    if not isinstance(configs, Mapping):
        raise TypeError(f"configs must be a dict, not {type(configs).__name__}")
    for key, value in configs.items():
        if not isinstance(key, str):
            raise ConfigError(name=java_str(key), value=value, message="Key must be a string.")
    types = _TYPES[client]
    native: dict[str, str] = {}
    for key, value in configs.items():
        if key in given_serdes:
            continue
        type_name = types.get(key)
        parsed = coerce(key, value, type_name) if type_name is not None else value
        if key == _INTERCEPTOR_CLASSES and parsed:
            raise ConfigError(name=key, value=value, message="The client runs no interceptors.")
        if key == _PARTITIONER_CLASS and parsed is not None and parsed != _ROUND_ROBIN_PARTITIONER:
            raise ConfigError(
                name=key, value=value,
                message=f"The client runs no custom partitioner; {_ROUND_ROBIN_PARTITIONER} "
                "is the only partitioner class it has.")
        text = convert_to_string(parsed, type_name)
        if key in _SERDE_KEYS or text is None:
            continue
        native[key] = text
    return RecordingConfigs(configs), native


def log_unused(originals: RecordingConfigs, *, client: Client) -> None:
    """``AbstractConfig.logUnused()``: log once, at INFO, the supplied keys
    that the client's ``ConfigDef`` does not define and no serde read."""
    types = _TYPES[client]
    unused = sorted(k for k in originals if k not in types and k not in originals.used)
    if unused:
        _LOG.info("These configurations '%s' were supplied but are not used yet.",
                  java_str(unused))


def duration_to_ms(timeout: float | timedelta | None, *, default_ms: int) -> int:
    """A ``Duration`` (seconds, or a ``timedelta``) in milliseconds; ``None`` is
    ``default_ms``. A negative value raises
    ``IllegalArgumentError(message="The timeout cannot be negative.")``, Java's
    text for every client's negative-timeout check."""
    if timeout is None:
        return default_ms
    if isinstance(timeout, timedelta):
        millis = timeout.total_seconds() * 1000.0
    else:
        millis = float(timeout) * 1000.0
    if millis < 0:
        raise IllegalArgumentError(message="The timeout cannot be negative.")
    return int(millis)
