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

"""Configuration helpers shared by the clients (spec §5.7, rule 9).

The ``config`` argument to every client is a ``dict`` of Java's dotted property
names. These helpers implement the parts of Java's ``ConfigDef`` / ``AbstractConfig``
the binding needs, so the client families (P4/P5) share one implementation:

* ``coerce_config_value`` — Java's ``ConfigDef.parseType`` coercion (``"true"``/
  ``True`` equivalent, ``"1000"``/``1000`` equivalent), with Java's ``ConfigException``
  messages surfaced as ``ConfigError``.
* ``log_unused`` — Java's ``AbstractConfig.logUnused()`` — unknown keys are
  accepted, not rejected, with one INFO log.
* ``duration_to_ms`` — a ``Duration`` (float seconds or ``timedelta``) to
  milliseconds; a negative value raises ``IllegalArgumentError("Timeout must not
  be negative")``.
* ``reject_callback_config_keys`` — the librdkafka callback keys the old client
  used as config (``error_cb``, ``logger``, ``on_delivery`` …) are not config
  entries here (spec §5.7 / §11.1); each is rejected with a ``ConfigError``
  naming its replacement.

``group.id`` is deliberately **not** rejected: it is optional at construction
(Java's ``ConsumerConfig`` default ``null``); the group APIs raise
``InvalidGroupIdError`` when it is missing. This module never rejects it.

The clients are wired to these helpers in P4/P5; nothing here constructs a client.
"""

from __future__ import annotations

import enum
import logging
from datetime import timedelta

from confluent_kafka import Duration, IllegalArgumentError
from confluent_kafka.common.config._generated_errors import ConfigError

_LOG = logging.getLogger("confluent_kafka")

__all__ = [
    "ConfigType",
    "coerce_config_value",
    "log_unused",
    "duration_to_ms",
    "reject_callback_config_keys",
    "REJECTED_CALLBACK_KEYS",
]


class ConfigType(enum.Enum):
    """The value types the client config keys use — Java's ``ConfigDef.Type``.

    Only the value-carrying members are modelled (the documentation-importance
    members ``HIGH``/``MEDIUM``/``LOW`` are not value types). ``PASSWORD`` coerces
    like ``STRING`` for our purposes (we do not model Java's ``Password`` wrapper
    on the surface).
    """

    BOOLEAN = "boolean"
    STRING = "string"
    INT = "int"
    SHORT = "short"
    LONG = "long"
    DOUBLE = "double"
    LIST = "list"
    CLASS = "class"
    PASSWORD = "password"


_INT32_MIN, _INT32_MAX = -(2**31), 2**31 - 1
_INT16_MIN, _INT16_MAX = -(2**15), 2**15 - 1
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1


def _config_exception(name: str, value: object, message: str) -> ConfigError:
    """Java's ``ConfigException(name, value, message)`` text as a ``ConfigError``."""
    return ConfigError(f"Invalid value {value} for configuration {name}: {message}")


def coerce_config_value(name: str, value: object, config_type: ConfigType) -> object:
    """Coerce ``value`` to ``config_type``, mirroring Java's ``ConfigDef.parseType``.

    ``None`` passes through (Java returns ``null``). A non-coercible value raises
    ``ConfigError`` with Java's ``ConfigException`` message. String forms are
    trimmed first (Java trims before parsing).
    """
    if value is None:
        return None

    if config_type is ConfigType.BOOLEAN:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered == "true":
                return True
            if lowered == "false":
                return False
            raise _config_exception(
                name, value, "Expected value to be either true or false"
            )
        # In Python, ``bool`` is a subclass of ``int``; check it first.
        if isinstance(value, bool):
            return value
        raise _config_exception(
            name, value, "Expected value to be either true or false"
        )

    if config_type in (ConfigType.STRING, ConfigType.PASSWORD):
        if isinstance(value, str):
            return value.strip()
        raise _config_exception(
            name,
            value,
            f"Expected value to be a string, but it was a {type(value).__name__}",
        )

    if config_type is ConfigType.INT:
        return _coerce_int(
            name, value, _INT32_MIN, _INT32_MAX, "a 32-bit integer", "INT"
        )

    if config_type is ConfigType.SHORT:
        return _coerce_int(
            name, value, _INT16_MIN, _INT16_MAX, "a 16-bit integer (short)", "SHORT"
        )

    if config_type is ConfigType.LONG:
        return _coerce_int(
            name, value, _INT64_MIN, _INT64_MAX, "a 64-bit integer (long)", "LONG"
        )

    if config_type is ConfigType.DOUBLE:
        # Java: any Number -> doubleValue(); a String -> Double.parseDouble.
        if isinstance(value, bool):
            raise _config_exception(
                name, value, "Expected value to be a double, but it was a bool"
            )
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip())
            except ValueError as exc:
                raise _config_exception(
                    name, value, "Not a number of type DOUBLE"
                ) from exc
        raise _config_exception(
            name,
            value,
            f"Expected value to be a double, but it was a {type(value).__name__}",
        )

    if config_type is ConfigType.LIST:
        # Java: a List passes through; a String splits on comma-with-whitespace;
        # an empty string is the empty list.
        if isinstance(value, (list, tuple)):
            return list(value)
        if isinstance(value, str):
            trimmed = value.strip()
            if trimmed == "":
                return []
            return [part.strip() for part in trimmed.split(",")]
        raise _config_exception(name, value, "Expected a comma separated list.")

    # CLASS: a class object or a dotted-path string passes through unchanged;
    # actual resolution happens in the serde supply route (Type.CLASS).
    if config_type is ConfigType.CLASS:
        if isinstance(value, (type, str)):
            return value
        raise _config_exception(name, value, "Expected a Class instance or class name.")

    raise ConfigError(f"Unknown config type for {name}")  # unreachable


def _coerce_int(
    name: str,
    value: object,
    lo: int,
    hi: int,
    label: str,
    type_name: str,
) -> int:
    # ``bool`` is an ``int`` subclass in Python but is not an integer config value.
    if isinstance(value, bool):
        raise _config_exception(
            name, value, f"Expected value to be {label}, but it was a bool"
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError as exc:
            # Java throws NumberFormatException -> "Not a number of type <TYPE>".
            raise _config_exception(
                name, value, f"Not a number of type {type_name}"
            ) from exc
    else:
        raise _config_exception(
            name,
            value,
            f"Expected value to be {label}, but it was a {type(value).__name__}",
        )
    # Java's Integer/Short/Long.parseXxx enforce the fixed width; a String out of
    # range throws NumberFormatException. Reproduce that for the String route
    # (a same-typed Java Integer/Short/Long is already in range).
    if not (lo <= parsed <= hi) and not isinstance(value, int):
        raise _config_exception(
            name, value, f"Not a number of type {type_name}"
        )
    return parsed


def log_unused(unused_keys: set[str]) -> None:
    """Log unknown/unused config keys once at INFO — Java's ``logUnused()``.

    Java: ``log.info("These configurations '{}' were supplied but are not used
    yet.", unusedKeys)``. Unknown keys are accepted (the serde config route
    relies on pass-through keys riding in the dict), only logged.
    """
    if unused_keys:
        _LOG.info(
            "These configurations '%s' were supplied but are not used yet.",
            unused_keys,
        )


def duration_to_ms(timeout: Duration | None, *, default_ms: int) -> int:
    """Convert a ``Duration`` (float seconds or ``timedelta``) to milliseconds.

    ``None`` -> ``default_ms`` (Java's ``default.api.timeout.ms`` fallback for the
    ``Duration``-less overloads). A negative value raises
    ``IllegalArgumentError("Timeout must not be negative")`` (Java rejects a
    negative ``Duration``). There is no infinite sentinel (spec §11.1).
    """
    if timeout is None:
        return default_ms
    if isinstance(timeout, timedelta):
        millis = timeout.total_seconds() * 1000.0
    else:
        # float | int seconds
        millis = float(timeout) * 1000.0
    if millis < 0:
        # Java's exact text for every client's negative-timeout guard
        # (KafkaProducer.java:1393, AsyncKafkaConsumer.java:1552, ...).
        raise IllegalArgumentError("The timeout cannot be negative.")
    return int(millis)


# The librdkafka callback config keys the old client used, mapped to their
# replacement here (spec §5.7 / §11.1). None of these is a config entry in the
# new client; each is rejected with a ConfigError naming the replacement.
REJECTED_CALLBACK_KEYS: dict[str, str] = {
    "error_cb": (
        "no global error callback — operation errors raise or fail the future; "
        "attach a logging.Handler to 'confluent_kafka' for connectivity events"
    ),
    "logger": (
        "no logger parameter — configure logging.getLogger('confluent_kafka')"
    ),
    "on_delivery": (
        "pass on_delivery to send(record=..., on_delivery=cb) instead of config"
    ),
    "on_commit": (
        "pass on_commit to commit_nowait(on_commit=cb) instead of config"
    ),
    "stats_cb": "use metrics() — statistics are pull-based",
    "throttle_cb": "not part of this surface",
    "oauth_cb": "not part of this surface",
    "dr_cb": (
        "use the delivery future or send(record=..., on_delivery=cb)"
    ),
    "dr_msg_cb": (
        "use the delivery future or send(record=..., on_delivery=cb)"
    ),
    "rebalance_cb": (
        "pass a ConsumerRebalanceListener to subscribe(listener=...)"
    ),
}


def reject_callback_config_keys(config: dict[str, object]) -> None:
    """Raise ``ConfigError`` for any old-client callback key present in ``config``.

    Callbacks are never config entries here (spec §5.7); the message names the
    replacement for each key found.
    """
    for key in config:
        replacement = REJECTED_CALLBACK_KEYS.get(key)
        if replacement is not None:
            raise ConfigError(
                f"'{key}' is not a configuration key in this client: {replacement}"
            )
