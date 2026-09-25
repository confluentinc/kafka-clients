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

"""Serde supply routes and lifecycle calls, shared by every client.

The clients call ``resolve_serde`` to pick a serde, ``configure_if_defined``
after construction on the config route and ``close_if_defined`` at client close
(CLAUDE.md, Python Binding Conventions, Serialization):

* a constructor argument (any callable or instance) wins and the config key is
  ignored; it is not configured; a class raises ``IllegalArgumentError``;
* else the config key (``key.serializer``, ``value.deserializer``, ...), a
  dotted path or a class, is resolved, no-arg constructed and configured; an
  instance or an unresolvable name raises ``ConfigError``;
* else the ``bytes_*`` default.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from collections.abc import Callable
from typing import cast

from confluent_kafka.common.config.config_error import ConfigError
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.illegal_argument_error import IllegalArgumentError

_LOG = logging.getLogger("confluent_kafka")


def configure_if_defined(serde: object, configs: dict[str, object], is_key: bool) -> None:
    """Call ``serde.configure(configs, is_key)`` iff it is defined.

    Used only on the config route: Java configures only the serdes it
    constructs from config (``Deserializers``). An absent method is a no-op,
    so a bare function is a complete serde.
    """
    configure = getattr(serde, "configure", None)
    if callable(configure):
        configure(configs, is_key)


def close_if_defined(serde: object) -> None:
    """Call ``serde.close()`` iff it is defined, logging any exception.

    Java's ``Utils.closeQuietly``: a failing ``close`` must not mask the
    client's own shutdown, so the exception is logged, never raised.
    """
    close = getattr(serde, "close", None)
    if callable(close):
        try:
            close()
        except Exception:  # noqa: BLE001 - closeQuietly: log, never raise
            _LOG.warning("Exception while closing serde %r", serde, exc_info=True)


def _resolve_class(key: str, dotted_path: str) -> type:
    """Resolve a dotted path ``pkg.mod.Cls`` to the class, as Java's
    ``ConfigDef`` parses a ``CLASS`` value with ``Class.forName``: an
    unresolvable name raises ``ConfigError(name=key, value=dotted_path,
    message="Class <name> could not be found.")``."""
    trimmed = dotted_path.strip()
    module_name, _, attr = trimmed.rpartition(".")
    obj: object = None
    cause: BaseException | None = None
    if module_name:
        try:
            obj = getattr(importlib.import_module(module_name), attr)
        except (ImportError, AttributeError) as exc:
            cause = exc
    if not isinstance(obj, type):
        raise ConfigError(name=key, value=dotted_path,
                          message=f"Class {trimmed} could not be found.") from cause
    return obj


def _class_name(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _construct_from_class(key: str, cls: type, configs: dict[str, object],
                          is_key: bool) -> Callable[..., object]:
    """No-arg construct ``cls`` and configure it, as Java's
    ``AbstractConfig.getConfiguredInstance`` does."""
    try:
        instance = cls()
    except TypeError as exc:
        raise KafkaError(
            message="Could not find a public no-argument constructor for " + _class_name(cls)
        ) from exc
    if not callable(instance):
        base = "Deserializer" if key.endswith("deserializer") else "Serializer"
        raise KafkaError(
            message=f"class {_class_name(cls)} is not an instance of "
            f"confluent_kafka.common.serialization.{base}")
    configure_if_defined(instance, configs, is_key)
    return cast("Callable[..., object]", instance)


def resolve_serde(
    kwarg: object,
    config: dict[str, object],
    key: str,
    *,
    is_key: bool,
    default: Callable[..., object],
) -> Callable[..., object]:
    """Pick a serde from the kwarg route, the config route, or the default.

    * ``kwarg`` — a callable/instance is used as-is (``configure`` skipped); a
      **class** is rejected with a redirect error; ``None`` falls through.
    * ``config[key]`` — a dotted-path string or class object is resolved,
      constructed, and ``configure``-called; an instance in config is rejected.
    * neither → ``default``.
    """
    if kwarg is not None:
        if inspect.isclass(kwarg):
            raise IllegalArgumentError(
                message=f"{key}: pass an instance, not the class — did you forget '()'?"
            )
        if not callable(kwarg):
            raise IllegalArgumentError(
                message=f"{key} must be a callable serializer/deserializer, "
                f"got {type(kwarg).__name__}"
            )
        return cast("Callable[..., object]", kwarg)

    config_value = config.get(key)
    if config_value is None:
        return default
    if isinstance(config_value, str):
        return _construct_from_class(key, _resolve_class(key, config_value), config, is_key)
    if inspect.isclass(config_value):
        return _construct_from_class(key, config_value, config, is_key)
    # Java's ConfigDef takes a name or a Class for a CLASS key, never an instance.
    raise ConfigError(name=key, value=config_value,
                      message="Expected a Class instance or class name.")
