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

"""Serde supply routes and lifecycle helpers (spec §5.4, D6).

The clients (P4/P5) do not construct serdes themselves; they call
``resolve_serde`` to pick one from the two supply routes, ``configure_if_defined``
after construction on the config route, and ``close_if_defined`` at client close.
These live here so the two client families share one implementation and cannot
diverge on the error messages or the route precedence.

Route precedence (Java-exact):

* **Kwarg wins.** A constructor kwarg (a callable or instance) is used and the
  matching config key is ignored — Java's ``config.ignore(...)``. ``configure``
  is *not* called on the kwarg route (constructor args are the injection
  channel). A **class** passed as a kwarg is rejected with a redirect error.
* **Config route.** ``key.deserializer`` / ``value.deserializer`` (or the
  serializer keys) may be a dotted-path string or a class object (Java's
  ``Type.CLASS``); it is resolved, no-arg constructed, then ``configure(conf,
  is_key)``-called if defined. Instances are rejected in config.
* **Neither** → the ``bytes_*`` default.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from collections.abc import Callable
from typing import cast

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common.config._generated_errors import ConfigError

_LOG = logging.getLogger("confluent_kafka")


def configure_if_defined(serde: object, configs: dict[str, object], is_key: bool) -> None:
    """Call ``serde.configure(configs, is_key)`` iff it is defined.

    Used only on the config route — Java skips ``configure`` for a kwarg
    instance (``Deserializers.java:52-64``). An absent method is a no-op, so a
    bare function is a complete serde.
    """
    configure = getattr(serde, "configure", None)
    if callable(configure):
        configure(configs, is_key)


def close_if_defined(serde: object) -> None:
    """Call ``serde.close()`` iff it is defined, logging any exception.

    Java's ``Utils.closeQuietly``: a failing ``close`` must not mask the
    client's own shutdown, so the exception is logged, never raised (spec §5.4).
    ``close`` must be idempotent.
    """
    close = getattr(serde, "close", None)
    if callable(close):
        try:
            close()
        except Exception:  # noqa: BLE001 - closeQuietly: log, never raise
            _LOG.warning("Exception while closing serde %r", serde, exc_info=True)


def _resolve_class(dotted_path: str) -> type:
    """Resolve a dotted path ``pkg.mod.Cls`` to the class object.

    Java's ``Class.forName``; here ``importlib.import_module`` + ``getattr``. A
    bad path raises ``ConfigError`` at construction time (Java's
    ``ConfigException`` timing).
    """
    module_name, _, attr = dotted_path.rpartition(".")
    if not module_name:
        raise ConfigError(f"Class {dotted_path} could not be found.")
    try:
        module = importlib.import_module(module_name)
        obj = getattr(module, attr)
    except (ImportError, AttributeError) as exc:
        raise ConfigError(f"Class {dotted_path} could not be found.") from exc
    if not isinstance(obj, type):
        raise ConfigError(
            f"Class {dotted_path} could not be found."
        )
    return obj


def _construct_from_class(cls: type, configs: dict[str, object], is_key: bool) -> Callable[..., object]:
    """No-arg construct ``cls`` and ``configure`` it — Java's ``newInstance`` + ``configure``."""
    try:
        instance = cls()
    except TypeError as exc:
        # Java: "Could not find a public no-argument constructor for <cls>".
        raise ConfigError(
            f"Could not find a public no-argument constructor for "
            f"{cls.__module__}.{cls.__qualname__}"
        ) from exc
    if not callable(instance):
        raise ConfigError(
            f"{cls.__module__}.{cls.__qualname__} is not a serializer/deserializer"
        )
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
                f"{key}: pass an instance, not the class — did you forget '()'?"
            )
        if not callable(kwarg):
            raise IllegalArgumentError(
                f"{key} must be a callable serializer/deserializer, "
                f"got {type(kwarg).__name__}"
            )
        return cast("Callable[..., object]", kwarg)

    config_value = config.get(key)
    if config_value is None:
        return default
    if isinstance(config_value, str):
        cls = _resolve_class(config_value)
        return _construct_from_class(cls, config, is_key)
    if inspect.isclass(config_value):
        return _construct_from_class(config_value, config, is_key)
    # An instance (or any non-str, non-class value) in config is rejected —
    # Java's config dict stays pure data (Type.CLASS accepts a name or a Class).
    raise ConfigError(
        f"Invalid value {config_value!r} for configuration {key}: "
        f"Expected a Class instance or class name."
    )
