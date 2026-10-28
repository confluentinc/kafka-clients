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

A serde from either route must accept ``**kwargs`` (it is called with keyword
arguments named after Java's parameters): one without raises
``IllegalArgumentError("<param> must accept **kwargs")``, naming the
constructor parameter (``key_serializer``) on the argument route and the config
key (``key.serializer``) on the config route, where it is checked before
``configure``, the instance then being closed, as Java's
``getConfiguredInstance`` checks the type before it configures.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from collections.abc import Callable
from typing import cast

from confluent_kafka._java import java_trim
from confluent_kafka._kwargs import accepts_kwargs, require_kwargs
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
    ``ConfigDef`` parses a ``CLASS`` value with ``Class.forName`` on the
    trimmed name: an unresolvable name raises ``ConfigError(name=key,
    value=dotted_path, message="Class <value> could not be found.")`` with the
    value as given and no cause (``ConfigDef.parseType``)."""
    module_name, _, attr = java_trim(dotted_path).rpartition(".")
    obj: object = None
    if module_name:
        try:
            obj = getattr(importlib.import_module(module_name), attr)
        except (ImportError, AttributeError):
            obj = None
    if not isinstance(obj, type):
        raise ConfigError(name=key, value=dotted_path,
                          message=f"Class {dotted_path} could not be found.")
    return obj


def _class_name(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _lacks_no_arg_constructor(cls: type) -> bool:
    """Whether ``cls()`` cannot even be bound: Java's ``NoSuchMethodException`` case.
    A class whose signature cannot be read is taken as one that needs arguments."""
    try:
        inspect.signature(cls).bind()
    except TypeError:
        return True
    except ValueError:
        return True
    return False


def _construct_from_class(key: str, cls: type) -> Callable[..., object]:
    """No-arg construct ``cls`` and check its type, as Java's
    ``AbstractConfig.getConfiguredInstance`` does before it configures the
    instance (``resolve_serde`` configures it)."""
    try:
        instance = cls()
    except TypeError as exc:
        # Java's Utils.newInstance: NoSuchMethodException is "Could not find a public
        # no-argument constructor"; an exception thrown by the constructor itself is
        # "Could not instantiate class". A TypeError tells the two apart by whether the
        # class can be called with no argument at all.
        if _lacks_no_arg_constructor(cls):
            raise KafkaError(
                message="Could not find a public no-argument constructor for " + _class_name(cls),
                cause=exc) from exc
        raise KafkaError(message="Could not instantiate class " + _class_name(cls), cause=exc) from exc
    except Exception as exc:
        raise KafkaError(message="Could not instantiate class " + _class_name(cls), cause=exc) from exc
    if not callable(instance):
        base = "Deserializer" if key.endswith("deserializer") else "Serializer"
        raise KafkaError(
            message=f"class {_class_name(cls)} is not an instance of "
            f"confluent_kafka.common.serialization.{base}")
    return cast("Callable[..., object]", instance)


def resolve_serde(
    kwarg: object,
    config: dict[str, object],
    key: str,
    *,
    is_key: bool,
    default: Callable[..., object],
    construct_failure: Callable[[Exception], BaseException] | None = None,
) -> Callable[..., object]:
    """Pick a serde from the kwarg route, the config route, or the default.

    * ``kwarg`` — a callable/instance is used as-is (``configure`` skipped); a
      **class** is rejected with a redirect error; ``None`` falls through.
    * ``config[key]`` — a dotted-path string or class object is resolved,
      constructed, and ``configure``-called; an instance in config is rejected.
    * neither → ``default``.

    ``construct_failure`` wraps an error of the construction or ``configure`` step
    (the client's ``KafkaException("Failed to construct kafka producer", t)``: Java
    builds the serdes inside its constructor's try); the resolution of the class
    name is Java's ``ConfigDef`` parsing, before that try, and stays unwrapped.
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
        require_kwargs(kwarg, key.replace(".", "_"))
        return cast("Callable[..., object]", kwarg)

    config_value = config.get(key)
    if config_value is None:
        return default
    if isinstance(config_value, str):
        cls = _resolve_class(key, config_value)
    elif inspect.isclass(config_value):
        cls = config_value
    else:
        # Java's ConfigDef takes a name or a Class for a CLASS key, never an instance.
        raise ConfigError(name=key, value=config_value,
                          message="Expected a Class instance or class name.")
    try:
        instance = _construct_from_class(key, cls)
    except Exception as exc:
        if construct_failure is None:
            raise
        raise construct_failure(exc) from exc
    # The keyword contract is a type check: Java's getConfiguredInstance checks
    # the type (and closes a wrong one) before it configures the instance
    # (org.apache.kafka.common.config.AbstractConfig#getConfiguredInstance(Object,Class,Map)).
    if not accepts_kwargs(instance):
        close_if_defined(instance)
        require_kwargs(instance, key)
    try:
        configure_if_defined(instance, config, is_key)
    except Exception as exc:
        if construct_failure is None:
            raise
        raise construct_failure(exc) from exc
    return instance
