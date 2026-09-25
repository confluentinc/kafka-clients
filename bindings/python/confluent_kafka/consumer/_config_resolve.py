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

"""Consumer construction: config validation, serde resolution, and building the
native handle (spec §5.7 / §5.4).

Shared by ``KafkaConsumer`` and ``AsyncKafkaConsumer`` so the two constructors
cannot diverge.
"""

from __future__ import annotations

from typing import Any, Callable

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka.common.errors import from_ffi_error
from confluent_kafka.common.serialization import (
    bytes_deserializer, resolve_serde,
)
from confluent_kafka._config import reject_callback_config_keys

# Config keys that carry the serde class on the config route.
_KEY_DESERIALIZER_KEY = "key.deserializer"
_VALUE_DESERIALIZER_KEY = "value.deserializer"

# The keys the binding consumes itself (serde class keys) — not passed to the
# native client, which does its own deserialization only for the byte defaults.
_BINDING_ONLY_KEYS = frozenset({_KEY_DESERIALIZER_KEY, _VALUE_DESERIALIZER_KEY})


def _stringify(value: object) -> str:
    """Coerce a config value to the string the native properties map wants
    (Java's ``ConfigDef`` accepts ``"true"``/``True`` and ``"1000"``/``1000``
    equivalently; the native side re-parses per the key's declared type)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def resolve_consumer_construction(
    *,
    config: dict[str, Any],
    key_deserializer: Callable[..., object] | None,
    value_deserializer: Callable[..., object] | None,
) -> tuple[int, Callable[..., object], Callable[..., object]]:
    """Validate config, resolve the key/value deserializers, and build the
    native consumer handle. Returns ``(handle, key_deser, value_deser)``.

    Raises the typed construction error (e.g. ``InvalidGroupIdError`` for an
    empty ``group.id``) rather than a bare ``RuntimeError``.
    """
    if not isinstance(config, dict):
        raise TypeError("config must be a dict")
    # Reject the old-client callback config keys with a ConfigError naming the
    # replacement (spec §5.7 / §11.1). group.id is deliberately not rejected.
    reject_callback_config_keys(config)

    key_deser = resolve_serde(
        key_deserializer, config, _KEY_DESERIALIZER_KEY,
        is_key=True, default=bytes_deserializer,
    )
    value_deser = resolve_serde(
        value_deserializer, config, _VALUE_DESERIALIZER_KEY,
        is_key=False, default=bytes_deserializer,
    )
    # Honour the optional configure(conf, is_key) lifecycle (config route only;
    # a kwarg serde is used as-is). resolve_serde already called configure for
    # the config route, so this is a no-op for the kwarg route.

    # Build the native config: string-coerced, binding-only keys stripped.
    native_config = {
        k: _stringify(v)
        for k, v in config.items()
        if k not in _BINDING_ONLY_KEYS
    }

    handle, error = _lib.Consumer_KafkaConsumer_new_typed(native_config)
    if error:
        # Java wraps the construction failure; surface the typed cause.
        raise from_ffi_error(error)
    return handle, key_deser, value_deser
