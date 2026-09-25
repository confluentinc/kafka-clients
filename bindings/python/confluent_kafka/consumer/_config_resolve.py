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

"""Consumer construction: config parsing, serde resolution, and building the
native handle (CLAUDE.md, Python Binding Conventions, Configuration and
Serialization).

Shared by ``KafkaConsumer`` and ``AsyncKafkaConsumer`` so the two constructors
cannot diverge.
"""

from __future__ import annotations

from typing import Any, Callable

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._config import log_unused, prepare
from confluent_kafka._errors import from_ffi_error
from confluent_kafka.common.serialization import bytes_deserializer
from confluent_kafka.common.serialization._supply import resolve_serde

# Config keys that carry the serde class on the config route.
_KEY_DESERIALIZER_KEY = "key.deserializer"
_VALUE_DESERIALIZER_KEY = "value.deserializer"


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
    # Java's ConsumerConfig parsing: coerced values, the serde keys left to
    # the binding, the unused keys logged once the serdes are configured. A
    # given deserializer argument replaces its config key, which is then not
    # parsed (ConsumerConfig.appendDeserializerToConfig).
    given = [key for key, argument in ((_KEY_DESERIALIZER_KEY, key_deserializer),
                                       (_VALUE_DESERIALIZER_KEY, value_deserializer))
             if argument is not None]
    originals, native_config = prepare(config, client="consumer", given_serdes=given)
    key_deser = resolve_serde(
        key_deserializer, originals, _KEY_DESERIALIZER_KEY,
        is_key=True, default=bytes_deserializer(),
    )
    value_deser = resolve_serde(
        value_deserializer, originals, _VALUE_DESERIALIZER_KEY,
        is_key=False, default=bytes_deserializer(),
    )

    handle, error = _lib.Consumer_KafkaConsumer_new_typed(native_config)
    if error:
        # Java wraps the construction failure; surface the typed cause.
        raise from_ffi_error(error)
    log_unused(originals, client="consumer")
    return handle, key_deser, value_deser
