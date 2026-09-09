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

"""``KafkaProducer`` / ``AsyncKafkaProducer`` — the real producer clients.

Translated from ``org.apache.kafka.clients.producer.KafkaProducer`` (Apache
Kafka 4.3.1). Java's ``KafkaProducer(Properties, Serializer, Serializer)``
collapses to one keyword-only constructor (spec §6.1); all methods are inherited
from :class:`Producer` / :class:`AsyncProducer`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, cast

from confluent_kafka import IllegalArgumentError
from confluent_kafka._config import reject_callback_config_keys
from confluent_kafka.common.serialization import bytes_serializer, resolve_serde

from .async_producer import AsyncProducer
from .producer import Producer

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common.serialization import Serializer

K = TypeVar("K")
V = TypeVar("V")

# Java's ProducerConfig keys for a serializer supplied through config
# (a dotted class path / class object). The kwarg wins over these (spec §5.4).
_KEY_SERIALIZER_KEY = "key.serializer"
_VALUE_SERIALIZER_KEY = "value.serializer"


def _prepare_config(config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise IllegalArgumentError("config must be a dict")
    # Reject the old-client callback config keys (§11.1); each names its
    # replacement. group.id is intentionally not rejected.
    reject_callback_config_keys(config)
    return config


def _resolve_serializers(
        config: dict[str, Any],
        key_serializer: Serializer[Any],
        value_serializer: Serializer[Any],
) -> tuple[Serializer[Any], Serializer[Any]]:
    """Resolve the key/value serializers (kwarg wins over the config route),
    honouring ``configure(conf, is_key)`` on the config route (spec §5.4)."""
    key = resolve_serde(
        key_serializer, config, _KEY_SERIALIZER_KEY,
        is_key=True, default=bytes_serializer())
    value = resolve_serde(
        value_serializer, config, _VALUE_SERIALIZER_KEY,
        is_key=False, default=bytes_serializer())
    # ``resolve_serde`` returns the generic ``Callable`` supply type; both routes
    # yield a callable of the ``Serializer`` shape (kwarg is checked, config
    # constructs one), so the narrower stored type is exact.
    return cast("Serializer[Any]", key), cast("Serializer[Any]", value)


class KafkaProducer(Producer[K, V]):
    """A Kafka producer connected to a real cluster.

    Java: ``KafkaProducer<K, V>`` — constructor only; every method is inherited
    from :class:`Producer`."""

    def __init__(self, *, config: dict[str, Any],
                 key_serializer: Serializer[Any] = bytes_serializer(),
                 value_serializer: Serializer[Any] = bytes_serializer(),
                 partitioner: object | None = None) -> None:
        Producer.__init__(self)
        config = _prepare_config(config)
        self._key_serializer, self._value_serializer = _resolve_serializers(
            config, key_serializer, value_serializer)
        _reject_partitioner(partitioner)
        self._init_kafka(config)

    def close(self, *, timeout: Duration | None = None) -> None:
        super().close(timeout=timeout)
        _close_serializers(self)


class AsyncKafkaProducer(AsyncProducer[K, V]):
    """The asyncio-native real client — same constructor as ``KafkaProducer``;
    methods inherited from :class:`AsyncProducer`."""

    def __init__(self, *, config: dict[str, Any],
                 key_serializer: Serializer[Any] = bytes_serializer(),
                 value_serializer: Serializer[Any] = bytes_serializer(),
                 partitioner: object | None = None) -> None:
        AsyncProducer.__init__(self)
        config = _prepare_config(config)
        self._key_serializer, self._value_serializer = _resolve_serializers(
            config, key_serializer, value_serializer)
        _reject_partitioner(partitioner)
        self._init_kafka(config)

    async def close(self, *, timeout: Duration | None = None) -> None:
        await super().close(timeout=timeout)
        _close_serializers(self)


def _reject_partitioner(partitioner: object | None) -> None:
    """The ``partitioner=`` argument declares the pluggable surface (spec §6.1);
    the custom-partitioner plumbing is finalized in the plugin design pass. Until
    the FFI has a hook, a non-None value is rejected rather than silently ignored
    (the default Java partitioning always applies). A clarification records the
    gap."""
    if partitioner is not None:
        raise IllegalArgumentError(
            "custom partitioner is not yet supported; the pluggable partitioner "
            "surface is finalized in the plugin design pass (spec §6.1)")


def _close_serializers(producer: object) -> None:
    """Java `Serializer.close()` on both serializers at producer close
    (spec §5.4). Idempotent; a failing close is logged, not raised."""
    from confluent_kafka.common.serialization import close_if_defined
    close_if_defined(producer._key_serializer)   # type: ignore[attr-defined]
    close_if_defined(producer._value_serializer)  # type: ignore[attr-defined]
