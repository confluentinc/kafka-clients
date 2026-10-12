# Copyright 2026 Confluent Inc.
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

"""``KafkaAdminClient``: Java's ``org.apache.kafka.clients.admin.KafkaAdminClient``.

Every method is :class:`Admin`'s (CLAUDE.md, Python Binding Conventions, Class
family). Java's constructors are private and its static factories, the three
``createInternal``, package-private, so ``__init__`` raises ``TypeError`` naming
the public factory that returns the class, ``Admin.create``; ``Admin.create``
builds it through the private ``_create_internal``, Java's
``createInternal(new AdminClientConfig(configs, true), null)``, which maps to
``kafka_admin_AdminClient_new``. Java's ``AdminClient`` parent is not generated
(KIP-476 keeps it only for compatibility), so the class extends ``Admin`` directly.
"""

from __future__ import annotations

from typing import Any

from .admin import Admin, _AdminState

__all__ = ["KafkaAdminClient"]


class KafkaAdminClient(Admin):
    """The default implementation of ``Admin``. An instance of this class is
    created by invoking ``Admin.create(configs=…)``.

    This class is thread-safe.

    The API of this class is evolving, see ``Admin`` for details.

    Java: ``org.apache.kafka.clients.admin.KafkaAdminClient``.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("KafkaAdminClient cannot be constructed directly; use Admin.create(...)")

    @classmethod
    def _create_internal(cls, *, configs: dict[str, Any]) -> KafkaAdminClient:
        """Java's package-private ``createInternal(AdminClientConfig, …)``: a client
        built from ``configs``."""
        client = cls.__new__(cls)
        _AdminState.__init__(client)
        client._start(configs)
        return client
