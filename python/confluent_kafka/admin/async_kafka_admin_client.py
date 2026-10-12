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

"""``AsyncKafkaAdminClient``: the asyncio peer of
:class:`~confluent_kafka.admin.KafkaAdminClient`.

By rule 3, ``KafkaAdminClient``'s construction over :class:`AsyncAdmin`'s methods
(CLAUDE.md, Python Binding Conventions, Class family): ``__init__`` raises
``TypeError`` naming ``AsyncAdmin.create``, which builds it through the private
``_create_internal``.
"""

from __future__ import annotations

from typing import Any

from .admin import _AdminState
from .async_admin import AsyncAdmin

__all__ = ["AsyncKafkaAdminClient"]


class AsyncKafkaAdminClient(AsyncAdmin):
    """The asyncio peer of ``KafkaAdminClient``, the default implementation of
    ``AsyncAdmin``: see ``KafkaAdminClient``. An instance is created by invoking
    ``AsyncAdmin.create(configs=…)``.

    Java: ``org.apache.kafka.clients.admin.KafkaAdminClient``.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "AsyncKafkaAdminClient cannot be constructed directly; use AsyncAdmin.create(...)")

    @classmethod
    def _create_internal(cls, *, configs: dict[str, Any]) -> AsyncKafkaAdminClient:
        """See ``KafkaAdminClient._create_internal``."""
        from confluent_kafka._async import Undelivered

        client = cls.__new__(cls)
        _AdminState.__init__(client)
        client._undelivered = Undelivered()
        client._start(configs)
        return client
