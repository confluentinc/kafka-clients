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

"""``AbstractOptions``: Java's ``org.apache.kafka.clients.admin.AbstractOptions``.

Java's ``abstract`` class is a base class whose construction raises ``TypeError``;
it gets no keyword constructor, and its subclasses construct as Java's do (CLAUDE.md,
Python Binding Conventions, Class family). Its type parameter
``T extends AbstractOptions`` only stands for the class itself, so the setter returns
``typing.Self``. ``timeoutMs()`` and ``timeoutMs(Integer)`` share one name: one
method, two stubs; its argument defaults to ``_args.UNSET``, so an explicit ``None``
sets Java's ``null`` instead of reading (Signatures).
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import UNSET

__all__ = ["AbstractOptions"]


class AbstractOptions:
    """The common APIs shared by the Options classes of the admin client commands.

    Java: ``org.apache.kafka.clients.admin.AbstractOptions`` (``abstract``).
    """

    __slots__ = ("_timeout_ms", "_setters")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if type(self) is AbstractOptions:
            raise TypeError("AbstractOptions is an abstract class; use one of its subclasses")
        # Java's field initializer: ``protected Integer timeoutMs = null``.
        self._timeout_ms: int | None = None
        # The setters that ran, the latest last (``_record``): the client runs the same
        # ones on the C options (CLAUDE.md conversion table, "an Options object").
        self._setters: dict[str, Any] = {}

    def timeout_ms(self, *, timeout_ms: int | None = UNSET) -> Any:
        """With ``timeout_ms``: set the timeout in milliseconds for this operation or
        ``None`` if the default api timeout for the AdminClient should be used, and
        return this object.

        Without: the timeout in milliseconds for this operation or ``None`` if the
        default api timeout for the AdminClient should be used.
        """
        if timeout_ms is UNSET:
            return self._timeout_ms
        self._timeout_ms = timeout_ms
        self._record("timeout_ms", self._timeout_ms)
        return self

    def _record(self, setter: str, value: Any) -> None:
        """Record that ``setter`` ran with ``value`` (the latest run last)."""
        self._setters.pop(setter, None)
        self._setters[setter] = value

    def _setters_ran(self) -> list[tuple[str, Any]]:
        """The ``(setter, value)`` pairs of the setters that ran, in the order they
        last ran."""
        return list(self._setters.items())
