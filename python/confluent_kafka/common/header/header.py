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

"""``Header``: Java's ``org.apache.kafka.common.header.Header``.

Java's interface has one concrete class, the internal
``org.apache.kafka.common.header.internals.RecordHeader``; here they are one
class, built with the keyword constructor ``Header(key=…, value=…)``
*(deviation: Java's concrete class is the internal ``RecordHeader``)*, with
``RecordHeader``'s equality, hash and ``toString`` (CLAUDE.md, Python Binding
Conventions, Behaviour with no other home).

The value is never copied (CLAUDE.md §14): a header keeps a read-only
``memoryview`` of the caller's bytes, or of the fetch batch for a fetched
record, and ``value()`` hands out a fresh view of the same bytes, so a caller
releasing it (``with h.value() as v:``) leaves the header's own intact.
"""

from __future__ import annotations

import copyreg
from typing import Any

__all__ = ["Header"]

_BYTE_LIKE = (bytes, bytearray, memoryview)


def _value_view(value: bytes | bytearray | memoryview | None, name: str) -> memoryview | None:
    """The header's own read-only view of ``value`` (``None`` stays ``None``):
    one C-contiguous run of bytes, cast to unsigned bytes, never copied. A
    value that is not bytes-like or not contiguous raises ``TypeError``; a
    released ``memoryview`` raises ``ValueError``."""
    if value is None:
        return None
    if not isinstance(value, _BYTE_LIKE):
        raise TypeError(f"{name} must be bytes-like or None; got {type(value).__name__}")
    view = memoryview(value)  # a released memoryview raises ValueError here
    if not view.c_contiguous:
        raise TypeError(f"{name} must be a C-contiguous buffer")
    if view.format != "B" or view.ndim != 1:
        view = view.cast("B")
    return view if view.readonly else view.toreadonly()


def _signed_bytes(view: memoryview | None) -> str:
    """Java's ``Arrays.toString(byte[])``: signed bytes, ``None`` for null."""
    if view is None:
        return "None"
    return "[" + ", ".join(str(b - 256 if b > 127 else b) for b in view) + "]"


class Header:
    """A record header: a ``str`` key and a ``bytes`` value, which may be
    ``None``.

    Java: ``org.apache.kafka.common.header.Header``, implemented by
    ``org.apache.kafka.common.header.internals.RecordHeader``.
    """

    __slots__ = ("_key", "_value")

    _key: str
    _value: memoryview | None

    def __init__(self, *, key: str, value: bytes | bytearray | memoryview | None) -> None:
        """Creates a header with the given key and value (``None`` is allowed).
        A ``None`` key raises ``TypeError``, as Java's
        ``Objects.requireNonNull(key, "Null header keys are not permitted")``.
        The value is viewed, not copied."""
        if key is None:
            raise TypeError("key must not be None")
        if not isinstance(key, str):
            raise TypeError(f"key must be a str; got {type(key).__name__}")
        self._key = key
        self._value = _value_view(value, "value")

    def key(self) -> str:
        """Returns the key of the header: the header's key; never ``None``."""
        return self._key

    def value(self) -> memoryview | None:
        """Returns the value of the header: the header's value; may be
        ``None``. A fresh read-only view of the same bytes, never a copy."""
        value = self._value
        return None if value is None else value[:]

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, Header):
            return NotImplemented
        # RecordHeader.equals: `getClass() != o.getClass()` is false.
        if type(other) is not type(self):
            return False
        mine, theirs = self.value(), other.value()
        if mine is None or theirs is None:
            return self.key() == other.key() and mine is theirs
        return self.key() == other.key() and mine == theirs

    def __hash__(self) -> int:
        value = self.value()
        if value is None:
            return hash((self.key(), None))
        try:
            # A read-only byte view hashes its bytes in place, as `bytes` does,
            # when the object it views is hashable itself.
            return hash((self.key(), value))
        except TypeError:
            # A view of a `bytearray` (or another unhashable buffer): the same
            # hash from the same bytes.
            return hash((self.key(), value.tobytes()))

    def __str__(self) -> str:
        return f"RecordHeader(key = {self.key()}, value = {_signed_bytes(self.value())})"

    def __copy__(self) -> Header:
        # A shallow copy, of the same class and state, shares the viewed
        # bytes, as Java's header reference.
        copied = type(self).__new__(type(self))
        _set_state(copied, _state(self))
        return copied

    def __getstate__(self) -> dict[str, Any]:
        """``pickle`` and ``deepcopy`` keep the class and its state, a viewed
        value as its bytes, restored as a read-only view *(deviation: Java's
        ``RecordHeader`` is not ``Serializable``. Why: a record carries its
        headers, and Python's default pickling of a record needs them to
        pickle)* (CLAUDE.md, Python Binding Conventions, Behaviour with no other
        home)."""
        state = _state(self)
        value = state.get("_value")
        if isinstance(value, memoryview):
            state["_value"] = value.tobytes()
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        value = state.get("_value")
        if isinstance(value, bytes):
            state = {**state, "_value": memoryview(value)}
        _set_state(self, state)


def _of(key: str, value: memoryview | None) -> Header:
    """A ``Header`` holding ``key`` and the read-only byte view ``value`` as
    they are: what the constructor builds, without its checks."""
    header = Header.__new__(Header)
    header._key = key
    header._value = value
    return header


def _state(obj: object) -> dict[str, Any]:
    """An object's own state: its ``__dict__``, if any, and every slot set
    along its class's MRO (a subclass's included), under the name Python
    stores it (a private ``__x`` slot is mangled to ``_Class__x``), as
    ``copyreg._slotnames`` lists them for the default pickling."""
    state: dict[str, Any] = dict(getattr(obj, "__dict__", {}))
    for name in copyreg._slotnames(type(obj)):  # type: ignore[attr-defined]  # stdlib private, pickle's own
        if hasattr(obj, name):
            state[name] = getattr(obj, name)
    return state


def _set_state(obj: object, state: dict[str, Any]) -> None:
    for name, value in state.items():
        object.__setattr__(obj, name, value)
