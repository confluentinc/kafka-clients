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

"""``Headers``: Java's ``org.apache.kafka.common.header.Headers``.

Java's interface has one concrete class, the internal
``org.apache.kafka.common.header.internals.RecordHeaders``; here they are one
class with the interface's methods and ``RecordHeaders``' constructors,
equality, hash, ``toString`` and read-only state: a ``Headers`` is mutable
until its record is sent (``KafkaProducer.send``, once the serializers ran)
and read-only after, as Java's ``KafkaProducer`` seals it, ``add`` and
``remove`` then raising
``IllegalStateError("RecordHeaders has been closed.")`` (CLAUDE.md, Python
Binding Conventions, Behaviour with no other home). A fetched record's
headers stay writable, as Java's ``CompletedFetch`` leaves them.

Where headers are taken (``ProducerRecord``, ``ConsumerRecord``,
``Headers(headers=…)``), each element is a ``Header`` or a ``(key, value)``
pair, which is turned into a ``Header`` *(deviation: a convenience for writing
headers)*. Header values are not copied (CLAUDE.md §14).
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any, Union, overload

from confluent_kafka._args import MISSING, Form, java_forms
from confluent_kafka.common.header.header import Header, _of as _header, _set_state, _state, _value_view

__all__ = ["Headers"]

#: An element where headers are taken: a ``Header``, or a ``(key, value)`` pair.
_WrittenHeader = Union[Header, "tuple[str, bytes | bytearray | memoryview | None]"]

_CLOSED = "RecordHeaders has been closed."
_NULL_KEY = "key cannot be None."


def _header_of(item: object, index: int) -> Header:
    """One taken element as a ``Header``: a ``Header`` as it is (Java's
    ``RecordHeaders`` keeps the given object), a ``(key, value)`` pair as a new
    one. ``None`` raises ``TypeError``, as Java's
    ``Objects.requireNonNull(header, "Header cannot be null.")``, with the
    Types row's message for a ``None`` element, and so does any other element,
    or a pair whose key is ``None`` (``RecordHeader``'s ``requireNonNull``) or
    not a ``str``."""
    if isinstance(item, Header):
        return item
    if item is None:
        raise TypeError("headers must not contain None")
    if not isinstance(item, tuple) or len(item) != 2:
        raise TypeError(
            f"headers[{index}] must be a Header or a (key, value) tuple; got {item!r}")
    key, value = item
    if key is None:
        raise TypeError("headers must not contain a None key")
    if not isinstance(key, str):
        raise TypeError(f"headers[{index}] key must be a str; got {type(key).__name__}")
    return _header(key, _value_view(value, f"headers[{index}] value"))


class Headers:
    """The headers of a record: an ordered list of ``Header`` s, in which a key
    may repeat.

    Java: ``org.apache.kafka.common.header.Headers``, implemented by
    ``org.apache.kafka.common.header.internals.RecordHeaders``.
    """

    # `_headers` is a tuple until the first change, then a list: a fetched
    # record's headers are built as a tuple and copied into a list only if
    # they are changed. `_read_only` is RecordHeaders' `isReadOnly`.
    __slots__ = ("_headers", "_read_only")

    _headers: list[Header] | tuple[Header, ...]
    _read_only: bool

    @overload
    def __init__(self) -> None: ...
    @overload
    def __init__(self, *, headers: tuple[Header, ...] | None) -> None: ...
    @overload
    def __init__(self, *, headers: Iterable[_WrittenHeader] | None) -> None: ...

    def __init__(self, *, headers: Iterable[_WrittenHeader] | None = MISSING) -> None:
        """Creates headers: empty, or holding the given headers in their order
        (``None`` is no headers, as Java's ``RecordHeaders((Iterable) null)``).
        The given ``Header`` objects are kept, not copied; a ``(key, value)``
        pair becomes a ``Header``. A ``None`` element raises ``TypeError``, as
        Java's ``Objects.requireNonNull(header, "Header cannot be null.")``.
        The new headers are writable, also when copied from read-only ones
        *(deviation: Java's concrete class is the internal ``RecordHeaders``,
        whose ``()``, ``(Header[])`` and ``(Iterable<Header>)`` constructors
        these are)*."""
        self._read_only = False
        if headers is MISSING or headers is None:
            self._headers = ()
        elif isinstance(headers, Headers):
            # RecordHeaders' "efficient copy constructor".
            self._headers = tuple(headers._headers)
        else:
            taken: list[Header] = []
            for i, item in enumerate(headers):
                taken.append(item if type(item) is Header else _header_of(item, i))
            self._headers = tuple(taken)

    @overload
    def add(self, *, header: Header) -> Headers: ...
    @overload
    def add(self, *, key: str, value: bytes | bytearray | memoryview | None) -> Headers: ...

    @java_forms(Form("header"), Form("key", "value"))
    def add(self, *, header: Header = MISSING, key: str = MISSING,
            value: bytes | bytearray | memoryview | None = MISSING) -> Headers:
        """Adds a header (key inside), to the end, returning the headers, so
        calls can be chained: ``add(header=…)``, or ``add(key=…, value=…)``,
        which creates the header from the key and value (``None`` is allowed
        for the value). Raises ``IllegalStateError`` if the headers are in a
        read-only state, and ``TypeError`` for a ``None`` header or key, as
        Java's ``Objects.requireNonNull``."""
        if header is MISSING:
            header = Header(key=key, value=value)
        elif header is None:
            raise TypeError("header must not be None")
        elif not isinstance(header, Header):
            raise TypeError(f"header must be a Header; got {type(header).__name__}")
        self._can_write()
        self._writable().append(header)
        return self

    def remove(self, *, key: str) -> Headers:
        """Removes all headers for the given key, returning the headers, so
        calls can be chained. Raises ``IllegalStateError`` if the headers are
        in a read-only state, and ``IllegalArgumentError`` for a ``None``
        key."""
        self._can_write()
        _check_key(key)
        if any(h.key() == key for h in self._headers):
            self._headers = [h for h in self._headers if h.key() != key]
        return self

    def last_header(self, *, key: str) -> Header | None:
        """Returns just one (the very last) header for the given key, if
        present, else ``None``. Raises ``IllegalArgumentError`` for a ``None``
        key."""
        _check_key(key)
        for header in reversed(self._headers):
            if header.key() == key:
                return header
        return None

    def headers(self, *, key: str) -> list[Header]:
        """Returns all headers for the given key, in the order they were added
        in. Raises ``IllegalArgumentError`` for a ``None`` key."""
        _check_key(key)
        return [h for h in self._headers if h.key() == key]

    def to_array(self) -> tuple[Header, ...]:
        """Returns all headers as an array, in the order they were added in."""
        return tuple(self._headers)

    def __iter__(self) -> Iterator[Header]:
        return iter(self._headers)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, Headers):
            return NotImplemented
        # RecordHeaders.equals: false for another class; the read-only state
        # is not compared.
        if type(other) is not type(self):
            return False
        return list(self._headers) == list(other._headers)

    def __hash__(self) -> int:
        # RecordHeaders.hashCode, its list's: Java hashes these mutable
        # headers too.
        return hash(tuple(self._headers))

    def __str__(self) -> str:
        return ("RecordHeaders(headers = [" + ", ".join(str(h) for h in self._headers)
                + "], isReadOnly = " + str(self._read_only) + ")")

    # -- RecordHeaders' public methods outside the interface, used by the client

    def _set_read_only(self) -> None:
        """Java's ``RecordHeaders.setReadOnly()``: ``KafkaProducer.send`` calls
        it on the record's headers once the serializers ran."""
        self._read_only = True

    def _is_read_only(self) -> bool:
        """Java's ``RecordHeaders.isReadOnly()``."""
        return self._read_only

    def _can_write(self) -> None:
        """RecordHeaders.canWrite."""
        if self._read_only:
            from confluent_kafka.illegal_state_error import IllegalStateError

            raise IllegalStateError(message=_CLOSED)

    def _writable(self) -> list[Header]:
        headers = self._headers
        if isinstance(headers, tuple):
            headers = self._headers = list(headers)
        return headers

    def __copy__(self) -> Headers:
        # A shallow copy, of the same class and state: new headers holding
        # the same Header objects, with the same read-only state. Python's
        # default would share the list, so a change to one would change both.
        # Nothing else is defined for `pickle` or `deepcopy`: Java's
        # `RecordHeaders` is not `Serializable` (CLAUDE.md, Python Binding
        # Conventions, Types).
        copied = type(self).__new__(type(self))
        state = _state(self)
        state["_headers"] = tuple(self._headers)
        _set_state(copied, state)
        return copied

    def _pairs(self) -> tuple[tuple[str, Any], ...]:
        """The ``(key, value)`` pairs the native send path reads: each value
        the header's own view, never a copy (or what a ``Header`` subclass's
        ``value()`` returns). No headers are the shared empty tuple, as Java's
        ``toArray()`` returns the shared ``Record.EMPTY_HEADERS``."""
        if not self._headers:
            return ()
        return tuple((h._key, h._value) if type(h) is Header else (h.key(), h.value())
                     for h in self._headers)


def _check_key(key: object) -> None:
    """RecordHeaders.checkKey."""
    if key is None:
        from confluent_kafka.illegal_argument_error import IllegalArgumentError

        raise IllegalArgumentError(message=_NULL_KEY)


def _of(items: tuple[Header, ...]) -> Headers:
    """New, writable headers holding ``items`` as they are: what the
    constructor builds from ``Header`` objects, without its checks (the hot
    path of a record built with no headers)."""
    headers = Headers.__new__(Headers)
    headers._headers = items
    headers._read_only = False
    return headers


def _taken(headers: Iterable[_WrittenHeader] | None) -> Headers:
    """Java's ``new RecordHeaders(Iterable<Header>)``, which ``ProducerRecord``
    calls: new, writable headers holding the given ones (``None`` is none).
    What the constructor builds, with the common elements (a ``Header``, a
    ``(str, bytes)`` pair) taken without a call each: this runs per record."""
    if headers is None or (type(headers) is tuple and not headers):
        return _of(())
    if isinstance(headers, Headers):
        return _of(tuple(headers._headers))
    taken: list[Header] = []
    new = Header.__new__
    for i, item in enumerate(headers):
        if type(item) is tuple and len(item) == 2 and type(item[0]) is str \
                and type(item[1]) is bytes:
            header = new(Header)
            header._key = item[0]
            header._value = memoryview(item[1])  # read-only, format "B"
            taken.append(header)
        elif type(item) is Header:
            taken.append(item)
        else:
            taken.append(_header_of(item, i))
    return _of(tuple(taken))


def _kept(headers: Iterable[_WrittenHeader]) -> Headers:
    """A ``Headers`` argument as it is, as Java's ``ConsumerRecord`` and
    ``RecordDeserializationException`` keep the object they are given; any
    other iterable as new headers holding its elements."""
    if isinstance(headers, Headers):
        return headers
    if type(headers) is tuple and not headers:
        return _of(())
    return Headers(headers=headers)
