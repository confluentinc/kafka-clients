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

"""``java.lang.Throwable``'s state for the generated error classes.

Every generated error class (the ``KafkaError`` hierarchy and the Java built-in
exceptions at the package root) records what Java's ``Throwable`` constructors
record: the detail message (``getMessage()``, ``str(e)``) and the cause
(``getCause()``, ``e.__cause__``). The constructor arguments are kept by name so
``copy`` and ``pickle`` can rebuild the error through its keyword-only
constructor (CLAUDE.md, Python Binding Conventions, Errors).

Private: the generated classes call these helpers; nothing else should.
"""

from __future__ import annotations

import warnings
from collections.abc import Collection, Iterable, Mapping
from typing import Any, TypeVar, cast

from ._args import UNSET

__all__ = [
    "cause_message",
    "copy_dict",
    "copy_set",
    "headers",
    "init",
    "kwargs",
    "message_text",
    "rebuild",
    "reduce",
    "singleton",
    "to_string",
    "view",
]

_MESSAGE = "_java_message"
_KWARGS = "_java_kwargs"
_SINGLETON = "_java_singleton"


def init(self: BaseException, message: str | None, cause: BaseException | None) -> None:
    """Java's ``Throwable(message, cause)``: record the detail message (``None``
    is Java's ``null``) and the cause (``__cause__``)."""
    setattr(self, _MESSAGE, message)
    BaseException.__init__(self, *(() if message is None else (message,)))
    if cause is not None:
        self.__cause__ = cause


def java_message(t: BaseException) -> str | None:
    """Java's ``getMessage()``: a generated error's recorded message; for any
    other exception its ``str``, ``None`` when that is empty."""
    try:
        recorded = object.__getattribute__(t, _MESSAGE)
    except AttributeError:
        text = str(t)
        return text if text else None
    return recorded  # type: ignore[no-any-return]


def message_text(t: BaseException) -> str:
    """``str(e)``: Java's ``getMessage()``, ``""`` when it is ``null``."""
    message = java_message(t)
    return "" if message is None else message


def to_string(t: BaseException) -> str:
    """Java's ``Throwable.toString()``: ``"<module>.<class>"``, plus
    ``": <message>"`` when the throwable has a message."""
    cls = type(t)
    name = f"{cls.__module__}.{cls.__qualname__}"
    message = java_message(t)
    return name if message is None else f"{name}: {message}"


def cause_message(cause: BaseException | None) -> str | None:
    """Java's ``Throwable(Throwable cause)`` message:
    ``cause == null ? null : cause.toString()``."""
    return None if cause is None else to_string(cause)


def kwargs(**given: Any) -> dict[str, Any]:
    """The constructor arguments to rebuild from, ``UNSET`` ones left out."""
    return {k: v for k, v in given.items() if v is not UNSET}


def rebuild(cls: type[BaseException], arguments: dict[str, Any]) -> BaseException:
    """Rebuild an error from its constructor arguments (the target of
    :func:`reduce`); a deprecated constructor form warned when the error was
    first built, not again when it is copied."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return cls(**arguments)


def _portable(value: Any) -> Any:
    """A constructor argument as it pickles: a ``memoryview`` (a buffer the
    error views, such as a fetched record's key) as the bytes it shows."""
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, (list, tuple)):
        return type(value)(_portable(v) for v in value)
    return value


def reduce(self: BaseException) -> str | tuple[Any, ...]:
    """``__reduce__`` for every generated error: a singleton pickles by name;
    any other error rebuilds from its constructor arguments by keyword. The
    constructor rebuilds the error's own state; only attributes a caller added
    (public names) travel as state."""
    singleton_name = self.__dict__.get(_SINGLETON)
    if singleton_name is not None:
        return str(singleton_name)
    state = {k: v for k, v in self.__dict__.items() if not k.startswith("_")} or None
    arguments = self.__dict__.get(_KWARGS)
    if arguments is None:
        # Built without its constructor (an error from the core whose Java
        # constructors cannot take what the core reported): keep the message.
        return (_rebuild_private, (type(self), java_message(self)), state)
    return (rebuild, (type(self), {k: _portable(v) for k, v in arguments.items()}), state)


def _rebuild_private(cls: type[BaseException], message: str | None) -> BaseException:
    return new(cls, message)


def new(cls: type[BaseException], message: str | None) -> BaseException:
    """An error built without its constructor, carrying only a message."""
    error = cls.__new__(cls)
    init(error, message, None)
    return error


def singleton(cls: type[BaseException], name: str, message: str | None,
              cause: BaseException | None) -> Any:
    """A ``public static final`` instance (``DisconnectError.INSTANCE``), built
    as Java's constructor builds it; it pickles and copies as itself."""
    error = cls.__new__(cls)
    init(error, message, cause)
    setattr(error, _KWARGS, {})
    setattr(error, _SINGLETON, name)
    return error


_I = TypeVar("_I", bound="Iterable[Any] | None")


def materialize(value: _I) -> _I:
    """An ``Iterable`` argument read once, as Java's ``Set.copyOf`` does: a
    collection (or ``None``, ``UNSET``) as it is, any other iterable (a
    generator) as a tuple of its elements, in order."""
    if value is None or value is UNSET or isinstance(value, Collection):
        return value
    return cast(_I, tuple(value))


def copy_set(value: Iterable[Any] | None) -> set[Any] | None:
    """A ``Set`` field from an ``Iterable`` argument (``None`` stays ``None``)."""
    return None if value is None else set(value)


def copy_dict(value: Mapping[Any, Any] | None) -> dict[Any, Any] | None:
    """A ``Map`` field from a ``Mapping`` argument (``None`` stays ``None``)."""
    return None if value is None else dict(value)


def view(value: bytes | bytearray | memoryview | None) -> memoryview | None:
    """A ``ByteBuffer`` field: a ``memoryview`` of the argument, never a copy."""
    return None if value is None else memoryview(value)


def headers(value: Iterable[tuple[str, bytes | bytearray | memoryview | None]]
            ) -> tuple[tuple[str, memoryview | None], ...]:
    """A ``Headers`` field: the read form, header values as ``memoryview``."""
    from .common.headers import _read_headers

    return _read_headers(value)
