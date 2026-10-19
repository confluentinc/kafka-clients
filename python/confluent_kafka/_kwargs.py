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

"""The keyword contract of the user callables the clients call (private).

Serdes and single-method callbacks are called with keyword arguments named
after Java's parameters and must accept ``**kwargs``, so a parameter Java adds
later reaches them as one more keyword without breaking existing ones; a
callable without ``**kwargs`` is rejected with ``IllegalArgumentError`` where
it is given (CLAUDE.md, Python Binding Conventions, Signatures and
Serialization). :func:`require_kwargs` is that check: one look at the code
object for a function, a bound method or an object whose ``__call__`` is a
function (the usual callables, and the check runs on every ``send()`` with a
callback), ``inspect.signature`` for anything else. A callable whose signature
cannot be read is taken as accepting ``**kwargs``: nothing tells otherwise.
:func:`serde_caller` picks, once per serde, the cheapest call that keeps that
contract on the per-record path.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from types import FunctionType, MethodType
from typing import cast

from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["DESERIALIZER_NAMES", "SERIALIZER_NAMES", "accepts_kwargs", "require_kwargs",
           "serde_caller"]

_CO_VARKEYWORDS = inspect.CO_VARKEYWORDS


def _function_of(fn: object) -> FunctionType | None:
    """The plain function a call of ``fn`` runs, when there is one: ``fn``
    itself, a bound method's function, or the ``__call__`` of ``fn``'s class."""
    if type(fn) is FunctionType:
        return fn
    if type(fn) is MethodType:
        inner = fn.__func__
        return inner if type(inner) is FunctionType else None
    call = getattr(type(fn), "__call__", None)
    if type(call) is FunctionType and not isinstance(fn, type):
        return call
    return None


def accepts_kwargs(fn: object) -> bool:
    """Whether ``fn`` declares ``**kwargs`` (``True`` when its signature
    cannot be read).

    It answers as ``inspect.signature`` does, without its cost on the usual
    callables: a function (a bound method's, a callable object's
    ``__call__``) is read from its code object, after following
    ``__wrapped__`` to what a ``functools.wraps`` decorator wraps; a
    ``functools.partial`` accepts extra keywords exactly when its ``func``
    does, so it is unwrapped first. Anything else goes through
    ``inspect.signature``."""
    seen: set[int] | None = None
    while True:
        function = _function_of(fn)
        if function is not None:
            wrapped = getattr(function, "__wrapped__", None)
            if wrapped is None:
                return bool(function.__code__.co_flags & _CO_VARKEYWORDS)
            # A __wrapped__ cycle (functools.wraps(f)(f) sets f.__wrapped__ = f)
            # ends the walk, as inspect.unwrap stops on one; the answer is the
            # function reached. The set exists only once a wrapper is met.
            if seen is None:
                seen = set()
            seen.add(id(function))
            if id(wrapped) in seen or id(_function_of(wrapped)) in seen:
                return bool(function.__code__.co_flags & _CO_VARKEYWORDS)
            fn = wrapped
        elif type(fn) is functools.partial:
            fn = fn.func
        else:
            break
    try:
        parameters = inspect.signature(fn).parameters.values()  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return True
    return any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters)


def require_kwargs(fn: object, name: str) -> None:
    """Raise ``IllegalArgumentError("<name> must accept **kwargs")`` unless
    ``fn`` accepts ``**kwargs``."""
    if not accepts_kwargs(fn):
        raise IllegalArgumentError(message=f"{name} must accept **kwargs")


# The modules of the built-in serdes, whose private ``_fast`` entry is trusted.
_BUILT_IN_SERDES = "confluent_kafka.common.serialization."

#: Java's ``Serializer.serialize`` / ``Deserializer.deserialize`` parameter names.
SERIALIZER_NAMES = ("topic", "data", "headers")
DESERIALIZER_NAMES = ("topic", "data", "headers")


def serde_caller(serde: Callable[..., object],
                 names: tuple[str, str, str]) -> Callable[..., object]:
    """The positional callable a client calls ``serde`` through, once per key
    and per value, as ``call(topic, data, headers)``; chosen once,
    where the serde is given (CLAUDE.md, Python Binding Conventions,
    Signatures: the binding may call a registered callable positionally when
    its Java-named parameters allow it; the contract is unchanged).

    * A built-in serde (the factories of ``confluent_kafka.common.serialization``)
      is called through its private positional entry ``_fast``, which skips
      the keyword binding and the ``**kwargs`` dict of its public ``__call__``.
    * A function, bound method or callable object whose positional parameters
      are Java's ``names``, in Java's order (more only with defaults), is
      called positionally: only the empty ``**kwargs`` dict its own signature
      asks for remains.
    * Anything else (Java's names keyword-only or in another order, or a
      signature that cannot be read) is called by keyword through one closure
      made here.

    A parameter Java adds later goes by keyword to every serde, the positional
    ones included (``call(topic, data, headers, new=...)``), so one that does
    not name it receives it in its ``**kwargs``."""
    if type(serde).__module__.startswith(_BUILT_IN_SERDES):
        fast = getattr(serde, "_fast", None)
        if fast is not None:
            return cast("Callable[..., object]", fast)
    if _positional(serde, names):
        return serde
    def by_keyword(topic: str, data: object, headers: object) -> object:
        return serde(topic=topic, data=data, headers=headers)

    return by_keyword


def _positional(fn: object, names: tuple[str, ...]) -> bool:
    """Whether ``fn``'s positional parameters start with ``names``, in order,
    and any after them have defaults."""
    function = _function_of(fn)
    if function is None:
        return False
    code = function.__code__
    positional = code.co_varnames[:code.co_argcount]
    defaults = len(function.__defaults__ or ())
    if type(fn) is not FunctionType:  # a bound method or a __call__: skip self
        positional = positional[1:]
    return (positional[:len(names)] == names
            and len(positional) - len(names) <= defaults)
