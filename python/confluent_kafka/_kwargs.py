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
"""

from __future__ import annotations

import inspect
from types import FunctionType, MethodType

from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["accepts_kwargs", "require_kwargs"]

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
    cannot be read)."""
    function = _function_of(fn)
    if function is not None:
        return bool(function.__code__.co_flags & _CO_VARKEYWORDS)
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
