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

"""Java overload matching for collapsed methods (``java_forms``, ``UNSET``).

Java resolves an overload at compile time. A Python method that collapses a
Java overload set into one keyword-only signature (CLAUDE.md, Python Binding
Conventions, Signatures) accepts the union of the overloads' parameters, so it
must reject at runtime any set of given arguments that matches no Java
overload. ``java_forms`` is the one mechanism for that: it lists the method's
Java overloads, unfolded, each with its parameters in Java declaration order and
the Java-given defaults a shorter overload passes to it.

A set of given names matches an overload only when it is exactly that
overload's parameter set; nothing is filled in to make a match, except that a
serializer / deserializer the overload names in ``serdes`` may be left out on
its own (then ``None``). The parameters a matched set leaves out are filled
with the values Java's shorter overload passes for them: the Java-given
defaults of the overload with the most parameters (the earliest-declared on a
tie) that the set reaches by leaving out only defaulted parameters. When no
overload matches, the call raises ``IllegalArgumentError`` naming every
overload::

    close() takes one of (), (timeout), (option); got (timeout, option)

"Given" means "not left at its default": a keyword argument equal to its
implementation-signature default counts as not given. ``UNSET`` is that default
for a parameter one form requires and another defaults, so a given value equal
to the Java default (``acknowledge(type=AcknowledgeType.ACCEPT)``) still counts
as given, and for a nullable parameter whose omission leaves a set that is no
Java overload, so a ``None`` for it counts as given
(``ProducerRecord(topic=…, partition=0, key=None, value=…)``).

The check is built once, when the decorated function is defined (at class
definition), not on every call.
"""

from __future__ import annotations

import functools
import inspect
import linecache
import warnings
from collections.abc import Callable, Mapping
from typing import Any, TypeVar

__all__ = ["UNSET", "Form", "is_given", "java_forms", "reject_str"]

_F = TypeVar("_F", bound=Callable[..., Any])


class _Unset:
    """The type of :data:`UNSET`; a private singleton."""

    __slots__ = ()

    _instance: _Unset | None = None

    def __new__(cls) -> _Unset:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNSET"

    def __reduce__(self) -> str:
        return "UNSET"


#: The "not given" default of a parameter one form requires and another
#: defaults. Typed ``Any`` so it can be the implementation-signature default of
#: a parameter of any type; the ``@overload`` stubs show the Java value.
UNSET: Any = _Unset()

#: Keyword a decorated function may declare (keyword-only, private) to receive
#: the index of the Java overload that matched. It is not part of the public
#: signature, and a caller cannot pass it.
_FORM_KEYWORD = "_java_form"


class Form:
    """One Java overload: its parameters and its Java-given defaults.

    ``params`` are the overload's Python parameter names in Java declaration
    order. ``defaults`` maps a parameter of this overload to the value a
    shorter overload passes to it (a ``null``, a constant or an empty
    collection): the fill of the shorter overload's exact set, never a
    parameter left out to make a match. ``serdes`` names the serializer /
    deserializer parameters of this overload, the one exception to exact
    matching: each may be left out on its own, and is then ``None``.
    ``deprecated`` is Java's ``@deprecated`` note when the overload is
    ``@Deprecated``: calling it emits a ``DeprecationWarning``.
    """

    __slots__ = ("params", "defaults", "serdes", "deprecated")

    def __init__(self, *params: str, defaults: Mapping[str, object] | None = None,
                 serdes: tuple[str, ...] = (), deprecated: str | None = None) -> None:
        self.params: tuple[str, ...] = params
        self.defaults: dict[str, object] = dict(defaults or {})
        self.serdes: frozenset[str] = frozenset(serdes)
        self.deprecated = deprecated
        unknown = (set(self.defaults) | self.serdes) - set(params)
        if unknown:
            raise TypeError(
                f"Form{params}: defaults or serdes name parameters it does not have: "
                f"{sorted(unknown)}")


class _Required:
    __slots__ = ()


_REQUIRED = _Required()


def _is_not_given(value: object, marker: object) -> bool:
    """Whether ``value`` is left at its implementation default ``marker``."""
    if value is marker:
        return True
    if marker is None or marker is UNSET or marker is _REQUIRED:
        return False
    # A constant default: equal and of the same type (``0`` is not ``False``).
    try:
        return type(value) is type(marker) and bool(value == marker)
    except Exception:  # noqa: BLE001 - an exotic __eq__ never reads as "not given"
        return False


def reject_str(name: str, value: object, *, element: str = "str") -> None:
    """Refuse a ``str``, ``bytes`` or ``bytearray`` given for ``name``, a parameter
    whose Java type is a collection of ``String`` (CLAUDE.md, Python Binding
    Conventions, Signatures). Java cannot receive one there; Python would iterate
    it by character, so ``subscribe(topics="orders")`` would subscribe to "o", "r",
    "d", "e", "s" and ``delete_topics(topics="orders")`` would delete them. Python's
    own ``TypeError``, raised before any other check, like a positional call's. ``element``
    names what the iterable holds when it is not ``str`` (``"lists of str"`` for a
    ``List<List<String>>``)."""
    if isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{name} must be an iterable of {element}, not {type(value).__name__}")


def is_given(value: object, default: object) -> bool:
    """Whether ``value`` counts as given against its implementation default
    ``default`` (the generated error constructors test given-ness with it)."""
    return not _is_not_given(value, default)


class _Missing:
    """A keyword a specialised wrapper's call left out (see ``_specialise``)."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<missing>"


_MISSING: Any = _Missing()


def _specialise(fn: Callable[..., Any], generic: Callable[..., Any],
                params: list[inspect.Parameter], n_positional: int, pass_form: bool,
                always: int, bit: Mapping[str, int], marker: Mapping[str, object],
                table: Mapping[int, tuple[int, tuple[tuple[str, object], ...], str | None]],
                illegal: Callable[[int], Exception]) -> Callable[..., Any] | None:
    """The wrapper of a ``java_forms`` method, written for its own keywords.

    The generic wrapper packs a call's keywords into a dict and reads them one
    by one to find the Java overload. This one is generated once, when the
    class is defined, with the method's positional parameters and keywords as
    its own parameters (at the method's defaults), and one branch per set of
    given names that the table admits, each calling ``fn`` with that set's
    Java-given defaults written in. So a call costs a few ``is`` checks, one
    integer comparison per branch tried and the call itself. It gives the same
    results: the given mask, the overload, the Java-given defaults, the error
    and the deprecation warning are the generic wrapper's. A call that one
    leaves to ``fn`` (an unknown or a missing keyword, a missing or an extra
    positional argument, ``_java_form`` passed in) is handed to the generic
    wrapper with only the arguments it was given, so Python raises its own
    ``TypeError``, word for word.
    A method with ``*args`` or ``**kwargs`` of its own keeps the generic one.
    """
    if any(p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD) for p in params):
        return None
    names = [p.name for p in params if p.kind is p.KEYWORD_ONLY and p.name != _FORM_KEYWORD]
    # The positional parameters, positional-only so that a keyword of the same
    # name lands in **_jf_extra (and so in the generic wrapper), as before.
    positional = [f"_jf_p{i}" for i in range(n_positional)]
    ns: dict[str, Any] = {
        "_jf_fn": fn, "_jf_generic": generic,
        "_jf_illegal": illegal, "_jf_warn": warnings.warn, "_jf_dw": DeprecationWarning,
        "_jf_missing": _MISSING, "_jf_is_not_given": _is_not_given,
    }
    signature: list[str] = []
    missing: list[str] = [f" or {p} is _jf_missing" for p in positional]
    checks: list[str] = []
    start = always
    for i, name in enumerate(names):
        m = marker[name]
        if m is _REQUIRED:
            signature.append(f"{name}=_jf_missing")
            missing.append(f" or {name} is _jf_missing")
            start |= bit[name]  # given whenever present
        else:
            # Left out, it is the method's default (not given); passed, it is
            # given unless it reads as that default.
            ns[f"_jf_m{i}"] = m
            signature.append(f"{name}=_jf_missing")
            test = (f"{name} is not _jf_m{i}" if m is None or m is UNSET
                    else f"not _jf_is_not_given({name}, _jf_m{i})")
            checks.append(f"    if {name} is _jf_missing:\n        {name} = _jf_m{i}\n"
                          f"    elif {test}:\n        _jf_g |= {bit[name]}")
    is_async = inspect.iscoroutinefunction(fn)
    await_ = "await " if is_async else ""
    # One branch per admitted set of given names: the overload's Java-given
    # defaults (the same objects the table holds) go straight into the call,
    # and a deprecated set warns from the wrapper, as the generic one does.
    pos_args = "".join(f"{p}, " for p in positional)
    branches: list[str] = []
    for k, (given, (index, fills, note)) in enumerate(table.items()):
        filled = dict(fills)
        call: list[str] = []
        for i, name in enumerate(names):
            if name in filled:
                ns[f"_jf_v{k}_{i}"] = filled[name]
                call.append(f"{name}=_jf_v{k}_{i}")
            else:
                call.append(f"{name}={name}")
        if pass_form:
            call.append(f"_java_form={index}")
        warn = ""
        if note is not None:
            ns[f"_jf_note{k}"] = note
            warn = f"        _jf_warn(_jf_note{k}, _jf_dw, stacklevel=2)\n"
        branches.append(f"    if _jf_g == {given}:\n{warn}"
                        f"        return {await_}_jf_fn({pos_args}{', '.join(call)})\n")
    passed = ", ".join(f"({name!r}, {name})" for name in names)
    # The positional arguments a call that goes to the generic wrapper gave:
    # they fill the parameters left to right, then the rest is in _jf_more.
    passed_pos = (f"(*(_jf_x for _jf_x in ({', '.join(positional)},) if _jf_x is not "
                  f"_jf_missing), *_jf_more)" if positional else "_jf_more")
    params_src = ", ".join(signature)
    src = (
        f"{'async ' if is_async else ''}def _jf_wrapper("
        f"{''.join(f'{p}=_jf_missing, ' for p in positional)}{'/, ' if positional else ''}"
        f"*_jf_more{', ' + params_src if params_src else ''}, **_jf_extra):\n"
        f"    if _jf_extra or _jf_more{''.join(missing)}:\n"
        f"        return {await_}_jf_generic(*{passed_pos}, **{{_jf_n: _jf_x for _jf_n, _jf_x in "
        f"({passed}{',' if len(names) == 1 else ''}) if _jf_x is not _jf_missing}}, "
        f"**_jf_extra)\n"
        f"    _jf_g = {start}\n"
        + "".join(c + "\n" for c in checks)
        + "".join(branches)
        + "    raise _jf_illegal(_jf_g)\n"
    )
    # Registered with linecache so a traceback through the wrapper shows its
    # source line, as attrs and dataclasses do for the code they generate.
    # The module is in the name, so two classes of one name cannot share it.
    filename = f"<java_forms {fn.__module__}.{fn.__qualname__}>"
    linecache.cache[filename] = (len(src), None, src.splitlines(True), filename)
    exec(compile(src, filename, "exec"), ns)  # noqa: S102
    specialised: Any = ns["_jf_wrapper"]
    specialised.__code__ = specialised.__code__.replace(co_name=fn.__name__)
    return functools.wraps(fn)(specialised)


def _method_name(fn: Callable[..., Any], name: str | None) -> str:
    if name is not None:
        return name
    if fn.__name__ == "__init__":
        # The class name for a constructor ("TopicIdPartition.__init__").
        parts = fn.__qualname__.split(".")
        if len(parts) >= 2:
            return parts[-2]
    return fn.__name__


def java_forms(*forms: Form, name: str | None = None) -> Callable[[_F], _F]:
    """Decorate a collapsed method with its Java overloads (see the module doc).

    ``forms`` are the overloads, unfolded, in Java declaration order. ``name``
    overrides the method name of the error message (the class name for a
    constructor, else the function name).
    """

    def decorate(fn: _F) -> _F:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        n_positional = sum(
            1 for p in params
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD))
        keyword = [p for p in params if p.kind is p.KEYWORD_ONLY]
        pass_form = any(p.name == _FORM_KEYWORD for p in keyword)
        keyword = [p for p in keyword if p.name != _FORM_KEYWORD]
        order = [p.name for p in keyword]
        bit = {n: 1 << i for i, n in enumerate(order)}
        marker: dict[str, object] = {
            p.name: (_REQUIRED if p.default is p.empty else p.default)
            for p in keyword
        }
        required = tuple(n for n in order if marker[n] is _REQUIRED)

        def mask_of(names: tuple[str, ...] | list[str]) -> int:
            m = 0
            for n in names:
                if n not in bit:
                    raise TypeError(
                        f"java_forms on {fn.__qualname__}: {n!r} is not a "
                        "keyword-only parameter")
                m |= bit[n]
            return m

        def in_order(m: int) -> str:
            return "(" + ", ".join(n for n in order if m & bit[n]) + ")"

        def size(m: int) -> int:
            return bin(m).count("1")

        method = _method_name(fn, name)
        form_masks = [mask_of(f.params) for f in forms]
        # A parameter in every overload that no overload defaults is required: it
        # always counts as given, even at a None default (the errors' `cause`,
        # which Java requires but which may be null).
        always = -1
        for m in form_masks:
            always &= m
        always = always if forms else 0
        for form in forms:
            always &= ~mask_of(list(form.defaults) + list(form.serdes))
        # given mask -> (form index, fills, deprecation warning text). Exact
        # matching: the given sets are the overloads' own parameter sets, with
        # or without each of their serdes. A set's fill comes from the overload
        # with the most parameters (the earliest-declared on a tie) that reaches
        # it by leaving out only parameters it defaults, or serdes (then None):
        # the values Java's shorter overload passes for what it lacks.
        table: dict[int, tuple[int, tuple[tuple[str, object], ...], str | None]] = {}
        for f_index, form in enumerate(forms):
            serdes = [n for n in form.params if n in form.serdes]
            for subset in range(1 << len(serdes)):
                given = form_masks[f_index] & ~mask_of(
                    [serdes[i] for i in range(len(serdes)) if subset & (1 << i)])
                if given in table:
                    continue
                best: tuple[int, list[str]] | None = None
                for index, candidate in enumerate(forms):
                    if given & ~form_masks[index]:
                        continue
                    left_out = [n for n in candidate.params if not given & bit[n]]
                    if any(n not in candidate.defaults and n not in candidate.serdes
                           for n in left_out):
                        continue
                    # Most parameters wins; the earliest-declared on a tie.
                    if best is None or size(form_masks[index]) > size(form_masks[best[0]]):
                        best = (index, left_out)
                assert best is not None  # the form itself reaches its own set
                index, left_out = best
                table[given] = (
                    index, tuple((n, forms[index].defaults.get(n)) for n in left_out), None)
        # A deprecated overload warns when it is the match, or when it is called
        # with exactly its own parameters.
        for given, (index, fills, _) in list(table.items()):
            note = forms[index].deprecated
            if note is None:
                for j, form in enumerate(forms):
                    if form.deprecated is not None and form_masks[j] == given:
                        note = form.deprecated
                        break
            if note is not None:
                table[given] = (
                    index, fills, f"{method}{in_order(given)} is deprecated. {note}")
        prefix = (f"{method}() takes one of "
                  f"{', '.join(in_order(m) for m in form_masks)}; got ")

        def resolve(args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
            """Match the given names, fill the Java-given defaults into
            ``kwargs``, or raise. A call Python itself rejects (a positional
            argument, an unknown or a missing keyword) is left to ``fn``, so it
            raises Python's own ``TypeError``."""
            if pass_form and _FORM_KEYWORD in kwargs:
                raise TypeError(
                    f"{fn.__qualname__}() got an unexpected keyword argument "
                    f"'{_FORM_KEYWORD}'")
            if len(args) != n_positional:
                return
            given = always
            for key, value in kwargs.items():
                b = bit.get(key)
                if b is None:
                    return
                if not _is_not_given(value, marker[key]):
                    given |= b
            for key in required:
                if key not in kwargs:
                    return
            entry = table.get(given)
            if entry is None:
                from .illegal_argument_error import IllegalArgumentError

                raise IllegalArgumentError(message=prefix + in_order(given))
            index, fills, note = entry
            for key, value in fills:
                kwargs[key] = value
            if note is not None:
                warnings.warn(note, DeprecationWarning, stacklevel=3)
            if pass_form:
                kwargs[_FORM_KEYWORD] = index

        generic: Any
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                resolve(args, kwargs)
                return await fn(*args, **kwargs)

            generic = async_wrapper
        else:
            @functools.wraps(fn)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                resolve(args, kwargs)
                return fn(*args, **kwargs)

            generic = sync_wrapper

        def illegal(given: int) -> Exception:
            from .illegal_argument_error import IllegalArgumentError

            return IllegalArgumentError(message=prefix + in_order(given))

        wrapper: Any = _specialise(fn, generic, params, n_positional, pass_form, always,
                                   bit, marker, table, illegal) or generic

        if pass_form:
            wrapper.__signature__ = sig.replace(
                parameters=[p for p in params if p.name != _FORM_KEYWORD])
        wrapper.__java_forms__ = forms
        result: _F = wrapper
        return result

    return decorate
