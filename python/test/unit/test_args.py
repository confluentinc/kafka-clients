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

"""Tests for ``confluent_kafka._args``: the ``java_forms`` overload matcher,
``MISSING`` and the rename aliases (CLAUDE.md, Python Binding Conventions,
Signatures and Stability)."""

from __future__ import annotations

import asyncio
import copy
import inspect
import pickle
import pydoc
import warnings
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka._args import MISSING, Form, java_forms


class _Close:
    """``close()``, ``close(Duration timeout)``, ``close(CloseOptions option)``."""

    @java_forms(Form(), Form("timeout"), Form("option"))
    def close(self, *, timeout: float | None = MISSING,
              option: object | None = MISSING) -> tuple[Any, Any]:
        return timeout, option


class _Acknowledge:
    """``acknowledge(record)``, ``acknowledge(record, type)``,
    ``acknowledge(topic, partition, offset, type)``: ``type`` is required by
    the last form and filled for the first."""

    @java_forms(Form("record"),
                Form("record", "type", defaults={"type": "ACCEPT"}),
                Form("topic", "partition", "offset", "type"))
    def acknowledge(self, *, record: object | None = MISSING,
                    topic: str | None = MISSING, partition: int | None = MISSING,
                    offset: int | None = MISSING, type: str = MISSING) -> Any:  # noqa: A002
        return record, topic, partition, offset, type


class _OffsetAndMetadata:
    """Java's three ``OffsetAndMetadata`` constructors."""

    @java_forms(Form("offset", "leader_epoch", "metadata",
                     defaults={"leader_epoch": None}),
                Form("offset", "metadata", defaults={"metadata": ""}),
                Form("offset"))
    def __init__(self, *, offset: int, leader_epoch: int | None = MISSING,
                 metadata: str = MISSING, _java_form: int = -1) -> None:
        self.values = (offset, leader_epoch, metadata)
        self.form = _java_form


def test_exact_message_lists_every_overload_and_the_given_names() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        _Close().close(timeout=1.0, option=object())
    assert str(exc.value) == (
        "close() takes one of (), (timeout), (option); got (timeout, option)")


def test_every_java_overload_is_accepted() -> None:
    c = _Close()
    assert c.close() == (MISSING, MISSING)
    assert c.close(timeout=2.0) == (2.0, MISSING)
    marker = object()
    assert c.close(option=marker) == (MISSING, marker)


def test_constructor_message_uses_the_class_name() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        _OffsetAndMetadata(offset=1, leader_epoch=3)
    assert str(exc.value) == (
        "_OffsetAndMetadata() takes one of (offset, leader_epoch, metadata), "
        "(offset, metadata), (offset); got (offset, leader_epoch)")


def test_java_given_defaults_are_filled_for_the_matched_overload() -> None:
    # (offset, metadata) matches (offset, leader_epoch, metadata), whose
    # leader_epoch Java-given default is Optional.empty(); the longest wins.
    o = _OffsetAndMetadata(offset=1, metadata="m")
    assert o.values == (1, None, "m")
    assert o.form == 0
    # (offset) matches (offset, metadata) with metadata="" filled; leader_epoch,
    # in no form (offset) reaches, stays MISSING, for the body to tell.
    o = _OffsetAndMetadata(offset=1)
    assert o.values == (1, MISSING, "")
    assert o.form == 1


def test_strict_matching_rejects_a_default_another_overload_receives() -> None:
    # Nothing passes `type` to acknowledge(topic, partition, offset, type).
    with pytest.raises(IllegalArgumentError) as exc:
        _Acknowledge().acknowledge(topic="t", partition=0, offset=1)
    assert str(exc.value) == (
        "acknowledge() takes one of (record), (record, type), "
        "(topic, partition, offset, type); got (topic, partition, offset)")


def test_a_given_value_equal_to_the_java_default_is_given() -> None:
    a = _Acknowledge()
    assert a.acknowledge(record="r") == ("r", MISSING, MISSING, MISSING, "ACCEPT")
    assert a.acknowledge(record="r", type="ACCEPT") == (
        "r", MISSING, MISSING, MISSING, "ACCEPT")
    assert a.acknowledge(topic="t", partition=0, offset=1, type="ACCEPT") == (
        MISSING, "t", 0, 1, "ACCEPT")


def test_an_explicit_none_is_javas_null_and_counts_as_given() -> None:
    # timeout=None is Java's close(null), not close().
    assert _Close().close(timeout=None) == (None, MISSING)
    assert _Close().close(option=None) == (MISSING, None)
    with pytest.raises(IllegalArgumentError) as exc:
        _Close().close(timeout=None, option=None)
    assert str(exc.value) == (
        "close() takes one of (), (timeout), (option); got (timeout, option)")


def test_missing_passed_explicitly_is_the_same_as_left_out() -> None:
    assert _Close().close(timeout=MISSING) == _Close().close() == (MISSING, MISSING)
    assert _Close().close(timeout=MISSING, option=MISSING) == (MISSING, MISSING)
    assert _Close().close(timeout=MISSING, option=7) == (MISSING, 7)
    assert _OffsetAndMetadata(offset=1, leader_epoch=MISSING).values == (1, MISSING, "")


def test_a_value_equal_to_a_filled_default_selects_the_longer_overload() -> None:
    class Node:
        @java_forms(Form("id"),
                    Form("id", "is_fenced", defaults={"is_fenced": False}),
                    Form("id", "rack"))
        def __init__(self, *, id: int, is_fenced: bool = MISSING,  # noqa: A002
                     rack: str | None = MISSING) -> None:
            self.fenced = is_fenced

    # Left out: Java's (id), filled with what it passes on.
    assert Node(id=1).fenced is False
    # is_fenced=False is given: Java's (id, is_fenced) with false.
    assert Node(id=1, is_fenced=False).fenced is False
    with pytest.raises(IllegalArgumentError) as exc:
        Node(id=1, is_fenced=False, rack="r")
    assert str(exc.value) == (
        "Node() takes one of (id), (id, is_fenced), (id, rack); got (id, is_fenced, rack)")


def test_missing_is_the_real_default_shown_by_signature_and_help() -> None:
    sig = inspect.signature(_Close.close)
    assert str(sig) == (
        "(self, *, timeout: 'float | None' = MISSING, option: 'object | None' = MISSING)"
        " -> 'tuple[Any, Any]'")
    assert sig.parameters["timeout"].default is MISSING
    assert sig.parameters["option"].default is MISSING
    assert "timeout: 'float | None' = MISSING" in str(sig)
    assert "= MISSING" in pydoc.render_doc(_Close.close, renderer=pydoc.plaintext)


def test_most_parameters_win_and_the_earliest_declared_on_a_tie() -> None:
    class Tie:
        @java_forms(Form("a", "b", defaults={"b": 1}),
                    Form("a", "c", defaults={"c": 2}),
                    Form("a"))
        def f(self, *, a: int, b: int | None = MISSING, c: int | None = MISSING,
              _java_form: int = -1) -> Any:
            return _java_form, b, c

    # (a) reaches both two-parameter forms; the first declared fills it.
    assert Tie().f(a=0) == (0, 1, MISSING)
    assert Tie().f(a=0, c=5) == (1, MISSING, 5)


def test_matching_is_exact() -> None:
    # Java's Node: (id, host, port) passes (null, false) to the longest, but
    # (id, host, port, is_fenced) is no overload, so it is rejected; nothing
    # is filled in to make a match.
    class Node:
        @java_forms(Form("id", "host", "port"),
                    Form("id", "host", "port", "rack"),
                    Form("id", "host", "port", "rack", "is_fenced",
                         defaults={"rack": None, "is_fenced": False}))
        def __init__(self, *, id: int, host: str, port: int,  # noqa: A002
                     rack: str | None = MISSING, is_fenced: bool = MISSING) -> None:
            self.values = (rack, is_fenced)

    with pytest.raises(IllegalArgumentError) as exc:
        Node(id=1, host="h", port=1, is_fenced=True)
    assert str(exc.value) == (
        "Node() takes one of (id, host, port), (id, host, port, rack), "
        "(id, host, port, rack, is_fenced); got (id, host, port, is_fenced)")
    # The matched (id, host, port) is filled with what it passes on.
    assert Node(id=1, host="h", port=1).values == (None, False)
    # rack=None is given (Java's null) and selects the longest.
    assert Node(id=1, host="h", port=1, rack=None, is_fenced=True).values == (None, True)


def test_a_serde_may_be_left_out_on_its_own() -> None:
    # KafkaProducer's (configs) and (configs, keySerializer, valueSerializer):
    # a serializer may be left out on its own, and is then None (Signatures:
    # the one exception to exact matching); a given None is Java's null.
    class Producer:
        @java_forms(Form("configs"),
                    Form("configs", "key_serializer", "value_serializer",
                         serdes=("key_serializer", "value_serializer")))
        def __init__(self, *, configs: dict[str, Any], key_serializer: object | None = MISSING,
                     value_serializer: object | None = MISSING) -> None:
            self.serdes = (key_serializer, value_serializer)

    serializer = object()
    assert Producer(configs={}).serdes == (None, None)
    assert Producer(configs={}, key_serializer=serializer).serdes == (serializer, None)
    assert Producer(configs={}, value_serializer=serializer).serdes == (None, serializer)
    assert Producer(configs={}, key_serializer=None, value_serializer=serializer).serdes == (
        None, serializer)
    with pytest.raises(TypeError, match="defaults, serdes or aliases name parameters"):
        Form("configs", serdes=("key_serializer",))


def test_positional_unknown_and_missing_arguments_are_type_errors() -> None:
    with pytest.raises(TypeError):
        _Close().close(1.0)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        _Close().close(nope=1)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        _OffsetAndMetadata()  # type: ignore[call-arg]


def test_the_form_keyword_is_private() -> None:
    with pytest.raises(TypeError):
        _OffsetAndMetadata(offset=1, _java_form=2)
    assert "_java_form" not in inspect.signature(_OffsetAndMetadata).parameters


def test_a_deprecated_overload_warns_when_called() -> None:
    class Records:
        @java_forms(Form("records", deprecated="Since 4.0. Use (records, next_offsets)."),
                    Form("records", "next_offsets"))
        def __init__(self, *, records: object,
                     next_offsets: object | None = MISSING) -> None:
            pass

    with pytest.warns(DeprecationWarning) as record:
        Records(records={})
    assert str(record[0].message) == (
        "Records(records) is deprecated. Since 4.0. Use (records, next_offsets).")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Records(records={}, next_offsets={})


def test_a_deprecated_overload_warns_at_the_callers_line() -> None:
    # The wrapper java_forms writes for the method warns from its own frame, so
    # the warning names the caller's line, as the generic wrapper's does.
    class Records:
        @java_forms(Form("records", deprecated="Since 4.0. Use (records, next_offsets)."),
                    Form("records", "next_offsets"))
        def __init__(self, *, records: object,
                     next_offsets: object | None = MISSING) -> None:
            pass

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame = inspect.currentframe()
        assert frame is not None
        Records(records={}); line = frame.f_lineno  # noqa: E702 (one line: the call's)
    assert [(w.filename, w.lineno) for w in caught] == [(__file__, line)]


def test_pythons_own_type_errors_are_word_for_word() -> None:
    # A call the generated wrapper does not match (an extra positional
    # argument, an unknown or a missing keyword) goes to the generic wrapper,
    # which leaves it to the method, so Python raises its own TypeError.
    with pytest.raises(TypeError) as extra:
        _Close().close(1.0)  # type: ignore[call-arg]
    assert str(extra.value) == "_Close.close() takes 1 positional argument but 2 were given"
    with pytest.raises(TypeError) as unknown:
        _Close().close(nope=1)  # type: ignore[call-arg]
    assert str(unknown.value) == "_Close.close() got an unexpected keyword argument 'nope'"
    with pytest.raises(TypeError) as missing:
        _OffsetAndMetadata()  # type: ignore[call-arg]
    assert str(missing.value) == (
        "_OffsetAndMetadata.__init__() missing 1 required keyword-only argument: 'offset'")
    # The receiver by keyword is the method's own business, as before.
    assert _Close.close(self=_Close()) == (MISSING, MISSING)  # type: ignore[call-arg]


def test_a_rejected_call_shows_the_generated_line_in_its_traceback() -> None:
    # The generated wrapper's source is registered with linecache, so a
    # traceback through it shows the line that raised.
    import traceback

    with pytest.raises(IllegalArgumentError) as err:
        _Close().close(timeout=1.0, option=object())
    frames = [f for f in traceback.extract_tb(err.value.__traceback__)
              if f.filename.startswith("<java_forms ")]
    assert [(f.filename, f.name) for f in frames] == [
        (f"<java_forms {_Close.close.__module__}._Close.close>", "close")]
    assert frames[0].line == "raise _jf_illegal(_jf_g)"


def test_a_coroutine_function_stays_a_coroutine_function() -> None:
    class Async:
        @java_forms(Form(), Form("timeout"), Form("option"))
        async def close(self, *, timeout: float | None = MISSING,
                        option: object | None = MISSING) -> float | None:
            return timeout

    assert inspect.iscoroutinefunction(Async.close)
    assert asyncio.run(Async().close(timeout=3.0)) == 3.0
    with pytest.raises(IllegalArgumentError):
        asyncio.run(Async().close(timeout=1.0, option=object()))


def test_the_check_is_built_when_the_class_is_defined() -> None:
    with pytest.raises(TypeError, match="'nope' is not a keyword-only parameter"):
        class Broken:
            @java_forms(Form("nope"))
            def f(self, *, a: int) -> None:
                pass
    assert _Close.close.__java_forms__[1].params == ("timeout",)  # type: ignore[attr-defined]


def test_missing_is_a_singleton_that_pickles_to_itself() -> None:
    assert repr(MISSING) == "MISSING"
    assert copy.copy(MISSING) is MISSING
    assert copy.deepcopy(MISSING) is MISSING
    assert pickle.loads(pickle.dumps(MISSING)) is MISSING
    assert type(MISSING)() is MISSING
    assert MISSING is not None
    assert not hasattr(MISSING, "__dict__")


def test_missing_is_not_exported_by_a_public_module() -> None:
    import importlib
    import pkgutil

    import confluent_kafka

    for info in pkgutil.walk_packages(confluent_kafka.__path__, "confluent_kafka."):
        if any(part.startswith("_") for part in info.name.split(".")[1:]):
            continue
        module = importlib.import_module(info.name)
        assert "MISSING" not in getattr(module, "__all__", ()), info.name


def test_illegal_argument_error_is_a_runtime_error() -> None:
    with pytest.raises(RuntimeError):
        _Close().close(timeout=1.0, option=object())


def _decorated_methods() -> list[Any]:
    """Every java_forms-decorated method of the package."""
    import importlib
    import pkgutil

    import confluent_kafka

    found: list[Any] = []
    for info in pkgutil.walk_packages(confluent_kafka.__path__, "confluent_kafka."):
        try:
            module = importlib.import_module(info.name)
        except Exception:  # noqa: BLE001 - a module that needs an optional extra
            continue
        for obj in list(vars(module).values()):
            if inspect.isclass(obj) and obj.__module__ == module.__name__:
                for member in vars(obj).values():
                    f = member.__func__ if isinstance(member, (staticmethod, classmethod)) else member
                    if callable(f) and hasattr(f, "__java_forms__"):
                        found.append(f)
    return found


def _recorder(f: Any) -> Any:
    """A function with the decorated method's signature that returns what it
    received, so the wrappers can be compared without running the method."""
    fn = f.__wrapped__
    sig = inspect.signature(fn)
    namespace: dict[str, Any] = {}
    parts: list[str] = []
    star = False
    for i, p in enumerate(sig.parameters.values()):
        if p.kind is p.KEYWORD_ONLY and not star:
            parts.append("*")
            star = True
        default = ""
        if p.default is not p.empty:
            namespace[f"_d{i}"] = p.default
            default = f"=_d{i}"
        parts.append(f"{p.name}{default}")
    is_async = inspect.iscoroutinefunction(fn)
    exec(f"{'async ' if is_async else ''}def {fn.__name__}({', '.join(parts)}):\n"  # noqa: S102
         f"    return dict(locals())\n", namespace)
    recorder = namespace[fn.__name__]
    recorder.__qualname__ = fn.__qualname__
    recorder.__module__ = fn.__module__
    return recorder


def _outcome(wrapper: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, ...]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            result = wrapper(*args, **kwargs)
            if inspect.iscoroutine(result):
                result = asyncio.run(result)
            got: Any = ("ok", tuple(sorted((k, repr(v)) for k, v in result.items())))
        except Exception as e:  # noqa: BLE001
            got = ("raised", type(e), str(e))
    return got, tuple((w.category, str(w.message), w.lineno) for w in caught)


def test_the_generated_wrapper_matches_the_generic_one_for_every_decorated_method() -> None:
    # The wrapper java_forms writes for a method, against the generic wrapper
    # built from the same overload table: for every keyword combination (each
    # keyword left out, given a value, None or MISSING; a fixed sample above six
    # keywords) and a few misuses, the method receives the same values, or the
    # same error with the same text is raised, with the same warnings.
    import itertools
    import random

    methods = _decorated_methods()
    assert len(methods) >= 20
    value = object()
    compared = 0
    for f in methods:
        decorated = java_forms(*f.__java_forms__)(_recorder(f))
        generic = decorated.__globals__.get("_jf_generic")
        if generic is None:
            continue  # java_forms keeps the generic wrapper for this method
        params = list(inspect.signature(f.__wrapped__).parameters.values())
        positional = tuple(object() for p in params
                           if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD))
        names = [p.name for p in params if p.kind is p.KEYWORD_ONLY and p.name != "_java_form"]
        states = [None, value, None, MISSING]  # left out, a value, None, MISSING
        if len(names) <= 6:
            combos = list(itertools.product(range(4), repeat=len(names)))
        else:  # drawn one by one: 4**12 combinations would not fit in memory
            rng = random.Random(f.__qualname__)
            combos = [tuple(rng.randrange(4) for _ in names) for _ in range(4096)]
        cases = [(positional, {n: states[s] for n, s in zip(names, c) if s}) for c in combos]
        full = {n: value for n in names}
        cases += [(positional[:-1], full), (positional + (value,), full),
                  (positional, {**full, "not_a_parameter": value})]
        if any(p.name == "_java_form" for p in params):
            cases.append((positional, {**full, "_java_form": 0}))
        for args, kwargs in cases:
            assert _outcome(decorated, args, kwargs) == _outcome(generic, args, kwargs), (
                f.__qualname__, sorted(kwargs))
            compared += 1
    assert compared > 10_000


# ---- rename aliases (CLAUDE.md, Stability; decision 9) ----------------------
# A test-only method: Java renamed `name` to `listener` (as Java did for
# Endpoint.listenerName). No alias is declared in the package until the release
# baseline gives the former names.

class _Endpoint:
    @java_forms(Form("listener", "host", "port", aliases={"listener": ("name",)}))
    def f(self, *, listener: str, host: str, port: int) -> tuple[str, str, int]:
        return listener, host, port


class _AliasedClose:
    @java_forms(Form(), Form("timeout", aliases={"timeout": ("duration",)}))
    def close(self, *, timeout: float | None = MISSING) -> Any:
        return timeout


def test_the_current_name_works_without_a_warning() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert _Endpoint().f(listener="L", host="h", port=1) == ("L", "h", 1)


def test_the_former_name_works_with_a_deprecation_warning_naming_both() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame = inspect.currentframe()
        assert frame is not None
        got = _Endpoint().f(name="L", host="h", port=1); line = frame.f_lineno  # type: ignore[call-arg]  # noqa: E501, E702
    assert got == ("L", "h", 1)
    assert [(w.category, str(w.message), w.filename, w.lineno) for w in caught] == [
        (DeprecationWarning, "f(): the keyword 'name' is deprecated; use 'listener'",
         __file__, line)]


def test_both_names_given_is_rejected_listing_the_alias_form() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        _Endpoint().f(name="L", listener="L", host="h", port=1)  # type: ignore[call-arg]
    assert str(exc.value) == (
        "f() takes one of (listener, host, port), (name, host, port); "
        "got (listener, name, host, port)")


def test_the_signature_shows_the_current_names_only() -> None:
    assert list(inspect.signature(_Endpoint.f).parameters) == [
        "self", "listener", "host", "port"]
    assert list(inspect.signature(_AliasedClose.close).parameters) == ["self", "timeout"]


def test_an_alias_counts_as_its_parameter_for_given_ness_and_fills() -> None:
    a = _AliasedClose()
    assert a.close() is MISSING
    with pytest.warns(DeprecationWarning, match="'duration' is deprecated; use 'timeout'"):
        assert a.close(duration=2.0) == 2.0  # type: ignore[call-arg]
    with pytest.warns(DeprecationWarning):
        assert a.close(duration=None) is None  # type: ignore[call-arg]
    # MISSING under the former name is left out, like under the current one.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert a.close(duration=MISSING) is MISSING  # type: ignore[call-arg]
    with pytest.raises(IllegalArgumentError) as exc:
        a.close(duration=1.0, timeout=1.0)  # type: ignore[call-arg]
    assert str(exc.value) == (
        "close() takes one of (), (timeout), (duration); got (timeout, duration)")


def test_an_alias_works_on_a_coroutine_function() -> None:
    class Async:
        @java_forms(Form(), Form("timeout", aliases={"timeout": ("duration",)}))
        async def close(self, *, timeout: float | None = MISSING) -> Any:
            return timeout

    with pytest.warns(DeprecationWarning):
        assert asyncio.run(Async().close(duration=3.0)) == 3.0  # type: ignore[call-arg]


def test_an_alias_must_be_a_former_name() -> None:
    with pytest.raises(TypeError, match="defaults, serdes or aliases name parameters"):
        Form("a", aliases={"b": ("c",)})
    with pytest.raises(TypeError, match="must be former names"):
        Form("a", "b", aliases={"a": ("b",)})


def test_no_alias_is_declared_in_the_package() -> None:
    # The former names come from the release baseline (decision 12, deferred).
    for f in _decorated_methods():
        assert all(not form.aliases for form in f.__java_forms__), f.__qualname__


def test_mypy_reports_the_deprecated_stub_of_a_former_name(tmp_path: Any) -> None:
    from mypy import api

    source = tmp_path / "aliased.py"
    source.write_text(
        "from __future__ import annotations\n"
        "from typing import Any, overload\n"
        "from typing_extensions import deprecated\n"
        "from confluent_kafka._args import MISSING, Form, java_forms\n"
        "class Endpoint:\n"
        "    @overload\n"
        "    def f(self, *, listener: str, host: str, port: int) -> None: ...\n"
        "    @overload\n"
        "    @deprecated(\"the keyword 'name' is deprecated; use 'listener'\")\n"
        "    def f(self, *, name: str, host: str, port: int) -> None: ...\n"
        "    @java_forms(Form('listener', 'host', 'port', aliases={'listener': ('name',)}))\n"
        "    def f(self, *, listener: str = MISSING, host: str, port: int, **_: Any) -> None:\n"
        "        pass\n"
        "Endpoint().f(listener='L', host='h', port=1)\n"
        "Endpoint().f(name='L', host='h', port=1)\n")
    out, _, status = api.run([str(source), "--strict", "--enable-error-code", "deprecated",
                              "--no-incremental", "--cache-dir", str(tmp_path / "cache")])
    assert status == 1, out
    lines = [line for line in out.splitlines() if ": error:" in line]
    assert len(lines) == 1, out
    assert "aliased.py:15:" in lines[0] and "deprecated" in lines[0], out
