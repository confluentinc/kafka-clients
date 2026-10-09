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

"""The ``@overload`` stubs and the runtime accept the same sets of keywords.

CLAUDE.md, Python Binding Conventions, Signatures: "A stub admits a set of
given parameters only if that set is a Java overload", and ``java_forms``
accepts exactly the Java overloads' sets. So for every public class and method
with stubs (inline ``@overload``\\ s in a ``.py`` file, or a generated ``.pyi``
stub), every subset of the implementation signature's keyword-only parameters
is admitted by some stub iff the runtime accepts it. A method ``java_forms``
checks without stubs counts its own signature as its one stub, which then has
to admit only Java sets as well (it cannot: such a method needs stubs).

The runtime is asked without running the method's body: a stand-in with the
implementation's signature, decorated with the method's own Java forms
(``__java_forms__``) when it has them, is called with a value that counts as
given for each keyword. It refuses a set with ``IllegalArgumentError``
(``java_forms``), or when a required keyword-only parameter is missing
(Python's ``TypeError``); an unknown keyword cannot occur, the sets being drawn
from the signature. The constructors of abstract and non-instantiable bases are
skipped: they raise ``TypeError`` whatever is given. Stubs are read from the
source with ``ast``, so a signature spanning lines is read whole.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import itertools
import warnings
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

import confluent_kafka
from confluent_kafka import IllegalArgumentError
from confluent_kafka._args import java_forms

_PACKAGE = Path(confluent_kafka.__file__).parent

# A stub: its keyword-only parameter names and the required ones among them.
_Stub = tuple[frozenset[str], frozenset[str]]


def _module_name(path: Path) -> str:
    parts = path.relative_to(_PACKAGE.parent).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def _is_overload(decorator: ast.expr) -> bool:
    return (isinstance(decorator, ast.Name) and decorator.id == "overload") or (
        isinstance(decorator, ast.Attribute) and decorator.attr == "overload")


def _stubs() -> dict[tuple[str, str, str], list[_Stub]]:
    """(module, class, method) -> its stubs, for public classes only."""
    found: dict[tuple[str, str, str], list[_Stub]] = {}
    for path in sorted(_PACKAGE.rglob("*.py*")):
        if path.suffix not in (".py", ".pyi") or "__pycache__" in path.parts:
            continue
        module = _module_name(path)
        if any(part.startswith("_") for part in module.split(".")[1:]):
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        for cls in tree.body:
            if not isinstance(cls, ast.ClassDef) or cls.name.startswith("_"):
                continue
            for fn in cls.body:
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if not any(_is_overload(d) for d in fn.decorator_list):
                    continue
                if fn.args.posonlyargs or len(fn.args.args) > 1 or fn.args.vararg:
                    continue  # a stub taking positional arguments (fluent setters)
                names = frozenset(a.arg for a in fn.args.kwonlyargs)
                required = frozenset(a.arg for a, d in zip(fn.args.kwonlyargs, fn.args.kw_defaults)
                                     if d is None)
                found.setdefault((module, cls.name, fn.name), []).append((names, required))
    return found


_STUBS = _stubs()


def _keyword_only(signature: inspect.Signature) -> list[inspect.Parameter]:
    return [p for p in signature.parameters.values()
            if p.kind is p.KEYWORD_ONLY and not p.name.startswith("_")]


def _runtime_accepts(method: Callable[..., Any]) -> Callable[[frozenset[str]], bool]:
    """Whether the runtime accepts a set of given keywords: the method's Java
    forms matched over a stand-in with the method's signature."""
    signature = inspect.signature(method)
    keyword = _keyword_only(signature)
    required = {p.name for p in keyword if p.default is p.empty}
    positional = [p for p in signature.parameters.values()
                  if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    forms = getattr(method, "__java_forms__", None)
    check: Callable[..., Any] | None = None
    if forms is not None:
        def stand_in(*args: Any, **kwargs: Any) -> None:
            return None

        stand_in.__signature__ = signature  # type: ignore[attr-defined]
        check = java_forms(*forms)(stand_in)

    def accepts(given: frozenset[str]) -> bool:
        if not required <= given:
            return False  # Python's TypeError: a required keyword is missing
        if check is None:
            return True
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            try:
                # object() is never a parameter's default, so each counts as given.
                check(*[None] * len(positional), **{name: object() for name in given})
            except IllegalArgumentError:
                return False
        return True

    return accepts


def _decorated() -> set[tuple[str, str, str]]:
    """(module, class, method) for every public method checked by
    ``java_forms``, keyed by the file that defines the class, as the stubs are:
    one without stubs has its own signature as its one stub, which must then
    admit only Java sets too."""
    found: set[tuple[str, str, str]] = set()
    for path in sorted(_PACKAGE.rglob("*.py")):
        module_name = _module_name(path)
        if any(part.startswith("_") for part in module_name.split(".")[1:]):
            continue
        module = importlib.import_module(module_name)
        tree = ast.parse(path.read_text(), filename=str(path))
        for cls in tree.body:
            if not isinstance(cls, ast.ClassDef) or cls.name.startswith("_"):
                continue
            for method_name, member in vars(getattr(module, cls.name)).items():
                if isinstance(member, (staticmethod, classmethod)):
                    member = member.__func__
                if getattr(member, "__java_forms__", None) is not None:
                    found.add((module_name, cls.name, method_name))
    return found


def _cases() -> Iterator[tuple[str, Callable[..., Any], list[_Stub]]]:
    keys = sorted(set(_STUBS) | set(_decorated()))
    for module_name, class_name, method_name in keys:
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        method = inspect.getattr_static(cls, method_name)
        if isinstance(method, (staticmethod, classmethod)):
            method = method.__func__
        if method_name == "__init__":
            source = inspect.getsource(getattr(method, "__wrapped__", method))
            if "non-instantiable base" in source or "abstract catch-only base" in source:
                continue
        stubs = _STUBS.get((module_name, class_name, method_name))
        if stubs is None:
            keyword = _keyword_only(inspect.signature(method))
            stubs = [(frozenset(p.name for p in keyword),
                      frozenset(p.name for p in keyword if p.default is p.empty))]
        yield f"{module_name}.{class_name}.{method_name}", method, stubs


_CASES = list(_cases())


def test_stubs_are_found() -> None:
    # The inline stubs and the generated .pyi stubs are both read.
    ids = {case_id for case_id, _, _ in _CASES}
    for expected in ("confluent_kafka.producer.producer_record.ProducerRecord.__init__",
                     "confluent_kafka.common.node.Node.__init__",
                     "confluent_kafka.consumer.consumer.Consumer.commit_nowait",
                     "confluent_kafka.producer.mock_producer.MockProducer.__init__",
                     "confluent_kafka.common.errors.topic_authorization_error."
                     "TopicAuthorizationError.__init__"):
        assert expected in ids
    assert len(_STUBS["confluent_kafka.producer.producer_record", "ProducerRecord",
                      "__init__"]) == 22


@pytest.mark.parametrize("case_id, method, stubs", _CASES, ids=[c[0] for c in _CASES])
def test_some_stub_admits_a_set_iff_the_runtime_accepts_it(
        case_id: str, method: Callable[..., Any], stubs: list[_Stub]) -> None:
    names = [p.name for p in _keyword_only(inspect.signature(method))]
    accepts = _runtime_accepts(method)
    mismatches = []
    for n in range(len(names) + 1):
        for subset in itertools.combinations(names, n):
            given = frozenset(subset)
            admitted = any(given <= params and required <= given for params, required in stubs)
            if admitted != accepts(given):
                mismatches.append(
                    f"({', '.join(subset)}): stub {'admits' if admitted else 'refuses'}, "
                    f"runtime {'accepts' if not admitted else 'refuses'}")
    assert not mismatches, f"{case_id}: " + "; ".join(mismatches)
