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

"""Sphinx extension: signatures from the stubs.

autodoc reads a module's runtime code, where a parameter some Java overload
omits defaults to the private ``MISSING`` marker. The stubs a type checker
reads instead are a module's ``.pyi`` (the generated error, config, admin
Options and admin Result modules) or, in a hand-written module, the
``if TYPE_CHECKING:`` branch of a class (the keyword constructor of a class
Java builds with setters, a single stub that ``@overload`` cannot express). For
every class, method and function a stub declares, this extension shows the
stub's signatures, a ``.pyi`` shadowing its ``.py`` as it does for a type
checker; ``@overload`` stubs written in the ``.py`` autodoc shows itself.

It also drops the ``(*args, **kwargs)`` signature ``typing.Protocol`` gives a
protocol class, which users implement rather than construct, and warns on any
rendered signature that still shows ``MISSING`` or the mocked C extension, or
has no name, so a build with ``-W`` fails.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import re
import sys
from pathlib import Path
from typing import Any

from docutils import nodes
from sphinx import addnodes, version_info
from sphinx.application import Sphinx
from sphinx.ext.autodoc.mock import mock
from sphinx.util import logging

logger = logging.getLogger(__name__)

_LEAK = re.compile(r"\bMISSING\b|_confluentkafka|_MockObject")
_NAME = re.compile(r"[\w.]+")

_FunctionNode = ast.FunctionDef | ast.AsyncFunctionDef

# id(class) -> (class, constructor signatures); the class is kept so its id
# stays unique while the build runs.
_classes: dict[int, tuple[type, list[str]]] = {}
# (defining module, qualified name) -> signatures, for methods and functions.
_functions: dict[tuple[str, str], list[str]] = {}


def _decorated(node: _FunctionNode, name: str) -> bool:
    return any((isinstance(d, ast.Name) and d.id == name)
               or (isinstance(d, ast.Attribute) and d.attr == name)
               for d in node.decorator_list)


def _defs_by_name(body: list[ast.stmt]) -> dict[str, list[_FunctionNode]]:
    """The defs of one scope by name: the ``@overload`` stubs, else the last def."""
    found: dict[str, list[_FunctionNode]] = {}
    for node in body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            found.setdefault(node.name, []).append(node)
    return {name: [d for d in defs if _decorated(d, "overload")] or defs[-1:]
            for name, defs in found.items()}


def _param(arg: ast.arg, default: ast.expr | None, prefix: str = "") -> str:
    text = prefix + arg.arg
    if arg.annotation is not None:
        text += f": {ast.unparse(arg.annotation)}"
        if default is not None:
            text += f" = {ast.unparse(default)}"
    elif default is not None:
        text += f"={ast.unparse(default)}"
    return text


def _signature(node: _FunctionNode, *, drop_first: bool, with_return: bool) -> str:
    a = node.args
    positional = [*a.posonlyargs, *a.args]
    defaults: list[ast.expr | None] = [None] * (len(positional) - len(a.defaults))
    defaults += a.defaults
    n_posonly = len(a.posonlyargs)
    params = []
    for i, (arg, default) in enumerate(zip(positional, defaults)):
        if not (drop_first and i == 0):
            params.append(_param(arg, default))
        if i == n_posonly - 1 and len(params) > 0:
            params.append("/")
    if a.vararg is not None:
        params.append(_param(a.vararg, None, "*"))
    elif a.kwonlyargs:
        params.append("*")
    params += [_param(arg, default) for arg, default in zip(a.kwonlyargs, a.kw_defaults)]
    if a.kwarg is not None:
        params.append(_param(a.kwarg, None, "**"))
    text = f"({', '.join(params)})"
    if with_return and node.returns is not None:
        text += f" -> {ast.unparse(node.returns)}"
    return text


def _index_scope(cls: type | None, modname: str, body: list[ast.stmt], prefix: str) -> None:
    """The defs of one stub scope: a module's (``cls`` ``None``) or a class's."""
    by_name = _defs_by_name(body)
    for name, defs in by_name.items():
        drop_first = cls is not None and not _decorated(defs[0], "staticmethod")
        _functions[(modname, prefix + name)] = [
            _signature(d, drop_first=drop_first, with_return=True) for d in defs]
    if cls is not None and "__init__" in by_name:
        _classes[id(cls)] = (cls, [_signature(d, drop_first=True, with_return=False)
                                   for d in by_name["__init__"]])


def _index_pyi(owner: Any, modname: str, body: list[ast.stmt], prefix: str) -> None:
    """A ``.pyi``: every def and class it declares."""
    _index_scope(owner if isinstance(owner, type) else None, modname, body, prefix)
    for node in body:
        if isinstance(node, ast.ClassDef):
            cls = getattr(owner, node.name, None)
            if isinstance(cls, type):
                _index_pyi(cls, modname, node.body, f"{prefix}{node.name}.")


def _index_type_checking(owner: Any, modname: str, body: list[ast.stmt],
                         prefix: str) -> None:
    """A ``.py`` without a ``.pyi``: the ``if TYPE_CHECKING:`` branches of its classes."""
    for node in body:
        if not isinstance(node, ast.ClassDef):
            continue
        cls = getattr(owner, node.name, None)
        if not isinstance(cls, type):
            continue
        qual = f"{prefix}{node.name}."
        for stmt in node.body:
            if (isinstance(stmt, ast.If)
                    and ast.unparse(stmt.test) in ("TYPE_CHECKING", "typing.TYPE_CHECKING")):
                _index_scope(cls, modname, stmt.body, qual)
        _index_type_checking(cls, modname, node.body, qual)


def _load_stubs(app: Sphinx) -> None:
    root = Path(app.config.pyi_signatures_root)
    with mock(app.config.autodoc_mock_imports):
        for source in sorted(root.glob(f"{app.config.pyi_signatures_package}/**/*.py")):
            parts = source.relative_to(root).with_suffix("").parts
            if parts[-1] == "__init__":
                parts = parts[:-1]
            modname = ".".join(parts)
            stub = source.with_suffix(".pyi")
            if stub.exists():
                tree = ast.parse(stub.read_text(encoding="utf-8"), str(stub))
                _index_pyi(importlib.import_module(modname), modname, tree.body, "")
                continue
            text = source.read_text(encoding="utf-8")
            if "TYPE_CHECKING" in text:
                tree = ast.parse(text, str(source))
                _index_type_checking(importlib.import_module(modname), modname, tree.body, "")


def _constructor(cls: type) -> list[str] | None:
    """The stub signatures of the ``__init__`` ``cls`` runs, when a stub declares it."""
    for klass in cls.__mro__:
        entry = _classes.get(id(klass))
        if entry is not None and entry[0] is klass:
            return entry[1]
        if "__init__" in vars(klass):
            return None
    return None


def _joined(name: str, signatures: list[str]) -> str:
    """The signatures as the one value the event returns, one per line.

    Sphinx 8 splits the value into lines and writes the object's name before
    each; Sphinx 9 writes it before the first only, so the others carry it here:
    the full name less its module, as the directive header writes it.
    """
    if version_info < (9,):
        return "\n".join(signatures)
    parts = name.split(".")
    module = next(i for i in range(len(parts) - 1, 0, -1) if ".".join(parts[:i]) in sys.modules)
    dotted = ".".join(parts[module:])
    return "\n".join([signatures[0], *(dotted + s for s in signatures[1:])])


def _process_signature(app: Sphinx, what: str, name: str, obj: Any, options: Any,
                       signature: str | None, return_annotation: str | None
                       ) -> tuple[str, str | None] | None:
    if what in ("class", "exception") and isinstance(obj, type):
        init = _constructor(obj)
        if init is not None:
            return _joined(name, init), None
        if getattr(obj, "_is_protocol", False):
            return "", None
    elif what in ("method", "function"):
        func = inspect.unwrap(getattr(obj, "__func__", obj))
        key = (getattr(func, "__module__", ""), getattr(func, "__qualname__", ""))
        if key in _functions:
            return _joined(name, _functions[key]), None
    return None


def _check_signatures(app: Sphinx, doctree: nodes.document) -> None:
    # The rendered signatures: autodoc passes a class's implementation
    # signature to the hook above even when it then shows the ``@overload``
    # stubs of its ``.py`` instead.
    for node in doctree.findall(addnodes.desc_signature):
        leak = _LEAK.search(node.astext())
        if leak is not None:
            logger.warning("a signature shows %s: %s", leak.group(0), node.astext(),
                           location=node, type="pyi_signatures")
        # A stub line the directive could not parse: the Python domain then
        # makes its whole text the name.
        names = [n.astext() for n in node.findall(addnodes.desc_name)]
        if not names or _NAME.fullmatch(names[0]) is None:
            logger.warning("a signature has no name: %s", node.astext(),
                           location=node, type="pyi_signatures")


def setup(app: Sphinx) -> dict[str, Any]:
    app.add_config_value("pyi_signatures_root", None, "env")
    app.add_config_value("pyi_signatures_package", "confluent_kafka", "env")
    app.connect("builder-inited", _load_stubs)
    app.connect("autodoc-process-signature", _process_signature)
    app.connect("doctree-read", _check_signatures)
    return {"version": "1", "parallel_read_safe": True}
