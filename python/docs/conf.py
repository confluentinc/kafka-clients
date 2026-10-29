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

"""Sphinx configuration of the ``confluent_kafka`` API reference.

The layout follows confluent-kafka-python's API reference (an index grouped by
role, a section per class, members in declaration order, source links through
``viewcode``), in the Furo theme. autodoc imports the package from this source tree, with the C
extension ``_confluentkafka`` mocked, so the build needs neither the Rust
library nor the C extension; ``pyi_signatures`` (``_ext/``) takes the
signatures from the stubs a type checker reads, a module's ``.pyi`` or a
class's ``if TYPE_CHECKING:`` branch. Every public module, a package whose path
has no ``_``-prefixed part, gets one page under ``api/``, written here at each
build, with a section for each name in its ``__all__``; ``index.rst`` lists the
pages by role, and a page it does not list fails the build.
"""

from __future__ import annotations

import importlib
import inspect
import re
import sys
from pathlib import Path
from typing import Any

from sphinx.ext.autodoc.mock import mock

_DOCS = Path(__file__).resolve().parent
_ROOT = _DOCS.parent
_PACKAGE = "confluent_kafka"
_MOCKED = ["_confluentkafka"]

# This tree first: an installed distribution of the same import name (PyPI's
# librdkafka-based confluent-kafka) must not be the one documented.
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_DOCS / "_ext"))


def _version() -> str:
    text = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    found = re.search(r'^version = "([^"]+)"', text, re.MULTILINE)
    return found.group(1) if found else ""


def _public_modules() -> list[str]:
    modules = []
    for init in (_ROOT / _PACKAGE).rglob("__init__.py"):
        parts = init.parent.relative_to(_ROOT).parts
        if not any(part.startswith("_") for part in parts):
            modules.append(".".join(parts))
    return sorted(modules)


def _directive(obj: Any) -> str:
    if inspect.isclass(obj):
        return "autoexception" if issubclass(obj, BaseException) else "autoclass"
    if inspect.isroutine(obj):
        return "autofunction"
    return "autodata"


def _page(module: str) -> str:
    """The module's docstring, then a section per name in its ``__all__``."""
    exported = importlib.import_module(module)
    lines = [module, "=" * len(module), "", f".. automodule:: {module}", ""]
    for name in exported.__all__:
        directive = _directive(getattr(exported, name))
        lines += [name, "-" * len(name), "", f".. {directive}:: {module}.{name}"]
        if directive in ("autoclass", "autoexception"):
            # ``__call__``: the whole interface of a callback or serde protocol.
            lines += ["   :members:", "   :special-members: __call__"]
        lines.append("")
    return "\n".join(lines)


def _write_api_pages() -> None:
    api = _DOCS / "api"
    api.mkdir(exist_ok=True)
    with mock(_MOCKED):
        pages = {f"{module}.rst": _page(module) for module in _public_modules()}
    for stale in api.glob("*.rst"):
        if stale.name not in pages:
            stale.unlink()
    for name, text in pages.items():
        page = api / name
        if not page.exists() or page.read_text(encoding="utf-8") != text:
            page.write_text(text, encoding="utf-8")


_write_api_pages()

project = "confluent-kafka"
author = "Confluent Inc."
copyright = "Confluent Inc."
release = _version()

extensions = ["sphinx.ext.autodoc", "sphinx.ext.viewcode", "pyi_signatures"]
exclude_patterns = ["_build"]
html_theme = "furo"

autodoc_mock_imports = _MOCKED
autodoc_default_options = {
    "undoc-members": True,
    "show-inheritance": True,
    # Java's declaration order (Class family).
    "member-order": "bysource",
}

pyi_signatures_root = str(_ROOT)
pyi_signatures_package = _PACKAGE
