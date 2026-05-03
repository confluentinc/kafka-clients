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

"""Shared pytest fixtures."""

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _no_pr_description_update_by_default(request):
    """Most CLI tests pin streaming.run_with_prefix call counts and
    side_effect lists; the description-update path adds an extra
    streaming call after each successful plan/impl r2 invocation,
    which would break those tests for reasons unrelated to what
    they're actually testing. Default the helper to a no-op (returns
    None == success) so plan/impl tests don't have to think about
    PR-description internals.

    Scoped to `test_cli.py` only -- other test files (test_github,
    test_prompts, etc.) exercise the underlying modules directly and
    must NOT have their `github.update_pr_body` / `github.prepend_pr_body`
    calls intercepted, because patching `translation_agent.cli.github.X`
    targets the same module object as `translation_agent.github.X`.

    Tests that DO want to exercise the real helper from cli context
    opt out by depending on the `real_pr_description` fixture; this
    autouse yields without patching when that opt-out is requested.
    """
    if request.path.name != "test_cli.py":
        yield
        return
    if "real_pr_description" in request.fixturenames:
        yield
        return
    with patch(
        "translation_agent.cli._update_pr_description_via_r2",
        return_value=None,
    ), patch(
        "translation_agent.cli.github.prepend_pr_body",
    ), patch(
        "translation_agent.cli.github.update_pr_body",
    ):
        yield


@pytest.fixture
def real_pr_description():
    """Opt-out marker for the autouse PR-description mock above.

    Add `real_pr_description` as a parameter to a test to disable the
    default no-op patches and exercise the real
    `_update_pr_description_via_r2` / `github.update_pr_body` /
    `github.prepend_pr_body` code paths. The fixture itself is a
    no-op at runtime; its only role is to be detectable in the
    autouse fixture's `request.fixturenames`.
    """
    yield
