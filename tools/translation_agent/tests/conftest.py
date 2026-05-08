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
    they're actually testing. Default the helper AND every gh-side
    PR-state mutator to a no-op (returns None == success) so plan/
    impl/dep-eval tests don't have to think about PR-description
    or label internals -- and so the suite never invokes a real
    `gh` subprocess against an unauthenticated environment.

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
    ), patch(
        "translation_agent.cli.github.get_pr_body", return_value="",
    ), patch(
        "translation_agent.cli.github.add_pr_label",
    ), patch(
        "translation_agent.cli.github.remove_pr_label",
    ):
        yield


@pytest.fixture(autouse=True)
def _no_artifact_io_in_cli_tests(request, monkeypatch):
    """For test_cli.py: replace the four `locked_db` artifact primitives
    (push-no-force = lock acquire, yank = lock release, pull = DB pull,
    push = DB push) with silent no-ops so the suite can run in
    environments without the Semaphore `artifact` CLI installed.

    Tests that specifically want to verify artifact-IO behavior should
    re-patch the same target with their own mock -- pytest's
    `monkeypatch` here uses fixture finalization order, so a later
    `with patch(...)` inside a test overrides this autouse default.

    Scoped to `test_cli.py` only -- test_locked_db.py / test_semaphore.py
    exercise the artifact path directly and must NOT have it stubbed.
    """
    if request.path.name != "test_cli.py":
        yield
        return
    monkeypatch.setattr(
        "translation_agent.locked_db.semaphore.push_project_artifact_no_force",
        lambda name, file_path, destination=None: None,
    )
    monkeypatch.setattr(
        "translation_agent.locked_db.semaphore.push_project_artifact",
        lambda name, file_path: None,
    )
    monkeypatch.setattr(
        "translation_agent.locked_db.semaphore.yank_project_artifact",
        lambda name: None,
    )
    monkeypatch.setattr(
        "translation_agent.locked_db.semaphore.pull_project_artifact",
        lambda name, dest_dir: None,
    )
    yield


@pytest.fixture
def real_pr_description():
    """Opt-out marker for the autouse PR-description mock above.

    Add `real_pr_description` as a parameter to a test to disable the
    default no-op patches and exercise the real
    `_update_pr_description_via_r2` / `github.update_pr_body` /
    `github.prepend_pr_body` / `github.add_pr_label` /
    `github.remove_pr_label` / `github.get_pr_body` code paths. The
    fixture itself is a no-op at runtime; its only role is to be
    detectable in the autouse fixture's `request.fixturenames`.
    """
    yield
