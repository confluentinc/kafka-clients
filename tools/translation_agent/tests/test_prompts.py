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

from translation_agent import prompts


def test_dep_eval_template_substitutes():
    p = prompts.DEPENDENCY_EVAL_PROMPT_TEMPLATE.format(
        ak_commit="abc", ak_repo_path="/x", batch_listing="- d\n- e",
    )
    assert "abc" in p
    assert "/x" in p
    assert "- d" in p


def test_parse_clean_json():
    out = '{"plan_dependency": "abc", "implementation_dependency": "def"}'
    assert prompts.parse_dep_eval_json(out) == ("abc", "def")


def test_parse_with_log_noise_around_json():
    out = (
        "Loading commit info...\n"
        "Analyzing dependencies...\n"
        '{"plan_dependency": "abc", "implementation_dependency": null}\n'
        "Done.\n"
    )
    assert prompts.parse_dep_eval_json(out) == ("abc", None)


def test_parse_picks_last_json_when_multiple():
    out = (
        '{"plan_dependency": "first", "implementation_dependency": "first2"}\n'
        "Wait, recomputing...\n"
        '{"plan_dependency": "final", "implementation_dependency": "final2"}\n'
    )
    assert prompts.parse_dep_eval_json(out) == ("final", "final2")


def test_parse_both_null():
    out = '{"plan_dependency": null, "implementation_dependency": null}'
    assert prompts.parse_dep_eval_json(out) == (None, None)


def test_parse_no_json_returns_none():
    assert prompts.parse_dep_eval_json("nothing here") is None


def test_parse_malformed_json_returns_none():
    out = '{"plan_dependency": "abc" "implementation_dependency": "def"}'
    assert prompts.parse_dep_eval_json(out) is None


def test_parse_missing_one_key_skips():
    out = '{"plan_dependency": "abc"}\n{"plan_dependency": "x", "implementation_dependency": "y"}'
    # The first object is missing the impl key; the parser walks back from
    # the last match, which has both keys.
    assert prompts.parse_dep_eval_json(out) == ("x", "y")


def test_parse_non_string_dep_normalized_to_none():
    out = '{"plan_dependency": 42, "implementation_dependency": "ok"}'
    assert prompts.parse_dep_eval_json(out) == (None, "ok")
