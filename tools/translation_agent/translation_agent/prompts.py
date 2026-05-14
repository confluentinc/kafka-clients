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

"""Prompt templates and the dep-eval JSON parser.

Centralized so the wording can evolve (and be tested) independently of
the orchestration code that invokes them.
"""

import json
import re
from typing import NamedTuple, Optional


DEPENDENCY_EVAL_PROMPT_TEMPLATE = """\
You are a dependency analyst for the Confluent Kafka Rust translation project.

Given a target Apache Kafka commit, determine its dependencies WITHIN a
specific batch of related commits, for two purposes:

- plan_dependency: the LATEST commit in the batch whose translation plan
  must complete before this commit's plan can begin.
- implementation_dependency: the LATEST commit in the batch whose
  implementation must complete before this commit's implementation can
  begin.

Target AK commit: {ak_commit}
AK repo path:     {ak_repo_path}

Other AK commits in the current batch (any of these may be a dependency):
{batch_listing}

Output a single JSON object on stdout, with no other text:

{{"plan_dependency": "<full-sha or null>", "plan_dependency_reason": "<one-sentence explanation or null>", "implementation_dependency": "<full-sha or null>", "implementation_dependency_reason": "<one-sentence explanation or null>"}}

If the target's plan does not depend on any commit in the batch, set
plan_dependency to null. Same for implementation_dependency. There must
be at most one plan_dependency and at most one implementation_dependency
(the LATEST one if multiple would otherwise apply).

Reason fields:
- If a dependency is null, its corresponding reason MUST also be null.
- If a dependency is set, the reason should be a short single sentence
  explaining why this commit's plan / implementation depends on the
  named commit (e.g. "introduces the FetchRequest builder this commit
  consumes").
"""


PLAN_GENERATION_PROMPT_TEMPLATE = """\
You are the Manager agent for the Confluent Kafka Rust translation
project, following the workflow in `.claude/rules/agent-roles.md`.

Write a translation design document for the Apache Kafka commit listed
below. The document must be saved at the file path below using the
Write tool. After writing, commit it locally on the current branch.

File to write: ./design/history/{pr_number}_description/plan.md
Commit message: "Design document"
Current branch: `{branch_name}`

Inputs:
- AK commit:       {ak_commit}
- AK branch:       {ak_branch}
- PR number:       #{pr_number}
- Rust branch:     {branch_name}
- Rust repo root:  current working directory

IMPORTANT: do NOT enter plan mode. Use the Write tool directly to
create the file, then `git add` and `git commit -m "Design document"`.
Do NOT run `git push` -- it is denied in this sandbox; the orchestrator
pushes after you exit. Do not propose, do not ask for approval, do not
call ExitPlanMode. Just write and commit. Exit 0 on success.
"""


IMPLEMENTATION_PROMPT_TEMPLATE = """\
You are the Manager agent for the Confluent Kafka Rust translation
project, following the workflow in `.claude/rules/agent-roles.md`.

Execute the translation task described in the design document at:

    ./design/history/{pr_number}_description/plan.md

Spawn the Actor agent to implement the changes the document describes;
spawn the Critic agent to review them; iterate through the comment/fix
loop until no comments remain. Commit the changes locally on
`{branch_name}`.

Inputs:
- AK commit:       {ak_commit}
- AK branch:       {ak_branch}
- PR number:       #{pr_number}
- Rust branch:     {branch_name}
- Rust repo root:  current working directory

IMPORTANT: do NOT enter plan mode. Read the design document and
execute it directly. Do NOT run `git push` -- it is denied in this
sandbox; the orchestrator pushes after you exit. Exit 0 only when the
implementation is complete, all tests pass, and the commits are made
locally on `{branch_name}`.
"""


# Literal trailers the plan-phase PR-description prompt asks claude to
# pick exactly one of, depending on whether the plan calls for any
# Rust-file changes. The orchestrator scans the resulting body for an
# exact match: IMPLEMENTATION_NEEDED_MARKER applies
# LABEL_IMPLEMENTATION_NEEDED; NO_IMPLEMENTATION_NEEDED_MARKER skips
# the label (and proactively removes it, in case a previous body had
# it). Keep these constants and the strings baked into
# PR_DESCRIPTION_PROMPT_TEMPLATE in lockstep.
IMPLEMENTATION_NEEDED_MARKER = "Next steps: **Implementation needed**"
NO_IMPLEMENTATION_NEEDED_MARKER = "Next steps: **No implementation needed**"

# Per-state PR labels applied by the orchestrator at state transitions.
# Sequence (applied / removed at the corresponding sweep step):
#   status 0 -> 1 (dep-eval done):  +LABEL_DEPENDENCIES_EVALUATED
#   status 1 -> 2 (plan created):   -LABEL_DEPENDENCIES_EVALUATED, +LABEL_PLAN_CREATED
#   plan body has impl marker:      +LABEL_IMPLEMENTATION_NEEDED
#   plan body has no-op marker:     -LABEL_IMPLEMENTATION_NEEDED (defensive,
#                                     covers re-plan after a non-no-op body)
#   status 3 -> 4 (impl done):      -{deps-evaluated, plan-created,
#                                     implementation-needed}, +LABEL_IMPLEMENTATION_DONE
LABEL_DEPENDENCIES_EVALUATED = "dependencies-evaluated"
LABEL_PLAN_CREATED = "plan-created"
LABEL_IMPLEMENTATION_NEEDED = "implementation-needed"
LABEL_IMPLEMENTATION_DONE = "implementation-done"


PR_DESCRIPTION_PROMPT_TEMPLATE = """\
You are the Manager agent for the Confluent Kafka Rust translation
project, following the workflow in `.claude/rules/agent-roles.md`.

Generate a concise pull-request description for the work on this
branch (`{branch_name}`).

Phase: {phase}    # "plan" or "impl"

Inputs available in the cwd:
- Plan document: ./design/history/{pr_number}_description/plan.md
- Branch git log: `git log --no-merges origin/{base_branch}..HEAD`
- AK source upstream:
  - commit: {ak_commit}
  - link:   https://github.com/apache/kafka/commit/{ak_commit}

Write the PR description (markdown) to ./pr_body.md. Do NOT commit it.
Do NOT run `gh` -- the orchestrator updates the PR after you exit.
Do NOT run `git push`. Exit 0 when ./pr_body.md exists and is non-empty.

Body shape (~400 words max, reviewers shouldn't have to scroll):
- Open with one sentence summarizing the change.
- Cite the AK commit it translates (link form).
- For the "plan" phase: summarize the plan's scope, approach, and any
  notable risks. Close the body with EXACTLY ONE of the following two
  lines as the final paragraph (the orchestrator scans for it verbatim
  to decide whether to label the PR as awaiting implementation):

      Next steps: **Implementation needed**

  Use this when the plan calls for at least one Rust file to be
  created or modified. OR:

      Next steps: **No implementation needed**

  Use this when the plan concludes the change is a no-op for the Rust
  client (e.g. the Java change has no Rust counterpart, the behavior
  is already covered, or no Rust file needs to be created or
  modified). Include exactly one of these markers -- never both,
  never neither. Picking the no-op marker tells the orchestrator to
  skip applying the implementation-needed label so the PR stays in a
  human-review state.

- For the "impl" phase: summarize what was implemented, what tests
  cover it, and any follow-up TODOs. Do NOT include either of the
  "Next steps: **...**" lines in the impl-phase body -- the
  implementation is no longer needed at that point.
"""


# Shared instruction block embedded in every ASK_* prompt: claude must
# write its answer to ./ask_answer.md AND the file's first lines must be
# the user's command quoted as a markdown blockquote (one `> ` prefix
# per line of the original). The orchestrator relays the file as-is into
# a PR comment, so this is the ONLY guarantee the question shows up
# verbatim in the review thread.
_ASK_ANSWER_FILE_INSTRUCTION = """\
Write your answer to ./ask_answer.md. The FIRST lines of the file MUST
be the reviewer's command below quoted as a markdown blockquote -- prefix
EACH line of the original command with `> ` (a greater-than sign and a
space), preserving line breaks. Then a blank line, then your answer.

Reviewer's command (verbatim, between the BEGIN/END markers):
--- BEGIN REVIEWER COMMAND ---
{user_command}
--- END REVIEWER COMMAND ---
"""


# The PR description as last seen on GitHub, inlined so the agent can
# reason about the PR's stated scope, dep section, and summary without
# having to fetch it itself. Concatenated into every ASK_*_TEMPLATE.
_ASK_PR_DESCRIPTION_INSTRUCTION = """\
PR description (verbatim, between the BEGIN/END markers):
--- BEGIN PR DESCRIPTION ---
{pr_body}
--- END PR DESCRIPTION ---

"""


ASK_QUESTION_ONLY_PROMPT_TEMPLATE = """\
You are answering a reviewer's question about an in-flight translation
PR. The PR has been dependency-evaluated but no plan has been written
yet, so this invocation is QUESTION-ONLY: you must not edit any file
other than ./ask_answer.md, and you must not create any commits. The
orchestrator will discard any commits you make.

Context:
- AK commit:       {ak_commit}
- AK branch:       {ak_branch}
- PR number:       #{pr_number}
- Rust branch:     {branch_name}
- Rust repo root:  current working directory

""" + _ASK_PR_DESCRIPTION_INSTRUCTION + _ASK_ANSWER_FILE_INSTRUCTION + """\

IMPORTANT:
- Do NOT enter plan mode. Use the Write tool directly to create
  ./ask_answer.md, then exit.
- Do NOT run `git commit`, `git push`, or `gh pr edit` -- the sandbox
  denies the latter two and the orchestrator will discard any commits.
- Exit 0 once ./ask_answer.md exists and starts with the quoted command.
"""


ASK_PLAN_FIXUP_PROMPT_TEMPLATE = """\
You are responding to a reviewer's request about an in-flight
translation PR whose translation plan has already been written. You
may either (a) just answer in ./ask_answer.md, or (b) answer AND
adjust the plan with one or more git fixup commits.

Context:
- AK commit:       {ak_commit}
- AK branch:       {ak_branch}
- PR number:       #{pr_number}
- Rust branch:     {branch_name}
- Plan file:       {plan_path}
- Rust repo root:  current working directory

""" + _ASK_PR_DESCRIPTION_INSTRUCTION + _ASK_ANSWER_FILE_INSTRUCTION + """\

If the reviewer is asking for plan changes:
1. Edit {plan_path} to apply them.
2. Find the SHA of the commit that introduced the plan:
   `git log -1 --format=%H -- {plan_path}` -- that is the fixup target.
3. Create a fixup commit:
   `git add {plan_path} && git commit --fixup=<that-sha>` and ensure
   the commit message body starts with the same `> `-quoted reviewer
   command you put at the top of ./ask_answer.md (use `git commit
   --fixup=<sha> -m "fixup! <subject>" -m "<quoted command>"` or open
   the editor; either works).
4. You may make multiple fixup commits if the change spans logically
   distinct parts of the plan.

IMPORTANT:
- Do NOT enter plan mode. Edit and commit directly.
- Do NOT run `git push` or `gh pr edit` -- they are denied in this
  sandbox; the orchestrator pushes after you exit.
- Exit 0 once ./ask_answer.md exists and (if you made any) the fixup
  commits are on the branch tip.
"""


ASK_IMPL_OR_PLAN_FIXUP_PROMPT_TEMPLATE = """\
You are responding to a reviewer's request about a translation PR whose
implementation has already landed locally on this branch. You may
either (a) just answer in ./ask_answer.md, or (b) answer AND adjust
the plan or the implementation (or both) with git fixup commits.

Context:
- AK commit:       {ak_commit}
- AK branch:       {ak_branch}
- PR number:       #{pr_number}
- Rust branch:     {branch_name}
- Plan file:       {plan_path}
- Rust repo root:  current working directory

""" + _ASK_PR_DESCRIPTION_INSTRUCTION + _ASK_ANSWER_FILE_INSTRUCTION + """\

If the reviewer is asking for code or plan changes:
1. Make the edits.
2. For each change, identify the commit it logically belongs to via
   `git log` (look for the commit that introduced the file or the
   relevant block). That commit's SHA is the fixup target.
3. Create one fixup commit per logical change:
   `git add <files> && git commit --fixup=<that-sha>` and ensure each
   commit message body starts with the same `> `-quoted reviewer
   command you put at the top of ./ask_answer.md.
4. If the change touches Rust source, run `make verify` to confirm
   it still builds and tests pass before committing.

IMPORTANT:
- Do NOT enter plan mode. Edit, build/test, and commit directly.
- Do NOT run `git push` or `gh pr edit` -- they are denied in this
  sandbox; the orchestrator pushes after you exit.
- Exit 0 once ./ask_answer.md exists and (if you made any) the fixup
  commits are on the branch tip.
"""


DRY_RUN_NOTE = """\
NOTE: This is a dry run. The worktree and your commits will be
preserved on disk for inspection but the orchestrator will skip the
post-invocation push. The PR's status in its sqlite DB will not
advance. A subsequent real run will re-do this work.
"""


# Match a brace-block that contains both expected keys, lenient about
# whitespace/order. Anchored on the keys, not the braces, so we tolerate
# the inner Claude printing log lines around the JSON.
_JSON_OBJ_RE = re.compile(
    r'\{[^{}]*"(?:plan_dependency|implementation_dependency)"[^{}]*\}',
    re.DOTALL,
)


class DepEvalResult(NamedTuple):
    """Parsed dep-eval JSON: two SHA fields plus their per-dep reasons.

    Reasons are passed straight through to the PR description and not
    persisted in the DB. A reason field is always coupled to its dep:
    if the dep SHA is None, the parser forces the reason to None too.
    """
    plan_dependency: Optional[str]
    plan_dependency_reason: Optional[str]
    implementation_dependency: Optional[str]
    implementation_dependency_reason: Optional[str]


def _normalize_reason(value: object) -> Optional[str]:
    """Coerce a raw JSON reason value to a clean string or None.

    Lenient: missing keys, JSON null, non-string values, and
    whitespace-only strings all collapse to None so the renderer can
    skip the sub-bullet.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def parse_dep_eval_json(stdout: str) -> Optional[DepEvalResult]:
    """Extract the dep-eval JSON answer from a (potentially noisy) stdout.

    Returns a DepEvalResult on success; None if no parseable JSON object
    with the two required SHA keys is found. Reason fields are optional
    and tolerated when missing or malformed.
    """
    matches = list(_JSON_OBJ_RE.finditer(stdout))
    if not matches:
        return None
    # Try matches from latest to earliest -- tolerate "thinking aloud" earlier
    # in the output.
    for m in reversed(matches):
        try:
            obj = json.loads(m.group(0))
        except json.JSONDecodeError:
            continue
        if "plan_dependency" not in obj or "implementation_dependency" not in obj:
            continue
        plan = obj["plan_dependency"]
        impl = obj["implementation_dependency"]
        if plan is not None and not isinstance(plan, str):
            plan = None
        if impl is not None and not isinstance(impl, str):
            impl = None
        plan_reason = _normalize_reason(obj.get("plan_dependency_reason"))
        impl_reason = _normalize_reason(
            obj.get("implementation_dependency_reason")
        )
        # Coupling: a null dep can never carry a reason.
        if plan is None:
            plan_reason = None
        if impl is None:
            impl_reason = None
        return DepEvalResult(plan, plan_reason, impl, impl_reason)
    return None
