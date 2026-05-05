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
from typing import Optional, Tuple


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

{{"plan_dependency": "<full-sha or null>", "implementation_dependency": "<full-sha or null>"}}

If the target's plan does not depend on any commit in the batch, set
plan_dependency to null. Same for implementation_dependency. There must
be at most one plan_dependency and at most one implementation_dependency
(the LATEST one if multiple would otherwise apply).
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


def parse_dep_eval_json(
    stdout: str,
) -> Optional[Tuple[Optional[str], Optional[str]]]:
    """Extract the dep-eval JSON answer from a (potentially noisy) stdout.

    Returns (plan_dependency, implementation_dependency) on success; None
    if no parseable JSON object with the right keys is found. Either
    dependency may be None inside the tuple (meaning "no dep").
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
        return plan, impl
    return None
