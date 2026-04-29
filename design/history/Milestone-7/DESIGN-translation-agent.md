# Milestone 7: Create a translation agent for Apache Kafka commits

In this milestone an agent must be implemented that looks at new commits in
Apache Kafka and creates PR in this repository with corresponding translations
to Rust code.

The workflow should be a Python application that does these things, using a sqlite db for storing the tables.

0. the Semaphore CI pipeline loads the sqlite db from project artifacts.
1. the agent runs with a given AK branch and Rust branch it has a table `branch_commit` with
the AK branch and commit hash, Rust client branch and commit hash
2. it runs with a Rust branch. It takes the latest commit and gets the corresponding commit
   in the corresponding AK branch looking in `branch_commit` table.
3. it get the next 10 commits on AK branch and for each commit it creates a branch and a PR, starting from the initial AK branch.
   It inserts into a table `pr_commit`. This table has the PR number, the Rust branch, the corresponding AK commit
   and two optional columns `plan_dependency` and `implementation_dependency` that contain commits (hashes) that
   are a precondition before planning this commit translation or before starting the implementation.
   The table also contain a status enum:
   - 0: no plan
   - 1: dependencies evaluated
   - 2: plan created
   - 3: plan approved
   - 4: implementation done
4. for each PR that has status (0: no plan) it starts Claude Code with r2 command, like:
   `r2 sandbox claude -p "Claude Code prompt"`, to identify the dependencies
   of that commit for planning or for implementing. It outputs the dependencies in a JSON file.
   There should be only a single `plan_dependency` and a single `implementation_dependency`:
   the latest commit that is a dependency.
5. the application reads the dependencies and updates the `pr_commit` table with those
   and sets the status to (1: dependencies evaluated)
6. for each PR that has status (1: dependencies evaluated) and has no `plan_dependency`
   or the plan dependency is not among those in the table (open ones) or present but with
   status >= (3: plan approved), it runs claude with `r2` and asks the manager
   agent to create a plan and to save it to `./design/history/<pr_number>_description/plan.md`.
   The Claude Code runs with `r2` should be in parallel, and the output should
   be flushed every 100 lines and written to stdout preceded with
   ">>>>> From agent #<pr_number>".
   Each agent commits the plan and pushes it to the branch corresponding to the AK commit.
   The commit message should be "Design document". It updates the status for that PRs to
   (2: plan created). 
7. when run with `--pr <number>` and `--plan-approve` it changes the status of the corresponding
   PR from (2: plan created) to (3: plan approved) and continues with (8).
   When run with `--pr <number>` only it just checks the status of that PR.
   `--plan-approve` happens when the Semaphore CI PR pipeline is running and a manual promotion is triggered.
8. for each PR that has status (3: plan approved) and has no `implementation_dependency`
   or the implementation dependency is not among those in the table (open ones),
   it runs claude with `r2` and asks the manager
   agent to start the implementation of the plan at `./design/history/<pr_number>_description/plan.md`.
   Running the actor and critic loop and the final handoff.
   The Claude Code agents with `r2` should be in parallel, and the output should
   be flushed every 100 lines and written to stdout preceded with
   ">>>>> From agent #<pr_number>".
   It pushes the generated commits to the branch corresponding to the AK commit.
   It updates the status for that PR to (4: implementation done).
9. last two steps can be done in parallel.
10. finally after all agents complete successfully with a semaphore command it saves the sqlite database as a project artifact.
