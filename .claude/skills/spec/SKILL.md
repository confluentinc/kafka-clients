---
name: spec
description: Use when starting any new feature, fix, or change — before writing implementation code. Enforces a collaborative spec-writing process that produces a spec file, GitHub issue, and draft PR.
---

# /spec — Define a Change Before Implementing It

Every change starts here. Collaborate with the user to define what needs to change and why, write a spec, publish it as a GitHub issue with a draft PR, then stop. Implementation is a separate concern.

## Hard Rules

- Do NOT write implementation code. This skill produces only the spec file.
- Do NOT skip user approval. The user must explicitly approve the spec before publishing.
- Do NOT proceed past any phase gate without explicit user confirmation.
- Ask only one question at a time during collaboration.

## Phase 1 — Load Context

Before asking the user anything:

1. Re-read `CLAUDE.md` and all files in `.claude/rules/` to load the current project instructions.
2. If the user provided a description with `/spec`, use it as the starting point. Otherwise ask what they want to build or fix.

## Phase 2 — Collaborate on the Spec

1. Explore relevant code to understand the current state of what will change.
2. Ask clarifying questions **one at a time** until the change is well-defined. Prefer multiple-choice questions when possible.
3. Draft the spec using the template below.
4. Present the full spec to the user.
5. Iterate until the user **explicitly says they approve**.

### Spec Template

```markdown
# Spec: {Title}

**Issue:** #{issue-number}
**Date:** YYYY-MM-DD
**Status:** Draft

## Context
Why this change is needed. What problem it solves or what capability it adds.

## Requirements
Numbered list of specific, testable requirements.

## Approach
How the change should be implemented at a high level.
Which modules/files are affected and why.

## Affected Files
- `path/to/file.rs` — what changes and why

## Acceptance Criteria
- [ ] Criterion 1
- [ ] Criterion 2

## Verification
How to verify the change works end-to-end.
Specific commands to run, behaviors to observe.
```

Scale each section to the size of the change. A small fix needs 2-3 sentences per section.

## Phase 3 — Publish

Once the user approves the spec:

1. **Write the spec file** to `specs/spec-DRAFT-{slug}.md` where `{slug}` is a short kebab-case summary (e.g., `add-producer-batching`).
2. **Create a GitHub issue** in the current repo (detected from `git remote`):
   - Title: the spec title
   - Body: a summary of the spec + a link to the spec file path in the repo
3. **Rename the spec file** to `specs/spec-{issue-number}-{slug}.md` using the issue number from step 2.
4. **Create a branch** named `{issue-number}-{slug}`.
5. **Commit** the spec file to the branch and push.
6. **Open a draft PR** with the issue linked in the body.
7. **Update the GitHub issue** body to include a link to the draft PR.
8. Report what was created (spec file path, issue URL, PR URL) and **stop**.

## What This Skill Does NOT Do

- No implementation. The spec and draft PR are the deliverables.
- No labels, milestones, or assignees. The user adds those manually if needed.
- No modification of existing code. Only the spec file is created.
