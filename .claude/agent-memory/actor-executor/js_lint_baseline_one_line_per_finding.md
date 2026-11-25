---
name: js-lint-baseline-one-line-per-finding
description: javascript eslint-baseline.txt needs one line per finding; a key that waives two findings fails test/lint/runner.test.js; probes that run only make lint-rules miss it
metadata:
  type: feedback
---

In `javascript/`, `make lint-rules` accepts a baseline key that waives several findings, but
`test/lint/runner.test.js` ("the real tree without baselines") compares the sorted list of
findings with the sorted baseline lines, so each finding needs its own line, and the baseline
refuses duplicate lines ("listed twice"). `check-type-mapping` reports one finding per
mismatched part (the return and each option), so a member with two mismatches cannot be
baselined at all.

**Why:** found in the JS consumer P1 (2026-11-25): the plan's three
`type-mismatch ConsumerRebalanceListener.<method>` lines cover six findings; the Manager's probe
had run only `make lint-rules` and two rules' tests, not the lint project's runner test.

**How to apply:** when a plan adds baseline lines, count the findings per key with
`KAFKA_LINT_NO_BASELINE=1 npx eslint . -f json` before committing, and run the whole
`jest --selectProjects lint` (it ignores path filters and takes about 10 minutes). Pinned
linter lists also have their own tests (`test/lint/data_objects.test.js` pins `DATA_CLASSES`).
A scratch copy of `javascript/` (node_modules, native/build and ../rust, ../kafka symlinked)
runs lint-rules, unit, typing and typecheck-tests in parallel with the real tree's checks.
