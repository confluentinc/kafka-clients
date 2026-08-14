---
name: skips-recorded-in-prose
description: Tests are parked in comments as well as #[ignore] — sweep the Rust tree for the blocker's words, not just the attribute
metadata:
  type: feedback
---

When closing a blocker, grep the **Rust test tree** for the words used to describe it,
not only for `#[ignore]`. Untranslated tests are frequently parked in a plain comment,
which no test runner and no attribute sweep will ever list.

**Why:** closing PLAN §9.1 was reported with "no `#[ignore]` cites this gap", which was
true and useless. Two Java tests (`MessageTest.testDefaultValues`,
`testNonIgnorableFieldWithDefaultNull`) were skipped in
`tests/common/message/message_test.rs` behind a prose comment naming the blocker almost
verbatim — "the version-gated UVE checks are a generator-level feature not yet
implemented". The classifier was `#[ignore]`; the population was "tests skipped for any
reason". That is the same shape as loop 50's recurring defect: a check narrower than the
claim it supports.

The same error then repeated in the write-up: a claim about how many Java tests assert a
behaviour was corrected once (one → two) and was still wrong (four), because both
revisions swept the **Java** corpus for assertions instead of the **Rust** tree for
skips. Those are different populations, and only the second one can tell you what this
repo has not translated.

**How to apply:**

  - Grep for the section number **and** the phrase, across `src/`, `tests/`, `examples/`:
    a blocker cited as "§9.1" in one file is cited as "not yet implemented in the
    generator" in another.
  - Do it as part of the fix, not afterwards — the skip note is usually *evidence about
    the fix's own coverage*. Here the two parked tests turned out to cover exactly the
    two predicate branches the change had rewritten and nothing else tested.
  - A stale skip note is a false statement about the code, so delete it in the same
    commit that falsifies it.

See also [[new_error_path_needs_its_catch]] and [[loop50_split_panic_notes]].
