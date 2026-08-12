---
name: review-expectations
description: How this user wants Critic reviews delivered — pre-verified facts excluded, findings ranked blocker-vs-follow-up, explicit verdicts on the Actor's self-declared deviations
metadata:
  type: feedback
---

When asked for a Critic review, this user hands over a scoped brief and expects the
review to conform to it rather than restart from scratch:

- **Respect the "already verified by me" list.** They pre-check mechanical facts
  (file modes, tracked-file counts, test pass/fail, profile values) and list
  already-filed issues. Re-reporting anything on that list is waste.
- **Rank every finding by consequence**, in their words: (a) corrupts the results,
  (b) crashes or wedges a long run, (c) merely untidy — and state explicitly which
  findings block the imminent deadline versus which are follow-ups.
- **Give a plain verdict on each deviation the Actor self-declared.** They ask
  "say plainly if any is wrong" and want a yes/no per item, not hedging. A table
  works well.
- **Assess *risk* for unexercised code paths**, not just existence — and say
  whether each needs a test before shipping.
- **Record what was verified as correct**, so the next round does not re-litigate
  settled ground.

**Why:** they are shipping against a hard date and are triaging, not collecting a
list. An unranked pile of findings with no blocker/follow-up split does not help
them decide what to fix tonight. They also state "avoid false positives" every
time — a wrong finding costs them more than a missed nit.

**How to apply:** structure the COMMENTS file as: verified-correct section →
blockers → fix-if-cheap → notes → deviation verdicts → rule/doc suggestions.
Prove claims by executing code or diffing sources where possible (e.g. extract the
Rust config match arms and diff them against the Python list) rather than reading
and asserting; they respond to demonstrated checks.
