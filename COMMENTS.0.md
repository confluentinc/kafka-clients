# Critic 0 — Milestone 9 Phase 0a (`common.metrics` foundation)

Both issues from the round-1 review are resolved; see `COMMENTS.DONE.0.md`.
The rule-update suggestion below is left for the coordinator to action.

## Rule-update suggestions (per agent-roles.md)
`COMMENTS.FP.md` / `COMMENTS.FN.md` do not exist at repo root yet. If Issue 1
recurs across translated `toString`/message code, consider adding to `CLAUDE.md`
a note that **`double`→string parity requires a Java-`Double.toString`-style
formatter** (Rust `{}` on `f64` drops the trailing `.0` for integral values and
uses different scientific-notation thresholds) whenever the rendered text is part
of a behavioral/message contract.
