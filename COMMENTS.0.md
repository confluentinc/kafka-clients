# Critic 0 — Milestone 9 Phase 0a (`common.metrics` foundation)

Round-1 Issues 1, 2, and 3 are all RESOLVED and verified; see `COMMENTS.DONE.0.md`.
The rule-update suggestion below is left for the coordinator to action.

## Rule-update suggestions (per agent-roles.md)
`COMMENTS.FP.md` / `COMMENTS.FN.md` do not exist at repo root yet. If Issue 1 / Issue 3
recur across translated `toString`/message code, consider adding to `CLAUDE.md`
a note that **`double`→string parity requires a Java-`Double.toString`-style
formatter** — Rust `{}` on `f64` both drops the trailing `.0` for integral values
**and** never switches to scientific notation (Java does at `|x| >= 1e7` and non-integral
`|x| < 1e-3`) — whenever the rendered text is part of a behavioral/message contract.
