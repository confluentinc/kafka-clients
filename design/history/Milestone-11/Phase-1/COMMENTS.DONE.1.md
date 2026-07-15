# Critic 1 — Milestone 11 Phase 1 review — RESOLVED entries

These findings cover the early acknowledgement support types
(`Acknowledgements`, `ShareAcquireMode`) that landed alongside the Phase 1 wire
layer (commit `631f2bf`); both were resolved in fixup `38a3ecf`.

## RESOLVED (fixup 38a3ecf): `AcknowledgementsTest` (20 methods) not translated
Original finding: the tests module in `src/consumer/internals/acknowledgements.rs`
shipped ~10 weaker hand-written tests instead of the 20 Java `AcknowledgementsTest`
methods; the loose `batches.iter().any(...)` checks would not catch an off-by-one in
`maybeOptimiseAcknowledgeTypes`.

Resolution: replaced the weak tests with faithful translations of all 20 Java @Test
methods, asserting exact firstOffset/lastOffset/acknowledgeTypes per split batch,
`testCompleteSuccess` (`complete(null)` leaves exception null + completed true), every
gap/state permutation, and the repeated second `get_acknowledgement_batches()` call
(non-destructive check). Java `add(offset, null)` maps to `add_gap`. 20 tests pass.

## RESOLVED (fixup 38a3ecf): `ShareAcquireMode.Validator` and two tests omitted
Original finding: the nested `Validator` (ConfigDef.Validator) was dropped without
rationale, taking `testValidator`/`testValidatorToString` and the `of("")` case with it.

Resolution (deferral, option b): added an explicit `deferred:` note on
`ShareAcquireMode` documenting that the `Validator` belongs to the config-wiring phase
(Phase 6) — the project has no `ConfigDef::Validator` trait yet — and that an invalid
`share.acquire.mode` is still rejected by `ShareAcquireMode::of` at
`ShareFetchConfig::from_consumer_config` time. Replaced the two weaker `of` tests with
`test_from_string` mirroring `ShareAcquireModeTest.testFromString` including the `of("")`
empty-string rejection; `of(null)` has no Rust analogue (`&str` is non-null) and is
documented as omitted.

## Verdict

Phase 1 clean after fixup `38a3ecf`. No wire-encoding, session-handler, or
correctness findings on the request/response wrappers.
