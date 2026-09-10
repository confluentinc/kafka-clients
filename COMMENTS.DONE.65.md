# Critic 65 — resolved items

## Pass 3 — Note C addressed in `463f2921`, coverage extended in `4b73ca28` (2026-09-10)

`flush()`, `partitions_for()` (both `Producer` and `AsyncProducer`) and the
`_MockProducerMixin` helpers (`complete_next`, `error_next`, `history_count`,
`clear`) now call `_check_closed()` before handing `self.c_producer` to the FFI,
matching `send()`, `metrics()` and the five transaction ops. `flush()` keeps the
pass-2 post-wait re-check (different case: `close()` releasing a parked drain
waiter). Docstrings state this is a Python-level safety measure because the
binding's `close()` frees the native producer — Java's `flush()`/`partitionsFor()`
have no `throwIfProducerClosed()` and no fidelity claim is made.

Tests: each after-close test swaps the FFI entry point for a stub that raises
`AssertionError`, proving the guard fires before C is entered — a bare
`pytest.raises(RuntimeError)` would have no teeth for `flush()` because its
post-wait check raises the same error. Teeth verified: with `producer.py` stashed,
exactly the 5 new guard tests fail; restored, all pass. `4b73ca28` adds 13 more:
flush twins of T4–T7 (parked and released by close, cancelled, timed out — no
transaction involved), flush inside an open transaction (completes but does not
commit), after-close guards on `KafkaProducer` / `AsyncKafkaProducer` and the async
mock helpers, and a per-instance closed-flag check. Totals: 143 producer / 398 unit
tests green; timing-sensitive twins stable over repeated runs.

Judged minor and done directly without an Actor/Critic pass; the Critic-style
checks (sweep of every `self.c_producer` use, teeth check, full suite) were run by
hand. Not covered, by design: the check-then-act window between the closed check
and the C call when another thread closes concurrently (the same window Java has
for every op; no assertable outcome), `flush()` from a delivery callback during
`close()`, and `close()` failing midway.
