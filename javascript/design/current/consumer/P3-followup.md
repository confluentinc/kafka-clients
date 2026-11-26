# P3 follow-up: enforceRebalance dropped, 152-2 recorded (N=152)

Status: **approved** by the user on 2026-11-26 ("do it", through the coordinator), with the fix-up shape of item 1.
Base `0f1d45fb9`, pushed. Done before P4, not folded into it.
The user's decisions of 2026-11-26: 152-2 accepted as a known difference ("Okay just record it"), 151-3 unchanged;
PLAN decision 9 reversed ("remove it entirely as python").

## Changes (Actor 152, reviewed by Critic 152, as in P3's loop)

1. **Code and tests**, as fix-ups of the commits that added them, so an autosquash leaves P3 without the method:
   - `git commit --fixup=amend:1513ab98c`, its new message without "and enforceRebalance": both forms in
     `consumer.ts` and `kafka_consumer.ts`, `native.ts`, `js_consumer_enforce_rebalance` in `consumer.c` with its
     `addon.c` and `util.h` lines, and the unit, busy, form-error, typing and memory tests;
   - a fix-up of `8f316efaf`: the two calls in `test/integration/consumer.test.js`;
   - a fix-up of `3ba87a4b8`: `NOTES.md`'s paragraph becomes the dropped note (item 4), `src/consumer/index.ts`
     lists the method as left out, and `api/consumer.api.md` loses its 6 lines (`make update-api`).
   One plain commit would also do, but it would leave both the addition and the removal in the history.
2. **The unused C entry points.** `header_conformance.test.js` moves `enforce_rebalance_cb` and `_with_reason_cb`
   from its must-be-called list to its never-called list, beside their blocking forms. That pins the drop, as
   Python's conversions table lists `Consumer_enforce_rebalance` under "nothing: no generated method calls it".
   No other list names them: `check-native-registry` follows the registrations, and no allowlist or fixture does.
3. **The linter.** `check-java-completeness` then reports `missing` for `Consumer#enforceRebalance` and
   `KafkaConsumer#enforceRebalance`. Each gets an `eslint-baseline.txt` line, kept for good as the FFI gaps' lines
   are, under a comment naming the user's decision. P4 does the same for the mock's `enforceRebalance`,
   `shouldRebalance` and `resetShouldRebalance`.
4. **The record**, in one plain commit:
   - PLAN.md's decision 9 row: reversed by the user on 2026-11-26. Its echoes go from §3's P3 and P4 rows, §4.3's
     Promise list, §5's P3 paragraph and decision 8's row.
   - P3.md: an "After the hand-off" note giving the drop's commits; the `reason` question is moot.
   - 152-2, accepted on 2026-11-26: in P3.md, in `NOTES.md`'s order section and in PLAN §6's ask 7.
   - `NOTES.md`'s dropped note: "*(deviation, as in Python)* `enforceRebalance()` and `enforceRebalance(reason)`
     aren't translated. Why: under KIP-848, the only protocol here, Java's `AsyncKafkaConsumer` only logs them as
     unsupported. Goes when: the client supports the classic protocol."

## Checks and gate

- Each commit passes `make test-javascript-unit lint-javascript`. The last also passes `make verify-javascript` and
  `make -C javascript test-memory-mutation`, and then the Critic reviews.
- The Manager's gate is P3's JavaScript steps: `make lint-javascript`, `make verify-javascript` and
  `make -C javascript test-memory-mutation`. Nothing outside `javascript/` changes (checked with
  `git diff --stat`), so the Rust, C and Python steps are skipped. Explicit paths, no AI trailer, hooks on, no push.

## For the user

`javascript/CLAUDE.md` has no rule for this drop, while Python's conventions have one (Class family). A one-line
draft for its Scope group, after the "Left out, as in Python" bullet: "- Dropped, as in Python (Class family):
`enforceRebalance()` and `enforceRebalance(reason)`, which Java's `AsyncKafkaConsumer` only logs as unsupported, and
the mock's `shouldRebalance()` and `resetShouldRebalance()`, which serve only them *(deviation. Why: under KIP-848,
the only protocol here, the method does nothing)*." P8 is unaffected: `consumer_service.proto` has no such RPC.
