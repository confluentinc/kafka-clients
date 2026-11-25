---
name: js-consumer-plan-linter-probe
description: Before a JavaScript consumer phase plan (P2–P8) goes to the user, probe the rule linter over the phase's planned TypeScript (and a C skeleton for native phases); P1's plan missed 79 findings
metadata:
  type: project
---

Before asking the user to approve a JavaScript consumer phase plan, have a helper run the rule linter
(`make lint-rules`, both commands) over a scratch copy that holds the phase's planned TypeScript shapes, and put every
finding, with its fixes, into the phase plan.

**Why:** P1's plan (2026-11-25) counted only `check-overload-shape`. A probe found 79 findings under 73 baseline keys
from `check-type-mapping`, `check-java-completeness` and `check-public-audience`: an interface with no implementing
class fails every Promise return; a callback method's `unknown` return and a `TopicPartitionSet` handed to it;
`[Symbol.iterator]()` not counted as Java's `iterator()`; `CloseOptions`' keyed constructor. P1 stopped before any
Actor ran, with four more decisions for the user.

**How to apply:** scratch copy with `git archive HEAD javascript`, `node_modules` symlinked from the main checkout,
`kafka/` and `rust/` symlinked beside it, and `TMPDIR` inside the scratch dir (the lint tests write fixtures to
`os.tmpdir()`). Land an interface in the phase that adds its first implementing class, as the producer did.

Run the whole lint test project too (`make -C javascript test-lint`), not just `make lint-rules`: it holds checks
`lint-rules` doesn't run, such as `test/lint/runner.test.js` ("the real tree without baselines"). P1's confirmation
run (2026-11-25) checked only `lint-rules` and two rule tests, so it missed that this test then wanted one baseline
line per finding, which the listener's two findings under one key broke. The user's choice 9(a) (`8f072137b`) made
it one line per key, as the baseline itself works.

A phase with native code needs a C skeleton in the probe too: `check-waiting-call-promise` follows each
`native.<fn>` through `native/addon.c` into `native/*.c` to a `_cb` twin, so without one every Promise method is
`unresolved-native`. Write it after `producer.c`'s patterns (`cbq_call_begin` before each `_cb` call, owned handles
destroyed), so the C rules' findings name allowlists, not the skeleton. P2's probe (2026-11-25) also showed that an
allowlist entry can pass `make lint-rules` and still fail `make test-lint`: a `TEST_ONLY` entry in
`native_registry.js` needs its twin in `test/lint/_native_fixture.js`, as the admin branch's `cd564288c` did. A
probe file's comments must not name a phase (`check-no-tracking-ids`). Each make target takes about 3 and 11
minutes on this machine.
