# Multilanguage conversion: findings

The Rust-only broker integration tests in `rust/tests/integration/` are being
converted into multilanguage scenarios (`multilanguage_test!`,
`multilanguage_consumer_test!`, `multilanguage_admin_test!`), so each one also
runs against the Python (sync and asyncio) and C clients through the gRPC
backends. A new arm can show that a binding behaves differently from the Rust
client and from Java. This file lists every such difference. The bindings are
not changed by the conversion; a finding is recorded here and handled
separately.

A test-only cause (a timing margin, a wrong fixture) is fixed in the test and
is not listed.

Each entry gives the scenario, the arms that show the difference, what Java and
the Rust client do, what the arm does, the failing assertion's text, and the
state:

- **open**: the scenario still runs on every arm and fails on the arms named.
- **reverted to Rust-only**: the test went back to its Rust-only form, marked
  `// MULTILANGUAGE: blocked by finding #k`, so the suite stays green.
- **needs owner**: the conversion cannot continue without a decision.

## Findings

None so far.
