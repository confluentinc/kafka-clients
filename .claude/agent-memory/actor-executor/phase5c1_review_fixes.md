---
name: Phase 5c-1 review fix patterns
description: Reusable patterns from Phase 5c-1 review (Java fixture parity, hot-path i32 keys, dead-code allow on pub(crate) future-callee types)
type: feedback
---

Phase 5c-1 review (commit `c66c3b3`) surfaced four reusable patterns:

**Java test-fixture parity over a "skip" guard.** When a translated test
diverges from Java by skipping cases the Java fixture doesn't see (e.g.
`if !api_key.has_valid_version() { continue; }`), the right fix is
usually NOT to drop the guard. The Rust fixture itself often differs
from Java's — Java's `defaultApiVersionsResponse` runs through
`filterApis` / `toApiVersionForApiResponse`, which filters unstable
APIs upstream, so its loop never sees them. Mirror that filter in the
*fixture* construction so the loop assertion can be unconditional. If
you just drop the guard without fixing the fixture, the test fails on
unstable APIs (e.g. `StreamsGroupHeartbeat` with `enable_unstable=false`
returning max=-1).

Why: faithful translation = same behaviour AND same shape, not just
same shape. The "if it ever fails on a spec change, that's the right
signal" hint is true once the fixture is also Java-faithful.

How to apply: when a Critic flags a test with a skip-guard not present
in Java, audit the test FIXTURE first (does Java's pipeline pre-filter
the data?), then remove the guard and rebuild the fixture to mirror
Java's filtering pipeline.

**Hot-path identifier interning end-to-end.** When CLAUDE.md rule 11 +
NOTES.md "Hot-path identifier interning" mandate `i32` connection ids
across a network-client surface, every sibling class must agree —
`InFlightRequests`, `ClusterConnectionStates`, `Selectable`,
`KafkaClient`, AND `ApiVersions` (`update`/`remove`/`get`) AND
`MetadataUpdater::handle_server_disconnect` (with its
`ManualMetadataUpdater` impl). One sibling on `&str` forces a
`format!()` allocation per call at the i32-keyed call site, defeating
the whole optimisation. Audit ALL sibling classes when applying the
rule, not just the obviously-hot ones.

How to apply: each translation phase that introduces sibling classes
in a network-client surface should pick a single key type up-front and
apply it consistently across all classes — diverging key types between
sibling APIs is a red flag.

**`pub(crate)` future-callee + `#[allow(dead_code)]` lint annotation.**
When a Java package-private class lands ahead of its first non-test
caller (e.g. `ClusterConnectionStates` in 5c-1, before NetworkClient
in 5d), `pub(crate)` alone trips `deny(warnings)` because nothing
outside `cfg(test)` references it. Add
`#[allow(dead_code)] // Phase Xy <ClassName> is the first non-test caller`
on the struct, every `impl` block, AND each module-level constant.
Drop any premature `pub use foo::Foo` re-export from `lib.rs` until a
real caller exists — sister `InFlightRequests` is the precedent.

How to apply: when filing a `pub(crate)` future-callee class, the
annotation always goes on (a) the struct, (b) every `impl` block,
(c) each `pub(crate) const`, (d) any private inner struct + its `impl`
block. Forgetting any of these surfaces as `deny(warnings)` build
errors that look unrelated.

**Dead `let _ = x;` is a refactor smell.** When a Critic spots a
`let _ = host_changed;` style discard, the structural fix is usually
to invert the early-return shape: `if let Some(state) = ... { if
condition { early_return } else { log/side-effect } } /* fall through
*/ create_new_state()`. The original `match` returning a `bool` only
to discard it is a Java-to-Rust translation artefact (Java uses
imperative early returns; Rust's `match` wants every arm to yield a
value of the same type, even when the value is meaningless). Match
the Java structure, not the Rust idiom.
