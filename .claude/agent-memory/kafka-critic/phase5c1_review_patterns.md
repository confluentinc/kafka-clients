---
name: Phase 5c-1 review patterns
description: Patterns from Phase 5c-1 review (connection-state plumbing + Selectable/KafkaClient trait surfaces) — particularly hot-path identifier-interning consistency
type: project
---

Phase 5c-1 review (commit `c66c3b3`) — the connection-state plumbing
(`InFlightRequests`, `ClusterConnectionStates`, `ConnectionState`,
`LeastLoadedNode`) + the trait surfaces the Phase 5c-2 Selector and
Phase 5d NetworkClient will implement (`Selectable`, `KafkaClient`,
`MetadataUpdater`, `ManualMetadataUpdater`) + ApiVersions/NodeApiVersions.

**Why:** First sub-phase where multiple sibling classes had to be
translated together with a shared design rule (i32 connection ids
instead of String, per CLAUDE.md rule 11 + NOTES.md hot-path
interning). The rule was applied inconsistently — some classes use
i32, others use &str — and that's a high-yield axis to sweep.

**How to apply:** When a translation rule is invoked at the phase
level (commit message announces "all X are Y"), audit *every* method
on *every* class for compliance, including transitive dependencies.
Don't rely on the actor's claim — sample at least one method per
class that takes the relevant identifier.

## Recurring high-yield axes for connection/network code

1. **Hot-path identifier interning consistency.** When a phase
   announces "all connection ids are i32, not String", verify every
   method that takes a node id. Sibling classes (ApiVersions,
   MetadataUpdater) often slip through because they were translated
   in isolation by following Java's String literally. The Phase 5d
   call site will then need to stringify the i32, defeating the
   rule's purpose. Caller side is where the perf cost lands —
   reviewing the trait/struct in isolation can miss this.

2. **Java package-private (`final class` without `public`) → Rust
   `pub` is wrong.** CLAUDE.md rule 2 only mandates `pub(crate)` for
   `internal` packages, but Java's package-private semantics map
   naturally onto `pub(crate)` regardless. Sibling classes
   (`InFlightRequests` `pub(crate)` vs `ClusterConnectionStates`
   `pub`) reveal the inconsistency. Sweep all classes in the diff,
   not just the ones in `internal` packages.

3. **Test fixture defaults silently diverge from Java.** Java
   `addRequest(...)` passes `expectResponse = false`; Rust
   `make_request` defaulted to `true`. None of the current tests
   exercise the field, so the divergence is invisible — but it's a
   trap for future tests that copy the fixture. When reviewing test
   modules, eyeball every constructor argument against Java even
   when the test doesn't assert on it.

4. **`@ParameterizedTest` `@EnumSource` translated as Rust loop —
   verify no silent skip clauses.** Phase 5c-1 added
   `if !api_key.has_valid_version() { continue; }` inside the loop
   that wasn't in the Java original. Java relies on the empirical
   fact that `apisForListener(scope)` only contains valid keys at
   the current spec — adding a skip masks future spec changes that
   would loudly fail in Java.

5. **Dead-code variables hint at imperfect refactor of Java
   if/else-into-match.** `let _ = host_changed;` is the smell.
   When translating Java's
   ```java
   if (state != null && state.host().equals(host)) {
       /* update + return */
   } else if (state != null) {
       /* log */
   }
   /* unconditional create-new-state */
   ```
   into a Rust `match`, the actor sometimes adds a placeholder
   variable just to make the arms type-check. Code clarity issue,
   not a behaviour bug, but worth flagging — it usually means the
   match could be restructured to eliminate the placeholder.

## Process patterns that worked

- **Self-withdrawing findings** — when in doubt about a perf
  observation (e.g. "Rust does 3 lookups vs Java's 2"), re-read the
  Java carefully. Java often does the same triple-lookup via
  helper-method indirection. Better to record the finding with a
  "withdrawn" disposition than to delete it silently — leaves an
  audit trail of "I looked at this and verified it matched".

- **Trait surface freeze risk reminder.** Phase 5c-1 introduces
  three traits (`Selectable`, `KafkaClient`, `MetadataUpdater`) that
  Phase 5c-2 + 5d will implement. Cross-phase pattern from 5b: each
  sub-phase missed an accessor the next sub-phase needed. For 5c-1,
  every public method on Java's `Selectable` and `KafkaClient` is
  on the Rust trait — verified by reading the .java line-by-line.
  No missed accessors this round.

- **State-machine sampling.** For ClusterConnectionStates, sampled
  every public method's state transition logic against the Java
  source. Worth doing exhaustively — a single typo in the order of
  `state.state = X; clearAddresses();` (vs `clearAddresses(); state.state = X;`)
  could silently re-resolve DNS one transition too late.

## Round 2 follow-up (fixup `fdec4c6`)

When the actor's #3 fix justifies a fixture filter as "mirroring Java's
`filterApis(enable_unstable_last_version=false)`", verify the Java code
*actually* passes `false`. In this case Java passes `true` end-to-end
(`defaultApiVersionsResponse` → `filterApis(listenerType, true, true)`,
`latestVersion()` no-arg returns `highestSupportedVersion(true)`), so the
actor's "mirroring Java's pipeline" is approximate, not exact. The real
reason the filter is needed is a pre-existing Phase 2 translation choice
in `src/common/protocol/api_keys.rs` where Rust's `latest_version()`
calls `highest_supported_version(false)` (stable-only) while Java's
no-arg `latestVersion()` includes unstable. Not raised as a new finding
because (a) it's pre-existing, (b) the test still fails correctly on
real regressions for the keys it retains, and (c) shifting the Phase 2
semantics is well out of scope. But worth noting: when an actor's
disposition leans on a Java reference, spot-check the Java rather than
trusting the framing — this would have escalated to a comment if the
test had been silently masking a regression.
