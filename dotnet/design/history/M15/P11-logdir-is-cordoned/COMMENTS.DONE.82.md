# COMMENTS.82 — Critic 82

Scope: commit `1101bea3` only ("fix(dotnet/admin): render ToString() booleans
lowercase, as Java does"). Reference: Kafka Java public API
(`kafka/clients/src/main/java/org/apache/kafka/clients/admin/*.java`) + the C ABI
header. Not a re-review of M15/P11.

## Verdict

**Clean pass on behaviour.** All five rendering fixes match their Java
counterparts, the sweep's "5 rendered bools across 4 types" claim holds against an
independent sweep, both deliberate near-misses were correctly left alone, and the
new/changed assertions pin literal text rather than re-deriving it from the
implementation. One non-blocking citation defect below.

---

### 1. [INFO — does NOT block] Two of the four Java line citations point past EOF in this repo's `kafka/` checkout

**Files:**
- commit message of `1101bea3`
- `bindings/dotnet/tests/Confluent.Kafka.UnitTests/PublicAdminConfigsShapeParityTests.cs:308`
- `bindings/dotnet/tests/Confluent.Kafka.UnitTests/PublicAdminP2aShapeParityTests.cs:240`

**Reference:** `kafka/clients/src/main/java/org/apache/kafka/clients/admin/`

The commit and two of the three new test comments cite:

- `TopicDescription.java:150` — the file is **141 lines**; `toString()` is at
  **136–139**.
- `ConfigEntry.java:305-313` — the file is **294 lines**; `ConfigEntry.toString()`
  is at **183–194** (`:305-313` would land inside the nested `ConfigSynonym`, which
  also ends at 293).

The other two are correct: `ReplicaInfo.java:64-70` and `TopicListing.java:67`
match exactly.

Behaviourally this is nothing — the *renderings* were verified against the real
Java bodies and are right (see below). But a citation past EOF is not checkable by
the next reader, and two of the three new comments were added specifically so the
lowercase requirement would be verifiable without re-deriving it. Worth correcting
the four numbers (commit message is immutable; the two comments are not).

---

## What was verified clean

**(1) The five fixes vs Java.** Each C# format string and argument order matches
its Java `toString()` body verbatim, with the boolean now lowercase:

| Site | Java body | Match |
|---|---|---|
| `ReplicaInfo.cs:75-79` | `ReplicaInfo.java:64-70` — `"ReplicaInfo(" + "size=" + size + ", offsetLag=" + offsetLag + ", isFuture=" + isFuture + ')'` | ✅ text, order, lowercase |
| `TopicListing.cs:66-70` | `TopicListing.java:67` — `"(name=" + name + ", topicId=" + topicId + ", internal=" + internal + ")"` | ✅ |
| `TopicDescription.cs:172-177` | `TopicDescription.java:136-139` — `"(name=" + name + ", internal=" + internal + ", partitions=" + … + ", authorizedOperations=" + … + ")"` | ✅ |
| `ConfigEntry.cs:287-299` (×2) | `ConfigEntry.java:183-194` — `name`, `value` (redacted), `source`, `isSensitive`, `isReadOnly`, `synonyms`, `type`, `documentation` | ✅ both booleans, field order preserved |

**(2) The sweep claim holds — no 6th outlier.** Independently re-derived rather
than taken on trust: enumerated every `bool` / `bool?` member under
`src/Confluent.Kafka` (all accessibilities), intersected with every
`public override string ToString` (61 files), and read the body of each
bool-bearing type. Ten rendered-bool sites exist in total; the five pre-existing
ones already use the ternary (`ClientQuotaFilter.Strict`,
`MemberDescription.Upgraded`, `LogDirDescription.IsCordoned`,
`ConsumerGroupDescription.IsSimpleConsumerGroup`,
`ConsumerGroupListing.IsSimpleConsumerGroup`) and the five in this commit are the
remainder. The bool members that are **not** rendered were each checked against
their type's `ToString` body and confirmed absent from it
(`KafkaPrincipal.TokenAuthenticated`, `ClassicGroupDescription` /
`GroupListing.IsSimpleConsumerGroup`, `ConfigEntry.IsDefault`, every `IsUnknown`,
the `*Options` flags, `TopicMetadataAndConfig.HasMetadata`,
`RemoveMembersFromConsumerGroup*.RemoveAll`). Also confirmed there is **no**
`record` type in `src/` (a compiler-generated `ToString` would render `True`/`False`
and escape a hand-written-`ToString` sweep entirely), no non-override `ToString`,
and no interpolated-string `ToString` rendering a bool.

**(3) Both near-misses correctly left alone.**
- `MemberDescription.Upgraded` (`MemberDescription.cs:268`) already reads
  `Upgraded is null ? "null" : (Upgraded.Value ? "true" : "false")`, which is the
  exact rendering of Java's `", upgraded=" + upgraded.orElse(null)`
  (`MemberDescription.java:248`) — `Optional<Boolean>.orElse(null)` concatenates as
  `true` / `false` / `null`. Nothing to change.
- `ConfigEntry`'s `IsSensitive ? "Redacted" : Value` is Java's
  `", value=" + (isSensitive ? "Redacted" : value)` (`ConfigEntry.java:186`) — a
  string-valued ternary, not a rendered bool. Correctly out of scope of this fix.

**(4) Assertions pin literal text, both polarities.** Every new/changed assertion
uses an `Ordinal` literal for the boolean substring, and each of the four types now
has both `true` and `false` covered:
- `PublicAdminLogDirsTests.cs:287-288` — literal `isFuture=true` / `isFuture=false`;
  `:308` literal nested `isFuture=false` inside the `LogDirDescription` rendering.
- `PublicAdminP2aShapeParityTests.cs:243-247` — `StartsWith("(name=t, internal=false, ")`
  / `("(name=t, internal=true, ")`, against `bare` (ctor `isInternal: false`) and a
  fresh `isInternal: true` instance.
- `PublicAdminP2bShapeParityTests.cs:161-164` — literal `internal=true` / `internal=false`.
- `PublicAdminConfigsShapeParityTests.cs:306-320` — `Contains("isSensitive=false, isReadOnly=false")`
  against `new ConfigEntry("k","v")` (whose 2-arg ctor delegates with
  `isSensitive: false, isReadOnly: false`), and `Contains("isSensitive=true, isReadOnly=true")`
  against the 8-arg `internal` ctor with both flags set.

No assertion interpolates the boolean property or rebuilds the expected substring
through the implementation. `PublicAdminP2bShapeParityTests.cs:161/163` does splice
`listing.TopicId` into the expected string, but that is (a) pre-existing on the
changed line, and (b) not the property under test — the boolean is a literal. Not a
finding.

Also confirmed no stale capitalized assertion survives anywhere in `tests/`: a
sweep for `internal=True` / `isFuture=False` / `isSensitive=True` / `upgraded=True`
/ `strict=True` etc. returns only lowercase hits.
