### 125-S1b-1 (Low) The companion-field rule clears a Java *instance* field of an enum, a form E13 rules out

- Commit: `499d105c`
- File: `dotnet/analyzers/Confluent.Kafka.Analyzers/JavaNameAnalyzer.cs:434-438`
- What is wrong: `onCompanion` accepts a `public static readonly` field of `<Enum>Methods` for **any** Java enum
  field (`field.Kind == JavaMemberKind.Field`). `java-api.txt` does not say whether a field is static
  (`java_api.rs` writes `field <class> <NAME>`; `java.rs`'s `JavaClass.fields` records no `static`), and 9 of the
  11 enum `field` lines in it are Java **instance** fields: `SecurityProtocol#id`/`#name`, `GroupProtocol#name`,
  `ElectionType#value`, `TimestampType#id`/`#name`, `AcknowledgeType#id`, `Sensor$RecordingLevel#id`/`#name`
  (`public final short id;` etc.); only `SslClientAuth#VALUES` and `RecordingLevel#MAX_RECORDING_LEVEL_KEY` are
  static. A static field cannot hold a per-constant value, and E13 / ledger 373 rule that these fields have no
  .NET form (the cast and `ToString()`; N2 allowlist). R3 states the rule for `SslClientAuth.VALUES`, and the
  code comment's reason ("as an extension block holds none") is about static fields only.
- Evidence: a scratch test at HEAD (fixture `field ...SecurityProtocol\tid`, `public enum SecurityProtocol` plus
  `public static class SecurityProtocolMethods { public static readonly short id = 0; }`) reports **no**
  diagnostic; without the field it reports `missing ... SecurityProtocol#id`. So once S2 creates
  `SecurityProtocolMethods` / `GroupProtocolMethods`, such a field clears the three E13 lines and N2's three, and
  CKRDA0014 then demands deleting the list lines that record the ruling.
- Expected fix: accept the companion field only for a Java static field: have `cargo xtask java-api` mark a
  field's scope (for example `field <class> <static|instance> <NAME>`, as `method` lines do), parse it in
  `JavaApi`, and require it in `onCompanion`; add a test that a static companion field does not clear a Java
  instance field. If that is out of this phase, record the limit (rule R3 says "a Java static field", the code
  comment says the analyzer cannot tell) as a plan deviation.
- Resolution: fixed in `4ef36add` (fixup of `499d105c`): `cargo xtask java-api` writes `field <class> <static|instance> <NAME>`, the analyzer reads the scope, and the companion field clears only a Java static field; tested by `AJavaEnumsInstanceField_AsAStaticReadonlyFieldOfItsMethodsClass_IsReported` and `marks_a_fields_scope`.

### 125-S1b-2 (Low) Two new conditions are not pinned: the companion field's `Public`, and the static `ToString`'s parameter type

- Commit: `499d105c`
- Files: `dotnet/analyzers/Confluent.Kafka.Analyzers/JavaNameAnalyzer.cs:438` (`DeclaredAccessibility:
  Accessibility.Public`), `:751` (`IsReceiver(method.Parameters[0].Type, type)`);
  `dotnet/analyzers/Confluent.Kafka.Analyzers.UnitTests/JavaNameAnalyzerTests.cs:1113`, `:1171`, `:1196`
- What is wrong: the commit says each new condition is killed by a mutant, but these two are not among the six it
  lists, and no test reaches them.
- Evidence (scratch copy of HEAD, `dotnet test analyzers/Confluent.Kafka.Analyzers.UnitTests -c Release -f
  net10.0`):
  - Mutant M1, `IFieldSymbol { IsStatic: true, IsReadOnly: true }` (no `Public`): all 232 committed tests pass;
    an `internal static readonly IReadOnlyList<SslClientAuth> VALUES` on `SslClientAuthMethods` then clears
    `missing ...SslClientAuth#VALUES` (0 diagnostics) for a member no user can see.
  - Mutant M2, `IsEnumToString` without the `IsReceiver` check: all 232 pass; `extension(GroupState) { public
    static string ToString(int state) => ""; }` then satisfies Java's `GroupState.toString()` (0 diagnostics).
    At HEAD the same source gives `missing ...GroupState#toString()` plus `overload
    M:Confluent.Kafka.Common.GroupStateMethods.ToString(System.Int32): Java's GroupState.toString has no
    overload with 1 parameter(s)`.
- Expected fix: add `internal static readonly ... VALUES` as an `InlineData` of
  `AJavaEnumsField_AbsentOrNotAStaticReadonlyFieldOfItsMethodsClass_IsReported`, and a test that a static
  `ToString` of another parameter type in the `extension(GroupState)` block (`int`, and another enum) is
  reported with both findings above, exact messages asserted.
- Resolution: fixed in `4ef36add` (fixup of `499d105c`): an `internal static readonly VALUES` InlineData, and `AJavaEnumsToString_AsAStaticToStringOfAnotherType_IsReported` (`int`, `System.DayOfWeek`) asserting both findings; mutants M1 and M2 each fail them.

### 125-S3-1 (Low) A second copy of Java's `AclOperation.fromCode` table stays in `AuthorizedOperationsMarshal`

- Commit: `a82fad5b`
- File: `dotnet/src/Confluent.Kafka/Internal/Interop/AuthorizedOperationsMarshal.cs:94-97` (beside the new
  `dotnet/src/Confluent.Kafka/Common/Acl/AclOperation.cs:129-130`)
- What is wrong: the step adds the one public translation of Java's `AclOperation.fromCode(byte)` and routes
  `AclEnumCodes.DefinedOrUnknown` through it, "(one table)" as the plan (E3) and the commit message say, but the
  internal `AuthorizedOperationsMarshal.FromCode(int)` keeps its own copy of the same table
  (`code >= UNKNOWN && code <= TWO_PHASE_COMMIT ? (AclOperation)code : UNKNOWN`), used by both `CopyOut` (`:43`)
  and `CopyOutList` (`:66`), i.e. every describe-topics / describe-cluster / describe-groups authorized-operations
  read. That is the same Java method translated twice (definition-of-done #6). Today both answer the same for
  every `int`, but they are two bounds to keep in step: when Java adds a 17th operation and the enum,
  `FromString` and `FromCode` gain it, the authorized-operations decode keeps mapping its code to `UNKNOWN`
  unless this copy is found too, and no test ties the two together.
- Evidence: `grep -n TWO_PHASE_COMMIT` over `dotnet/src` finds the bound in exactly two lookups,
  `AclOperation.cs:130` and `AuthorizedOperationsMarshal.cs:95`; the commit touches only the first.
- Fix: make `AuthorizedOperationsMarshal.FromCode(int)` read the one table, e.g.
  `AclEnumCodes.DefinedOrUnknown((AclOperation)code)` (same answer for every `int`: 0–15 kept, anything else,
  negative or above a byte, `UNKNOWN`), or call it directly at `:43` / `:66` and delete the helper.
- Resolution: fixed in `00f9579a` (fixup of `a82fad5b`): `CopyOut` and `CopyOutList` read each code through `AclEnumCodes.DefinedOrUnknown((AclOperation)code)` and the `FromCode(int)` helper is deleted, so `AclOperation.FromCode` is the one table; every answer kept (0–15 the member, anything else UNKNOWN), pinned by `AuthorizedOperationsMarshalTests` (every byte code is `FromCode`'s answer; 256, 259, 271, -1, -253, int.MinValue/MaxValue UNKNOWN), which a low-byte mutant fails.

### 125-S6-1 (Low) `GroupType.Parse`'s case direction is unpinned: an upper-casing mutant passes every `GroupType` test

- Commit: `5056273d`
- Files: `dotnet/tests/Confluent.Kafka.UnitTests/PublicGroupTypeTests.cs:78-84`
  (`Parse_LowerCasesBothSidesAsJavasToLowerCaseRoot`); the code under test is
  `dotnet/src/Confluent.Kafka/Common/GroupType.cs:81,108`.
- What is wrong: Java's `GroupType.parse` lower-cases both sides (`toLowerCase(Locale.ROOT)`), unlike
  `GroupState.parse`, which upper-cases them. The two rules answer differently only for the ledger-395 letters whose
  upper case is ASCII and whose lower case is themselves: Java `GroupType.parse("Claſſic")` and
  `parse("claßic")` are `UNKNOWN` (`"claſſic"` is not `"classic"`), and Python's
  `GroupType.parse` agrees (`"Claſſic".lower()` is `'claſſic'`). The test's rows (`CLASSİC`,
  `İ`, `UNKNOWN`, `SHARE`) answer the same under either direction (`UNKNOWN` is `UNKNOWN` either way,
  since `UNKNOWN` is also the fallback), so nothing pins the direction the test is named after.
- Evidence: in a `git archive` copy of `5056273d`, `GroupType.cs` changed to fold both sides with
  `JavaText.ToUpperRoot` (lines 81 and 108, GroupState's rule): `dotnet test ... -f net8.0 --filter
  FullyQualifiedName~PublicGroupTypeTests` gives 18 passed, 0 failed. That mutant answers `CLASSIC` for
  `"Claſſic"` and `"claßic"`, where Java answers `UNKNOWN`; HEAD answers `UNKNOWN` (probe).
- Expected fix: add rows that only the lower-casing rule answers correctly, such as
  `("Claſſic", UNKNOWN)`, `("claßic", UNKNOWN)`, `("ſhare", UNKNOWN)`, `("Conſumer", UNKNOWN)`,
  and check that the `ToUpperRoot` / `ToUpperInvariant` mutants now fail.
- Resolution: fixed in `1dc87f5c` (fixup of `5056273d`): `Parse_LowerCasesBothSidesAsJavasToLowerCaseRoot` gains `"Claſſic"`, `"claßic"`, `"Classıc"`, `"ſhare"`, `"Conſumer"`, `"ﬆreams"` → `UNKNOWN`; the `JavaText.ToUpperRoot` mutant fails 6 rows and the `ToUpperInvariant` mutant 3.

### 125-S6-2 (Low) The gRPC server's group-state filter still uses its own lookup, which answers where Python's server (`GroupState.parse`) and `GroupState.Parse` differ

- Commit: `5056273d` (open note 2)
- File: `dotnet/grpc-server/TranslateAdmin.cs:44-53` (the three reverse tables and their comment), `:710-720`,
  `:756-760`, `:1285-1319` (`BuildTransactionStateTable`, `BuildNameTable`, `ParseNames`).
- What is wrong: Python's server reads the filters with the enums' own lookups
  (`python/grpc_translate.py` `g_list_groups`: `GroupState.parse(name=s)`, `GroupType.parse(name=s)`;
  `g_list_transactions`: `TransactionState.parse(name=s)`). This commit publishes those lookups as
  `GroupState.Parse` / `GroupType.Parse` / `TransactionState.Parse`, but the .NET server keeps its private tables, and
  `s_groupStatesByName` (`StringComparer.OrdinalIgnoreCase`) is not Java's upper-casing: it does not fold the
  ledger-395 letters. So `ListGroups` with `group_states` `"Aßigning"`, `"ſtable"`, `"Assıgnıng"` or
  `"ﬆable"` sends `UNKNOWN` from the .NET server, where Python's server (`"Aßigning".upper()` is
  `ASSIGNING`) and Java's `parse` give `ASSIGNING` / `STABLE`. The comment above the tables also misstates Java:
  "GroupState.parse lower-cases its input" (it upper-cases; `GroupType.parse` lower-cases). The group-type and
  transaction-state tables answer like the public lookups today, but they are second copies of the same lookup. The
  harness's own drivers send Java's texts, so no current run is affected; a legal request to the two servers is.
- Evidence: invoking the built server's `TranslateAdmin.GroupStates` by reflection
  (`grpc-server/bin/Release/net8.0`): `"Aßigning"`, `"ſtable"`, `"Assıgnıng"`, `"ﬆable"` all give
  `UNKNOWN`, while `GroupState.Parse` gives `ASSIGNING`, `STABLE`, `ASSIGNING`, `STABLE`; `"Stable"` gives `STABLE` in
  both. Python 3: `"Aßigning".upper()` is `ASSIGNING`, `"ſtable".upper()` is `STABLE`.
- Expected fix: as Python's server, map each name with the public lookup: `GroupStates` calls `GroupState.Parse`,
  `GroupTypes` calls `GroupType.Parse`, `TransactionStates` calls `TransactionState.Parse`; delete the three tables,
  `BuildTransactionStateTable`, `BuildNameTable`, `ParseNames` and the comment. Add a `TranslateAdminEnumNameTests`
  row such as `GroupStates(new[] { "Aßigning" })` -> `ASSIGNING`.
- Resolution: fixed in `1dc87f5c` (fixup of `5056273d`): `TranslateAdmin.GroupStates` / `GroupTypes` / `TransactionStates` call `GroupState.Parse` / `GroupType.Parse` / `TransactionState.Parse`; the three tables, their comment, `BuildTransactionStateTable` and `BuildNameTable` are deleted (`ParseNames` now takes the lookup). New `TranslateAdminEnumNameTests` rows: `"Aßigning"`, `"ſtable"`, `"Assıgnıng"`, `"ﬆable"` → `ASSIGNING` / `STABLE` (4 fail on the old server), `""` → `UNKNOWN`; group-type and transaction-state rows too. `test-grpc-server-dotnet`: 228 passed.

### 125-S8-1 (Low) The T8 allowlist comment names Java's third `match()` state "any entity"; Java's is "any specified name", which excludes the default entity

- Commit: `9be88787`
- File: `dotnet/analyzers/java-name-allowlist.txt:64-66`
- What is wrong: the new T8 comment lists the three states as "(a name, the default entity, any entity)". Java's
  `ClientQuotaFilterComponent.match()` Javadoc (and its private constructor's `@param match`) says: "if present, the
  name that's matched exactly / if empty, matches the default name / if null, matches any specified name". The third
  state (`ofEntityType`, wire `DescribeClientQuotasRequest.MATCH_TYPE_SPECIFIED`) matches only entities **with** a
  name, not the default entity. "Any entity" reads as including the default entity, which blurs the very
  default-vs-specified distinction the T8 group exists to record. The binding's own docs say it right:
  `ClientQuotaMatchType.Specified` is "Match any *named* entity of the type", and `MatchType()`'s `<returns>` says
  "for any specified name". The brief asked that the T8 text match Java's `match()`; it does not.
- Evidence: `kafka/clients/src/main/java/org/apache/kafka/common/quota/ClientQuotaFilterComponent.java`, the
  `match()` Javadoc and the `ofEntityType` Javadoc ("matches any specified name for the entity type");
  `dotnet/src/Confluent.Kafka/Common/Quota/ClientQuotaMatchType.cs` (`Specified`).
- Expected fix: write Java's words, e.g. "three states (the name matched exactly, the default name, any specified
  name)". Comment-only; the list lines and counts are unchanged.
- Resolution: fixed in `d7b96028` (fixup of `9be88787`): the T8 comment now reads "three states (the name matched exactly, the default name, any specified name)", Java's `match()` words; comment only, list lines unchanged, baseline 0 removed / 0 added.
