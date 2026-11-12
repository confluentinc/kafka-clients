# Plan: Java's enum members in Java's shape (M23/P4)

Status: **approved 2026-11-11** (Pranav, relayed by the coordinator: "I agree with the plan and PM's lean.",
then "Yes to all." to the five follow-up questions). Every lean as picked: E10 is B, E15 is B, E16 is A. Ledger
rows 483–501 Agreed (the plan's draft numbers 481–498 shifted by 2, because M23/P3's close-out took 481 and 482;
501 records the approval of §7's drafts R1–R6). Manager N=125 (M23/P3 is N=124). Branch
`prashah_dev_dotnet_enum_methods_java_shape`, cut from `master` at `37d8209c` (#328) in
`TempDir/example-confluent-kafka-rust`. Mode A (no C ABI, no Python, no proto change), plus one Rust tooling step
(S1: `rust/xtask/src/java.rs`). §7's drafts are applied to `dotnet/CLAUDE.md` by the main session, which holds
Pranav's own instruction (row 501); until they land, §7 is the governing text for this phase.

**Phase number.** `dotnet/design/history/M23/` on master holds P1 (two folders) and P2; the other clone also
holds P3 (the serdes, N=124, branch `prashah_dev_dotnet_serdes_java_shape`, not merged). Both clones'
`dotnet/design/current/STATUS.md` agree. So this is **M23/P4**.

**Scope.** The enum family of the review page "Enum Family Parity" (claude.ai artifact
`HsN6joPAzeo63E99q6ndhD`, snapshot master `37d8209cb`): its 7 open decisions, the 41 Java enum members Python
has and .NET lacks (31 new, 10 moves), the 53 enum lines of `java-name-baseline.txt` and the 10 of
`java-name-allowlist.txt`. It re-decides, item by item, every earlier ruling that touches the enum family (§5).
Out of scope: the rebalance-listener base renames of the older CKRDA0013 page's T4–T6 row (rule 10 work), and
every non-enum CKRDA0013 line.

**Release status.** `dotnet/Directory.Build.props` has `<Version>4.4.0-alpha.1</Version>`; `dotnet/README.md`
says "a pre-release of a new major version"; there is no CHANGELOG; no `dotnet/*` tag exists; the NuGet index
for `Confluent.Kafka` (`api.nuget.org/v3-flatcontainer/confluent.kafka/index.json`, read 2026-11-11) ends at
`2.16.0`, so no 4.x build was ever published. A breaking rename costs users nothing today; it costs only this
repo's own callers (the gRPC server and the tests, listed per step).

---

## 1. Summary

"Breaking" means a public name or behaviour that compiles or runs differently for a caller of today's API.
Effort: XS < 1 h, S ≈ half a day, M ≈ a day, for the Actor including tests.

| # | Item | Options | Lean | Breaking | Effort |
|---|---|---|---|---|---|
| E1 | The five `<Enum>s` classes (`ScramMechanisms`, `UpgradeTypes`, `SecurityProtocols`, `SslClientAuths`, `GroupProtocols`) | A: rename to `<Enum>Methods`, statics in `extension(<Enum>)` blocks · B: A plus `[Obsolete]` forwarders · C: keep `<Enum>s`, re-open rule 9 | **A** | yes (5 type names) | M |
| E2 | `groupStatesForType` and `GroupStateExtensions` | A: static `GroupState.GroupStatesForType(GroupType)` in `GroupStateMethods` · B: keep the extension on `GroupType` · C: both | **A** | yes (receiver, class name) | S |
| E3 | ACL family, 17 new members (`AclOperation`, `AclPermissionType`, `ResourceType`, `PatternType`) | A: managed tables in `<Enum>Methods` · B: call the core's `from_code` entry points | **A** | no | M |
| E4 | Id lookups, 6 new (`ConfigResourceType`, `AlterConfigOp.OpType`, `IsolationLevel`: `Id()`, `ForId(byte)`) | add, each with Java's fallback (UNKNOWN / null / throw) | **add** | no | S |
| E5 | Text lookups, 5 new (`TimestampType.ForName`, `Parse` on `TransactionState`, `GroupState`, `GroupType`, `ClassicGroupState`) | add, taking Java's `toString()` text | **add** | no | S |
| E6 | `ScramMechanism.Type()`, `UpgradeType.Code()` (2 new) | A: add · B: keep "a cast gives it" | **A** | no | XS |
| E7 | `ElectionType.ValueOf(byte)` (decision 6) | A: add, and the analyzer reports it · B: keep "not ported", allowlist it | **A** | no | S |
| E8 | `AlterConfigOpType` (Java `AlterConfigOp.OpType`) | A: rename to `OpType` beside `AlterConfigOp` · B: keep, the pair permanent | **A** | yes (type name) | S |
| E9 | `ConfigResourceType` (Java `ConfigResource.Type`) | A: keep the name, declare it in Java's order · B: bare `Type` · C: keep as is | **A** | no | XS |
| E10 | Java's `toString()` text of 7 enums (decision 4) | A: keep, no public form · B: static `ToString(<Enum>)` in `extension(<Enum>)` · C: instance `ToJavaString()` · D: enums become classes | **B** | no (D: yes) | M |
| E11 | `SslClientAuth#toString()` (decision 2) | follows E10; the line goes where the other six go | **with the six** | no | XS |
| E12 | `SslClientAuth.VALUES` (decision 3) | A: `static readonly` field on `SslClientAuthMethods`, analyzer looks there · B: static extension property · C: both | **A** | rename only (E1) | S |
| E13 | Java fields `SecurityProtocol#id`, `#name`, `GroupProtocol#name` (decision 1) | A: N2 allowlist group · B: C# 14 extension properties `id` / `name` · C: leave in the baseline | **A** | no | XS |
| E14 | C# enum built-ins as Java's `values()`, `ordinal()`, `valueOf()`, `compareTo()` (decision 5) | A: accept, document, pin each enum's Java constants by a test · B: Java's `Values()`/`Ordinal()`/`ValueOf()` on every enum · C: B for the 2 enums that differ · D: renumber those 2 | **A** | no | S |
| E15 | An undefined value (`(ResourceType)99`) passed in (decision 7) | A: always `ArgumentOutOfRangeException` · B: read it as the enum's own Java lookup reads its number, else `ArgumentOutOfRangeException` · C: Python's way, the core decides · D: leave per call | **B** | yes, at 3 call sites | S |
| E16 | T8: `ClientQuotaMatchType` and `ClientQuotaFilterComponent.MatchType()` | A: keep both, allowlist the 2 lines · B: remove both (Python's public surface) | **A** | no | XS |
| X | Cross-cutting forms for every new member (§3): managed code, Java `byte` → C# `byte`, Java's null answers, Java's case folding, Java's parameter names, no `Enum.Parse` | — | §3 | — | — |

Ledger rows: 483–501, Agreed (§8). After the phase the baseline has lost 54 lines (the 53 enum lines and T8's
`MatchType` line) and gained none; in the allowlist the N1 group is gone (E10 B), N2 has 6 lines (E13) and a new
T8 group has 2 (E16). Under E10 A instead, N1 has 7 lines.

How the 53 lines split: 40 `missing` lines are cleared by code (E1 9, E2 1, E3 17, E4 6, E5 5, E6 2), 3 move to
N2 (E13), 2 are cleared by the analyzer change (E11, E12), and the 8 `unmatched` lines by the renames (E1 5, E2 1,
E8 1) and T8 (E16 1). E7's `ElectionType.valueOf(byte)` is the 41st member Python has; no line reports it until
S1.

---

## 2. What was verified, and how

**Built and run** (probe projects in the session scratchpad, `…/scratchpad/m23p4_probe/`, outside the repo;
.NET SDK 10.0.400, a `netstandard2.0;net10.0` library with `LangVersion latest`, consumed by `net10.0` apps):

| Claim | Result |
|---|---|
| Extension blocks compile for netstandard2.0 and net10.0 | yes |
| C# 14 `GroupState.Parse("stable")` binds to the extension, not `Enum.Parse` | yes, `STABLE` |
| C# 12 and C# 7.3 callers reach statics as `GroupStateMethods.Parse(…)` and instance members as `op.Code()` | yes, both |
| A static field in an extension block | `CS9282` |
| A static `ToString(GroupState)` in `extension(GroupState)`: C# 14 `GroupState.ToString(s)` | binds, `"Stable"` |
| An *instance* extension `ToString()` | never chosen: `s.ToString()` stays `"STABLE"`; and it collides with a static `ToString(GroupState)` in the same class (`CS0111`) |
| A static extension property `VALUES`: C# 14 `SslClientAuth.VALUES`; C# 12 | C# 14 yes; C# 12 only as `SslClientAuthMethods.get_VALUES()` |
| `Enum.GetValues` / `GetNames` / `CompareTo` with `ConfigResourceType` declared in Java's order or reversed | the same either way: sorted by value; only `typeof(E).GetFields()` follows the declaration |
| `Enum.GetValues<TimestampType>()` (`-1, 0, 1`); `NO_TIMESTAMP_TYPE.CompareTo(CREATE_TIME)` | `CREATE_TIME, LOG_APPEND_TIME, NO_TIMESTAMP_TYPE` (unsigned sort); `-1` (signed) |
| `Enum.Parse<AclOperation>("READ, WRITE")`, `("99")`, `("XYZ")` | `ALTER`; `99` (undefined); `System.ArgumentException` |
| A namespace-level enum named `Type` beside `ConfigResource`, used with implicit usings | `CS0104` ambiguous with `System.Type` |
| `"İ".ToLowerInvariant()` and `"K".ToLowerInvariant()`, ICU and invariant globalization | `U+0130` unchanged and `k`, in both modes |
| `GroupState.Parse(GroupState.PREPARING_REBALANCE.ToString())` with Java's parse | `UNKNOWN` (Java's `parse` takes `"PreparingRebalance"`) |

**Re-run:** the page's Python probe (`…/scratchpad/pyprobe.py`) against this clone's `python/` (CPython 3.12,
modules imported directly): the same 2 differences from Java (`TimestampType.for_name(None)`'s message,
`SecurityProtocol.names()` a new list per call). A script comparing every .NET enum's constants, order and values
with the Java source: all 22 Java-backed enums match; only `ConfigResourceType`'s declaration order differs.

**Read** (not run): the Java 4.4.0-rc4 sources and tests (no JDK on this machine, so Java behaviour is from
reading); the analyzer (`JavaNameAnalyzer.cs:49-50`, `:282`, `:414`, `:429-435`, `:660`) and `java.rs:1219-1248`;
the .NET sources and tests cited below. The 53 enum lines of the baseline were compared line by line with the
page's list: identical. I did **not** build the binding with the baseline off, and did not run its tests.

**Page claims found wrong, incomplete or stale:**

1. Decision 5 asks whether `ConfigResourceType` is "declared in Java's order". Declaring it so changes nothing a
   caller sees through `Enum.GetValues`, `GetNames` or `CompareTo` (all sorted by value, measured above); it
   changes the source and reflection order only. `values()` order cannot be fixed by declaration (E9, E14).
2. "Undefined values" is incomplete: `ListTransactionsOptions.FilterStates` with an undefined
   `TransactionState` silently sends `"Unknown"` (`TransactionMarshal.WireName`, `Internal/Interop/TransactionMarshal.cs:29`),
   a fourth behaviour the page does not list; and the ACL value types normalize in their own constructors
   (`AccessControlEntry.cs:64-65`, `AccessControlEntryFilter.cs:81-82`, `ResourcePattern.cs:69-70`,
   `ResourcePatternFilter.cs:64-66`), not only "in the marshal" (E15).
3. `GroupState.parse` "ignores case", but it matches Java's display text, not the constant name:
   `parse("PREPARING_REBALANCE")` is `UNKNOWN` in Java. The page's round-trip point holds, and is sharper than it
   says (E10).
4. The brief's and `dotnet/CLAUDE.md`'s "ledger 368" for the `UpgradeType` move (idiom map, "a member .NET forces
   to be a property") does not match `DOTNET-PORT-DECISIONS.md`, whose row 368 is "how `SecurityProtocol`,
   `SslClientAuth`, `GroupProtocol` are written". The move was decided in M22/P4 PLAN L-5
   (`dotnet/design/history/M22/P4-getters-as-methods/PLAN.md:17`). The idiom map's cites 365–371 appear to come
   from another ledger copy; §9 lists it as unresolved.
5. The older CKRDA0013 page's N1 row ("on the `<Enum>s` classes") and T4–T6 row ("`GroupStateExtensions` to
   `GroupStates`") are stale: rule 9 (#328) names the class `<Enum>Methods`. The new page already says so.
6. The gRPC server keeps its own copies of Java's text for `TransactionState`, `GroupState`, `GroupType` and
   `ClassicGroupState` (`dotnet/grpc-server/TranslateAdmin.cs:1284-1340`), because no public form exists; Python's
   server writes `str(x)` (`python/grpc_translate.py:1288-1391`). The page does not mention it; it bears on E10.

---

## 3. Cross-cutting forms (apply to every new member)

These are leans, each a proposed ledger row; they make the 31 new members uniform.

**X1 — Managed code, not the C ABI.** Each member is a small table in its `<Enum>Methods` class, as today's
`ScramMechanisms.FromType` and as Python's pure-Python enums (root `CLAUDE.md`, Implementation over the FFI:
"enums … are a direct Python translation of its Java source"). The core's `kafka_common_acl_*_from_code_err`
entry points stay what the marshal uses for the native singletons. *Alternative:* P/Invoke per call (needs the
native library loaded for a constant lookup; adds a singleton → enum conversion). Lean: managed.

**X2 — Java `byte` is C# `byte`.** `Code()`, `Id()`, `Type()` return `byte`; `FromCode(byte)`, `ForId(byte)`,
`FromType(byte)`, `ValueOf(byte)` take it, as today's `ScramMechanism : byte`, `UpgradeType : byte` and
`ScramMechanisms.FromType(byte)`. The enums' underlying types do not change. *Alternative:* `sbyte`, Java's signed
range, so a message prints `-1` where Java prints `-1` (C# `byte` prints `255` for the same bits). Every Java code
of these enums is 0–32, so lookups answer the same; only the text of `IsolationLevel.ForId` / `ElectionType.ValueOf`
for a value ≥ 128 differs. Lean: `byte` (the existing public precedent; no ruling exists either way).

**X3 — Java's null answers.** Where Java throws `NullPointerException`, .NET throws `ArgumentNullException`
naming Java's parameter, as `SecurityProtocols.ForName` does (idiom map, `KafkaException` row). Where Java answers
null without throwing, .NET answers the same: `TransactionState.Parse(null)`, `GroupType.Parse(null)`,
`PatternType.FromString(null)`, `ScramMechanism.FromMechanismName(null)` → `UNKNOWN`; `SslClientAuth.ForConfig(null)`
→ `NONE`; `TimestampType.ForName(null)` → `NoSuchElementException("Invalid timestamp type null")` (Java's `null`,
as .NET prints Java's spellings, ledger 359 / 413; Python prints `None` by its own rule). The parameter is
annotated `string?` only where Java's body has an explicit null branch (`GroupType.parse`, `SslClientAuth.forConfig`),
Python's nullability rule.

**X4 — Java's case folding.** `toUpperCase(Locale.ROOT)` is `JavaText.ToUpperRoot` (the 10-letter table, ledger
395), so `AclOperation.FromString("deſcribe")` is `DESCRIBE`, as Java's. `GroupType.parse`'s
`toLowerCase(Locale.ROOT)` is `ToLowerInvariant`: the only non-ASCII letters whose Java lower case could matter
are `U+212A` (both give `k`) and `U+0130` (Java gives `i̇`, .NET leaves it; neither matches a name), measured in
both globalization modes.

**X5 — Java's parameter names** (rule 6): `FromString(string str)` on the three ACL enums, `FromString(string name)`
on `PatternType`, `ForId(byte id)`, `FromCode(byte code)`, `FromType(byte type)`, `Parse(string name)`,
`ForName(string name)`, `ValueOf(byte value)`, `GroupStatesForType(GroupType type)`.

**X6 — Never `Enum.Parse` / `Enum.TryParse` inside a lookup.** They accept `"3"`, `"READ, WRITE"` and trim spaces,
which Java's `valueOf` refuses. An ordinal `switch` on the folded text, as `SecurityProtocols.ForName` does.

**X7 — Instance members in an `extension(<Enum> x)` block**, not `this` methods (rule 9 allows both). One form per
class; older C# still calls them as `x.Code()` (measured on C# 7.3).

---

## 4. Items

Each item: Java (4.4.0-rc4), Python (`python/confluent_kafka`), .NET today, then the options.

### E1 — The five `<Enum>s` classes become `<Enum>Methods` (rule 9)

```java
// Java
ScramMechanism m = ScramMechanism.fromMechanismName("SCRAM-SHA-256");
String name = m.mechanismName();
```
```python
# Python
m = ScramMechanism.from_mechanism_name(mechanism_name="SCRAM-SHA-256")
name = m.mechanism_name()
```
```csharp
// .NET today (Admin/ScramMechanism.cs:42) — also UpgradeTypes, SecurityProtocols, SslClientAuths, GroupProtocols
public static class ScramMechanisms
{
    public static string MechanismName(this ScramMechanism mechanism) => …;
    public static ScramMechanism FromType(byte type) => …;
    public static ScramMechanism FromMechanismName(string mechanismName) => …;
}
var m = ScramMechanisms.FromMechanismName("SCRAM-SHA-256");
```

**A (lean)** — rename, statics into an `extension(<Enum>)` block:
```csharp
public static class ScramMechanismMethods
{
    extension(ScramMechanism)
    {
        public static ScramMechanism FromType(byte type) => …;
        public static ScramMechanism FromMechanismName(string mechanismName) => …;
    }

    extension(ScramMechanism mechanism)
    {
        public string MechanismName() => …;
        public byte Type() => (byte)mechanism;          // E6
    }
}
// C# 14
var m = ScramMechanism.FromMechanismName("SCRAM-SHA-256");
// C# 7.3 – 13
var m = ScramMechanismMethods.FromMechanismName("SCRAM-SHA-256");
// every version
string name = m.MechanismName();
```
Pros: rule 9 as ruled (T5, 2026-11-10; #328); C# 14 callers write Java's call. Cons: breaks the five names
(unpublished). **B** — A plus `[Obsolete]` forwarding classes under the old names: keeps five `unmatched` lines
and two spellings, for a package nobody has installed. **C** — keep `<Enum>s`: re-opens the T5 ruling and the
analyzer rule of #328; no new reason to.

The same shape for `UpgradeTypeMethods` (`FromCode`, `Code`), `SecurityProtocolMethods` (`Names`, `ForId`,
`ForName`), `SslClientAuthMethods` (`ForConfig`; `VALUES`, E12) and `GroupProtocolMethods` (`Of`). Bodies do not
change. Clears 5 `unmatched` lines and 9 `missing` ones (the members were there under a class the analyzer did not
pair).

### E2 — `groupStatesForType` goes on `GroupState`

```java
Set<GroupState> s = GroupState.groupStatesForType(GroupType.CLASSIC);
```
```python
s = GroupState.group_states_for_type(type=GroupType.CLASSIC)
```
```csharp
// .NET today (Common/GroupState.cs:85-140): an extension of GroupType
var s = GroupType.CLASSIC.GroupStatesForType();          // GroupStateExtensions
```
**A (lean)** — Java's receiver:
```csharp
public static class GroupStateMethods
{
    extension(GroupState)
    {
        public static IReadOnlyCollection<GroupState> GroupStatesForType(GroupType type) => …; // body unchanged
    }
}
// C# 14:   GroupState.GroupStatesForType(GroupType.CLASSIC)
// older:   GroupStateMethods.GroupStatesForType(GroupType.CLASSIC)
```
**B** — keep `GroupType.CLASSIC.GroupStatesForType()`: not Java's (Java's is a static of `GroupState`); the
analyzer reports it on the wrong type. **C** — both: two spellings, one not Java's. Lean A. Clears 1 `unmatched`
and 1 `missing` line. The class also takes `Parse` (E5) and `ToString` (E10).

### E3 — The ACL family: `code()`, `fromCode`, `fromString`, `isUnknown`, `isSpecific`

```java
// AclOperation (AclPermissionType and ResourceType are the same shape)
public static AclOperation fromString(String str) {                 // NPE on null
    try { return AclOperation.valueOf(str.toUpperCase(Locale.ROOT)); }
    catch (IllegalArgumentException e) { return UNKNOWN; }
}
public static AclOperation fromCode(byte code) { … return operation == null ? UNKNOWN : operation; }
public byte code() { return code; }
public boolean isUnknown() { return this == UNKNOWN; }
// PatternType differs: fromString(String name) is exact (case-sensitive) and null gives UNKNOWN;
// isSpecific() is this != UNKNOWN && this != ANY && this != MATCH.
```
```python
AclOperation.from_string(str="read")     # READ; from_code(code=120) -> UNKNOWN; op.code(); op.is_unknown()
PatternType.from_string(name="literal")  # UNKNOWN (case-sensitive); p.is_specific()
```
```csharp
// .NET today: no public member. Internal: AclEnumCodes.DefinedOrUnknown, AclMarshal (C ABI from_code).
```
**A (lean)** — managed tables (X1):
```csharp
public static class AclOperationMethods
{
    extension(AclOperation)
    {
        public static AclOperation FromString(string str) =>
            JavaText.ToUpperRoot(str ?? throw new ArgumentNullException(nameof(str))) switch
            {
                nameof(AclOperation.UNKNOWN) => AclOperation.UNKNOWN,
                nameof(AclOperation.ANY) => AclOperation.ANY,
                // … one arm per constant
                _ => AclOperation.UNKNOWN,
            };

        public static AclOperation FromCode(byte code) =>
            code <= (byte)AclOperation.TWO_PHASE_COMMIT ? (AclOperation)code : AclOperation.UNKNOWN;
    }

    extension(AclOperation operation)
    {
        public byte Code() => (byte)operation;
        public bool IsUnknown() => operation == AclOperation.UNKNOWN;
    }
}
// C# 14
AclOperation op = AclOperation.FromString("read");   // READ
byte code = op.Code();                               // 3
// older
AclOperation op = AclOperationMethods.FromString("read");
byte code = op.Code();
```
**B** — call the core (`kafka_common_acl_AclOperation_from_code_err`, already in `NativeMethods.AdminAcls.cs:101`)
and map the singleton back: a P/Invoke and a native load for a constant, and `fromString` has no entry point the
binding uses. Lean A. 17 members, 17 `missing` lines. The internal `AclEnumCodes.DefinedOrUnknown` becomes
`FromCode((byte)value)` (one table).

### E4 — Id lookups: `ConfigResourceType`, `OpType`, `IsolationLevel`

```java
ConfigResource.Type.forId((byte) 1);   // UNKNOWN for an unknown id
AlterConfigOp.OpType.forId((byte) 7);  // null
IsolationLevel.forId((byte) 5);        // IllegalArgumentException("Unknown isolation level 5")
type.id(); opType.id(); level.id();    // byte
```
```python
ConfigResource.Type.for_id(id=1)       # UNKNOWN
AlterConfigOp.OpType.for_id(id=7)      # None
IsolationLevel.for_id(id=7)            # IllegalArgumentError("Unknown isolation level 7")
```
```csharp
// .NET today: none public; ConfigResourceMarshal.TypeFromId is internal (Internal/Interop/ConfigResourceMarshal.cs:28).
```
Lean — add, one shape:
```csharp
extension(ConfigResourceType) { public static ConfigResourceType ForId(byte id) => …; }        // UNKNOWN fallback
extension(ConfigResourceType type) { public byte Id() => (byte)type; }
extension(OpType) { public static OpType? ForId(byte id) => …; }                               // null, as Java
extension(IsolationLevel) { public static IsolationLevel ForId(byte id) => id switch
    { 0 => IsolationLevel.READ_UNCOMMITTED, 1 => IsolationLevel.READ_COMMITTED,
      _ => throw new IllegalArgumentException("Unknown isolation level " + id) }; }
// C# 14: ConfigResourceType.ForId(2), OpType.ForId(7) == null, level.Id()
// older: ConfigResourceTypeMethods.ForId(2), OpTypeMethods.ForId(7), level.Id()
```
No real alternative: Java and Python have them, and nothing in .NET forbids them. 6 `missing` lines.

### E5 — Text lookups: `forName` and `parse`

```java
TimestampType.forName("CreateTime");        // exact; else NoSuchElementException("Invalid timestamp type x")
TransactionState.parse("PrepareAbort");     // exact; unknown or null -> UNKNOWN
GroupState.parse("stable");                 // upper-cases both sides; NPE on null; "PREPARING_REBALANCE" -> UNKNOWN
GroupType.parse("CONSUMER");                // lower-cases both sides; null -> UNKNOWN
ClassicGroupState.parse("preparingrebalance"); // as GroupState
```
```python
TimestampType.for_name(name="CreateTime"); TransactionState.parse(name="CompleteCommit")
GroupState.parse(name="notready")          # NOT_READY; GroupType.parse(name=None) -> UNKNOWN
```
```csharp
// .NET today: the doc comments say "Java's parse(String) is not published (a recorded deviation)"
// (GroupState.cs:39, GroupType.cs:35, ClassicGroupState.cs:40). The text tables exist internally
// (GroupMarshal.NameFrom*, TransactionMarshal.NameFromState, ConsumerRecord.TimestampTypeText).
```
Lean — add, in each enum's `<Enum>Methods`, reading the same table the binding already uses for the wire:
```csharp
// C# 14
GroupState s = GroupState.Parse("stable");                 // STABLE
TimestampType t = TimestampType.ForName("CreateTime");     // CREATE_TIME
// older
GroupState s = GroupStateMethods.Parse("stable");
```
They take **Java's `toString()` text**, which a .NET caller can only get from E10. The doc comments that call
`parse` "not published" go (§5, row 7). 5 `missing` lines.

### E6 — `ScramMechanism.type()` and `UpgradeType.code()`

```java
byte t = mechanism.type();  byte c = upgradeType.code();
```
```python
t = mechanism.type();  c = upgrade_type.code()
```
```csharp
// .NET today: FeatureUpdate.cs:139 "Java's code() needs no counterpart: a cast yields it."
byte c = (byte)upgradeType;
```
**A (lean)** — `extension(ScramMechanism mechanism) { public byte Type() => (byte)mechanism; }` and
`extension(UpgradeType upgradeType) { public byte Code() => (byte)upgradeType; }`; the cast keeps working.
Rule 2 (a Java method is a C# method); Python has both; the ACL enums get `Code()` in E3, so leaving these two out
would be the odd ones. **B** — keep "a cast gives it" and allowlist the two lines: a third pattern for the same
Java method. Lean A. (Inside `FeatureUpdate`, `UpgradeType` names the method, so the class's own code keeps writing
`Admin.UpgradeType.…`, idiom map.)

### E7 — `ElectionType.valueOf(byte)` (decision 6)

```java
ElectionType.valueOf((byte) 5);  // IllegalArgumentException("Value 5 must be one of [PREFERRED, UNCLEAN]")
```
```python
ElectionType.value_of(value=5)   # IllegalArgumentError("Value 5 must be one of [PREFERRED, UNCLEAN]")
```
```csharp
// .NET today (ElectionType.cs:28): "Java's value field and valueOf(byte) factory are not ported".
```
**A (lean)** — add it, and make the analyzer report it:
```csharp
public static class ElectionTypeMethods
{
    extension(ElectionType)
    {
        public static ElectionType ValueOf(byte value) => value switch
        {
            0 => ElectionType.PREFERRED,
            1 => ElectionType.UNCLEAN,
            _ => throw new IllegalArgumentException(
                "Value " + value + " must be one of [PREFERRED, UNCLEAN]"),
        };
    }
}
// C# 14: ElectionType.ValueOf(1)      older: ElectionTypeMethods.ValueOf(1)     (both measured)
```
It is a Java static, not the built-in `valueOf(String)` (rule 4); `Enum.Parse` covers only the built-in. The
analyzer misses it because it skips every enum method named `valueOf` (`JavaNameAnalyzer.cs:49-50`, `:414`), and
`java-api.txt` lacks the built-in `valueOf(String)` because the generator adds a built-in only when no method of
that name exists (`java.rs:1234`). S1 fixes both. **B** — keep it out and allowlist the line: Python has it, and
nothing stops .NET. Lean A.

### E8 — `AlterConfigOpType` becomes `OpType`

```java
new AlterConfigOp(entry, AlterConfigOp.OpType.SET);   op.opType();
```
```python
AlterConfigOp(config_entry=e, operation_type=AlterConfigOp.OpType.SET)
```
```csharp
// .NET today (Admin/AlterConfigOpType.cs): flattened because AlterConfigOp has a method OpType() (CS0102)
new AlterConfigOp(entry, AlterConfigOpType.SET);
```
**A (lean)** — rule 1 (a nested type C# cannot nest sits beside its outer type with its bare name), as
`UpgradeType` and `GroupMembershipOperation` do; `java-name-pairs.txt` already says "until it is renamed OpType":
```csharp
namespace Confluent.Kafka.Admin;
public enum OpType { SET = 0, DELETE = 1, APPEND = 2, SUBTRACT = 3 }
public static class OpTypeMethods { … }                           // E4
new AlterConfigOp(entry, OpType.SET);                             // user code
// inside AlterConfigOp, where OpType names the method: Admin.OpType.SET (idiom map)
```
No `System`/common type is named `OpType`. **B** — keep `AlterConfigOpType` with a permanent pair line. Lean A;
the older page's T4–T6 row ruled it too. The file is renamed `OpType.cs` and stays where it is.

### E9 — `ConfigResourceType` keeps its name; declared in Java's order

```java
public enum Type { GROUP((byte) 32), CLIENT_METRICS((byte) 16), BROKER_LOGGER((byte) 8),
                   BROKER((byte) 4), TOPIC((byte) 2), UNKNOWN((byte) 0); }   // nested in ConfigResource
```
```csharp
// .NET today (Common/Config/ConfigResourceType.cs:31): UNKNOWN = 0 first … GROUP = 32 last
```
**A (lean)** — keep `ConfigResourceType` (M15/P3 D16; ledger 262): it cannot nest (`ConfigResource.Type()` is a
method, CS0102), and a bare `Type` beside it is `CS0104` against `System.Type` at every user site with implicit
usings (measured). Declare the members in Java's order: the source then reads as Java's, at no cost; it does
**not** change `Enum.GetValues` (§2), which E14 documents. **B** — bare `Type`: unusable without qualification.
**C** — keep the reversed declaration: no reason to. The T4 allowlist line and the pair stay.

### E10 — Java's `toString()` text (decision 4)

Seven Java enums override `toString()`: `GroupState` (`"Stable"`), `GroupType` (`"Consumer"`), `ClassicGroupState`
(`"PreparingRebalance"`), `TransactionState` (`"PrepareAbort"`), `TimestampType` (`"CreateTime"`), `IsolationLevel`
(`"read_committed"`), `SslClientAuth` (`"required"`). Java uses the text as the round-trip of `parse`/`forName`/
`forConfig`, and for `IsolationLevel` and `SslClientAuth` it is the config value (`isolation.level=read_committed`).

```java
GroupState s = GroupState.parse(state.toString());     // round-trips
```
```python
s = GroupState.parse(name=str(state))                  # str() is Java's toString()
```
```csharp
// .NET today: ToString() prints the constant (STABLE), a recorded deviation (ledger 285, 310, 390).
// The binding prints Java's text through internal maps; the gRPC server keeps its own copies
// (TranslateAdmin.cs:1284-1340). Nothing public gives "Stable".
GroupState.Parse(state.ToString());                    // with E5: UNKNOWN for PREPARING_REBALANCE (measured)
```

**A** — keep: no public form. Round-trip impossible; ledger 310 left "a public helper later" open.

**B (lean)** — a static `ToString(<Enum>)` in the `extension(<Enum>)` block:
```csharp
public static class GroupStateMethods
{
    extension(GroupState)
    {
        public static string ToString(GroupState state) => …;   // "Stable"; the table the marshal uses
    }
}
// C# 14
string text = GroupState.ToString(state);              // "Stable"   (measured)
GroupState back = GroupState.Parse(GroupState.ToString(state));
// older
string text = GroupStateMethods.ToString(state);
// unchanged
string name = state.ToString();                        // "STABLE"
```
Pros: Java's member name, in the class that holds every other Java method of the enum; restores the round-trip;
the gRPC server drops its private copies, as Python's server uses `str()`. Cons: two `ToString`s that differ
(`state.ToString()` is the constant, `GroupState.ToString(state)` Java's text); interpolation still prints the
constant. The analyzer learns that Java's `toString()` override of an enum is this static (S1), and the N1 group
goes. It must be the static form: an instance extension `ToString()` is never chosen over `Enum.ToString()`
(measured), so it would look right and silently print the constant.

**C** — an instance extension with a new name, `state.ToJavaString()`: reads naturally, but the name is invented
(no Java or Python counterpart; the analyzer flags it `unmatched` unless allowlisted). Not `Name()`: Java's
`name()` is the constant name.

**D** — the seven enums become sealed classes with `static readonly` instances overriding `ToString()`: exact
Java behaviour (and Java's `values()`/`ordinal()` too), but breaks `switch` on constants and `const` use, splits
the 22 enums into two kinds, and is the option D the T5 ruling did not take.

Lean B; if the two `ToString`s are judged too confusing, A (today) is the fallback, not C.

### E11 — `SslClientAuth#toString()` (decision 2)

```java
SslClientAuth.REQUIRED.toString();   // "required"
```
```python
str(SslClientAuth.REQUIRED)          # "required"
```
```csharp
SslClientAuth.REQUIRED.ToString();   // "REQUIRED"; pinned by PublicConfigEnumsTests.cs:189; ledger 390
```
It is the same case as the six, listed differently (baseline, not N1; `dotnet/CLAUDE.md:737` names six). Lean:
whatever E10 decides for the six applies to it. Under E10 B it gets `SslClientAuth.ToString(auth)` → `"required"`
(amends ledger 390); under E10 A its line joins N1 and the rule text names seven.

### E12 — `SslClientAuth.VALUES` (decision 3)

```java
public static final List<SslClientAuth> VALUES = List.of(SslClientAuth.values());   // on the enum
```
```python
SslClientAuth.VALUES                 # a class attribute, a tuple of the three members
```
```csharp
// .NET today: SslClientAuths.VALUES, a static readonly IReadOnlyList (ledger 386 amended);
// PublicJavaMemberShapeTests.cs:186 and PublicConfigEnumsTests.cs:174 pin it as a field.
```
An extension block cannot hold a field (`CS9282`, measured).

**A (lean)** — the field on `SslClientAuthMethods`, and the analyzer accepts an enum's Java field there:
```csharp
public static class SslClientAuthMethods
{
    public static readonly IReadOnlyList<SslClientAuth> VALUES = …;   // outside any block
    extension(SslClientAuth) { public static SslClientAuth? ForConfig(string? key) => …; }
}
var all = SslClientAuthMethods.VALUES;                                   // every C# version
```
Pros: keeps "a field, never a property" (idiom map; ledger 386 amended names this very field); one spelling on
every C# version. Cons: not `SslClientAuth.VALUES` even on C# 14.

**B** — a static extension property: C# 14 writes Java's `SslClientAuth.VALUES`, but C# 7.3–13 must write
`SslClientAuthMethods.get_VALUES()` (measured), and it is a property against the idiom map. **C** — both: two
public spellings of one constant. Lean A.

### E13 — Java's fields `SecurityProtocol#id`, `#name`, `GroupProtocol#name` (decision 1)

```java
short id = SecurityProtocol.SSL.id;   String n = SecurityProtocol.SSL.name;   GroupProtocol.CONSUMER.name;
```
```python
SecurityProtocol.SSL.value == 1;  str(SecurityProtocol.SSL) == "SSL"   # no id / name attributes
```
```csharp
short id = (short)SecurityProtocol.SSL;   string n = SecurityProtocol.SSL.ToString();   // ledger 373
```
**A (lean)** — move the three lines to the N2 group, whose reason (Python's enum rule: the integer field is the
value, the others are reached through Java's methods) and ruling (ledger 373: "Java's `id` is the cast and its
`name` field `ToString()`") already cover them. **B** — C# 14 extension properties `protocol.id` / `protocol.name`:
lower-case properties, against the idiom map and .NET naming, C# 14 only. **C** — leave them in the baseline (the
ledger 444 "review later" state). Lean A.

N2's own three lines (`ElectionType#value`, `TimestampType#id`, `#name`) stay. Its comment changes under E10 B:
`TimestampType#name`'s text is then reached through `TimestampType.ToString(t)`.

### E14 — C# enum built-ins as Java's `values()`, `ordinal()`, `valueOf()`, `compareTo()` (decision 5)

Rule 7 says a C# enum covers `name`, `ordinal`, `values` and `valueOf`. Measured differences:

| Java | .NET | Where it differs |
|---|---|---|
| `values()` in declaration order | `Enum.GetValues<E>()` sorted by the value's unsigned magnitude | `ConfigResourceType` (`UNKNOWN, TOPIC, …, GROUP`; Java `GROUP` first), `TimestampType` (`CREATE_TIME, LOG_APPEND_TIME, NO_TIMESTAMP_TYPE`: `-1` is `0xFFFFFFFF`, so it sorts last; Java first) |
| `ordinal()` | `(int)e` | `ConfigResourceType` (`GROUP` 0 vs 32), `TimestampType` (`NO_TIMESTAMP_TYPE` 0 vs -1) |
| `compareTo` by ordinal | `CompareTo` by signed value | `ConfigResourceType` only (`GROUP.CompareTo(TOPIC)` is 1, Java negative); `TimestampType` agrees (`NO_TIMESTAMP_TYPE.CompareTo(CREATE_TIME)` is -1) |
| `valueOf("READ, WRITE")`, `("99")` throw | `Enum.Parse` gives `ALTER`, `99` | every enum |
| `valueOf("XYZ")`: `IllegalArgumentException("No enum constant …")` | `System.ArgumentException` | every enum (the binding's `IllegalArgumentException` derives from `ArgumentException`, so a `catch (ArgumentException)` catches both) |

All measured (§2). The other 20 enums have value = ordinal, so only the `Enum.Parse` rows touch them.

Python (the tie-breaker) uses its own built-ins: iteration in declaration order (matches Java), comparison by
value (differs from Java like .NET), `E[name]` for `valueOf`.

**A (lean)** — accept the built-ins, as rule 7 and Python do; document the differences once in rule 7 and on the
two enums; add one test per enum pinning Java's constants and values in Java's order (S8), which also catches a
Kafka upgrade that adds a constant. **B** — Java's `Values()`, `Ordinal()`, `ValueOf(string)` on every
`<Enum>Methods` (66 members): contradicts rule 7, and the analyzer skips these built-ins by design. **C** — B only
for `ConfigResourceType` and `TimestampType`: two enums with an API the other 20 lack. **D** — renumber the two so
value = ordinal and expose the id only through `Id()`: breaks the "value is Java's integer field" rule (N2, ledger
373) and the C ABI, which passes ids. Lean A.

### E15 — An undefined enum value passed in (decision 7)

A C# cast makes any number an enum value (`(ResourceType)99`); Java and Python enums cannot hold one (a Python
caller can pass a plain `int`; Python then stores it and the FFI decides). Today, per call site (re-read):

| Site | Today |
|---|---|
| `AccessControlEntry`, `AccessControlEntryFilter`, `ResourcePattern`, `ResourcePatternFilter` constructors; the ACL marshal | `UNKNOWN` (M15/P13.3 F8: the core normalizes the callback keys, so .NET must) |
| `ConfigResource` constructor | `UNKNOWN` (M15/P13.2 G2-1, "the user's direction") |
| `ScramCredentialInfo` → marshal; `FeatureUpdate` → `UpdateFeatures` | the core's `from_type` / `from_code` → `UNKNOWN` (ledger 197) |
| `ListTransactionsOptions.FilterStates` | sends `"Unknown"` |
| `AlterConfigOp` constructor; `CloseOptions.WithGroupMembershipOperation`; `ElectLeaders`; `ListOffsets`' isolation level; `ListGroupsOptions` state and type filters | `ArgumentOutOfRangeException` |

**A** — always `ArgumentOutOfRangeException` at the call (the idiom map's ".NET-only precondition" class):
reverses ledger 197, G2-1 and F8, and F8's reason (key normalization) still holds.

**B (lean)** — read the number as the enum's own Java lookup reads it, where Java has one; otherwise
`ArgumentOutOfRangeException`:

| Enum | Java's lookup | Undefined value |
|---|---|---|
| the 4 ACL enums, `ConfigResourceType`, `ScramMechanism`, `UpgradeType` | `fromCode` / `forId` / `fromType` → `UNKNOWN` | `UNKNOWN` (today) |
| `IsolationLevel` | `forId` throws | `IllegalArgumentException("Unknown isolation level 5")` (was `ArgumentOutOfRangeException`) |
| `ElectionType` | `valueOf(byte)` throws | `IllegalArgumentException("Value 5 must be one of [PREFERRED, UNCLEAN]")` (was `ArgumentOutOfRangeException`) |
| `OpType` | `forId` → null, which Java's request builder dereferences | `ArgumentOutOfRangeException` (today) |
| `TransactionState`, `GroupState`, `GroupType`, `GroupMembershipOperation` | none (text only, or no methods) | `ArgumentOutOfRangeException` (`TransactionState` filter: was `"Unknown"`) |

Pros: one rule, stated by Java's own methods (which E3–E7 add); keeps the three earlier rulings. Cons: three sites
change (two exception classes, one silent send becomes a throw); both new classes still derive from
`ArgumentException`.

**C** — Python's way: no managed check; the core's lookup decides. Needs a per-site C ABI check and reverses the
`ArgumentOutOfRangeException` sites, which exist because some ABI calls take a singleton the binding must pick.
**D** — leave it per call, documented in one list. Lean B.

Receivers: an instance member reads the value as it is, and never throws: `((AclOperation)99).Code()` is `99`,
`.IsUnknown()` is `false` (Java's `this == UNKNOWN`), `GroupState.ToString((GroupState)99)` is `"99"` (C#'s own
text, as the owners' `ToString`s print today, `TransactionListing.cs:90`).

### E16 — T8: `ClientQuotaMatchType` and `MatchType()`

```java
public Optional<String> match();   // Optional.of(name) | Optional.empty() | null: three states
```
```python
def match(self) -> str | None      # None for both ofDefaultEntity and ofEntityType; the third state is private
```
```csharp
public string? Match();  public ClientQuotaMatchType MatchType();   // Exact | Default | Specified
```
**A (lean)** — keep both and allowlist `unmatched T:…ClientQuotaMatchType` and `unmatched M:…MatchType` under a T8
group: the idiom map already records it ("a .NET-only getter … `ClientQuotaFilterComponent.MatchType()` tells
Java's three `match()` states apart … *(deviation)*"; M22/P4 D-a2). **B** — remove both, as Python's public
surface: a caller can no longer tell `ofDefaultEntity` from `ofEntityType`. Lean A. (`ClientQuotaMatchType` keeps
.NET casing: no Java constant behind it, ledger 285.)

---

## 5. Earlier rulings and recorded deviations, re-evaluated

| # | What | Where recorded | Decided, and why | Holds against Java's shape? | Lean |
|---|---|---|---|---|---|
| 1 | N1 placement: "on the `<Enum>s` classes" | older CKRDA0013 page, N1 row (Ruled) | Python's pattern, before rule 9 | superseded by rule 9 (#328, T5 ruling 2026-11-10) | **change**: `<Enum>Methods` (E1) |
| 2 | `<Enum>s` static classes, plain statics | ledger 388 (M22/P2 D22) | `ScramMechanisms` / `UpgradeTypes` precedent over `*Extensions` | superseded by the T5 ruling | **change** (E1) |
| 3 | T4–T6: `AlterConfigOpType` → `OpType` | older page T4–T6 (Ruled); `java-name-pairs.txt` | rule 1 | yes | **keep**, do it (E8) |
| 4 | T4–T6: `GroupStateExtensions` → `GroupStates` | older page T4–T6 (Ruled) | `<Enum>s` naming | the name is superseded; the receiver was never Java's | **change**: `GroupStateMethods`, static on `GroupState` (E2) |
| 5 | N1 allowlist group, enum `toString()` | ledger 285, 310 ("(a) accept, a public helper later"), allowlist N1 | C# cannot override an enum's `ToString()` | the fact holds; the "helper later" is this phase | **change** under E10 B (group deleted); else keep + `SslClientAuth` |
| 6 | `SslClientAuth` prints `REQUIRED`, no map | ledger 390 | never printed or sent by the binding | Java's text is the config value | **amend** under E10 B (E11) |
| 7 | "Java's `parse(String)` is not published (a recorded deviation)" | doc comments `GroupState.cs:39`, `GroupType.cs:35`, `ClassicGroupState.cs:40` | "decodes a wire spelling the caller never sees" | no: Java and Python publish it, and the caller sees the text under E10 | **drop** (E5) |
| 8 | "Java's `code()` needs no counterpart: a cast yields it" | `FeatureUpdate.cs:139` | a cast gives the byte | rule 2 makes Java's methods methods | **drop** (E6) |
| 9 | "`value` field and `valueOf(byte)` are not ported" | `ElectionType.cs:28` | a cast gives the value | the field: yes (N2); the method: no | **change**: add `ValueOf`, keep the field remark (E7) |
| 10 | "`ToString()` gives the Java constant name … a recorded deviation" on 7 enums | each enum's remarks; idiom map (`dotnet/CLAUDE.md:737`) | as row 5 | the fact holds | **keep**, add the pointer to `<Enum>.ToString(e)` (E10 B) |
| 11 | N2 fields group (`ElectionType#value`, `TimestampType#id`, `#name`) | allowlist N2; ledger 373 | Python's enum rule | yes | **keep**, add the 3 of E13 |
| 12 | `ConfigResourceType` flattened, kept that name | M15/P3 D16; ledger 262 (namespace) | `System.Type` clash | yes (`CS0104` measured) | **keep**; declare in Java's order (E9) |
| 13 | `UpgradeType` beside `FeatureUpdate` | M22/P4 PLAN L-5 (cited in `dotnet/CLAUDE.md` as "ledger 368", which does not match the local ledger) | CS0102 with `upgradeType()` | yes, rule 1 | **keep** |
| 14 | `GroupMembershipOperation` beside `CloseOptions` | ledger 109, 262 | CS0102 with the factory/getter | yes, rule 1 | **keep** |
| 15 | `AlterConfigOpType` flattened under a prefixed name | ledger 262; M15/P3 D18 naming | CS0102 with `opType()` | the flattening yes, the prefix no (rule 1) | **change** (E8) |
| 16 | `ConfigEntry.ConfigType` / `ConfigSource` stay nested | M15/P3 D16 | they can nest | yes | **keep** |
| 17 | `VALUES` is a `static readonly` field | ledger 386 (amended 2026-11-10) | Java's split; never a property | yes | **keep** (E12 A) |
| 18 | `SecurityProtocol : short` by id; id is the cast, name is `ToString()` | ledger 373 | Python's enum rule | yes | **keep**; basis of E13 |
| 19 | The 10-letter upper-case table | ledger 395 | Java's full `toUpperCase(Locale.ROOT)` | yes | **keep**; reuse in E3/E5 (X4) |
| 20 | M22/P2's enum baseline lines "review later" | ledger 444 | baseline now, sort later | — | **this phase reviews them** (E11–E13) |
| 21 | Undefined `UpgradeType` → `UNKNOWN` | ledger 197 | Python's way through `from_code` | yes | **keep**; generalized by E15 B |
| 22 | Undefined `ConfigResourceType` → `UNKNOWN`; ACL → `UNKNOWN` | M15/P13.2 G2-1; M15/P13.3 F8 | user's direction; the core normalizes keys | yes | **keep** (E15 B) |
| 23 | `MatchType()` public, .NET-only | idiom map ("ledger 369"); M22/P4 D-a2 | Java's three-state `Optional` | yes | **keep**, allowlist (E16) |
| 24 | Rule 9 itself | `dotnet/CLAUDE.md` §3 rule 9 (#328) | C# 14 extension blocks, `<Enum>Methods` | yes | **keep**; add two sentences (fields on the class; `toString()` as the static), §7 R3 |

---

## 6. Steps for the Actor (dependency order)

Every step: build, test, commit; fixups for Critic comments (`COMMENTS.125.md`). Java's tests first; Python's
where Java has none (`definition-of-done.md` §3: assert messages, Java's exact data).

**S0 — Rule text.** §7's drafts R1–R6 are approved (row 501). R1–R5 change `dotnet/CLAUDE.md`, which an
agent of this loop does not edit on a relayed instruction (as M23/P3 recorded), so the main session applies them
word for word; R6 (the allowlist comments) lands with the steps that move those lines. Until R1–R5 land, §7
governs this phase.

**S1 — Tooling** (Rust + analyzer; the pre-commit hook runs, `RUST_TEST_THREADS=6`).
- `rust/xtask/src/java.rs` `add_implicit_members`: add an implicit member unless an overload of the same name
  **and parameters** exists, so `java-api.txt` lists `ElectionType.valueOf(String)` beside `valueOf(byte)`.
  Regenerate `dotnet/analyzers/java-api.txt`; a unit test in `java.rs`.
- `JavaNameAnalyzer`: skip only the four built-ins by signature (`values()`, `valueOf(String)`, `name()`,
  `ordinal()`), so `valueOf(byte)` is required; accept an enum's Java field as a public `static readonly` field of
  its `<Enum>Methods` (E12 A); accept Java's enum `toString()` override as `public static string
  ToString(<Enum>)` in the companion's `extension(<Enum>)` block (E10 B). Analyzer unit tests for each, including
  a `ToString()` instance extension that must still be reported.
- Lists: `cargo xtask dotnet-java-name-baseline --add` for the one new finding
  (`missing … ElectionType#valueOf(byte)`), removed again in S5.
- Overlap: M23/P3 (other clone) edits `JavaNameAnalyzer.cs` in the same blocks (`:404-445`, `:634-657`), the two
  lists and `ConsumerRecord.cs`. If M23/P3 merges first, rebase onto it before S1; otherwise expect a small
  conflict there.

**S2 — Renames (E1, E2, E8).** `ScramMechanismMethods`, `UpgradeTypeMethods`, `SecurityProtocolMethods`,
`SslClientAuthMethods` (with `VALUES`), `GroupProtocolMethods`, `GroupStateMethods` (`GroupStatesForType(GroupType)`
static), `OpType` (+ file). Callers: `Internal/Interop/UserScramCredentialMarshal.cs:225`,
`grpc-server/TranslateAdmin.cs:1278`, `PublicConfigEnumsTests`, `PublicAdminP7Tests:560-562`,
`PublicNamespaceLayoutTests:243-336` (rows), `PublicJavaMemberShapeTests:186`, every `AlterConfigOpType` use.
Lists: the 7 `unmatched` lines, the 10 rename/move `missing` lines, `SslClientAuth#VALUES` (S1's analyzer now
finds the field on `SslClientAuthMethods`) and the `AlterConfigOpType` pair go.
Tests: the moved `PublicConfigEnumsTests` (Python's `test_config_classes`) under the new names; a theory that each
of the six classes is a top-level static class beside its enum, with no `<Enum>s` type left.

**S3 — The ACL family (E3, X1–X6).** 17 members; `AclEnumCodes.DefinedOrUnknown` reads `FromCode`.
Tests: Java's `AclOperationTest`, `AclPermissionTypeTest`, `ResourceTypeTest` (each `testIsUnknown`, `testCode`,
`testName`, `testExhaustive`, Java's tables, `fromCode((byte) 120)`, `fromString("something")`); Python's
`test_pattern_type` (no Java test); `test_none_dereference` rows (`FromString(null)` → `ArgumentNullException`
with `ParamName` `"str"`; `PatternType.FromString(null)` → `UNKNOWN`); `FromString("READ, WRITE")`, `("3")`,
`(" read")` → `UNKNOWN`; `FromString("deſcribe")` → `DESCRIBE`. 17 `missing` lines go.

**S4 — Id lookups (E4) and `ConfigResourceType`'s order (E9).** Tests: Java's `ConfigResourceTest`
(`shouldGetTypeFromId`, `shouldReturnUnknownForUnknownCode` with `unchecked((byte)-1)`, `0`, `1`,
`shouldRoundTripEveryType`); Python's `test_enums` (`OpType` part: `ForId(7)` null, `APPEND.Id()` 2) and
`test_election_type_and_isolation_level` (`IsolationLevel` part, "Unknown isolation level 7"). 6 lines go.

**S5 — `Type()`, `Code()`, `ValueOf(byte)` (E6, E7).** Tests: Java's `ScramMechanismTest.testFromMechanismName`
in full (`"UNKNOWN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`, `"some string"`, `"scram-sha-256"`), replacing the
partial `ScramMechanism_RoundTripsItsName`; Python's `test_from_mechanism_name`, `test_enums` (`from_type`,
`UpgradeType`) and the `ElectionType` part ("Value 5 must be one of [PREFERRED, UNCLEAN]"). The doc remarks of
§5 rows 8–9 change. The S1 line and 2 more go.

**S6 — Text (E5, E10, E11).** 5 lookups and, under E10 B, 7 static `ToString`s, each reading one table per enum
that the internal maps (`GroupMarshal.NameFrom*`, `TransactionMarshal.NameFromState`,
`ConsumerRecord.TimestampTypeText`) then call, so there is one copy; the gRPC server's
`TranslateAdmin.TransactionStateName` and its three siblings call the public form. Tests: Python's
`test_group_state_and_types`, `test_timestamp_type`, `test_enums` (`TransactionState` part), the
`test_none_dereference` rows (`GroupState`/`ClassicGroupState.Parse(null)` → `ArgumentNullException`); for every
constant of the seven, `Parse(ToString(c)) == c` (`ForName`, `ForConfig` likewise) and Java's exact text;
`GroupState.Parse("PREPARING_REBALANCE")` → `UNKNOWN`; `TimestampType.ForName(null)` → "Invalid timestamp type
null". Replace `PublicConfigEnumsTests.cs:189`'s pin with both texts. Doc remarks (§5 rows 7, 10). Lists: 5
`missing` lines, and the N1 group (or `SslClientAuth#toString()` into N1 under E10 A). Gate: also
`make -C dotnet test-grpc-server-dotnet`.

**S7 — Undefined values (E15 B).** `ElectLeaders` (`NativeAdminClient.Reassignments.cs:84`) and `ListOffsets`
(`NativeAdminClient.Partitions.cs:308`) throw through `ElectionType.ValueOf` / `IsolationLevel.ForId`;
`ListTransactionsOptions.FilterStates` with an undefined state throws `ArgumentOutOfRangeException` as the group
filters do. Update the tests that pin today's classes; add one per site; `ElectionType.cs` remarks.

**S8 — Pins and bookkeeping (E13, E14, E16).** One theory per Java-backed enum (22) pinning Java's constants and
values in Java's order, from Java's source (the page's "analyzer gap 4" becomes a test, not an analyzer feature).
Write the rule-7 note and the two enums' remarks from E14's measured table. Allowlist: the 3 lines into N2 with its comment updated; a T8 group with its 2 lines. STATUS entry.

---

## 7. Rule drafts for `dotnet/CLAUDE.md` (approved, row 501; applied by the main session)

- **R1** §2, the "one file per Java top-level class" bullet: drop "today's `ScramMechanisms`, … are baselined until
  renamed"; replace `AlterConfigOpType` (Java `AlterConfigOp.OpType`) with `OpType`.
- **R2** §3 idiom map, the constants row: "where Java's enum overrides `toString()` (`GroupState`,
  `ClassicGroupState`, `GroupType`, `TransactionState`, `TimestampType`, `IsolationLevel`, `SslClientAuth`) the
  enum's `ToString()` prints the constant, and Java's text is the static `<Enum>.ToString(value)` of its
  `<Enum>Methods` class, which the binding also prints and sends" (E10 B; under A, add `SslClientAuth` only).
- **R3** §3 rule 9, two sentences: "A Java field of the enum (`SslClientAuth.VALUES`) is a `static readonly` field
  of the `<Enum>Methods` class, outside the blocks, since a block cannot hold one (CS9282). Java's `toString()`
  override is a static `ToString(<Enum>)` in the `extension(<Enum>)` block: an instance extension named `ToString`
  is never called."
- **R4** §3 rule 7, after "a C# enum covers `name`, `ordinal`, `values` and `valueOf`": "`Enum.GetValues` and
  `CompareTo` order by value, not by Java's declaration (`ConfigResourceType`…), and `Enum.Parse` accepts numbers
  and comma lists; the binding's own lookups never use it."
- **R5** §3, the "Call-site checks" list, one bullet for E15 B's rule.
- **R6** The allowlist comments: N1 deleted (E10 B), N2 reworded (E13), T8 added (E16).

---

## 8. Ledger rows (Agreed 2026-11-11; rows 483–501 of `DOTNET-PORT-DECISIONS.md`)

| # | Decision | Lean |
|---|---|---|
| 483 | Logistics: M23/P4, N=125, branch `prashah_dev_dotnet_enum_methods_java_shape`, plan folder `dotnet/design/history/M23/P4-enum-methods-java-shape/` | as stated |
| 484 | X1: the new members are managed tables, not C ABI calls | managed |
| 485 | X2: Java `byte` → C# `byte` in the public API | `byte` |
| 486 | X3: Java's null answers; `string?` only where Java has a null branch | as stated |
| 487 | X4: case folding through `JavaText.ToUpperRoot` / `ToLowerInvariant` | as stated |
| 488 | E1: rename the five `<Enum>s` classes, no forwarders | A |
| 489 | E2: `GroupState.GroupStatesForType(GroupType)` | A |
| 490 | E3–E5: add the 28 lookups and getters of the ACL, id and text families | add |
| 491 | E6: `ScramMechanism.Type()`, `UpgradeType.Code()` | A |
| 492 | E7: `ElectionType.ValueOf(byte)`; the analyzer and `java-api` compare built-ins by signature | A |
| 493 | E8: `AlterConfigOpType` → `OpType` | A |
| 494 | E9: `ConfigResourceType` kept (re-affirms D16, ledger 262); declared in Java's order | A |
| 495 | E10: static `<Enum>.ToString(value)` for Java's `toString()` text; closes ledger 310's follow-up | B |
| 496 | E11: `SslClientAuth` as the six; amends ledger 390 | with the six |
| 497 | E12: `VALUES` a field of `SslClientAuthMethods`; the analyzer looks there | A |
| 498 | E13: the three Java fields into N2 (ledger 373 covers them) | A |
| 499 | E14: C# built-ins accepted and documented; a Java-order pin test per enum | A |
| 500 | E15: undefined values read through the enum's Java lookup, else `ArgumentOutOfRangeException`; E16: T8 allowlisted | B; A |
| 501 | §7: apply the rule drafts R1–R6 to `dotnet/CLAUDE.md` word for word in S0 | as drafted |

---

## 9. Gates, risks, and what is not verified

**Per commit** (macOS, Release): `dotnet build` of the library, tests, analyzers, gRPC server and soak, 0 warnings
/ 0 errors; `dotnet format --verify-no-changes`; UnitTests on net8.0 and net10.0 (`DOTNET_ROLL_FORWARD=Major` for
net8.0 if its runtime is absent, as M22/P4 did); the analyzer unit tests; from `rust/`, `cargo xtask java-api
--check ../dotnet/analyzers/java-api.txt`. S1 also `cargo test -p xtask`, `cargo xtask lint`, `cargo xtask
format-check` and the hook. S6 also `make -C dotnet test-grpc-server-dotnet`. **Close:** `RUST_TEST_THREADS=6 make
verify-dotnet-macos-docker`, once. **Done when** `cargo xtask dotnet-java-name-baseline` shows 54 lines removed
against master (the 53 enum lines and `MatchType`) and none added, and the allowlist holds N2 (6) and T8 (2), plus
N1 (7) only under E10 A.

**Brief the Actor on this machine's traps:** `grep` is ugrep (use `/usr/bin/grep`); a zsh argument starting with
`=` fails; a test filter that matches nothing passes; `cd` only inside `( … )`.

**Risks.** M23/P3's analyzer and list edits (S1). E10 B's two `ToString`s may confuse a reader; the remarks of each
enum must say which is which. E15 B changes two exception classes that a caller might catch by
`ArgumentOutOfRangeException`.

**Not verified.**
1. Java behaviour was read, not run (no JDK here). Python's was run.
2. The binding library was not built, and its tests not run, in this pass; the analyzer changes are untested.
3. Whether the repo's own analyzers (CA rules, `EnforceCodeStyleInBuild`) warn on a public static method named
   `ToString(GroupState)` (E10 B): the probe uses plain SDK defaults. S1/S6 builds will show it.
4. Which ledger `dotnet/CLAUDE.md`'s "ledger 365–371" cites refer to (§2, item 4). Not resolved; the local ledger's
   rows of those numbers are M22/P2's.
5. NuGet: checked only the public flat-container index (no 4.x); a private feed was not checked.
6. Reflection field order follows declaration in practice but is not guaranteed, so no test relies on it (S8 pins
   values by name).
