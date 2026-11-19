# COMMENTS.DONE.108

## S3 — `182dc80f4`, fixed in `80f9b9bf8` and `4d30b103a`

### 108-S3-1 — Low — `182dc80f4` — `src/Internal/JavaText.cs:76-79` (`ValueOf`) and `:51-62` (`CoreText`, key not in the table)

**What is wrong.** Both functions print every `IEnumerable` as `[a, b]` and only a non-generic
`IDictionary` as `{k=v}`. Java's `String.valueOf` gives `[a, b]` only for a `java.util.Collection`
(`AbstractCollection.toString`) and `{k=v}` for a `Map`. Any other object, including an
`Iterable` that is not a `Collection`, prints its own `toString()`. Python's `java_str` draws the
same line: `Mapping` → `{k=v}`, and `(list, tuple, set, frozenset)` → `[a, b]`
(`python/confluent_kafka/_java.py:109`, `:112`); every other value is `str(value)`. The plan's
D16 table limits `[a, b]` to "a list, array or set; a dictionary" and sends "any other object"
to .NET's `ToString()` (PLAN.md:370, :373). D15 (PLAN.md:172) says the same for an unknown key's
text. The difference is not recorded. Probed against the built net8.0 assembly at HEAD:

- The binding's own `Headers` (`IEnumerable<Header>` only, `Common/Header/Headers.cs:27`)
  prints `[RecordHeader(key = a, value = [1])]`. Java's `RecordHeaders.toString`, its
  `ToString()`, and Python's `str(headers)` all print
  `RecordHeaders(headers = [RecordHeader(key = a, value = [1])], isReadOnly = false)`.
- A lazy sequence (`hosts.Select(…)`, a `yield` iterator) is enumerated to build the message
  (counted: 1 enumeration per `ValueOf`; `CoreText(lazy, null)` enumerates it too). That message
  comes from the refusal path that D3 itself prescribes for "a lazy `IEnumerable`" on a LIST key.
  An endless or one-shot sequence there either never returns or is consumed by an error message.
  Java and Python print the object without iterating it: a `Stream`'s `toString`,
  `<generator object …>`.
- A generic-only dictionary (`System.Dynamic.ExpandoObject`, a custom
  `IReadOnlyDictionary<,>`) prints `[[a, 1]]`. Java's `Map` and Python's `Mapping` print `{a=1}`.

**Fix.** Make the `[a, b]` arm match Java's `Collection`: a materialised collection
(`ICollection`, or a type implementing `ICollection<T>` / `IReadOnlyCollection<T>`; this covers
arrays, `List<T>`, `HashSet<T>`, `Queue<T>`). Make the `{k=v}` arm cover `IDictionary` plus the
generic `IDictionary<,>` / `IReadOnlyDictionary<,>`. Everything else goes to the invariant
`ToString()` in both `ValueOf` and `CoreText`'s untyped branch. Add a test each for `Headers`, a
lazy iterator (assert it was never enumerated) and `ExpandoObject`. Alternatively, if `[a, b]`
for a lazy sequence is wanted (it reads better in the D3 message), record it as a deviation in
the D16 row, with the enumeration side effect named.

Resolved in `80f9b9bf8`: `CollectionText` prints `{k=v}` for `IDictionary` and the generic `IDictionary<,>` / `IReadOnlyDictionary<,>`, `[a, b]` only for `ICollection` and the generic `ICollection<>` / `IReadOnlyCollection<>`, and every other object by its invariant `ToString()`, never enumerated, in both `ValueOf` and `CoreText`; 4 new `JavaTextTests` (Headers, a lazy `Select` and a throwing iterator, `ExpandoObject`, an `IReadOnlyDictionary<,>`-only and an `IReadOnlyCollection<>`-only type, `HashSet<int>`, `ReadOnlyCollection<string>`), mutation-checked.

### 108-S3-2 — Low — `80f9b9bf8` — `tests/Confluent.Kafka.UnitTests/Internal/JavaTextTests.cs:235` (`ValueOf_GenericCollections_AreJavasCollectionText`); arm at `src/Confluent.Kafka/Internal/JavaText.cs:104`

**What is wrong.** No test covers the new `ICollection<>` arm of `CollectionText`. Each collection
in the tests also reaches `[a, b]` by another arm. `HashSet<int>` and `ReadOnlyBag` implement
`IReadOnlyCollection<T>`, and `ReadOnlyCollection<string>` implements non-generic `ICollection`.
Only a type implementing `ICollection<T>` alone depends on this arm, such as a user's own class
written against `ICollection<string>` (`ICollection<T>` does not extend `IReadOnlyCollection<T>`).
For that type, D16's "a list, array or set → `[a, b]`" (PLAN.md:370) and Java's
`AbstractCollection.toString` both say `[a, b]`. I checked this in an archive tree of `80f9b9bf8`,
running net8.0 Release with the `JavaTextTests` filter:

- CTRL: 50/50 passed.
- M1, which reduces line 104 to `collection |= definition == typeof(IReadOnlyCollection<>);`:
  50/50 passed, so the mutant survives.
- M1 plus a probe test with an `ICollection<int>`-only type: the probe fails (49 + 1 passed,
  1 failed). The same probe passes on unmutated `80f9b9bf8` (51/51). So the arm is reachable and
  correct, but nothing guards it.
- For comparison, M2 (dropping only the `IDictionary<,>` arm) is killed by the `ExpandoObject`
  assertion.

The commit body names the arms it mutation-checked and leaves this one out.

**Fix.** Add an `ICollection<int>`-only helper type (no `IReadOnlyCollection<T>`, no `ICollection`)
to `ValueOf_GenericCollections_AreJavasCollectionText`. Assert `"[1, 2]"` from both `ValueOf` and
`CoreText(…, null)`.

Resolved in `4d30b103a`: `ValueOf_GenericCollections_AreJavasCollectionText` adds `MutableBag`, a test type that implements only `ICollection<int>` (no `IReadOnlyCollection<T>`, no non-generic `ICollection`, no `IList<T>`), and asserts `"[1, 2]"` from both `ValueOf` and `CoreText(value, null)`. Mutation-checked: with line 104 reduced to the `IReadOnlyCollection<>` test, `JavaTextTests` on net8.0 Release went 49 passed / 1 failed on the new `ValueOf` assertion; the source was restored byte for byte (`cmp`). No production change.

## S4 — `104ff29cb`, fixed in `5e1922b46`

### 108-S4-1 — Low — `104ff29cb` — `src/Confluent.Kafka/Internal/ClientConfigs.cs:161-162` (`IsList`)

**What is wrong.** `IsList` takes any non-generic `IList`. .NET 9 and later add
`System.Collections.Generic.OrderedDictionary<TKey, TValue>`, which implements both `IDictionary`
and `IList`. So a dictionary on a LIST key is accepted, and it is sent as its `KeyValuePair` texts
joined with `,`. I probed the built net10.0 Release assembly of `104ff29cb` with
`["bootstrap.servers"] = new OrderedDictionary<string, int> { ["a"] = 1, ["b"] = 2 }`:

- It is sent as `bootstrap.servers=[a, 1],[b, 2]`, with no error.
- The same value on an INT key prints `{a=1, b=2}` in the message.
- On an unknown key it is sent as `{a=1, b=2}`.

So `JavaText` reads it as a map everywhere else. The net8.0 asset gives the same answer when an app
runs it on .NET 9 or later.

- Java: the LIST arm of `parseType` takes only a `List` (`ConfigDef.java:768-777`). A `Map` gets
  "Expected a comma separated list."
- Python: `check_type` takes only `list` / `tuple` (`python/confluent_kafka/_config.py:125-127`). A
  `dict` gets the same message.
- .NET's own test `Lists_AListKeyRefusesASetADictionaryAndAnArrayThatIsNotAList`
  (`ClientConfigsTests.cs:772`) states the rule for `Dictionary<,>`. No test covers a type that is
  both a dictionary and a list.

**Fix.** Refuse an `IDictionary` before the `IList` test, for example
`value is IList and not IDictionary` in the non-array arm. Add the `OrderedDictionary<,>` case to
that test under `#if NET9_0_OR_GREATER`, expecting
"Invalid value {a=1, b=2} for configuration bootstrap.servers: Expected a comma separated list."

Resolved in `5e1922b46`: `IsList` refuses a dictionary before the `IList` test: a non-generic `IDictionary`, and an `IList` that implements the generic `IDictionary<,>` or `IReadOnlyDictionary<,>` (`IsGenericMap`, read from its interfaces; the value is never enumerated). The new test `Lists_AListKeyRefusesADictionaryThatIsAlsoAList` covers three test types (`ListMap`: `Hashtable` + `IList`; `PairList`: `Collection<KeyValuePair<string, int>>` + `IDictionary<,>`; `ReadOnlyPairList`: + `IReadOnlyDictionary<,>`), each "Invalid value {a=1} for configuration bootstrap.servers: Expected a comma separated list.", and under `NET9_0_OR_GREATER` `OrderedDictionary<string, int>` with "{a=1, b=2}". Mutation-checked: HEAD's `IsList` replayed verbatim fails it on both TFMs, and with only the `OrderedDictionary` assertion kept net10.0 fails "No exception was thrown"; removing the `IDictionary` arm, the generic scan, `IReadOnlyDictionary<,>` or `IDictionary<,>` each fails its own test type's assertion; changing the `OrderedDictionary` expected text fails net10.0 and passes net8.0. Sources restored byte for byte (`cmp`).

### 108-S4-2 — Low — `104ff29cb` — `tests/Confluent.Kafka.UnitTests/Internal/ClientConfigsTests.cs` (no generic type in any refusal); code at `src/Confluent.Kafka/Internal/ClientConfigs.cs:145` (`TypeName`)

**What is wrong.** D7 fixes the type text as `value.GetType().ToString()` (PLAN.md:399). F17
(PLAN.md:107) chose it over `FullName` because the two differ only for a generic type, where
`FullName` is assembly-qualified. Every type name the tests assert is non-generic, from
`System.Double` to the two nested test enums, and for those the two texts are equal.

I checked with a mutation in a `git archive` tree of `104ff29cb` (net8.0 Release, bin/obj wiped,
0 warnings / 0 errors, the `ClientConfigsTests` filter):

- CTRL: 1161/1161 passed.
- `TypeName` changed to `value.GetType().FullName!`: 1161/1161 passed.

So nothing pins F17's correction. A regression to `FullName` would print
"…but it was a System.Collections.Generic.List`1[[System.String, System.Private.CoreLib, Version=…]]"
and every test would still pass.

**Fix.** Add one refusal with a generic type. For example, `new List<string> { "a" }` on
`client.id` should give "Invalid value [a] for configuration client.id: Expected value to be a
string, but it was a System.Collections.Generic.List`1[System.String]".

Resolved in `5e1922b46`: the new test `JavaClasses_AGenericTypeIsNamedByItsToStringNotItsAssemblyQualifiedFullName` asserts `List<string>` on `client.id` ("Invalid value [a] for configuration client.id: Expected value to be a string, but it was a System.Collections.Generic.List`1[System.String]") and `Dictionary<string, int>` on `batch.size` ("Invalid value {a=1} for configuration batch.size: Expected value to be a 32-bit integer, but it was a System.Collections.Generic.Dictionary`2[System.String,System.Int32]"). Mutation-checked: `TypeName` switched to `value.GetType().FullName!` fails this test on net8.0 and net10.0 (1 failed / 1214 passed on the filter); restored byte for byte (`cmp`). No production change.

### Watch point (S4 review, "Verified, no item") — fixed in `28e29f71c` (a fixup of `182dc80f4`)

A user `IFormattable` whose `ToString(format, provider)` returns `null` reached the pairs as a `null` text (and `""` inside a LIST and in a message). Resolved in `28e29f71c`: `JavaText.InvariantText` maps a `null` result of any arm to "null", so `CoreText` and `ValueOf` never return `null`; new test `JavaTextTests.CoreText_AToStringThatReturnsNull_IsJavasNull`. The Prepare-level test `ClientConfigsTests.Text_AToStringThatReturnsNullIsJavasNull` is in `5e1922b46`, because `ClientConfigs.Prepare` arrives in S4. Mutation-checked: HEAD's `JavaText.cs` replayed verbatim fails both tests on both TFMs (pairs `["unknown.formattable"] = null`, `["bootstrap.servers"] = "null,"`); restored byte for byte (`cmp`).

## S5 — `4a0d22a9a`, fixed in `e87aa4672`

### 108-S5-1 — Low — `4a0d22a9a` — `tests/Interop/ProducerConfigMarshalTests.cs:141`, `tests/Interop/ConsumerConfigMarshalTests.cs:79`, `tests/PublicAdminTeardownTests.cs:254`

The three rewritten "null with no validator" tests are named `Create_NullValueWithNoValidator_LeavesTheKeyUnset`
(producer, consumer) and `RealClient_LeavesANullConfigValueWithNoValidatorUnset` (admin), but they
assert only that the client constructs. Their keys cannot tell "left out" from "sent as `""`". I
probed the Release DLL at HEAD (net8.0): `client.id = ""` constructs on the producer and the admin
client, and `client.rack = ""` constructs on the consumer. So a mutant that sends a `null` as `""`
passes all three, and D5's "any other `null` leaves the key out" (PLAN.md:164) is pinned only by
S4's helper tests. Plan :703 asks only that such a `null` "constructs", so the gap is in the names'
claim, not in plan coverage.
**Fix:** use a key whose `""` the core refuses while a `null` constructs. Probed at HEAD:
- consumer `group.instance.id`: `""` gives `ConfigException` "Invalid value  for configuration
  group.instance.id: String must be non-empty", and `null` constructs;
- producer `transactional.id`: `""` gives the same message for that key, and `null` constructs.

Either key turns the test into a witness for "unset". Alternatively, rename the three tests to say
what they show ("…Constructs"). The admin test can keep `client.id` under such a name.

Resolved in `e87aa4672`: the three tests keep their names and now use `sasl.login.connect.timeout.ms` on all three clients. Java defines that key INT with a `null` default and no validator, it has no entry in `ProducerNull`, `ConsumerNull` or `AdminNull`, and the core refuses its `""` at construction. Each test asserts that a `null` constructs (and disposes it), then that `""` on the same key throws `ConfigException`, code −10, "Invalid value  for configuration sasl.login.connect.timeout.ms: Not a number of type INT". The suggested `transactional.id` and `group.instance.id` were not used: Java gives both `ConfigDef.NonEmptyString` (`ProducerConfig` `TRANSACTIONAL_ID_CONFIG`, `ConsumerConfig` `GROUP_INSTANCE_ID_CONFIG`), so the names ("WithNoValidator") would have misstated them; the INT key also refuses a `null` sent as the text "null". Mutation-checked: `ClientConfigs.Prepare` made to add a `null` as `""` fails all three tests on net8.0 with that message; the source was restored byte for byte (`cmp`).

### 108-S5-2 — Low (record) — `4a0d22a9a` — commit body, gate 3

The body says the UnitTests counts are "Unchanged from the migrated suite (this commit adds no unit
test)". In fact the commit adds four tests: 4 `[Fact]` lines are added and none removed, and the
total goes from 6417 at `5e1922b46` (its body) to 6421 here. The four are F13's rewrite: +1
producer, +1 consumer, +2 admin. The standing rules require each count change to be explained
(gate 3). **Fix:** state 6417 → 6421 (+4, F13's tests) in the next fixup's body or in S8's
close-out record. History is not rewritten.

Resolved in `e87aa4672`: its body's paragraph "108-S5-2, correction to 4a0d22a9a's body" states that S5 adds four tests, 6417 → 6421 per TFM against `5e1922b46` (`ProducerConfigMarshalTests` 8 → 9, `ConsumerConfigMarshalTests` 4 → 5, `PublicAdminTeardownTests` 16 → 18, counted by `[Fact]`/`[Theory]` lines at both commits); the fixup itself adds none (6421 / 6419 / 2 / 0 on net8.0 and net10.0). History not rewritten.

## S7 — fixed in `58565dd8c`

### 108-S4-3 — Comment-only, deferred to S7 — `104ff29cb` — `src/Confluent.Kafka/Internal/ClientConfigs.cs:160`

"A multi-dimensional or non-zero-based array is not a list, as no Java array is a List." This
reason would refuse every array, but line 162 accepts a one-dimensional, zero-based array, as D3
rules (PLAN.md:166). The reason behind this decision (ledger 363, as the Actor reported it) is
different: such an array's `IList` indexer throws, for rank > 1 or for index 0 below the lower
bound, so it cannot be read by position. **Fix:** state that reason instead.

Resolved in `58565dd8c`: the comment on `IsList` now reads "Only a one-dimensional, zero-based array is a list: any other array's indexer throws, so it cannot be read by position." It states the decision and its reason, with no history.

### 108-S5-3 — Comment-only, deferred to S7 — `4a0d22a9a` — `src/Admin/KafkaAdminClient.cs:30`, `:38`; `src/Admin/AsyncKafkaAdminClient.cs:32`, `:41`

The class summaries and the constructor summaries of both admin clients still say the client is
built from "Java's `Admin.create(Properties)`". The parameter is now Java's `Map<String, Object>`
form, and DC §4 **Config** says "Java's `Map<String, Object>`, with no `Properties` form" (D1). The
producer and consumer summaries already name the `Map` overload.
**Fix:** say `Admin.create(Map)` in all four places.

Resolved in `58565dd8c`: the class and constructor summaries of `KafkaAdminClient` and `AsyncKafkaAdminClient` (four places) now say Java's `Admin.create(Map)`; `git grep 'Admin.create(Properties)' -- dotnet/src dotnet/tests` finds none.

### 108-S5-4 — Comment-only, deferred to S7 — `4a0d22a9a` — `tests/Interop/ProducerConfigMarshalTests.cs:27-31`, `tests/Interop/ConsumerConfigMarshalTests.cs:25-28`

This confirms the Actor's candidate. The class summaries still describe the S4 world: "a bad
argument throws a .NET exception before any native call". Both classes now also test the following:
- a `null` refused by its validator, which throws `ConfigException`, a `KafkaException`, before
  any native call;
- a `null` with no validator, which constructs.

**Fix:** describe the three cases: a core −10, a binding `ConfigException` before any native call,
and the `null` map's `ArgumentNullException`.

The other stale-doc candidates were checked and are not items:
- `soak/README.md:430` (row 8) is still true. Soak's parsed config is still
  `IReadOnlyDictionary<string, string>`. It reaches the clients unchanged through
  `SoakConfig.ClientConfigs`. Python's soak still has `stringify_config` for the stated reason
  (`python/soak/soakclient.py:581-587`).
- `design/current/STATUS.md:1139` is the M11/P1 historical record.
- `design/current/PLAN-M16-soak-client.md` is untracked (`git ls-files` does not know it).

Resolved in `58565dd8c`: both class summaries now list what the classes test: a valid config builds the client; a config the core rejects throws its `ConfigException` (code -10); a `null` its key's validator refuses throws `ConfigException` before any native call, and one with no validator leaves its key unset; a `null` map throws `ArgumentNullException`.

## S8 — `582cddd33`, fixed in `d9d5ebdbf`

### 108-S8-1 — Low (record) — `582cddd33` — `dotnet/design/current/STATUS.md:19` (Docker, run 1)
- **What is wrong.** The entry says of the two tests that stopped runs 1 and 2: "this phase touches
  neither test". That is false for run 1's test. S5 (`4a0d22a9a`) changed
  `tests/Confluent.Kafka.UnitTests/Interop/SafeProducerHandleTests.cs:184`: the `Config()` helper went from
  `Dictionary<string, string>` to `Dictionary<string, object?>`
  (`git diff 95aa08565 58565dd8c -- …/SafeProducerHandleTests.cs`). `CreateAndDisposeMany_NoLeakOrCrash`
  (`:93-101`) calls `NativeProducer.Create`, which S5 rewrote. It now runs `ClientConfigs.Prepare`,
  `ConfigMarshal.NewClient` and `MaxBlockMsOf(prepared)` (`src/Confluent.Kafka/Internal/NativeProducer.cs:93-110`).
  The Manager's evidence file has the same error ("Neither test file nor its path was touched").
  Run 2's test, `ProducerSubmitHandleRefTests`, is untouched; it uses `NativeProducer.CreateMock` (`:51`),
  which the phase did not change.
- **Why it matters.** This sentence is the record's reason for treating run 1's stop as unrelated to the
  phase.
- **Fix.** State what holds:
  - The phase changed only that test's `Config()` type and the creation step it calls.
  - The 30 s bound wraps `producer.Dispose` (`:99`), and the phase does not change the Dispose path.
    `NativeProducer.cs`'s diff covers only `Create`, the removed `NewKafkaProducer` and `MaxBlockMsOf`.
    For this config `max.block.ms` is unset, so it is 60000 before and after.
  - `ProducerSubmitHandleRefTests` is untouched.
  - The conclusion stands.

Resolved in `d9d5ebdbf`: the Docker paragraph now reads "The phase touches only run 1's test, and only its construction: S5 changed its `Config()` helper's type and rewrote `NativeProducer.Create` (`ClientConfigs.Prepare`, `ConfigMarshal`), while the 30 s bound wraps `Dispose`, which the phase does not change; run 2's file and its mock path are untouched." Checked: `git diff --stat 95aa08565..58565dd8c` changes 1 line of `SafeProducerHandleTests.cs` (the `Config()` type, `4a0d22a9a` only) and nothing in `ProducerSubmitHandleRefTests.cs`; `NativeProducer.cs`'s hunks cover only `Create`, the removed `NewKafkaProducer` and `MaxBlockMsOf`, with `CreateMock` and `Dispose` outside them; with `max.block.ms` unset `MaxBlockMsOf` gives `DefaultMaxBlockMs` (60000) before and after. That last detail stays out of the entry to keep its length.

### 108-S8-2 — Low (record) — `582cddd33` — `dotnet/design/current/STATUS.md:16` (Tests)
- **What is wrong.** "`cargo test -p xtask` 326 → 327" sits beside "UnitTests per TFM 5173 → 6506", which is
  phase start → end. So it reads as the phase adding one xtask test.
- **The facts.**
  - S1's body says "326 passed (3 new …)". `git diff 95aa08565..4511b2eb7 -- rust/xtask` adds 3 `#[test]`.
  - The fixup `eff44f30c` adds 1 ("327 passed (326 + 1)").
  - So the phase took xtask from 323 to 327.
- **Fix.** Write "`cargo test -p xtask` 323 → 327 (S1 +3, its fixup +1)".

Resolved in `d9d5ebdbf`: the Tests bullet reads "`cargo test -p xtask` 323 → 327 (S1 +3, its fixup +1)". Checked: `4511b2eb7`'s body says "326 passed (3 new …)" and `git diff 95aa08565..4511b2eb7 -- rust/xtask` adds 3 `#[test]`; `eff44f30c`'s body says "327 passed (326 + 1)" and adds 1.

### 108-S8-3 — Low (record) — `582cddd33` — `dotnet/design/current/STATUS.md:18` (Gates per commit); also `:16`
- **What is wrong (1): `28e29f71c`'s count.** "`28e29f71c` and `5e1922b46` 6417" gives `28e29f71c` a count
  its tree never had. Its body says the gates were "run once on the tree with this commit and the S4 fixup
  that follows". It adds 1 test to S4's 6413, so its own tree holds 6414 per TFM, and that tree was never
  built or tested alone.
- **What is wrong (2): "Every code commit".** "Every code commit: builds 0 warnings / 0 errors (solution,
  gRPC server and tests, soak and its tests, PerfV3, PerfV2, PerformanceTests) and `dotnet format` clean on
  the five targets" overstates five commits. Their bodies record fewer runs and call the other targets
  unchanged:
  - `4511b2eb7`: the solution build, and one `dotnet format` with no target named.
  - `eff44f30c`: the library and the solution.
  - `4d30b103a`: the solution build and solution format; "no other project changed", "the other four
    format targets did not change".
  - `e87aa4672`: the same as `4d30b103a`.
  - `28e29f71c`: not run on its own tree.
  The inference is sound (no file of the other projects changed), but the entry records it as a run.
- **Related attribution (`:16`).** "S3 … +52, then +4 in a fixup" and "S4 … then +4 in fixups" split the
  fixups' 8 tests 4 / 4. The S3 fixups added 5 (`80f9b9bf8` +4, `28e29f71c` +1, the latter a
  `JavaTextTests` case) and the S4 fixup `5e1922b46` added 3. The totals are right.
- **Fix.**
  - Give `28e29f71c` and `5e1922b46` one joint count ("`28e29f71c` + `5e1922b46`, gated together: 6417").
  - Say the eight-target build and five-target format ran on every code commit except S1, its fixup,
    `4d30b103a` and `e87aa4672`. Those four built the solution (S1's fixup also the library) and formatted
    it, with the other targets unchanged.
  - Optionally attribute the fixup tests as S3 +5, S4 +3.

Resolved in `d9d5ebdbf`:
- (1) The count row reads "`28e29f71c` + `5e1922b46`, gated together, 6417 (`28e29f71c`'s own tree, 6414, was never built or tested alone)".
- (2) The gate sentence credits the eight-target build and five-target format to "S0, S2–S7, `80f9b9bf8` and the pair `28e29f71c` + `5e1922b46`", whose bodies each record both, and adds: "S1 and `eff44f30c` built the solution (the fixup also the library) and ran one `dotnet format` with no target named; `4d30b103a` and `e87aa4672` built and formatted the solution only; none of the four touched a file of the other targets". S1 and its fixup change only `rust/xtask` and the library's `ConfigTypes.g.cs`; the other two only UnitTests files. The full Rust hook of S1 and its fixup stays in the next sentence.
- (3) The Tests bullet reads "+52, then +5 in fixups (`80f9b9bf8` +4, `28e29f71c` +1)" and "then +3 in its fixup (`5e1922b46`)", as those bodies state. Totals unchanged (5173 → 6506).
