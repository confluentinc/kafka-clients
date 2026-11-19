# Critic 119: drop the Java names (`design/current/PLAN-drop-java-names.md`), uncommitted working tree on `dev_dotnet-error-categorization`

Scope: `git diff HEAD` (on top of `bbc0a14ef`) plus the untracked
`tests/Confluent.Kafka.UnitTests/TimeoutExceptionNameTests.cs`. Reference: the plan, the C ABI
header and the Java public API.

## Checked, no finding

- **No silent rebinding in code.** At HEAD every bare `TimeoutException` in `src/`, `grpc-server/`,
  `soak/` and the test projects meant either `System.TimeoutException` or the alias that was
  imported. Each one is now spelled out (`System.TimeoutException`, `global::System.TimeoutException`,
  `Common.Errors.TimeoutException` or `Confluent.Kafka.TimeoutException`), and the meaning is
  unchanged. Examples: the gRPC `catch (System.TimeoutException)` at
  `grpc-server/ProducerServiceImpl.cs:114` and `AsyncProducerServiceImpl.cs:116`, and the soak
  test's alias, renamed to `FutureTimeoutException = Confluent.Kafka.TimeoutException`.
  `System` declares none of the other seven names, and `Common.Errors` declares none of them, so
  they cannot rebind.
- **Generator.** Under `rust/`, only `rust/xtask/src/error_hierarchy/csharp.rs` changed. `java.*`
  maps to `Confluent.Kafka` and the files go to the project root. `cargo xtask check-generated`
  passes and the xtask `error_hierarchy` tests pass (31). Nothing under `python/` or `c/` changed.
  The `Java/` folder is gone.
- **Layout test.** `NoPublicType_IsInTheRootNamespace` compares the sorted root-namespace exported
  names with exactly the eight JDK classes, so it fails on a missing class and on any extra one.
  `NamespaceOf` maps a JDK package to the root.
- **Scan test.** `TheScan_FlagsOnlyABareNameInCode` covers code, comments, crefs and strings. The
  stripper does not recognise an `@$"` verbatim string or a raw `"""` string, but neither appears
  in `src/Confluent.Kafka`, so this is theoretical.
- **Behaviour.** No code reflects on the old namespace (`"Confluent.Kafka.Java…"`). The
  `FullName` uses (`ValueText.TypeAndMessage`, `ManagedExceptionCarrier`) change only the message
  text, which the plan documents. gRPC `Translate` and the soak `ErrorCode` / `FailureOf` map by
  type, not by name.
- **Docs.** In `dotnet/CLAUDE.md` §2, the root-namespace exception, the parent table, the
  name-lookup and CS0104 notes match the plan. The three namespace mentions in `ffi-marshalling.md`
  are updated. There are no internal links, page IDs, milestones or dates in the new text.
- Filtered tests on net8.0 (`TimeoutExceptionNameTests`, `PublicNamespaceLayoutTests`,
  `ValueTextTests`): 919 / 919 pass.

## Findings

1. **`src/Confluent.Kafka/Internal/KafkaFutureImpl.cs:28` and `:208`: the docs still name the
   `Java` waits.** Severity: low (stale internal docs; this plan's requirement).
   - Line 28: "The `<c>Java</c>` waits are what the public futures call".
   - Line 208: "and so is the one of the `<c>Java</c>` async waits".
   - These members were renamed by this change: `JavaGet` / `JavaGetNow` / `JavaWait*` became
     `GetWrapped` / `GetNowWrapped` / `WaitWrapped*`. So the summaries now point at a name that
     does not exist.
   - The plan's goal is to remove the "Java" identifiers, and these are its leftovers.
   - Fix: write `<c>Wrapped</c>` waits, or cite the members, e.g.
     `<see cref="KafkaFutureImpl{T}.GetWrapped()"/>`.

2. **`design/current/STATUS.md:11`: the Phase 6 entry is half-renamed.** Severity: low.
   - The entry was edited in place. It now says `Confluent.Kafka.ExecutionException`, but it still
     says the message is "`JavaText.ToString` of it" and that the wrapping is in
     "`KafkaFutureImpl`'s new `Java*` waits".
   - Neither name exists after this change: they are now `ValueText.TypeAndMessage` and the
     `*Wrapped` waits.
   - The entry therefore names the new namespace next to the old method names. It is accurate
     neither for Phase 6 nor for now.
   - Plan §4 asks for STATUS.md to record the namespace. No entry of its own was added for this
     change.
   - Fix (either one):
     - keep the Phase 6 entry as it was written, and add a new top entry for this change: the
       namespace move, the renames and the scan test;
     - or finish the in-place rename (`ValueText.TypeAndMessage`, the `*Wrapped` waits).

3. **`tests/Confluent.Kafka.UnitTests/TestTimeout.cs:30` and `TestTimeoutTeardownTests.cs:25`:
   the crefs silently rebind.** Severity: low (docs only; it is the very hazard the plan
   describes).
   - Both files are in `namespace Confluent.Kafka.UnitTests` and both say
     `<see cref="TimeoutException"/>`.
   - At HEAD this resolved to `System.TimeoutException` (through `using System;`), which is what
     `TestTimeout` throws.
   - Now the enclosing `Confluent.Kafka` namespace wins, so it resolves to
     `Confluent.Kafka.TimeoutException`, a class the helper never throws. The code at the throw
     sites was correctly changed to `System.TimeoutException`. The crefs were missed, and the scan
     test does not look at crefs or test code.
   - Fix: `<see cref="System.TimeoutException"/>` in both.

## Rule-update suggestions

None. The new `dotnet/CLAUDE.md` §2 note on name lookup already states the hazard behind
finding 3. Extending it to say "crefs too" would make that explicit.

## Resolution

All three findings fixed: the two `KafkaFutureImpl` summaries name the wrapped waits; the Phase 6 entry of `STATUS.md` is restored as it was and this change has its own entry; the two test crefs read `System.TimeoutException`.
