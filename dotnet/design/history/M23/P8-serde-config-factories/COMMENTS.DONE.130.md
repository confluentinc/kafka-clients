# Critic 130 — M23/P8 — comments done

1. **major — the asynchronous `Create` configures the value serde with the caller's dictionary as it is *after*
   `Create` returned, read on another thread.**
   - Where: `373c8f843` / `776d3dd04`, `dotnet/src/Confluent.Kafka/Internal/ConfiguredSerdes.cs:247` (keeps the
     caller's `IReadOnlyDictionary` reference), `:448` `Snapshot()` (enumerates it), called at `:435` from
     `BuiltAwaited`, which runs for the value at `:312` only after `await BuiltAwaited(key…)` at `:300`.
   - What is wrong: when the key's `Configure` actually waits (the schema-registry case row 571 exists for), `Create`
     returns its `Task` to the caller, and the value serde's copy is taken later, on the thread that completed the
     key's `Configure`. So (a) a caller that keeps using its dictionary (sets `client.id` or `transactional.id` for the
     next client, a common way to build several clients from one map) gets a value serde configured with entries
     the core never saw and the key serde never saw; (b) the enumeration races with the caller's writes on a
     non-thread-safe `Dictionary`. When the enumerator detects the change it throws
     `InvalidOperationException` ("Collection was modified…") inside `BuiltAwaited`, so `Create` fails with a spurious
     "Failed to construct kafka producer". The sync clients are not affected (no await between the two snapshots).
   - Confirmed: scratch console app against the tip's built `Confluent.Kafka.dll` (outside the repo):
     `AsyncKafkaProducer<string,string>.Create(configs)` with a key `Configure` waiting on a gate; after the call
     returned (task not completed), `configs["client.id"] = "second"` and `configs["value.serializer.encoding"] =
     "UTF-16"`; then the gate was released. Output: `key Configure saw client.id = first`, `value Configure saw
     client.id = second, value.serializer.encoding present = True`. The core was built with the call-time
     configuration (`client.id=first`), because `Prepare`/`Validate` run synchronously.
   - Java: `ProducerConfig.appendSerializerToConfig` / `ConsumerConfig.appendDeserializerToConfig` copy the map at
     the call (`new HashMap<>(configs)`). `AbstractConfig` keeps that copy as `originals`, and
     `config.originals(singletonMap(CLIENT_ID_CONFIG, clientId))` (`AbstractConfig.originals(Map)`) hands each
     `configure` a copy of that one call-time snapshot. Key, value and client see the same configuration. Row 561
     ("a fresh copy … for each call … as Java's `originals()` copies") and the `<remarks>` of the four `Create`s
     ("a copy of `configs`") read as that. The plan has no row for a later read.
   - Expected: take the snapshot once, synchronously, in the `ConfiguredSerdes` constructor (it already reads the
     dictionary there for checks 2–3), and hand each `Configure` its own copy of that snapshot. That keeps row 561's
     "fresh copy per call", and `AssertACopyOf` still holds. A unit test should mutate the dictionary while the key's
     `Configure` is pending and assert what the value's `Configure` received.
   - **Done:** fixup `f757a87cd` (of `776d3dd04`). `ConfiguredSerdes<,>`'s constructor copies `configs` once, right after the null check; the missing-key check, the kind check, `ClientConfigs.Prepare`, the core and each `Configure` (its own copy of that copy) read it, on all four clients. Tests: `AsyncCreate_ReadsTheConfigurationOnce_AtTheCall_WhateverChangesWhileTheKeysConfigureIsPending` (both async clients) and `Create_ReadsTheConfigurationOnce_AtTheCall_WhateverAFunctionDoesToTheCallersDictionary` (all four); all six rows fail without the fix. The plan text (row 561, check table, Implementation, Tests) is in the plan fixup of `0c4543ac3`. Its plan text (decision 4, the checks table, row 561) is in the plan fixup `091ce567d` (of `0c4543ac3`).

2. **minor — on the consumers a *given* key deserializer is disposed when the value deserializer fails to build. Java's
   consumer leaves it to its owner, and row 562 says Java closes it.**
   - Where: `373c8f843`, `ConfiguredSerdes.cs:285-286` (`Build`: "Java's constructor holds the key serde by now,
     given or built, and closes it." → `Disposal(TakeKey(builtOnly: false), null)`), and `:318` (`BuildAwaited`).
     These are shared by the producer and the consumer factories. Locked in by
     `PublicClientCreateTests.cs:343` `Create_WhenTheValueFunctionFails_IsJavasWrappedFailure_AndDisposesTheKey_BuiltOrGiven`
     for the `KafkaConsumer` and `AsyncKafkaConsumer` rows ("given key Dispose").
   - Confirmed: by the test's assertions (passing, net8.0 and net10.0), and by the scratch app:
     `KafkaConsumer.Create(configs, givenKey, null)` with a throwing `value.deserializer` function prints
     `Failed to construct kafka consumer / no value; given key disposals = 1`.
   - Java: the producer matches the row. `KafkaProducer`'s constructor assigns `keySerializerPlugin` before building
     the value serializer, and its `catch` → `close(Duration.ofMillis(0), true)` closes it. The consumer differs.
     `AsyncKafkaConsumer`'s constructor builds both deserializers inside one call, `this.deserializers = new
     Deserializers<>(config, keyDeserializer, valueDeserializer, metrics)`. When the value's
     `getConfiguredInstance`/`configure` throws inside `Deserializers`' constructor, `this.deserializers` stays null.
     The `catch` → `close(Duration.ZERO, …)` → `closeQuietly(deserializers, "consumer deserializers", …)` then closes
     nothing: a given key deserializer is not closed, and a built one is leaked. Disposing a *built* key is a
     defensible improvement (row 562's "the user holds no reference"). Disposing a *given* one disposes a
     caller-owned object Java leaves alone; a caller that retries `Create` with it gets a disposed deserializer.
   - Ledger and plan text to correct either way: row 562 (`PLAN.md:448`) and the disposal table (`PLAN.md:227`, "as
     Java's constructor holds the key by then and closes it") describe only the producer. The row's "Java closes
     less in one case" is incomplete for the consumer. The table header (`PLAN.md:220`, "key then value") is not the
     order on a value failure: the failed value instance is disposed first, then the key (`Create_WhenConfigureThrows…`
     asserts `"failing value Dispose", "key Dispose"`).
   - Expected: either leave a given key deserializer alone on a value failure on the two consumers (still disposing a
     built one), or keep the uniform behaviour and record it in row 562 as a deviation from Java's consumer.
   - **Done:** fixup `2adc07659` (of `776d3dd04`, with the sync half that `373c8f843` introduced): `ConfiguredSerdes` takes `holdsKeyBeforeValue` (true for the producers, false for the consumers); a value failure disposes a given key only on a producer, a built key on all four. Tests `Create_WhenTheValueFunctionFails_IsJavasWrappedFailure_DisposesABuiltKey_AndAGivenOneOnlyOnAProducer` and `Create_WhenTheValuesConfigureThrows_DisposesTheValue_AndAGivenKeyOnlyOnAProducer` (sync and async flavors). The Critic's reading of `KafkaProducer` and `AsyncKafkaConsumer` / `Deserializers` was checked against `kafka/` and holds. Plan text (disposal table, row 562 as Proposed, R3) is in the plan fixup `091ce567d` (of `0c4543ac3`).

3. **minor — rule drafts: one stale sketch and one stale sentence have no draft, and R3 omits a disposal case.**
   - `dotnet/CLAUDE.md:607-609`, the §3 sketch of the synchronous `KafkaConsumer`, still shows
     `public KafkaConsumer(IReadOnlyDictionary<string, object?> configs, IDeserializer<TKey> keyDeserializer,
     IDeserializer<TValue> valueDeserializer);  // 3-param (decision A)`. S5 removed that constructor. R8
     (`PLAN.md:598`) covers only `:330-334` (`AsyncKafkaProducer`) and `:532-537` (`AsyncKafkaConsumer`).
     Confirmed: `grep -n 'public KafkaConsumer(' dotnet/CLAUDE.md` → line 608; there is no `KafkaProducer` sketch.
   - `dotnet/CLAUDE.md:960`: "`ConfigException` … the client constructors throw it for a configuration value the
     binding refuses". After S5 the four clients' `Create` throws it, and none of them has a public constructor.
   - R3 (`PLAN.md:535`) lists when a given serde is disposed: at close, and on the core's wrapped failure. It omits
     row 562's case where a given key is disposed when the value's function or `Configure` fails. Whatever finding 2
     settles should appear there.
   - **Done:** fixup `091ce567d` (of `0c4543ac3`): drafts R13 (`dotnet/CLAUDE.md:607-609`) and R14 (`:960`), R3 extended with the per-client disposal of a given key on a value failure, and the list of rule-file mentions of a constructor that were checked and stay (`dotnet/CLAUDE.md:770`, `:1703-1704`, `ffi-marshalling.md:102,105,484`, the root files). No rule file was edited. The Critic's four open questions are the plan's open questions 2 and 8 to 10.

## Round 2 (`0c4543ac3..091ce567d`)

The three round-1 findings are fixed. `f757a87cd` fixes finding 1 on all four clients, `2adc07659` fixes finding 2
on both paths, and `091ce567d` adds R13 and R14 and extends R3. One minor bookkeeping item is open.

### Findings

1. **minor — plan bookkeeping: the gate counts do not include the fixups' tests.**
   - Where: `091ce567d`, `dotnet/design/history/M23/P8-serde-config-factories/PLAN.md:425-427` ("Gates per step":
     S4 9,622, S5 and S6 9,628 unit tests per framework).
   - What is wrong: `f757a87cd` and `2adc07659` add 10 test cases, and the plan records them nowhere:
     - 4 + 2 rows of the two `…ReadsTheConfigurationOnce…` tests;
     - 4 rows of `Create_WhenTheValuesConfigureThrows…`.
     Both fixups target S4 (`fixup! … (M23/P8 S4)`), so after the autosquash the S4–S6 rows are 10 low. The fixups'
     own gates (build warnings, full suites, format) are not recorded either.
   - Confirmed:
     - `PublicClientCreateTests` runs 102 cases on net8.0 and on net10.0, against 92 at `0c4543ac3`.
     - `git show --stat` shows that the two fixups change no other test file.
   - Expected: a gates row for the fixups, or S4–S6 corrected when they are squashed.

### Owner questions

No new question. The round-1 questions are now the plan's open questions 2, 8, 9 and 10. Two facts found this
round bear on them:

- Question 2 (row 581, whether a token already cancelled should fault the task instead of throwing):
  - The binding already throws a cancelled token's `OperationCanceledException` from the call elsewhere:
    `NativeProducer.SendViaPump` (`NativeProducer.cs:328`), which `AsyncKafkaProducer.Send` returns directly when both serdes are built-in ones (`AsyncKafkaProducer.cs:269`), and
    `SendOffsetsToTransactionWithCallback` (`NativeProducer.Transactions.cs:112`). The **Cancellation** row
    (`dotnet/CLAUDE.md:953`) states the same for `Send`.
  - `Create`'s check 3a follows that convention, so changing it is a decision for the whole binding, not only for
    `Create`.
- Question 10 (`ConsumerConfig.appendDeserializerToConfig`): `dotnet/CLAUDE.md:770` already rules that the config
  classes' Java methods are not translated (M22/P2), so leaving the method out is the current rule.

### Closed at close-out

Round 2's finding 1 is closed by the Manager in the close-out commit: the plan's "Gates per step" table has a
row for the three fixups with their gates (unit tests 9,638 / 51 / 0 per framework, integration tests 31,
build with no warning, format clean), and says that the S4–S6 counts are as run before the fixups.
No third review round was run for that one table row.
