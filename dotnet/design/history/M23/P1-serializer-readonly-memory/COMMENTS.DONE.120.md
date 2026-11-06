# Critic 120 — M23/P1 (serializers return `ReadOnlyMemory<byte>?`)

## S2 — `a299e9e81` (tests U1–U7)

Checked and found sound: U1 (reflection on the open generic), U2 (empty cases new; the null side is
`SerdesTests.String_SerializeNull_ReturnsNull`, `ByteArray_SerializeNull_ReturnsNull` and
`Null_SerializeReturnsNull_DeserializeReturnsDefault`, each `Assert.Null` on the `ReadOnlyMemory<byte>?`), U3
(`SerdesTests.ByteArray_RoundTrips_AndDeserializeCopiesOut`: same array, offset 0, full length), U4 (array identity
and offset, manager identity and start), U5 (`data == base + offset` and `len`, both slices in both positions, read
back through the production `ProducerSendMarshal.BuildRecord`), the async half of U6 (exactly one `Pin` and one
`Unpin` per buffer when the first stage completes; `HandOver` releases before it completes the stage, so the assert
is not racy), U7 (large-minus-small marginal, best of 4, the existing 512 B budget), `NativeMemoryManager` (frees
once, honours `elementIndex`, disposed after the producer and after `pins.Release()` on every path), and the
`internal` widening in `ProducerRecordBuildTests`. Re-run on net10.0 (`--no-build`, a filter on the 16 names):
Passed 16, Total 16.

### 120-S2-1 — the rejected-send half of U6 cannot fail for a pin imbalance
- **Severity**: Low
- **Commit**: `a299e9e81`
- **File**: `dotnet/tests/Confluent.Kafka.UnitTests/Interop/ProducerSendPinLifetimeTests.cs:211-229`
  (`AsyncSend_RejectedBeforeTheHandOver_LeavesThePinsBalanced`)
- **Rule / plan**: PLAN §14 "Pins. `Pin()` / `Dispose` balanced on every path of the async send, including failures
  (U6)"; `ffi-marshalling.md` §A4 Tests required, "Pin/unpin balance on every path, including the failure paths".
- **What is wrong**: `ProducerSendMarshal.SendWithHandOver` calls `RejectAlteredStrings` (line 83) before
  `pins.Pin` (line 84), as its own comment says ("the string is rejected before anything is pinned"). So on this test
  `PinCount` and `UnpinCount` are 0 on every build, and `InRange(0, 1)` plus `Equal(PinCount, UnpinCount)` accept
  that. No single change to the rejection path's pin handling turns the test red. In a mutation tree (archive of
  `a299e9e81`, net10.0):
  - A: `_pins.Release()` removed from `AsyncDeliveryState.Abandon` (the only release on a failure after the pin; it
    would leak a pinned `GCHandle` and the `kafka_Bytes_t` block): green, 119/119 on
    `~ProducerSendPinLifetimeTests|~ProducerSend|~AsyncMockProducer`.
  - B: `RejectAlteredStrings` moved after `pins.Pin`: green, 10/10 on `~ProducerSendPinLifetimeTests` (Abandon
    releases, so 1/1).
  - A and B together (control): this test is the only red, 1 failed / 9 passed.

  So the test proves only the conjunction. The pinned failure paths §A4 lists (the P/Invoke throwing, a hand-over
  `cb` carrying an error, a `Send` racing `Close`) are reached by no S2 test.
- **Fix**: assert what the code promises, `Assert.Equal(0, key.PinCount)`, `Assert.Equal(0, key.UnpinCount)` and
  the same for `value` (mutant B then fails), and name the test for that (e.g.
  `AsyncSend_RejectedTopic_IsRefusedBeforeAnythingIsPinned`). For a pinned failure, drive the path that reaches
  `Abandon` after `pins.Pin`, for example `AsyncDeliveryState.Send` with a closed `SafeProducerHandle`, where the
  P/Invoke marshaller's `DangerousAddRef` throws after the pin, and assert one `Pin` and one `Unpin` per buffer. If
  that is not done, say in PLAN §7 (U6) that the pinned failure paths are not covered, instead of "balanced".
- **Resolution** (`57ccfcc7c`, a `fixup!` of `a299e9e81`): the rejected-topic test is renamed `AsyncSend_RejectedTopic_IsRefusedBeforeAnythingIsPinned` and asserts 0 `Pin` / 0 `Unpin` per buffer (red when `RejectAlteredStrings` is moved after `pins.Pin`); the new `AsyncSend_FailingAfterThePin_UnpinsEachBufferOnce` calls `AsyncDeliveryState.Send` with the closed handle of a disposed `NativeProducer.CreateMock` producer, so the marshaller throws `ObjectDisposedException` after the pin, and asserts one `Pin` and one `Unpin` per buffer (red when `_pins.Release()` is dropped from `Abandon`). A core-refused record was tried first: it cannot be driven, since `SetPartition` and the timestamp cross a negative value as absent.

### 120-S2-2 — `unsafe` in the unit-test project reverses decision M1/P1 D2 without a record
- **Severity**: Low
- **Commit**: `a299e9e81`
- **File**: `dotnet/tests/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj:22-23`,
  `dotnet/tests/Confluent.Kafka.UnitTests/NativeMemoryManager.cs:27`
- **Rule / plan**: `dotnet/CLAUDE.md` §4 ("records any deviation in the phase PLAN or COMMENTS.DONE");
  `dotnet/design/current/STATUS.md:3034-3035`, under "Decisions in force (M1/P1)": "**D2** — `<AllowUnsafeBlocks>` on
  the LIBRARY csproj only; test project stays unsafe-free"; `STATUS.md:2634` lists `Confluent.Kafka.UnitTests/` as
  "unsafe-free".
- **What is wrong**: the commit turns on `AllowUnsafeBlocks` in the unit-test project and adds an `unsafe` class. That
  is needed: a counting `MemoryManager<byte>` over `Marshal.AllocHGlobal`, which PLAN U5/U6 ask for, needs the
  `unsafe` `MemoryHandle(void*, …)` and `Span<byte>(void*, int)` constructors. But the PLAN never mentions it (no
  "unsafe" in the PLAN), no ledger row covers it, and the commit message says only that "the library's rule … is
  unchanged", which leaves out D2's test-project half. `STATUS.md` now states the opposite of the tree in two places.
  The commit message also says the integration tests will link `NativeMemoryManager.cs` (B1/B2), so that project
  would need the same switch.
- **Fix**: record the deviation in the PLAN (a line under §7, or D-row) or in `COMMENTS.DONE.120.md`: the unit-test
  project (and the integration project, if B1/B2 link the file) allows `unsafe`, confined to `NativeMemoryManager.cs`,
  because a native-memory `MemoryManager<byte>` cannot be written without it. Supersede D2 at `STATUS.md:3034-3035`
  and `:2634` when the phase closes.
- **Resolution**: no code change. Ruling 408 (Agreed 2026-11-07), "(a) Allow it": `AllowUnsafeBlocks` stays in the unit-test project, and in the integration-test project when it links the file, with `unsafe` confined to `NativeMemoryManager.cs`; the library rule is unchanged. `STATUS.md` records the amendment of M1/P1 D2 (`:3034-3035`, `:2634`) at S4.

## Review rounds (PM summary)

- S0 `56a945d5e`: no issues. S1 `09c8ffc70`: no issues (xUnit 2.9.3 binds `Assert.Equal(byte[], ReadOnlyMemory<byte>?)`
  to `Equal<T>(T, T)`, a reference compare; S1 turned all 11 such asserts into content compares).
- S2 `a299e9e81`: 120-S2-1 and 120-S2-2, above. Fixups `ba9954d6e` (R5, ledger 407) and `57ccfcc7c` (120-S2-1): no
  issues. S3 `c668782ba`: no issues.
- Found while fixing 120-S2-1, outside this phase: when the async send's P/Invoke throws before native runs, the
  `Callback_t` from `DeliveryState.Register()` is not destroyed; the public API cannot reach that path (ledger 409,
  Pending).

## Merge preview against M22/P2

`git merge-tree --write-tree prashah_dev_dotnet_config_constants c668782ba` (M22/P2 is not on master): one text
conflict, in `dotnet/CLAUDE.md`, one hunk over the §4 **Serializers** and **Config** rows (M22/P2 edits both; this
phase the Serializers row). Resolve by keeping M22/P2's two rows and re-applying R3 and R5 to the Serializers row.
`dotnet/.claude/rules/ffi-marshalling.md` and `ISerializer.cs` auto-merge. After the merge, five of M22/P2's test
serializers still return `byte[]?` and will not compile: `Internal/SerdeDisposalTests.cs` L175, L203, L289
(`=> null`: return type only) and `PublicSerializerDisposalTests.cs` L644, L674 (`=> data`: return type and the
`(ReadOnlyMemory<byte>?)null` form). `design/current/STATUS.md` will conflict too once both top entries exist: keep
both.

## S4 — `c6618fff5` (STATUS entry and review record, docs only)

Checked and found true: the shas and what each did; 16 tests in `a299e9e81` (8 `[Fact]` + 4 `[Theory]` x 2) plus 1 in
`57ccfcc7c` = 17, base 7032 (`base-test-net8b.log`) to 7049; IntegrationTests 8 to 12 (4 `[SkippableFact]`), slice
`AsMemory(3, 4)`, `NativeValueLength = 1024`, U7's `512 * 1024`; `fixed` over `.Span` in `ProducerSendMarshal`, `Pin()` in
`SendPins`, release in `Abandon` and the hand-over; the null forms in `StringSerde`, `ByteArraySerde`, `SoakRecord` and
the test serializers; no change under `rust`, `c`, `python`, no proto, header or `Internal/Interop/` change; every gate
number against `verify-macos.log`, `final-it-net8.0.log`, `final-it-net10.0.log`, `final-grpcunit.log`; ledger
400–409 dates and states; the D2 amendment at `STATUS.md:2646` and `:3048-3050` says what 408 says; the archive equals
the review (A 119/119, B 10/10, A+B 1/9 in `c120/`; 11 `Assert.Equal(byte[], …)` rewrites in S1); the merge preview
re-run at `c668782ba` (one conflict hunk, the Serializers and Config rows; this phase edits only the Serializers row);
the five M22/P2 serializers at `SerdeDisposalTests.cs` L175/L203/L289 and `PublicSerializerDisposalTests.cs`
L644/L674; two files in the commit, PLAN.md untouched, the `Co-Authored-By` trailer.
### 120-S4-1 — a third STATUS line still says `AllowUnsafeBlocks` is library-only
- **Severity**: Low
- **Commit**: `c6618fff5`
- **File**: `dotnet/design/current/STATUS.md:2580`
- **Rule / plan**: ledger 408 (M1/P1 D2 superseded for the test projects, "recorded in `STATUS.md` at S4");
  120-S2-2's fix.
- **What is wrong**: the structure tree's `Directory.Build.props` entry reads "— NO AllowUnsafeBlocks here
  (library-only, M1/P1 D2)". That the props file sets none is still true, but the reason it gives, that the switch is
  library-only, is D2's test-project half, which 408 reversed: the switch is now also in
  `Confluent.Kafka.UnitTests.csproj:23` and `Confluent.Kafka.IntegrationTests.csproj:17`. The commit amended the two
  lines 120-S2-2 named (`:2646`, `:3048-3050`) and left this one; 120-S2-2 should have named it too. (`:2589`, "M1/P1:
  + <AllowUnsafeBlocks> (library only)", describes the M1/P1 change to the library csproj and can stay.) Related: the
  120-S2-2 resolution in `COMMENTS.DONE.120.md:66` places the S4 amendment at "`:3034-3035`, `:2634`"; in the tree
  that contains it, the 12-line top entry moved them to `:3048-3050` and `:2646`, and `:3034-3035` / `:2634` are now
  unrelated lines.
- **Fix**: `— NO AllowUnsafeBlocks here (set per project: the library, M1/P1 D2; the two test projects, M23/P1,
  ledger 408)`. In the archive, cite `:2646` and `:3048-3050`, or say "the lines cited above, as of `a299e9e81`".
- **Resolution**: fixed in the fixup of `c6618fff5`: the `Directory.Build.props` line of the structure tree now says `AllowUnsafeBlocks` is set per project (the library, and the two test projects for `NativeMemoryManager.cs`; M1/P1 D2 as amended by ledger 408). 120-S2-2's line numbers are those of `a299e9e81`; after this fixup the D2 bullet and its amendment are `STATUS.md:3048-3052` and the unit-test tree line `:2648`.
### 120-S4-2 — "Each new test was shown to fail under a mutation" is not true of U1
- **Severity**: Low
- **Commit**: `c6618fff5`
- **File**: `dotnet/design/current/STATUS.md:17` (end of the **Gates** bullet)
- **Rule / plan**: accuracy of the close-out record.
- **What is wrong**: the phase's mutation runs are M1–M8 (`battery-1..3.log`), the fixup's `fx120-mutA/B.log` and
  `mut1/2.log`, and S3's `s3-m1/m1b/m2.log`. Together they turn red 20 of the 21 new tests. None turns red
  `SerdesTests.ISerializer_Serialize_ReturnsNullableReadOnlyMemory` (U1), and no log in the scratchpad shows it failing.
  A mutant for U1 must change the interface's return type and every implementer with it (in effect, revert S1), and
  none of the runs did that, so "each" overstates what was shown.
- **Fix**: either run that mutant (revert `09c8ffc70`'s return types, keep the test) and keep the sentence, or write
  "Each new test but U1, the reflection check, was shown to fail under a mutation of the code it guards."
- **Resolution**: fixed in the fixup of `c6618fff5`: "Each new test but U1 (a reflection check of the return type) was shown to fail under a mutation".
### 120-S4-3 — the STATUS merge preview leaves out the STATUS.md conflict this commit adds
- **Severity**: Low
- **Commit**: `c6618fff5`
- **File**: `dotnet/design/current/STATUS.md:19` (**Merging with M22/P2**)
- **Rule / plan**: PLAN §11 item 2 ("`dotnet/design/current/STATUS.md`: both add a top entry. Keep both.");
  `COMMENTS.DONE.120.md:86`.
- **What is wrong**: the bullet says "`git merge-tree` shows one text conflict" without naming the commit. That is true
  at `c668782ba`, but this entry is what makes a second one: `git merge-tree --write-tree --name-only
  prashah_dev_dotnet_config_constants c6618fff5` lists `dotnet/CLAUDE.md` and `dotnet/design/current/STATUS.md`, both
  `CONFLICT (content)`. The archive and PLAN §11 record the STATUS.md conflict; the STATUS entry, which is what a merger
  reads first, does not.
- **Fix**: "`git merge-tree` at `c668782ba` shows one text conflict, …; at the branch tip `STATUS.md` also conflicts,
  both top entries (keep both)."
- **Resolution**: fixed in the fixup of `c6618fff5`: the merge bullet now says `STATUS.md` conflicts once both top entries exist (keep both).
