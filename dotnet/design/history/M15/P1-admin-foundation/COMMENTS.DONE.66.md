# COMMENTS.DONE.66 — M15/P1 (admin foundation + the per-key bridge)

Closed by dotnet-actor 66 against `bindings/dotnet/COMMENTS.66.md` (dotnet-critic 66's
review of `b35fb304`, `5f898c72`, `78548041`).

**Local working file — never `git add` this.** The tracked record is the Manager's
archived copy under `design/history/M15/P1-admin-foundation/`
(`bindings/dotnet/CLAUDE.md §8.4`).

---

## DoD §10 / §11 — the statements the rules require (Finding 5)

**DoD #10 (hot-path allocation audit): N/A for the whole of M15/P1.**
`.claude/rules/admin-client.md` §10 makes this a *stated* N/A, never a silent skip.
Admin RPCs are batch/administrative — `createTopics` is one call per *request*, not per
record — so there is no per-message hot path to audit and no per-record allocation to
budget. Concretely: the only per-item work in this phase is one `NewTopic_t` per
requested topic on the way in and one `TaskCompletionSource` + one marshalled
`TopicMetadataAndConfig` per key on the way out, all of them per-RPC-key, none of them
per-record. Nothing in the phase touches the producer send path or the consumer receive
path, where the budget tests live.

**DoD #11 (consumer trait surface check): N/A, but its spirit is verified.**
`admin-client.md` §10 asks for the spirit even where the letter does not apply:

  - every RPC method is a plain **sync** `fn` returning a `*Result` — `CreateTopics`
    returns `CreateTopicsResult` immediately, exactly as Java's `createTopics` returns
    without blocking (§1);
  - **only** `Close(TimeSpan)` returns a `Task`, because Java's `close(Duration)` joins
    the background thread;
  - no `async` reached the marshallers, the callbacks or `KeyedResultMarshal` — the
    completion trampolines are plain sync methods running on whichever thread the ABI
    fired them on;
  - no `block_on`-style managed sync-over-async anywhere.

---

## Finding 1 [Medium] — `CreateTopicsResult.Values` published the private map · FIXED

**The Critic is right and the phase PLAN's sketch was wrong.** Java holds a *private*
`Map<String, KafkaFuture<TopicMetadataAndConfig>>` (`CreateTopicsResult.java:33`) and
publishes `Map<String, KafkaFuture<Void>> values()` (`:43-48`), deliberately erasing the
metadata with `thenApply(v -> null)`; the metadata is reachable **only** through the four
typed accessors. Verified against the Java source directly rather than the PLAN.

`Values` is now `IReadOnlyDictionary<string, Task>`, projected in the constructor from a
private typed map that is never handed out. `All()` still uses the typed map internally.

**Recorded at the site:** the erasure is a reference upcast — each entry in the published
view is the *same* `Task` instance as the private typed map's, so the view adds no per-key
allocation and introduces no second `Task` whose fault could go unobserved.

⚠ **Superseded rationale.** This entry originally justified the upcast by claiming a
derived continuation "would nest [the exception] one level deeper than the four typed
accessors report". That claim is **false and measured false** — see round 2, finding 7.
The decision it defended is unchanged and endorsed; only the reason was wrong.

**Public signature change** (breaking; pre-publish, which is why the checkpoint exists):
`IReadOnlyDictionary<string, Task<TopicMetadataAndConfig>> Values`
→ `IReadOnlyDictionary<string, Task> Values`.

## Finding 2 [Medium] — `replicationFactor` typed `short` on the result side · FIXED

Java's **result** side is `int` throughout: `KafkaFuture<Integer> replicationFactor(String)`
(`:104`), `int replicationFactor()` (`:141`), ctor `(Uuid, int, int, Config)` (`:115`),
field `private final int replicationFactor` (`:112`). The ABI agrees (`int32_t`,
`confluent_kafka.h:2358`). `short` is the **request** side only
(`NewTopic.replicationFactor()`), and `NewTopic` correctly keeps it.

All three public signatures are now `int`, and the unchecked narrowing cast in
`TopicMetadataAndConfigMarshal` is gone — **along with the comment that justified it**,
which asserted a Java contract that does not exist ("the ABI widens Java's `short`"). The
replacement comment states the actual relation and names the request side that does use
`short`.

**Public signature changes** (breaking; pre-publish):
`Task<short> ReplicationFactor(string)` → `Task<int>`;
`short TopicMetadataAndConfig.ReplicationFactor()` → `int`;
ctor `(Uuid, int, short, Config)` → `(Uuid, int, int, Config)`.

## Finding 3 [Low] — `ConfigEntry`'s public 5-arg constructor · FIXED

Java's `ConfigEntry` has exactly two public constructors and **neither takes
`isDefault`** — it is derived (`return source == ConfigSource.DEFAULT_CONFIG`,
`ConfigEntry.java:102-104`). The flag-taking constructor is now `internal`; its only
caller was always the same-assembly result marshaller.

The "purely additive" remark is corrected rather than deleted: carrying `IsDefault` as a
stored **property** is *forced* by the flattened ABI accessor
(`kafka_admin_TopicMetadataAndConfig_config_is_default`, which arrives with no source
beside it), but publishing a **constructor** for it is not — it would let a caller build
an entry whose `IsDefault` contradicts the `Source` a later phase adds, a state Java
cannot represent. The remark now says so and names what happens when `Source` lands.

## Finding 4 [Low] — `Uuid.Parse` accepted `+` and `/` · FIXED, plus one more divergence found

The alphabet screen is in: `Parse`/`TryParse` now reject a literal `+` or `/` before
decoding, matching `Base64.getUrlDecoder()`. Without it a non-canonical id parses and
then **prints a different string** than the text it was parsed from, since `ToString()`
always emits the URL-safe alphabet.

**Additionally — a second divergence in the same method, not in the Critic's finding.**
While making the xmldoc's parity claim honest I checked it against `Uuid.java:131` and
found the length gate was wrong too: Java rejects at `length() > 24`, this binding at
`> 22`. The two extra characters are not slack — they are Java's tolerance for the
**padded** 24-character form, which `Base64.getUrlDecoder()` decodes to exactly 16 bytes,
so `Uuid.fromString("AAAAAAAAAAEAAAAAAAAAAg==")` **succeeds in Java** and threw here.
The gate is now Java's 24, which fixes three things at once: the padded form parses; a
23- or 24-character input reports the decoded byte count (Java's message) instead of
"too long"; and the "too long" message quotes the first 24 characters, as Java does.

So the xmldoc is **corrected, not softened**: the two length conditions now genuinely
carry Java's messages, and the third (invalid base64) is stated honestly as carrying this
binding's own message — with the reason, that Java surfaces the JDK decoder's text, which
is a decoder implementation detail rather than part of Kafka's contract.

## Finding 5 [Low] — the DoD §10 statement was never written · FIXED

Stated above, and in the fixup commit message so it lives in a committed artifact.

## Manager's carried correction — the un-verifiable header citation · FIXED

The sentence *"This is plain bad input, not only a programming error…"* is **not** in
`target/include/confluent_kafka.h`; it is `src/ffi/admin.rs:56-63`, a `//!` module doc
cbindgen does not emit. Verified both ways. For P1's two entry points
(`create_topics_async`, `close_async`) the header documents exactly one inline-callback
trigger: **a NULL `admin` handle**.

Three code sites attributed the wider claim to the header and are corrected:
`AdminCallbacks.cs`, `AdminOperation.cs`, and the `CallbackOnTheSubmittingThread_…` test
docstring. The **defensive implementation is unchanged** — `AdminCallbacks` is the
family-wide class and the wider trigger set is real for the entry points P2 will declare;
the remark now says that explicitly, and says why being ready early costs nothing, rather
than mis-citing the header to justify it.

---

## Verification

  - `cargo build --features ffi` — clean; header SHA-256 **unchanged**:
    `45912ea9ec15b2ecd2b156076c5c26fa836d63bd5e3ba9efd7448ae1afe85105`.
  - **Mode A holds**: `git diff f24add9e -- src/ cbindgen.toml target/include/confluent_kafka.h generator/`
    is empty (0 lines).
  - `dotnet build -c Release --no-incremental` — **0 warnings / 0 errors**, 6 TFM outputs
    (netstandard2.0 · net8.0 · net10.0 for the library; net462 · net8.0 · net10.0 for the
    tests).
  - `dotnet test -f net10.0` → **889 passed / 0 failed**; `-f net8.0` → **889 passed / 0
    failed** (883 before; +6). No `Test Run Aborted` in either.
  - `dotnet format --verify-no-changes` clean; `cargo xtask format-check` and
    `cargo xtask lint` clean from the repo root.

### Sensitivity — every new test was proven by re-injection

| Injected defect | Test that turned red |
|---|---|
| `Values` widened back to `IReadOnlyDictionary<string, Task<TopicMetadataAndConfig>>` | `CreateTopicsResult_Values_ErasesTheMetadataLikeJava` |
| the `+`/`/` screen deleted from `DecodeUrlSafeBase64` | `Parse_RejectsTheStandardBase64Alphabet` |
| `ConfigEntry`'s 5-arg ctor made `public` again | `ConfigEntry_PublishesOnlyTheConstructorJavaHas` |
| result-side `replicationFactor` narrowed back to `short` (3 files) | `ReplicationFactor_IsIntOnTheResultSide_AndShortOnTheRequestSide` |

The four shape tests are **reflection** assertions on purpose. A widened signature still
compiles and still passes every behavioural test — the compiler accepts
`Task<TopicMetadataAndConfig>` where `Task` was meant, and `int` where `short` was — so
nothing else in the suite can go red when the shape drifts. All three of P1's shape
defects were of exactly that kind.

---

## Observation for the Manager — two pre-existing **consumer** test flakes

Not mine, not in this diff, and I am not fixing them here (consumer files, outside
M15/P1). Recording them because they can fabricate a false FAIL on any future admin
round, and one of them is the *same defect class* the Critic credited me with fixing on
the admin side.

  - `ConsumerPollBridgeTests.ResultBridge_RunsContinuationsAsynchronously_OffTheCompletingThread`
    (`:76`) asserts `completingThreadId != continuationThreadId`. It failed once with
    both equal to 30: the dedicated completer thread had exited and the pool handed the
    continuation a thread with the **recycled managed id**. This is precisely the
    thread-id-comparison trap the admin inline-continuation probe hit during this phase,
    and it was fixed there by switching to a thread-static flag. The same fix applies
    here — the test also asserts `IsThreadPoolThread`, which is the sound half.
  - `ConsumerRebalanceListenerBridgeTests.DisposeAsync_WithLiveRegistration_ReturnsWithoutHanging`
    (`:330`) failed once, timing-dependent.

`ConsumerRebalanceListenerBridgeTests.cs` was last touched by `b25bf7f0` (M9/P6);
`ConsumerPollBridgeTests.cs` by **`26761aa4`** (M6 serde foundation / typed consumers) —
this entry originally attributed both to `b25bf7f0`, corrected per the Critic's round-2
observation. Both predate M15 either way. Neither reproduced in 5 consecutive full
net10.0 runs afterwards (5/5 green, 889/889), and my diff touches no consumer file.
Suggest a small consumer-side follow-up phase.

---

# ROUND 2 — closing the two Low findings on the fixup `3b4f6c79`

Both propagate verbatim into P2…P8, which is why they are closed here rather than carried.
Fixed in a second fixup; neither touches Rust, Mode A unchanged.

## Finding 6 [Low] — `Uuid.Parse`/`TryParse` accepted malformed padding Java rejects · FIXED

**The Critic is right, and I verified the Java half independently rather than taking the
reading on trust.** There is no JDK on this machine (`/usr/libexec/java_home -V` →
*"Unable to locate a Java Runtime"*; `/usr/bin/java` is the macOS stub, and no `src.zip`
exists anywhere on the box), so the Java side is a **source** argument, not a run. It is
however a two-step argument in which only the second step is unverifiable by execution:

1. **Measured in-repo.** `Uuid.fromString`
   (`kafka/clients/src/main/java/org/apache/kafka/common/Uuid.java:130-144`) does exactly
   two things before the 16-byte check: the `length() > 24` gate, then
   `Base64.getUrlDecoder().decode(str)`. There is no Kafka-side padding tolerance to
   appeal to — whatever the JDK decoder does *is* Kafka's contract here.
2. **From the JDK source.** `java.util.Base64.Decoder.decode0` handles a padding byte with

   ```java
   if (b == -2) {                      // padding byte '='
       // =     shiftto==18 unnecessary padding
       // x=    shiftto==12 a dangling single x
       // xx=   shiftto==6&&sp==sl missing last =
       // xx=y  shiftto==6 last is not =
       if (shiftto == 6 && (sp == sl || src[sp++] != '=') || shiftto == 18) {
           throw new IllegalArgumentException(
               "Input byte array has wrong 4-byte ending unit");
       }
       break;
   }
   ```

   `shiftto` starts at 18 and drops by 6 per accepted data character, resetting to 18
   after each complete 4-character group — so `shiftto == 6` means *exactly two* data
   characters have been consumed in the current group. For `"AAAAAAAAAAEAAAAAAAAAAg="`
   (22 data characters = 5 whole groups + 2, then a single `'='` as the final byte):
   `shiftto == 6`, and `sp == sl` after the `sp++` that consumed the `'='`, so the first
   disjunct holds and it **throws**. The Critic's reading of `decode0` is correct, and I
   am not disputing the finding.

**Measured on our side, before the fix** (real `Confluent.Kafka.dll`, net10.0):
`Uuid.Parse("AAAAAAAAAAEAAAAAAAAAAg=")` → `Uuid(1, 2)`, whose `ToString()` is
`"AAAAAAAAAAEAAAAAAAAAAg"` — a *different* string, exactly finding 4's stated failure
mode. The 24 gate was not the cause; the unconditional re-pad was.

**Fix (the Critic's first option — two lines).** `DecodeUrlSafeBase64` gains a second
screen alongside the alphabet one: text that carries **any** `'='` of its own must already
be a whole number of 4-character units. Only unpadded text gets padding synthesized, so
`ToStandardBase64` can no longer *complete* a malformed terminal unit. Measured after:
that input now throws ``Input string `…` is not a valid base64 UUID``, and both
`Parse("…Ag")` (22, unpadded) and `Parse("…Ag==")` (24, padded) still parse.

**xmldoc, per the Manager's instruction to re-check it.** `DecodeUrlSafeBase64`'s summary
claimed it *"decodes the URL-safe base64 text the way Java's `Base64.getUrlDecoder()`
does, throwing `FormatException` on anything it would reject."* That is **still false
after the fix**, and measurably so: `Convert.FromBase64String` silently ignores embedded
whitespace (measured — `"AAAAAAAAAAEAAAAAAAAA    "` → 15 bytes, no throw; a leading space
and a trailing newline are likewise ignored), where Java's URL decoder rejects it as an
illegal character. So the blanket parity claim is **narrowed to what the code
guarantees**: the summary now says what the method does (two screens, then translate and
decode), each screen carries its own specific Java citation, and a final paragraph states
plainly that this is **not** a stand-in for Java's decoder, names whitespace as the
residue, and records why `Parse` is nevertheless unaffected — whitespace only *shortens*
the decodable text, so such an input is rejected or decodes to fewer than 16 bytes, and
reaching 16 bytes would take ≥ 25 characters, past the length gate.

`Parse`'s own exception list said *"Three conditions do that"*; the count is dropped
(a count is exactly the kind of clause that goes stale) and the padding case is folded
into the existing "not valid URL-safe base64" bullet with its Java citation.

**Recorded so it is not re-opened: one accepted input still prints a different string, and
that one is Java-faithful.** `Parse("AAAAAAAAAAEAAAAAAAAAAh")` → `Uuid(1, 2)`, printing
`"…Ag"`. The surplus low bits of the terminal character are discarded by
`Convert.FromBase64String` (measured) **and** by `decode0`, which at `shiftto == 6` writes
only `(byte)(bits >> 16)`. So "prints a different string" is a *symptom* of the real rule
(match what Java accepts), not the rule itself — this case is a match, not a defect.

**Test:** `PublicUuidTests.Parse_RejectsPaddingJavaRejects`, built by deleting one `'='`
from a form `Parse` accepts, so the padding shape is provably the only thing wrong with
it; it also asserts the narrowness of the screen (unpadded text still gets padded).

## Finding 7 [Low] — the `Values` xmldoc rationale was measurably false · FIXED (doc only)

**No dispute — I reproduced the Critic's measurement myself** on net10.0 before touching
anything. Faulting a `Task<T>` with a single instance `E` and comparing the three shapes:

| Shape | `await` throws | same instance as `E`? | `.Exception` |
|---|---|---|---|
| reference upcast (shipped) | `E` | **True** | `AggregateException(count=1, inner0 same instance)` |
| derived `async Task Erase(Task<T> s) => await s;` | `E` | **True** | `AggregateException(count=1, inner0 same instance)` |
| the file's own `Project` | `E` | **True** | `AggregateException(count=1, inner0 same instance)` |

A derived continuation does not nest — `await` unwraps and the state machine rethrows via
`ExceptionDispatchInfo`. And the Critic's self-refutation point holds: the "typed
accessors" the clause contrasted against **are** derived continuations
(`Apply` → `Project`).

**Implementation unchanged** — the upcast stays, as the Critic endorsed. Per
`ffi-marshalling.md` §A6's round-5 amendment I **deleted** the comparative rather than
re-wording it: the whole second `<remarks>` paragraph on `Values` is gone from the public
xmldoc. The load-bearing part is restated in the constructor comment, next to the
projection it describes, with no comparison to an unimplemented alternative and no
uniqueness quantifier — *"each entry here is the SAME Task instance as the typed map's, so
the view adds no per-key allocation and introduces no second Task whose fault could go
unobserved"*. All three claims are properties of this code, verifiable by reading it.

The same false clause in this file's own Finding 1 entry is corrected above, with the
supersession stated rather than silently rewritten.

## Round-2 re-injection

| Injected defect | Test that turned red |
|---|---|
| the padding screen deleted from `DecodeUrlSafeBase64` | `Parse_RejectsPaddingJavaRejects` — **1 failed / 889 passed** |

Exactly one test red and 889 green is the point: it proves the new test is sensitive to
the fix *and* that nothing already in the suite covered the widened acceptance — the same
"a wrong shape passes every behavioural test" class as round 1's four reflection tests.
Finding 7 is a documentation deletion with no behavioural surface, so it has no test.

---

# ROUND 3 — convergence check (Manager's archive note)

**Verdict: CONVERGED.** The Critic re-ran every gate independently (890/890 on
both TFMs, no `Test Run Aborted`; header hash `45912ea9…e85105`; Mode-A diff 0
files) and re-verified the memory-safety core after all three fixups. Findings
1–7 are closed above.

## Finding 8 [Low] — CARRIED, not fixed. Deliberate.

`src/Confluent.Kafka/Uuid.cs:246` says `Convert.FromBase64String` is more
permissive than Java's `Base64.getUrlDecoder()` *"in **exactly** these two
places"* — but the same remarks block at `:268-271` names a **third** (embedded
whitespace, which .NET ignores and Java rejects). Measured: `"AAAA AAAA"`,
`"AAAA\tAAAA"`, `"AAAA\nAAAA"` all decode to 6 bytes. The doc self-refutes four
paragraphs down.

**Scope:** documentation only, on a `private` method, zero behavioural surface.
`Parse` / `TryParse` are provably unaffected (the Critic re-derived the
arithmetic: 16 bytes forces `W ≡ 0 mod 4`, so the first reachable length is 26 —
past the 24-char gate — and confirmed it with six whitespace probes, all
rejected).

**Why carried rather than fixed:** the fix is deleting one word (`exactly`), and
a full Actor→Critic round-trip costs more than the defect. Both the Critic and
the Manager independently reached the same recommendation.

⚠ **But it is the quantifier-drift class this repo has been bitten by before**
(M14/P1's five-cycle tail; `ffi-marshalling.md §A6` round-5 amendment; the
`dotnet-actor` memory note *"delete the quantifier, don't re-scope it"*). It was
introduced by the finding-6 fix, which deleted one quantifier and added another
— which is exactly how that tail started. So it is recorded here rather than
dropped.

**Disposition:** delete the word `exactly` on the first P2 commit that touches
`Uuid.cs` (P2 uses `Uuid.Parse` for `TopicCollection.OfTopicIds`, so it will).
Do **not** re-word the sentence — delete the quantifier.

Every *other* new quantifier in the P1 diff was checked and holds: "exactly one
trigger" matches the header verbatim at both entry points; "exactly two public
constructors" matches `ConfigEntry.java:44,:59`; the test file's "three
conditions" is still true.

## Observation carried out of P1 — two pre-existing consumer test flakes

Out of M15/P1's scope (neither file is in `git diff f24add9e..HEAD`), recorded
for a future consumer-side slice:

  - `ConsumerPollBridgeTests.ResultBridge_RunsContinuationsAsynchronously_OffTheCompletingThread`
    (last touched by `26761aa4`, M6) asserts
    `completingThreadId != continuationThreadId` and was seen failing with both
    equal to 30 — **managed thread ids are recycled**, so the assertion is
    unsound. `Thread.CurrentThread` reference identity, or the `[ThreadStatic]`
    flag M15/P1 used on the admin probe, is the sound form.
  - `ConsumerRebalanceListenerBridgeTests.DisposeAsync_WithLiveRegistration_ReturnsWithoutHanging`
    (last touched by `b25bf7f0`, M9/P6) — seen failing once, then 5/5 green.

This is a **false-FAIL** generator, the mirror of the false-PASS class in the
plan's §12. It can burn a future round by reading as a regression it is not.
