# Critic 130: M24/P1 resolved items

### 130-S1-1 (Low, behaviour): `LoadProps` decodes a file with a byte order mark as UTF-8 or UTF-16, not ISO 8859-1

- **Where**: `dotnet/tools/VerifiableClients/VerifiableClients/Common.cs:133`
  (`File.ReadAllText(filename, Encoding.Latin1)`).
- **What**: `File.ReadAllText(path, encoding)` still detects a byte order mark and then ignores the encoding
  it was given. A properties file that starts with `EF BB BF` is read as UTF-8 with the mark removed, and
  one that starts with `FF FE` / `FE FF` is read as UTF-16. Java's `Properties.load(InputStream)` and
  Python's `open(filename, encoding="latin-1")` read every byte as ISO 8859-1: the mark becomes three
  characters at the start of the first key, and a UTF-8 `é` becomes `Ã©`. PLAN §5.5 / V17 say the file is
  read as ISO 8859-1; this case was not ruled.
- **Evidence**: a throwaway xunit probe in the archive copy called `Utils.LoadProps` on three files:
  - bytes `EF BB BF 'a' '=' C3 A9 '\n'` gave key `a` (U+0061) and value U+00E9. Java and Python give key
    U+00EF U+00BB U+00BF `a` and value U+00C3 U+00A9;
  - the same bytes without the mark gave value U+00C3 U+00A9, as Java and Python do;
  - bytes `FF FE 'a' 00 '=' 00 'b' 00` gave `a=b`, decoded as UTF-16.
- **Why it matters**: the reader is shared with the consumer (CP2). For such a file the tool builds a
  different configuration from the one Java's tool builds: a key Java would mangle works here, and
  non-ASCII values are decoded differently.
- **Fix**: decode the bytes as ISO 8859-1 without detecting a mark, for example
  `Encoding.Latin1.GetString(File.ReadAllBytes(filename))`, or a `StreamReader` with
  `detectEncodingFromByteOrderMarks: false`. Keep the same exception filter.

- **Resolved in c6b2ea674**: `LoadProps` decodes `File.ReadAllBytes` as ISO 8859-1 (no byte order mark detection), exception filter unchanged; regression `CommonTests.LoadPropsReadsEveryByteAsIso88591` (new test, fails before the fix), 67 tests per TFM.

### 130-S2-1 (Low, test-flakiness): the second-signal test works only if the tool writes more than the pipe holds within 2 s

- **Where**: `dotnet/tools/VerifiableClients/VerifiableClients.Tests/CommonTests.cs:160-180`, in particular
  `:171` (`await Task.Delay(TimeSpan.FromSeconds(2))`) and `:174` (the second `Signal("TERM")`);
  `dotnet/tools/VerifiableClients/VerifiableClients.Tests/ToolProcess.cs:88` (`Assert.Equal(0, kill.ExitCode)`).
- **What**: the hook's `Close` is held only once the failed-send lines have filled the stdout pipe, so the
  completion thread blocks while printing. The test assumes the pipe is full after a fixed 2 s. The pipe
  holds 16 pages: 64 KiB with 4 KiB pages, but 256 KiB with 16 KiB pages and 1 MiB with 64 KiB pages (some
  arm64 Linux kernels). If the pipe is not full when the first SIGTERM arrives, the hook finishes at once
  and the tool exits. The second `kill` then fails with "No such process", and the test fails on the
  `kill` exit code, with nothing that names the cause.
- **Evidence**:
  - With `max.block.ms=1` (the test's config) on this Mac, the tool wrote 878 lines (190 149 bytes) in 2 s,
    about 3 times the 64 KiB pipe. It was still running 0.5 s, 3 s and 8 s after the first SIGTERM while
    stdout was unread, then exited 0 once stdout was drained.
  - With `max.block.ms=20` the tool wrote 20 105 bytes in 2 s. The tool had already exited at the second
    SIGTERM, and `kill -TERM <pid>` exited 1 ("No such process"). That is the failure this test would report.
  - The test passed 12/12 with 36 `yes` processes running on the 18 cores. The flood rate is set by the 1 ms
    metadata wait, so CPU load alone barely affects it. The test still depends on that rate, on the page
    size and on a fixed sleep.
- **Why it matters**: PLAN R5 asks for signal tests with no built-in race. This test depends on a
  throughput and a page size that nothing checks. On a kernel with larger pages it fails every time, and
  on a slow runner it fails sometimes. Either way the failure looks like a signal-handling bug.
- **Fix**: make the hold a checked precondition instead of an assumption. Before the second signal, assert
  that the tool is still running after the first, with a message that names the pipe. Give the flood a wide
  margin over the pipe's capacity, either with a longer wait before the first SIGTERM or with more output
  per send. Alternatively, detect that the pipe is full before the first signal. The comment at `:168-170`
  should then state the assumption.

- **Resolved in 1bf3b6d98**: the hold is a checked precondition (`Assert.False(process.HasExited, …)` naming the stdout pipe, 1 s after the first SIGTERM); the flood margin comes from a 2000-character topic (~4 KB a line, ~5 MB in the 3 s before the first signal, 5× a 1 MiB pipe); a short-topic `max.block.ms=20` mutant fails on the new assertion.

### 130-S4-1 (Low, test-coverage): no test checks the `.0` that `JsonOutput.WriteNumber` adds to a whole-number `avg_throughput`

- **Where**: `dotnet/tools/VerifiableClients/VerifiableClients/Common.cs:96-98` (the hand-written `"R"` text
  plus `".0"`); `dotnet/tools/VerifiableClients/VerifiableClients.Tests/VerifiableProducerTests.cs:289-296`
  (`ToolDataFieldsInJavaOrder`, which uses 4.5).
- **What**: PLAN §5.4 says `avg_throughput` keeps a `.0` on a whole number, as Java and Python write it.
  PLAN §3.3 says the exact-string assertions cover "the Rust tool's JSON vectors without extra tests". The
  Rust tool's vector is `tool_data_json` in `rust/tools/verifiable-clients/src/verifiable_producer.rs`,
  with `avg_throughput: 50.0` →
  `{"timestamp":42,"name":"tool_data","sent":10,"acked":8,"target_throughput":100,"avg_throughput":50.0}`.
  The .NET translation keeps Python's 4.5, which never reaches the `.0` branch, and the script tests do not
  read `avg_throughput`. Python's tests need no such case, because `json.dumps` writes a float's `.0`
  itself. In .NET the `.0` is the binding's own code.
- **Evidence**:
  - Mutation in an archive copy: `writer.WriteRawValue(text)`, the `.0` removed. Build 0 errors; the suite
    passed 66/66 on net8.0.
  - A probe of the unmutated code wrote `0.0`, `500.0`, `-0.0` and `428.57142857142856`, so the code is
    right today; only the guard is missing.
- **Why it matters**: these exact bytes are what the plan says the suite pins. A change to the number
  formatting would make the tool write `"avg_throughput":0` for every run with no acks, with no test failing.
- **Fix**: within the planned test count, add Rust's whole-number vector as a second assertion in
  `ToolDataFieldsInJavaOrder`, for example `ToolData(_output, 10, 8, 100, 50.0)` →
  `…"target_throughput":100,"avg_throughput":50.0}`. A `0.0` case is worth adding too.

- **Resolved in a9c4c8e45**: `ToolDataFieldsInJavaOrder` also asserts the Rust vector `"avg_throughput":50.0` (sent 10, acked 8, target 100) and a `0.0` case; no new test; a mutant writing the raw "R" text fails at the 50.0 line.

### 130-S6-1 (Low, docs-accuracy): a pre-3.7 consumer never runs `classic`; the tool stops at argument parsing
- **Where:** `dotnet/tools/VerifiableClients/README.md:152-155`
- **What:** the README says "no `--group-protocol` before 3.7, so the consumer runs `classic`, and `--broker-list` (the
  consumer's before 4.0, the producer's before 2.5), which the tools do not take." The `start_cmd` facts are right:
  `--group-protocol` is passed only from `V_3_7_0`, and `--broker-list` below `V_4_0_0` for the consumer and below
  `V_2_5_0` for the producer. But every release before 3.7 is also before 4.0, so its consumer gets `--broker-list` too,
  and no `--bootstrap-server`. The tool exits at argument parsing before it builds a consumer, so it never runs
  `classic`.
- **Evidence:** I ran the built tools (`dotnet …/bin/Release/net8.0/<Tool>.dll`) with each older service's argument set:
  - a 3.6 consumer's: `--assignment-strategy …RangeAssignor --broker-list localhost:1 --reset-policy earliest --group-id g
    --topic t --consumer.config /dev/null`;
  - a 3.9 consumer's, with `--group-protocol consumer`;
  - a 2.4 producer's: `--topic t --broker-list localhost:1 --max-messages 1 --producer.config /dev/null`.

  Each exits 1 with "error: one of the arguments --bootstrap-server is required". In `VerifiableConsumer.start_cmd` the
  `--broker-list` branch is `node.version < V_4_0_0`, which every version below `V_3_7_0` meets.
- **Why it matters:** the sentence describes a run that cannot happen. A reader debugging an older-release test is sent to
  the group protocol, when the tool never got past its arguments. The one cause, for every pre-4.0 consumer and every
  pre-2.5 producer, is the broker option.
- **Fix:** drop the group-protocol clause, e.g. "Tests that run an older release's clients get these tools with that
  release's arguments, which name the brokers with `--broker-list` (the consumer's before 4.0, the producer's before 2.5);
  the tools take only `--bootstrap-server`, so they exit at argument parsing."

- **Resolved in 37933c7f5**: the clause about `--group-protocol` and `classic` is gone; the item now says an older release's clients name the brokers with `--broker-list` (the consumer's before 4.0, the producer's before 2.5), and the tools take only `--bootstrap-server`, so they exit at argument parsing.

### 130-S6-2 (Low, docs-accuracy): only a service's `start()` empties the capture file; a bounced node keeps appending to it
- **Where:** `dotnet/tools/VerifiableClients/README.md:132-135`
- **What:** the README says "A service that restarts a tool first removes the tool's directory on the node, so a failed
  test's capture file (see Logging) holds only the tool's last run". Only ducktape's `Service.start()` removes it: it runs
  `stop_node` and then `clean_node` (`rm -rf /mnt/verifiable_consumer`, or `/mnt/verifiable_producer`) on each node
  before `start_node`. A node restarted with `stop_node` and `start_node` is not cleaned. `consumer_test.py`'s
  `rolling_bounce_consumers` and `bounce_all_consumers` restart them that way, and they serve `test_consumer_bounce`, the
  table's second case. Such a restart goes through `BackgroundThreadService.start_node` and the service's `_worker`,
  which only runs `mkdir -p`, and `start_cmd` appends (`2>> … | tee -a …`). So the file keeps every run since the
  service's last `start()`.
- **Evidence:** ducktape 0.14.0, the version `tests/setup.py` pins:
  - `Service.start` calls `stop_node`, then `if kwargs.get("clean", True): self.clean_node(node)` for each node, then
    `start_node`;
  - `BackgroundThreadService.start_node` only spawns `_protected_worker`, and calls neither `clean_node` nor `stop_node`.

  In kafkatest, `VerifiableConsumer._worker` and `VerifiableProducer._worker` run `mkdir -p` only, and the bounce helpers
  call `consumer.stop_node(node, clean_shutdown)` and `consumer.start_node(node)`. The fencing example is right:
  `conflict_consumer.start()` is a `Service.start()`.
- **Why it matters:** in a failed bounce test, the reader finds every run of each bounced node in
  `verifiable_consumer.stdout`, where the README says there is one. The sentence gives a wrong rule for which runs a
  capture file holds, and that rule is the one a reader relies on to read a failure.
- **Fix:** e.g. "A service's `start()` first removes the tool's directory on each of its nodes, so a capture file holds
  only the runs since the service last started. When a test starts a service again, as the fencing test does its
  conflict consumers, the earlier runs (the fenced first run) are not in it. A node bounced with `stop_node` and
  `start_node`, as in `test_consumer_bounce`, appends to the same file."

- **Resolved in 37933c7f5**: a service's `start()` removes the tool's directory on each of its nodes, so a capture file holds only the runs since the service last started; the fencing test's conflict consumers are the example of a service started again, and a node bounced with `stop_node` and `start_node`, as in `test_consumer_bounce`, appends to the same file.

---

# Appendix: the review log and the comment-only notes (COMMENTS.130.md at close-out)

## Critic 130: M24/P1 CP1 (S1–S4), `81ddfa9c0..46dcaae3c`

Reviewed against the PLAN §3 parity anchor, Python's `python/verifiable_clients/` (tools and tests),
Java's `VerifiableProducer` / `ThroughputThrottler`, CPython 3.10's `Lib/argparse.py` (3.10 branch) and
kafkatest's `verifiable_producer.py`. No interop change in CP1, so the persona's C ABI checklist does not apply.

Gates run in an archive copy of `46dcaae3c` (native via `-p:NativeLibraryPath=…/rust/target/release/libconfluent_kafka.dylib`):
build `0 Warning(s) 0 Error(s)`; `dotnet format <each tools csproj> --verify-no-changes` rc 0;
`dotnet test -c Release` 66/66 on net8.0 and 66/66 on net10.0, repeated 6 times per TFM, all green.

## Review of 25580b58a (S1)

130-S1-1: resolved, moved to `COMMENTS.DONE.130.md`.

## Review of d6748f9bf (S2)

130-S2-1: resolved, moved to `COMMENTS.DONE.130.md`.

## Review of 84225cbd3 (S3)

No open items. `ThroughputThrottler` matches `ThroughputThrottler.java` line for line:
- `float` elapsed seconds;
- the zero-target wait keeps the flag set, and the timed path clears it;
- the wait rounds up to whole milliseconds, as `Object.wait(ms, nanos)` does.

The 12 tests are Python's. `WakeupPending` stands for Java's `wakeup` field and is read only by the tests.

## Review of 46dcaae3c (S4)

130-S4-1: resolved, moved to `COMMENTS.DONE.130.md`.

## Review of 46dcaae3c..a9c4c8e45 (CP1 fixups)

Gates in an archive copy of `a9c4c8e45` (native via `-p:NativeLibraryPath=…/rust/target/release/libconfluent_kafka.dylib`):
the solution builds with `0 Warning(s) 0 Error(s)`; `dotnet format` on the solution and on each tools csproj rc 0
(control: an over-indented line gives rc 2); `dotnet test -c Release` 67/67 on net8.0 and 67/67 on net10.0. The three
fixups touch 4 files, all under `dotnet/tools/VerifiableClients/`, none forbidden; no fixup and no hunk is comment-only.

No open items.

- 130-S1-1: resolved. `Encoding.Latin1.GetString(File.ReadAllBytes(filename))` decodes every byte as ISO 8859-1. The
  old `ReadAllText` line fails `LoadPropsReadsEveryByteAsIso88591` on both TFMs (`[a]=é` against `[ï»¿a]=Ã©`), and a
  mutant that decodes only an `FF FE` file as UTF-16 fails the second assertion (`[a]=b`), so each mark is pinned. The
  literals are U+00EF U+00BB U+00BF `a` / U+00C3 U+00A9 and U+00FF U+00FE `a` NUL / NUL `b` NUL, the key and value
  `Properties.load(InputStream)` gives. The filter is unchanged, and six unreadable paths (missing file, missing
  directory, a directory, mode 000, `""`, a NUL) give the same type, filter result and message with both read calls, on
  both TFMs.
- 130-S2-1: resolved. The hold is checked: `Assert.False(process.HasExited, …)` 1 s after the first SIGTERM, naming the
  stdout pipe; the short-topic `max.block.ms=20` mutant fails there 3/3 with that message. Each line is 4 213 bytes;
  1 MiB is written 579 ms after `startup_complete` (607 ms with 36 `yes` on 18 cores) against the 3 s wait, about 5×.
  Once the pipe is full the hook cannot finish while stdout is unread, and an unheld tool exits about 50 ms after
  SIGTERM, so the 1 s check has 20× room. 12/12 runs per TFM, 4.26–4.31 s each; 5/5 under that load.
- 130-S4-1: resolved. `ToolDataFieldsInJavaOrder` asserts `"avg_throughput":50.0` and `"avg_throughput":0.0` exactly.
  Removing the `.0` (`WriteRawValue(text)`) fails it at position 1 on both TFMs, and a mutant that drops the `.0` only
  for zero fails at position 2.

## Review of 8617d1092 (S5): no open items

Checked against PLAN §3–§5 and §9, Python's `verifiable_consumer.py` and its 49 tests, Java's `VerifiableConsumer` and kafkatest's consumer service (no interop change, so the C ABI checklist does not apply): every event byte for byte and in Java's field order, `ParseArgs`'s configuration key for key and type for type, help and 13 argument errors identical to Python 3.12's argparse; in an archive copy the solution builds `0 Warning(s) 0 Error(s)`, `dotnet format` rc 0 on each tools csproj, 118/118 on net8.0 and net10.0 (5 runs each), 49 Python names plus the overflow test present, 17 production mutants each killed by the matching test, and the signal theory 5/5 per TFM, with `shutdown_complete` 2 997–3 005 ms after `startup_complete` (2 999–3 005 ms with 36 `yes` on 18 cores).

## Review of ead4ffa66 (wakeup DEBUG line): no open items

Checked against the ledger 564 ruling, Python's `verifiable_consumer.py` (`_LOG.debug`, logger `verifiable_consumer`) and `_common.py`'s `basicConfig`: the real tool, SIGTERMed while polling an unreachable broker, writes `[<ts>] DEBUG Caught WakeupException because consumer is shutdown, ignore and terminate. (verifiable_consumer)\n` and nothing else at `DEBUG`, `debug` and `NOTSET`, and nothing at `INFO` or unset, as Python 3.12's logging does; after the `yyyy-MM-dd HH:mm:ss,fff` timestamp the line equals Python's byte for byte. `Log.Error` only inlines the same timestamp expression, in the same evaluation order. In an archive copy: `0 Warning(s) 0 Error(s)`; `dotnet format` rc 0 on both tools csprojs (control rc 2); 118/118 on net8.0 and net10.0, 4 runs each, no abort. Removing the call fails `RunEndsQuietlyOnWakeup` at the `Assert.Matches` (empty stderr), and removing the level check fails it at the default-level `DoesNotContain("] DEBUG ")`, on both TFMs. A probe shows the collection starts only after the parallel tests end, and that `AtLogLevel` leaves the variable as it found it and the level at `WARNING` when its action throws, with the ambient variable unset, `ERROR` or `BOGUS`. The 3 paths are under `dotnet/tools/`, none forbidden, and no comment is added.

## Review of 8c3638209 (S6)

Checked against PLAN §5.9, §5.11 and §9 (ledger 562), Python's `python/verifiable_clients/README.md`, the S7 logs,
kafkatest at 4.4.0-rc4 (`verifiable_client.py`, both services' `start_cmd`, `consumer_test.py`, `pluggable_test.py`,
`ducker-ak`, `tests/docker/Dockerfile`), ducktape 0.14.0 (`Service.start`, `BackgroundThreadService.start_node`) and the
core (`ssl_factory.rs`, `sasl_client_authenticator.rs`). Verified and correct:
- **Publish:** the README's command for `VerifiableConsumer`, from the repository root, into a fresh scratch
  `--artifacts-path` and `-o`, with a stamp touched first, rc 0. 0 files under `dotnet/` are newer, and none
  elsewhere outside `.git` and `kafka/` (control: a file touched after the stamp is found). 193 files: an ELF aarch64
  apphost, `libconfluent_kafka.so` byte-identical to `rust/target-linux/release/`, no `.dylib`, and
  `System.Globalization.Invariant: true`. The repository's `src/Confluent.Kafka/bin/Release/net8.0/` still holds the
  `.dylib`. Scratch output deleted.
- **Selectors:** byte-identical to S7's `logs/case{1,2,3,4}.sel`, and each names every injected argument of
  `--collect-only`, with JSON booleans. The four `@cluster` sizes are 7, 7, 10 and 8, so 11 nodes (ducker01 is left
  out of `cluster.json`) and the default 14 both work.
- **Globals:** valid JSON, equal to PLAN §5.9.3 and byte-identical to S7's `tests/dotnet-globals.json`.
- **Results table:** PASS for every case, with the run times rounded from the case logs. The session ids are
  `2026-11-16--003/004/006/007`. The gitlink is `1156b2752`, tag `4.4.0-rc4`.
- **The `env` tip:** checked in `ubuntu:22.04` (bash 5.1.16, as on the jammy nodes, whose `ducker` user's shell is bash)
  with the published tool, mimicking `VerifiableConsumer.start_cmd` under `bash -c`: the exports, the appended
  arguments, and `2>> f | tee -a f &`. The README's `pids` finds exactly the tool's pid, because `env` execs it. The
  default `pgrep -f 'env …'` finds nothing. Both variables are in `/proc/<pid>/environ`. SIGTERM gives
  `startup_complete`, `shutdown_requested` and `shutdown_complete`. The capture file gets 233 DEBUG lines (the wakeup
  line among them) and 232 Rust debug lines, and the tee stream the service parses gets only the 3 JSON lines. The
  plan's `exec_cmd` finds the same pid and logs nothing.
- **Signals**, on Linux with the published tool:
  - Started with `&` from a script: SigIgn is `0x1006` (SIGINT ignored), and the tool is still alive 5 s after a
    SIGINT. A SIGTERM then makes it exit 0.
  - With job control on (`0x1000`): SIGINT shuts it down cleanly, exit 0.
  - Three SIGTERMs: one shutdown sequence, exit 0, empty stderr.
  - The producer's `shutdown_complete`, then `tool_data`, is pinned by its tests and by Java's hook.
  - The Windows sentence agrees with .NET's documentation: SIGTERM can be cancelled only on Unix.
- **What cannot pass yet:** the SSL, SASL, pluggable and copier items, and the globals applying to every version
  (`create_verifiable_client_implementation` ignores the version). The older-releases item is 130-S6-1.
- **Tests section:** the target exists. `build-dotnet` runs `cargo build --features ffi --release`, and
  `DOTNET_CONFIG ?= Release`. The four projects are in the solution, and the five signal tests are
  `[UnixFact]` / `[UnixTheory]`.
- **Gates:** `dotnet test -c Release`: 118/118 on net8.0 and on net10.0, no `Test Run Aborted`.
- **Hygiene:** two files, both under `dotnet/`. No phase or review id and no line reference (control: the same pattern
  finds 8 in the plan). The relative link to Python's README resolves. The package line is one sentence with an
  absolute URL, like the examples line above it.

130-S6-1: resolved, moved to `COMMENTS.DONE.130.md`.

130-S6-2: resolved, moved to `COMMENTS.DONE.130.md`.

## Review of 37933c7f5, 8fe68412e (S6 fixup, S8): no open items

Checked 130-S6-1 and 130-S6-2 against kafkatest's `start_cmd` (`--broker-list` below `V_4_0_0` for the consumer and `V_2_5_0` for the producer; the tools take the deprecated `--consumer.config` / `--producer.config`, so the broker option is the one cause), `clean_node` (`rm -rf` of the root holding the capture file), `consumer_test.py` (the conflict consumers' second `start()`, the bounces' `stop_node` / `start_node`) and ducktape v0.14.0's `Service.start` and `BackgroundThreadService.start_node` (fetched): both fixed, no new wrong fact; `8fe68412e` is comment-only (0 changed non-comment lines in the `.cs` hunks, against 9 comment lines; each file's comment-stripped text hashes the same at both commits, while the full text differs), and its three texts are accurate (Python's `0.0`, the .NET mock's lack of `schedulePollTask`, the interleaved capture); across the phase's 21 files all 30 `///` blocks are the summaries of the 30 types, no phase or review id and no line cite, and the 11 kept body comments each state a recorded decision or deviation; in an archive copy of `8fe68412e` the solution builds `0 Warning(s) 0 Error(s)`, `dotnet format` on the solution rc 0 (control rc 2), 118/118 on net8.0 and net10.0 with no `Test Run Aborted`; all 21 paths are under `dotnet/`, none forbidden (control: the same pattern finds 187 in the history).

## Notes for the comment-only commit

- `dotnet/tools/VerifiableClients/VerifiableClients/VerifiableProducer.cs:386`: the comment says only what
  Java does. State the decision too: "…; here 0.0, as Python's tool" (comment rule 3).
- `dotnet/design/history/M24/P1-verifiable-clients/PLAN.md:246-247` (Manager-owned) **[done by the Manager, 2026-11-16, in the uncommitted plan]**: replace "unrecognized
  arguments, then the required options in declaration order, then the group" with argparse's order, which
  the code implements: the required options, then the group, then unrecognized arguments (CPython 3.10's
  `_parse_known_args` and `parse_args`).
- `dotnet/design/history/M24/P1-verifiable-clients/PLAN.md:128` (Manager-owned) **[done by the Manager, 2026-11-16, in the uncommitted plan]**: the test project's path is
  `dotnet/tools/VerifiableClients/VerifiableClients.Tests/`, the §5.1 tree, not `dotnet/tools/VerifiableClients.Tests/`.
- `dotnet/design/history/M24/P1-verifiable-clients/PLAN.md:501-503` (Manager-owned) **[done by the Manager, 2026-11-16, in the uncommitted plan]**: with
  `UnsafeRelaxedJsonEscaping`, only BMP non-ASCII is written as raw UTF-8. A supplementary character, such
  as an emoji, is written as a `🚀` pair, as Python writes it and unlike Java (probe:
  `"v🚀<>&'+/\u007F "`). Harmless for the services, which use `json.loads`.
- (Manager, at the squash; not for the comment-only commit) **[Manager: no history rewrite this phase; the retarget goes in the final report for the user]** `git rebase -i --autosquash 81ddfa9c0` stops twice.
  `c6b2ea674` (target `25580b58a`) edits `CommonTests.cs`, which `d6748f9bf` adds (modify/delete conflict).
  `1bf3b6d98` (target `d6748f9bf`) adds `HasExited` after `RunAsync` in `ToolProcess.cs`, and `46dcaae3c` adds
  `RunAsync` (content conflict). Retarget `c6b2ea674` to `d6748f9bf` and `1bf3b6d98` to `46dcaae3c`: verified in a
  scratch repo, the series then squashes cleanly into the four step commits, the final tree equals `a9c4c8e45`'s, and
  the S2 commit builds and passes 18/18 per TFM. Moot if the phase is squashed into one commit.
- `dotnet/design/history/M24/P1-verifiable-clients/PLAN.md:131` and `:555` (Manager-owned) **[Manager: the plan stays as approved; the 118 and the extra test are recorded in COMMENTS.DONE.130 and STATUS]**: with
  `LoadPropsReadsEveryByteAsIso88591`, the 130-S1-1 regression, CommonTests has 16 tests and the phase 118, not 15 and
  117. Python's `test_common.py` has 15 cases and none for `load_props` that the new assertions could join.
- `dotnet/tools/VerifiableClients/VerifiableClients.Tests/ScriptedConsumer.cs:25`: "what Java's and Python's tests
  use" is not accurate, since Java has no test of the tool. The scripted commits, the state at close and the wakeup
  count stand for Python's `_CommitScript`, `_StateAtClose` and `_WakeupCounter`. Say "what Python's tests use and the
  .NET mock lacks: poll tasks (Java's `MockConsumer.schedulePollTask`, …), …".
- `dotnet/tools/VerifiableClients/README.md:166-167` (S6): "append stderr to the stdout capture file …, after the JSON
  stream they parse". Stderr (`2>>`) and the tee'd stdout are appended to the file as they are written, so the log lines
  are interleaved with the JSON. Probe: in the capture of one consumer run, the JSON is on lines 1, 2 and 4 and the DEBUG
  wakeup line on line 3. Suggest "…, interleaved with the JSON stream they parse". Python's README has the same sentence
  (`python/verifiable_clients/README.md:125-126`); whether to change it too is the Manager's call. **[Manager: the README line goes in S8; Python's README is out of this phase's scope, listed as a follow-up]**
