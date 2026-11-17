# JavaScript producer: decisions log

What was decided while aligning and completing the JavaScript producer (PRs #312 and #333), why, and where each
decision is recorded. The rules are the root `CLAUDE.md` "JavaScript Binding Conventions" from PR #327, checked against
`05abb9564`. The plan is `PLAN-producer-completion.md` in this folder. `javascript/CLAUDE.md` holds the binding's own
rules and readings. Newest entries are at the end; dates are in IST.

## How rules are read

1. **The rules come from PR #327's head**, never from a branch's copy of `CLAUDE.md` (2026-11-13). PR #327 is still
   open and still changing, and the 2026-11-12 plan was wrong because it read a stale copy. Fetch
   `origin/kraina-js-claude-rules` and diff it before planning or quoting a rule.
2. **A gap is first checked against a similar decided case** (2026-11-14). The same rule applies there, from the JS
   conventions, the Python conventions they defer to ("as in Python"), or root §2/§3/§4.
3. **Terms for things that stop work.** The rules use these:
   - **stop and ask:** a "Not decided yet" row;
   - **no row:** a type or FFI shape missing from its table;
   - **Future improvements:** never built;
   - **left out:** no FFI entry point, so the member is listed in the docs.

   A **blocker** is only something that needs a C FFI or Rust core change.
4. **A conflict between two root rules** is implemented as the root text reads, then marked **(root conflict)** in
   `javascript/CLAUDE.md` for the rule owner. The exception is text that, read literally, breaks the client (see
   decision 16) or misdescribes what the client does (see decision 27): there the code and docs follow Java and the
   core, and the mark stays.

## Structure

5. **Phases.** Phase A aligns PR #312's existing producer with the rules, and Phase B completes the producer in PR #333.
   Moved from A to B because they need serializers: the string-key `TypeError` (A6) and the null binding (part of A16).
6. **Tooling at the top of the stack** (2026-11-15). Jest, TypeDoc, the docs lint, Prettier and publint (A13/A14) land
   on PR #333, not PR #312. Moving every test to Jest and a repo-wide format on #312 would conflict with almost every
   file #333 changes. Both PRs land together.

## Buffers, zero copy and workers

7. **Zero copy on produce (Q21).** The binding borrows the app's key and value Buffers (a serializer's output and a
   `SharedArrayBuffer` included) with a reference, released at the hand-over.
   - **How the core takes the bytes:**
     - It copies them into the batch buffer when it registers the record: the FFI's `BytesPassthrough`, the
       accumulator's `append` and `DefaultRecord::write_to` (`rust/src/ffi/producer/kafka_producer.rs`,
       `rust/src/common/record/internal/memory_records_builder.rs`).
     - A queued record holds only a pointer (`GenericValue`) until then.
     - So once `send()` settles, the core has its own copy.
   - **Misuse is documented, not guarded.** Writing to, transferring or resizing a Buffer before `send()` settles is
     covered by the rule's docs text. Customers make a new Buffer per record; the owner discussed this with Emanuele
     (2026-11-13).
8. **Pinning.** Node-API's reference keeps a borrowed Buffer alive while the binding holds it, and Node never moves its
   bytes. What a reference can't do is outlive the worker's heap when the worker ends. Only V8's C++ API could hold the
   memory itself, which would tie the addon to one Node major version, so it isn't used.
9. **Workers.**
   - The preview doesn't support clients in worker threads (Q16, Q21); the docs say so, and the binding has no check.
   - Why JS differs from .NET: a .NET thread owns only its stack, and a pinned array lives on the shared heap. A Node
     worker owns its own heap, and Node frees it when the worker ends.
   - `worker.terminate()`, a `process.exit()` elsewhere in the worker, or an uncaught error can end a worker while an
     awaited `send()` is still pending. The process and the core keep running, so the core could read freed memory.
   - The cleanup hook that would prevent it is a Future improvement, needing FFI changes for all three clients.
10. **The debug check for borrowed Buffers** is a Future improvement. It isn't built.

## Readings by analogy (2026-11-14)

11. **Java's types and fields:**
    - A Java array taken is any iterable, as a `List` taken is.
    - A Java public mutable field becomes a setter only (`setSendException`), as Python's rule does for the same
      `MockProducer` field.
    - `metrics()` returns a snapshot, as Python's does. Java's view is live; that is a recorded deviation.
12. **Checks and test runner:**
    - The NUL-byte refusal on strings that cross as C strings stays. It is a run-time type check TypeScript can't
      enforce, and Python's marshaling layer does the same.
    - Every test moves to Jest, the rule linter's tests included.
13. **Signatures:**
    - An enum's `toString()` follows Java's override (`SslClientAuth`), and `toJSON()` stays the constant's name.
    - A `null` key or value binds `never` through extra signatures, one per null combination, as Python's Binding rule
      adds a stub per combination.
14. **"Callers await what the client returns"** (`javascript/CLAUDE.md`, 2026-11-13). A finding that needs a caller
    who doesn't await is not a bug of the client. What the rules promise without `await` is still tested: a delivery
    future need not be awaited, sends can be awaited together, and a callback's Promise is awaited.

## Producer details

15. **Record futures wait on the core's future** (A7). `get()` uses `get_cb` and `get({ timeout })` uses
    `get_with_timeout_cb`; there is no per-record finalizer.
    - The A/B against the base is about 2% below on send throughput (−1.9% median, −2.4% mean), with GC time unchanged.
      Accepted.
    - A timed get's message is the core's text, a deviation that goes when the core uses Java's text.
16. **Serde config keys (Q53).** A key a serde config function reads is left out of the core's config only when the core
    doesn't know it, such as `schema.registry.url`. Read literally, the root would drop `bootstrap.servers` as well,
    but Java only marks read keys as used. Marked (root conflict).
    - Deviations: the function gets no computed `client.id`, and configured serializers run before the core checks
      the config.
17. **Serializer factories:**
    - `stringSerializer()` is built: the Types rule names it. The rest of the factory list stays parked.
    - It matches Java byte for byte for Java's six standard charsets.
    - Any other charset name throws Java's `SerializationError("Unsupported encoding <name>")`, as a deviation.
18. **`MockProducer` wraps the FFI's mock (Q62).**
    - The partitioner is never called, because Java's `send()` doesn't call it with `Cluster.empty()`
      (`MockProducer.java:311-318`), and the `Cluster` form is left out (Q30).
    - A `null` serializer means bytes.
    - A mock keeps Node running until `close()`.
    - The core's mock completes every record in `flush()` and completes a future after a throwing callback. Both are
      recorded as deviations; the callback's error still comes out of the completing call.
19. **Metrics:**
    - `KafkaMetric.config(MetricConfig)`, the setter, is left out (Q30).
    - `MetricName` keeps a `Map` argument by reference, as Java does; a plain object is read into a new `Map`.
    - `setMockMetrics` is left out: the FFI has no `KafkaMetric` constructor (entry 26).
20. **Error payloads:**
    - Every public type the errors reach is translated (`TimestampType`, `OffsetAndMetadata`,
      `RecordDeserializationError.DeserializationExceptionOrigin`). Q30 says untranslated types appear only in
      `MockProducer` and `KafkaMetric`.
    - `TopicPartitionMap` and `TopicPartitionSet` accept `null`, as Java's collections do.

## Headers (A8, decided by the owner 2026-11-15)

21. **Option 1: copy while the `Headers` can still change.** The root row is still parked ("How a `Header` from a
    `Headers` that can still change … stays valid", Q31), so the rule's text needs this decision added by its owner.
    - **The rule:** in JavaScript, `Headers` and `Header` wrap the FFI's `RecordHeaders` and `RecordHeader`. This is
      js-only; Python and .NET keep plain headers and build the native ones at the send boundary. The reasons are that
      the core holds the headers and enforces read-only after `send()`, and a consumed record's header values stay
      Buffers over the fetched bytes.
    - **The problem:** `lastHeader()`, `headers()` and `toArray()` hand out a `Header` over a borrowed view into the
      native list. That view is valid only until the `Headers` next changes, while Java's `Header` stays valid.

      ```js
      const h = headers.lastHeader('trace');
      headers.add('span', bytes);   // the native list may move
      h.value();                    // could read freed memory
      ```

      Java allows this, so it isn't misuse. Only a `Headers` that can still change is affected: an app's own `Headers`
      before `send()`. A consumed record's headers, and a `Headers` sealed by `send()`, never change, but the FFI
      still frees a handle's views on every mutating call (`add_*`, `remove`), a refused one included, as its module
      docs say ("until the next mutating call on the same handle"). So the binding never makes such a call on read-only
      headers (below).
    - **The decision:** a `Header` handed out from a changeable `Headers` is the binding's own copy of that header's key
      and value. A read-only `Headers` hands out views. This needs no core change, and the `Header` stays valid as Java's
      does. The alternative, an FFI that hands out headers that stay valid, would be a core change.
    - **Zero copy holds:** the rule covers record keys and values. Header values are always copied on produce ("the C
      API has no borrowed form for them"), and `add()` copies already. The extra copy happens only when an app reads a
      header back before sending. The consume side stays zero copy.
    - **Python and .NET** have no such problem, because neither wraps the FFI. Python's `Header`/`Headers` are Python
      objects; a consumed value is a memoryview into the fetched batch. .NET's are a managed `List<Header>` with owned
      `byte[]` values, and consumed headers are copies.
    - **Implementation notes (A8):**
      - `native/headers.c` wraps each `RecordHeaders` and `RecordHeader` in a type-tagged, `napi_wrap`ed object over a
        refcounted box, so the handle is destroyed once, on the JS thread, when its last reference goes. A view's box
        holds a reference on its `RecordHeaders`' box, and a value `Buffer` (`napi_create_external_buffer` over
        `kafka_common_header_Header_value`'s bytes) holds one on its header's box, so no JS object references another
        through a strong `napi_ref` that the GC could never collect. Where the runtime refuses external buffers, the
        value is copied.
      - The addon picks copy or view from `RecordHeaders_is_read_only`, inside `lastHeader`, `headers(key)`,
        `toArray()` and iteration (`RecordHeaders`' `last_header`, `headers`, `to_array` and `iter` getters). A copy
        is `RecordHeader_new_err` over the borrowed header's key and value.
      - A `Headers` that never held a header has no `RecordHeaders` of its own: it reads one shared empty one, and
        sealing it gives it one shared empty read-only one, so every `ProducerRecord` without headers (the common
        case) still makes no native object for them. The first `add` makes its own.
      - `send()` seals the record's headers with `RecordHeaders_set_read_only` once the serializers have run, and
        hands their handle to `ProducerRecordOptionsBuilder_set_headers`, which copies it; the rebuild from
        `[key, value]` pairs in `native/record.c` is gone.
      - A key crosses as `encodeLikeJava`'s bytes in a C string, so a NUL is refused when the header is made (it
        used to be at `send()`), and `key()` reads a lone surrogate back as `?`, where Java's `RecordHeader` keeps
        the `String` (a recorded deviation).
      - The core reports the null checks with Java's text (a null header, key or header key).
      - On read-only headers, `add` and `remove` go, with their arguments unchanged, to the shared empty read-only
        `RecordHeaders`, which hands out no view, so the core's error comes from a handle whose views nothing holds
        and the views the app holds stay valid, as Java's `Header` does after a refused change. It is a check of the
        binding's own (`javascript/CLAUDE.md`, boundary rule 1), which goes when the FFI keeps a handle's views
        across a refused call. A suggestion for the core team, not made here: the FFI could drop the views only once
        a mutation has succeeded (`HeadersInner::headers_mut` would stop clearing, and the three mutating functions
        would clear only on `Ok`), and its module docs would then say "until the next successful mutation".
        `set_read_only` already keeps them, so a record sent twice keeps its views.
      - Java's `remove` checks the read-only state before the key, the FFI the key first, so on read-only headers
        `remove(null)` is the FFI's `IllegalArgumentError` "key cannot be null." where Java's is
        `IllegalStateException` (a recorded deviation: root §4 has the FFI check a NULL argument first, and Q35 has the
        binding repeat no precondition; it goes when the FFI checks the read-only state first).
      - `headerStats()`, a test-only registration, counts the handles made and freed, the copies and the views.
        `test/unit/common/header/headers.test.js` proves the two cases of option 1 through it and, in a
        `--expose-gc` child under `MallocScribble`, that a view keeps its `Headers` alive, a value `Buffer` its
        `Header`, that the views of a sent record's headers stay valid through refused `add` and `remove` calls and a
        second send, and that every handle is freed exactly once.
      - Not built: a consumed record's `Headers`, over `kafka_consumer_ConsumerRecord_headers`, which comes with
        the consumer. The row on plain `[key, value]` pairs stays parked: `ProducerRecord` takes `Header`
        objects only, as before.

## The tests' type check (A14, 2026-11-15)

22. **`checkJs`, not ts-jest.** `tsc --strict` runs over the tests (Q67, Q74) as `make typecheck-tests`, with
    `test/tsconfig.json` (`allowJs`, `checkJs`, `noEmit`, the library's own strict settings, the package typed by
    `dist/`'s declarations). The tests stay JavaScript: jest-light-runner runs them in plain Node.js and transforms
    nothing, so ts-jest cannot run under it (Q66, as Critic 128 confirmed).
    - Scope: every test file, the ones under `test/` and the verifiable clients' under `verifiable_clients/test/`, of
      every JavaScript and TypeScript extension (`.js`, `.mjs`, `.cjs`, `.ts`, `.mts`, `.cts`; the script checks the
      config's `include` against its list). A declaration file there is refused, since `skipLibCheck` would skip it.
    - The files the tests require from elsewhere (`scripts/`, `grpc-server/`, the verifiable clients themselves, the
      undeclared `dist/internal/*.js`) are read but not failed on: they are not tests, and their own checks are the
      build and the linter's and the clients' tests. A path filter in the script draws that line. `// @ts-nocheck` in
      those files would draw it too, but edits files that are not tests (`dist/` is generated), and `checkJs: false`
      with a `// @ts-check` opt-in in each test lets a test that forgets the comment go unchecked; the filter checks
      every test file with no comment.
    - A call into `dist/internal/*.js` is typed by inference over the compiled JavaScript, so most such calls are not
      checked (implicit `any` parameters). The three that the tests needed typed take their signature from the
      source, `typeof import('../../../src/internal/throwable').create` and `...config').configEntries`, so they
      cannot drift from `src/`.
    - It runs in the sub-make that builds `dist/` (`javascript/`'s `test`, the root `test-javascript-unit`), not in
      `lint-javascript`: under `make -j` a second `build-ts` beside the tests would delete `dist/` while they read it.
23. **What `@ts-expect-error` marks.** Only a misuse `tsc` catches, with a reason naming the run-time check it reaches
    and which kind it is:
    - a wrong argument;
    - a `null` the types forbid by the nullability rule: Java only stores it, so the type is `T`, while the run time
      stores the `null` as Java does (`OffsetOutOfRangeError`'s partitions, `NoOffsetForPartitionError`'s partition,
      `RecordDeserializationError`'s `timestampType` and `headers`);
    - a class instance given bare to `KafkaFuture.completedFuture` or `getNow`: TypeScript cannot tell it from a plain
      object, which the run time reads as the options object, so the types take it by name, `{ value }`.

    A valid call is never marked. The ones the published types rejected were fixed in `src/`: the options forms of
    `TopicPartitionMap`, `TopicPartitionSet` and `MetricNameMap` (Q15, as `partitionsFor` declares both). The
    library-only members the tests reach go through `test/_internal.js`. `@ts-ignore` and `@ts-nocheck` are refused.
24. **Types narrowed by the check.** `TimestampType.forName` takes a `string` (Java gives a null name no meaning).
25. **Limits of `tsc`**, the misuses the tests make that it lets through, so they carry no directive:
    - number ranges and integers: a fractional or out-of-range partition, leader epoch or batch index, a timestamp or
      offset past 2^53 or 2^63, a negative offset, negative timeouts, `NaN` and `-Infinity`, a `Temporal.Duration` in
      months;
    - a key computed at run time (`{ [field]: null }` for `MetricName`, `{ [name]: undefined }` for `ProducerRecord`,
      the serde config keys) and an options object from `Object.create`;
    - a parameter typed `unknown`: the placeholders (`KafkaMetric`'s `valueProvider`, `config`, `time` as `null`) and
      the value of a `TopicPartitionMap` built empty (`set(tp, undefined)`);
    - an `any[]`, such as `new Array(1).fill(null)` for `Headers`;
    - a `string` where an iterable of strings is taken (`unauthorizedTopics: 'abc'`), a string being one;
    - `undefined` for an optional key (`send({ record, callback: undefined })`), which only
      `exactOptionalPropertyTypes` would refuse, one of the other `tsc` settings that stay parked (Q74);
    - `undefined` for a value whose type is a type variable, which binds it to `undefined`: `ProducerRecord`'s
      `value: undefined` (`ProducerRecord<never, undefined>`) and `KafkaFuture.completedFuture({ value: undefined })`.
      The run time refuses both. A constraint refusing `undefined` would refuse every unconstrained generic caller as
      well, `<V>(x: V) => KafkaFuture.completedFuture({ value: x })`, the form the TSDoc tells generic code to use, so
      the types stay unconstrained, and `test/typing/usage.ts` pins that generic form;
    - config contents, which the core checks (Q52, Q54): a value of the wrong type (`'linger.ms': true`,
      `'batch.size': 42.5`, `acks: 5`), a missing `bootstrap.servers`, `{ configs: null }` read as a config map, and a
      NUL in a config key or value, a topic or a header key.

## Nothing left blocked on the core (2026-11-17)

26. **`QuotaViolationError.metric()` from the core** is built as the .NET client builds it: a `KafkaMetric` whose
    `MetricName` is the view's (`kafka_common_QuotaViolationError_metric_name`: name, group, description, tags) and
    whose value is the error's `value`, the value Java's `Sensor.checkQuotas` measured, measurable as Java's quota
    metric always is. Python keeps only the name and the group, behind a value that is not measurable, so the JS
    metric is the closer to Java's. With no name from the view it has an empty one, as .NET's. `metric()` is typed `KafkaMetric`. The items still left out match the
    other bindings: `setMockMetrics` is left out as in .NET, whose mock also wraps the FFI's, and
    `CorrelationIdMismatchError`'s ids are `-1` in all three. The core issues the producer's tests found are listed in entry
    31; the worker cleanup hook stays a Future improvement.

## Calls from inside a send callback (2026-11-17)

27. **`close()` from inside a send callback** closes at once, as Java's `KafkaProducer.close` documents and the core
    does (a logged warning, then `close(0)`, the records still pending failing with "Producer is closed
    forcefully."); only `flush()` is refused there. Root Q46 says the core refuses these calls before the callback's
    first `await`, and that awaiting a later record's future waits for that same callback. In the core each record's
    future waits only for its own callback's report, so a later record's future completes while the callback is
    still pending; only the callback's own record's future waits for it, and the docs name that one. Both are a
    **(root conflict)** marked in `javascript/CLAUDE.md`, the docs following Java and the core (entry 4's exception
    for text that, read literally, misdescribes what the client does).

## Java's configuration tests through the constructor (2026-11-18)

28. **`SaslConfigsTest` is ported through `new KafkaProducer`**, a reading by analogy (entry 2) with how
    `test/unit/producer/producer_config.test.js` ports `ProducerConfigTest`'s rejected values. Java's tests parse a
    `ConfigDef` built by `SaslConfigs.addClientSaslSupport`, which is not translated (Scope: the `ConfigDef`
    machinery), but the core's `ProducerConfig` defines the same four `sasl.login.refresh.*` keys with Java's ranges
    and checks them when the producer is built, so the ten tests of valid and refused values are observable with
    Java's `ConfigDef.Range` messages (decided by the Manager, 2026-11-18). The skip reason "the ConfigDef machinery"
    no longer applies to them; `testSaslLoginRefreshDefaults` stays skipped, as it reads parsed values no getter
    exposes. `ProducerConfigTest.testUpperboundCheckOfEnableIdempotence` is ported the same way: the constructor
    reaches the core's post-processing.

## Serializer errors in send() (decided by the owner, 2026-11-18)

29. **`KafkaProducer.send()` answers a serializer's throw as Java's `KafkaProducer#doSend` does**, whether the key or
    the value serializer throws or its Promise rejects:
    - an `ApiError` (Java's `catch (ApiException e)`) does not reject `send()`: the callback runs once, inside the call,
      with Java's failure metadata, `new RecordMetadata(appendCallbacks.topicPartition(), -1, -1,
      RecordBatch.NO_TIMESTAMP, -1, -1)` (the record's topic and its partition, or -1 before partitioning, as
      `KafkaProducer.AppendCallbacks#topicPartition` gives it), and `send()` resolves to a done future failed with the
      error, Java's `FutureFailure`. What that callback throws, or its Promise rejects with, rejects `send()`, as
      Java's `onCompletion` throws out of `doSend`'s catch;
    - a `TypeError`, the JavaScript form of Java's `ClassCastException`, rejects as a `SerializationError` with
      Java's message, "Can't convert key of class <class> to class <serializer> specified in key.serializer" (or
      `value`), the `TypeError` its `cause`. The classes are spelled as JavaScript names them: the value's constructor
      (`Buffer`), a primitive's wrapper (`String`), `null` for null, the serializer's class, or a built-in's factory
      (`stringSerializer`), a recorded deviation from Java's fully qualified names (and Java's null key throws
      `NullPointerException` there);
    - any other error rejects as it is, as Java rethrows it.

    No failure `send()` reports, a serializer's included, is counted in `record-error-rate` or `record-error-total`,
    which Java's `doSend` records for an `ApiException` or a `KafkaException` (a deviation of the core's: its `send()`
    records no error in that sensor; it goes when the core records them, entry 31). `MockProducer` keeps passing every serializer error out raw, as Java's
    `MockProducer#send` serializes with no `try`. `PlaintextProducerSendTest.testWrongSerializer` is ported. The .NET
    binding answers a synchronous callback the same way (`PublicProducerSerializerFailureTests`); for an async
    callback, .NET's async producer completes the send's first stage at once, fails the future once the callback
    ends, and keeps a faulted async callback out of the send, where this binding awaits the callback's Promise and
    rejects `send()` with its rejection, as Java's `onCompletion` throwing out of `send()`. Python passes the error
    through. A throw or rejection from that callback rejecting `send()` departs from the root's "the producer logs a
    send callback's error", which describes the paths the core runs; it is a **(root conflict)** marked in
    `javascript/CLAUDE.md` (entry 4).

## MockProducer callback rejections (decided by the owner, 2026-11-18)

30. **A rejected callback Promise is the callback's error on `MockProducer`, as a synchronous throw is**, restoring
    the design accepted earlier for the TypeScript mock: `send()` with `autoComplete` and `flush()` reject
    with it; `completeNext()` and `errorNext()`, being synchronous, keep it in a FIFO, and the next `send()`,
    `flush()` or `close()` raises the oldest, once (a deviation, as Python's `AsyncMockProducer`); no kept error is
    lost; `flush()` and `close()` (and `[Symbol.asyncDispose]`) wait for every callback Promise already started
    (`allSettled` semantics), then raise the oldest kept error, `flush()` flushing first even with a kept error.
    Nothing is logged. Only the record's own `send()` gets its callback's error: with `autoComplete` a record's
    callback is its own `send()`'s whichever drain runs it, so a nested `send()`, or a `send()` that drains a pending
    `flush()`'s callbacks, leaves their errors to their own calls. The callback queue does not nest, so a `send()`
    made from a callback's synchronous part has its callback run by the outer drain once that callback returns, and
    waits for it before it settles, rejecting with its error, as Java's nested `send()` throws it; a `completeNext()`
    or `errorNext()` made there cannot wait, so the next record's throw comes out of the outer completing call (a
    deviation). A `close()` from one of the mock's callbacks, before its first `await`, skips the wait, as Python's
    `AsyncMockProducer`; after it the core's close waits for that callback's report, and a `flush()` from a callback
    always does, so a callback that awaits either never settles (`KafkaProducer` refuses a `flush()` made before the
    first `await`; Java's mock makes no such check). Java's mock calls `onCompletion`
    with no `try` (`MockProducer.Completion#complete`), so its error propagates and the record's future stays
    incomplete; here the core's mock completes the future once the Promise settled, the deviation already recorded
    for a throw, which now covers this case too.

## Core issues (2026-11-18)

31. **Two divergences of the core's producer from Java's**, found by the ported tests. Each is a core change, so a
    blocker by entry 3, and is handed to the core team; neither is fixed in this PR:
    - **`buffer-exhausted-total` counts 3 for one dropped send, where Java counts 1.** The FFI's `send_cb` runs the
      core's non-waiting `try_send` on the calling thread, `submission_loop` runs it again, and then `send_waiter`
      runs the real wait with `max.block.ms`; each runs `BufferPool::allocate` and records one exhaustion, where
      Java's `BufferPool#awaitMemory` records once. The ported test is skipped; its skip is removed when the core
      records one exhaustion per dropped send (a `SendWait::Never` allocation not recording it, for example).
    - **`record-error-rate` and `record-error-total` count no failure of `send()`.** Java's `KafkaProducer#doSend`
      records the `errors` sensor for an `ApiException`, an `InterruptedException` or a `KafkaException`, a
      serializer's `SerializationException` included; the core's `KafkaProducer::do_send` records it for none, so
      the client's deviation (`send()`'s TSDoc) goes when the core records them.
