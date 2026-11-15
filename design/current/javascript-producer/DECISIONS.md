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
   `javascript/CLAUDE.md` for the rule owner. The exception is text that, read literally, breaks the client; see
   decision 16.

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
    - `setMockMetrics` is left out: the FFI has no `KafkaMetric` constructor.
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
      before `send()`. A consumed record's headers, and a `Headers` sealed by `send()`, never change.
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
      - The core reports the null checks with Java's text (a null header, key or header key). Java's `remove`
        checks the read-only state before the key, the FFI the other way: for a key the core or the binding
        refuses, `remove` first asks read-only headers to remove nothing, which the core refuses with its own
        error.
      - `headerStats()`, a test-only registration, counts the handles made and freed, the copies and the views.
        `test/unit/common/header/headers.test.js` proves the two cases of option 1 through it and, in a
        `--expose-gc` child under `MallocScribble`, that a view keeps its `Headers` alive, a value `Buffer` its
        `Header`, and that every handle is freed exactly once.
      - Not built: a consumed record's `Headers`, over `kafka_consumer_ConsumerRecord_headers`, which comes with
        the consumer. The row on plain `[key, value]` pairs stays parked: `ProducerRecord` takes `Header`
        objects only, as before.
