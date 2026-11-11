# JavaScript (Node.js) binding

The Node.js client in `javascript/` is built only on the C FFI of root
`CLAUDE.md` §4. It is a Node-API addon written in plain C against
`node_api.h` (`native/`), under a thin layer written in TypeScript (`src/`),
which tsc compiles, with its declarations, into `dist/`, the package's entry
points.

The rules are the root `CLAUDE.md`'s **JavaScript Binding Conventions**
("the root conventions" below). They decide every question they answer, and
this file does not repeat them: where they state a rule, this file points to
the group that states it ("root conventions, Signatures"). This file holds
what they leave to the binding: the addon's internals, liveness, the file
layout, the generated files and the rule linter. An item the root conventions
list under "Not decided yet" is marked **(not decided yet in the root
conventions)** here: the text records the current choice, and nobody treats it
as settled. A **(root conflict)** mark names a place where the root
conventions disagree with themselves or with a reviewed fact; the code follows
the root text literally, as the owner chose, and the mark records the
conflict. Root `CLAUDE.md` still applies
wherever both are silent. The approved design is
`design/current/javascript-producer/PLAN.md`.

## Ground truth

These decide every question, in this order:

1. The root conventions (root `CLAUDE.md`, JavaScript Binding Conventions).
2. The C ABI in `rust/target/include/confluent_kafka_bindings.h`, the header
   the addon includes (which header: not decided yet in the root conventions).
3. The Java public API (`KafkaProducer`, `ProducerRecord`, `RecordMetadata`,
   `Callback`, Apache Kafka 4.4.0 in `kafka/`).
4. Root `CLAUDE.md` §4 "C FFI Conventions".

## Boundary rules

1. **Java shape, host idiom, no Kafka logic.** Batching, partitioning, retries
   and config validation live only in the Rust core. The binding's own checks
   are the listed few of the root conventions (Implementation over the FFI),
   in their order: the form of the arguments, then the closed client, then
   the rest. Each has its Why and its Goes when:
   - **The form of the arguments** (`JavaForms`, unknown option keys). Why:
     one JavaScript method stands for Java's overload set, and only the
     binding sees which form was written. Goes when: never; it is the
     language mapping, not a missing FFI feature.
   - **The closed client** (`IllegalStateError` with Java's message). Why:
     `close()` frees the native handle, so the check also guards memory.
     Goes when: the native object outlives `close()` and the core reports the
     closed state itself.
   - **Run-time types and a missing topic.** Why: TypeScript's types are not
     checked at run time, and a wrong JavaScript type has no C form to cross
     as. Goes when: never, for the same reason.
   - **A negative partition or timestamp.** Why: the C record constructors
     read a negative value as Java's `null`, so the core cannot see it. Goes
     when: the FFI carries "absent" separately from the value.
   - **A negative timeout.** Why: the FFI reads a negative `timeout_ms` as
     the untimed form. Goes when: the FFI carries "no timeout" separately and
     the core checks the sign.
   - **Config key and value types**, and the refusal of the serde config
     keys. Why: the FFI takes every config value as a C string, so only the
     binding sees the value's type; the serde keys name classes (root §2).
     Goes when: the FFI takes typed values.
   - **A NUL byte in a string or bytes that cross as a C string** (the topic,
     the header keys, the config keys and values: `js_string_dup` and
     `js_bytes_cstring` in `native/record.c`), a `TypeError` "... without null
     bytes". Why: every C reader stops at the first NUL, so the value would be
     truncated silently at the ABI. Goes when: these values cross the ABI with
     a length, as `kafka_Bytes_t` does.

   So `close({timeout})` on a closing or closed producer
   returns that close before it reads its timeout; on an open one, a negative
   timeout is rejected by the binding before `close()` changes any state
   (although the core checks it too): the core's verdict is queued and
   reaches JavaScript only at the next drain, after `close()` has marked the
   producer closed, so a `send()` or `close()` in between would see a
   producer closing that then stays open.
2. **The notify hook only signals.** `cbq_notify` (`native/callback_queue.c`)
   calls `napi_call_threadsafe_function(..., napi_tsfn_nonblocking)` and
   nothing else, under the client queue's `notify_mtx` and only while
   `notify_open`. It may fire on any thread, including the JS thread inside
   `send_with_callback_cb`. Replacing the hook does not wait for a call in
   flight, so the JS thread clears `notify_open` under that lock before the
   threadsafe function is released or once it is finalized; no hook call can
   then reach a deleted threadsafe function.
3. **One drainer per client.** `kafka_producer_Producer__execute_callbacks_err`
   runs only on the JS main thread, in one drain loop (looped while it returns
   > 0, `cbq_drain`) run from the threadsafe function's `call_js` or from
   `send()`'s pump (below), and drains never nest: a drain started inside a
   drain (a `send()` from a delivery callback) or once destroy began is a
   no-op. The binding never calls `_destroy` from inside a drain: a close
   completion only marks the producer for destruction, and the drain destroys
   it after its loop.

   **Cooperative scheduling.** The rule is the root conventions' (Threads and
   callbacks): every 64th send that is accepted at once, and the first send
   after one that had to wait, runs the producer's queued callbacks before
   sending, and every 1000th send yields to the event loop with
   `setImmediate` (Python's `AsyncProducer` `_PUMP_EVERY` /
   `_FAST_PATH_YIELD_EVERY`). Why: a `send()` the producer takes at once
   settles its Promise inside the call, and awaiting a settled Promise runs
   only microtasks, so a loop awaiting such sends would never return to the
   event loop: no timer, no I/O, no `call_js`, and every record's delivery
   state would pile up until the loop ended. In this binding the count is of
   the sends whose hand-over completed inside the call since this producer's
   last deferral turn (its own `setImmediate`; other event-loop turns and
   `call_js` do not restart it) or since a send whose hand-over had to wait,
   which restarts it, as Python's does, so the next send runs the queued
   callbacks; the yield settles the sends' Promises after one event-loop
   turn, from a single `setImmediate` that settles them in order and restarts
   the count, while a send settling during that turn, queued or taken at
   once, settles in it too, behind the sends already deferred. The record
   itself is handed over at once in every case (rule 7): only the resolution
   waits, so a deferred `send()` may settle after its record's delivery
   callback ran.
4. **Waiting calls use the `_cb` twin** (root conventions, Futures and
   waiting): `send_with_callback_cb`, `flush_cb`, `close_cb`,
   `close_with_timeout_cb`. The blocking forms would block the event loop and
   run callbacks on the caller's thread. One exception, the environment
   teardown's force-close (below): `producer_force_close` calls the blocking
   `kafka_producer_Producer_close_with_timeout(view, 0, ...)` once, only while
   the handle is finalized, where no JS runs and no event-loop turn will come
   (`test/static/header_conformance.test.js` pins it there). The root
   conventions' "from JavaScript, the binding calls only `_cb` forms" does not
   name this call, which no JavaScript makes; the cleanup hook it belongs to is
   not decided yet in the root conventions.
5. **Never call `KafkaFuture_get`.** The `kafka_common_KafkaFuture_t*` a send's
   hand-over completion receives is destroyed unread; the delivery result comes
   from the record's `kafka_producer_Callback_t`.
6. **One `Callback_t` per record.** In `on_completion` the order is: run the
   user callback; complete the record's `KafkaFuture`; call
   `__set_callback_result_err(view, id, NULL, err)` exactly once, `err` NULL or
   an owned `kafka_common_Error_local_callback` error carrying what the
   callback threw. No report is sent once destroy has begun (`_destroy`
   completes the unreported ones itself). A callback may return a Promise
   (root conventions, Threads and callbacks): the binding wraps any result
   but `undefined` (Java's `void`, reported at once, the cheap common case)
   with `Promise.resolve(result)`, and the future completes, and the one
   report is made, only once that settled (`send_defer` hands the Promise a function
   owning the record's context, as both its handlers, `callback_id` kept in C),
   a rejection reported as a throw. The function is held by a strong reference
   until it reports, so a garbage collection never reports a Promise early,
   even one nothing references: a Promise that never settles keeps its record
   pending until the environment's teardown. Before a producer is closed or
   destroyed by its environment's teardown, a callback Promise still pending is
   reported as returned (`send_report_awaiting`): the force-close waits for
   every report, and no JS will settle it.
7. **No binding-side send queue, buffer, cap or lock** (PLAN D8; root
   conventions, Threads and callbacks: no tracking and no async locks of the
   binding's own). The core does NOT serialise a caller that does not wait: a
   record it cannot take at once is queued and may be overtaken by a later
   one, and `flush` / `close` do not wait for it
   (`rust/src/ffi/producer/mod.rs`, module docs on `send_cb`). Ordering is put
   on the caller instead, as the FFI says: `send()` resolves at the hand-over
   completion (inline inside `send_with_callback_cb`, or later), where Java's
   blocking `send()` returns, so a caller that awaits each `send()` meets the
   FFI's "wait for the hand-over before the next send" contract. Sends not
   awaited one by one are concurrent, like Java sends on several threads; this
   is documented on `send`, in its compiled declaration and in the README. The
   binding never holds records back itself.
8. **One copy of the app's key and value; strings encoded as Java encodes
   them.** The copy is the root conventions' (Serialization, and the FFI table
   of Implementation over the FFI): the binding copies each key and value the
   app hands over as a `Buffer` or `Uint8Array`, one a serializer returns
   included, and frees the copy when the record completes; bytes the binding
   makes itself skip the copy; header values are always copied. Here the addon
   makes the copy (`native/record.c` `js_bytes_copy`, one allocation for
   both) into memory it owns, which the core reads as a `kafka_Bytes_t`; a
   string's bytes, the binding's own Buffer, cross without a copy (kept by a
   `napi_ref` until the record completes; `producerSend`'s `owned` bits say
   which). The addon frees the copy, and drops those references, when the
   record completes: in its delivery callback (`send_on_completion`, which
   also runs at close and at environment teardown), or at a failed hand-over;
   a record that cannot be built frees it at once; each path frees once
   (`send_ctx_release_bytes` clears the pointer). **(root conflict)** The
   core copies at append, so freeing at the hand-over completion would be
   safe and use less memory (a review verified it); the code follows the root
   text. The user-facing contract,
   documented wherever `send` is: the caller may change, reuse or detach its
   bytes as soon as `send()` returns. `src/` copies no bytes itself (that
   would be a second copy). Header values are handed over during the call,
   in which the core copies them: the C API keeps no borrowed header value.

   Strings follow the root conventions (Types, the paragraph after the
   table): one function, `encodeLikeJava(s)` in `src/internal/encode.ts`, is
   the only place the binding turns a string into bytes, writing each lone
   surrogate as `?` as Java's `String.getBytes(UTF_8)` does (Node and
   Node-API write U+FFFD). A key or value given as a string, the headers code
   for header keys, and the config code once per client, for the config keys
   and values (group IDs among them), call it, and the addon reads those
   bytes into C strings (`native/record.c` `js_bytes_cstring`); topic names
   skip it and cross as JS strings. What plain JavaScript
   gets for a string key or value without a serializer is not decided yet in
   the root conventions; the producer now encodes it through the same rule.
   `check-one-copy` and `check-string-encoding` partly enforce this (The rule
   linter, below). Not checked: which strings go through `encodeLikeJava`,
   when the copy is freed, and copies in other `native/` files.
9. **Ownership follows the signature.** Errors from `out_err` and from `_cb`
   completions are owned: copied into a JS error (`src/internal/errors.ts`),
   then destroyed, before the Promise settles (root conventions, Errors). An
   error with nowhere else to go (a failed `__set_callback_result`, a failed
   destroy of a handle, a JS exception the addon would clear) is handed to
   `process.emitWarning` through the `warn` function `src/internal/native.ts`
   registers (`error_warn`, `js_warn_thrown`) wherever JS can run; at an
   environment teardown, where none can, it is only freed. The
   error and metadata a delivery callback receives are borrowed: copied,
   never destroyed. If a send fails at hand-over, its per-record context stays
   on the producer's orphan list until after destroy (the delivery callback
   may still fire, once).
10. **Teardown order:** refuse new calls (JS side, synchronously) → `close_cb`
    → its completion → `KafkaProducer_destroy_err` on the JS thread, outside a
    drain → release the threadsafe function, whose finalizer frees the notify
    opaque.

## Threads and liveness

The rules are the root conventions' (Threads and callbacks, Lifetime). In this
binding:

- Every callback the core queues (delivery callbacks, hand-over completions,
  `flush` / `close` completions) runs on the JS thread that owns the client,
  inside the drain, so JS values are created there directly.
- The threadsafe function is `ref`'d while any send, flush or close is
  outstanding (an awaited callback's Promise included) and `unref`'d when the
  count reaches zero: an idle producer does not keep the process alive, one
  with pending work does (whether a client keeps Node running while work is
  pending: not decided yet in the root conventions).
- There is no garbage-collection finalizer: the native handle object is held
  by a strong reference from its creation until the producer is destroyed
  (`cbq_attach` / `cbq_release`), so a producer never closed keeps running
  until its environment ends, even once the `KafkaProducer` object is
  collected, with a warning (root conventions, Lifetime). The binding warns
  once, when the event loop is about to empty (`process` `'beforeExit'`), with
  the number of producers whose `close()` was never called
  (`src/internal/lifecycle.ts`, code `CONFLUENT_KAFKA_PRODUCER_NOT_CLOSED`);
  an explicit `process.exit()` or `worker.terminate()` skips it (when and how
  this warning is logged: not decided yet in the root conventions).
- The environment's teardown (a worker ending, the main thread's event loop
  ending) finalizes the handle object of a producer not destroyed, and that
  finalizer destroys it, running no JS (clients in worker threads and the
  cleanup hook: not decided yet in the root conventions): it first reports
  every callback Promise still pending as returned (`send_report_awaiting`);
  then it replaces the notify hook with a no-op, so the pending callbacks only
  free their contexts; then it force-closes the producer,
  `close(Duration.ZERO)`: destroy would otherwise close an unclosed producer
  gracefully itself, blocking the JS thread up to `max.block.ms` +
  `delivery.timeout.ms` (60 s + 120 s, three minutes by default) for records
  no broker may take, so a `worker.terminate()` would stall. Unsent records
  fail instead, as soon as the I/O task leaves a wait it is in (for a node's
  connection, up to `socket.connection.setup.timeout.max.ms`), as with Java's
  `close(Duration.ZERO)`, whose Sender waits the same way; their delivery
  callbacks run, inline, without JS, and still report through
  `__set_callback_result` (`destroying` is set only after the close).
  `await producer.close()` is unaffected: a completed close leaves nothing for
  the finalizer to do.
- The preview warning follows the root conventions (Implementation over the
  FFI): `process.emitWarning` with the type `ExperimentalWarning`, once per
  process, with no code (the root's invented names list only deprecation
  codes), silenced by `--no-warnings`; its text is not decided yet in the
  root conventions. Debug lines use `util.debuglog('confluent_kafka')`
  (`src/internal/debug.ts`): the addon's load, the producer's creation,
  close and pump. The main thread and the
  workers share the one notice: the addon, loaded once per process and kept
  loaded (`RTLD_NODELETE`, so a worker ending does not unload it), answers
  `previewNoticeDue()` true only once.

## Java shape, JS idiom

The rules are the root conventions' (Names, Class family, Futures and
waiting, Signatures, Types, Errors, Configuration). What this binding adds:

- Supported import forms: `require`, named imports, `import * as`, and an ES
  module's default import under Node's own loader. A default import through an
  interop helper (`esModuleInterop`, Babel) is not supported: every public
  `index.ts` declares `export declare const __esModule: true;` (tsc emits the
  marker itself, the declaration no JS), so TypeScript rejects it (TS1192)
  rather than letting it read a missing `default` (`test/typing` pins this).
  The module format is not decided yet in the root conventions (the lean is
  ESM only, with `require(esm)` for CommonJS).
- Data objects (root conventions, Class family): their Java getters are public
  `readonly` fields, set once in the constructor (not getter methods, not
  `get` accessors), so a spread and `JSON.stringify` keep them
  (`structuredClone` / `postMessage` keep every field but
  `ProducerRecord.headers`, a class instance, which becomes `{}`); a field has
  its Java getter's name, `is` / `has` included (`metadata.offset`,
  `metadata.hasOffset`), and Java's `-1` and `null` stay (never `undefined`).
  The objects are not frozen (but `TopicPartition`), so
  `ProducerRecord.internalToNative` runs the constructor's checks again at
  `send()` on the fields it reads, with the constructor's errors, rejecting
  the send (a field reassigned from plain JavaScript is never sent
  unchecked). The data classes are listed in `DATA_CLASSES`
  (`scripts/lint/java_ts.js`), which `check-data-objects` reads:
  `ProducerRecord`, `RecordMetadata`, `TopicPartition` and `Node` today. **(root conflict)** The root
  conventions' list (the records, `TopicPartition`, `OffsetAndMetadata`,
  `RecordMetadata`, `Node`, the admin description classes) does not name
  `Header`, which its one-line test would admit; the code follows the list, so
  `Header` keeps Java's `key()` and `value()`. Java's `public final` instance fields are
  `readonly` fields of a frozen object (`SecurityProtocol.id`); `public static
  final` constants are static properties (`RecordMetadata.UNKNOWN_PARTITION`).
  Value classes have `toJSON()` *(js-only)*, whose output is frozen once
  released (`Header`'s writes `{ key, value }`, `Headers`' its array, an
  enum's its name); how it writes a `long` is not decided yet in the root
  conventions. `toString()` is Java's but an enum's (a fixed table for
  `toString()` is not decided yet in the root conventions). A constant Java
  `List` is a frozen array, a constant `Set` a read-only set
  (`src/internal/read_only_set.ts`) whose `add`, `delete` and `clear` throw
  `UnsupportedOperationError`.
- Headers are Java's `Header` and `Headers` (`common/header`), as the Python
  client's (whether `Headers` is a plain class or wraps the C `RecordHeaders`,
  and whether plain-object headers join the binding's own checks: not decided
  yet in the root conventions): `Header` keeps Java's methods, `key()` and
  `value()`, over private fields (`new Header({ key, value })`, the
  constructor of Java's internal `RecordHeader`), its value bytes, taken as a
  `Uint8Array` and given back as a `Buffer` over the same memory (root
  conventions, Types), so a string value throws `TypeError` asking to encode
  it, and a `toJSON()`; `Headers` keeps Java's methods, in Java's order
  (`add(header)` / `add({ key, value })`, `remove(key)`, `lastHeader(key)`,
  `headers(key)`, an iterable read when iterated, `toArray()`,
  `[Symbol.iterator]`, Java's `Iterable`), with Java's internal
  `RecordHeaders`' three constructors as one `headers` parameter (`()`,
  `(Header[])`, `(Iterable<Header>)`), its `toString()`, and a `toJSON()`
  writing its array. Where Java takes `Headers` or `Iterable<Header>`, a
  `Headers` or an iterable of `Header` is taken, as Java takes them;
  `ProducerRecord` copies what it is given into its own `Headers`, as Java's
  `new RecordHeaders(headers)` (the `Header` objects kept), so a record with
  no headers has an empty one, and a place Java keeps the given `Headers`
  would keep it as it is. The library reads a header through
  `Header.internalKeyOf` / `internalValueOf`, never through methods plain
  JavaScript may have reassigned on the instance. `send()` seals the
  record's headers (`internalSetReadOnly`, Java's `setReadOnly`) after the
  form, closed and field checks and before the hand-over; a later `add` or
  `remove` throws `IllegalStateError` "RecordHeaders has been closed.". A null
  header is Java's "Header cannot be null." (`TypeError`), a null key of
  `remove` / `lastHeader` / `headers` Java's "key cannot be null."
  (`IllegalArgumentError`).
- Enums (root conventions, Class family): a class whose constants are frozen
  instances, with `java.lang.Enum`'s `name()` and `ordinal()`, static
  `values()` and `valueOf(name)` (exact; `IllegalArgumentError` "No enum
  constant <Java canonical class name>.<name>", `TypeError` "Name is null" for
  `null`), and `toJSON()`, the name (it ignores the key `JSON.stringify`
  passes it). `toString()` returns the name too. **(root conflict)** Rule 1
  keeps Java's content, and Java's `SslClientAuth` overrides `toString()` to
  write the name in lower case; the code follows the root text, so
  `SslClientAuth.REQUIRED.toString()` is `'REQUIRED'`, marked *(deviation)*
  on the method. Java's `SecurityProtocol.name` field and `name()` are the
  one method.
- Signatures (root conventions, Signatures): one Java overload set is one
  method taking one options object of Java's parameter names
  (`new KafkaProducer({configs})`, `send({record, callback})`,
  `close()` / `close({timeout})`, `new ProducerRecord({topic, value})` up to
  `{topic, partition, timestamp, key, value, headers}`), with one TypeScript
  overload signature per Java form (`send(options: { record; callback })` and
  `send(options: { record; callback?: never })`, `close()` and
  `close(options: CloseArgs)`, `get()` and `get(options: { timeout })`), each
  closing the other forms' names with `never`, plus one for the bare value of
  a one-parameter form (`send(record)`, `close(timeout)`, `forName(name)`,
  the configs themselves); each option key is documented on its options type.
  The given names (own enumerable keys, which is all the methods read) must
  be exactly one Java form's (`src/internal/args.ts`, Python's `java_forms`):
  a set no form has, or a name no form has, throws `IllegalArgumentError`
  "<m>() takes one of (<a>, <b>), (<c>); got (<given>)" (a constructor's `<m>`
  is its class name). A bare value picks its form by its type, each
  one-parameter form's test (`bareTypes`: `typeof`, `Array.isArray`,
  `instanceof`, `null` where Java takes a reference), and one that fits no
  form throws the same message ending in Python's form, `got a string`, `got
  a number`, `got an array`, `got null`, `got a Map`. `null` is Java's `null`
  and given; `undefined` throws the root conventions' `TypeError` naming the
  parameter, a bare `undefined` (`close(undefined)`, `flush(undefined)`,
  `get(undefined)`) and `undefined` inside the headers included, except a
  config value, which is dropped (`ProducerRecord`'s `value`: "value must not
  be undefined. Pass null for Java's null, which sends a tombstone."). A
  method passes `arguments.length` to the matcher: more than one argument is
  the form error "got 2 arguments, not one object of named parameters", so
  Java's `send(record, callback)` is refused, never run without its callback;
  a method of no parameter (`flush()`) refuses any argument with the form
  error, and a method that only reads (`isDone()`, `name()`, `values()`,
  `toArray()`, `key()`: the `reader` option) with `TypeError` "<m>() only
  reads: it takes no argument". `toString()` is
  JavaScript's protocol and takes none. In TypeScript a bare value whose type
  is a type variable cannot tell a plain object from an instance of the
  caller's class, so its signature takes only what no plain object can be
  (`BareValue`: primitives, `null`, arrays, functions, `Map`, `Set`, `Date`,
  `ArrayBuffer`, `DataView`, typed arrays): `completedFuture<U>(value: U,
  ...check: BareCheck<U>)` infers `U` from the value alone and refuses any
  other `U` through the rest parameter, which no call gives, and a member of a
  class's `T` takes `Bare<T>` (`getNow(valueIfAbsent: Bare<T | null>)`); any
  other object goes by name there, while plain JavaScript takes it bare.
- Serializers (root conventions, Serialization): `Serializer` is an object
  type in `common/serialization`, run in JavaScript inside `send()`
  (`src/internal/serde.ts`), key then value, after the form, closed and
  record checks and before the pump and the hand-over; a thenable result is
  awaited, and the producer is checked for closed again after the await, as
  its native producer may be gone. A null serializer, or none, sends the
  record's own bytes (a string encoded by `encodeLikeJava`, checked at
  `send()`: a `ProducerRecord<K, V>` holds any key and value). TypeScript
  infers `null`, not `never`, for a null key or value, so `send` takes a
  `ProducerRecord<K | null, V | null>`.
- A Java `Duration` is the root's `Duration` (root conventions, Signatures): a
  number of milliseconds, or a `Temporal.Duration` (built in from Node 26, a
  polyfill's before), recognised by its `Symbol.toStringTag` and read with
  `total({ unit: 'millisecond' })`, typed by its structure. One private helper
  converts them all (`src/internal/duration.ts`), as the root conventions say:
  rounded down; `Infinity` is Java's `Long.MAX_VALUE` milliseconds, handed to
  the addon exactly as a bigint, and a timeout left out crosses as `-1`
  (`close()`, which the addon sends to `close_cb`, Java's untimed form); a
  negative one is Java's "The timeout cannot be negative."
  (`IllegalArgumentError`) for `close`; one with years, months or weeks, `NaN`
  and above `Number.MAX_SAFE_INTEGER` ms are a plain `RangeError`; `null` is
  `TypeError('timeout must not be null')`, and any other value `TypeError`. `-Infinity`, which the root conventions do not name, is
  `RangeError`. A wait's timeout (`KafkaFuture.get(timeout)`, Java's
  `CompletableFuture.get`) is read by `waitDurationToMillis` instead: a
  negative one is no error but no wait, as Java's times out at once, and its
  range is ±`Number.MAX_SAFE_INTEGER` ms (`RangeError` beyond); a `get` longer
  than a Node timer's 2^31 - 1 ms waits in steps of at most that (how
  `KafkaFuture.get({ timeout })` waits: not decided yet in the root
  conventions).
- Promises (root conventions, Futures and waiting): a method returns a Promise
  if and only if its entry point has a `_cb` twin, and a method returning a
  Promise never throws: every failure, the argument and form checks and the
  closed check included, rejects it. `send` returns
  `Promise<KafkaFuture<RecordMetadata>>` (PLAN D8), resolving at the
  hand-over, and the future's `get()` returns `Promise<RecordMetadata>`,
  settling at delivery. `KafkaFuture` is Java's, with its members in Java's
  order: static `completedFuture(value)` and `allOf(futures)` (Java's varargs:
  an array, bare or `{futures}`), `toCompletionStage()` (a `Promise` settling
  with the future, rejecting with the raw failure, the same Promise every
  call), `thenApply(function)`, `whenComplete(action)`,
  `cancel(mayInterruptIfRunning)`, `get()` and `get(timeout)` (Java's `(long,
  TimeUnit)` as one `Duration`), `getNow(valueIfAbsent)`, `isCancelled()`,
  `isCompletedExceptionally()`, `isDone()`, plus `KafkaFutureImpl`'s
  `toString()`; the nested `BaseFunction` / `BiConsumer` are type aliases on
  the class's namespace (`KafkaFuture.BaseFunction<A, B>`). One class has two
  value sources, as Python's: a record's future (`send()`'s, Java's
  `FutureRecordMetadata`: `cancel()` false, never cancelled, a timed-out `get`
  rejecting with `TimeoutError` "Timeout after waiting for <ms> ms.") and
  Java's `KafkaFutureImpl` (`completedFuture`, `allOf`, the dependants:
  `cancel()` completes the future and its own dependants with a
  `CancellationError`, never its source, and answers as Java's
  `CompletableFuture.cancel`: true when this call or an earlier one cancelled
  it on its own, false for a dependant cancelled through its source or by its
  own function; a timed-out `get` is `TimeoutError` with no message,
  `CompletableFuture`'s). `get()` and `getNow()` on a failed future reject or
  throw `ExecutionError` (Java's `ExecutionException`), the failure its `cause`
  and the cause's `String()` its message; on a cancelled one, the
  `CancellationError` itself (the root `CancellationError extends
  IllegalStateError`, Java's `CancellationException`). Any thrown value is a
  failure: one that is not an `Error` (`undefined`, `Object.create(null)`) is
  the `ExecutionError`'s cause too, its message a `String()` that cannot throw
  (`''` for `null` and `undefined`, as the `(cause)` form gives), and so is
  one whose class cannot be read (a revoked Proxy). `whenComplete`'s action
  gets the raw failure. What depends on a future runs where it completes
  (inside the delivery drain for a record), or inside the registering call
  when it is done, as `CompletableFuture` runs it. A dependant's completion is
  queued for the loop already running, as Java's `postComplete` runs a
  dependant's (no recursion, so a chain of any length completes; one waiter
  that throws stops none of the others), while an explicit completion
  (`cancel()`, the client's) finishes its own dependants, and an `allOf` over
  it, before it returns, even inside another's function. An `allOf` reads its
  sources' states, as Java's relays on their results: it completes once every
  source is done, whichever source's completion runs it, its failure the first
  in the sources' order. Nothing waits or allocates a Promise until a `get()`
  or `toCompletionStage()` (the root conventions' lazy Promise). *(Deviation
  from the one-object rule for single-method callbacks, Python's too:)*
  `thenApply`'s function and `whenComplete`'s action are called positionally,
  `function(value)` and `action(value, error)`, as Java calls `apply(a)` and
  `accept(a, b)`: Java names those parameters `a` and `b`, which carry no
  meaning as names. A `null` function or action throws `TypeError` at the
  call, where Java's dependant would fail with a `NullPointerException` when
  the source completes.
- A hand-over error (what Java's `doSend` rethrows) rejects the `send()`
  Promise and never reaches the callback; that Promise is never marked
  handled, so an un-awaited failing send surfaces as an unhandled rejection,
  as Java's `send()` would throw. Only delivery failures are kept from
  surfacing unread: a future rejects only the Promises its `get()` and
  `toCompletionStage()` returned, so a failed future nobody reads raises no
  unhandled rejection, and neither do its dependants; a `get()` Promise the
  caller drops still raises one, as any rejected Promise does.
- `Callback` is a function of one object of named parameters,
  `({ metadata, exception }) => void` (root conventions, Class family), Java's
  `onCompletion(metadata, exception)` named as Java names them (the one place
  `exception` is an identifier, allowlisted in `check-exception-word`), run
  before the future completes, as Java fires the callback before completing
  the future. A callback that returns a Promise is awaited (rule 6): the
  record's future completes, and a `flush()` or `close()` covering the record
  returns, only once it settled; a rejection is a throw, logged by the core
  and never propagated. Such a Promise must not wait for `flush()` or
  `close()` of its own producer, which wait for it (root conventions, Threads
  and callbacks: anti-patterns inside a send callback).
- `KafkaProducer` has `close()` and `[Symbol.asyncDispose]()`, which calls
  `close()`, so `await using` closes it.
- A Java `long` crosses as a JS `number` (offsets and timestamps stay below
  2^53); how `long` values are typed is not decided yet in the root
  conventions, and neither are `boolean`, `int` and the other primitives'
  rows (the proposal is `boolean` and `number`).
- Errors (root conventions, Errors): the generated error hierarchy is the
  Python client's: one class per FFI id, Java's name with its `Exception`
  suffix as `Error`, Java's `extends` chain (`SaslAuthenticationError extends
  AuthenticationError extends ApiError extends KafkaError`), in the folder of
  its Java package (`common/errors/`, `java.*` at the root). Every
  `kafka_common_Error_t` becomes an instance of the class of its FFI id
  (`src/internal/errors.ts` `fromNative`, Python's `_errors.py` `_build`: an
  unknown id is the base `KafkaError`, and `UNKNOWN_SERVER_ERROR` without
  `is_api_error` is the bare `KafkaError`, not `UnknownServerError`), built
  with the core's message and cause whatever constructor forms its class has
  (`src/internal/throwable.ts` `create`, which captures the caller's stack
  frames, as the binding's own `IllegalArgumentError` / `IllegalStateError`
  do through it). Users classify with `instanceof`, `name` the fallback, and
  there is no `code`, `errorName`, `retriable` or `apiError`. The FFI id is
  kept in the private `_ffiId`, for the binding and the test harness (root
  conventions, Errors; `check-internal-members` allows the name on the error
  classes and asks `KafkaError` for it): each generated class with an id
  declares `static readonly _ffiId`, marked `@internal` so `stripInternal`
  leaves it out of the declarations, and an error from the core whose id
  differs from its class's (an unknown id) keeps it in its own
  non-enumerable `_ffiId`. **(root conflict)** The root conventions' Class
  family names library-only members through `@internal` members named
  `internal...`, while `_ffiId` is an underscore name; the code follows the
  root text. `src/internal/errors.ts` `errorFfiId` gives the id to the gRPC
  server and the tests. The Java built-ins (`IllegalStateError`, ...)
  extend `Error`, not `KafkaError`, except `IllegalArgumentError` and
  `IndexOutOfBoundsError`, which extend `RangeError`, and `CancellationError`,
  which extends `IllegalStateError` as Java's `CancellationException` does (a
  built-in only this client has a class for: Python answers a cancelled future
  with its inner future's `CancelledError`). A value of the wrong JS type
  throws `TypeError`; a `null` Java rejects with `requireNonNull` or a bare
  dereference throws `TypeError` "<param> must not be null" ("<param> must not
  contain null" for an element, "<param> must not contain a <Class> with a null
  <field>" for an element's field), but where Java throws its own message
  ("Topic cannot be null.", "Null header keys are not permitted",
  `Enum.valueOf`'s "Name is null") the binding keeps Java's text and class.
  The constructors take one options object with `message` and `cause`, Java's
  other parameters keeping their names, and a single value read by its type
  (a string is the message, an `Error` the cause: `new KafkaError('boom')`,
  `new KafkaError(cause)`), as the root conventions say; matched by name
  through `src/internal/args.ts`, one TypeScript signature per form, each
  key documented on the options type. Each form's message is the one Java's
  constructor chain gives: the cause's `String()` for `(cause)`, as Java's is
  `cause.toString()`, or Java's own fixed text. A second argument (ES2022's
  `new Error(message, { cause })`) throws an error pointing to
  `new X({ message, cause })`; its class, `IllegalArgumentError` today, is
  not decided yet in the root conventions. Java's abstract bases (`RetriableError`, ...)
  are catch-only: constructing one throws `TypeError` (how a Java abstract
  class is presented: not decided yet in the root conventions). A user's
  subclass takes the forms of its nearest generated class. The constructors
  that take a payload, the payload getters (`TopicAuthorizationError`'s
  topics, ...), the `INSTANCE` singletons (`DisconnectError`,
  `CoordinatorNotAvailableError`) and
  `RecordDeserializationError.DeserializationExceptionOrigin` are not
  generated yet (each module's header comment says so); a class whose every
  Java constructor takes a payload (`QuotaViolationError`, ...) has a
  `protected` constructor that throws `TypeError` until then, and no payload is
  read across the FFI.
- The closed producer (root conventions, Implementation over the FFI): once
  `close()` was called (while it runs too), `send` rejects with
  `IllegalStateError` and Java's `throwIfProducerClosed` message, and so does
  `flush()` *(deviation, the Python client's: Java's `flush()` has no closed
  check, since its object outlives `close()`; here the native producer does
  not. Goes when: the native producer outlives `close()`)*. The closed check
  runs before the timeout check, so a second `close` returns the close in
  progress, or resolves, whatever its timeout; on an open producer a negative
  `timeout` is `IllegalArgumentError` "The timeout cannot be negative.", and
  such a close changes nothing (rule 1).
- Configuration (root conventions, Configuration): a plain object of Java's
  config keys (a `Map`, or another non-plain object, throws `TypeError`),
  checked as Python's `_config.py` checks it (`src/internal/config.ts`): a
  value of a key Java's `ProducerConfig` defines must have a JS type its
  `ConfigDef` type takes (the generated `src/internal/config_types.ts`; a
  string is always accepted but for `STRING` / `PASSWORD`, which must be
  strings), else `ConfigError` with Java's `parseType` message, a `PASSWORD`
  value shown as `[hidden]`; an unknown key's value must be a string, a number
  or a boolean; a `kafkaJS` block fails the value check with a pointer to the
  migration guide; a `null` for a key whose validator rejects `null` throws
  that validator's `ConfigError`; other `null`s and every `undefined` are
  dropped; a key that is not a string (a symbol) throws `ConfigError` "Key must
  be a string."; `key.serializer` and `value.serializer` throw `ConfigError`
  pointing to the constructor arguments, whatever their value. Values are
  then stringified (`true` → `"true"`, numbers in decimal, never in exponent
  form, arrays joined with `,` for a `LIST` key; a bigint by `String` for an
  integer key, while the `long` row is not decided yet in the root
  conventions), and the keys and values cross as `encodeLikeJava`'s bytes. The core
  validates them. What else happens to an unknown config key, and a warning
  for 1.x-only keys, are not decided yet in the root conventions, and neither
  is how the config classes are translated. Debug lines use
  `util.debuglog('confluent_kafka')`, shown with `NODE_DEBUG=confluent_kafka`;
  no constructor takes a logger or an error callback.

## Files and layout

The layout is the root conventions' (Layout and modules).
`test/static/layout.test.js` checks the parts of this a test can see.

- `native/`: the addon, its C sources and its build file. `binding.gyp` and
  `addon.exports.map` (the Linux export list of a static build) are here, not
  at the package root, so npm never compiles on install; `make build-addon`
  runs node-gyp in `native/`, which builds `native/build/Release/` (the build
  tool, the build target and paths and the exported symbols: not decided yet
  in the root conventions). `util.h` is the only file including the bindings
  header and holds the `out_err` macros (`FFI_ERR`, `FFI_SET`,
  `FFI_DISCARD`); `addon.c` is the entry point and registers every native
  function; `callback_queue.c` is a client's notify hook, threadsafe
  function, drain, keep-alive and teardown (each client embeds a
  `CallbackQueue` and passes its own `__execute_callbacks`); `error.c`,
  `record.c` (`ProducerRecord`, the copy of the app's key and value, and
  headers), `producer.c` (the `KafkaProducer` handle).
- `src/`: TypeScript sources only (`.ts`; no `.js`, no `.d.ts`), one folder
  per Java package (`clients` dropped), one public Java type per file,
  snake_case names. The package's own helpers are in `src/internal/` (never
  exported, never declared; `dist/internal/` is not in `exports`). Every other
  folder is a package with one import path
  (`@confluentinc/kafka-javascript/producer`,
  `@confluentinc/kafka-javascript/common/header`): its `index.ts` re-exports
  only the modules of its own folder, by name, `export { A } from './a'`
  (`export type { ... }` for a type), plus the one
  `export declare const __esModule: true;` (above); no index re-exports a child
  folder or exports a namespace, and the root `index.ts` exports only Java's
  built-in errors (Java's `java.*` packages map to the root) and `Duration`
  (`check-module-exports`). The addon is loaded, and its one-time setup done,
  in `src/internal/native.ts`, never in an `index.ts`;
  `src/internal/native_loader.ts` finds it, the platform package's
  (`@confluentinc/kafka-javascript-<platform>`) first, then the development
  build `native/build/Release/`.
- Types are TypeScript, under `strict` (`tsconfig.json`). The docs follow the
  root conventions (Documentation): TSDoc translated from the Javadoc, Java's
  contract first, at most 150% of the Javadoc's words, no Java file or line
  references, and the tags `@param` (describing the options object as a
  whole), `@returns`, `@throws` naming the error classes, `@defaultValue` for
  Java's starting values, `@deprecated` and one `@remarks` for the JavaScript
  notes, such as deviations, with no JSDoc type annotations; `@internal` marks
  a library-only member (below). A JavaScript addition is marked *(js-only)*
  and a departure *(deviation)*, with its Why, in that `@remarks`. There is no
  custom tag: an item with no Java counterpart is on the list of invented
  names, and one the naming convention cannot map is listed with its Java
  target, both in `lint-mapping.json` (The rule linter); `src/` carries no
  `@java` or `@jsOnly` tag. Each module's header comment lists the Java
  overloads it leaves out. There is no docs lint in the preview. `make build` (`build-ts`)
  compiles `src/` into `dist/` (`.js`, `.d.ts`, both with maps): CommonJS
  today (`module: node16`, the package having no `"type"`), so `require`
  loads it and an ES module `import`s it through Node's CommonJS interop,
  ES2022 (and ES2024's string methods) for Node >= 22.14.0, `engines.node`
  (the module format, the other `tsc` settings and `engines.node`: not decided
  yet in the root conventions). `dist/` is git-ignored and is what
  `package.json` points at and publishes; a `dist/internal/` module's `.d.ts`
  is removed after compiling. A Java constructor that is not public is a
  TypeScript `private` constructor (`protected` where Java's is), and so is
  one users must not call (`RecordMetadata`, whose Java constructor takes an
  untranslated `TopicPartition`); there is no run-time guard (root
  conventions, Class family). The library builds such an object through one
  static factory, `internalCreate`, and calls its own methods on public
  classes (`ProducerRecord.internalToNative`, `KafkaFuture.internalComplete`)
  with dot access: each is a public member marked `@internal` in its TSDoc
  and named `internal...`, which tsc's `stripInternal` leaves out of the
  declarations; no member is `_`-prefixed but the error classes' `_ffiId`
  (`check-internal-members`). The declarations keep each private constructor
  as `private constructor();`, without which TypeScript would assume a public
  one. `test/typing` type-checks `test/typing/usage.ts` against `dist/`'s
  declarations under `--strict` (the `@internal` members and the private
  constructors refused there) and checks no public declaration refers to a
  private module; its type assertions go through `expect-type`'s
  `expectTypeOf` (a pinned devDependency; root conventions, Tests):
  `toEqualTypeOf` for an exact type, `toExtend` for a value of a wider type,
  and `@ts-expect-error` for what must not compile.
- `package.json`: `main`, `types`, an `exports` entry (`types` + `default`) per
  package folder and its `typesVersions` all point into `dist/`, and the
  folders behind the import paths also exist in the package, for TypeScript
  setups that do not read `exports`: `pack-npm.sh` writes
  `<path>/package.json` for each import path (`check-npm.py path-folders`),
  its `main` and `types` pointing into `dist/`, and `files` names their
  top-level folders (`common/`, `consumer/`, `producer/`); the package check
  verifies them, and `test/unit/index.test.js` that a deep import fails with
  `ERR_PACKAGE_PATH_NOT_EXPORTED`; `"gypfile": false`, no install scripts,
  `files` naming `dist/` and `NOTICE` (Apache Kafka's credit line and
  copyright, as Kafka's own NOTICE asks of a derived work), with the README
  and the CHANGELOG; npm adds the README and the LICENSE on its own. Not
  published: `src/`, the build inputs, the tests, `tools/` and the
  gRPC server.
- `api/` is the interface check's baseline (root conventions, Stability):
  api-extractor's report of each import path's compiled declarations
  (`<path>.api.md`), the `exports` map (`exports.json`) and the version they
  were taken at (`baseline.json`). `make check-api` (`scripts/api_report.js`,
  run by `make test` and the root `make verify`) compares the build with it: a
  difference warns while the version or the baseline is a pre-release and in
  a major release, and fails in any other release; `make update-api` records a
  meant change, and the baseline of each release. `@microsoft/api-extractor`
  is a pinned devDependency.
- `npm/` holds one folder per platform package; `test/` and `examples/` sit
  beside it (root conventions, Layout and modules).
- Tests (root conventions, Tests): `test/static` (no build),
  `test/unit/<module>/<file>.test.js` for a Java test class, named after the
  `src/<module>/<file>.ts` it tests: the Java test class without its `Test`
  suffix, snake_cased (`KafkaProducerTest` -> `producer/kafka_producer.test.js`;
  `test/static/layout.test.js` checks it), other unit tests named after what
  they test, `test/typing`, `test/integration` (broker-backed), `test/lint`
  (the rule linter's own). The tests, the example and the gRPC server stay
  JavaScript and run against the compiled package (every target that runs
  them builds it first; `test/static` reads the sources and needs no build):
  they import the package by its own name
  (`require('@confluentinc/kafka-javascript/producer')`, Node's
  self-reference), so no install is needed; tests of private helpers, and the
  gRPC server, require `dist/internal/*.js` directly.
- The gRPC conformance server lives at `javascript/grpc-server/`
  (`grpc_server.js`, `grpc_translate.js`) and serves, over the public API,
  the `ProducerService` and the chaos harness's `ChaosWorkloadService`
  (`verifiable_clients/chaos_workload.js`), with an image on port 50054; the
  consumer and admin services come with their clients. The Rust harness runs
  it as `BackendKind::Js`, one backend, and its `JS_GRPC_SKIPS` list
  (`rust/Makefile`) gives each skip a reason and a removal condition (root
  conventions, Tests).

## Packaging

The rules are the root conventions' (Build, packaging and release). In this
binding:

- The repository's `package.json` stays `"private": true`; the published one
  is written by `.semaphore/check-npm.py manifest`: this one without `private`,
  `devDependencies` and `scripts`. The platform packages are pinned in
  `optionalDependencies` at exactly its version, with no download fallback.
  The pins are written into the published copy only: in the repository's
  `package.json`, `npm ci` would fail once the version is on the registry,
  since the lockfile cannot lock a package not yet published
  (`test/static/lockfile.test.js`). Each platform package is a folder of
  `npm/` (`npm/README.md`): its committed `package.json`, published as it is,
  with npm's `os`, `cpu` and, on Linux, `libc` set and no install scripts, and
  its README. The preview has linux-x64 (glibc) and darwin-arm64 (the
  platforms after the preview, musl and the CPU floor: not decided yet in the
  root conventions). Each platform package's `files` name `dist/` and
  `NOTICE`, the addon and `THIRD_PARTY_LICENSES.txt`, the licences of the
  crates linked into the addon. **(root conflict)** The root conventions
  (Build, packaging and release) have every JavaScript package's `files` name
  `dist/` and `NOTICE`, but a platform package has no `dist/`; the code follows
  the root text literally, so the entry is there and matches nothing. CI
  builds the licence file with cargo-about (`npm/about.toml`,
  `npm/about.hbs`, `.semaphore/third-party-licenses.sh`) from
  `rust/Cargo.lock`; npm adds the
  README and the LICENSE (each package's `LICENSE` file and `license` field:
  not decided yet in the root conventions). `test/static/layout.test.js` pins
  the repository's side.
- No package has an install script or builds at install; `native/` (with
  `binding.gyp`), `src/`, `scripts/`, `test/`, `tools/` and the gRPC
  server are never published (`check-npm.py package` checks each tarball's
  exact file list).
- The released addon links the Rust core statically
  (`CONFLUENT_KAFKA_LINK=static`, `native/binding.gyp`), needs glibc 2.28 or
  macOS 11.0 at most (the glibc floor: not decided yet in the root
  conventions), loads only the system's libraries and exports only
  `napi_register_module_v1` and `node_api_module_get_api_version_v1`
  (`check-npm.py native`). Development builds, the tests and the gRPC image
  link it dynamically, the default.
- The version follows Kafka's line, now `4.4.0-preview.1`: `"version"` in
  `package.json`, repeated in `package-lock.json` and in every
  `npm/<platform>/package.json`, which `cargo xtask bump-version` writes
  together (`make bump-version JAVASCRIPT_VERSION=...`), `-preview.N` included
  (between dev and rc in semver order), which every version check of the
  release pipeline accepts. It refuses a version whose `MAJOR.MINOR` is not
  the Kafka release of `kafka/gradle.properties`, or whose `PATCH` does not
  follow `rfc/versioning.md` (`AK_patch × 100 + core_patch × 10 +
  binding_patch`, the hundreds Kafka's patch, a digit back at 0 when one to
  its left moves up). The tag build of `javascript/v<version>` records the
  tested tarballs, and its manual "Publish to npm" promotion
  (`.semaphore/publish-npm.yml`, `.semaphore/publish-npm.sh`) publishes them
  from Semaphore with a granular npm token from Vault and no provenance. The
  dist-tag is `check-npm.py dist-tag`'s: pre-releases go under `preview` only,
  and it refuses to put one on `latest`; a release goes under `latest`; 1.x
  fixes go out under `legacy`, from the earlier client's repository (the
  publish job refuses a version below 4). There is no manual publish script.
  `javascript/` is mirrored read-only to `confluent-kafka-javascript` and
  released from `<major>.<minor>.x` branches.
- CI runs the binding on Node 22 (every suite) and on Node 24 and 26 (the
  static, unit and typing tests, which need no broker); CI on Node 22, 24 and
  26 is not decided yet in the root conventions.

## Generated files

`cargo xtask generate-error-codes` generates, from the Java sources and the
FFI enum (the Python client's generators, `rust/xtask/src/javascript_gen.rs`):
the error classes and `src/internal/error_registry.ts`, the config classes'
constants (`ProducerConfig`, `SslConfigs`, `SaslConfigs`, `SecurityConfig`)
and `src/internal/config_types.ts`, all TypeScript (typed: a config
constant's type is written out, `static readonly X: string = '...'`), which
`make build` compiles with the rest. The root conventions (Build, packaging
and release) add the form tables, now hand-written in the modules that call
`src/internal/args.ts`. A generated file carries the license and `GENERATED,
DO NOT EDIT` naming its generator, and is never edited by hand;
`cargo xtask check-generated` fails on a stale, missing or orphaned one (a
`.ts`, or a generated `.js` left in `src/`). A folder holding only generated
modules has a generated `index.ts`; in a hand-written one (`src/`, `common/`,
`common/config/`, `producer/`) the generator owns the
`BEGIN GENERATED EXPORTS` / `END GENERATED EXPORTS` block.

## The rule linter

`make lint-rules` checks the rules of this file and of the root conventions
from the sources, as `cargo xtask lint-custom` checks the Rust crate's
(`design/current/javascript-rule-linter/PLAN.md`). It has two halves: ESLint
(`eslint.config.cjs`, the `kafka/` rules, with `eslint-baseline.txt`) runs the
rules that read TypeScript, beside typescript-eslint's recommended
type-checked rules on `src/` (`no-floating-promises` among them); the runner
(`scripts/lint_rules.js`, with `lint-baseline.txt`) runs the addon's C rules
and the other files. `make test-lint` runs the linter's own tests
(`test/lint/`): each rule's tests over a fixture, and a mutation test that
injects one known violation per rule into a copy of the real tree and expects
exactly that rule to fail.

A baseline lists findings not burnt down yet, each with a comment saying why;
a line that matches no finding any more is itself a finding. Code that does
not follow a rule yet is a finding to fix, never a new baseline line. The
mapping lists are in `lint-mapping.json`: `invented`, the list of invented
names for the exported items and public members (root conventions, Layout and
modules), each with its reason, and `javaTargets`, the Java class or member of
an item the naming convention does not find; the other invented names are
`NODE_DEBUG=confluent_kafka`, the warning code
`CONFLUENT_KAFKA_PRODUCER_NOT_CLOSED` (of the warning for a producer never
closed, whose form is not decided yet in the root conventions), and the
platform package names.

The rules, each named in its finding:

- `check-no-todo`: no unfinished-work marker in any file of the client.
- `check-exception-word`: no identifier contains the word "exception" (root
  §2, §4), Java's member names the rules keep aside.
- `check-java-line`: no comment cites a Java source line.
- `check-internal-members`: a library-only member is a public `@internal`
  member named `internal...`, called with dot access; no member is
  `_`-prefixed, but `_ffiId` on an error class, which `KafkaError` declares.
- `check-java-name`, `check-java-members`, `check-java-completeness`,
  `check-public-audience`, `check-no-deprecated-translation`: every export
  translates a public, live Java class by name (or is listed), in its file,
  every public member a Java member, and every public Java member is
  translated (an enum's `java.lang.Enum` members and `toJSON()` included).
- `check-invented-names`: every entry of `lint-mapping.json` is needed and
  resolves, and an invented options type is named `...Args`.
- `check-module-exports`: each package has one import path; an index
  re-exports only its own folder's modules, with no namespace, and the root
  exports only Java's built-in errors and `Duration`.
- `check-data-objects`: the data classes' getters are `readonly` fields, every
  other class keeps methods.
- `check-overload-shape`: one Java overload set is one method taking one
  options object of Java's parameter names, with one TypeScript overload
  signature per Java form and the bare value of a one-parameter form.
- `check-type-mapping`: each member's type is the one the Types table gives
  (a `byte[]` taken as a `Uint8Array`, returned as a `Buffer`; the rows not
  decided yet in the root conventions keep their current mapping).
- `check-waiting-call-promise`: a method returns a Promise if and only if its
  native path reaches a `_cb` twin.
- `check-native-registry`: the addon's registrations and the native handle's
  uses match, and only the loader names a `.node` file.
- `check-out-err`, `check-cb-twin-only`, `check-notify-hook`,
  `check-single-drainer`, `check-completion-order`, `check-ownership`,
  `check-no-binding-queue`: boundary rules 1 to 10, in the addon.
- `check-one-copy`: the one copy of the app's key and value (rule 8), and no
  copy of bytes in `src/`.
- `check-string-encoding`: `encodeLikeJava` in `src/internal/encode.ts` is the
  only place a string becomes bytes (no `Buffer.from(string)`, `TextEncoder`
  or string write elsewhere).
- `check-directives`, `check-config`: no inline ESLint configuration, a
  targeted and explained disable that names no `kafka/` rule, and one ESLint
  configuration.

## Files and process

- Every file carries the Apache 2.0 header, copyright Confluent Inc. (the
  header of each file is not decided yet in the root conventions). No TODO
  or FIXME.
- No Rust or C changes for the binding: a missing FFI function is reported,
  not added (root conventions, Implementation over the FFI).
- `test/static/header_conformance.test.js` asserts that every `kafka_*`
  function `native/*.c` calls is declared in the bindings header and is not a
  plain name that has an `_err` twin. Keep it passing when adding calls.
- Tests use `node --test` only (the test runner is not decided yet in the
  root conventions; the lean is `node:test`). The integration tests are
  skipped, with a visible message, unless `KAFKA_BOOTSTRAP_SERVERS` is set.
- Assert the core's exact error messages in tests.
