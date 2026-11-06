# JavaScript (Node.js) binding

The Node.js client in `javascript/` is built only on the C FFI of root
`CLAUDE.md` §4, like the Python client. It is a Node-API addon written in plain
C against `node_api.h` (`src/`), built with node-gyp, under a thin layer
written in TypeScript (`lib/`), which tsc compiles into CommonJS with its
declarations (`dist/`, the package's entry points).
This file holds the rules the Actor and the Critic apply to it; root
`CLAUDE.md` still applies wherever this file is silent. The approved design is
`design/current/javascript-producer/PLAN.md`.

## Ground truth

These decide every question, in this order:

1. The C ABI in `rust/target/include/confluent_kafka_bindings.h`, the only
   header the addon includes.
2. The Java public API (`KafkaProducer`, `ProducerRecord`, `RecordMetadata`,
   `Callback`, Apache Kafka 4.3.1 in `kafka/`).
3. Root `CLAUDE.md` §4 "C FFI Conventions".

## Boundary rules

1. **Java shape, host idiom, no Kafka logic.** Batching, partitioning, retries
   and config validation live only in the Rust core. The binding repeats no
   check the core makes; it checks only what would otherwise be undefined
   behaviour or silently misread at the ABI (a missing topic, wrong argument
   types, a negative partition or timestamp, which the C record constructors
   read as Java's `null`) and whether the producer is already closed.
   One exception: `close({timeoutMs})` rejects a negative timeout itself,
   before touching any state and in every state, although the core checks it
   too. Java's `close(Duration)` checks it before anything else, but the
   core's verdict is queued and reaches JS only at the next drain, after
   `close()` has marked the producer closed; checked only there, a `send()` or
   `close()` in between would see a producer closing that then stays open.
2. **The notify hook only signals.** `cbq_notify` (`src/callback_queue.c`) calls
   `napi_call_threadsafe_function(..., napi_tsfn_nonblocking)` and nothing
   else, under the client queue's `notify_mtx` and only while `notify_open`. It
   may fire on any thread, including the JS thread inside
   `send_with_callback_cb`. Replacing the hook does not wait for a call in
   flight, so the JS thread clears `notify_open` under that lock before the
   threadsafe function is released or once it is finalized; no hook call can
   then reach a deleted threadsafe function.
3. **One drainer per client.** `kafka_producer_Producer__execute_callbacks_err`
   runs only on the JS main thread, in one drain loop (looped while it returns
   > 0, `cbq_drain`) run from the threadsafe function's `call_js` or from
   `send()`'s pump (below), and drains never nest: a drain started inside a
   drain (a `send()` from a delivery callback) or once destroy began is a
   no-op. The binding
   never calls `_destroy` from inside a drain: a close completion only marks
   the producer for destruction, and the drain destroys it after its loop.

   **Cooperative scheduling** (Python's `AsyncProducer`
   `_FAST_PATH_YIELD_EVERY` / `_PUMP_EVERY`): a `send()` the producer takes at
   once settles its Promise inside the call, and awaiting a settled Promise
   runs only microtasks, so a loop awaiting such sends would never return to
   the event loop: no timer, no I/O, no `call_js`, and every record's delivery
   state would pile up until the loop ended. So `send()` counts the sends
   whose hand-over completed inside the call since this producer's last
   deferral turn (its own `setImmediate`, below; other event-loop turns do not
   restart the count, and `call_js` does not either): every 64th runs the
   drain on entry, before its record is handed over, and once 1000 are
   counted, every further one (whichever the caller awaits: a loop may fire
   some sends without awaiting them) settles its Promise only after one
   event-loop turn, from a single `setImmediate` that settles them in order
   and restarts the count. A queued hand-over does not restart it: its
   completion may run in another `send()`'s pump, with no turn. While that
   deferral turn is pending, a queued hand-over that completes settles in it
   too, behind the sends already deferred. The record itself is handed over at
   once in every case (rule 7): only the resolution waits, so a deferred
   `send()` may settle after its record's delivery callback ran.
4. **Waiting calls use the `_cb` twin.** `send_with_callback_cb`, `flush_cb`,
   `close_cb`, `close_with_timeout_cb`. The blocking forms would block the
   event loop and run callbacks on the caller's thread. One exception, the
   finalizer's force-close (below): `producer_force_close` calls the blocking
   `kafka_producer_Producer_close_with_timeout(view, 0, ...)` once, only while
   the handle is finalized, where no JS runs and no event-loop turn will come
   (`test/static/header_conformance.test.js` pins it there).
5. **Never call `KafkaFuture_get`.** The `kafka_common_KafkaFuture_t*` a send's
   hand-over completion receives is destroyed unread; the delivery result comes
   from the record's `kafka_producer_Callback_t`.
6. **One `Callback_t` per record.** In `on_completion` the order is: run the
   user callback; complete the record's `KafkaFuture`; call
   `__set_callback_result_err(view, id, NULL, err)` exactly once, `err` NULL or
   an owned `kafka_common_Error_local_callback` error carrying the message the
   callback threw. No report is sent once destroy has begun (`_destroy`
   completes the unreported ones itself).
7. **No binding-side send queue, buffer, cap or lock** (PLAN D8). The core
   does NOT serialise a caller that does not wait: a record it cannot take at
   once is queued and may be overtaken by a later one, and `flush` / `close`
   do not wait for it (`rust/src/ffi/producer/mod.rs`, module docs on
   `send_cb`). Ordering is put on the caller instead, as the FFI says: `send()`
   resolves at the hand-over completion (inline inside
   `send_with_callback_cb`, or later), where Java's blocking `send()`
   returns, so a caller that awaits each `send()` meets the FFI's "wait for
   the hand-over before the next send" contract. Sends not awaited one by one
   are concurrent, like Java sends on several threads; this is documented on
   `send`, in its compiled declaration and in the README. The binding never
   holds records back itself.
8. **Zero extra copies of key and value** (PLAN D9). A `Buffer` /
   `Uint8Array` crosses as a `kafka_Bytes_t` over its own memory; the addon
   keeps a `napi_ref` on it until the hand-over completion fires (the bytes
   are borrowed until then). The user-facing contract, documented wherever
   `send` is: a caller's `Buffer` / `Uint8Array` must not be modified or
   detached until `send()` resolves, and may be reused after. A string is
   encoded to UTF-8 once (`Buffer.from`) into a Buffer of the binding's own,
   the equivalent of Java's `StringSerializer`, so it is unaffected. Header
   values are copied by the core during the call.
9. **Ownership follows the signature.** Errors from `out_err` and from `_cb`
   completions are owned: copied into a JS error (`lib/_errors.ts`), then destroyed. The
   error and metadata a delivery callback receives are borrowed: copied, never
   destroyed. If a send fails at hand-over, its per-record context stays on the
   producer's orphan list until after destroy (the delivery callback may still
   fire, once).
10. **Teardown order:** refuse new calls (JS side, synchronously) → `close_cb`
    → its completion → `KafkaProducer_destroy_err` on the JS thread, outside a
    drain → release the threadsafe function, whose finalizer frees the notify
    opaque.

## Threads and liveness

- Every callback the core queues (delivery callbacks, hand-over completions,
  `flush` / `close` completions) runs on the JS main thread, inside the drain,
  so JS values are created there directly.
- The threadsafe function is `ref`'d, and the native handle object held by a
  strong reference, while any send, flush or close is outstanding, and both
  are `unref`'d when the count reaches zero: an idle producer neither keeps the
  process alive nor is kept from garbage collection; one with pending work
  keeps the process alive.
- A producer collected or torn down without `close()` is destroyed by its
  finalizer, which first replaces the notify hook with a no-op and runs no JS:
  the pending callbacks then only free their contexts. Before the destroy it
  force-closes the producer, `close(Duration.ZERO)` (D11): destroy would
  otherwise close an unclosed producer gracefully itself, blocking the JS
  thread up to `max.block.ms` + `delivery.timeout.ms` (60 s + 120 s, three
  minutes by default) for records no broker may take, so a
  `worker.terminate()` would stall. Unsent records fail instead, as soon as
  the I/O task leaves a wait it is in (for a node's connection, up to
  `socket.connection.setup.timeout.max.ms`), as with Java's
  `close(Duration.ZERO)`, whose Sender waits the same way; their delivery
  callbacks run, inline, without JS, and still report through
  `__set_callback_result`
  (`destroying` is set only after the close). `await producer.close()` is
  unaffected: a completed close leaves nothing for the finalizer to do.

## Java shape, JS idiom

- Supported import forms: `require`, named imports, `import * as`, and an ES
  module's default import under Node's own loader. A default import through an
  interop helper (`esModuleInterop`, Babel) is not supported: every public
  `index.ts` declares `export declare const __esModule: true;` (tsc emits the
  marker itself, the declaration no JS), so TypeScript rejects it (TS1192)
  rather than letting it read a missing `default` (`test/typing` pins this).
- Class and member names follow Java (`KafkaProducer`, `ProducerRecord`,
  `RecordMetadata`, `send`, `flush`, `close`); methods are camelCase.
- Java getters are read-only properties named by Python's rule: a leading `get`
  is dropped, `is` / `has` stay (`metadata.offset`, `metadata.hasOffset`).
  `public static final` constants are static properties
  (`RecordMetadata.UNKNOWN_PARTITION`).
- One Java overload set is one function. Overloaded constructors take one
  options object (`new ProducerRecord({topic, partition, timestamp, key, value,
  headers})`), `close()` / `close(Duration)` is `close({timeoutMs}?)`.
- A blocking Java method returns a `Promise` (`void` ones `Promise<void>`).
  `send` blocks in Java and returns a `Future`, so it is two-step (PLAN D8):
  `send()` returns `Promise<KafkaFuture<RecordMetadata>>`, resolving at the
  hand-over, and the future's `get()` returns `Promise<RecordMetadata>`,
  settling at delivery. `KafkaFuture` is Java's: `get()`, `isDone()`,
  `isCompletedExceptionally()`, `isCancelled()` (false) and `cancel()`
  (false, as `FutureRecordMetadata`).
- A hand-over error (what Java's `doSend` rethrows) rejects the `send()`
  Promise and never reaches the callback; that Promise is never marked
  handled, so an un-awaited failing send surfaces as an unhandled rejection,
  as Java's `send()` would throw. Only delivery failures are kept from
  surfacing unread: a future rejects only the Promises its `get()` returned,
  so a failed future nobody reads raises no unhandled rejection.
- `Callback` is a function `(metadata, error) => void`, run before the
  future completes, as Java fires the callback before completing the future.
- Java `null` is `null` (`undefined` accepted on input). A Java `long` crosses
  as a JS `number` (offsets and timestamps stay below 2^53).
- Errors: the generated error hierarchy (below) is the Python client's:
  one class per Java exception class, Java's name with its `Exception` suffix
  as `Error`, Java's `extends` chain (`SaslAuthenticationError extends
  AuthenticationError extends ApiError extends KafkaError`), in the folder of
  its Java package (`common/errors/`, `java.*` at the root). Every
  `kafka_common_Error_t` becomes an instance of the class of its FFI id
  (`lib/_errors.ts` `fromNative`, Python's `_errors.py` `_build`: an unknown
  id is the base `KafkaError`, and `UNKNOWN_SERVER_ERROR` without
  `is_api_error` is the bare `KafkaError`, not `UnknownServerError`). `name` is
  the class name (JavaScript's convention); `code` (the `kafka_common_Error_e`
  value), `errorName` (its enumerator name, e.g. `TOPIC_AUTHORIZATION_FAILED`),
  `retriable`, `apiError`, `message` (the core's text, unchanged) and `cause`
  (the `Error_source` chain) are kept on every class, the Java built-ins
  included. The Java built-ins (`IllegalStateError`, `IllegalArgumentError`,
  ...) extend `Error`, not `KafkaError`, as Java's do not extend
  `KafkaException`; a check the binding makes where Java throws
  `IllegalArgumentException` / `IllegalStateException` throws them with Java's
  message. A value of the wrong JS type throws `TypeError`. Java's abstract
  bases (`RetriableError`, ...) are catch-only: constructing one throws
  `TypeError`. *(Deviation from the Python classes:)* every class takes
  JavaScript's `(message?, {cause}?)`, not Java's constructor forms; the
  payload getters (`TopicAuthorizationError`'s topics, ...), the `INSTANCE`
  singletons (`DisconnectError`, `CoordinatorNotAvailableError`) and
  `RecordDeserializationError.DeserializationExceptionOrigin` are not
  generated, and no payload is read across the FFI.
- After a completed `close()`, `send` throws `LOCAL_ILLEGAL_STATE` with Java's
  `throwIfProducerClosed` message, and `flush()` resolves at once: Java's
  `flush()` has no closed check and returns immediately on the empty
  accumulator a completed close leaves. A negative `timeoutMs` is rejected
  by the binding first, in every state (rule 1's exception), with
  `LOCAL_ILLEGAL_ARGUMENT` and Java's message "The timeout cannot be
  negative.", as Java's `close(Duration)` throws `IllegalArgumentException`
  before anything else: such a close changes nothing, and an open producer
  stays usable.
- Configuration is a plain object (or a `Map`) of Java's config keys, checked
  as Python's `_config.py` checks it (`lib/_config.ts`): a value of a key
  Java's `ProducerConfig` defines must have a JS type its `ConfigDef` type
  takes (the generated `lib/_config_types.ts`; a string is always accepted
  but for `STRING` / `PASSWORD`, which must be strings), else `ConfigError`
  with Java's `parseType` message, a `PASSWORD` value shown as `[hidden]`; a
  `null` for a key whose validator rejects `null` throws that validator's
  `ConfigError`; other `null`s and every `undefined` are dropped; a key that
  is not a string (a `Map`'s) throws `ConfigError` "Key must be a string.". Values are
  then stringified (`true` → `"true"`, numbers and bigints by `String`, arrays
  joined with `,`). The core validates them.

## Files and layout

`test/static/layout.test.js` checks the parts of this a test can see.

- `src/`: `util.h` is the only file including the bindings header and holds
  the `out_err` macros (`FFI_ERR`, `FFI_SET`, `FFI_DISCARD`); `addon.c` is the
  entry point and registers every native function; `callback_queue.c` is a
  client's notify hook, threadsafe function, drain, keep-alive and teardown
  (each client embeds a `CallbackQueue` and passes its own
  `__execute_callbacks`); `error.c`, `record.c` (`ProducerRecord` and
  headers), `producer.c` (the `KafkaProducer` handle).
- `lib/`: TypeScript sources only (`.ts`; no `.js`, no `.d.ts`), one public
  Java type per file, snake_case names, `_` prefix for private files (never
  exported, never declared). Every folder is a module: its `index.ts` only
  re-exports, `export { A } from './a'` (`export type { ... }` for a type) for
  its public modules and `export * as errors from './errors'` for its child
  folders (the namespaces, `common.errors`), plus the one
  `export declare const __esModule: true;` (above); the root `index.ts` exports the
  namespaces (`producer`, `common`, `consumer`) and the root's own modules,
  the Java built-in errors (Java's `java.*` packages map to the root, as in the
  Python client). The addon is loaded, and its one-time setup done, in
  `lib/_native.ts`, never in an `index.ts`. Java getters stay properties.
- Types are TypeScript, under `strict` (`tsconfig.json`); the prose docs are
  TSDoc comments (`@param name -`, `@returns`, `@throws` with the error class in
  backquotes), with no JSDoc type annotations. `make build` (`build-ts`)
  compiles `lib/` into `dist/` (`.js`, `.d.ts`, both with maps): CommonJS
  (`module: node16`, the package having no `"type"`), so `require` loads it and
  an ES module `import`s it through Node's CommonJS interop, ES2022 for
  Node >= 18. `dist/` is git-ignored and is what `package.json` points at and
  publishes; a private module's `.d.ts` is removed after compiling. A `_`
  member another class calls is TypeScript `private` and called with bracket
  access (`record['_toNative']()`), which TypeScript's `private` allows, so it
  is declared `private`; a private constructor the native layer calls
  (`RecordMetadata`) is reached through a cast in `lib/_native.ts`.
  `test/typing` type-checks `test/typing/usage.ts` against `dist/`'s
  declarations under `--strict` and checks no public declaration refers to a
  private module.
- `package.json`: `main`, `types`, an `exports` entry (`types` + `default`) per
  module folder and its `typesVersions` all point into `dist/`;
  `"gypfile": false`, no install scripts, `files` the compiled `dist/` (not
  `lib/`, the build inputs, the tests or the gRPC harness).
- Tests: `test/static` (no build),
  `test/unit/<module>/<file>.test.js` for a Java test class, named after the
  `lib/<module>/<file>.ts` it tests: the Java test class without its `Test`
  suffix, snake_cased (`KafkaProducerTest` -> `producer/kafka_producer.test.js`;
  `test/static/layout.test.js` checks it), other unit tests
  named after what they test, `test/typing`,
  `test/integration` (broker-backed). The tests, the example and the gRPC
  server stay JavaScript and run against the compiled package (every target
  that runs them builds it first; `test/static` reads the sources and needs no
  build): they import the package by its own name
  (`require('@confluentinc/kafka-javascript/producer')`, Node's
  self-reference), so no install is needed; tests of private helpers, and the
  gRPC server, require `dist/_*.js` directly.

## Generated files

`cargo xtask generate-error-codes` generates, from the Java sources and the
FFI enum (the Python client's generators, `rust/xtask/src/javascript_gen.rs`):
the error classes and `lib/_error_registry.ts`, the config classes'
constants (`ProducerConfig`, `SslConfigs`, `SaslConfigs`, `SecurityConfig`)
and `lib/_config_types.ts`, all TypeScript (typed: a config constant's type is
written out, `static readonly X: string = '...'`), which `make build` compiles
with the rest. A generated file carries the license and `GENERATED, DO NOT
EDIT` naming its generator, and is never edited by hand;
`cargo xtask check-generated` fails on a stale, missing or orphaned one (a
`.ts`, or a generated `.js` left in `lib/`). A folder holding only generated
modules has a generated `index.ts`; in a hand-written one (`lib/`, `common/`,
`common/config/`, `producer/`) the generator owns the
`BEGIN GENERATED EXPORTS` / `END GENERATED EXPORTS` block.

## Files and process

- Every file carries the Apache 2.0 header, copyright Confluent Inc. No TODO
  or FIXME.
- No Rust or C changes for the binding: a missing FFI function is reported,
  not added.
- `test/static/header_conformance.test.js` asserts that every `kafka_*`
  function `src/*.c` calls is declared in the bindings header and is not a plain name
  that has an `_err` twin. Keep it passing when adding calls.
- Tests use `node --test` only. The integration tests are skipped, with a
  visible message, unless `KAFKA_BOOTSTRAP_SERVERS` is set.
- Assert the core's exact error messages in tests.
