# JavaScript (Node.js) binding

The Node.js client in `javascript/` is built only on the C FFI of root
`CLAUDE.md` §4, like the Python client. It is a Node-API addon written in plain
C against `node_api.h` (`src/`), built with node-gyp, under a thin CommonJS
layer (`lib/`) with a hand-written `index.d.ts`. This file holds the rules the
Actor and the Critic apply to it; root `CLAUDE.md` still applies wherever this
file is silent. The approved design is
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
2. **The notify hook only signals.** `producer_callbacks_notify` calls
   `napi_call_threadsafe_function(..., napi_tsfn_nonblocking)` and nothing
   else. It may fire on any thread, including the JS thread inside
   `send_with_callback_cb`.
3. **One drainer per client.** `kafka_producer_Producer__execute_callbacks_err`
   runs only on the JS main thread, in the threadsafe function's `call_js`,
   looped while it returns > 0. The binding never calls `_destroy` from inside
   a drain: a close completion only marks the producer for destruction, and
   `call_js` destroys it after its drain loop.
4. **Waiting calls use the `_cb` twin.** `send_with_callback_cb`, `flush_cb`,
   `close_cb`, `close_with_timeout_cb`. The blocking forms would block the
   event loop and run callbacks on the caller's thread.
5. **Never call `KafkaFuture_get`.** The `kafka_common_KafkaFuture_t*` a send's
   hand-over completion receives is destroyed unread; the delivery result comes
   from the record's `kafka_producer_Callback_t`.
6. **One `Callback_t` per record.** In `on_completion` the order is: run the
   user callback; settle the record's Promise; call
   `__set_callback_result_err(view, id, NULL, err)` exactly once, `err` NULL or
   an owned `kafka_common_Error_local_callback` error carrying the message the
   callback threw. No report is sent once destroy has begun (`_destroy`
   completes the unreported ones itself).
7. **No binding-side send queue, cap or lock.** The core serialises sends.
8. **Zero extra copies of key and value.** A `Buffer` / `Uint8Array` crosses as
   a `kafka_Bytes_t` over its own memory; the addon keeps a `napi_ref` on it
   until the hand-over completion fires (the bytes are borrowed until then).
   A string is encoded to UTF-8 once (`Buffer.from`), the equivalent of Java's
   `StringSerializer`. Header values are copied by the core during the call.
9. **Ownership follows the signature.** Errors from `out_err` and from `_cb`
   completions are owned: copied into a JS `KafkaError`, then destroyed. The
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
  the pending callbacks then only free their contexts.

## Java shape, JS idiom

- Class and member names follow Java (`KafkaProducer`, `ProducerRecord`,
  `RecordMetadata`, `send`, `flush`, `close`); methods are camelCase.
- Java getters are read-only properties named by Python's rule: a leading `get`
  is dropped, `is` / `has` stay (`metadata.offset`, `metadata.hasOffset`).
  `public static final` constants are static properties
  (`RecordMetadata.UNKNOWN_PARTITION`).
- One Java overload set is one function. Overloaded constructors take one
  options object (`new ProducerRecord({topic, partition, timestamp, key, value,
  headers})`), `close()` / `close(Duration)` is `close({timeoutMs}?)`.
- `Future<RecordMetadata>` is `Promise<RecordMetadata>`; `void` blocking
  methods return `Promise<void>`. `Callback` is a function
  `(metadata, error) => void`, run before the Promise settles, as Java fires
  the callback before completing the future. When a callback is given the
  returned Promise is marked handled, so a fire-and-forget send with a
  callback does not raise an unhandled rejection; awaiting it still rejects.
- Java `null` is `null` (`undefined` accepted on input). A Java `long` crosses
  as a JS `number` (offsets and timestamps stay below 2^53).
- Errors: every `kafka_common_Error_t` becomes a `KafkaError extends Error`
  with `code` (the `kafka_common_Error_e` value), `name` (its enumerator name,
  e.g. `TOPIC_AUTHORIZATION_FAILED`), `message` (the core's text, unchanged),
  `retriable`, `apiError` and `cause` (the `Error_source` chain). A check the
  binding makes where Java throws `IllegalArgumentException` /
  `IllegalStateException` throws a `KafkaError` with the core's own code for
  that class (`LOCAL_ILLEGAL_ARGUMENT`, `LOCAL_ILLEGAL_STATE`) and Java's
  message; a value of the wrong JS type throws `TypeError`.
- Configuration is a plain object (or a `Map`) of Java's config keys; values
  are stringified (`true` → `"true"`, numbers and bigints by `String`, arrays
  joined with `,`), `null` / `undefined` dropped. The core validates them.

## Files and process

- Every file carries the Apache 2.0 header, copyright Confluent Inc. No TODO
  or FIXME.
- No Rust or C changes for the binding: a missing FFI function is reported,
  not added.
- `test/unit/header_conformance.test.js` asserts that every `kafka_*` function
  `src/*.c` calls is declared in the bindings header and is not a plain name
  that has an `_err` twin. Keep it passing when adding calls.
- Tests use `node --test` only. The integration tests are skipped, with a
  visible message, unless `KAFKA_BOOTSTRAP_SERVERS` is set.
- Assert the core's exact error messages in tests.
