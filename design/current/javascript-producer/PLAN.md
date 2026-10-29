# JavaScript (Node.js) client — Phase 1: the producer

Status: **approved 2026-10-29** (D1 Java shape, D2 plain C on `node_api.h` + node-gyp, D3 plain JavaScript + `index.d.ts`, D4 standalone `verify-javascript`, D5 `javascript/CLAUDE.md`, D6 working name only, D7 Python `verifiable_consumer.py`). Milestone: produce one record from Node; more work follows on the same PR. Branch `dev/javascript-producer-bindings`, cut from `origin/master` at e47f8c25b.

## 1. Goal and scope

**Goal.** Add a Node.js client to the repository, built on the C FFI in the same way as the Python client (`python/`) and the .NET client (PR #259, `dotnet/`). The first slice is the producer, ending in a sample script that produces one record to a real broker. A Python or Rust consumer then reads that record back.

**In scope:**
- A new top-level `javascript/` directory, alongside `c/`, `python/` and (once merged) `dotnet/`.
- A native addon written against **Node-API** (`node_api.h`), so one build works on every Node release that supports that Node-API version. It includes only `confluent_kafka_bindings.h` and links `libconfluent_kafka` dynamically, as Python does.
- The producer:
  - create (configs → `ProducerConfig_new` → `KafkaProducer_new` with NULL serializers, so bytes pass through);
  - `send`, `flush`, `close`, and teardown;
  - delivery results;
  - error conversion.
- A thin JS layer with a hand-written `index.d.ts`.
- `javascript/examples/producer.js`, which produces one record.
- Unit tests with no broker, plus one broker-backed test that is skipped when no broker is configured.
- A `javascript/Makefile` and root `Makefile` delegators.

**Out of scope for this phase** (each fails loudly or is simply absent; nothing is stubbed silently):
- Consumer and admin.
- Transactions.
- `MockProducer`.
- User-defined serializers through the FFI serde interfaces. Only the Buffer and string conversion described in §4.3 is supported.
- `partitionsFor` and `metrics`.
- Prebuilt binaries and npm publishing.
- The gRPC multi-language test arm.
- Semaphore CI jobs.

**No Rust or C changes are expected.** The .NET PR needed none, and every function listed in §3 exists on master today. If one turns out to be missing, the Actor stops and reports it rather than adding FFI.

## 2. Ground truth and rules

These sources decide every question, in this order:
1. The C ABI in `rust/target/include/confluent_kafka_bindings.h`.
2. The Java public API (`KafkaProducer`, `ProducerRecord`, `RecordMetadata`, `Callback`).
3. Root `CLAUDE.md` §4 "C FFI Conventions".

The binding-agnostic rules below come from root `CLAUDE.md` "Python Binding Conventions" and the .NET PR's `dotnet/.claude/rules/bindings.md` and `ffi-marshalling.md`:

1. **Java shape, host idiom, no Kafka logic.** Batching, partitioning, retries and config validation live only in the Rust core. The binding repeats no check the core makes; it checks only what would otherwise be undefined behaviour at the ABI (missing topic, wrong argument types) and whether the producer is already closed.
2. **The notify hook only signals.** It never calls into the client, and it may fire on any thread, including the JS thread inside `send_with_callback_cb`.
3. **One drainer per client.** `__execute_callbacks` runs only on the JS main thread, looped while it returns > 0. The binding never calls `_destroy` from inside a drain.
4. **Waiting calls use the `_cb` twin.** The blocking forms would block the event loop and run callbacks on the caller's thread.
5. **Never call `KafkaFuture_get`.** The `KafkaFuture_t*` handed to a send's completion is destroyed unread. The delivery result comes from the per-record `Callback_t`.
6. **One `Callback_t` per record.** In `on_completion` the order is:
   1. run the user callback;
   2. complete the record's `KafkaFuture` (D8; the `send()` Promise settles at hand-over, or one event-loop turn later when the fast-path yield defers it, so it can settle after the callback);
   3. call `__set_callback_result(view, id, NULL, err)` exactly once.

   No reports are sent once destroy has begun.
7. **No binding-side send queue, buffer, cap or lock** (reaffirmed 2026-10-29, D8). The core's contract (`rust/src/ffi/producer/mod.rs`, module docs on `send_cb`) puts ordering on the caller: a caller that wants its records in sending order, and in the next `flush`/`close`, waits for each record's hand-over before its next send. The binding meets this the way Python async and .NET async do: `send()` resolves at hand-over (D8), so the caller's `await` is the wait. The binding never holds records back itself.
8. **Zero extra copies of key and value.** The binding keeps a `napi_ref` on each Buffer until the hand-over callback fires (the bytes are borrowed until then, see `rust/src/ffi/producer/mod.rs`). Headers are copied by the core. Because `send()` resolves at hand-over (D8), the documented contract is: a caller's `Buffer`/`Uint8Array` key or value must not be modified or detached until `send()` resolves, after which it may be reused (D9; .NET pins until hand-over likewise).
9. **Ownership follows the signature.**
   - Errors from `out_err` and from `_cb` completions are owned; the binding copies them into a JS object and then destroys them.
   - The error and the metadata passed to a delivery callback are borrowed.
   - If a send fails at hand-over, its per-record context is kept until after destroy (the "orphan rule"), because the delivery callback may still fire.
10. **Teardown order:** refuse new calls → `close_cb` → wait for it → `KafkaProducer_destroy` on the JS thread → release the threadsafe function and the notify opaque.

These rules will be written into `javascript/CLAUDE.md` (decision D5), so the Actor and Critic read them every time.

## 3. The C surface used

All of these come from `confluent_kafka_bindings.h`. Names ending `_err` are the panic-reporting twins that the bindings header uses for functions with no `out_err` slot of their own.

| Purpose | Functions |
|---|---|
| Config | `kafka_Map_new_err`, `kafka_Map_put_err`, `kafka_Map_destroy_err`, `kafka_producer_ProducerConfig_new`, `kafka_producer_ProducerConfig_destroy_err` |
| Client | `kafka_producer_KafkaProducer_new(config, NULL, NULL, &err)`, `kafka_producer_KafkaProducer__as_Producer_err`, `kafka_producer_KafkaProducer_destroy_err` |
| Pump | `kafka_producer_Producer__set_callbacks_notify_err`, `kafka_producer_Producer__execute_callbacks_err`, `kafka_producer_Producer__set_callback_result_err` |
| Record | `kafka_common_header_internals_RecordHeaders_new_err` / `__as_Headers_err` / `_destroy_err`, `kafka_common_header_Headers_add_with_key_value`, `kafka_producer_ProducerRecord_with_partition_timestamp_key` (or `_with_options` when headers are present), `kafka_producer_ProducerRecord_destroy_err` |
| Send | `kafka_producer_Callback_new_err`, `kafka_producer_Producer_send_with_callback_cb`, `kafka_common_KafkaFuture_destroy_err` |
| Metadata | `kafka_producer_RecordMetadata_{topic,partition,has_offset,offset,has_timestamp,timestamp,serialized_key_size,serialized_value_size}_err` |
| Control | `kafka_producer_Producer_flush_cb`, `kafka_producer_Producer_close_cb`, `kafka_producer_Producer_close_with_timeout_cb` |
| Errors | `kafka_common_Error__enum_err`, `_message_err`, `_source_err`, `_is_retriable_error_err`, `_is_api_error_err`, `_destroy_err` |

The closest existing templates are:
- `python/_confluentkafka.c`, sections `py_KafkaProducer_new`, `producer_callbacks_notify`, `py_Producer_send_cb`, `producer_on_completion` and `py_Producer_destroy`.
- `c/tests/test_kafka_producer.c`, the `send_cb` + pump flow.

## 4. Design

### 4.1 Layout

```
javascript/
  CLAUDE.md                 # JS binding rules (§2), read by the Actor and Critic
  README.md                 # build, run the example, run the tests
  Makefile                  # build / test-unit / test-integration / test / clean, REPO_ROOT and RUST_PROJECT_ROOT like c/ and python/
  package.json              # "private": true, engines.node >= 18, scripts call node-gyp and node --test
  binding.gyp               # compiles src/*.c and links ../rust/target/<profile>/libconfluent_kafka with an rpath
  src/
    addon.c                 # module init and exports
    producer.c              # client handle, notify hook → threadsafe function, send / flush / close / destroy
    record.c                # Buffer/string → kafka_Bytes_t, headers, ProducerRecord build
    error.c                 # kafka_common_Error_t → a plain JS object {code, name, message, retriable, apiError, cause}
    util.h
  lib/
    index.js                # public exports
    producer/kafka_producer.js, producer_record.js, record_metadata.js, callback.js
    common/kafka_error.js   # KafkaError extends Error
    _config.js              # configs object → [[key, value]] strings (true→"true", arrays comma-joined, null/undefined dropped)
  index.d.ts
  examples/producer.js
  test/unit/*.test.js       # node:test runner, no broker
  test/integration/*.test.js  # needs KAFKA_BOOTSTRAP_SERVERS, otherwise skipped with a visible message
```

Every file carries the Apache 2.0 header with Confluent Inc. as copyright holder.

### 4.2 Native layer: threads and the pump

**Client handle.** Each client is a C struct held by the JS `KafkaProducer` through a `napi_wrap`. It contains:
- the `KafkaProducer_t*` and its borrowed `Producer_t*` view;
- a `napi_threadsafe_function` (tsfn);
- a `closed` / `destroying` state;
- an in-flight counter;
- the orphan list.

**Notify hook.** The hook calls `napi_call_threadsafe_function(tsfn, NULL, napi_tsfn_nonblocking)` and does nothing else. This is the N-API form of what the existing librdkafka JS client does with `uv_async_send`, and of Python's condvar or wake-fd signal.

**Drain.** The tsfn's `call_js` runs on the JS thread and loops `__execute_callbacks_err` while it returns > 0. `send()` also runs the same drain on entry, periodically (the pump, mirroring Python `AsyncProducer._PUMP_EVERY`), so a loop of sends the producer takes at once still releases delivery callbacks; a drain started inside another drain is a no-op, and a run of such sends periodically yields one event-loop turn (`_FAST_PATH_YIELD_EVERY`). Neither holds a record back. All delivery callbacks and `_cb` completions therefore run on the JS thread, so JS values can be created there directly with no extra hop.

**Keeping Node alive.** The tsfn is `ref`'d while any send, flush or close is outstanding and `unref`'d when the count reaches zero. An idle producer does not keep the process alive; one with pending work does, which is what users expect from a socket-like resource.

**Hand-over callback.** `send_with_callback_cb`'s hand-over callback can fire inside the call on the JS thread. All per-record state is therefore set up before the call, and no lock is held across it.

### 4.3 Public JS API (Java shape, JS idiom; D1)

```js
const { KafkaProducer, ProducerRecord } = require('<package>');

const producer = new KafkaProducer({ 'bootstrap.servers': 'localhost:9092' });
const future = await producer.send(                    // resolves at hand-over
  new ProducerRecord({ topic: 'demo', key: 'k1', value: 'hello' }),
  (metadata, error) => { /* optional Java Callback */ });
const metadata = await future.get();                   // resolves at delivery
console.log(metadata.topic, metadata.partition, metadata.offset, metadata.timestamp);
await producer.flush();
await producer.close();          // or close({ timeoutMs })
```

**Classes and names:**
- Class and member names follow Java: `KafkaProducer`, `ProducerRecord`, `RecordMetadata`, `send`, `flush`, `close`.
- Methods are camelCase. Getters are read-only properties, with Java's `get`/`has` prefix rules applied as in Python.

**`ProducerRecord`:**
- Java's overloaded constructors become one constructor taking an options object: `{topic, partition?, timestamp?, key?, value?, headers?}`.
- Key and value may be a `Buffer`, a `Uint8Array`, a `string` (UTF-8, the equivalent of Java's `StringSerializer`) or `null`.
- `headers` is an array of `{key, value}`.

**`send`:**
- **Two-step send (D8).** `send(record, callback?)` returns a `Promise<KafkaFuture<RecordMetadata>>`. The Promise resolves at **hand-over**, the point where Java's blocking `send()` returns, with a `KafkaFuture` whose `get()` returns a `Promise<RecordMetadata>` that settles at **delivery**. This is the shape Python's `AsyncProducer.send` and .NET's `AsyncKafkaProducer.Send` use, and Java's `send()` itself returns a `Future`.
- A **hand-over** error (what Java's `doSend` rethrows, e.g. a closed producer or a bootstrap resolution failure) rejects the `send()` Promise and never reaches the callback, as in Java.
- A **delivery** error reaches the optional `callback(metadata, error)` first, then fails the future, mirroring Java's callback-then-future order. A future whose `get()` is never called raises no unhandled rejection (Java ignores an unread failed `Future` too).
- **Ordering is the caller's.** Records from code that awaits each `send()` before the next reach the producer in order and are covered by a following `flush()`/`close()`. Sends that are not awaited one by one (e.g. `Promise.all`) are concurrent, like Java `send()` calls on several threads, with no order between them. This is documented on `send`, in `index.d.ts` and in the README.
- Argument errors (no topic, wrong type) and "producer is closed" throw synchronously before any FFI call, as Java's `send` does.

**`flush` and `close`:**
- `flush()` returns `Promise<void>`.
- `close({timeoutMs}?)` returns `Promise<void>`. It is idempotent, and destroys the native client once the close completes.
- Calling `flush()` inside a callback rejects with the core's own error text.

**Errors:**
- `KafkaError extends Error` with `code` (numeric), `name` (the `kafka_common_Error_e` name, e.g. `TOPIC_AUTHORIZATION_FAILED`), `retriable`, `apiError` and `cause`, which follows the `Error_source` chain.
- The full per-class error hierarchy, as Python has, is a later phase.

### 4.4 Build

- `node-gyp` is a devDependency, run through `npm run build` (D2).
- The library directory is `CONFLUENT_KAFKA_LIB_DIR`, defaulting to `../rust/target/release`, the same variable Python uses. The header directory is `../rust/target/include`.
- Linking is dynamic, with an rpath to that directory.
- The Rust library is a prerequisite built by `make -C rust build`, as the other bindings require.
- Root `Makefile`, new targets: `build-javascript` (depends on `build-rust-all-features`), `test-javascript`, `verify-javascript`.
- These are **not** added to `make verify` or Semaphore in this phase (D4).

### 4.5 Example and end-to-end check

`javascript/examples/producer.js <bootstrap> <topic> [key] [value]` produces one record, prints its `RecordMetadata`, then flushes and closes. It exits non-zero on error.

The manual end-to-end run uses a scratch broker:

```bash
docker run -d --rm --name kafka-js -p 9092:9092 apache/kafka:4.3.1
docker exec kafka-js /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic js-demo --partitions 1 --replication-factor 1
make build-javascript
node javascript/examples/producer.js localhost:9092 js-demo k1 hello-from-node
```

The record is then read back with an independent consumer, in this order of preference:
1. **Python:** `python/verifiable_clients/verifiable_consumer.py --bootstrap-server localhost:9092 --topic js-demo --group-id js-verify --group-protocol consumer --max-messages 1 --verbose --reset-policy earliest`. This needs `make build-python`; if the CodeArtifact index returns 401, use `PIP_INDEX_URL=https://pypi.org/simple`.
2. **Rust:** the Rust verifiable consumer in `rust/tools/verifiable-clients`, with `--max-messages 1`.
3. **Cross-check:** `kafka-console-consumer.sh` inside the broker container, printing the key and value.

The phase is done when the consumer prints `k1` / `hello-from-node` at the partition and offset the JS example printed.

### 4.6 Tests

**Unit tests** (no broker; `node --test`, so there are no test-framework dependencies):
- **Config conversion:** stringifying values, dropping `null`, and rejecting non-string keys.
- **`ProducerRecord` validation and key/value conversion.**
- **Error conversion:** an invalid config gives a `KafkaError` with a config error code and the core's message, asserted exactly.
- **Unreachable broker:**
  - `bootstrap.servers=127.0.0.1:1` with a short `max.block.ms` and `delivery.timeout.ms`;
  - `send` rejects with the core's timeout error, and the message is asserted;
  - the user callback runs exactly once, before the rejection.
- **After close:** `send` throws, `flush` rejects, and `close` is idempotent.
- **Event loop:** an idle producer lets the process exit, and pending sends keep it alive.
- **Inside a callback:** calling `flush` from inside a callback rejects.

**Integration test** (skipped unless `KAFKA_BOOTSTRAP_SERVERS` is set):
- produce one record with key, value and headers;
- assert the metadata (topic, partition, offset ≥ 0, timestamp present);
- flush and close.

`make -C javascript test-integration` starts and stops its own scratch broker via `docker run`, as the manual steps above do.

**Header conformance check.** A static test asserts that every `kafka_*` name called in `src/*.c` is declared in `confluent_kafka_bindings.h`, and that none is a plain name that has an `_err` twin. It mirrors Python's `test_ffi_twins.py` and .NET's `NativeAbiConformanceTests`, and it is cheap protection against using the wrong header form.

## 5. Execution: Actor and Critic

**Roles:**
- Manager: this session.
- Actor: `actor-executor`, number **N = 100**.
- Critic: `kafka-critic`, number 100, writing `COMMENTS.100.md`.
- Loop per `agent-roles.md`: Actor → Critic → Actor fixes (fixup commits, comments moved to `COMMENTS.DONE.100.md`) → Critic, until no comments remain.

**Phases:**

| Phase | Actor delivers | Commit(s) |
|---|---|---|
| P1 Scaffold | `javascript/` layout, `package.json`, `binding.gyp`, `Makefile`, `CLAUDE.md`, `README.md`, an addon that loads and exports a version probe; builds against the release lib | `javascript: scaffold the Node-API binding` |
| P2 Producer core | native client, notify → tsfn pump, `send` / `flush` / `close` / destroy, error conversion, JS layer, `index.d.ts`, unit tests | one commit per coherent step |
| P3 Example + end-to-end | `examples/producer.js`, integration test, root `Makefile` targets, the §4.5 run with the actual consumer output pasted into the phase notes | `javascript: example producer and end-to-end test` |
| Critic loop | review P1–P3 against §2 and §3, the bindings header and Java's `KafkaProducer` contract | fixups |

**Commits and gate:**
- Working commits use `git commit --no-verify`, with no AI co-author trailer.
- Nothing is pushed unless you ask.
- Before the last commit, I run the full gate:
  - from `rust/`: `cargo xtask format-check`, `cargo xtask lint` and `cargo xtask lint-custom`;
  - from the repo root: `make doc-check` and `make verify`;
  - plus `make verify-javascript`.
- I report every result, including failures caused by the environment.

**Environment notes for the subagents:**
- Put `~/.rustup/toolchains/1.95.0-aarch64-apple-darwin/bin` first on `PATH`, because the Nix `cargo` has no clippy.
- Use `PIP_INDEX_URL=https://pypi.org/simple` for the Python consumer.
- Never use `git stash`, and kill processes only by PID.
- Use one shared scratch broker; the Docker VM has 8 GB.
- Stage explicit paths only.

## 6. Open decisions

| # | Decision | Recommendation | Alternative |
|---|---|---|---|
| D1 | Public API shape | **Java shape** (`KafkaProducer`, `ProducerRecord`, `send()` → `Promise<RecordMetadata>`). This is the rule both Python and .NET follow ("target the Java shape, not the language's existing ecosystem client"). | Shape compatible with the existing `confluent-kafka-javascript` / KafkaJS API (`new Kafka().producer()`, `connect()`, `send({topic, messages})`). This eases migration, but departs from the cross-binding rule. |
| D2 | Native layer and build | **Plain C against `node_api.h`, built with node-gyp.** It matches Python's hand-written C extension, adds no C++ dependency, and is the standard toolchain for Node addons. | `node-addon-api` (C++ wrapper, less boilerplate) and/or `cmake-js`. |
| D3 | Language of the JS layer | **Plain JavaScript (CommonJS) plus a hand-written `index.d.ts`.** No compile step. | TypeScript sources compiled with `tsc`. |
| D4 | CI and `make verify` | **Standalone `verify-javascript` for now**; add it to `verify` and Semaphore (Node install and a `REQUIRED_CI_JOBS` entry) in a later phase. | Wire it into `make verify` now. Every machine running `make verify` would then need Node ≥ 18. |
| D5 | Where the JS rules live | **`javascript/CLAUDE.md`**, picked up automatically inside `javascript/`, like `dotnet/CLAUDE.md`. Root `CLAUDE.md` is not edited (it is closed to automatic agents). | `.claude/rules/javascript-binding.md` |
| D6 | Package name | **`"private": true` with a working name `confluent-kafka-javascript-rust`**; the real name is settled before any publish, since `@confluentinc/kafka-javascript` is taken by the existing client. | Choose the final name now. |
| D7 | Consumer for the end-to-end check | **Python `verifiable_consumer.py`**, falling back to the Rust verifiable consumer. | Rust only, which avoids the Python build and its pip index issue. |
| D8 | Ordering after Critic 100 items 1–2 (added 2026-10-29) | **Approved: two-step send.** `send()` resolves at hand-over with a `KafkaFuture`; `future.get()` resolves at delivery. No binding-side buffering. | A binding FIFO (rejected: no binding-side buffering); a blocking send on the JS thread (rejected: freezes the event loop up to `max.block.ms`); an ordered mode in the Rust core (possible later; changes the C API). |
| D9 | Caller's Buffer (Critic 100 item 4) | **Approved with D8:** borrowed until `send()` resolves, documented; no copy. | Copy every caller Buffer at `send()`. |
