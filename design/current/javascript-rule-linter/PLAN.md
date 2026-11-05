# Plan: a Claude-rule linter for the Node.js client

Status: approved for implementation, 2026-11-05. Based on PR #312 at `b86bb9ebc`
(which adds `lib/_native_loader.ts`, `tools/` and `release/`).

## 1. Goal

Check the rules of `javascript/CLAUDE.md`, and the root `CLAUDE.md` rules it inherits,
mechanically. The model is what `cargo xtask lint-custom` does for the Rust crate:

- one command;
- every rule runs, and every finding is reported in one pass;
- the run exits non-zero if anything fails;
- each failing rule prints one fix hint;
- a rule that cannot run fails the run instead of being skipped.

Behaviour (ordering, timing, messages) stays with the tests. The linter checks what can
be read from the source: names, shapes, visibility, docs, and which C functions are
called where.

Command: `make -C javascript lint-rules`, which runs
`node scripts/lint_rules.js [check-<name>] [--no-baseline]`. `verify-javascript` runs it.

## 2. What already exists (not redone)

| Existing check | What it pins |
|---|---|
| `test/static/layout.test.js` | `lib/` has only `.ts` files with snake_case names and `_` for private ones; every folder has an `index.ts` that only re-exports; the root `index.ts` exports; `package.json` `exports` / `typesVersions` / `files` / scripts / engines; `dist/` and `build/` untracked; unit-test file names; only `util.h` includes the header; the license header; the generated-file header |
| `test/static/header_conformance.test.js` | `src/*.c` calls only functions the header declares, and the `_err` twin where one exists; the plan's functions are called, and the blocking `send` / `flush` / `close` and `KafkaFuture_get` are not; the single blocking close sits in the finalizer; `FFI_NAMES` matches `kafka_common_Error_e` |
| `test/typing/typing.test.js` | the declarations type-check under `--strict`, node10 resolution and no ambient types; no public declaration names a private module; a default import fails under `esModuleInterop` |
| `test/unit/_errors.test.js` | the error classes match Java's `extends` chain in both directions; names, folders, FFI ids, abstract bases, `fromNative` |
| `test/unit/_config.test.js`, `producer_config.test.js` | config type checks and the constants against Java's literals |
| `cargo xtask check-generated` | generated `.ts` files are current, none missing, none orphaned |

These keep running. The linter adds what they do not cover. Open decision 14 is whether
the static tests later move into the runner.

## 3. Architecture

```
kafka/ Java sources ──java.rs scanner──▶ cargo xtask java-index --json ──▶ build/java-index.json ─┐
design/current/ Public + deprecated lists ──────────────────────────────────────────────────────┤
javascript/lib/**/*.ts ──TypeScript compiler API (program + type checker)─────────────────────────┼─▶ rules ─▶ findings ─▶ baseline ─▶ report / exit 1
javascript/src/*.c + confluent_kafka_bindings.h ──C tokenizer (from header_conformance)───────────┤
javascript/lint-baseline.txt ───────────────────────────────────────────────────────────────────┘
```

### 3.1 Java side: reuse `rust/xtask/src/java.rs`

Add `cargo xtask java-index --json <out>`, which writes the `JavaIndex` the Rust lint
already builds. For each main class it records:

- package, nested path, kind, visibility, `abstract`, `@FunctionalInterface`, supertypes;
- each overload: name, parameter types and names, visibility, `static`, `default`,
  `@Deprecated`, and its Javadoc translated by `javadoc.rs` (the `@param` / `@return` /
  `@throws` tags split out, plus the word count);
- fields: visibility, `static final`, deprecated;
- enum constants;
- whether the class is `@InterfaceAudience.Public` in 4.4, whether its package is
  "not supported", and whether it appears in the deprecation list.

New in the scanner: the **return type** of each overload and the **type** of each field,
as written. `java.rs` does not record these today, and rule J7 needs them.

Why not scan Java again in JS: it would duplicate `java.rs` (DoD #6), and the two would
drift on what "deprecated", "public" or "getter" means. The Rust, Python and JS lints
should share one Java truth.

### 3.2 TypeScript side: the compiler API

Use `typescript` (5.9.3), already a devDependency and already used by
`scripts/typescript.js`. `ts.createProgram` runs over `lib/` with `tsconfig.json`. The
type checker supplies each module's exports, class members, modifiers
(`private` / `static` / `readonly`), accessors, signatures and **resolved types**. No new
dependency. This is stronger than the Rust lint, which works on syntax only.

### 3.3 C side: a tokenizer, not libclang

Move `stripComments` / `calledFunctions` out of `header_conformance.test.js` into
`scripts/lint/c_source.js` and extend it to:

- find function bodies;
- list call sites with their argument lists;
- list `napi_property_descriptor` registrations.

Prototypes are parsed from `confluent_kafka_bindings.h`, as the test already does.

### 3.4 Mapping Java to TS: by convention, plus two tags

Rust needs a marker on every item because its names change (snake_case, `_with_`). JS
keeps Java's names, so the mapping can be derived:

- **Files:** `lib/<pkg>/<snake>.ts` maps to Java class `<pkg>.<Pascal>`. `clients.` is
  dropped from the package, and `java.*` packages map to the root.
- **Methods:** a method has its Java method's name.
- **Getters:** a getter property `x` maps to Java `x()` or `getX()`; `isX` / `hasX` stay
  (a property `isX` maps to `isX()`, never a property `x`).
- **Static-ness:** a member maps only to a Java member of its own static-ness; a mismatch is
  J2's `static-mismatch`.
- **Exports:** a public module's exports are the type checker's (`export` declarations,
  `export { X }` re-exports, `export declare` ones). J1 asks a function or constant export
  for `@jsOnly` (`jsonly-required`) and reports an enum or namespace export (`export-form`).
- **Shapes:** an exported interface or object type mapped to a Java class (`Header`) goes
  through J2 and J3 as a class does: its property and method signatures are its members.
- **Constants:** a `static readonly X` maps to Java `public static final X`.
- **Constructors:** the constructor maps to Java's constructors.

Two TSDoc tags cover the exceptions. A tag that exempts nothing is a finding.

- `@jsOnly <reason>`: an exported item with no Java counterpart (`ProducerConfigs`,
  `CloseOptions`, `ProducerRecordOptions`, `Data`). It plays the role of Rust's
  `rust-only`.
- `@java <fqn>[#member]`: where the convention does not find the Java item, e.g. the
  `Header` type maps to `org.apache.kafka.common.header.Header`.
- `check-java-tags` reports a tag that is malformed or not needed: an `@jsOnly` with no
  reason, or on an item the convention maps; an `@java` naming no indexed class, a member
  its class does not declare, what the convention already finds, or another class or
  member than the one the convention finds (`java-remap`); both tags on one item.

Three kinds of Java class map differently, by one allowance each in
`scripts/lint/java_ts.js`, never by per-file tags (the files are generated):

- **Errors** (`lib/_error_registry.ts`, javascript/CLAUDE.md "Errors"): their Java class is
  the registry's; they are exempt from J4's audience rule; their members are the fixed set
  (`name`, `code`, `errorName`, `retriable`, `apiError`, `message`, `cause`), which J8 also
  allows as data properties; J3 asks nothing of them.
- **Config classes** (`ProducerConfig`, `SslConfigs`, `SaslConfigs`, `SecurityConfig`): J3
  asks for their non-deprecated `public static final` constants only.
- **Function types** (`Callback`): mapped by J1, no member rule applies.

J3 also leaves out the members `java.lang.Enum` gives every enum (`values`, `valueOf`,
`name`, `ordinal`), which J2 still maps, and on an enum J2 maps `toString` to
`java.lang.Enum#toString` when the enum declares none. J3 does not ask for the constructor
of an abstract Java class, which only a subclass calls. J5 also reports a member a
deprecated Java supertype declares.

### 3.5 The runner

- **Rule modules:** `scripts/lint_rules.js` plus one module per rule in
  `scripts/lint/rules/`. Each exports `{ name, kinds, needs, check(ctx) -> { findings,
  checked }, hint }`: `kinds` are its finding kinds, unique across rules, so a line of the
  shared baseline belongs to one rule.
- **Context:** the TS program, the Java index, the C sources and the header, loaded once.
- **Failure modes:** a missing `kafka/`, header or `java-index.json` fails the run with a
  reason. A rule that checks 0 items fails too, so a rule cannot pass by checking nothing
  (the idea behind Rust's `check-consumer-receiver`, which fails when either trait is
  missing).
- **Baseline:** `javascript/lint-baseline.txt` works like
  `rust/xtask/src/lint_custom/baseline.rs`. Each line is a `<kind> <subject>` key or a
  `prefix:` line. A line that waives nothing is a finding, so the file only shrinks.

## 4. The rules: what is tested

Each rule lists: the rule it enforces, the exact finding condition, an example finding,
and what already covers part of it.

### J — Java surface (Java index + TS checker)

**J1 `check-java-name`** (JS "Files and layout", root §2)
- Every exported class, interface or type of a public module maps, by 3.4, to a Java class
  of its folder's package, unless it is tagged.
- The file name is `snake_case(class)`, and each file holds one public Java type, except
  `Header` beside `ProducerRecord` in `producer_record.ts` (decision 5).
- An `Exception` class is named `…Error`.
- Example: `lib/producer/record_meta.ts: exports RecordMetadata (clients.producer.RecordMetadata); the file must be record_metadata.ts`.
- Already covered: `layout.test.js` checks the snake_case form, and `_errors.test.js`
  checks the error classes. Nothing ties the hand-written classes to Java.

**J2 `check-java-members`** (JS "Java shape, JS idiom")
- Every public member of an exported class maps to a public member of its Java class:
  a method by its own name, a property by the getter rule, a `static readonly` by a
  `public static final` field of the same name.
- A public member with no Java counterpart is a finding unless it is tagged `@jsOnly`.
  TS-`private` and `_` members are exempt.
- Example: `RecordMetadata.getOffset: no Java member; Java's getter offset() is the property offset`.

**J3 `check-java-completeness`** (DoD #2)
- Every public, non-deprecated Java constructor, method and constant of a translated class
  is present, or appears in the baseline as `missing <Class>#<member>`, `<Class>` the Java
  class below `org.apache.kafka.` (`clients.producer.KafkaProducer#initTransactions`), one
  line per Java name (its overloads collapse).
- Today the baseline holds `KafkaProducer`'s transaction methods, `partitionsFor`,
  `metrics`, `clientInstanceId` and the metric-subscription methods. They are burned down
  as they are translated. See decision 4; `equals` / `hashCode` are never asked for
  (decision 6).

**J4 `check-public-audience`** (root §2, §3)
- An exported class must be `@InterfaceAudience.Public` in Kafka 4.4, and not in an
  `internals` or "not a supported Kafka API" package.
- An exported member must not translate a `private`, package-private or `protected` Java
  member.
- Exempt: error classes (one per FFI-enum exception, Public or not, as in the Python rules)
  and items tagged `@jsOnly`.

**J5 `check-no-deprecated-translation`** (root §3)
- No exported class, member or constant translates a `@Deprecated` Java class, overload
  or field. "Deprecated" means the `kafka/` tree plus `design/current/java-deprecated.txt`.
- Already covered for config constants: the producer config test reads the deprecated set.
  This rule covers everything else.

**J6 `check-overload-shape`** (JS "One Java overload set is one function")
- No two TS members map to one Java name.
- A Java overload set with more than one form is one function taking one options object.
  Its property names must equal the union of the Java overloads' parameter names, with a
  `Duration x` becoming `xMs` (decision 8: milliseconds, `xMs: number`).
- A property is required exactly when every Java overload has it.
- Example: `ProducerRecordOptions.ts: no ProducerRecord constructor names it (topic, partition, timestamp, key, value, headers)`.
- Example: `ProducerRecordOptions.value is optional but every Java constructor takes value`.

**J7 `check-type-mapping`** (JS "Java null is null", "long crosses as number", `Future` → `KafkaFuture`)

Each parameter, return and property type follows one table:

| Java | TS |
|---|---|
| `long` / `int` / `short` / `byte` | `number` |
| `boolean` | `boolean` |
| `String` | `string` |
| key/value `K` / `V` | `Data \| null` |
| `Duration` | `number` (milliseconds), the name taking `Ms` (decision 8) |
| `Future<T>` | `KafkaFuture<T>` |
| a value that can be `null` | `T \| null` |
| `void` of a blocking method | `Promise<void>` |

A type the table does not cover is an `unmapped` finding, as in the Rust FFI rule. This
needs the return and field types from 3.1.

As built in P3 (`scripts/lint/rules/type_mapping.js` holds the full table): `null` /
`undefined` are stripped from the TypeScript type first, and a return may be a `Promise` of
the mapped type (a blocking method; A1 says which). The table also maps `byte[]` to
`Data`; `List` / `Set` / `Collection<T>` and `T[]` to an array of `T`; `Iterable<T>` and
`Headers` to an iterable or array (of `Header`); `Map` / `Properties` (a client's
configuration) to the `...Configs` type; `Object` to `unknown`; a type variable to a type
parameter; and a Java class lib/ translates to its TypeScript type. An options object's
properties are checked against the Java parameters of their names.

**J8 `check-getters-readonly`** (JS "Java getters are read-only properties", root §3 no public field)
- Every public property of an exported class is a `get` accessor with no `set`, or a
  `static readonly`.
- There are no public mutable fields. A `readonly` instance data property is a finding
  too (`readonly-field`): TypeScript's `readonly` is gone at run time. Except (decision 7):
  one translating a Java `public final` instance field, or an enum's `name`, on a class
  whose constructor runs `Object.freeze(this);` at its top level.

### A — async shape and the native registry

**A1 `check-waiting-call-promise`** (JS rule 4, "A blocking Java method returns a Promise")
- A public method whose native path reaches a `_cb` twin returns a `Promise`; one whose
  path reaches none does not.
- The path is followed: TS method, then a `native.<fn>` call in its body (directly or
  through a private method of the same class), then the `addon.c` registration
  `{"<fn>", Impl}`, then the `kafka_*` functions `Impl` calls.
- Example: `KafkaProducer.flush reaches kafka_producer_Producer_flush_cb but returns void`.
- The other direction is `promise-not-waiting`. A path that leads nowhere (a `native.<fn>`
  no descriptor registers, or an implementation no `src/*.c` defines) is
  `unresolved-native`, never a pass. A method with no native use is not checked.
- The path follows every call the type checker resolves into a lib/ function or method,
  transitively (a module helper, another class's method), and the handle is recognised by
  its type under any name (`const n = native`), in A1 and A2 alike.
- J7 accepts a Promise return only on a method A1 resolves to a `_cb` twin, or on a listed
  pure-JS waiting member (`KafkaFuture#get`: Java's `get()` blocks; the JS future settles
  from the delivery callback).

**A2 `check-native-registry`** (JS "Files and layout")
- Every function `addon.c` registers is used from `lib/`, and every `native.<fn>` used
  exists in `addon.c`.
- Only `lib/_native_loader.ts` resolves the `.node` file: the platform package
  `@confluentinc/kafka-javascript-<platform>` first, then `build/Release/`.
- Only `lib/_native.ts` calls the loader and does the one-time setup.
- No `index.ts`, and no other `lib/` file, `require`s a `.node` file or the loader; no
  `index.ts` imports `lib/_native.ts` either (`index-native`).

### N — the native addon (C)

**N1 `check-out-err`** (root §4: `out_err` never NULL; nothing silently dropped)
- Every call to a function whose prototype ends in `kafka_common_Error_t **out_err` passes
  an error slot.
- That slot is read by `FFI_ERR` / `FFI_SET` / `FFI_DISCARD`, or tested right after the
  call. It is never `NULL`.

**N2 `check-cb-twin-only`** (JS rule 4)
- For every header function `f` that has an `f_cb`, `src/` calls `f_cb`, except an
  allowlist entry with its reason (the finalizer's force-close).
- This generalizes `header_conformance`'s fixed producer list, so a consumer or admin
  binding is covered without editing the test.

**N3 `check-notify-hook`** (JS rule 2)
- The body of `cbq_notify` contains only the `notify_mtx` lock / unlock, the
  `notify_open` test and one `napi_call_threadsafe_function(…, napi_tsfn_nonblocking)`.

**N4 `check-single-drainer`** (JS rules 3, 10)
- `__execute_callbacks_err` is called only from `cbq_drain`.
- `cbq_drain` is called only from the threadsafe function's `call_js` and from `send()`'s
  pump.
- No `_destroy_err` appears lexically inside the drain loop.

**N5 `check-completion-order`** (JS rule 6)
- In each delivery-completion function the calls appear in this order: user callback,
  then future completion, then `__set_callback_result_err`, each at one call site.
- The check is lexical, not path-sensitive. Decision 13 covers what it cannot see.

**N6 `check-ownership`** (JS rule 9)
- Every handle type `src/` receives as owned (from a `_new`, from `out_err`, from a `_cb`
  completion) has its `_destroy` called somewhere in `src/`. This is a set-level check.
- The borrowed error and metadata a delivery callback receives are never passed to a
  `_destroy` in that function.

**N7 `check-zero-copy`** (JS rule 8) *(heuristic; decision 13: built, with an allowlist)*
- On the send path (`record.c`, `producer.c`), the key and value cross through
  `napi_get_buffer_info` / `napi_get_typedarray_info` into a `kafka_Bytes_t`.
- There is no `memcpy`, `malloc` or `napi_create_buffer_copy` on them.

**N8 `check-no-binding-queue`** (JS rule 7) *(heuristic; decision 13: built, with an allowlist)*
- There is no mutex outside `callback_queue.c`, and no array, queue or cap of records in
  `lib/producer/`.

### D — documentation (TSDoc)

**D1 `check-tsdoc-present`:** every export and public member has a TSDoc comment.

**D2 `check-tsdoc-tags`** (JS "Types are TypeScript … TSDoc comments")
- `@param name -` for every parameter (and each property of an options object).
- `@returns` for a non-`void` return.
- A `` @throws `XError` `` for each Java `@throws` of the member, the name mapped, and
  every class named this way exists in `lib/`.
- No JSDoc type annotations (`@param {string}`) in `lib/`.

**D3 `check-tsdoc-size`** *(open decision 15)*
- The doc is at most 150% of the Javadoc's word count, measured with the same
  `javadoc.rs` count, which reaches JS through `java-index.json`.
- `words` in `java-index.json` counts a Javadoc as written. A Javadoc that only points
  elsewhere (`@see KafkaConsumer#commitSync()`, `See {@link KafkaProducer#close()}`)
  counts its pointer, where `check-api-docs` measures the Javadoc pointed to
  (documentation.md §4). If decision 15 keeps D3, P5 adds the followed count.

**D4 `check-java-line`** (documentation.md §7)
- No comment in `lib/`, `src/`, `test/`, `scripts/`, `tools/` or the gRPC harness
  (`grpc_server.js`, `grpc_translate.js`) cites a Java file and line (`Foo.java:123`
  or the short `:142` form).

### H — hygiene

**H1 `check-no-todo`** (JS "Files and process"; no current check)
- No `TODO` / `FIXME` in any file of the client ("every file"): every file under
  `javascript/`, read from the file system (never from git, whose index and
  environment vary with the caller), so the gRPC harness, `binding.gyp`, the Dockerfile
  and the README / CHANGELOG too. Not read: `node_modules/`, `build/`, `dist/`,
  `.claude/`, `package-lock.json` and binary files.
- The sentence of `javascript/CLAUDE.md` that states the rule ("No TODO or FIXME.") is
  exempt; any other marker of that file is a finding, and so is the exemption once the
  sentence is not found.

**H2 `check-exception-word`** (root §2, §4)
- No identifier in `lib/` or `src/` contains "exception", in any case. Comments are
  exempt.

**H3 `check-private-members`** (JS "Files and layout")
- Every `_`-prefixed member is TS `private`.
- Every bracket access `x['_m']` targets a `private` member of another class.

## 5. Not linted (left to tests and review)

These depend on runtime behaviour, so a static check would be a guess:

- cooperative scheduling (drain every 64th send, defer after 1000);
- unhandled-rejection semantics of `send()` and of futures;
- the callback running before the future completes, at runtime;
- teardown and finalizer order;
- keep-alive `ref` / `unref`;
- the core's exact error messages;
- `null` / `undefined` handling;
- config validation;
- the negative-timeout precedence in `close`;
- "no Kafka logic in the binding" beyond what N8 catches.

## 6. How the linter itself is tested

1. **Fixture tests, per rule** (`test/lint/<rule>.test.js`, `node --test`).
   - Each test builds a tiny project in a temp directory: a couple of `lib/*.ts`, a
     `src/*.c`, a minimal header and a hand-written `java-index.json`.
   - It runs one rule and asserts the **exact** finding text.
   - Every rule has, at least:
     - a passing case;
     - one case per finding kind;
     - one case per exemption (tag, allowlist, baseline line);
     - the "cannot run" case (missing input, which must fail, not skip);
     - the "checked 0 items" case.
2. **The real tree.** The runner over the repository exits 0 with the checked-in baseline.
3. **Mutation smoke test** (`test/lint/mutation.test.js`).
   - Copy `lib/` and `src/` to a temp directory.
   - Apply one known violation per rule. Examples:
     - rename the `offset` getter to `getOffset`;
     - add a public mutable field;
     - call `kafka_producer_Producer_flush` instead of `_cb`;
     - drop an `FFI_ERR`;
     - add a `TODO`;
     - make `ProducerRecordOptions.value` optional.
   - Assert that exactly that rule fires. This catches a rule whose path resolution is
     silently broken, which a fixture test of the rule alone would miss.
4. **The baseline module.** Stale line, duplicate line, `prefix:` waiving, and a missing
   file waiving nothing.
5. **Rust side** (`cargo test -p xtask`). Cover:
   - the `java-index` JSON for a fixture class: schema, return types, field types, the
     Public / unsupported / deprecated flags, translated Javadoc tags;
   - the new return-type scanning in `java.rs`.

## 7. Phases

Each phase runs the Actor / Critic loop of `.claude/rules/agent-roles.md`. Commits use
`--no-verify`; the full gate runs once, before the last push.

| Phase | Content | Done when |
|---|---|---|
| P1 | `cargo xtask java-index --json` (+ return and field types in `java.rs`), runner, baseline, `c_source.js`, H1–H3, D4 | runner green on the tree; fixture tests for each |
| P2 | J1, J2, J4, J5, J8; J3 with its baseline | as above, plus the mutation cases for these rules |
| P3 | J6, J7, A1, A2 | as above |
| P4 | N1–N8 (decision 13) | as above |
| P5 | D1, D2 (D3 if decision 15 says so) | as above |
| P6 | `make -C javascript lint-rules`, `verify-javascript`, Semaphore; a proposal of "(checked by `check-…`)" references for `javascript/CLAUDE.md`, for the user to apply | CI runs it |

## 8. Decisions

Answered 2026-11-05:

1. **Architecture:** the hybrid of 3.1–3.3. `cargo xtask java-index --json` is a tooling
   change, not a core or FFI change, so it is outside "no Rust or C changes for the
   binding".
2. **Mapping:** convention plus the `@jsOnly` / `@java` tags (3.4).
3. **Java reference version:** the same as Rust. Audience is read at `java::AUDIENCE_REF`
   (4.4), deprecations at `java::DEPRECATION_REFS` (4.3.1 + 4.4). A one-line fix to
   `javascript/CLAUDE.md` ("Apache Kafka 4.3.1") is proposed to the user, not applied.
4. **J3 completeness:** enforced now; the untranslated members go in
   `javascript/lint-baseline.txt`.
5. **`Header` beside `ProducerRecord`:** a stated exception (the user adds the sentence
   to `javascript/CLAUDE.md` "Files and layout"). J1 allows exactly the type `Header` in
   `lib/producer/producer_record.ts`, beside `ProducerRecord`; any other second type
   there, or `Header` beside another type elsewhere, is a finding.
6. **`equals` / `hashCode`:** not translated in JavaScript; J3 never asks for them.
7. **A Java `public final` instance field** may be a `readonly` property of a frozen
   object: J8 accepts a `readonly` instance data property when the Java class (or a
   supertype) declares a public final instance field of that name and the constructor
   runs `Object.freeze(this);` as a top-level statement of its body (not under a
   condition, not in an inner function). When a name is both a public final field and a
   getter method (`SecurityProtocol`'s `name`), the field wins. Extended the same day: on
   a class translating a Java enum, the property `name` translating
   `java.lang.Enum#name()` passes on the same terms (`SslClientAuth.name`). Any other
   one translating a Java getter method stays a `readonly-field`.
8. **`Duration` naming** (for P3): a Java `Duration x` parameter or option becomes
   `xMs: number` (milliseconds), for every Java `Duration` parameter, single-form methods
   included. J6 checks an option's name; J7 checks a positional parameter's
   (`duration-name`) and both types.

Answered 2026-11-05, for P3:

9. **Trailing optional parameters:** when Java's forms differ only by trailing
   parameters, a JavaScript function taking the longest form's parameters, those the
   shorter forms lack optional, is accepted beside the options object
   (`send(record, callback?)`).
10. **An option whose Java parameter may be null** may be left out, omission meaning
    null. "May be null" is read from the Java index: the parameter's type is a boxed
    primitive (`Integer`, `Long`, ...) or a key / value type variable (`K`, `V`), or the
    form's Javadoc `@param` for it contains the word `null`.
11. **Gaps kept in the baseline:** the serializer constructor overloads
    (`not-options KafkaProducer.constructor`) and `KafkaFuture.get(timeout)`
    (`not-options KafkaFuture.get`).
12. **`probe`** is an A2 allowlist entry: a test-only registration, called by
    `test/unit/_native.test.js`. An entry that is not an unused registration any more is
    an `unused-allowance` finding.
13. **N1–N8 are all built**, the heuristic N5, N7 and N8 included, each with an explicit
    allowlist whose entries carry a reason; an entry that waives nothing is a finding.

Also following the Python conventions (root CLAUDE.md, "Signatures"): J6 merges the
parameters of different forms at one position whose Java types map to one host type into
one, named after the earliest-declared form (`KafkaProducer(Map configs)` /
`(Properties properties)` -> `configs`).

Still open, to be asked when their phase starts:

14. **Runner versus tests** (P6): should `layout.test.js` and `header_conformance.test.js`
   move into the runner?
15. **Doc depth** (P5): the 150% size rule and the `WARNING` / `doc-contract-changes.md`
   flow for JS (D3)?
