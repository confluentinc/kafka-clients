<!--
Copyright 2026 Confluent Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Proposal: `javascript/CLAUDE.md` wording for the rule linter

This is a proposal for the user to apply. No agent edits `javascript/CLAUDE.md`. It has two parts:

- **Part 1** adds a "(checked by `kafka/…` / `check-…`)" reference to each rule a linter rule checks. The
  wording of each rule does not change.
- **Part 2** proposes the wording changes that PLAN.md §8 decisions 2, 3 and 5 to 19 call for.

Each rule's checks are described in the comment at the top of its file in
`javascript/scripts/lint/rules/`, and in PLAN.md §4. The linter is two tools (PLAN.md §3.5):
ESLint runs the rules that read TypeScript, named `kafka/<rule>`, with typescript-eslint's
recommended type-checked rules on `lib/`; the runner runs the addon's C rules, named
`check-<rule>`. no-todo, exception-word and java-line are split by file type, so they are
named on both sides (`kafka/no-todo and check-no-todo`). It runs as:

- `make -C javascript lint-rules`;
- its own tests, `make -C javascript test-lint`;
- both, as part of `make verify-javascript`, through `make lint-javascript`.

## Part 1: "(checked by …)" references

The references below are listed in the file's order. Each rule gets one of three forms:

- "(checked by `kafka/…` / `check-…`)" only where every mutation of the rule's text that the linter can
  see fails it;
- "(partly checked by `kafka/…` / `check-…`: … ; not: …)", naming what is checked and what is left to tests
  and review;
- "(not checked by the linter: tests / review)".

The parenthesis goes at the end of the rule or sentence named. What is not checked stays with the
tests and the reviewer (PLAN.md §5).

### Ground truth

- Item 2, "The Java public API": `(partly checked by kafka/java-name, kafka/java-members,
  kafka/java-completeness, kafka/public-audience, kafka/no-deprecated-translation,
  kafka/overload-shape and kafka/type-mapping: the surface, i.e. names, members, completeness,
  audience, deprecation, forms and types; not: behaviour)`.

### Boundary rules

1. "Java shape, host idiom, no Kafka logic": `(partly checked by check-no-binding-queue: no
   binding-side lock or record store; not: the checks the binding makes or repeats, nor Kafka
   logic in general)`.
2. "The notify hook only signals": `(partly checked by check-notify-hook: the hook is exactly the
   notify_mtx lock, the notify_open test guarding one nonblocking threadsafe-function call, and
   the unlock, no other statement; not:
   notify_open cleared under the lock before the threadsafe function is released or finalized)`.
3. "One drainer per client": `(partly checked by check-single-drainer: __execute_callbacks_err
   only in the executor cbq_drain calls, cbq_drain only from call_js and send()'s pump, its loop,
   and no destroy inside a drain or reached from a completion; not: the guard that makes a nested
   drain or one after destroy began a no-op, nor the cooperative scheduling of the second
   paragraph)`.
4. "Waiting calls use the `_cb` twin": `(checked by check-cb-twin-only and
   kafka/waiting-call-promise; the exception's single caller, the finalizing branch, by
   test/static/header_conformance.test.js)`.
5. "Never call `KafkaFuture_get`": `(partly checked by check-cb-twin-only: every
   kafka_common_KafkaFuture_get* call, the _cb forms included; not: the future being destroyed
   unread)`.
6. "One `Callback_t` per record": `(partly checked by check-completion-order: the order (user
   callback, future, report), one report, and no report outside the destroying guard; not: the
   error passed, NULL or an owned local-callback error carrying the message thrown)`.
7. "No binding-side send queue, buffer, cap or lock": `(partly checked by check-no-binding-queue,
   a heuristic with an allowlist: no lock outside callback_queue.c, no module or instance store
   of records; not: that the ordering contract is documented on send, in its declaration and in
   the README)`.
8. "Zero extra copies of key and value": `(partly checked by check-zero-copy, a heuristic with an
   allowlist: no copy or allocation of the key and value on the send path, the kafka_Bytes_t
   over the buffer's own memory, and in lib/ no copy of a bytes-typed value (Buffer.from,
   new Uint8Array, Uint8Array.from, slice); not: the napi_ref kept until the hand-over
   completion, nor the user-facing contract)`.
9. "Ownership follows the signature": `(partly checked by check-ownership: every owned handle type
   destroyed somewhere, by type and not per path, and no borrowed one destroyed in a delivery
   completion; the out_err slots by check-out-err; not: a leak on one path, nor the orphan
   list)`.
10. "Teardown order": `(partly checked by check-single-drainer: the destroy outside a drain; not:
    the order of the steps)`.

### Threads and liveness

- `(not checked by the linter: tests / review)`.

### Java shape, JS idiom

- Supported import forms: `(not checked by the linter: test/typing)`.
- "Class and member names follow Java": `(checked by kafka/java-name for the classes and
  kafka/java-members for the members)`.
- "Java getters are read-only properties …": `(partly checked by kafka/getters-readonly and
  kafka/java-members: a property is read-only (a get accessor or a static readonly constant)
  and named by the getter rule; not: which Java getters are properties, since a getter kept as
  a method passes, as KafkaFuture's isDone() is)`.
- "One Java overload set is one function …": `(checked by kafka/overload-shape)`.
- "A blocking Java method returns a `Promise` …": `(partly checked by kafka/waiting-call-promise
  and kafka/type-mapping: the return types, from the native path and the type table; not: when
  each Promise settles)`.
- The hand-over error and unhandled-rejection bullet, and the `Callback` bullet: `(not checked
  by the linter: tests)`.
- "Java `null` is `null` … A Java `long` crosses as a JS `number`": `(partly checked by
  kafka/type-mapping: the type table and long as number; not: nullability, which it strips
  before comparing)`.
- "Errors: …": `(partly checked by kafka/java-name: the class names, their Error suffix and their
  folders; not: the hierarchy, the fields or fromNative, which the generator, check-generated
  and the tests cover)`.
- The after-close and configuration bullets: `(not checked by the linter: tests)`.

### Files and layout

- `src/` bullet: `(not checked by the linter: test/static/layout.test.js)`.
- `lib/` bullet:
  - "one public Java type per file, snake_case names": `(checked by kafka/java-name)`;
  - "A `_` member another class calls is TypeScript `private` …": `(checked by
    kafka/private-members)`;
  - "The addon is loaded, and its one-time setup done, in `lib/_native.ts`, never in an
    `index.ts`; `lib/_native_loader.ts` finds it, …": `(partly checked by kafka/native-registry:
    the loader is imported only by lib/_native.ts and every registered function is used
    through it; not: the loader's search order, platform package first, which the unit tests
    cover)`.
- Types bullet, "the prose docs are TSDoc comments (`@param name -`, `@returns`, `@throws` …), with
  no JSDoc type annotations": `(partly checked by kafka/tsdoc-present and kafka/tsdoc-tags: a
  comment with a description on every export and public member; @param for every parameter,
  function types and backquoted options included; @returns on functions; every @throws
  backquoted, naming a class that exists, and one per Java @throws; no JSDoc types; not: what the
  text says)`.

### Files and process

- "No TODO or FIXME": `(checked by kafka/no-todo and check-no-todo)`. The license header is not checked by the
  linter.
- The `header_conformance.test.js` bullet: see Part 2, item 13.

### Root rules the linter also checks for the client

These are root rules, not `javascript/CLAUDE.md` rules. A sentence under "Files and process"
could name them:

- root §2 and §4, the word "exception": `kafka/exception-word and check-exception-word`;
- `.claude/rules/documentation.md` §7, Java file:line references: `kafka/java-line and check-java-line`;
- root §4, `out_err`: `check-out-err`;
- the `@jsOnly` / `@java` mapping tags: `kafka/java-tags`.

## Part 2: wording proposals

Each item gives the decision, the place in `javascript/CLAUDE.md`, and the proposed text.

1. **Java reference version** (decision 3).
   - Place: "Ground truth", item 2.
   - Replace "Apache Kafka 4.3.1 in `kafka/`" with: "Apache Kafka in `kafka/`, read as the Rust
     client reads it: `@InterfaceAudience.Public` at 4.4, deprecations at 4.3.1 and 4.4
     (`AUDIENCE_REF` / `DEPRECATION_REFS` in `xtask/src/java.rs`)".

2. **The mapping tags** (decision 2).
   - Place: "Java shape, JS idiom", after the names bullet.
   - Add: "An export or member with no Java counterpart carries `@jsOnly <reason>`. One translating
     a Java class it is not named after carries `@java <fqn>`. A tag that is not needed is a
     finding."

3. **`Header` beside `ProducerRecord`** (decision 5).
   - Place: "Files and layout", `lib/` bullet, after "one public Java type per file".
   - Add: "(one exception: `Header` is declared in `producer/producer_record.ts`, beside
     `ProducerRecord`)".

4. **`equals` / `hashCode`** (decision 6).
   - Place: "Java shape, JS idiom".
   - Add: "Java's `equals` and `hashCode` are not translated."

5. **A `public final` field** (decision 7).
   - Place: the getters bullet.
   - Add: "A Java `public final` instance field may instead be a `readonly` property of an object
     the constructor freezes (`Object.freeze(this)` as a top-level statement), and so may an
     enum's `name`, which translates `Enum#name()`. When a name is both a public final field and a
     getter, the field wins."

6. **`Duration`** (decision 8).
   - Place: the overload bullet, after "`close({timeoutMs}?)`".
   - Add: "A Java `Duration x`, a parameter or an option, is `xMs: number`, in milliseconds."

7. **Trailing optional parameters** (decision 9).
   - Place: the overload bullet.
   - Add: "When the forms differ only by trailing parameters, one function taking the longest
     form, the shorter forms' missing parameters optional, is accepted instead of an options
     object (`send(record, callback?)`)."

8. **Merged forms** (the Python convention, root CLAUDE.md "Signatures").
   - Place: the overload bullet.
   - Add: "Parameters of different forms at one position, whose Java types map to one JS type,
     are one option, named after the earliest-declared form (`KafkaProducer(Map configs)` /
     `(Properties properties)` -> `configs`). A type variable is a type of its own: `K key` and
     `V value` never merge, although both are `Data` in JavaScript (`ProducerRecord` keeps
     `key` and `value`, as Python's `ProducerRecord[K, V]` does)."

9. **An option whose Java parameter may be null** (decision 10).
   - Place: the overload bullet.
   - Add: "Such an option may be left out, omission meaning `null`. 'May be null' needs a positive
     statement: a boxed primitive or a `K` / `V` type, or a Javadoc `@param` saying 'may be null',
     'can be null', 'if null', 'null means', 'null for', 'or null' or 'nullable', and none of
     'not null', 'non-null', 'may not be null', 'must not be null', 'cannot be null' or 'not be
     null'."

10. **The test-only registration** (decision 12).
    - Place: "Files and layout", the `lib/_native.ts` sentence.
    - Add: "Every function `src/addon.c` registers is used from `lib/`, except `probe`, which only
      `test/unit/_native.test.js` calls."

11. **The addon rules' allowlists** (decisions 13 and 14).
    - Place: "Boundary rules", a closing paragraph.
    - Add: "N1 to N8 read the addon lexically. N1, N2, N7 and N8 keep an allowlist in their rule
      file, keyed by call site (`file:function:callee#n`, or the exact call) or by declaration,
      each entry with a reason; N4 and N5 name the functions they allow (the drain's callers,
      the completions), each with a reason; N3's is the fixed list of the calls a notify hook
      may make (the queue lock and the threadsafe-function call), with no reasons; N6 has none.
      An entry of N1, N2, N4, N5, N7 or N8 that allows nothing is a finding. One accepted entry is `error_discard`'s unread slot on the destroy of a panic
      error, which is the end of the chain: retrying could loop."

12. **Docs** (decision 15).
    - Place: the Types bullet, after "with no JSDoc type annotations".
    - Add: "There is no size limit and no `WARNING` flow for these docs (root CLAUDE.md §6's 150%
      rule and `doc-contract-changes.md` are the Rust and C APIs')."
    - Add (decision 18): "On a method returning a Promise, a `` @throws `XError` `` documents a
      rejection, its text saying it rejects ('rejects with it if …'), and is how a Java
      `@throws` of a blocking method is documented."

13. **Static tests beside the linter** (decision 16).
    - Place: the `header_conformance.test.js` bullet under "Files and process".
    - Replace it with: "`test/static/header_conformance.test.js` asserts that every `kafka_*`
      function `src/*.c` calls is declared in the bindings header and is not a plain name that has
      an `_err` twin, that the producer calls the functions of the plan, and that the one blocking
      close is the finalizer's. The rule linter checks the rest of rules 4 and 5
      (`check-cb-twin-only`). Keep both passing when adding calls."

14. **Generated config-constant docs** (decision 17).
    - Place: "Generated files", after the config classes' constants.
    - Add: "Each constant's TSDoc is Java's own `_DOC` string of it (the sibling `<NAME>_DOC`,
      or `X_DOC` for `X_CONFIG`), translated as a Javadoc is; a constant without one gets a line
      naming it."

15. **ESLint and typescript-eslint** (decision 19).
    - Place: "Files and process".
    - Add: "`make -C javascript lint-rules` runs the rule linter: ESLint (`eslint.config.cjs`,
      the local `kafka/` rules and typescript-eslint's recommended type-checked rules on
      `lib/`) and the runner (`scripts/lint_rules.js`, the addon's C rules). A finding of the
      standard rules that the code means is kept with `// eslint-disable-next-line <rule> --
      <reason>`, the reason naming the rule of this file it follows."

16. **The baseline.**
    - Place: "Files and process".
    - Add: "`javascript/eslint-baseline.txt` (ESLint's rules) and `javascript/lint-baseline.txt`
      (the runner's) list the linter's findings not burnt down yet, each group with its
      reason. Each only shrinks: a line that matches no finding is itself a finding."
