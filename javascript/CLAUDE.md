# Confluent Kafka JavaScript Binding — Claude Rules

**What this is.** The rules that agents follow when they generate the JavaScript client in `javascript/`, the 4.x line of `@confluentinc/kafka-javascript`. This file holds the JavaScript client's own rules, as `dotnet/CLAUDE.md` holds .NET's.

**How it loads.** Working anywhere under `javascript/` stacks two rulebooks: the root `CLAUDE.md`, then this file. The root file holds the Translation Rules, whose numbered items a number such as §4 names, and the Python Binding Conventions that "As in Python (Group)" points to. A path such as `src/internal/` is inside `javascript/`, and a path that starts with a top-level folder, such as `kafka/` or `design/`, is from the repository root.

**Design documents.** The JavaScript client's plans and status go in `javascript/design/current/`. When a milestone is finished, its documents move to `javascript/design/history/<Milestone>/`, as .NET's do in `dotnet/design/`.

---

## JavaScript Binding Conventions

The JavaScript client, npm package `@confluentinc/kafka-javascript` in `javascript/`, is built only on the C FFI of §4, through a Node-API addon. Its surface and implementation are generated from this section, the Java source, the C FFI header and the previous release's interface; the same inputs give the same result on every run. The source is TypeScript, and `tsc` emits the published `.js` and `.d.ts` files. "As in Python (Group)" means that the named group of the Python Binding Conventions in the root `CLAUDE.md` applies in its JavaScript form. A number such as (Q15) names the team's design question whose agreed JavaScript answer decided the rule. An item under "Not decided yet" stops the work: the agent asks, and invents nothing for it.

- **General rules**
  - Rule 1: Java decides the content, and JavaScript decides only the form, through the rules below, as in Python's rule 1. A change to content is marked *(deviation)*, with its Why, and with a Goes when if it is temporary, and every deviation is agreed in review. A useful JavaScript language feature from 1.x may be kept, marked *(js-only)* (Q1, Q65).
  - Rule 2: Java's standard types map to JavaScript types by the fixed table in Types.
  - Rule 3: where Java has nothing, the standard JavaScript convention applies, marked *(js-only)*.
  - TypeScript first: the types are erased at run time, and a plain JavaScript caller never sees them, so the run-time checks stay: the JavaScript rows of the binding's own checks (Implementation over the FFI), the form check (Q15) and the number checks (Q22). The one exception is a constructor that Java doesn't make public, which only the published types guard (Class family) (Q4, Q16).
  - Every rule that shapes what a caller writes or gets back is checked against TypeScript first, when it's added or changed and again before code is generated from it. Its JavaScript form is written as TypeScript, and it says what `tsc` catches for a TypeScript caller and what a run-time check must still catch for a plain JavaScript caller. An agent that finds such a rule that doesn't say both stops and asks, as for an item under Not decided yet.
  - The Java source is the Apache Kafka release in `kafka/`, the one the Rust core is built from (Q2).
  - Code is generated wherever it can be, and anything that has to differ is marked *(deviation)*. Generated and hand-written code alike is TypeScript source, and the hand-written helpers live in `src/internal/` (Q4, Q13).
- **Scope**
  - In scope are the client families and types of Python's Scope, limited to what the C FFI exposes: the producer, consumer and admin families, every public type reachable from them, their nested types, the config constants and the key-name classes. The share consumer is out, because the FFI has none. As in Python, the assignors, interceptors, partitioner implementations, the `ConfigDef` machinery and `CommonClientConfigs` aren't translated (Q5).
  - Nothing is added except by a JavaScript standard convention (rule 3) or as a deviation. The additions Java lacks are all *(js-only)*, and each one is on the list of invented names (Layout and modules): `[Symbol.asyncDispose]` on the clients, `[Symbol.iterator]` where Java has `Iterable`, `KafkaProducer.create()` and `KafkaConsumer.create()` (Q50, Q53), `TopicPartitionMap`, `TopicPartitionSet`, `MetricNameMap`, the `*Args` option types, and `toJSON()` on value classes, whose output is frozen once released (Q6).
  - Left out, as in Python, are a producer `poll()`, `clientId()`, an event emitter, and `connect()` and `disconnect()`: a client is ready once it's constructed, as in Java, and `close()` replaces `disconnect()`. KafkaJS-style `run()` helpers, an async iterator over `poll()` and `AbortSignal` options are GA candidates, not in the preview, and an `AbortSignal` would only ever come alongside `wakeup()`, never in its place (Q6).
  - The preview offers one API, shaped like Java's. Whether the client also gets a KafkaJS-style API, as 1.x has beside its callback API, is decided after GA, and so is how the migration guide handles the consumer, which loses `partitionsConsumedConcurrently` with one API (decided 2026-11-13). Until then an agent builds none of the KafkaJS-style API (Q1, Q6).
  - A Java form whose only extra parameter has a type that isn't translated is left out *(deviation. Why: a placeholder typed `unknown` checks nothing)*. Where a signature can't be dropped, the type is a placeholder with Java's name, as in `export type Partitioner = unknown`. A type that Java doesn't make public, such as `Time`, is `unknown` with no name *(deviation, as in Python)*. Untranslated types appear only in `MockProducer` and `KafkaMetric`, which gives this list (Q30):
    - Left out: `MockProducer(Cluster, boolean, Partitioner, Serializer, Serializer)`, whose only extra parameter over the next form is `cluster`, so the mock's cluster is always Java's `Cluster.empty()`; and the setter `KafkaMetric.config(MetricConfig)`, whose only extra parameter over the getter is `config`, and which Java says is for the server only.
    - Kept with placeholders: `MockProducer(boolean, Partitioner, Serializer, Serializer)`, with `partitioner` typed `Partitioner | null`; `KafkaMetric`'s constructor, where `valueProvider` is `MetricValueProvider`, `config` is `MetricConfig`, `time` is `unknown`, and `lock` is `unknown` as Java's `Object` is (Q26); `KafkaMetric.config()`, which returns `MetricConfig`; and `KafkaMetric.measurable()`, which returns `Measurable`.
    - So the placeholders are `Partitioner`, `MetricValueProvider`, `MetricConfig` and `Measurable`. `Cluster` and `Gauge`, which Python has, aren't needed, because no JavaScript signature names them.
  - Nothing that Java has deprecated by this client's first major release is generated, as in Python (Class family) (Q71).
- **Layout and modules**
  - In `javascript/`, `src/` holds the TypeScript source, with one folder per Java package and `clients` dropped (`src/consumer/`, `src/common/acl/`), and `src/internal/` holds the hand-written helpers, the addon's generated types (Q4) and the helper classes for Java's `default` methods (Q28). `native/` holds the addon and its build file, node-gyp's `binding.gyp`, and the build runs only there, so npm never compiles on install. `dist/` holds `tsc`'s output, is git-ignored and is the only code published. `npm/` holds one folder per platform package, and `test/`, `examples/` and `bench/` sit beside them (Q13, Q73).
  - Each Java class is one generated snake_case file in its package's folder, which also holds its nested classes (`src/common/topic_partition.ts`). Each package has an index that re-exports its classes, and users import from the package, never from a file: a deep import fails with Node's `ERR_PACKAGE_PATH_NOT_EXPORTED` (Q13).
  - Each Java package has one import path, such as `@confluentinc/kafka-javascript/consumer` and `@confluentinc/kafka-javascript/common/acl`. The root exports only Java's built-in errors and `Duration`. The `exports` field of `package.json` lists only the package paths, so internal files need no `_` prefix, and the folders behind the import paths also exist in the package, for TypeScript setups that don't read `exports` (Q7, Q72).
  - The package is CommonJS only, as 1.x is. `tsc` emits CommonJS with `module: nodenext`, `package.json` has no `"type": "module"`, and each `exports` entry has the `types` and `default` conditions. ESM apps still use named imports, such as `import { KafkaProducer } from '@confluentinc/kafka-javascript/producer'`, because Node finds the names in `tsc`'s CommonJS output, through `export { }` and `export *` alike. One build means one copy of each class, so `instanceof` works across `import` and `require` (Q15), and a CommonJS Jest test can load the client and `jest.mock()` it on Node 22, which an ESM-only package breaks. The indexes re-export only with `tsc`'s own `export { }` and `export *` forms, because Node finds names only in the patterns it knows. For a TypeScript caller, `tsc` checks the imports under `node16`, `nodenext` and from an ESM file. A plain JavaScript caller that imports a name the package lacks gets Node's `SyntaxError: Named export '…' not found` at load, so no run-time check is needed (Q13).
  - A name that JavaScript invents is kept once released. If Java later adds an item of that name, Java's item gets another JavaScript name, chosen with the owner when it happens, as in Python (Modules). The list of invented names also holds `NODE_DEBUG=confluent_kafka`, the `diagnostics_channel` name once it exists, the deprecation warning codes and the platform package names, such as `@confluentinc/kafka-javascript-linux-x64-gnu`. An invented options type ends in `Args` (Q14, Q72).
- **Names**
  - Types are PascalCase. A class name's `Exception` suffix becomes `Error`, and nothing else changes, so `InvalidRegularExpression` keeps its name, and so does `Headers`, although JavaScript has a global of that name (Q8, Q33).
  - Methods keep Java's camelCase names, a leading `get` included, as in `offsetsForTimes()` and `getMostSignificantBits()`. Option keys are Java's parameter names, taken from the interface's declaration, as in Python (Signatures) (Q9).
  - A static member sits on the class that defines it, as in `Topic.GROUP_METADATA_TOPIC_NAME` and `Uuid.randomUuid()`. A constant `List` is a frozen array, and a constant `Set` is a read-only set whose `add` and `delete` throw. `Admin.create()` is on a runtime object named after the interface, beside the TypeScript type of the same name (Q10).
  - A nested Java type is a static member of its class, as in `CloseOptions.GroupMembershipOperation.LEAVE_GROUP`, and a TypeScript namespace with the class's name holds its types (Q11).
  - A static method and an instance method with the same Java name both keep it, as in `CloseOptions.timeout({ timeout })` and `options.timeout()` (Q17).
- **Class family**
  - A data object has public `readonly` fields, set once when it's built. The data classes are the records, `TopicPartition`, `OffsetAndMetadata`, `RecordMetadata`, `Node` and the admin description classes. The generator keeps their list, which is frozen at GA. In a data class, a Java method that takes no arguments and returns a value becomes a field, as in `tp.topic`. Client objects, option classes with read and write pairs, classes that change (such as `Headers`) and names that start with `get` keep methods. The test in one line: if reading it twice can give different answers, cost real work or fail, it's a method *(deviation. Why: the JavaScript guides read data through plain fields, 1.x and KafkaJS already write `tp.topic`, and a field can't fail silently through a forgotten `()`. Permanent)* (Q9). Each data class calls `Object.freeze(this)` at the end of its constructor, so a write such as `tp.topic = 'x'` throws `TypeError` in strict-mode code, which includes ES modules and classes, and does nothing in sloppy-mode code. The spike measures the cost for `ConsumerRecord` (Q9).
  - A `TopicPartition` is frozen when it's created, so a key can't change after it goes into a map (Q25).
  - A Java constructor that isn't public becomes a `private` TypeScript constructor, or a `protected` one where Java's is protected. The binding builds the object through one `@internal` static factory per class, such as `CreateTopicsResult.internalCreate(futures)`, which `stripInternal` removes from the `.d.ts` files. The `.d.ts` files keep `private constructor();` *(not checked)*, and its doc comment says where instances come from, as Python's `TypeError` does in "DescribeTopicsResult cannot be constructed directly; use the admin client's describe_topics()". It names the class's public static factories, or, when the class has none, the methods that return it, such as the admin client's `createTopics()` for `CreateTopicsResult`. Unlike Python, nothing checks this at run time; CI compiles a test against the published `.d.ts` files instead (Q16).
  - A Java abstract class, such as `AbstractOptions`, becomes a TypeScript `abstract class`, so `tsc` refuses `new`, and Java's abstract methods stay `abstract`. For plain JavaScript, its constructor throws `TypeError` when `new.target` is the abstract class itself, as Python raises `TypeError`. An abstract error class is only for catching and has no FFI id (Q16, Q32).
  - Where Java takes a value class, JavaScript takes only an instance of it, and a plain object throws an error with a hint. Java's `Map` and `Properties` constructors become one form with the key `configs`, which also takes the bare map (Signatures). Records are built through their public constructors (Q16).
  - The option classes take one keyed object in their constructor, with one key per setter field, named after the Java field, as in `new CreateTopicsOptions({ timeoutMs: 5000, validateOnly: true })`. They are the `*Options` classes, `CloseOptions`, `ListConsumerGroupOffsetsSpec` and `MockAdminClient.Builder`, and such a class gets this constructor even without a public Java constructor, as `CloseOptions` does *(deviation: Java builds them only through their setters. Why: one call builds a configured object, and the setters stay, so Java's chain works too)*. After construction, each given key, `null` included, calls its setter in a fixed order, not the order the user wrote: the parent's fields first, then Java's field order, and a setter that reads a field runs after the one that sets it. A missing key leaves Java's starting value, and the FFI gets a `set_<field>` call only for the setters that ran. The starting values are documented with `@defaultValue`. Which classes qualify, the key names and the rule for a field with several setters follow Python's rules. Other classes keep only Java's constructors, as in `new NewTopic({ name, numPartitions, replicationFactor }).configs({ 'cleanup.policy': 'compact' })` (Q19).
  - A Java getter and setter pair with one name is one method. Called with no argument, it reads, as in `opts.timeoutMs()`; called with one argument, it sets and returns what Java's setter returns. The setter takes the value on its own or the options object, as in `opts.timeoutMs(1000)` or `opts.timeoutMs({ timeoutMs: 1000 })`, and the setter of a config map also takes the bare map. A reader with its own Java name keeps it, so a pair with two Java names keeps both: `validateOnly(true)` sets, `shouldValidateOnly()` reads, and `validateOnly()` with no argument throws `TypeError`, as a setter whose value is missing does. `null` sets Java's null. `timeoutMs(undefined)` and `timeoutMs({})` throw `TypeError`, and a method that only reads throws `TypeError` when it's given any argument. An option class is read and set only through its methods, and there are no accessor properties. So in plain JavaScript, `opts.timeoutMs = 1000` hides the method and sets nothing, and `if (opts.shouldValidateOnly)` tests the function, which is always true. TypeScript flags both *(not checked)*, and the docs warn about them (Q17).
  - An enum is a class with frozen instances, carrying Java's methods and fields plus `name()`, `ordinal()`, `values()` and `valueOf()`. `toString()` and `toJSON()` return the name, and values compare with `===`. There's no TypeScript `enum`, no string values and no closed string union in a returned type (Q12, Q69).
  - Generic classes keep Java's letters. A method's own type parameter stays generic, a self type becomes `this`, and the records are covariant through `out`. An omitted or `null` key or value binds `never`. Unlike Python, a `*Result` class has no marker for the kind of client, because there's no synchronous family (Q26).
  - TypeScript infers the type arguments from the serdes, and no code, test or doc writes them, except for a mock built without serdes. An omitted serde binds `KafkaProducer<Uint8Array, Uint8Array>` and `KafkaConsumer<Buffer, Buffer>` (Q27). In plain JavaScript, a string key or value given to a client without a serializer throws `TypeError`, with a message that points to `new StringSerializer()`, because a string can't reach the FFI as bytes (Q24, Q27).
  - Each method of an interface that the user implements takes one object with Java's parameter names, as in `onPartitionsRevoked({ partitions })`, and a callback is a function such as `({ metadata, exception }) => void`. Java's abstract methods are required, and its `default` methods are optional members of the TypeScript interface. Each interface the user implements that has a `default` method gets a helper class, such as `ConsumerRebalanceListenerDefaults`, holding Java's default bodies translated from the Java source. When the user's object lacks one of those methods, the binding runs the helper's method with the user's object as `this`, so a missing `onPartitionsLost` calls the user's `onPartitionsRevoked`, as in Java. The binding checks for the method each time it's called and keeps no state. So the binding always gives the FFI a function for a `default` method, never NULL, and the FFI's own NULL default isn't used, as in Python and .NET *(Why: a review comment on Q28 asked for an optional method plus a helper class with the default behaviour; Python keeps Java's default on its `abc.ABC` base class and .NET on `ConsumerRebalanceListenerBase`)*. Serdes get their defaults the same way, so a missing serde `close()` does nothing. The helper classes are in `src/internal/` and aren't exported in the preview, so they add no invented name; exporting one later, so that a user's method can call Java's default as Python's `super()` and .NET's `base` do, is additive (Q28).
  - A client interface, `Producer`, `Consumer` or `Admin`, becomes a TypeScript `interface`, and a runtime object of the same name holds its statics, such as `Admin.create` (Q10). An interface the client hands out, such as `Metric`, and a multi-method interface the user implements, such as `ConsumerRebalanceListener`, become TypeScript `interface`s too, and `Runnable` becomes `() => void`. The binding doesn't check at run time that a user's object has the required methods. The addon passes NULL for a missing one to the FFI's `_err` twin, such as `kafka_consumer_ConsumerRebalanceListener_new_err`, never to the plain entry point, which has no error slot and aborts the process on a NULL, and it turns the FFI's `LocalNullPointerError` ("on_partitions_revoked must not be null") into a `TypeError` (Q28, Q36).
  - Java's JDK interfaces follow a fixed table, as in Python, and an interface missing from it stops generation until it gets a row (Q29). `AutoCloseable` and `Closeable` become `close(): Promise<void>` plus `[Symbol.asyncDispose]`, so `await using` works, and there's no `[Symbol.dispose]`, because a synchronous dispose can't wait for `close()`. `Iterable<T>` becomes `[Symbol.iterator]()`, without Java's `forEach` and `spliterator`. `Comparable<T>` keeps `compareTo()`, and `equals` and `hashCode` stay methods. `toString()` keeps Java's format, with JavaScript's spelling of values (Q37). `Future` becomes `KafkaFuture` (Q44), and `Serializable` and `Principal` aren't translated. The docs say that when an `await using` block throws and `close()` throws too, JavaScript raises a `SuppressedError`.
  - A client has `close()` and `[Symbol.asyncDispose]`, so `await using` closes it (Q21).
  - A map keyed by `TopicPartition` is a `TopicPartitionMap` *(js-only. Why: JavaScript has no value equality for map keys)*. It wraps a `Map` keyed by a string such as `'orders:3'`, whose separator can't occur in a topic name, and keeps each original `TopicPartition`, so `keys()` and `entries()` return `TopicPartition` objects. It doesn't extend `Map`, so `instanceof Map` is false, and it has a `toJSON()` (Q25).
  - A map keyed by `MetricName`, which `metrics()` returns, is a `MetricNameMap` *(js-only, for the same reason)*. It's built like `TopicPartitionMap`: its string key is made from what Java's `MetricName.equals` compares, the group, the name and the tags sorted by key, written so that two different names can't give the same string, such as with `JSON.stringify`. So `get()` with a new `MetricName` of the same fields finds the entry, and the description isn't part of the key, as in Java. `keys()` and `entries()` return the original `MetricName` objects (Q25).
  - Where Java takes a `Collection`, `List` or `Set`, JavaScript takes any iterable, such as an array, a `Set` or a generator, and a bare string where Java takes `Collection<String>` throws `TypeError('topics must be an iterable of strings, not string')`, as Python's guard does. Where Java takes a map keyed by `TopicPartition`, as `commitSync({ offsets })` and `sendOffsetsToTransaction` do, only a `TopicPartitionMap` is taken, and a plain `Map` throws `TypeError` with a hint. A map keyed by strings may be a `Map` or a plain object. A `Set<TopicPartition>` that the client returns, as from `assignment()`, is a `TopicPartitionSet`, the set form of `TopicPartitionMap`, which matches by value *(js-only, as `TopicPartitionMap`)*. A returned collection is a new object that the caller may change, and constants are frozen (Q10, Q25).
- **Futures and waiting**
  - A method returns a Promise if and only if the entry point it calls has a `_cb` twin in the C FFI header (§4), so the split matches Python's, except for the static `create()` of `KafkaProducer` and `KafkaConsumer`, which waits in JavaScript for the serde config functions and then calls the constructor's entry point (Lifetime). From JavaScript, the binding calls only `_cb` forms, accessors and constructors, never a blocking C function. An admin call returns its `*Result` at once, holding a future per key. Constructors are synchronous, as in Java. There's no synchronous family and no `Async*` name. A custom check fails the build when a public method returns a Promise but its entry point has no `_cb` twin, or the other way round, and it allows only those two `create()`s (Q41, Q74).
  - Java's names stay, `commitSync()` and `commitAsync()`, and the same rule covers any other `xSync` and `xAsync` pair. Both return Promises. `commitSync()` settles when the broker answers, and as in Java it retries until it succeeds, fails with an error that can't be retried, or runs out of time. `commitAsync()` settles once the core has taken the commit, and the broker's answer goes to its optional `callback`, which runs inside the next waiting call, such as `poll()`, `commitSync()` or `close()`. Without a callback, a failed commit isn't reported, as in Java. Both must be awaited, because a commit that isn't awaited keeps the consumer busy. The docs say that both return Promises and that only `commitSync()` waits for the broker (Q43).
  - Java's `Future<T>` and `KafkaFuture<T>` are one `KafkaFuture<T>` class, `send()`'s included, with Java's `KafkaFuture` members in Java's order *(deviation, as in Python (Futures): Java's `send()` returns a plain `Future`)*. `get()` and its timed form return Promises, while `getNow({ valueIfAbsent })`, `isDone()`, `isCancelled()` and `isCompletedExceptionally()` are synchronous. The rest are `thenApply()`, `whenComplete()`, `toCompletionStage()`, which returns a native Promise, and the statics `allOf()` and `completedFuture()`. `allOf` and the cancel methods are built in JavaScript, as in Python, and `cancel()` on a `send()` future returns `false` (Q44). On a future over a native handle, `get()` calls `kafka_common_KafkaFuture_get_cb` and `get({ timeout })` calls `kafka_common_KafkaFuture_get_with_timeout_cb`, so the core enforces the deadline and no JavaScript timer runs (Q22). On a future with no native handle, such as `allOf()`'s, `get({ timeout })` races the future against a `setTimeout`, as Python's `asyncio.wait` does. The timer is cleared as soon as either side wins, and when the timer wins, the call rejects with `TimeoutError` and no message, as Java's `CompletableFuture` throws it. A zero or negative timeout rejects at once unless the future is done (Q22, Q44).
  - The Promise behind `get()` is created only when something first waits on the future, because Node ends the process on an unhandled rejection by default, so a failed send that nobody awaits must not create a rejected Promise. This mirrors Python's lazy inner future and gets the same A/B performance check. There's no `then` method, so `await` doesn't wait for delivery, and a send takes two steps: `const future = await producer.send(record)` means the record was accepted, and `await future.get()` means it was delivered (Q44).
  - A failed `get()` rejects with `ExecutionError`, whose `cause` is the failure (Q34).
  - Transactions keep Java's names, on the producer: `initTransactions()`, `beginTransaction()`, `sendOffsetsToTransaction()`, `commitTransaction()` and `abortTransaction()`. A send counts in a transaction once its `send()` Promise has resolved, and the docs state that guarantee instead of telling apps to await: "A send is part of the transaction once its `send()` Promise has resolved. `commitTransaction()` waits for its delivery, so there's no need to await `future.get()` first. A send whose Promise hasn't resolved yet may be left out." Their example awaits the sends with `Promise.all`, for an app that needs every send in the transaction. `beginTransaction()` waits too, so it returns a Promise *(deviation, as in Python: Java's returns at once)*. The binding adds nothing to `commitTransaction()`, `abortTransaction()` or `flush()`, because the core includes every send that returned a `KafkaFuture` and completes it before the call ends (Q51).
  - A new method that waits is one more Promise method when it's on a client. On a class that no client owns, it's an ask for the core (Q42).
- **Signatures**
  - A Java overload set is one method. Where it has several forms, the method takes one options object whose keys are Java's parameter names, and the given keys must match one Java form exactly; otherwise the call throws `IllegalArgumentError` naming every form. Unknown keys throw, and nothing is filled in, so a record with headers or a timestamp needs `partition: null`. Each Java form has one TypeScript overload signature (Q15, Q18, Q58).
  - A call with one parameter takes the value on its own, as in `poll(100)` and `send(record)`. A plain object is always the options object, except for config maps. Any other single value picks its form by `typeof`, `Array.isArray` or `instanceof`, and a value that fits no form throws `IllegalArgumentError` in Python's format, as in `commitSync() takes one of (), (timeout), (offsets), (offsets, timeout); got a string`. There's no bare form where two forms' values can't be told apart. Once released, a bare value keeps its form, and an overload that Java adds later and that would collide is reached only through its options object (Q15).
  - A config map may be passed bare, as in `new KafkaConsumer({ 'bootstrap.servers': 'localhost:9092', 'group.id': 'g' })` and `newTopic.configs({ 'cleanup.policy': 'compact' })`, and `{ configs }` stays as a second shape. A plain object is the options object when one of its keys is a parameter name of that method, and a config map otherwise. This matches Java and departs from Python. Only a config map is taken as a bare plain object. For any other map keyed by strings, a plain object given alone is read as the options object, so the map goes under its parameter name, as in `createPartitions({ newPartitions: { orders: NewPartitions.increaseTo(6) } })`, and a `Map` given alone picks its form by `instanceof`, under the one-parameter rule (Q15).
  - Forms with the same keys are told apart with plain `instanceof`, as in `deleteTopics({ topics: TopicCollection.ofTopicIds(ids) })`, and there's no `Symbol.for` brand. The docs say: "The client supports one copy of the package in an app. If `node_modules` holds two versions, calls that take the client's classes can fail." (Q15)
  - A pattern is only a `SubscriptionPattern`, and a `RegExp` throws an error that points to it (Q15).
  - `null` is Java's null, and it's accepted only where Java accepts null. `undefined` means "not given" only where a key is optional, which is in config maps and in the constructor keys of option classes. Anywhere else, `undefined` throws a `TypeError` that names the parameter and says to leave the key out or pass `null`, inside a list or headers too, and a required key that's missing gets the form error. Results use `null`, never `undefined`, and `Optional<T>` becomes `T | null` (Q18, Q20).
  - A timeout is a `number` of milliseconds or a `Temporal.Duration`. The root exports `type Duration = number | <the Temporal type>`, whose TSDoc says that a number is milliseconds, and every parameter or option key that takes a Java `Duration` is typed `Duration`. Java's `get(long, TimeUnit)` becomes `get({ timeout })`. A timeout key that's left out selects the overload without a timeout: it's sent as `-1`, and the core applies Java's default. A timeout that every overload has, as in `poll`, is required (Q22).
  - A `null` timeout throws `TypeError('timeout must not be null')`, except for `CloseOptions.timeout` and the admin `timeoutMs`, where `null` means the default. The binding doesn't check a negative timeout and passes it to the FFI as it is, because it repeats no precondition (Q35). What happens next depends on the client, as checked in the FFI on 2026-11-13 (Q22, Q35):
    - Consumer: the FFI's `ms()` (`rust/src/ffi/consumer/mod.rs`) rejects it with `IllegalArgumentError` and the core's message, "Timeout must not be negative", from `poll`, `commitSync`, `position`, `committed`, `partitionsFor`, `listTopics`, `beginningOffsets`, `endOffsets` and `offsetsForTimes`, the `ConsumerHandle` forms included *(deviation: Java says "Invalid negative timeout -1" from `poll`, `commitSync` and `position`, and in the other six Java makes no check and waits forever, because its deadline calculation overflows. Why: the binding repeats no precondition. Goes when: the FFI returns Java's messages)*.
    - Consumer `close`: a `CloseOptions` timeout goes through the FFI's `duration_ms()` (`rust/src/ffi/consumer/close_options.rs`), which reads a negative value as zero, so `close` doesn't wait *(deviation: Java's `AsyncKafkaConsumer.close` throws `IllegalArgumentException("The timeout cannot be negative.")`. Why: the binding repeats no precondition. Goes when: the FFI returns Java's error)*.
    - Producer: `close` rejects it with `IllegalArgumentError("The timeout cannot be negative.")`, Java's own message, from the FFI. A `MockProducer` reads it as zero, and Java's `MockProducer` has no check either.
    - Admin client: `close` reads it as zero, through the FFI's `close_timeout()` (`rust/src/ffi/admin/mod.rs`) *(deviation: Java's `KafkaAdminClient.close` throws `IllegalArgumentException("The timeout cannot be negative.")`. Why: the binding repeats no precondition. Goes when: the FFI returns Java's error)*. A negative `timeoutMs` in the admin options follows its own rule below: the binding sends it as `0`.
    - Futures: a timed `get` reads it as zero (`kafka_common_KafkaFuture_get_with_timeout_cb`), so it rejects with `TimeoutError` at once unless the future is done. That matches Java, and the rule for a future with no native handle (Q44).
  - `NaN`, and a finite value above `Number.MAX_SAFE_INTEGER`, throw `RangeError`, and fractions are rounded down. `Infinity` is sent as `Long.MAX_VALUE` *(js-only)*. A `Temporal.Duration` is converted with `total({ unit: 'millisecond' })` and rounded down, one with years, months or weeks throws `RangeError`, and a negative one is handled as the same negative number, by the rule above. Polyfills are accepted, and the `.d.ts` files declare their own small Temporal type (Q22).
  - The admin `timeoutMs` is a Java `Integer`: a value outside 32 bits, or `Infinity`, throws `RangeError`, a negative value is sent as `0`, and `null` means `default.api.timeout.ms` (Q22).
  - A timeout getter returns a `number`, or `null` when it's unset, and `Infinity` for `Long.MAX_VALUE`. The core enforces deadlines, never a JavaScript timer, except on a future with no native handle, such as `allOf()`'s, where `get({ timeout })` races a `setTimeout`, as Futures and waiting says (Q22, Q44).
- **Types** (rule 2; a type that isn't in the table stops generation until its row is decided)

  | Java | JavaScript |
  |---|---|
  | `boolean`, `byte`, `short`, `int`, `float`, `double` | `boolean`, and `number` for the others, so Java's `int` values, such as partitions, stay a `number`. A whole `number` outside the Java type's range, such as an `int` above 2^31 - 1, throws `RangeError`, as Python raises `OverflowError` (Q23). |
  | `long` | `bigint`, as for offsets, timestamps, producer IDs, sizes, lags and the two halves of a `Uuid`. An argument also takes a whole `number`, and a `number` above 2^53 throws `RangeError` instead of rounding; a returned value is always a `bigint`. Timeouts stay Q22's milliseconds or `Temporal.Duration`, and config values follow Q52. `toJSON()` writes a `bigint` as a decimal string, such as `"42"`, which `BigInt()` reads back exactly *(Why: `JSON.stringify` throws on a `bigint`, and a `number` loses digits above 2^53)* (Q23). |
  | `String` | `string`, turned into bytes by the rule below (Q24). |
  | `Object` | `unknown`, never `any` (Q26). |
  | `byte[]` | `Uint8Array` when the caller passes it, and `Buffer` when the client returns it (Q21, Q27). |
  | `Optional<T>` | `T \| null` (Q26). |
  | `Duration` | `Duration` when the caller passes it, and a `number` of milliseconds when the client returns it (Q22). |
  | `Map<TopicPartition, V>` | `TopicPartitionMap<V>` (Q25). |
  | `Map<MetricName, V>` returned | `MetricNameMap<V>` (Q25). |
  | Other collections taken | Any iterable where Java takes a `Collection`, `List` or `Set`, and a `Map` or a plain object for a map keyed by strings (Q25). |
  | A `List` or a Java array returned | An array (Q25). |
  | A `Set` of strings or numbers returned | A `Set` (Q25). |
  | `Set<TopicPartition>` returned | `TopicPartitionSet` (Q25). |
  | A `Collection` returned | A `Set` only where the Java implementation builds a `Set`, and an array otherwise, as in Python (Q25). |
  | A map keyed by strings returned | A `Map` (Q25). |
  | A wildcard, `? extends T` or `? super T` | `T` (Q26). |
  | `Iterable<T>` | `[Symbol.iterator]` (Q6). |
  | `Headers` or `Iterable<Header>` taken | An iterable of `Header` objects and `[key, value]` pairs, as Serialization says (Q31). |
  | A client's `Closeable` | `close()` and `[Symbol.asyncDispose]` (Q6, Q21). |
  | `NullPointerException` | `TypeError` (Q36). |
  | `Pattern` | Left out, with `SubscriptionPattern` in its place (Q15). |

  - A lone surrogate in a string becomes `?`, as Java's encoder writes it. One function, `encodeLikeJava(s)` in `src/internal/encode.ts`, is the only place where the binding turns a string into bytes: it returns `Buffer.from(s, 'utf8')` when `s.isWellFormed()`, and otherwise first replaces each lone surrogate with `s.replace(/\p{Surrogate}/gu, '?')`. `StringSerializer` and `UUIDSerializer` call it for keys and values, the headers code for header keys, and the config code once per client, for group IDs and config values; topic names skip it. A lint bans `Buffer.from(string)` everywhere else, and the binding never cuts a string inside a surrogate pair (Q24).
- **Errors**
  - `KafkaError extends Error` heads the error tree, with one generated class per id in `kafka_common_Error_e`, following Java's class chain. `generate-error-codes` has a TypeScript emitter, and `check-generated` fails on stale output. The class is the check: there's no `is*()` method and no `code` property, and `name` is the fallback. The FFI id is kept in the private `_ffiId`, for the binding and the test harness (Q32).
  - Every class sets `name` to its class name, which is frozen once released. The root exports Java's built-in errors, such as `IllegalStateError`, `IllegalArgumentError`, `UnsupportedOperationError`, `ConcurrentModificationError` and `TimeoutError`. As in Python, `IllegalArgumentError` and `IndexOutOfBoundsError` extend `RangeError`, and the others extend `Error`. There are two `TimeoutError` classes, the root's and Kafka's in `common/errors`. Neither is the `DOMException` that `AbortSignal.timeout()` throws, and the docs say which call throws which (Q33).
  - A synchronous method throws. A method that returns a Promise never throws: it rejects, argument errors included. The binding copies a native error into JavaScript and frees it before the Promise settles, and an error with nowhere to go goes to `process.emitWarning` (Q34).
  - The binding repeats none of Java's preconditions. The FFI checks the mandatory parameters and the core checks the rest, and their error reaches the user as it comes, with Java's message where the core has it. The one exception is `group.id`: `groupMetadata()` and `committed(null)` without `group.id` throw `InvalidGroupIdError` with Java's message from the binding *(temporary. Why: the core checks `groupMetadata()` with a panic, which the FFI turns into `IllegalStateError` with `InvalidGroupIdError`'s message. Goes when: the FFI returns `InvalidGroupIdError`)*. A panic that the core catches reaches the user unchanged, and the docs say to close that client and create a new one (Q35).
  - A null where Java forbids one throws `TypeError`, and there's no `NullPointerError` *(deviation: Java throws `NullPointerException`. Why: `TypeError` is JavaScript's error for a null where an object is needed)*. The FFI's `LocalNullPointerError` becomes a `TypeError` that keeps Java's text, such as "topic cannot be null". The binding's own null checks are listed in Implementation over the FFI, and all checks run in Java's order (Q36).
  - Messages pass through unchanged: `err.message` is Java's `getMessage()`, or `''` when that is null. The binding writes messages only for its own errors, and those name JavaScript types. An error built from a cause alone takes the cause's `toString()` as its message, as Java's does. No message is reworded, the busy message included (Q37, Q48).
  - `cause` is read from `kafka_common_Error_source`. A value that the user's callback throws, or rejects with, crosses the FFI as `LocalCallbackError` and comes back as the same value, held through a Node-API reference. It's wrapped where Java wraps it, as in `KafkaError('User rebalance callback throws an error')` with the user's value as its `cause`, and a `KafkaError` passes unwrapped. An error injected into a mock is thrown again as the same instance (Q38, Q47).
  - An error's constructor takes one options object with `message` and `cause`, and Java's other parameters keep their names, as in `new TopicAuthorizationError({ message, unauthorizedTopics })` *(deviation: Java's names differ from class to class. Why: one shape for every error, so the generator and the mocks build any error the same way)*. A single value is read by its type: a plain object is the options object, a string is the message, and an `Error` is the cause, as in `new TimeoutError('Timed out')`. A second argument throws `IllegalArgumentError` in the form check's format (Q15), listing that class's forms, as in `KafkaError() takes one of (), (message), (cause), ({ message, cause }); got 2 arguments`, because a second argument is a call that fits no form. Payload getters stay methods, over private fields (Q39).
- **Serialization**
  - The core gets NULL serdes, and the binding runs the serdes in JavaScript, as in Python: serializers inside `send()`, before the record reaches the core, and deserializers inside `poll()`, after the core returns records. A serializer is called for `null` too, and a deserializer isn't, as in Python. A missing serde means bytes *(deviation, as in Python: Java fails)* (Q50).
  - A serde is an object with Java's methods, such as `{ serialize({ topic, headers, data }), close?() }`, with `deserialize` in place of `serialize` for a deserializer *(deviation: one method for Java's two `serialize` methods. Why: JavaScript can't have two methods with one name, and with positions a serde written like Java's two-argument form would get the headers as its data)*. `close()` runs when the client closes, and a failure there goes to `process.emitWarning`, never thrown. There's no `configure()`: a constructor argument comes ready, and Java doesn't call `configure()` for those either, while a function in a serde config key gets the config as its argument (Q50, Q53).
  - `serialize` and `deserialize` may return a Promise, which the binding awaits *(js-only, as 1.x's serdes do. Why: Schema Registry serdes fetch schemas over the network, and JavaScript can't block)*. A serde that returns a plain value adds no microtask. Sends that aren't awaited may then reach the core out of call order, like Java threads, and `await send()` keeps the order (Q50).
  - The built-in serdes are Java's classes, built with `new` as in Java: `new StringSerializer()` as a constructor argument, and `() => new StringSerializer()` in a serde config key. That differs from Python, whose serde classes are private behind factories such as `string_serializer()`. They are the serializer and the deserializer of `String`, `ByteArray`, `Short`, `Integer`, `Long`, `Float`, `Double`, `Boolean`, `UUID` and `Void`, translated from the Java source and tested with Java's vectors, so they match Java byte for byte and keep Java's messages. Their types follow the Types table, so `LongSerializer` takes a `bigint` or a whole `number` and `LongDeserializer` returns a `bigint`, and `ByteArrayDeserializer` returns the `Buffer` it's given, over the fetched bytes (Q24). Two types hold for these classes only: `Void` is `null`, and a `UUID` is a string in the canonical form, in either case, such as `crypto.randomUUID()` returns, which `UUIDSerializer` writes in lowercase, as Java's `UUID.toString()` does. A string that isn't such a UUID, or bytes that don't hold one, throw `SerializationError('Error parsing data into UUID')`, Java's message for the bytes *(deviation: Java's parser also reads some shorter forms, such as `1-2-3-4-5`. Why: they're a quirk of the JDK's parser, and Java's serializer never writes them)*. The `String` and `UUID` serdes have no `configure()` and use UTF-8 only *(deviation: Java's read a charset from `key.serializer.encoding`, `value.serializer.encoding` or `serializer.encoding`, or the matching `deserializer` keys, in `configure()`. Why: serdes have no `configure()` here, and Node can write strings in only a few of Java's charsets. Permanent)*. Java's `ByteBuffer`, `Bytes` and `List` serdes are left out *(deviation. Why: JavaScript has no `ByteBuffer`, Kafka's `Bytes` isn't translated, and the `List` serdes read class names in `configure()`, which JavaScript can't load. Goes when, for `List` only: a rule says how its inner serde is given)*. There are no serde factories (Q50, Q53).
  - For a TypeScript caller, `tsc` refuses a class where an instance belongs, as in `keySerializer: StringSerializer`, and a key or value of another type than its serde's, because the client's types come from its serdes (Q27). In plain JavaScript, any function given as a serde constructor argument throws `TypeError` with "did you forget `new`?", as Python's "did you forget `()`?" does for a class. A built-in serializer given a value of another type, such as a number for `StringSerializer`, throws `SerializationError`, as Java's `send()` does for a serializer's `ClassCastException`, with a message that names the JavaScript types (Q50).
  - When a deserializer fails, `poll()` rejects with `RecordDeserializationError`, and the binding seeks back, so the next poll returns the failed record again *(deviation, as in Python)* (Q50).
  - A serde comes as a constructor argument, `keySerializer`, `valueSerializer`, `keyDeserializer` or `valueDeserializer`, or through the config keys `key.serializer`, `value.serializer`, `key.deserializer` and `value.deserializer`, as in Java and Python. A config key takes a function, `({ configs, isKey }) => serde`, as in `{ 'key.serializer': () => new StringSerializer() }`, which the client calls once with the whole config, in place of Java's class and its `configure(configs, isKey)` call *(deviation: Java and Python take a class or a class name. Why: JavaScript can't load a class by name synchronously)*. The constructor takes only a synchronous function, and `create()` also takes one that returns a Promise (Lifetime). A class name or a serde object throws `ConfigError`, and so does a class, as in `{ 'key.serializer': StringSerializer }`, with the hint "did you mean `() => new StringSerializer()`?", naming the class. A function that returns a Promise throws `ConfigError` in the constructor, with a message that points to `create()`. For a TypeScript caller, `tsc` refuses a class name, a serde object or a class in these keys, because their type is the function's. The constructor and `create()` share one config type, so `tsc` doesn't refuse a function that returns a Promise given to the constructor; the run-time check catches it, as it catches all of these for a plain JavaScript caller. A constructor argument wins, and its config key is then ignored, as in Java and Python. The client's own serde keys never reach the core, as in Python, while the other role's keys do, and the core lists them as unused. The function gets a config object that records the keys it reads, and the binding leaves those keys out of the config it sends to the core, as Java's `originals()` marks them as used, so a key such as `schema.registry.url` isn't listed as unused (Q53).
  - The binding doesn't copy a key or value that the app hands over as a `Buffer` or `Uint8Array`, one that a serializer returns included *(deviation: Java copies the bytes inside `send()`. Why: it saves a memory copy of each Buffer, as the Python binding does)*. The core reads the app's own bytes until it registers the record, and the binding holds a reference to the Buffer until the send settles and drops it on the JavaScript thread. Header values are always copied, because the C API has no borrowed form for them. `send()`'s docs and the README say: "The client reads a `Buffer` or `Uint8Array` key or value after `send()` returns. Don't write to it, transfer it or resize it until the Promise from `send()` settles; awaiting `send()` is enough. A serializer must return a new Buffer for each record." A `Buffer` over a `SharedArrayBuffer` is borrowed the same way, not copied, and the docs add: "If other workers share the Buffer's memory, they mustn't write to it until then either." This rule covers produce only (Q21).
  - The binding doesn't copy a consumed key or value either. Each is a `Buffer` over the record's own bytes in the core's fetch buffer, made with `napi_create_external_arraybuffer`, as the Python binding hands out memoryviews into the core's batch. The binding keeps the native record alive until every `Buffer` over it has been garbage collected, and then destroys it. Where the runtime refuses external buffers with `napi_no_external_buffers_allowed`, as Electron does, the binding copies the bytes instead. The consumer's docs and the README say: "A key or value from `poll()` points into the memory the client fetched, and a value you keep holds that whole fetch in memory. To keep a value for long, copy it with `Buffer.from(value)`." They also list Electron as a known limitation: "Electron doesn't allow external buffers, so on Electron the client copies each consumed key and value." (Q24)
  - Node isn't told the size of the fetch behind a consumed key or value: the binding makes no `napi_adjust_external_memory` call, and the FFI needs no change. Giving that memory back is the app's job, as the docs above say. The binding keeps nothing alive on its own, so once the app drops every record from a fetch, with its keys, values and headers, a later garbage collection frees that fetch's memory. No type changes, so neither `tsc` nor a run-time check has anything to catch. The spike still measures whether copying a small value costs less than wrapping it, as a performance check only, which changes no rule by itself (Q24).
  - `Headers` and `RecordHeader` are plain TypeScript classes that hold their own data, translated from the Java source, as Java's, Python's and .NET's do, and the core gets a copy of the headers only at `send()` (Q31). `Headers` joins Java's `Headers` interface and its class `RecordHeaders`, as Python's does: it has the interface's methods, `RecordHeaders`' constructors `()`, `(Header[])` and `(Iterable<Header>)`, its `equals()`, `hashCode()` and `toString()`, and its read-only state, whose `setReadOnly()` and `isReadOnly()` are `@internal` *(deviation, as in Python. Why: Java's `RecordHeaders` is internal, and its public constructors are how Java users build headers)*. `Header` is Java's interface, with `key()` and `value()`, and `RecordHeader` is the class an app builds, through Java's `(String key, byte[] value)` constructor. It keeps the value it's given without copying, as Java's does, and its `value()` returns that value as a `Buffer` over the same memory, or `null`. `add(header)` stores that very object, as Java's does, and `add(key, value)` makes a new `RecordHeader`. The list lives in JavaScript, so a `Header` the app has read stays valid however its `Headers` changes later, as in Java. These are ordinary classes, so `tsc` checks their calls, and a plain JavaScript caller gets the usual run-time type checks (Q31).
  - A `ProducerRecord` makes its own `Headers` from the headers it's given, holding the same `Header` objects, as Java's `new RecordHeaders(headers)` does. Inside `send()`, once the serializers have run, the record's `Headers` becomes read-only, so its `add()` and `remove()` throw `IllegalStateError('RecordHeaders has been closed.')`, as Java's do *(deviation, as in Python: Java makes the headers read-only after its metadata wait, so a send that times out there leaves Java's headers writable. Why: the serializers run in JavaScript before the record reaches the core)*. The binding then copies each key and value into a native `RecordHeaders`, made with `kafka_common_header_internals_RecordHeaders_new`, and passes it to `kafka_producer_ProducerRecordOptionsBuilder_set_headers`, which makes its own copy, so the binding destroys its `RecordHeaders` right after the call. No type changes, so `tsc` has nothing to catch, and the read-only error is a run-time error for every caller (Q31).
  - Each consumed record has its own `Headers`, which the app can change, as Java's can, and a change touches only that record. A deserializer gets the record's own `Headers`, as in Java, so the app later reads what the deserializers left. The `Headers` copies its list out of the record's headers, which `kafka_consumer_ConsumerRecord_headers` lends out `const`, the first time the app or a deserializer reads or changes it, not at `poll()`, as Java waits for a header's first `value()` to copy its bytes. Each key becomes a string, and each value a new `Buffer` with its own copy of the bytes, or `null`. Until that copy, the `Headers` keeps the native record alive, as Q24's Buffers do, and after it, it holds nothing native. `record.headers` is never `null`: it's an empty `Headers` when the record has none. No type changes, so neither `tsc` nor a run-time check has anything new to catch (Q31).
  - Wherever Java takes `Headers` or `Iterable<Header>`, as in `ProducerRecord`, `ConsumerRecord`, `RecordDeserializationError` and `new Headers()`, the binding also takes `[key, value]` pairs, as Python's does: each element is a `Header` or a pair, mixed in any order, and a pair becomes a new `RecordHeader` *(deviation, as in Python: Java takes only `Header` objects. Why: a convenience for writing headers. Decided for now on 2026-11-24)*. A `Headers` given where Java takes `Headers` is kept as it is, as in Java. The type is `Iterable<Header | readonly [string, Uint8Array | null]>`, so `tsc` refuses any other element. For a plain JavaScript caller, a run-time check throws `TypeError` for an array element that isn't such a pair, with Python's messages in JavaScript's words, such as `TypeError('headers[1] key must be a string; got number')`. A `null` element and a `null` key throw Java's own `TypeError`s, "Header cannot be null." and "Null header keys are not permitted" (Q31, Q60).
- **Configuration**
  - `configs` is a plain object with Java's dotted keys, and a `Map` throws `TypeError`, because its entries aren't object keys. The binding has no `ConfigDef` table and checks no value. It converts each value to the string that the FFI takes, where `true` becomes `'true'`, arrays are joined with commas, and numbers and `bigint`s are written in decimal, so a `long` above 2^53 goes as its exact digits and the core checks it. It passes every key to the core except the client's own two serde keys and the keys that a serde config function reads, so the other role's serde keys reach the core, which lists them as unused, as Serialization says (Q53). `null` and `undefined` leave the key unset. The core checks types, ranges, enums, unknown keys and cross-key rules, with Java's messages that name the key, so `{ 'linger.ms': true }` fails with the core's "Not a number of type LONG" rather than Java's text. Any other value becomes text too, so an object becomes `[object Object]` (Q52).
  - librdkafka's keys and a 1.x `kafkaJS` block reach the core as unknown keys, which the core lists in one INFO log line. The binding adds no check or warning of its own, so the migration guide says that such keys are now ignored (Q54). Every consumer needs `group.protocol: 'consumer'` until the core changes its default, and every example sets it. `interceptor.classes` and `partitioner.class` are unknown keys that the core reports, as in Python, and `partitioner.type` selects a built-in partitioner and passes through (Q52, Q53).
  - The default partitioner is the core's, which hashes keys with CRC-32 (`ConsistentRandomPartitioner`, librdkafka's `consistent_random`), and the binding doesn't change it. 1.x's KafkaJS-style API and Java hash keys with murmur2, so the same key can land on another partition. The migration guide says so, and names `partitioner.type: 'Murmur2RandomPartitioner'` for an app that must keep murmur2's placement (Q53).
  - No config value is a callback, and no constructor takes an error callback or a logger; errors throw or reject. The core logs to stderr through `env_logger` under `RUST_LOG`, and once the core has a log hook, its logs go to a `node:diagnostics_channel` channel. The binding's own warnings, such as the preview warning and deprecations, use `process.emitWarning`, and its debug lines use `util.debuglog('confluent_kafka')`, shown with `NODE_DEBUG=confluent_kafka`. 1.x's `rebalance_cb` becomes a listener passed to `subscribe()`, and its `offset_commit_cb` the commit call's callback. A custom OAuth token callback has no replacement until the core has a token hook (Q56).
  - As in Python, `ProducerConfig`, `ConsumerConfig` and `AdminClientConfig` are translated for their constants only, such as `ProducerConfig.BOOTSTRAP_SERVERS_CONFIG` *(deviation: their `ConfigDef` machinery isn't translated. Why: the binding has no `ConfigDef` table (Q52))*. Key-name and value classes, such as `TopicConfig` and `SaslConfigs`, are translated in full, without their `ConfigDef` methods, and `CommonClientConfigs` isn't translated. The TypeScript type of `configs` lists the known keys for autocompletion and still takes any string, through `(string & {})`, because unknown keys go to the core *(js-only, types only)* (Q54, Q55).
- **Threads and callbacks**
  - Every callback runs on the JavaScript thread that owns the client, the main thread or the worker that created it, and never on a Rust thread. Each client has one thread-safe function: the FFI's notify hook only signals it, and the binding then drains `<Client>__execute_callbacks()` on the JavaScript thread. The binding adds no thread (Q45).
  - Listeners and commit callbacks run while the call that delivers them (`poll`, `commit`, `close`) is pending, and that call settles only after they report. Send callbacks run in a later turn of the event loop, or inside a later `send()`. So that a tight `await send()` loop can't starve deliveries, every 64th send that is accepted at once, and the first send after one that had to wait, runs the producer's queued callbacks before sending, and every 1000th send yields to the event loop with `setImmediate`. This is Python's fix, and its numbers are measured again in Node (Q45).
  - The rebalance listener's methods, the commit callback and the send callback may return a Promise *(deviation: Java's callback methods return `void`. Why: JavaScript can't block, so a Promise is the only way a callback can wait for async work, such as a commit in `onPartitionsRevoked`)*. The binding wraps the result with `Promise.resolve(result)` and calls `__set_callback_result` only when it settles. A rejection, or a throw before the callback returns, is the callback's error, handled as in Java: it crosses the core as `LocalCallbackError`, the consumer call that ran the callback (`poll`, `commit*`, `close`) fails with it, the producer logs a send callback's error, and no rejection is left unhandled. The core waits for send callbacks too, so a slow send callback delays its record's future and `flush()` (Q47).
  - While a listener's or commit callback's report is pending, the `ConsumerHandle` methods work, through their `_cb` forms, and the binding keeps draining callbacks; the consumer's `poll`, `subscribe`, `assign` and `close` fail with `ConcurrentModificationError` *(deviation, as in Python)*. Inside a send callback, `flush()`, `close()`, the transaction calls and awaiting a later record's future would wait for that same callback, so they're documented as anti-patterns: the core refuses them only before the callback's first `await`, and after it they can hang (Q46).
  - A callback that never settles has no timeout, as in Java: it blocks the call that ran it, such as `poll()` or `close()`, `wakeup()` doesn't end it, and the docs say so. The binding doesn't destroy a consumer while a report is pending, and never reports once destroy has returned (Q49).
  - `wakeup()` is a plain synchronous method that goes straight to the FFI and works at any time from any code on the client's thread, such as a timer or a signal handler. JavaScript has no asynchronous exceptions, so there's no Ctrl+C rule like Python's: the app wires `SIGINT` and `SIGTERM` to `wakeup()` and closes once its loop ends (Q48). There's no seek-back after a wakeup, because the core's `poll` either returns records or fails *(not checked)*. The preview has no per-call cancellation, and an `AbortSignal` may be added later, but never in place of `wakeup()` (Q49).
  - The producer and the admin client take any number of calls at once. The consumer takes one call at a time: the binding passes the core's error through, with no tracking and no async locks of its own, and `wakeup()`, `metrics()` and the callback window are exempt. The busy message is the FFI's, unchanged: "KafkaConsumer is not safe for multi-threaded access." for an overlapping consumer call, and, for a `ConsumerHandle` call outside a callback, the core's longer message, which starts with the same sentence. `close()` during a pending `poll()` fails, as in Java, so the app calls `wakeup()`, lets its loop exit, and then awaits `close()` (Q48).
  - A client stays in the thread that created it. The preview doesn't support clients in worker threads, because a worker that ends with its client still open, through `worker.terminate()`, `process.exit()` or an uncaught error, may crash the whole process until the cleanup hook under Future improvements exists. The binding doesn't refuse a client in a worker and keeps no check for it, so the rules above still say how one behaves. The docs say: "The preview doesn't support clients in worker threads. Create clients in the main thread, and send plain data to and from workers." They also show how to turn `Buffer` and `Headers` values into plain data (Q16, Q21).
- **Lifetime**
  - No finalizer closes or destroys a client that's still open. The Node-API finalizer only calls `_destroy` for a client that's already closed (Implementation over the FFI), and `close()` releases the registrations, on the JavaScript thread. A client that's never closed runs until the process exits, with a warning *(js-only: Java logs nothing)*. Apps use `await client.close()` or `await using` (Q21, Q61).
  - `await KafkaProducer.create(...)` and `await KafkaConsumer.create(...)` build a client whose serde config functions may return a Promise, as a Schema Registry serde's may, while the constructor takes only synchronous ones *(js-only, by rule 3: a constructor can't wait, and a static `create()` that returns a Promise is JavaScript's usual way to build an object that must wait)*. `create()` takes the constructor's arguments, awaits each serde config function in turn, the key's first, as Java builds the key's serde first, and then builds the client as the constructor does. It makes no network call of its own. `new KafkaProducer(...)` and `new KafkaConsumer(...)` stay, for clients whose serdes don't wait. For a TypeScript caller, `create()` takes the constructor's types and returns a Promise of the client, so `tsc` reports a forgotten `await` as a missing method, such as `send`; a plain JavaScript caller then gets JavaScript's own `TypeError`, such as `producer.send is not a function`. The admin client takes no serdes: `Admin.create()` is Java's factory and stays synchronous, and the docs say why the two `create()`s differ. The mocks take no serde config keys, so they have no `create()` (Q50, Q53).
  - When building a client fails after a serde config function has returned, the client closes the serdes that those functions returned, as Java's constructors close what they've built. A serde config function that throws, or whose Promise rejects, fails the build with `KafkaError('Failed to construct kafka producer')`, or `KafkaError('Failed to construct kafka consumer')`, whose `cause` is the failure, as Java's constructors wrap it: the constructor throws it, and `create()` rejects with it. The `ConfigError`s of the serde config keys (Serialization) aren't wrapped, as Java's `ConfigException` from reading the config isn't. A Promise that the constructor refuses is still handled: the client closes the serde it resolves to, and its rejection isn't left unhandled. Errors carry no types, so all of this is run-time behaviour for TypeScript and plain JavaScript callers alike (Q50, Q53).
  - A client keeps Node running from the moment it's created until `close()` closes it, as KafkaJS and 1.x do from `connect()` to `disconnect()`. The binding leaves the client's thread-safe function referenced, as Node-API creates it, and calls `napi_unref_threadsafe_function` once `close()` has closed the client, so nothing is counted. In a worker, it keeps the worker running the same way. A script that forgets `close()` never exits on its own *(deviation: Java's client threads are daemon threads, so a program can exit with a client still open. Why: Node exits in the middle of an `await` when nothing holds it, so this way no pending send or poll is cut off, as with KafkaJS and 1.x)* (Q21).
- **Implementation over the FFI**
  - Only `KafkaProducer`, `KafkaConsumer`, the admin client and the three mocks call the addon, because the mocks wrap the FFI's mocks (Q62). Value types, records, enums, errors, `*Options`, `*Result`, serdes, `Headers` and `RecordHeader` are plain TypeScript, translated from the Java source. The one call into the addon outside the clients is a consumed record's `Headers` copying its headers out of the native record, once, on its first read or change (Q31, Q57).
  - The binding adds no FFI entry points, and it follows the naming rules of §2 and §4. When an entry point is missing, its overload is left out and listed in the docs (Q3, Q58).
  - The addon is a Node-API addon in C or C++, which the spike confirms, with the core linked statically into the `.node` file (Q3, Q73).
  - The generator that writes the addon's C wrappers from the C FFI header also writes their TypeScript types, as an `interface` in the generated `src/internal/native_types.ts`, and the hand-written native loader gives the loaded addon that type, so no call into the addon goes through `any`. Each C type maps to one TypeScript type in the generator's table: each handle pointer gets a branded handle type of its own, so a consumer handle can't be passed where a producer handle belongs, `const char*` becomes `string`, and a C callback becomes a function type. The types are a `.ts` file, not a `.d.ts`, because `tsc` doesn't copy a hand-written `.d.ts` into `dist/` *(not checked)*. A test loads the addon and checks that its exports and the declared functions are the same set (Q4).
  - Values cross the FFI by this table, and a shape that isn't in it stops generation until its row is decided (Q59):

  | Java | Across the FFI |
  |---|---|
  | `TopicPartition` | A pair of topic and partition. |
  | `Map` | A list of pairs. |
  | An enum | Its integer code, or its name where Java has no code. |
  | `Duration` | An `int64` of milliseconds. A key that's left out, or `null` where Java accepts it, is sent as `-1`. |
  | An absent epoch, partition or timestamp | `-1`, which is `null` in JavaScript. |
  | `configs` | A `kafka_Map_t` of strings. |
  | An Options object | `<X>Options_new`, then `set_<field>` for each setter that ran. |
  | A key or value on produce | A `kafka_Bytes_t` over the app's own bytes, which the binding holds until the send settles. |
  | A header | A copy of its key and value: into a native `RecordHeaders` at `send()`, and out of the record's headers when a consumed record's `Headers` is first read or changed (Q31). |
  | A core error | An owned handle, which is read and then freed. |
  | A `*Result` | The handle, with its futures per key. |
  | A `ConsumerHandle` call in a listener | The `_cb` form. |
  | `long` | An `int64_t`, converted from and to a `bigint` (Q23). |
  | A key or value on consume | A `Buffer` over the record's bytes, with no copy, holding the native record until it's garbage collected, or a copy where external buffers are refused (Q24). |

  - As in Python, the binding makes no checks of its own except a listed few, each with a Why and a Goes when, and they run in Python's order: the form of the arguments first, then the rest. Of Python's five, only the `group.id` check for `groupMetadata()` and `committed(null)` stays, as a temporary one. The other four are left to the FFI and the core: a negative timeout and Java's null checks (Q35), config value types (Q52) and a closed client (Q61). JavaScript adds run-time type checks that TypeScript can't enforce, unknown option keys, the checks on the serde config keys (Q53), a string key or value given without a serializer (Q24, Q27), the form of a header given as a `[key, value]` pair (Q31), the `new.target` check of an abstract class (Q16, Q32), the 2^53 check on a `number` given for a Java `long`, and the range check on a `number` given for a narrower integer type (Q23, Q58, Q60).
  - The binding keeps no closed flag. A call after `close()` goes to the FFI, and the error is the core's, with Java's message, as in `IllegalStateError('This consumer has already been closed.')`: a synchronous method throws it, and an async one rejects with it. `close()` doesn't free the native handle. The Node-API finalizer calls `_destroy`, and only for a client that's already closed, so a late call never touches freed memory, and the finalizer never closes a client (Q21). Python's deviations for the admin calls, `flush`, `partitionsFor` and `metrics` don't carry over, because those calls behave as the core does. `send()` on a closed producer runs the serializer before the core's error *(deviation: Java checks before it serializes. Why: the binding keeps no state, and the serializer runs in JavaScript before the call reaches the core)*. Some consumer methods panic with the closed message in the core instead of returning it, and whether the FFI turns that panic into `IllegalStateError` isn't checked yet (Q61).
  - The mocks wrap the FFI's mocks, as .NET does, and aren't translations of Java's. The binding passes each JavaScript key and value through as the FFI's `void *`, which the FFI's mock keeps without reading, so `history()` returns the app's own records, which .NET's wrapper doesn't offer. The binding holds a reference to each key and value the mock keeps until `clear()` or the mock is destroyed, and it still calls the serializers on every send, as Java's mock does. A method that the Java mock doesn't support fails with `UnsupportedOperationError` and Java's message, now from the core (Q62).
  - During the preview, the client warns through `process.emitWarning` with the type `ExperimentalWarning`, which `--no-warnings` silences. It warns once per process, not once per client as Python does, and the mocks stay silent *(deviation, as in Python. Goes when: GA, in 4.5.0)*. The text is Python's `PREVIEW_WARNING`, with "JavaScript" in place of "Python": "The JavaScript client is in preview and not yet recommended for production use. The public API is not stable before the GA release and may change. We welcome your feedback while the design is still open" (Q63).
- **Documentation**
  - Doc comments are TSDoc in the TypeScript source, which `tsc` carries into the `.d.ts` files, and an HTML reference is built from them with TypeDoc. They are translated from the Javadoc and stay short and focused on the contract: Java's contract comes first, a comment has at most 150% of the Javadoc's words, and there are no Java file or line references, as `.claude/rules/documentation.md` says for Rust. The tags are `@param`, which describes only the options object as a whole, `@returns`, `@throws` naming the error classes, `@defaultValue` for Java's starting values, `@deprecated` and one `@remarks` for the JavaScript notes, such as deviations. Each option key is documented on its options type. A comment states the values that Java's shorter overloads pass, and the module docs list the overloads left out. CI runs a docs lint that fails when a public function or method has no `@param` for one of its arguments or no `@returns` for its return value, through the `require-param` and `require-returns` rules of `eslint-plugin-jsdoc`, which the spike confirms with TypeScript. It's set so that an options object needs one `@param`, not one per key *(js-only: Python has no docs lint in the preview)* (Q64, Q74).
  - As in Python, a departure is a *(deviation)* with a Why and a Goes when, agreed in review. JavaScript keeps its own list of contract changes, apart from the shared `design/current/doc-contract-changes.md`, and its name and place aren't set yet. When the docs are updated, the doc step checks each method against Java's contract, adds each deviation it finds to that list, and puts a warning in the method's docs. Only a maintainer accepts an entry, and the warning comes off once the entry is accepted. `javascript/CHANGELOG.md` lists each change in the release where it lands (Q65).
  - The user docs carry every warning on the team's agreed list, which the list below follows. A migration guide to 4.x, on the same template as the Python client's, tells users of 1.x and KafkaJS what replaces what, what isn't offered and what changes silently, such as `await send()` and the default partitioner. It covers each change they meet: listeners must finish and must await what they start; the anti-patterns inside callbacks; stopping the poll loop before `close()`; `partition: null`; `SubscriptionPattern`; reads that need `()`, except on data objects; milliseconds, with `0` meaning "don't wait"; `close()` having no time limit on the producer and the admin client; only `get()` wrapping the failure; errors that follow Java and have no `code`; `await send()` meaning accepted; the transaction and commit names, with both commits returning Promises; no logging options and no config callbacks; the preview and the `latest` tag; Schema Registry; string keys needing `new StringSerializer()`; Java's config keys; no `connect()`, `disconnect()`, `run()` or events; one-parameter calls; serdes in the constructor; listeners that take one object; `TopicPartitionMap` and `TopicPartitionSet`; `undefined` not being a value; data objects being class instances; deep imports that stop working; admin results and options being classes; lone surrogates becoming `?`; a Buffer that must be left alone until its `send()` settles, because 1.x copies a plain Buffer inside `send()` with `RK_MSG_COPY` (Q21); 64-bit values being `bigint`s, where 1.x's callback API gives numbers and its KafkaJS-style API gives strings (Q23); the supported Node versions; and 1.x's builder keys (Q64, Q65).
- **Tests**
  - As in Python (Tests and typing), Java's tests of every translated class that is plain TypeScript are translated to Jest tests, `KafkaFutureTest` included, and they assert Java's messages word for word. A test is skipped only when it's out of scope, isn't generated, or needs a Java test double that the binding can't inject, such as `MockClient` or `MockTime`, and the reason goes in the file. A class that Java ships without tests takes the Rust core's tests of it, and each translated test awaits wherever Java blocks. The tests run on Jest, with ts-jest or a similar tool for the TypeScript, because 1.x already runs its promisified and Schema Registry tests on Jest (Q66).
  - The binding's own tests cover the call shapes, where a shape that fits no form throws, an illegal combination of keys throws `IllegalArgumentError` with the exact message, and every overload signature compiles and works; the error hierarchy, both ways and against the FFI ids, with the fields and with two copies of the package; the serde vectors; the FFI twins, a static test that the addon calls only the header's fallible functions, `_cb` functions and `_err` twins, as Python's `test_ffi_twins.py` does, with an ABI check of the addon's declarations against the header, as .NET has; `tsc --strict` over the tests, with type assertions through `expect-type` or `tsd`; no unhandled rejection; an event loop that keeps turning; deep imports that fail; loading from ESM and from CommonJS; and a closed client. There are no pickle tests (Q40, Q67).
  - `make verify` gains the JavaScript build, lint, type check and unit tests. CI runs `test-integration-javascript`, with its `-ssl` and `-sasl-ssl` variants. For the gRPC conformance tests, `javascript/grpc-server/` serves the four services over the public API, with an image on port 50054, and the Rust harness runs it as `BackendKind::Js`. It's one backend, because JavaScript has one interface, and its `JS_GRPC_SKIPS` list gives each skip a reason and a removal condition. The chaos, performance and soak harnesses run where JavaScript has a hot path, and so do the verifiable clients. The JavaScript jobs join `REQUIRED_CI_JOBS` in the same pull request (Q68).
- **Stability**
  - A new Java overload adds one TypeScript signature, after the existing ones, and a new parameter is an optional key. Overloads left out are added later. A return that becomes nullable only warns, and a return type never gets narrower. Nothing is renamed or removed in a minor release. An option key never changes meaning, and a new enum constant, method or field is additive, because enums are classes. Public classes keep their state in private fields, so TypeScript checks them by name rather than by shape. A method that Java adds with a default body is an optional member, and a user who leaves it out gets Java's default, while one without a default body breaks implementors, as in Java. Breaking changes wait for Kafka's next major version (Q69).
  - The built `.d.ts` files, through an API report such as api-extractor's, and the `exports` map of `package.json` are compared with the previous release's by a check that runs in CI. During development a rule change may change them in a pull request, in preview releases a change only warns, and from GA on the check is required: a change fails it in a minor release and warns in a major one. A change to types alone counts, because it can break a user's `tsc` build, and the names outside the types have their own test (Q70).
  - A method, overload or key that Java deprecates after this client's first major release keeps working. It gets `@deprecated` with Java's note, and a `DeprecationWarning` through `process.emitWarning`, once per process and with its own code, emitted inside the user's call. For an interface that the user implements, the warning comes when the user passes the object in. A renamed key keeps the old one as an alias until the next major release, the types keep a deprecated overload signature for each form that used the old key, and giving both keys throws `IllegalArgumentError` naming the forms. The old names come from the last release's API report, never from a list kept by hand. A method that Java removes stays, deprecated, until the next major release. Editors and typescript-eslint's `no-deprecated` report deprecated use (Q71).
  - Frozen once released are Java's names, which change only through deprecations and aliases; the names of enum values and what their `toString()` and `toJSON()` return; the paths in `exports` and the folders behind them; what an option key means; milliseconds as the unit; what `await send()` means; each error's `name` matching its class; the `toJSON()` output of value classes; and the invented names (Layout and modules). Defaults aren't frozen by the binding: they're Java's, set by the core, so they change when Java's do (Q72).
- **Build, packaging and release**
  - CI runs `cargo xtask generate-error-codes` with a TypeScript emitter, and `check-generated` fails on any stale generated file: the error classes, the config constants and types, and the form tables. `make verify` runs `tsc --strict` over the source and the tests. CI also runs the type tests and the FFI-twins test, the interface check, the Promise-rule check, ESLint with typescript-eslint and `no-floating-promises`, Prettier as the formatter, publint and the package checks, which cover the published file list, no install scripts, and the `exports` and types map, and the `Buffer.from(string)` ban (Q24, Q64, Q74).
  - The client supports the Node versions that 1.x supports, limited to LTS versions that haven't reached end of life, and the release notes say when a version is dropped. `engines.node` is `>=22.14.0`, which follows that rule (Q73).
  - The addon includes only `rust/target/include/confluent_kafka_bindings.h`. As for C and Python, `javascript/` finds the core in `../rust`, `CONFLUENT_KAFKA_LIB_DIR` overrides that, and the target is `make build-javascript`. The `.node` file exports only Node-API's registration symbols, so the core's own symbols stay private. The addon targets Node-API 9, and CI runs on Node 22, 24 and 26. After the preview, the platforms are the release RFC's list: Linux with glibc on x86-64 and arm64, macOS on arm64, and Windows on x86-64. Each is built natively, because the TLS library's C build makes cross-compiling fragile, and the Linux floor is glibc 2.28, as for Python's wheels. Also as for Python's wheels, there are no musl packages (`.semaphore/build-wheels.sh:41`), and release builds compile the core with no `target-cpu` flag (`.semaphore/build-ffi-library.sh:79`), so x86-64 needs only Rust's baseline CPU, not the AVX2 that the Makefile's local `x86-64-v3` build needs. The build tool is node-gyp, and it runs only in CI. A missing platform package fails at load with an error that names the package and the usual causes: optional dependencies left out at install, a lockfile made on another OS, or a platform with no package (Q73).
  - The version follows Kafka's line, from `4.4.0-beta.1` at the preview, as in Python, to `4.5.0` at GA, so `bump-version` writes `-beta.N` for JavaScript. A preview is published under npm's `beta` tag, never `latest`, so a plain `npm install` keeps getting the current release. The patch digits follow the versioning RFC: `AK_patch × 100 + core_patch × 10 + binding_patch`. `cargo xtask bump-version` learns `javascript/package.json` and the platform packages. The platform packages are pinned in `optionalDependencies`, with `os`, `cpu` and `libc` set, no install scripts and no download fallback; the preview has linux-x64 (glibc) and darwin-arm64 (Q75). The version and the npm tag say `beta`, while the docs and the warning call the stage the preview, as in Python (Q63, Q75).
  - `javascript/` is mirrored read-only to `confluent-kafka-javascript` and released from `<major>.<minor>.x` branches, and the librdkafka-based code moves to that repo's `2.x` branch. Semaphore publishes with a granular npm token and no provenance. Pre-releases go out under the `beta` tag, and a check refuses to put one on `latest`; 1.x fixes go out under `legacy`. The README, the docs and the issue form say that the `2.x` branch holds the npm 1.x releases. Users keep `@confluentinc/schemaregistry`, which by the preview ships its own copy of the client types and takes the one-object serde call (Q75).
  - The licence is Apache-2.0. The client counts as derived from Apache Kafka, which is why it ships Kafka's NOTICE line, and that doesn't wait on legal (decided 2026-11-13). Every client ships a NOTICE file with Kafka's credit line, and each JavaScript package's `files` list names `dist/` and `NOTICE`, because npm adds the README and the LICENSE on its own but not a NOTICE file. CI builds one third-party licence file with a tool such as `cargo about`, which every JavaScript platform package ships (Q76).
  - Every file carries Python's header, Confluent's copyright line with the Apache 2.0 text (`python/confluent_kafka/__init__.py:1-13`), as the core's files do. That covers the TypeScript source, the addon, the build scripts and the generated files. A file copied unchanged from Apache Kafka keeps the Foundation's header, and code translated from OpenJDK, if any, keeps GPL with the Classpath Exception. Each package, the main one and every platform package, declares `"license": "Apache-2.0"` and ships its own `LICENSE`, which the build copies into the package's folder, because npm packs a `LICENSE` only from there. `removeComments` stays off, so `tsc` keeps the header in each `.js` and `.d.ts` file *(not checked)*, and a CI check confirms that every `.js` and `.d.ts` file in each tarball starts with it. 1.x stays MIT on the `2.x` branch, and the 4.x release notes say that the licence changed (Q76).
- **Future improvements** (each entry is decided, and left out of the preview on purpose, by decisions of 2026-11-13: an agent builds none of these until a rule takes the entry's place)
  - A cleanup hook for clients in worker threads (Q21). A worker can end while its client is still open only when `worker.terminate()`, `process.exit()` or an uncaught error stops it before its `close()` has finished, because an open client otherwise keeps the worker running. Node then frees the worker's memory while the core keeps running the client, so the core may read a Buffer that a pending send still borrows, which may crash the whole process. The thread-safe function isn't a risk: Node keeps it alive until every thread has let go of it (`MaybeDelete` in Node's `src/node_api.cc`), so the core thread's next call gets `napi_closing`, and a test on 2026-11-11 found no crash, even with `MallocScribble=1`. Only Node 24 was tested. The addon would register the hook with `napi_add_env_cleanup_hook` when it creates a client and remove it with `napi_remove_env_cleanup_hook` once the client's `close()` succeeds, so Node runs it only for a client that's still open. The lean is that the hook clears the client's notify hook, closes the client with a zero timeout and then calls `_destroy`. The zero timeout fails the pending sends at once with Java's "Producer is closed forcefully.", so no send still borrows a Buffer, while a consumer skips its final auto-commit and may not leave its group. The other way only clears the notify hook and leaves the client to leak, with the warning. Either way the FFI changes first, for the producer, the consumer and the admin client: `__set_callbacks_notify` must accept `NULL` to clear the hook, which it rejects today, and clearing must wait until a call of the hook in flight has returned, which it doesn't do today. A test ends a worker with `worker.terminate()` while sends are pending and checks that the process keeps running.
  - `[util.inspect.custom]` on data objects, so `console.log(tp)` prints Java's `toString()` format, such as `orders-3` (Q29). Until then `console.log(tp)` prints the fields, as `TopicPartition { topic: 'orders', partition: 3 }`, and `` `${tp}` `` gives Java's format.
  - A debug check that a Buffer borrowed by a pending send didn't change before the send settled (Q21). It would hash each Buffer at `send()` and again when the send settles, so it costs time on every send, and no other client has one. The lean is to turn it on with `NODE_DEBUG=confluent_kafka`, which already shows the binding's debug log.
  - A closed client that's garbage collected while one of its calls still has an answer queued (Q49). The core's `_destroy` handles a pending report cleanly: it wakes a call still waiting, which fails with the wakeup error, waits for every `_cb` call, and runs each queued report exactly once before it frees the client (`kafka_consumer_KafkaConsumer_destroy`). But it runs those reports on the thread that calls it. So when Q61's finalizer calls `_destroy` for a closed client that still has an answer queued, such as the core's error for a call made after `close()`, the binding's JavaScript runs during garbage collection, and `_destroy` may hold the thread for up to 5 seconds while the client's runtime stops. Only an app that drops a closed client without awaiting such a call meets this. The lean is that the binding keeps a client reachable while one of its calls is pending, so the finalizer only ever finds an empty queue. Keeping Node running counts nothing (Q21), so this needs a count of its own, or a reference that each pending call holds. The other way is to run `_destroy` after the garbage collection, through `node_api_post_finalizer`, if the Node-API version that the addon targets has it.
  - Refusing a call made inside a callback after the callback's first `await`, when the call waits for that same callback, such as `await producer.flush()` inside a send callback (Q46). The core refuses such a call only during the callback's synchronous part, so after an `await` the call hangs, and the docs list these calls as anti-patterns. Node's `AsyncLocalStorage` could carry an "inside this callback" mark across `await`s, so that the binding throws instead. Python tracks the same thing with a `ContextVar`, for the producer's async send callbacks (`python/confluent_kafka/producer/async_producer.py:137`) and the consumer's callbacks (`python/confluent_kafka/consumer/_base.py:211`). The mark adds work to every callback, and send callbacks run once per record, so a spike measures the cost first. It's also a check that the binding keeps itself, not the FFI or the core.
- **Not decided yet** (an agent that reaches one of these stops and asks)

  | Item | With |
  |---|---|
  | An existing method that starts to wait (Q42, case 3). | With its owner. |
  | When and how the warning for a client that's never closed is logged (Q21). | Parked for design discussions 2. |
  | A review comment that the serde methods must be all async (Q50). | Waiting on the reviewer's answer. |
  | The Schema Registry serdes' functions for the serde config keys, which `create()` can wait for (Lifetime), and their reading of Java's key names such as `schema.registry.url`, to agree with the Schema Registry team before GA (Q53, Q75). | With the Schema Registry team. |
  | The other `tsc` settings, such as the JavaScript target, and the oldest TypeScript version (Q74). | Parked for design discussions 2. |
  | Schema Registry's release for 4.x (Q75). | The Schema Registry team's decision. Check later that it has shipped. |
  | Where Schema Registry's source lives once `master` becomes the 4.x mirror (Q75). | Parked for design discussions 2, with the Schema Registry team. |

---

## The binding's notes

The Node.js client in `javascript/` is built only on the C FFI of root
`CLAUDE.md` §4. It is a Node-API addon written in plain C against
`node_api.h` (`native/`), under a thin layer written in TypeScript (`src/`),
which tsc compiles, with its declarations, into `dist/`, the package's entry
points.

The rules are the **JavaScript Binding Conventions** in the first part of
this file ("the JS conventions" below). They decide every question they
answer, and these notes do not repeat them: where they state a rule, the
notes point to the group that states it ("JS conventions, Signatures"). The
notes hold what the conventions leave to the binding: the addon's internals,
liveness, the file layout, the generated files and the rule linter. An item
the JS conventions list under "Not decided yet" is marked **(not decided yet
in the JS conventions)** here: the text records the current choice, and
nobody treats it as settled. A **(conventions conflict)** mark names a place
where the JS conventions disagree with themselves or with a reviewed fact;
the code follows the conventions' text literally, as the owner chose, except
where that text, read literally, breaks the client or misdescribes what it
does: there the code and docs follow Java and the core
(`javascript/design/current/producer/DECISIONS.md`, entry 4). Either way the
mark records the conflict for the rules' owner. Root `CLAUDE.md` still applies
wherever both are silent. The approved design is
`javascript/design/current/producer/PLAN.md`.

### Ground truth

These decide every question, in this order:

1. The JS conventions (the JavaScript Binding Conventions, the first part of
   this file).
2. The C ABI in `rust/target/include/confluent_kafka_bindings.h`, the only
   header the addon includes (JS conventions, Build, packaging and
   release).
3. The Java public API (`KafkaProducer`, `ProducerRecord`, `RecordMetadata`,
   `Callback`, `Admin` and its classes, `MockAdminClient` from Java's test
   fixtures, Apache Kafka 4.4.0 in `kafka/`).
4. Root `CLAUDE.md` §4 "C FFI Conventions".

### Boundary rules

1. **Java shape, host idiom, no Kafka logic.** Batching, partitioning, retries
   and config validation live only in the Rust core. The binding's own checks
   are the listed few of the JS conventions (Implementation over the FFI),
   in their order: the form of the arguments, then the rest. There is no
   closed-client check (JS conventions, Q61): the binding keeps no closed
   flag, and a call after `close()` goes to the FFI, whose error is the
   core's. Each check has its Why and its Goes when:
   - **The form of the arguments** (`JavaForms`, unknown option keys). Why:
     one JavaScript method stands for Java's overload set, and only the
     binding sees which form was written. Goes when: never; it is the
     language mapping, not a missing FFI feature.
   - **Run-time types and a missing topic.** Why: TypeScript's types are not
     checked at run time, and a wrong JavaScript type has no C form to cross
     as. Goes when: never, for the same reason.
   - **A negative partition or timestamp.** Why: the C record constructors
     read a negative value as Java's `null`, so the core cannot see it. Goes
     when: the FFI carries "absent" separately from the value.
   - **The configuration's shape**: a `Map` or another non-plain object is a
     `TypeError`, and a symbol key a `ConfigError`. Why: a `Map`'s entries
     and a symbol have no string to cross as. Goes when: never. No config
     value is checked (JS conventions, Configuration), except the
     producer's serde keys, which never reach the core: `key.serializer` and
     `value.serializer` take a synchronous `({ configs, isKey }) => serde`
     (JS conventions, Serialization, Q53), and a class name, a serde
     object, a class or a function that returns a Promise or no serializer
     is a `ConfigError` saying what to pass (`src/internal/serde.ts`
     `configuredSerializer`). Why: the binding runs the serdes, so only it
     reads these keys. Goes when: never.
   - **A NUL byte in a string or bytes that cross as a C string** (the topic,
     the header keys, the config keys and values: `js_string_dup` and
     `js_bytes_cstring` in `native/record.c`, which `native/headers.c` uses for
     the header keys at `send()`), a `TypeError` "... without null
     bytes". Why: every C reader stops at the first NUL, so the value would be
     truncated silently at the ABI. Goes when: these values cross the ABI with
     a length, as `kafka_Bytes_t` does.

   No precondition of Java's is repeated (JS conventions, Q35): a negative
   timeout goes to the FFI as it is, and the form called, not the value,
   picks `close_cb` or `close_with_timeout_cb`; the producer FFI's close
   rejects a negative one with Java's `IllegalArgumentError("The timeout
   cannot be negative.")` and closes nothing. What stays is the conversion a
   number or a `Temporal.Duration` needs: `NaN`, a count beyond 2^53 - 1 and
   a calendar unit are `RangeError`s, and `null` is a `TypeError`
   (`src/internal/duration.ts`). A `ProducerRecord` is frozen, so `send()`
   checks no field again ("Java shape, JS idiom", the data objects).
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
   no-op. The binding never calls `_destroy` from inside a drain, nor from a
   completion or a call: only the handle object's finalizer destroys a
   producer, and only a closed one (rule 10).

   **Cooperative scheduling.** The rule is the JS conventions' (Threads and
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
4. **Waiting calls use the `_cb` twin** (JS conventions, Futures and
   waiting): `send_with_callback_cb`, `flush_cb`, `close_cb`,
   `close_with_timeout_cb`. The blocking forms would block the event loop and
   run callbacks on the caller's thread. There is no exception: no finalizer
   closes a client, so no teardown needs a blocking close
   (`test/static/header_conformance.test.js` and `check-cb-twin-only` pin
   it).
5. **Never the blocking `KafkaFuture_get`; `get_cb` and
   `get_with_timeout_cb` are the waits.** The `kafka_common_KafkaFuture_t*` a
   send's hand-over completion receives is the record's future. The send's
   context (`SendCtx.future`) owns it, and JS gets the number of the
   context's slot in the producer's table (`Producer.slots`, a small integer
   kept in the record's `KafkaFuture` with the producer's handle), so no
   object, and no finalizer, is made per record. The record's `KafkaFuture`
   completes with the delivery its callback saw, as Java's
   `completeFutureAndFireCallbacks` completes the future once the callback
   returned (`KafkaFuture.internalSetDelivery`, rule 6), and from then on
   `get()`, `getNow()` and `isDone()` answer from it, as Python reads a done
   future's value at once. A wait started before that goes through
   `kafka_common_KafkaFuture_get_cb` (`get()`, and a first dependant:
   `thenApply`, `whenComplete`, `allOf`, `toCompletionStage`) or
   `kafka_common_KafkaFuture_get_with_timeout_cb` (`get({ timeout })`, the
   FFI keeping the time, a negative timeout passed on as it is), each created
   on first use, so nothing waits, and no Promise exists, for a future nobody
   reads. A wait's completion runs in the producer's drain (inline inside the
   call once the producer is destroyed); one the destroy runs, from the
   handle object's finalizer, reaches no JS. Each wait holds one reference on
   the context and one unit of the producer struct (`owners`). The context
   destroys the native future exactly once, on the JS thread, running no JS
   (`send_ctx_maybe_destroy_future`): once the callback reported, the last
   wait completed and JS recorded the delivery (or was never handed the
   slot), giving the slot back then. A context whose delivery reached no JS
   while JS holds its slot (an environment teardown) is kept, so the slot
   never names freed memory. A callback Promise still pending when the
   finalizer destroys a closed producer is not such a case, although a
   completed close does not rule it out (a `close()` made from the callback
   before its first `await` closes at once, the Promise still pending): the
   destroy reports it as returned (rule 6), and JS holds its future no more,
   since a `KafkaFuture` holding a slot holds the handle object being
   finalized, so its future, slot and context are freed as a recorded
   one's are (`send_report_awaiting`). Only a future with no native
   handle (`allOf()`, `completedFuture()`, the dependants) runs a JS timer,
   for a timed `get`.

   The cost was measured as the JS conventions' Futures and waiting asks (Python's
   gate, an A/B run of the producer performance test): send-only,
   100-byte values, `linger.ms` 5, 3M records, 11 interleaved runs each
   against the code before the native future: 394.6k
   records/s (median 394.6k) before, 385.0k (median 387.2k) after, -2.4% (-1.9%
   by median), GC time 79 ms against 81 ms. A first form, one wrapped handle
   object with a finalizer per record, cost 7-10% and 2.7 times the GC time;
   a `napi_external` without one, 2.3% and 2.5 times.
6. **One `Callback_t` per record.** In `on_completion` the order is: run the
   user callback; record the delivery on the record's `KafkaFuture`
   (`internalSetDelivery`, which completes it); call
   `__set_callback_result_err(view, id, NULL, err)` exactly once, `err` NULL or
   an owned `kafka_common_Error_local_callback` error carrying what the
   callback threw. No report is sent once destroy has begun (`_destroy`
   completes the unreported ones itself). A callback may return a Promise
   (JS conventions, Threads and callbacks): the binding wraps any result
   but `undefined` (Java's `void`, reported at once, the cheap common case)
   with `Promise.resolve(result)`, and the delivery is recorded, completing
   the future, and the one report is made, only once that settled (`send_defer` hands the Promise a function
   owning the record's context, as both its handlers, `callback_id` kept in C),
   a rejection reported as a throw. The function is held by a strong reference
   until it reports, so a garbage collection never reports a Promise early,
   even one nothing references: a Promise that never settles keeps its record
   pending, and a `flush` or `close` covering it. Before the finalizer
   destroys a closed producer, a callback Promise still pending is reported as
   returned (`send_report_awaiting`): no JS will settle it, and no report may
   follow the destroy.

   **Callbacks run one at a time, in completion order** (the JS conventions
   leave this open; the owner decided it on 2026-11-18, for Java parity).
   Java runs its callbacks one after another on the producer's I/O thread, so
   a slow callback holds back the next; Python's `AsyncProducer` and the .NET
   binding do the same. While a callback runs or its Promise is awaited
   (`Producer.current`), a callback the core delivers waits at the end of the
   run queue (`send_enqueue`) with the `RecordMetadata` and error it will
   receive, made when the core delivered it (the core's are borrowed), and it
   runs, and reports, once every callback before it has ended
   (`send_run_queued`, from `js_send_on_settled` once the awaited Promise
   settled; its loop runs the waiting ones in turn, and stops at the next one
   that returns a Promise). One the run queue can't take (a Node-API call
   failed) runs at once, out of turn, rather than never, and the callback
   whose turn it is keeps it. So each record's future still completes only after
   its own callback, and a `flush` or `close` covering a waiting callback
   still waits for it; a later record's future completes only once the
   earlier callback's Promise settled, so a callback's timed
   `get({ timeout })` on it rejects with `TimeoutError`, as Java's
   `get(timeout)` on the I/O thread throws `TimeoutException`. A Promise that
   never settles holds the callbacks behind it too, and their records'
   futures, which hold the handle object, keep the producer from being
   collected, as an I/O thread stuck in a callback keeps Java's producer. The
   destroy reports the callbacks still waiting as returned, after the awaited
   one, running no JS (`send_report_queued`). The rest stays as it was: the
   hand-overs and the cooperative pump and yield (rule 3).

   A waiting callback runs after the drain, from `js_send_on_settled` (a
   Promise reaction), and the core counts as inside a callback only its own
   `__execute_callbacks` (`Client::in_callback`). So, as after a callback's
   first `await`, its `flush()` is not refused and its `close()` is not
   Java's `close(0)`: an awaited one waits for that same callback, one of the
   anti-patterns inside a send callback (JS conventions, Threads and
   callbacks). Only a callback the drain runs keeps, until its first
   `await`, the refusal of `flush()` and the `close(0)`. Refusing them in a
   waiting callback too is the JS conventions' Future improvement
   (refusing a call made inside a callback after its first `await`), which
   the owner kept as one: no core change and no binding-side mark. Any
   result but `undefined` takes the Promise path (`send_defer`), an
   expression-bodied arrow such as `() => seen.push(x)` included, so the
   callbacks the same drain delivers after such a callback are waiting ones.
7. **No binding-side send queue, buffer, cap or lock** (PLAN D8; JS
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
   binding never holds records back itself. The run queue of rule 6 is no
   send queue: it holds the callbacks of records the core has already
   delivered, waiting for their turn, as Python's does, and no record.
8. **No copy of the app's key and value; strings encoded as Java encodes
   them.** The rule is the JS conventions' (Serialization, and the FFI table
   of Implementation over the FFI): the binding doesn't copy a key or value
   the app hands over as a `Buffer` or `Uint8Array`, one a serializer returns
   included, nor the Buffer it makes itself for a string. The addon reads
   each as a `kafka_Bytes_t` over the Buffer's own memory (`native/record.c`
   `js_bytes`) and holds a `napi_ref` on the Buffer (`SendCtx.key_ref` /
   `value_ref`, taken in `js_producer_send`), which it drops in the send's
   hand-over completion (`send_on_handover`, through
   `send_ctx_release_buffers`), whether the record was taken or refused, on
   the JS thread: the FFI borrows the bytes only until that completion
   (`rust/src/ffi/producer/mod.rs`, module docs), and the core copies them
   when it appends the record. A record that can't be built drops them at
   once. Header values are copied at `send()`, as the JS conventions'
   FFI table says (the C API keeps no borrowed header value): each key and
   value go into a native `RecordHeaders` made for the call
   (`native/headers.c` `headers_build`), which the record's options builder
   copies and `native/record.c` destroys right after. The user-facing
   contract, documented on `send`, in its compiled declaration and in the
   README, is the JS conventions' text: "The client reads a `Buffer` or
   `Uint8Array` key or value after `send()` returns. Don't write to it,
   transfer it or resize it until the Promise from `send()` settles;
   awaiting `send()` is enough. A serializer must return a new Buffer for
   each record.", plus, for a Buffer over a `SharedArrayBuffer`, "If other
   workers share the Buffer's memory, they mustn't write to it until then
   either." `src/` copies no bytes itself.

   Strings follow the JS conventions (Types, the paragraph after the
   table): one function, `encodeLikeJava(s)` in `src/internal/encode.ts`, is
   the only place the binding turns a string into bytes, writing each lone
   surrogate as `?` as Java's `String.getBytes(UTF_8)` does (Node and
   Node-API write U+FFFD). `stringSerializer()` for keys and values, the
   headers code for header keys, and the config code once per client, for the
   config keys and values (group IDs among them), call it, and the addon reads
   the header and config bytes into C strings (`native/record.c`
   `js_bytes_cstring`); topic names skip it and cross as JS strings. A string
   key or value given without a serializer throws `TypeError` pointing to
   `stringSerializer()` (JS conventions, Class family, Q24, Q27), checked at
   `send()` (`src/producer/producer_record.ts` `rawBytes`).
   `check-zero-copy` and `check-string-encoding` partly enforce this (The rule
   linter, below), and an integration test checks that an accepted record's
   Buffers are collectable once its `send()` resolved, before delivery. Not
   checked: which strings go through `encodeLikeJava`, and copies in other
   `native/` files.
9. **Ownership follows the signature.** Errors from `out_err` and from `_cb`
   completions are owned: copied into a JS error (`src/internal/errors.ts`),
   then destroyed, before the Promise settles (JS conventions, Errors). An
   error with nowhere else to go (a failed `__set_callback_result`, a failed
   destroy of a handle, a JS exception the addon would clear) is handed to
   `process.emitWarning` through the `warn` function `src/internal/native.ts`
   registers (`error_warn`, `js_warn_thrown`) wherever JS can run; at an
   environment teardown, where none can, it is only freed. A completion whose
   answer can't be built settles its call with the failure instead (the pending
   JS exception, or an `Error` naming the step: `js_take_failure`), and a
   delivery callback still runs once, with the failure as its error and, when
   its metadata can't be built, Java's metadata of a failed record, with the
   partition the core gave it. A wait on a record's future fails the future
   with it only while the future is pending (its delivery reached no JS), and
   warns it otherwise; a timed wait that timed out rejects its `get()` with it,
   or warns it once that `get()` settled. The
   error and metadata a delivery callback receives are borrowed: copied,
   never destroyed. If a send fails at hand-over, its per-record context stays
   on the producer's orphan list until after destroy (the delivery callback
   may still fire, once).
10. **Closed state and teardown** (JS conventions, Lifetime and
    Implementation over the FFI, Q21, Q61): `close()` goes to `close_cb` /
    `close_with_timeout_cb` every time, a second close included, and does not
    free the native handle. Its completion (`close_on_complete`) records the
    core's answer: any outcome but a rejected timeout closed the producer, so
    `cbq_closed` lets Node go once no call made on the producer waits for its
    answer ("Threads and liveness"), makes the queue's reference on the
    handle object weak, and drops the `on_complete` function of every send
    context parked on the orphan list (a hand-over refused after the close
    drops its own at once): no delivery callback can
    fire for those records any more, and the functions' closures hold the
    `KafkaProducer`, which would never be collected. The handle object's Node-API
    finalizer (`producer_finalize`) then calls `KafkaProducer_destroy_err`,
    on the JS thread, outside a drain, running no JS (`finalizing`), only for
    a producer that `cbq_is_closed` says is closed, and releases the
    threadsafe function, whose finalizer frees the notify opaque. No
    finalizer closes or destroys an open producer: only an environment
    teardown finalizes one (the reference is strong until the close), and it
    leaves the producer to the core and its memory to the process. So a late
    call never touches freed memory: the handle is destroyed only once no JS
    can reach it.

### Threads and liveness

The rules are the JS conventions' (Threads and callbacks, Lifetime). In this
binding:

- Every callback the core queues (delivery callbacks, hand-over completions,
  `flush` / `partitionsFor` / `close` completions, the waits on a record's
  future) runs on the JS thread that owns the client, inside the drain, so JS
  values are created there directly.
- A producer keeps Node running from its creation until `close()` closes it
  (JS conventions, Lifetime), and a call made on it keeps Node running
  until that call settles, closed or not, as Java's call returns. The
  threadsafe function is created referenced, as Node-API creates it, and is
  referenced while the producer is open or while one of its `_cb` calls waits
  for its completion; `cbq_hold_update` (`native/callback_queue.c`) is the
  one place that references or unreferences it, only when that answer
  changes. Node-API counts neither call (a uv handle's ref is a flag), so the
  callback queue counts the calls itself: `cbq_call_begin` as the statement
  right before each `_cb` call, in the same block, after every early return
  of the function making it, and `cbq_call_end` as the last statement of its
  completion (only the release of the producer struct, which may free the
  queue, comes after), on every path: the drain, inline inside the call, and
  the destroy, where the hold no longer changes since the threadsafe function
  is being released. Neither appears anywhere else: a delivery callback ends
  no count. Every `_cb` call is counted: a send's hand-over
  (`send_with_callback_cb`, ended in `send_on_handover`), `flush_cb`,
  `partitions_for_cb` (ended in `partitions_for_on_complete`), `close_cb` and
  `close_with_timeout_cb` (ended in `close_on_complete`, after the JS
  callback), and the waits on a record's future (`get_cb`,
  `get_with_timeout_cb`, ended in `future_on_wait`). A `MockProducer` makes
  the same calls through the same view and sites, so its calls are counted
  the same way; the mock's own calls (`history`, `clear`, `completeNext`,
  `errorNext`, the error setters) are answered inside the call and have no
  completion to count. A second close made while
  the first runs is answered after the first close's completion; the
  hand-overs and the waits have been answered before it in every scenario
  found, so their counts are guarded by `check-tsfn-lifetime` and by the test
  of a late `close()` and `flush()` after a record's callback ran, not by a
  test of a late answer. `cbq_closed`, run by the close completion once the
  core reported the producer closed, ends the open half, so a closed producer
  with no call waiting lets Node exit right after its last answer ran. The
  threadsafe function is not released then: the completions of calls made
  after the close still run, and each such call holds Node until it has. A
  script that never calls `close()` never exits on its own.
- **(conventions conflict)** The JS conventions' Lifetime rule says the binding
  "calls `napi_unref_threadsafe_function` once `close()` has closed the
  client, so nothing is counted". Read literally, that lets Node go at the
  first close completion, so a completion the core queues after it is lost
  when nothing else holds Node: an awaited `close()`, `close(timeout)`,
  `[Symbol.asyncDispose]()`, `flush()` or `partitionsFor()` on a closed
  `KafkaProducer` or `MockProducer` never settles, the code after its
  `await` never runs, Node exits with code 0, and of two `close()` calls at
  once one is lost. That reading breaks the client, and with it the rule's
  own reason ("this way no pending send or poll is cut off"), the JS
  conventions' "an async one rejects with it" (Implementation over the FFI:
  an async call after `close()` settles with the core's answer), and Java
  parity, where each of those calls returns. So the code follows the rule's
  intent, the hold lasting while a call waits: a closed client holds Node
  only while a call made on it is still waiting for its answer
  (`DECISIONS.md`, entries 4 and 33;
  `javascript/design/current/producer/PLAN-close-liveness.md`). The JS conventions'
  wording needs its owner; the wording proposed: "…calls
  `napi_unref_threadsafe_function` once `close()` has closed the client and
  every call made on it has had its answer."
- The handle object is held by a strong reference from its creation until
  the close (`cbq_attach` / `cbq_closed`), so garbage collection never
  finalizes an open producer, even once the `KafkaProducer` object is
  collected; after the close the reference is weak, and the finalizer
  destroys the closed producer (rule 10). A construction that fails once the
  handle object exists drops the reference (`cbq_detach`), so the finalizer
  frees the struct.
- A producer never closed gets no warning. The code once counted the open
  producers and warned at `process` `'beforeExit'`, but an open producer
  keeps the event loop from emptying, so `'beforeExit'` never comes while
  one is open and the warning could never print. A review comment on Q21 says the case "should not happen, because
  producer is alive", so the dead code was removed (`DECISIONS.md`, entry
  35), and the admin client gets none either. **(conventions conflict)** The JS
  conventions' Lifetime rule says "A client that's never closed runs until
  the process exits, with a warning", and their "Not decided yet" table has
  a row on when and how that warning is logged; both describe a case the
  comment says can't happen, so the binding logs nothing. The wording
  proposed to the rules' owner drops "with a warning" and the row.
- Clients in worker threads are not supported in the preview (JS
  conventions, Threads and callbacks); the binding keeps no check for one. A
  worker ended with its producer open (`worker.terminate()`,
  `process.exit()`, an uncaught error) leaves the producer to the core, as
  an environment teardown does (rule 10); the cleanup hook that would close
  it is a Future improvement of the JS conventions, not built.
- The preview warning follows the JS conventions (Implementation over the
  FFI): `process.emitWarning` with the type `ExperimentalWarning`, once per
  process, with no code (the JS conventions' invented names list only deprecation
  codes), silenced by `--no-warnings`; its text is the JS conventions',
  Python's `PREVIEW_WARNING` with "JavaScript" in place of "Python"
  (`src/internal/lifecycle.ts`). Debug lines use `util.debuglog('confluent_kafka')`
  (`src/internal/debug.ts`): the addon's load, the producer's creation,
  close and pump. The main thread and the
  workers share the one notice: the addon, loaded once per process and kept
  loaded (`RTLD_NODELETE`, so a worker ending does not unload it), answers
  `previewNoticeDue()` true only once.

### Java shape, JS idiom

The rules are the JS conventions' (Names, Class family, Futures and
waiting, Signatures, Types, Errors, Configuration). What this binding adds:

- Supported import forms: `require`, named imports, `import * as`, and an ES
  module's default import under Node's own loader. A default import through an
  interop helper (`esModuleInterop`, Babel) is not supported: every public
  `index.ts` declares `export declare const __esModule: true;` (tsc emits the
  marker itself, the declaration no JS), so TypeScript rejects it (TS1192)
  rather than letting it read a missing `default` (`test/typing` pins this).
  The package is CommonJS only, built with `module: nodenext` (JS
  conventions, Layout and modules, Q13): `package.json` has no `"type": "module"`, and
  each `exports` entry has the `types` and `default` conditions.
- Data objects (JS conventions, Class family): their Java getters are public
  `readonly` fields, set once in the constructor (not getter methods, not
  `get` accessors), so a spread and `JSON.stringify` keep them
  (`structuredClone` / `postMessage` keep every field but
  `ProducerRecord.headers`, a class instance, which becomes `{}`); a field has
  its Java getter's name, `is` / `has` included (`metadata.offset`,
  `metadata.hasOffset`), and Java's `-1` and `null` stay (never `undefined`).
  Every data class is frozen (`Object.freeze(this)` in its constructor,
  which `check-data-objects` requires), so `ProducerRecord.internalToNative` reads
  the fields its constructor checked and checks nothing again at `send()`
  (JS conventions, Q35: no repeated precondition); its `Headers` stays
  writable until `send()` makes it read-only. The data classes are listed in `DATA_CLASSES`
  (`scripts/lint/java_ts.js`), which `check-data-objects` reads:
  `ProducerRecord`, `RecordMetadata`, `TopicPartition`, `Node`,
  `OffsetAndMetadata` and the admin client's seven `*Description` classes
  ("The admin client"). `RecordHeader` is not on the list and keeps
  Java's `key()` and `value()`, the methods of Java's `Header` interface (JS
  conventions, Serialization). Java's `public final` instance fields are
  `readonly` fields of a frozen object (`SecurityProtocol.id`); `public static
  final` constants are static properties (`RecordMetadata.UNKNOWN_PARTITION`).
  Value classes have `toJSON()` *(js-only)*, whose output is frozen once
  released (`RecordHeader`'s writes `{ key, value }`, `Headers`' its array, an
  enum's its name, `ProducerRecord`'s and `RecordMetadata`'s their fields
  with each `long` as a decimal string, JS conventions, Types).
  `toString()` is Java's, an enum's included (below). A constant Java
  `List` is a frozen array, a constant `Set` a read-only set
  (`src/internal/read_only_set.ts`) whose `add`, `delete` and `clear` throw
  `UnsupportedOperationError`.
- Headers follow the JS conventions (Serialization, Q31): `Header` is Java's
  interface, a TypeScript `interface` with `key()` and `value()`;
  `RecordHeader` is the class an app builds a header with, Java's
  `(String key, byte[] value)` constructor taking `{ key, value }`; `Headers`
  joins Java's `Headers` interface and its class `RecordHeaders`. All three
  are in `common/header`, `RecordHeader` too *(deviation, as in Python:
  Java's `RecordHeader` is in `common.header.internals`, and "one import path
  per Java package" would make the class apps build headers with public
  under an "internals" path. Why: it belongs beside `Header` and `Headers`,
  as Python's header class sits in `confluent_kafka.common.header`)*, its
  `lint-mapping.json` `javaTargets` entry giving that reason. They are plain
  TypeScript, translated from the Java source, holding their own data, and
  none calls the addon. A `RecordHeader` keeps the key as given, a lone
  surrogate or a NUL included, as Java keeps the `String`, and the value it
  is given without copying, and `value()` returns that value as a `Buffer`
  over the same memory, the same `Buffer` on every call, or `null`. Its
  `equals`, `hashCode` and `toString` are Java's, read through its `key()` and
  `value()`. `Headers` holds its own list of the very `Header` objects added
  (`add(header)` stores that object, a `RecordHeader` given bare and any
  `Header` by name, `add({ header })`, as "TypeScript first" below says;
  `add({ key, value })` a new `RecordHeader`), so a `Header` read from it
  stays valid however it changes later, as in Java; it keeps Java's methods
  in Java's order, `RecordHeaders`' three constructors as one `headers` parameter
  (`()`, `(Header[])`, `(Iterable<Header>)`: a `Headers` copied as a new list
  of the same objects, its read-only state not), Java's `equals`, `hashCode`
  and `toString`, a `toJSON()` writing its array, and the read-only state,
  `setReadOnly()` and `isReadOnly()`, `@internal` under Java's names (JS
  conventions, Serialization; `check-internal-members` lists the two). The
  checks run in Java's order: `add` refuses a null header ("Header cannot be
  null.") before the read-only state, `remove` checks the read-only state
  before the null key (`IllegalStateError` "RecordHeaders has been closed.",
  then `IllegalArgumentError` "key cannot be null."), and the run-time types,
  which TypeScript cannot enforce, come first. A `Header` is any object, not
  an array, with `key()` and `value()` methods, as Java takes any
  implementation of its interface: it is kept as it is and read through its
  methods, and its equality is its own, identity for a plain object. Where
  Java takes `Headers` or `Iterable<Header>` (`ProducerRecord`, `new
  Headers()`, `RecordDeserializationError`), each element is a `Header` or a
  `[key, value]` pair, mixed in any order, a pair becoming a new
  `RecordHeader` (`src/common/header/headers.ts` `headerOf`), with Python's
  messages in JavaScript's words, naming the element's position
  (`headers[1] key must be a string; got number`), and Java's own
  `TypeError`s for a null element and a null key. A `Headers` given where Java
  takes `Headers` is kept as it is (`RecordDeserializationError`,
  `src/internal/payload.ts`). An iterator over a `Headers`, or over
  `headers(key)`, reads the live list, as a JavaScript array's iterator does:
  a change during an iteration is seen and throws nothing *(deviation: Java's
  iterators throw `ConcurrentModificationException` on a best-effort basis.
  Why: the headers are a JavaScript array, as Python's are a Python list.
  Goes when: never)*. A `ProducerRecord` makes its own `Headers` from the
  headers it is given, Java's `new RecordHeaders(headers)`, so a record with
  no headers has an empty one. `send()` makes the record's headers read-only
  once the serializers have run and the record's fields are converted
  (`ProducerRecord.internalToNative`, Java's `setReadOnly`), so a later `add`
  or `remove` throws "RecordHeaders has been closed.", then reads each
  header's key, as `encodeLikeJava`'s bytes, and value through its `key()` and
  `value()` (`Headers.internalToNative`), which the addon copies into a native
  `RecordHeaders` made for the call, with `RecordHeaders_new`,
  `__as_Headers` and `Headers_add_with_key_value` (`native/headers.c`
  `headers_build`), passed to `ProducerRecordOptionsBuilder_set_headers`,
  which copies it, and destroyed right after the call (`native/record.c`).
  So a NUL in a header key is refused at `send()`, the boundary rule 1 check,
  and a lone surrogate crosses as `?`; a record with no headers makes no
  native `RecordHeaders`. `MockProducer` leaves the record's headers
  writable, as Java's mock does, and keeps the app's own record, its
  `Headers` included, for `history()`. `headerStats()`, registered for the
  tests only, counts the native `RecordHeaders` the sends made and destroyed.
  A consumed record's `Headers` comes with the consumer.
- Enums (JS conventions, Class family): a class whose constants are frozen
  instances, with `java.lang.Enum`'s `name()` and `ordinal()`, static
  `values()` and `valueOf(name)` (exact; `IllegalArgumentError` "No enum
  constant <Java canonical class name>.<name>", `TypeError` "Name is null" for
  `null`), and `toJSON()`, the name (it ignores the key `JSON.stringify`
  passes it). `toString()` returns the name, unless Java overrides it:
  `SslClientAuth.REQUIRED.toString()` is `'required'`, Java's
  `super.toString().toLowerCase(Locale.ROOT)`, while its `toJSON()` stays
  `'REQUIRED'`. The JS conventions decide it: their JDK-interfaces row (Class family,
  Q29) has "`toString()` keeps Java's format, with JavaScript's spelling of
  values (Q37)"; the Class family's enum sentence, `toString()` and
  `toJSON()` returning the name, says nothing of Java's override, and Rule 1
  puts content with Java, as root §3 prints a constant's name "unless Java
  overrides it" and Python reaches Java's `toString()` through `__str__`. Java's
  `SecurityProtocol.name` field and `name()` are the one method.
- A `null` key or value binds `never` (JS conventions, Class family, Q26),
  through extra TypeScript signatures for the null combinations, as Python's
  Binding rule adds one stub per combination beside its one stub per Java
  overload, so the per-form rule (Q15) and the binding (Q26) both hold:
  `ProducerRecord`'s constructor has, before each Java constructor's
  signature, one with a `null` key, one with a `null` value and one with
  both, each typed `null`, and `send` takes a `ProducerRecord<K, V>`.
- Signatures (JS conventions, Signatures): one Java overload set is one
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
  and given; `undefined` throws the JS conventions' `TypeError` naming the
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
- Collections (JS conventions, Types): a Java array or collection taken is
  any iterable (`PartitionInfo`'s `Node[]`, plan §5 item 1), and one
  returned is a new array each call (`PartitionInfo.replicas()`,
  `partitionsFor()`'s `List`). A map keyed by strings taken is a `Map` or a
  plain object (the JS conventions' Types row "Other collections taken", Q25):
  `MetricName`'s constructor takes its `tags` so: a `Map` is kept as given
  and `tags()` returns it, as Java stores the map; a plain object is read
  into a new `Map`. Java's `Map<MetricName, ? extends Metric>` is a `MetricNameMap`
  *(js-only, on the invented-names list)*, a read-only map keyed by the
  JSON text of the group, the name and the tags sorted by key (the fields
  `MetricName.equals` compares), keeping the original `MetricName`s, with a
  `toJSON()` writing the entries by that string key, as `TopicPartitionMap`'s
  does. Java's `Map<TopicPartition, V>` is a `TopicPartitionMap` and the
  `Set<TopicPartition>` the client returns a `TopicPartitionSet` *(js-only, on
  the invented-names list)* (JS conventions, Class family and Types, Q25):
  each wraps a `Map` keyed by `'<topic>:<partition>'` (`'orders:3'`;
  `'-<partition>'` for a null topic, and the partition is an integer, so two
  topic partitions never share a key; a `null` key or member, which Java's
  `HashMap` and `HashSet` hold, is the string key `'null'`, which no
  `TopicPartition` gives, and prints as Java's `null`; no key is an
  integer-like property name, so `toJSON()` keeps the entries' order),
  refuses an `undefined` value (`set(tp)` included) with a `TypeError`
  (Signatures, Q18), keeps the original `TopicPartition`s,
  which `keys()`, `entries()` and iteration return, and does not extend `Map`
  or `Set`. Both are mutable (`set` / `add`, `delete`, `clear`), `size()` is a
  method, as `MetricNameMap`'s, `get()` returns `null` for a missing key,
  `toString()` is Java's `AbstractMap` / `AbstractCollection` text
  (`{orders-0=5}`, `[orders-0]`), and `toJSON()` writes a map's entries by
  the string key, a `bigint` value as a decimal string, and a set as the
  array of its members. Where Java takes a `Map<TopicPartition, V>`, only a
  `TopicPartitionMap` is taken, and a `Map` is refused with a hint;
  `check-type-mapping` holds the rows.
- `metrics()` (plan §5 item 3) reads every metric through the C ABI when it
  runs: a `MetricNameMap` of `KafkaMetric`s, plain TypeScript (Java's public
  constructor, its untranslated `MetricValueProvider`, `MetricConfig` and
  `Measurable` placeholder types typed `unknown`, read by their Java
  methods), each built with a provider returning the value read. *(deviation:
  Java's map is a live view. Why: only the clients call the addon, and
  Python's `metrics()` is a snapshot too. Goes when: the JavaScript metrics
  wrap the core's.)* A long value is a `bigint`.
- Serializers (JS conventions, Serialization): `Serializer` is an object
  type in `common/serialization`, run in JavaScript inside `send()`
  (`src/internal/serde.ts`), key then value, after the form and argument
  checks and before the pump and the hand-over; a thenable result is
  awaited. The binding keeps no closed state, so a serializer runs on a
  closed producer too, and the core reports the closed producer at the
  hand-over (rule 1). What a serializer returns is borrowed until the
  hand-over, as the app's bytes are (rule 8). With no serializer (a null
  one, or none, and no config key) the record's own bytes are sent, checked
  at `send()`: a `ProducerRecord<K, V>` holds any key and value, a run-time
  type TypeScript cannot enforce, and a string throws `TypeError` pointing to
  `stringSerializer()`. What a serializer throws, or its Promise rejects
  with, is answered as Java's `KafkaProducer#doSend` answers it (owner's
  decision 2026-11-18; `src/internal/serde.ts` `serializerFailure`,
  `kafka_producer.ts` `serializerFailed`): an `ApiError` runs the callback
  once, inside `send()`, with Java's failure metadata
  (`RecordMetadata(appendCallbacks.topicPartition(), -1, -1, NO_TIMESTAMP,
  -1, -1)`: the record's topic and its partition or -1) and resolves `send()`
  to a done future failed with it (Java's `FutureFailure`), what that callback
  throws or rejects with rejecting `send()` instead; a `TypeError`, Java's
  `ClassCastException`, rejects as a `SerializationError` "Can't convert key
  of class <class> to class <serializer> specified in key.serializer" (or
  `value`), its `cause` the `TypeError`, the classes spelled as JavaScript
  names them (a value's constructor, a primitive's wrapper, `null`, the
  serializer's class, a built-in's factory name, `stringSerializer`)
  *(deviation: Java writes fully qualified Java names, and a null key throws
  `NullPointerException` there. Why: JavaScript has no names of that form)*;
  anything else rejects as it is. No failure `send()` reports, a
  serializer's included, is counted in `record-error-rate` or
  `record-error-total`, which Java's `doSend` records for an `ApiException`
  or a `KafkaException` *(deviation. Why: the core's `send()` records no
  error in that sensor, and a serializer's failure is answered in
  JavaScript (`serializerFailed`), before the core. Goes when: the core
  records its `send()`'s errors, a core issue, `DECISIONS.md` entry 31; and,
  for a serializer's failure, the binding counts it, which it cannot today:
  the FFI hands out only an unmodifiable live view of the producer's metrics
  (`kafka_producer_Producer_metrics`), never its `Metrics` registry or its
  `errors` sensor, and the binding gives the core no serializer (Serialization,
  Q50). It needs an FFI entry that records a send failure on the producer, or
  the core running the serializers)*. The
  binding's own check of what a serializer returns stays a `TypeError`.
  `MockProducer` passes every serializer error out raw, as Java's
  `MockProducer#send` serializes with no `try`. `stringSerializer()`
  (`src/common/serialization/string_serializer.ts`) is Java's
  `StringSerializer` through `encodeLikeJava`; it takes an optional
  `{ configs, isKey }`, Java's `configure`, so it goes in a serde config key
  without `()`, and writes Java's six standard charsets (`US-ASCII`,
  `ISO-8859-1`, `UTF-8`, `UTF-16BE`, `UTF-16LE`, `UTF-16`) byte for byte,
  under their canonical names and the JDK's aliases, ignoring case, as
  `Charset.forName` matches them. `encodeLikeJava(s, charset)` writes them by
  hand where Node's encoders differ: a character US-ASCII or ISO-8859-1
  cannot map is `?`, a lone surrogate in UTF-16 is U+FFFD, and `UTF-16` is
  big-endian after the BOM `FE FF`. Any other name is Java's
  `SerializationError` "Unsupported encoding <name>" *(deviation. Why: Node
  has no Java charset providers. Goes when: never needed for the standard
  set)*. The rest of the factory list
  is not decided yet in the JS conventions. `close()` closes both serializers once
  the core reports the producer closed; a construction the core fails with
  Java's relabelled "Failed to construct kafka producer" closes them too.
  An omitted or `null` key or value binds `never` (JS conventions, Class
  family, Q26), through extra `ProducerRecord` signatures for the null
  combinations beside each Java constructor's, so `send` takes the
  `ProducerRecord<K, V>` of the producer's serializers.
- A Java `Duration` is the root's `Duration` (JS conventions, Signatures): a
  number of milliseconds, or a `Temporal.Duration` (built in from Node 26, a
  polyfill's before), recognised by its `Symbol.toStringTag` and read with
  `total({ unit: 'millisecond' })`, typed by its structure. One private helper
  converts them all (`src/internal/duration.ts`), as the JS conventions say:
  rounded down; `Infinity` is Java's `Long.MAX_VALUE` milliseconds, handed to
  the addon exactly as a bigint. The form called picks the call: `close()`
  passes `null`, which the addon sends to `close_cb`, Java's untimed form,
  and `close({ timeout })` the timeout, to `close_with_timeout_cb`. A
  negative timeout is passed on as it is: for `close` the FFI answers Java's
  "The timeout cannot be negative." (`IllegalArgumentError`) and closes
  nothing; for a wait (`KafkaFuture.get(timeout)`, Java's
  `CompletableFuture.get`) it is no error but no wait, as Java's times out at
  once. One with years, months or weeks, `NaN` and beyond
  ±`Number.MAX_SAFE_INTEGER` ms are a plain `RangeError`; `null` is
  `TypeError('timeout must not be null')`, and any other value `TypeError`.
  `-Infinity`, which the JS conventions do not name, is `RangeError`. A
  record's future waits natively until its delivery (rule 5), so the core
  enforces the deadline; a future with no native handle races a
  `setTimeout`, as the JS conventions decide (Futures and waiting, and
  Signatures, Q22, Q44), and one longer than a Node timer's 2^31 - 1 ms
  waits in steps of at most that.
- Promises (JS conventions, Futures and waiting): a method returns a Promise
  if and only if its entry point has a `_cb` twin, and a method returning a
  Promise never throws: every failure, the argument and form checks and the
  core's errors included, rejects it. `send` returns
  `Promise<KafkaFuture<RecordMetadata>>` (PLAN D8), resolving at the
  hand-over, and the future's `get()` returns `Promise<RecordMetadata>`,
  settling at delivery. `KafkaFuture` is Java's: a TypeScript `abstract
  class`, as Java's is, whose public constructor throws `TypeError`
  ("KafkaFuture cannot be constructed directly; use
  KafkaFuture.completedFuture(...) or KafkaFuture.allOf(...)", Python's text)
  when `new.target` is `KafkaFuture` itself, so a caller's own subclass may be
  built and `allOf` reads one through its `whenComplete`; every future the
  client makes is of its private subclass `KafkaFutureImpl`
  (`src/common/kafka_future.ts`, not exported, its `name` the public class's),
  Python's `_KafkaFutureImpl`, as the JS conventions present a Java
  abstract class (Class family, Q16, Q32: a TypeScript `abstract class`, so
  `tsc` refuses `new`, Java's abstract methods `abstract`, and a constructor
  that throws `TypeError` for plain JavaScript when `new.target` is the
  abstract class itself). Its members, in Java's order: static `completedFuture(value)` and `allOf(futures)` (Java's varargs:
  an array, bare or `{futures}`), `toCompletionStage()` (a `Promise` settling
  with the future, rejecting with the raw failure, the same Promise every
  call), `thenApply(function)`, `whenComplete(action)`,
  `cancel(mayInterruptIfRunning)`, `get()` and `get(timeout)` (Java's `(long,
  TimeUnit)` as one `Duration`), `getNow(valueIfAbsent)`, `isCancelled()`,
  `isCompletedExceptionally()`, `isDone()`, plus `KafkaFutureImpl`'s
  `toString()` on the subclass; the nested `BaseFunction` / `BiConsumer` are type aliases on
  the class's namespace (`KafkaFuture.BaseFunction<A, B>`). One class has two
  value sources, as Python's: a record's future (`send()`'s, Java's
  `FutureRecordMetadata`, over the native future, rule 5: `cancel()` false,
  never cancelled, a timed-out `get` rejecting with the root `TimeoutError`
  and the core's message, "Timed out waiting for KafkaFuture after <ms> ms",
  a negative timeout printed as 0 ms, where Java's is "Timeout after waiting
  for <ms> ms." with the value given *(deviation: the text is the Rust
  core's `KafkaFutureImpl::get_with_timeout`,
  `rust/src/common/internals/kafka_future_impl.rs`, which every future
  shares; changing it is a core change)*) and
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
  (inside the producer's drain for a record), or inside the registering call
  when it is done, as `CompletableFuture` runs it. A dependant's completion is
  queued for the loop already running, as Java's `postComplete` runs a
  dependant's (no recursion, so a chain of any length completes; one waiter
  that throws stops none of the others), while an explicit completion
  (`cancel()`, the client's) finishes its own dependants, and an `allOf` over
  it, before it returns, even inside another's function. An `allOf` reads its
  sources' states, as Java's relays on their results: it completes once every
  source is done, whichever source's completion runs it, its failure the first
  in the sources' order. Nothing waits or allocates a Promise until a `get()`
  or `toCompletionStage()` (the JS conventions' lazy Promise). *(Deviation
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
  unhandled rejection, and neither do its dependants, and starts no native
  wait; a `get()` Promise the
  caller drops still raises one, as any rejected Promise does.
- `Callback` is a function of one object of named parameters,
  `({ metadata, exception }) => void` (JS conventions, Class family), Java's
  `onCompletion(metadata, exception)` named as Java names them (the one place
  `exception` is an identifier, allowlisted in `check-exception-word`), run
  before the future completes, as Java fires the callback before completing
  the future (rules 5 and 6). A callback that returns a Promise is awaited
  (rule 6): the
  record's future completes, and a `flush()` or `close()` covering the record
  returns, only once it settled; a rejection is a throw, logged by the core
  and never propagated, except the callback a serializer's `ApiError` runs
  inside `send()`, whose throw or rejection rejects `send()`, as Java's
  `onCompletion` throws out of `KafkaProducer#doSend`'s `ApiException` catch,
  which has no `try` around it. **(conventions conflict)** The JS conventions,
  Threads and callbacks, say the producer logs a send callback's error; that
  holds where the core runs the callback (the delivery path, and a hand-over
  the core refuses, where the core logs the throw and `send()` resolves), but
  not for the serializer's catch, which runs in JavaScript. The code follows
  Java there (`DECISIONS.md`, entries 4 and 29): the JS conventions' wording needs its
  owner. Such a Promise must not wait for `flush()` or
  `close()` of its own producer, which wait for it (JS conventions, Threads
  and callbacks: anti-patterns inside a send callback). In a callback the
  core runs in its drain, before its first `await`, `flush()` is refused, as
  Java's throws, and `close()` closes at once as `close({ timeout: 0 })`, with
  Java's warning, failing the records still pending with "Producer is closed
  forcefully.", as Java's `KafkaProducer.close` documents. A callback that
  waited for its turn behind an earlier callback's Promise (rule 6) runs from
  the settled Promise's continuation, outside the drain, so the core sees no
  callback running: it is as after its first `await`, its `flush()` is not
  refused and its `close()` waits for it; refusing them there stays the JS
  conventions' Future improvement (owner's decision 2026-11-19,
  `DECISIONS.md` entry 34). **(conventions conflict)** The JS conventions' Q46
  says the core refuses these calls before the callback's first `await`;
  Java and the core refuse only `flush()` and close as `close(0)`. The code
  and docs follow Java and the core (`DECISIONS.md`, entries 4 and 27): the
  JS conventions' wording needs its owner. Q46's other statement, that awaiting a later
  record's future waits for that same callback, holds: a later record's
  future completes only after its own callback ran, which waits for this one
  (rule 6).
- Java's `Producer` interface is a TypeScript `interface` (JS conventions,
  Class family, Q10) with the members `KafkaProducer` translates (`send`,
  `flush`, `close`, and `[Symbol.asyncDispose]` for Java's `Closeable`), in
  `src/producer/producer.ts`; `KafkaProducer implements Producer`. Java's
  `Producer` has no static member, so it has no runtime object (one would
  hold the statics, as `Admin`'s holds `Admin.create()`).
- `KafkaProducer` has `close()` and `[Symbol.asyncDispose]()`, which calls
  `close()`, so `await using` closes it.
- `MockProducer` (`src/producer/mock_producer.ts`) wraps the FFI's
  `MockProducer` (JS conventions, Implementation over the FFI, Q62): the
  addon builds it through `MockProducerOptionsBuilder` and
  `MockProducer_with_options` (`autoComplete` set, the cluster left
  `Cluster.empty()`, no partitioner and no serializers), and it is the same
  native handle as a `KafkaProducer`'s, over its `__as_Producer` view, so
  `send`, `flush`, `close`, `partitionsFor` and `metrics` go through the
  producer's `_cb` twins and drain, with the same lifetime rules (rules 3, 5,
  6, 10: no closed flag, `close()` keeps the handle, the finalizer destroys
  only a closed mock, which `MockProducer_closed` decides, as a close that
  threw `closeException` leaves Java's mock open), each `_cb` call counted as
  a producer's, so a call made on a closed mock holds Node until its answer
  ("Threads and liveness"). Its constructors are
  Java's `()` and `{ autoComplete, partitioner, keySerializer,
  valueSerializer }`, `partitioner` typed `Partitioner | null`, the
  placeholder (Scope, Q30), and never called, as the cluster is empty; the
  `Cluster` form is left out. A send passes the app's `ProducerRecord`: its
  key and value cross as the `void *` addresses of a `MockRecord`'s tags,
  which the FFI's mock keeps without reading, and the addon holds the record
  by a reference from the send until `clear()` (for a record a hand-over
  took) or the destroy (`native/producer.c`, `MockRecord`), so `history()`
  returns the app's own records; a record the mock refused is released at
  its hand-over, with its send's context, as the core's mock refuses before
  it keeps the record or makes a completion. No header crosses, the record
  keeping its own, writable, as Java's mock leaves them, and no partition:
  the core's mock reports partition 0 and Java's offsets whatever the record
  says, as Java's empty-cluster mock sends to partition 0. A waiting send's
  delivery function is held by the `MockProducer` (`#pending`) and only
  weakly by the addon, so a closed mock nothing references is collected and
  destroyed, its sends pending at the close or dropped by `clear()` freed
  then (`Producer.mock_pending`), while a `completeNext()` after `close()`
  still runs the callback, as Java's does; an error given to the mock is
  released once the core holds no copy of it (`injected_errors_sweep`, run
  by every mock call). The serializers run in
  JavaScript on every send, key then value, each with its own fresh empty
  `Headers` (Java's `new RecordHeaders()` per call when the cluster has no
  partition), awaited when they return
  a thenable, and what they return is not kept; a null one is not run, as
  the core's mock skips it *(deviation: Java's dereferences it. Why: the
  mock is the core's)*. The serializers run before the core's mock checks
  that it is closed, fenced or has a send error, which Java checks first
  *(deviation, as for `KafkaProducer`)*. After each send, `completeNext()`
  and `errorNext()` the mock runs the producer's pump, so a callback runs
  inside the call that completed its record, as Java's mock runs it, and
  what a callback throws comes out of that call, as Java's `onCompletion`
  runs with no `try`: `completeNext()` and `errorNext()` throw it, `send()`
  (with `autoComplete`) and `flush()` reject with it, and nothing logs it.
  The record's future completes all the same *(deviation: Java's mock leaves
  it incomplete. Why: the core's mock completes it. Goes when: the core's
  mock leaves it incomplete)*, and `flush()` completes every waiting record,
  running each callback, before it rejects with the first throw
  *(deviation: Java's `flush()` stops at the first callback that throws, the
  later records staying waiting. Why: the core's mock completes all of them
  in one call. Goes when: the core's mock stops at the first throw)*. A callback's Promise that rejects is the
  callback's error, as a throw is, and nothing logs it (owner's decision 2026-11-18, restoring an earlier accepted design;
  `DECISIONS.md` entry 30): `send()` with `autoComplete` waits for its callback's Promise and rejects with its rejection, and `flush()`
  rejects with it; `completeNext()` and `errorNext()`, being synchronous, keep it in a FIFO (`#callbackErrors`), and
  the next `send()`, `flush()` or `close()` raises the oldest, once *(deviation, as Python's `AsyncMockProducer`:
  Java's mock throws it from the completing call. Why: these calls return before the Promise settles)*. `flush()`
  and `close()` (so `[Symbol.asyncDispose]` too) wait for every callback Promise already started (`#running`,
  `allSettled` semantics), and for the callbacks waiting for their turn, then raise the oldest kept error, so no kept
  error is lost; `flush()` with a kept error still flushes first. The mock's callbacks go through the addon's run queue
  (rule 6), so they run one at a time in completion order, as the producer's (`DECISIONS.md` entry 34): one that waits
  for its turn behind an earlier callback's Promise runs once that settled (`send_run_queued`), after the
  `completeNext()` or `errorNext()` that completed its record returned, so what it throws goes to the FIFO, as a
  rejection does (`#completing` counts the completing calls that can still throw it) *(deviation, as Python's
  `AsyncMockProducer`: Java's runs it inside the call. Why: a callback starts only once the one before it settled)*,
  and the addon holds its function strongly from the moment it waits, so `clear()`, which drops `#pending`, leaves it
  to run (`send_enqueue`). A callback's error belongs to the call that completed its record only: with `autoComplete`
  that is the record's own `send()`, whichever drain runs its callback, and otherwise `completeNext()`,
  `errorNext()` or `flush()` (the throw to the completing call, the rejection to the FIFO), so a `send()` nested in a
  callback, or a `send()` whose drain runs the callbacks a pending `flush()` completed, leaves those errors to their
  own calls. The callback queue does not nest (the addon's `cbq_drain` returns inside a drain, and the FFI's
  `CallbackQueue::execute` returns 0 there), so with `autoComplete` a `send()` made from a callback's synchronous part
  has its record's callback run in its turn, once that callback and the Promise it returns settled; that `send()`
  waits for it before it settles, and rejects with its throw or rejection, as Java's nested `send()` throws it, so a
  callback that awaits a `send()` it made never settles *(deviation: Java's runs the callback inside the nested
  `send()`. Why: the callback queue does not nest, and the callbacks run one at a time. Goes when: a drain started
  inside a drain runs the callbacks, where `cbq_drain` and `CallbackQueue::execute` return at once)*. A nested completing call
  (`#callbackDepth > 0`) neither clears nor takes `#callbackThrown`, so an earlier record's throw stays the outer
  call's, as Java's first `onCompletion` throw ends it. A `completeNext()` or `errorNext()` made
  there cannot wait: the next record's callback runs in its turn, and its throw comes out of the outer completing call,
  or goes to the FIFO when none runs it *(deviation: Java's nested call throws it. Why: the callback queue does not
  nest, and those calls are synchronous)*. A `close()` made from one of the mock's
  callbacks before its first `await`, a callback the core runs in its drain, does not wait for the callbacks, its own
  among them, as Python's `AsyncMockProducer` skips the wait (`_in_async_callback`); the mock counts the callbacks
  running their synchronous part (`#callbackDepth`). One that waited for its turn is as after its first `await`. After
  that `await` the core's close waits for the running callback's
  report (the FFI's `in_callback` is the drain's thread only), and a `flush()` from a callback waits for that
  callback's report too, unless a flush error is set, which it rejects with at once, as Java's (the core's `flush_cb`
  waits for the reports only on success), so a callback that awaits either never settles, the anti-pattern the producer documents
  (`KafkaProducer` refuses a `flush()` made before the callback's first `await`; Java's mock makes no such check); Python's mock, not over the core, skips the wait after an `await` too. The record's future completes
  once the Promise settled, the deviation above for a throw covering this case too.
  `partitionsFor(null)` rejects with the FFI's `TypeError` "topic cannot be
  null" *(deviation: Java's mock answers `[]`, as its empty cluster has no
  topic. Why: the FFI checks its mandatory parameters, and the binding
  repeats no precondition, Q35. Goes when: the FFI's mock accepts a null
  topic)*. `send(null)` on a closed mock rejects with `TypeError` "record
  must not be null" *(deviation: Java's mock checks `closed` first, an
  `IllegalStateException` "MockProducer is already closed.". Why: the
  binding keeps no closed state (Q61), and the record is checked in
  JavaScript, by `#send`'s own TypeScript check, before the call reaches the
  core's mock, which holds the closed state, the reason `KafkaProducer.send`
  records for the same order)*. Java's
  public error fields are setters only (plan §5 item 2, Python's
  `set_send_exception`): `setSendException`, `setFlushException`,
  `setPartitionsForException`, `setCloseException`, over
  `MockProducer_set_*_error`, as `errorNext(e)` is over `error_next`; the
  addon hands the core a `LocalCallbackError` whose opaque is an
  `InjectedError` holding the JS value, and `native/error.c` hands that very
  value back wherever the core reports the error (JS conventions, Errors,
  Q38). Left out: transactions (by request), `setMockMetrics` (the FFI takes
  a native `KafkaMetric_t`, which the binding cannot build, as .NET's mock,
  which also wraps the FFI's, leaves it out), and the
  telemetry and metric-subscription calls (no FFI); `metrics()` is therefore
  always empty. The mock emits no preview warning.
- A Java `long` is a `bigint` (JS conventions, Types): returned always as a
  `bigint` (`RecordMetadata.offset` and `.timestamp`, `ProducerRecord.timestamp`,
  a `long` constant such as `SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN`),
  and taken as a `bigint` or a whole `number` up to 2^53 - 1, a larger number
  throwing `RangeError` rather than rounding; the addon reads and writes it
  with `napi_get_value_bigint_int64` / `napi_create_bigint_int64`. A Java
  `int` is a `number`, and one outside Java's range throws `RangeError`
  (`ProducerRecord`'s partition). Timeouts stay milliseconds or a
  `Temporal.Duration` (Q22).
- Errors (JS conventions, Errors): the generated error hierarchy is the
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
  kept in the private `_ffiId`, for the binding and the test harness (JS
  conventions, Errors; `check-internal-members` allows the name on the error
  classes and asks `KafkaError` for it): each generated class with an id
  declares `static readonly _ffiId`, marked `@internal` so `stripInternal`
  leaves it out of the declarations, and an error from the core whose id
  differs from its class's (an unknown id) keeps it in its own
  non-enumerable `_ffiId`. **(conventions conflict)** The JS conventions' Class
  family names library-only members through `@internal` members named
  `internal...`, while `_ffiId` is an underscore name; the code follows the
  JS conventions' text. `src/internal/errors.ts` `errorFfiId` gives the id to the gRPC
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
  `new KafkaError(cause)`), as the JS conventions say; matched by name
  through `src/internal/args.ts`, one TypeScript signature per form, each
  key documented on the options type. Each form's message is the one Java's
  constructor chain gives: the cause's `String()` for `(cause)`, as Java's is
  `cause.toString()`, or Java's own fixed text. A second argument (ES2022's
  `new Error(message, { cause })`) throws `IllegalArgumentError` in the form
  check's format (JS conventions, Errors, Q39), listing the class's forms
  as JavaScript calls them, by number of parameters, ties in Java's
  declaration order: `()`, a one-parameter form's value on its own,
  `(message)` or `(cause)`, and a longer form's options object,
  `({ message, cause })`, as in `KafkaError() takes one of (), (message),
  (cause), ({ message, cause }); got 2 arguments` (the `callShapes` option of
  `src/internal/args.ts`, which every error class's matcher sets, so its
  other form errors list the forms the same way). A method's form error
  keeps its own format (Signatures, above). Java's abstract bases (`RetriableError`, ...)
  are catch-only, `abstract` classes without an FFI id: constructing one
  throws `TypeError` (JS conventions, Class family, Q16, Q32). A user's
  subclass takes the forms of its nearest generated class. The constructors
  that take a payload are generated too (JS conventions, Errors, Q39), in
  the same one-object shape, Java's other parameters by their Java names
  (`new TopicAuthorizationError({ message, unauthorizedTopics })`); a payload
  is given by name only, never as a bare value, so a form error lists such a
  one-parameter form as its options object, `({ unauthorizedTopics })`. Each
  form's message and the fields it sets are Java's constructor chain, which
  the generator evaluates as JavaScript (Java's `String.valueOf` and
  `Double.toString` for a message it builds, `src/internal/java_string.ts`),
  and each payload parameter is taken by its kind (`src/internal/payload.ts`:
  a `Set<String>` an iterable of strings, a string refused; a
  `Map<TopicPartition, Long>` a `TopicPartitionMap` only, a `Map` refused with
  a hint; a `long` a `bigint` or a whole number). The payload getters are
  methods over private fields (`unauthorizedTopics()`, `partitions()`), which
  only read and return a new collection the caller may change
  (`Set<String>` a `Set`, `Set<TopicPartition>` a `TopicPartitionSet`,
  `Map<TopicPartition, Long>` a `TopicPartitionMap<bigint>`), with Java's
  static factory (`GroupAuthorizationError.forGroupId`) and Java's
  `toString()` override (`QuotaViolationError`'s, its class name written as
  the error's `name`). An error the core reports carries its payload: the
  addon reads it through the FFI's view of the class and its accessors
  (`native/error_payload.c`, generated from the table in
  `rust/xtask/src/error_hierarchy/js.rs`), and `create` runs the class's
  constructors with a private token so each class sets its fields.
  `QuotaViolationError`'s `metric()` is a `KafkaMetric` built, as the .NET
  client builds it (Python keeps the name and group only), from the metric
  name the view gives and the error's `value` (the value Java's `Sensor.checkQuotas` measured), with an
  empty name when the view gives none. A field the core cannot give keeps
  its default, listed in its module's header comment:
  `CorrelationIdMismatchError`'s ids are
  `-1`, as Python's *(deviation: Java's error carries them. Why: the class is
  in `common.requests`, not a supported API, so the C API has no view of it.
  Goes when: the C FFI gives a view of it)*. A form's doc says what Java's
  shorter forms leave the getters returning (Q64: "`groupId()` returns
  `null`"). The public types a
  payload reaches are translated, as Python's Scope translates them (JS
  conventions, Scope, Q30: only `MockProducer` and `KafkaMetric` keep
  untranslated types): `TimestampType` (`common/record`, an enum whose Java
  field `name`, `'CreateTime'`, is reached through Java's `toString()`, as
  Python's Enum rule reaches it, since a field cannot share the name of
  `Enum.name()`), `OffsetAndMetadata` (`consumer`, a data class, its
  `leaderEpoch` Java's `Optional<Integer>` getter as `number | null`), and
  Java's nested `RecordDeserializationException.DeserializationExceptionOrigin`,
  a class of the namespace merged with `RecordDeserializationError`, so a
  static member of it (Names, Q11). A `ByteBuffer` payload is taken as a
  `Uint8Array` and kept and returned as a `Buffer` over the same memory, a
  `Headers` one as the `Headers` given, or a new one from an iterable of
  `Header` objects and `[key, value]` pairs; from the core, both are copies,
  as the error outlives the core's handle. Java's `public static final` instances are: `INSTANCE` on
  `DisconnectError` and `CoordinatorNotAvailableError`, a static property of
  the class that is not writable, built by Java's no-argument chain (an empty
  message, no cause) whatever that constructor's visibility
  (`CoordinatorNotAvailableException`'s is private), as Python's
  `_throwable.singleton` (each module's header comment says so); a class whose every
  Java constructor takes a type the client does not translate has a
  `protected` constructor that throws `TypeError` until then.
- The closed producer (JS conventions, Implementation over the FFI, Q61):
  the binding keeps no closed flag. A call after `close()` goes to the core:
  `send` rejects with its `IllegalStateError` "Cannot perform operation after
  producer has been closed", after the record was checked and converted in
  JavaScript *(deviation: Java checks the closed producer before it
  serializes. Why: the binding keeps no state, and the conversion runs in
  JavaScript before the call reaches the core)*; `flush()` and a second
  `close()` do what the core does (they resolve), with no deviation. A
  negative `timeout` is `IllegalArgumentError` "The timeout cannot be
  negative.", from the FFI, and such a close changes nothing (rule 1).
- Configuration (JS conventions, Configuration, Q52, Q54, Q55): a plain
  object of Java's config keys (a `Map`, or another non-plain object, throws
  `TypeError`), and a key that is not a string (a symbol) throws
  `ConfigError` "Key must be a string.", as it has no string to cross as
  (`src/internal/config.ts`). The binding has no `ConfigDef` table and checks
  no value: each value becomes the string the FFI takes (`true` → `"true"`,
  numbers in decimal, never in exponent form, bigints in decimal, so a `long`
  above 2^53 goes as its exact digits, arrays' items written so and joined
  with `,`, anything else by `String()`, an object as `[object Object]`);
  `null` and `undefined` leave the key unset; unknown keys, a `kafkaJS` block
  included, pass, and the core lists the ones it does not use. The keys and
  values cross as `encodeLikeJava`'s bytes, and the core checks types,
  ranges, enums, unknown keys and cross-key rules with Java's messages naming
  the key. `key.serializer` and `value.serializer` take a synchronous
  function `({ configs, isKey }) => serializer`, which the producer calls
  once, key then value, with a frozen copy of the whole configuration, each
  value as given, through a `Proxy` that records each of its keys the
  function reads. The two serde keys are left out of the configuration the
  core gets, and so is each read key that is not a producer key, of Java's
  or of the core's own (`partitioner.type`):
  `src/internal/producer_config_keys.ts`, generated with the
  `ProducerConfigs` type, from Java's `ProducerConfig` and the core's
  `rust/src/producer/producer_config.rs`. A key such as
  `schema.registry.url` is left out, so that the core
  doesn't list it as unused; the consumer's serde keys reach the core, which
  lists them as unused (JS conventions, Serialization, Q53). **(conventions
  conflict)** The JS conventions' text says "the binding leaves those keys out of the
  config it sends to the core, as Java's `originals()` marks them as used",
  which read literally drops every key the function reads, `bootstrap.servers`
  included, so a function that spreads the configuration would leave the
  core with none. The code follows the rule's purpose and Java: Java only
  marks the keys it reads and never removes them, so a producer key the
  function reads still reaches the core, and only a read key the core doesn't
  know is left out, which is what keeps `schema.registry.url` from being
  listed as unused. Two deviations follow from the function running before
  the core exists, to know which keys it reads *(deviation. Why: the binding
  calls the function before the core is built. Goes when: the FFI exposes the
  computed client id and the configuration check before construction)*: the
  function gets the configuration as the user gave it, without the computed
  `client.id` Java's `originals()` carries, and a configured serializer is
  built before the core checks the configuration, where Java checks it
  first. A serializer given as a
  constructor argument wins, and its config key is then ignored; a null one
  is the config key's, as Java's `if (keySerializer == null)`. A function
  that throws is Java's "Failed to construct kafka producer" `KafkaError`,
  with the error as its `cause`, and the serializers made so far are closed;
  those the config functions made are closed on any construction failure,
  since only the producer holds them. The `ProducerConfigs` type, generated into
  `src/producer/producer_configs.ts` by the `config_types` emitter
  (`rust/xtask/src/config_types/js.rs`), lists the keys Java's
  `ProducerConfig` defines (with its SSL and SASL keys) for autocompletion
  and takes any other string through `(string & {})` *(js-only, types
  only)*. Debug lines use `util.debuglog('confluent_kafka')`, shown with
  `NODE_DEBUG=confluent_kafka`; no constructor takes a logger or an error
  callback.

### The admin client

The rules are the JS conventions' (Scope, Class family, Futures and
waiting, Signatures, Types, Implementation over the FFI) and
`.claude/rules/admin-client.md` (§1: each RPC is a plain method returning its
Result, only `close()` waits; §9: `MockAdminClient` stays public). The
decisions it rests on are `DECISIONS.md` entries 35 to 53. What this binding
adds:

- **The native layer** (`native/admin.h`, `admin.c`, `admin_values.c`, all
  hand-written, entry 32). `KafkaAdminClient` and `MockAdminClient` share one
  native handle type, `Admin` (`admin.h`), over a `kafka_admin_Admin_t` view:
  the client's own handle, or the mock's `MockAdminClient__as_Admin`, as
  `KafkaProducer` and `MockProducer` share theirs. It embeds a
  `CallbackQueue` over `kafka_admin_Admin__set_callbacks_notify_err` and
  `__execute_callbacks_err` (`admin_execute`), so boundary rules 2, 3, 9 and
  10 hold as they do for the producer: one drain, the notify hook only
  signals, `close()` goes to `close_cb` or `close_with_timeout_cb` every time
  (the form called picks the call) and keeps the handle, and the handle
  object's finalizer (`admin_finalize`) destroys only a closed client
  (`admin_destroy`), running no JS. Each RPC is one native function
  (`adminCreateTopics`, ...): it reads plain JS values, builds the C inputs
  by the FFI tables (a `kafka_List_t` or `kafka_Map_t`, a value class
  through its C constructor, the options through `<Rpc>Options_new` and one
  `set_<field>` per setter that ran, in last-call order), calls
  the RPC's `..._options` entry point (`kafka_admin_Admin_<rpc>_with_options`,
  or the form's own, such as `describe_topics_with_topic_names_options`) and
  wraps the `<Rpc>Result_t` in a box whose
  finalizer destroys it. A value returned is read through its accessors into
  plain JS values, which TypeScript builds the class from (Python's
  `*_to_py`, entry 43); an enum crosses as its Java code where Java has one,
  else as its ordinal, the C `_e` order being Java's. Each RPC's tables are
  written in a section that binds the RPC once (`#define ADMIN_RPC <Rpc>`
  ... `#undef`), whose rows `ADMIN_CHECKED` (`_Generic`) checks against the
  header's types for that RPC, so a row naming another RPC's destroy,
  accessor or setter does not compile; each row still names its header
  function in full, which the lints read. A mutable static is
  `_Thread_local` (the addon is loaded once per process and shared by every
  worker): `converting`, the future whose value is being converted, and
  `remove_members_pending`.
- **Futures and Results** (`src/internal/admin_result.ts`,
  `src/internal/admin_*_calls.ts`). Each `kafka_common_KafkaFuture_t` a
  Result hands out lives in a context (`AdminFuture`) in the client's slot
  table, as a record's future does (boundary rule 5), with its value
  converter: owned, or `const`-borrowed with a reference on the box that
  owns it (`ADMIN_ACC_ONE_BORROWED`), or borrowed from the value of another
  future (`ListTransactionsResult.byBrokerId`'s, through
  `admin_future_new_borrowed_from_value`, which keeps that future's context
  alive). Its untimed wait (`get_cb`) starts when the Result is made, not on
  first use as a record's does (entry 44): `getNow()`, `isDone()` and the
  other synchronous reads must see the outcome, and the binding calls no
  blocking C function. The wait's completion (`future_on_wait`) converts the
  borrowed value and completes the JS `KafkaFuture` in the client's drain,
  then drops the context; `get({ timeout })` calls `get_with_timeout_cb` for
  the deadline alone. No Promise exists until a `get()` or
  `toCompletionStage()`, so a failed future nobody reads raises no unhandled
  rejection. Each wait is counted as a `_cb` call ("Threads and liveness"),
  so a closed client with an unanswered future holds Node until the core
  answers it (entry 33). The outcome arrives on a later turn of the event
  loop, never inside the call, the mock's included (entry 36). The Results
  are Java's classes in TypeScript, holding the futures Java's constructor
  holds and composing `all()`, `values()` and the refinements with `allOf` and
  `thenApply` where Java does; where Python's Result takes the C future
  (`DescribeUserScramCredentialsResult`, `AlterConsumerGroupOffsetsResult`,
  `RemoveMembersFromConsumerGroupResult`, `ListGroupsResult`,
  `DeleteConsumerGroupOffsetsResult`, and refinements such as
  `CreateTopicsResult.config(topic)`), so does this one. A Result's
  constructor is Java's: `private`, built through `internalCreate`, or
  `protected` / public where Java's is, and its doc names the call that
  returns it.
- **Options** (`src/internal/options.ts`). Each `*Options` class keeps the
  setters that ran, in last-call order, which the addon turns into
  `set_<field>` calls; `timeoutMs` is read by `admin_option_timeout`
  (`null` as `-1`, the core's `default.api.timeout.ms`; a negative value as
  `0`, entry 48). Java's `null` options cross as a marker the entry point
  turns into `NULL`, which the FFI refuses ("options must not be null").
- **Nulls the C API cannot carry**, which Java stores and reads later or
  never (a group id, a transactional id, a topic of a `TopicPartition`, ...),
  are a `TypeError` at the call, each listed as a *(deviation)* in the admin
  package's documentation (`src/admin/index.ts`) with its Goes when (entry
  51). Each such check runs in Java's order, after the form check.
- **`MockAdminClient` wraps the FFI's mock** (JS conventions,
  Implementation over the FFI, Q62; entry 45), as the .NET client's does, so
  it answers as the core's mock does. Its constructors are Java's `()` and
  `{ brokers, controller }`, `MockAdminClient.create()` returns its `Builder`
  (a keyed constructor and Java's setters, over the FFI's `Builder`), and
  its own methods are those of Java's mock that have an entry point; the
  others are listed in the admin package's documentation. It differs from
  Java's mock where the core's does (entry 51), chiefly: a call Java's mock
  does not implement returns its Result, whose futures fail with
  `UnsupportedOperationError`, where Java's mock throws from the call; and
  its futures are not done when the call returns (entry 36). Every such
  difference is a *(deviation)*, with its Why and Goes when: in the admin
  package's documentation, or, for the futures not done after the call and
  `createDelegationToken` with no renewer, on the class, which that
  documentation points to. A call made on
  a closed mock holds Node until its answer, as a closed client's.
- **The value-keyed maps** (entry 38) are each a class over a `Map` keyed by a
  string made from the fields the Java class's `equals` compares, built as
  `TopicPartitionMap` is (Java shape, JS idiom), with its methods, and listed
  in `lint-mapping.json` as invented names. A map keyed by strings that a
  call takes is a `Map` or a plain object (entry 49); one a Result returns is
  a `Map`, keyed by `number` for broker ids (a `ReadonlyMap` for Java's
  unmodifiable view) **(not decided yet in the JS conventions**: the
  Types table has no row for `Map<Integer, V>`, `OptionalLong` or a
  returned `Throwable`, typed here `Map<number, V>`, `bigint | null` and
  `unknown`; entry 52**)**.
- **The data classes** are the seven `*Description` classes (entry 39), in
  `DATA_CLASSES`; a collection they hold is read-only
  (`internalSetReadOnly` on `TopicPartitionMap` and `TopicPartitionSet`,
  `src/internal/read_only_map.ts`).
- **Strings.** The SCRAM upsertion's password goes through `encodeLikeJava`,
  as Java's `getBytes(UTF_8)`, and so does its random salt. The other admin
  strings (ACL resource names, principals, hosts, user names, group and
  transactional ids, feature names) cross as JS strings, as topic names do,
  so a lone surrogate there is written as U+FFFD, and a Result keyed by such
  a string no longer finds the caller's key **(not decided yet in the JS
  conventions**: Q24 lists neither; entry 52**)**.
- **The gRPC server** serves the `AdminService` (`grpc-server/grpc_admin.js`,
  its 47 RPCs: the 45, `CreateAdmin` and `Close`) over the public API, the
  proto translation in `grpc-server/grpc_admin_translate.js`, as .NET's
  `TranslateAdmin.cs` and Python's servicer have theirs. A per-key failure
  is that key's entry error, the whole call's the top-level error, and an
  argument the server refuses is `LOCAL_ILLEGAL_ARGUMENT` "js server: ...".
  The Rust harness runs `multilanguage_admin_test!`'s `__grpc_js` arm through
  `multilanguage_admin_js_test!` and `JsGrpcFactory`'s `AdminBackendFactory`;
  every admin case runs, so `JS_GRPC_SKIPS` has no admin entry.
  `test/grpc_server/admin_service.test.js` calls the handlers in-process,
  every RPC of the proto included.
- **The rule linter reads the admin client as it reads the producer** (The
  rule linter): the C rules know its names (`admin_execute` the drain's
  execute for `check-single-drainer`, `admin_release` for
  `check-tsfn-lifetime`, the future waits `adminFutureWait` and
  `adminFutureGet` for `check-completion-order`, the destroys taken into the
  descriptor tables for `check-ownership`, the constructors' failure paths
  for `check-out-err`); `check-public-audience` keeps `MockAdminClient`, a
  Java test fixture, public; `check-java-completeness` reads Java's
  `MockAdminClient` and its `Builder` from the Java index and counts
  `Admin.create` on the runtime object; `check-type-mapping` holds the rows
  of the value-keyed maps and sets; `check-waiting-call-promise` follows a
  Result's `all()` to the futures it composes; `check-exception-word` keeps
  Java's `DeleteAclsResult.FilterResult#exception()`; and
  `eslint-baseline.txt` lists the members of Java's `Admin`,
  `KafkaAdminClient` and `MockAdminClient` left out for want of an entry
  point, with the admin package's documentation as their reason.
  `test/lint/mutation.test.js` injects a violation into the admin files for
  the C rules too.
- **The examples** (`examples/admin/`, `examples/README.md`): one per RPC,
  the JavaScript twin of `rust/examples/admin/`'s of the same name, with the
  same command line (`admin/common.js`: `-X key=value`, `--flag`,
  `--name=value`, positional arguments, `--help`, `KAFKA_BOOTSTRAP_SERVERS`;
  exit codes 0, 1 and 2) and the same output, run by
  `test/integration/admin_examples.test.js` (entry 42). They use the client
  as users do (How the client is used, below), and `make typecheck-examples`
  and the linter check them (The rule linter).

### Files and layout

The layout is the JS conventions' (Layout and modules).
`test/static/layout.test.js` checks the parts of this a test can see.

- `native/`: the addon, its C sources and its build file. `binding.gyp` and
  `addon.exports.map` (the Linux export list of a static build) are here, not
  at the package root, so npm never compiles on install; `make build-addon`
  runs node-gyp in `native/`, which builds `native/build/Release/` (JS
  conventions, Build, packaging and release, Q73: the build tool is node-gyp,
  `javascript/` finds the core in `../rust`, `CONFLUENT_KAFKA_LIB_DIR`
  overrides that, the target is `make build-javascript`, the addon targets
  Node-API 9 (`NAPI_VERSION=9`), and a release `.node` exports only Node-API's
  registration symbols). `util.h` is the only file including the bindings
  header and holds the `out_err` macros (`FFI_ERR`, `FFI_SET`,
  `FFI_DISCARD`); `addon.c` is the entry point and registers every native
  function; `callback_queue.c` is a client's notify hook, threadsafe
  function, drain, keep-alive and teardown (each client embeds a
  `CallbackQueue` and passes its own `__execute_callbacks`); `error.c`,
  `record.c` (`ProducerRecord`, the borrowed key and value), `headers.c`
  (the copy of a record's headers a send hands the core), `record_spy.c`
  (the test-only read-back of the record a send built), `producer.c` (the
  `KafkaProducer` and `MockProducer` handles, which share one native handle
  type), `admin.h`, `admin.c` and `admin_values.c` (the `KafkaAdminClient`
  and `MockAdminClient` handle, its futures and results, and each RPC's
  marshaling: "The admin client").
- `src/`: TypeScript sources only (`.ts`; no `.js`, no `.d.ts`), one folder
  per Java package (`clients` dropped), one public Java type per file,
  snake_case names; `RecordHeader`, of Java's `common.header.internals`, is
  in `src/common/header/` (Headers, above). The package's own helpers are in
  `src/internal/` (never exported, never declared; `dist/internal/` is not in
  `exports`). Every other folder is a package with one import path
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
  build `native/build/Release/`. Each copy of the package loads an addon
  instance of its own (`process.dlopen` with a module object of its own, not
  `require()`, whose cache would hand two copies resolving the same `.node`
  file the one instance), and `setFactories` registers that copy's classes on
  its instance, so a copy's producers build that copy's errors and
  `RecordMetadata`, as each copy of Java's kafka-clients, loaded by a class
  loader of its own, throws its own classes. The `.node` file itself is loaded
  once per process, so what the addon keeps per process (the preview notice,
  `addon_pin_loaded`) stays once per process.
- Types are TypeScript, under `strict` (`tsconfig.json`). The docs follow the
  JS conventions (Documentation): TSDoc translated from the Javadoc, Java's
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
  overloads it leaves out; the producer package's is its `index.ts`'s
  `@packageDocumentation` TSDoc, so the declarations show it (the API report
  records only that the package has one). The docs lint, the `require-param`,
  `require-returns` and `require-hyphen-before-param-description` rules of
  `eslint-plugin-jsdoc` (Documentation, Q64, Q74), runs in `make lint-rules` (The rule linter), and `make docs` builds the HTML
  reference with TypeDoc (`typedoc.config.cjs`, JS conventions,
  Documentation, Q64) into `build/docs/`: one module per import path, each
  package's `@packageDocumentation` its page, the `@internal` members left out
  as `stripInternal` leaves them out, and a warning failing the build. It is
  not part of `verify`; `test/static/docs.test.js` checks that it builds. `make
  build` (`build-ts`) compiles `src/` into `dist/` (`.js`, `.d.ts`, both with
  maps): CommonJS (`module: nodenext`, the package's `"type"` being `"commonjs"`, JS
  conventions, Layout and modules), so `require` loads it and an ES module
  `import`s it through Node's CommonJS interop, ES2022 (and ES2024's string
  methods) for Node >= 22.14.0, `engines.node` (JS conventions, Build,
  packaging and release); the other `tsc` settings, such as the target, are
  not decided yet in the JS conventions. `scripts/dist_headers.js` then
  puts each source's licence header first in its `.js` (above tsc's
  `"use strict";`) and `.d.ts` (where tsc drops it), the source maps shifted
  to match. `dist/` is git-ignored and is what
  `package.json` points at and publishes; a `dist/internal/` module's `.d.ts`
  is removed after compiling. A Java constructor that is not public is a
  TypeScript `private` constructor (`protected` where Java's is); there is no
  run-time guard (JS conventions, Class family). The library builds such an
  object through one static factory, `internalCreate` (`RecordMetadata`'s
  builds it from the native layer's fields, through its public constructor
  with a module-private mark that no caller can give), and calls its own methods on public
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
  `expectTypeOf` (a pinned devDependency; JS conventions, Tests):
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
- `api/` is the interface check's baseline (JS conventions, Stability):
  api-extractor's report of each import path's compiled declarations
  (`<path>.api.md`), the `exports` map (`exports.json`) and the version they
  were taken at (`baseline.json`). `make check-api` (`scripts/api_report.js`,
  run by `make test` and the root `make verify`) compares the build with it: a
  difference warns while the version or the baseline is a pre-release and in
  a major release, and fails in any other release; `make update-api` records a
  meant change, and the baseline of each release. `@microsoft/api-extractor`
  is a pinned devDependency.
- `npm/` holds one folder per platform package; `test/` and `examples/` sit
  beside it (JS conventions, Layout and modules).
- Tests (JS conventions, Tests): `test/static` (no build),
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
  (`grpc_server.js`, `grpc_translate.js`, `grpc_admin.js`,
  `grpc_admin_translate.js`) and serves, over the public API, the
  `ProducerService`, the `AdminService` ("The admin client") and the chaos
  harness's `ChaosWorkloadService` (`verifiable_clients/chaos_workload.js`),
  with an image on port 50054; the consumer service comes with its client. The Rust harness runs
  it as `BackendKind::Js`, one backend, and its `JS_GRPC_SKIPS` list
  (`rust/Makefile`) gives each skip a reason and a removal condition (JS
  conventions, Tests).

### Packaging

The rules are the JS conventions' (Build, packaging and release). In this
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
  its README. The preview has linux-x64 (glibc) and darwin-arm64; after the
  preview the platforms are the release RFC's list (Linux with glibc on x86-64
  and arm64, macOS on arm64, Windows on x86-64), each built natively, with no
  musl package and the core built with no `target-cpu` flag, so x86-64 needs
  only Rust's baseline CPU (JS conventions, Build, packaging and release,
  Q73). Each platform package's `files` name `dist/` and
  `NOTICE`, the addon and `THIRD_PARTY_LICENSES.txt`, the licences of the
  crates linked into the addon. **(conventions conflict)** The JS conventions
  (Build, packaging and release) have every JavaScript package's `files` name
  `dist/` and `NOTICE`, but a platform package has no `dist/`; the code follows
  the JS conventions' text literally, so the entry is there and matches nothing. CI
  builds the licence file with cargo-about (`npm/about.toml`,
  `npm/about.hbs`, `.semaphore/third-party-licenses.sh`) from
  `rust/Cargo.lock`. Each package declares `"license": "Apache-2.0"` and
  ships its own `LICENSE`, which `pack-npm.sh` copies into the package's
  folder, since npm packs a `LICENSE` only from there; npm adds the README
  and that LICENSE (JS conventions, Q76). `test/static/layout.test.js`
  pins the repository's side.
- publint is among the package checks (JS conventions, Build, packaging
  and release, Q74): `make pack-check` lints each tarball it packs before
  installing it, and an error, a warning or a suggestion fails it. Its one
  suggestion, to name the module type, is why every package declares
  `"type": "commonjs"` (the platform packages the main one's,
  `check-npm.py`).
- Prettier is the formatter (JS conventions, Build, packaging and release,
  Q74): `prettier.config.cjs` (width 140, single quotes, trailing commas, the
  style the sources had), run by `scripts/format.js` over the TypeScript and
  JavaScript of `src/`, `test/`, `scripts/`, `verifiable_clients/`,
  `grpc-server/`, `tools/`, `examples/` and the root's config files. `make
  format-check` fails on a file Prettier would change, and the root
  `lint-javascript` runs it, so `verify` does; `make format` rewrites them. A
  generated file (its "GENERATED, DO NOT EDIT" header) is left out: its
  generator's output is what `cargo xtask check-generated` accepts.
- No package has an install script or builds at install; `native/` (with
  `binding.gyp`), `src/`, `scripts/`, `test/`, `tools/` and the gRPC
  server are never published (`check-npm.py package` checks each tarball's
  exact file list), and every `.js` and `.d.ts` in each tarball starts with
  the licence header (`check-npm.py package`, Q76).
- The released addon links the Rust core statically
  (`CONFLUENT_KAFKA_LINK=static`, `native/binding.gyp`), needs glibc 2.28 or
  macOS 11.0 at most (the glibc floor is 2.28, as for Python's wheels: JS
  conventions, Q73), loads only the system's libraries and exports only
  `napi_register_module_v1` and `node_api_module_get_api_version_v1`
  (`check-npm.py native`). Development builds, the tests and the gRPC image
  link it dynamically, the default.
- The version follows Kafka's line, now `4.4.0-beta.1`, the preview (JS
  conventions, Q75): `"version"` in `package.json`, repeated in
  `package-lock.json` and in every `npm/<platform>/package.json`, which
  `cargo xtask bump-version` writes together (`make bump-version
  JAVASCRIPT_VERSION=...`), a preview as
  `-beta.N`, as Python's is `bN`; there is no `-preview.N`. The version and
  the npm tag say `beta`, while the docs and the warning call the stage the
  preview. It refuses a version whose `MAJOR.MINOR` is not
  the Kafka release of `kafka/gradle.properties`, or whose `PATCH` does not
  follow `rfc/versioning.md` (`AK_patch × 100 + core_patch × 10 +
  binding_patch`, the hundreds Kafka's patch, a digit back at 0 when one to
  its left moves up). The tag build of `javascript/v<version>` records the
  tested tarballs, and its manual "Publish to npm" promotion
  (`.semaphore/publish-npm.yml`, `.semaphore/publish-npm.sh`) publishes them
  from Semaphore with a granular npm token from Vault and no provenance. The
  dist-tag is `check-npm.py dist-tag`'s: pre-releases go under `beta` only,
  and it refuses to put one on `latest`; a release goes under `latest`; 1.x
  fixes go out under `legacy`, from the earlier client's repository (the
  publish job refuses a version below 4). There is no manual publish script.
  `javascript/` is mirrored read-only to `confluent-kafka-javascript` and
  released from `<major>.<minor>.x` branches.
- CI runs the binding on Node 22 (every suite), and on Node 24 and Node 26
  (the static, unit and typing tests, and the broker-backed tests against the
  scratch broker `test-integration` starts), as the JS conventions ask (CI
  on Node 22, 24 and 26, Q73). The gRPC arms and the SSL and SASL_SSL tests
  run on Node 22.

### Generated files

`cargo xtask generate-error-codes` generates, from the Java sources and the
FFI enum (the Python client's generators, `rust/xtask/src/javascript_gen.rs`):
the error classes (their payload constructors and getters included) and
`src/internal/error_registry.ts`, `native/error_payload.c` (the payload of an
error the core reports, read through the FFI's view of its class, from the
emitter's table, which the C compiler checks against the header), the config classes'
constants (`ProducerConfig`, the client config class, its constants only;
`SslConfigs`, `SaslConfigs`, `SecurityConfig`, the key-name classes,
translated in full, Java's public default constructor included, but the
`addClient...Support(ConfigDef)` methods: JS conventions, Scope, Q55)
and the producer's configuration keys (the `ProducerConfigs` type,
`src/producer/producer_configs.ts`, and the key list
`src/internal/producer_config_keys.ts`), all TypeScript but
`native/error_payload.c` (typed: a config constant's type is written out, `static
readonly X: string = '...'`, a `long` one a `bigint`), which
`make build` compiles with the rest. *(deviation: the JS conventions
(Implementation over the FFI, Build, packaging and release, Class family)
generate more, which is hand-written here: the addon's C wrappers
(`native/`) and their TypeScript types (`src/internal/native.ts`, the JS conventions'
generated `native_types.ts`), the class modules and their form tables (in the
modules that call `src/internal/args.ts`), and the data-class list, kept in
the linter (`DATA_CLASSES`, `scripts/lint/java_ts.js`). Why: the owner reads
the JS conventions' "Code is generated wherever it can be" as meant for the error
codes, so no generator is planned for the rest
(`javascript/design/current/producer/DECISIONS.md`, entry 32).)* A generated file carries the license and `GENERATED,
DO NOT EDIT` naming its generator, and is never edited by hand;
`cargo xtask check-generated` fails on a stale, missing or orphaned one (a
`.ts`, or a generated `.js` left in `src/`, or a generated `.c` or `.h` left
in `native/`). A folder holding only generated
modules has a generated `index.ts`, whose `@packageDocumentation` lists what
its classes leave out (their header comments' notes); in a hand-written one
(`src/`, `common/`, `common/config/`, `common/metrics/`, `producer/`) the
generator owns the `BEGIN GENERATED EXPORTS` / `END GENERATED EXPORTS` block.

### The rule linter

`make lint-rules` checks the rules of this file and of the JS conventions
from the sources, as `cargo xtask lint-custom` checks the Rust crate's
(`javascript/design/current/rule-linter/PLAN.md`). It has two halves: ESLint
(`eslint.config.cjs`, the `kafka/` rules, with `eslint-baseline.txt`) runs the
rules that read TypeScript, beside typescript-eslint's recommended
type-checked rules on `src/` (`no-floating-promises` among them) and the docs
lint, `eslint-plugin-jsdoc`'s `require-param`, `require-returns` and
`require-hyphen-before-param-description` on `src/` (JS conventions,
Documentation, Q64, Q74). It reads each doc comment on a function, a method,
an overload signature of either or of a constructor, an abstract method and
an interface method (`scripts/lint/eslint/config.js`, the plugin's default
contexts plus `TSEmptyBodyFunctionExpression` and `TSMethodSignature`), and
asks for a `@param` for each parameter, an options object as one parameter
(`checkDestructured` off), in TSDoc's form `@param name - text`, and a
`@returns` unless the declaration returns no value (`void`, `undefined` or
`never`, alone or in a `Promise`; a union of them, such as
`void | Promise<void>`, is not exempted yet) or is an interface's setter (a
bodied function's only when its body returns a value). A doc comment of one
`@remarks` block alone asks for no tag: it is the remark of a class member
that implements an interface member, whose interface carries the tags
("Comments", point 1), and `check-implementation-docs` allows it nowhere
else. Not checked: a member with no
doc comment (which members need one is not the lint's), and function types
(a type alias or a property). A generated file gets its tags from its
generator. `test/lint/docs_lint.test.js` pins the configuration; the runner
(`scripts/lint_rules.js`, with `lint-baseline.txt`) runs the addon's C rules
and the other files. `make test-lint` runs the linter's own tests
(`test/lint/`): each rule's tests over a fixture, and a mutation test that
injects one known violation per rule into a copy of the real tree and expects
exactly that rule to fail.

A baseline lists findings not burnt down yet, each with a comment saying why;
a line that matches no finding any more is itself a finding. Code that does
not follow a rule yet is a finding to fix, never a new baseline line. The
mapping lists are in `lint-mapping.json`: `invented`, the list of invented
names for the exported items and public members (JS conventions, Layout and
modules), a member named by a well-known symbol keyed
`<Class>.[Symbol.<name>]` (`KafkaProducer.[Symbol.asyncDispose]`,
`Headers.[Symbol.iterator]`), each with its reason, `javaTargets`, the Java class or member of
an item the naming convention does not find (`{ java, reason }` for a class
it places outside its Java package's folder, with that deviation's reason),
and `nativeCallers`, the classes,
exports and modules outside the wrappers the JS conventions name that call
the addon, each with the reason the rules give (`check-native-wrappers`; a
module's top-level code keyed `src/<module>.ts`); the other invented names are
`NODE_DEBUG=confluent_kafka` and the platform package names (the
never-closed warning and its code are gone: "Threads and liveness").

The rules, each named in its finding:

- `check-no-todo`: no unfinished-work marker in any file of the client. The
  design documents under `design/` are not read.
- `check-exception-word`: no identifier contains the word "exception" (root
  §2, §4), Java's member names the rules keep aside.
- `check-java-line`: no comment cites a Java source line ("Comments", point
  4), in the forms of the Rust lint's own check
  (`.claude/rules/documentation.md` §7). The rules on comments read every
  file of the directories of point 7 whose comments the linter can read
  (`scripts/lint/comment_sources.js`): a TypeScript or JavaScript file through
  the TypeScript parser, a C source or header, a Java source, a linker map or
  a JSON file through the C tokenizer, and a gyp file's `#` comments, never a
  string or a document such as a README.
  `check-java-line` also reads `examples/`, which point 7 leaves out as user
  code. Of the rules, only the file-wide ones read the examples
  (`check-no-todo`, `check-java-line`, `check-directives`): the rules that
  check a translation of Java's API (`check-java-name`,
  `check-overload-shape`, `check-type-mapping`, ...) read `src/` only, as an
  example translates nothing. What checks the examples' calls is
  typescript-eslint's recommended type-checked rules, which the ESLint half
  runs on `examples/` too (through `examples/tsconfig.eslint.json`, which
  maps the package's import paths to `src/`, so no build is needed;
  `no-require-imports` and `restrict-template-expressions` are off there, as
  a CommonJS example `require`s the package and prints values through Java's
  `toString()`; `no-base-to-string` stays on, with one explained disable in
  `examples/admin/common.js`, where an error's cause may be any value), and `make typecheck-examples`
  (`scripts/typecheck_tests.js examples`, `examples/tsconfig.json`): `tsc
  --strict` with `checkJs` over every example against `dist/`'s
  declarations, as an app's code types against them, in `make test` and the
  root `test-javascript-unit`.
- `check-source-line`: no comment cites a line of a Python, C#, Rust, Scala
  or C++ source (point 4): a file name of those languages followed by a colon
  and a line or range, by a range, or by the word `line` and a line or range,
  or the word `line` and a number, or `at` and a range, next to such a file
  name or a language's name. A single number after a file name and a space is
  read as a count, so it is not checked.
- `check-no-tracking-ids`: no comment holds a phase, review or question ID
  (point 5): `Q` and a number; one capital, a number and an optional
  lowercase letter (`A8`, `P8a`), as a whole word, but `V8` and the C
  standards (`C11`, ...); `Critic` or `Actor` and a
  number, and a `COMMENTS.<n>` file; and a test plan's row label, two to five
  capitals or digits, a dash and two digits, but `UTF-16`, `UTF-32` and
  `CRC-32`. An ID inside an identifier, a hex number, a colour or a code
  point is none.
- `check-summaries`: every class, class expression and interface in the
  directories of point 7, exported or not, has a TSDoc (Javadoc, in the Java
  tool) summary, the text of its doc comment before the first tag (point 2),
  and every file of `native/` opens, after its licence header, with a
  comment (not checked: that the comment says what the file does).
- `check-implementation-docs`: a member of a class of `src/` that implements
  a member of an interface of `src/` the class `implements` (found through
  the type checker, an inherited member included) carries no doc comment, or
  one holding a single `@remarks` block and nothing else (point 1); a doc
  comment of one `@remarks` alone sits nowhere else, since the docs lint asks
  no tags of it. A member implementing an interface outside `src/`, such as
  TypeScript's `Iterable`, keeps its own TSDoc, and so do the keys of an
  options object in a member's signature, which document the options type.
  Not checked: that the interface's TSDoc is the one TypeDoc shows (TypeDoc
  shows only the remark on a signature that carries one), and that a public
  member no interface declares has TSDoc (not the lint's, as above).
- `check-internal-members`: a library-only member is a public `@internal`
  member named `internal...`, called with dot access, but `Headers`'
  `setReadOnly()` and `isReadOnly()`, which the JS conventions name by Java's
  names (Serialization; the rule's `JAVA_NAMED`, each entry an `@internal`
  member of its class); no member is `_`-prefixed, but `_ffiId` on an error
  class, which `KafkaError` declares.
- `check-java-name`, `check-java-members`, `check-java-completeness`,
  `check-public-audience`, `check-no-deprecated-translation`: every export
  translates a public, live Java class by name (or is listed), in its file
  (a TypeScript `enum` is `check-java-kind`'s),
  every public member a Java member (the members of an interface's runtime
  object, `Admin.create`, its static methods), and every public Java member is
  translated (`check-public-audience` takes Java's internal `RecordHeader`
  as public, the class the JS conventions have an app build a header with,
  Serialization, the rule's `PUBLIC_BY_RULE`) (an enum's `java.lang.Enum` members and `toJSON()` included, a
  public mutable field as its setter `set<Field>`, plan §5 item 2). A nested
  class, a class, interface or type alias of the namespace merged with an
  export that Java's nested class of its name translates (`Outer.Inner` as
  `Outer$Inner`, `scripts/lint/java_ts.js` `mappedItems`), is read as an
  export is, its members included; its file and name are its outer class's.
- `check-java-kind`: each export translating a Java type (the mapping of
  `check-java-name`, `javaTargets` included) takes the form of its Java kind
  (JS conventions, Class family, Q12, Q16, Q28): a Java interface is a
  TypeScript `interface`, beside which a `const` of its name holds its
  statics (`Admin.create`) when the Java interface has public static
  methods, and only then; a class, record or enum is a class, abstract
  exactly when Java's is, a Java abstract class's constructor throwing
  `TypeError` when `new.target` is the class itself (`KafkaFuture`'s) and its
  translations of Java's abstract methods `abstract` (the Java index's
  `abstract`); no export is a TypeScript `enum`. The rule's table holds the
  forms the JS conventions give otherwise: `Headers` is a class
  (Serialization: it joins Java's interface with its class `RecordHeaders`),
  `Callback` a function type ("a callback is a function"),
  and a type not translated the placeholder `type X = unknown` (Scope). Not
  checked: an export whose Java class the index does not hold (the root's
  built-in errors), and an abstract Java method with a concrete overload. A
  nested class is checked as an export is (`KafkaFuture.BaseFunction` and
  `BiConsumer`, function types, are in the table with `Callback`).
- `check-java-supertypes`: each exported class or interface translating a
  Java type has as its direct supertypes (a class's `extends` and
  `implements`, an interface's `extends`) the translations of its Java type's
  direct supertypes, and no other, an invented base included (JS conventions,
  Class family and Errors, Q29, Q32): a translated type by its export, a
  class in `extends` and an interface in `implements`; a JDK type by the JS
  conventions' JDK table, `Closeable` and `AutoCloseable` as `close()`
  returning a Promise plus `[Symbol.asyncDispose]` (a serde's as its optional
  `close()`, Serialization), `Iterable` as `[Symbol.iterator]()` (TypeScript's
  `Iterable<T>` allowed in `implements`), `Comparable` as `compareTo()`,
  `Future`, `Serializable`, `Principal` and `Object` as nothing,
  `RuntimeException`, `Exception` and `Throwable` as `Error`, a Java built-in
  error as the root's class and `NullPointerException` as `TypeError` (Types),
  a JDK type missing from the table being a finding; a Java supertype of the
  index the client does not translate (`AbstractConfig`, an internal one,
  `AdminClient`) as its own supertypes, recursively, so `KafkaAdminClient`
  implements `Admin` through Java's `AdminClient`.
  Not checked: type arguments, an export whose form `check-java-kind`
  refuses, and the root's built-in errors, whose Java classes the index does
  not hold.
- `check-doc-size`: a TSDoc has at most 150% of its Javadoc's words (JS
  conventions, Documentation, Q64), counted as `.claude/rules/documentation.md`
  §5 counts a Rust doc's: the Java words are the Java index's (`cargo xtask
  java-index` counts each Javadoc with `check-api-docs`' translator and
  counter, one that only points elsewhere replaced by the one it points to,
  `{@inheritDoc}` filled in); the TSDoc counts its description, `@remarks`,
  `@see` and `@example`, a `@param` Java has (the options object's when Java
  has any), a `@typeParam` Java has as `@param <T>`, `@returns` when Java
  has `@return` and a `@throws` naming, anywhere in it, the error of a Java
  `@throws` (as the Rust lint's `names_throw`), never `@defaultValue`,
  `@deprecated` or a modifier tag. It reads the TSDoc of each export, nested
  type (a class, interface or type alias of the namespace merged with an
  export, against Java's nested class of its name, recursively), public
  member (an interface's runtime object's against Java's statics) and
  overload signature mapping to a Java member with a Javadoc: a signature
  against the Java form the overload-shape mapping finds for it, else (an
  error class's constructors always) against every form of the set, passing
  when it fits one; a setter against its field's Javadoc; a function type
  translating a one-method interface (`Callback`, `KafkaFuture.BaseFunction`)
  against the interface's Javadoc and its method's together. A generated
  file passes through its generator. Not checked: a member with no Java
  counterpart (an invented one, an `...Args` key, an options object's keys)
  or no Javadoc, a nested type that no Java nested class of its name
  translates, and the members of an interface's runtime object that is not
  an object literal.
- `check-native-wrappers`: only the classes the JS conventions name call the
  addon (Implementation over the FFI, Q31, Q57, Q62): each exported class of
  that set (`KafkaProducer`, `KafkaConsumer`, the admin client, every
  `Mock...`; `Headers` and `RecordHeader` are plain TypeScript, and a
  consumed record's `Headers`, the one call outside the clients, comes with
  the consumer) holds a native handle, its construction (its constructor, its field
  initializers and, through `extends`, a base class's) or a static method
  that constructs it (`internalCreate`, for a private constructor) reaching
  a use of the addon, as the mocks wrap the FFI's mocks: `MockProducer` and
  `MockAdminClient` today; any other code of a `src/` module outside
  `src/internal/` that reaches one is listed in `lint-mapping.json`'s
  `nativeCallers` with the reason the rules give, and every entry is needed:
  a class, exported or not (its members, field initializers and
  construction, a base class's included), an exported function, an exported
  const by its initializer (an arrow function, an object of methods such as
  an interface's statics), and the module's top-level code with any use in
  it that none of those reaches (a function nothing calls or names), keyed
  by the module; the record's future, `KafkaFutureImpl`, over its native
  future (Futures and waiting), today. A use is `native.<fn>`,
  `native['<fn>']` or `const { <fn> } = native`, and counts when the code
  reaches it directly or through the `src/internal/` functions and its
  module's unexported functions it calls or hands on as a callback
  (`scripts/lint/native_uses.js`); a call into another such class, or
  another exported function or const, is that one's. Not checked: what a use
  does (that it creates the FFI's object, that every method goes through
  it), an access by a computed name (`native[name]`), and a destructuring of
  the handle in a parameter or an assignment.
- `check-invented-names`: every entry of `lint-mapping.json` is needed and
  resolves, and an invented options type is named `...Args`. A `javaTargets`
  entry that places a class outside the folder of its Java package
  (`RecordHeader`) is `{ java, reason }`, its reason the deviation's
  (`java-no-reason` without it); every other entry is the Java name alone
  (`java-reason-unneeded`).
- `check-module-exports`: each package has one import path; an index
  re-exports only its own folder's modules, with no namespace, and the root
  exports only Java's built-in errors and `Duration`.
- `check-data-objects`: the data classes' getters are `readonly` fields, every
  other class keeps methods.
- `check-overload-shape`: one Java overload set is one method taking one
  options object of Java's parameter names, with one TypeScript overload
  signature per Java form, the signatures of its null combinations (a `null`
  key or value, above) and the bare value of a one-parameter form; a nested
  class's sets too (`mappedItems`), and a member two classes inherit from
  one Java class (`java.lang.Enum#toString`) translated once per class.
- `check-type-mapping`: each member's type is the one the Types table gives
  (a `byte[]` taken as a `Uint8Array`, returned as a `Buffer`; a `long`
  returned as a `bigint`, taken as a `bigint` or a whole `number`; where Java
  takes `Headers`, `Iterable<Header>` or `Header[]`, an element is a `Header`
  or a `readonly [string, Uint8Array | null]` pair, the pair included; a bare
  `Header`, as `Headers.add(header)` takes it, is a `RecordHeader`, the rule's
  `BARE_CLASSES`, "TypeScript first"), a nested
  class's members' too, a nested class named by its own name
  (`DeserializationExceptionOrigin`); a Java type the rule's table lacks is
  `unmapped`.
- `check-waiting-call-promise`: a method returns a Promise if and only if its
  native path reaches a `_cb` twin.
- `check-native-registry`: the addon's registrations and the native handle's
  uses match, and only the loader names a `.node` file.
- `check-out-err`, `check-cb-twin-only`, `check-notify-hook`,
  `check-single-drainer`, `check-completion-order`, `check-ownership`,
  `check-no-binding-queue`: boundary rules 1 to 10, in the addon.
- `check-zero-copy`: no copy of the app's key and value on the send path; a
  `napi_ref` on each borrowed Buffer, dropped unconditionally by the
  hand-over completion and never by the delivery callback (rule 8); and in
  `src/` no copy of a bytes-typed value through `Buffer.from`,
  `Buffer.copyBytesFrom`, `Buffer.concat`, `new Uint8Array`,
  `Uint8Array.from`, `.slice()`, `.copy()` or `.set(bytes)` (a copy by
  another means, such as a loop over the bytes, is not checked).
- `check-string-encoding`: `encodeLikeJava` in `src/internal/encode.ts` is the
  only place a string becomes bytes (no `Buffer.from(string)`, `TextEncoder`
  or string write elsewhere).
- `check-no-closed-flag`: no property of a `src/` class and no member or
  variable of `native/` is named for a closed or closing state (`closed`,
  `closing`, `isClosed`, with `#`, `_` or `is_` prefixes), and no string
  literal holds one of Java's closed-client messages ("Cannot perform
  operation after producer has been closed", ...), which only the core
  reports; a `+` chain of strings, adjacent C literals and a template's
  pieces read as one literal (`scripts/lint/literals.js`) (JS conventions,
  Implementation over the FFI, Q61; boundary rules 1 and 10). Not checked:
  a closed state under another name, or in a module-level or closure
  variable of `src/`.
- `check-no-repeated-precondition`: no string literal of `src/` or
  `native/`, read as `check-no-closed-flag` reads them, holds the message of a precondition the FFI or the core checks
  ("The timeout cannot be negative.", "Invalid negative timeout", "Timeout
  must not be negative", the busy message, ...), so the binding cannot raise
  it itself (JS conventions, Errors, Q35, Q58, Q60). The binding's own
  listed checks write their own messages.
- `check-tsfn-lifetime`: `napi_ref_threadsafe_function` and
  `napi_unref_threadsafe_function` appear in `native/` only in the hold
  update, `cbq_hold_update`, which calls both; `cbq_closed`, where the open
  half of the hold ends, is called only by the close completion,
  `close_on_complete`; every `kafka_*_cb(` call is a whole statement right
  after a `cbq_call_begin` that is a whole statement too, in the same block
  (so under the same condition), one each, and a `cbq_call_begin` anywhere
  else is a finding; a `cbq_call_end` appears only in a completion passed to
  a `_cb` call (the argument of the header parameter typed `..._cb_t`), and
  no function calling it is called directly (not from the delivery callback,
  not through a helper); and each such completion calls `cbq_call_end` once,
  as a statement of its body's own block, with no `return` in the body and
  nothing after it but the release of the client struct
  (`producer_release`, `admin_release`). So a client keeps Node running
  while it is open or a call made on it waits for its answer (JS
  conventions, Lifetime, Q21; "Threads and liveness"). Checked by function,
  lexically, as the other `native/` rules read the C sources: not that a
  begin and its call name the same client's queue, that the core fires each
  completion once (the FFI's contract), nor what the hold update decides,
  which the unit tests check.
- `check-default-helpers`: each interface the user implements (an
  exported interface or function type translating a Java interface that no
  exported class implements: `Callback`, not `Producer`) whose Java
  interface has a `default` method that the TypeScript interface keeps as an
  optional member (the Java index's flag, and `name?`: a default the JS conventions
  drop or merge, such as a serde's `configure` or second `serialize`,
  needs none) has its helper class, holding Java's default bodies (JS
  conventions, Class family, Q28). The JS conventions name their example, `ConsumerRebalanceListenerDefaults`,
  and leaves the scheme open; this binding fixes it: the class
  `<Interface>Defaults`, in `src/internal/<interface>_defaults.ts`, with a
  method of each default method's name. A `...Defaults` class nothing asks
  for is a finding too. `Callback` has no default method, so no helper
  exists yet.
- `check-encode-callers`: only the string serializer (`src/common/serialization/`),
  the headers code (`src/common/header/`), the config code
  (`src/internal/config.ts`) and the SCRAM upsertion
  (`src/admin/user_scram_credential_upsertion.ts`, Java's
  `getBytes(UTF_8)` of its password and salt) use `encodeLikeJava` (JS
  conventions, Types, Q24, which lists the first three: the fourth is an
  open question for the rules' owner, `DECISIONS.md` entry 52); a string key
  or value given without a serializer is a `TypeError` (Class family, Q27),
  never encoded.
- `check-commonjs-pin`: `package.json` has no `"type": "module"` (none, or `"commonjs"`), `tsconfig.json`'s
  `module` and `moduleResolution` are `nodenext`, and each `exports` entry
  has exactly the conditions `types` then `default` (JS conventions,
  Layout and modules, Q13).
- `check-directives`, `check-config`: no inline ESLint configuration, a
  targeted and explained disable that names no `kafka/` rule, and one ESLint
  configuration.

Of "Comments", points 3, 6, 7 and 8 are not checked: whether a comment
records a decision that needs it (point 3) and which commit of a phase cleans
its area (point 8) are a reviewer's to judge, a licence header (point 6) is
left as it is, and point 7 sets where the rules on comments read.

### How the client is used, in tests and reviews

- **Callers await what the client returns.** Every method that returns a
  Promise is meant to be awaited: `await producer.send(record)`,
  `await producer.flush()`, `await producer.close()`. The client is built and
  documented for that use: zero copy holds "until the Promise from `send()`
  settles; awaiting `send()` is enough", and a send is part of a transaction
  once its `send()` Promise has resolved (JS conventions, Serialization and
  Futures and waiting). The binding's own code is held to it by typescript-eslint's
  `no-floating-promises`, which lints `src/`. Tests, examples and reviews use
  the client the same way.
- **A misuse without `await` is not a bug.** A test or a finding that depends
  on a caller who doesn't await is not a defect of the client. That covers a
  caller who drops a call's Promise, who changes, transfers or reuses a call's
  arguments before its Promise settles, or who relies on the order of calls
  that aren't awaited one by one. A Critic doesn't report one and an Actor
  doesn't write a test for one. If the docs don't already tell users to
  await at that point, it is a documentation item, not a code change.
- **What the JS conventions promise without `await` is still tested,**
  because it's part of the contract:
  - a send's delivery future need not be awaited: `await send()` is
    expected and `await future.get()` isn't, and a failed future that nobody
    awaits leaves no unhandled rejection (JS conventions, Futures and
    waiting; Tests);
  - several calls in flight at once and awaited together, as in
    `await Promise.all(sends)`, work on the producer, with no order promised
    between them (JS conventions, Threads and callbacks; rule 7);
  - a callback that returns a Promise is awaited by the binding (JS
    conventions, Threads and callbacks).

### Comments

1. TSDoc goes only on the public API, as the JS conventions' Documentation
   group sets it out: translated from the Javadoc, with the tags the docs lint
   requires. It sits on the interface that declares a member. A class member
   that implements an interface member carries no TSDoc of its own, because
   TypeDoc shows the interface's. The exception is one `@remarks` for what
   that class does differently, such as `MockProducer`'s completion. A public
   member that no interface declares gets its own TSDoc, which the docs lint
   checks.
2. Every class and interface, internal ones included, gets one concise
   summary of what it does, and so does every file in `native/`, at its top.
3. Every other comment goes. The exceptions are a concise comment for a
   recorded or very important decision or deviation, where it is needed, and
   the *(deviation)* and *(js-only)* marks in a `@remarks`. A comment states
   the decision, not its history.
4. No references to Java, Python, .NET or Rust code with line numbers: line
   numbers change and cause needless edits. A reference may name the class,
   member or file.
5. No phase, review or question IDs in comments, such as `A8`, `Critic 140`
   or `Q21`. The design notes and the rules hold them.
6. Licence headers stay as they are.
7. The rule covers `src/`, `native/`, the tests, `verifiable_clients/`,
   `grpc-server/`, `scripts/` and `tools/`. It doesn't cover rule files or
   design notes.
8. Each phase cleans its own area in one commit at the end of the phase: its
   comments and TSDoc by this rule, and the user docs it changed
   (`README.md`, `MIGRATION.md`, `CHANGELOG.md`). Commits before that one
   leave both alone.

### Files and process

- Every file carries Python's header, Confluent's copyright line with the
  Apache 2.0 text, as the core's files do (JS conventions, Build, packaging
  and release, Q76), and code translated from OpenJDK keeps GPL with the
  Classpath Exception. The Foundation's header, for a file copied unchanged
  from Apache Kafka, has no place in `src/`, since Kafka ships no
  TypeScript, so the build refuses it. `make build-ts` puts the header first
  in each compiled `.js` and `.d.ts` (`scripts/dist_headers.js`), and the
  package check confirms it in each tarball. No TODO or FIXME.
- No Rust or C changes for the binding: a missing FFI function is reported,
  not added (JS conventions, Implementation over the FFI).
- `test/static/header_conformance.test.js` asserts that every `kafka_*`
  function `native/*.c` calls is declared in the bindings header and is not a
  plain name that has an `_err` twin. Keep it passing when adding calls.
- The tests run on Jest (JS conventions, Tests, Q66; `jest.config.cjs`),
  one project per Makefile target (`static`, `lint`, `unit`, `typing`,
  `verifiable-clients`, `integration`; `make test-unit` runs
  `jest --selectProjects unit`; `JEST_ARGS` passes more, before the project,
  so `make test-unit JEST_ARGS=<test file>` runs only that file). They run in plain
  Node.js, not in Jest's vm contexts: the runner, `test/jest_runner.mjs`,
  runs jest-light-runner's test worker in a fresh child process per file, as
  `node --test` did. A vm context would hand the tests another realm than the
  addon's (its arrays and `TypeError`s fail `deepStrictEqual` and
  `instanceof`), its own module resolver (no exports-map refusal of a deep
  import, no ES module `import()`, no second copy of the package) and no
  `require` of an ES module, which the linter's tests need; and a process
  reused for the next file keeps what a file changed in it (a second copy's
  error classes in the addon). The tests use `test()` and `node:assert/strict`;
  a timeout is the last argument of `test`, `it`, `test.each`'s test,
  `test.only`, `fit` and the hooks, which `test/jest_setup.js` applies (the
  light runner registers it but does not enforce it), and refuses for
  `test.concurrent`; `test.failing` is refused whatever it is given, as the
  light runner runs it as a plain test; a `done` test or hook ends when it
  calls `done`, and one that also returns something fails with Jest's
  message, with a timeout or not (`test/static/jest_setup.test.js` runs each
  form through Jest and checks its outcome); a conditional skip
  is `(SKIP ? test.skip : test)(...)` with the reason printed once by the
  file, and cleanup is `afterEach`. ts-jest is not used: the light runner
  transforms nothing, and the tests are JavaScript. The integration tests run
  in band (`--runInBand`), one file at a time, and are skipped, with a
  visible message, unless `KAFKA_BOOTSTRAP_SERVERS` is set.
- `tsc --strict` runs over the tests (JS conventions, Tests, Q67, and
  Build, packaging and release, Q74): `make typecheck-tests`
  (`scripts/typecheck_tests.js`, in `make test` and the root
  `test-javascript-unit`, so in `verify`: the sub-make that builds `dist/`,
  so no second build deletes it under `make -j`) checks every test file,
  under `test/` and `verifiable_clients/test/`, of every JavaScript and
  TypeScript extension, with `test/tsconfig.json` (whose `include` the script
  checks against its list; a `.d.ts` there is refused, as `skipLibCheck`
  skips it), the JavaScript tests through `checkJs` under the library's own
  settings, the package's import paths typed by `dist/`'s declarations (after
  `build-ts`), as a user's code is. It fails on any finding in a test file;
  the files the tests require from elsewhere (`scripts/`, `grpc-server/`, the
  verifiable clients, the undeclared `dist/internal/*.js`) are read but not
  failed on, by a path filter in the script, which checks every test file
  with no opt-in comment and edits no file that is not a test. A call into
  `dist/internal/` is typed by inference over the compiled JavaScript, except
  where a test takes the signature from `src/` (`typeof import(...)` of the
  `.ts` source). ts-jest is not used (the light runner transforms
  nothing, and the tests stay JavaScript), so the tests are typed with JSDoc:
  precise types, never `any` (`unknown` and narrowing where a value's type is
  not known), a nullable value narrowed by an assertion the test makes
  (`assert.ok(x)`), not a cast. A deliberate misuse a test makes to reach a
  run-time check (a wrong type, a `null` the types forbid, an unknown key, a
  string without a serializer) carries `// @ts-expect-error <the run-time
  check it covers>` on the line above it: the directive also proves `tsc`
  catches the misuse for a TypeScript caller, the other half of the
  TypeScript-first rule, while the test proves the run time catches it for a
  plain JavaScript caller. A directive marks only a misuse `tsc` catches,
  and its reason says which kind: a wrong argument, a `null` the types forbid
  by the nullability rule (Java only stores it, so the type is `T` while the
  run time stores the `null` as Java does), or a class instance given bare
  (below). A valid call is never marked: one the published types reject is a
  type to fix in `src/`; one that reaches a library-only member (`internal...`,
  `_ffiId`) goes through `test/_internal.js`, the one typed view of them; a
  table of valid forms whose union `tsc` picks no overload for calls the
  implementation's parameter through a cast. A misuse `tsc` lets through gets
  no directive: number ranges, `NaN` and integer checks, a key computed at run
  time, a parameter typed `unknown` (the placeholders, `V` of a map built
  empty) and an `any[]` such as `fill(null)`'s are limits of `tsc`, recorded
  in `DECISIONS.md`. `@ts-ignore`, `@ts-nocheck` and an `@ts-expect-error`
  without a reason are refused.
- What `tsc` catches against the run time (TypeScript first): a bare value
  that may be any non-plain object (`KafkaFuture.completedFuture(value)`,
  `getNow(valueIfAbsent)`) is typed to take only built-in values bare, since
  TypeScript cannot tell a class instance from a plain object and the run
  time reads a plain object as the options object, so a TypeScript caller
  passes an instance of its own class by name, `{ value }`, while plain
  JavaScript may pass it bare. So does a bare `Header`: `Headers.add(header)`
  takes only a `RecordHeader` bare, a class `tsc` tells from a plain object
  by its private fields, and any other `Header` by name, `add({ header })`.
- Assert the core's exact error messages in tests.
