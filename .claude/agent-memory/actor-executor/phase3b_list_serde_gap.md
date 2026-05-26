---
name: Phase 3b ListSerializer/ListDeserializer cross-process gap
description: ListSerializer doesn't embed the inner-class FQN — Rust output won't parse in Java. Documented translation limit.
type: project
---

`org.apache.kafka.common.serialization.ListSerializer<Inner>` works by
recording `inner.getClass()` (the runtime serializer class) and
implicitly relying on the Java reader knowing how to instantiate it via
`Class.forName(className).newInstance()`. The class FQN is **not** part
of the on-the-wire payload directly — Java instead uses the
`SerializationStrategy.ConstantSize` flag plus a static map
`FIXED_LENGTH_DESERIALIZERS` that maps `Class<? extends Deserializer<?>>
→ Integer` to recover the per-element size.

In Rust:

- We have no class-FQN reflection. There is no `Class.forName`.
- Therefore the Java `configure(Map, boolean)` path that loads the
  inner serde from a config string (e.g.
  `default.list.value.serde.inner` = `"...Serdes$IntegerSerde"`) does
  not translate. The `configure` method on `ListSerializer` /
  `ListDeserializer` is a no-op — `ConfigException` cannot be raised
  here because there is no string-to-class loader to fail.
- The wire format is otherwise faithful: strategy byte + null-index
  list (CONSTANT_SIZE) or per-entry length prefix (VARIABLE_SIZE) +
  list size + entries.

## What we do support

- Round-trip via `ListSerializer::new(inner, InnerKind::FixedSize(n))` /
  `ListSerializer::new(inner, InnerKind::VariableSize)`.
- Null entries in both strategies.
- Empty lists.
- All primitive inner serdes (Integer, Short, Long, Float, Double,
  UUID) with their fixed sizes preserved (4, 2, 8, 4, 8, 36).
- `ListSerializerTest`-style verification of fixed byte counts (e.g.
  `byte_count_is_21` for 3 ints) — translated.

## What we don't support (deferred)

- The Java `ListSerializerTest` and `ListDeserializerTest` tests focus
  on the `configure(Map, boolean)` runtime-class-loading path
  (`Utils.newInstance(class_name)`, `Class.forName`, etc.). Those
  scenarios have no Rust analog — `ConfigException` can't be raised
  without the class loader. The tests have been omitted from the Rust
  translation with that justification (see `tests.rs` skip list).
- Java's `LinkedList` / `Stack` `listClass` parameter — Rust always
  returns `Vec<Option<T>>`. The two `assertInstanceOf(LinkedList.class,
  ...)` / `assertInstanceOf(Stack.class, ...)` tests are not
  applicable.
- Cross-process parsing — bytes serialized by Rust's `ListSerializer`
  will not be parsed correctly by Java's `ListDeserializer` *unless*
  Java's deserializer is configured with a matching inner-serde class
  (which is the same configuration concern as same-process Java). For
  any Rust reader, the `InnerKind` tag is what's used to recover
  per-element size, and that's preserved via the `ConstantSize` /
  `VariableSize` flag byte — same as Java.

## Why this is acceptable for Phase 3b

PLAN.md notes that List serdes are a corner case rarely used by the
producer client. Phase 3b's translation gives a faithful round-trip
for all the cases the Java `SerializationTest`'s `listSerde*` tests
exercise (round-trip, fixed-size byte counts, null entries,
non-primitive inner). The cross-process Java-class-FQN scenario is the
only translation gap, and it's documented at the top of
`list_serializer.rs` for any future agent grepping for "phase 3b list".
