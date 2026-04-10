# Lifetimes in Rust — Complete Summary

## What they are

A **compile-time label** that tracks how long borrowed data lives. No runtime cost — purely a compiler check that prevents dangling pointers.

## Why they exist

Rust has no garbage collector. When data goes out of scope, it's freed immediately. Lifetimes prevent you from holding a reference to freed memory.

## The core borrowing rules

1. **You can have EITHER** one mutable reference (`&mut`) **OR** any number of immutable references (`&`) — never both
2. **References cannot outlive** the data they point to
3. **You cannot mutate** data while it's borrowed immutably

## When you need explicit `'a`

| Situation | Need `'a`? | Why |
|---|---|---|
| Function, 1 ref in, 1 ref out | No — compiler infers (elision) | Only one possible source |
| Function, 2+ ref in, ref out | **Yes** | Compiler can't guess which input the output comes from |
| Struct with reference fields | **Yes** | Compiler can't guess how long the struct will be used |
| Function, no ref output | No | Nothing to track |

## What `'a` means in each position

```rust
struct Holder<'a> {          // "I borrow something, calling its lifespan 'a"
    data: &'a str,           // "this field's source must live for 'a"
}

impl<'a> Holder<'a> {        // "methods work with the same 'a"
    fn new(s: &'a str)       // "input is tied to the same 'a as the struct"
        -> Holder<'a>        // "output carries the same 'a"

    fn get(&self) -> &str    // return borrows from self — elided, no 'a needed
}
```

## Single vs multiple lifetimes

- **One `'a`** = all references must live equally long (e.g., `ProducerRecord` — topic, key, value all come from the same caller scope)
- **Multiple `'a, 'b`** = references have independent lifetimes (e.g., one from caller, one from a local buffer)
- This is a **design decision** the author makes — the compiler can't guess the intent

## Owned vs borrowed

| Type | Owns? | Needs lifetime? | Can outlive source? | Java analogy |
|---|---|---|---|---|
| `String` | Yes | No | Yes | `new String("...")` |
| `&'a str` | No | Yes | No | Reference to someone else's String |
| `&'static str` | No (special) | No | Yes — lives forever | String literal in constant pool |
| `Vec<u8>` | Yes | No | Yes | `new byte[]` |
| `&'a [u8]` | No | Yes | No | A view into someone else's byte[] |

## Why the compiler can't auto-assign `'a` for structs

Three possible auto strategies, all bad:

| Auto strategy | Problem |
|---|---|
| One `'a` for all fields | Too restrictive — forces unrelated references to live equally long |
| Separate `'a`, `'b`, `'c` per field | Methods become ambiguous — which lifetime does a return value have? |
| Try to infer from usage | Usage can differ across functions, modules, even crates — no single right answer |

Lifetimes are **design decisions**, not mechanical facts.

## How our producer code uses lifetimes

```
User's data (key, value bytes)
  |
  |  &'a [u8] — zero-copy borrow
  v
ProducerRecord<'a>          <-- borrows, doesn't copy
  |
  |  &[u8] — borrow for function call only
  v
accumulator.append()        <-- still no copy
  |
  |  extend_from_slice()    <-- ONE copy into batch buffer
  v
ProducerBatch.buffer: Vec<u8>  <-- OWNS the data, no lifetime needed
  |
  |  (batch sent to broker, response comes back)
  v
RecordMetadata { topic: String }  <-- OWNS its data, no lifetime needed
```

**Key insight**: Lifetimes flow downward until ownership is taken. Once data is copied into an owned type (`Vec`, `String`), lifetimes disappear — the owner is self-sufficient.

## Mutation safety (vs Java)

In Java:
```java
byte[] key = "original".getBytes();
ProducerRecord record = new ProducerRecord<>("topic", key, value);
key[0] = 'X';  // mutates! record.key() now sees "Xriginal"
```

In Rust:
```rust
let mut key_data = vec![1, 2, 3];
let record = ProducerRecord::new("topic").key(&key_data);
key_data[0] = 99;  // COMPILE ERROR: cannot mutate while borrowed
```

Rust's borrowing rules make this class of bug impossible.

## Common pitfalls to watch for

1. **Returning a reference to local data** — won't compile, the local dies at function end
2. **Storing a reference in a long-lived struct** — the source must live at least as long
3. **Holding a borrow across `.await`** — can cause issues if the borrow crosses a suspend point
4. **Over-constraining with one `'a`** — forcing unrelated references to share a lifetime when they don't need to

## The mental model

Think of `'a` as a **wristband at a concert**. The compiler checks: "does everyone wearing the same color wristband leave at the same time or later?" If someone tries to leave early while others still reference them — compilation fails.
