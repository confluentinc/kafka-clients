# Lifetime Examples — From Our Conversations

## Example 1: Why bare `&` doesn't work in structs

```rust
// WON'T COMPILE — missing lifetime
struct Holder {
    data: &str,
}

// COMPILES — lifetime label tells compiler the contract
struct Holder<'a> {
    data: &'a str,
}
```

## Example 2: Compiler figures it out for simple functions (elision)

```rust
// No 'a needed — one input ref, one output ref, obvious link
fn first_word(s: &str) -> &str {
    &s[0..5]
}

// Compiler secretly rewrites as:
fn first_word<'a>(s: &'a str) -> &'a str {
    &s[0..5]
}
```

## Example 3: Two inputs — compiler gets stuck

```rust
// WON'T COMPILE — which input does the return come from?
fn pick(a: &str, b: &str) -> &str {
    if a.len() > b.len() { a } else { b }
}

// FIX — tell the compiler both share the same lifetime
fn pick<'a>(a: &'a str, b: &'a str) -> &'a str {
    if a.len() > b.len() { a } else { b }
}
```

Why it matters:
```rust
let result;
let a = String::from("hello");
{
    let b = String::from("hi");
    result = pick(&a, &b);    // if result comes from b...
}   // b is dropped here
println!("{}", result);        // ...DANGLING POINTER! Compiler prevents this.
```

## Example 4: Struct borrowing — works

```rust
fn good() {
    let name = String::from("kafka");   // name lives here -----+
    let h = Holder { data: &name };     // h borrows name       |
    println!("{}", h.data);             // use h                |
}   // both dropped — name outlived h, all good ----------------+
```

## Example 5: Struct borrowing — fails

```rust
fn bad() -> Holder<'???> {
    let name = String::from("kafka");   // name lives here -----+
    let h = Holder { data: &name };     // h borrows name       |
    return h;                           // try to return h      |
}   // name dropped — h.data would be dangling! REJECTED -------+
```

## Example 6: Mutation blocked while borrowed

```rust
let mut key_data = vec![1, 2, 3];
let record = ProducerRecord::new("topic")
    .key(&key_data);          // immutable borrow starts

key_data[0] = 99;            // COMPILE ERROR
// "cannot borrow `key_data` as mutable because it is
//  also borrowed as immutable"
```

Options:
```rust
// Option 1: Mutate BEFORE borrowing
let mut key_data = vec![1, 2, 3];
key_data[0] = 99;                    // no borrow yet
let record = ProducerRecord::new("topic").key(&key_data);

// Option 2: Mutate AFTER the borrow ends
let mut key_data = vec![1, 2, 3];
let record = ProducerRecord::new("topic").key(&key_data);
producer.send(&record).await;        // record consumed/dropped
key_data[0] = 99;                    // borrow is over

// Option 3: Clone (separate copy, like Java)
let key_data = vec![1, 2, 3];
let key_copy = key_data.clone();
let record = ProducerRecord::new("topic").key(&key_copy);
```

## Example 7: Two different lifetimes for different fields

```rust
// One 'a for both — forces both to live equally long (may be too restrictive)
struct Parser<'a> {
    input: &'a str,
    error_buf: &'a str,
}

// Two lifetimes — each can be independent
struct Parser<'input, 'err> {
    input: &'input str,       // tied to caller's data
    error_buf: &'err str,     // tied to local buffer, can be shorter
}
```

## Example 8: Our ProducerRecord — real code

```rust
// From src/clients/producer/record.rs
pub struct ProducerRecord<'a> {
    topic: &'a str,
    partition: Option<i32>,
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    timestamp: Option<i64>,
    headers: Vec<Header<'a>>,
}
```

One `'a` for all — correct design choice because topic, key, value, and headers
all come from the caller's scope and need to live until `send()` completes.

After `send()`, the accumulator copies the data into the batch buffer (`Vec<u8>`)
and the borrow ends. The caller's data is free.
