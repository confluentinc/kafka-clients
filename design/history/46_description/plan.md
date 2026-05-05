# Translation Design: KAFKA-12392 – Deprecate `--max-partition-memory-bytes` in ConsoleProducer

**AK commit:** `4fd1bedaba2ab9dda932457a2efa55186c15c069`
**AK branch:** trunk
**PR:** #46
**Rust branch:** `kafka-translate/4fd1bedaba2ab9dda932457a2efa55186c15c069`

---

## Summary of Java Commit

This commit implements [KIP-1231](https://cwiki.apache.org/confluence/x/xQl3Fw): the
`--max-partition-memory-bytes` CLI option in `kafka-console-producer` is deprecated in favour of
the existing `--batch-size` option.  Both options control `batch.size` in the producer config;
having two options was confusing.  The changes in the Java commit are:

1. **`ConsoleProducer.java`**
   - `maxPartitionMemoryBytesOpt` field annotated `@Deprecated(since = "4.2", forRemoval = true)`.
   - Description of `--max-partition-memory-bytes` updated to start with `(Deprecated)` and
     note removal in Kafka 5.0 with a pointer to `--batch-size`.
   - Description of `--batch-size` expanded with a fuller explanation (it previously noted that
     `--max-partition-memory-bytes` would override it; that note is now reversed: the deprecated
     option overrides the preferred one).
   - `checkArgs()`: a runtime `System.out.println` warning is printed whenever
     `--max-partition-memory-bytes` is present on the command line.
   - Stale comment about "2 options to set batch.size … KIP-717" removed from `producerProps()`.

2. **`docs/upgrade.html`** – deprecation notice added to the 4.2 notable-changes section.

3. **`checkstyle/suppressions.xml`** – `ConsoleProducer` added to the `NPathComplexity` suppression
   list (the new `if` branch in `checkArgs` raised the NPath score above the threshold).

---

## Rust Translation Scope

### Does `ConsoleProducer` exist yet?

No.  The current Rust codebase (`src/`) does not contain a `console_producer` binary or module.
The `KafkaProducer` required by `ConsoleProducer` is also not yet translated (only the internal
`producer/` skeleton exists).  Therefore **this PR creates the `console_producer` binary from
scratch**, incorporating all current Java state (including the deprecation introduced by this
commit).  There is no need for a separate "add deprecation" step because we are writing the
current-state file.

### Files to create

| Rust path | Java source |
|-----------|-------------|
| `src/bin/console_producer.rs` | `tools/src/main/java/org/apache/kafka/tools/ConsoleProducer.java` |

No documentation HTML is maintained in this Rust repo, and checkstyle is a Java-only concern,
so those two Java files have no Rust equivalents.

---

## Implementation Plan

### 1. `src/bin/console_producer.rs`

Translate `ConsoleProducer.java` (current state, post-KIP-1231) to a Tokio async binary.

**CLI argument parsing** – use the [`clap`](https://docs.rs/clap) crate (derive API) instead of
jopt-simple.  Map each Java `OptionSpec` to a `clap` field:

| Java option | Rust `clap` long | Type | Default |
|---|---|---|---|
| `--topic` | `--topic` | `String` | required |
| `--bootstrap-server` | `--bootstrap-server` | `String` | required |
| `--sync` | `--sync` | flag | false |
| `--compression-codec` | `--compression-codec` | `Option<String>` | none (→ `none`) |
| `--batch-size` | `--batch-size` | `u32` | 16384 |
| `--message-send-max-retries` | `--message-send-max-retries` | `u32` | 3 |
| `--retry-backoff-ms` | `--retry-backoff-ms` | `u64` | 100 |
| `--timeout` | `--timeout` | `u64` | 1000 |
| `--request-required-acks` | `--request-required-acks` | `String` | `-1` |
| `--request-timeout-ms` | `--request-timeout-ms` | `u32` | 1500 |
| `--metadata-expiry-ms` | `--metadata-expiry-ms` | `u64` | 300000 |
| `--max-block-ms` | `--max-block-ms` | `u64` | 60000 |
| `--max-memory-bytes` | `--max-memory-bytes` | `u64` | 33554432 |
| `--max-partition-memory-bytes` *(deprecated)* | `--max-partition-memory-bytes` | `Option<u32>` | none |
| `--line-reader` | `--line-reader` | `String` | `LineMessageReader` |
| `--socket-buffer-size` | `--socket-buffer-size` | `u32` | 102400 |
| `--reader-property` | `--reader-property` | `Vec<String>` | empty |
| `--reader-config` | `--reader-config` | `Option<String>` | none |
| `--command-property` | `--command-property` | `Vec<String>` | empty |
| `--command-config` | `--command-config` | `Option<String>` | none |
| `--property` *(deprecated)* | `--property` | `Vec<String>` | empty |
| `--producer-property` *(deprecated)* | `--producer-property` | `Vec<String>` | empty |
| `--producer.config` *(deprecated)* | `--producer-config` | `Option<String>` | none |

**Deprecation warnings** – after parsing, before connecting, print the same warnings as Java:

```rust
if args.max_partition_memory_bytes.is_some() {
    println\!("Warning: --max-partition-memory-bytes is deprecated and will be removed \
              in Apache Kafka 5.0. Use --batch-size instead.");
}
// similarly for --producer-property, --producer.config, --property
```

**`batch_size` resolution** – mirror Java's override logic: if `--max-partition-memory-bytes` is
provided it takes precedence over `--batch-size` (both map to `batch.size`):

```rust
let batch_size = args.max_partition_memory_bytes
    .unwrap_or(args.batch_size);
```

**Producer config assembly** – build a `HashMap<String, String>` (equivalent of
`ConsoleProducerOptions::producerProps()`) and pass it to `KafkaProducer::new()`.

**Main loop** – read lines from `tokio::io::stdin()` (async), construct
`ProducerRecord<Vec<u8>, Vec<u8>>`, call `producer.send(record).await`.  If `--sync` is set,
`.await` the `Future` returned by `send` before reading the next line; otherwise spawn a detached
task for the callback (error logging).

**Struct layout:**

```
ConsoleProducer          // top-level, owns producer + reader
ConsoleProducerOptions   // parsed CLI args (clap struct)
LineMessageReader        // reads stdin, yields ProducerRecord
```

### 2. `Cargo.toml` – add binary target

```toml
[[bin]]
name = "console-producer"
path = "src/bin/console_producer.rs"
```

Add `clap` with `derive` feature if not already present:

```toml
clap = { version = "4", features = ["derive"] }
```

### 3. Tests

No Java unit tests exist for `ConsoleProducer` in the upstream repo (the class is exercised only
via integration/shell tests).  Provide:

- A unit test for `batch_size` resolution (deprecated option overrides preferred option).
- A unit test for warning messages being emitted when deprecated flags are set (capture stdout).

---

## Dependencies

- `KafkaProducer` must be sufficiently implemented to accept a config map and send records.
  If `KafkaProducer` is not yet available, stub it with a `todo\!()` body and note the dependency
  in a `COMMENTS.0.md` item so a follow-up milestone can wire it up.
- `clap 4` (already commonly used in Rust CLI projects; verify it is not already in `Cargo.toml`
  before adding).

---

## Out of Scope

- `docs/upgrade.html` – no HTML docs in this repo.
- `checkstyle/suppressions.xml` – Java tooling only.
- Translation of other tools in `kafka/tools/` – out of scope for this PR.
