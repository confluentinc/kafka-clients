# Confluent Kafka JavaScript (Node.js) client — preview

A Node.js client for Apache Kafka built on the C FFI of the Confluent Kafka
Rust client, in the same way as the Python client in `../python`. It is a
Node-API addon written in plain C (`src/`), so one build works on every Node
release supporting Node-API version 8, under a thin CommonJS layer (`lib/`)
with TypeScript declarations (`index.d.ts`).

The API follows the Java client's shape (`KafkaProducer`, `ProducerRecord`,
`RecordMetadata`), with JavaScript idioms (`Promise` for `Future`). This first
slice covers the producer only; the package is private and not published.

## Requirements

- Node.js 18 or newer, with its headers (official tarballs, Homebrew and Nix
  installs ship them under `<prefix>/include/node`).
- A C compiler and Python 3 (for node-gyp). node-gyp itself is the copy npm
  bundles; no registry access is needed.
- The Rust library and its bindings header:

  ```bash
  make -C rust build-all-features   # rust/target/release/libconfluent_kafka.*
                                    # rust/target/include/confluent_kafka_bindings.h
  ```

## Build and test

```bash
make -C javascript build             # or, from the repository root: make build-javascript
make -C javascript test-unit         # no broker needed
make -C javascript test-integration  # starts a scratch broker with docker, or uses
                                     # KAFKA_BOOTSTRAP_SERVERS when it is set
```

`CONFLUENT_KAFKA_LIB_DIR` (default `../rust/target/release`) and
`CONFLUENT_KAFKA_INCLUDE_DIR` (default `../rust/target/include`) select the
library and header; the addon links the library dynamically with an rpath to
that directory.
