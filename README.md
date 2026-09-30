# Rust-Based Kafka Clients

> [!WARNING]
> **Preview:** The Rust client is in preview and not yet recommended for production use. The public API is not stable before the 1.0 GA release and may change. We welcome your feedback while the design is still open, see [RFCs and feedback](#rfcs-and-feedback).

A high-fidelity, architecture-preserving Rust translation of the Apache Kafka Java client (client only), generated instruction-by-instruction from the original Java sources. This project aims to provide a native Rust Kafka client with the same API, structure, and semantics as the official Java client, following Rust idioms and conventions where appropriate.

It also contains bindings for multiple languages, automatically generated following translation rules, optimized and reviewed for performance as well.
## Status

**Rust Client: Preview**. The Rust client is in preview and not yet recommended for production use. The public API is not stable before the 1.0 GA release and may change. We welcome your feedback while the design is still open.

**Python, C and .NET Clients: In Development**. The API is subject to large changes and the stability is not guaranteed.

## Repository Structure

```
rust/                   # The Rust client — see rust/README.md
python/                 # Python binding — see python/README.md
c/                      # C binding — see c/README.md
kafka/                  # Apache Kafka Java sources (submodule), the translation reference
design/                 # Design documents (design/current describes the code as it stands)
rfc/                    # Repository-level RFCs
tools/                  # Non-Rust tooling (Java perf test, translation agent, ...)
Makefile                # Cross-language targets, delegating to each language's Makefile
```

Every language builds on the Rust library: the bindings link
`rust/target/<profile>/libconfluent_kafka` and compile against the C header
`rust/target/include/confluent_kafka.h`.

## RFCs and feedback

The design of these clients is proposed through RFCs in the [`rfc/`](rfc/) folder, covering topics such as repository structure, versioning, client APIs, licensing, contributions, release and distribution, and AI-assisted migration.

We want your feedback while these proposals are still open. To comment on an RFC, open a [GitHub issue](https://github.com/confluentinc/kafka-clients/issues) or start a [GitHub discussion](https://github.com/confluentinc/kafka-clients/discussions) that references the RFC. See [rfc/README.md](rfc/README.md) for the status of each proposal and how the review process works.

## Getting Started

### Clone with submodules

```bash
git clone --recurse-submodules <repo-url>
```

Or if already cloned:

```bash
git submodule update --init
```

### Set up

```bash
make init
```

- Initializes the submodules, creates the shared Python `venv/`, and installs
  the Python development dependencies.

## Building and Testing

The root `Makefile` builds and tests every language:

```bash
make build     # the Rust library, then the C and Python bindings
make test      # every language's tests
make verify    # build, format and lint checks, and every test suite
```

Per-language targets exist too, e.g. `make build-c`, `make test-python`,
`make verify-rust`. Each of them delegates to the language's own Makefile
(`rust/Makefile`, `python/Makefile`, `c/Makefile`), which can also be run
directly, e.g. `make -C rust test`. The root only orders the builds across
languages and sets up what they share (submodules, git hooks, the Python venv).

To work on one language directly, see its README:
[rust/README.md](rust/README.md) (cargo: building, formatting, linting, tests,
coverage), [python/README.md](python/README.md) and [c/README.md](c/README.md).

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
