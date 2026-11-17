# Rust-Based Kafka Clients

> [!WARNING]
> **Rust client: Generally Available. Python client: Preview.** The Python client is not yet recommended for production use, and its public API is not stable before its GA release and may change. We welcome your feedback while the design is still open, see [RFCs and feedback](#rfcs-and-feedback).

A high-fidelity, architecture-preserving Rust translation of the Apache Kafka Java client (client only), generated instruction-by-instruction from the original Java sources. This project aims to provide a native Rust Kafka client with the same API, structure, and semantics as the official Java client, following Rust idioms and conventions where appropriate.

It also contains bindings for multiple languages, automatically generated following translation rules, optimized and reviewed for performance as well. The Python client, `confluent_kafka`, is described in [python/README.md](python/README.md).

## Status

**Rust Client: Generally Available**. This is the first GA release of the Rust client, version 1.0.0. We welcome your feedback, see [RFCs and feedback](#rfcs-and-feedback).

**Python Client: Preview**. The Python client is in preview and not yet recommended for production use. The public API is not stable before the GA release and may change. We welcome your feedback while the design is still open.

**C and .NET Clients: In Development**. The API is subject to large changes and the stability is not guaranteed.

Windows is not supported yet.

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

Each language has its own Makefile (`rust/Makefile`, `c/Makefile`,
`python/Makefile`, `dotnet/Makefile`), run as `make -C <language> <target>`, for
example `make -C rust test`, `make -C c test`, `make -C python verify` or
`make -C dotnet test-dotnet`. Each builds what it needs first: the Rust library,
the C binding its Unity submodule, the Python binding its venv. The root holds
only what spans several languages or the whole repository (the combined builds
and gates, submodules, git hooks, the version bump, the test selection).

To work on one language directly, see its README:
[rust/README.md](rust/README.md) (cargo: building, formatting, linting, tests,
coverage), [python/README.md](python/README.md) and [c/README.md](c/README.md).

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
