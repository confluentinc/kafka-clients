# RFC: Client release and distribution

- **Status:** Proposed
- **Audience:** users and contributors of Confluent's Kafka clients

## Summary

Release the clients on a cadence aligned to Apache Kafka, publish each language's client to
its standard package registry under its existing name, and ship prebuilt native binaries so
installation needs no build toolchain, no post-install scripts, and no network access beyond
the registry.

## Release cadence

Releases track Apache Kafka. A client `MAJOR.MINOR` corresponds to an Apache Kafka
binding release together at the same `MAJOR.MINOR`. Patch releases follow the three-stream patch
encoding defined in the versioning proposal, so each binding's patch number may differ.
encoding defined in the versioning proposal.

## Preview and GA

New releases ship first as a release candidate and then as GA. Preview releases are for
evaluation, published to the same registries and clearly marked as pre-release, so existing
production workloads are unaffected until a user opts in. GA releases are production-ready.

## Distribution per language

Each client is published to its language's standard registry under its existing name:

| Language | Registry | Install | Native artifact |
| --- | --- | --- | --- |
| Rust | crates.io | `cargo add <crate>` | pure-Rust crate, built by cargo |
| Python | PyPI | `pip install confluent-kafka` | prebuilt wheels per platform |
| .NET | NuGet | `dotnet add package Confluent.Kafka` | native runtime assets in the package |
| JavaScript | npm | `npm install @confluentinc/kafka-javascript` | prebuilt platform packages (`optionalDependencies`) |
| C/C++ | N/A | i.e. `apt-get install confluent-kafka-c-dev` or `./configure && make && make install` |  |

## Installation without a build toolchain

Because the core is native code, each binding ships prebuilt binaries for supported platforms
rather than compiling on the user's machine. This means:

- installation does not require a compiler or build tools;
- installation runs no post-install or build scripts, so it works under restricted policies
  (for example npm `--ignore-scripts`, or CI that forbids `node-gyp` / `node-pre-gyp`); and
- installation needs no network access beyond the package registry, so it works in airgapped
  environments.

## Platform support

All language clients will be made available for the following platforms.

| Platform | Architectures |
| --- | --- |
| Linux (glibc) | x86-64, arm64 |
| macOS | arm64 |
| Windows | x86-64 |

Users can build from source for unsupported platforms.
