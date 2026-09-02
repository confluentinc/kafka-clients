# RFC: Client release and distribution

- **Status:** Draft
- **Audience:** users and contributors of Confluent's Kafka clients

## Summary

Release the clients on a cadence aligned to Apache Kafka, publish each language's client to
its standard package registry under its existing name, and ship prebuilt native binaries so
installation needs no build toolchain, no post-install scripts, and no network access beyond
the registry.

## Release cadence

Releases track Apache Kafka. A client `MAJOR.MINOR` corresponds to an Apache Kafka
`MAJOR.MINOR` (see [Client versioning](client-versioning.md)); the Rust core and every
binding release together at the same version. Patch releases follow the three-stream patch
encoding defined in the versioning proposal.

## Preview and GA

Each capability ships first as a preview (beta/RC) and then as GA. Preview releases are for
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
| C/C++ | TBD (see [Client repository structure](client-repository-structure.md)) | TBD | TBD |

The publishing/mirror model is covered in
[Client repository structure](client-repository-structure.md). C/C++ distribution — its
registry and location, its install path, and whether it ships prebuilt binaries only or also
C source — is TBD, pending the C/C++ publishing decision that is open in that proposal.

## Installation without a build toolchain

Because the core is native code, each binding ships prebuilt binaries for supported platforms
rather than compiling on the user's machine. This means:

- installation does not require a compiler or build tools;
- installation runs no post-install or build scripts, so it works under restricted policies
  (for example npm `--ignore-scripts`, or CI that forbids `node-gyp` / `node-pre-gyp`); and
- installation needs no network access beyond the package registry, so it works in airgapped
  environments.

## Platform support

Initial target platforms for prebuilt binaries:

| Platform | Architectures |
| --- | --- |
| Linux (glibc) | x86-64, arm64 |
| macOS | arm64 |
| Windows | x86-64 |

The exact matrix is developed on this proposal. A source build remains available where a
platform is not prebuilt.

## Coexistence of major versions

The new major version and the legacy librdkafka-based major version are published side by
side, so an application can install and migrate at its own pace (see
[Client versioning](client-versioning.md) and
[Client repository structure](client-repository-structure.md)).

## Open questions

- The exact prebuilt-platform matrix (for example musl/Alpine Linux, macOS x86-64, Windows
  arm64).
- Whether Python also ships a source distribution that builds from source, and the minimum
  toolchain if so.
