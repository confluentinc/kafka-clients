# RFC: Client repository structure

- **Status:** Draft
- **Audience:** users and contributors of Confluent's Kafka clients

## Summary

Publish the next-generation clients from a single monorepo, `confluentinc/kafka-clients`,
that holds the Rust core and every language binding, and mirror each binding to a
dedicated, read-only per-language repository that keeps its existing name.

## Motivation

The clients share one core. Keeping the core and every binding in one repository lets a
single change cross language boundaries atomically and gives contributors one place to
work and one issue backlog. At the same time, users look for a language's client by that
language's name, so each language still needs a recognizable home with its own README,
stars, and links. This proposal reconciles those two needs.

## The monorepo

`confluentinc/kafka-clients` holds:

- the Rust core (the native Rust client),
- each language binding in its own subfolder (for example `./python`, `./dotnet`,
  `./javascript`), and
- the C/C++ libraries that replace what librdkafka provides today, produced as build
  artifacts of the Rust core.

The monorepo is the source of truth. All new-client changes, issues, and pull requests
happen here.

## Per-language mirror repositories

Each binding subfolder is published to a dedicated, read-only mirror repository that keeps
its existing name:

| Binding subfolder | Mirror repository |
| --- | --- |
| `./python` | `confluent-kafka-python` |
| `./dotnet` | `confluent-kafka-dotnet` |
| `./javascript` | `confluent-kafka-javascript` |

**Read-only** means the `main` branch of each mirror is published from the monorepo and is
not edited directly; all new-client changes are made in `confluentinc/kafka-clients` and
flow outward. The existing librdkafka-based code for each language moves to a legacy branch
on its mirror repository (for example `2.x`), which stays writable for its supported
lifetime.

We keep dedicated per-language repositories, rather than pointing users at the monorepo, so
that search results, package-registry links, stars, and existing bookmarks continue to
resolve to a familiar per-language landing page and README.

## C/C++ client

C/C++ users consume the Kafka client directly rather than as a dependency of a higher-level
wrapper. Under this proposal the Rust core produces the C/C++ libraries as build artifacts,
and the new C library carries a different name than `librdkafka`.

**Open question.** Where the C/C++ client is published is not settled. Two options:

1. Publish the C/C++ artifacts from the monorepo and deprecate the `librdkafka` repository.
2. Publish the C/C++ client from a new `confluentinc/kafka-c` repository, mirrored from the
   monorepo like the per-language repositories for the other languages.

The choice depends on whether the Rust core emits C source for the client, not only
compiled binaries:

- **Compiled artifacts only, no generated C source: prefer option 1.** A dedicated
  repository holding only build artifacts with no corresponding source diverges from how
  the language-binding mirrors work (those mirror real source subfolders), so the monorepo
  is the better home in that case.
- **Generated C source (headers and a C API layer): prefer option 2.** `confluentinc/kafka-c`
  then mirrors real generated source, exactly like the other per-language mirrors, and keeps
  the C/C++ client consistent with them.

Feedback on this trade-off, and on whether the core emits C source, is specifically
requested. See also the C/C++ API proposal in [apis/c-cpp.md](apis/c-cpp.md).

## Prior art

The monorepo-with-mirrors model has precedent:

- **Kubernetes** publishes libraries from the `staging/` directory of
  [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes) to standalone
  repositories such as [client-go](https://github.com/kubernetes/client-go), using the
  [publishing-bot](https://github.com/kubernetes/publishing-bot).
- **Symfony** splits components out of the
  [symfony/symfony](https://github.com/symfony/symfony) monorepo into per-package
  repositories such as [symfony/console](https://github.com/symfony/console).

## Open questions

- The C/C++ publishing location, above.
- The mirroring tooling (for example a git subtree split versus a publishing bot).
