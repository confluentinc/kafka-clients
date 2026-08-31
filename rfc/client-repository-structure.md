# RFC: Client repository structure

- **Status:** Proposed
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

## Filing issues and pull requests

- **Issues.** New-client issues are filed in `confluentinc/kafka-clients`. The mirror
  repositories direct reporters there through notes in `CONTRIBUTING.md` and issue
  templates. Issues about the legacy client continue to be filed on the mirror
  repository's legacy branch.
- **Pull requests.** New-client pull requests go to `confluentinc/kafka-clients`. The
  mirror repositories accept pull requests only for legacy fixes. A pull request opened
  against a mirror cannot be merged into the monorepo, because one is not a fork of the
  other.

The mirrors keep issues and pull requests enabled while their legacy branch is still
supported; they can be disabled after that support window ends.

## C/C++ client

C/C++ users consume the Kafka client directly rather than as a dependency of a higher-level
wrapper. Under this proposal the Rust core produces the C/C++ libraries as build artifacts,
and the new C library carries a different name than `librdkafka`.

**Open question.** Where the C/C++ client is published is not settled. Two options:

1. Publish the C/C++ artifacts from the monorepo and deprecate the `librdkafka` repository.
2. Keep publishing from the `librdkafka` repository, treating it as the C client's home the
   way the binding repositories serve the other languages.

Option 1 risks producing a repository that holds only build artifacts with no corresponding
source, which diverges from how the language-binding mirrors work (those mirror real source
subfolders). Feedback on this trade-off is specifically requested. See also the C/C++ API
proposal in [client-apis/c-cpp.md](client-apis/c-cpp.md).

## Prior art

The monorepo-with-mirrors model has precedent, including Kubernetes (a `staging/` area
published to standalone repositories such as `client-go` via a publishing bot) and Symfony
(components split from one monorepo to per-package repositories).

## Open questions

- The C/C++ publishing location, above.
- The mirroring tooling (for example a git subtree split versus a publishing bot).
