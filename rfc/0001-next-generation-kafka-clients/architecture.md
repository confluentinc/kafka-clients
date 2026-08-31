# Architecture and repository model

Part of [RFC-0001](README.md).

## The monorepo

All new-client source lives in a single repository, `confluentinc/kafka-clients`. It holds:

- the Rust core (the native Rust client), and
- each language binding in its own subfolder (for example `./python`, `./dotnet`,
  `./javascript`), and
- the C/C++ libraries that replace what librdkafka provides today, produced as build
  artifacts of the Rust core.

The monorepo is the source of truth. All 4.x changes, and all 4.x issues and pull
requests, happen here. Keeping the core and every binding together lets a single change
cross language boundaries atomically and gives contributors one place to work.

## Per-language mirror repositories

Users still find each language's client where they expect it. Each binding subfolder is
published to a dedicated, read-only mirror repository that keeps its existing name:

| Binding subfolder | Mirror repository |
| --- | --- |
| `./python` | `confluent-kafka-python` |
| `./dotnet` | `confluent-kafka-dotnet` |
| `./javascript` | `confluent-kafka-javascript` |

**Read-only** means the `main` branch of each mirror is published from the monorepo and is
not edited directly; all 4.x changes are made in `confluentinc/kafka-clients` and flow
outward. The existing librdkafka-based code for each language moves to a legacy branch on
its mirror repository (for example `2.x`), which stays writable for its supported lifetime.

We keep dedicated per-language repositories, rather than pointing users at the monorepo,
so that search, package-registry links, stars, and existing bookmarks continue to resolve
to a familiar per-language landing page and README.

## Filing issues and pull requests

- **Issues.** 4.x issues are filed in `confluentinc/kafka-clients`. The mirror
  repositories direct 4.x reporters there through notes in `CONTRIBUTING.md` and issue
  templates. Issues about the legacy client continue to be filed on the mirror
  repository's legacy branch.
- **Pull requests.** 4.x pull requests go to `confluentinc/kafka-clients`. The mirror
  repositories accept pull requests only for legacy fixes. A pull request opened against a
  mirror cannot be merged into the monorepo, because one is not a fork of the other.

The mirrors keep issues and pull requests enabled while their legacy branch is still
supported; they can be disabled after that support window ends.

## C/C++ client

C/C++ users consume the Kafka client directly rather than as a dependency of a
higher-level wrapper. Under this proposal the Rust core produces the C/C++ libraries as
build artifacts, and the new C library carries a different name than `librdkafka`. Its API
aligns with the Java client, so it is not a drop-in `librdkafka` ABI replacement; C/C++
users face the same categories of migration work as the other languages.

**Open question.** Where the C/C++ client is published is not settled. Two options are on
the table:

1. Publish the C/C++ artifacts from the monorepo and deprecate the `librdkafka`
   repository.
2. Keep publishing from the `librdkafka` repository, treating it as the C client's home
   the way the binding repositories serve the other languages.

Option 1 risks producing a repository that holds only build artifacts with no
corresponding source, which diverges from how the language-binding mirrors work (those
mirror real source subfolders). Feedback on this trade-off is specifically requested.

## Prior art

The monorepo-with-mirrors model has precedent, including Kubernetes (a `staging/` area
published to standalone repositories such as `client-go` via a publishing bot) and Symfony
(components split from one monorepo to per-package repositories). We are evaluating these
approaches for the mirror tooling.
