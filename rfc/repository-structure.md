# RFC: GitHub repository structure

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

## Prior art

The monorepo-with-mirrors model has precedent:

- **Kubernetes** publishes libraries from the `staging/` directory of
  [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes) to standalone
  repositories such as [client-go](https://github.com/kubernetes/client-go), using the
  [publishing-bot](https://github.com/kubernetes/publishing-bot).
- **Symfony** splits components out of the
  [symfony/symfony](https://github.com/symfony/symfony) monorepo into per-package
  repositories such as [symfony/console](https://github.com/symfony/console).
