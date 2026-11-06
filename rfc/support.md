# RFC: Client support

- **Status:** Proposed
- **Audience:** users and contributors of Confluent's Kafka clients

## Summary

- New features and KIPs land in the new **4.x** clients built on the Rust core.
- **librdkafka** and the derived clients built on it keep receiving security patches and
  critical bug fixes, so existing applications stay safe while users move to 4.x on their own
  schedule.

## Librdkafka and derived clients

librdkafka has powered Confluent's non-Java clients for over a decade, and a large number of
applications depend on it. Moving to 4.x includes configuration, API, behavior changes, and using a new Rust core
(see [Client APIs](apis/README.md)).

- Users who want new Kafka features get them in 4.x, close to when they ship in the Java
  client.
- Users who stay on librdkafka keep a maintained, secure client while they plan their
  migration.

Derived clients include Confluent's librdkafka, Python, .NET, JavaScript and Go clients at version 2.x (JavaScript at version 1.x).

## Release lines

| Release line | Clients | Core | Where the code lives |
| --- | --- | --- | --- |
| `4.x` | Rust, Python, .NET, JavaScript, C/C++ | Rust core | `confluentinc/kafka-clients`, mirrored to the per-language repositories (see [Repository structure](repository-structure.md)) |
| `2.x` | Python, .NET, JavaScript, Go, and librdkafka for C/C++ | librdkafka | The existing repositories. Python, and .NET keep the librdkafka-based code on a `2.x` branch. |
| `1.x` | JavaScript | librdkafka | The existing repository on a `1.x` branch. |

## New features go to 4.x

- New KIPs and features from the Java client land in 4.x, in a new `MAJOR.MINOR` release
  aligned to Apache Kafka (see [Client versioning](versioning.md)).
- Bug fixes and security patches for 4.x ship as patch releases.
- 4.x issues and pull requests are filed in `confluentinc/kafka-clients` (see
  [Community contributions](contributions.md)).

## librdkafka and derived clients get security and critical fixes

- Security patches and critical bug fixes continue to be applied to librdkafka and shipped in
  1.x or 2.x maintenance releases of each client.
- New features are not added to librdkafka or the 1.x/2.x clients.
- Existing packages stay on their registries, and existing applications keep working.
- 1.x/2.x issues and pull requests stay in the existing repositories (for example,
  `confluentinc/librdkafka` or `confluentinc/confluent-kafka-python`). In the Python, .NET, and
  JavaScript repositories, the `1.x`/`2.x` branch stays open for these fixes, while `main` carries
  4.x.

## Moving from 1.x/2.x to 4.x

Migration support has three parts:

- **Documented configuration differences:** a published, machine-readable mapping from each
  librdkafka config key to its Java-aligned equivalent.
- **Documented behavioral differences:** per-language migration guides that list where
  defaults and behavior change, with before-and-after examples.
- **AI-assisted migration:** a skill that applies the config mapping and guides developers
  through the API and behavioral changes in their code (see
  [AI-assisted migration](ai-assisted-migration.md)).
