# RFC: Client licensing

- **Status:** Draft
- **Audience:** users and contributors of Confluent's Kafka clients

## Scope

This RFC covers the licensing of the open-source clients: the Rust core and the language
bindings, which are translated from the Apache Kafka Java client.

## Summary

License the Rust core and every language binding under
[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0), matching upstream Apache Kafka.

## Motivation

The core and bindings are translated from the Apache-2.0 Apache Kafka Java client, so Apache
2.0 is the natural license: it matches upstream, it is permissive and familiar to the Kafka
community.

## The license

- **[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)** for the Rust core and every
  language binding.

Every open-source package ships the full Apache 2.0 `LICENSE` file at its root, carries the
standard Apache 2.0 header in its sources, and declares the license in its ecosystem's
package metadata.

## Change from the librdkafka-based clients

The open-source clients standardize on Apache 2.0. Two change from their current license:

| Client | Current license | New license |
| --- | --- | --- |
| Rust (core) | (new) | Apache 2.0 |
| [Python](https://github.com/confluentinc/confluent-kafka-python) | Apache 2.0 | Apache 2.0 |
| [.NET](https://github.com/confluentinc/confluent-kafka-dotnet) | Apache 2.0 | Apache 2.0 |
| [JavaScript](https://github.com/confluentinc/confluent-kafka-javascript) | MIT | Apache 2.0 |
| [C/C++](https://github.com/confluentinc/librdkafka) | BSD 2-Clause (librdkafka) | Apache 2.0 |

## Contributions

Contributions to the core and bindings are accepted under Apache 2.0 (see `CONTRIBUTING.md`).

## Relationship to the Apache Software Foundation

Keeping the core and bindings Apache 2.0 keeps open the option of donating them to the Apache
Software Foundation.

## Open questions

- Whether a `NOTICE` file is needed to attribute bundled third-party dependencies, per
  Apache 2.0 section 4.
