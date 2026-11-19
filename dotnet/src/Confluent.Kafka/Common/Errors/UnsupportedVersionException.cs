// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// GENERATED, DO NOT EDIT. Produced from the Java source by `cargo xtask generate-error-codes`
// and checked for staleness by `cargo xtask check-generated`.

using System;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Errors;

/// <summary>
/// Indicates that a request API or version needed by the client is not supported by the broker. This is typically a
/// fatal error as Kafka clients will downgrade request versions as needed except in cases where a needed feature is not
/// available in old versions. Fatal errors can generally only be handled by closing the client instance, although in
/// some cases it may be possible to continue without relying on the underlying feature. For example, when the producer
/// is used with idempotence enabled, this error is fatal since the producer does not support reverting to weaker
/// semantics. On the other hand, if this error is raised from
/// <c>org.apache.kafka.clients.consumer.KafkaConsumer.offsetsForTimes(Map)</c>, it would be possible to revert to
/// alternative logic to set the consumer's position. Java's
/// <c>org.apache.kafka.common.errors.UnsupportedVersionException</c>.
/// </summary>
public class UnsupportedVersionException : global::Confluent.Kafka.Common.Errors.InvalidConfigurationException
{
    /// <summary>
    /// Initializes the exception, as Java's <c>UnsupportedVersionException(String message, Throwable cause)</c>.
    /// </summary>
    public UnsupportedVersionException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>UnsupportedVersionException(String message)</c>.</summary>
    public UnsupportedVersionException(string? message)
        : base(message)
    {
    }

    internal UnsupportedVersionException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 35;
}
