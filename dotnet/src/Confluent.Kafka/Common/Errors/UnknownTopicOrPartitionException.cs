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
/// This topic/partition doesn't exist. This exception is used in contexts where a topic doesn't seem to exist based on
/// possibly stale metadata. This exception is retriable because the topic or partition might subsequently be created.
/// Java's <c>org.apache.kafka.common.errors.UnknownTopicOrPartitionException</c>.
/// </summary>
public class UnknownTopicOrPartitionException : global::Confluent.Kafka.Common.Errors.InvalidMetadataException
{
    /// <summary>Initializes the exception, as Java's <c>UnknownTopicOrPartitionException()</c>.</summary>
    public UnknownTopicOrPartitionException()
    {
    }

    /// <summary>Initializes the exception, as Java's <c>UnknownTopicOrPartitionException(String message)</c>.</summary>
    public UnknownTopicOrPartitionException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>UnknownTopicOrPartitionException(Throwable throwable)</c>.
    /// </summary>
    public UnknownTopicOrPartitionException(Exception? innerException)
        : base(innerException)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>UnknownTopicOrPartitionException(String message, Throwable
    /// throwable)</c>.
    /// </summary>
    public UnknownTopicOrPartitionException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    internal UnknownTopicOrPartitionException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => 3;
}
