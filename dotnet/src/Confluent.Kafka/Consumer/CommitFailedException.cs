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

namespace Confluent.Kafka.Consumer;

/// <summary>
/// This exception is raised when an offset commit with <c>KafkaConsumer.commitSync()</c> fails with an unrecoverable
/// error. This exception is generated on the client side, typically when a group rebalance completes before the commit
/// could be successfully applied. In this case, the commit cannot generally be retried because some of the partitions
/// may have already been assigned to another member in the group. Java's
/// <c>org.apache.kafka.clients.consumer.CommitFailedException</c>.
/// </summary>
public class CommitFailedException : global::Confluent.Kafka.Common.KafkaException
{
    /// <summary>Initializes the exception, as Java's <c>CommitFailedException(String message)</c>.</summary>
    public CommitFailedException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>CommitFailedException()</c>.</summary>
    public CommitFailedException()
        : base("Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records.")
    {
    }

    internal CommitFailedException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => -19;
}
