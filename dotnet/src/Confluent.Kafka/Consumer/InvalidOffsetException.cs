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
using System.Collections.Generic;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// Thrown when the offset for a set of partitions is invalid (either undefined or out of range), and no reset policy
/// has been configured. Java's <c>org.apache.kafka.clients.consumer.InvalidOffsetException</c>.
/// </summary>
public abstract class InvalidOffsetException : global::Confluent.Kafka.Common.KafkaException
{
    /// <summary>Initializes the exception, as Java's <c>InvalidOffsetException(String message)</c>.</summary>
    protected InvalidOffsetException(string? message)
        : base(message)
    {
    }

    internal InvalidOffsetException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    /// <summary>Java's <c>partitions()</c>.</summary>
    public abstract IReadOnlyCollection<TopicPartition> Partitions();
}
