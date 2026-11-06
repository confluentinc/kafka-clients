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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Errors;

/// <summary>
/// This record is larger than the maximum allowable size. Java's
/// <c>org.apache.kafka.common.errors.RecordTooLargeException</c>.
/// </summary>
public class RecordTooLargeException : global::Confluent.Kafka.Common.Errors.ApiException
{
    private readonly IReadOnlyDictionary<TopicPartition, long>? _recordTooLargePartitions;

    /// <summary>Initializes the exception, as Java's <c>RecordTooLargeException()</c>.</summary>
    public RecordTooLargeException()
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>RecordTooLargeException(String message, Throwable cause)</c>.
    /// </summary>
    public RecordTooLargeException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>RecordTooLargeException(String message)</c>.</summary>
    public RecordTooLargeException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>RecordTooLargeException(Throwable cause)</c>.</summary>
    public RecordTooLargeException(Exception? innerException)
        : base(innerException)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>RecordTooLargeException(String message, Map&lt;TopicPartition, Long&gt;
    /// recordTooLargePartitions)</c>.
    /// </summary>
    public RecordTooLargeException(string? message, IReadOnlyDictionary<TopicPartition, long> recordTooLargePartitions)
        : base(message)
    {
        _recordTooLargePartitions = recordTooLargePartitions;
    }

    internal RecordTooLargeException(CoreError _, string? message, Exception? innerException, IReadOnlyDictionary<TopicPartition, long>? recordTooLargePartitions)
        : base(_, message, innerException)
    {
        _recordTooLargePartitions = recordTooLargePartitions;
    }

    /// <summary>Java's <c>recordTooLargePartitions()</c>.</summary>
    public IReadOnlyDictionary<TopicPartition, long>? RecordTooLargePartitions() => _recordTooLargePartitions;

    internal override int FfiId => 10;
}
