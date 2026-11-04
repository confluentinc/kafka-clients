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

using System;
using System.Collections.Generic;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Common.Requests;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds the error classes that carry fields from a core error, for the generated
/// <see cref="ErrorClasses"/> registry. Each reader takes the core's message and cause and the error handle,
/// which stays owned by the caller. The readers do not read the C views yet: the fields hold their Java
/// defaults, except <see cref="CorrelationIdMismatchException"/>'s ids, which the C ABI does not expose and
/// read -1.
/// </summary>
internal static class ErrorPayloadMarshal
{
    internal static Exception ReadTopicAuthorizationException(string? message, Exception? innerException, IntPtr error) =>
        new TopicAuthorizationException(default(CoreError), message, innerException, Array.Empty<string>());

    internal static Exception ReadGroupAuthorizationException(string? message, Exception? innerException, IntPtr error) =>
        new GroupAuthorizationException(default(CoreError), message, innerException, null);

    internal static Exception ReadInvalidTopicException(string? message, Exception? innerException, IntPtr error) =>
        new InvalidTopicException(default(CoreError), message, innerException, Array.Empty<string>());

    internal static Exception ReadResourceNotFoundException(string? message, Exception? innerException, IntPtr error) =>
        new ResourceNotFoundException(default(CoreError), message, innerException, null);

    internal static Exception ReadDuplicateResourceException(string? message, Exception? innerException, IntPtr error) =>
        new DuplicateResourceException(default(CoreError), message, innerException, null);

    internal static Exception ReadRecordTooLargeException(string? message, Exception? innerException, IntPtr error) =>
        new RecordTooLargeException(default(CoreError), message, innerException, null);

    internal static Exception ReadThrottlingQuotaExceededException(
        string? message, Exception? innerException, IntPtr error) =>
        new ThrottlingQuotaExceededException(default(CoreError), message, innerException, 0);

    internal static Exception ReadQuotaViolationException(string? message, Exception? innerException, IntPtr error) =>
        new QuotaViolationException(
            default(CoreError),
            message,
            innerException,
            new KafkaMetric(
                new MetricName(
                    string.Empty, string.Empty, string.Empty, EmptyReadOnlyDictionary<string, string>.Instance),
                0.0),
            0.0,
            0.0);

    internal static Exception ReadOffsetOutOfRangeException(string? message, Exception? innerException, IntPtr error) =>
        new Consumer.OffsetOutOfRangeException(
            default(CoreError), message, innerException, EmptyReadOnlyDictionary<TopicPartition, long>.Instance);

    internal static Exception ReadLogTruncationException(string? message, Exception? innerException, IntPtr error) =>
        new LogTruncationException(
            default(CoreError),
            message,
            innerException,
            EmptyReadOnlyDictionary<TopicPartition, long>.Instance,
            EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance);

    internal static Exception ReadNoOffsetForPartitionException(
        string? message, Exception? innerException, IntPtr error) =>
        new NoOffsetForPartitionException(default(CoreError), message, innerException, Array.Empty<TopicPartition>());

    internal static Exception ReadCorrelationIdMismatchException(
        string? message, Exception? innerException, IntPtr error) =>
        new CorrelationIdMismatchException(default(CoreError), message, innerException, -1, -1);
}
