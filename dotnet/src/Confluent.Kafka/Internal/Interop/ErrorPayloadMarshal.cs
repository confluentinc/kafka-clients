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
/// Builds the error classes that carry fields from a core error, for the generated <see cref="ErrorClasses"/>
/// registry. Each reader takes the core's message and cause and the error handle, which stays owned by the caller,
/// reads the class's payload view and copies its fields out. A view is borrowed from the error and never destroyed;
/// the containers its accessors return are owned and freed here. Without a view (a <c>NULL</c> error) the fields keep
/// their Java defaults. <see cref="CorrelationIdMismatchException"/>'s ids read -1: the C ABI does not expose them.
/// </summary>
internal static class ErrorPayloadMarshal
{
    /// <summary>The payload views of a core error, one per class with fields.</summary>
    internal enum View
    {
        TopicAuthorization,
        GroupAuthorization,
        InvalidTopic,
        ResourceNotFound,
        DuplicateResource,
        RecordTooLarge,
        ThrottlingQuotaExceeded,
        QuotaViolation,
        ConsumerOffsetOutOfRange,
        ConsumerLogTruncation,
        ConsumerNoOffsetForPartition,
    }

    /// <summary>Hands out a payload view of a core error, or <c>IntPtr.Zero</c> when the error has none.</summary>
    internal interface IViewSource
    {
        IntPtr Get(IntPtr error, View view);
    }

    private delegate IntPtr Accessor(IntPtr self, ref IntPtr outErr);

    internal static Exception ReadTopicAuthorizationException(string? message, Exception? innerException, IntPtr error) =>
        ReadTopicAuthorizationException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadTopicAuthorizationException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.TopicAuthorization);
        IReadOnlyCollection<string> topics = view == IntPtr.Zero
            ? Array.Empty<string>()
            : StringsAndDestroy(Call(NativeMethods.TopicAuthorizationErrorUnauthorizedTopics, view));
        return new TopicAuthorizationException(default(CoreError), message, innerException, topics);
    }

    internal static Exception ReadGroupAuthorizationException(string? message, Exception? innerException, IntPtr error) =>
        ReadGroupAuthorizationException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadGroupAuthorizationException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.GroupAuthorization);
        string? groupId = view == IntPtr.Zero
            ? null
            : Utf8Marshal.PtrToString(Call(NativeMethods.GroupAuthorizationErrorGroupId, view));
        return new GroupAuthorizationException(default(CoreError), message, innerException, groupId);
    }

    internal static Exception ReadInvalidTopicException(string? message, Exception? innerException, IntPtr error) =>
        ReadInvalidTopicException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadInvalidTopicException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.InvalidTopic);
        IReadOnlyCollection<string> topics = view == IntPtr.Zero
            ? Array.Empty<string>()
            : StringsAndDestroy(Call(NativeMethods.InvalidTopicErrorInvalidTopics, view));
        return new InvalidTopicException(default(CoreError), message, innerException, topics);
    }

    internal static Exception ReadResourceNotFoundException(string? message, Exception? innerException, IntPtr error) =>
        ReadResourceNotFoundException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadResourceNotFoundException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.ResourceNotFound);
        string? resource = view == IntPtr.Zero
            ? null
            : Utf8Marshal.PtrToString(Call(NativeMethods.ResourceNotFoundErrorResource, view));
        return new ResourceNotFoundException(default(CoreError), message, innerException, resource);
    }

    internal static Exception ReadDuplicateResourceException(string? message, Exception? innerException, IntPtr error) =>
        ReadDuplicateResourceException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadDuplicateResourceException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.DuplicateResource);
        string? resource = view == IntPtr.Zero
            ? null
            : Utf8Marshal.PtrToString(Call(NativeMethods.DuplicateResourceErrorResource, view));
        return new DuplicateResourceException(default(CoreError), message, innerException, resource);
    }

    internal static Exception ReadRecordTooLargeException(string? message, Exception? innerException, IntPtr error) =>
        ReadRecordTooLargeException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadRecordTooLargeException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.RecordTooLarge);
        IReadOnlyDictionary<TopicPartition, long>? partitions = null;
        if (view != IntPtr.Zero)
        {
            // A NULL map is Java's null recordTooLargePartitions.
            IntPtr map = Call(NativeMethods.RecordTooLargeErrorRecordTooLargePartitions, view);
            partitions = map == IntPtr.Zero ? null : LongOffsetMapMarshal.CopyOutAndDestroy(map);
        }

        return new RecordTooLargeException(default(CoreError), message, innerException, partitions);
    }

    internal static Exception ReadThrottlingQuotaExceededException(
        string? message, Exception? innerException, IntPtr error) =>
        ReadThrottlingQuotaExceededException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadThrottlingQuotaExceededException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.ThrottlingQuotaExceeded);
        int throttleTimeMs = 0;
        if (view != IntPtr.Zero)
        {
            IntPtr outErr = IntPtr.Zero;
            throttleTimeMs = NativeMethods.ThrottlingQuotaExceededErrorThrottleTimeMs(view, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return new ThrottlingQuotaExceededException(default(CoreError), message, innerException, throttleTimeMs);
    }

    internal static Exception ReadQuotaViolationException(string? message, Exception? innerException, IntPtr error) =>
        ReadQuotaViolationException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadQuotaViolationException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.QuotaViolation);
        if (view == IntPtr.Zero)
        {
            return new QuotaViolationException(
                default(CoreError), message, innerException, new KafkaMetric(EmptyMetricName(), 0.0), 0.0, 0.0);
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr metricName = NativeMethods.QuotaViolationErrorMetricName(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        double value = NativeMethods.QuotaViolationErrorValue(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        double bound = NativeMethods.QuotaViolationErrorBound(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        MetricName name = metricName == IntPtr.Zero ? EmptyMetricName() : MetricsViewMarshal.CopyOutName(metricName);
        KafkaMetric metric = new KafkaMetric(name, value);

        // Java's constructor builds the message from the metric, the value and the bound, as Python's does.
        string builtMessage = new QuotaViolationException(metric, value, bound).Message;
        return new QuotaViolationException(default(CoreError), builtMessage, innerException, metric, value, bound);
    }

    internal static Exception ReadOffsetOutOfRangeException(string? message, Exception? innerException, IntPtr error) =>
        ReadOffsetOutOfRangeException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadOffsetOutOfRangeException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.ConsumerOffsetOutOfRange);
        IReadOnlyDictionary<TopicPartition, long> partitions = view == IntPtr.Zero
            ? EmptyReadOnlyDictionary<TopicPartition, long>.Instance
            : LongOffsetMapMarshal.CopyOutAndDestroy(
                Call(NativeMethods.ConsumerOffsetOutOfRangeErrorOffsetOutOfRangePartitions, view));
        return new Consumer.OffsetOutOfRangeException(default(CoreError), message, innerException, partitions);
    }

    internal static Exception ReadLogTruncationException(string? message, Exception? innerException, IntPtr error) =>
        ReadLogTruncationException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadLogTruncationException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.ConsumerLogTruncation);
        if (view == IntPtr.Zero)
        {
            return new LogTruncationException(
                default(CoreError),
                message,
                innerException,
                EmptyReadOnlyDictionary<TopicPartition, long>.Instance,
                EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance);
        }

        IReadOnlyDictionary<TopicPartition, long> fetchOffsets = LongOffsetMapMarshal.CopyOutAndDestroy(
            Call(NativeMethods.ConsumerLogTruncationErrorOffsetOutOfRangePartitions, view));
        IntPtr divergent = Call(NativeMethods.ConsumerLogTruncationErrorDivergentOffsets, view);
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> divergentOffsets;
        try
        {
            divergentOffsets = OffsetMapMarshal.CopyOut(divergent);
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MapDestroy(divergent, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return new LogTruncationException(default(CoreError), message, innerException, fetchOffsets, divergentOffsets);
    }

    internal static Exception ReadNoOffsetForPartitionException(
        string? message, Exception? innerException, IntPtr error) =>
        ReadNoOffsetForPartitionException(message, innerException, error, NativeViews.Instance);

    internal static Exception ReadNoOffsetForPartitionException(
        string? message, Exception? innerException, IntPtr error, IViewSource views)
    {
        IntPtr view = views.Get(error, View.ConsumerNoOffsetForPartition);
        if (view == IntPtr.Zero)
        {
            return new NoOffsetForPartitionException(
                default(CoreError), message, innerException, Array.Empty<TopicPartition>());
        }

        IReadOnlyCollection<TopicPartition> partitions = TopicPartitionListMarshal.CopyOutAndDestroy(
            Call(NativeMethods.ConsumerNoOffsetForPartitionErrorPartitions, view));

        // The core's message is already Java's: it follows the constructor Java's call site uses, the singular
        // (TopicPartition) one or the plural (Collection) one. The partitions are a set, as both constructors keep.
        return new NoOffsetForPartitionException(
            default(CoreError), message, innerException, new HashSet<TopicPartition>(partitions));
    }

    internal static Exception ReadCorrelationIdMismatchException(
        string? message, Exception? innerException, IntPtr error) =>
        new CorrelationIdMismatchException(default(CoreError), message, innerException, -1, -1);

    private static IntPtr Call(Accessor accessor, IntPtr view)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr result = accessor(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return result;
    }

    private static IReadOnlyCollection<string> StringsAndDestroy(IntPtr list) =>
        list == IntPtr.Zero ? Array.Empty<string>() : StringListMarshal.CopyOutAndDestroy(list);

    private static MetricName EmptyMetricName() =>
        new MetricName(string.Empty, string.Empty, string.Empty, EmptyReadOnlyDictionary<string, string>.Instance);

    /// <summary>Reads the payload views through the C ABI's <c>kafka_common_Error_*_err</c> functions.</summary>
    internal sealed class NativeViews : IViewSource
    {
        internal static readonly NativeViews Instance = new NativeViews();

        private NativeViews()
        {
        }

        public IntPtr Get(IntPtr error, View view)
        {
            if (error == IntPtr.Zero)
            {
                return IntPtr.Zero;
            }

            IntPtr outErr = IntPtr.Zero;
            IntPtr result = view switch
            {
                View.TopicAuthorization => NativeMethods.ErrorTopicAuthorizationView(error, ref outErr),
                View.GroupAuthorization => NativeMethods.ErrorGroupAuthorizationView(error, ref outErr),
                View.InvalidTopic => NativeMethods.ErrorInvalidTopicView(error, ref outErr),
                View.ResourceNotFound => NativeMethods.ErrorResourceNotFoundView(error, ref outErr),
                View.DuplicateResource => NativeMethods.ErrorDuplicateResourceView(error, ref outErr),
                View.RecordTooLarge => NativeMethods.ErrorRecordTooLargeView(error, ref outErr),
                View.ThrottlingQuotaExceeded => NativeMethods.ErrorThrottlingQuotaExceededView(error, ref outErr),
                View.QuotaViolation => NativeMethods.ErrorQuotaViolationView(error, ref outErr),
                View.ConsumerOffsetOutOfRange => NativeMethods.ErrorConsumerOffsetOutOfRangeView(error, ref outErr),
                View.ConsumerLogTruncation => NativeMethods.ErrorConsumerLogTruncationView(error, ref outErr),
                View.ConsumerNoOffsetForPartition =>
                    NativeMethods.ErrorConsumerNoOffsetForPartitionView(error, ref outErr),
                _ => throw new ArgumentOutOfRangeException(nameof(view)),
            };
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return result;
        }
    }
}
