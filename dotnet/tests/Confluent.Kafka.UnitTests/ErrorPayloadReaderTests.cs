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
using System.Linq;
using System.Runtime.InteropServices;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Common.Requests;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

using Xunit;

using View = Confluent.Kafka.Internal.Interop.ErrorPayloadMarshal.View;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The fields of the error classes that carry a payload are read from the core error's payload view: errors built by
/// the core's factories, payload handles handed to the readers through a fake view source, and the errors the core's
/// mock consumer raises. A read leaves the error and its view alive, so each test reads twice and frees the error once.
/// </summary>
public sealed class ErrorPayloadReaderTests
{
    private const string NonAsciiTopic = "tøpic-Ω-日本";

    private const string NonAsciiMessage = "café-Ω-日本語-😀";

    private const string OtherTopic = "other-topic";

    private static readonly IntPtr s_sentinelError = new IntPtr(0x5EED);

    [Fact]
    public void TopicAuthorization_ReadsItsUnauthorizedTopics_AndTheCoreMessage()
    {
        IntPtr error;
        using (Pins pins = new Pins())
        {
            IntPtr topics = pins.StringList(NonAsciiTopic, OtherTopic);
            error = Call((ref IntPtr outErr) => Native.ErrorTopicAuthorization(topics, ref outErr));
        }

        string coreMessage = CoreMessage(error);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                TopicAuthorizationException e =
                    Assert.IsType<TopicAuthorizationException>(KafkaException.FromFfiError(error, owned: false));
                Assert.Equal(coreMessage, e.Message);
                Assert.Equal(new HashSet<string> { NonAsciiTopic, OtherTopic }, new HashSet<string>(e.UnauthorizedTopics));
                Assert.Equal(29, e.Code);
                Assert.Null(e.InnerException);
            }
        }
        finally
        {
            Destroy(error);
        }
    }

    [Fact]
    public void TopicAuthorization_KeepsAGivenMessage_ThroughAnOwnedRead()
    {
        IntPtr error;
        using (Pins pins = new Pins())
        {
            IntPtr topics = pins.StringList(NonAsciiTopic);
            IntPtr message = pins.String(NonAsciiMessage);
            error = Call((ref IntPtr outErr) => Native.ErrorTopicAuthorizationMessage(topics, message, ref outErr));
        }

        TopicAuthorizationException e =
            Assert.IsType<TopicAuthorizationException>(KafkaException.FromFfiError(error, owned: true));

        Assert.Equal(NonAsciiMessage, e.Message);
        Assert.Equal(new[] { NonAsciiTopic }, e.UnauthorizedTopics.ToArray());
    }

    [Fact]
    public void InvalidTopic_ReadsItsInvalidTopics()
    {
        IntPtr plain;
        IntPtr withMessage;
        using (Pins pins = new Pins())
        {
            IntPtr topics = pins.StringList(NonAsciiTopic, OtherTopic);
            IntPtr message = pins.String(NonAsciiMessage);
            plain = Call((ref IntPtr outErr) => Native.ErrorInvalidTopics(topics, ref outErr));
            withMessage = Call((ref IntPtr outErr) => Native.ErrorInvalidTopicsMessage(topics, message, ref outErr));
        }

        string coreMessage = CoreMessage(plain);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                InvalidTopicException fromPlain =
                    Assert.IsType<InvalidTopicException>(KafkaException.FromFfiError(plain, owned: false));
                InvalidTopicException fromMessage =
                    Assert.IsType<InvalidTopicException>(KafkaException.FromFfiError(withMessage, owned: false));
                Assert.Equal(coreMessage, fromPlain.Message);
                Assert.Equal(NonAsciiMessage, fromMessage.Message);
                Assert.Equal(new HashSet<string> { NonAsciiTopic, OtherTopic }, new HashSet<string>(fromPlain.InvalidTopics));
                Assert.Equal(new HashSet<string> { NonAsciiTopic, OtherTopic }, new HashSet<string>(fromMessage.InvalidTopics));
            }
        }
        finally
        {
            Destroy(plain);
            Destroy(withMessage);
        }
    }

    [Fact]
    public void GroupAuthorization_ReadsItsGroupId_OrNullWithoutOne()
    {
        IntPtr forGroup;
        IntPtr withMessage;
        IntPtr withoutGroup;
        using (Pins pins = new Pins())
        {
            IntPtr group = pins.String(NonAsciiTopic);
            IntPtr message = pins.String(NonAsciiMessage);
            forGroup = Call((ref IntPtr outErr) => Native.ErrorGroupAuthorization(group, ref outErr));
            withMessage = Call((ref IntPtr outErr) => Native.ErrorGroupAuthorizationWithMessage(group, message, ref outErr));
            withoutGroup = Call(
                (ref IntPtr outErr) => Native.ErrorGroupAuthorizationWithMessage(IntPtr.Zero, message, ref outErr));
        }

        string forGroupMessage = CoreMessage(forGroup);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                GroupAuthorizationException a =
                    Assert.IsType<GroupAuthorizationException>(KafkaException.FromFfiError(forGroup, owned: false));
                GroupAuthorizationException b =
                    Assert.IsType<GroupAuthorizationException>(KafkaException.FromFfiError(withMessage, owned: false));
                GroupAuthorizationException c =
                    Assert.IsType<GroupAuthorizationException>(KafkaException.FromFfiError(withoutGroup, owned: false));
                Assert.Equal(forGroupMessage, a.Message);
                Assert.Equal(NonAsciiTopic, a.GroupId);
                Assert.Equal(NonAsciiMessage, b.Message);
                Assert.Equal(NonAsciiTopic, b.GroupId);
                Assert.Equal(NonAsciiMessage, c.Message);
                Assert.Null(c.GroupId);
            }
        }
        finally
        {
            Destroy(forGroup);
            Destroy(withMessage);
            Destroy(withoutGroup);
        }
    }

    [Fact]
    public void ThrottlingQuotaExceeded_ReadsItsThrottleTime()
    {
        IntPtr error;
        using (Pins pins = new Pins())
        {
            IntPtr message = pins.String(NonAsciiMessage);
            error = Call((ref IntPtr outErr) => Native.ErrorThrottlingQuotaExceeded(1234, message, ref outErr));
        }

        try
        {
            for (int read = 0; read < 2; read++)
            {
                ThrottlingQuotaExceededException e =
                    Assert.IsType<ThrottlingQuotaExceededException>(KafkaException.FromFfiError(error, owned: false));
                Assert.Equal(NonAsciiMessage, e.Message);
                Assert.Equal(1234, e.ThrottleTimeMs);
            }
        }
        finally
        {
            Destroy(error);
        }
    }

    [Fact]
    public void CorrelationIdMismatch_ReadsMinusOne_ForBothIds()
    {
        IntPtr error;
        using (Pins pins = new Pins())
        {
            IntPtr message = pins.String(NonAsciiMessage);
            error = Call((ref IntPtr outErr) => Native.ErrorCorrelationIdMismatch(message, 3, 4, ref outErr));
        }

        try
        {
            CorrelationIdMismatchException e =
                Assert.IsType<CorrelationIdMismatchException>(KafkaException.FromFfiError(error, owned: false));
            Assert.Equal(NonAsciiMessage, e.Message);
            Assert.Equal(-1, e.RequestCorrelationId);
            Assert.Equal(-1, e.ResponseCorrelationId);
        }
        finally
        {
            Destroy(error);
        }
    }

    [Fact]
    public void RecordTooLarge_FromTheCore_HasNullPartitions()
    {
        IntPtr error;
        using (Pins pins = new Pins())
        {
            IntPtr message = pins.String(NonAsciiMessage);
            error = Call((ref IntPtr outErr) => Native.ErrorRecordTooLarge(message, ref outErr));
        }

        try
        {
            for (int read = 0; read < 2; read++)
            {
                RecordTooLargeException e =
                    Assert.IsType<RecordTooLargeException>(KafkaException.FromFfiError(error, owned: false));
                Assert.Equal(NonAsciiMessage, e.Message);
                Assert.Null(e.RecordTooLargePartitions);
            }
        }
        finally
        {
            Destroy(error);
        }
    }

    [Fact]
    public void EachCoreError_HasItsOwnView_AndNoOther()
    {
        var rows = new List<(IntPtr Error, View? Own)>();
        using (Pins pins = new Pins())
        {
            IntPtr topics = pins.StringList(NonAsciiTopic);
            IntPtr message = pins.String(NonAsciiMessage);
            IntPtr group = pins.String(NonAsciiTopic);
            rows.Add((Call((ref IntPtr e) => Native.ErrorTopicAuthorization(topics, ref e)), View.TopicAuthorization));
            rows.Add((Call((ref IntPtr e) => Native.ErrorInvalidTopics(topics, ref e)), View.InvalidTopic));
            rows.Add((Call((ref IntPtr e) => Native.ErrorGroupAuthorization(group, ref e)), View.GroupAuthorization));
            rows.Add((Call((ref IntPtr e) => Native.ErrorThrottlingQuotaExceeded(1, message, ref e)),
                View.ThrottlingQuotaExceeded));
            rows.Add((Call((ref IntPtr e) => Native.ErrorRecordTooLarge(message, ref e)), View.RecordTooLarge));
            rows.Add((Call((ref IntPtr e) => Native.ErrorCorrelationIdMismatch(message, 1, 2, ref e)), null));
        }

        try
        {
            var problems = new List<string>();
            foreach ((IntPtr error, View? own) in rows)
            {
                foreach (View view in Enum.GetValues(typeof(View)).Cast<View>())
                {
                    bool present = ErrorPayloadMarshal.NativeViews.Instance.Get(error, view) != IntPtr.Zero;
                    if (present != (view == own))
                    {
                        problems.Add($"{own?.ToString() ?? "CorrelationIdMismatch"}: view {view} present={present}");
                    }
                }
            }

            Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
            Assert.Equal(IntPtr.Zero, ErrorPayloadMarshal.NativeViews.Instance.Get(IntPtr.Zero, View.TopicAuthorization));
        }
        finally
        {
            foreach ((IntPtr error, View? _) in rows)
            {
                Destroy(error);
            }
        }
    }

    [Fact]
    public void ResourceNotFound_ReadsItsResource()
    {
        IntPtr payload;
        using (Pins pins = new Pins())
        {
            IntPtr resource = pins.String(NonAsciiTopic);
            IntPtr message = pins.String(NonAsciiMessage);
            payload = Call((ref IntPtr outErr) => Native.ResourceNotFoundWithResource(resource, message, ref outErr));
        }

        var cause = new InvalidOperationException("cause");
        var views = new FakeViews(View.ResourceNotFound, payload);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                ResourceNotFoundException e = Assert.IsType<ResourceNotFoundException>(
                    ErrorPayloadMarshal.ReadResourceNotFoundException("m", cause, s_sentinelError, views));
                Assert.Equal("m", e.Message);
                Assert.Same(cause, e.InnerException);
                Assert.Equal(NonAsciiTopic, e.Resource);
            }

            views.AssertOnlyAsked(View.ResourceNotFound);
        }
        finally
        {
            DestroyPayload(Native.ResourceNotFoundDestroy, payload);
        }
    }

    [Fact]
    public void DuplicateResource_ReadsItsResource()
    {
        IntPtr payload;
        using (Pins pins = new Pins())
        {
            IntPtr resource = pins.String(NonAsciiTopic);
            IntPtr message = pins.String(NonAsciiMessage);
            payload = Call((ref IntPtr outErr) => Native.DuplicateResourceWithResource(resource, message, ref outErr));
        }

        var views = new FakeViews(View.DuplicateResource, payload);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                DuplicateResourceException e = Assert.IsType<DuplicateResourceException>(
                    ErrorPayloadMarshal.ReadDuplicateResourceException("m", null, s_sentinelError, views));
                Assert.Equal("m", e.Message);
                Assert.Null(e.InnerException);
                Assert.Equal(NonAsciiTopic, e.Resource);
            }

            views.AssertOnlyAsked(View.DuplicateResource);
        }
        finally
        {
            DestroyPayload(Native.DuplicateResourceDestroy, payload);
        }
    }

    [Fact]
    public void RecordTooLarge_ReadsItsPartitions()
    {
        IntPtr payload;
        using (Pins pins = new Pins())
        {
            IntPtr map = pins.LongOffsetMap((NonAsciiTopic, 0, 100L), (OtherTopic, 3, 7L));
            IntPtr message = pins.String(NonAsciiMessage);
            payload = Call((ref IntPtr outErr) => Native.RecordTooLargeWithPartitions(message, map, ref outErr));
        }

        var views = new FakeViews(View.RecordTooLarge, payload);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                RecordTooLargeException e = Assert.IsType<RecordTooLargeException>(
                    ErrorPayloadMarshal.ReadRecordTooLargeException("m", null, s_sentinelError, views));
                Assert.Equal("m", e.Message);
                Assert.Equal(
                    new Dictionary<TopicPartition, long>
                    {
                        [new TopicPartition(NonAsciiTopic, 0)] = 100L,
                        [new TopicPartition(OtherTopic, 3)] = 7L,
                    },
                    e.RecordTooLargePartitions!.ToDictionary(entry => entry.Key, entry => entry.Value));
            }

            views.AssertOnlyAsked(View.RecordTooLarge);
        }
        finally
        {
            DestroyPayload(Native.RecordTooLargeDestroy, payload);
        }
    }

    [Fact]
    public void QuotaViolation_ReadsItsMetricValueAndBound_WithJavasMessage()
    {
        IntPtr payload;
        using (Pins pins = new Pins())
        {
            IntPtr tags = pins.StringMap(("client-id", "c-日本"));
            IntPtr metricName = Call((ref IntPtr outErr) => Native.MetricNameNew(
                pins.String("produce-throttle-Ω"), pins.String("producer-metrics"), pins.String("d-é"), tags, ref outErr));
            try
            {
                payload = Call((ref IntPtr outErr) => Native.QuotaViolationNew(metricName, 12.5, 10.0, ref outErr));
            }
            finally
            {
                DestroyPayload(Native.MetricNameDestroy, metricName);
            }
        }

        var cause = new InvalidOperationException("cause");
        var views = new FakeViews(View.QuotaViolation, payload);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                QuotaViolationException e = Assert.IsType<QuotaViolationException>(
                    ErrorPayloadMarshal.ReadQuotaViolationException("core text", cause, s_sentinelError, views));
                Assert.Equal(
                    "'MetricName [name=produce-throttle-Ω, group=producer-metrics, description=d-é, "
                    + "tags={client-id=c-日本}]' violated quota. Actual: 12.5, Threshold: 10.0",
                    e.Message);
                Assert.Same(cause, e.InnerException);
                Assert.Equal(12.5, e.Value);
                Assert.Equal(10.0, e.Bound);
                Assert.Equal("produce-throttle-Ω", e.Metric.Name.Name);
                Assert.Equal("producer-metrics", e.Metric.Name.Group);
                Assert.Equal("d-é", e.Metric.Name.Description);
                Assert.Equal(new Dictionary<string, string> { ["client-id"] = "c-日本" }, e.Metric.Name.Tags);
                Assert.Equal(12.5, e.Metric.Value);
            }

            views.AssertOnlyAsked(View.QuotaViolation);
        }
        finally
        {
            DestroyPayload(Native.QuotaViolationDestroy, payload);
        }
    }

    [Fact]
    public void OffsetOutOfRange_ReadsItsPartitionOffsets_AndThePartitions()
    {
        IntPtr payload;
        using (Pins pins = new Pins())
        {
            IntPtr map = pins.LongOffsetMap((NonAsciiTopic, 0, 42L), (OtherTopic, 1, 9L));
            IntPtr message = pins.String(NonAsciiMessage);
            payload = Call((ref IntPtr outErr) => Native.OffsetOutOfRangeWithMessage(message, map, ref outErr));
        }

        var views = new FakeViews(View.ConsumerOffsetOutOfRange, payload);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                Consumer.OffsetOutOfRangeException e = Assert.IsType<Consumer.OffsetOutOfRangeException>(
                    ErrorPayloadMarshal.ReadOffsetOutOfRangeException("m", null, s_sentinelError, views));
                Assert.Equal("m", e.Message);
                Assert.Equal(
                    new Dictionary<TopicPartition, long>
                    {
                        [new TopicPartition(NonAsciiTopic, 0)] = 42L,
                        [new TopicPartition(OtherTopic, 1)] = 9L,
                    },
                    e.OffsetOutOfRangePartitions.ToDictionary(entry => entry.Key, entry => entry.Value));
                Assert.Equal(
                    new HashSet<TopicPartition> { new TopicPartition(NonAsciiTopic, 0), new TopicPartition(OtherTopic, 1) },
                    new HashSet<TopicPartition>(e.Partitions));
            }

            views.AssertOnlyAsked(View.ConsumerOffsetOutOfRange);
        }
        finally
        {
            DestroyPayload(Native.OffsetOutOfRangeDestroy, payload);
        }
    }

    [Fact]
    public void LogTruncation_ReadsItsFetchAndDivergentOffsets_AndThePartitions()
    {
        IntPtr payload;
        using (Pins pins = new Pins())
        {
            IntPtr fetch = pins.LongOffsetMap((NonAsciiTopic, 0, 50L));
            IntPtr divergent = pins.OffsetMap((NonAsciiTopic, 0, 40L, 7, "m-Ω"));
            IntPtr message = pins.String(NonAsciiMessage);
            payload = Call(
                (ref IntPtr outErr) => Native.LogTruncationWithMessage(message, fetch, divergent, ref outErr));
        }

        var views = new FakeViews(View.ConsumerLogTruncation, payload);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                LogTruncationException e = Assert.IsType<LogTruncationException>(
                    ErrorPayloadMarshal.ReadLogTruncationException("m", null, s_sentinelError, views));
                Assert.Equal("m", e.Message);
                TopicPartition tp = new TopicPartition(NonAsciiTopic, 0);
                Assert.Equal(
                    new Dictionary<TopicPartition, long> { [tp] = 50L },
                    e.OffsetOutOfRangePartitions.ToDictionary(entry => entry.Key, entry => entry.Value));
                Assert.Equal(new[] { tp }, e.Partitions.ToArray());
                Assert.Equal(
                    new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(40L, "m-Ω", 7) },
                    e.DivergentOffsets.ToDictionary(entry => entry.Key, entry => entry.Value));
            }

            views.AssertOnlyAsked(View.ConsumerLogTruncation);
        }
        finally
        {
            DestroyPayload(Native.LogTruncationDestroy, payload);
        }
    }

    [Fact]
    public void NoOffsetForPartition_ReadsItsPartitionsAsASet_AndKeepsTheCoreMessage()
    {
        IntPtr payload;
        using (Pins pins = new Pins())
        {
            IntPtr list = pins.TopicPartitionList((NonAsciiTopic, 2), (OtherTopic, 1), (NonAsciiTopic, 2));
            payload = Call((ref IntPtr outErr) => Native.NoOffsetForPartitionWithPartitions(list, ref outErr));
        }

        var views = new FakeViews(View.ConsumerNoOffsetForPartition, payload);
        try
        {
            for (int read = 0; read < 2; read++)
            {
                NoOffsetForPartitionException e = Assert.IsType<NoOffsetForPartitionException>(
                    ErrorPayloadMarshal.ReadNoOffsetForPartitionException("core text", null, s_sentinelError, views));
                Assert.Equal("core text", e.Message);
                Assert.IsAssignableFrom<ISet<TopicPartition>>(e.Partitions);
                Assert.Equal(2, e.Partitions.Count);
                Assert.Equal(
                    new HashSet<TopicPartition> { new TopicPartition(NonAsciiTopic, 2), new TopicPartition(OtherTopic, 1) },
                    new HashSet<TopicPartition>(e.Partitions));
            }

            views.AssertOnlyAsked(View.ConsumerNoOffsetForPartition);
        }
        finally
        {
            DestroyPayload(Native.NoOffsetForPartitionDestroy, payload);
        }
    }

    [Fact]
    public void WithoutAView_TheReadersKeepTheCoreMessage_AndJavasDefaults()
    {
        var views = new FakeViews(null, IntPtr.Zero);
        var cause = new InvalidOperationException("cause");

        Assert.Empty(Assert.IsType<TopicAuthorizationException>(
            ErrorPayloadMarshal.ReadTopicAuthorizationException("m", cause, s_sentinelError, views)).UnauthorizedTopics);
        Assert.Null(Assert.IsType<GroupAuthorizationException>(
            ErrorPayloadMarshal.ReadGroupAuthorizationException("m", cause, s_sentinelError, views)).GroupId);
        Assert.Null(Assert.IsType<RecordTooLargeException>(
            ErrorPayloadMarshal.ReadRecordTooLargeException("m", cause, s_sentinelError, views)).RecordTooLargePartitions);
        Assert.Equal(0, Assert.IsType<ThrottlingQuotaExceededException>(
            ErrorPayloadMarshal.ReadThrottlingQuotaExceededException("m", cause, s_sentinelError, views)).ThrottleTimeMs);
        Assert.Empty(Assert.IsType<NoOffsetForPartitionException>(
            ErrorPayloadMarshal.ReadNoOffsetForPartitionException("m", cause, s_sentinelError, views)).Partitions);
        Exception quota = ErrorPayloadMarshal.ReadQuotaViolationException("m", cause, s_sentinelError, views);
        Assert.Equal("m", quota.Message);
        Assert.Same(cause, quota.InnerException);
    }

    [Fact]
    public void TheMockConsumer_RaisesNoOffsetForPartition_WithItsPartition()
    {
        TopicPartition tp = new TopicPartition(NonAsciiTopic, 0);
        using MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "none");
        consumer.Assign(new[] { tp });

        NoOffsetForPartitionException e =
            Assert.Throws<NoOffsetForPartitionException>(() => consumer.Poll(TimeSpan.Zero));

        Assert.Equal("Undefined offset with no reset policy for partition: " + NonAsciiTopic + "-0", e.Message);
        Assert.Equal(new[] { tp }, e.Partitions.ToArray());
    }

    [Fact]
    public void TheMockConsumer_RaisesOffsetOutOfRange_WithThePosition()
    {
        TopicPartition tp = new TopicPartition(NonAsciiTopic, 0);
        using MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { tp });
        consumer.UpdateBeginningOffset(NonAsciiTopic, 0, 5);
        consumer.Seek(tp, 2);
        consumer.AddRecord(NonAsciiTopic, 0, 2, null, new byte[] { 1 });

        Consumer.OffsetOutOfRangeException e =
            Assert.Throws<Consumer.OffsetOutOfRangeException>(() => consumer.Poll(TimeSpan.Zero));

        Assert.Equal(
            new Dictionary<TopicPartition, long> { [tp] = 2L },
            e.OffsetOutOfRangePartitions.ToDictionary(entry => entry.Key, entry => entry.Value));
        Assert.Equal(new[] { tp }, e.Partitions.ToArray());
    }

    private delegate IntPtr Factory(ref IntPtr outErr);

    private delegate void Destructor(IntPtr self, ref IntPtr outErr);

    private static IntPtr Call(Factory factory)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr result = factory(ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, result);
        return result;
    }

    private static string CoreMessage(IntPtr error) => Utf8Marshal.PtrToString(NativeMethods.Message(error))!;

    private static void Destroy(IntPtr error)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ErrorDestroy(error, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
    }

    private static void DestroyPayload(Destructor destroy, IntPtr payload)
    {
        IntPtr outErr = IntPtr.Zero;
        destroy(payload, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
    }

    /// <summary>Hands out one payload handle for one view and records every view asked for.</summary>
    private sealed class FakeViews : ErrorPayloadMarshal.IViewSource
    {
        private readonly View? _view;
        private readonly IntPtr _payload;
        private readonly List<View> _asked = new List<View>();

        internal FakeViews(View? view, IntPtr payload)
        {
            _view = view;
            _payload = payload;
        }

        public IntPtr Get(IntPtr error, View view)
        {
            Assert.Equal(s_sentinelError, error);
            _asked.Add(view);
            return view == _view ? _payload : IntPtr.Zero;
        }

        internal void AssertOnlyAsked(View view) => Assert.All(_asked, asked => Assert.Equal(view, asked));
    }

    /// <summary>
    /// The native inputs of one factory call: pinned strings and the handles, containers and offsets built from them,
    /// all freed once the call has copied them.
    /// </summary>
    private sealed class Pins : IDisposable
    {
        private readonly List<Utf8Marshal.PinnedUtf8String> _strings = new List<Utf8Marshal.PinnedUtf8String>();
        private readonly List<IntPtr> _topicPartitions = new List<IntPtr>();
        private readonly List<IntPtr> _offsets = new List<IntPtr>();
        private readonly List<IntPtr> _longs = new List<IntPtr>();
        private readonly List<IntPtr> _lists = new List<IntPtr>();
        private readonly List<IntPtr> _maps = new List<IntPtr>();

        internal IntPtr String(string value)
        {
            Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(value);
            _strings.Add(pinned);
            return pinned.Pointer;
        }

        internal IntPtr StringList(params string[] values)
        {
            IntPtr list = NewList();
            foreach (string value in values)
            {
                Add(list, String(value));
            }

            return list;
        }

        internal IntPtr TopicPartitionList(params (string Topic, int Partition)[] partitions)
        {
            IntPtr list = NewList();
            foreach ((string topic, int partition) in partitions)
            {
                Add(list, TopicPartition(topic, partition));
            }

            return list;
        }

        internal IntPtr StringMap(params (string Key, string Value)[] entries)
        {
            IntPtr map = NewMap();
            foreach ((string key, string value) in entries)
            {
                Put(map, String(key), String(value));
            }

            return map;
        }

        internal IntPtr LongOffsetMap(params (string Topic, int Partition, long Offset)[] entries)
        {
            IntPtr map = NewMap();
            foreach ((string topic, int partition, long offset) in entries)
            {
                IntPtr value = Marshal.AllocHGlobal(sizeof(long));
                _longs.Add(value);
                Marshal.WriteInt64(value, offset);
                Put(map, TopicPartition(topic, partition), value);
            }

            return map;
        }

        internal IntPtr OffsetMap(params (string Topic, int Partition, long Offset, int LeaderEpoch, string Metadata)[] entries)
        {
            IntPtr map = NewMap();
            foreach ((string topic, int partition, long offset, int leaderEpoch, string metadata) in entries)
            {
                IntPtr metadataPointer = String(metadata);
                IntPtr value = Call((ref IntPtr outErr) =>
                    NativeMethods.OffsetAndMetadataWithLeaderEpochMetadata(offset, leaderEpoch, metadataPointer, ref outErr));
                _offsets.Add(value);
                Put(map, TopicPartition(topic, partition), value);
            }

            return map;
        }

        public void Dispose()
        {
            IntPtr outErr = IntPtr.Zero;
            foreach (IntPtr map in _maps)
            {
                NativeMethods.MapDestroy(map, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr list in _lists)
            {
                NativeMethods.ListDestroy(list, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr offset in _offsets)
            {
                NativeMethods.OffsetAndMetadataDestroy(offset, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr topicPartition in _topicPartitions)
            {
                NativeMethods.TopicPartitionDestroy(topicPartition, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr value in _longs)
            {
                Marshal.FreeHGlobal(value);
            }

            foreach (Utf8Marshal.PinnedUtf8String pinned in _strings)
            {
                pinned.Dispose();
            }
        }

        private IntPtr TopicPartition(string topic, int partition)
        {
            IntPtr topicPointer = String(topic);
            IntPtr handle = Call((ref IntPtr outErr) => NativeMethods.TopicPartitionNew(topicPointer, partition, ref outErr));
            _topicPartitions.Add(handle);
            return handle;
        }

        private IntPtr NewList()
        {
            IntPtr list = Call(NativeMethods.ListNew);
            _lists.Add(list);
            return list;
        }

        private IntPtr NewMap()
        {
            IntPtr map = Call(NativeMethods.MapNew);
            _maps.Add(map);
            return map;
        }

        private static void Add(IntPtr list, IntPtr element)
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ListAdd(list, element, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }

        private static void Put(IntPtr map, IntPtr key, IntPtr value)
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MapPut(map, key, value, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }
    }

    /// <summary>The C factories and destructors only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_common_Error_topic_authorization_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorTopicAuthorization(IntPtr topics, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_topic_authorization_message_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorTopicAuthorizationMessage(IntPtr topics, IntPtr message, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_invalid_topics_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorInvalidTopics(IntPtr topics, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_invalid_topics_message_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorInvalidTopicsMessage(IntPtr topics, IntPtr message, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_group_authorization_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorGroupAuthorization(IntPtr groupId, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_group_authorization_with_message_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorGroupAuthorizationWithMessage(IntPtr groupId, IntPtr message, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_throttling_quota_exceeded_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorThrottlingQuotaExceeded(int throttleTimeMs, IntPtr message, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_record_too_large_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorRecordTooLarge(IntPtr message, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_correlation_id_mismatch_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorCorrelationIdMismatch(
            IntPtr message, int requestCorrelationId, int responseCorrelationId, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ResourceNotFoundError_with_resource_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ResourceNotFoundWithResource(IntPtr resource, IntPtr message, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ResourceNotFoundError_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ResourceNotFoundDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_DuplicateResourceError_with_resource_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr DuplicateResourceWithResource(IntPtr resource, IntPtr message, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_DuplicateResourceError_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void DuplicateResourceDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_RecordTooLargeError_with_record_too_large_partitions_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr RecordTooLargeWithPartitions(IntPtr message, IntPtr partitions, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_RecordTooLargeError_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void RecordTooLargeDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_MetricName_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MetricNameNew(
            IntPtr name, IntPtr group, IntPtr description, IntPtr tags, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_MetricName_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void MetricNameDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_QuotaViolationError_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr QuotaViolationNew(IntPtr metricName, double value, double bound, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_QuotaViolationError_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void QuotaViolationDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ConsumerOffsetOutOfRangeError_with_message_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr OffsetOutOfRangeWithMessage(IntPtr message, IntPtr partitions, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ConsumerOffsetOutOfRangeError_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OffsetOutOfRangeDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ConsumerLogTruncationError_with_message_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr LogTruncationWithMessage(
            IntPtr message, IntPtr offsetOutOfRangePartitions, IntPtr divergentOffsets, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ConsumerLogTruncationError_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void LogTruncationDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ConsumerNoOffsetForPartitionError_with_partitions_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr NoOffsetForPartitionWithPartitions(IntPtr partitions, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ConsumerNoOffsetForPartitionError_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void NoOffsetForPartitionDestroy(IntPtr self, ref IntPtr outErr);
    }
}
