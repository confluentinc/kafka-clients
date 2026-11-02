// Copyright 2025 Confluent Inc.
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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The offset-commit callback through the public API of both mock consumers: what <c>OnComplete</c> receives, where
/// it runs, its exceptions, the preconditions, and the overloads each consumer interface declares.
/// </summary>
public sealed class PublicConsumerCommitCallbackTests
{
    private const string Topic = "commit-callback-topic";

    private const string PumpThreadName = "confluent-kafka-consumer-pump";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void CommitAsyncWithOffsetsAndCallback_DeliversThoseOffsetsAndANullException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition first = new TopicPartition(Topic, 0);
        TopicPartition second = new TopicPartition(Topic, 1);
        consumer.Assign(new[] { first, second });
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [first] = new OffsetAndMetadata(42, "meta-x", 7),
                [second] = new OffsetAndMetadata(9),
            },
            callback);

        Completion completion = Assert.Single(callback.Completions);
        Assert.Null(completion.Exception);
        Assert.NotNull(completion.Offsets);
        Assert.Equal(2, completion.Offsets!.Count);
        Assert.Equal(42, completion.Offsets[first].Offset);
        Assert.Equal("meta-x", completion.Offsets[first].Metadata);
        Assert.Equal(7, completion.Offsets[first].LeaderEpoch);
        Assert.Equal(9, completion.Offsets[second].Offset);
        Assert.Equal(string.Empty, completion.Offsets[second].Metadata);
        Assert.Null(completion.Offsets[second].LeaderEpoch);
    }

    [Fact]
    public void CommitAsyncWithCallback_RunsInsideTheCall_OnTheCallersThread()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(callback);

        Completion completion = Assert.Single(callback.Completions);
        Assert.Same(Thread.CurrentThread, completion.Thread);
        Assert.Null(completion.Exception);
        Assert.NotNull(completion.Offsets);
        Assert.Empty(completion.Offsets!);
    }

    [Fact]
    public void CommitAsyncWithOffsetsAndCallback_EmptyMap_DeliversTheEmptyMap()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(new Dictionary<TopicPartition, OffsetAndMetadata>(), callback);

        Completion completion = Assert.Single(callback.Completions);
        Assert.NotNull(completion.Offsets);
        Assert.Empty(completion.Offsets!);
        Assert.Null(completion.Exception);
    }

    [Fact]
    public async Task AsyncMock_CommitAsyncWithOffsetsAndCallback_DeliversThoseOffsetsOnThePumpThread()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        TopicPartition partition = new TopicPartition(Topic, 2);
        await TestTimeout.Run(() => consumer.Assign(new[] { partition }), s_deadline);
        RecordingCommitCallback callback = new RecordingCommitCallback();

        await TestTimeout.Run(
            () => consumer.CommitAsync(
                new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(5, "m") },
                callback),
            s_deadline);
        Completion completion = await TestTimeout.Run(() => callback.First, s_deadline);

        Assert.Equal(PumpThreadName, completion.Thread.Name);
        Assert.Null(completion.Exception);
        Assert.Equal(5, completion.Offsets![partition].Offset);
        Assert.Equal("m", completion.Offsets[partition].Metadata);
    }

    [Fact]
    public async Task AsyncMock_CommitAsyncWithCallback_RunsOnThePumpThread()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        RecordingCommitCallback callback = new RecordingCommitCallback();

        await TestTimeout.Run(() => consumer.CommitAsync(callback), s_deadline);
        Completion completion = await TestTimeout.Run(() => callback.First, s_deadline);

        Assert.Equal(PumpThreadName, completion.Thread.Name);
        Assert.Null(completion.Exception);
        Assert.Empty(completion.Offsets!);
    }

    [Fact]
    public void CommitAsyncWithCallback_AfterAPoll_DeliversTheConsumedPositions()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition partition = new TopicPartition(Topic, 0);
        consumer.Assign(new[] { partition });
        consumer.Seek(partition, 0);
        consumer.AddRecord(Topic, 0, offset: 0, new[] { (byte)'k' }, new[] { (byte)'v' });
        Assert.Single(consumer.Poll(TimeSpan.Zero));
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(callback);

        AssertTheConsumedPosition(partition, Assert.Single(callback.Completions));
    }

    [Fact]
    public async Task AsyncMock_CommitAsyncWithCallback_AfterAPoll_DeliversTheConsumedPositionsOnThePumpThread()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        TopicPartition partition = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { partition }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(partition, 0), s_deadline);
        consumer.AddRecord(Topic, 0, offset: 0, new[] { (byte)'k' }, new[] { (byte)'v' });
        Assert.Single(await TestTimeout.Run(() => consumer.Poll(TimeSpan.Zero), s_deadline));
        RecordingCommitCallback callback = new RecordingCommitCallback();

        await TestTimeout.Run(() => consumer.CommitAsync(callback), s_deadline);
        Completion completion = await TestTimeout.Run(() => callback.First, s_deadline);

        Assert.Equal(PumpThreadName, completion.Thread.Name);
        AssertTheConsumedPosition(partition, Assert.Single(callback.Completions));
    }

    [Fact]
    public async Task AsyncMock_CommitAsyncWithOffsets_NoCallback_StillCommits()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        TopicPartition partition = new TopicPartition(Topic, 6);
        await TestTimeout.Run(() => consumer.Assign(new[] { partition }), s_deadline);

        await TestTimeout.Run(
            () => consumer.CommitAsync(
                new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(17) }),
            s_deadline);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
            await TestTimeout.Run(() => consumer.Committed(new[] { partition }), s_deadline);
        Assert.Equal(17, committed[partition]!.Offset);
    }

    [Fact]
    public void CommitAsyncWithOffsets_NoCallback_StillCommits()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition partition = new TopicPartition(Topic, 3);
        consumer.Assign(new[] { partition });

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [partition] = new OffsetAndMetadata(31, "no-callback-meta", 2),
            });

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed = consumer.Committed(new[] { partition });
        Assert.Equal(31, committed[partition]!.Offset);
        Assert.Equal("no-callback-meta", committed[partition]!.Metadata);
        Assert.Equal(2, committed[partition]!.LeaderEpoch);
    }

    [Fact]
    public void CommitAsyncWithOffsets_NoCallback_Churned_StaysHealthy()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition partition = new TopicPartition(Topic, 4);
        consumer.Assign(new[] { partition });

        for (int i = 0; i < 500; i++)
        {
            consumer.CommitAsync(
                new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(i) });
        }

        Assert.Equal(499, consumer.Committed(new[] { partition })[partition]!.Offset);
    }

    [Fact]
    public void ThrowingCallback_PropagatesTheSameInstanceOutOfTheCallThatRanIt_AndTheCommitLands()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition partition = new TopicPartition(Topic, 5);
        consumer.Assign(new[] { partition });
        InvalidCastException thrown = new InvalidCastException("boom");
        ThrowingCommitCallback throwing = new ThrowingCommitCallback(thrown);

        Exception? propagated = Record.Exception(() => consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(3) },
            throwing));

        Assert.Same(thrown, propagated);
        Assert.Equal(1, throwing.Invocations);
        Assert.Equal(3, consumer.Committed(new[] { partition })[partition]!.Offset);

        RecordingCommitCallback recording = new RecordingCommitCallback();
        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(4) },
            recording);
        Assert.Single(recording.Completions);
        Assert.Equal(4, consumer.Committed(new[] { partition })[partition]!.Offset);
    }

    [Fact]
    public void NonAsciiTopic_RoundTripsThroughTheDeliveredOffsets()
    {
        const string NonAscii = "témas-日本語-🎉";
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        TopicPartition partition = new TopicPartition(NonAscii, 0);
        consumer.Assign(new[] { partition });
        RecordingCommitCallback callback = new RecordingCommitCallback();

        consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [partition] = new OffsetAndMetadata(12, "métadonnées-メタ"),
            },
            callback);

        KeyValuePair<TopicPartition, OffsetAndMetadata> entry = Assert.Single(Assert.Single(callback.Completions).Offsets!);
        Assert.Equal(NonAscii, entry.Key.Topic);
        Assert.Equal(0, entry.Key.Partition);
        Assert.Equal(12, entry.Value.Offset);
        Assert.Equal("métadonnées-メタ", entry.Value.Metadata);
    }

    [Fact]
    public void CommitAsync_NullCallback_ThrowsArgumentNullException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentNullException error =
            Assert.Throws<ArgumentNullException>(() => consumer.CommitAsync((IOffsetCommitCallback)null!));

        Assert.Equal("callback", error.ParamName);
    }

    [Fact]
    public async Task AsyncMock_CommitAsync_NullCallback_ThrowsArgumentNullExceptionSynchronously()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => { _ = consumer.CommitAsync((IOffsetCommitCallback)null!); });

        Assert.Equal("callback", error.ParamName);
    }

    [Fact]
    public void CommitAsync_NullOffsets_ThrowsArgumentNullException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => consumer.CommitAsync(
                (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>)null!,
                new RecordingCommitCallback()));

        Assert.Equal("offsets", error.ParamName);
    }

    [Fact]
    public async Task AsyncMock_CommitAsync_NullOffsets_ThrowsArgumentNullExceptionSynchronously()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => { _ = consumer.CommitAsync((IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>)null!, new RecordingCommitCallback()); });

        Assert.Equal("offsets", error.ParamName);
    }

    [Fact]
    public void CommitAsync_NegativePartition_ThrowsArgumentOutOfRangeBeforeAnyNativeCall()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        RecordingCommitCallback callback = new RecordingCommitCallback();

        ArgumentOutOfRangeException error = Assert.Throws<ArgumentOutOfRangeException>(
            () => consumer.CommitAsync(
                new Dictionary<TopicPartition, OffsetAndMetadata>
                {
                    [new TopicPartition(Topic, -1)] = new OffsetAndMetadata(1),
                },
                callback));

        Assert.Equal("offsets", error.ParamName);
        Assert.Equal(-1, error.ActualValue);
        Assert.StartsWith("Partition must not be negative.", error.Message, StringComparison.Ordinal);
        Assert.Empty(callback.Completions);
    }

    [Fact]
    public void CommitAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.CommitAsync(new RecordingCommitCallback()));
        Assert.Throws<ObjectDisposedException>(() => consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>()));
    }

    [Fact]
    public async Task AsyncMock_CommitAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        await TestTimeout.DisposeAsync(consumer);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.CommitAsync(new RecordingCommitCallback()));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.CommitAsync(
            new Dictionary<TopicPartition, OffsetAndMetadata>(), new RecordingCommitCallback()));
    }

    [Fact]
    public void TheCallbackOverloads_AreDeclaredByEachFlavorsInterface_NotByTheSharedBase()
    {
        Assert.DoesNotContain(typeof(IConsumerCommon).GetMethods(), m => m.Name == "CommitAsync");

        MethodInfo syncCallback = Single(typeof(IConsumer<byte[], byte[]>), typeof(IOffsetCommitCallback));
        MethodInfo syncOffsets = Single(
            typeof(IConsumer<byte[], byte[]>),
            typeof(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>),
            typeof(IOffsetCommitCallback));
        Assert.Equal(typeof(void), syncCallback.ReturnType);
        Assert.Equal(typeof(void), syncOffsets.ReturnType);
        Assert.True(syncOffsets.GetParameters()[1].HasDefaultValue);

        MethodInfo asyncCallback = Single(
            typeof(IAsyncConsumer<byte[], byte[]>), typeof(IOffsetCommitCallback), typeof(CancellationToken));
        MethodInfo asyncOffsets = Single(
            typeof(IAsyncConsumer<byte[], byte[]>),
            typeof(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>),
            typeof(IOffsetCommitCallback),
            typeof(CancellationToken));
        Assert.Equal(typeof(Task), asyncCallback.ReturnType);
        Assert.Equal(typeof(Task), asyncOffsets.ReturnType);
        Assert.True(asyncOffsets.GetParameters()[1].HasDefaultValue);
    }

    [Fact]
    public void OnComplete_TakesNullableOffsetsAndAnException_AndReturnsVoid()
    {
        MethodInfo onComplete = Assert.Single(typeof(IOffsetCommitCallback).GetMethods());
        Assert.Equal(nameof(IOffsetCommitCallback.OnComplete), onComplete.Name);
        Assert.Equal(typeof(void), onComplete.ReturnType);
        ParameterInfo[] parameters = onComplete.GetParameters();
        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>), typeof(Exception) },
            parameters.Select(p => p.ParameterType).ToArray());
    }

    private static MethodInfo Single(Type surface, params Type[] parameters) =>
        Assert.Single(
            surface.GetMethods(),
            m => m.Name == "CommitAsync" && m.GetParameters().Select(p => p.ParameterType).SequenceEqual(parameters));

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static void AssertTheConsumedPosition(TopicPartition partition, Completion completion)
    {
        Assert.Null(completion.Exception);
        KeyValuePair<TopicPartition, OffsetAndMetadata> entry = Assert.Single(completion.Offsets!);
        Assert.Equal(partition, entry.Key);
        Assert.Equal(new OffsetAndMetadata(1), entry.Value);
        Assert.Equal(1L, entry.Value.Offset);
        Assert.Equal(string.Empty, entry.Value.Metadata);
        Assert.Null(entry.Value.LeaderEpoch);
    }

    /// <summary>One completion: its offsets, its exception and the thread it ran on.</summary>
    private sealed class Completion
    {
        internal Completion(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception)
        {
            Offsets = offsets;
            Exception = exception;
        }

        internal IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? Offsets { get; }

        internal Exception? Exception { get; }

        internal Thread Thread { get; } = Thread.CurrentThread;
    }

    /// <summary>Records every completion, and exposes the first as a task.</summary>
    private sealed class RecordingCommitCallback : IOffsetCommitCallback
    {
        private readonly TaskCompletionSource<Completion> _first =
            new TaskCompletionSource<Completion>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal List<Completion> Completions { get; } = new List<Completion>();

        internal Task<Completion> First => _first.Task;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception)
        {
            Completion completion = new Completion(offsets, exception);
            lock (Completions)
            {
                Completions.Add(completion);
            }

            _first.TrySetResult(completion);
        }
    }

    /// <summary>A commit callback that counts its calls and throws the exception it is given.</summary>
    private sealed class ThrowingCommitCallback : IOffsetCommitCallback
    {
        private readonly Exception _exception;

        internal ThrowingCommitCallback(Exception exception) => _exception = exception;

        internal int Invocations { get; private set; }

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception)
        {
            Invocations++;
            throw _exception;
        }
    }
}
