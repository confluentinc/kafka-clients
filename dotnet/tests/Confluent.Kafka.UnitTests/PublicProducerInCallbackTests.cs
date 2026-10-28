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
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The waits called from inside a delivery callback, on <see cref="KafkaProducer{TKey, TValue}"/> and
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/>: <c>Flush</c> is refused, a queued wait ends after
/// <c>max.block.ms</c>, and <c>Close</c> closes with a 0 ms timeout and tears down after the callback. The callback
/// is a record's failure on an unreachable broker.
/// </summary>
public sealed class PublicProducerInCallbackTests
{
    private const int MaxBlockMs = 500;

    private const string FlushMessage =
        "KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.";

    private const string CloseOverride =
        "Overriding close timeout {0} ms to 0 ms in order to prevent useless blocking due to " +
        "self-join. This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.";

    private const string Close = "Close";
    private const string CloseWithTimeout = "Close(TimeSpan)";
    private const string CloseWithZeroTimeout = "Close(TimeSpan.Zero)";
    private const string Dispose = "Dispose";

    private static readonly TimeSpan s_closeTimeout = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task Flush_FromADeliveryCallback_ThrowsJavasDeadlockMessage(string flavor)
    {
        Exception? refused = null;
        bool faultedInCall = false;
        await Run(flavor, "in-callback-flush", producer =>
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            if (producer is KafkaProducer<byte[], byte[]> sync)
            {
                refused = Record(() => sync.Flush());
            }
            else
            {
                Task flush = ((AsyncKafkaProducer<byte[], byte[]>)producer).Flush();
                faultedInCall = flush.IsFaulted;
                refused = flush.Exception?.InnerException;
            }

            Assert.True(elapsed.ElapsedMilliseconds < MaxBlockMs, $"Flush waited {elapsed.ElapsedMilliseconds} ms");
        });

        KafkaException error = Assert.IsType<KafkaException>(refused);
        Assert.Equal(FlushMessage, error.Message);
        Assert.Equal(-1, error.Code);
        Assert.False(error.IsRetriable);
        Assert.Equal(flavor == AsyncFlavor, faultedInCall);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task PartitionsFor_FromADeliveryCallback_EndsAfterMaxBlockMs_WithTheCallbackTimeout(string flavor)
    {
        string topic = Topic("in-callback-partitions-for");
        Exception? timedOut = null;
        long elapsedMs = 0;
        await Run(flavor, "in-callback-partitions-for-vehicle", producer =>
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            timedOut = producer is KafkaProducer<byte[], byte[]> sync
                ? Record(() => sync.PartitionsFor(topic))
                : Record(() => WaitFor(((AsyncKafkaProducer<byte[], byte[]>)producer).PartitionsFor(topic)));
            elapsedMs = elapsed.ElapsedMilliseconds;
        });

        KafkaException error = Assert.IsType<KafkaException>(timedOut);
        Assert.Equal(7, error.Code);
        Assert.True(error.IsRetriable);
        Assert.Equal(
            $"Timeout expired after {MaxBlockMs}ms while awaiting the metadata of topic {topic} from a send() " +
            "callback, which blocks the producer's completion thread",
            error.Message);
        Assert.InRange(elapsedMs, MaxBlockMs - 50, (long)Deadline.TotalMilliseconds);
    }

    [Theory]
    [InlineData(SyncFlavor, Close)]
    [InlineData(SyncFlavor, CloseWithTimeout)]
    [InlineData(SyncFlavor, CloseWithZeroTimeout)]
    [InlineData(SyncFlavor, Dispose)]
    [InlineData(AsyncFlavor, Close)]
    [InlineData(AsyncFlavor, CloseWithTimeout)]
    [InlineData(AsyncFlavor, CloseWithZeroTimeout)]
    [InlineData(AsyncFlavor, Dispose)]
    public async Task Close_FromADeliveryCallback_ClosesWithAZeroTimeout_AndTearsDownAfterTheCallback(
        string flavor,
        string how)
    {
        using TraceCapture trace = new TraceCapture("Overriding close timeout");
        int destroysBeforeTheCallbackReturned = -1;
        int closingThread = -1;
        long closeMs = -1;
        bool closeTaskDone = false;
        object? producerSeen = null;
        DestroyLog? destroys = null;
        await Run(
            flavor,
            "in-callback-close",
            producer =>
            {
                closingThread = Environment.CurrentManagedThreadId;
                Stopwatch elapsed = Stopwatch.StartNew();
                if (producer is KafkaProducer<byte[], byte[]> sync)
                {
                    switch (how)
                    {
                        case Close:
                            sync.Close();
                            break;
                        case CloseWithTimeout:
                            sync.Close(s_closeTimeout);
                            break;
                        case CloseWithZeroTimeout:
                            sync.Close(TimeSpan.Zero);
                            break;
                        default:
                            sync.Dispose();
                            break;
                    }

                    closeTaskDone = true;
                }
                else
                {
                    AsyncKafkaProducer<byte[], byte[]> async = (AsyncKafkaProducer<byte[], byte[]>)producer;
                    closeTaskDone = how switch
                    {
                        Close => async.Close().IsCompleted,
                        CloseWithTimeout => async.Close(s_closeTimeout).IsCompleted,
                        CloseWithZeroTimeout => async.Close(TimeSpan.Zero).IsCompleted,
                        _ => async.DisposeAsync().IsCompleted,
                    };
                }

                closeMs = elapsed.ElapsedMilliseconds;
                Thread.Sleep(Settle);
                destroysBeforeTheCallbackReturned = destroys!.Destroys().Count;
            },
            producer =>
            {
                producerSeen = producer;
                destroys = WatchDestroy(producer);
            });

        Assert.True(closeTaskDone, "Close from the callback did not return a completed close");
        Assert.InRange(closeMs, 0, MaxBlockMs);
        if (how == CloseWithZeroTimeout)
        {
            Assert.Empty(trace.LinesFrom(closingThread));
        }
        else
        {
            long overridden = how == CloseWithTimeout ? (long)s_closeTimeout.TotalMilliseconds : long.MaxValue;
            Assert.Equal(
                string.Format(CultureInfo.InvariantCulture, CloseOverride, overridden),
                Assert.Single(trace.LinesFrom(closingThread)));
        }

        Assert.True(destroys!.WaitForADestroy(Deadline), "the teardown never ran after the callback");
        (int _, string? destroyThread) = Assert.Single(destroys.Destroys());
        Assert.Equal(flavor == SyncFlavor ? NativeProducer.CompletionThreadName : NativeProducer.PumpThreadName, destroyThread);
        Assert.Equal(0, destroysBeforeTheCallbackReturned);
        Assert.Throws<ObjectDisposedException>(() => SendAfterClose(producerSeen!));
    }

    private static IReadOnlyList<PartitionInfo> WaitFor(Task<IReadOnlyList<PartitionInfo>> partitions) =>
        partitions.GetAwaiter().GetResult();

    private static Exception? Record(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception failure)
        {
            return failure;
        }
    }

    private static void SendAfterClose(object producer)
    {
        if (producer is KafkaProducer<byte[], byte[]> sync)
        {
            _ = sync.Send(UnreachableProducers.Record("in-callback-after-close"));
        }
        else
        {
            _ = ((AsyncKafkaProducer<byte[], byte[]>)producer).Send(UnreachableProducers.Record("in-callback-after-close"));
        }
    }

    private static async Task Run(
        string flavor,
        string topicPrefix,
        Action<object> inCallback,
        Action<object>? beforeSend = null)
    {
        object producer = flavor == SyncFlavor ? Sync(MaxBlockMs) : Async(MaxBlockMs);
        ProbeCallback callback = new ProbeCallback((_, _) => inCallback(producer));
        try
        {
            beforeSend?.Invoke(producer);
            ProducerRecord<byte[], byte[]> record = UnreachableProducers.Record(Topic(topicPrefix));
            if (producer is KafkaProducer<byte[], byte[]> sync)
            {
                _ = sync.Send(record, callback);
            }
            else
            {
                _ = await TestTimeout.Run(
                    () => ((AsyncKafkaProducer<byte[], byte[]>)producer).Send(record, callback).AsTask(),
                    Deadline);
            }

            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Null(callback.BodyFailure);
            Assert.Equal(1, callback.Calls);
            Assert.Equal(
                flavor == SyncFlavor ? NativeProducer.CompletionThreadName : NativeProducer.PumpThreadName,
                callback.ThreadName);
        }
        finally
        {
            if (producer is KafkaProducer<byte[], byte[]> sync)
            {
                TestTimeout.Run(sync.Dispose, Deadline);
            }
            else
            {
                await TestTimeout.Run(() => ((AsyncKafkaProducer<byte[], byte[]>)producer).DisposeAsync().AsTask(), Deadline);
            }
        }
    }
}
