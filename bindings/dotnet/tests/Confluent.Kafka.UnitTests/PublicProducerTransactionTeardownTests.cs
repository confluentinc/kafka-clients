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
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M17/P1 S12: D11 — <c>Dispose</c> while <c>InitTransactions</c> is inside the core, on both real
/// flavours, broker-free, with <c>max.block.ms=3000</c>. <c>Dispose</c> returns, the operation
/// completes once with the outcome measured at CP5, and the producer's handle is closed afterwards
/// (the span-the-op and call-scoped references balanced).
/// </summary>
public sealed class PublicProducerTransactionTeardownTests
{
    // max.block.ms, plus the teardown path's own 30 s bound, plus slack.
    private static readonly TimeSpan s_bound = TimeSpan.FromSeconds(3 + 30 + 10);

    private static readonly TimeSpan s_releaseBound = TimeSpan.FromSeconds(30);

    private const string Concurrent = "Transactional methods of KafkaProducer are not safe for concurrent access.";

    private const string InitTimedOut =
        "Timeout expired after 3000ms while awaiting InitProducerId. InitTransactions timed out - did not complete "
        + "coordinator discovery or receive the InitProducerId response within max.block.ms.";

    /// <summary>
    /// Async: <c>InitTransactions</c> has submitted when it returns, so the disposal overlaps it
    /// for certain. The <see cref="Task"/> ends faulted with the core's timeout — the core lets the
    /// operation run out its <c>max.block.ms</c> — and is never stranded. The handle closes when
    /// the completion callback drops its reference, which it does on the dispatcher thread after
    /// the <see cref="Task"/> has faulted, so the test polls for it (2 ms step, 30 s bound).
    /// </summary>
    [Fact]
    public async Task Dispose_WhileInitTransactionsIsInFlight_Returns_AndTheTaskCompletesOnce()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            SafeProducerHandle handle = Handle(producer);
            Task init = producer.InitTransactions();
            Assert.False(init.IsCompleted, "InitTransactions completed before the disposal could overlap it");

            TestTimeout.Run(producer.Dispose, s_bound);

            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => init, s_bound));
            Assert.Equal(TaskStatus.Faulted, init.Status);
            Assert.Equal(7, failure.Code);
            Assert.Equal(InitTimedOut, failure.Message);
            await PollUntil(() => handle.IsClosed, s_releaseBound, "the producer handle is still open after Dispose and the completion");
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_bound);
        }
    }

    /// <summary>
    /// Sync: a worker is inside <c>InitTransactions()</c> — proven by D10's retry loop observing the
    /// core's -2 — when the test thread disposes. The blocked call's reference defers the native
    /// destroy until it returns, which it does with the core's timeout.
    /// </summary>
    [Fact]
    public async Task Dispose_WhileInitTransactionsIsInFlight_Returns_AndTheCallReturnsOnce_Sync()
    {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            SafeProducerHandle handle = Handle(producer);
            (KafkaException? refused, Task init) = PublicProducerTransactionConcurrencyTests.RetryUntilRefused(
                producer.CommitTransaction, () => Task.Run(producer.InitTransactions));
            Assert.NotNull(refused);
            Assert.Equal(-2, refused!.Code);
            Assert.Equal(Concurrent, refused.Message);

            TestTimeout.Run(producer.Dispose, s_bound);

            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => init, s_bound));
            Assert.Equal(7, failure.Code);
            Assert.Equal(InitTimedOut, failure.Message);
            Assert.True(handle.IsClosed, "the producer handle is still open after Dispose and the call");
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_bound);
        }
    }

    // Stops at the first observation; the bound only turns a missing release into a failure.
    private static async Task PollUntil(Func<bool> condition, TimeSpan timeout, string because)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!condition() && elapsed.Elapsed < timeout)
        {
            await Task.Delay(2);
        }

        Assert.True(condition(), because);
    }

    private static Dictionary<string, string> Config() => new Dictionary<string, string>
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["transactional.id"] = "teardown-txn",
        ["max.block.ms"] = "3000",
    };

    // Read while the producer is open: the accessor throws once it is closed.
    private static SafeProducerHandle Handle(object producer)
    {
        NativeProducer native = (NativeProducer)producer.GetType()
            .GetField("_native", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(producer)!;
        return native.Handle;
    }
}
