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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M17/P1 S11: D10 — a control call that overlaps another is rejected by the core with Code -2,
/// surfaced verbatim; the binding holds no lock of its own. Both real flavours, broker-free, with
/// <c>max.block.ms=5000</c> so the first call stays in the core for five seconds. The mocks are
/// excluded: their control calls finish too fast to open a window.
/// </summary>
public sealed class PublicProducerTransactionConcurrencyTests
{
    private const string Concurrent = "Transactional methods of KafkaProducer are not safe for concurrent access.";

    private const string InitTimedOut =
        "Timeout expired after 5000ms while awaiting InitProducerId. InitTransactions timed out - did not complete "
        + "coordinator discovery or receive the InitProducerId response within max.block.ms.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_retryStep = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// The deterministic async form: <c>InitTransactions</c> takes the core's control slot inside
    /// the call (no <c>Send</c> has happened, so there is nothing to drain), so a
    /// <c>CommitTransaction</c> issued after it returns is refused inline — its <see cref="Task"/>
    /// is already faulted when the call returns. Canceling the first call's token then cancels its
    /// awaiter (D5 row 3), and <c>Dispose</c> returns within its bound.
    /// </summary>
    [Fact]
    public async Task CommitTransaction_WhileInitTransactionsIsInTheCore_IsRefusedInline()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            using CancellationTokenSource cts = new CancellationTokenSource();
            Task first = producer.InitTransactions(cts.Token);

            Task second = producer.CommitTransaction();

            Assert.True(second.IsFaulted, $"the overlapping call was not refused inline ({second.Status})");
            KafkaException failure = Assert.IsType<KafkaException>(second.Exception!.InnerException);
            Assert.Equal(-2, failure.Code);
            Assert.Equal(Concurrent, failure.Message);

            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TestTimeout.Run(() => first, s_deadline));
            Assert.Equal(TaskStatus.Canceled, first.Status);
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    /// <summary>
    /// The sync twin (R10): a blocking call cannot signal that it has entered the core, so the test
    /// thread retries <c>CommitTransaction()</c> every 50 ms while a worker is inside
    /// <c>InitTransactions()</c>, and stops at the first -2. It asserts one was observed before the
    /// worker returned, and that the worker then fails with the core's timeout (Code 7).
    /// </summary>
    [Fact]
    public async Task CommitTransaction_WhileInitTransactionsIsInTheCore_IsRefused_Sync()
    {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(Config(), Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            (KafkaException? refused, Task worker) = RetryUntilRefused(producer.CommitTransaction, () => Task.Run(producer.InitTransactions));

            Assert.NotNull(refused);
            Assert.Equal(-2, refused!.Code);
            Assert.Equal(Concurrent, refused.Message);
            KafkaException timedOut = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => worker, s_deadline));
            Assert.Equal(7, timedOut.Code);
            Assert.Equal(InitTimedOut, timedOut.Message);
        }
        finally
        {
            TestTimeout.Run(producer.Dispose, s_deadline);
        }
    }

    // Starts `startWorker`, then retries `call` every 50 ms until the core refuses it with -2
    // (returned with the worker), or the worker finishes (null). Other core errors mean the call
    // landed before the worker entered the core, and retry. The race runs both ways: a worker that
    // arrives while a retried `call` holds the core's control slot is itself refused with -2 and
    // returns at once; it never entered the core, so it is started again.
    internal static (KafkaException? Refused, Task Worker) RetryUntilRefused(Action call, Func<Task> startWorker)
    {
        Stopwatch sw = Stopwatch.StartNew();
        Task worker = startWorker();
        while (true)
        {
            if (worker.IsCompleted)
            {
                if (worker.Exception?.InnerException is KafkaException { Code: -2 })
                {
                    worker = startWorker();
                }
                else
                {
                    return (null, worker);
                }
            }

            try
            {
                call();
            }
            catch (KafkaException failure) when (failure.Code == -2)
            {
                return (failure, worker);
            }
            catch (KafkaException)
            {
            }

            if (sw.Elapsed > s_deadline)
            {
                throw new TimeoutException("The worker neither returned nor was overlapped within the deadline — treated as a hang.");
            }

            Thread.Sleep(s_retryStep);
        }
    }

    private static Dictionary<string, string> Config() => new Dictionary<string, string>
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["transactional.id"] = "concurrent-txn",
        ["max.block.ms"] = "5000",
    };
}
