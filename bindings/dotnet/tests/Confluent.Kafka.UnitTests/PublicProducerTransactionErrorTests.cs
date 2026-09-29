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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M17/P1 S9: D6's classification end to end. The ten-row truth table is produced by the core
/// mock (<c>ErrorNext(code, message)</c> fails a pending send; the core builds the typed error)
/// and read back through the five predicates on both mocks; the commit hook (Python 995 / 1011)
/// on both; and the remarks' recovery compositions evaluated over the ten rows.
/// </summary>
public sealed class PublicProducerTransactionErrorTests
{
    private const string Topic = "txn-error-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly int[] s_tableCodes = { 120, 90, 47, 29, 53, 35, 45, 59, 7, 48 };

    // ---- D6 truth table ----

    /// <summary>
    /// One D6 row: a pending send failed by the core with <paramref name="code"/> surfaces a
    /// <see cref="KafkaException"/> with that code, the message given, and exactly the row's
    /// predicate answers.
    /// </summary>
    [Theory]
    [MemberData(nameof(TruthTable))]
    public async Task ErrorNext_ReportsTheRowsClassification(
        string flavour,
        int code,
        bool abortable,
        bool applicationRecoverable,
        bool invalidConfiguration,
        bool authorization,
        bool outOfOrder,
        bool retriable)
    {
        string message = $"classified row {code}";

        KafkaException failure = await FailASend(flavour, code, message);

        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
        Assert.Equal(abortable, failure.IsTransactionAbortableError);
        Assert.Equal(applicationRecoverable, failure.IsApplicationRecoverableError);
        Assert.Equal(invalidConfiguration, failure.IsInvalidConfigurationError);
        Assert.Equal(authorization, failure.IsAuthorizationError);
        Assert.Equal(outOfOrder, failure.IsOutOfOrderSequenceError);
        Assert.Equal(retriable, failure.IsRetriable);
    }

    /// <summary>D6's table, once per mock.</summary>
    public static IEnumerable<object[]> TruthTable()
    {
        object[][] rows =
        {
            new object[] { 120, true, false, false, false, false, false },
            new object[] { 90, false, true, false, false, false, false },
            new object[] { 47, false, true, false, false, false, false },
            new object[] { 29, false, false, true, true, false, false },
            new object[] { 53, false, false, true, true, false, false },
            new object[] { 35, false, false, true, false, false, false },
            new object[] { 45, false, false, false, false, true, false },
            new object[] { 59, false, false, false, false, true, false },
            new object[] { 7, false, false, false, false, false, true },
            new object[] { 48, false, false, false, false, false, false },
        };
        foreach (string flavour in new[] { "sync", "async" })
        {
            foreach (object[] row in rows)
            {
                yield return new object[] { flavour }.Concat(row).ToArray();
            }
        }
    }

    // ---- the commit hook (Python 995 / 1011) ----

    /// <summary>
    /// Python 995 <c>test_txn_commit_failure_requires_abort</c> (and the async 1437): an abortable
    /// commit failure carries code 120, its message and <c>IsTransactionAbortableError</c>; the
    /// abort then succeeds, and once the hook is cleared a fresh transaction commits.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task CommitTransaction_WhenAbortable_IsRecoveredByAborting(string flavour)
    {
        using IDisposable owner = CreateTransactional(flavour, out Func<Task> init, out Action begin, out Func<Task> commit, out Func<Task> abort, out Action<int, string> setError, out Action clearError);
        await Run(init());
        begin();
        setError(120, "commit failed abortably");

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Run(commit()));

        Assert.Equal(120, failure.Code);
        Assert.Equal("commit failed abortably", failure.Message);
        Assert.True(failure.IsTransactionAbortableError);
        Assert.False(failure.IsRetriable);
        await Run(abort());
        clearError();
        begin();
        await Run(commit());
    }

    /// <summary>
    /// Python 1011 <c>test_txn_commit_failure_non_abortable</c>: a timed-out commit is not
    /// abortable and is retriable.
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task CommitTransaction_WhenTimedOut_IsRetriableAndNotAbortable(string flavour)
    {
        using IDisposable owner = CreateTransactional(flavour, out Func<Task> init, out Action begin, out Func<Task> commit, out _, out Action<int, string> setError, out _);
        await Run(init());
        begin();
        setError(7, "commit timed out");

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Run(commit()));

        Assert.Equal(7, failure.Code);
        Assert.Equal("commit timed out", failure.Message);
        Assert.False(failure.IsTransactionAbortableError);
        Assert.True(failure.IsRetriable);
    }

    // ---- the remarks' recovery compositions (R15) ----

    /// <summary>
    /// The two compositions the <see cref="IProducer{TKey, TValue}"/> remarks give, evaluated over
    /// the ten classified rows, select exactly the "close" sets D6 states: the literal form
    /// {90, 45, 59, 29, 53}; the KIP-1050 form that set plus 47 (the other application-recoverable
    /// codes are not among the ten). Only 120 selects "abort and retry".
    /// </summary>
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task RecoveryCompositions_SelectTheDocumentedSets(string flavour)
    {
        List<KafkaException> failures = new List<KafkaException>();
        foreach (int code in s_tableCodes)
        {
            failures.Add(await FailASend(flavour, code, $"composition row {code}"));
        }

        int[] literalClose = failures
            .Where(ex => ex.Code == 90 || ex.IsOutOfOrderSequenceError || ex.IsAuthorizationError)
            .Select(ex => ex.Code).OrderBy(c => c).ToArray();
        int[] kip1050Close = failures
            .Where(ex => ex.IsApplicationRecoverableError || ex.IsOutOfOrderSequenceError || ex.IsAuthorizationError)
            .Select(ex => ex.Code).OrderBy(c => c).ToArray();
        int[] abortAndRetry = failures.Where(ex => ex.IsTransactionAbortableError).Select(ex => ex.Code).ToArray();

        Assert.Equal(new[] { 29, 45, 53, 59, 90 }, literalClose);
        Assert.Equal(new[] { 29, 45, 47, 53, 59, 90 }, kip1050Close);
        Assert.Equal(new[] { 120 }, abortAndRetry);
    }

    // ---- helpers ----

    // Fails one pending manual send through the core mock's ErrorNext and returns what the send
    // surfaced. Async: the send's Task faults. Sync: the blocking Send, on a worker, throws.
    private static async Task<KafkaException> FailASend(string flavour, int code, string message)
    {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(Topic, new byte[] { 0x01 }, partition: 0);
        if (flavour == "async")
        {
            using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
            Task<RecordMetadata> send = producer.Send(record);
            producer.WaitForSendsToReachCore(s_deadline);
            Assert.True(producer.ErrorNext(code, message), "no pending send to fail");
            return await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => send, s_deadline));
        }

        using MockProducer<byte[], byte[]> syncProducer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        Task<RecordMetadata> worker = Task.Run(() => syncProducer.Send(record));
        DriveUntilResolved(() => syncProducer.ErrorNext(code, message));
        return await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => worker, s_deadline));
    }

    // The same retry-until-registered drive as PublicSyncProducerMockControlTests.DriveUntilResolved:
    // a blocking Send cannot signal that it has reached the core.
    private static void DriveUntilResolved(Func<bool> drive)
    {
        Stopwatch sw = Stopwatch.StartNew();
        while (!drive())
        {
            if (sw.Elapsed > s_deadline)
            {
                throw new TimeoutException("No pending send registered to drive within the deadline — treated as a hang.");
            }

            Thread.Sleep(2);
        }
    }

    private static IDisposable CreateTransactional(
        string flavour,
        out Func<Task> init,
        out Action begin,
        out Func<Task> commit,
        out Func<Task> abort,
        out Action<int, string> setError,
        out Action clearError)
    {
        if (flavour == "async")
        {
            AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
            init = () => producer.InitTransactions();
            begin = producer.BeginTransaction;
            commit = () => producer.CommitTransaction();
            abort = () => producer.AbortTransaction();
            setError = (code, message) => producer.SetCommitTransactionError(code, message);
            clearError = producer.ClearCommitTransactionError;
            return producer;
        }

        MockProducer<byte[], byte[]> syncProducer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        init = () => Inline(syncProducer.InitTransactions);
        begin = syncProducer.BeginTransaction;
        commit = () => Inline(syncProducer.CommitTransaction);
        abort = () => Inline(syncProducer.AbortTransaction);
        setError = (code, message) => syncProducer.SetCommitTransactionError(code, message);
        clearError = syncProducer.ClearCommitTransactionError;
        return syncProducer;
    }

    private static Task Inline(Action action)
    {
        try
        {
            action();
            return Task.CompletedTask;
        }
        catch (Exception failure)
        {
            return Task.FromException(failure);
        }
    }

    private static Task Run(Task task) => TestTimeout.Run(() => task, s_deadline);
}
