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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The five transaction operations, built as <c>Flush</c> is: the closed check, the mock's in-drain branch, then the
/// <c>_cb</c> form and a managed wait. The core checks the arguments and the state, and rejects an operation that
/// overlaps another one on the same producer.
/// </summary>
internal sealed partial class NativeProducer
{
    // The operation names of the in-callback timeout message, Python's (producer.py, async_producer.py).
    private const string InitTransactionsOperation = "InitProducerId";

    private const string BeginTransactionOperation = "beginTransaction";

    private const string SendOffsetsOperation = "AddOffsetsToTxn";

    private const string CommitTransactionOperation = "EndTxn(true)";

    private const string AbortTransactionOperation = "EndTxn(false)";

    private delegate void BlockingCall(ref IntPtr outErr);

    internal void InitTransactions() =>
        Transaction(
            InitTransactionsOperation,
            waitsForReports: false,
            opaque => NativeMethods.ProducerInitTransactionsCb(_handle, s_voidCompletion, opaque),
            (ref IntPtr outErr) => NativeMethods.ProducerInitTransactions(_handle, ref outErr));

    internal void BeginTransaction() =>
        Transaction(
            BeginTransactionOperation,
            waitsForReports: false,
            opaque => NativeMethods.ProducerBeginTransactionCb(_handle, s_voidCompletion, opaque),
            (ref IntPtr outErr) => NativeMethods.ProducerBeginTransaction(_handle, ref outErr));

    internal void SendOffsetsToTransaction(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets,
        ConsumerGroupMetadata? groupMetadata)
    {
        ThrowIfClosed();
        using TransactionOffsets arguments = new TransactionOffsets(offsets, groupMetadata);
        IntPtr map = arguments.Offsets;
        IntPtr metadata = arguments.GroupMetadata;
        Transaction(
            SendOffsetsOperation,
            waitsForReports: false,
            opaque => NativeMethods.ProducerSendOffsetsToTransactionCb(_handle, map, metadata, s_voidCompletion, opaque),
            (ref IntPtr outErr) => NativeMethods.ProducerSendOffsetsToTransaction(_handle, map, metadata, ref outErr));
    }

    internal void CommitTransaction() =>
        Transaction(
            CommitTransactionOperation,
            waitsForReports: true,
            opaque => NativeMethods.ProducerCommitTransactionCb(_handle, s_voidCompletion, opaque),
            (ref IntPtr outErr) => NativeMethods.ProducerCommitTransaction(_handle, ref outErr));

    internal void AbortTransaction() =>
        Transaction(
            AbortTransactionOperation,
            waitsForReports: true,
            opaque => NativeMethods.ProducerAbortTransactionCb(_handle, s_voidCompletion, opaque),
            (ref IntPtr outErr) => NativeMethods.ProducerAbortTransaction(_handle, ref outErr));

    internal Task InitTransactionsWithCallback(CancellationToken cancellationToken = default) =>
        TransactionWithCallback(
            InitTransactionsOperation,
            waitsForReports: false,
            opaque => NativeMethods.ProducerInitTransactionsCb(_handle, s_voidCompletion, opaque),
            (ref IntPtr outErr) => NativeMethods.ProducerInitTransactions(_handle, ref outErr),
            cancellationToken);

    internal Task SendOffsetsToTransactionWithCallback(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets,
        ConsumerGroupMetadata? groupMetadata,
        CancellationToken cancellationToken = default)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();

        // The core copies the map and takes its own reference on the metadata during the call, so both are freed
        // once the submit has returned.
        using TransactionOffsets arguments = new TransactionOffsets(offsets, groupMetadata);
        IntPtr map = arguments.Offsets;
        IntPtr metadata = arguments.GroupMetadata;
        return TransactionWithCallback(
            SendOffsetsOperation,
            waitsForReports: false,
            opaque => NativeMethods.ProducerSendOffsetsToTransactionCb(_handle, map, metadata, s_voidCompletion, opaque),
            (ref IntPtr outErr) => NativeMethods.ProducerSendOffsetsToTransaction(_handle, map, metadata, ref outErr),
            cancellationToken);
    }

    // Commit and abort flush, so they also wait for the async callbacks of serializer failures, as Flush does.
    internal Task CommitTransactionWithCallback(CancellationToken cancellationToken = default) =>
        _failureCallbacks.After(
            TransactionWithCallback(
                CommitTransactionOperation,
                waitsForReports: true,
                opaque => NativeMethods.ProducerCommitTransactionCb(_handle, s_voidCompletion, opaque),
                (ref IntPtr outErr) => NativeMethods.ProducerCommitTransaction(_handle, ref outErr),
                cancellationToken),
            cancellationToken);

    internal Task AbortTransactionWithCallback(CancellationToken cancellationToken = default) =>
        _failureCallbacks.After(
            TransactionWithCallback(
                AbortTransactionOperation,
                waitsForReports: true,
                opaque => NativeMethods.ProducerAbortTransactionCb(_handle, s_voidCompletion, opaque),
                (ref IntPtr outErr) => NativeMethods.ProducerAbortTransaction(_handle, ref outErr),
                cancellationToken),
            cancellationToken);

    // On a mock, the blocking form from a sync delivery callback that the drain runs: the core sees the callback and
    // does not wait for the reports, and a nested drain runs no other callback. From a mock callback the core cannot
    // see, the blocking form too for the three operations that wait for no report: it runs no queued callback and
    // completes at once, as Java's mock does (ffi-marshalling.md §A7). Everywhere else the _cb form, whose wait is
    // bounded by max.block.ms inside a callback, where its completion is queued behind it.
    private void Transaction(string operation, bool waitsForReports, Action<IntPtr> submit, BlockingCall blocking)
    {
        ThrowIfClosed();
        if (MockTakesTheBlockingForm(waitsForReports))
        {
            TransactionBlocking(blocking);
            return;
        }

        Completion<IntPtr> done = new Completion<IntPtr>(FreeError);
        IntPtr error = WaitQueued(done, new Action<IntPtr>(done.Complete), submit, operation, waitsForReports);
        KafkaException.ThrowIfFfiFailed(ref error);
    }

    // Not async, so the closed check and an already cancelled token throw at the call.
    private Task TransactionWithCallback(
        string operation,
        bool waitsForReports,
        Action<IntPtr> submit,
        BlockingCall blocking,
        CancellationToken cancellationToken)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();
        if (MockTakesTheBlockingForm(waitsForReports))
        {
            try
            {
                TransactionBlocking(blocking);
                return Task.CompletedTask;
            }
            catch (Exception exception)
            {
                return Task.FromException(exception);
            }
        }

        TimeSpan timeout = waitsForReports ? ReportWaitTimeout : WaitTimeout;
        string format = CallbackWaitFormat;
        AsyncCompletion<IntPtr> done = new AsyncCompletion<IntPtr>(FreeError);
        Submit(new Action<IntPtr>(done.Complete), submit);
        return AwaitVoid(done, timeout, format, operation, cancellationToken);
    }

    private bool MockTakesTheBlockingForm(bool waitsForReports) =>
        IsMock && (_completion.IsDraining || (!waitsForReports && InCallback));

    private void TransactionBlocking(BlockingCall blocking)
    {
        IntPtr outErr = IntPtr.Zero;
        EnterUse();
        try
        {
            blocking(ref outErr);
        }
        finally
        {
            ExitUse();
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    /// <summary>
    /// The native arguments of one <c>SendOffsetsToTransaction</c> call: the offsets map and a reference on the group
    /// metadata's handle, both released after the call. A null argument crosses as <c>NULL</c>, which the core reports
    /// with Java's error; with a null group metadata the offsets are not read, as the core reads them only after it.
    /// </summary>
    private sealed class TransactionOffsets : IDisposable
    {
        private readonly NativeCallArguments? _arguments;
        private readonly SafeConsumerGroupMetadataHandle? _metadata;
        private bool _added;

        internal TransactionOffsets(
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets,
            ConsumerGroupMetadata? groupMetadata)
        {
            if (groupMetadata is null)
            {
                return;
            }

            try
            {
                if (offsets is not null)
                {
                    OffsetsSnapshot snapshot = OffsetsSnapshot.Of(offsets);
                    _arguments = new NativeCallArguments();
                    Offsets = _arguments.OffsetMap(snapshot);
                }

                _metadata = groupMetadata.Handle;
                _metadata.DangerousAddRef(ref _added);
                GroupMetadata = _metadata.DangerousGetHandle();
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        internal IntPtr Offsets { get; }

        internal IntPtr GroupMetadata { get; }

        public void Dispose()
        {
            if (_added)
            {
                _added = false;
                _metadata!.DangerousRelease();
            }

            _arguments?.Dispose();
        }
    }
}
