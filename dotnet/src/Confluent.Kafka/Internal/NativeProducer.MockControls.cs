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

using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The mock producer's transaction controls: the injected errors, fencing, and the state readers. Each is a class-only
/// call on the mock, made under a use of the handle, so a closed producer throws its closed exception.
/// </summary>
internal sealed partial class NativeProducer
{
    // The core mock's flush error, for the flush the binding makes itself from a callback the core cannot see.
    private Exception? _mockFlushError;

    // The mock's close error, kept here and not in the core, as Python's mock keeps its close_exception: Close throws it
    // before anything else, as Java's MockProducer.close() throws closeException before it sets closed, so the mock
    // stays open and every Close throws it. Java's close is not synchronized, so no Close waits for another.
    private Exception? _mockCloseError;

    private delegate void MockErrorSetter(IntPtr mock, IntPtr error, ref IntPtr outErr);

    private delegate sbyte MockFlagReader(IntPtr mock, ref IntPtr outErr);

    internal void MockSetSendError(Exception? exception) =>
        MockSetError(NativeMethods.MockProducerSetSendError, exception);

    internal void MockSetFlushError(Exception? exception)
    {
        MockSetError(NativeMethods.MockProducerSetFlushError, exception);
        Volatile.Write(ref _mockFlushError, exception);
    }

    internal void MockSetPartitionsForError(Exception? exception) =>
        MockSetError(NativeMethods.MockProducerSetPartitionsForError, exception);

    internal void MockSetCloseError(Exception? exception)
    {
        ThrowIfClosed();
        Volatile.Write(ref _mockCloseError, exception);
    }

    // Read from the binding's own state, not the core's MockProducer_closed: a close that succeeds destroys the mock,
    // so the core could answer only while it is still open.
    internal bool MockClosed => Volatile.Read(ref _closed) != 0;

    internal void MockSetInitTransactionError(Exception? exception) =>
        MockSetError(NativeMethods.MockProducerSetInitTransactionError, exception);

    internal void MockSetBeginTransactionError(Exception? exception) =>
        MockSetError(NativeMethods.MockProducerSetBeginTransactionError, exception);

    internal void MockSetSendOffsetsToTransactionError(Exception? exception) =>
        MockSetError(NativeMethods.MockProducerSetSendOffsetsToTransactionError, exception);

    internal void MockSetCommitTransactionError(Exception? exception) =>
        MockSetError(NativeMethods.MockProducerSetCommitTransactionError, exception);

    internal void MockSetAbortTransactionError(Exception? exception) =>
        MockSetError(NativeMethods.MockProducerSetAbortTransactionError, exception);

    internal void MockFenceProducer() =>
        CallMock((IntPtr mock, ref IntPtr outErr) =>
        {
            NativeMethods.MockProducerFenceProducer(mock, ref outErr);
            return 0;
        });

    internal bool MockTransactionInitialized() => MockFlag(NativeMethods.MockProducerTransactionInitialized);

    internal bool MockTransactionInFlight() => MockFlag(NativeMethods.MockProducerTransactionInFlight);

    internal bool MockTransactionCommitted() => MockFlag(NativeMethods.MockProducerTransactionCommitted);

    internal bool MockTransactionAborted() => MockFlag(NativeMethods.MockProducerTransactionAborted);

    internal bool MockFlushed() => MockFlag(NativeMethods.MockProducerFlushed);

    internal bool MockSentOffsets() => MockFlag(NativeMethods.MockProducerSentOffsets);

    internal long MockCommitCount() =>
        CallMock((IntPtr mock, ref IntPtr outErr) => NativeMethods.MockProducerCommitCount(mock, ref outErr));

    // The owned containers are copied out after the use ends: they do not borrow from the mock.
    internal IReadOnlyList<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>>
        MockConsumerGroupOffsetsHistory() =>
        GroupOffsetsMarshal.CopyOutListAndDestroy(
            CallMock((IntPtr mock, ref IntPtr outErr) =>
                NativeMethods.MockProducerConsumerGroupOffsetsHistory(mock, ref outErr)));

    internal IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> MockUncommittedOffsets() =>
        GroupOffsetsMarshal.CopyOutMapAndDestroy(
            CallMock((IntPtr mock, ref IntPtr outErr) => NativeMethods.MockProducerUncommittedOffsets(mock, ref outErr)));

    private bool MockFlag(MockFlagReader reader) =>
        CallMock((IntPtr mock, ref IntPtr outErr) => reader(mock, ref outErr)) != 0;

    // The error is built only once the use is held, so a closed producer leaves nothing to free; its ownership moves
    // to the mock, which hands the same instance back through the carrier when the operation throws it. The core
    // resolves the mock handle before it takes the error, so the error could leak only on a NULL-handle panic, which
    // a held use rules out.
    private void MockSetError(MockErrorSetter setter, Exception? exception) =>
        CallMock((IntPtr mock, ref IntPtr outErr) =>
        {
            setter(mock, exception is null ? IntPtr.Zero : ManagedExceptionCarrier.ToError(exception), ref outErr);
            return 0;
        });
}
