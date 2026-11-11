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
    // Held while a setter sets the core's error and the binding's copy of it, so concurrent calls leave the two equal.
    private readonly object _mockErrorGate = new object();

    // The core mock's flush error, for the flush the binding makes itself from a callback the core cannot see. The
    // core keeps its own copy, which its Flush, CommitTransaction and AbortTransaction throw.
    private Exception? _mockFlushError;

    // The core mock's send error and fenced state, so Send checks them before it serializes, as Java's
    // MockProducer.send does; the core checks them only once the record is serialized.
    private Exception? _mockSendError;

    private bool _mockFenced;

    // The mock's close error, kept here and not in the core, as Python's mock keeps its close_exception: Close throws it
    // before anything else, as Java's MockProducer.close() throws closeException before it sets closed, so the mock
    // stays open and every Close throws it. Java's close is not synchronized, so no Close waits for another.
    private Exception? _mockCloseError;

    private delegate void MockErrorSetter(IntPtr mock, IntPtr error, ref IntPtr outErr);

    private delegate sbyte MockFlagReader(IntPtr mock, ref IntPtr outErr);

    internal void MockSetSendError(Exception? exception)
    {
        lock (_mockErrorGate)
        {
            MockSetError(NativeMethods.MockProducerSetSendError, exception);
            Volatile.Write(ref _mockSendError, exception);
        }
    }

    internal void MockSetFlushError(Exception? exception)
    {
        lock (_mockErrorGate)
        {
            MockSetError(NativeMethods.MockProducerSetFlushError, exception);
            Volatile.Write(ref _mockFlushError, exception);
        }
    }

    // What Java's MockProducer.send throws after its closed check and before it serializes: the fenced error, then the
    // send exception; null when neither applies.
    internal Exception? MockSendRefusal()
    {
        if (Volatile.Read(ref _mockFenced))
        {
            return new KafkaException(
                "MockProducer is fenced.", new Common.Errors.ProducerFencedException("Fenced"));
        }

        return Volatile.Read(ref _mockSendError);
    }

    internal void MockSetPartitionsForError(Exception? exception) =>
        MockSetError(NativeMethods.MockProducerSetPartitionsForError, exception);

    // Settable after close too, as Java's public field: Close throws it whatever the state.
    internal void MockSetCloseError(Exception? exception) => Volatile.Write(ref _mockCloseError, exception);

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

    internal void MockFenceProducer()
    {
        CallMock((IntPtr mock, ref IntPtr outErr) =>
        {
            NativeMethods.MockProducerFenceProducer(mock, ref outErr);
            return 0;
        });
        Volatile.Write(ref _mockFenced, true);
    }

    internal bool MockTransactionInitialized() => MockFlag(NativeMethods.MockProducerTransactionInitialized);

    internal bool MockTransactionInFlight() => MockFlag(NativeMethods.MockProducerTransactionInFlight);

    internal bool MockTransactionCommitted() => MockFlag(NativeMethods.MockProducerTransactionCommitted);

    internal bool MockTransactionAborted() => MockFlag(NativeMethods.MockProducerTransactionAborted);

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
