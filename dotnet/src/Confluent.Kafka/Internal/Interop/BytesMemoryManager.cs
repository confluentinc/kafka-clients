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
using System.Buffers;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The memory of one key or value of a polled batch, over the batch's native bytes. It references the batch's
/// <see cref="SafeConsumerRecordsHandle"/> for its whole life, so the bytes stay valid while the memory is reachable,
/// and a pin adds a reference to the handle, so they also stay valid until the pin is released. A revoked memory
/// refuses new spans and pins; the pins taken before stay valid.
/// </summary>
internal sealed unsafe class BytesMemoryManager : MemoryManager<byte>
{
    internal const string RevokedMessage =
        "The bytes of a record's key or value were valid only until the deserializer's call completed.";

    internal const string ReleasedMessage = "The records these bytes belong to were released.";

    private readonly SafeConsumerRecordsHandle _batch;
    private readonly IntPtr _data;
    private readonly int _length;
    private volatile bool _revoked;

    internal BytesMemoryManager(SafeConsumerRecordsHandle batch, IntPtr data, int length)
    {
        _batch = batch;
        _data = data;
        _length = length;
    }

    internal ReadOnlyMemory<byte> ReadOnlyMemory => CreateMemory(_length);

    internal void Revoke() => _revoked = true;

    public override Span<byte> GetSpan()
    {
        ThrowIfRevoked();
        if (_batch.IsClosed)
        {
            throw new ObjectDisposedException(null, ReleasedMessage);
        }

        return new Span<byte>((void*)_data, _length);
    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        if ((uint)elementIndex > (uint)_length)
        {
            throw new ArgumentOutOfRangeException(nameof(elementIndex));
        }

        ThrowIfRevoked();
        bool added = false;
        try
        {
#pragma warning disable CKRDA0006 // A pin keeps the batch alive until its Unpin releases it.
            _batch.DangerousAddRef(ref added);
#pragma warning restore CKRDA0006
        }
        catch (ObjectDisposedException)
        {
            throw new ObjectDisposedException(null, ReleasedMessage);
        }

        return new MemoryHandle((byte*)_data + elementIndex, default, this);
    }

    public override void Unpin() => _batch.DangerousRelease();

    protected override void Dispose(bool disposing)
    {
    }

    private void ThrowIfRevoked()
    {
        if (_revoked)
        {
            throw new ObjectDisposedException(null, RevokedMessage);
        }
    }
}
