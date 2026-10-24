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
using System.Diagnostics;
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The value source of a <see cref="KafkaFutureImpl{T}"/> over a native <c>kafka_common_KafkaFuture_t</c>: one
/// <c>get_cb</c> settles it and then releases the handle; until then every read asks the handle, and a read that finds
/// it released reads the settled managed state.
/// </summary>
internal sealed class NativeFutureSource<T> : IFutureValueSource<T>
{
    private static readonly NativeMethods.KafkaFutureGetCallback s_delivered = OnDelivered;

    private readonly SafeKafkaFutureHandle _handle;

    private readonly Func<IntPtr, T> _convert;

    private readonly KafkaFutureImpl<T> _future;

    private readonly Action<IntPtr>? _onDelivered;

    private NativeFutureSource(SafeKafkaFutureHandle handle, Func<IntPtr, T> convert, Action<IntPtr>? onDelivered)
    {
        _handle = handle;
        _convert = convert;
        _onDelivered = onDelivered;
        _future = new KafkaFutureImpl<T>(mayCancel: true, this);
    }

    internal KafkaFutureImpl<T> Future => _future;

    public bool IsDone
    {
        get
        {
            bool added = false;
            try
            {
                if (!TryAddRef(ref added))
                {
                    return true;
                }

                return IsHandleDone();
            }
            finally
            {
                if (added)
                {
                    _handle.DangerousRelease();
                }
            }
        }
    }

    internal static KafkaFutureImpl<T> Start(SafeKafkaFutureHandle handle, Func<IntPtr, T> convert)
    {
        NativeFutureSource<T> source = Create(handle, convert);
        source.Issue();
        return source._future;
    }

    internal static NativeFutureSource<T> Create(
        SafeKafkaFutureHandle handle, Func<IntPtr, T> convert, Action<IntPtr>? onDelivered = null) =>
        new NativeFutureSource<T>(handle, convert, onDelivered);

    internal void Issue()
    {
        GCHandle self = GCHandle.Alloc(this);
        try
        {
            NativeMethods.KafkaFutureGetCb(_handle, s_delivered, GCHandle.ToIntPtr(self));
        }
        catch
        {
            self.Free();
            _handle.Dispose();
            throw;
        }
    }

    public bool TryRead(out T value, out Exception? error)
    {
        value = default!;
        error = null;
        bool added = false;
        try
        {
            if (!TryAddRef(ref added) || !IsHandleDone())
            {
                return false;
            }

            IntPtr outErr = IntPtr.Zero;
            IntPtr result = NativeMethods.KafkaFutureGet(_handle, ref outErr);
            if (outErr != IntPtr.Zero)
            {
                error = KafkaException.FromFfiError(outErr, owned: true);
                return true;
            }

            try
            {
                value = _convert(result);
            }
            catch (Exception e)
            {
                error = e;
            }

            return true;
        }
        finally
        {
            if (added)
            {
                _handle.DangerousRelease();
            }
        }
    }

    private static void OnDelivered(IntPtr value, IntPtr error, IntPtr opaque)
    {
        try
        {
            GCHandle self = GCHandle.FromIntPtr(opaque);
            var source = (NativeFutureSource<T>)self.Target!;
            self.Free();
            source.Deliver(value, error);
        }
        catch (Exception e)
        {
            TraceDeliveryFailure(e);
        }
    }

    private static void TraceDeliveryFailure(Exception e)
    {
        try
        {
            Trace.TraceError("Confluent.Kafka: a native future's delivery failed: {0}", e);
        }
        catch (Exception)
        {
            // A throwing TraceListener must not escape into native code.
        }
    }

    private void Deliver(IntPtr value, IntPtr error)
    {
        try
        {
            if (error != IntPtr.Zero)
            {
                _future.CompleteWithError(KafkaException.FromFfiError(error, owned: true)!);
            }
            else
            {
                bool settle = !_future.IsSettled;
                T converted = default!;
                try
                {
                    if (settle)
                    {
                        converted = _convert(value);
                    }

                    _onDelivered?.Invoke(value);
                }
                catch (Exception e)
                {
                    _future.CompleteWithError(e);
                    return;
                }

                if (settle)
                {
                    _future.Complete(converted);
                }
            }
        }
        finally
        {
            _handle.Dispose();
        }
    }

    private bool TryAddRef(ref bool added)
    {
        try
        {
            _handle.DangerousAddRef(ref added);
            return true;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }

    private bool IsHandleDone()
    {
        IntPtr outErr = IntPtr.Zero;
        bool done = NativeMethods.KafkaFutureIsDone(_handle, ref outErr) != 0;
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return done;
    }
}
