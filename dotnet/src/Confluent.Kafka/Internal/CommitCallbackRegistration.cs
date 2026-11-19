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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The <c>self_</c> of one offset-commit callback registration: rooted by a <see cref="GCHandle"/> until its
/// <c>onComplete</c> has reported, it runs the user's callback, or none, and reports the call's end once through
/// its consumer's reporter.
/// </summary>
internal sealed class CommitCallbackRegistration
{
    private static readonly NativeMethods.OffsetCommitCallbackOnComplete s_onComplete = OnComplete;

    private readonly IOffsetCommitCallback? _callback;
    private readonly IAsyncOffsetCommitCallback? _asyncCallback;
    private readonly bool _emptyOffsets;
    private readonly ConsumerCallbackReporter _reporter;
    private GCHandle _gcHandle;
    private int _released;

    private CommitCallbackRegistration(
        IOffsetCommitCallback? callback,
        IAsyncOffsetCommitCallback? asyncCallback,
        bool emptyOffsets,
        ConsumerCallbackReporter reporter)
    {
        _callback = callback;
        _asyncCallback = asyncCallback;
        _emptyOffsets = emptyOffsets;
        _reporter = reporter;
    }

    internal IntPtr Self => GCHandle.ToIntPtr(_gcHandle);

    internal bool IsReleased => Volatile.Read(ref _released) != 0;

    // A null callback is Java's commitAsync(Map, null): the registration reports and runs nothing. An explicit empty
    // offsets map reaches the callback as null offsets, as Java's commit of an empty map completes with null.
    internal static CommitCallbackRegistration Root(
        IOffsetCommitCallback? callback,
        bool emptyOffsets,
        ConsumerCallbackReporter reporter) =>
        Root(new CommitCallbackRegistration(callback, null, emptyOffsets, reporter));

    internal static CommitCallbackRegistration Root(
        IAsyncOffsetCommitCallback callback,
        bool emptyOffsets,
        ConsumerCallbackReporter reporter) =>
        Root(new CommitCallbackRegistration(null, callback, emptyOffsets, reporter));

    private static CommitCallbackRegistration Root(CommitCallbackRegistration registration)
    {
        registration._gcHandle = GCHandle.Alloc(registration, GCHandleType.Normal);
        return registration;
    }

    internal IntPtr NewHandle()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr handle = NativeMethods.OffsetCommitCallbackNew(Self, s_onComplete, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return handle;
    }

    internal static void DestroyHandle(IntPtr handle)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.OffsetCommitCallbackDestroy(handle, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }

    internal void Release()
    {
        if (Interlocked.Exchange(ref _released, 1) == 0 && _gcHandle.IsAllocated)
        {
            _gcHandle.Free();
        }
    }

    private static void OnComplete(IntPtr self, IntPtr offsets, IntPtr error, long callbackId)
    {
        try
        {
            if (GCHandle.FromIntPtr(self).Target is CommitCallbackRegistration registration)
            {
                registration.Invoke(offsets, error, callbackId);
            }
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    private void Invoke(IntPtr offsets, IntPtr error, long callbackId)
    {
        bool reportsLater = false;
        try
        {
            if (_callback is null && _asyncCallback is null)
            {
                _reporter.Report(callbackId, null);
                return;
            }

            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? copied;
            Exception? exception;
            try
            {
                exception = KafkaException.FromFfiError(error, owned: false);
                copied = exception is null && !_emptyOffsets ? OffsetMapMarshal.CopyOut(offsets) : null;
            }
            catch (Exception unreadable)
            {
                _reporter.Report(callbackId, unreadable);
                return;
            }

            if (_asyncCallback is not null)
            {
                Task? task = null;
                Exception? started = null;
                ConsumerCallbackReporter.CallbackScope asyncScope = _reporter.Enter(asyncCallback: true);
                try
                {
                    task = _asyncCallback.OnComplete(copied, exception);
                }
                catch (Exception thrown)
                {
                    started = thrown;
                }
                finally
                {
                    _reporter.Exit(asyncScope);
                }

                reportsLater = true;
                if (_reporter.InHandleCommit)
                {
                    _reporter.ReportAndKeep(callbackId, task, started, Release);
                }
                else
                {
                    _reporter.ReportWhenEnded(callbackId, task, started, Release);
                }

                return;
            }

            Exception? failure = null;
            ConsumerCallbackReporter.CallbackScope scope = _reporter.Enter(asyncCallback: false);
            try
            {
                _callback!.OnComplete(copied, exception);
            }
            catch (Exception thrown)
            {
                failure = thrown;
            }
            finally
            {
                _reporter.Exit(scope);
            }

            _reporter.Report(callbackId, failure);
        }
        finally
        {
            if (!reportsLater)
            {
                Release();
            }
        }
    }
}
