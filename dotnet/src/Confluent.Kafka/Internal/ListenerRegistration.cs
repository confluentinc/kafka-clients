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

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The <c>self_</c> of one rebalance-listener registration: rooted by a <see cref="GCHandle"/> across every fire,
/// it runs the user's sync or async listener and reports each call's end once through its consumer's reporter.
/// </summary>
internal sealed class ListenerRegistration
{
    private static readonly NativeMethods.ConsumerRebalanceListenerMethod s_revoked = OnRevoked;

    private static readonly NativeMethods.ConsumerRebalanceListenerMethod s_assigned = OnAssigned;

    private static readonly NativeMethods.ConsumerRebalanceListenerMethod s_lost = OnLost;

    private readonly IConsumerRebalanceListener? _listener;
    private readonly IAsyncConsumerRebalanceListener? _asyncListener;
    private readonly ConsumerCallbackReporter _reporter;
    private GCHandle _gcHandle;
    private int _released;

    private ListenerRegistration(
        IConsumerRebalanceListener? listener,
        IAsyncConsumerRebalanceListener? asyncListener,
        ConsumerCallbackReporter reporter)
    {
        _listener = listener;
        _asyncListener = asyncListener;
        _reporter = reporter;
    }

    private enum Method
    {
        Revoked,
        Assigned,
        Lost,
    }

    internal IntPtr Self => GCHandle.ToIntPtr(_gcHandle);

    internal bool IsReleased => Volatile.Read(ref _released) != 0;

    internal static ListenerRegistration Root(IConsumerRebalanceListener listener, ConsumerCallbackReporter reporter) =>
        Root(new ListenerRegistration(listener, null, reporter));

    internal static ListenerRegistration Root(
        IAsyncConsumerRebalanceListener listener,
        ConsumerCallbackReporter reporter) =>
        Root(new ListenerRegistration(null, listener, reporter));

    internal IntPtr NewHandle()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr handle = NativeMethods.ConsumerRebalanceListenerNew(Self, s_revoked, s_assigned, s_lost, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return handle;
    }

    internal static void DestroyHandle(IntPtr handle)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerRebalanceListenerDestroy(handle, ref outErr);
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

    private static ListenerRegistration Root(ListenerRegistration registration)
    {
        registration._gcHandle = GCHandle.Alloc(registration, GCHandleType.Normal);
        return registration;
    }

    private static void OnRevoked(IntPtr self, IntPtr partitions, long callbackId) =>
        Fire(self, partitions, callbackId, Method.Revoked);

    private static void OnAssigned(IntPtr self, IntPtr partitions, long callbackId) =>
        Fire(self, partitions, callbackId, Method.Assigned);

    private static void OnLost(IntPtr self, IntPtr partitions, long callbackId) =>
        Fire(self, partitions, callbackId, Method.Lost);

    private static void Fire(IntPtr self, IntPtr partitions, long callbackId, Method method)
    {
        try
        {
            if (GCHandle.FromIntPtr(self).Target is ListenerRegistration registration)
            {
                registration.Invoke(partitions, callbackId, method);
            }
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    private void Invoke(IntPtr partitions, long callbackId, Method method)
    {
        IReadOnlyCollection<TopicPartition> copied;
        try
        {
            copied = TopicPartitionListMarshal.CopyOut(partitions);
        }
        catch (Exception unreadable)
        {
            _reporter.Report(callbackId, unreadable);
            return;
        }

        if (_listener is not null)
        {
            Exception? failure = null;
            ConsumerCallbackReporter.CallbackScope scope = _reporter.Enter(asyncCallback: false);
            try
            {
                Call(_listener, copied, method);
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
            return;
        }

        Task? task = null;
        Exception? started = null;
        ConsumerCallbackReporter.CallbackScope asyncScope = _reporter.Enter(asyncCallback: true);
        try
        {
            task = Call(_asyncListener!, copied, method);
        }
        catch (Exception thrown)
        {
            started = thrown;
        }
        finally
        {
            _reporter.Exit(asyncScope);
        }

        _reporter.ReportWhenEnded(callbackId, task, started);
    }

    private static void Call(
        IConsumerRebalanceListener listener,
        IReadOnlyCollection<TopicPartition> partitions,
        Method method)
    {
        switch (method)
        {
            case Method.Revoked:
                listener.OnPartitionsRevoked(partitions);
                break;
            case Method.Assigned:
                listener.OnPartitionsAssigned(partitions);
                break;
            default:
                listener.OnPartitionsLost(partitions);
                break;
        }
    }

    private static Task Call(
        IAsyncConsumerRebalanceListener listener,
        IReadOnlyCollection<TopicPartition> partitions,
        Method method) =>
        method switch
        {
            Method.Revoked => listener.OnPartitionsRevoked(partitions),
            Method.Assigned => listener.OnPartitionsAssigned(partitions),
            _ => listener.OnPartitionsLost(partitions),
        };
}
