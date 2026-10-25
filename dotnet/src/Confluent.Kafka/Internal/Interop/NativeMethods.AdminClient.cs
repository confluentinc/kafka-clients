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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

internal static partial class NativeMethods
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void AdminVoidCallback(IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClientConfig_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminClientConfigNew(IntPtr props, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClientConfig_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminClientConfigDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_AdminClient_create", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminClientCreate(IntPtr config, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin__execute_callbacks_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int AdminExecuteCallbacks(SafeAdminClientHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin__set_callbacks_notify_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminSetCallbacksNotify(
        SafeAdminClientHandle self, CompletionThread.NotifyCallback notify, IntPtr opaque, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_close_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminCloseCb(SafeAdminClientHandle self, AdminVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_close_with_timeout_cb", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void AdminCloseWithTimeoutCb(
        SafeAdminClientHandle self, long timeout, AdminVoidCallback cb, IntPtr opaque);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaFuture_completed_future_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaFutureCompletedFuture(IntPtr result, IntPtr resultError, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaFuture_is_done_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern sbyte KafkaFutureIsDone(SafeKafkaFutureHandle self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaFuture_get", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr KafkaFutureGet(SafeKafkaFutureHandle self, ref IntPtr outErr);
}
