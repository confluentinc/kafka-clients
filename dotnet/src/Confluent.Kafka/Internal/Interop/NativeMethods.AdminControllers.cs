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
    [DllImport(DllName, EntryPoint = "kafka_admin_Admin_unregister_controller_with_options_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr AdminUnregisterControllerWithOptions(
        SafeAdminClientHandle self, int controllerId, IntPtr options, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UnregisterControllerResult_all_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UnregisterControllerResultAll(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UnregisterControllerResult_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UnregisterControllerResultDestroy(IntPtr self, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UnregisterControllerOptions_new_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr UnregisterControllerOptionsNew(ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UnregisterControllerOptions_set_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UnregisterControllerOptionsSetTimeoutMs(IntPtr self, int timeoutMs, ref IntPtr outErr);

    [DllImport(DllName, EntryPoint = "kafka_admin_UnregisterControllerOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void UnregisterControllerOptionsDestroy(IntPtr self, ref IntPtr outErr);
}
