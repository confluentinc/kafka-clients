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

namespace Confluent.Kafka.ShareConsumer.Internal.Interop;

/// <summary>
/// The single P/Invoke boundary over the Rust core's C ABI
/// (<c>confluent_kafka.h</c>). Classic <c>[DllImport]</c> declarations, uniform
/// across every TFM (netstandard2.0 is the floor, so no <c>[LibraryImport]</c> /
/// <c>LPUTF8Str</c> / <c>Marshal.PtrToStringUTF8</c> — ffi-marshalling.md §0.1).
///
/// Type map (ffi §0.1, verbatim): opaque <c>*_t</c> → <see cref="IntPtr"/>;
/// <c>const char*</c> (in and out) → <see cref="IntPtr"/> (hand-marshalled via
/// <see cref="Utf8"/>, §A3/§B3); <c>int32_t</c> → <see cref="int"/>; <c>bool</c> →
/// <c>[MarshalAs(UnmanagedType.I1)]</c> (C <c>bool</c> is 1 byte, not a 4-byte
/// Win32 <c>BOOL</c>).
///
/// The C# method names drop the <c>kafka_&lt;pkg&gt;_</c> prefix (CLAUDE.md §6.3),
/// so each declaration carries the full ABI symbol as its <c>EntryPoint</c> —
/// otherwise the marshaller probes the short name and throws
/// <see cref="EntryPointNotFoundException"/> at runtime.
///
/// M1/P1 declares only the shared <c>KafkaError</c> foundation plus the
/// <c>ConsumerProperties</c> trio exercised by the native-load probe. The
/// <c>KafkaError</c> functions are declared but not yet called — that is
/// intentional (the shared error foundation later phases build on); their
/// <c>EntryPoint</c>s are runtime-validated once a caller lands.
/// </summary>
internal static class Native
{
    /// <summary>
    /// The bare DLL name. The runtime maps it per-OS to
    /// <c>confluent_kafka.dll</c> / <c>libconfluent_kafka.so</c> /
    /// <c>libconfluent_kafka.dylib</c> (ffi §0.2). Never a hardcoded filename or
    /// absolute path — the MSBuild native-copy target places the matching binary
    /// in the output dir where default probing resolves it.
    /// </summary>
    private const string DllName = "confluent_kafka";

    // ---- kafka_common_KafkaError_t — the shared error handle (ffi §A5/§B5) ----

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_code", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int Code(IntPtr error);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_message", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr Message(IntPtr error);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_is_retriable", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool IsRetriable(IntPtr error);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_is_fatal", CallingConvention = CallingConvention.Cdecl)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool IsFatal(IntPtr error);

    [DllImport(DllName, EntryPoint = "kafka_common_KafkaError_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ErrorDestroy(IntPtr error);

    // ---- kafka_consumer_ConsumerProperties_t — config (ffi §0.1 "put") ----

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerProperties_new", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ConsumerPropertiesNew();

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerProperties_put", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPropertiesPut(IntPtr props, IntPtr key, IntPtr value);

    [DllImport(DllName, EntryPoint = "kafka_consumer_ConsumerProperties_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ConsumerPropertiesDestroy(IntPtr props);
}
