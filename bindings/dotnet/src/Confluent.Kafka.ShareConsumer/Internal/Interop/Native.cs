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

namespace Confluent.Kafka.ShareConsumer.Internal.Interop;

/// <summary>
/// The P/Invoke boundary onto the Rust core's C ABI (generated
/// <c>confluent_kafka.h</c>).
/// </summary>
/// <remarks>
/// Shell only for this milestone — no <c>[DllImport]</c> declarations yet. Per
/// <c>ffi-marshalling.md</c> §0.1, the classic
/// <c>[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]</c>
/// declarations (one set for every target framework) land in this class as the
/// producer/consumer surfaces are wired. Nothing P/Invokes here today.
/// </remarks>
internal static class Native
{
    /// <summary>
    /// The native library name resolved by default <c>[DllImport]</c> probing. The
    /// bare name maps per-OS to <c>confluent_kafka.dll</c> /
    /// <c>libconfluent_kafka.so</c> / <c>libconfluent_kafka.dylib</c> — one name for
    /// every platform (<c>ffi-marshalling.md</c> §0.2).
    /// </summary>
    internal const string DllName = "confluent_kafka";
}
