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

#if NETFRAMEWORK
using System.Runtime.CompilerServices;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Loads the native library before any test runs. Many tests declare their own
/// <c>[DllImport("confluent_kafka")]</c>, which on .NET Framework finds the library only once the
/// static constructor of <see cref="NativeMethods"/> has loaded it from <c>runtimes\&lt;rid&gt;\native</c>:
/// a test project has no RID, so no library sits beside the test assembly. Without this, such a test
/// running before the first call into <see cref="NativeMethods"/>, or while another thread is inside its
/// static constructor, fails with <see cref="System.DllNotFoundException"/>.
/// </summary>
internal static class NativeLibraryInitializer
{
#pragma warning disable CA2255 // the test assembly's own setup, not a library's
    [ModuleInitializer]
#pragma warning restore CA2255
    internal static void LoadNativeLibrary()
    {
        RuntimeHelpers.RunClassConstructor(typeof(NativeMethods).TypeHandle);
    }
}
#endif
