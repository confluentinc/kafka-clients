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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The .NET Framework preload of the native library (<c>NativeMethods.NetFramework.cs</c>): where it looks, in
/// which order, and that the process ends up with one copy of the native library.
/// </summary>
public sealed class NativeLibraryPreloadTests
{
    [Fact]
    public void TheOnlyImportOutsideTheCore_IsTheLoaderOfTheNetFrameworkPreload()
    {
        string[] others = NativeMethodsPrelinkTests.AllImports()
            .Select(method => method.GetCustomAttribute<DllImportAttribute>()!)
            .Where(import => import.Value != "confluent_kafka")
            .Select(import => $"{import.Value}!{import.EntryPoint}")
            .ToArray();

#if NETFRAMEWORK
        Assert.Equal(new[] { "kernel32.dll!LoadLibraryExW" }, others);
#else
        Assert.Empty(others);
#endif
    }

#if NETFRAMEWORK
    [Fact]
    public void Candidates_AreTheAssemblyFolder_ThenItsCodeBase_ThenTheApplication_EachBesideThenUnderRuntimes()
    {
        Assert.Equal(
            new[]
            {
                @"C:\shadow\a1\confluent_kafka.dll",
                @"C:\shadow\a1\runtimes\win-x64\native\confluent_kafka.dll",
                @"C:\site\bin\confluent_kafka.dll",
                @"C:\site\bin\runtimes\win-x64\native\confluent_kafka.dll",
                @"C:\site\confluent_kafka.dll",
                @"C:\site\runtimes\win-x64\native\confluent_kafka.dll",
            },
            NativeMethods.NativeLibraryCandidates(
                @"C:\shadow\a1\Confluent.Kafka.dll", "file:///C:/site/bin/Confluent.Kafka.DLL", @"C:\site\", "bin", is64BitProcess: true));
    }

    [Fact]
    public void Candidates_OfA32BitProcess_AreTheX86Natives()
    {
        Assert.Equal(
            new[] { @"C:\app\confluent_kafka.dll", @"C:\app\runtimes\win-x86\native\confluent_kafka.dll" },
            NativeMethods.NativeLibraryCandidates(
                @"C:\app\Confluent.Kafka.dll", "file:///C:/app/Confluent.Kafka.dll", @"C:\app\", null, is64BitProcess: false));
    }

    [Fact]
    public void Candidates_LeaveOutAnAssemblyWithoutLocation_AndACodeBaseThatIsNotAFile()
    {
        Assert.Equal(
            new[] { @"C:\app\confluent_kafka.dll", @"C:\app\runtimes\win-x64\native\confluent_kafka.dll" },
            NativeMethods.NativeLibraryCandidates(string.Empty, "http://example.com/Confluent.Kafka.dll", @"C:\app", null, is64BitProcess: true));
    }

    [Fact]
    public void Candidates_KeepADrivesRoot()
    {
        Assert.Equal(
            new[] { @"C:\confluent_kafka.dll", @"C:\runtimes\win-x86\native\confluent_kafka.dll" },
            NativeMethods.NativeLibraryCandidates(@"C:\Confluent.Kafka.dll", null, null, null, is64BitProcess: false));
    }

    [Fact]
    public void TheProcess_HoldsOneCopyOfTheNativeLibrary()
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ErrorDestroy(IntPtr.Zero, ref outErr);

        string[] loaded = Process.GetCurrentProcess().Modules.Cast<ProcessModule>()
            .Where(module => string.Equals(module.ModuleName, "confluent_kafka.dll", StringComparison.OrdinalIgnoreCase))
            .Select(module => module.FileName)
            .ToArray();

        Assert.Single(loaded);
    }
#endif
}
