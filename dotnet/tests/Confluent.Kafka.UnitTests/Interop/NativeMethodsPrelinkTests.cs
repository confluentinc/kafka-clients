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
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Every <c>[DllImport]</c> in the library resolves against the native it ships with.
/// </summary>
public sealed class NativeMethodsPrelinkTests
{
    private const int ExpectedImportCount = 622;

    [Fact]
    public void EveryDllImport_ResolvesAgainstTheLoadedNative()
    {
        MethodInfo[] imports = LibraryImports()
            .Where(method => !NativeAbiAllowlist.Contains(EntryPointOf(method)!))
            .ToArray();
        Assert.NotEmpty(imports);

        List<string> failures = new List<string>();
        foreach (MethodInfo method in imports)
        {
            try
            {
                Marshal.Prelink(method);
            }
            catch (Exception e)
            {
                failures.Add(
                    $"{method.DeclaringType!.FullName}.{method.Name} -> \"{EntryPointOf(method)}\": "
                    + $"{e.GetType().Name}: {e.Message}");
            }
        }

        Assert.True(
            failures.Count == 0,
            $"{failures.Count} of {imports.Length} P/Invokes do not resolve against the native:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }

    [Fact]
    public void TheSweepFindsEveryDllImport()
    {
        MethodInfo[] imports = LibraryImports();

        Assert.Equal(ExpectedImportCount, imports.Length);
        Assert.All(
            imports,
            method => Assert.False(string.IsNullOrEmpty(EntryPointOf(method)), $"{method.Name} must set EntryPoint"));
    }

    internal static MethodInfo[] LibraryImports() =>
        typeof(NativeMethods).Assembly
            .GetTypes()
            .SelectMany(type => type.GetMethods(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance
                | BindingFlags.DeclaredOnly))
            .Where(method => method.GetCustomAttribute<DllImportAttribute>() is not null)
            .ToArray();

    private static string? EntryPointOf(MethodInfo method) =>
        method.GetCustomAttribute<DllImportAttribute>()?.EntryPoint;
}
