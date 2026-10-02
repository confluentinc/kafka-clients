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
/// <remarks>
/// <para>
/// ⚠ <b>The guard this exists for:</b> a P/Invoke whose <c>EntryPoint</c> names a symbol
/// the native no longer exports <b>compiles</b>, and fails only when it is first
/// <b>called</b>, with an <see cref="EntryPointNotFoundException"/>. Master #209 renamed
/// 74 ABI functions to their Java package names and removed 15 more; a missed rename in
/// a path no behavioural test reaches would ship green. The structural sweeps
/// (<see cref="AdminNativeMethodsMarshallingTests"/>) assert only that an
/// <c>EntryPoint</c> is <i>set</i>, never that it <i>exists</i>.
/// </para>
/// <para>
/// <see cref="Marshal.Prelink(MethodInfo)"/> runs a P/Invoke's one-time setup — load the
/// library, look up the entry point — without calling it, so the whole surface is checked
/// without any native side effect. The sweep enumerates by attribute over every type in
/// the library assembly, not by a hand list of classes, so a P/Invoke added outside the
/// <c>NativeMethods</c> partials is covered too.
/// </para>
/// </remarks>
public sealed class NativeMethodsPrelinkTests
{
    /// <summary>
    /// The number of <c>[DllImport]</c> declarations in the library, reconciled with the
    /// source-level count <c>git grep -h -o 'EntryPoint = "…"' -- dotnet/src | wc -l</c>.
    /// </summary>
    /// <remarks>
    /// Exact, not a floor: it is the zero-match guard for the sweep below (a reflection
    /// filter that found nothing would otherwise make the Prelink test pass vacuously), and
    /// it also catches a declaration that goes missing. Adding or removing a P/Invoke
    /// changes it on purpose.
    /// </remarks>
    private const int ExpectedImportCount = 578;

    /// <summary>
    /// <see cref="Marshal.Prelink(MethodInfo)"/> succeeds for every P/Invoke, against the
    /// native copied into the test output. All failures are collected, so one run names
    /// every unresolved entry point rather than the first.
    /// </summary>
    [Fact]
    public void EveryDllImport_ResolvesAgainstTheLoadedNative()
    {
        MethodInfo[] imports = LibraryImports();

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

    /// <summary>
    /// The sweep sees exactly the declared surface — the control-positive for
    /// <see cref="EveryDllImport_ResolvesAgainstTheLoadedNative"/>, and every declaration
    /// names its full ABI symbol (ffi §0.1), so the marshaller never probes a short C# name.
    /// </summary>
    [Fact]
    public void TheSweepFindsEveryDllImport()
    {
        MethodInfo[] imports = LibraryImports();

        Assert.Equal(ExpectedImportCount, imports.Length);
        Assert.All(
            imports,
            method => Assert.False(string.IsNullOrEmpty(EntryPointOf(method)), $"{method.Name} must set EntryPoint"));
    }

    /// <summary>
    /// Every method in the library assembly that carries <c>[DllImport]</c>, across all
    /// types (nested and non-public included).
    /// </summary>
    private static MethodInfo[] LibraryImports() =>
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
