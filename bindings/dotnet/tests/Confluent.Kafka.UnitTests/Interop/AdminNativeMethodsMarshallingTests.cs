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

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Pins the marshalling attributes on the <b>admin</b> P/Invoke surface, which no
/// behavioural test can reach.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>This exists because a behavioural test for <c>MarshalAs(I1)</c> was measured and
/// found NOT to be sensitive (M15/P2b).</b> The reasoning behind the behavioural form —
/// "a one-byte C <c>bool</c> widened to a four-byte Win32 <c>BOOL</c> corrupts the
/// argument that follows" — assumes the arguments are packed adjacently. On the
/// platforms this suite runs on they are not: each argument gets its own register, so
/// deleting the attribute changed nothing observable, and a round trip through the real
/// ABI stayed green with it removed. Verified by injection, not assumed.
/// </para>
/// <para>
/// So the attribute is asserted <b>structurally</b>, where it is unambiguous. Reflection
/// does surface it: <c>MarshalAsAttribute</c> is a pseudo-custom-attribute stored as
/// metadata, but <see cref="ParameterInfo.GetCustomAttributes(System.Type, bool)"/>
/// reconstructs it — confirmed empirically before this test was written, since a
/// reflection query that silently returns nothing would make this a test that can only
/// pass.
/// </para>
/// <para>
/// The sweep is deliberately family-wide rather than P2b-only. The rule
/// (<c>ffi-marshalling.md</c> §0.1's type map) is family-wide, the cost of covering every
/// admin declaration is one predicate, and scoping it to one phase would leave the next
/// phase's first <c>bool</c> unguarded again.
/// </para>
/// </remarks>
public sealed class AdminNativeMethodsMarshallingTests
{
    /// <summary>
    /// Every <c>bool</c> <b>parameter</b> on an admin P/Invoke carries
    /// <c>[MarshalAs(UnmanagedType.I1)]</c>.
    /// </summary>
    [Fact]
    public void EveryAdminBoolParameter_IsMarshalledAsI1()
    {
        List<string> unmarked = new List<string>();

        foreach (MethodInfo method in AdminImports())
        {
            foreach (ParameterInfo parameter in method.GetParameters())
            {
                if (parameter.ParameterType == typeof(bool) && MarshalAs(parameter) != UnmanagedType.I1)
                {
                    unmarked.Add($"{method.Name}({parameter.Name})");
                }
            }
        }

        Assert.Equal(new List<string>(), unmarked);
    }

    /// <summary>
    /// Every <c>bool</c>-<b>returning</b> admin P/Invoke carries
    /// <c>[return: MarshalAs(UnmanagedType.I1)]</c>.
    /// </summary>
    [Fact]
    public void EveryAdminBoolReturn_IsMarshalledAsI1()
    {
        List<string> unmarked = AdminImports()
            .Where(method => method.ReturnType == typeof(bool))
            .Where(method => MarshalAs(method.ReturnParameter) != UnmanagedType.I1)
            .Select(method => method.Name)
            .ToList();

        Assert.Equal(new List<string>(), unmarked);
    }

    /// <summary>
    /// The sweep really sees the declarations it claims to — a control-positive, because
    /// a filter that matched nothing would make both tests above pass vacuously.
    /// </summary>
    [Fact]
    public void TheSweepFindsTheAdminSurface_AndTheBoolsWithinIt()
    {
        MethodInfo[] imports = AdminImports();

        // A floor, not an exact count, so adding a declaration does not fail this test.
        Assert.True(imports.Length >= 40, $"expected the admin P/Invoke surface, found {imports.Length}");

        int boolParameters = imports.Sum(
            method => method.GetParameters().Count(parameter => parameter.ParameterType == typeof(bool)));
        int boolReturns = imports.Count(method => method.ReturnType == typeof(bool));

        Assert.True(boolParameters >= 10, $"expected admin bool parameters, found {boolParameters}");
        Assert.True(boolReturns >= 3, $"expected admin bool returns, found {boolReturns}");

        // Every declaration is Cdecl, matching the Rust exports' extern "C".
        Assert.All(
            imports,
            method => Assert.Equal(
                CallingConvention.Cdecl, method.GetCustomAttribute<DllImportAttribute>()!.CallingConvention));

        // And each names its full ABI symbol, so the marshaller cannot probe the short C#
        // name and throw EntryPointNotFoundException at runtime (ffi §0.1).
        Assert.All(
            imports,
            method => Assert.False(
                string.IsNullOrEmpty(method.GetCustomAttribute<DllImportAttribute>()!.EntryPoint),
                $"{method.Name} must set EntryPoint"));
    }

    private static MethodInfo[] AdminImports() =>
        typeof(NativeMethods)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(method => method.Attributes.HasFlag(MethodAttributes.PinvokeImpl))
            .Where(method =>
                method.GetCustomAttribute<DllImportAttribute>()?.EntryPoint?.StartsWith(
                    "kafka_admin_", System.StringComparison.Ordinal) == true)
            .ToArray();

    private static UnmanagedType? MarshalAs(ParameterInfo parameter)
    {
        object[] attributes = parameter.GetCustomAttributes(typeof(MarshalAsAttribute), inherit: false);
        return attributes.Length == 0 ? null : ((MarshalAsAttribute)attributes[0]).Value;
    }
}
