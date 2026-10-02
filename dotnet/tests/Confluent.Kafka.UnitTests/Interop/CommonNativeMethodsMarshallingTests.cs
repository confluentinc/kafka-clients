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
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Structural pins for the declarations M15/P13.3 CP5 changed outside the
/// <c>kafka_admin_*</c> family, which <see cref="AdminNativeMethodsMarshallingTests"/>'s sweep
/// does not reach: <c>kafka_common_Error_cause</c> (D11), <c>kafka_common_Node_is_fenced</c>
/// (D12) — and, inside the admin family, that the flat
/// <c>kafka_admin_TopicMetadataAndConfig_config_*</c> accessors are gone in favour of the
/// single <c>Config_t</c> getter (D13).
/// </summary>
public sealed class CommonNativeMethodsMarshallingTests
{
    /// <summary>
    /// ⚠ A C <c>bool</c> is one byte; without <c>[return: MarshalAs(UnmanagedType.I1)]</c>
    /// the marshaller reads a four-byte Win32 <c>BOOL</c>, so the upper three bytes are
    /// whatever the register held. Neither mock can report a fenced node, so the
    /// <see langword="true"/> half has no end-to-end vehicle and this pin is what stands in
    /// for it.
    /// </summary>
    [Fact]
    public void NodeIsFenced_IsDeclaredAgainstItsAbiSymbol_AndReturnsAnI1()
    {
        MethodInfo method = Import("kafka_common_Node_is_fenced");

        Assert.Equal(nameof(NativeMethods.NodeIsFenced), method.Name);
        Assert.Equal(typeof(bool), method.ReturnType);
        Assert.Equal(UnmanagedType.I1, method.ReturnParameter.GetCustomAttribute<MarshalAsAttribute>()?.Value);
        Assert.Equal(new[] { typeof(IntPtr) }, method.GetParameters().Select(p => p.ParameterType));
    }

    /// <summary>
    /// The cause is returned as a raw <see cref="IntPtr"/>, not a <see cref="SafeHandle"/>:
    /// it is a Category-2 transient that <c>FromHandle</c> reads and frees at once, and it
    /// is taken from a <b>borrowed</b> error as often as an owned one, so no handle type
    /// could state its ownership.
    /// </summary>
    [Fact]
    public void ErrorCause_IsDeclaredAgainstItsAbiSymbol()
    {
        MethodInfo method = Import("kafka_common_Error_cause");

        Assert.Equal(nameof(NativeMethods.ErrorCause), method.Name);
        Assert.Equal(typeof(IntPtr), method.ReturnType);
        Assert.Equal(new[] { typeof(IntPtr) }, method.GetParameters().Select(p => p.ParameterType));
    }

    /// <summary>
    /// The six flat accessors could carry only name / value / three flags, so
    /// <c>createTopics</c> entries had to <em>guess</em> a source from <c>is_default</c>.
    /// They are deleted, and the result is read through the <c>Config_t</c> getter with the
    /// same reader <c>describeConfigs</c> uses — a re-added flat accessor would be a second,
    /// divergent path to the same entries.
    /// </summary>
    [Fact]
    public void TopicMetadataAndConfig_IsReadThroughTheConfigGetter_NotTheFlatAccessors()
    {
        string[] flat = Imports()
            .Select(method => method.GetCustomAttribute<DllImportAttribute>()!.EntryPoint!)
            .Where(entryPoint => entryPoint.StartsWith("kafka_admin_TopicMetadataAndConfig_config_", StringComparison.Ordinal))
            .ToArray();
        Assert.Empty(flat);

        MethodInfo getter = Import("kafka_admin_TopicMetadataAndConfig_config");
        Assert.Equal(nameof(NativeMethods.TopicMetadataAndConfigConfig), getter.Name);
        Assert.Equal(typeof(IntPtr), getter.ReturnType);
        Assert.Equal(new[] { typeof(IntPtr) }, getter.GetParameters().Select(p => p.ParameterType));
    }

    private static MethodInfo Import(string entryPoint)
    {
        MethodInfo method = Assert.Single(
            Imports(),
            candidate => candidate.GetCustomAttribute<DllImportAttribute>()!.EntryPoint == entryPoint);
        Assert.Equal(CallingConvention.Cdecl, method.GetCustomAttribute<DllImportAttribute>()!.CallingConvention);
        return method;
    }

    private static MethodInfo[] Imports() =>
        typeof(NativeMethods)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(method => method.Attributes.HasFlag(MethodAttributes.PinvokeImpl))
            .ToArray();
}
