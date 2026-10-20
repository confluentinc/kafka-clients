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
/// Pins the declarations of <c>kafka_common_Error_source_err</c>, the <c>int8_t</c> error predicates,
/// <c>kafka_common_Node_is_fenced_err</c> and the <c>kafka_admin_TopicMetadataAndConfig_config</c> getter.
/// </summary>
public sealed class CommonNativeMethodsMarshallingTests
{
    [Fact]
    public void NodeIsFenced_IsDeclaredAgainstItsErrTwin_AndReturnsAnSByte()
    {
        MethodInfo method = Import("kafka_common_Node_is_fenced_err");

        Assert.Equal(nameof(NativeMethods.NodeIsFenced), method.Name);
        Assert.Equal(typeof(sbyte), method.ReturnType);
        Assert.Null(method.ReturnParameter.GetCustomAttribute<MarshalAsAttribute>());
        Assert.Equal(
            new[] { typeof(IntPtr), typeof(IntPtr).MakeByRefType() },
            method.GetParameters().Select(p => p.ParameterType));
    }

    [Fact]
    public void ErrorSource_IsDeclaredAgainstItsBorrowedAbiSymbol()
    {
        MethodInfo method = Import("kafka_common_Error_source_err");

        Assert.Equal(nameof(NativeMethods.ErrorSource), method.Name);
        Assert.Equal(typeof(IntPtr), method.ReturnType);
        Assert.Equal(
            new[] { typeof(IntPtr), typeof(IntPtr).MakeByRefType() },
            method.GetParameters().Select(p => p.ParameterType));
    }

    [Theory]
    [InlineData("kafka_common_Error_is_retriable_error_err")]
    [InlineData("kafka_common_Error_is_api_error_err")]
    public void ErrorPredicates_ReturnTheirInt8AsAnSByte(string entryPoint)
    {
        MethodInfo method = Import(entryPoint);

        Assert.Equal(typeof(sbyte), method.ReturnType);
        Assert.Null(method.ReturnParameter.GetCustomAttribute<MarshalAsAttribute>());
        Assert.Equal(
            new[] { typeof(IntPtr), typeof(IntPtr).MakeByRefType() },
            method.GetParameters().Select(p => p.ParameterType));
    }

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
