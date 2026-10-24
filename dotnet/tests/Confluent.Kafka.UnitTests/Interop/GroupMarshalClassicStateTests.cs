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
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <see cref="ClassicGroupState"/> and the <see cref="GroupMarshal"/> members that decode it from the C
/// singletons and name it, checked against Java's six constants.
/// </summary>
public sealed class GroupMarshalClassicStateTests
{
    private static readonly (ClassicGroupState State, string Name)[] s_javaConstants =
    {
        (ClassicGroupState.Unknown, "Unknown"),
        (ClassicGroupState.PreparingRebalance, "PreparingRebalance"),
        (ClassicGroupState.CompletingRebalance, "CompletingRebalance"),
        (ClassicGroupState.Stable, "Stable"),
        (ClassicGroupState.Dead, "Dead"),
        (ClassicGroupState.Empty, "Empty"),
    };

    private static readonly Native.Singleton[] s_singletons =
    {
        Native.Unknown,
        Native.PreparingRebalance,
        Native.CompletingRebalance,
        Native.Stable,
        Native.Dead,
        Native.Empty,
    };

    [Fact]
    public void TheEnum_DeclaresExactlyJavasSixConstantsInOrder()
    {
        ClassicGroupState[] declared =
            (ClassicGroupState[])Enum.GetValues(typeof(ClassicGroupState));
        string[] names = Enum.GetNames(typeof(ClassicGroupState));

        Assert.Equal(s_javaConstants.Select(c => c.State).ToArray(), declared);
        Assert.DoesNotContain("Assigning", names);
        Assert.DoesNotContain("Reconciling", names);
        Assert.DoesNotContain("NotReady", names);
    }

    [Fact]
    public void TheEnum_IsNotMarkedObsolete()
    {
        Assert.Empty(typeof(ClassicGroupState)
            .GetCustomAttributes(typeof(ObsoleteAttribute), inherit: false));
    }

    [Fact]
    public void EveryCSingleton_DecodesToJavasConstant_AndKeepsJavasName()
    {
        for (int i = 0; i < s_javaConstants.Length; i++)
        {
            (ClassicGroupState state, string expectedName) = s_javaConstants[i];
            IntPtr singleton = Read(s_singletons[i]);

            Assert.Equal(state, GroupMarshal.ClassicStateFrom(singleton));
            Assert.Equal(expectedName, GroupMarshal.NameFromClassicState(state));
            Assert.Equal(expectedName, ReadName(singleton));
        }
    }

    [Theory]
    [InlineData("CompletingRebalance")]
    [InlineData("completingrebalance")]
    [InlineData("COMPLETINGREBALANCE")]
    public void TheCoresParse_DecodesRegardlessOfCasing(string name)
    {
        Assert.Equal(ClassicGroupState.CompletingRebalance, GroupMarshal.ClassicStateFrom(Parse(name)));
    }

    [Theory]
    [InlineData("Assigning")]
    [InlineData("Reconciling")]
    [InlineData("NotReady")]
    [InlineData("SomeStateFromANewerBroker")]
    [InlineData("")]
    public void AnUnrecognisedName_DecodesToUnknown(string name)
    {
        Assert.Equal(ClassicGroupState.Unknown, GroupMarshal.ClassicStateFrom(Parse(name)));
    }

    [Fact]
    public void ANullPointer_DecodesToAbsenceNotUnknown()
    {
        ClassicGroupState? decoded = GroupMarshal.ClassicStateFrom(IntPtr.Zero);

        Assert.Null(decoded);
        Assert.NotEqual(ClassicGroupState.Unknown, decoded);
    }

    [Fact]
    public void AnUndefinedValue_EncodesToNull()
    {
        IReadOnlyCollection<ClassicGroupState> defined = s_javaConstants.Select(c => c.State).ToArray();

        Assert.DoesNotContain((ClassicGroupState)int.MaxValue, defined);
        Assert.Null(GroupMarshal.NameFromClassicState((ClassicGroupState)int.MaxValue));
        Assert.Null(GroupMarshal.NameFromClassicState((ClassicGroupState)(-1)));
    }

    private static IntPtr Read(Native.Singleton getter)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = getter(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Assert.NotEqual(IntPtr.Zero, singleton);
        return singleton;
    }

    private static IntPtr Parse(string name)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(name);
        IntPtr outErr = IntPtr.Zero;
        IntPtr parsed = Native.ParseName(pinned.Pointer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Assert.NotEqual(IntPtr.Zero, parsed);
        return parsed;
    }

    private static string? ReadName(IntPtr singleton)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr name = Native.Name(singleton, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return Utf8Marshal.PtrToString(name);
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        internal delegate IntPtr Singleton(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_unknown_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr Unknown(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_preparing_rebalance_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr PreparingRebalance(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_completing_rebalance_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr CompletingRebalance(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_stable_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr Stable(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_dead_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr Dead(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_empty_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr Empty(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_name_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr Name(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_ClassicGroupState_parse_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ParseName(IntPtr name, ref IntPtr outErr);
    }
}
