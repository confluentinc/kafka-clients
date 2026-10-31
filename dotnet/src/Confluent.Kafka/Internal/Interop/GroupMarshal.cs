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
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Converts between the group enums and the C ABI's singletons: a singleton the core hands out is
/// decoded through its <c>__enum</c> id, a filter value is encoded as its singleton, and the
/// <c>NameFrom*</c> functions give Java's <c>toString()</c> names.
/// </summary>
/// <remarks>
/// Each mapping is an explicit switch, never a cast, so the C and C# orders cannot drift apart. An unknown C id
/// decodes to <c>Unknown</c>, as Java's <c>parse</c> does, and a null singleton, Java's <c>Optional.empty()</c>,
/// to <see langword="null"/>.
/// </remarks>
internal static class GroupMarshal
{
    internal static GroupType? TypeFrom(IntPtr type)
    {
        if (type == IntPtr.Zero)
        {
            return null;
        }

        IntPtr outErr = IntPtr.Zero;
        int id = NativeMethods.GroupTypeEnum(type, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return id switch
        {
            1 => GroupType.Consumer,
            2 => GroupType.Classic,
            3 => GroupType.Share,
            4 => GroupType.Streams,
            _ => GroupType.Unknown,
        };
    }

    internal static GroupState? StateFrom(IntPtr state)
    {
        if (state == IntPtr.Zero)
        {
            return null;
        }

        IntPtr outErr = IntPtr.Zero;
        int id = NativeMethods.GroupStateEnum(state, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return id switch
        {
            1 => GroupState.PreparingRebalance,
            2 => GroupState.CompletingRebalance,
            3 => GroupState.Stable,
            4 => GroupState.Dead,
            5 => GroupState.Empty,
            6 => GroupState.Assigning,
            7 => GroupState.Reconciling,
            8 => GroupState.NotReady,
            _ => GroupState.Unknown,
        };
    }

    internal static ClassicGroupState? ClassicStateFrom(IntPtr state)
    {
        if (state == IntPtr.Zero)
        {
            return null;
        }

        IntPtr outErr = IntPtr.Zero;
        int id = NativeMethods.ClassicGroupStateEnum(state, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return id switch
        {
            1 => ClassicGroupState.PreparingRebalance,
            2 => ClassicGroupState.CompletingRebalance,
            3 => ClassicGroupState.Stable,
            4 => ClassicGroupState.Dead,
            5 => ClassicGroupState.Empty,
            _ => ClassicGroupState.Unknown,
        };
    }

    internal static IntPtr TypeSingleton(GroupType type)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = type switch
        {
            GroupType.Unknown => NativeMethods.GroupTypeUnknown(ref outErr),
            GroupType.Consumer => NativeMethods.GroupTypeConsumer(ref outErr),
            GroupType.Classic => NativeMethods.GroupTypeClassic(ref outErr),
            GroupType.Share => NativeMethods.GroupTypeShare(ref outErr),
            GroupType.Streams => NativeMethods.GroupTypeStreams(ref outErr),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Not a defined GroupType member."),
        };
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    internal static IntPtr StateSingleton(GroupState state)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = state switch
        {
            GroupState.Unknown => NativeMethods.GroupStateUnknown(ref outErr),
            GroupState.PreparingRebalance => NativeMethods.GroupStatePreparingRebalance(ref outErr),
            GroupState.CompletingRebalance => NativeMethods.GroupStateCompletingRebalance(ref outErr),
            GroupState.Stable => NativeMethods.GroupStateStable(ref outErr),
            GroupState.Dead => NativeMethods.GroupStateDead(ref outErr),
            GroupState.Empty => NativeMethods.GroupStateEmpty(ref outErr),
            GroupState.Assigning => NativeMethods.GroupStateAssigning(ref outErr),
            GroupState.Reconciling => NativeMethods.GroupStateReconciling(ref outErr),
            GroupState.NotReady => NativeMethods.GroupStateNotReady(ref outErr),
            _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Not a defined GroupState member."),
        };
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    internal static string? NameFromType(GroupType type) =>
        type switch
        {
            GroupType.Unknown => "Unknown",
            GroupType.Consumer => "Consumer",
            GroupType.Classic => "Classic",
            GroupType.Share => "Share",
            GroupType.Streams => "Streams",
            _ => null,
        };

    internal static string? NameFromState(GroupState state) =>
        state switch
        {
            GroupState.Unknown => "Unknown",
            GroupState.PreparingRebalance => "PreparingRebalance",
            GroupState.CompletingRebalance => "CompletingRebalance",
            GroupState.Stable => "Stable",
            GroupState.Dead => "Dead",
            GroupState.Empty => "Empty",
            GroupState.Assigning => "Assigning",
            GroupState.Reconciling => "Reconciling",
            GroupState.NotReady => "NotReady",
            _ => null,
        };

    internal static string? NameFromClassicState(ClassicGroupState state) =>
        state switch
        {
            ClassicGroupState.Unknown => "Unknown",
            ClassicGroupState.PreparingRebalance => "PreparingRebalance",
            ClassicGroupState.CompletingRebalance => "CompletingRebalance",
            ClassicGroupState.Stable => "Stable",
            ClassicGroupState.Dead => "Dead",
            ClassicGroupState.Empty => "Empty",
            _ => null,
        };
}
