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
/// decodes to <c>UNKNOWN</c>, as Java's <c>parse</c> does, and a null singleton, Java's <c>Optional.empty()</c>,
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
            1 => GroupType.CONSUMER,
            2 => GroupType.CLASSIC,
            3 => GroupType.SHARE,
            4 => GroupType.STREAMS,
            _ => GroupType.UNKNOWN,
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
            1 => GroupState.PREPARING_REBALANCE,
            2 => GroupState.COMPLETING_REBALANCE,
            3 => GroupState.STABLE,
            4 => GroupState.DEAD,
            5 => GroupState.EMPTY,
            6 => GroupState.ASSIGNING,
            7 => GroupState.RECONCILING,
            8 => GroupState.NOT_READY,
            _ => GroupState.UNKNOWN,
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
            1 => ClassicGroupState.PREPARING_REBALANCE,
            2 => ClassicGroupState.COMPLETING_REBALANCE,
            3 => ClassicGroupState.STABLE,
            4 => ClassicGroupState.DEAD,
            5 => ClassicGroupState.EMPTY,
            _ => ClassicGroupState.UNKNOWN,
        };
    }

    internal static IntPtr TypeSingleton(GroupType type)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = type switch
        {
            GroupType.UNKNOWN => NativeMethods.GroupTypeUnknown(ref outErr),
            GroupType.CONSUMER => NativeMethods.GroupTypeConsumer(ref outErr),
            GroupType.CLASSIC => NativeMethods.GroupTypeClassic(ref outErr),
            GroupType.SHARE => NativeMethods.GroupTypeShare(ref outErr),
            GroupType.STREAMS => NativeMethods.GroupTypeStreams(ref outErr),
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
            GroupState.UNKNOWN => NativeMethods.GroupStateUnknown(ref outErr),
            GroupState.PREPARING_REBALANCE => NativeMethods.GroupStatePreparingRebalance(ref outErr),
            GroupState.COMPLETING_REBALANCE => NativeMethods.GroupStateCompletingRebalance(ref outErr),
            GroupState.STABLE => NativeMethods.GroupStateStable(ref outErr),
            GroupState.DEAD => NativeMethods.GroupStateDead(ref outErr),
            GroupState.EMPTY => NativeMethods.GroupStateEmpty(ref outErr),
            GroupState.ASSIGNING => NativeMethods.GroupStateAssigning(ref outErr),
            GroupState.RECONCILING => NativeMethods.GroupStateReconciling(ref outErr),
            GroupState.NOT_READY => NativeMethods.GroupStateNotReady(ref outErr),
            _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Not a defined GroupState member."),
        };
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    internal static string? NameFromType(GroupType type) =>
        type switch
        {
            GroupType.UNKNOWN => "Unknown",
            GroupType.CONSUMER => "Consumer",
            GroupType.CLASSIC => "Classic",
            GroupType.SHARE => "Share",
            GroupType.STREAMS => "Streams",
            _ => null,
        };

    internal static string? NameFromState(GroupState state) =>
        state switch
        {
            GroupState.UNKNOWN => "Unknown",
            GroupState.PREPARING_REBALANCE => "PreparingRebalance",
            GroupState.COMPLETING_REBALANCE => "CompletingRebalance",
            GroupState.STABLE => "Stable",
            GroupState.DEAD => "Dead",
            GroupState.EMPTY => "Empty",
            GroupState.ASSIGNING => "Assigning",
            GroupState.RECONCILING => "Reconciling",
            GroupState.NOT_READY => "NotReady",
            _ => null,
        };

    internal static string? NameFromClassicState(ClassicGroupState state) =>
        state switch
        {
            ClassicGroupState.UNKNOWN => "Unknown",
            ClassicGroupState.PREPARING_REBALANCE => "PreparingRebalance",
            ClassicGroupState.COMPLETING_REBALANCE => "CompletingRebalance",
            ClassicGroupState.STABLE => "Stable",
            ClassicGroupState.DEAD => "Dead",
            ClassicGroupState.EMPTY => "Empty",
            _ => null,
        };
}
