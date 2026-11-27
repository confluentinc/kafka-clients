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
using System.Runtime.InteropServices;
using System.Threading;
using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The values of a user's <see cref="IConsumerGroupMetadata"/>, read once and copied to native memory, which the core
/// reads through the C implementation <see cref="Register"/> makes of them. The memory is freed once, when the handle
/// is released: by the caller once the operation it was passed to has completed, or by the finalizer.
/// </summary>
internal sealed class ConsumerGroupMetadataValues : SafeHandleZeroIsInvalid
{
    private static readonly int s_memberIdOffset = IntPtr.Size;
    private static readonly int s_groupInstanceIdOffset = 2 * IntPtr.Size;
    private static readonly int s_generationIdOffset = 3 * IntPtr.Size;

    private static readonly NativeMethods.ConsumerGroupMetadataStringGetter s_groupId = GroupId;
    private static readonly NativeMethods.ConsumerGroupMetadataIntGetter s_generationId = GenerationId;
    private static readonly NativeMethods.ConsumerGroupMetadataStringGetter s_memberId = MemberId;
    private static readonly NativeMethods.ConsumerGroupMetadataStringGetter s_groupInstanceId = GroupInstanceId;

    // Java's ConsumerGroupMetadata constructor refuses a null group id and member id with these messages; the C ABI
    // takes NULL only for the group instance id, as Java's Optional.empty().
    private const string NullGroupIdMessage = "group.id can't be null";
    private const string NullMemberIdMessage = "member.id can't be null";

    // The blocks not yet freed: a test reads it to check that a block outlives a wait its caller gave up, until the
    // operation completes, and is freed once.
    private static int s_live;

    private ConsumerGroupMetadataValues()
    {
    }

    /// <summary>The number of value blocks allocated and not yet freed.</summary>
    internal static int Live => Volatile.Read(ref s_live);

    /// <summary>
    /// Reads the four values of <paramref name="metadata"/> on the calling thread and copies them to native memory, a
    /// <see langword="null"/> group instance id as <c>NULL</c>, Java's <c>Optional.empty()</c>.
    /// </summary>
    /// <param name="metadata">The user's group metadata.</param>
    /// <returns>The owned copy.</returns>
    /// <exception cref="ArgumentNullException">The group id or the member id is <see langword="null"/>, which Java's
    /// <c>ConsumerGroupMetadata</c> refuses.</exception>
    /// <exception cref="ArgumentException">A value contains a NUL character.</exception>
    internal static ConsumerGroupMetadataValues Of(IConsumerGroupMetadata metadata)
    {
        string? groupId = metadata.GroupId();
        int generationId = metadata.GenerationId();
        string? memberId = metadata.MemberId();
        string? groupInstanceId = metadata.GroupInstanceId();
        if (groupId is null)
        {
            throw new ArgumentNullException("groupMetadata", NullGroupIdMessage);
        }

        if (memberId is null)
        {
            throw new ArgumentNullException("groupMetadata", NullMemberIdMessage);
        }

        NativeStrings.Validate(groupId, NativeStrings.InvalidGroupMetadataMessage, "groupMetadata");
        NativeStrings.Validate(memberId, NativeStrings.InvalidGroupMetadataMessage, "groupMetadata");
        NativeStrings.Validate(groupInstanceId, NativeStrings.InvalidGroupMetadataMessage, "groupMetadata");

        ConsumerGroupMetadataValues values = new ConsumerGroupMetadataValues();
        IntPtr block = Marshal.AllocHGlobal(s_generationIdOffset + sizeof(long));
        Marshal.WriteIntPtr(block, 0, IntPtr.Zero);
        Marshal.WriteIntPtr(block, s_memberIdOffset, IntPtr.Zero);
        Marshal.WriteIntPtr(block, s_groupInstanceIdOffset, IntPtr.Zero);
        Marshal.WriteInt32(block, s_generationIdOffset, generationId);
        values.SetHandle(block);
        Interlocked.Increment(ref s_live);
        try
        {
            Marshal.WriteIntPtr(block, 0, Copy(groupId));
            Marshal.WriteIntPtr(block, s_memberIdOffset, Copy(memberId));
            Marshal.WriteIntPtr(block, s_groupInstanceIdOffset, Copy(groupInstanceId));
            return values;
        }
        catch
        {
            values.Dispose();
            throw;
        }
    }

    /// <summary>
    /// The C implementation of these values, for one call. The core takes its own reference during the call, so the
    /// returned handle may be released once it has returned; these values must stay until the operation completes.
    /// </summary>
    /// <returns>The owned <c>kafka_consumer_ConsumerGroupMetadata_t</c>.</returns>
    internal SafeConsumerGroupMetadataHandle Register()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr metadata = NativeMethods.ConsumerGroupMetadataNew(
            handle, s_groupId, s_generationId, s_memberId, s_groupInstanceId, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new SafeConsumerGroupMetadataHandle(metadata);
    }

    protected override bool ReleaseHandle()
    {
        Free(Marshal.ReadIntPtr(handle, 0));
        Free(Marshal.ReadIntPtr(handle, s_memberIdOffset));
        Free(Marshal.ReadIntPtr(handle, s_groupInstanceIdOffset));
        Marshal.FreeHGlobal(handle);
        Interlocked.Decrement(ref s_live);
        return true;
    }

    private static IntPtr Copy(string? value)
    {
        if (value is null)
        {
            return IntPtr.Zero;
        }

        byte[] bytes = Utf8Marshal.QuestionMarkUtf8.GetBytes(value);
        IntPtr copy = Marshal.AllocHGlobal(bytes.Length + 1);
        Marshal.Copy(bytes, 0, copy, bytes.Length);
        Marshal.WriteByte(copy, bytes.Length, 0);
        return copy;
    }

    private static void Free(IntPtr copy)
    {
        if (copy != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(copy);
        }
    }

    private static IntPtr GroupId(IntPtr self)
    {
        try
        {
            return Marshal.ReadIntPtr(self, 0);
        }
        catch (Exception)
        {
            // Never unwind into native.
            return default;
        }
    }

    private static int GenerationId(IntPtr self)
    {
        try
        {
            return Marshal.ReadInt32(self, s_generationIdOffset);
        }
        catch (Exception)
        {
            // Never unwind into native.
            return -1;
        }
    }

    private static IntPtr MemberId(IntPtr self)
    {
        try
        {
            return Marshal.ReadIntPtr(self, s_memberIdOffset);
        }
        catch (Exception)
        {
            // Never unwind into native.
            return default;
        }
    }

    private static IntPtr GroupInstanceId(IntPtr self)
    {
        try
        {
            return Marshal.ReadIntPtr(self, s_groupInstanceIdOffset);
        }
        catch (Exception)
        {
            // Never unwind into native.
            return default;
        }
    }
}
