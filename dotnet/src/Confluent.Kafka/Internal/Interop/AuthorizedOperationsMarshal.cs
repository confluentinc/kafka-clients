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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies the authorized operations the ABI hands back, a <c>kafka_List_t</c> of <c>AclOperation_t</c> singletons
/// or <c>NULL</c> for Java's null set, into an owned collection; <c>NULL</c> stays <see langword="null"/>.
/// </summary>
internal static class AuthorizedOperationsMarshal
{
    internal static IReadOnlyCollection<AclOperation>? CopyOut(
        IntPtr owner,
        Func<IntPtr, bool> has,
        Func<IntPtr, int> count,
        Func<IntPtr, int, int> read)
    {
        if (!has(owner))
        {
            return null;
        }

        int total = count(owner);
        List<AclOperation> operations = new List<AclOperation>(Math.Max(total, 0));
        for (int index = 0; index < total; index++)
        {
            operations.Add(FromCode(read(owner, index)));
        }

        return operations;
    }

    internal static IReadOnlyCollection<AclOperation>? CopyOutList(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return null;
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<AclOperation> operations = new List<AclOperation>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr operation = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte code = NativeMethods.AclOperationCode(operation, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            operations.Add(FromCode(code));
        }

        return operations;
    }

    internal static IReadOnlyCollection<AclOperation>? CopyOutAndDestroy(IntPtr list)
    {
        if (list == IntPtr.Zero)
        {
            return null;
        }

        try
        {
            return CopyOutList(list);
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.ListDestroy(list, ref destroyErr);
            if (destroyErr != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(destroyErr);
            }
        }
    }

    internal static AclOperation FromCode(int code) =>
        code >= (int)AclOperation.Unknown && code <= (int)AclOperation.TwoPhaseCommit
            ? (AclOperation)code
            : AclOperation.Unknown;
}
