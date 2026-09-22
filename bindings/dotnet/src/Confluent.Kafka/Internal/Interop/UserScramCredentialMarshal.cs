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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Reads one row of the <c>describeUserScramCredentials</c> table — the user, that user's
/// <b>borrowed</b> error, and the nested <c>(i, j)</c> walk over its credential infos.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The inner walk is bounded by <c>get_credential_count(i)</c>, never by the outer
/// <c>count</c>.</b> The two are unrelated: the inner count is the number of credentials for
/// that one user, and is <c>0</c> for a user the call failed to describe
/// (<c>confluent_kafka.h:9424-9425</c>). Driving it from the outer count compiles and reads
/// both past the end of a short row and short of a long one.
/// </para>
/// <para>
/// The accessor set is a <b>parameter</b>, for the same reason
/// <c>AclRowMarshal.ReadFilter</c>'s is: the mock mirrors Java's own
/// <c>UnsupportedOperationException</c> for this RPC, so the ABI offers no way to construct a
/// populated result to assert the nested walk on.
/// </para>
/// </remarks>
internal static class UserScramCredentialMarshal
{
    /// <summary>The production <c>kafka_admin_DescribeUserScramCredentialsResult_*</c> set.</summary>
    internal static readonly Accessors NativeAccessors = new Accessors(
        NativeMethods.DescribeUserScramCredentialsResultGetUser,
        NativeMethods.DescribeUserScramCredentialsResultGetError,
        NativeMethods.DescribeUserScramCredentialsResultGetCredentialCount,
        NativeMethods.DescribeUserScramCredentialsResultGetCredentialMechanism,
        NativeMethods.DescribeUserScramCredentialsResultGetCredentialIterations);

    /// <summary>Reads row <paramref name="index"/> through the production accessors.</summary>
    /// <param name="result">The owned result root.</param>
    /// <param name="index">The row index, inside the result's own count.</param>
    /// <returns>The copied-out row.</returns>
    internal static UserScramCredentialEntry ReadEntry(IntPtr result, int index) =>
        ReadEntry(result, index, NativeAccessors);

    /// <summary>Reads row <paramref name="index"/> through an injected accessor set.</summary>
    /// <param name="result">The result root, or a stand-in under an injected set.</param>
    /// <param name="index">The row index.</param>
    /// <param name="accessors">The five accessors to decode it with.</param>
    /// <returns>The copied-out row.</returns>
    internal static UserScramCredentialEntry ReadEntry(IntPtr result, int index, Accessors accessors)
    {
        string user = KeyedResultMarshal.ReadStringKey(accessors.GetUser(result, index));

        // ⚠ BORROWED — read, never destroy; it dies with the result root.
        KafkaException? error = KafkaException.FromBorrowedHandle(accessors.GetError(result, index));

        // ⚠ Its own count — never the outer one. See the type remarks.
        int credentialCount = accessors.GetCredentialCount(result, index);
        List<ScramCredentialInfo> infos = new List<ScramCredentialInfo>(Math.Max(credentialCount, 0));
        for (int credential = 0; credential < credentialCount; credential++)
        {
            int code = accessors.GetCredentialMechanism(result, index, credential);
            infos.Add(new ScramCredentialInfo(
                code >= byte.MinValue && code <= byte.MaxValue
                    ? ScramMechanisms.FromType((byte)code)
                    : ScramMechanism.Unknown,
                accessors.GetCredentialIterations(result, index, credential)));
        }

        return new UserScramCredentialEntry(user, error, new UserScramCredentialsDescription(user, infos));
    }

    /// <summary>Reads a value indexed by <c>(row, credential)</c>.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The row index.</param>
    /// <param name="credentialIndex">The credential index within that row.</param>
    /// <returns>The value, or <c>-1</c> when either index is out of range.</returns>
    internal delegate int CredentialAccessor(IntPtr result, int index, int credentialIndex);

    /// <summary>The five per-row accessors, as one set.</summary>
    internal sealed class Accessors
    {
        /// <summary>Creates a set, in the ABI's own accessor order.</summary>
        /// <param name="getUser">The row's user name, borrowed.</param>
        /// <param name="getError">The row's error, borrowed, or null on success.</param>
        /// <param name="getCredentialCount">The row's credential count — the inner bound.</param>
        /// <param name="getCredentialMechanism">One credential's mechanism type code.</param>
        /// <param name="getCredentialIterations">One credential's iteration count.</param>
        internal Accessors(
            KeyedResultMarshal.IndexedAccessor getUser,
            KeyedResultMarshal.IndexedAccessor getError,
            Func<IntPtr, int, int> getCredentialCount,
            CredentialAccessor getCredentialMechanism,
            CredentialAccessor getCredentialIterations)
        {
            GetUser = getUser;
            GetError = getError;
            GetCredentialCount = getCredentialCount;
            GetCredentialMechanism = getCredentialMechanism;
            GetCredentialIterations = getCredentialIterations;
        }

        /// <summary><c>get_user(i)</c>.</summary>
        internal KeyedResultMarshal.IndexedAccessor GetUser { get; }

        /// <summary><c>get_error(i)</c> — borrowed.</summary>
        internal KeyedResultMarshal.IndexedAccessor GetError { get; }

        /// <summary><c>get_credential_count(i)</c> — the inner walk's bound.</summary>
        internal Func<IntPtr, int, int> GetCredentialCount { get; }

        /// <summary><c>get_credential_mechanism(i, j)</c>.</summary>
        internal CredentialAccessor GetCredentialMechanism { get; }

        /// <summary><c>get_credential_iterations(i, j)</c>.</summary>
        internal CredentialAccessor GetCredentialIterations { get; }
    }
}
