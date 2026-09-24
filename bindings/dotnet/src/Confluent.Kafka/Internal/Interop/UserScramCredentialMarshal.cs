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
/// Reads one <c>kafka_admin_UserScramCredentialsDescription_t</c> — the user name and the
/// walk over its credential infos.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The credential walk is bounded by that description's own
/// <c>credential_count</c>, never by any outer count</b> (the <c>all()</c> row count, the
/// <c>users()</c> count). They are unrelated: the inner count is the number of credentials on
/// this one description, and may be <c>0</c> (a user the broker reports as having none).
/// Driving it from an outer count compiles and reads both past the end of a short description
/// and short of a long one.
/// </para>
/// <para>
/// ⚠⚠ <b>The two readers differ only in ownership, and the difference has no managed
/// symptom.</b> <see cref="Read(IntPtr)"/> is for a description <b>borrowed</b> from
/// <c>all_get_description</c> — the result root owns it, so destroying it is a double free.
/// <see cref="ReadAndDestroy(IntPtr)"/> is for a description <b>owned</b> out of
/// <c>description(user)</c> — not destroying it leaks one per call. Both are shipped side by
/// side, the <c>OffsetMapMarshal.CopyOutAndDestroy</c> precedent, so the two call sites cannot
/// be confused.
/// </para>
/// <para>
/// The accessor set is a <b>parameter</b>, for the same reason
/// <c>AclRowMarshal.ReadFilter</c>'s is: the mock mirrors Java's own
/// <c>UnsupportedOperationException</c> for this RPC, so the ABI offers no way to construct a
/// populated result to assert the walk on.
/// </para>
/// </remarks>
internal static class UserScramCredentialMarshal
{
    /// <summary>The production <c>kafka_admin_UserScramCredentialsDescription_*</c> set.</summary>
    internal static readonly Accessors NativeAccessors = new Accessors(
        NativeMethods.UserScramCredentialsDescriptionName,
        NativeMethods.UserScramCredentialsDescriptionCredentialCount,
        NativeMethods.UserScramCredentialsDescriptionCredentialMechanism,
        NativeMethods.UserScramCredentialsDescriptionCredentialIterations);

    /// <summary>
    /// Reads a <b>borrowed</b> description through the production accessors. Does <b>not</b>
    /// destroy it — the result root owns it.
    /// </summary>
    /// <param name="description">The borrowed description, from <c>all_get_description</c>.</param>
    /// <returns>The copied-out description.</returns>
    internal static UserScramCredentialsDescription Read(IntPtr description) =>
        Read(description, NativeAccessors);

    /// <summary>
    /// Reads a <b>borrowed</b> description through an injected accessor set. Does <b>not</b>
    /// destroy it.
    /// </summary>
    /// <param name="description">The description, or a stand-in under an injected set.</param>
    /// <param name="accessors">The four accessors to decode it with.</param>
    /// <returns>The copied-out description.</returns>
    internal static UserScramCredentialsDescription Read(IntPtr description, Accessors accessors)
    {
        string name = KeyedResultMarshal.ReadStringKey(accessors.Name(description));

        // ⚠ Its own count — never an outer one. See the type remarks.
        int credentialCount = accessors.CredentialCount(description);
        List<ScramCredentialInfo> infos = new List<ScramCredentialInfo>(Math.Max(credentialCount, 0));
        for (int credential = 0; credential < credentialCount; credential++)
        {
            int code = accessors.CredentialMechanism(description, credential);
            infos.Add(new ScramCredentialInfo(
                code >= byte.MinValue && code <= byte.MaxValue
                    ? ScramMechanisms.FromType((byte)code)
                    : ScramMechanism.Unknown,
                accessors.CredentialIterations(description, credential)));
        }

        return new UserScramCredentialsDescription(name, infos);
    }

    /// <summary>
    /// Reads an <b>owned</b> description through the production accessors and destroys it.
    /// </summary>
    /// <param name="description">The owned description, from <c>description(user)</c>.</param>
    /// <returns>The copied-out description.</returns>
    internal static UserScramCredentialsDescription ReadAndDestroy(IntPtr description) =>
        ReadAndDestroy(description, NativeAccessors);

    /// <summary>
    /// Reads an <b>owned</b> description through an injected accessor set and destroys it.
    /// </summary>
    /// <param name="description">The owned description, or a stand-in under an injected set.</param>
    /// <param name="accessors">The four accessors to decode it with.</param>
    /// <returns>The copied-out description.</returns>
    internal static UserScramCredentialsDescription ReadAndDestroy(
        IntPtr description, Accessors accessors)
    {
        try
        {
            return Read(description, accessors);
        }
        finally
        {
            // Null-safe. Exactly once, on every path — including a throwing read.
            NativeMethods.UserScramCredentialsDescriptionDestroy(description);
        }
    }

    /// <summary>Reads a value indexed by credential.</summary>
    /// <param name="description">The description.</param>
    /// <param name="credentialIndex">The credential index within it.</param>
    /// <returns>The value, or <c>-1</c> when the index is out of range.</returns>
    internal delegate int CredentialAccessor(IntPtr description, int credentialIndex);

    /// <summary>The four per-description accessors, as one set.</summary>
    internal sealed class Accessors
    {
        /// <summary>Creates a set, in the ABI's own accessor order.</summary>
        /// <param name="name">The description's user name, borrowed.</param>
        /// <param name="credentialCount">Its credential count — the walk's bound.</param>
        /// <param name="credentialMechanism">One credential's mechanism type code.</param>
        /// <param name="credentialIterations">One credential's iteration count.</param>
        internal Accessors(
            Func<IntPtr, IntPtr> name,
            Func<IntPtr, int> credentialCount,
            CredentialAccessor credentialMechanism,
            CredentialAccessor credentialIterations)
        {
            Name = name;
            CredentialCount = credentialCount;
            CredentialMechanism = credentialMechanism;
            CredentialIterations = credentialIterations;
        }

        /// <summary><c>name()</c>.</summary>
        internal Func<IntPtr, IntPtr> Name { get; }

        /// <summary><c>credential_count()</c> — the walk's bound.</summary>
        internal Func<IntPtr, int> CredentialCount { get; }

        /// <summary><c>credential_mechanism(j)</c>.</summary>
        internal CredentialAccessor CredentialMechanism { get; }

        /// <summary><c>credential_iterations(j)</c>.</summary>
        internal CredentialAccessor CredentialIterations { get; }
    }
}
