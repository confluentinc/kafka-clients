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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a borrowed <c>kafka_common_DelegationToken_t</c> — and the
/// <c>TokenInformation_t</c> / <c>KafkaPrincipal_t</c> tree under it — out into the owned
/// managed shape.
/// </summary>
/// <remarks>
/// <para>
/// Every pointer in that tree is <c>const</c>, so it is <b>borrowed</b> and dies with the
/// <c>*Result_t</c> root: nothing here is destroyed, and everything is copied out before the
/// trampoline's <c>finally</c> destroys that root (ffi §B2 Category 4).
/// </para>
/// <para>
/// ⚠⚠ <b>The MAC is length-delimited, not NUL-terminated</b>
/// (<c>confluent_kafka.h:8740-8746</c>) — the only such slice on the admin surface. It is read
/// through <c>out_len</c>; a NUL-scan would truncate at the first interior zero byte, which a
/// uniformly random 32-byte MAC contains roughly one time in eight, and the truncated MAC then
/// round-trips into a broker-side rejection rather than a local failure (ffi §B3).
/// </para>
/// </remarks>
internal static class DelegationTokenMarshal
{
    /// <summary>
    /// Builds the per-index reader used by <c>describeDelegationToken</c>'s collection walk,
    /// over that result's own <c>get_token(i)</c>.
    /// </summary>
    /// <param name="getToken">That result's <c>get_token(i)</c>.</param>
    /// <returns>A reader over <c>(result, index)</c>.</returns>
    internal static Func<IntPtr, int, DelegationToken> TokenReader(
        KeyedResultMarshal.IndexedAccessor getToken) =>
        (result, index) => Read(getToken(result, index));

    /// <summary>Copies one borrowed token out into the owned managed shape.</summary>
    /// <param name="token">The borrowed token pointer.</param>
    /// <returns>The owned token.</returns>
    /// <exception cref="KafkaException">The ABI produced no token where one was expected.</exception>
    internal static DelegationToken Read(IntPtr token)
    {
        if (token == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced no delegation token.");
        }

        return new DelegationToken(
            ReadTokenInformation(NativeMethods.DelegationTokenTokenInfo(token)),
            ReadHmac(token));
    }

    /// <summary>Reads the MAC's <c>(ptr, out_len)</c> pair off a token.</summary>
    /// <param name="token">The borrowed token pointer.</param>
    /// <param name="length">The MAC's byte length.</param>
    /// <returns>The borrowed MAC pointer.</returns>
    internal delegate IntPtr HmacAccessor(IntPtr token, out int length);

    /// <summary>The production <c>kafka_common_DelegationToken_hmac</c>.</summary>
    internal static readonly HmacAccessor NativeHmac = NativeMethods.DelegationTokenHmac;

    /// <summary>
    /// Copies the MAC out using <c>out_len</c>. ⚠ Never NUL-scanned — see the type remarks.
    /// </summary>
    /// <param name="token">The borrowed token pointer.</param>
    /// <returns>The owned MAC bytes.</returns>
    internal static byte[] ReadHmac(IntPtr token) => ReadHmac(token, NativeHmac);

    /// <summary>Copies the MAC out through an injected accessor.</summary>
    /// <remarks>
    /// The accessor is a parameter because the mock mints its MAC from a Uuid's UTF-8, which
    /// never carries an interior zero — so the ABI cannot produce the input this read exists to
    /// survive (the <c>AclRowMarshal.ReadFilter</c> precedent).
    /// </remarks>
    /// <param name="token">The borrowed token pointer, or a stand-in under an injected accessor.</param>
    /// <param name="accessor">The accessor to read the pair with.</param>
    /// <returns>The owned MAC bytes.</returns>
    internal static byte[] ReadHmac(IntPtr token, HmacAccessor accessor)
    {
        IntPtr bytes = accessor(token, out int length);
        if (bytes == IntPtr.Zero || length <= 0)
        {
            return Array.Empty<byte>();
        }

        byte[] hmac = new byte[length];
        Marshal.Copy(bytes, hmac, 0, length);
        return hmac;
    }

    /// <summary>Copies one borrowed <c>TokenInformation_t</c> out.</summary>
    /// <param name="info">The borrowed metadata pointer.</param>
    /// <returns>The owned metadata.</returns>
    /// <exception cref="KafkaException">The ABI produced no metadata or no token id.</exception>
    internal static TokenInformation ReadTokenInformation(IntPtr info)
    {
        if (info == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced a delegation token with no metadata.");
        }

        int renewerCount = NativeMethods.TokenInformationRenewerCount(info);
        List<KafkaPrincipal> renewers = new List<KafkaPrincipal>(Math.Max(renewerCount, 0));
        for (int i = 0; i < renewerCount; i++)
        {
            renewers.Add(ReadPrincipal(NativeMethods.TokenInformationGetRenewer(info, i)));
        }

        return new TokenInformation(
            Utf8Marshal.PtrToString(NativeMethods.TokenInformationTokenId(info))
                ?? throw new KafkaException("The admin result produced a delegation token with no token id."),
            ReadPrincipal(NativeMethods.TokenInformationOwner(info)),
            ReadPrincipal(NativeMethods.TokenInformationTokenRequester(info)),
            renewers,
            NativeMethods.TokenInformationIssueTimestamp(info),
            NativeMethods.TokenInformationMaxTimestamp(info),
            NativeMethods.TokenInformationExpiryTimestamp(info));
    }

    /// <summary>Copies one borrowed <c>KafkaPrincipal_t</c> out.</summary>
    /// <param name="principal">The borrowed principal pointer.</param>
    /// <returns>The owned principal.</returns>
    /// <exception cref="KafkaException">The ABI produced no principal, or no type or name for one.</exception>
    internal static KafkaPrincipal ReadPrincipal(IntPtr principal)
    {
        if (principal == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced no principal.");
        }

        return new KafkaPrincipal(
            Utf8Marshal.PtrToString(NativeMethods.KafkaPrincipalPrincipalType(principal))
                ?? throw new KafkaException("The admin result produced a principal with no type."),
            Utf8Marshal.PtrToString(NativeMethods.KafkaPrincipalName(principal))
                ?? throw new KafkaException("The admin result produced a principal with no name."),
            NativeMethods.KafkaPrincipalTokenAuthenticated(principal));
    }

    /// <summary>
    /// Projects a principal collection onto the ABI's two parallel <c>(type, name)</c> arrays,
    /// with call-scoped pins the caller releases.
    /// </summary>
    /// <param name="principals">The principals, in request order.</param>
    /// <returns>The pinned columns.</returns>
    internal static PrincipalRows PinPrincipals(IReadOnlyList<KafkaPrincipal> principals)
    {
        PrincipalRows rows = new PrincipalRows(principals.Count);
        try
        {
            for (int i = 0; i < principals.Count; i++)
            {
                rows.Set(i, principals[i]);
            }

            return rows;
        }
        catch
        {
            rows.Dispose();
            throw;
        }
    }

    /// <summary>One submit's principal columns plus the call-scoped string pins behind them.</summary>
    internal sealed class PrincipalRows : IDisposable
    {
        private readonly List<Utf8Marshal.PinnedUtf8String> _pinned;

        /// <summary>Allocates the two columns for <paramref name="count"/> principals.</summary>
        /// <param name="count">The principal count.</param>
        internal PrincipalRows(int count)
        {
            Count = count;
            PrincipalTypes = new IntPtr[count];
            Names = new IntPtr[count];
            _pinned = new List<Utf8Marshal.PinnedUtf8String>(count * 2);
        }

        /// <summary>The row count both columns are sized to.</summary>
        internal int Count { get; }

        /// <summary>Column 0 — each principal's type.</summary>
        internal IntPtr[] PrincipalTypes { get; }

        /// <summary>Column 1 — each principal's name.</summary>
        internal IntPtr[] Names { get; }

        /// <summary>Fills one row, pinning its two strings.</summary>
        /// <param name="index">The row index.</param>
        /// <param name="principal">The principal for that row.</param>
        internal void Set(int index, KafkaPrincipal principal)
        {
            PrincipalTypes[index] = AclRowMarshal.PinName(principal.PrincipalType, _pinned);
            Names[index] = AclRowMarshal.PinName(principal.Name, _pinned);
        }

        /// <summary>Releases every string pin — call-scoped, in the submit's <c>finally</c>.</summary>
        public void Dispose()
        {
            foreach (Utf8Marshal.PinnedUtf8String name in _pinned)
            {
                name.Dispose();
            }

            _pinned.Clear();
        }
    }
}
