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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Common.Security.Token.Delegation;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds the principal handles the delegation-token options take, passes a MAC to the core, and copies the
/// tokens the delegation-token futures deliver into managed values.
/// </summary>
internal static unsafe class DelegationTokenMarshal
{
    private delegate void NativeDestroy(IntPtr native, ref IntPtr outErr);

    internal static T WithHmac<T>(byte[]? hmac, Func<KafkaBytes, T> call)
    {
        byte empty = 0;
        fixed (byte* data = hmac)
        {
            // A null MAC crosses as a null pointer; an empty one as a non-null pointer of length 0.
            KafkaBytes bytes = hmac is null
                ? new KafkaBytes(IntPtr.Zero, 0)
                : new KafkaBytes(hmac.Length == 0 ? (IntPtr)(&empty) : (IntPtr)data, hmac.Length);
            return call(bytes);
        }
    }

    internal static IntPtr NewPrincipal(KafkaPrincipal principal)
    {
        using Utf8Marshal.PinnedUtf8String principalType = Utf8Marshal.Pin(principal.PrincipalType);
        using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(principal.Name);
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.KafkaPrincipalNew(principalType.Pointer, name.Pointer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    internal static void DestroyPrincipal(IntPtr principal) =>
        Destroy(principal, NativeMethods.KafkaPrincipalDestroy);

    internal static IntPtr[] NewPrincipals(IReadOnlyList<KafkaPrincipal> principals)
    {
        IntPtr[] natives = new IntPtr[principals.Count];
        try
        {
            for (int i = 0; i < natives.Length; i++)
            {
                natives[i] = NewPrincipal(principals[i]);
            }

            return natives;
        }
        catch
        {
            DestroyPrincipals(natives);
            throw;
        }
    }

    internal static void DestroyPrincipals(IntPtr[] principals)
    {
        foreach (IntPtr principal in principals)
        {
            DestroyPrincipal(principal);
        }
    }

    internal static IReadOnlyList<DelegationToken> CopyOutTokens(IntPtr tokens)
    {
        if (tokens == IntPtr.Zero)
        {
            return Array.Empty<DelegationToken>();
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(tokens, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<DelegationToken> copied = new List<DelegationToken>(Math.Max(count, 0));
        for (int i = 0; i < count; i++)
        {
            IntPtr token = NativeMethods.ListGet(tokens, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied.Add(CopyOutToken(token));
        }

        return copied;
    }

    internal static DelegationToken CopyOutToken(IntPtr token)
    {
        if (token == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced no delegation token.");
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr info = NativeMethods.DelegationTokenTokenInfo(token, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        KafkaBytes hmac = NativeMethods.DelegationTokenHmac(token, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new DelegationToken(CopyOutTokenInformation(info), CopyBytes(hmac));
    }

    internal static byte[] CopyBytes(KafkaBytes bytes)
    {
        if (bytes.Data == IntPtr.Zero || bytes.Length <= 0)
        {
            return Array.Empty<byte>();
        }

        byte[] copied = new byte[bytes.Length];
        Marshal.Copy(bytes.Data, copied, 0, bytes.Length);
        return copied;
    }

    internal static TokenInformation CopyOutTokenInformation(IntPtr info)
    {
        if (info == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced a delegation token with no metadata.");
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr tokenIdPointer = NativeMethods.TokenInformationTokenId(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        string tokenId = Utf8Marshal.PtrToString(tokenIdPointer)
            ?? throw new KafkaException("The admin result produced a delegation token with no token id.");
        IntPtr owner = NativeMethods.TokenInformationOwner(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr requester = NativeMethods.TokenInformationTokenRequester(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IReadOnlyList<KafkaPrincipal> renewers = CopyOutRenewers(info);
        long issueTimestamp = NativeMethods.TokenInformationIssueTimestamp(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long maxTimestamp = NativeMethods.TokenInformationMaxTimestamp(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long expiryTimestamp = NativeMethods.TokenInformationExpiryTimestamp(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);

        return requester == IntPtr.Zero
            ? new TokenInformation(
                tokenId, CopyOutPrincipal(owner), renewers, issueTimestamp, maxTimestamp, expiryTimestamp)
            : new TokenInformation(
                tokenId,
                CopyOutPrincipal(owner),
                CopyOutPrincipal(requester),
                renewers,
                issueTimestamp,
                maxTimestamp,
                expiryTimestamp);
    }

    internal static KafkaPrincipal CopyOutPrincipal(IntPtr principal)
    {
        if (principal == IntPtr.Zero)
        {
            throw new KafkaException("The admin result produced no principal.");
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr typePointer = NativeMethods.KafkaPrincipalPrincipalType(principal, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        string principalType = Utf8Marshal.PtrToString(typePointer)
            ?? throw new KafkaException("The admin result produced a principal with no type.");
        IntPtr namePointer = NativeMethods.KafkaPrincipalName(principal, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        string name = Utf8Marshal.PtrToString(namePointer)
            ?? throw new KafkaException("The admin result produced a principal with no name.");
        sbyte tokenAuthenticated = NativeMethods.KafkaPrincipalTokenAuthenticated(principal, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new KafkaPrincipal(principalType, name, tokenAuthenticated != 0);
    }

    private static IReadOnlyList<KafkaPrincipal> CopyOutRenewers(IntPtr info)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr renewers = NativeMethods.TokenInformationRenewers(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            if (renewers == IntPtr.Zero)
            {
                return Array.Empty<KafkaPrincipal>();
            }

            int count = NativeMethods.ListSize(renewers, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<KafkaPrincipal> copied = new List<KafkaPrincipal>(Math.Max(count, 0));
            for (int i = 0; i < count; i++)
            {
                IntPtr renewer = NativeMethods.ListGet(renewers, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                copied.Add(CopyOutPrincipal(renewer));
            }

            return copied;
        }
        finally
        {
            Destroy(renewers, NativeMethods.ListDestroy);
        }
    }

    private static void Destroy(IntPtr native, NativeDestroy destroy)
    {
        if (native == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        destroy(native, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
