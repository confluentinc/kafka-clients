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
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Builds the owned <c>kafka_admin_UserScramCredentialAlteration_t</c> handles <c>alterUserScramCredentials</c> takes,
/// and copies the descriptions and user names the <c>describeUserScramCredentials</c> futures deliver into managed
/// values.
/// </summary>
internal static unsafe class UserScramCredentialMarshal
{
    private delegate void NativeDestroy(IntPtr native, ref IntPtr outErr);

    private delegate IntPtr ToAlteration(IntPtr value, ref IntPtr outErr);

    internal static IntPtr NewAlteration(UserScramCredentialAlteration alteration)
    {
        if (alteration is UserScramCredentialDeletion deletion)
        {
            IntPtr native = NewDeletion(deletion);
            try
            {
                return Alteration(native, NativeMethods.UserScramCredentialAlterationDeletion);
            }
            finally
            {
                DestroyDeletion(native);
            }
        }

        IntPtr upsertion = NewUpsertion((UserScramCredentialUpsertion)alteration);
        try
        {
            return Alteration(upsertion, NativeMethods.UserScramCredentialAlterationUpsertion);
        }
        finally
        {
            DestroyUpsertion(upsertion);
        }
    }

    internal static void DestroyAlteration(IntPtr alteration) =>
        Destroy(alteration, NativeMethods.UserScramCredentialAlterationDestroy);

    internal static IntPtr NewDeletion(UserScramCredentialDeletion deletion)
    {
        using Utf8Marshal.PinnedUtf8String user = Utf8Marshal.Pin(deletion.User);
        IntPtr mechanism = Mechanism(deletion.Mechanism);
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.UserScramCredentialDeletionNew(user.Pointer, mechanism, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    internal static void DestroyDeletion(IntPtr deletion) =>
        Destroy(deletion, NativeMethods.UserScramCredentialDeletionDestroy);

    internal static IntPtr NewUpsertion(UserScramCredentialUpsertion upsertion)
    {
        IntPtr mechanism = Mechanism(upsertion.CredentialInfo.Mechanism);
        IntPtr outErr = IntPtr.Zero;
        IntPtr info = NativeMethods.ScramCredentialInfoNew(mechanism, upsertion.CredentialInfo.Iterations, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            using Utf8Marshal.PinnedUtf8String user = Utf8Marshal.Pin(upsertion.User);
            byte[] password = upsertion.Password;
            byte[]? salt = upsertion.Salt;
            byte empty = 0;
            fixed (byte* passwordData = password)
            fixed (byte* saltData = salt)
            {
                // An empty array crosses as a non-null pointer of length 0, which kafka_Bytes_t keeps apart from null.
                KafkaBytes passwordBytes = new KafkaBytes(
                    password.Length == 0 ? (IntPtr)(&empty) : (IntPtr)passwordData, password.Length);
                IntPtr native = salt is null
                    ? NativeMethods.UserScramCredentialUpsertionWithBytes(user.Pointer, info, passwordBytes, ref outErr)
                    : NativeMethods.UserScramCredentialUpsertionWithSalt(
                        user.Pointer,
                        info,
                        passwordBytes,
                        new KafkaBytes(salt.Length == 0 ? (IntPtr)(&empty) : (IntPtr)saltData, salt.Length),
                        ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                return native;
            }
        }
        finally
        {
            Destroy(info, NativeMethods.ScramCredentialInfoDestroy);
        }
    }

    internal static void DestroyUpsertion(IntPtr upsertion) =>
        Destroy(upsertion, NativeMethods.UserScramCredentialUpsertionDestroy);

    internal static IReadOnlyDictionary<string, UserScramCredentialsDescription> CopyOutDescriptions(IntPtr descriptions)
    {
        Dictionary<string, UserScramCredentialsDescription> copied =
            new Dictionary<string, UserScramCredentialsDescription>(StringComparer.Ordinal);
        if (descriptions == IntPtr.Zero)
        {
            return copied;
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(descriptions, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        for (int i = 0; i < count; i++)
        {
            IntPtr user = NativeMethods.MapKey(descriptions, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr description = NativeMethods.MapValue(descriptions, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied[Utf8Marshal.PtrToString(user) ?? string.Empty] = CopyOutDescription(description);
        }

        return copied;
    }

    internal static UserScramCredentialsDescription CopyOutDescription(IntPtr description)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr name = NativeMethods.UserScramCredentialsDescriptionName(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr infos = NativeMethods.UserScramCredentialsDescriptionCredentialInfos(description, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int count = NativeMethods.ListSize(infos, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<ScramCredentialInfo> copied = new List<ScramCredentialInfo>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr info = NativeMethods.ListGet(infos, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                copied.Add(CopyOutCredentialInfo(info));
            }

            return new UserScramCredentialsDescription(Utf8Marshal.PtrToString(name) ?? string.Empty, copied);
        }
        finally
        {
            Destroy(infos, NativeMethods.ListDestroy);
        }
    }

    internal static IReadOnlyList<string> CopyOutUsers(IntPtr users)
    {
        List<string> copied = new List<string>();
        if (users == IntPtr.Zero)
        {
            return copied;
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(users, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        copied.Capacity = count;
        for (int i = 0; i < count; i++)
        {
            IntPtr user = NativeMethods.ListGet(users, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied.Add(Utf8Marshal.PtrToString(user) ?? string.Empty);
        }

        return copied;
    }

    private static ScramCredentialInfo CopyOutCredentialInfo(IntPtr info)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr mechanism = NativeMethods.ScramCredentialInfoMechanism(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        sbyte type = NativeMethods.ScramMechanismType(mechanism, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int iterations = NativeMethods.ScramCredentialInfoIterations(info, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new ScramCredentialInfo(ScramMechanisms.FromType(unchecked((byte)type)), iterations);
    }

    private static IntPtr Mechanism(ScramMechanism mechanism)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.ScramMechanismFromType(unchecked((sbyte)mechanism), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static IntPtr Alteration(IntPtr value, ToAlteration convert)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = convert(value, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
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
