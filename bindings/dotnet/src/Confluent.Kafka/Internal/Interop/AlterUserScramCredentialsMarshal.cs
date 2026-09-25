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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The ten-array row projector behind <c>alter_user_scram_credentials_async</c> — one
/// projector for both row kinds of the closed <see cref="UserScramCredentialAlteration"/>
/// hierarchy, with a deletion's payload columns left unset.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b><c>has_salts[i]</c> is set from <c>Salt is not null</c>, at this one site</b>, so the
/// discriminant cannot diverge between construction and marshalling. A cleared flag asks the
/// core to <em>generate</em> a salt; a set flag uses the supplied bytes verbatim, including a
/// zero-length salt (<c>confluent_kafka.h:8864-8872</c>). Deriving the flag from the salt's
/// <em>length</em> would silently upgrade an explicit empty salt into a generated one, and the
/// inverse would store a credential with no salt at all.
/// </para>
/// <para>
/// ⚠ <b>Secret material.</b> <c>passwords[i]</c> carries a raw SCRAM password. Nothing here
/// logs, traces or renders a password or salt byte, and neither appears in any exception
/// message this file or its callers produce.
/// </para>
/// <para>
/// ⚠ <b>An empty password is deliberately NOT rejected</b> (<c>h:8858-8863</c>): Java records
/// <c>UnacceptableCredentialException</c> against that user and still sends every other user's
/// alteration, so the failure arrives per-user through <c>get_error</c>. A managed
/// precondition would convert a per-key failure into a whole-call throw and drop the other
/// users' alterations.
/// </para>
/// <para>
/// All pins are <b>call-scoped</b> (ffi §A4): the ABI copies every row out during the submit,
/// so <see cref="Rows"/> is disposed in the caller's <c>finally</c>.
/// </para>
/// </remarks>
internal static class AlterUserScramCredentialsMarshal
{
    /// <summary>Projects the alterations onto the ten columns.</summary>
    /// <param name="alterations">The alterations, in request order.</param>
    /// <returns>The pinned columns.</returns>
    internal static Rows Pin(IReadOnlyList<UserScramCredentialAlteration> alterations)
    {
        Rows rows = new Rows(alterations.Count);
        try
        {
            for (int i = 0; i < alterations.Count; i++)
            {
                rows.Set(i, alterations[i]);
            }

            rows.Seal();
            return rows;
        }
        catch
        {
            rows.Dispose();
            throw;
        }
    }

    /// <summary>One submit's ten columns plus the call-scoped pins behind five of them.</summary>
    internal sealed class Rows : IDisposable
    {
        private readonly List<Utf8Marshal.PinnedUtf8String> _pinnedStrings;
        private readonly List<GCHandle> _pinnedArrays;
        private readonly byte[] _isDeletions;
        private readonly byte[] _hasSalts;

        /// <summary>Allocates the ten columns for <paramref name="count"/> rows.</summary>
        /// <param name="count">The row count.</param>
        internal Rows(int count)
        {
            Count = count;
            Users = new IntPtr[count];
            _isDeletions = new byte[count];
            Mechanisms = new int[count];
            Iterations = new int[count];
            Passwords = new IntPtr[count];
            PasswordLens = new int[count];
            Salts = new IntPtr[count];
            SaltLens = new int[count];
            _hasSalts = new byte[count];
            _pinnedStrings = new List<Utf8Marshal.PinnedUtf8String>(count);
            _pinnedArrays = new List<GCHandle>((count * 2) + 2);
        }

        /// <summary>The row count every column is sized to.</summary>
        internal int Count { get; }

        /// <summary>Column 0 — each row's user name.</summary>
        internal IntPtr[] Users { get; }

        /// <summary>
        /// Column 1 — <c>const bool *</c>, as a pinned <c>byte[]</c> of 0/1. Valid only after
        /// <see cref="Seal"/>.
        /// </summary>
        internal IntPtr IsDeletions { get; private set; }

        /// <summary>Column 2 — <c>ScramMechanism.type()</c> per row.</summary>
        internal int[] Mechanisms { get; }

        /// <summary>Column 3 — the iteration count; ignored by the ABI for a deletion row.</summary>
        internal int[] Iterations { get; }

        /// <summary>Column 4 — the pinned password bytes, or null on a deletion row.</summary>
        internal IntPtr[] Passwords { get; }

        /// <summary>Column 5 — each password's length.</summary>
        internal int[] PasswordLens { get; }

        /// <summary>Column 6 — the pinned salt bytes, or null when none is supplied.</summary>
        internal IntPtr[] Salts { get; }

        /// <summary>Column 7 — each salt's length.</summary>
        internal int[] SaltLens { get; }

        /// <summary>
        /// Column 8 — the salt discriminant, as a pinned <c>byte[]</c> of 0/1. Valid only after
        /// <see cref="Seal"/>.
        /// </summary>
        internal IntPtr HasSalts { get; private set; }

        /// <summary>The 0/1 deletion flags, for assertions; the ABI reads <see cref="IsDeletions"/>.</summary>
        internal byte[] IsDeletionFlags => _isDeletions;

        /// <summary>The 0/1 salt discriminants, for assertions; the ABI reads <see cref="HasSalts"/>.</summary>
        internal byte[] HasSaltFlags => _hasSalts;

        /// <summary>Fills one row, pinning its user name and any payload bytes.</summary>
        /// <param name="index">The row index.</param>
        /// <param name="alteration">The alteration for that row.</param>
        /// <exception cref="ArgumentException">The alteration is of an unknown kind.</exception>
        internal void Set(int index, UserScramCredentialAlteration alteration)
        {
            Users[index] = AclRowMarshal.PinName(alteration.User, _pinnedStrings);

            switch (alteration)
            {
                case UserScramCredentialUpsertion upsertion:
                    _isDeletions[index] = 0;
                    Mechanisms[index] = (int)upsertion.CredentialInfo.Mechanism;
                    Iterations[index] = upsertion.CredentialInfo.Iterations;
                    Passwords[index] = PinBytes(upsertion.Password);
                    PasswordLens[index] = upsertion.Password.Length;

                    // ⚠ The discriminant is presence, never length. See the type remarks.
                    byte[]? salt = upsertion.Salt;
                    _hasSalts[index] = salt is null ? (byte)0 : (byte)1;
                    Salts[index] = salt is null ? IntPtr.Zero : PinBytes(salt);
                    SaltLens[index] = salt?.Length ?? 0;
                    break;

                case UserScramCredentialDeletion deletion:
                    _isDeletions[index] = 1;
                    Mechanisms[index] = (int)deletion.Mechanism;

                    // The ABI ignores every payload column on a deletion row.
                    Iterations[index] = 0;
                    Passwords[index] = IntPtr.Zero;
                    PasswordLens[index] = 0;
                    Salts[index] = IntPtr.Zero;
                    SaltLens[index] = 0;
                    _hasSalts[index] = 0;
                    break;

                default:
                    throw new ArgumentException(
                        "The SCRAM credential alterations must be upsertions or deletions.",
                        nameof(alteration));
            }
        }

        /// <summary>Pins the two <c>bool</c> columns, once every row has been filled.</summary>
        internal void Seal()
        {
            // The core rejects a NULL is_deletions outright, and pinning a zero-length array
            // yields an undocumented address (ffi §A4) — so an empty request pins a one-byte
            // stand-in the ABI never reads, its count being 0.
            IsDeletions = PinArray(Count == 0 ? new byte[1] : _isDeletions);
            HasSalts = PinArray(Count == 0 ? new byte[1] : _hasSalts);
        }

        /// <summary>Releases every pin — call-scoped, in the submit's <c>finally</c>.</summary>
        public void Dispose()
        {
            foreach (Utf8Marshal.PinnedUtf8String pin in _pinnedStrings)
            {
                pin.Dispose();
            }

            _pinnedStrings.Clear();

            foreach (GCHandle pin in _pinnedArrays)
            {
                pin.Free();
            }

            _pinnedArrays.Clear();
            IsDeletions = IntPtr.Zero;
            HasSalts = IntPtr.Zero;
        }

        // A zero-length payload crosses as (NULL, 0): the core reads both that and a non-null
        // pointer with length 0 as an empty slice, and AddrOfPinnedObject on an empty array is
        // undocumented (ffi §A4).
        private IntPtr PinBytes(byte[] bytes) => bytes.Length == 0 ? IntPtr.Zero : PinArray(bytes);

        private IntPtr PinArray(Array inner)
        {
            GCHandle pin = GCHandle.Alloc(inner, GCHandleType.Pinned);
            _pinnedArrays.Add(pin);
            return pin.AddrOfPinnedObject();
        }
    }
}
