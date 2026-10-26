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
using System.Collections.ObjectModel;
using System.Globalization;
using System.Security.Cryptography;

namespace Confluent.Kafka;

/// <summary>
/// A Kafka identifier, such as a topic id, Java's <c>Uuid</c>: 128 bits whose text form is the URL-safe,
/// unpadded base64 of its 16 big-endian bytes, ordered as Java orders it, by the signed high half, then the
/// signed low half.
/// </summary>
/// <remarks>It is not a <see cref="System.Guid"/>, whose text form and byte order differ from Kafka's.</remarks>
public readonly struct Uuid : IEquatable<Uuid>, IComparable<Uuid>
{
    private const int ByteCount = 16;

    private const int MaxTextLength = 24;

    private static readonly ReadOnlyCollection<Uuid> s_reserved =
        new ReadOnlyCollection<Uuid>(new[] { Zero, One });

    private static readonly Func<Uuid> s_unsafeRandomUuid = UnsafeRandomUuid;

    /// <summary>Initializes an id from its high and low 64 bits.</summary>
    public Uuid(long mostSignificantBits, long leastSignificantBits)
    {
        MostSignificantBits = mostSignificantBits;
        LeastSignificantBits = leastSignificantBits;
    }

    /// <summary>The all-zero id, Java's <c>Uuid.ZERO_UUID</c>.</summary>
    public static Uuid Zero => default;

    /// <summary>The id <c>(0, 1)</c>, Java's <c>Uuid.ONE_UUID</c>.</summary>
    public static Uuid One => new Uuid(0L, 1L);

    /// <summary>The id of the KRaft metadata topic, <see cref="One"/>: Java's <c>Uuid.METADATA_TOPIC_ID</c>.</summary>
    public static Uuid MetadataTopicId => One;

    /// <summary>
    /// The ids <see cref="RandomUuid()"/> never returns, <see cref="Zero"/> and <see cref="One"/>: Java's
    /// <c>Uuid.RESERVED</c>.
    /// </summary>
    public static IReadOnlyCollection<Uuid> Reserved => s_reserved;

    /// <summary>The high 64 bits.</summary>
    public long MostSignificantBits { get; }

    /// <summary>The low 64 bits.</summary>
    public long LeastSignificantBits { get; }

    /// <summary>Determines whether two ids are equal.</summary>
    public static bool operator ==(Uuid left, Uuid right) => left.Equals(right);

    /// <summary>Determines whether two ids differ.</summary>
    public static bool operator !=(Uuid left, Uuid right) => !left.Equals(right);

    /// <summary>
    /// Parses the URL-safe base64 text form, padded or not: Java's <c>Uuid.fromString</c>. Throws
    /// <see cref="ArgumentNullException"/> for <see langword="null"/>, and <see cref="ArgumentException"/> for
    /// text longer than 24 characters, not valid URL-safe base64, or not decoding to 16 bytes.
    /// </summary>
    public static Uuid Parse(string value)
    {
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        if (value.Length > MaxTextLength)
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Input string with prefix `{0}` is too long to be decoded as a base64 UUID",
                    value.Substring(0, MaxTextLength)),
                nameof(value));
        }

        byte[] bytes;
        try
        {
            bytes = DecodeUrlSafeBase64(value);
        }
        catch (FormatException exception)
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Input string `{0}` is not a valid base64 UUID",
                    value),
                nameof(value),
                exception);
        }

        if (bytes.Length != ByteCount)
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Input string `{0}` decoded as {1} bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
                    value,
                    bytes.Length),
                nameof(value));
        }

        return new Uuid(ReadBigEndianInt64(bytes, 0), ReadBigEndianInt64(bytes, 8));
    }

    /// <summary>
    /// Parses as <see cref="Parse(string)"/> does, but returns <see langword="false"/>, with <see cref="Zero"/>,
    /// instead of throwing.
    /// </summary>
    public static bool TryParse(string? value, out Uuid result)
    {
        if (value is null || value.Length > MaxTextLength)
        {
            result = Zero;
            return false;
        }

        byte[] bytes;
        try
        {
            bytes = DecodeUrlSafeBase64(value);
        }
        catch (FormatException)
        {
            result = Zero;
            return false;
        }

        if (bytes.Length != ByteCount)
        {
            result = Zero;
            return false;
        }

        result = new Uuid(ReadBigEndianInt64(bytes, 0), ReadBigEndianInt64(bytes, 8));
        return true;
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        byte[] bytes = new byte[ByteCount];
        WriteBigEndianInt64(bytes, 0, MostSignificantBits);
        WriteBigEndianInt64(bytes, 8, LeastSignificantBits);

        return Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    /// <summary>
    /// A random version-4 id that is not reserved and has no <c>'-'</c> in its text form: Java's
    /// <c>Uuid.randomUuid()</c>.
    /// </summary>
    public static Uuid RandomUuid() => RandomUuid(s_unsafeRandomUuid);

    /// <inheritdoc/>
    public int CompareTo(Uuid other)
    {
        if (MostSignificantBits > other.MostSignificantBits)
        {
            return 1;
        }

        if (MostSignificantBits < other.MostSignificantBits)
        {
            return -1;
        }

        if (LeastSignificantBits > other.LeastSignificantBits)
        {
            return 1;
        }

        if (LeastSignificantBits < other.LeastSignificantBits)
        {
            return -1;
        }

        return 0;
    }

    /// <inheritdoc/>
    public bool Equals(Uuid other) =>
        MostSignificantBits == other.MostSignificantBits &&
        LeastSignificantBits == other.LeastSignificantBits;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Uuid other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        long value = MostSignificantBits ^ LeastSignificantBits;
        return (int)(value ^ (value >> 32));
    }

    internal static Uuid RandomUuid(Func<Uuid> unsafeRandomUuid)
    {
        Uuid uuid = unsafeRandomUuid();

        while (s_reserved.Contains(uuid) || uuid.ToString().IndexOf('-') >= 0)
        {
            uuid = unsafeRandomUuid();
        }

        return uuid;
    }

    internal static Uuid FromVersion4Bytes(byte[] randomBytes)
    {
        randomBytes[6] &= 0x0f;
        randomBytes[6] |= 0x40;
        randomBytes[8] &= 0x3f;
        randomBytes[8] |= 0x80;
        return new Uuid(ReadBigEndianInt64(randomBytes, 0), ReadBigEndianInt64(randomBytes, 8));
    }

    private static Uuid UnsafeRandomUuid()
    {
        byte[] bytes = new byte[ByteCount];

        // A fresh instance per draw: RandomNumberGenerator's instance members carry no
        // documented thread-safety guarantee on every target, and RandomUuid() may be
        // called concurrently. It is not a hot path.
        using (RandomNumberGenerator random = RandomNumberGenerator.Create())
        {
            random.GetBytes(bytes);
        }

        return FromVersion4Bytes(bytes);
    }

    // Rejects '+', '/' and padding short of a 4-character unit, which Convert.FromBase64String accepts and
    // Java's URL decoder rejects.
    private static byte[] DecodeUrlSafeBase64(string value)
    {
        if (value.IndexOf('+') >= 0 || value.IndexOf('/') >= 0)
        {
            throw new FormatException(
                "Input contains a character outside the URL-safe base64 alphabet.");
        }

        if (value.IndexOf('=') >= 0 && value.Length % 4 != 0)
        {
            throw new FormatException(
                "Input carries base64 padding but is not a whole number of 4-character units.");
        }

        return Convert.FromBase64String(ToStandardBase64(value));
    }

    private static string ToStandardBase64(string value)
    {
        string standard = value.Replace('-', '+').Replace('_', '/');
        int padding = standard.Length % 4;
        return padding == 0 ? standard : standard + new string('=', 4 - padding);
    }

    private static long ReadBigEndianInt64(byte[] bytes, int offset)
    {
        long value = 0;
        for (int i = 0; i < 8; i++)
        {
            value = (value << 8) | bytes[offset + i];
        }

        return value;
    }

    private static void WriteBigEndianInt64(byte[] bytes, int offset, long value)
    {
        for (int i = 0; i < 8; i++)
        {
            bytes[offset + i] = (byte)(value >> ((7 - i) * 8));
        }
    }
}
