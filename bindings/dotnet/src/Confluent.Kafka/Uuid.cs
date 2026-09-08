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
using System.Globalization;

namespace Confluent.Kafka;

/// <summary>
/// A Kafka topic / cluster identifier — the .NET realization of Java's
/// <c>org.apache.kafka.common.Uuid</c>. A 128-bit value whose text form is the
/// <b>URL-safe, unpadded base64</b> encoding of its 16 big-endian bytes (Java's
/// <c>Uuid.toString()</c>), which is also the form the C ABI hands topic ids across in.
/// </summary>
/// <remarks>
/// <para>
/// Deliberately <b>not</b> <see cref="System.Guid"/>: a <see cref="System.Guid"/>'s
/// canonical text form is the hyphenated hex layout and its byte order is
/// mixed-endian, so it would neither round-trip Kafka's wire identity nor match what a
/// broker, a Java client or another binding prints for the same topic.
/// </para>
/// <para>
/// Java's package is <c>org.apache.kafka.common</c> rather than <c>…clients.admin</c>,
/// so this type lives in the root <c>Confluent.Kafka</c> namespace, not under
/// <c>Confluent.Kafka.Admin</c>.
/// </para>
/// </remarks>
public readonly struct Uuid : IEquatable<Uuid>
{
    /// <summary>The number of bytes a Kafka <see cref="Uuid"/> occupies.</summary>
    private const int ByteCount = 16;

    /// <summary>
    /// Java's own "too long" bound — <c>str.length() &gt; 24</c> (<c>Uuid.java:131</c>).
    /// The canonical form is 22 unpadded characters, but the extra two are Java's
    /// tolerance for the <b>padded</b> 24-character form, which
    /// <c>Base64.getUrlDecoder()</c> decodes to exactly 16 bytes and <c>fromString</c>
    /// therefore accepts. Gating at 22 would reject that, and would also report a 23- or
    /// 24-character input as "too long" where Java reports the decoded byte count.
    /// </summary>
    private const int MaxTextLength = 24;

    /// <summary>
    /// Initializes a new instance from its two 64-bit halves (Java's
    /// <c>Uuid(long, long)</c>).
    /// </summary>
    /// <param name="mostSignificantBits">The high 64 bits.</param>
    /// <param name="leastSignificantBits">The low 64 bits.</param>
    public Uuid(long mostSignificantBits, long leastSignificantBits)
    {
        MostSignificantBits = mostSignificantBits;
        LeastSignificantBits = leastSignificantBits;
    }

    /// <summary>
    /// The all-zero identifier — Java's <c>Uuid.ZERO_UUID</c>, and the value a
    /// <see cref="Uuid"/> takes when a broker returned no topic id.
    /// </summary>
    public static Uuid Zero => default;

    /// <summary>The high 64 bits (Java's <c>getMostSignificantBits()</c>).</summary>
    public long MostSignificantBits { get; }

    /// <summary>The low 64 bits (Java's <c>getLeastSignificantBits()</c>).</summary>
    public long LeastSignificantBits { get; }

    /// <summary>Whether two identifiers are equal.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns><see langword="true"/> if both halves match.</returns>
    public static bool operator ==(Uuid left, Uuid right) => left.Equals(right);

    /// <summary>Whether two identifiers differ.</summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns><see langword="true"/> if either half differs.</returns>
    public static bool operator !=(Uuid left, Uuid right) => !left.Equals(right);

    /// <summary>
    /// Parses the URL-safe, unpadded base64 text form — Java's
    /// <c>Uuid.fromString(String)</c>.
    /// </summary>
    /// <param name="value">The 22-character base64 text (a padded 24-character form is accepted too).</param>
    /// <returns>The parsed identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="value"/> is rejected. Three conditions do that, and each matches
    /// what Java's <c>fromString</c> rejects:
    /// <list type="bullet">
    /// <item><description>
    /// longer than 24 characters (Java's own bound, <c>Uuid.java:131</c>) — <b>same
    /// message</b> as Java;
    /// </description></item>
    /// <item><description>
    /// it decodes to some number of bytes other than 16 — <b>same message</b> as Java;
    /// </description></item>
    /// <item><description>
    /// it is not valid URL-safe base64 — including a literal <c>+</c> or <c>/</c>, which
    /// Java's <c>Base64.getUrlDecoder()</c> rejects. The <b>message here is this
    /// binding's own</b>, not Java's: Java surfaces the JDK decoder's text
    /// (<c>"Illegal base64 character 2b"</c>), which is an implementation detail of the
    /// decoder rather than part of Kafka's contract.
    /// </description></item>
    /// </list>
    /// </exception>
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
    /// Attempts to parse the URL-safe, unpadded base64 text form without throwing — the
    /// .NET counterpart of <see cref="Parse(string)"/>.
    /// </summary>
    /// <param name="value">The base64 text, or <see langword="null"/>.</param>
    /// <param name="result">
    /// The parsed identifier on success; <see cref="Zero"/> on failure.
    /// </param>
    /// <returns><see langword="true"/> if <paramref name="value"/> parsed.</returns>
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

    /// <summary>
    /// The URL-safe, unpadded base64 text form of the 16 big-endian bytes — Java's
    /// <c>Uuid.toString()</c>, and the exact form the C ABI exchanges topic ids in.
    /// </summary>
    /// <returns>A 22-character base64 string.</returns>
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

    /// <summary>
    /// Decodes the URL-safe base64 text the way Java's
    /// <c>Base64.getUrlDecoder()</c> does, throwing <see cref="FormatException"/> on
    /// anything it would reject.
    /// </summary>
    /// <remarks>
    /// ⚠ The alphabet screen is the load-bearing part. The floor has no base64url
    /// decoder, so the text is translated to the standard alphabet and handed to
    /// <see cref="Convert.FromBase64String(string)"/> — which <b>accepts</b> the standard
    /// <c>+</c> and <c>/</c> that Java's URL decoder rejects. Without this screen,
    /// <c>Parse("AAAAAAAAAAAAAAAAAAAA+/")</c> would succeed here and throw in Java, and
    /// the resulting <see cref="Uuid"/> would print a <em>different</em> string from the
    /// one it was parsed from.
    /// </remarks>
    private static byte[] DecodeUrlSafeBase64(string value)
    {
        // IndexOf(char) is ordinal, so this is a plain character scan.
        if (value.IndexOf('+') >= 0 || value.IndexOf('/') >= 0)
        {
            throw new FormatException(
                "Input contains a character outside the URL-safe base64 alphabet.");
        }

        return Convert.FromBase64String(ToStandardBase64(value));
    }

    /// <summary>
    /// Converts the URL-safe form back to standard base64, re-padding to a multiple of
    /// four, so <see cref="Convert.FromBase64String(string)"/> can read it (the floor has
    /// no base64url decoder).
    /// </summary>
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
