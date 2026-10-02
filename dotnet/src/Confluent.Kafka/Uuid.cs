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
/// <para>
/// Ordered as Java orders it (<c>Comparable&lt;Uuid&gt;</c>, <c>Uuid.java:154-167</c>):
/// the two halves compared as <b>signed</b> 64-bit values, most significant first.
/// </para>
/// </remarks>
public readonly struct Uuid : IEquatable<Uuid>, IComparable<Uuid>
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
    /// The backing store of <see cref="Reserved"/>, and the set <see cref="RandomUuid(Func{Uuid})"/>
    /// tests against — one instance, so the published set and the one the generator avoids
    /// cannot drift apart. Read-only, so a caller cannot un-reserve a value by casting.
    /// </summary>
    private static readonly ReadOnlyCollection<Uuid> s_reserved =
        new ReadOnlyCollection<Uuid>(new[] { Zero, One });

    /// <summary>The real v4 source <see cref="RandomUuid()"/> hands to the seam.</summary>
    private static readonly Func<Uuid> s_unsafeRandomUuid = UnsafeRandomUuid;

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

    /// <summary>
    /// A reserved identifier, <c>(0, 1)</c> — Java's <c>Uuid.ONE_UUID</c>
    /// (<c>Uuid.java:37</c>). <see cref="RandomUuid()"/> never returns it.
    /// </summary>
    public static Uuid One => new Uuid(0L, 1L);

    /// <summary>
    /// The id of the metadata topic in KRaft mode — Java's <c>Uuid.METADATA_TOPIC_ID</c>,
    /// which is <see cref="One"/> (<c>Uuid.java:42</c>). <see cref="RandomUuid()"/> never
    /// returns it.
    /// </summary>
    public static Uuid MetadataTopicId => One;

    /// <summary>
    /// The identifiers <see cref="RandomUuid()"/> never returns — exactly
    /// <see cref="Zero"/> and <see cref="One"/> — Java's <c>Uuid.RESERVED</c>
    /// (<c>Uuid.java:52</c>).
    /// </summary>
    /// <remarks>
    /// Java's type is <c>Set&lt;Uuid&gt;</c>; <c>IReadOnlySet&lt;T&gt;</c> post-dates
    /// netstandard2.0, so this is the binding's standing read-only collection substitute
    /// (the same as <c>ListTopicsResult</c>). Java's <c>Set.of</c> has no defined iteration
    /// order, so none is promised here either.
    /// </remarks>
    public static IReadOnlyCollection<Uuid> Reserved => s_reserved;

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
    /// <paramref name="value"/> is rejected, for one of:
    /// <list type="bullet">
    /// <item><description>
    /// longer than 24 characters (Java's own bound, <c>Uuid.java:131</c>) — <b>same
    /// message</b> as Java;
    /// </description></item>
    /// <item><description>
    /// it decodes to some number of bytes other than 16 — <b>same message</b> as Java;
    /// </description></item>
    /// <item><description>
    /// it is not valid URL-safe base64. That covers a literal <c>+</c> or <c>/</c>, which
    /// Java's <c>Base64.getUrlDecoder()</c> rejects, and a terminal unit that carries
    /// padding yet is short of it (<c>"…Ag="</c>), which the same decoder rejects as a
    /// wrong 4-byte ending unit — see <see cref="DecodeUrlSafeBase64"/>. The
    /// <b>message here is this binding's own</b>, not Java's: Java surfaces the JDK
    /// decoder's text (<c>"Illegal base64 character 2b"</c>), which is an implementation
    /// detail of the decoder rather than part of Kafka's contract.
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

    /// <summary>
    /// A random version-4 identifier that is neither reserved nor prints with a leading
    /// <c>'-'</c> — Java's <c>Uuid.randomUuid()</c> (<c>Uuid.java:71-82</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Java's loop, literally: draw a version-4, IETF-variant UUID (the same kind
    /// <c>java.util.UUID.randomUUID()</c> produces, from a cryptographically strong source),
    /// and draw again while it is in <see cref="Reserved"/> or its text form starts with
    /// <c>'-'</c>. The second condition is why a Kafka topic id never looks like a
    /// command-line flag.
    /// </para>
    /// <para>
    /// This is a C# helper rather than an ABI call (M15/P13.2, D4): no ABI function
    /// generates a <see cref="Uuid"/>, and the type's text form is already C#, so the
    /// generator is value-type scaffolding of the same kind, not Kafka behaviour.
    /// </para>
    /// </remarks>
    /// <returns>A random identifier.</returns>
    public static Uuid RandomUuid() => RandomUuid(s_unsafeRandomUuid);

    /// <summary>
    /// Compares two identifiers exactly as Java's <c>compareTo</c> does
    /// (<c>Uuid.java:154-167</c>): <see cref="MostSignificantBits"/> first, then
    /// <see cref="LeastSignificantBits"/>, each as a <b>signed</b> <see cref="long"/> — so an
    /// identifier whose high bit is set sorts <em>before</em> <see cref="Zero"/>.
    /// </summary>
    /// <param name="other">The identifier to compare with.</param>
    /// <returns>Exactly <c>1</c>, <c>-1</c> or <c>0</c>, as Java returns.</returns>
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

    /// <summary>
    /// <see cref="RandomUuid()"/>'s loop over an injectable candidate source — the test
    /// seam. <see cref="RandomUuid()"/> is this method called with the real source, so the
    /// loop a test drives is the loop production runs (<c>definition-of-done.md</c> §12).
    /// </summary>
    /// <param name="unsafeRandomUuid">
    /// Java's <c>unsafeRandomUuid()</c>: yields one candidate per call, reserved or not.
    /// </param>
    /// <returns>The first candidate that is neither reserved nor <c>'-'</c>-leading.</returns>
    internal static Uuid RandomUuid(Func<Uuid> unsafeRandomUuid)
    {
        Uuid uuid = unsafeRandomUuid();

        // `ToString()[0] == '-'` is Java's `toString().startsWith("-")`: the text form is
        // always 22 characters, so index 0 exists.
        while (s_reserved.Contains(uuid) || uuid.ToString()[0] == '-')
        {
            uuid = unsafeRandomUuid();
        }

        return uuid;
    }

    /// <summary>
    /// Stamps 16 random bytes as a version-4, IETF-variant UUID — the bit layout of
    /// <c>java.util.UUID.randomUUID()</c> — and reads them as the two big-endian halves
    /// (Java's <c>getMostSignificantBits</c> / <c>getLeastSignificantBits</c>).
    /// </summary>
    /// <param name="randomBytes">Exactly 16 random bytes; stamped in place.</param>
    /// <returns>The identifier.</returns>
    internal static Uuid FromVersion4Bytes(byte[] randomBytes)
    {
        randomBytes[6] &= 0x0f; // clear the version
        randomBytes[6] |= 0x40; // version 4
        randomBytes[8] &= 0x3f; // clear the variant
        randomBytes[8] |= 0x80; // the IETF variant (10xx)
        return new Uuid(ReadBigEndianInt64(randomBytes, 0), ReadBigEndianInt64(randomBytes, 8));
    }

    /// <summary>
    /// Java's <c>unsafeRandomUuid()</c> (<c>Uuid.java:66-69</c>): one version-4 candidate,
    /// which may still be reserved or <c>'-'</c>-leading.
    /// </summary>
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

    /// <summary>
    /// Decodes URL-safe base64 text into bytes, screening two inputs
    /// <see cref="Convert.FromBase64String(string)"/> would otherwise let through, then
    /// translating to the standard alphabet and decoding. Throws
    /// <see cref="FormatException"/> on either screen or on a decoder failure.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ Both screens are load-bearing, and both exist for the same reason: the floor has
    /// no base64url decoder, so the text is handed to
    /// <see cref="Convert.FromBase64String(string)"/>, which is more permissive than
    /// Java's <c>Base64.getUrlDecoder()</c> in these two places.
    /// </para>
    /// <para>
    /// <b>Alphabet.</b> <see cref="Convert.FromBase64String(string)"/> <b>accepts</b> the
    /// standard <c>+</c> and <c>/</c> that Java's URL decoder rejects. Without this
    /// screen, <c>Parse("AAAAAAAAAAAAAAAAAAAA+/")</c> would succeed here and throw in
    /// Java, and the resulting <see cref="Uuid"/> would print a <em>different</em> string
    /// from the one it was parsed from.
    /// </para>
    /// <para>
    /// <b>Padding shape.</b> <see cref="ToStandardBase64"/> synthesizes the padding an
    /// unpadded input needs, and pads by length alone — so it would just as happily
    /// <em>complete</em> a terminal unit that carries padding but is short of it, turning
    /// <c>"…Ag="</c> (two data characters and a single <c>=</c>) into the well-formed
    /// <c>"…Ag=="</c> and admitting an id Java rejects. Java's
    /// <c>Base64.Decoder.decode0</c> labels that case
    /// <c>xx=&#160;&#160;&#160;shiftto==6&amp;&amp;sp==sl missing last =</c> and throws
    /// <c>IllegalArgumentException("Input byte array has wrong 4-byte ending unit")</c>.
    /// So text carrying any padding of its own must already be a whole number of
    /// 4-character units; only unpadded text gets padding synthesized.
    /// </para>
    /// <para>
    /// This is <b>not</b> a stand-in for Java's decoder and must not be reused as one:
    /// <see cref="Convert.FromBase64String(string)"/> silently ignores embedded
    /// whitespace, which Java rejects as an illegal character.
    /// <see cref="Parse(string)"/> is unaffected — whitespace only shortens the decodable
    /// text, so such an input is rejected outright or decodes to fewer than 16 bytes, and
    /// reaching 16 would take 25 or more characters, past the length gate.
    /// </para>
    /// </remarks>
    private static byte[] DecodeUrlSafeBase64(string value)
    {
        // IndexOf(char) is ordinal, so these are plain character scans.
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

    /// <summary>
    /// Converts the URL-safe form back to standard base64, synthesizing the padding
    /// <see cref="Convert.FromBase64String(string)"/> requires (the floor has no base64url
    /// decoder). It pads by length alone and so cannot tell absent padding from malformed
    /// padding; <see cref="DecodeUrlSafeBase64"/> screens the latter out before calling.
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
