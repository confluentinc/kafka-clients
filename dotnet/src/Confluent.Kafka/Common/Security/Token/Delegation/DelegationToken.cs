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

namespace Confluent.Kafka.Common.Security.Token.Delegation;

/// <summary>
/// A delegation token — Java's
/// <c>org.apache.kafka.common.security.token.delegation.DelegationToken</c>.
/// </summary>
public sealed class DelegationToken
{
    private readonly byte[] _hmac;

    /// <summary>Creates a token — Java's constructor.</summary>
    /// <param name="tokenInfo">The token metadata.</param>
    /// <param name="hmac">The raw MAC bytes. May contain interior zero bytes.</param>
    /// <exception cref="ArgumentNullException">Either argument is null.</exception>
    public DelegationToken(TokenInformation tokenInfo, byte[] hmac)
    {
        TokenInfo = tokenInfo ?? throw new ArgumentNullException(nameof(tokenInfo));
        _hmac = hmac ?? throw new ArgumentNullException(nameof(hmac));
    }

    /// <summary>The token metadata — Java's <c>tokenInfo()</c>.</summary>
    public TokenInformation TokenInfo { get; }

    /// <summary>The raw MAC bytes — Java's <c>hmac()</c>.</summary>
    public byte[] Hmac => _hmac;

    /// <summary>
    /// The MAC base64-encoded — Java's <c>hmacAsBase64String()</c>, derived from
    /// <see cref="Hmac"/>.
    /// </summary>
    public string HmacAsBase64String => Convert.ToBase64String(_hmac);

    /// <summary>
    /// Value equality over the metadata and the MAC — Java's <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same token.</returns>
    /// <remarks>
    /// The MAC compare takes the same time wherever the first differing byte is, as Java's
    /// <c>MessageDigest.isEqual</c>, so it leaks nothing about secret material through timing.
    /// </remarks>
    public override bool Equals(object? obj) =>
        obj is DelegationToken other
        && TokenInfo.Equals(other.TokenInfo)
        && ConstantTimeEquals(_hmac, other._hmac);

    /// <summary>The hash of the metadata and the MAC — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = TokenInfo.GetHashCode();
            foreach (byte value in _hmac)
            {
                hash = (hash * 31) + value;
            }

            return hash;
        }
    }

    /// <summary>
    /// A diagnostic rendering in Java's <c>toString()</c> format, the MAC masked as
    /// <c>[*******]</c>, since it is secret material.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "DelegationToken{{tokenInformation={0}, hmac=[*******]}}",
            TokenInfo);

    private static bool ConstantTimeEquals(byte[] left, byte[] right)
    {
        int difference = left.Length ^ right.Length;
        int common = Math.Min(left.Length, right.Length);
        for (int i = 0; i < common; i++)
        {
            difference |= left[i] ^ right[i];
        }

        return difference == 0;
    }
}
