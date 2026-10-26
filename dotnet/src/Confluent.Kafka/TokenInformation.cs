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
using System.Globalization;
using System.Linq;

namespace Confluent.Kafka;

/// <summary>
/// The metadata of a delegation token — Java's
/// <c>org.apache.kafka.common.security.token.delegation.TokenInformation</c>.
/// </summary>
/// <remarks>
/// A recorded deviation: <see cref="Equals(object)"/> and <see cref="GetHashCode"/> cover the same
/// fields, <see cref="ExpiryTimestamp"/> included, where Java's <c>equals</c> leaves it out and its
/// <c>hashCode</c> does not. The type is immutable (Java has <c>setExpiryTimestamp</c>), and Java's
/// <c>fromRecord</c> is not bound, as it duplicates the seven-argument constructor.
/// </remarks>
public sealed class TokenInformation
{
    private readonly KafkaPrincipal[] _renewers;

    /// <summary>
    /// Creates token metadata whose requester is its owner — Java's six-argument constructor.
    /// </summary>
    /// <param name="tokenId">The token id.</param>
    /// <param name="owner">The token owner.</param>
    /// <param name="renewers">The principals allowed to renew the token.</param>
    /// <param name="issueTimestamp">When the token was issued, in ms since the epoch.</param>
    /// <param name="maxTimestamp">The token's maximum lifetime, in ms since the epoch.</param>
    /// <param name="expiryTimestamp">When the token expires, in ms since the epoch.</param>
    /// <exception cref="ArgumentNullException">A reference argument is null.</exception>
    public TokenInformation(
        string tokenId,
        KafkaPrincipal owner,
        IEnumerable<KafkaPrincipal> renewers,
        long issueTimestamp,
        long maxTimestamp,
        long expiryTimestamp)
        : this(tokenId, owner, owner, renewers, issueTimestamp, maxTimestamp, expiryTimestamp)
    {
    }

    /// <summary>Creates token metadata — Java's seven-argument constructor.</summary>
    /// <param name="tokenId">The token id.</param>
    /// <param name="owner">The token owner.</param>
    /// <param name="tokenRequester">The principal that requested the token.</param>
    /// <param name="renewers">The principals allowed to renew the token.</param>
    /// <param name="issueTimestamp">When the token was issued, in ms since the epoch.</param>
    /// <param name="maxTimestamp">The token's maximum lifetime, in ms since the epoch.</param>
    /// <param name="expiryTimestamp">When the token expires, in ms since the epoch.</param>
    /// <exception cref="ArgumentNullException">A reference argument is null.</exception>
    public TokenInformation(
        string tokenId,
        KafkaPrincipal owner,
        KafkaPrincipal tokenRequester,
        IEnumerable<KafkaPrincipal> renewers,
        long issueTimestamp,
        long maxTimestamp,
        long expiryTimestamp)
    {
        TokenId = tokenId ?? throw new ArgumentNullException(nameof(tokenId));
        Owner = owner ?? throw new ArgumentNullException(nameof(owner));
        TokenRequester = tokenRequester ?? throw new ArgumentNullException(nameof(tokenRequester));
        if (renewers is null)
        {
            throw new ArgumentNullException(nameof(renewers));
        }

        _renewers = renewers.ToArray();
        IssueTimestamp = issueTimestamp;
        MaxTimestamp = maxTimestamp;
        ExpiryTimestamp = expiryTimestamp;
    }

    /// <summary>The token id — Java's <c>tokenId()</c>.</summary>
    public string TokenId { get; }

    /// <summary>The token owner — Java's <c>owner()</c>.</summary>
    public KafkaPrincipal Owner { get; }

    /// <summary>The requesting principal — Java's <c>tokenRequester()</c>.</summary>
    public KafkaPrincipal TokenRequester { get; }

    /// <summary>The principals allowed to renew — Java's <c>renewers()</c>.</summary>
    public IReadOnlyList<KafkaPrincipal> Renewers => _renewers;

    /// <summary>When the token was issued — Java's <c>issueTimestamp()</c>.</summary>
    public long IssueTimestamp { get; }

    /// <summary>When the token expires — Java's <c>expiryTimestamp()</c>.</summary>
    public long ExpiryTimestamp { get; }

    /// <summary>The token's maximum lifetime — Java's <c>maxTimestamp()</c>.</summary>
    public long MaxTimestamp { get; }

    /// <summary><see cref="Owner"/> rendered as a string — Java's <c>ownerAsString()</c>.</summary>
    public string OwnerAsString => Owner.ToString();

    /// <summary>
    /// <see cref="TokenRequester"/> rendered as a string — Java's <c>tokenRequesterAsString()</c>.
    /// </summary>
    public string TokenRequesterAsString => TokenRequester.ToString();

    /// <summary><see cref="Renewers"/> rendered as strings — Java's <c>renewersAsString()</c>.</summary>
    public IReadOnlyList<string> RenewersAsString =>
        Array.ConvertAll(_renewers, renewer => renewer.ToString());

    /// <summary>
    /// Whether <paramref name="principal"/> is the owner, the requester or a renewer — Java's
    /// <c>ownerOrRenewer(KafkaPrincipal)</c>.
    /// </summary>
    /// <param name="principal">The principal to test.</param>
    /// <returns>Whether the principal may act on the token.</returns>
    public bool OwnerOrRenewer(KafkaPrincipal principal) =>
        Owner.Equals(principal)
        || TokenRequester.Equals(principal)
        || Array.IndexOf(_renewers, principal) >= 0;

    /// <summary>
    /// Value equality over every field, <see cref="ExpiryTimestamp"/> included, unlike Java's
    /// <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same token.</returns>
    public override bool Equals(object? obj) =>
        obj is TokenInformation other
        && string.Equals(TokenId, other.TokenId, StringComparison.Ordinal)
        && Owner.Equals(other.Owner)
        && TokenRequester.Equals(other.TokenRequester)
        && IssueTimestamp == other.IssueTimestamp
        && MaxTimestamp == other.MaxTimestamp
        && ExpiryTimestamp == other.ExpiryTimestamp
        && _renewers.SequenceEqual(other._renewers);

    /// <summary>The hash of the same field set — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(TokenId);
            hash = (hash * 31) + Owner.GetHashCode();
            hash = (hash * 31) + TokenRequester.GetHashCode();
            hash = (hash * 31) + IssueTimestamp.GetHashCode();
            hash = (hash * 31) + MaxTimestamp.GetHashCode();
            hash = (hash * 31) + ExpiryTimestamp.GetHashCode();
            foreach (KafkaPrincipal renewer in _renewers)
            {
                hash = (hash * 31) + renewer.GetHashCode();
            }

            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "TokenInformation{{owner={0}, tokenRequester={1}, renewers=[{2}], issueTimestamp={3}, "
            + "maxTimestamp={4}, expiryTimestamp={5}, tokenId='{6}'}}",
            Owner,
            TokenRequester,
            string.Join(", ", RenewersAsString),
            IssueTimestamp,
            MaxTimestamp,
            ExpiryTimestamp,
            TokenId);
}
