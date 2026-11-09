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
using Confluent.Kafka.Common.Security.Auth;

namespace Confluent.Kafka.Common.Security.Token.Delegation;

/// <summary>
/// The metadata of a delegation token — Java's
/// <c>org.apache.kafka.common.security.token.delegation.TokenInformation</c>.
/// </summary>
/// <remarks>
/// A recorded deviation: <see cref="Equals(object)"/> and <see cref="GetHashCode"/> cover the same
/// fields, <see cref="ExpiryTimestamp()"/> included, where Java's <c>equals</c> leaves it out and its
/// <c>hashCode</c> does not. The type is immutable (Java has <c>setExpiryTimestamp</c>), and Java's
/// <c>fromRecord</c> is not bound, as it duplicates the seven-argument constructor.
/// </remarks>
public sealed class TokenInformation
{
    private readonly KafkaPrincipal[] _renewers;
    private readonly string _tokenId;
    private readonly KafkaPrincipal _owner;
    private readonly KafkaPrincipal _tokenRequester;
    private readonly long _issueTimestamp;
    private readonly long _expiryTimestamp;
    private readonly long _maxTimestamp;

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
        _tokenId = tokenId ?? throw new ArgumentNullException(nameof(tokenId));
        _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        _tokenRequester = tokenRequester ?? throw new ArgumentNullException(nameof(tokenRequester));
        if (renewers is null)
        {
            throw new ArgumentNullException(nameof(renewers));
        }

        _renewers = renewers.ToArray();
        _issueTimestamp = issueTimestamp;
        _maxTimestamp = maxTimestamp;
        _expiryTimestamp = expiryTimestamp;
    }

    /// <summary>The token id — Java's <c>tokenId()</c>.</summary>
    /// <returns>The token id.</returns>
    public string TokenId() => _tokenId;

    /// <summary>The token owner — Java's <c>owner()</c>.</summary>
    /// <returns>The token owner.</returns>
    public KafkaPrincipal Owner() => _owner;

    /// <summary>The requesting principal — Java's <c>tokenRequester()</c>.</summary>
    /// <returns>The requesting principal.</returns>
    public KafkaPrincipal TokenRequester() => _tokenRequester;

    /// <summary>The principals allowed to renew — Java's <c>renewers()</c>.</summary>
    /// <returns>The principals allowed to renew.</returns>
    public IReadOnlyList<KafkaPrincipal> Renewers() => _renewers;

    /// <summary>When the token was issued — Java's <c>issueTimestamp()</c>.</summary>
    /// <returns>When the token was issued.</returns>
    public long IssueTimestamp() => _issueTimestamp;

    /// <summary>When the token expires — Java's <c>expiryTimestamp()</c>.</summary>
    /// <returns>When the token expires.</returns>
    public long ExpiryTimestamp() => _expiryTimestamp;

    /// <summary>The token's maximum lifetime — Java's <c>maxTimestamp()</c>.</summary>
    /// <returns>The token's maximum lifetime.</returns>
    public long MaxTimestamp() => _maxTimestamp;

    /// <summary><see cref="Owner()"/> rendered as a string — Java's <c>ownerAsString()</c>.</summary>
    /// <returns><see cref="Owner()"/> rendered as a string.</returns>
    public string OwnerAsString() => Owner().ToString();

    /// <summary>
    /// <see cref="TokenRequester()"/> rendered as a string — Java's <c>tokenRequesterAsString()</c>.
    /// </summary>
    /// <returns><see cref="TokenRequester()"/> rendered as a string.</returns>
    public string TokenRequesterAsString() => TokenRequester().ToString();

    /// <summary><see cref="Renewers()"/> rendered as strings — Java's <c>renewersAsString()</c>.</summary>
    /// <returns><see cref="Renewers()"/> rendered as strings.</returns>
    public IReadOnlyList<string> RenewersAsString() =>
        Array.ConvertAll(_renewers, renewer => renewer.ToString());

    /// <summary>
    /// Whether <paramref name="principal"/> is the owner, the requester or a renewer — Java's
    /// <c>ownerOrRenewer(KafkaPrincipal)</c>.
    /// </summary>
    /// <param name="principal">The principal to test.</param>
    /// <returns>Whether the principal may act on the token.</returns>
    public bool OwnerOrRenewer(KafkaPrincipal principal) =>
        Owner().Equals(principal)
        || TokenRequester().Equals(principal)
        || Array.IndexOf(_renewers, principal) >= 0;

    /// <summary>
    /// Value equality over every field, <see cref="ExpiryTimestamp()"/> included, unlike Java's
    /// <c>equals</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same token.</returns>
    public override bool Equals(object? obj) =>
        obj is TokenInformation other
        && string.Equals(TokenId(), other.TokenId(), StringComparison.Ordinal)
        && Owner().Equals(other.Owner())
        && TokenRequester().Equals(other.TokenRequester())
        && IssueTimestamp() == other.IssueTimestamp()
        && MaxTimestamp() == other.MaxTimestamp()
        && ExpiryTimestamp() == other.ExpiryTimestamp()
        && _renewers.SequenceEqual(other._renewers);

    /// <summary>The hash of the same field set — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 1;
            hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(TokenId());
            hash = (hash * 31) + Owner().GetHashCode();
            hash = (hash * 31) + TokenRequester().GetHashCode();
            hash = (hash * 31) + IssueTimestamp().GetHashCode();
            hash = (hash * 31) + MaxTimestamp().GetHashCode();
            hash = (hash * 31) + ExpiryTimestamp().GetHashCode();
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
            Owner(),
            TokenRequester(),
            string.Join(", ", RenewersAsString()),
            IssueTimestamp(),
            MaxTimestamp(),
            ExpiryTimestamp(),
            TokenId());
}
