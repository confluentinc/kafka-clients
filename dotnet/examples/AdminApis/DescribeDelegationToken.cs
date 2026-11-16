// Copyright 2026 Confluent Inc.
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

// Describes delegation tokens with IAdmin.DescribeDelegationToken. With owner principals (<type>:<name>,
// for example User:alice) only their tokens are returned, otherwise every token the caller may describe:
// its own, those it renews and those it has DESCRIBE permission on.
//
// A token's HMAC is its password: with the token id it authenticates as the owner. It is printed only
// with --show-hmac.
//
// The broker must have delegation tokens enabled and the connection must be authenticated with SASL (not
// with a delegation token) or two-way TLS.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeDelegationToken \
//         -X security.protocol=SASL_PLAINTEXT ... User:alice

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Common.Security.Token.Delegation;

/// <summary>The DescribeDelegationToken example.</summary>
internal static class DescribeDelegationToken
{
    public const string Arguments = "[--show-hmac] [<type>:<name>]...";

    public static void Run(IAdmin admin, Args args)
    {
        bool showHmac = args.Flag("show-hmac");
        List<KafkaPrincipal> owners = [];
        foreach (string text in args.Positional())
        {
            owners.Add(ParsePrincipal(text));
        }

        // No owner means every token: a null list, where an empty one would match none.
        DescribeDelegationTokenResult result = admin.DescribeDelegationToken(
            new DescribeDelegationTokenOptions().Owners(owners.Count == 0 ? null : owners));
        IReadOnlyList<DelegationToken> tokens = result.DelegationTokens().Get();
        if (tokens.Count == 0)
        {
            Console.WriteLine("No delegation tokens");
        }

        foreach (DelegationToken token in tokens.OrderBy(token => token.TokenInfo().TokenId(), StringComparer.Ordinal))
        {
            TokenInformation info = token.TokenInfo();
            Console.WriteLine(
                $"Token {info.TokenId()}: owner {info.OwnerAsString()}, " +
                $"requester {info.TokenRequesterAsString()}, " +
                $"renewers [{string.Join(",", info.RenewersAsString())}], issued {info.IssueTimestamp()}, " +
                $"expires {info.ExpiryTimestamp()}, max {info.MaxTimestamp()}");
            if (showHmac)
            {
                Console.WriteLine($"  HMAC (hex): {Cli.ToHex(token.Hmac())}");
            }
        }
    }

    private static KafkaPrincipal ParsePrincipal(string text)
    {
        int separator = text.IndexOf(':');
        if (separator <= 0 || separator == text.Length - 1)
        {
            throw new UsageException($"invalid principal \"{text}\", expected <type>:<name>");
        }

        return new KafkaPrincipal(text[..separator], text[(separator + 1)..]);
    }
}
