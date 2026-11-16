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

// Creates a delegation token with IAdmin.CreateDelegationToken and prints its information and HMAC. The
// HMAC, in hexadecimal, is what the RenewDelegationToken and ExpireDelegationToken examples take. It is the
// token's password, so the output is a credential.
//
// Principals are written <type>:<name>, for example User:alice. Without --owner the token belongs to the
// authenticated caller; without --max-lifetime-ms the broker's delegation.token.max.lifetime.ms applies.
//
// The broker must have delegation tokens enabled (delegation.token.secret.key) and the connection must be
// authenticated with SASL (not with a delegation token) or two-way TLS.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 CreateDelegationToken \
//         -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=PLAIN \
//         -X 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="secret";' \
//         --renewer=User:bob

using System;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Common.Security.Token.Delegation;

/// <summary>The CreateDelegationToken example.</summary>
internal static class CreateDelegationToken
{
    public const string Arguments = "[--renewer=<type>:<name>]... [--owner=<type>:<name>] [--max-lifetime-ms=<ms>]";

    public static void Run(IAdmin admin, Args args)
    {
        List<KafkaPrincipal> renewers = [];
        foreach (string text in args.Values("renewer"))
        {
            renewers.Add(ParsePrincipal(text));
        }

        KafkaPrincipal? owner = args.Value("owner") is { } ownerText ? ParsePrincipal(ownerText) : null;
        long? maxLifetimeMs =
            args.Value("max-lifetime-ms") is { } lifetimeText ? Cli.Parse<long>("max lifetime", lifetimeText) : null;
        if (args.Positional().Length > 0)
        {
            throw new UsageException("unexpected positional argument");
        }

        CreateDelegationTokenOptions options = new CreateDelegationTokenOptions().Renewers(renewers).Owner(owner);
        if (maxLifetimeMs is { } lifetime)
        {
            options = options.MaxLifetimeMs(lifetime);
        }

        DelegationToken token = admin.CreateDelegationToken(options).DelegationToken().Get();
        TokenInformation info = token.TokenInfo();
        Console.WriteLine($"Created delegation token {info.TokenId()}");
        Console.WriteLine($"Owner: {info.OwnerAsString()}");
        Console.WriteLine($"Requester: {info.TokenRequesterAsString()}");
        Console.WriteLine($"Renewers: {string.Join(",", info.RenewersAsString())}");
        Console.WriteLine($"Issue timestamp: {info.IssueTimestamp()}");
        Console.WriteLine($"Expiry timestamp: {info.ExpiryTimestamp()}");
        Console.WriteLine($"Max timestamp: {info.MaxTimestamp()}");

        // The HMAC is the token's password: with the token id it authenticates as the owner. This is the
        // only time it is shown without asking for it, so keep it as you would a password.
        Console.WriteLine($"HMAC (base64): {token.HmacAsBase64String()}");
        Console.WriteLine($"HMAC (hex): {Cli.ToHex(token.Hmac())}");
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
