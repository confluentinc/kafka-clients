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

// Renews a delegation token with IAdmin.RenewDelegationToken and prints its new expiry timestamp. The token
// is named by its HMAC in hexadecimal, as the CreateDelegationToken example prints it. Without --period-ms
// the broker's delegation.token.expiry.time.ms applies; the expiry never passes the token's max timestamp.
//
// Only the owner or a renewer may renew a token, over a connection authenticated with SASL (not with a
// delegation token) or two-way TLS.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 RenewDelegationToken \
//         -X security.protocol=SASL_PLAINTEXT ... --period-ms=3600000 <hmac_hex>

using System;
using Confluent.Kafka.Admin;

/// <summary>The RenewDelegationToken example.</summary>
internal static class RenewDelegationToken
{
    public const string Arguments = "[--period-ms=<ms>] <hmac_hex>";

    public static void Run(IAdmin admin, Args args)
    {
        long? periodMs = args.Value("period-ms") is { } text ? Cli.Parse<long>("renew period", text) : null;
        string[] positional = args.Positional();
        if (positional.Length != 1)
        {
            throw new UsageException("expected exactly one HMAC");
        }

        byte[] hmac = Cli.FromHex(positional[0]);

        RenewDelegationTokenOptions options = new();
        if (periodMs is { } period)
        {
            options = options.RenewTimePeriodMs(period);
        }

        long expiryTimestamp = admin.RenewDelegationToken(hmac, options).ExpiryTimestamp().Get();
        Console.WriteLine($"Delegation token renewed, expiry timestamp: {expiryTimestamp}");
    }
}
