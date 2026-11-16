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

// Expires a delegation token with IAdmin.ExpireDelegationToken and prints its new expiry timestamp. The
// token is named by its HMAC in hexadecimal, as the CreateDelegationToken example prints it. Without
// --period-ms (or with a negative one) the token expires at once; otherwise it expires that many
// milliseconds from now.
//
// Only the owner or a renewer may expire a token, over a connection authenticated with SASL (not with a
// delegation token) or two-way TLS.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ExpireDelegationToken \
//         -X security.protocol=SASL_PLAINTEXT ... <hmac_hex>

using System;
using Confluent.Kafka.Admin;

/// <summary>The ExpireDelegationToken example.</summary>
internal static class ExpireDelegationToken
{
    public const string Arguments = "[--period-ms=<ms>] <hmac_hex>";

    public static void Run(IAdmin admin, Args args)
    {
        long? periodMs = args.Value("period-ms") is { } text ? Cli.Parse<long>("expiry period", text) : null;
        string[] positional = args.Positional();
        if (positional.Length != 1)
        {
            throw new UsageException("expected exactly one HMAC");
        }

        byte[] hmac = Cli.FromHex(positional[0]);

        ExpireDelegationTokenOptions options = new();
        if (periodMs is { } period)
        {
            options = options.ExpiryTimePeriodMs(period);
        }

        long expiryTimestamp = admin.ExpireDelegationToken(hmac, options).ExpiryTimestamp().Get();
        Console.WriteLine($"Delegation token expired, expiry timestamp: {expiryTimestamp}");
    }
}
