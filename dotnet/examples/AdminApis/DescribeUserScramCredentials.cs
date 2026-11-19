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

// Describes SASL/SCRAM credentials with IAdmin.DescribeUserScramCredentials: the mechanisms and iteration
// counts of each user's credentials, never the salted passwords.
//
// Without users every user with a credential is described. A given user without one fails with a
// resource-not-found error.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeUserScramCredentials alice bob

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

/// <summary>The DescribeUserScramCredentials example.</summary>
internal static class DescribeUserScramCredentials
{
    public const string Arguments = "[<user>...]";

    public static void Run(IAdmin admin, Args args)
    {
        string[] users = args.Positional();

        if (users.Length == 0)
        {
            IReadOnlyDictionary<string, UserScramCredentialsDescription> descriptions =
                admin.DescribeUserScramCredentials().All().Get();
            foreach (string name in descriptions.Keys.Order(StringComparer.Ordinal))
            {
                PrintDescription(descriptions[name]);
            }

            return;
        }

        // One request for all users; each user's description is its own future.
        DescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials(users);
        foreach (string user in users)
        {
            try
            {
                PrintDescription(result.Description(user).Get());
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to describe SCRAM credentials of user {user}", e);
            }
        }
    }

    private static void PrintDescription(UserScramCredentialsDescription description)
    {
        IEnumerable<string> credentials = description.CredentialInfos()
            .Select(info => $"{info.Mechanism().MechanismName()} iterations={info.Iterations()}")
            .Order(StringComparer.Ordinal);
        Console.WriteLine($"User {description.Name()}: {string.Join(", ", credentials)}");
    }
}
