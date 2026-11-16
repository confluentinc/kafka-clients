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

// Creates, updates and deletes SASL/SCRAM credentials with IAdmin.AlterUserScramCredentials.
//
// The arguments are a sequence of alterations: UPSERT <user> <mechanism> <iterations> <password> [<salt>]
// sets a credential, salting the password with a random salt unless one is given, and
// DELETE <user> <mechanism> removes one. The mechanism is SCRAM-SHA-256 or SCRAM-SHA-512. A KRaft
// controller accepts only one alteration per user in a request: a user with several fails with a
// duplicate-resource error. The broker needs no SCRAM listener to store credentials, only a KRaft metadata
// version that supports them.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 AlterUserScramCredentials \
//         UPSERT alice SCRAM-SHA-256 8192 secret DELETE bob SCRAM-SHA-512

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

/// <summary>The AlterUserScramCredentials example.</summary>
internal static class AlterUserScramCredentials
{
    public const string Arguments =
        "(UPSERT <user> <mechanism> <iterations> <password> [<salt>] | DELETE <user> <mechanism>)...";

    public static void Run(IAdmin admin, Args args)
    {
        List<UserScramCredentialAlteration> alterations = ParseAlterations(args.Positional());

        // The result has one future per user, covering all of that user's alterations.
        List<string> users = [];
        foreach (UserScramCredentialAlteration alteration in alterations)
        {
            if (!users.Contains(alteration.User()))
            {
                users.Add(alteration.User());
            }
        }

        AlterUserScramCredentialsResult result = admin.AlterUserScramCredentials(alterations);
        foreach (string user in users)
        {
            try
            {
                result.Values()[user].Get();
                Console.WriteLine($"Altered SCRAM credentials of user {user}");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to alter SCRAM credentials of user {user}", e);
            }
        }
    }

    // The usage errors below never repeat an argument: with an argument missing, the one in its place may
    // be a password.

    private static List<UserScramCredentialAlteration> ParseAlterations(string[] args)
    {
        List<UserScramCredentialAlteration> alterations = [];
        int next = 0;
        while (next < args.Length)
        {
            string keyword = args[next++];
            if (keyword == "UPSERT")
            {
                string[] fields = Take(args, ref next, "UPSERT", 4);
                ScramCredentialInfo info = new(ParseMechanism(fields[1]), ParseIterations(fields[2]));

                // The salt is optional: the next argument is one unless it starts another alteration.
                if (next < args.Length && args[next] is not ("UPSERT" or "DELETE"))
                {
                    byte[] salt = Encoding.UTF8.GetBytes(args[next++]);
                    alterations.Add(
                        new UserScramCredentialUpsertion(fields[0], info, Encoding.UTF8.GetBytes(fields[3]), salt));
                }
                else
                {
                    alterations.Add(new UserScramCredentialUpsertion(fields[0], info, fields[3]));
                }
            }
            else if (keyword == "DELETE")
            {
                string[] fields = Take(args, ref next, "DELETE", 2);
                alterations.Add(new UserScramCredentialDeletion(fields[0], ParseMechanism(fields[1])));
            }
            else
            {
                throw new UsageException("expected UPSERT or DELETE");
            }
        }

        if (alterations.Count == 0)
        {
            throw new UsageException("no alteration given");
        }

        return alterations;
    }

    /// <summary>Takes the next <paramref name="count"/> arguments of the alteration.</summary>
    private static string[] Take(string[] args, ref int next, string keyword, int count)
    {
        if (args.Length - next < count)
        {
            throw new UsageException($"{keyword} needs {count} arguments");
        }

        string[] taken = args[next..(next + count)];
        next += count;
        return taken;
    }

    private static ScramMechanism ParseMechanism(string text)
    {
        ScramMechanism mechanism = ScramMechanismMethods.FromMechanismName(text.ToUpperInvariant());
        return mechanism == ScramMechanism.UNKNOWN
            ? throw new UsageException("invalid mechanism, expected SCRAM-SHA-256 or SCRAM-SHA-512")
            : mechanism;
    }

    private static int ParseIterations(string text)
    {
        return int.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int iterations)
            ? iterations
            : throw new UsageException("invalid iterations, expected a number");
    }
}
