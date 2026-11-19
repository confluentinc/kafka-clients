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

// Force-terminates the transaction of a transactional id with IAdmin.ForceTerminateTransaction (KIP-939).
// Like the FenceProducers example it bumps the producer epoch and aborts any ongoing transaction, but it
// targets one id and also aborts a prepared two-phase-commit transaction, which a producer with
// transaction.two.phase.commit.enable leaves open on purpose until its external coordinator decides.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ForceTerminateTransaction my-transactional-id

using System;
using Confluent.Kafka.Admin;

/// <summary>The ForceTerminateTransaction example.</summary>
internal static class ForceTerminateTransaction
{
    public const string Arguments = "<transactional_id>";

    public static void Run(IAdmin admin, Args args)
    {
        string[] positional = args.Positional();
        if (positional.Length != 1)
        {
            throw new UsageException("expected exactly one transactional id");
        }

        string transactionalId = positional[0];
        admin.ForceTerminateTransaction(transactionalId).Result().Get();
        Console.WriteLine($"Transaction {transactionalId} terminated");
    }
}
