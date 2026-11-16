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

// Fences the producers of transactional ids with IAdmin.FenceProducers. The coordinator bumps each id's
// producer epoch and aborts its open transaction, so a producer still using the id fails with
// ProducerFencedException on its next transactional call. The example prints the producer id and the new
// epoch.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 FenceProducers my-transactional-id

using System;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

/// <summary>The FenceProducers example.</summary>
internal static class FenceProducers
{
    public const string Arguments = "<transactional_id>...";

    public static void Run(IAdmin admin, Args args)
    {
        string[] transactionalIds = args.Positional();
        if (transactionalIds.Length == 0)
        {
            throw new UsageException("no transactional id given");
        }

        FenceProducersResult result = admin.FenceProducers(transactionalIds);
        foreach (string transactionalId in transactionalIds)
        {
            try
            {
                long producerId = result.ProducerId(transactionalId).Get();
                short epoch = result.EpochId(transactionalId).Get();
                Console.WriteLine($"Fenced {transactionalId}: producer id {producerId}, epoch {epoch}");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to fence {transactionalId}", e);
            }
        }
    }
}
