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

// Lists the transactions the transaction coordinators know about with IAdmin.ListTransactions. Every broker
// is asked, so on a large cluster filter the result: by state (--states, Java's state names such as Ongoing
// or PrepareCommit, in any case), by producer id, by transactional id regular expression, or with
// --duration-ms to keep only transactions running for longer than that.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ListTransactions --states=Ongoing \
//         --duration-ms=60000

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Admin;

/// <summary>The ListTransactions example.</summary>
internal static class ListTransactions
{
    public const string Arguments =
        "[--states=<state>,...] [--producer-ids=<id>,...] [--duration-ms=<ms>] [--pattern=<regex>]";

    public static void Run(IAdmin admin, Args args)
    {
        string? states = args.Value("states");
        string? producerIds = args.Value("producer-ids");
        string? duration = args.Value("duration-ms");
        long? durationMs = duration is null ? null : Cli.Parse<long>("duration", duration);
        string? pattern = args.Value("pattern");
        if (args.Positional().Length > 0)
        {
            throw new UsageException("unexpected positional argument");
        }

        ListTransactionsOptions options = new();
        if (states is not null)
        {
            List<TransactionState> filteredStates = [.. Cli.CommaList(states).Select(ParseState)];
            options.FilterStates(filteredStates);
        }

        if (producerIds is not null)
        {
            List<long> filteredProducerIds =
                [.. Cli.CommaList(producerIds).Select(id => Cli.Parse<long>("producer id", id))];
            options.FilterProducerIds(filteredProducerIds);
        }

        if (durationMs is { } ms)
        {
            options.FilterOnDuration(ms);
        }

        if (pattern is not null)
        {
            options.FilterOnTransactionalIdPattern(pattern);
        }

        IReadOnlyCollection<TransactionListing> listings = admin.ListTransactions(options).All().Get();
        if (listings.Count == 0)
        {
            Console.WriteLine("No transactions");
        }

        foreach (TransactionListing listing in
            listings.OrderBy(item => item.TransactionalId(), StringComparer.Ordinal))
        {
            // The static ToString gives Java's text (Ongoing); the enum's own gives the constant (ONGOING).
            Console.WriteLine(
                $"Transaction {listing.TransactionalId()}: producer id {listing.ProducerId()}, " +
                $"state {TransactionStateMethods.ToString(listing.State())}");
        }
    }

    private static TransactionState ParseState(string text)
    {
        return Cli.Choose(
            "state",
            text,
            ("ongoing", TransactionState.ONGOING),
            ("prepareabort", TransactionState.PREPARE_ABORT),
            ("preparecommit", TransactionState.PREPARE_COMMIT),
            ("completeabort", TransactionState.COMPLETE_ABORT),
            ("completecommit", TransactionState.COMPLETE_COMMIT),
            ("empty", TransactionState.EMPTY),
            ("prepareepochfence", TransactionState.PREPARE_EPOCH_FENCE));
    }
}
