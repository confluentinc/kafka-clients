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

// Lists the groups known to the cluster with IAdmin.ListGroups. --states keeps only the groups in the given
// states and --types only the groups of the given types. The call asks every broker; a broker that fails is
// reported and the listings of the others are still printed.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ListGroups --states=stable,empty \
//         --types=consumer,classic

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

/// <summary>The ListGroups example.</summary>
internal static class ListGroups
{
    public const string Arguments = "[--states=state,...] [--types=type,...]";

    public static void Run(IAdmin admin, Args args)
    {
        string states = args.Value("states") ?? "";
        string types = args.Value("types") ?? "";
        if (args.Positional().Length > 0)
        {
            throw new UsageException("unexpected positional argument");
        }

        HashSet<GroupState> groupStates = [.. Cli.CommaList(states).Select(ParseState)];
        HashSet<GroupType> groupTypes = [.. Cli.CommaList(types).Select(ParseType)];

        // An empty set of states or types keeps the groups in every state or of every type.
        ListGroupsResult result = admin.ListGroups(
            new ListGroupsOptions().InGroupStates(groupStates).WithTypes(groupTypes));

        // Valid() never fails: a broker's failure is in Errors() instead.
        foreach (GroupListing listing in result.Valid().Get().OrderBy(l => l.GroupId(), StringComparer.Ordinal))
        {
            string type = listing.Type() is { } groupType ? GroupTypeMethods.ToString(groupType) : "none";
            string state =
                listing.GroupState() is { } groupState ? GroupStateMethods.ToString(groupState) : "none";
            Console.WriteLine(
                $"Group {listing.GroupId()}: type {type}, state {state}, protocol \"{listing.Protocol()}\", " +
                $"simple {(listing.IsSimpleConsumerGroup() ? "true" : "false")}");
        }

        foreach (Exception error in result.Errors().Get())
        {
            Cli.Fail("Failed to list groups", error);
        }
    }

    private static GroupState ParseState(string text)
    {
        return Cli.Choose(
            "group state",
            text,
            ("preparing_rebalance", GroupState.PREPARING_REBALANCE),
            ("completing_rebalance", GroupState.COMPLETING_REBALANCE),
            ("stable", GroupState.STABLE),
            ("dead", GroupState.DEAD),
            ("empty", GroupState.EMPTY),
            ("assigning", GroupState.ASSIGNING),
            ("reconciling", GroupState.RECONCILING),
            ("not_ready", GroupState.NOT_READY),
            ("unknown", GroupState.UNKNOWN));
    }

    private static GroupType ParseType(string text)
    {
        return Cli.Choose(
            "group type",
            text,
            ("consumer", GroupType.CONSUMER),
            ("classic", GroupType.CLASSIC),
            ("share", GroupType.SHARE),
            ("streams", GroupType.STREAMS));
    }
}
