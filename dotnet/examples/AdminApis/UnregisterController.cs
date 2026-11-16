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

// Unregisters a KRaft controller with IAdmin.UnregisterController (KAFKA-20395): the active controller
// removes the controller's registration, for example after the node has been removed from the controller
// quorum (KIP-853) and shut down for good. The controller refuses an id that is not registered
// (ControllerIdNotRegisteredException) and the id of the active controller itself (InvalidRequestException).
//
// A broker forwards the request to the active controller. With -X bootstrap.controllers=... the client sends
// it to the controllers directly; the first argument is then not used:
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 UnregisterController 3001
//     dotnet run -c Release --project AdminApis -- - UnregisterController \
//         -X bootstrap.controllers=localhost:9093 3001

using System;
using Confluent.Kafka.Admin;

/// <summary>The UnregisterController example.</summary>
internal static class UnregisterController
{
    public const string Arguments = "<controller_id>";

    public static void Run(IAdmin admin, Args args)
    {
        string[] positional = args.Positional();
        if (positional.Length != 1)
        {
            throw new UsageException("expected exactly one controller id");
        }

        int controllerId = Cli.Parse<int>("controller id", positional[0]);

        admin.UnregisterController(controllerId).All().Get();
        Console.WriteLine($"Controller {controllerId} unregistered");
    }
}
