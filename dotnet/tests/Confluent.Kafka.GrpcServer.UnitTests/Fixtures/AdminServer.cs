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

using System;
using System.Collections;
using System.Reflection;
using System.Threading.Tasks;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// One admin servicer, sync (<see cref="AdminServiceImpl"/>) or async
/// (<see cref="AsyncAdminServiceImpl"/>), driven directly with a <see cref="TestServerCallContext"/>;
/// its mock admin clients come from its own <c>CreateAdmin</c> with an empty config, as the harness
/// selects one.
/// </summary>
internal sealed class AdminServer : IDisposable
{
    private readonly IDisposable _servicer;

    private AdminServer(Proto.AdminService.AdminServiceBase service, IDisposable servicer)
    {
        Service = service;
        _servicer = servicer;
    }

    internal Proto.AdminService.AdminServiceBase Service { get; }

    internal TestServerCallContext Context { get; } = new TestServerCallContext(new RecordingStreamWriter());

    internal static AdminServer Create(string flavour)
    {
        switch (flavour)
        {
            case "sync":
                AdminServiceImpl sync = new AdminServiceImpl();
                return new AdminServer(sync, sync);
            case "async":
                AsyncAdminServiceImpl async = new AsyncAdminServiceImpl();
                return new AdminServer(async, async);
            default:
                throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "sync or async");
        }
    }

    internal async Task<ulong> CreateMockAdmin()
    {
        Proto.CreateAdminResponse response =
            await Service.CreateAdmin(new Proto.CreateAdminRequest(), Context).ConfigureAwait(false);
        Assert.Null(response.Error);
        return response.AdminId;
    }

    internal object Client(ulong adminId)
    {
        FieldInfo admins = _servicer.GetType().GetField("_admins", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return ((IDictionary)admins.GetValue(_servicer)!)[adminId]!;
    }

    public void Dispose()
    {
        _servicer.Dispose();
        Context.Dispose();
    }
}
