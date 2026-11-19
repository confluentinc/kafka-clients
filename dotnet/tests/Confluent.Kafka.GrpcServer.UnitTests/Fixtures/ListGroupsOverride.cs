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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.ExceptionServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// A mock admin client whose <c>ListGroups</c> answers the given listings and errors, as a client does when some
/// brokers answer with an error; every other call goes to the mock. The result keeps the mock's own native handle.
/// </summary>
internal class ListGroupsOverride : DispatchProxy
{
    private object _inner = null!;
    private IReadOnlyCollection<GroupListing> _valid = Array.Empty<GroupListing>();
    private IReadOnlyCollection<Exception> _errors = Array.Empty<Exception>();

    internal static object Wrap(object inner, IReadOnlyCollection<GroupListing> valid, IReadOnlyCollection<Exception> errors)
    {
        object proxy = inner switch
        {
            IAsyncAdmin => DispatchProxy.Create<IAsyncAdmin, ListGroupsOverride>(),
            IAdmin => DispatchProxy.Create<IAdmin, ListGroupsOverride>(),
            _ => throw new ArgumentException($"{inner.GetType()} is not an admin client", nameof(inner)),
        };
        ListGroupsOverride state = (ListGroupsOverride)proxy;
        state._inner = inner;
        state._valid = valid;
        state._errors = errors;
        return proxy;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        object? answer;
        try
        {
            answer = targetMethod!.Invoke(_inner, args);
        }
        catch (TargetInvocationException e) when (e.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(e.InnerException).Throw();
            throw;
        }

        return answer switch
        {
            ListGroupsResult real => ListGroupsResult.OfClient((Failed(), Valid(), Errors(), real.Native)),
            AsyncListGroupsResult real => AsyncListGroupsResult.OfClient((Failed(), Valid(), Errors(), real.Native)),
            _ => answer,
        };
    }

    // All() fails with the first error, as Java's does when any broker answered with one.
    private KafkaFutureImpl<IReadOnlyCollection<GroupListing>> Failed()
    {
        var all = new KafkaFutureImpl<IReadOnlyCollection<GroupListing>>();
        all.CompleteWithError(_errors.First());
        return all;
    }

    private KafkaFutureImpl<IReadOnlyCollection<GroupListing>> Valid() => KafkaFutureImpl.CompletedFuture(_valid);

    private KafkaFutureImpl<IReadOnlyCollection<Exception>> Errors() => KafkaFutureImpl.CompletedFuture(_errors);
}
