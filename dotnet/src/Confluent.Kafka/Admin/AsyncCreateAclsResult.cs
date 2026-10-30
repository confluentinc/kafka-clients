// Copyright 2025 Confluent Inc.
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

using System.Collections.Generic;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.CreateAcls"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreateAclsResult</c>: one future per ACL binding.
/// </summary>
/// <remarks>
/// A binding that fails faults only its own future. The dictionary is keyed by
/// <see cref="AclBinding"/> value equality, so any value-equal binding finds its entry.
/// </remarks>
/// <seealso cref="CreateAclsResult"/>
public sealed class AsyncCreateAclsResult
{
    private readonly IReadOnlyDictionary<AclBinding, AsyncKafkaFuture> _values;
    private readonly IReadOnlyList<KafkaFutureImpl> _nativeFutures;
    private readonly SafeAdminResultHandle _native;

    private AsyncCreateAclsResult(
        IReadOnlyDictionary<AclBinding, AsyncKafkaFuture> values,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested binding, keyed by the binding — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<AclBinding, AsyncKafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every binding has been created, and faults with the first failure to
    /// complete if any binding failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures));

    internal static AsyncCreateAclsResult OfClient(
        (IReadOnlyDictionary<AclBinding, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<AclBinding, AsyncKafkaFuture> values =
            new Dictionary<AclBinding, AsyncKafkaFuture>(native.Values.Count);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.Values.Count);
        foreach (KeyValuePair<AclBinding, KafkaFutureImpl<object?>> entry in native.Values)
        {
            values.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new AsyncCreateAclsResult(values, nativeFutures, native.Result);
    }
}
