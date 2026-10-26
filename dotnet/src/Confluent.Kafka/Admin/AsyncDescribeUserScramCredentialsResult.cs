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

using System;
using System.Collections.Generic;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DescribeUserScramCredentials"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult</c>: three accessors over one response,
/// each treating a per-user failure differently.
/// </summary>
/// <remarks>
/// Each call of an accessor returns a new future, as Java's do.
/// </remarks>
/// <seealso cref="DescribeUserScramCredentialsResult"/>
public sealed class AsyncDescribeUserScramCredentialsResult
{
    private readonly SafeAdminResultHandle _native;

    private AsyncDescribeUserScramCredentialsResult(SafeAdminResultHandle native)
    {
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// Every described user, keyed by name — Java's <c>all()</c>. Faults with the <b>first</b> per-user failure
    /// if any user could not be described; a user that was not found is in the map with no credentials.
    /// </summary>
    /// <returns>A future over the whole map.</returns>
    public AsyncKafkaFuture<IReadOnlyDictionary<string, UserScramCredentialsDescription>> All() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<string, UserScramCredentialsDescription>>(
            NativeAdminClient.DescribeUserScramCredentialsAll(_native));

    /// <summary>
    /// The users the response carried — Java's <c>users()</c>.
    /// </summary>
    /// <remarks>
    /// A per-user failure does not fault it: as Java's javadoc says, the list includes users that
    /// have a credential but could not be described. A user that was not found is left out.
    /// </remarks>
    /// <returns>A future over the user names.</returns>
    public AsyncKafkaFuture<IReadOnlyList<string>> Users() =>
        new AsyncKafkaFuture<IReadOnlyList<string>>(NativeAdminClient.DescribeUserScramCredentialsUsers(_native));

    /// <summary>
    /// One user's credentials — Java's <c>description(String)</c>. Faults with <c>RESOURCE_NOT_FOUND</c> when
    /// the user was not part of the request or has no credentials, or with the user's own failure.
    /// </summary>
    /// <param name="userName">The user to look up.</param>
    /// <returns>A future over that user's description.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="userName"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="userName"/> contains a NUL character or an unpaired surrogate.
    /// </exception>
    /// <remarks>
    /// Both checks throw at the call. A null <paramref name="userName"/> is a deviation: Java's
    /// <c>description(null)</c> returns a future failed with <c>"No such user: null"</c>.
    /// </remarks>
    public AsyncKafkaFuture<UserScramCredentialsDescription> Description(string userName)
    {
        if (userName is null)
        {
            throw new ArgumentNullException(nameof(userName));
        }

        AdminStrings.Validate(userName, nameof(userName));

        return new AsyncKafkaFuture<UserScramCredentialsDescription>(
            NativeAdminClient.DescribeUserScramCredentialsDescription(_native, userName));
    }

    internal static AsyncDescribeUserScramCredentialsResult OfClient(SafeAdminResultHandle native) =>
        new AsyncDescribeUserScramCredentialsResult(native);
}
