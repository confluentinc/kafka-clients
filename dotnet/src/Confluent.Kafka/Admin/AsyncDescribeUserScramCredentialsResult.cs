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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DescribeUserScramCredentials"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult</c>: three accessors
/// derived from one underlying response (<c>:37</c>), each treating a per-user failure
/// differently.
/// </summary>
/// <seealso cref="DescribeUserScramCredentialsResult"/>
public sealed class AsyncDescribeUserScramCredentialsResult
{
    private readonly Task<DescribeUserScramCredentialsViews> _views;

    internal AsyncDescribeUserScramCredentialsResult(Task<DescribeUserScramCredentialsViews> views)
    {
        _views = views;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncDescribeUserScramCredentialsResult From(
        SingleAdminOperation<DescribeUserScramCredentialsViews> operation) =>
        new AsyncDescribeUserScramCredentialsResult(operation.Task);

    /// <summary>
    /// Every described user, keyed by name — Java's <c>all()</c> (<c>:54</c>). Faults with the
    /// <b>first</b> per-user failure if any user could not be described (<c>:68-69</c>).
    /// </summary>
    /// <returns>A future over the whole map.</returns>
    public AsyncKafkaFuture<IReadOnlyDictionary<string, UserScramCredentialsDescription>> All() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<string, UserScramCredentialsDescription>>(AllCore());

    private async Task<IReadOnlyDictionary<string, UserScramCredentialsDescription>> AllCore()
    {
        DescribeUserScramCredentialsViews views = await _views.ConfigureAwait(false);
        return views.AllError is null ? views.All : throw views.AllError;
    }

    /// <summary>
    /// The users the response carried — Java's <c>users()</c> (<c>:92</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ Deliberately does <b>not</b> fault on a per-user failure: Java's javadoc says the list
    /// "will include users that have a credential but that could not be described"
    /// (<c>:89-90</c>). Only a failure of the call itself faults it.
    /// </remarks>
    /// <returns>A future over the user names.</returns>
    public AsyncKafkaFuture<IReadOnlyList<string>> Users() =>
        new AsyncKafkaFuture<IReadOnlyList<string>>(UsersCore());

    private async Task<IReadOnlyList<string>> UsersCore()
    {
        DescribeUserScramCredentialsViews views = await _views.ConfigureAwait(false);
        return views.Users;
    }

    /// <summary>
    /// One user's credentials — Java's <c>description(String)</c> (<c>:114</c>). Faults when
    /// the response carried no such user (<c>:124-125</c>) or when that user failed
    /// (<c>:128-131</c>).
    /// </summary>
    /// <param name="userName">The user to look up.</param>
    /// <returns>A future over that user's description.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="userName"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="userName"/> contains a NUL character or an unpaired surrogate (see the
    /// <see cref="IAsyncAdmin"/> remarks): the lookup would otherwise ask for a different, truncated
    /// user name.
    /// </exception>
    /// <remarks>
    /// Both guards throw <b>synchronously</b> — this method is deliberately not <c>async</c>,
    /// so a precondition failure is not deferred into the returned <see cref="AsyncKafkaFuture"/>
    /// (ffi §A5). A null <paramref name="userName"/> is rejected stricter than Java, whose
    /// <c>description(null)</c> returns a future failed with <c>"No such user: null"</c>
    /// (<c>DescribeUserScramCredentialsResult.java:122-125</c>) — the binding's rule for null
    /// keys (see the <see cref="IAsyncAdmin"/> remarks).
    /// </remarks>
    public AsyncKafkaFuture<UserScramCredentialsDescription> Description(string userName)
    {
        if (userName is null)
        {
            throw new ArgumentNullException(nameof(userName));
        }

        AdminStrings.Validate(userName, nameof(userName));

        return new AsyncKafkaFuture<UserScramCredentialsDescription>(DescriptionCore(userName));
    }

    private async Task<UserScramCredentialsDescription> DescriptionCore(string userName)
    {
        DescribeUserScramCredentialsViews views = await _views.ConfigureAwait(false);

        using (Utf8Marshal.PinnedUtf8String pinnedUser = Utf8Marshal.Pin(userName))
        {
            // ⚠ The error is OWNED, and so is the written description — the opposite of the
            // all() view's borrowed one, hence ReadAndDestroy rather than the plain read.
            IntPtr error = NativeMethods.DescribeUserScramCredentialsResultDescription(
                views.Root, pinnedUser.Pointer, out IntPtr description);

            KafkaException? failure = KafkaException.FromHandle(error);
            if (failure is not null)
            {
                throw failure;
            }

            return UserScramCredentialMarshal.ReadAndDestroy(description);
        }
    }
}
