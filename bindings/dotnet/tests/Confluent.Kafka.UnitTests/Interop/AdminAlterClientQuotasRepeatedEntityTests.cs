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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P13.3 F7 — <c>alterClientQuotas</c> with the same entity in more than one alteration,
/// through production's own submit and the real ABI.
/// </summary>
/// <remarks>
/// <para>
/// Java sends every alteration, a repeated entity's included, and its per-entity future map
/// collapses the repeat to one future (<c>KafkaAdminClient.java:4314-4318</c>). Since PR #201
/// round 70 the core does the same: a repeated entity across alterations "is sent twice, as
/// in Java, and is one key", answered by one callback (header,
/// <c>kafka_admin_AdminClient_alter_client_quotas_async</c>). The binding used to reject the
/// request instead.
/// </para>
/// <para>
/// Each test asserts all three halves of that contract. The recorded rows prove <b>every</b>
/// alteration reached the core, in order (both values are decoded from the pinned rows during
/// the submit). The single awaitable proves the key set is distinct. And the released client
/// proves the countdown was armed with the number of <b>distinct entities</b>, not rows: armed
/// with the row count it never reaches zero, and although the one awaitable still resolves,
/// the <c>GCHandle</c> and the span-the-op client reference are held for the process
/// lifetime.
/// </para>
/// <para>
/// The mock answers every entity with Java's own
/// <c>UnsupportedOperationException("Not implement yet")</c> — Java's typo, mirrored by the
/// core's mock (<c>MockAdminClient.java:1248-1250</c>).
/// </para>
/// </remarks>
public sealed class AdminAlterClientQuotasRepeatedEntityTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_releaseBound = TimeSpan.FromSeconds(5);

    /// <summary>The code the mock reports for an RPC Java's mock does not implement.</summary>
    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implement yet";

    /// <summary>
    /// The same entity altered twice: both alterations are sent, in order, and the entity has
    /// one awaitable carrying the core's answer.
    /// </summary>
    [Fact]
    public async Task ARepeatedEntity_IsSentForEveryAlteration_AnsweredOnce_AndReleases()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        List<Row> sent = new List<Row>();
        AlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration("f7-dup", 1d), Alteration("f7-dup", 2d) },
            options: null,
            (nativeHandle, entityTypes, entityNames, entityCounts, opKeys, opValues, opHasValues, opCounts, count,
                timeoutMs, validateOnly, callback, userData) =>
            {
                sent.AddRange(Decode(entityTypes, entityNames, entityCounts, opValues, opCounts, count));
                NativeMethods.AdminClientAlterClientQuotasAsync(
                    nativeHandle, entityTypes, entityNames, entityCounts, opKeys, opValues, opHasValues, opCounts,
                    count, timeoutMs, validateOnly, callback, userData);
            });

        Assert.Equal(2, sent.Count);
        Assert.All(sent, row => Assert.Equal(new[] { "user=f7-dup" }, row.Entries));
        Assert.Equal(new[] { 1d }, sent[0].Values);
        Assert.Equal(new[] { 2d }, sent[1].Values);

        KeyValuePair<ClientQuotaEntity, Task> only = Assert.Single(result.Values);
        Assert.Equal(Entity("f7-dup"), only.Key);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[Entity("f7-dup")], s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must count the distinct entities the core answers, not the alterations sent");
    }

    /// <summary>
    /// Two empty entities are one entity altered twice (M15/P13.2 G4-4 made the empty entity
    /// sendable): both rows go out with a count of 0, and the one awaitable completes.
    /// </summary>
    [Fact]
    public async Task TwoEmptyEntities_AreSentForEveryAlteration_AnsweredOnce_AndRelease()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        ClientQuotaAlteration.Op[] noOps = Array.Empty<ClientQuotaAlteration.Op>();
        List<Row> sent = new List<Row>();
        AlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { new ClientQuotaAlteration(EmptyEntity(), noOps), new ClientQuotaAlteration(EmptyEntity(), noOps) },
            options: null,
            (nativeHandle, entityTypes, entityNames, entityCounts, opKeys, opValues, opHasValues, opCounts, count,
                timeoutMs, validateOnly, callback, userData) =>
            {
                sent.AddRange(Decode(entityTypes, entityNames, entityCounts, opValues, opCounts, count));
                NativeMethods.AdminClientAlterClientQuotasAsync(
                    nativeHandle, entityTypes, entityNames, entityCounts, opKeys, opValues, opHasValues, opCounts,
                    count, timeoutMs, validateOnly, callback, userData);
            });

        Assert.Equal(2, sent.Count);
        Assert.All(sent, row => Assert.Empty(row.Entries));

        Assert.Equal(EmptyEntity(), Assert.Single(result.Values).Key);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[EmptyEntity()], s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must count the distinct entities the core answers, not the alterations sent");
    }

    private static ClientQuotaEntity Entity(string user) =>
        new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [ClientQuotaEntity.User] = user,
            });

    private static ClientQuotaEntity EmptyEntity() =>
        new ClientQuotaEntity(new Dictionary<string, string?>(StringComparer.Ordinal));

    private static ClientQuotaAlteration Alteration(string user, double value) =>
        new ClientQuotaAlteration(
            Entity(user), new[] { new ClientQuotaAlteration.Op("producer_byte_rate", value) });

    /// <summary>
    /// Copies each row the submit handed native out of its pinned inner arrays — valid only
    /// during the submit, which is where the seam calls this.
    /// </summary>
    private static List<Row> Decode(
        IntPtr[] entityTypes, IntPtr[] entityNames, int[] entityCounts, IntPtr[] opValues, int[] opCounts, int count)
    {
        List<Row> rows = new List<Row>(count);
        for (int i = 0; i < count; i++)
        {
            string[] entries = new string[entityCounts[i]];
            for (int entry = 0; entry < entries.Length; entry++)
            {
                IntPtr type = Marshal.ReadIntPtr(entityTypes[i], entry * IntPtr.Size);
                IntPtr name = Marshal.ReadIntPtr(entityNames[i], entry * IntPtr.Size);
                entries[entry] = Utf8Marshal.PtrToString(type) + "=" + (Utf8Marshal.PtrToString(name) ?? "<default>");
            }

            double[] values = new double[opCounts[i]];
            if (values.Length > 0)
            {
                Marshal.Copy(opValues[i], values, 0, values.Length);
            }

            rows.Add(new Row(entries, values));
        }

        return rows;
    }

    /// <summary>
    /// Disposes the client, then waits — bounded — for the native release: a per-key
    /// trampoline resolves its key before it releases the operation, so an awaiter can reach
    /// <c>Dispose</c> a moment early. A leak never releases, so the bound only decides how
    /// long a red takes to report.
    /// </summary>
    private static bool DisposeAndAwaitRelease(NativeAdminClient admin, SafeAdminHandle handle)
    {
        TestTimeout.Run(admin.Dispose, s_deadline);
        return SpinWait.SpinUntil(() => handle.IsClosed, s_releaseBound);
    }

    /// <summary>One alteration as the ABI received it: its <c>type=name</c> entries and its op values.</summary>
    private sealed class Row
    {
        public Row(string[] entries, double[] values)
        {
            Entries = entries;
            Values = values;
        }

        public string[] Entries { get; }

        public double[] Values { get; }
    }
}
