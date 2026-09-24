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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Grpc.Core;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// Maps the <c>AdminService</c> RPCs onto the binding's <see cref="KafkaAdminClient"/> /
/// <see cref="MockAdminClient"/> — the .NET port of <c>grpc_server.py</c>'s
/// <c>AdminService</c> (M15/P12). Slice G1 (topics &amp; partitions) is implemented here;
/// G2-G6 are added additively by later checkpoints and answer from the generated base
/// until then.
/// </summary>
/// <remarks>
/// <para>
/// <b>No per-op lock</b> — the producer precedent, not the consumer's. <see cref="IAdmin"/>
/// permits concurrent operations (the admin ABI has no single-owner access guard), so the
/// servicer needs only a thread-safe id map plus an <see cref="Interlocked"/> counter to make
/// <c>CreateAdmin</c> / <c>Close</c> races safe.
/// </para>
/// <para>
/// <b>Error placement.</b> A raised error is a whole-call failure and goes in the response's
/// top-level <c>error</c>, leaving <c>entries</c> empty; per-key failures never raise and
/// arrive in the keyed map (<c>admin_service.proto</c>'s envelope commentary). Each handler
/// therefore wraps its whole body in one <c>catch</c>, and
/// <see cref="TranslateAdmin.Resolve{TValue}"/> peels off only the per-key
/// <see cref="KafkaException"/>s.
/// </para>
/// <para>
/// <b>Singleton</b> (<c>Program.cs</c>): it owns the <c>admin_id -&gt; IAdmin</c> map every RPC
/// shares.
/// </para>
/// </remarks>
internal sealed class AdminServiceImpl : Proto.AdminService.AdminServiceBase, IDisposable
{
    private readonly ConcurrentDictionary<ulong, IAdmin> _admins = new ConcurrentDictionary<ulong, IAdmin>();

    private long _nextId;

    /// <summary>
    /// Drains the admin registry at shutdown — the admin twin of
    /// <c>ProducerServiceImpl.Dispose</c>. The map is emptied only by the <c>Close</c> RPC and
    /// this backend process is shared across scenarios, so a scenario that skips <c>Close</c>
    /// would otherwise leave a live native client behind. Each teardown is individually guarded.
    /// </summary>
    public void Dispose()
    {
        foreach (KeyValuePair<ulong, IAdmin> pair in _admins)
        {
            if (!_admins.TryRemove(pair.Key, out IAdmin? admin))
            {
                continue;
            }

            try
            {
                admin.Dispose();
            }
            catch (Exception)
            {
                // One failing client must not abort the sweep.
            }
        }
    }

    /// <inheritdoc/>
    public override Task<Proto.CreateAdminResponse> CreateAdmin(Proto.CreateAdminRequest request, ServerCallContext context)
    {
        Dictionary<string, string> config = new Dictionary<string, string>(request.Config);
        int numBrokers = request.HasNumBrokers ? request.NumBrokers : 1;
        IAdmin admin;
        try
        {
            admin = TranslateAdmin.SelectsMock(config)
                ? new MockAdminClient(numBrokers)
                : (IAdmin)new KafkaAdminClient(config);
        }
        catch (Exception ex)
        {
            return Task.FromResult(
                new Proto.CreateAdminResponse { AdminId = 0, Error = TranslateAdmin.ConstructorError(ex) });
        }

        ulong id = (ulong)Interlocked.Increment(ref _nextId);
        _admins[id] = admin;
        return Task.FromResult(new Proto.CreateAdminResponse { AdminId = id });
    }

    /// <inheritdoc/>
    public override async Task<Proto.StatusResponse> Close(Proto.AdminCloseRequest request, ServerCallContext context)
    {
        if (!_admins.TryRemove(request.AdminId, out IAdmin? admin))
        {
            // Close is idempotent — silent success on an unknown id (Python / Java parity).
            return new Proto.StatusResponse();
        }

        try
        {
            if (request.HasTimeoutMs)
            {
                await admin.Close(TimeSpan.FromMilliseconds(request.TimeoutMs)).ConfigureAwait(false);
            }
            else
            {
                // Java's no-argument close() — wait indefinitely. IAdmin spells that
                // DisposeAsync (IAdmin.Close(TimeSpan) has no "unset" spelling); the one
                // divergence is that DisposeAsync swallows a close failure, so this path
                // reports success where Python would forward the error.
                await admin.DisposeAsync().ConfigureAwait(false);
            }

            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    // -- Topics & partitions (slice G1) --------------------------------------------------

    /// <inheritdoc/>
    public override async Task<Proto.CreateTopicsResponse> CreateTopics(Proto.CreateTopicsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.CreateTopicsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            CreateTopicsResult result = admin.CreateTopics(
                TranslateAdmin.NewTopics(request.Topics),
                new CreateTopicsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    ValidateOnly = request.ValidateOnly,
                    RetryOnQuotaViolation =
                        TranslateAdmin.RetryOnQuota(request.HasRetryOnQuotaViolation, request.RetryOnQuotaViolation),
                });

            Proto.CreateTopicsResponse response = new Proto.CreateTopicsResponse();
            foreach (KeyValuePair<string, Task> pair in result.Values)
            {
                Proto.CreateTopicsEntry entry = new Proto.CreateTopicsEntry { Key = TranslateAdmin.NameKey(pair.Key) };
                Proto.KafkaError? error = await TranslateAdmin.ResolveVoid(pair.Value).ConfigureAwait(false);
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = await MetadataAndConfig(result, pair.Key).ConfigureAwait(false);
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.CreateTopicsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> DeleteTopics(Proto.DeleteTopicsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            bool byIds = request.TopicsCase == Proto.DeleteTopicsRequest.TopicsOneofCase.TopicIds;
            DeleteTopicsResult result = admin.DeleteTopics(
                TranslateAdmin.TopicCollectionOf(byIds, request.Names, request.TopicIds),
                new DeleteTopicsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    RetryOnQuotaViolation =
                        TranslateAdmin.RetryOnQuota(request.HasRetryOnQuotaViolation, request.RetryOnQuotaViolation),
                });

            // Exactly one of the two views is non-null, following the request's oneof.
            return result.TopicIdValues is not null
                ? await TranslateAdmin.VoidResponse(result.TopicIdValues, TranslateAdmin.TopicIdKey).ConfigureAwait(false)
                : await TranslateAdmin.VoidResponse(result.TopicNameValues!, TranslateAdmin.NameKey).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.AdminListTopicsResponse> ListTopics(Proto.AdminListTopicsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.AdminListTopicsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Whole-value response (envelope addendum (a)): one future over the entire map, so a
            // failure has no key to land on and becomes the top-level error.
            ListTopicsResult result = admin.ListTopics(new ListTopicsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                ListInternal = request.ListInternal,
            });

            IReadOnlyDictionary<string, TopicListing> listings =
                await result.NamesToListings().ConfigureAwait(false);
            Proto.AdminListTopicsResponse response = new Proto.AdminListTopicsResponse();
            foreach (TopicListing listing in listings.Values)
            {
                response.Listings.Add(TranslateAdmin.ListingToProto(listing));
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.AdminListTopicsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeTopicsResponse> DescribeTopics(Proto.DescribeTopicsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeTopicsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            bool byIds = request.TopicsCase == Proto.DescribeTopicsRequest.TopicsOneofCase.TopicIds;
            DescribeTopicsOptions options = new DescribeTopicsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                IncludeAuthorizedOperations = request.IncludeAuthorizedOperations,
            };
            if (request.HasPartitionSizeLimitPerResponse)
            {
                options.PartitionSizeLimitPerResponse = request.PartitionSizeLimitPerResponse;
            }

            DescribeTopicsResult result = admin.DescribeTopics(
                TranslateAdmin.TopicCollectionOf(byIds, request.Names, request.TopicIds), options);

            Proto.DescribeTopicsResponse response = new Proto.DescribeTopicsResponse();
            if (result.TopicIdValues is not null)
            {
                foreach (KeyValuePair<Uuid, Task<TopicDescription>> pair in result.TopicIdValues)
                {
                    response.Entries.Add(
                        await DescribeTopicsEntry(TranslateAdmin.TopicIdKey(pair.Key), pair.Value).ConfigureAwait(false));
                }
            }
            else
            {
                foreach (KeyValuePair<string, Task<TopicDescription>> pair in result.TopicNameValues!)
                {
                    response.Entries.Add(
                        await DescribeTopicsEntry(TranslateAdmin.NameKey(pair.Key), pair.Value).ConfigureAwait(false));
                }
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeTopicsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> CreatePartitions(Proto.CreatePartitionsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            CreatePartitionsResult result = admin.CreatePartitions(
                TranslateAdmin.NewPartitionsMap(request.Partitions),
                new CreatePartitionsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    ValidateOnly = request.ValidateOnly,
                    RetryOnQuotaViolation =
                        TranslateAdmin.RetryOnQuota(request.HasRetryOnQuotaViolation, request.RetryOnQuotaViolation),
                });

            return await TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.NameKey).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DeleteRecordsResponse> DeleteRecords(Proto.DeleteRecordsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DeleteRecordsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DeleteRecordsResult result = admin.DeleteRecords(
                TranslateAdmin.RecordsToDeleteMap(request.Records),
                new DeleteRecordsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DeleteRecordsResponse response = new Proto.DeleteRecordsResponse();
            foreach (KeyValuePair<TopicPartition, Task<DeletedRecords>> pair in result.LowWatermarks)
            {
                (DeletedRecords deleted, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(pair.Value).ConfigureAwait(false);
                Proto.DeleteRecordsEntry entry = new Proto.DeleteRecordsEntry
                {
                    Key = TranslateAdmin.PartitionKey(pair.Key),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = new Proto.DeletedRecords { LowWatermark = deleted.LowWatermark };
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DeleteRecordsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <summary>
    /// One <c>describeTopics</c> entry: the key plus that key's own description or error.
    /// </summary>
    private static async Task<Proto.DescribeTopicsEntry> DescribeTopicsEntry(
        Proto.ResultKey key, Task<TopicDescription> future)
    {
        (TopicDescription description, Proto.KafkaError? error) =
            await TranslateAdmin.Resolve(future).ConfigureAwait(false);
        Proto.DescribeTopicsEntry entry = new Proto.DescribeTopicsEntry { Key = key };
        if (error is not null)
        {
            entry.Error = error;
        }
        else
        {
            entry.Value = TranslateAdmin.DescriptionToProto(description);
        }

        return entry;
    }

    /// <summary>
    /// One created topic's <c>TopicMetadataAndConfig</c> — envelope exception 3: the per-key
    /// future resolved, but the four accessors rethrow when the broker created the topic without
    /// returning its metadata, and that third state must cross as the value's own error arm.
    /// </summary>
    private static async Task<Proto.TopicMetadataAndConfig> MetadataAndConfig(CreateTopicsResult result, string topic)
    {
        try
        {
            Proto.TopicMetadata metadata = new Proto.TopicMetadata
            {
                TopicId = (await result.TopicId(topic).ConfigureAwait(false)).ToString(),
                NumPartitions = await result.NumPartitions(topic).ConfigureAwait(false),
                ReplicationFactor = await result.ReplicationFactor(topic).ConfigureAwait(false),
            };
            Config config = await result.Config(topic).ConfigureAwait(false);
            foreach (ConfigEntry entry in config.Entries)
            {
                metadata.Configs.Add(TranslateAdmin.ConfigEntryToProto(entry));
            }

            return new Proto.TopicMetadataAndConfig { Metadata = metadata };
        }
        catch (KafkaException ex)
        {
            return new Proto.TopicMetadataAndConfig { Error = Translate.ToProto(ex) };
        }
    }

    private IAdmin? Get(ulong adminId) =>
        _admins.TryGetValue(adminId, out IAdmin? admin) ? admin : null;
}
