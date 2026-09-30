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
/// <c>AdminService</c> (M15/P12). Slices G1 (topics &amp; partitions), G2 (cluster, configs,
/// log dirs), G3 (elections, reassignments, offsets), G6 (producers &amp; transactions), G4
/// (groups) and G5 (acls, quotas, scram, tokens, features) are all implemented here.
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

    // -- Cluster, configs & log dirs (slice G2) ------------------------------------------

    /// <inheritdoc/>
    public override async Task<Proto.DescribeClusterResponse> DescribeCluster(Proto.DescribeClusterRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeClusterResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DescribeClusterResult result = admin.DescribeCluster(new DescribeClusterOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                IncludeAuthorizedOperations = request.IncludeAuthorizedOperations,
                IncludeFencedBrokers = request.IncludeFencedBrokers,
            });

            // Whole-value response: Java's four futures are over attributes of one cluster, so
            // there is nothing to key. All four are awaited before any error is reported, so
            // none is abandoned; when more than one failed the first in Java's declaration
            // order wins.
            (IReadOnlyCollection<Node> nodes, Proto.KafkaError? nodesError) =
                await TranslateAdmin.Resolve(result.Nodes()).ConfigureAwait(false);
            (Node? controller, Proto.KafkaError? controllerError) =
                await TranslateAdmin.Resolve(result.Controller()).ConfigureAwait(false);
            (string? clusterId, Proto.KafkaError? clusterIdError) =
                await TranslateAdmin.Resolve(result.ClusterId()).ConfigureAwait(false);
            (IReadOnlyCollection<AclOperation>? operations, Proto.KafkaError? operationsError) =
                await TranslateAdmin.Resolve(result.AuthorizedOperations()).ConfigureAwait(false);

            Proto.KafkaError? error = nodesError ?? controllerError ?? clusterIdError ?? operationsError;
            if (error is not null)
            {
                return new Proto.DescribeClusterResponse { Error = error };
            }

            // Java's clusterId() is null on the old-broker Metadata fallback, and a proto3
            // string cannot be null, so it is sent as "" — as the C server sends it (its `cstr`
            // maps NULL to "") (M15/P13.3 D15).
            Proto.ClusterDescription description = new Proto.ClusterDescription { ClusterId = clusterId ?? "" };
            foreach (Node node in nodes)
            {
                description.Nodes.Add(Translate.NodeToProto(node));
            }

            // Both are nullable in Java: no current controller, and "the broker did not report
            // the operations" — which is not the same as reporting that none are authorized.
            if (controller is not null)
            {
                description.Controller = Translate.NodeToProto(controller);
            }

            if (operations is not null)
            {
                Proto.AclOperationList list = new Proto.AclOperationList();
                foreach (AclOperation operation in operations)
                {
                    list.Operations.Add((int)operation);
                }

                description.AuthorizedOperations = list;
            }

            return new Proto.DescribeClusterResponse { Description = description };
        }
        catch (Exception ex)
        {
            return new Proto.DescribeClusterResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeConfigsResponse> DescribeConfigs(Proto.DescribeConfigsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeConfigsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DescribeConfigsResult result = admin.DescribeConfigs(
                TranslateAdmin.ConfigResources(request.Resources),
                new DescribeConfigsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    IncludeSynonyms = request.IncludeSynonyms,
                    IncludeDocumentation = request.IncludeDocumentation,
                });

            Proto.DescribeConfigsResponse response = new Proto.DescribeConfigsResponse();
            foreach (KeyValuePair<ConfigResource, Task<Config>> pair in result.Values)
            {
                (Config config, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(pair.Value).ConfigureAwait(false);
                Proto.DescribeConfigsEntry entry = new Proto.DescribeConfigsEntry
                {
                    Key = TranslateAdmin.ConfigResourceKey(pair.Key),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = TranslateAdmin.ConfigToProto(config);
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeConfigsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> IncrementalAlterConfigs(Proto.IncrementalAlterConfigsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AlterConfigsResult result = admin.IncrementalAlterConfigs(
                TranslateAdmin.AlterConfigsMap(request.Configs),
                new AlterConfigsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    ValidateOnly = request.ValidateOnly,
                });

            return await TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.ConfigResourceKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.ListConfigResourcesResponse> ListConfigResources(Proto.ListConfigResourcesRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListConfigResourcesResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // An empty repeated field is Java's empty Set: every supported type. It is passed
            // through — the binding treats empty and absent alike.
            ListConfigResourcesResult result = admin.ListConfigResources(
                TranslateAdmin.ConfigResourceTypes(request.ResourceTypes),
                new ListConfigResourcesOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            IReadOnlyCollection<ConfigResource> resources = await result.All().ConfigureAwait(false);
            Proto.ListConfigResourcesResponse response = new Proto.ListConfigResourcesResponse();
            foreach (ConfigResource resource in resources)
            {
                response.Resources.Add(TranslateAdmin.ConfigResourceToProto(resource));
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.ListConfigResourcesResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.ListClientMetricsResourcesResponse> ListClientMetricsResources(Proto.ListClientMetricsResourcesRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListClientMetricsResourcesResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        // Deprecated in Java since 4.1, but every binding still exposes it, so the harness
        // exercises it — hence the local suppression rather than dropping the RPC. The
        // listing type carries the attribute too, so the scope spans the whole body.
#pragma warning disable CS0618 // Type or member is obsolete
        try
        {
            ListClientMetricsResourcesResult result = admin.ListClientMetricsResources(
                new ListClientMetricsResourcesOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            IReadOnlyCollection<ClientMetricsResourceListing> resources =
                await result.All().ConfigureAwait(false);
            Proto.ListClientMetricsResourcesResponse response = new Proto.ListClientMetricsResourcesResponse();
            foreach (ClientMetricsResourceListing listing in resources)
            {
                response.Resources.Add(new Proto.ClientMetricsResourceListing { Name = listing.Name });
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.ListClientMetricsResourcesResponse { Error = Translate.ToProto(ex) };
        }
#pragma warning restore CS0618
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeLogDirsResponse> DescribeLogDirs(Proto.DescribeLogDirsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeLogDirsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DescribeLogDirsResult result = admin.DescribeLogDirs(
                new List<int>(request.Brokers),
                new DescribeLogDirsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DescribeLogDirsResponse response = new Proto.DescribeLogDirsResponse();
            foreach (KeyValuePair<int, Task<IReadOnlyDictionary<string, LogDirDescription>>> pair in
                result.Descriptions)
            {
                (IReadOnlyDictionary<string, LogDirDescription> descriptions, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(pair.Value).ConfigureAwait(false);
                Proto.DescribeLogDirsEntry entry = new Proto.DescribeLogDirsEntry
                {
                    Key = TranslateAdmin.BrokerIdKey(pair.Key),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    // The value is nested: one description per log-dir path, each with its own
                    // error. Flattening broker and path into one key would make "the broker
                    // answered with zero log dirs" unrepresentable.
                    Proto.LogDirDescriptionMap value = new Proto.LogDirDescriptionMap();
                    foreach (KeyValuePair<string, LogDirDescription> dir in descriptions)
                    {
                        value.LogDirs[dir.Key] = TranslateAdmin.LogDirDescriptionToProto(dir.Value);
                    }

                    entry.Value = value;
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeLogDirsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> AlterReplicaLogDirs(Proto.AlterReplicaLogDirsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
                TranslateAdmin.ReplicaLogDirAssignments(request.Assignments),
                new AlterReplicaLogDirsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            return await TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.ReplicaKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeReplicaLogDirsResponse> DescribeReplicaLogDirs(Proto.DescribeReplicaLogDirsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeReplicaLogDirsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DescribeReplicaLogDirsResult result = admin.DescribeReplicaLogDirs(
                TranslateAdmin.Replicas(request.Replicas),
                new DescribeReplicaLogDirsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DescribeReplicaLogDirsResponse response = new Proto.DescribeReplicaLogDirsResponse();
            foreach (KeyValuePair<TopicPartitionReplica, Task<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> pair
                in result.Values)
            {
                (DescribeReplicaLogDirsResult.ReplicaLogDirInfo info, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(pair.Value).ConfigureAwait(false);
                Proto.DescribeReplicaLogDirsEntry entry = new Proto.DescribeReplicaLogDirsEntry
                {
                    Key = TranslateAdmin.ReplicaKey(pair.Key),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    Proto.ReplicaLogDirInfo value = new Proto.ReplicaLogDirInfo
                    {
                        CurrentReplicaOffsetLag = info.GetCurrentReplicaOffsetLag(),
                        FutureReplicaOffsetLag = info.GetFutureReplicaOffsetLag(),
                    };

                    // Both dirs are nullable: no replica hosted here, and no pending move.
                    if (info.GetCurrentReplicaLogDir() is string current)
                    {
                        value.CurrentReplicaLogDir = current;
                    }

                    if (info.GetFutureReplicaLogDir() is string future)
                    {
                        value.FutureReplicaLogDir = future;
                    }

                    entry.Value = value;
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeReplicaLogDirsResponse { Error = Translate.ToProto(ex) };
        }
    }

    // -- Elections, reassignments & offsets (slice G3) ------------------------------------

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> ElectLeaders(Proto.ElectLeadersRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            ElectLeadersResult result = admin.ElectLeaders(
                (ElectionType)request.ElectionType,
                TranslateAdmin.OptionalPartitions(request.Partitions),
                new ElectLeadersOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // Both error levels differ from the other void RPCs. Java holds a SINGLE future
            // over the whole map, so its failure is a whole-call failure; the per-entry error
            // is the Optional<Throwable> inside the resolved map, absent meaning that
            // partition's election succeeded.
            (IReadOnlyDictionary<TopicPartition, KafkaException?> partitions, Proto.KafkaError? error) =
                await TranslateAdmin.Resolve(result.Partitions()).ConfigureAwait(false);
            if (error is not null)
            {
                return new Proto.VoidKeyedResponse { Error = error };
            }

            Proto.VoidKeyedResponse response = new Proto.VoidKeyedResponse();
            foreach (KeyValuePair<TopicPartition, KafkaException?> pair in partitions)
            {
                Proto.VoidResultEntry entry = new Proto.VoidResultEntry
                {
                    Key = TranslateAdmin.PartitionKey(pair.Key),
                };
                if (pair.Value is not null)
                {
                    entry.Error = Translate.ToProto(pair.Value);
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> AlterPartitionReassignments(Proto.AlterPartitionReassignmentsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
                TranslateAdmin.Reassignments(request.Reassignments),
                new AlterPartitionReassignmentsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    // Java's default is true, hence the presence flag rather than a bare bool.
                    AllowReplicationFactorChange = !request.HasAllowReplicationFactorChange
                        || request.AllowReplicationFactorChange,
                });

            // Genuinely per-key futures here, so the top-level error keeps its ordinary
            // narrow meaning (unlike ElectLeaders above).
            return await TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.PartitionKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.ListPartitionReassignmentsResponse> ListPartitionReassignments(Proto.ListPartitionReassignmentsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListPartitionReassignmentsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Whole-value response (envelope addendum (a)): one future over the entire map, so
            // no individual reassignment carries an error.
            ListPartitionReassignmentsResult result = admin.ListPartitionReassignments(
                TranslateAdmin.OptionalPartitions(request.Partitions),
                new ListPartitionReassignmentsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments =
                await result.Reassignments().ConfigureAwait(false);
            Proto.ListPartitionReassignmentsResponse response = new Proto.ListPartitionReassignmentsResponse();
            foreach (KeyValuePair<TopicPartition, PartitionReassignment> pair in reassignments)
            {
                response.Reassignments.Add(new Proto.OngoingPartitionReassignment
                {
                    Partition = Translate.TpToProto(pair.Key),
                    Reassignment = TranslateAdmin.ReassignmentToProto(pair.Value),
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.ListPartitionReassignmentsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.ListOffsetsResponse> ListOffsets(Proto.ListOffsetsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListOffsetsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Inside the try: proto3 C# reads an unset submessage as null (Python reads a
            // default instance), so a malformed request must fault the top-level error rather
            // than escape as a gRPC status.
            Dictionary<TopicPartition, OffsetSpec>? specs =
                TranslateAdmin.OffsetSpecs(request.Specs, out string? invalid);
            if (specs is null)
            {
                return new Proto.ListOffsetsResponse { Error = TranslateAdmin.RequestError(invalid!) };
            }

            ListOffsetsResult result = admin.ListOffsets(
                specs,
                new ListOffsetsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    IsolationLevel = (IsolationLevel)request.IsolationLevel,
                });

            // The result exposes no keyed future map, so the requested keys drive the walk —
            // Java's own PartitionResult(tp) pattern. The Dictionary already de-duplicates.
            Proto.ListOffsetsResponse response = new Proto.ListOffsetsResponse();
            foreach (TopicPartition partition in specs.Keys)
            {
                (ListOffsetsResult.ListOffsetsResultInfo info, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(result.PartitionResult(partition)).ConfigureAwait(false);
                Proto.ListOffsetsEntry entry = new Proto.ListOffsetsEntry
                {
                    Key = TranslateAdmin.PartitionKey(partition),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = TranslateAdmin.OffsetInfoToProto(info);
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.ListOffsetsResponse { Error = Translate.ToProto(ex) };
        }
    }

    // -- Producers & transactions (slice G6) ---------------------------------------------

    /// <inheritdoc/>
    public override async Task<Proto.DescribeProducersResponse> DescribeProducers(Proto.DescribeProducersRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeProducersResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            List<TopicPartition> partitions = TranslateAdmin.DistinctKeys(PartitionsOf(request.Partitions));
            DescribeProducersOptions options = new DescribeProducersOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
            };

            // An absent broker_id is Java's empty OptionalInt (query each partition's leader);
            // broker 0 is legal, so the presence flag carries the distinction.
            if (request.HasBrokerId)
            {
                options.BrokerId = request.BrokerId;
            }

            DescribeProducersResult result = admin.DescribeProducers(partitions, options);

            Proto.DescribeProducersResponse response = new Proto.DescribeProducersResponse();
            foreach (TopicPartition partition in partitions)
            {
                (DescribeProducersResult.PartitionProducerState state, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(result.PartitionResult(partition)).ConfigureAwait(false);
                Proto.DescribeProducersEntry entry = new Proto.DescribeProducersEntry
                {
                    Key = TranslateAdmin.PartitionKey(partition),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = TranslateAdmin.PartitionProducerStateToProto(state);
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeProducersResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeTransactionsResponse> DescribeTransactions(Proto.DescribeTransactionsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeTransactionsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            List<string> ids = TranslateAdmin.DistinctKeys(request.TransactionalIds);
            DescribeTransactionsResult result = admin.DescribeTransactions(
                ids,
                new DescribeTransactionsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DescribeTransactionsResponse response = new Proto.DescribeTransactionsResponse();
            foreach (string id in ids)
            {
                (TransactionDescription description, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(result.Description(id)).ConfigureAwait(false);
                Proto.DescribeTransactionsEntry entry = new Proto.DescribeTransactionsEntry
                {
                    Key = TranslateAdmin.NameKey(id),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = TranslateAdmin.TransactionDescriptionToProto(description);
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeTransactionsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.StatusResponse> AbortTransaction(Proto.AbortTransactionRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.StatusResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Inside the try, for the same reason as ListOffsets above.
            AbortTransactionSpec? spec = TranslateAdmin.AbortSpec(request, out string? invalid);
            if (spec is null)
            {
                return new Proto.StatusResponse { Error = TranslateAdmin.RequestError(invalid!) };
            }

            // Java's AbortTransactionResult carries no data and no per-key granularity, so
            // success is simply an absent error.
            await admin.AbortTransaction(
                spec,
                new AbortTransactionOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                }).All().ConfigureAwait(false);

            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.StatusResponse> ForceTerminateTransaction(Proto.ForceTerminateTransactionRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.StatusResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            await admin.ForceTerminateTransaction(
                request.TransactionalId,
                new TerminateTransactionOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                }).Result().ConfigureAwait(false);

            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.ListTransactionsResponse> ListTransactions(Proto.ListTransactionsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListTransactionsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            ListTransactionsResult result = admin.ListTransactions(new ListTransactionsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                // Java's default for both collections is an empty set meaning "no filter", so
                // empty needs no null form.
                FilteredStates = TranslateAdmin.TransactionStates(request.States),
                FilteredProducerIds = new List<long>(request.ProducerIds),
                // Java's own -1 sentinel: negative means no duration filter.
                FilteredDuration = request.DurationMs,
                FilteredTransactionalIdPattern =
                    request.HasTransactionalIdPattern ? request.TransactionalIdPattern : null,
            });

            // Keyed by broker: byBrokerId() is the only one of Java's three views that keeps a
            // per-broker error, so a partial listing survives. Only the broker-DISCOVERY
            // future's failure is a whole-call failure.
            (IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>> brokers,
                Proto.KafkaError? error) =
                await TranslateAdmin.Resolve(result.ByBrokerId()).ConfigureAwait(false);
            if (error is not null)
            {
                return new Proto.ListTransactionsResponse { Error = error };
            }

            Proto.ListTransactionsResponse response = new Proto.ListTransactionsResponse();
            foreach (KeyValuePair<int, Task<IReadOnlyCollection<TransactionListing>>> broker in brokers)
            {
                (IReadOnlyCollection<TransactionListing> listings, Proto.KafkaError? brokerError) =
                    await TranslateAdmin.Resolve(broker.Value).ConfigureAwait(false);
                Proto.ListTransactionsEntry entry = new Proto.ListTransactionsEntry
                {
                    Key = TranslateAdmin.BrokerIdKey(broker.Key),
                };
                if (brokerError is not null)
                {
                    entry.Error = brokerError;
                }
                else
                {
                    Proto.TransactionListingList value = new Proto.TransactionListingList();
                    foreach (TransactionListing listing in listings)
                    {
                        value.Listings.Add(TranslateAdmin.TransactionListingToProto(listing));
                    }

                    entry.Value = value;
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.ListTransactionsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.FenceProducersResponse> FenceProducers(Proto.FenceProducersRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.FenceProducersResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            FenceProducersResult result = admin.FenceProducers(
                TranslateAdmin.DistinctKeys(request.TransactionalIds),
                new FenceProducersOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.FenceProducersResponse response = new Proto.FenceProducersResponse();
            foreach (string id in result.FencedProducers.Keys)
            {
                // Both accessors project the same underlying per-id future, so they resolve
                // identically; (-1, -1) is Java's ProducerIdAndEpoch.NONE, a legal value.
                (long producerId, Proto.KafkaError? idError) =
                    await TranslateAdmin.Resolve(result.ProducerId(id)).ConfigureAwait(false);
                (short epoch, Proto.KafkaError? epochError) =
                    await TranslateAdmin.Resolve(result.EpochId(id)).ConfigureAwait(false);
                Proto.FenceProducersEntry entry = new Proto.FenceProducersEntry
                {
                    Key = TranslateAdmin.NameKey(id),
                };
                Proto.KafkaError? error = idError ?? epochError;
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = new Proto.ProducerIdAndEpoch { ProducerId = producerId, Epoch = epoch };
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.FenceProducersResponse { Error = Translate.ToProto(ex) };
        }
    }

    // -- Groups (slice G4) ---------------------------------------------------------------

    /// <inheritdoc/>
    public override async Task<Proto.ListGroupsResponse> ListGroups(Proto.ListGroupsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Empty filter lists are Java's empty sets, i.e. the filter left unset.
            ListGroupsResult result = admin.ListGroups(new ListGroupsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                GroupStates = TranslateAdmin.GroupStates(request.GroupStates),
                ProtocolTypes = new List<string>(request.ProtocolTypes),
                Types = TranslateAdmin.GroupTypes(request.Types_),
            });

            // Whole-value response whose value is Java's valid()/errors() split: the two lists
            // are independent and of unrelated length, so nothing may be zipped across them.
            // A failure of the single underlying future faults both awaits and becomes the
            // top-level error via the catch below.
            IReadOnlyCollection<GroupListing> valid = await result.Valid().ConfigureAwait(false);
            IReadOnlyCollection<KafkaException> errors = await result.Errors().ConfigureAwait(false);

            Proto.ListGroupsResponse response = new Proto.ListGroupsResponse();
            foreach (GroupListing listing in valid)
            {
                response.Valid.Add(TranslateAdmin.GroupListingToProto(listing));
            }

            foreach (KafkaException error in errors)
            {
                response.ListingErrors.Add(Translate.ToProto(error));
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.ListGroupsResponse { Error = Translate.ToProto(ex) };
        }
    }

    // Java deprecates this RPC and its three types; mirrored, not avoided.
#pragma warning disable CS0618
    /// <inheritdoc/>
    public override async Task<Proto.ListConsumerGroupsResponse> ListConsumerGroups(Proto.ListConsumerGroupsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListConsumerGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Only GroupStates is set: this options type's deprecated `States` is the SAME
            // filter on the older enum (assigning one replaces the other), and the wire has
            // one field for both, so setting both would just overwrite.
            ListConsumerGroupsResult result = admin.ListConsumerGroups(new ListConsumerGroupsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                GroupStates = TranslateAdmin.GroupStates(request.GroupStates),
                Types = TranslateAdmin.GroupTypes(request.Types_),
            });

            IReadOnlyCollection<ConsumerGroupListing> valid = await result.Valid().ConfigureAwait(false);
            IReadOnlyCollection<KafkaException> errors = await result.Errors().ConfigureAwait(false);

            Proto.ListConsumerGroupsResponse response = new Proto.ListConsumerGroupsResponse();
            foreach (ConsumerGroupListing listing in valid)
            {
                response.Valid.Add(TranslateAdmin.ConsumerGroupListingToProto(listing));
            }

            foreach (KafkaException error in errors)
            {
                response.ListingErrors.Add(Translate.ToProto(error));
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.ListConsumerGroupsResponse { Error = Translate.ToProto(ex) };
        }
    }
#pragma warning restore CS0618

    /// <inheritdoc/>
    public override async Task<Proto.DescribeConsumerGroupsResponse> DescribeConsumerGroups(Proto.DescribeConsumerGroupsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeConsumerGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DescribeConsumerGroupsResult result = admin.DescribeConsumerGroups(
                TranslateAdmin.DistinctKeys(request.GroupIds),
                new DescribeConsumerGroupsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    IncludeAuthorizedOperations = request.IncludeAuthorizedOperations,
                });

            Proto.DescribeConsumerGroupsResponse response = new Proto.DescribeConsumerGroupsResponse();
            foreach (KeyValuePair<string, Task<ConsumerGroupDescription>> pair in result.DescribedGroups)
            {
                (ConsumerGroupDescription description, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(pair.Value).ConfigureAwait(false);
                Proto.DescribeConsumerGroupsEntry entry = new Proto.DescribeConsumerGroupsEntry
                {
                    Key = TranslateAdmin.NameKey(pair.Key),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = TranslateAdmin.ConsumerGroupDescriptionToProto(description);
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeConsumerGroupsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeClassicGroupsResponse> DescribeClassicGroups(Proto.DescribeClassicGroupsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeClassicGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DescribeClassicGroupsResult result = admin.DescribeClassicGroups(
                TranslateAdmin.DistinctKeys(request.GroupIds),
                new DescribeClassicGroupsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    IncludeAuthorizedOperations = request.IncludeAuthorizedOperations,
                });

            Proto.DescribeClassicGroupsResponse response = new Proto.DescribeClassicGroupsResponse();
            foreach (KeyValuePair<string, Task<ClassicGroupDescription>> pair in result.DescribedGroups)
            {
                (ClassicGroupDescription description, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(pair.Value).ConfigureAwait(false);
                Proto.DescribeClassicGroupsEntry entry = new Proto.DescribeClassicGroupsEntry
                {
                    Key = TranslateAdmin.NameKey(pair.Key),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = TranslateAdmin.ClassicGroupDescriptionToProto(description);
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeClassicGroupsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.ListConsumerGroupOffsetsResponse> ListConsumerGroupOffsets(Proto.ListConsumerGroupOffsetsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListConsumerGroupOffsetsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Inside the try, for the same reason as ListOffsets above.
            Dictionary<string, ListConsumerGroupOffsetsSpec>? specs =
                TranslateAdmin.GroupOffsetSpecs(request.GroupSpecs, out string? invalid);
            if (specs is null)
            {
                return new Proto.ListConsumerGroupOffsetsResponse { Error = TranslateAdmin.RequestError(invalid!) };
            }

            ListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
                specs,
                new ListConsumerGroupOffsetsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    RequireStable = request.RequireStable,
                });

            // Two levels, like describeLogDirs: the per-group future carries a whole map, and
            // an inner null is Java's null map value ("no committed offset for that
            // partition"), which stays absent on the wire rather than becoming offset 0.
            Proto.ListConsumerGroupOffsetsResponse response = new Proto.ListConsumerGroupOffsetsResponse();
            foreach (string groupId in specs.Keys)
            {
                (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> offsets, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(result.PartitionsToOffsetAndMetadata(groupId))
                        .ConfigureAwait(false);
                Proto.ListConsumerGroupOffsetsEntry entry = new Proto.ListConsumerGroupOffsetsEntry
                {
                    Key = TranslateAdmin.NameKey(groupId),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    Proto.GroupOffsets value = new Proto.GroupOffsets();
                    foreach (KeyValuePair<TopicPartition, OffsetAndMetadata?> pair in offsets)
                    {
                        Proto.GroupOffset offset = new Proto.GroupOffset
                        {
                            Partition = Translate.TpToProto(pair.Key),
                        };
                        if (pair.Value is not null)
                        {
                            offset.Offset = Translate.OamToProto(pair.Value);
                        }

                        value.Offsets.Add(offset);
                    }

                    entry.Value = value;
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.ListConsumerGroupOffsetsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> AlterConsumerGroupOffsets(Proto.AlterConsumerGroupOffsetsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            Dictionary<TopicPartition, OffsetAndMetadata> offsets =
                TranslateAdmin.GroupOffsetCommits(request.Offsets);
            AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
                request.GroupId,
                offsets,
                new AlterConsumerGroupOffsetsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // The result exposes no keyed future map (Java holds one future over the whole
            // per-partition map), so the requested keys drive the walk — Java's own
            // partitionResult(tp) pattern. With an EMPTY request there is no per-partition
            // slot at all and the response is simply empty; the RPC is still submitted.
            // ⚠ That empty response DROPS a whole-call error. Java's all() is a thenApply over
            // the one future, so it propagates a whole-call failure whatever the key count,
            // and since M15/P13.1 (single-callback ABI) an empty request here waits for the
            // core's real outcome too, so result.All() carries that error even at zero keys.
            // This servicer does not await it. Python's servicer has the identical gap, so
            // the behaviour change is deferred to a cross-binding harness item rather than
            // made here alone.
            Dictionary<TopicPartition, Task> futures = new Dictionary<TopicPartition, Task>();
            foreach (TopicPartition partition in offsets.Keys)
            {
                futures[partition] = result.PartitionResult(partition);
            }

            return await TranslateAdmin.VoidResponse(futures, TranslateAdmin.PartitionKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> DeleteConsumerGroupOffsets(Proto.DeleteConsumerGroupOffsetsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            List<TopicPartition> partitions = TranslateAdmin.DistinctKeys(PartitionsOf(request.Partitions));
            DeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
                request.GroupId,
                partitions,
                new DeleteConsumerGroupOffsetsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // Same single-future shape as AlterConsumerGroupOffsets above, empty case included —
            // including the dropped whole-call error at zero keys and its deferral.
            Dictionary<TopicPartition, Task> futures = new Dictionary<TopicPartition, Task>();
            foreach (TopicPartition partition in partitions)
            {
                futures[partition] = result.PartitionResult(partition);
            }

            return await TranslateAdmin.VoidResponse(futures, TranslateAdmin.PartitionKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> DeleteConsumerGroups(Proto.DeleteConsumerGroupsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DeleteConsumerGroupsResult result = admin.DeleteConsumerGroups(
                TranslateAdmin.DistinctKeys(request.GroupIds),
                new DeleteConsumerGroupsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // Genuinely per-key futures here (Map<String, KafkaFuture<Void>>), so the
            // top-level error keeps its ordinary narrow meaning.
            return await TranslateAdmin.VoidResponse(result.DeletedGroups, TranslateAdmin.NameKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> RemoveMembersFromConsumerGroup(Proto.RemoveMembersFromConsumerGroupRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Absent members selects Java's no-argument constructor (removeAll); a present
            // list — including an empty one, which the collection constructor rejects with
            // Java's exact message — selects the other. Presence is the discriminant.
            List<MemberToRemove>? members = TranslateAdmin.MembersToRemove(request.Members);
            RemoveMembersFromConsumerGroupOptions options = members is null
                ? new RemoveMembersFromConsumerGroupOptions()
                : new RemoveMembersFromConsumerGroupOptions(members);
            options.TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs);
            if (request.HasReason)
            {
                options.Reason = request.Reason;
            }

            RemoveMembersFromConsumerGroupResult result =
                admin.RemoveMembersFromConsumerGroup(request.GroupId, options);

            if (result.RemoveAll)
            {
                // In removeAll mode Java's memberResult refuses, so all() is the only
                // observable: its failure is the top-level error and its success reports no
                // per-member entry at all. Awaiting it here runs the removal to completion
                // before responding.
                Proto.KafkaError? error = await TranslateAdmin.ResolveVoid(result.All()).ConfigureAwait(false);
                return error is null
                    ? new Proto.VoidKeyedResponse()
                    : new Proto.VoidKeyedResponse { Error = error };
            }

            Dictionary<MemberToRemove, Task> futures = new Dictionary<MemberToRemove, Task>();
            foreach (MemberToRemove member in members!)
            {
                futures[member] = result.MemberResult(member);
            }

            return await TranslateAdmin
                .VoidResponse(futures, member => TranslateAdmin.NameKey(member.GroupInstanceId))
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    // -- ACLs, quotas, SCRAM, tokens & features (slice G5) -------------------------------

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> CreateAcls(Proto.CreateAclsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            CreateAclsResult result = admin.CreateAcls(
                TranslateAdmin.AclBindings(request.Acls),
                new CreateAclsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            return await TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.AclBindingKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeAclsResponse> DescribeAcls(Proto.DescribeAclsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeAclsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            if (request.Filter is null)
            {
                return new Proto.DescribeAclsResponse
                {
                    Error = TranslateAdmin.RequestError("describe_acls requires a filter"),
                };
            }

            DescribeAclsResult result = admin.DescribeAcls(
                TranslateAdmin.AclFilter(request.Filter),
                new DescribeAclsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // Whole-value: one future for the whole call, so an empty list is a successful
            // "nothing matched" rather than a per-binding error arm.
            Proto.DescribeAclsResponse response = new Proto.DescribeAclsResponse();
            foreach (AclBinding binding in await result.Values().ConfigureAwait(false))
            {
                response.Acls.Add(TranslateAdmin.AclBindingToProto(binding));
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeAclsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DeleteAclsResponse> DeleteAcls(Proto.DeleteAclsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DeleteAclsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DeleteAclsResult result = admin.DeleteAcls(
                TranslateAdmin.AclFilters(request.Filters),
                new DeleteAclsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DeleteAclsResponse response = new Proto.DeleteAclsResponse();
            foreach (KeyValuePair<AclBindingFilter, Task<DeleteAclsResult.FilterResults>> pair in result.Values)
            {
                (DeleteAclsResult.FilterResults results, Proto.KafkaError? error) =
                    await TranslateAdmin.Resolve(pair.Value).ConfigureAwait(false);
                Proto.DeleteAclsEntry entry = new Proto.DeleteAclsEntry
                {
                    Key = TranslateAdmin.AclFilterKey(pair.Key),
                };
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = FilterResultsToProto(results);
                }

                response.Entries.Add(entry);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DeleteAclsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeClientQuotasResponse> DescribeClientQuotas(Proto.DescribeClientQuotasRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeClientQuotasResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            ClientQuotaFilter? filter = TranslateAdmin.QuotaFilter(request, out string? invalid);
            if (filter is null)
            {
                return new Proto.DescribeClientQuotasResponse
                {
                    Error = TranslateAdmin.RequestError(invalid!),
                };
            }

            DescribeClientQuotasResult result = admin.DescribeClientQuotas(
                filter,
                new DescribeClientQuotasOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // A removed quota is absent from the inner map rather than reported as zero, which
            // is the only observable separating a removal from a zero-valued set.
            Proto.DescribeClientQuotasResponse response = new Proto.DescribeClientQuotasResponse();
            IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
                await result.Entities().ConfigureAwait(false);
            foreach (KeyValuePair<ClientQuotaEntity, IReadOnlyDictionary<string, double>> pair in entities)
            {
                Proto.EntityQuotas reported = new Proto.EntityQuotas
                {
                    Entity = TranslateAdmin.QuotaEntityToProto(pair.Key),
                };
                foreach (KeyValuePair<string, double> quota in pair.Value)
                {
                    reported.Values.Add(new Proto.QuotaValue { Key = quota.Key, Value = quota.Value });
                }

                response.Entities.Add(reported);
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeClientQuotasResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> AlterClientQuotas(Proto.AlterClientQuotasRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AlterClientQuotasResult result = admin.AlterClientQuotas(
                TranslateAdmin.QuotaAlterations(request.Entries),
                new AlterClientQuotasOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    ValidateOnly = request.ValidateOnly,
                });

            return await TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.QuotaEntityKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeUserScramCredentialsResponse> DescribeUserScramCredentials(Proto.DescribeUserScramCredentialsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeUserScramCredentialsResponse
            {
                Error = Translate.UnknownAdmin(request.AdminId),
            };
        }

        try
        {
            // An empty `users` is Java's no-argument overload: describe every user.
            DescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials(
                new List<string>(request.Users),
                new DescribeUserScramCredentialsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // RAW per-user rows, so the client can rebuild all three Java views. users()
            // excludes RESOURCE_NOT_FOUND users, so all()'s keys supply those; all() itself
            // faults on a hard per-user error, and then every non-RNF user is already listed.
            List<string> candidates = new List<string>(await result.Users().ConfigureAwait(false));
            try
            {
                foreach (string user in (await result.All().ConfigureAwait(false)).Keys)
                {
                    if (!candidates.Contains(user))
                    {
                        candidates.Add(user);
                    }
                }
            }
            catch (KafkaException)
            {
            }

            Proto.DescribeUserScramCredentialsResponse response =
                new Proto.DescribeUserScramCredentialsResponse();
            foreach (string user in candidates)
            {
                response.Entries.Add(await ScramCredentialsEntry(result, user).ConfigureAwait(false));
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeUserScramCredentialsResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> AlterUserScramCredentials(Proto.AlterUserScramCredentialsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AlterUserScramCredentialsResult result = admin.AlterUserScramCredentials(
                TranslateAdmin.ScramAlterations(request.Alterations),
                new AlterUserScramCredentialsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            return await TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.NameKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.CreateDelegationTokenResponse> CreateDelegationToken(Proto.CreateDelegationTokenRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.CreateDelegationTokenResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // An absent owner leaves Java's field unset, making the requesting principal the
            // owner; both halves of the principal are absent together.
            CreateDelegationTokenOptions options = new CreateDelegationTokenOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                Renewers = TranslateAdmin.Principals(request.Renewers),
                MaxLifetimeMs = request.MaxLifetimeMs,
            };
            if (request.Owner is not null)
            {
                options.Owner = TranslateAdmin.Principals(new[] { request.Owner })[0];
            }

            CreateDelegationTokenResult result = admin.CreateDelegationToken(options);
            DelegationToken token = await result.DelegationToken().ConfigureAwait(false);
            return new Proto.CreateDelegationTokenResponse
            {
                Token = TranslateAdmin.DelegationTokenToProto(token),
            };
        }
        catch (Exception ex)
        {
            return new Proto.CreateDelegationTokenResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DelegationTokenExpiryResponse> RenewDelegationToken(Proto.RenewDelegationTokenRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DelegationTokenExpiryResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            RenewDelegationTokenResult result = admin.RenewDelegationToken(
                request.Hmac.ToByteArray(),
                new RenewDelegationTokenOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    RenewTimePeriodMs = request.RenewTimePeriodMs,
                });

            return new Proto.DelegationTokenExpiryResponse
            {
                ExpiryTimestampMs = await result.ExpiryTimestamp().ConfigureAwait(false),
            };
        }
        catch (Exception ex)
        {
            return new Proto.DelegationTokenExpiryResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DelegationTokenExpiryResponse> ExpireDelegationToken(Proto.ExpireDelegationTokenRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DelegationTokenExpiryResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            ExpireDelegationTokenResult result = admin.ExpireDelegationToken(
                request.Hmac.ToByteArray(),
                new ExpireDelegationTokenOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    ExpiryTimePeriodMs = request.ExpiryTimePeriodMs,
                });

            return new Proto.DelegationTokenExpiryResponse
            {
                ExpiryTimestampMs = await result.ExpiryTimestamp().ConfigureAwait(false),
            };
        }
        catch (Exception ex)
        {
            return new Proto.DelegationTokenExpiryResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeDelegationTokenResponse> DescribeDelegationToken(Proto.DescribeDelegationTokenRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeDelegationTokenResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DescribeDelegationTokenResult result = admin.DescribeDelegationToken(
                new DescribeDelegationTokenOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    Owners = TranslateAdmin.TokenOwners(request.Owners),
                });

            Proto.DescribeDelegationTokenResponse response = new Proto.DescribeDelegationTokenResponse();
            foreach (DelegationToken token in await result.DelegationTokens().ConfigureAwait(false))
            {
                response.Tokens.Add(TranslateAdmin.DelegationTokenToProto(token));
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeDelegationTokenResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.DescribeFeaturesResponse> DescribeFeatures(Proto.DescribeFeaturesRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeFeaturesResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // An absent node_id is Java's empty OptionalInt; node 0 is a legal broker, so
            // presence is what carries the absence.
            DescribeFeaturesResult result = admin.DescribeFeatures(
                new DescribeFeaturesOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    NodeId = request.HasNodeId ? request.NodeId : (int?)null,
                });

            FeatureMetadata metadata = await result.FeatureMetadata().ConfigureAwait(false);
            return new Proto.DescribeFeaturesResponse
            {
                Metadata = TranslateAdmin.FeatureMetadataToProto(metadata),
            };
        }
        catch (Exception ex)
        {
            return new Proto.DescribeFeaturesResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <inheritdoc/>
    public override async Task<Proto.VoidKeyedResponse> UpdateFeatures(Proto.UpdateFeaturesRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            Dictionary<string, FeatureUpdate>? updates =
                TranslateAdmin.FeatureUpdates(request.FeatureUpdates, out string? invalid);
            if (updates is null)
            {
                return new Proto.VoidKeyedResponse { Error = TranslateAdmin.RequestError(invalid!) };
            }

            // An empty map is not a no-op: Java rejects it synchronously, and that throw is
            // exactly what the top-level error is for.
            UpdateFeaturesResult result = admin.UpdateFeatures(
                updates,
                new UpdateFeaturesOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    ValidateOnly = request.ValidateOnly,
                });

            return await TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.NameKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    /// <summary>
    /// One <c>deleteAcls</c> filter's matched ACLs — envelope exception 3: the per-filter future
    /// resolved, yet an individual matched ACL can still have failed to delete. Both halves are
    /// written independently, so a backend that set neither or both stays visible.
    /// </summary>
    private static Proto.FilterResults FilterResultsToProto(DeleteAclsResult.FilterResults results)
    {
        Proto.FilterResults proto = new Proto.FilterResults();
        foreach (DeleteAclsResult.FilterResult result in results.Values)
        {
            Proto.DeletedAcl deleted = new Proto.DeletedAcl();
            if (result.Binding is not null)
            {
                deleted.Binding = TranslateAdmin.AclBindingToProto(result.Binding);
            }

            if (result.Error is not null)
            {
                deleted.Exception = Translate.ToProto(result.Error);
            }

            proto.Values.Add(deleted);
        }

        return proto;
    }

    /// <summary>
    /// One RAW <c>describeUserScramCredentials</c> row: the user, plus either its credentials or
    /// the wire error code that <c>description(user)</c> faulted with.
    /// </summary>
    private static async Task<Proto.DescribeUserScramCredentialsEntry> ScramCredentialsEntry(
        DescribeUserScramCredentialsResult result, string user)
    {
        Proto.DescribeUserScramCredentialsEntry entry =
            new Proto.DescribeUserScramCredentialsEntry { User = user };
        try
        {
            UserScramCredentialsDescription description =
                await result.Description(user).ConfigureAwait(false);
            foreach (ScramCredentialInfo info in description.CredentialInfos)
            {
                entry.CredentialInfos.Add(TranslateAdmin.ScramInfoToProto(info));
            }
        }
        catch (KafkaException ex)
        {
            entry.ErrorCode = ex.Code;
            entry.ErrorMessage = ex.Message ?? string.Empty;
        }

        return entry;
    }

    /// <summary>Proto <c>TopicPartition</c>s -&gt; binding ones.</summary>
    private static IEnumerable<TopicPartition> PartitionsOf(IEnumerable<Proto.TopicPartition> protos)
    {
        foreach (Proto.TopicPartition proto in protos)
        {
            yield return Translate.Tp(proto);
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
