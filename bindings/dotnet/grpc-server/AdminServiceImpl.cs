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
/// log dirs), G3 (elections, reassignments, offsets) and G6 (producers &amp; transactions) are
/// implemented here; G4 (groups) and G5 (acls, quotas, scram, tokens, features) are added
/// additively by later checkpoints and answer from the generated base until then.
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
            (string clusterId, Proto.KafkaError? clusterIdError) =
                await TranslateAdmin.Resolve(result.ClusterId()).ConfigureAwait(false);
            (IReadOnlyCollection<AclOperation>? operations, Proto.KafkaError? operationsError) =
                await TranslateAdmin.Resolve(result.AuthorizedOperations()).ConfigureAwait(false);

            Proto.KafkaError? error = nodesError ?? controllerError ?? clusterIdError ?? operationsError;
            if (error is not null)
            {
                return new Proto.DescribeClusterResponse { Error = error };
            }

            Proto.ClusterDescription description = new Proto.ClusterDescription { ClusterId = clusterId };
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
