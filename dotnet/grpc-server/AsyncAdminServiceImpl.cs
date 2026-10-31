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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Common.Security.Token.Delegation;
using Confluent.Kafka.Consumer;
using Grpc.Core;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// The async-flavour <c>AdminService</c> over <see cref="IAsyncAdmin"/>, the .NET port of Python's
/// async <c>AdminService</c>; each handler awaits its futures. Kept parallel to
/// <see cref="AdminServiceImpl"/>.
/// </summary>
internal sealed class AsyncAdminServiceImpl : Proto.AdminService.AdminServiceBase, IDisposable
{
    private readonly ConcurrentDictionary<ulong, IAsyncAdmin> _admins = new ConcurrentDictionary<ulong, IAsyncAdmin>();

    private long _nextId;

    public void Dispose()
    {
        foreach (KeyValuePair<ulong, IAsyncAdmin> pair in _admins)
        {
            if (!_admins.TryRemove(pair.Key, out IAsyncAdmin? admin))
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

    public override Task<Proto.CreateAdminResponse> CreateAdmin(Proto.CreateAdminRequest request, ServerCallContext context)
    {
        Dictionary<string, string> config = new Dictionary<string, string>(request.Config);
        int numBrokers = request.HasNumBrokers ? request.NumBrokers : 1;
        IAsyncAdmin admin;
        try
        {
            admin = TranslateAdmin.SelectsMock(config)
                ? AsyncMockAdminClient.Create().NumBrokers(numBrokers).Build()
                : (IAsyncAdmin)new AsyncKafkaAdminClient(config);
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

    public override async Task<Proto.StatusResponse> Close(Proto.AdminCloseRequest request, ServerCallContext context)
    {
        if (!_admins.TryRemove(request.AdminId, out IAsyncAdmin? admin))
        {
            // Close is idempotent: an unknown id is a silent success, as in Python.
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
                await admin.Close().ConfigureAwait(false);
            }

            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
        finally
        {
            // A mock's Close does nothing, as Java's mock; DisposeAsync frees the removed client.
            await admin.DisposeAsync().ConfigureAwait(false);
        }
    }

    public override async Task<Proto.CreateTopicsResponse> CreateTopics(Proto.CreateTopicsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.CreateTopicsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncCreateTopicsResult result = admin.CreateTopics(
                TranslateAdmin.NewTopics(request.Topics),
                new CreateTopicsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    ValidateOnly = request.ValidateOnly,
                    RetryOnQuotaViolation =
                        TranslateAdmin.RetryOnQuota(request.HasRetryOnQuotaViolation, request.RetryOnQuotaViolation),
                });

            Proto.CreateTopicsResponse response = new Proto.CreateTopicsResponse();
            foreach (KeyValuePair<string, AsyncKafkaFuture> pair in result.Values)
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

    public override async Task<Proto.VoidKeyedResponse> DeleteTopics(Proto.DeleteTopicsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            bool byIds = request.TopicsCase == Proto.DeleteTopicsRequest.TopicsOneofCase.TopicIds;
            AsyncDeleteTopicsResult result = admin.DeleteTopics(
                TranslateAdmin.TopicCollectionOf(byIds, request.Names, request.TopicIds),
                new DeleteTopicsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    RetryOnQuotaViolation =
                        TranslateAdmin.RetryOnQuota(request.HasRetryOnQuotaViolation, request.RetryOnQuotaViolation),
                });

            return result.TopicIdValues is not null
                ? await TranslateAdmin.VoidResponse(result.TopicIdValues, TranslateAdmin.TopicIdKey).ConfigureAwait(false)
                : await TranslateAdmin.VoidResponse(result.TopicNameValues!, TranslateAdmin.NameKey).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.AdminListTopicsResponse> ListTopics(Proto.AdminListTopicsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.AdminListTopicsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncListTopicsResult result = admin.ListTopics(new ListTopicsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                ListInternal = request.ListInternal,
            });

            IReadOnlyDictionary<string, TopicListing> listings =
                await result.NamesToListings().Get().ConfigureAwait(false);
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

    public override async Task<Proto.DescribeTopicsResponse> DescribeTopics(Proto.DescribeTopicsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
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

            AsyncDescribeTopicsResult result = admin.DescribeTopics(
                TranslateAdmin.TopicCollectionOf(byIds, request.Names, request.TopicIds), options);

            Proto.DescribeTopicsResponse response = new Proto.DescribeTopicsResponse();
            if (result.TopicIdValues is not null)
            {
                foreach (KeyValuePair<Uuid, AsyncKafkaFuture<TopicDescription>> pair in result.TopicIdValues)
                {
                    response.Entries.Add(
                        await DescribeTopicsEntry(TranslateAdmin.TopicIdKey(pair.Key), pair.Value).ConfigureAwait(false));
                }
            }
            else
            {
                foreach (KeyValuePair<string, AsyncKafkaFuture<TopicDescription>> pair in result.TopicNameValues!)
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

    public override async Task<Proto.VoidKeyedResponse> CreatePartitions(Proto.CreatePartitionsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncCreatePartitionsResult result = admin.CreatePartitions(
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

    public override async Task<Proto.DeleteRecordsResponse> DeleteRecords(Proto.DeleteRecordsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DeleteRecordsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDeleteRecordsResult result = admin.DeleteRecords(
                TranslateAdmin.RecordsToDeleteMap(request.Records),
                new DeleteRecordsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DeleteRecordsResponse response = new Proto.DeleteRecordsResponse();
            foreach (KeyValuePair<TopicPartition, AsyncKafkaFuture<DeletedRecords>> pair in result.LowWatermarks)
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

    public override async Task<Proto.DescribeClusterResponse> DescribeCluster(Proto.DescribeClusterRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeClusterResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDescribeClusterResult result = admin.DescribeCluster(new DescribeClusterOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                IncludeAuthorizedOperations = request.IncludeAuthorizedOperations,
                IncludeFencedBrokers = request.IncludeFencedBrokers,
            });

            // Java's four futures are over one cluster: all are awaited before any error is
            // reported, and when more than one failed the first in Java's declaration order wins.
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

    public override async Task<Proto.DescribeConfigsResponse> DescribeConfigs(Proto.DescribeConfigsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeConfigsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDescribeConfigsResult result = admin.DescribeConfigs(
                TranslateAdmin.ConfigResources(request.Resources),
                new DescribeConfigsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    IncludeSynonyms = request.IncludeSynonyms,
                    IncludeDocumentation = request.IncludeDocumentation,
                });

            Proto.DescribeConfigsResponse response = new Proto.DescribeConfigsResponse();
            foreach (KeyValuePair<ConfigResource, AsyncKafkaFuture<Config>> pair in result.Values)
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

    public override async Task<Proto.VoidKeyedResponse> IncrementalAlterConfigs(Proto.IncrementalAlterConfigsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncAlterConfigsResult result = admin.IncrementalAlterConfigs(
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

    public override async Task<Proto.ListConfigResourcesResponse> ListConfigResources(Proto.ListConfigResourcesRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListConfigResourcesResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncListConfigResourcesResult result = admin.ListConfigResources(
                TranslateAdmin.ConfigResourceTypes(request.ResourceTypes),
                new ListConfigResourcesOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            IReadOnlyCollection<ConfigResource> resources = await result.All().Get().ConfigureAwait(false);
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

    public override async Task<Proto.DescribeLogDirsResponse> DescribeLogDirs(Proto.DescribeLogDirsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeLogDirsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDescribeLogDirsResult result = admin.DescribeLogDirs(
                new List<int>(request.Brokers),
                new DescribeLogDirsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DescribeLogDirsResponse response = new Proto.DescribeLogDirsResponse();
            foreach (KeyValuePair<int, AsyncKafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> pair in
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
                    // Nested, not flattened into (broker, path) keys, so a broker with zero log
                    // dirs stays representable.
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

    public override async Task<Proto.VoidKeyedResponse> AlterReplicaLogDirs(Proto.AlterReplicaLogDirsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncAlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
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

    public override async Task<Proto.DescribeReplicaLogDirsResponse> DescribeReplicaLogDirs(Proto.DescribeReplicaLogDirsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeReplicaLogDirsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDescribeReplicaLogDirsResult result = admin.DescribeReplicaLogDirs(
                TranslateAdmin.Replicas(request.Replicas),
                new DescribeReplicaLogDirsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DescribeReplicaLogDirsResponse response = new Proto.DescribeReplicaLogDirsResponse();
            foreach (KeyValuePair<TopicPartitionReplica, AsyncKafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> pair
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

    public override async Task<Proto.VoidKeyedResponse> ElectLeaders(Proto.ElectLeadersRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncElectLeadersResult result = admin.ElectLeaders(
                (ElectionType)request.ElectionType,
                TranslateAdmin.OptionalPartitions(request.Partitions),
                new ElectLeadersOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // Java holds one future over the whole map, so its failure is the whole-call error; a
            // per-entry error is the Optional<Throwable> in the resolved map, absent meaning that
            // election succeeded.
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

    public override async Task<Proto.VoidKeyedResponse> AlterPartitionReassignments(Proto.AlterPartitionReassignmentsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncAlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
                TranslateAdmin.Reassignments(request.Reassignments),
                new AlterPartitionReassignmentsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    // Java's default is true, hence the presence flag rather than a bare bool.
                    AllowReplicationFactorChange = !request.HasAllowReplicationFactorChange
                        || request.AllowReplicationFactorChange,
                });

            return await TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.PartitionKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.ListPartitionReassignmentsResponse> ListPartitionReassignments(Proto.ListPartitionReassignmentsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListPartitionReassignmentsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncListPartitionReassignmentsResult result = admin.ListPartitionReassignments(
                TranslateAdmin.OptionalPartitions(request.Partitions),
                new ListPartitionReassignmentsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments =
                await result.Reassignments().Get().ConfigureAwait(false);
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

    public override async Task<Proto.ListOffsetsResponse> ListOffsets(Proto.ListOffsetsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListOffsetsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Inside the try: C# reads an unset submessage as null (Python reads a default
            // instance), so a malformed request must become the top-level error, not a gRPC status.
            Dictionary<TopicPartition, OffsetSpec>? specs =
                TranslateAdmin.OffsetSpecs(request.Specs, out string? invalid);
            if (specs is null)
            {
                return new Proto.ListOffsetsResponse { Error = TranslateAdmin.RequestError(invalid!) };
            }

            AsyncListOffsetsResult result = admin.ListOffsets(
                specs,
                new ListOffsetsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    IsolationLevel = (IsolationLevel)request.IsolationLevel,
                });

            // No keyed future map: the requested keys drive the walk, as Java's
            // PartitionResult(tp).
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

    public override async Task<Proto.DescribeProducersResponse> DescribeProducers(Proto.DescribeProducersRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
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

            if (request.HasBrokerId)
            {
                options.BrokerId = request.BrokerId;
            }

            AsyncDescribeProducersResult result = admin.DescribeProducers(partitions, options);

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

    public override async Task<Proto.DescribeTransactionsResponse> DescribeTransactions(Proto.DescribeTransactionsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeTransactionsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            List<string> ids = TranslateAdmin.DistinctKeys(request.TransactionalIds);
            AsyncDescribeTransactionsResult result = admin.DescribeTransactions(
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

    public override async Task<Proto.StatusResponse> AbortTransaction(Proto.AbortTransactionRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
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

            await admin.AbortTransaction(
                spec,
                new AbortTransactionOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                }).All().Get().ConfigureAwait(false);

            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.StatusResponse> ForceTerminateTransaction(Proto.ForceTerminateTransactionRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
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
                }).Result().Get().ConfigureAwait(false);

            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.StatusResponse> UnregisterController(Proto.UnregisterControllerRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.StatusResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            await admin.UnregisterController(
                request.ControllerId,
                new UnregisterControllerOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                }).All().Get().ConfigureAwait(false);

            return new Proto.StatusResponse();
        }
        catch (Exception ex)
        {
            return new Proto.StatusResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.ListTransactionsResponse> ListTransactions(Proto.ListTransactionsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListTransactionsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncListTransactionsResult result = admin.ListTransactions(new ListTransactionsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                FilteredStates = TranslateAdmin.TransactionStates(request.States),
                FilteredProducerIds = new List<long>(request.ProducerIds),
                FilteredDuration = request.DurationMs,
                FilteredTransactionalIdPattern =
                    request.HasTransactionalIdPattern ? request.TransactionalIdPattern : null,
            });

            // Keyed by broker: byBrokerId() is the only one of Java's three views that keeps a
            // per-broker error; only the broker-discovery future's failure is the whole-call error.
            (IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>> brokers,
                Proto.KafkaError? error) =
                await TranslateAdmin.Resolve(result.ByBrokerId()).ConfigureAwait(false);
            if (error is not null)
            {
                return new Proto.ListTransactionsResponse { Error = error };
            }

            Proto.ListTransactionsResponse response = new Proto.ListTransactionsResponse();
            foreach (KeyValuePair<int, AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>> broker in brokers)
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

    public override async Task<Proto.FenceProducersResponse> FenceProducers(Proto.FenceProducersRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.FenceProducersResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncFenceProducersResult result = admin.FenceProducers(
                TranslateAdmin.DistinctKeys(request.TransactionalIds),
                new FenceProducersOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.FenceProducersResponse response = new Proto.FenceProducersResponse();
            foreach (string id in result.FencedProducers.Keys)
            {
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

    public override async Task<Proto.ListGroupsResponse> ListGroups(Proto.ListGroupsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.ListGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncListGroupsResult result = admin.ListGroups(new ListGroupsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                GroupStates = TranslateAdmin.GroupStates(request.GroupStates),
                ProtocolTypes = new List<string>(request.ProtocolTypes),
                Types = TranslateAdmin.GroupTypes(request.Types_),
            });

            // valid() and errors() are independent lists of unrelated length: never zip them.
            IReadOnlyCollection<GroupListing> valid = await result.Valid().Get().ConfigureAwait(false);
            IReadOnlyCollection<KafkaException> errors = await result.Errors().Get().ConfigureAwait(false);

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

    public override async Task<Proto.DescribeConsumerGroupsResponse> DescribeConsumerGroups(Proto.DescribeConsumerGroupsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeConsumerGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDescribeConsumerGroupsResult result = admin.DescribeConsumerGroups(
                TranslateAdmin.DistinctKeys(request.GroupIds),
                new DescribeConsumerGroupsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    IncludeAuthorizedOperations = request.IncludeAuthorizedOperations,
                });

            Proto.DescribeConsumerGroupsResponse response = new Proto.DescribeConsumerGroupsResponse();
            foreach (KeyValuePair<string, AsyncKafkaFuture<ConsumerGroupDescription>> pair in result.DescribedGroups)
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

    public override async Task<Proto.DescribeClassicGroupsResponse> DescribeClassicGroups(Proto.DescribeClassicGroupsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeClassicGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDescribeClassicGroupsResult result = admin.DescribeClassicGroups(
                TranslateAdmin.DistinctKeys(request.GroupIds),
                new DescribeClassicGroupsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    IncludeAuthorizedOperations = request.IncludeAuthorizedOperations,
                });

            Proto.DescribeClassicGroupsResponse response = new Proto.DescribeClassicGroupsResponse();
            foreach (KeyValuePair<string, AsyncKafkaFuture<ClassicGroupDescription>> pair in result.DescribedGroups)
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

    public override async Task<Proto.ListConsumerGroupOffsetsResponse> ListConsumerGroupOffsets(Proto.ListConsumerGroupOffsetsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
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

            AsyncListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
                specs,
                new ListConsumerGroupOffsetsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    RequireStable = request.RequireStable,
                });

            // An inner null is Java's null map value (no committed offset): it stays absent, not
            // offset 0.
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

    public override async Task<Proto.VoidKeyedResponse> AlterConsumerGroupOffsets(Proto.AlterConsumerGroupOffsetsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            Dictionary<TopicPartition, OffsetAndMetadata> offsets =
                TranslateAdmin.GroupOffsetCommits(request.Offsets);
            AsyncAlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
                request.GroupId,
                offsets,
                new AlterConsumerGroupOffsetsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // No keyed future map: the requested keys drive the walk, as Java's
            // partitionResult(tp). An empty request gets an empty response, which drops a
            // whole-call error that All() carries, as Python's servicer does.
            Dictionary<TopicPartition, AsyncKafkaFuture> futures = new Dictionary<TopicPartition, AsyncKafkaFuture>();
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

    public override async Task<Proto.VoidKeyedResponse> DeleteConsumerGroupOffsets(Proto.DeleteConsumerGroupOffsetsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            List<TopicPartition> partitions = TranslateAdmin.DistinctKeys(PartitionsOf(request.Partitions));
            AsyncDeleteConsumerGroupOffsetsResult result = admin.DeleteConsumerGroupOffsets(
                request.GroupId,
                partitions,
                new DeleteConsumerGroupOffsetsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // Same shape as AlterConsumerGroupOffsets, the empty-request case included.
            Dictionary<TopicPartition, AsyncKafkaFuture> futures = new Dictionary<TopicPartition, AsyncKafkaFuture>();
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

    public override async Task<Proto.VoidKeyedResponse> DeleteConsumerGroups(Proto.DeleteConsumerGroupsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDeleteConsumerGroupsResult result = admin.DeleteConsumerGroups(
                TranslateAdmin.DistinctKeys(request.GroupIds),
                new DeleteConsumerGroupsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            return await TranslateAdmin.VoidResponse(result.DeletedGroups, TranslateAdmin.NameKey)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.VoidKeyedResponse> RemoveMembersFromConsumerGroup(Proto.RemoveMembersFromConsumerGroupRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            // Presence, not emptiness, selects Java's constructor: absent is removeAll; a present
            // list, even an empty one that Java's collection constructor rejects, is the other.
            List<MemberToRemove>? members = TranslateAdmin.MembersToRemove(request.Members);
            RemoveMembersFromConsumerGroupOptions options = members is null
                ? new RemoveMembersFromConsumerGroupOptions()
                : new RemoveMembersFromConsumerGroupOptions(members);
            options.TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs);
            if (request.HasReason)
            {
                options.Reason = request.Reason;
            }

            AsyncRemoveMembersFromConsumerGroupResult result =
                admin.RemoveMembersFromConsumerGroup(request.GroupId, options);

            if (options.RemoveAll)
            {
                // In removeAll mode Java's memberResult refuses, so All() is the only observable;
                // awaiting it runs the removal to completion before responding.
                Proto.KafkaError? error = await TranslateAdmin.ResolveVoid(result.All()).ConfigureAwait(false);
                return error is null
                    ? new Proto.VoidKeyedResponse()
                    : new Proto.VoidKeyedResponse { Error = error };
            }

            Dictionary<MemberToRemove, AsyncKafkaFuture> futures = new Dictionary<MemberToRemove, AsyncKafkaFuture>();
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

    public override async Task<Proto.VoidKeyedResponse> CreateAcls(Proto.CreateAclsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncCreateAclsResult result = admin.CreateAcls(
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

    public override async Task<Proto.DescribeAclsResponse> DescribeAcls(Proto.DescribeAclsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
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

            AsyncDescribeAclsResult result = admin.DescribeAcls(
                TranslateAdmin.AclFilter(request.Filter),
                new DescribeAclsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DescribeAclsResponse response = new Proto.DescribeAclsResponse();
            foreach (AclBinding binding in await result.Values().Get().ConfigureAwait(false))
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

    public override async Task<Proto.DeleteAclsResponse> DeleteAcls(Proto.DeleteAclsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DeleteAclsResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDeleteAclsResult result = admin.DeleteAcls(
                TranslateAdmin.AclFilters(request.Filters),
                new DeleteAclsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DeleteAclsResponse response = new Proto.DeleteAclsResponse();
            foreach (KeyValuePair<AclBindingFilter, AsyncKafkaFuture<DeleteAclsResult.FilterResults>> pair in result.Values)
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

    public override async Task<Proto.DescribeClientQuotasResponse> DescribeClientQuotas(Proto.DescribeClientQuotasRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
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

            AsyncDescribeClientQuotasResult result = admin.DescribeClientQuotas(
                filter,
                new DescribeClientQuotasOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DescribeClientQuotasResponse response = new Proto.DescribeClientQuotasResponse();
            IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
                await result.Entities().Get().ConfigureAwait(false);
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

    public override async Task<Proto.VoidKeyedResponse> AlterClientQuotas(Proto.AlterClientQuotasRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncAlterClientQuotasResult result = admin.AlterClientQuotas(
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

    public override async Task<Proto.DescribeUserScramCredentialsResponse> DescribeUserScramCredentials(Proto.DescribeUserScramCredentialsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeUserScramCredentialsResponse
            {
                Error = Translate.UnknownAdmin(request.AdminId),
            };
        }

        try
        {
            AsyncDescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials(
                new List<string>(request.Users),
                new DescribeUserScramCredentialsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            // Java's three views, composed as admin_service.proto specifies: all(); when it fails,
            // users() plus description(user) per user, and all()'s error when no user is left.
            IReadOnlyDictionary<string, UserScramCredentialsDescription> all;
            try
            {
                all = await result.All().Get().ConfigureAwait(false);
            }
            catch (Exception)
            {
                Proto.DescribeUserScramCredentialsResponse fallback =
                    new Proto.DescribeUserScramCredentialsResponse();
                foreach (string user in await result.Users().Get().ConfigureAwait(false))
                {
                    fallback.Entries.Add(await ScramCredentialsEntry(result, user).ConfigureAwait(false));
                }

                if (fallback.Entries.Count == 0)
                {
                    throw;
                }

                return fallback;
            }

            Proto.DescribeUserScramCredentialsResponse response =
                new Proto.DescribeUserScramCredentialsResponse();
            foreach (KeyValuePair<string, UserScramCredentialsDescription> pair in all)
            {
                response.Entries.Add(new Proto.DescribeUserScramCredentialsEntry
                {
                    Key = TranslateAdmin.NameKey(pair.Key),
                    Value = TranslateAdmin.ScramDescriptionToProto(pair.Value),
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            return new Proto.DescribeUserScramCredentialsResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.VoidKeyedResponse> AlterUserScramCredentials(Proto.AlterUserScramCredentialsRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncAlterUserScramCredentialsResult result = admin.AlterUserScramCredentials(
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

    public override async Task<Proto.CreateDelegationTokenResponse> CreateDelegationToken(Proto.CreateDelegationTokenRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.CreateDelegationTokenResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
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

            AsyncCreateDelegationTokenResult result = admin.CreateDelegationToken(options);
            DelegationToken token = await result.DelegationToken().Get().ConfigureAwait(false);
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

    public override async Task<Proto.DelegationTokenExpiryResponse> RenewDelegationToken(Proto.RenewDelegationTokenRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DelegationTokenExpiryResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncRenewDelegationTokenResult result = admin.RenewDelegationToken(
                request.Hmac.ToByteArray(),
                new RenewDelegationTokenOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    RenewTimePeriodMs = request.RenewTimePeriodMs,
                });

            return new Proto.DelegationTokenExpiryResponse
            {
                ExpiryTimestampMs = await result.ExpiryTimestamp().Get().ConfigureAwait(false),
            };
        }
        catch (Exception ex)
        {
            return new Proto.DelegationTokenExpiryResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.DelegationTokenExpiryResponse> ExpireDelegationToken(Proto.ExpireDelegationTokenRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DelegationTokenExpiryResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncExpireDelegationTokenResult result = admin.ExpireDelegationToken(
                request.Hmac.ToByteArray(),
                new ExpireDelegationTokenOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    ExpiryTimePeriodMs = request.ExpiryTimePeriodMs,
                });

            return new Proto.DelegationTokenExpiryResponse
            {
                ExpiryTimestampMs = await result.ExpiryTimestamp().Get().ConfigureAwait(false),
            };
        }
        catch (Exception ex)
        {
            return new Proto.DelegationTokenExpiryResponse { Error = Translate.ToProto(ex) };
        }
    }

    public override async Task<Proto.DescribeDelegationTokenResponse> DescribeDelegationToken(Proto.DescribeDelegationTokenRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeDelegationTokenResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            AsyncDescribeDelegationTokenResult result = admin.DescribeDelegationToken(
                new DescribeDelegationTokenOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    Owners = TranslateAdmin.TokenOwners(request.Owners),
                });

            Proto.DescribeDelegationTokenResponse response = new Proto.DescribeDelegationTokenResponse();
            foreach (DelegationToken token in await result.DelegationTokens().Get().ConfigureAwait(false))
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

    public override async Task<Proto.DescribeFeaturesResponse> DescribeFeatures(Proto.DescribeFeaturesRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return new Proto.DescribeFeaturesResponse { Error = Translate.UnknownAdmin(request.AdminId) };
        }

        try
        {
            DescribeFeaturesOptions options = new DescribeFeaturesOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
            };

            if (request.HasNodeId)
            {
                options.NodeId = request.NodeId;
            }

            AsyncDescribeFeaturesResult result = admin.DescribeFeatures(options);

            FeatureMetadata metadata = await result.FeatureMetadata().Get().ConfigureAwait(false);
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

    public override async Task<Proto.VoidKeyedResponse> UpdateFeatures(Proto.UpdateFeaturesRequest request, ServerCallContext context)
    {
        IAsyncAdmin? admin = Get(request.AdminId);
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

            // An empty map is forwarded: a real client throws (the top-level error) and a mock
            // returns an empty result, both as in Java.
            AsyncUpdateFeaturesResult result = admin.UpdateFeatures(
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

            if (result.Exception is not null)
            {
                deleted.Exception = Translate.ToProto(result.Exception);
            }

            proto.Values.Add(deleted);
        }

        return proto;
    }

    private static async Task<Proto.DescribeUserScramCredentialsEntry> ScramCredentialsEntry(
        AsyncDescribeUserScramCredentialsResult result, string user)
    {
        Proto.DescribeUserScramCredentialsEntry entry =
            new Proto.DescribeUserScramCredentialsEntry { Key = TranslateAdmin.NameKey(user) };
        try
        {
            entry.Value = TranslateAdmin.ScramDescriptionToProto(
                await result.Description(user).Get().ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            entry.Error = Translate.ToProto(ex);
        }

        return entry;
    }

    private static IEnumerable<TopicPartition> PartitionsOf(IEnumerable<Proto.TopicPartition> protos)
    {
        foreach (Proto.TopicPartition proto in protos)
        {
            yield return Translate.Tp(proto);
        }
    }

    private static async Task<Proto.DescribeTopicsEntry> DescribeTopicsEntry(
        Proto.ResultKey key, AsyncKafkaFuture<TopicDescription> future)
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

    // The per-key future resolved, but the accessors throw when the broker created the topic
    // without returning its metadata: that crosses as the value's own error arm.
    private static async Task<Proto.TopicMetadataAndConfig> MetadataAndConfig(AsyncCreateTopicsResult result, string topic)
    {
        try
        {
            Proto.TopicMetadata metadata = new Proto.TopicMetadata
            {
                TopicId = (await result.TopicId(topic).Get().ConfigureAwait(false)).ToString(),
                NumPartitions = await result.NumPartitions(topic).Get().ConfigureAwait(false),
                ReplicationFactor = await result.ReplicationFactor(topic).Get().ConfigureAwait(false),
            };
            Config config = await result.Config(topic).Get().ConfigureAwait(false);
            foreach (ConfigEntry entry in config.Entries)
            {
                metadata.Configs.Add(TranslateAdmin.ConfigEntryToProto(entry));
            }

            return new Proto.TopicMetadataAndConfig { Metadata = metadata };
        }
        catch (Exception ex)
        {
            return new Proto.TopicMetadataAndConfig { Error = Translate.ToProto(ex) };
        }
    }

    private IAsyncAdmin? Get(ulong adminId) =>
        _admins.TryGetValue(adminId, out IAsyncAdmin? admin) ? admin : null;
}
