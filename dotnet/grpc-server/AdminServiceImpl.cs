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
/// The sync-flavour <c>AdminService</c> over <see cref="IAdmin"/>, the .NET port of Python's
/// <c>AdminService</c>; each handler blocks in <c>Get()</c>, as a sync caller would. Kept parallel
/// to <see cref="AsyncAdminServiceImpl"/>.
/// </summary>
internal sealed class AdminServiceImpl : Proto.AdminService.AdminServiceBase, IDisposable
{
    private readonly ConcurrentDictionary<ulong, IAdmin> _admins = new ConcurrentDictionary<ulong, IAdmin>();

    private long _nextId;

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

    public override Task<Proto.CreateAdminResponse> CreateAdmin(Proto.CreateAdminRequest request, ServerCallContext context)
    {
        Dictionary<string, string> config = new Dictionary<string, string>(request.Config);
        int numBrokers = request.HasNumBrokers ? request.NumBrokers : 1;
        IAdmin admin;
        try
        {
            admin = TranslateAdmin.SelectsMock(config)
                ? MockAdminClient.Create().NumBrokers(numBrokers).Build()
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

    public override Task<Proto.StatusResponse> Close(Proto.AdminCloseRequest request, ServerCallContext context)
    {
        if (!_admins.TryRemove(request.AdminId, out IAdmin? admin))
        {
            // Close is idempotent: an unknown id is a silent success, as in Python.
            return Task.FromResult(new Proto.StatusResponse());
        }

        try
        {
            if (request.HasTimeoutMs)
            {
                admin.Close(TimeSpan.FromMilliseconds(request.TimeoutMs));
            }
            else
            {
                admin.Close();
            }

            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
        }
        finally
        {
            // A mock's Close does nothing, as Java's mock; Dispose frees the removed client.
            admin.Dispose();
        }
    }

    public override Task<Proto.CreateTopicsResponse> CreateTopics(Proto.CreateTopicsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.CreateTopicsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
            foreach (KeyValuePair<string, KafkaFuture> pair in result.Values)
            {
                Proto.CreateTopicsEntry entry = new Proto.CreateTopicsEntry { Key = TranslateAdmin.NameKey(pair.Key) };
                Proto.KafkaError? error = TranslateAdmin.ResolveVoid(pair.Value);
                if (error is not null)
                {
                    entry.Error = error;
                }
                else
                {
                    entry.Value = MetadataAndConfig(result, pair.Key);
                }

                response.Entries.Add(entry);
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.CreateTopicsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> DeleteTopics(Proto.DeleteTopicsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            return Task.FromResult(result.TopicIdValues is not null
                ? TranslateAdmin.VoidResponse(result.TopicIdValues, TranslateAdmin.TopicIdKey)
                : TranslateAdmin.VoidResponse(result.TopicNameValues!, TranslateAdmin.NameKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.AdminListTopicsResponse> ListTopics(Proto.AdminListTopicsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.AdminListTopicsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            ListTopicsResult result = admin.ListTopics(new ListTopicsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                ListInternal = request.ListInternal,
            });

            IReadOnlyDictionary<string, TopicListing> listings =
                result.NamesToListings().Get();
            Proto.AdminListTopicsResponse response = new Proto.AdminListTopicsResponse();
            foreach (TopicListing listing in listings.Values)
            {
                response.Listings.Add(TranslateAdmin.ListingToProto(listing));
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.AdminListTopicsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeTopicsResponse> DescribeTopics(Proto.DescribeTopicsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeTopicsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
                foreach (KeyValuePair<Uuid, KafkaFuture<TopicDescription>> pair in result.TopicIdValues)
                {
                    response.Entries.Add(
                        DescribeTopicsEntry(TranslateAdmin.TopicIdKey(pair.Key), pair.Value));
                }
            }
            else
            {
                foreach (KeyValuePair<string, KafkaFuture<TopicDescription>> pair in result.TopicNameValues!)
                {
                    response.Entries.Add(
                        DescribeTopicsEntry(TranslateAdmin.NameKey(pair.Key), pair.Value));
                }
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeTopicsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> CreatePartitions(Proto.CreatePartitionsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            return Task.FromResult(TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.NameKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DeleteRecordsResponse> DeleteRecords(Proto.DeleteRecordsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DeleteRecordsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
            foreach (KeyValuePair<TopicPartition, KafkaFuture<DeletedRecords>> pair in result.LowWatermarks)
            {
                (DeletedRecords deleted, Proto.KafkaError? error) =
                    TranslateAdmin.Resolve(pair.Value);
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DeleteRecordsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeClusterResponse> DescribeCluster(Proto.DescribeClusterRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeClusterResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            DescribeClusterResult result = admin.DescribeCluster(new DescribeClusterOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                IncludeAuthorizedOperations = request.IncludeAuthorizedOperations,
                IncludeFencedBrokers = request.IncludeFencedBrokers,
            });

            // Java's four futures are over one cluster: all are resolved before any error is
            // reported, and when more than one failed the first in Java's declaration order wins.
            (IReadOnlyCollection<Node> nodes, Proto.KafkaError? nodesError) =
                TranslateAdmin.Resolve(result.Nodes());
            (Node? controller, Proto.KafkaError? controllerError) =
                TranslateAdmin.Resolve(result.Controller());
            (string clusterId, Proto.KafkaError? clusterIdError) =
                TranslateAdmin.Resolve(result.ClusterId());
            (IReadOnlyCollection<AclOperation>? operations, Proto.KafkaError? operationsError) =
                TranslateAdmin.Resolve(result.AuthorizedOperations());

            Proto.KafkaError? error = nodesError ?? controllerError ?? clusterIdError ?? operationsError;
            if (error is not null)
            {
                return Task.FromResult(new Proto.DescribeClusterResponse { Error = error });
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

            return Task.FromResult(new Proto.DescribeClusterResponse { Description = description });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeClusterResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeConfigsResponse> DescribeConfigs(Proto.DescribeConfigsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeConfigsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
            foreach (KeyValuePair<ConfigResource, KafkaFuture<Config>> pair in result.Values)
            {
                (Config config, Proto.KafkaError? error) =
                    TranslateAdmin.Resolve(pair.Value);
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeConfigsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> IncrementalAlterConfigs(Proto.IncrementalAlterConfigsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            return Task.FromResult(TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.ConfigResourceKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.ListConfigResourcesResponse> ListConfigResources(Proto.ListConfigResourcesRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.ListConfigResourcesResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            ListConfigResourcesResult result = admin.ListConfigResources(
                TranslateAdmin.ConfigResourceTypes(request.ResourceTypes),
                new ListConfigResourcesOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            IReadOnlyCollection<ConfigResource> resources = result.All().Get();
            Proto.ListConfigResourcesResponse response = new Proto.ListConfigResourcesResponse();
            foreach (ConfigResource resource in resources)
            {
                response.Resources.Add(TranslateAdmin.ConfigResourceToProto(resource));
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.ListConfigResourcesResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeLogDirsResponse> DescribeLogDirs(Proto.DescribeLogDirsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeLogDirsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
            foreach (KeyValuePair<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> pair in
                result.Descriptions)
            {
                (IReadOnlyDictionary<string, LogDirDescription> descriptions, Proto.KafkaError? error) =
                    TranslateAdmin.Resolve(pair.Value);
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeLogDirsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> AlterReplicaLogDirs(Proto.AlterReplicaLogDirsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            AlterReplicaLogDirsResult result = admin.AlterReplicaLogDirs(
                TranslateAdmin.ReplicaLogDirAssignments(request.Assignments),
                new AlterReplicaLogDirsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            return Task.FromResult(TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.ReplicaKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeReplicaLogDirsResponse> DescribeReplicaLogDirs(Proto.DescribeReplicaLogDirsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeReplicaLogDirsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
            foreach (KeyValuePair<TopicPartitionReplica, KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> pair
                in result.Values)
            {
                (DescribeReplicaLogDirsResult.ReplicaLogDirInfo info, Proto.KafkaError? error) =
                    TranslateAdmin.Resolve(pair.Value);
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeReplicaLogDirsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> ElectLeaders(Proto.ElectLeadersRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            // Java holds one future over the whole map, so its failure is the whole-call error; a
            // per-entry error is the Optional<Throwable> in the resolved map, absent meaning that
            // election succeeded.
            (IReadOnlyDictionary<TopicPartition, KafkaException?> partitions, Proto.KafkaError? error) =
                TranslateAdmin.Resolve(result.Partitions());
            if (error is not null)
            {
                return Task.FromResult(new Proto.VoidKeyedResponse { Error = error });
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> AlterPartitionReassignments(Proto.AlterPartitionReassignmentsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            return Task.FromResult(TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.PartitionKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.ListPartitionReassignmentsResponse> ListPartitionReassignments(Proto.ListPartitionReassignmentsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.ListPartitionReassignmentsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            ListPartitionReassignmentsResult result = admin.ListPartitionReassignments(
                TranslateAdmin.OptionalPartitions(request.Partitions),
                new ListPartitionReassignmentsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments =
                result.Reassignments().Get();
            Proto.ListPartitionReassignmentsResponse response = new Proto.ListPartitionReassignmentsResponse();
            foreach (KeyValuePair<TopicPartition, PartitionReassignment> pair in reassignments)
            {
                response.Reassignments.Add(new Proto.OngoingPartitionReassignment
                {
                    Partition = Translate.TpToProto(pair.Key),
                    Reassignment = TranslateAdmin.ReassignmentToProto(pair.Value),
                });
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.ListPartitionReassignmentsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.ListOffsetsResponse> ListOffsets(Proto.ListOffsetsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.ListOffsetsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            // Inside the try: C# reads an unset submessage as null (Python reads a default
            // instance), so a malformed request must become the top-level error, not a gRPC status.
            Dictionary<TopicPartition, OffsetSpec>? specs =
                TranslateAdmin.OffsetSpecs(request.Specs, out string? invalid);
            if (specs is null)
            {
                return Task.FromResult(new Proto.ListOffsetsResponse { Error = TranslateAdmin.RequestError(invalid!) });
            }

            ListOffsetsResult result = admin.ListOffsets(
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
                    TranslateAdmin.Resolve(result.PartitionResult(partition));
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.ListOffsetsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeProducersResponse> DescribeProducers(Proto.DescribeProducersRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeProducersResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            DescribeProducersResult result = admin.DescribeProducers(partitions, options);

            Proto.DescribeProducersResponse response = new Proto.DescribeProducersResponse();
            foreach (TopicPartition partition in partitions)
            {
                (DescribeProducersResult.PartitionProducerState state, Proto.KafkaError? error) =
                    TranslateAdmin.Resolve(result.PartitionResult(partition));
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeProducersResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeTransactionsResponse> DescribeTransactions(Proto.DescribeTransactionsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeTransactionsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
                    TranslateAdmin.Resolve(result.Description(id));
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeTransactionsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.StatusResponse> AbortTransaction(Proto.AbortTransactionRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            // Inside the try, for the same reason as ListOffsets above.
            AbortTransactionSpec? spec = TranslateAdmin.AbortSpec(request, out string? invalid);
            if (spec is null)
            {
                return Task.FromResult(new Proto.StatusResponse { Error = TranslateAdmin.RequestError(invalid!) });
            }

            admin.AbortTransaction(
                spec,
                new AbortTransactionOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                }).All().Get();

            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.StatusResponse> ForceTerminateTransaction(Proto.ForceTerminateTransactionRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            admin.ForceTerminateTransaction(
                request.TransactionalId,
                new TerminateTransactionOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                }).Result().Get();

            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.StatusResponse> UnregisterController(Proto.UnregisterControllerRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            admin.UnregisterController(
                request.ControllerId,
                new UnregisterControllerOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                }).All().Get();

            return Task.FromResult(new Proto.StatusResponse());
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.ListTransactionsResponse> ListTransactions(Proto.ListTransactionsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.ListTransactionsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            ListTransactionsResult result = admin.ListTransactions(new ListTransactionsOptions
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
            (IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>> brokers,
                Proto.KafkaError? error) =
                TranslateAdmin.Resolve(result.ByBrokerId());
            if (error is not null)
            {
                return Task.FromResult(new Proto.ListTransactionsResponse { Error = error });
            }

            Proto.ListTransactionsResponse response = new Proto.ListTransactionsResponse();
            foreach (KeyValuePair<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>> broker in brokers)
            {
                (IReadOnlyCollection<TransactionListing> listings, Proto.KafkaError? brokerError) =
                    TranslateAdmin.Resolve(broker.Value);
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.ListTransactionsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.FenceProducersResponse> FenceProducers(Proto.FenceProducersRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.FenceProducersResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
                (long producerId, Proto.KafkaError? idError) =
                    TranslateAdmin.Resolve(result.ProducerId(id));
                (short epoch, Proto.KafkaError? epochError) =
                    TranslateAdmin.Resolve(result.EpochId(id));
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.FenceProducersResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.ListGroupsResponse> ListGroups(Proto.ListGroupsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.ListGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            ListGroupsResult result = admin.ListGroups(new ListGroupsOptions
            {
                TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                GroupStates = TranslateAdmin.GroupStates(request.GroupStates),
                ProtocolTypes = new List<string>(request.ProtocolTypes),
                Types = TranslateAdmin.GroupTypes(request.Types_),
            });

            // valid() and errors() are independent lists of unrelated length: never zip them.
            IReadOnlyCollection<GroupListing> valid = result.Valid().Get();
            IReadOnlyCollection<KafkaException> errors = result.Errors().Get();

            Proto.ListGroupsResponse response = new Proto.ListGroupsResponse();
            foreach (GroupListing listing in valid)
            {
                response.Valid.Add(TranslateAdmin.GroupListingToProto(listing));
            }

            foreach (KafkaException error in errors)
            {
                response.ListingErrors.Add(Translate.ToProto(error));
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.ListGroupsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeConsumerGroupsResponse> DescribeConsumerGroups(Proto.DescribeConsumerGroupsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeConsumerGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
            foreach (KeyValuePair<string, KafkaFuture<ConsumerGroupDescription>> pair in result.DescribedGroups)
            {
                (ConsumerGroupDescription description, Proto.KafkaError? error) =
                    TranslateAdmin.Resolve(pair.Value);
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeConsumerGroupsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeClassicGroupsResponse> DescribeClassicGroups(Proto.DescribeClassicGroupsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeClassicGroupsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
            foreach (KeyValuePair<string, KafkaFuture<ClassicGroupDescription>> pair in result.DescribedGroups)
            {
                (ClassicGroupDescription description, Proto.KafkaError? error) =
                    TranslateAdmin.Resolve(pair.Value);
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeClassicGroupsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.ListConsumerGroupOffsetsResponse> ListConsumerGroupOffsets(Proto.ListConsumerGroupOffsetsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.ListConsumerGroupOffsetsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            // Inside the try, for the same reason as ListOffsets above.
            Dictionary<string, ListConsumerGroupOffsetsSpec>? specs =
                TranslateAdmin.GroupOffsetSpecs(request.GroupSpecs, out string? invalid);
            if (specs is null)
            {
                return Task.FromResult(new Proto.ListConsumerGroupOffsetsResponse { Error = TranslateAdmin.RequestError(invalid!) });
            }

            ListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
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
                    TranslateAdmin.Resolve(result.PartitionsToOffsetAndMetadata(groupId));
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.ListConsumerGroupOffsetsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> AlterConsumerGroupOffsets(Proto.AlterConsumerGroupOffsetsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            // No keyed future map: the requested keys drive the walk, as Java's
            // partitionResult(tp). An empty request gets an empty response, which drops a
            // whole-call error that All() carries, as Python's servicer does.
            Dictionary<TopicPartition, KafkaFuture> futures = new Dictionary<TopicPartition, KafkaFuture>();
            foreach (TopicPartition partition in offsets.Keys)
            {
                futures[partition] = result.PartitionResult(partition);
            }

            return Task.FromResult(TranslateAdmin.VoidResponse(futures, TranslateAdmin.PartitionKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> DeleteConsumerGroupOffsets(Proto.DeleteConsumerGroupOffsetsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            // Same shape as AlterConsumerGroupOffsets, the empty-request case included.
            Dictionary<TopicPartition, KafkaFuture> futures = new Dictionary<TopicPartition, KafkaFuture>();
            foreach (TopicPartition partition in partitions)
            {
                futures[partition] = result.PartitionResult(partition);
            }

            return Task.FromResult(TranslateAdmin.VoidResponse(futures, TranslateAdmin.PartitionKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> DeleteConsumerGroups(Proto.DeleteConsumerGroupsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            DeleteConsumerGroupsResult result = admin.DeleteConsumerGroups(
                TranslateAdmin.DistinctKeys(request.GroupIds),
                new DeleteConsumerGroupsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            return Task.FromResult(TranslateAdmin.VoidResponse(result.DeletedGroups, TranslateAdmin.NameKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> RemoveMembersFromConsumerGroup(Proto.RemoveMembersFromConsumerGroupRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            RemoveMembersFromConsumerGroupResult result =
                admin.RemoveMembersFromConsumerGroup(request.GroupId, options);

            if (options.RemoveAll)
            {
                // In removeAll mode Java's memberResult refuses, so All() is the only observable;
                // waiting on it runs the removal to completion before responding.
                Proto.KafkaError? error = TranslateAdmin.ResolveVoid(result.All());
                return Task.FromResult(error is null
                    ? new Proto.VoidKeyedResponse()
                    : new Proto.VoidKeyedResponse { Error = error });
            }

            Dictionary<MemberToRemove, KafkaFuture> futures = new Dictionary<MemberToRemove, KafkaFuture>();
            foreach (MemberToRemove member in members!)
            {
                futures[member] = result.MemberResult(member);
            }

            return Task.FromResult(TranslateAdmin
                .VoidResponse(futures, member => TranslateAdmin.NameKey(member.GroupInstanceId)));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> CreateAcls(Proto.CreateAclsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            CreateAclsResult result = admin.CreateAcls(
                TranslateAdmin.AclBindings(request.Acls),
                new CreateAclsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            return Task.FromResult(TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.AclBindingKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeAclsResponse> DescribeAcls(Proto.DescribeAclsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeAclsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            if (request.Filter is null)
            {
                return Task.FromResult(new Proto.DescribeAclsResponse
                {
                    Error = TranslateAdmin.RequestError("describe_acls requires a filter"),
                });
            }

            DescribeAclsResult result = admin.DescribeAcls(
                TranslateAdmin.AclFilter(request.Filter),
                new DescribeAclsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DescribeAclsResponse response = new Proto.DescribeAclsResponse();
            foreach (AclBinding binding in result.Values().Get())
            {
                response.Acls.Add(TranslateAdmin.AclBindingToProto(binding));
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeAclsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DeleteAclsResponse> DeleteAcls(Proto.DeleteAclsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DeleteAclsResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
            foreach (KeyValuePair<AclBindingFilter, KafkaFuture<DeleteAclsResult.FilterResults>> pair in result.Values)
            {
                (DeleteAclsResult.FilterResults results, Proto.KafkaError? error) =
                    TranslateAdmin.Resolve(pair.Value);
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DeleteAclsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeClientQuotasResponse> DescribeClientQuotas(Proto.DescribeClientQuotasRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeClientQuotasResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            ClientQuotaFilter? filter = TranslateAdmin.QuotaFilter(request, out string? invalid);
            if (filter is null)
            {
                return Task.FromResult(new Proto.DescribeClientQuotasResponse
                {
                    Error = TranslateAdmin.RequestError(invalid!),
                });
            }

            DescribeClientQuotasResult result = admin.DescribeClientQuotas(
                filter,
                new DescribeClientQuotasOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            Proto.DescribeClientQuotasResponse response = new Proto.DescribeClientQuotasResponse();
            IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
                result.Entities().Get();
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeClientQuotasResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> AlterClientQuotas(Proto.AlterClientQuotasRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            return Task.FromResult(TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.QuotaEntityKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeUserScramCredentialsResponse> DescribeUserScramCredentials(Proto.DescribeUserScramCredentialsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeUserScramCredentialsResponse
            {
                Error = Translate.UnknownAdmin(request.AdminId),
            });
        }

        try
        {
            DescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials(
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
                all = result.All().Get();
            }
            catch (Exception)
            {
                Proto.DescribeUserScramCredentialsResponse fallback =
                    new Proto.DescribeUserScramCredentialsResponse();
                foreach (string user in result.Users().Get())
                {
                    fallback.Entries.Add(ScramCredentialsEntry(result, user));
                }

                if (fallback.Entries.Count == 0)
                {
                    throw;
                }

                return Task.FromResult(fallback);
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

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeUserScramCredentialsResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> AlterUserScramCredentials(Proto.AlterUserScramCredentialsRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            AlterUserScramCredentialsResult result = admin.AlterUserScramCredentials(
                TranslateAdmin.ScramAlterations(request.Alterations),
                new AlterUserScramCredentialsOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                });

            return Task.FromResult(TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.NameKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.CreateDelegationTokenResponse> CreateDelegationToken(Proto.CreateDelegationTokenRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.CreateDelegationTokenResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            CreateDelegationTokenResult result = admin.CreateDelegationToken(options);
            DelegationToken token = result.DelegationToken().Get();
            return Task.FromResult(new Proto.CreateDelegationTokenResponse
            {
                Token = TranslateAdmin.DelegationTokenToProto(token),
            });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.CreateDelegationTokenResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DelegationTokenExpiryResponse> RenewDelegationToken(Proto.RenewDelegationTokenRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DelegationTokenExpiryResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            return Task.FromResult(new Proto.DelegationTokenExpiryResponse
            {
                ExpiryTimestampMs = result.ExpiryTimestamp().Get(),
            });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DelegationTokenExpiryResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DelegationTokenExpiryResponse> ExpireDelegationToken(Proto.ExpireDelegationTokenRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DelegationTokenExpiryResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            return Task.FromResult(new Proto.DelegationTokenExpiryResponse
            {
                ExpiryTimestampMs = result.ExpiryTimestamp().Get(),
            });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DelegationTokenExpiryResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeDelegationTokenResponse> DescribeDelegationToken(Proto.DescribeDelegationTokenRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeDelegationTokenResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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
            foreach (DelegationToken token in result.DelegationTokens().Get())
            {
                response.Tokens.Add(TranslateAdmin.DelegationTokenToProto(token));
            }

            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeDelegationTokenResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.DescribeFeaturesResponse> DescribeFeatures(Proto.DescribeFeaturesRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.DescribeFeaturesResponse { Error = Translate.UnknownAdmin(request.AdminId) });
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

            DescribeFeaturesResult result = admin.DescribeFeatures(options);

            FeatureMetadata metadata = result.FeatureMetadata().Get();
            return Task.FromResult(new Proto.DescribeFeaturesResponse
            {
                Metadata = TranslateAdmin.FeatureMetadataToProto(metadata),
            });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.DescribeFeaturesResponse { Error = Translate.ToProto(ex) });
        }
    }

    public override Task<Proto.VoidKeyedResponse> UpdateFeatures(Proto.UpdateFeaturesRequest request, ServerCallContext context)
    {
        IAdmin? admin = Get(request.AdminId);
        if (admin is null)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.UnknownAdmin(request.AdminId) });
        }

        try
        {
            Dictionary<string, FeatureUpdate>? updates =
                TranslateAdmin.FeatureUpdates(request.FeatureUpdates, out string? invalid);
            if (updates is null)
            {
                return Task.FromResult(new Proto.VoidKeyedResponse { Error = TranslateAdmin.RequestError(invalid!) });
            }

            // An empty map is forwarded: a real client throws (the top-level error) and a mock
            // returns an empty result, both as in Java.
            UpdateFeaturesResult result = admin.UpdateFeatures(
                updates,
                new UpdateFeaturesOptions
                {
                    TimeoutMs = TranslateAdmin.Timeout(request.HasTimeoutMs, request.TimeoutMs),
                    ValidateOnly = request.ValidateOnly,
                });

            return Task.FromResult(TranslateAdmin.VoidResponse(result.Values, TranslateAdmin.NameKey));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new Proto.VoidKeyedResponse { Error = Translate.ToProto(ex) });
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

    private static Proto.DescribeUserScramCredentialsEntry ScramCredentialsEntry(
        DescribeUserScramCredentialsResult result, string user)
    {
        Proto.DescribeUserScramCredentialsEntry entry =
            new Proto.DescribeUserScramCredentialsEntry { Key = TranslateAdmin.NameKey(user) };
        try
        {
            entry.Value = TranslateAdmin.ScramDescriptionToProto(result.Description(user).Get());
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

    private static Proto.DescribeTopicsEntry DescribeTopicsEntry(
        Proto.ResultKey key, KafkaFuture<TopicDescription> future)
    {
        (TopicDescription description, Proto.KafkaError? error) =
            TranslateAdmin.Resolve(future);
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
    private static Proto.TopicMetadataAndConfig MetadataAndConfig(CreateTopicsResult result, string topic)
    {
        try
        {
            Proto.TopicMetadata metadata = new Proto.TopicMetadata
            {
                TopicId = result.TopicId(topic).Get().ToString(),
                NumPartitions = result.NumPartitions(topic).Get(),
                ReplicationFactor = result.ReplicationFactor(topic).Get(),
            };
            Config config = result.Config(topic).Get();
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

    private IAdmin? Get(ulong adminId) =>
        _admins.TryGetValue(adminId, out IAdmin? admin) ? admin : null;
}
