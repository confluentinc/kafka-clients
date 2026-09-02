#!/usr/bin/env python3
# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Asyncio-native gRPC server exposing the *async* bindings/python client
(AsyncKafkaProducer / AsyncKafkaConsumer / AsyncAdminClient) over the same
ProducerService / ConsumerService / AdminService protos as grpc_server.py.

This is the async twin of grpc_server.py: the Rust multilanguage integration
tests run each test body against a `python_async` backend that tunnels every
Producer/Consumer/Admin call to this server, which drives the asyncio-native
Python client. It exercises the async API through the full integration matrix
against a real broker (the sync server covers the sync client).

Uses grpc.aio (asyncio server) rather than a thread pool: the async client's
completions already marshal onto the event loop (loop.call_soon_threadsafe from
the C dispatcher thread), so awaiting the coroutines is the natural fit. All RPC
handlers run as tasks on a single event loop, so the producer/consumer id maps
need no lock — id allocation and dict pops happen without an intervening await.

The server listens on 0.0.0.0:50051 (the fixed internal port the Docker image
exposes; the test pool maps it to a random host port via testcontainers) and
prints "listening ..." to stderr, which the Rust BackendPool waits for.
"""

import asyncio
import logging
import os
import sys

import grpc

# Ensure the Python wrapper module is importable regardless of CWD.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import producer as kp  # noqa: E402  (AsyncKafkaProducer / AsyncMockProducer / KafkaError)
import consumer as kc  # noqa: E402  (AsyncKafkaConsumer / AsyncMockConsumer / TopicPartition / ...)
import admin as ka  # noqa: E402  (AsyncAdminClient / AsyncMockAdminClient / ...)
import producer_service_pb2 as pb  # noqa: E402  (generated)
import producer_service_pb2_grpc as pb_grpc  # noqa: E402  (generated)
import consumer_service_pb2 as cpb  # noqa: E402  (generated)
import consumer_service_pb2_grpc as cpb_grpc  # noqa: E402  (generated)
import admin_service_pb2 as apb  # noqa: E402  (generated)
import admin_service_pb2_grpc as apb_grpc  # noqa: E402  (generated)

# Proto<->Python translation helpers + variant constants shared with the sync
# server (see grpc_translate.py); client-agnostic, so reused verbatim.
from grpc_translate import (  # noqa: E402
    ILLEGAL_STATE,
    TIMEOUT,
    CallbackLog,
    LoggingRebalanceListener,
    _admin_abort_transaction_spec,
    _admin_acl_bindings,
    _admin_acl_filter,
    _admin_acl_filters,
    _admin_alter_client_quotas_response,
    _admin_alter_configs,
    _admin_close_timeout,
    _admin_cluster_description_response,
    _admin_config_resource_key,
    _admin_config_resources,
    _admin_constructor_error,
    _admin_create_acls_response,
    _admin_create_delegation_token_response,
    _admin_create_topics_response,
    _admin_delete_acls_response,
    _admin_delete_records_response,
    _admin_describe_acls_response,
    _admin_describe_classic_groups_response,
    _admin_describe_client_quotas_response,
    _admin_describe_configs_response,
    _admin_describe_consumer_groups_response,
    _admin_describe_delegation_token_response,
    _admin_describe_features_response,
    _admin_describe_log_dirs_response,
    _admin_describe_producers_response,
    _admin_describe_replica_log_dirs_response,
    _admin_describe_topics_response,
    _admin_describe_transactions_response,
    _admin_describe_user_scram_credentials_response,
    _admin_feature_updates,
    _admin_fence_producers_response,
    _admin_group_offset_commits,
    _admin_group_offset_specs,
    _admin_list_client_metrics_resources_response,
    _admin_list_config_resources_response,
    _admin_list_consumer_group_offsets_response,
    _admin_list_consumer_groups_response,
    _admin_list_groups_response,
    _admin_list_offsets_response,
    _admin_list_partition_reassignments_response,
    _admin_list_topics_response,
    _admin_list_transactions_response,
    _admin_members_to_remove,
    _admin_name_key,
    _admin_new_partitions,
    _admin_new_topics,
    _admin_offset_specs,
    _admin_optional_partitions,
    _admin_principals,
    _admin_quota_alterations,
    _admin_quota_filter,
    _admin_reassignments,
    _admin_records_to_delete,
    _admin_replica_key,
    _admin_replica_log_dir_assignments,
    _admin_replicas,
    _admin_retry_on_quota,
    _admin_scram_alterations,
    _admin_selects_mock,
    _admin_timeout,
    _admin_token_owners,
    _admin_topic_id_key,
    _admin_tp_tuple_key,
    _admin_transaction_id_pattern,
    _admin_transaction_states,
    _admin_void_response,
    _kafka_error_to_proto,
    _metric_to_proto,
    _oam_to_proto,
    _partition_info_to_proto,
    _proto_offsets_to_dict,
    _proto_to_producer_record,
    _record_metadata_to_proto,
    _record_to_proto,
    _tp,
    _tp_to_proto,
    make_logging_commit_callback,
    make_logging_delivery_callback,
)

LOG = logging.getLogger("grpc_server_async")


class ProducerService(pb_grpc.ProducerServiceServicer):
    """Async twin of grpc_server.ProducerService, driving AsyncKafkaProducer.

    The producer id map needs no lock: all handlers run on one event loop and id
    allocation happens without an intervening await."""

    def __init__(self):
        self._producers = {}
        self._next_id = 1
        # AsyncProducer invokes on_delivery on the loop (inside its completion
        # drain), so this log is in fact only touched from the loop; CallbackLog
        # locks anyway, which is what the consumer service genuinely needs.
        self._callback_log = CallbackLog()

    def _take_producer(self, producer_id):
        return self._producers.get(producer_id)

    async def CreateProducer(self, request, context):
        config = dict(request.config)
        try:
            # Empty config selects AsyncMockProducer for client-side smoke
            # testing — useful when developing without a real broker.
            if not config or all(not v for v in config.values()):
                producer = kp.AsyncMockProducer(auto_complete=True)
            else:
                producer = kp.AsyncKafkaProducer(config)
        except Exception as e:  # noqa: BLE001
            LOG.exception("CreateProducer failed")
            return pb.CreateProducerResponse(producer_id=0, error=_kafka_error_to_proto(e))

        producer_id = self._next_id
        self._next_id += 1
        self._producers[producer_id] = producer
        LOG.info("created async producer %d", producer_id)
        return pb.CreateProducerResponse(producer_id=producer_id)

    async def Send(self, request, context):
        producer = self._take_producer(request.producer_id)
        if producer is None:
            return pb.SendResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown producer_id {request.producer_id}",
                is_retriable=False, is_fatal=True))
        try:
            record = _proto_to_producer_record(request.record)
        except Exception as e:  # noqa: BLE001
            LOG.exception("invalid record")
            return pb.SendResponse(error=_kafka_error_to_proto(e))

        # with_callback => register a real on_delivery through producer.py so the
        # Rust harness can read back (via GetCallbackLog) what the binding's own
        # callback saw. On AsyncProducer it fires on the event loop.
        on_delivery = None
        if request.with_callback:
            on_delivery = make_logging_delivery_callback(self._callback_log, request.producer_id)
        # AsyncProducer.send is a coroutine that returns an asyncio.Future.
        try:
            future = await producer.send(record, on_delivery=on_delivery)
        except kp.KafkaError as e:
            return pb.SendResponse(error=_kafka_error_to_proto(e))
        except Exception as e:  # noqa: BLE001
            LOG.exception("send raised")
            return pb.SendResponse(error=_kafka_error_to_proto(e))

        # Await the record future on the event loop; completions hop onto the
        # loop from the C dispatcher thread. Bounded like the sync server's 120s.
        try:
            metadata = await asyncio.wait_for(future, timeout=120)
        except kp.KafkaError as e:
            return pb.SendResponse(error=_kafka_error_to_proto(e))
        except asyncio.TimeoutError:
            return pb.SendResponse(error=pb.KafkaError(
                variant=TIMEOUT, code=7,
                message="python async server: producer future timed out after 120s",
                is_retriable=True, is_fatal=False))
        except Exception as e:  # noqa: BLE001
            LOG.exception("awaiting future raised")
            return pb.SendResponse(error=_kafka_error_to_proto(e))
        return pb.SendResponse(metadata=_record_metadata_to_proto(metadata))

    async def Flush(self, request, context):
        producer = self._take_producer(request.producer_id)
        if producer is None:
            return pb.StatusResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown producer_id {request.producer_id}",
                is_retriable=False, is_fatal=True))
        try:
            await producer.flush()
        except kp.KafkaError as e:
            return pb.StatusResponse(error=_kafka_error_to_proto(e))
        return pb.StatusResponse()

    async def PartitionsFor(self, request, context):
        producer = self._take_producer(request.producer_id)
        if producer is None:
            return pb.PartitionsForResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown producer_id {request.producer_id}",
                is_retriable=False, is_fatal=True))
        try:
            infos = await producer.partitions_for(request.topic)
        except kp.KafkaError as e:
            return pb.PartitionsForResponse(error=_kafka_error_to_proto(e))
        return pb.PartitionsForResponse(partitions=[_partition_info_to_proto(i) for i in infos])

    async def Metrics(self, request, context):
        producer = self._take_producer(request.producer_id)
        if producer is None:
            return pb.MetricsResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown producer_id {request.producer_id}",
                is_retriable=False, is_fatal=True))
        try:
            # metrics() is sync on AsyncProducer (Java metrics() does not block).
            snapshot = producer.metrics()
        except kp.KafkaError as e:
            return pb.MetricsResponse(error=_kafka_error_to_proto(e))
        return pb.MetricsResponse(metrics=pb.MetricList(
            metrics=[_metric_to_proto(m) for m in snapshot]))

    async def Close(self, request, context):
        producer = self._producers.pop(request.producer_id, None)
        if producer is None:
            # Close is idempotent — silent success on unknown id mirrors
            # the Java client's behavior.
            return pb.StatusResponse()
        try:
            await producer.close()
        except kp.KafkaError as e:
            return pb.StatusResponse(error=_kafka_error_to_proto(e))
        return pb.StatusResponse()

    async def CloseTimeout(self, request, context):
        # AsyncProducer.close() doesn't take a timeout. Best-effort mapping:
        # ignore the timeout and call close() unconditionally.
        return await self.Close(pb.CloseRequest(producer_id=request.producer_id), context)

    async def GetCallbackLog(self, request, context):
        # Readable after Close on purpose — close() flushes, so the delivery
        # entries it drives land last.
        return self._callback_log.response(request.producer_id)


class ConsumerService(cpb_grpc.ConsumerServiceServicer):
    """Async twin of grpc_server.ConsumerService, driving AsyncKafkaConsumer.

    Ops that block in the Rust consumer are coroutines and are awaited (seek
    included: it awaits the background task, which may run a rebalance listener);
    the non-blocking state reads (assignment/subscription/paused/wakeup) live on
    the shared _ConsumerBase and are sync — called directly, never awaited."""

    def __init__(self):
        self._consumers = {}
        self._next_id = 1
        # This one genuinely needs CallbackLog's lock: rebalance-listener and
        # commit callbacks fire on the Rust dispatcher thread (the listener
        # methods are plain, so they run there directly, never on the loop),
        # while GetCallbackLog is served on the loop.
        self._callback_log = CallbackLog()

    def _get(self, consumer_id):
        return self._consumers.get(consumer_id)

    def _status_err(self, e):
        return pb.StatusResponse(error=_kafka_error_to_proto(e))

    def _unknown_consumer(self, consumer_id):
        return pb.KafkaError(
            variant=ILLEGAL_STATE, code=-1,
            message=f"unknown consumer_id {consumer_id}", is_retriable=False, is_fatal=True)

    async def CreateConsumer(self, request, context):
        config = dict(request.config)
        try:
            if not config or all(not v for v in config.values()):
                consumer = kc.AsyncMockConsumer("earliest")
            else:
                consumer = kc.AsyncKafkaConsumer(config)
        except Exception as e:  # noqa: BLE001
            LOG.exception("CreateConsumer failed")
            return cpb.CreateConsumerResponse(consumer_id=0, error=_kafka_error_to_proto(e))
        consumer_id = self._next_id
        self._next_id += 1
        self._consumers[consumer_id] = consumer
        LOG.info("created async consumer %d", consumer_id)
        return cpb.CreateConsumerResponse(consumer_id=consumer_id)

    async def _run_status(self, consumer_id, coro_fn):
        """Run an async consumer op returning StatusResponse. `coro_fn` returns
        the coroutine to await."""
        consumer = self._get(consumer_id)
        if consumer is None:
            return pb.StatusResponse(error=self._unknown_consumer(consumer_id))
        try:
            await coro_fn(consumer)
            return pb.StatusResponse()
        except kc.KafkaError as e:
            return self._status_err(e)
        except Exception as e:  # noqa: BLE001
            LOG.exception("consumer op raised")
            return self._status_err(e)

    async def Subscribe(self, request, context):
        # with_listener => a real ConsumerRebalanceListener whose invocations
        # land in the callback log. LoggingRebalanceListener's methods are plain
        # functions on purpose: a *coroutine* listener method must not await
        # AsyncConsumer FFI ops (the dispatcher thread is parked in
        # run_coroutine_threadsafe(...).result() waiting for it — deadlock).
        listener = None
        if request.with_listener:
            listener = LoggingRebalanceListener(self._callback_log, request.consumer_id)
        return await self._run_status(
            request.consumer_id,
            lambda c: c.subscribe(list(request.topics), listener=listener))

    async def Unsubscribe(self, request, context):
        return await self._run_status(request.consumer_id, lambda c: c.unsubscribe())

    async def Assign(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return await self._run_status(request.consumer_id, lambda c: c.assign(parts))

    async def Poll(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.PollResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            records = await consumer.poll(request.timeout_ms / 1000.0)
        except kc.KafkaError as e:
            return cpb.PollResponse(error=_kafka_error_to_proto(e))
        except Exception as e:  # noqa: BLE001
            LOG.exception("poll raised")
            return cpb.PollResponse(error=_kafka_error_to_proto(e))
        proto_records = [_record_to_proto(r) for r in records]
        return cpb.PollResponse(records=cpb.ConsumerRecordList(records=proto_records))

    async def CommitSync(self, request, context):
        async def do(c):
            offsets = _proto_offsets_to_dict(request.offsets)
            if offsets:
                await c.commit(offsets)
            else:
                await c.commit()
        return await self._run_status(request.consumer_id, do)

    async def CommitAsync(self, request, context):
        # commit_async is a sync local op on the shared _ConsumerBase in both
        # clients (it only *initiates* the commit), so it is called directly
        # rather than awaited. The callback fires on a later poll/commit/close.
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return pb.StatusResponse(error=self._unknown_consumer(request.consumer_id))
        callback = None
        if request.with_callback:
            callback = make_logging_commit_callback(self._callback_log, request.consumer_id)
        try:
            consumer.commit_async(_proto_offsets_to_dict(request.offsets) or None, callback=callback)
            return pb.StatusResponse()
        except kc.KafkaError as e:
            return self._status_err(e)
        except Exception as e:  # noqa: BLE001
            LOG.exception("commit_async raised")
            return self._status_err(e)

    async def Committed(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.CommittedResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            result = await consumer.committed([_tp(p) for p in request.partitions])
        except kc.KafkaError as e:
            return cpb.CommittedResponse(error=_kafka_error_to_proto(e))
        entries = [cpb.OffsetMapEntry(partition=_tp_to_proto(tp), offset=_oam_to_proto(oam))
                   for tp, oam in result.items()]
        return cpb.CommittedResponse(offsets=cpb.OffsetMap(entries=entries))

    async def Position(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.PositionResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            offset = await consumer.position(_tp(request.partition))
        except kc.KafkaError as e:
            return cpb.PositionResponse(error=_kafka_error_to_proto(e))
        return cpb.PositionResponse(offset=offset)

    async def Seek(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return pb.StatusResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            tp = _tp(request.partition)
            if request.HasField("metadata") or request.HasField("leader_epoch"):
                oam = kc.OffsetAndMetadata(
                    request.offset,
                    request.metadata if request.HasField("metadata") else "",
                    request.leader_epoch if request.HasField("leader_epoch") else None)
                await consumer.seek(tp, oam)
            else:
                await consumer.seek(tp, request.offset)
            return pb.StatusResponse()
        except kc.KafkaError as e:
            return self._status_err(e)
        except Exception as e:  # noqa: BLE001
            LOG.exception("seek raised")
            return self._status_err(e)

    async def SeekToBeginning(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return await self._run_status(request.consumer_id, lambda c: c.seek_to_beginning(parts))

    async def SeekToEnd(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return await self._run_status(request.consumer_id, lambda c: c.seek_to_end(parts))

    async def Pause(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return await self._run_status(request.consumer_id, lambda c: c.pause(parts))

    async def Resume(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return await self._run_status(request.consumer_id, lambda c: c.resume(parts))

    async def _long_offsets(self, request, end):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.LongOffsetsResponse(error=self._unknown_consumer(request.consumer_id))
        parts = [_tp(p) for p in request.partitions]
        try:
            result = await (consumer.end_offsets(parts) if end else consumer.beginning_offsets(parts))
        except kc.KafkaError as e:
            return cpb.LongOffsetsResponse(error=_kafka_error_to_proto(e))
        entries = [cpb.LongOffsetMapEntry(partition=_tp_to_proto(tp), offset=off) for tp, off in result.items()]
        return cpb.LongOffsetsResponse(offsets=cpb.LongOffsetMap(entries=entries))

    async def BeginningOffsets(self, request, context):
        return await self._long_offsets(request, end=False)

    async def EndOffsets(self, request, context):
        return await self._long_offsets(request, end=True)

    async def OffsetsForTimes(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.OffsetAndTimestampResponse(error=self._unknown_consumer(request.consumer_id))
        spec = {_tp(e.partition): e.timestamp for e in request.timestamps}
        try:
            result = await consumer.offsets_for_times(spec)
        except kc.KafkaError as e:
            return cpb.OffsetAndTimestampResponse(error=_kafka_error_to_proto(e))
        entries = [
            cpb.OffsetAndTimestampMapEntry(
                partition=_tp_to_proto(tp),
                offset=cpb.OffsetAndTimestamp(
                    offset=oat.offset, timestamp=oat.timestamp,
                    leader_epoch=oat.leader_epoch if oat.leader_epoch is not None else None))
            for tp, oat in result.items()
        ]
        return cpb.OffsetAndTimestampResponse(offsets=cpb.OffsetAndTimestampMap(entries=entries))

    async def PartitionsFor(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return pb.PartitionsForResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            infos = await consumer.partitions_for(request.topic)
        except kc.KafkaError as e:
            return pb.PartitionsForResponse(error=_kafka_error_to_proto(e))
        return pb.PartitionsForResponse(partitions=[_partition_info_to_proto(i) for i in infos])

    async def ListTopics(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.ListTopicsResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            topics = await consumer.list_topics()
        except kc.KafkaError as e:
            return cpb.ListTopicsResponse(error=_kafka_error_to_proto(e))
        entries = [cpb.TopicPartitionInfoEntry(topic=t, partitions=[_partition_info_to_proto(i) for i in infos])
                   for t, infos in topics.items()]
        return cpb.ListTopicsResponse(topics=cpb.TopicListing(topics=entries))

    async def Assignment(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.TopicPartitionListResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            tps = consumer.assignment()
        except Exception as e:  # noqa: BLE001
            return cpb.TopicPartitionListResponse(error=_kafka_error_to_proto(e))
        return cpb.TopicPartitionListResponse(
            partitions=cpb.TopicPartitionList(partitions=[_tp_to_proto(tp) for tp in tps]))

    async def Subscription(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.SubscriptionResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            topics = consumer.subscription()
        except Exception as e:  # noqa: BLE001
            return cpb.SubscriptionResponse(error=_kafka_error_to_proto(e))
        return cpb.SubscriptionResponse(topics=cpb.StringList(values=list(topics)))

    async def Metrics(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return pb.MetricsResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            snapshot = consumer.metrics()
        except Exception as e:  # noqa: BLE001
            return pb.MetricsResponse(error=_kafka_error_to_proto(e))
        return pb.MetricsResponse(metrics=pb.MetricList(
            metrics=[_metric_to_proto(m) for m in snapshot]))

    async def Paused(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.TopicPartitionListResponse(error=self._unknown_consumer(request.consumer_id))
        try:
            tps = consumer.paused()
        except Exception as e:  # noqa: BLE001
            return cpb.TopicPartitionListResponse(error=_kafka_error_to_proto(e))
        return cpb.TopicPartitionListResponse(
            partitions=cpb.TopicPartitionList(partitions=[_tp_to_proto(tp) for tp in tps]))

    async def Wakeup(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is not None:
            consumer.wakeup()
        return pb.StatusResponse()

    async def Close(self, request, context):
        consumer = self._consumers.pop(request.consumer_id, None)
        if consumer is None:
            return pb.StatusResponse()
        try:
            await consumer.close()
        except kc.KafkaError as e:
            return self._status_err(e)
        return pb.StatusResponse()

    async def GetCallbackLog(self, request, context):
        # Readable after Close on purpose — close() drains pending commit
        # callbacks and fires on_partitions_lost.
        return self._callback_log.response(request.consumer_id)


# ---------------------------------------------------------------------------
# Admin service
# ---------------------------------------------------------------------------


class AdminService(apb_grpc.AdminServiceServicer):
    """Async twin of grpc_server.AdminService, driving AsyncAdminClient /
    AsyncMockAdminClient.

    The admin id map needs no lock: all handlers run on one event loop and id
    allocation happens without an intervening await."""

    def __init__(self):
        self._admins = {}
        self._next_id = 1

    def _get(self, admin_id):
        return self._admins.get(admin_id)

    async def CreateAdmin(self, request, context):
        config = dict(request.config)
        num_brokers = request.num_brokers if request.HasField("num_brokers") else 1
        try:
            # See CreateAdminRequest in admin_service.proto: this predicate is
            # the normative mock-selection rule for all three servers.
            if _admin_selects_mock(config):
                client = ka.AsyncMockAdminClient(num_brokers)
            else:
                client = ka.AsyncAdminClient(config)
        except Exception as e:  # noqa: BLE001
            LOG.exception("CreateAdmin failed")
            return apb.CreateAdminResponse(admin_id=0, error=_admin_constructor_error(e))
        admin_id = self._next_id
        self._next_id += 1
        self._admins[admin_id] = client
        LOG.info("created async admin %d", admin_id)
        return apb.CreateAdminResponse(admin_id=admin_id)

    # -- Topics & partitions (slice G1) --------------------------------------
    #
    # Same three steps as the sync twin, except the admin.py call is awaited on
    # the event loop rather than blocking a worker thread. A raised KafkaError is
    # a whole-call failure and goes in the response's top-level `error`, leaving
    # `entries` empty — per-key failures never raise, they arrive in the dict.

    def _unknown_admin(self, admin_id):
        return pb.KafkaError(
            variant=ILLEGAL_STATE, code=-1,
            message=f"unknown admin_id {admin_id}",
            is_retriable=False, is_fatal=True)

    async def CreateTopics(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.CreateTopicsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.create_topics(
                _admin_new_topics(request.topics),
                timeout=_admin_timeout(request),
                validate_only=request.validate_only,
                retry_on_quota_violation=_admin_retry_on_quota(request),
            )
            return _admin_create_topics_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("create_topics raised")
            return apb.CreateTopicsResponse(error=_kafka_error_to_proto(e))

    async def DeleteTopics(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        by_ids = request.WhichOneof("topics") == "topic_ids"
        names = list(request.topic_ids.values if by_ids else request.names.values)
        try:
            if by_ids:
                outcomes = await client.delete_topics_by_ids(
                    names, timeout=_admin_timeout(request),
                    retry_on_quota_violation=_admin_retry_on_quota(request))
            else:
                outcomes = await client.delete_topics(
                    names, timeout=_admin_timeout(request),
                    retry_on_quota_violation=_admin_retry_on_quota(request))
            key_fn = _admin_topic_id_key if by_ids else _admin_name_key
            return _admin_void_response(outcomes, key_fn)
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_topics raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def ListTopics(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.AdminListTopicsResponse(error=self._unknown_admin(request.admin_id))
        try:
            listings = await client.list_topics(
                timeout=_admin_timeout(request), list_internal=request.list_internal)
            return _admin_list_topics_response(listings)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_topics raised")
            return apb.AdminListTopicsResponse(error=_kafka_error_to_proto(e))

    async def DescribeTopics(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeTopicsResponse(error=self._unknown_admin(request.admin_id))
        by_ids = request.WhichOneof("topics") == "topic_ids"
        topics = list(request.topic_ids.values if by_ids else request.names.values)
        limit = (request.partition_size_limit_per_response
                 if request.HasField("partition_size_limit_per_response") else None)
        try:
            method = client.describe_topics_by_ids if by_ids else client.describe_topics
            outcomes = await method(
                topics,
                timeout=_admin_timeout(request),
                include_authorized_operations=request.include_authorized_operations,
                partition_size_limit=limit,
            )
            key_fn = _admin_topic_id_key if by_ids else _admin_name_key
            return _admin_describe_topics_response(outcomes, key_fn)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_topics raised")
            return apb.DescribeTopicsResponse(error=_kafka_error_to_proto(e))

    async def CreatePartitions(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.create_partitions(
                _admin_new_partitions(request.partitions),
                timeout=_admin_timeout(request),
                validate_only=request.validate_only,
                retry_on_quota_violation=_admin_retry_on_quota(request),
            )
            return _admin_void_response(outcomes, _admin_name_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("create_partitions raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def DeleteRecords(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DeleteRecordsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.delete_records(
                _admin_records_to_delete(request.records), timeout=_admin_timeout(request))
            return _admin_delete_records_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_records raised")
            return apb.DeleteRecordsResponse(error=_kafka_error_to_proto(e))

    # -- Cluster, configs & log dirs (slice G2) -------------------------------
    #
    # Same shape as the sync twin, awaited on the event loop. describe_cluster /
    # list_config_resources / list_client_metrics_resources have one Java future
    # each, so admin.py raises and the failure lands in the top-level error; the
    # per-key RPCs never raise for a single key.

    async def DescribeCluster(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeClusterResponse(error=self._unknown_admin(request.admin_id))
        try:
            description = await client.describe_cluster(
                timeout=_admin_timeout(request),
                include_authorized_operations=request.include_authorized_operations,
                include_fenced_brokers=request.include_fenced_brokers,
            )
            return _admin_cluster_description_response(description)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_cluster raised")
            return apb.DescribeClusterResponse(error=_kafka_error_to_proto(e))

    async def DescribeConfigs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeConfigsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.describe_configs(
                _admin_config_resources(request.resources),
                timeout=_admin_timeout(request),
                include_synonyms=request.include_synonyms,
                include_documentation=request.include_documentation,
            )
            return _admin_describe_configs_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_configs raised")
            return apb.DescribeConfigsResponse(error=_kafka_error_to_proto(e))

    async def IncrementalAlterConfigs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.incremental_alter_configs(
                _admin_alter_configs(request.configs),
                timeout=_admin_timeout(request),
                validate_only=request.validate_only,
            )
            return _admin_void_response(outcomes, _admin_config_resource_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("incremental_alter_configs raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def ListConfigResources(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListConfigResourcesResponse(error=self._unknown_admin(request.admin_id))
        try:
            # An empty repeated field is Java's empty Set: every supported type.
            resources = await client.list_config_resources(
                list(request.resource_types), timeout=_admin_timeout(request))
            return _admin_list_config_resources_response(resources)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_config_resources raised")
            return apb.ListConfigResourcesResponse(error=_kafka_error_to_proto(e))

    async def ListClientMetricsResources(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListClientMetricsResourcesResponse(
                error=self._unknown_admin(request.admin_id))
        try:
            resources = await client.list_client_metrics_resources(
                timeout=_admin_timeout(request))
            return _admin_list_client_metrics_resources_response(resources)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_client_metrics_resources raised")
            return apb.ListClientMetricsResourcesResponse(error=_kafka_error_to_proto(e))

    async def DescribeLogDirs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeLogDirsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.describe_log_dirs(
                list(request.brokers), timeout=_admin_timeout(request))
            return _admin_describe_log_dirs_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_log_dirs raised")
            return apb.DescribeLogDirsResponse(error=_kafka_error_to_proto(e))

    async def AlterReplicaLogDirs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.alter_replica_log_dirs(
                _admin_replica_log_dir_assignments(request.assignments),
                timeout=_admin_timeout(request))
            return _admin_void_response(outcomes, _admin_replica_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("alter_replica_log_dirs raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def DescribeReplicaLogDirs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeReplicaLogDirsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.describe_replica_log_dirs(
                _admin_replicas(request.replicas), timeout=_admin_timeout(request))
            return _admin_describe_replica_log_dirs_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_replica_log_dirs raised")
            return apb.DescribeReplicaLogDirsResponse(error=_kafka_error_to_proto(e))

    # -- Elections, reassignments & offsets (slice G3) -------------------------
    #
    # electLeaders and alterPartitionReassignments both answer with the shared
    # VoidKeyedResponse, but their two error levels do not mean the same thing.
    # alter_partition_reassignments has one Java future per partition, so a
    # single partition's failure arrives inside the dict as usual.
    # elect_leaders has *one* future for the whole map: admin.py raises when it
    # fails, which is the top-level error, and the per-partition value inside the
    # resolved dict is Java's Optional<Throwable> -- None meaning that partition's
    # election succeeded. list_partition_reassignments is whole-value (one
    # future, so a raise), list_offsets is ordinary per-key.

    async def ElectLeaders(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            # `partitions=None` is Java's null Set: elect for every partition.
            outcomes = await client.elect_leaders(
                request.election_type,
                _admin_optional_partitions(request),
                timeout=_admin_timeout(request))
            return _admin_void_response(outcomes, _admin_tp_tuple_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("elect_leaders raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def AlterPartitionReassignments(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        # Java's default is true, so an absent field is true.
        allow_rf_change = (request.allow_replication_factor_change
                           if request.HasField("allow_replication_factor_change") else True)
        try:
            outcomes = await client.alter_partition_reassignments(
                _admin_reassignments(request.reassignments),
                timeout=_admin_timeout(request),
                allow_replication_factor_change=allow_rf_change)
            return _admin_void_response(outcomes, _admin_tp_tuple_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("alter_partition_reassignments raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def ListPartitionReassignments(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListPartitionReassignmentsResponse(
                error=self._unknown_admin(request.admin_id))
        try:
            reassignments = await client.list_partition_reassignments(
                _admin_optional_partitions(request), timeout=_admin_timeout(request))
            return _admin_list_partition_reassignments_response(reassignments)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_partition_reassignments raised")
            return apb.ListPartitionReassignmentsResponse(error=_kafka_error_to_proto(e))

    async def ListOffsets(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListOffsetsResponse(error=self._unknown_admin(request.admin_id))
        try:
            # A malformed OffsetSpec raises AdminRequestError out of
            # _admin_offset_specs: a whole-call failure carrying the
            # ILLEGAL_ARGUMENT *variant*, which is what the C++ server stamps for
            # the same condition. Agreeing on the level is not enough -- `variant`
            # is the field the Rust client matches on.
            outcomes = await client.list_offsets(
                _admin_offset_specs(request.specs),
                timeout=_admin_timeout(request),
                isolation_level=request.isolation_level)
            return _admin_list_offsets_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_offsets raised")
            return apb.ListOffsetsResponse(error=_kafka_error_to_proto(e))

    # -- Groups & offsets (slice G4) ------------------------------------------
    #
    # Same three error levels as the sync twin, awaited on the event loop:
    #
    #   - list_groups / list_consumer_groups hand back a *pair* of lists, so a
    #     per-broker listing failure is inside the second one, unkeyed, and only a
    #     whole-call failure raises.
    #   - describe_consumer_groups / describe_classic_groups /
    #     list_consumer_group_offsets have one future per group, so a per-group
    #     failure arrives in the dict.
    #   - alter_consumer_group_offsets / delete_consumer_group_offsets /
    #     remove_members_from_consumer_group have ONE future over the whole map, so
    #     its failure raises and becomes the top-level error with `entries` empty --
    #     which with an empty input (no partitions, or removeAll) is the only
    #     observable. delete_consumer_groups is the one with genuine per-key
    #     futures.

    async def ListGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListGroupsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcome = await client.list_groups(
                group_states=list(request.group_states),
                protocol_types=list(request.protocol_types),
                types=list(request.types),
                timeout=_admin_timeout(request))
            return _admin_list_groups_response(outcome)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_groups raised")
            return apb.ListGroupsResponse(error=_kafka_error_to_proto(e))

    async def ListConsumerGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListConsumerGroupsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcome = await client.list_consumer_groups(
                group_states=list(request.group_states),
                types=list(request.types),
                timeout=_admin_timeout(request))
            return _admin_list_consumer_groups_response(outcome)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_consumer_groups raised")
            return apb.ListConsumerGroupsResponse(error=_kafka_error_to_proto(e))

    async def DescribeConsumerGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeConsumerGroupsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.describe_consumer_groups(
                list(request.group_ids), timeout=_admin_timeout(request),
                include_authorized_operations=request.include_authorized_operations)
            return _admin_describe_consumer_groups_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_consumer_groups raised")
            return apb.DescribeConsumerGroupsResponse(error=_kafka_error_to_proto(e))

    async def DescribeClassicGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeClassicGroupsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.describe_classic_groups(
                list(request.group_ids), timeout=_admin_timeout(request),
                include_authorized_operations=request.include_authorized_operations)
            return _admin_describe_classic_groups_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_classic_groups raised")
            return apb.DescribeClassicGroupsResponse(error=_kafka_error_to_proto(e))

    async def ListConsumerGroupOffsets(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListConsumerGroupOffsetsResponse(
                error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.list_consumer_group_offsets(
                _admin_group_offset_specs(request.group_specs),
                timeout=_admin_timeout(request),
                require_stable=request.require_stable)
            return _admin_list_consumer_group_offsets_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_consumer_group_offsets raised")
            return apb.ListConsumerGroupOffsetsResponse(error=_kafka_error_to_proto(e))

    async def AlterConsumerGroupOffsets(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.alter_consumer_group_offsets(
                request.group_id, _admin_group_offset_commits(request.offsets),
                timeout=_admin_timeout(request))
            return _admin_void_response(outcomes, _admin_tp_tuple_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("alter_consumer_group_offsets raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def DeleteConsumerGroupOffsets(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.delete_consumer_group_offsets(
                request.group_id,
                [(tp.topic, tp.partition) for tp in request.partitions],
                timeout=_admin_timeout(request))
            return _admin_void_response(outcomes, _admin_tp_tuple_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_consumer_group_offsets raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def DeleteConsumerGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.delete_consumer_groups(
                list(request.group_ids), timeout=_admin_timeout(request))
            return _admin_void_response(outcomes, _admin_name_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_consumer_groups raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def RemoveMembersFromConsumerGroup(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            # `members=None` is Java's no-argument options constructor
            # (removeAll); an empty *list* is the collection constructor, which
            # Java rejects. _admin_members_to_remove keeps the two apart.
            outcomes = await client.remove_members_from_consumer_group(
                request.group_id, _admin_members_to_remove(request),
                reason=request.reason if request.HasField("reason") else None,
                timeout=_admin_timeout(request))
            # Keyed by group.instance.id, which is a plain string.
            return _admin_void_response(outcomes, _admin_name_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("remove_members_from_consumer_group raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    # -- ACLs, quotas, SCRAM, delegation tokens & features (slice G5) ----------
    #
    # Five of the thirteen hold ONE future for the whole call in Java
    # (describe_acls, describe_client_quotas, create_delegation_token,
    # renew/expire_delegation_token, describe_delegation_token,
    # describe_features), so admin.py *raises* on failure and the error becomes
    # the response's top-level error — there is no per-key slot for it. The other
    # eight have genuine per-key futures, so a per-key failure arrives inside the
    # returned dict and only a submission failure raises.
    #
    # update_features is the one whose *submission* can fail on a well-formed
    # request: an empty map raises against a real client (Java's
    # IllegalArgumentException) and yields an empty result against a mock. Both
    # are faithful, and the raise lands in the top-level error.

    async def CreateAcls(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.create_acls(_admin_acl_bindings(request.acls),
                                                timeout=_admin_timeout(request))
            return _admin_create_acls_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("create_acls raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def DescribeAcls(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeAclsResponse(error=self._unknown_admin(request.admin_id))
        try:
            bindings = await client.describe_acls(_admin_acl_filter(request.filter),
                                                  timeout=_admin_timeout(request))
            return _admin_describe_acls_response(bindings)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_acls raised")
            return apb.DescribeAclsResponse(error=_kafka_error_to_proto(e))

    async def DeleteAcls(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DeleteAclsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.delete_acls(_admin_acl_filters(request.filters),
                                                timeout=_admin_timeout(request))
            return _admin_delete_acls_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_acls raised")
            return apb.DeleteAclsResponse(error=_kafka_error_to_proto(e))

    async def DescribeClientQuotas(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeClientQuotasResponse(error=self._unknown_admin(request.admin_id))
        try:
            entities = await client.describe_client_quotas(_admin_quota_filter(request),
                                                           timeout=_admin_timeout(request))
            return _admin_describe_client_quotas_response(entities)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_client_quotas raised")
            return apb.DescribeClientQuotasResponse(error=_kafka_error_to_proto(e))

    async def AlterClientQuotas(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.alter_client_quotas(_admin_quota_alterations(request.entries),
                                                        timeout=_admin_timeout(request),
                                                        validate_only=request.validate_only)
            return _admin_alter_client_quotas_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("alter_client_quotas raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def DescribeUserScramCredentials(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeUserScramCredentialsResponse(
                error=self._unknown_admin(request.admin_id))
        try:
            # An empty `users` is Java's no-argument overload: describe every
            # user. admin.py takes None for the same thing and [] is equivalent.
            outcomes = await client.describe_user_scram_credentials(
                                                                    list(request.users), timeout=_admin_timeout(request))
            return _admin_describe_user_scram_credentials_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_user_scram_credentials raised")
            return apb.DescribeUserScramCredentialsResponse(error=_kafka_error_to_proto(e))

    async def AlterUserScramCredentials(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.alter_user_scram_credentials(
                                                                 _admin_scram_alterations(request.alterations), timeout=_admin_timeout(request))
            return _admin_void_response(outcomes, _admin_name_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("alter_user_scram_credentials raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))

    async def CreateDelegationToken(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.CreateDelegationTokenResponse(error=self._unknown_admin(request.admin_id))
        try:
            # An absent owner leaves Java's field empty, making the requesting
            # principal the owner; both halves are absent together.
            owner = (_admin_principals([request.owner])[0] if request.HasField("owner") else None)
            token = await client.create_delegation_token(
                                                         renewers=_admin_principals(request.renewers),
                                                         owner=owner,
                                                         max_lifetime_ms=request.max_lifetime_ms,
                                                         timeout=_admin_timeout(request))
            return _admin_create_delegation_token_response(token)
        except Exception as e:  # noqa: BLE001
            LOG.exception("create_delegation_token raised")
            return apb.CreateDelegationTokenResponse(error=_kafka_error_to_proto(e))

    async def RenewDelegationToken(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DelegationTokenExpiryResponse(error=self._unknown_admin(request.admin_id))
        try:
            expiry = await client.renew_delegation_token(
                                                         request.hmac, renew_time_period_ms=request.renew_time_period_ms,
                                                         timeout=_admin_timeout(request))
            return apb.DelegationTokenExpiryResponse(expiry_timestamp_ms=expiry)
        except Exception as e:  # noqa: BLE001
            LOG.exception("renew_delegation_token raised")
            return apb.DelegationTokenExpiryResponse(error=_kafka_error_to_proto(e))

    async def ExpireDelegationToken(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DelegationTokenExpiryResponse(error=self._unknown_admin(request.admin_id))
        try:
            expiry = await client.expire_delegation_token(
                                                          request.hmac, expiry_time_period_ms=request.expiry_time_period_ms,
                                                          timeout=_admin_timeout(request))
            return apb.DelegationTokenExpiryResponse(expiry_timestamp_ms=expiry)
        except Exception as e:  # noqa: BLE001
            LOG.exception("expire_delegation_token raised")
            return apb.DelegationTokenExpiryResponse(error=_kafka_error_to_proto(e))

    async def DescribeDelegationToken(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeDelegationTokenResponse(error=self._unknown_admin(request.admin_id))
        try:
            # `_admin_token_owners` returns None only for an *absent* wrapper,
            # which is Java's unset filter; an empty list stays a list.
            tokens = await client.describe_delegation_token(owners=_admin_token_owners(request),
                                                            timeout=_admin_timeout(request))
            return _admin_describe_delegation_token_response(tokens)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_delegation_token raised")
            return apb.DescribeDelegationTokenResponse(error=_kafka_error_to_proto(e))

    async def DescribeFeatures(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeFeaturesResponse(error=self._unknown_admin(request.admin_id))
        try:
            # An absent node_id is Java's empty OptionalInt; node 0 is a legal
            # broker, so HasField is what carries the absence.
            node_id = request.node_id if request.HasField("node_id") else None
            metadata = await client.describe_features(node_id=node_id, timeout=_admin_timeout(request))
            return _admin_describe_features_response(metadata)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_features raised")
            return apb.DescribeFeaturesResponse(error=_kafka_error_to_proto(e))

    async def UpdateFeatures(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.update_features(_admin_feature_updates(request),
                                                    timeout=_admin_timeout(request),
                                                    validate_only=request.validate_only)
            return _admin_void_response(outcomes, _admin_name_key)
        except Exception as e:  # noqa: BLE001
            LOG.exception("update_features raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))


    # -- Producers & transactions (slice G6) ----------------------------------
    #
    # Four of the six are ordinary per-key results (describe_producers,
    # describe_transactions, fence_producers, list_transactions). The other two,
    # abort_transaction and force_terminate_transaction, answer with the shared
    # StatusResponse because Java's AbortTransactionResult / TerminateTransaction
    # Result carry no data and no reachable per-key granularity — admin.py
    # resolves both to None, so success is an absent error.
    #
    # list_transactions is keyed by *broker*: byBrokerId() is the only one of
    # Java's three views that keeps a per-broker error, so a partial listing
    # survives. Only a failure of the broker-discovery step raises here.

    async def DescribeProducers(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeProducersResponse(error=self._unknown_admin(request.admin_id))
        try:
            # An absent broker_id is Java's empty OptionalInt (query each
            # partition's leader); broker 0 is legal, so HasField carries it.
            broker_id = request.broker_id if request.HasField("broker_id") else None
            outcomes = await client.describe_producers(
                [(tp.topic, tp.partition) for tp in request.partitions],
                broker_id=broker_id,
                timeout=_admin_timeout(request))
            return _admin_describe_producers_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_producers raised")
            return apb.DescribeProducersResponse(error=_kafka_error_to_proto(e))

    async def DescribeTransactions(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeTransactionsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.describe_transactions(
                list(request.transactional_ids), timeout=_admin_timeout(request))
            return _admin_describe_transactions_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_transactions raised")
            return apb.DescribeTransactionsResponse(error=_kafka_error_to_proto(e))

    async def AbortTransaction(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return pb.StatusResponse(error=self._unknown_admin(request.admin_id))
        try:
            await client.abort_transaction(_admin_abort_transaction_spec(request),
                                           timeout=_admin_timeout(request))
            return pb.StatusResponse()
        except Exception as e:  # noqa: BLE001
            LOG.exception("abort_transaction raised")
            return pb.StatusResponse(error=_kafka_error_to_proto(e))

    async def ForceTerminateTransaction(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return pb.StatusResponse(error=self._unknown_admin(request.admin_id))
        try:
            await client.force_terminate_transaction(request.transactional_id,
                                                     timeout=_admin_timeout(request))
            return pb.StatusResponse()
        except Exception as e:  # noqa: BLE001
            LOG.exception("force_terminate_transaction raised")
            return pb.StatusResponse(error=_kafka_error_to_proto(e))

    async def ListTransactions(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListTransactionsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.list_transactions(
                states=_admin_transaction_states(request),
                producer_ids=list(request.producer_ids) or None,
                duration_ms=request.duration_ms,
                transactional_id_pattern=_admin_transaction_id_pattern(request),
                timeout=_admin_timeout(request))
            return _admin_list_transactions_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_transactions raised")
            return apb.ListTransactionsResponse(error=_kafka_error_to_proto(e))

    async def FenceProducers(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.FenceProducersResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.fence_producers(
                list(request.transactional_ids), timeout=_admin_timeout(request))
            return _admin_fence_producers_response(outcomes)
        except Exception as e:  # noqa: BLE001
            LOG.exception("fence_producers raised")
            return apb.FenceProducersResponse(error=_kafka_error_to_proto(e))

    async def Close(self, request, context):
        client = self._admins.pop(request.admin_id, None)
        if client is None:
            # Close is idempotent — silent success on unknown id.
            return pb.StatusResponse()
        # admin.py's close() takes seconds or a timedelta; absent means Java's
        # no-argument close(). Shared helper so both servers convert the wire's
        # integer milliseconds exactly (see _admin_close_timeout).
        timeout = _admin_close_timeout(request)
        try:
            await client.close(timeout)
        except ka.KafkaError as e:
            return pb.StatusResponse(error=_kafka_error_to_proto(e))
        except Exception as e:  # noqa: BLE001
            LOG.exception("admin close raised")
            return pb.StatusResponse(error=_kafka_error_to_proto(e))
        return pb.StatusResponse()


async def serve():
    port = int(os.environ.get("GRPC_PORT", "50051"))
    server = grpc.aio.server()
    pb_grpc.add_ProducerServiceServicer_to_server(ProducerService(), server)
    cpb_grpc.add_ConsumerServiceServicer_to_server(ConsumerService(), server)
    apb_grpc.add_AdminServiceServicer_to_server(AdminService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    await server.start()
    # The Rust BackendPool waits for "listening" on stderr before connecting —
    # keep this string stable (matches grpc_server.py).
    print(f"listening on 0.0.0.0:{port}", file=sys.stderr, flush=True)
    await server.wait_for_termination()


def main():
    logging.basicConfig(
        level=os.environ.get("RUST_LOG", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    asyncio.run(serve())


if __name__ == "__main__":
    main()
