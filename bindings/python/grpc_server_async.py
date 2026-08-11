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
    _admin_constructor_error,
    _admin_create_topics_response,
    _admin_delete_records_response,
    _admin_describe_topics_response,
    _admin_list_topics_response,
    _admin_name_key,
    _admin_new_partitions,
    _admin_new_topics,
    _admin_records_to_delete,
    _admin_retry_on_quota,
    _admin_selects_mock,
    _admin_timeout,
    _admin_topic_id_key,
    _admin_void_response,
    _kafka_error_to_proto,
    _oam_to_proto,
    _partition_info_to_proto,
    _proto_to_producer_record,
    _record_metadata_to_proto,
    _record_to_proto,
    _tp,
    _tp_to_proto,
)

LOG = logging.getLogger("grpc_server_async")


class ProducerService(pb_grpc.ProducerServiceServicer):
    """Async twin of grpc_server.ProducerService, driving AsyncKafkaProducer.

    The producer id map needs no lock: all handlers run on one event loop and id
    allocation happens without an intervening await."""

    def __init__(self):
        self._producers = {}
        self._next_id = 1

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

        if request.with_callback:
            LOG.debug("send_with_callback (callback runs Rust-side)")
        # AsyncProducer.send is a coroutine that returns an asyncio.Future.
        try:
            future = await producer.send(record)
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


class ConsumerService(cpb_grpc.ConsumerServiceServicer):
    """Async twin of grpc_server.ConsumerService, driving AsyncKafkaConsumer.

    Blocking-in-Java ops are coroutines and are awaited; the non-blocking state
    reads and local ops (assignment/subscription/paused/wakeup/seek) live on the
    shared _ConsumerBase and are sync — called directly, never awaited."""

    def __init__(self):
        self._consumers = {}
        self._next_id = 1

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
        return await self._run_status(request.consumer_id, lambda c: c.subscribe(list(request.topics)))

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
            if request.offsets:
                offsets = {
                    _tp(e.partition): kc.OffsetAndMetadata(
                        e.offset.offset, e.offset.metadata,
                        e.offset.leader_epoch if e.offset.HasField("leader_epoch") else None)
                    for e in request.offsets
                }
                await c.commit(offsets)
            else:
                await c.commit()
        return await self._run_status(request.consumer_id, do)

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
        # seek() is a sync local op on _ConsumerBase (not a coroutine) — call it
        # directly rather than awaiting.
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
                consumer.seek(tp, oam)
            else:
                consumer.seek(tp, request.offset)
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
        except Exception as e:  # noqa: BLE001
            LOG.exception("create_topics raised")
            return apb.CreateTopicsResponse(error=_kafka_error_to_proto(e))
        return _admin_create_topics_response(outcomes)

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
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_topics raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        key_fn = _admin_topic_id_key if by_ids else _admin_name_key
        return _admin_void_response(outcomes, key_fn)

    async def ListTopics(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.AdminListTopicsResponse(error=self._unknown_admin(request.admin_id))
        try:
            listings = await client.list_topics(
                timeout=_admin_timeout(request), list_internal=request.list_internal)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_topics raised")
            return apb.AdminListTopicsResponse(error=_kafka_error_to_proto(e))
        return _admin_list_topics_response(listings)

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
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_topics raised")
            return apb.DescribeTopicsResponse(error=_kafka_error_to_proto(e))
        key_fn = _admin_topic_id_key if by_ids else _admin_name_key
        return _admin_describe_topics_response(outcomes, key_fn)

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
        except Exception as e:  # noqa: BLE001
            LOG.exception("create_partitions raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        return _admin_void_response(outcomes, _admin_name_key)

    async def DeleteRecords(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DeleteRecordsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = await client.delete_records(
                _admin_records_to_delete(request.records), timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_records raised")
            return apb.DeleteRecordsResponse(error=_kafka_error_to_proto(e))
        return _admin_delete_records_response(outcomes)

    async def Close(self, request, context):
        client = self._admins.pop(request.admin_id, None)
        if client is None:
            # Close is idempotent — silent success on unknown id.
            return pb.StatusResponse()
        # admin.py's close() takes seconds (or a timedelta); absent means
        # Java's no-argument close().
        timeout = request.timeout_ms / 1000.0 if request.HasField("timeout_ms") else None
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
