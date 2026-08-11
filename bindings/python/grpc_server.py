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

"""gRPC server exposing bindings/python/{producer,consumer,admin}.py over
the ProducerService / ConsumerService / AdminService defined in
multilanguage-test-server/proto/.

Used by the Rust integration tests under the multilanguage-tests
feature: the Rust MultilanguageProducer / MultilanguageConsumer /
MultilanguageAdmin clients tunnel every call to this server, which
translates each call into a producer.py KafkaProducer/MockProducer,
consumer.py KafkaConsumer/MockConsumer or admin.py
AdminClient/MockAdminClient call, returning the result over gRPC. See
design/history/MILESTONE-6/DESIGN-multilanguage-tests.md and
design/history/Milestone-11/PLAN-multilanguage-admin.md.

The server listens on 0.0.0.0:50051 (the fixed internal port the
Docker image exposes; the test pool maps it to a random host port via
testcontainers).

Sync grpc.server + thread pool is used because producer.py's underlying
ctypes library is thread-based — futures are completed by background
threads from the Rust send task, and concurrent.futures.Future.result()
blocks the calling thread until the future fires.
"""

import logging
import os
import sys
import threading
from concurrent import futures

import grpc

# Ensure the Python wrapper module is importable regardless of CWD.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import producer as kp  # noqa: E402  (KafkaProducer / MockProducer / KafkaError)
import consumer as kc  # noqa: E402  (KafkaConsumer / MockConsumer / TopicPartition / ...)
import admin as ka  # noqa: E402  (AdminClient / MockAdminClient / ...)
import producer_service_pb2 as pb  # noqa: E402  (generated)
import producer_service_pb2_grpc as pb_grpc  # noqa: E402  (generated)
import consumer_service_pb2 as cpb  # noqa: E402  (generated)
import consumer_service_pb2_grpc as cpb_grpc  # noqa: E402  (generated)
import admin_service_pb2 as apb  # noqa: E402  (generated)
import admin_service_pb2_grpc as apb_grpc  # noqa: E402  (generated)

LOG = logging.getLogger("grpc_server")

# Proto<->Python translation helpers + variant constants are shared with the
# async server (grpc_server_async.py) and live in grpc_translate.py. Only the
# constants the servicers reference directly are pulled into scope here.
from grpc_translate import (  # noqa: E402
    ILLEGAL_STATE,
    TIMEOUT,
    _admin_alter_configs,
    _admin_close_timeout,
    _admin_cluster_description_response,
    _admin_config_resource_key,
    _admin_config_resources,
    _admin_constructor_error,
    _admin_create_topics_response,
    _admin_delete_records_response,
    _admin_describe_classic_groups_response,
    _admin_describe_configs_response,
    _admin_describe_consumer_groups_response,
    _admin_describe_log_dirs_response,
    _admin_describe_replica_log_dirs_response,
    _admin_describe_topics_response,
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
    _admin_members_to_remove,
    _admin_name_key,
    _admin_new_partitions,
    _admin_new_topics,
    _admin_offset_specs,
    _admin_optional_partitions,
    _admin_reassignments,
    _admin_records_to_delete,
    _admin_replica_key,
    _admin_replica_log_dir_assignments,
    _admin_replicas,
    _admin_retry_on_quota,
    _admin_selects_mock,
    _admin_timeout,
    _admin_topic_id_key,
    _admin_tp_tuple_key,
    _admin_void_response,
    _kafka_error_to_proto,
    _metric_to_proto,
    _node_to_proto,
    _oam_to_proto,
    _partition_info_to_proto,
    _proto_to_producer_record,
    _record_metadata_to_proto,
    _record_to_proto,
    _tp,
    _tp_to_proto,
)


class ProducerService(pb_grpc.ProducerServiceServicer):
    """Server-side state: a dict of producer_id -> KafkaProducer and the
    next id to hand out. Both are protected by a single lock since
    CreateProducer / Close / Send may all race."""

    def __init__(self):
        self._producers = {}
        self._next_id = 1
        self._lock = threading.Lock()

    def _take_producer(self, producer_id):
        with self._lock:
            return self._producers.get(producer_id)

    def CreateProducer(self, request, context):
        config = dict(request.config)
        try:
            # Empty config selects MockProducer for client-side smoke
            # testing — useful when developing without a real broker.
            if not config or all(not v for v in config.values()):
                producer = kp.MockProducer(auto_complete=True)
            else:
                producer = kp.KafkaProducer(config)
        except Exception as e:  # noqa: BLE001
            LOG.exception("CreateProducer failed")
            return pb.CreateProducerResponse(producer_id=0, error=_kafka_error_to_proto(e))

        with self._lock:
            producer_id = self._next_id
            self._next_id += 1
            self._producers[producer_id] = producer
        LOG.info("created producer %d", producer_id)
        return pb.CreateProducerResponse(producer_id=producer_id)

    def Send(self, request, context):
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
        try:
            future = producer.send(record)
        except kp.KafkaError as e:
            return pb.SendResponse(error=_kafka_error_to_proto(e))
        except Exception as e:  # noqa: BLE001
            LOG.exception("send raised")
            return pb.SendResponse(error=_kafka_error_to_proto(e))

        # Block this gRPC worker thread until the producer future fires.
        # producer.py futures are concurrent.futures.Future instances
        # completed by Rust background tasks, so .result() is safe here.
        try:
            metadata = future.result(timeout=120)
        except kp.KafkaError as e:
            return pb.SendResponse(error=_kafka_error_to_proto(e))
        except futures.TimeoutError:
            return pb.SendResponse(error=pb.KafkaError(
                variant=TIMEOUT, code=7,
                message="python server: producer future timed out after 120s",
                is_retriable=True, is_fatal=False))
        except Exception as e:  # noqa: BLE001
            LOG.exception("future.result raised")
            return pb.SendResponse(error=_kafka_error_to_proto(e))
        return pb.SendResponse(metadata=_record_metadata_to_proto(metadata))

    def Flush(self, request, context):
        producer = self._take_producer(request.producer_id)
        if producer is None:
            return pb.StatusResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown producer_id {request.producer_id}",
                is_retriable=False, is_fatal=True))
        try:
            producer.flush()
        except kp.KafkaError as e:
            return pb.StatusResponse(error=_kafka_error_to_proto(e))
        return pb.StatusResponse()

    def PartitionsFor(self, request, context):
        producer = self._take_producer(request.producer_id)
        if producer is None:
            return pb.PartitionsForResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown producer_id {request.producer_id}",
                is_retriable=False, is_fatal=True))
        try:
            infos = producer.partitions_for(request.topic)
        except kp.KafkaError as e:
            return pb.PartitionsForResponse(error=_kafka_error_to_proto(e))
        # _partition_info_to_proto is defined in the consumer section below and
        # accepts the same PartitionInfo objects producer.partitions_for returns.
        return pb.PartitionsForResponse(partitions=[_partition_info_to_proto(i) for i in infos])

    def Metrics(self, request, context):
        producer = self._take_producer(request.producer_id)
        if producer is None:
            return pb.MetricsResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown producer_id {request.producer_id}",
                is_retriable=False, is_fatal=True))
        try:
            snapshot = producer.metrics()
        except kp.KafkaError as e:
            return pb.MetricsResponse(error=_kafka_error_to_proto(e))
        return pb.MetricsResponse(metrics=pb.MetricList(
            metrics=[_metric_to_proto(m) for m in snapshot]))

    def Close(self, request, context):
        with self._lock:
            producer = self._producers.pop(request.producer_id, None)
        if producer is None:
            # Close is idempotent — silent success on unknown id mirrors
            # the Java client's behavior.
            return pb.StatusResponse()
        try:
            producer.close()
        except kp.KafkaError as e:
            return pb.StatusResponse(error=_kafka_error_to_proto(e))
        return pb.StatusResponse()

    def CloseTimeout(self, request, context):
        # producer.py's close() doesn't take a timeout. Best-effort
        # mapping: ignore the timeout and call close() unconditionally.
        return self.Close(pb.CloseRequest(producer_id=request.producer_id), context)


# ---------------------------------------------------------------------------
# Consumer service
# ---------------------------------------------------------------------------


class ConsumerService(cpb_grpc.ConsumerServiceServicer):
    """Maps ConsumerService RPCs onto bindings/python/consumer.py. The sync
    consumer API blocks the gRPC worker thread on its threading.Event, which
    is fine in the thread-pool server."""

    def __init__(self):
        self._consumers = {}
        self._next_id = 1
        self._lock = threading.Lock()

    def _get(self, consumer_id):
        with self._lock:
            return self._consumers.get(consumer_id)

    def _status_err(self, e):
        return pb.StatusResponse(error=_kafka_error_to_proto(e))

    def CreateConsumer(self, request, context):
        config = dict(request.config)
        try:
            if not config or all(not v for v in config.values()):
                consumer = kc.MockConsumer("earliest")
            else:
                consumer = kc.KafkaConsumer(config)
        except Exception as e:  # noqa: BLE001
            LOG.exception("CreateConsumer failed")
            return cpb.CreateConsumerResponse(consumer_id=0, error=_kafka_error_to_proto(e))
        with self._lock:
            consumer_id = self._next_id
            self._next_id += 1
            self._consumers[consumer_id] = consumer
        LOG.info("created consumer %d", consumer_id)
        return cpb.CreateConsumerResponse(consumer_id=consumer_id)

    def _run_status(self, consumer_id, fn):
        consumer = self._get(consumer_id)
        if consumer is None:
            return pb.StatusResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {consumer_id}", is_retriable=False, is_fatal=True))
        try:
            fn(consumer)
            return pb.StatusResponse()
        except kc.KafkaError as e:
            return self._status_err(e)
        except Exception as e:  # noqa: BLE001
            LOG.exception("consumer op raised")
            return self._status_err(e)

    def Subscribe(self, request, context):
        return self._run_status(request.consumer_id, lambda c: c.subscribe(list(request.topics)))

    def Unsubscribe(self, request, context):
        return self._run_status(request.consumer_id, lambda c: c.unsubscribe())

    def Assign(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return self._run_status(request.consumer_id, lambda c: c.assign(parts))

    def Poll(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.PollResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        try:
            records = consumer.poll(request.timeout_ms / 1000.0)
        except kc.KafkaError as e:
            return cpb.PollResponse(error=_kafka_error_to_proto(e))
        except Exception as e:  # noqa: BLE001
            LOG.exception("poll raised")
            return cpb.PollResponse(error=_kafka_error_to_proto(e))
        proto_records = [_record_to_proto(r) for r in records]
        return cpb.PollResponse(records=cpb.ConsumerRecordList(records=proto_records))

    def CommitSync(self, request, context):
        def do(c):
            if request.offsets:
                offsets = {
                    _tp(e.partition): kc.OffsetAndMetadata(
                        e.offset.offset, e.offset.metadata,
                        e.offset.leader_epoch if e.offset.HasField("leader_epoch") else None)
                    for e in request.offsets
                }
                c.commit(offsets)
            else:
                c.commit()
        return self._run_status(request.consumer_id, do)

    def Committed(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.CommittedResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        try:
            result = consumer.committed([_tp(p) for p in request.partitions])
        except kc.KafkaError as e:
            return cpb.CommittedResponse(error=_kafka_error_to_proto(e))
        entries = [cpb.OffsetMapEntry(partition=_tp_to_proto(tp), offset=_oam_to_proto(oam))
                   for tp, oam in result.items()]
        return cpb.CommittedResponse(offsets=cpb.OffsetMap(entries=entries))

    def Position(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.PositionResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        try:
            offset = consumer.position(_tp(request.partition))
        except kc.KafkaError as e:
            return cpb.PositionResponse(error=_kafka_error_to_proto(e))
        return cpb.PositionResponse(offset=offset)

    def Seek(self, request, context):
        def do(c):
            tp = _tp(request.partition)
            if request.HasField("metadata") or request.HasField("leader_epoch"):
                oam = kc.OffsetAndMetadata(
                    request.offset,
                    request.metadata if request.HasField("metadata") else "",
                    request.leader_epoch if request.HasField("leader_epoch") else None)
                c.seek(tp, oam)
            else:
                c.seek(tp, request.offset)
        return self._run_status(request.consumer_id, do)

    def SeekToBeginning(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return self._run_status(request.consumer_id, lambda c: c.seek_to_beginning(parts))

    def SeekToEnd(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return self._run_status(request.consumer_id, lambda c: c.seek_to_end(parts))

    def Pause(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return self._run_status(request.consumer_id, lambda c: c.pause(parts))

    def Resume(self, request, context):
        parts = [_tp(p) for p in request.partitions]
        return self._run_status(request.consumer_id, lambda c: c.resume(parts))

    def _long_offsets(self, request, end):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.LongOffsetsResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        parts = [_tp(p) for p in request.partitions]
        try:
            result = consumer.end_offsets(parts) if end else consumer.beginning_offsets(parts)
        except kc.KafkaError as e:
            return cpb.LongOffsetsResponse(error=_kafka_error_to_proto(e))
        entries = [cpb.LongOffsetMapEntry(partition=_tp_to_proto(tp), offset=off) for tp, off in result.items()]
        return cpb.LongOffsetsResponse(offsets=cpb.LongOffsetMap(entries=entries))

    def BeginningOffsets(self, request, context):
        return self._long_offsets(request, end=False)

    def EndOffsets(self, request, context):
        return self._long_offsets(request, end=True)

    def OffsetsForTimes(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.OffsetAndTimestampResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        spec = {_tp(e.partition): e.timestamp for e in request.timestamps}
        try:
            result = consumer.offsets_for_times(spec)
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

    def PartitionsFor(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return pb.PartitionsForResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        try:
            infos = consumer.partitions_for(request.topic)
        except kc.KafkaError as e:
            return pb.PartitionsForResponse(error=_kafka_error_to_proto(e))
        return pb.PartitionsForResponse(partitions=[_partition_info_to_proto(i) for i in infos])

    def ListTopics(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.ListTopicsResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        try:
            topics = consumer.list_topics()
        except kc.KafkaError as e:
            return cpb.ListTopicsResponse(error=_kafka_error_to_proto(e))
        entries = [cpb.TopicPartitionInfoEntry(topic=t, partitions=[_partition_info_to_proto(i) for i in infos])
                   for t, infos in topics.items()]
        return cpb.ListTopicsResponse(topics=cpb.TopicListing(topics=entries))

    def Assignment(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.TopicPartitionListResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        try:
            tps = consumer.assignment()
        except Exception as e:  # noqa: BLE001
            return cpb.TopicPartitionListResponse(error=_kafka_error_to_proto(e))
        return cpb.TopicPartitionListResponse(
            partitions=cpb.TopicPartitionList(partitions=[_tp_to_proto(tp) for tp in tps]))

    def Subscription(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.SubscriptionResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        try:
            topics = consumer.subscription()
        except Exception as e:  # noqa: BLE001
            return cpb.SubscriptionResponse(error=_kafka_error_to_proto(e))
        return cpb.SubscriptionResponse(topics=cpb.StringList(values=list(topics)))

    def Metrics(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return pb.MetricsResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        try:
            snapshot = consumer.metrics()
        except Exception as e:  # noqa: BLE001
            return pb.MetricsResponse(error=_kafka_error_to_proto(e))
        return pb.MetricsResponse(metrics=pb.MetricList(
            metrics=[_metric_to_proto(m) for m in snapshot]))

    def Paused(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is None:
            return cpb.TopicPartitionListResponse(error=pb.KafkaError(
                variant=ILLEGAL_STATE, code=-1,
                message=f"unknown consumer_id {request.consumer_id}", is_retriable=False, is_fatal=True))
        try:
            tps = consumer.paused()
        except Exception as e:  # noqa: BLE001
            return cpb.TopicPartitionListResponse(error=_kafka_error_to_proto(e))
        return cpb.TopicPartitionListResponse(
            partitions=cpb.TopicPartitionList(partitions=[_tp_to_proto(tp) for tp in tps]))

    def Wakeup(self, request, context):
        consumer = self._get(request.consumer_id)
        if consumer is not None:
            consumer.wakeup()
        return pb.StatusResponse()

    def Close(self, request, context):
        with self._lock:
            consumer = self._consumers.pop(request.consumer_id, None)
        if consumer is None:
            return pb.StatusResponse()
        try:
            consumer.close()
        except kc.KafkaError as e:
            return self._status_err(e)
        return pb.StatusResponse()


# ---------------------------------------------------------------------------
# Admin service
# ---------------------------------------------------------------------------


class AdminService(apb_grpc.AdminServiceServicer):
    """Maps AdminService RPCs onto bindings/python/admin.py's *synchronous*
    AdminClient / MockAdminClient (the async twin is driven by
    grpc_server_async.py).

    Java's Admin methods return per-key futures, but admin.py's `_run_sync`
    already waits for the FFI completion callback and hands back resolved data,
    so blocking the gRPC worker thread here is what the binding does anyway."""

    def __init__(self):
        self._admins = {}
        self._next_id = 1
        self._lock = threading.Lock()

    def _get(self, admin_id):
        with self._lock:
            return self._admins.get(admin_id)

    def CreateAdmin(self, request, context):
        config = dict(request.config)
        num_brokers = request.num_brokers if request.HasField("num_brokers") else 1
        try:
            # See CreateAdminRequest in admin_service.proto: this predicate is
            # the normative mock-selection rule for all three servers.
            if _admin_selects_mock(config):
                client = ka.MockAdminClient(num_brokers)
            else:
                client = ka.AdminClient(config)
        except Exception as e:  # noqa: BLE001
            LOG.exception("CreateAdmin failed")
            return apb.CreateAdminResponse(admin_id=0, error=_admin_constructor_error(e))
        with self._lock:
            admin_id = self._next_id
            self._next_id += 1
            self._admins[admin_id] = client
        LOG.info("created admin %d", admin_id)
        return apb.CreateAdminResponse(admin_id=admin_id)

    # -- Topics & partitions (slice G1) --------------------------------------
    #
    # Every handler follows the same three steps: look the handle up, call the
    # admin.py method (which blocks this worker thread until the FFI completion
    # callback fires, exactly as any admin.py caller does), and wrap the
    # resolved dict in the per-key envelope. A raised KafkaError is a
    # whole-call failure and goes in the response's top-level `error`, leaving
    # `entries` empty — per-key failures never raise, they arrive inside the
    # dict.

    def _unknown_admin(self, admin_id):
        return pb.KafkaError(
            variant=ILLEGAL_STATE, code=-1,
            message=f"unknown admin_id {admin_id}",
            is_retriable=False, is_fatal=True)

    def CreateTopics(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.CreateTopicsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.create_topics(
                _admin_new_topics(request.topics),
                timeout=_admin_timeout(request),
                validate_only=request.validate_only,
                retry_on_quota_violation=_admin_retry_on_quota(request),
            )
        except Exception as e:  # noqa: BLE001
            LOG.exception("create_topics raised")
            return apb.CreateTopicsResponse(error=_kafka_error_to_proto(e))
        return _admin_create_topics_response(outcomes)

    def DeleteTopics(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        by_ids = request.WhichOneof("topics") == "topic_ids"
        names = list(request.topic_ids.values if by_ids else request.names.values)
        try:
            if by_ids:
                outcomes = client.delete_topics_by_ids(
                    names, timeout=_admin_timeout(request),
                    retry_on_quota_violation=_admin_retry_on_quota(request))
            else:
                outcomes = client.delete_topics(
                    names, timeout=_admin_timeout(request),
                    retry_on_quota_violation=_admin_retry_on_quota(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_topics raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        key_fn = _admin_topic_id_key if by_ids else _admin_name_key
        return _admin_void_response(outcomes, key_fn)

    def ListTopics(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.AdminListTopicsResponse(error=self._unknown_admin(request.admin_id))
        try:
            listings = client.list_topics(
                timeout=_admin_timeout(request), list_internal=request.list_internal)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_topics raised")
            return apb.AdminListTopicsResponse(error=_kafka_error_to_proto(e))
        return _admin_list_topics_response(listings)

    def DescribeTopics(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeTopicsResponse(error=self._unknown_admin(request.admin_id))
        by_ids = request.WhichOneof("topics") == "topic_ids"
        topics = list(request.topic_ids.values if by_ids else request.names.values)
        limit = (request.partition_size_limit_per_response
                 if request.HasField("partition_size_limit_per_response") else None)
        try:
            method = client.describe_topics_by_ids if by_ids else client.describe_topics
            outcomes = method(
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

    def CreatePartitions(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.create_partitions(
                _admin_new_partitions(request.partitions),
                timeout=_admin_timeout(request),
                validate_only=request.validate_only,
                retry_on_quota_violation=_admin_retry_on_quota(request),
            )
        except Exception as e:  # noqa: BLE001
            LOG.exception("create_partitions raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        return _admin_void_response(outcomes, _admin_name_key)

    def DeleteRecords(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DeleteRecordsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.delete_records(
                _admin_records_to_delete(request.records), timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_records raised")
            return apb.DeleteRecordsResponse(error=_kafka_error_to_proto(e))
        return _admin_delete_records_response(outcomes)

    # -- Cluster, configs & log dirs (slice G2) -------------------------------
    #
    # Same three steps as the G1 handlers. Note the split in what "failure"
    # means: describe_cluster / list_config_resources /
    # list_client_metrics_resources have one Java future each, so admin.py
    # *raises* and the failure lands in the top-level error; the per-key RPCs
    # never raise for a single key, its KafkaError arrives inside the dict.

    def DescribeCluster(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeClusterResponse(error=self._unknown_admin(request.admin_id))
        try:
            description = client.describe_cluster(
                timeout=_admin_timeout(request),
                include_authorized_operations=request.include_authorized_operations,
                include_fenced_brokers=request.include_fenced_brokers,
            )
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_cluster raised")
            return apb.DescribeClusterResponse(error=_kafka_error_to_proto(e))
        return _admin_cluster_description_response(description)

    def DescribeConfigs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeConfigsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.describe_configs(
                _admin_config_resources(request.resources),
                timeout=_admin_timeout(request),
                include_synonyms=request.include_synonyms,
                include_documentation=request.include_documentation,
            )
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_configs raised")
            return apb.DescribeConfigsResponse(error=_kafka_error_to_proto(e))
        return _admin_describe_configs_response(outcomes)

    def IncrementalAlterConfigs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.incremental_alter_configs(
                _admin_alter_configs(request.configs),
                timeout=_admin_timeout(request),
                validate_only=request.validate_only,
            )
        except Exception as e:  # noqa: BLE001
            LOG.exception("incremental_alter_configs raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        return _admin_void_response(outcomes, _admin_config_resource_key)

    def ListConfigResources(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListConfigResourcesResponse(error=self._unknown_admin(request.admin_id))
        try:
            # An empty repeated field is Java's empty Set: every supported type.
            resources = client.list_config_resources(
                list(request.resource_types), timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_config_resources raised")
            return apb.ListConfigResourcesResponse(error=_kafka_error_to_proto(e))
        return _admin_list_config_resources_response(resources)

    def ListClientMetricsResources(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListClientMetricsResourcesResponse(
                error=self._unknown_admin(request.admin_id))
        try:
            resources = client.list_client_metrics_resources(timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_client_metrics_resources raised")
            return apb.ListClientMetricsResourcesResponse(error=_kafka_error_to_proto(e))
        return _admin_list_client_metrics_resources_response(resources)

    def DescribeLogDirs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeLogDirsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.describe_log_dirs(
                list(request.brokers), timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_log_dirs raised")
            return apb.DescribeLogDirsResponse(error=_kafka_error_to_proto(e))
        return _admin_describe_log_dirs_response(outcomes)

    def AlterReplicaLogDirs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.alter_replica_log_dirs(
                _admin_replica_log_dir_assignments(request.assignments),
                timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("alter_replica_log_dirs raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        return _admin_void_response(outcomes, _admin_replica_key)

    def DescribeReplicaLogDirs(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeReplicaLogDirsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.describe_replica_log_dirs(
                _admin_replicas(request.replicas), timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_replica_log_dirs raised")
            return apb.DescribeReplicaLogDirsResponse(error=_kafka_error_to_proto(e))
        return _admin_describe_replica_log_dirs_response(outcomes)

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

    def ElectLeaders(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            # `partitions=None` is Java's null Set: elect for every partition.
            # admin.py requires the argument explicitly for exactly that reason.
            outcomes = client.elect_leaders(
                request.election_type,
                _admin_optional_partitions(request),
                timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("elect_leaders raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        return _admin_void_response(outcomes, _admin_tp_tuple_key)

    def AlterPartitionReassignments(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        # Java's default is true, so an absent field is true.
        allow_rf_change = (request.allow_replication_factor_change
                           if request.HasField("allow_replication_factor_change") else True)
        try:
            outcomes = client.alter_partition_reassignments(
                _admin_reassignments(request.reassignments),
                timeout=_admin_timeout(request),
                allow_replication_factor_change=allow_rf_change)
        except Exception as e:  # noqa: BLE001
            LOG.exception("alter_partition_reassignments raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        return _admin_void_response(outcomes, _admin_tp_tuple_key)

    def ListPartitionReassignments(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListPartitionReassignmentsResponse(
                error=self._unknown_admin(request.admin_id))
        try:
            reassignments = client.list_partition_reassignments(
                _admin_optional_partitions(request), timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_partition_reassignments raised")
            return apb.ListPartitionReassignmentsResponse(error=_kafka_error_to_proto(e))
        return _admin_list_partition_reassignments_response(reassignments)

    def ListOffsets(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListOffsetsResponse(error=self._unknown_admin(request.admin_id))
        try:
            # A malformed OffsetSpec raises out of _admin_offset_specs, which is a
            # whole-call failure -- the same level the C++ server reports it at.
            outcomes = client.list_offsets(
                _admin_offset_specs(request.specs),
                timeout=_admin_timeout(request),
                isolation_level=request.isolation_level)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_offsets raised")
            return apb.ListOffsetsResponse(error=_kafka_error_to_proto(e))
        return _admin_list_offsets_response(outcomes)

    # -- Groups & offsets (slice G4) ------------------------------------------
    #
    # Three levels of error, and which one an RPC uses follows its Java future
    # shape rather than a template:
    #
    #   - list_groups / list_consumer_groups: admin.py returns a *pair* of lists.
    #     A raise is the whole-call error; a per-broker listing failure is inside
    #     the second list, unkeyed.
    #   - describe_consumer_groups / describe_classic_groups /
    #     list_consumer_group_offsets: one future per group, so a per-group
    #     failure arrives in the dict and only a submission failure raises.
    #   - alter_consumer_group_offsets / delete_consumer_group_offsets /
    #     remove_members_from_consumer_group: Java holds ONE future over the whole
    #     map, so a failure of that future raises here and becomes the top-level
    #     error with `entries` empty. With an empty input (no partitions, or
    #     removeAll) that is the *only* observable, and admin.py returns an empty
    #     dict. delete_consumer_groups is the one of the four with genuine
    #     per-key futures.

    def ListGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListGroupsResponse(error=self._unknown_admin(request.admin_id))
        try:
            # Empty filter lists leave the filter unset, which is Java's empty
            # set; admin.py takes None for the same thing, and [] is equivalent.
            outcome = client.list_groups(
                group_states=list(request.group_states),
                protocol_types=list(request.protocol_types),
                types=list(request.types),
                timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_groups raised")
            return apb.ListGroupsResponse(error=_kafka_error_to_proto(e))
        return _admin_list_groups_response(outcome)

    def ListConsumerGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListConsumerGroupsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcome = client.list_consumer_groups(
                group_states=list(request.group_states),
                types=list(request.types),
                timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_consumer_groups raised")
            return apb.ListConsumerGroupsResponse(error=_kafka_error_to_proto(e))
        return _admin_list_consumer_groups_response(outcome)

    def DescribeConsumerGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeConsumerGroupsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.describe_consumer_groups(
                list(request.group_ids), timeout=_admin_timeout(request),
                include_authorized_operations=request.include_authorized_operations)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_consumer_groups raised")
            return apb.DescribeConsumerGroupsResponse(error=_kafka_error_to_proto(e))
        return _admin_describe_consumer_groups_response(outcomes)

    def DescribeClassicGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.DescribeClassicGroupsResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.describe_classic_groups(
                list(request.group_ids), timeout=_admin_timeout(request),
                include_authorized_operations=request.include_authorized_operations)
        except Exception as e:  # noqa: BLE001
            LOG.exception("describe_classic_groups raised")
            return apb.DescribeClassicGroupsResponse(error=_kafka_error_to_proto(e))
        return _admin_describe_classic_groups_response(outcomes)

    def ListConsumerGroupOffsets(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.ListConsumerGroupOffsetsResponse(
                error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.list_consumer_group_offsets(
                _admin_group_offset_specs(request.group_specs),
                timeout=_admin_timeout(request),
                require_stable=request.require_stable)
        except Exception as e:  # noqa: BLE001
            LOG.exception("list_consumer_group_offsets raised")
            return apb.ListConsumerGroupOffsetsResponse(error=_kafka_error_to_proto(e))
        return _admin_list_consumer_group_offsets_response(outcomes)

    def AlterConsumerGroupOffsets(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.alter_consumer_group_offsets(
                request.group_id, _admin_group_offset_commits(request.offsets),
                timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("alter_consumer_group_offsets raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        return _admin_void_response(outcomes, _admin_tp_tuple_key)

    def DeleteConsumerGroupOffsets(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.delete_consumer_group_offsets(
                request.group_id,
                [(tp.topic, tp.partition) for tp in request.partitions],
                timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_consumer_group_offsets raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        return _admin_void_response(outcomes, _admin_tp_tuple_key)

    def DeleteConsumerGroups(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            outcomes = client.delete_consumer_groups(
                list(request.group_ids), timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("delete_consumer_groups raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        return _admin_void_response(outcomes, _admin_name_key)

    def RemoveMembersFromConsumerGroup(self, request, context):
        client = self._get(request.admin_id)
        if client is None:
            return apb.VoidKeyedResponse(error=self._unknown_admin(request.admin_id))
        try:
            # `members=None` is Java's no-argument options constructor
            # (removeAll); an empty *list* is the collection constructor, which
            # Java rejects. _admin_members_to_remove keeps the two apart.
            outcomes = client.remove_members_from_consumer_group(
                request.group_id, _admin_members_to_remove(request),
                reason=request.reason if request.HasField("reason") else None,
                timeout=_admin_timeout(request))
        except Exception as e:  # noqa: BLE001
            LOG.exception("remove_members_from_consumer_group raised")
            return apb.VoidKeyedResponse(error=_kafka_error_to_proto(e))
        # Keyed by group.instance.id, which is a plain string.
        return _admin_void_response(outcomes, _admin_name_key)

    def Close(self, request, context):
        with self._lock:
            client = self._admins.pop(request.admin_id, None)
        if client is None:
            # Close is idempotent — silent success on unknown id, as the
            # producer and consumer services do.
            return pb.StatusResponse()
        # admin.py's close() takes seconds or a timedelta; absent means Java's
        # no-argument close(). Shared helper so both servers convert the wire's
        # integer milliseconds exactly (see _admin_close_timeout).
        timeout = _admin_close_timeout(request)
        try:
            client.close(timeout)
        except ka.KafkaError as e:
            return pb.StatusResponse(error=_kafka_error_to_proto(e))
        except Exception as e:  # noqa: BLE001
            LOG.exception("admin close raised")
            return pb.StatusResponse(error=_kafka_error_to_proto(e))
        return pb.StatusResponse()


def main():
    logging.basicConfig(
        level=os.environ.get("RUST_LOG", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    port = int(os.environ.get("GRPC_PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pb_grpc.add_ProducerServiceServicer_to_server(ProducerService(), server)
    cpb_grpc.add_ConsumerServiceServicer_to_server(ConsumerService(), server)
    apb_grpc.add_AdminServiceServicer_to_server(AdminService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    # The Rust BackendPool waits for "listening" on stderr before
    # connecting — keep this string stable.
    print(f"listening on 0.0.0.0:{port}", file=sys.stderr, flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
