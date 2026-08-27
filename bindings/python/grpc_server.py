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

"""gRPC server exposing bindings/python/producer.py over the
ProducerService defined in
multilanguage-test-server/proto/producer_service.proto.

Used by the Rust integration tests under the multilanguage-tests
feature: the Rust MultilanguageProducer client tunnels every Producer
trait call to this server, which translates each call into a
producer.py KafkaProducer/MockProducer call, returning the result over
gRPC. See design/history/MILESTONE-6/DESIGN-multilanguage-tests.md.

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
import producer_service_pb2 as pb  # noqa: E402  (generated)
import producer_service_pb2_grpc as pb_grpc  # noqa: E402  (generated)
import consumer_service_pb2 as cpb  # noqa: E402  (generated)
import consumer_service_pb2_grpc as cpb_grpc  # noqa: E402  (generated)

LOG = logging.getLogger("grpc_server")

# Proto<->Python translation helpers + variant constants are shared with the
# async server (grpc_server_async.py) and live in grpc_translate.py. Only the
# constants the servicers reference directly are pulled into scope here.
from grpc_translate import (  # noqa: E402
    ILLEGAL_STATE,
    TIMEOUT,
    CallbackLog,
    LoggingRebalanceListener,
    _kafka_error_to_proto,
    _metric_to_proto,
    _node_to_proto,
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


class ProducerService(pb_grpc.ProducerServiceServicer):
    """Server-side state: a dict of producer_id -> KafkaProducer and the
    next id to hand out. Both are protected by a single lock since
    CreateProducer / Close / Send may all race."""

    def __init__(self):
        self._producers = {}
        self._next_id = 1
        self._lock = threading.Lock()
        # Delivery-callback log, keyed by producer_id. Has its own lock inside
        # (callbacks fire on the producer's completion thread, GetCallbackLog on
        # a gRPC worker).
        self._callback_log = CallbackLog()

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

        # with_callback => register a *real* on_delivery through producer.py, so
        # the Rust harness can assert on what the binding's callback actually saw
        # (via GetCallbackLog). The Rust client-side closure only proves its own
        # plumbing.
        on_delivery = None
        if request.with_callback:
            on_delivery = make_logging_delivery_callback(self._callback_log, request.producer_id)
        try:
            future = producer.send(record, on_delivery=on_delivery)
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

    def GetCallbackLog(self, request, context):
        # Deliberately readable after Close (the log outlives the producer) —
        # the entry a delivery callback appends during close()'s flush is
        # exactly the interesting one.
        return self._callback_log.response(request.producer_id)


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
        # Rebalance-listener / commit-callback log, keyed by consumer_id. Both
        # callback families are invoked from the Rust dispatcher thread, never
        # the gRPC worker that serves GetCallbackLog — hence CallbackLog's lock.
        self._callback_log = CallbackLog()

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
        # with_listener => subscribe with a real ConsumerRebalanceListener built
        # by consumer.py, whose invocations land in the callback log. Registration
        # is per-subscribe: a listener-less Subscribe releases it.
        listener = None
        if request.with_listener:
            listener = LoggingRebalanceListener(self._callback_log, request.consumer_id)
        return self._run_status(
            request.consumer_id,
            lambda c: c.subscribe(list(request.topics), listener=listener))

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
            offsets = _proto_offsets_to_dict(request.offsets)
            if offsets:
                c.commit(offsets)
            else:
                c.commit()
        return self._run_status(request.consumer_id, do)

    def CommitAsync(self, request, context):
        callback = None
        if request.with_callback:
            callback = make_logging_commit_callback(self._callback_log, request.consumer_id)

        def do(c):
            # commit_async is non-blocking in both clients (a local op on the
            # shared _ConsumerBase, not a coroutine) — the callback fires on a
            # later poll/commit/close, exactly as in Java.
            c.commit_async(_proto_offsets_to_dict(request.offsets) or None, callback=callback)
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

    def GetCallbackLog(self, request, context):
        # Readable after Close on purpose: close() drains pending commit
        # callbacks and fires on_partitions_lost, so those entries land last.
        return self._callback_log.response(request.consumer_id)


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
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    # The Rust BackendPool waits for "listening" on stderr before
    # connecting — keep this string stable.
    print(f"listening on 0.0.0.0:{port}", file=sys.stderr, flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
