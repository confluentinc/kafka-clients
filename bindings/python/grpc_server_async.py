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
(AsyncKafkaProducer / AsyncKafkaConsumer) over the same ProducerService /
ConsumerService protos as grpc_server.py.

This is the async twin of grpc_server.py: the Rust multilanguage integration
tests run each test body against a `python_async` backend that tunnels every
Producer/Consumer trait call to this server, which drives the asyncio-native
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
import producer_service_pb2 as pb  # noqa: E402  (generated)
import producer_service_pb2_grpc as pb_grpc  # noqa: E402  (generated)
import consumer_service_pb2 as cpb  # noqa: E402  (generated)
import consumer_service_pb2_grpc as cpb_grpc  # noqa: E402  (generated)

# Proto<->Python translation helpers + variant constants shared with the sync
# server (see grpc_translate.py); client-agnostic, so reused verbatim.
from grpc_translate import (  # noqa: E402
    ILLEGAL_STATE,
    TIMEOUT,
    CallbackLog,
    LoggingRebalanceListener,
    _kafka_error_to_proto,
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


async def serve():
    port = int(os.environ.get("GRPC_PORT", "50051"))
    server = grpc.aio.server()
    pb_grpc.add_ProducerServiceServicer_to_server(ProducerService(), server)
    cpb_grpc.add_ConsumerServiceServicer_to_server(ConsumerService(), server)
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
