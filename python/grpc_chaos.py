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

"""ChaosWorkloadService for the Python gRPC servers.

Runs a chaos-harness workload (tests/chaos/) inside this process, against the
``confluent_kafka`` package, and streams its events back to the harness -- see
multilanguage-test-server/proto/chaos_service.proto for the contract.

The loops here are the Python counterparts of the Rust harness's in-process
``ProducerWorkload`` / ``ConsumerWorkload`` / ``ChaosRebalanceListener``
(tests/chaos/workload.rs) and follow them step for step, so a run with a Python
workload exercises the same client behaviour as a run with a Rust one:

* the producer sends on an absolute schedule without awaiting each record, so
  the binding batches and pipelines as an application's would, and reports each
  record's outcome from its delivery callback;
* the consumer subscribes with a rebalance listener that reports every callback
  and, in ``on_partitions_revoked``, commits through its consumer and reads the
  commit back, as a real application flushing offsets does (the package routes
  a listener's call back into its consumer through the core's
  ``ConsumerHandle``, the Rust listener's path);
* the drain is the same: the producer closes (every record settles first), the
  consumer commits, reads back, reports it is closing, closes.

Every event is queued from the callback or loop step that observed it, and
``MarkWorkload`` queues its marker on the same FIFO, so the harness can tell
which events its client observed before a topic-id switch however far behind
the stream it reads (chaos_service.proto, "Topic ids").

``ChaosWorkloadService`` serves the synchronous server (grpc_server.py): each
workload runs on its own thread with the synchronous client.
``AsyncChaosWorkloadService`` serves the asyncio server (grpc_server_async.py):
each workload is a task on the server's event loop using the asyncio client, so
the async flavour of the binding is what gets exercised.
"""

import asyncio
import logging
import queue
import threading
import time

import chaos_service_pb2 as chpb
import chaos_service_pb2_grpc as chpb_grpc
import producer_service_pb2 as pb
from confluent_kafka.consumer import (
    AsyncKafkaConsumer,
    ConsumerRebalanceListener,
    KafkaConsumer,
)
from confluent_kafka.producer import AsyncKafkaProducer, KafkaProducer, ProducerRecord

# The codes the servers stamp on errors of their own making (each error class's
# ``_ffi_id``), shared with grpc_server.py / grpc_server_async.py.
from grpc_translate import _LOCAL_ILLEGAL_ARGUMENT, _LOCAL_ILLEGAL_STATE, _kafka_error_to_proto

LOG = logging.getLogger("grpc_chaos")

# Upper bound on events per stream message. A server flushes whatever it has
# queued each time it writes; this only caps one message's size when the harness
# falls behind a very fast workload.
_MAX_BATCH = 4096

# Pause after a failed poll before polling again, so a persistent error does not
# spin at full CPU (tests/chaos/workload.rs POLL_ERROR_BACKOFF).
_POLL_ERROR_BACKOFF_S = 0.1

# A schedule more than this far behind (a long `send` block while the client
# waits out a fault) resumes from now rather than replaying the backlog.
_MAX_SCHEDULE_LAG_S = 1.0

# An asyncio producer that is behind schedule (or unlimited) yields to the event
# loop once per this many records. The loop also runs the completion drain and
# the gRPC stream, so it must get turns, but a yield per record is a full loop
# iteration per record and capped the producer at ~33k records/s; every 10th
# record measured ~113k (the sync server's rate) with no loss of promptness.
_ASYNC_YIELD_EVERY = 10

_REBALANCE_KIND = {
    "assigned": chpb.REBALANCE_KIND_ASSIGNED,
    "revoked": chpb.REBALANCE_KIND_REVOKED,
    "lost": chpb.REBALANCE_KIND_LOST,
}


# ---------------------------------------------------------------------------
# Record encoding and event construction (shared by both flavours)
# ---------------------------------------------------------------------------


def _key(index):
    """The record key: its 8-byte big-endian logical index."""
    return index.to_bytes(8, "big")


def _value(index, msg_size):
    """The record value: the index's 8 bytes zero-padded to ``msg_size``, or
    the first ``msg_size`` of those bytes when smaller
    (tests/chaos/workload.rs ``build_value``)."""
    idx = _key(index)
    if msg_size <= len(idx):
        return idx[:msg_size]
    return idx + bytes(msg_size - len(idx))


def _error(exc):
    return _kafka_error_to_proto(exc)


def _sent(index):
    return chpb.WorkloadEvent(sent=chpb.Sent(index=index))


def _outcome(index, metadata, exception):
    """The event settling record ``index`` from its delivery callback."""
    if exception is not None:
        return chpb.WorkloadEvent(send_failed=chpb.SendFailed(index=index, error=_error(exception)))
    if metadata is None:
        return chpb.WorkloadEvent(send_failed=chpb.SendFailed(index=index, error=pb.KafkaError(
            code=_LOCAL_ILLEGAL_STATE,
            message="python server: delivery callback fired with neither metadata nor error")))
    return chpb.WorkloadEvent(delivered=chpb.Delivered(
        index=index, partition=metadata.partition(), offset=metadata.offset()))


def _send_failed(index, exc):
    return chpb.WorkloadEvent(send_failed=chpb.SendFailed(index=index, error=_error(exc)))


class _RecordOutcomes:
    """Settles each record from its delivery callback, exactly once against a
    ``send()`` that raised.

    A ``send()`` can raise after the binding has queued the record (while it
    waits for buffer space), and then the record's callback still fires. So a
    raise is reported as ``SendFailed`` only if the callback has not fired yet,
    and the first callback after such a report is absorbed (logged): either
    way the record is settled once. Any other callback is reported, including a
    second one for the same record, so the verifier sees a client that settles
    a record twice.

    An error while building the outcome event is not left to the binding, which
    logs and swallows whatever a delivery callback raises (the record would then
    look unsettled and the client would be blamed): it is logged and ends the
    workload with ``Failed``, as a server-side fault.
    """

    def __init__(self, emit, workload_id):
        self._emit = emit
        self._workload_id = workload_id
        self._lock = threading.Lock()

    def on_delivery(self, index):
        """The delivery callback for record ``index``, and its settlement
        state for :meth:`send_raised`."""
        state = [0, False]  # callbacks fired, send() failure reported

        def on_delivery(metadata, exception):
            with self._lock:
                state[0] += 1
                absorbed = state[1] and state[0] == 1
            if absorbed:
                LOG.warning("chaos producer %s: record %d settled by its failed send(); its callback "
                            "fired afterwards with %s", self._workload_id, index,
                            "an error" if exception is not None else "metadata")
                return
            try:
                event = _outcome(index, metadata, exception)
            except Exception as e:  # noqa: BLE001
                LOG.exception("chaos producer %s: building record %d's outcome failed",
                              self._workload_id, index)
                event = _failed(e)
            self._emit(event)

        return on_delivery, state

    def send_raised(self, index, state, exc):
        """``send()`` of record ``index`` raised ``exc``."""
        with self._lock:
            report = state[0] == 0 and not state[1]
            state[1] = True
        if report:
            self._emit(_send_failed(index, exc))
        else:
            LOG.warning("chaos producer %s: send() of record %d raised after its callback had "
                        "settled it: %s", self._workload_id, index, exc)


def _stats(sent, elapsed):
    return chpb.WorkloadEvent(producer_stats=chpb.ProducerStats(sent=sent, elapsed_seconds=elapsed))


def _check_record(key, value, msg_size):
    """The record's index if it is what a producer wrote: an 8-byte big-endian
    index key and that index's ``_value`` at ``msg_size``. Otherwise raises
    ``ValueError`` saying what is wrong. Mirrors the Rust harness's
    ``check_record`` (``rust/tests/chaos/workload.rs``), messages included."""
    if key is None:
        raise ValueError("key is missing (expected the 8-byte index)")
    if len(key) != 8:
        raise ValueError(f"key is {len(key)} byte(s), expected the 8-byte index")
    index = int.from_bytes(bytes(key), "big")
    if value is None:
        if msg_size == 0:
            return index
        raise ValueError(f"value is missing (expected {msg_size} byte(s) encoding index {index})")
    if bytes(value) != _value(index, msg_size):
        raise ValueError(
            f"value of {len(value)} byte(s) does not match the producer's encoding of index {index} "
            f"({msg_size} byte(s))")
    return index


def _consumed_events(records, msg_size):
    """One event per record in a poll's batch: ``Consumed`` when it is what a
    producer wrote, ``Corrupted`` otherwise (see ``_check_record``). The
    consumers are byte-typed (no deserializer given), so ``key()`` / ``value()``
    are ``bytes`` or ``None``."""
    events = []
    for r in records:
        try:
            index = _check_record(r.key(), r.value(), msg_size)
        except ValueError as e:
            events.append(chpb.WorkloadEvent(corrupted=chpb.Corrupted(
                topic=r.topic(), partition=r.partition(), offset=r.offset(), detail=str(e))))
            continue
        events.append(chpb.WorkloadEvent(consumed=chpb.Consumed(
            index=index, topic=r.topic(), partition=r.partition(), offset=r.offset())))
    return events


def _rebalance(kind, partitions):
    observed_at = time.time_ns()
    refs = sorted((tp.topic(), tp.partition()) for tp in partitions)
    return chpb.WorkloadEvent(rebalance=chpb.Rebalance(
        kind=_REBALANCE_KIND[kind],
        partitions=[chpb.TopicPartitionRef(topic=t, partition=p) for t, p in refs],
        observed_at_unix_nanos=observed_at))


def _committed_events(offsets):
    """One ``Committed`` per partition ``committed()`` reports an offset for.
    A partition with no committed offset maps to ``None`` (Java's ``null``) and
    has no event; the Rust workload's map leaves such a partition out
    (tests/chaos/workload.rs ``record_committed``)."""
    return [chpb.WorkloadEvent(committed=chpb.Committed(
        topic=tp.topic(), partition=tp.partition(), offset=oam.offset()))
        for tp, oam in offsets.items() if oam is not None]


def _consumer_error(op, exc):
    return chpb.WorkloadEvent(consumer_error=chpb.ConsumerError(op=op, error=_error(exc)))


def _commit_callback(emit):
    """The ``commit_nowait`` (Java's ``commitAsync``) completion callback:
    reports a failed commit, which otherwise nobody would see (the call itself
    only initiates the commit)."""

    def on_commit(_offsets, exception):
        if exception is not None:
            emit(_consumer_error(chpb.CONSUMER_OP_COMMIT, exception))

    return on_commit


def _marker(marker):
    return chpb.WorkloadEvent(marker=chpb.Marker(marker=marker))


def _closing():
    return chpb.WorkloadEvent(consumer_closing=chpb.ConsumerClosing())


def _closed():
    return chpb.WorkloadEvent(consumer_closed=chpb.ConsumerClosed())


def _finished():
    return chpb.WorkloadEvent(finished=chpb.Finished())


def _failed(exc):
    return chpb.WorkloadEvent(failed=chpb.Failed(error=_error(exc)))


def _is_terminal(event):
    return event.WhichOneof("event") in ("finished", "failed")


def _check_commits(request):
    """Whether to read committed offsets back after the revoke-time and final
    commits (both synchronous, so in either commit mode). The periodic
    poll-loop read-back additionally needs COMMIT_MODE_SYNC: an async commit
    may not have reached the broker yet."""
    return request.commit_check_interval_ms > 0


class _ChaosRebalanceListener(ConsumerRebalanceListener):
    """The rebalance listener every synchronous chaos consumer registers
    (tests/chaos/workload.rs ``ChaosRebalanceListener``).

    It reports each callback, and in ``on_partitions_revoked`` commits through
    its consumer -- a listener's call back into its consumer, which the package
    routes through the core's ``ConsumerHandle`` -- then reads the commit back,
    unless the consumer is closing: once closing, the client only drains
    commits, so an offset fetch queued from this callback would wait out the API
    timeout, and the workload's own read-back after its final commit already
    covers that state.

    ``on_partitions_lost`` is implemented rather than left to the binding's
    Java-faithful default (delegate to revoked), so a fenced member's callback
    is reported as lost.

    The methods run on the caller's thread -- here the workload's own thread --
    inside the consumer call that delivers them (``poll()``, ``commit()``,
    ``committed()``, ``close()``), and the rebalance does not advance until they
    return (``ConsumerRebalanceListener``).
    """

    def __init__(self, emit, consumer, check_commits):
        self._emit = emit
        self._consumer = consumer
        self._check_commits = check_commits
        self.closing = False

    def on_partitions_assigned(self, partitions):
        self._emit(_rebalance("assigned", partitions))

    def on_partitions_revoked(self, partitions):
        self._emit(_rebalance("revoked", partitions))
        try:
            self._consumer.commit()
        except Exception as e:  # noqa: BLE001
            self._emit(_consumer_error(chpb.CONSUMER_OP_REVOKE_COMMIT, e))
            return
        if self._check_commits and not self.closing:
            try:
                self._emit_many(_committed_events(self._consumer.committed(partitions=partitions)))
            except Exception as e:  # noqa: BLE001
                self._emit(_consumer_error(chpb.CONSUMER_OP_READ_COMMITTED, e))

    def on_partitions_lost(self, partitions):
        self._emit(_rebalance("lost", partitions))

    def _emit_many(self, events):
        for event in events:
            self._emit(event)


class _AsyncChaosRebalanceListener(_ChaosRebalanceListener):
    """:class:`_ChaosRebalanceListener` for an asyncio chaos consumer.

    The asyncio consumer's ``commit()`` / ``committed()`` are coroutines, so
    ``on_partitions_revoked`` is an ``async def``, which the consumer awaits on
    the event loop; the other two methods only emit and stay plain. The calls
    back into the consumer run synchronously through the core's
    ``ConsumerHandle`` (it has no ``_async`` forms), so they block the loop
    while they run (``AsyncConsumer.subscribe``).
    """

    async def on_partitions_revoked(self, partitions):
        self._emit(_rebalance("revoked", partitions))
        try:
            await self._consumer.commit()
        except Exception as e:  # noqa: BLE001
            self._emit(_consumer_error(chpb.CONSUMER_OP_REVOKE_COMMIT, e))
            return
        if self._check_commits and not self.closing:
            try:
                self._emit_many(_committed_events(
                    await self._consumer.committed(partitions=partitions)))
            except Exception as e:  # noqa: BLE001
                self._emit(_consumer_error(chpb.CONSUMER_OP_READ_COMMITTED, e))


# ---------------------------------------------------------------------------
# Synchronous flavour (grpc_server.py): one thread per workload.
# ---------------------------------------------------------------------------


def _run_producer_sync(request, emit, stop):
    try:
        # No serializers given: keys and values are bytes.
        producer = KafkaProducer(configs=dict(request.config))
    except Exception as e:  # noqa: BLE001
        LOG.exception("chaos producer %s: construction failed", request.workload_id)
        emit(_failed(e))
        return

    topic = request.topic
    msg_size = request.msg_size
    interval = 1.0 / request.target_rps if request.target_rps else 0.0
    outcomes = _RecordOutcomes(emit, request.workload_id)
    started = time.monotonic()
    next_due = started
    index = 0
    try:
        while not stop.is_set():
            # Java's ProducerRecord(topic, key, value): no partition, no timestamp.
            record = ProducerRecord(topic=topic, key=_key(index), value=_value(index, msg_size))
            # Open the record's in-flight window before handing it over; the
            # delivery callback's event closes it.
            emit(_sent(index))
            on_delivery, settlement = outcomes.on_delivery(index)
            try:
                # The returned future is not waited on: the binding reports
                # the outcome through on_delivery (Java's Callback, on its
                # completion thread), which lets it batch and pipeline. send()
                # itself blocks while buffer.memory is exhausted, as it would
                # for any application.
                producer.send(record=record, callback=on_delivery)
            except Exception as e:  # noqa: BLE001
                outcomes.send_raised(index, settlement, e)
            index += 1
            if interval:
                next_due += interval
                now = time.monotonic()
                if next_due > now:
                    stop.wait(next_due - now)
                elif now - next_due > _MAX_SCHEDULE_LAG_S:
                    next_due = now
        emit(_stats(index, max(time.monotonic() - started, 1e-9)))
    except Exception as e:  # noqa: BLE001
        LOG.exception("chaos producer %s: send loop died", request.workload_id)
        _close_quietly(producer.close)
        emit(_failed(e))
        return

    try:
        # Close waits for every buffered record's outcome, so every delivery
        # callback has fired when it returns.
        producer.close()
    except Exception as e:  # noqa: BLE001
        emit(_failed(e))
        return
    emit(_finished())


def _run_consumer_sync(request, emit, stop):
    try:
        # No deserializers given: keys and values are bytes.
        consumer = KafkaConsumer(configs=dict(request.config))
    except Exception as e:  # noqa: BLE001
        LOG.exception("chaos consumer %s: construction failed", request.workload_id)
        emit(_failed(e))
        return

    check_commits = _check_commits(request)
    listener = _ChaosRebalanceListener(emit, consumer, check_commits)
    try:
        consumer.subscribe(topics=list(request.topics), callback=listener)
    except Exception as e:  # noqa: BLE001
        consumer.close()
        emit(_failed(e))
        return

    poll_timeout = request.poll_timeout_ms / 1000.0
    check_interval = request.commit_check_interval_ms / 1000.0
    sync_commit = request.commit_mode == chpb.COMMIT_MODE_SYNC
    on_commit = _commit_callback(emit)
    last_check = time.monotonic()

    def read_back():
        try:
            for event in _committed_events(consumer.committed(partitions=consumer.assignment())):
                emit(event)
        except Exception as e:  # noqa: BLE001
            emit(_consumer_error(chpb.CONSUMER_OP_READ_COMMITTED, e))

    while not stop.is_set():
        try:
            records = consumer.poll(timeout=poll_timeout)
        except Exception as e:  # noqa: BLE001
            emit(_consumer_error(chpb.CONSUMER_OP_POLL, e))
            stop.wait(_POLL_ERROR_BACKOFF_S)
            continue
        if records.is_empty():
            continue
        for event in _consumed_events(records, request.msg_size):
            emit(event)
        try:
            if sync_commit:
                consumer.commit()
            else:
                # commit_nowait (Java's commitAsync) only initiates the
                # commit; its failure arrives through on_commit, on this
                # thread, inside a later poll / commit / close.
                consumer.commit_nowait(callback=on_commit)
        except Exception as e:  # noqa: BLE001
            emit(_consumer_error(chpb.CONSUMER_OP_COMMIT, e))
            continue
        # Read back only after a sync commit: an async one may not have reached
        # the broker yet.
        if sync_commit and check_commits and time.monotonic() - last_check >= check_interval:
            last_check = time.monotonic()
            read_back()

    try:
        consumer.commit()
    except Exception as e:  # noqa: BLE001
        emit(_consumer_error(chpb.CONSUMER_OP_COMMIT, e))
    else:
        if check_commits:
            read_back()
    listener.closing = True
    emit(_closing())
    try:
        # The close-time on_partitions_revoked still commits through the
        # consumer; close() releases the listener afterwards.
        consumer.close()
    except Exception as e:  # noqa: BLE001
        # The consumer's error, not the workload's: it drained and is closed.
        emit(_consumer_error(chpb.CONSUMER_OP_CLOSE, e))
    emit(_closed())
    emit(_finished())


def _close_quietly(close):
    try:
        close()
    except Exception:  # noqa: BLE001
        LOG.exception("chaos: close after a failed loop raised")


class _Registry:
    """workload_id -> (stop signal, emit), shared by the Run*, StopWorkload and
    MarkWorkload RPCs."""

    def __init__(self):
        self._lock = threading.Lock()
        self._workloads = {}

    def add(self, workload_id, stop, emit):
        with self._lock:
            if workload_id in self._workloads:
                return False
            self._workloads[workload_id] = (stop, emit)
            return True

    def remove(self, workload_id):
        with self._lock:
            self._workloads.pop(workload_id, None)

    def get(self, workload_id):
        """The running workload's ``(stop, emit)``, or ``None``."""
        with self._lock:
            return self._workloads.get(workload_id)

    def stop(self, workload_id):
        entry = self.get(workload_id)
        if entry is not None:
            entry[0].set()

    def mark(self, workload_id, marker):
        """Queue a ``Marker`` behind every event the workload queued so far;
        whether it was running."""
        entry = self.get(workload_id)
        if entry is None:
            return False
        entry[1](_marker(marker))
        return True


def _duplicate_id(workload_id):
    return chpb.WorkloadEventBatch(events=[chpb.WorkloadEvent(failed=chpb.Failed(error=pb.KafkaError(
        code=_LOCAL_ILLEGAL_ARGUMENT,
        message=f"python server: workload_id {workload_id!r} is already running")))])


class ChaosWorkloadService(chpb_grpc.ChaosWorkloadServiceServicer):
    """ChaosWorkloadService over the synchronous binding. Each workload runs on
    its own thread; the RPC's worker thread streams its events."""

    def __init__(self):
        self._registry = _Registry()

    def RunProducer(self, request, context):
        return self._run(request, context, _run_producer_sync)

    def RunConsumer(self, request, context):
        return self._run(request, context, _run_consumer_sync)

    def StopWorkload(self, request, context):
        self._registry.stop(request.workload_id)
        return pb.StatusResponse()

    def MarkWorkload(self, request, context):
        # SimpleQueue.put is thread-safe, and FIFO with the workload's own puts.
        return chpb.MarkWorkloadResponse(found=self._registry.mark(request.workload_id, request.marker))

    def _run(self, request, context, loop):
        stop = threading.Event()
        events = queue.SimpleQueue()
        if not self._registry.add(request.workload_id, stop, events.put):
            yield _duplicate_id(request.workload_id)
            return
        # Headers now, not with the first event: the harness's call resolves on
        # them, and a consumer that is never assigned anything may emit nothing
        # for a long time (chaos_service.proto, lifecycle step 1).
        context.send_initial_metadata(())
        # The harness going away (cancelled RPC) stops the workload, which then
        # drains and closes its client on its own thread.
        context.add_callback(stop.set)
        worker = threading.Thread(
            target=self._guarded, args=(loop, request, events.put, stop),
            name=f"chaos-{request.workload_id}", daemon=True)
        worker.start()
        try:
            while True:
                batch = [events.get()]
                while len(batch) < _MAX_BATCH:
                    try:
                        batch.append(events.get_nowait())
                    except queue.Empty:
                        break
                yield chpb.WorkloadEventBatch(events=batch)
                if any(_is_terminal(e) for e in batch):
                    return
        finally:
            stop.set()
            self._registry.remove(request.workload_id)

    @staticmethod
    def _guarded(loop, request, emit, stop):
        try:
            loop(request, emit, stop)
        except Exception as e:  # noqa: BLE001  -- never leave the stream open
            LOG.exception("chaos workload %s died", request.workload_id)
            emit(_failed(e))


# ---------------------------------------------------------------------------
# asyncio flavour (grpc_server_async.py): one task per workload on the loop.
# ---------------------------------------------------------------------------


def _loop_emitter(loop, events):
    """An ``emit`` usable from the event loop and from any other thread.

    With the asyncio client every callback runs on the loop: delivery
    callbacks, the rebalance listener (inside the awaited ``poll()`` /
    ``commit()`` / ``committed()`` / ``close()`` that delivers it) and the
    ``commit_nowait`` callback (inside a later awaited call). Those queue
    directly, in the order they ran, so listener events are queued before the
    records of the poll that delivered them. An emit from another thread hops
    onto the loop with ``call_soon_threadsafe``, which keeps FIFO order too.
    """
    loop_thread = threading.get_ident()

    def emit(event):
        if threading.get_ident() == loop_thread:
            events.put_nowait(event)
        elif not loop.is_closed():
            loop.call_soon_threadsafe(events.put_nowait, event)

    return emit


async def _wait(stop, timeout):
    """Sleep up to ``timeout`` seconds, returning early when ``stop`` is set."""
    try:
        await asyncio.wait_for(stop.wait(), timeout)
    except asyncio.TimeoutError:
        pass


async def _run_producer_async(request, emit, stop):
    try:
        # No serializers given: keys and values are bytes.
        producer = AsyncKafkaProducer(configs=dict(request.config))
    except Exception as e:  # noqa: BLE001
        LOG.exception("chaos producer %s: construction failed", request.workload_id)
        emit(_failed(e))
        return

    topic = request.topic
    msg_size = request.msg_size
    interval = 1.0 / request.target_rps if request.target_rps else 0.0
    outcomes = _RecordOutcomes(emit, request.workload_id)
    started = time.monotonic()
    next_due = started
    index = 0
    try:
        while not stop.is_set():
            # Java's ProducerRecord(topic, key, value): no partition, no timestamp.
            record = ProducerRecord(topic=topic, key=_key(index), value=_value(index, msg_size))
            emit(_sent(index))
            on_delivery, settlement = outcomes.on_delivery(index)
            try:
                # Awaits only buffer capacity; the asyncio.Future it returns
                # is not awaited, the outcome arrives through on_delivery (on
                # the loop).
                await producer.send(record=record, callback=on_delivery)
            except Exception as e:  # noqa: BLE001
                outcomes.send_raised(index, settlement, e)
            index += 1
            if interval:
                next_due += interval
                now = time.monotonic()
                if next_due > now:
                    # At most one interval: the stop check next iteration is
                    # soon enough, and a plain sleep is cheaper per record than
                    # waiting on the stop event.
                    await asyncio.sleep(next_due - now)
                    continue
                if now - next_due > _MAX_SCHEDULE_LAG_S:
                    next_due = now
            # Behind schedule or unlimited: still yield regularly, so
            # completions, StopWorkload and the other workloads on this loop
            # get a turn (see _ASYNC_YIELD_EVERY).
            if index % _ASYNC_YIELD_EVERY == 0:
                await asyncio.sleep(0)
        emit(_stats(index, max(time.monotonic() - started, 1e-9)))
    except Exception as e:  # noqa: BLE001
        LOG.exception("chaos producer %s: send loop died", request.workload_id)
        try:
            await producer.close()
        except Exception:  # noqa: BLE001
            LOG.exception("chaos: close after a failed loop raised")
        emit(_failed(e))
        return

    try:
        await producer.close()
    except Exception as e:  # noqa: BLE001
        emit(_failed(e))
        return
    emit(_finished())


async def _run_consumer_async(request, emit, stop):
    try:
        # No deserializers given: keys and values are bytes.
        consumer = AsyncKafkaConsumer(configs=dict(request.config))
    except Exception as e:  # noqa: BLE001
        LOG.exception("chaos consumer %s: construction failed", request.workload_id)
        emit(_failed(e))
        return

    check_commits = _check_commits(request)
    listener = _AsyncChaosRebalanceListener(emit, consumer, check_commits)
    try:
        await consumer.subscribe(topics=list(request.topics), callback=listener)
    except Exception as e:  # noqa: BLE001
        await consumer.close()
        emit(_failed(e))
        return

    poll_timeout = request.poll_timeout_ms / 1000.0
    check_interval = request.commit_check_interval_ms / 1000.0
    sync_commit = request.commit_mode == chpb.COMMIT_MODE_SYNC
    # A plain function: it runs on the loop, inside a later awaited call.
    on_commit = _commit_callback(emit)
    last_check = time.monotonic()

    async def read_back():
        try:
            for event in _committed_events(
                    await consumer.committed(partitions=consumer.assignment())):
                emit(event)
        except Exception as e:  # noqa: BLE001
            emit(_consumer_error(chpb.CONSUMER_OP_READ_COMMITTED, e))

    while not stop.is_set():
        try:
            records = await consumer.poll(timeout=poll_timeout)
        except Exception as e:  # noqa: BLE001
            emit(_consumer_error(chpb.CONSUMER_OP_POLL, e))
            await _wait(stop, _POLL_ERROR_BACKOFF_S)
            continue
        if records.is_empty():
            continue
        for event in _consumed_events(records, request.msg_size):
            emit(event)
        try:
            if sync_commit:
                await consumer.commit()
            else:
                # commit_nowait (Java's commitAsync) is a plain def: it only
                # initiates the commit.
                consumer.commit_nowait(callback=on_commit)
        except Exception as e:  # noqa: BLE001
            emit(_consumer_error(chpb.CONSUMER_OP_COMMIT, e))
            continue
        if sync_commit and check_commits and time.monotonic() - last_check >= check_interval:
            last_check = time.monotonic()
            await read_back()

    try:
        await consumer.commit()
    except Exception as e:  # noqa: BLE001
        emit(_consumer_error(chpb.CONSUMER_OP_COMMIT, e))
    else:
        if check_commits:
            await read_back()
    listener.closing = True
    emit(_closing())
    try:
        # As in the sync flavour: the close-time on_partitions_revoked still
        # commits through the consumer; close() releases the listener after.
        await consumer.close()
    except Exception as e:  # noqa: BLE001
        # The consumer's error, not the workload's: it drained and is closed.
        emit(_consumer_error(chpb.CONSUMER_OP_CLOSE, e))
    emit(_closed())
    emit(_finished())


class AsyncChaosWorkloadService(chpb_grpc.ChaosWorkloadServiceServicer):
    """ChaosWorkloadService over the asyncio binding. Each workload is a task on
    the server's event loop; the RPC streams its events."""

    def __init__(self):
        self._registry = _Registry()

    async def RunProducer(self, request, context):
        async for batch in self._run(request, context, _run_producer_async):
            yield batch

    async def RunConsumer(self, request, context):
        async for batch in self._run(request, context, _run_consumer_async):
            yield batch

    async def StopWorkload(self, request, context):
        self._registry.stop(request.workload_id)
        return pb.StatusResponse()

    async def MarkWorkload(self, request, context):
        # On the loop thread, so emit queues the marker directly, FIFO with
        # the workload's events (those from the binding's threads hop onto the
        # loop first, in the order the binding produced them).
        return chpb.MarkWorkloadResponse(found=self._registry.mark(request.workload_id, request.marker))

    async def _run(self, request, context, loop_fn):
        stop = asyncio.Event()
        events = asyncio.Queue()
        emit = _loop_emitter(asyncio.get_running_loop(), events)
        if not self._registry.add(request.workload_id, stop, emit):
            yield _duplicate_id(request.workload_id)
            return
        # Headers now, not with the first event; see ChaosWorkloadService._run.
        await context.send_initial_metadata(())
        task = asyncio.create_task(self._guarded(loop_fn, request, emit, stop))
        try:
            while True:
                batch = [await events.get()]
                while len(batch) < _MAX_BATCH and not events.empty():
                    batch.append(events.get_nowait())
                yield chpb.WorkloadEventBatch(events=batch)
                if any(_is_terminal(e) for e in batch):
                    return
        finally:
            # Normal end, or the harness went away (the generator is closed or
            # cancelled): either way the workload stops, and it still drains and
            # closes its client, so wait for it without letting a cancellation
            # of this RPC cut that short.
            stop.set()
            self._registry.remove(request.workload_id)
            await asyncio.shield(task)

    @staticmethod
    async def _guarded(loop_fn, request, emit, stop):
        try:
            await loop_fn(request, emit, stop)
        except Exception as e:  # noqa: BLE001  -- never leave the stream open
            LOG.exception("chaos workload %s died", request.workload_id)
            emit(_failed(e))
