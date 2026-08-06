"""Pythonic Kafka admin client over the Rust admin C FFI.

Mirrors the Java ``Admin`` interface with Python naming, in both a synchronous
(:class:`AdminClient`, :class:`MockAdminClient`) and an asyncio-native
(:class:`AsyncAdminClient`, :class:`AsyncMockAdminClient`) form.

Design notes
------------
* The C extension (``_confluentkafka``) is a marshaling layer only: it converts
  Python objects to/from the C FFI and bridges the FFI's async callbacks back
  into Python. All orchestration lives here in pure Python.
* **Both APIs drive the async C bindings.** Even the synchronous methods submit
  an async FFI op and then wait on an interruptible Python primitive, so a slow
  admin RPC never parks the calling thread inside a native ``block_on`` where
  Python signal handlers cannot run.
* Unlike the consumer, the admin client has no ``wakeup()``: an in-flight RPC
  cannot be aborted. On ``KeyboardInterrupt`` the sync waiter therefore keeps
  waiting for the callback (so the result handles are freed rather than leaked)
  and then re-raises. This matches Java, where interrupting
  ``KafkaFuture.get()`` does not cancel the underlying admin request.
* The Rust admin client is thread-safe (Java's ``KafkaAdminClient`` is too), so
  concurrent calls from several threads or tasks are allowed — there is no
  single-owner guard as on the consumer.

Per-key results
---------------
Java returns one ``KafkaFuture<T>`` per key (per topic for ``createTopics``).
C has no ``KafkaFuture``, so each RPC delivers one flattened result handle
carrying a value *and* an error per key, and this module drains it into a plain
dict whose values are either the result object or a :class:`KafkaError`:

* ``create_topics`` -> ``{topic_name: TopicMetadataAndConfig | KafkaError}``
* ``delete_topics`` -> ``{topic_name: None | KafkaError}`` (Java's per-key
  future is ``KafkaFuture<Void>``, so ``None`` means success)
* ``describe_topics`` -> ``{topic_name: TopicDescription | KafkaError}``
* ``list_topics`` -> ``{topic_name: TopicListing}`` (Java has a single future
  here, so a failure raises instead of appearing per key)

A per-key failure therefore does **not** raise; iterate the dict and check for
``KafkaError`` values. Only a whole-call failure raises.
"""

import asyncio
import datetime as _dt
import threading

import _confluentkafka as _lib
from consumer import Node  # shared broker-node type
from producer import KafkaError  # shared error type


# --------------------------------------------------------------------------
# Supporting value types (mirror the Java admin types).
# --------------------------------------------------------------------------
class NewTopic:
    """A request to create a new topic (Java ``NewTopic``).

    Pass ``num_partitions`` / ``replication_factor`` as ``None`` to use the
    broker's ``num.partitions`` / ``default.replication.factor``.

    Setting ``replicas_assignments`` selects Java's
    ``NewTopic(name, Map<Integer, List<Integer>>)`` constructor, in which
    ``num_partitions`` and ``replication_factor`` are not sent.
    """

    __slots__ = ("name", "num_partitions", "replication_factor", "configs",
                 "replicas_assignments")

    def __init__(self, name, num_partitions=None, replication_factor=None,
                 configs=None, replicas_assignments=None):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.configs = dict(configs) if configs else {}
        self.replicas_assignments = (dict(replicas_assignments)
                                     if replicas_assignments else {})

    def _to_spec(self):
        """The tuple shape the C layer parses (see ``build_new_topics``)."""
        return (
            self.name,
            -1 if self.num_partitions is None else int(self.num_partitions),
            -1 if self.replication_factor is None else int(self.replication_factor),
            [(str(k), str(v)) for k, v in self.configs.items()],
            [(int(p), [int(b) for b in brokers])
             for p, brokers in self.replicas_assignments.items()],
        )

    def __repr__(self):
        return (f"NewTopic(name={self.name!r}, num_partitions={self.num_partitions}, "
                f"replication_factor={self.replication_factor})")


class ConfigEntry:
    """A topic configuration entry (Java ``ConfigEntry``).

    ``create_topics`` is the only B1 RPC that carries configs and the broker's
    ``CreateTopicsResponse`` populates only these fields; ``synonyms`` /
    ``config_type`` / ``documentation`` arrive with ``describe_configs``.
    """

    __slots__ = ("name", "value", "is_default", "is_sensitive", "is_read_only")

    def __init__(self, name, value, is_default, is_sensitive, is_read_only):
        self.name = name
        self.value = value
        self.is_default = is_default
        self.is_sensitive = is_sensitive
        self.is_read_only = is_read_only

    def __repr__(self):
        return f"ConfigEntry(name={self.name!r}, value={self.value!r})"


class TopicMetadataAndConfig:
    """Metadata for one created topic (Java
    ``CreateTopicsResult.TopicMetadataAndConfig``).

    If the broker created the topic but returned no metadata, ``error`` holds
    the exception Java's ``ensureSuccess()`` would rethrow from every accessor,
    ``topic_id`` is empty and the numeric fields are ``-1``. That is distinct
    from a per-key creation failure, which appears as a :class:`KafkaError`
    *instead of* this object.
    """

    __slots__ = ("topic_id", "num_partitions", "replication_factor", "configs", "error")

    def __init__(self, topic_id, num_partitions, replication_factor, configs, error):
        self.topic_id = topic_id
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.configs = configs
        self.error = error

    def __repr__(self):
        return (f"TopicMetadataAndConfig(topic_id={self.topic_id!r}, "
                f"num_partitions={self.num_partitions}, "
                f"replication_factor={self.replication_factor})")


class TopicListing:
    """A topic returned by ``list_topics`` (Java ``TopicListing``)."""

    __slots__ = ("name", "topic_id", "is_internal")

    def __init__(self, name, topic_id, is_internal):
        self.name = name
        self.topic_id = topic_id
        self.is_internal = is_internal

    def __repr__(self):
        return (f"TopicListing(name={self.name!r}, topic_id={self.topic_id!r}, "
                f"is_internal={self.is_internal})")


class TopicPartitionInfo:
    """One partition of a described topic (Java ``TopicPartitionInfo``).

    ``elr`` / ``last_known_elr`` are ``None`` when the broker did not report the
    eligible-leader-replica sets (Java returns null), and an empty list when it
    reported them empty.
    """

    __slots__ = ("partition", "leader", "replicas", "isr", "elr", "last_known_elr")

    def __init__(self, partition, leader, replicas, isr, elr, last_known_elr):
        self.partition = partition
        self.leader = leader
        self.replicas = replicas
        self.isr = isr
        self.elr = elr
        self.last_known_elr = last_known_elr

    def __repr__(self):
        return (f"TopicPartitionInfo(partition={self.partition}, "
                f"leader={self.leader!r})")


class TopicDescription:
    """A described topic (Java ``TopicDescription``).

    ``authorized_operations`` holds ``AclOperation`` wire codes (Java's
    ``AclOperation.code()``); it is empty unless the request asked for them.
    """

    __slots__ = ("name", "topic_id", "is_internal", "partitions", "authorized_operations")

    def __init__(self, name, topic_id, is_internal, partitions, authorized_operations):
        self.name = name
        self.topic_id = topic_id
        self.is_internal = is_internal
        self.partitions = partitions
        self.authorized_operations = authorized_operations

    def __repr__(self):
        return (f"TopicDescription(name={self.name!r}, topic_id={self.topic_id!r}, "
                f"partitions={len(self.partitions)})")


# --------------------------------------------------------------------------
# Conversions from the C drain dicts to the Python value types.
# --------------------------------------------------------------------------
def _to_error(raw):
    """``(code, message, is_retriable, is_fatal)`` -> KafkaError, or None."""
    return None if raw is None else KafkaError._from_parts(*raw)


def _to_node(n):
    return None if n is None else Node(n[0], n[1], n[2], n[3])


def _to_config_entry(raw):
    name, value, is_default, is_sensitive, is_read_only = raw
    return ConfigEntry(name, value, bool(is_default), bool(is_sensitive),
                       bool(is_read_only))


def _to_metadata(raw):
    topic_id, num_partitions, replication_factor, configs, error = raw
    return TopicMetadataAndConfig(topic_id, num_partitions, replication_factor,
                                  [_to_config_entry(c) for c in configs],
                                  _to_error(error))


def _to_create_topics(raw):
    """{name: (error, metadata)} -> {name: TopicMetadataAndConfig | KafkaError}"""
    out = {}
    for name, (error, metadata) in raw.items():
        out[name] = _to_error(error) if error is not None else _to_metadata(metadata)
    return out


def _to_delete_topics(raw):
    """{key: error} -> {key: None | KafkaError}"""
    return {key: _to_error(error) for key, error in raw.items()}


def _to_list_topics(raw):
    """{name: (name, topic_id, is_internal)} -> {name: TopicListing}"""
    return {name: TopicListing(n, topic_id, bool(is_internal))
            for name, (n, topic_id, is_internal) in raw.items()}


def _to_partition_info(raw):
    partition, leader, replicas, isr, elr, last_known_elr = raw
    return TopicPartitionInfo(
        partition,
        _to_node(leader),
        [_to_node(n) for n in replicas],
        [_to_node(n) for n in isr],
        None if elr is None else [_to_node(n) for n in elr],
        None if last_known_elr is None else [_to_node(n) for n in last_known_elr],
    )


def _to_description(raw):
    name, topic_id, is_internal, partitions, operations = raw
    return TopicDescription(name, topic_id, bool(is_internal),
                            [_to_partition_info(p) for p in partitions],
                            list(operations))


def _to_describe_topics(raw):
    """{key: (error, description)} -> {key: TopicDescription | KafkaError}"""
    out = {}
    for key, (error, description) in raw.items():
        out[key] = _to_error(error) if error is not None else _to_description(description)
    return out


def _ms(timeout):
    """Convert a timeout (seconds float, ``timedelta``, or None) to int32 ms.

    ``None`` becomes -1, which leaves the RPC's ``timeoutMs`` option unset so the
    client's ``default.api.timeout.ms`` applies (Java passes a null timeout).
    """
    if timeout is None:
        return -1
    if isinstance(timeout, _dt.timedelta):
        return int(timeout.total_seconds() * 1000)
    return int(float(timeout) * 1000)


def _close_ms(timeout):
    """Convert a close timeout to int64 ms; ``None`` becomes -1, which the C
    layer maps to Java's no-argument ``close()`` (wait indefinitely)."""
    if timeout is None:
        return -1
    if isinstance(timeout, _dt.timedelta):
        return int(timeout.total_seconds() * 1000)
    return int(float(timeout) * 1000)


# --------------------------------------------------------------------------
# Shared base: handle ownership and the per-method (submit, resolve, free)
# specs driven by _run_sync / _run_async.
# --------------------------------------------------------------------------
class _AdminBase:
    def __init__(self):
        self._h = None
        self.closed = False

    def _init_mock(self, num_brokers=1):
        self._h = _lib.Admin_MockAdminClient_new(num_brokers)

    def _init_kafka(self, config):
        self._h = _lib.Admin_AdminClient_new(config)

    def _check_closed(self):
        if self.closed:
            raise RuntimeError("AdminClient is already closed")

    def _destroy(self):
        if self._h is not None:
            _lib.Admin_destroy(self._h)
            self._h = None

    # ---- resolve / free pairs (by callback payload shape) ------------------
    @staticmethod
    def _resolve_void(payload):
        (error,) = payload
        if error:
            raise KafkaError._from_c(error)
        return None

    @staticmethod
    def _free_void(payload):
        if payload[0]:
            _lib.KafkaError_destroy(payload[0])

    @staticmethod
    def _resolve_value(drain, convert):
        def resolve(payload):
            handle, error = payload
            if error:
                raise KafkaError._from_c(error)
            return convert(drain(handle))
        return resolve

    @staticmethod
    def _free_value(drain):
        def free(payload):
            handle, error = payload
            if error:
                _lib.KafkaError_destroy(error)
            if handle:
                drain(handle)  # drain destroys the handle
        return free

    # ---- per-method specs: (submit, resolve, free) -------------------------
    def _close_spec(self, timeout):
        ms = _close_ms(timeout)
        return (lambda cb: _lib.Admin_close_async(self._h, ms, cb),
                self._resolve_void, self._free_void)

    def _create_topics_spec(self, new_topics, timeout, validate_only,
                            retry_on_quota_violation):
        spec = [t._to_spec() for t in new_topics]
        ms = _ms(timeout)
        drain = _lib.CreateTopicsResult_drain
        return (lambda cb: _lib.Admin_create_topics_async(
                    self._h, spec, ms, bool(validate_only),
                    bool(retry_on_quota_violation), cb),
                self._resolve_value(drain, _to_create_topics),
                self._free_value(drain))

    def _delete_topics_spec(self, topics, timeout, retry_on_quota_violation, by_ids):
        names = [str(t) for t in topics]
        ms = _ms(timeout)
        fn = (_lib.Admin_delete_topics_by_ids_async if by_ids
              else _lib.Admin_delete_topics_async)
        drain = _lib.DeleteTopicsResult_drain
        return (lambda cb: fn(self._h, names, ms, bool(retry_on_quota_violation), cb),
                self._resolve_value(drain, _to_delete_topics),
                self._free_value(drain))

    def _list_topics_spec(self, timeout, list_internal):
        ms = _ms(timeout)
        drain = _lib.ListTopicsResult_drain
        return (lambda cb: _lib.Admin_list_topics_async(
                    self._h, ms, bool(list_internal), cb),
                self._resolve_value(drain, _to_list_topics),
                self._free_value(drain))

    def _describe_topics_spec(self, topics, timeout, include_authorized_operations,
                              partition_size_limit, by_ids):
        names = [str(t) for t in topics]
        ms = _ms(timeout)
        limit = -1 if partition_size_limit is None else int(partition_size_limit)
        fn = (_lib.Admin_describe_topics_by_ids_async if by_ids
              else _lib.Admin_describe_topics_async)
        drain = _lib.DescribeTopicsResult_drain
        return (lambda cb: fn(self._h, names, ms,
                              bool(include_authorized_operations), limit, cb),
                self._resolve_value(drain, _to_describe_topics),
                self._free_value(drain))


class _MockAdminClientMixin:
    """Mock-only operations (test helper)."""

    def timeout_next_request(self, number_of_requests):
        """Make the next ``number_of_requests`` RPCs fail with a timeout
        (Java ``MockAdminClient.timeoutNextRequest``)."""
        e = _lib.MockAdminClient_timeout_next_request(self._h, number_of_requests)
        if e:
            raise KafkaError._from_c(e)


# --------------------------------------------------------------------------
# Synchronous API.
# --------------------------------------------------------------------------
class Admin(_AdminBase):
    """A synchronous Kafka admin client. Every RPC submits an async FFI op and
    waits on an interruptible event, so ``KeyboardInterrupt`` is honored
    promptly (though the request itself is not cancelled — see module
    docstring)."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _run_sync(self, submit, resolve, free):
        box = {}
        done = threading.Event()

        def cb(*payload):
            box["payload"] = payload
            done.set()

        submit(cb)
        interrupted = None
        while True:
            try:
                # Short slices keep the main-thread eval loop reachable so a
                # pending signal raises KeyboardInterrupt here rather than after
                # the whole op completes.
                while not done.wait(0.1):
                    pass
                break
            except KeyboardInterrupt as exc:
                interrupted = exc
                # There is no admin wakeup(): keep waiting for the callback so
                # the result handles are freed rather than leaked.
        payload = box["payload"]
        if interrupted is not None:
            free(payload)
            raise interrupted
        return resolve(payload)

    def create_topics(self, new_topics, timeout=None, validate_only=False,
                      retry_on_quota_violation=True):
        """Create topics. Returns
        ``{topic_name: TopicMetadataAndConfig | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._create_topics_spec(
            new_topics, timeout, validate_only, retry_on_quota_violation))

    def delete_topics(self, topics, timeout=None, retry_on_quota_violation=True):
        """Delete topics by name. Returns ``{topic_name: None | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._delete_topics_spec(
            topics, timeout, retry_on_quota_violation, by_ids=False))

    def delete_topics_by_ids(self, topic_ids, timeout=None,
                             retry_on_quota_violation=True):
        """Delete topics by base64 topic id. Returns
        ``{topic_id: None | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._delete_topics_spec(
            topic_ids, timeout, retry_on_quota_violation, by_ids=True))

    def list_topics(self, timeout=None, list_internal=False):
        """List topics. Returns ``{topic_name: TopicListing}``."""
        self._check_closed()
        return self._run_sync(*self._list_topics_spec(timeout, list_internal))

    def describe_topics(self, topics, timeout=None,
                        include_authorized_operations=False,
                        partition_size_limit=None):
        """Describe topics by name. Returns
        ``{topic_name: TopicDescription | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._describe_topics_spec(
            topics, timeout, include_authorized_operations, partition_size_limit,
            by_ids=False))

    def describe_topics_by_ids(self, topic_ids, timeout=None,
                               include_authorized_operations=False,
                               partition_size_limit=None):
        """Describe topics by base64 topic id. Returns
        ``{topic_id: TopicDescription | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._describe_topics_spec(
            topic_ids, timeout, include_authorized_operations, partition_size_limit,
            by_ids=True))

    def close(self, timeout=None):
        if self.closed:
            return
        self.closed = True
        try:
            self._run_sync(*self._close_spec(timeout))
        finally:
            self._destroy()


# --------------------------------------------------------------------------
# Asyncio-native API.
# --------------------------------------------------------------------------
class AsyncAdmin(_AdminBase):
    """An asyncio-native Kafka admin client. Every RPC is a coroutine that
    submits an async FFI op and ``await``s its completion on the event loop."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @staticmethod
    def _deliver(fut, payload, free):
        # Runs on the event loop thread.
        if fut.cancelled() or fut.done():
            free(payload)
            return
        fut.set_result(payload)

    async def _run_async(self, submit, resolve, free):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def cb(*payload):
            # Runs on the Rust dispatcher thread with the GIL held. asyncio
            # futures must be touched only on the loop thread.
            if loop.is_closed():
                free(payload)
                return
            loop.call_soon_threadsafe(self._deliver, fut, payload, free)

        submit(cb)
        # On cancellation the late callback frees the handles via _deliver (the
        # future is cancelled by then). There is no admin wakeup(), so the
        # request itself keeps running — as in Java.
        payload = await fut
        return resolve(payload)

    async def create_topics(self, new_topics, timeout=None, validate_only=False,
                            retry_on_quota_violation=True):
        self._check_closed()
        return await self._run_async(*self._create_topics_spec(
            new_topics, timeout, validate_only, retry_on_quota_violation))

    async def delete_topics(self, topics, timeout=None, retry_on_quota_violation=True):
        self._check_closed()
        return await self._run_async(*self._delete_topics_spec(
            topics, timeout, retry_on_quota_violation, by_ids=False))

    async def delete_topics_by_ids(self, topic_ids, timeout=None,
                                   retry_on_quota_violation=True):
        self._check_closed()
        return await self._run_async(*self._delete_topics_spec(
            topic_ids, timeout, retry_on_quota_violation, by_ids=True))

    async def list_topics(self, timeout=None, list_internal=False):
        self._check_closed()
        return await self._run_async(*self._list_topics_spec(timeout, list_internal))

    async def describe_topics(self, topics, timeout=None,
                              include_authorized_operations=False,
                              partition_size_limit=None):
        self._check_closed()
        return await self._run_async(*self._describe_topics_spec(
            topics, timeout, include_authorized_operations, partition_size_limit,
            by_ids=False))

    async def describe_topics_by_ids(self, topic_ids, timeout=None,
                                     include_authorized_operations=False,
                                     partition_size_limit=None):
        self._check_closed()
        return await self._run_async(*self._describe_topics_spec(
            topic_ids, timeout, include_authorized_operations, partition_size_limit,
            by_ids=True))

    async def close(self, timeout=None):
        if self.closed:
            return
        self.closed = True
        try:
            await self._run_async(*self._close_spec(timeout))
        finally:
            self._destroy()


# --------------------------------------------------------------------------
# Concrete variants.
# --------------------------------------------------------------------------
class AdminClient(Admin):
    """A synchronous admin client connected to a real cluster.

    Args:
        config: dict of configuration properties (e.g. ``bootstrap.servers``).
    """

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class MockAdminClient(_MockAdminClientMixin, Admin):
    """A synchronous, broker-less admin client for tests (Java
    ``MockAdminClient``)."""

    def __init__(self, num_brokers=1):
        super().__init__()
        self._init_mock(num_brokers)


class AsyncAdminClient(AsyncAdmin):
    """An asyncio-native admin client connected to a real cluster."""

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class AsyncMockAdminClient(_MockAdminClientMixin, AsyncAdmin):
    """An asyncio-native, broker-less admin client for tests."""

    def __init__(self, num_brokers=1):
        super().__init__()
        self._init_mock(num_brokers)
