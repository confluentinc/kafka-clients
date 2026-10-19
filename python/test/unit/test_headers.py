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

"""``Header`` and ``Headers``: ``RecordHeadersTest.java`` (Apache Kafka 4.4.0,
``clients/src/test/java/org/apache/kafka/common/header/internals``), every
``@Test`` in Java order, then ``RecordHeader``'s behaviour, the headers of the
records, and the headers the C extension builds for a fetched record.

Kafka 4.4.0 has no ``RecordHeaderTest.java``: ``RecordHeader``'s equality,
hash, ``toString`` and null key are covered below from its source. Java's
``RecordHeaders.setReadOnly()`` / ``isReadOnly()`` are not on the ``Headers``
interface; the client calls them as ``_set_read_only()`` /
``_is_read_only()``, and these tests do too. Java's ``Iterator.remove()`` has
no Python counterpart (a Python iterator only iterates), so the checks Java
makes through it are made on the headers' own methods instead, as each test
says.
"""

from __future__ import annotations

import copy
import pickle
import threading
from collections.abc import Callable
from typing import Any

import pytest

import _confluentkafka as _lib  # type: ignore[import-not-found]
from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka.common import Header, Headers, TimestampType, TopicPartition
from confluent_kafka.common.errors import RecordDeserializationError
from confluent_kafka.consumer import ConsumerRecord
from confluent_kafka.consumer._poll import deserialize_batch
from confluent_kafka.producer import MockProducer, ProducerRecord
from confluent_kafka.common.serialization import bytes_serializer


def _caller(deserializer: Any) -> Any:
    """The positional caller the consumer calls ``deserializer`` through
    (``_kwargs.serde_caller``), as ``deserialize_batch`` expects."""
    from confluent_kafka._kwargs import DESERIALIZER_NAMES, serde_caller

    return serde_caller(deserializer, DESERIALIZER_NAMES)


def _get_count(headers: Headers) -> int:
    return len(headers.to_array())


def _assert_header(key: str, value: str, actual: Header | None) -> None:
    assert actual is not None
    assert actual.key() == key
    assert bytes(actual.value() or b"") == value.encode()


# --------------------------------------------------------------------------- #
# RecordHeadersTest
# --------------------------------------------------------------------------- #


def test_add() -> None:
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))

    header = next(iter(headers))
    _assert_header("key", "value", header)

    headers.add(header=Header(key="key2", value=b"value2"))

    _assert_header("key2", "value2", headers.last_header(key="key2"))
    assert _get_count(headers) == 2


def test_add_headers_preserve_order() -> None:
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))
    headers.add(header=Header(key="key2", value=b"value2"))
    headers.add(header=Header(key="key3", value=b"value3"))

    headers_arr = headers.to_array()
    _assert_header("key", "value", headers_arr[0])
    _assert_header("key2", "value2", headers_arr[1])
    _assert_header("key3", "value3", headers_arr[2])

    assert _get_count(headers) == 3


def test_remove() -> None:
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))

    assert next(iter(headers), None) is not None

    headers.remove(key="key")

    assert next(iter(headers), None) is None


def test_preserve_order_after_remove() -> None:
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))
    headers.add(header=Header(key="key2", value=b"value2"))
    headers.add(header=Header(key="key3", value=b"value3"))

    headers.remove(key="key")
    headers_arr = headers.to_array()
    _assert_header("key2", "value2", headers_arr[0])
    _assert_header("key3", "value3", headers_arr[1])
    assert _get_count(headers) == 2

    headers.add(header=Header(key="key4", value=b"value4"))
    headers.remove(key="key3")
    headers_arr = headers.to_array()
    _assert_header("key2", "value2", headers_arr[0])
    _assert_header("key4", "value4", headers_arr[1])
    assert _get_count(headers) == 2


def test_add_remove_interleaved() -> None:
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))
    headers.add(header=Header(key="key2", value=b"value2"))

    assert next(iter(headers), None) is not None

    headers.remove(key="key")

    assert _get_count(headers) == 1

    headers.add(header=Header(key="key3", value=b"value3"))

    assert headers.last_header(key="key") is None

    _assert_header("key2", "value2", headers.last_header(key="key2"))

    _assert_header("key3", "value3", headers.last_header(key="key3"))

    assert _get_count(headers) == 2

    headers.remove(key="key2")

    assert headers.last_header(key="key") is None

    assert headers.last_header(key="key2") is None

    _assert_header("key3", "value3", headers.last_header(key="key3"))

    assert _get_count(headers) == 1

    headers.add(header=Header(key="key3", value=b"value4"))

    _assert_header("key3", "value4", headers.last_header(key="key3"))

    assert _get_count(headers) == 2

    headers.add(header=Header(key="key", value=b"valueNew"))

    assert _get_count(headers) == 3

    _assert_header("key", "valueNew", headers.last_header(key="key"))

    headers.remove(key="key3")

    assert _get_count(headers) == 1

    assert headers.last_header(key="key2") is None

    headers.remove(key="key")

    assert next(iter(headers), None) is None


def test_last_header() -> None:
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))
    headers.add(header=Header(key="key", value=b"value2"))
    headers.add(header=Header(key="key", value=b"value3"))

    _assert_header("key", "value3", headers.last_header(key="key"))
    assert _get_count(headers) == 3


def test_headers_iterator_remove() -> None:
    # Java: the iterator of headers(key) refuses remove() with
    # UnsupportedOperationException. Here headers(key) is a list (Java's
    # output Iterable, CLAUDE.md Types): changing it leaves the headers as
    # they are.
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))

    key_headers = headers.headers(key="key")
    key_headers.pop()
    assert _get_count(headers) == 1
    _assert_header("key", "value", headers.last_header(key="key"))


def test_read_only() -> None:
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))
    header_iterator_before_close = iter(headers)
    headers._set_read_only()  # noqa: SLF001

    with pytest.raises(IllegalStateError) as exc:
        headers.add(header=Header(key="key", value=b"value"))
    assert str(exc.value) == "RecordHeaders has been closed."

    with pytest.raises(IllegalStateError) as exc:
        headers.remove(key="key")
    assert str(exc.value) == "RecordHeaders has been closed."

    # Java's two iterator.remove() checks: an iterator has no remove() here;
    # iterating stays possible, before and after the close, and the headers
    # keep their header.
    header_iterator = iter(headers)
    _assert_header("key", "value", next(header_iterator))
    _assert_header("key", "value", next(header_iterator_before_close))
    assert _get_count(headers) == 1


def test_headers() -> None:
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))
    headers.add(header=Header(key="key1", value=b"key1value"))
    headers.add(header=Header(key="key", value=b"value2"))
    headers.add(header=Header(key="key2", value=b"key2value"))

    key_headers = iter(headers.headers(key="key"))
    _assert_header("key", "value", next(key_headers))
    _assert_header("key", "value2", next(key_headers))
    assert next(key_headers, None) is None

    key_headers = iter(headers.headers(key="key1"))
    _assert_header("key1", "key1value", next(key_headers))
    assert next(key_headers, None) is None

    key_headers = iter(headers.headers(key="key2"))
    _assert_header("key2", "key2value", next(key_headers))
    assert next(key_headers, None) is None


def test_new() -> None:
    headers = Headers()
    headers.add(header=Header(key="key", value=b"value"))
    headers._set_read_only()  # noqa: SLF001

    new_headers = Headers(headers=headers)
    new_headers.add(header=Header(key="key", value=b"value2"))

    # Ensure existing headers are not modified
    _assert_header("key", "value", headers.last_header(key="key"))
    assert _get_count(headers) == 1

    # Ensure new headers are modified
    _assert_header("key", "value2", new_headers.last_header(key="key"))
    assert _get_count(new_headers) == 2


def test_should_throw_npe_when_adding_null_header() -> None:
    record_headers = Headers()
    with pytest.raises(TypeError) as exc:
        record_headers.add(header=None)  # type: ignore[call-overload]
    assert str(exc.value) == "header must not be None"


def test_should_throw_npe_when_adding_collection_with_null_header() -> None:
    # Java's `new RecordHeaders(new Header[1])`: the Header[] form, a tuple.
    with pytest.raises(TypeError) as exc:
        Headers(headers=(None,))  # type: ignore[arg-type]
    assert str(exc.value) == "headers must not contain None"


def _assert_record_header_read_thread_safe(header: Header) -> None:
    thread_count = 16
    start_latch = threading.Event()
    errors: list[BaseException] = []

    def read() -> None:
        try:
            start_latch.wait()
            header.key()
            header.value()
        except BaseException as e:  # noqa: BLE001 - reported below
            errors.append(e)

    threads = [threading.Thread(target=read) for _ in range(thread_count)]
    for t in threads:
        t.start()
    start_latch.set()
    for t in threads:
        t.join()
    assert errors == []


def test_record_header_is_read_thread_safe() -> None:
    # @RepeatedTest(100). Java's header decodes its ByteBuffers lazily; this
    # one holds its key and view from the start, and reads them as Java's do.
    for _ in range(100):
        _assert_record_header_read_thread_safe(Header(key="key", value=b"value"))


def test_record_header_with_null_value_is_read_thread_safe() -> None:
    # @RepeatedTest(100).
    for _ in range(100):
        _assert_record_header_read_thread_safe(Header(key="key", value=None))


# --------------------------------------------------------------------------- #
# RecordHeader (Kafka 4.4.0 has no RecordHeaderTest): from its source
# --------------------------------------------------------------------------- #


def test_header_null_key_is_javas_requires_non_null() -> None:
    with pytest.raises(TypeError) as exc:
        Header(key=None, value=b"v")  # type: ignore[arg-type]
    assert str(exc.value) == "key must not be None"
    with pytest.raises(TypeError):
        Header("k", b"v")  # type: ignore[call-arg]
    with pytest.raises(TypeError) as exc:
        Header(key="k", value="text")  # type: ignore[arg-type]
    assert str(exc.value) == "value must be bytes-like or None; got str"


def test_header_equality_hash_and_string_are_record_headers() -> None:
    a = Header(key="k", value=b"\x01\xff")
    assert a == Header(key="k", value=bytearray(b"\x01\xff"))
    assert hash(a) == hash(Header(key="k", value=bytearray(b"\x01\xff")))
    assert a != Header(key="k", value=b"\x01")
    assert a != Header(key="j", value=b"\x01\xff")
    assert Header(key="n", value=None) == Header(key="n", value=None)
    assert Header(key="n", value=None) != Header(key="n", value=b"")
    assert hash(Header(key="n", value=None)) == hash(Header(key="n", value=None))

    class Other(Header):
        __slots__ = ()

    # RecordHeader.equals: false for another class.
    assert a != Other(key="k", value=b"\x01\xff")
    assert str(a) == "RecordHeader(key = k, value = [1, -1])"
    assert str(Header(key="n", value=None)) == "RecordHeader(key = n, value = None)"


def test_header_value_is_a_view_never_a_copy() -> None:
    raw = bytearray(b"v1")
    header = Header(key="k", value=raw)
    raw[0] = ord("V")
    value = header.value()
    assert isinstance(value, memoryview) and value.readonly and value.tobytes() == b"V1"
    # Each call hands out a fresh view: releasing one leaves the header's own.
    with header.value() as handed_out:  # type: ignore[union-attr]
        assert handed_out.tobytes() == b"V1"
    assert header.value().tobytes() == b"V1"  # type: ignore[union-attr]


def test_headers_null_key_checks_and_order() -> None:
    headers = Headers()
    for call in (lambda: headers.remove(key=None),  # type: ignore[arg-type]
                 lambda: headers.last_header(key=None),  # type: ignore[arg-type]
                 lambda: headers.headers(key=None)):  # type: ignore[arg-type]
        with pytest.raises(IllegalArgumentError) as exc:
            call()
        assert str(exc.value) == "key cannot be None."
    # remove: canWrite() before checkKey(key).
    headers._set_read_only()  # noqa: SLF001
    with pytest.raises(IllegalStateError):
        headers.remove(key=None)  # type: ignore[arg-type]
    # add(key, value): `new RecordHeader(key, value)` before canWrite().
    with pytest.raises(TypeError) as npe:
        headers.add(key=None, value=b"v")  # type: ignore[call-overload]
    assert str(npe.value) == "key must not be None"


def test_headers_add_forms_and_chaining() -> None:
    headers = Headers()
    assert headers.add(key="a", value=b"1").add(header=Header(key="b", value=None)) is headers
    assert [(h.key(), h.value() and h.value().tobytes()) for h in headers] == [  # type: ignore[union-attr]
        ("a", b"1"), ("b", None)]
    with pytest.raises(IllegalArgumentError) as exc:
        headers.add(key="k")  # type: ignore[call-overload]
    assert str(exc.value) == "add() takes one of (header), (key, value); got (key)"
    with pytest.raises(IllegalArgumentError) as exc:
        headers.add(header=Header(key="k", value=None), key="k", value=None)  # type: ignore[call-overload]
    assert str(exc.value) == "add() takes one of (header), (key, value); got (header, key, value)"
    with pytest.raises(TypeError):
        headers.add(Header(key="k", value=None))  # type: ignore[call-overload]
    assert headers.remove(key="a") is headers


def test_headers_taken_as_header_or_pair() -> None:
    kept = Header(key="h", value=b"x")
    headers = Headers(headers=[("p", b"1"), kept, ("n", None)])
    assert headers.to_array()[1] is kept  # RecordHeaders keeps the object
    assert [h.key() for h in headers] == ["p", "h", "n"]
    assert Headers(headers=None) == Headers() == Headers(headers=())
    with pytest.raises(TypeError) as exc:
        Headers(headers=[("k",)])  # type: ignore[list-item]
    assert str(exc.value) == "headers[0] must be a Header or a (key, value) tuple; got ('k',)"
    with pytest.raises(TypeError) as exc:
        Headers(headers=[(None, b"v")])  # type: ignore[list-item]
    assert str(exc.value) == "headers must not contain a None key"
    # The records and the error take headers the same way.
    for build in (lambda: ProducerRecord(topic="t", partition=None, key=None, value=b"v",
                                         headers=[None]),  # type: ignore[list-item]
                  lambda: _consumer_record([None]),
                  lambda: RecordDeserializationError(
                      origin=None, partition=TopicPartition(topic="t", partition=0),  # type: ignore[arg-type]
                      offset=0, timestamp=-1, timestamp_type=TimestampType.NO_TIMESTAMP_TYPE,
                      key_buffer=None, value_buffer=None, headers=[None],  # type: ignore[list-item]
                      message="m", cause=None)):
        with pytest.raises(TypeError) as exc:
            build()
        assert str(exc.value) == "headers must not contain None"
    with pytest.raises(TypeError) as exc:
        Headers(headers=[("k", "text")])  # type: ignore[list-item]
    assert str(exc.value) == "headers[0] value must be bytes-like or None; got str"


def test_headers_equality_hash_and_string_are_record_headers() -> None:
    a = Headers(headers=[("k", b"\x01\xff"), ("n", None)])
    b = Headers(headers=[("k", bytearray(b"\x01\xff")), ("n", None)])
    b._set_read_only()  # noqa: SLF001 - RecordHeaders.equals ignores it
    assert a == b and hash(a) == hash(b)
    assert a != Headers(headers=[("n", None), ("k", b"\x01\xff")])
    assert str(a) == ("RecordHeaders(headers = [RecordHeader(key = k, value = [1, -1]), "
                      "RecordHeader(key = n, value = None)], isReadOnly = False)")
    assert str(b).endswith("isReadOnly = True)")


def test_headers_pickle_and_deep_copy_with_values_as_bytes() -> None:
    # Not Java's (RecordHeaders is not Serializable): RecordDeserializationError
    # rebuilds from its constructor arguments, a Headers among them, so copy and
    # pickle work (CLAUDE.md, Errors); a viewed value pickles as its bytes.
    raw = bytearray(b"\x01v")
    headers = Headers(headers=[("k", raw), ("n", None), Header(key="m", value=memoryview(b"x"))])
    headers._set_read_only()  # noqa: SLF001
    for copied in (pickle.loads(pickle.dumps(headers)), copy.deepcopy(headers)):
        assert copied == headers and copied is not headers
        assert copied._is_read_only()  # noqa: SLF001
        assert all(isinstance(h, Header) for h in copied)
        raw[1] = ord("V")  # the copy holds its own bytes now
        assert bytes(copied.to_array()[0].value() or b"") == b"\x01v"
        raw[1] = ord("v")
    shallow = copy.copy(headers)
    assert shallow == headers and shallow.to_array()[0] is headers.to_array()[0]
    header = Header(key="k", value=b"v")
    assert pickle.loads(pickle.dumps(header)) == header == copy.deepcopy(header)
    assert copy.copy(header) == header
    error = RecordDeserializationError(
        origin=RecordDeserializationError.DeserializationExceptionOrigin.VALUE,
        partition=TopicPartition(topic="t", partition=0), offset=1, timestamp=2,
        timestamp_type=TimestampType.CREATE_TIME, key_buffer=None, value_buffer=b"v",
        headers=headers, message="m", cause=None)
    for copied_error in (pickle.loads(pickle.dumps(error)), copy.deepcopy(error)):
        assert copied_error.headers() == headers
        assert str(copied_error) == "m"


class _MyHeader(Header):
    __slots__ = ("extra",)
    extra: int


class _MyHeaders(Headers):
    __slots__ = ()


class _Lazy(Header):
    """A user's own implementation: overrides key() / value() and never sets
    Header's slots (Java's Header is an interface users implement)."""

    def __init__(self, key: str, value: bytes) -> None:  # noqa: D107
        self.k = key
        self.v = value

    def key(self) -> str:
        return self.k

    def value(self) -> memoryview | None:
        return memoryview(self.v)


class _Secretive(Header):
    """A subclass with a private slot, stored as ``_Secretive__secret``; module
    level so that pickle can find it."""

    __slots__ = ("__secret",)

    def __init__(self, *, key: str, value: bytes | None, secret: int) -> None:
        super().__init__(key=key, value=value)
        self.__secret = secret

    def secret(self) -> int:
        return self.__secret


def test_a_private_slot_of_a_subclass_survives_copy_and_pickle() -> None:
    # The state helper lists slots as copyreg does, mangled name included.
    original = _Secretive(key="k", value=b"v", secret=7)
    for again in (copy.copy(original), copy.deepcopy(original), pickle.loads(pickle.dumps(original))):
        assert type(again) is _Secretive and again == original and again.secret() == 7


def test_subclasses_keep_their_class_through_copy_deepcopy_and_pickle() -> None:
    mine = _MyHeader(key="k", value=memoryview(b"v"))
    mine.extra = 7
    lazy = _Lazy("l", b"z")
    headers = _MyHeaders(headers=[mine, lazy, ("p", b"1")])
    headers._set_read_only()  # noqa: SLF001
    def pickled(o: Any) -> Any:
        return pickle.loads(pickle.dumps(o))

    clones: tuple[Callable[[Any], Any], ...] = (copy.copy, copy.deepcopy, pickled)
    for clone in clones:
        for header in (mine, lazy):
            copied = clone(header)
            assert type(copied) is type(header) and copied == header
            assert (copied.key(), bytes(copied.value() or b"")) == (
                header.key(), bytes(header.value() or b""))
        assert clone(mine).extra == 7
        copied_headers = clone(headers)
        assert type(copied_headers) is _MyHeaders and copied_headers == headers
        assert copied_headers._is_read_only()  # noqa: SLF001
        assert [type(h) for h in copied_headers] == [_MyHeader, _Lazy, Header]
        error = RecordDeserializationError(
            origin=RecordDeserializationError.DeserializationExceptionOrigin.KEY,
            partition=TopicPartition(topic="t", partition=0), offset=1, timestamp=2,
            timestamp_type=TimestampType.CREATE_TIME, key_buffer=b"k", value_buffer=None,
            headers=headers, message="m", cause=None)
        copied_error = clone(error)
        assert type(copied_error.headers()) is _MyHeaders
        assert copied_error.headers() == headers


# --------------------------------------------------------------------------- #
# The records' headers (handover §7 "Headers")
# --------------------------------------------------------------------------- #


def _consumer_record(headers: Any) -> ConsumerRecord[str, str]:
    return ConsumerRecord(topic="t", partition=0, offset=1, timestamp=2,
                          timestamp_type=TimestampType.CREATE_TIME, serialized_key_size=1,
                          serialized_value_size=1, key="k", value="v",
                          headers=headers, leader_epoch=None)


def test_record_headers_iterate_as_header_objects() -> None:
    record = _consumer_record([("k", b"v"), ("n", None)])
    seen = [(h.key(), h.value()) for h in record.headers()]
    assert all(isinstance(h, Header) for h in record.headers())
    assert seen[0][0] == "k" and isinstance(seen[0][1], memoryview)
    assert seen[0][1].tobytes() == b"v"
    assert seen[1] == ("n", None)


def test_record_headers_last_header() -> None:
    record = _consumer_record([("k", b"1"), ("k", b"2")])
    _assert_header("k", "2", record.headers().last_header(key="k"))
    assert record.headers().last_header(key="absent") is None


def test_producer_record_takes_pairs_and_headers() -> None:
    from_pairs = ProducerRecord(topic="t", key="k", value="v")
    assert from_pairs.headers() == Headers()
    record = ProducerRecord(topic="t", partition=None, key="k", value="v",
                            headers=[("k", b"v")])
    (header,) = record.headers()
    assert isinstance(header, Header)
    _assert_header("k", "v", header)
    given = Header(key="k", value=b"v")
    record = ProducerRecord(topic="t", partition=None, key="k", value="v", headers=[given])
    assert record.headers().to_array()[0] is given
    assert record == ProducerRecord(topic="t", partition=None, key="k", value="v",
                                    headers=[("k", b"v")])
    # ProducerRecord copies a Headers argument into new, writable headers
    # (new RecordHeaders(headers)); headers() is the record's own.
    source = Headers(headers=[given])
    source._set_read_only()  # noqa: SLF001
    record = ProducerRecord(topic="t", partition=None, key="k", value="v", headers=source)
    assert record.headers() is not source and record.headers() == source
    assert record.headers() is record.headers()
    record.headers().add(key="more", value=None)
    assert _get_count(record.headers()) == 2 and _get_count(source) == 1


def test_record_headers_never_none() -> None:
    assert ProducerRecord(topic="t", value="v").headers() == Headers()
    # A null header list: Java's `new RecordHeaders(null)`, no headers.
    assert ProducerRecord(topic="t", partition=0, key=None, value="v",  # type: ignore[call-overload]
                          headers=None).headers() == Headers()
    short: ConsumerRecord[str, str] = ConsumerRecord(topic="t", partition=0, offset=0,
                                                     key="k", value="v")
    assert isinstance(short.headers(), Headers) and short.headers().to_array() == ()
    # Java's short constructors pass `new RecordHeaders()`: one per record.
    other: ConsumerRecord[str, str] = ConsumerRecord(topic="t", partition=0, offset=0,
                                                     key="k", value="v")
    assert short.headers() is not other.headers()


def test_consumer_record_keeps_a_headers_argument() -> None:
    given = Headers(headers=[("k", b"v")])
    assert _consumer_record(given).headers() is given


def test_mock_producer_leaves_the_headers_writable() -> None:
    # Java's MockProducer.send serializes with record.headers() and never
    # calls setReadOnly (MockProducer.java:614-615).
    producer: MockProducer[bytes, bytes] = MockProducer(
        auto_complete=True, partitioner=None, key_serializer=bytes_serializer(),
        value_serializer=bytes_serializer())
    record: ProducerRecord[bytes, bytes] = ProducerRecord(topic="t", value=b"v")
    producer.send(record=record)
    assert not record.headers()._is_read_only()  # noqa: SLF001
    record.headers().add(key="k", value=b"after")


# --------------------------------------------------------------------------- #
# The headers the C extension builds for a fetched record
# --------------------------------------------------------------------------- #


def _native_batch(records: int) -> object:
    h = _lib.Consumer_MockConsumer_new("earliest")
    assert _lib.Consumer_assign(h, [("t", 0)]) in (0, None)
    _lib.MockConsumer_update_beginning_offsets(h, [("t", 0, 0)])
    for offset in range(records):
        assert _lib.MockConsumer_add_record(h, "t", 0, offset, b"k", b"v") in (0, None)
    batch, err = _lib.Consumer_poll(h, 0)
    assert err in (0, None)
    return batch


def test_extension_builds_a_writable_headers_per_record() -> None:
    batch = _native_batch(2)
    first, second = batch.get(0).headers, batch.get(1).headers  # type: ignore[attr-defined]
    assert type(first) is Headers and first.to_array() == ()
    assert not first._is_read_only()  # noqa: SLF001
    assert first is not second
    first.add(key="k", value=b"v")  # a deserializer may change them, as in Java
    assert second.to_array() == ()


def test_poll_hands_out_writable_headers_after_the_deserializers() -> None:
    # Java's CompletedFetch leaves a fetched record's headers writable: only
    # KafkaProducer.doSend seals a record's headers.
    batch = _native_batch(1)
    seen: list[Headers] = []

    def deserializer(*, topic: str, data: memoryview | None, headers: Headers | None = None, **kwargs: Any) -> bytes:
        assert headers is not None and not headers._is_read_only()  # noqa: SLF001
        headers.add(key="added", value=b"by-deserializer")
        seen.append(headers)
        return bytes(data or b"")

    result = deserialize_batch(batch, key_deserializer=_caller(deserializer),
                               value_deserializer=_caller(lambda *, topic, data, headers=None, **_:
                                                          bytes(data or b"")))
    (record,) = result.records.records(partition=TopicPartition(topic="t", partition=0))
    assert record.headers() is seen[0]
    _assert_header("added", "by-deserializer", record.headers().last_header(key="added"))
    assert not record.headers()._is_read_only()  # noqa: SLF001
    record.headers().add(key="k", value=None)
    _assert_header("added", "by-deserializer", record.headers().to_array()[0])
    assert record.headers().last_header(key="k") is not None
