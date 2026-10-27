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

"""Tests for the generated error hierarchy (CLAUDE.md, Python Binding
Conventions, Errors).

The parent chains are checked against the Java sources directly, in both
directions, and each ``_ffi_id`` against the FFI enum, independently of the
generator. The constructors are checked against Java's: the message each
overload gives ``getMessage()``, the payload defaults, and the ``java_forms``
rejection of a combination Java has no constructor for.
"""

from __future__ import annotations

import builtins
import copy
import importlib
import pickle
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import confluent_kafka
from confluent_kafka import (
    ConcurrentModificationError,
    ExecutionError,
    IllegalArgumentError,
    IllegalStateError,
    IndexOutOfBoundsError,
    NoSuchElementError,
    UnsupportedOperationError,
)
from confluent_kafka import TimeoutError as RootTimeoutError
from confluent_kafka import _errors as errmod
from confluent_kafka import _throwable
from confluent_kafka.common import KafkaFuture
from confluent_kafka.common.kafka_future import _KafkaFutureImpl
from confluent_kafka._error_registry import ERRORS
from confluent_kafka._errors import (
    class_for_ffi_id,
    error_classes,
    from_borrowed_ffi_error,
    from_ffi_error,
    to_ffi_id,
)
from confluent_kafka.common.header import Headers
from confluent_kafka.common import InvalidRecordError, KafkaError, MetricName, TopicPartition
from confluent_kafka.common.metrics import KafkaMetric
from confluent_kafka.common.record import TimestampType
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.errors import (
    AuthenticationError,
    CoordinatorNotAvailableError,
    CorruptRecordError,
    DisconnectError,
    DuplicateResourceError,
    GroupAuthorizationError,
    InterruptError,
    InvalidMetadataError,
    InvalidTopicError,
    LogDirNotFoundError,
    NotLeaderOrFollowerError,
    RecordDeserializationError,
    RecordTooLargeError,
    ReplicaNotAvailableError,
    ResourceNotFoundError,
    RetriableError,
    ThrottlingQuotaExceededError,
    TopicAuthorizationError,
    TransactionAbortedError,
    UnknownServerError,
    WakeupError,
)
from confluent_kafka.common.metrics import QuotaViolationError
from confluent_kafka.common.network import InvalidReceiveError
from confluent_kafka.common.protocol.types import SchemaError
from confluent_kafka.common.requests import CorrelationIdMismatchError
from confluent_kafka.consumer import (
    CommitFailedError,
    InvalidOffsetError,
    LogTruncationError,
    NoOffsetForPartitionError,
    OffsetAndMetadata,
    OffsetOutOfRangeError,
    RetriableCommitFailedError,
)
from confluent_kafka.producer import BufferExhaustedError

_REPO_ROOT = Path(__file__).resolve().parents[3]
_JAVA_ROOT = _REPO_ROOT / "kafka/clients/src/main/java"
_TP = TopicPartition(topic="t", partition=0)


# ----------------------------------------------------------------------------
# The Java side, read independently of the generator
# ----------------------------------------------------------------------------


def _java_source(fqn: str) -> str:
    return (_JAVA_ROOT / (fqn.replace(".", "/") + ".java")).read_text()


def _java_parent(fqn: str) -> str:
    """The FQN of a Java exception class's parent, from its source."""
    text = _java_source(fqn)
    package_match = re.search(r"package\s+([\w.]+);", text)
    assert package_match is not None, fqn
    m = re.search(r"public\s+(?:abstract\s+|final\s+)?class\s+\w+\s+extends\s+(\w+)", text)
    assert m is not None, fqn
    parent = m.group(1)
    imported = re.search(rf"import\s+([\w.]+\.{parent});", text)
    if imported:
        return imported.group(1)
    if parent in ("RuntimeException", "Exception", "IllegalStateException"):
        return f"java.lang.{parent}"
    return f"{package_match.group(1)}.{parent}"


def _java_is_abstract(fqn: str) -> bool:
    return re.search(r"public\s+abstract\s+class\s", _java_source(fqn)) is not None


def _python_module(java_package: str) -> str:
    if java_package.startswith("java."):
        return "confluent_kafka"
    rest = java_package.removeprefix("org.apache.kafka.").removeprefix("clients.")
    return f"confluent_kafka.{rest}"


# The Java built-ins and their declared Python bases, in order (CLAUDE.md, Types).
_JDK: dict[str, tuple[type[BaseException], ...]] = {
    "java.lang.IllegalStateException": (RuntimeError,),
    "java.lang.IllegalArgumentException": (ValueError, RuntimeError),
    "java.util.ConcurrentModificationException": (RuntimeError,),
    "java.util.concurrent.TimeoutException": (builtins.TimeoutError,),
    "java.util.concurrent.ExecutionException": (Exception,),
    "java.util.NoSuchElementException": (RuntimeError,),
    "java.lang.IndexOutOfBoundsException": (IndexError, RuntimeError),
    "java.lang.UnsupportedOperationException": (NotImplementedError,),
}

_BY_JAVA: dict[str, type[BaseException]] = {
    java: getattr(importlib.import_module(module), name) for module, name, java in ERRORS
}


def _bridge() -> dict[str, str]:
    """The generator's reviewed id -> Java class table, parsed from its source."""
    text = (_REPO_ROOT / "rust/xtask/src/error_hierarchy.rs").read_text()
    body = text[text.index("const BRIDGE:"):]
    body = body[:body.index("];")]
    rows = re.findall(r'\(\s*"([A-Z0-9_]+)"\s*,\s*"([\w.]+)"\s*,?\s*\)', body)
    assert len(rows) == 166
    return dict(rows)


def _ffi_enum() -> dict[str, int]:
    """The FFI enum ``kafka_common_Error_e`` (``rust/src/ffi/common.rs``),
    from its generated Rust mirror, which ``cargo xtask check-generated``
    keeps in step with it."""
    text = (_REPO_ROOT / "rust/tests/common/error_code.rs").read_text()
    return {n: int(v) for n, v in re.findall(r"pub const (\w+): i32 = (-?\d+);", text)}


def _snake(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


# ----------------------------------------------------------------------------
# Parent chains, both directions
# ----------------------------------------------------------------------------


def test_every_java_class_has_its_python_class_with_the_java_parent() -> None:
    """Java -> Python: each class's immediate base is the Python class of its
    Java parent (a Python builtin for a JDK class and for KafkaException)."""
    for java, cls in _BY_JAVA.items():
        if java in _JDK:
            assert cls.__bases__ == _JDK[java], java
            continue
        parent = _java_parent(java)
        if parent == "java.lang.RuntimeException":
            assert java == "org.apache.kafka.common.KafkaException"
            assert cls.__bases__ == (RuntimeError,)
            continue
        assert cls.__bases__ == (_BY_JAVA[parent],), f"{java} extends {parent}"


def test_every_python_error_class_is_a_java_class() -> None:
    """Python -> Java: every error class the package defines is a generated one
    (no invented class), and the generated set is exactly the FFI enum's
    classes, Java's abstract bases, KafkaException and the Java built-ins."""
    def walk(cls: type[BaseException]) -> set[type[BaseException]]:
        out = {cls}
        for sub in cls.__subclasses__():
            if sub.__module__.startswith("confluent_kafka"):
                out |= walk(sub)
        return out

    defined = walk(KafkaError)
    for root in (IllegalStateError, IllegalArgumentError, ConcurrentModificationError,
                 RootTimeoutError, NoSuchElementError, ExecutionError,
                 UnsupportedOperationError, IndexOutOfBoundsError):
        defined |= walk(root)
    assert defined == set(error_classes())
    expected = set(_bridge().values()) | {
        "org.apache.kafka.common.KafkaException",
        "java.util.NoSuchElementException",
        "java.lang.IndexOutOfBoundsException",
        "java.util.concurrent.ExecutionException",
        "java.lang.UnsupportedOperationException",
        "org.apache.kafka.common.errors.RetriableException",
        "org.apache.kafka.common.errors.RefreshRetriableException",
        "org.apache.kafka.common.errors.InvalidMetadataException",
        "org.apache.kafka.common.errors.ApplicationRecoverableException",
        "org.apache.kafka.clients.consumer.InvalidOffsetException",
    }
    assert set(_BY_JAVA) == expected
    assert len(_BY_JAVA) == 175


def test_only_an_exception_suffix_becomes_error() -> None:
    # CLAUDE.md, Idiom translations: "a class name's Exception suffix becomes
    # Error, nothing else changes", so the two suffixless Java exceptions keep
    # their names.
    for _module, name, java in ERRORS:
        simple = java.rsplit(".", 1)[1]
        expected = simple[: -len("Exception")] + "Error" if simple.endswith("Exception") else simple
        assert name == expected, java
    from confluent_kafka.common.errors import InvalidRegularExpression, OffsetMetadataTooLarge

    assert InvalidRegularExpression.__name__ == "InvalidRegularExpression"
    assert OffsetMetadataTooLarge.__module__ == "confluent_kafka.common.errors"


def test_each_error_lives_in_the_module_of_its_java_package() -> None:
    for module, name, java in ERRORS:
        cls = _BY_JAVA[java]
        public = _python_module(java.rsplit(".", 1)[0])
        assert cls.__module__ == public
        package = importlib.import_module(public)
        assert getattr(package, name) is cls
        assert name in package.__all__
        assert module == f"{public}.{_snake(name)}"
    assert BufferExhaustedError.__module__ == "confluent_kafka.producer"
    assert InvalidRecordError.__module__ == "confluent_kafka.common"
    assert CorrelationIdMismatchError.__module__ == "confluent_kafka.common.requests"
    assert InvalidReceiveError.__module__ == "confluent_kafka.common.network"
    assert QuotaViolationError.__module__ == "confluent_kafka.common.metrics"
    assert SchemaError.__module__ == "confluent_kafka.common.protocol.types"


def test_the_root_exports_duration_and_the_java_built_ins_only() -> None:
    assert sorted(confluent_kafka.__all__) == sorted([
        "Duration", "ConcurrentModificationError", "ExecutionError", "IllegalArgumentError",
        "IllegalStateError", "IndexOutOfBoundsError", "NoSuchElementError", "TimeoutError",
        "UnsupportedOperationError"])
    assert issubclass(RootTimeoutError, builtins.TimeoutError)
    # A Java built-in Python has a builtin for subclasses that builtin (CLAUDE.md, Types).
    assert UnsupportedOperationError.__bases__ == (NotImplementedError,)
    assert IndexOutOfBoundsError.__bases__ == (IndexError, RuntimeError)
    assert issubclass(UnsupportedOperationError, RuntimeError)
    assert not issubclass(UnsupportedOperationError, KafkaError)
    for cls in (IllegalStateError, IllegalArgumentError, ConcurrentModificationError,
                NoSuchElementError, UnsupportedOperationError, IndexOutOfBoundsError):
        assert issubclass(cls, RuntimeError)
        assert not issubclass(cls, KafkaError)
    # ExecutionException is checked: an Exception, not a RuntimeError.
    assert ExecutionError.__bases__ == (Exception,)


def test_kafka_error_subclasses_runtime_error() -> None:
    assert KafkaError.__bases__ == (RuntimeError,)
    assert issubclass(ConfigError, KafkaError)
    assert issubclass(CorrelationIdMismatchError, IllegalStateError)


# ----------------------------------------------------------------------------
# FFI ids
# ----------------------------------------------------------------------------


def _instance(cls: type[BaseException]) -> BaseException:
    """An instance of a concrete class through one of its Java constructors."""
    tries: list[dict[str, Any]] = [
        {"message": "m"}, {}, {"partitions": [_TP]},
        {"offset_out_of_range_partitions": {_TP: 1}},
        {"fetch_offsets": {_TP: 1}, "divergent_offsets": {}},
        {"metric": KafkaMetric._snapshot(name="n", group="g", value=1.0), "value": 1.0,
         "bound": 0.5},
        {"origin": None, "partition": _TP, "offset": 1, "timestamp": 5,
         "timestamp_type": TimestampType.CREATE_TIME, "key_buffer": None, "value_buffer": b"v",
         "headers": (), "message": "m", "cause": ValueError("v")},
        {"message": "m", "request_correlation_id": 1, "response_correlation_id": 2},
    ]
    for kwargs in tries:
        try:
            return cls(**kwargs)
        except (TypeError, IllegalArgumentError):
            continue
    raise AssertionError(f"no constructor of {cls.__name__} matched")


def test_every_ffi_id_is_the_enum_value_of_its_bridge_row() -> None:
    enum = _ffi_enum()
    ids = set()
    for constant, java in _bridge().items():
        cls = _BY_JAVA[java]
        assert cls.__dict__["_ffi_id"] == enum[constant], java
        ids.add(enum[constant])
        # id -> class -> id round-trips.
        assert class_for_ffi_id(enum[constant]) is cls
        assert to_ffi_id(_instance(cls)) == enum[constant]
    assert len(ids) == 166


def test_the_base_carries_unknown_server_error_which_unknown_server_error_owns() -> None:
    assert KafkaError._ffi_id == -1 == _ffi_enum()["UNKNOWN_SERVER_ERROR"]
    assert class_for_ffi_id(-1) is UnknownServerError
    assert to_ffi_id(KafkaError(message="x")) == -1


def test_built_ins_the_core_does_not_model_have_no_id() -> None:
    # UnsupportedOperationError has the core's LOCAL_UNSUPPORTED_OPERATION id
    # (test_unsupported_operation_error_is_java_unsupported_operation_exception).
    assert not hasattr(NoSuchElementError, "_ffi_id")
    assert not hasattr(IndexOutOfBoundsError, "_ffi_id")
    assert to_ffi_id(IndexOutOfBoundsError(message="x")) == -1
    assert to_ffi_id(NoSuchElementError(message="x")) == -1
    assert not hasattr(ExecutionError, "_ffi_id")
    assert to_ffi_id(ExecutionError(cause=ValueError("x"))) == -1
    assert to_ffi_id(ValueError("not ours")) == -1


def test_abstract_bases_are_java_abstract_have_no_id_and_cannot_be_built() -> None:
    abstract = {java for java in _BY_JAVA if java not in _JDK and _java_is_abstract(java)}
    assert {_BY_JAVA[j] for j in abstract} == {
        RetriableError, InvalidMetadataError, InvalidOffsetError,
        _BY_JAVA["org.apache.kafka.common.errors.RefreshRetriableException"],
        _BY_JAVA["org.apache.kafka.common.errors.ApplicationRecoverableException"],
    }
    for java in abstract:
        cls = _BY_JAVA[java]
        assert "_ffi_id" not in cls.__dict__
        with pytest.raises(TypeError) as exc:
            cls(message="x")  # type: ignore[call-arg]
        assert str(exc.value) == (
            f"{cls.__name__} is an abstract catch-only base; it is never raised directly")

    class Mine(RetriableError):
        pass

    assert str(Mine(message="a subclass of an abstract base")) == (
        "a subclass of an abstract base")


def test_a_positional_call_is_a_type_error_for_every_class() -> None:
    for cls in error_classes():
        with pytest.raises(TypeError):
            cls("positional")


# ----------------------------------------------------------------------------
# Constructors: Java's forms and messages
# ----------------------------------------------------------------------------


def test_kafka_error_has_javas_four_constructors() -> None:
    assert str(KafkaError(message="m")) == "m"
    assert str(KafkaError()) == ""
    cause = ValueError("v")
    e = KafkaError(message="m", cause=cause)
    assert str(e) == "m" and e.__cause__ is cause
    # Throwable(Throwable cause): the message is cause.toString().
    e = KafkaError(cause=cause)
    assert str(e) == "builtins.ValueError: v" and e.__cause__ is cause
    assert e.args == ("builtins.ValueError: v",)
    assert str(KafkaError(cause=KafkaError())) == "confluent_kafka.common.KafkaError"
    assert str(KafkaError(cause=TopicAuthorizationError(message="no"))) == (
        "confluent_kafka.common.errors.TopicAuthorizationError: no")
    assert KafkaError().args == ()


def test_message_is_the_keyword_message() -> None:
    with pytest.raises(TypeError):
        KafkaError("positional")  # type: ignore[call-arg]
    assert str(IllegalStateError(message="s")) == "s"
    assert str(RootTimeoutError(message="t")) == "t"
    assert str(UnsupportedOperationError()) == ""


def test_topic_authorization_constructors() -> None:
    e = TopicAuthorizationError(unauthorized_topics={"t"})
    assert str(e) == "Not authorized to access topics: [t]"
    assert e.unauthorized_topics() == {"t"}
    # (String message) passes Collections.emptySet(): the Java-given default.
    assert TopicAuthorizationError(message="x").unauthorized_topics() == set()
    # MISSING: an explicit empty set is given, so (unauthorizedTopics) is used.
    assert str(TopicAuthorizationError(unauthorized_topics=())) == (
        "Not authorized to access topics: []")
    with pytest.raises(IllegalArgumentError) as exc:
        TopicAuthorizationError()  # type: ignore[call-overload]
    assert str(exc.value) == (
        "TopicAuthorizationError() takes one of (message, unauthorized_topics), "
        "(unauthorized_topics), (message); got ()")


def test_config_error_constructors() -> None:
    assert str(ConfigError(message="plain")) == "plain"
    assert str(ConfigError(name="a.b", value=5)) == "Invalid value 5 for configuration a.b"
    assert str(ConfigError(name="a.b", value=True, message="m")) == (
        "Invalid value True for configuration a.b: m")
    assert str(ConfigError(name="a.b", value=[1, 2], message="m")) == (
        "Invalid value [1, 2] for configuration a.b: m")
    with pytest.raises(IllegalArgumentError) as exc:
        ConfigError()  # type: ignore[call-overload]
    assert str(exc.value) == (
        "ConfigError() takes one of (message), (name, value), "
        "(name, value, message); got ()")


def test_constructors_with_a_literal_message() -> None:
    assert str(CommitFailedError()).startswith(
        "Commit cannot be completed since the group has already rebalanced")
    assert str(CommitFailedError(message="x")) == "x"
    assert str(TransactionAbortedError()) == "Failing batch since transaction was aborted"
    assert str(CorruptRecordError()) == (
        "This message has failed its CRC checksum, exceeds the valid size, has a null key "
        "for a compacted topic, or is otherwise corrupt.")
    cause = ValueError("v")
    assert str(CorruptRecordError(cause=cause)) == "builtins.ValueError: v"
    e = RetriableCommitFailedError(cause=cause)
    assert str(e) == (
        "Offset commit failed with a retriable exception. You should retry committing "
        "the latest consumed offsets.")
    assert e.__cause__ is cause
    with pytest.raises(IllegalArgumentError) as exc:
        RetriableCommitFailedError()  # type: ignore[call-overload]
    assert str(exc.value) == (
        "RetriableCommitFailedError() takes one of (cause), (message), "
        "(message, cause); got ()")


def test_invalid_topic_constructors() -> None:
    assert str(InvalidTopicError(invalid_topics=["a"])) == "Invalid topics: [a]"
    assert InvalidTopicError(message="m").invalid_topics() == set()
    assert InvalidTopicError().invalid_topics() == set()
    # An explicit None is Java's (String) null: getMessage() is null, str(e) "".
    assert str(InvalidTopicError(message=None)) == ""


def test_no_offset_for_partition_constructors() -> None:
    tp1 = TopicPartition(topic="t", partition=1)
    e = NoOffsetForPartitionError(partition=_TP)
    assert str(e) == "Undefined offset with no reset policy for partition: t-0"
    assert e.partitions() == {_TP}
    e = NoOffsetForPartitionError(partitions=[_TP, tp1])
    assert str(e) == "Undefined offset with no reset policy for partitions: [t-0, t-1]"
    assert e.partitions() == {_TP, tp1}


def test_offset_out_of_range_and_log_truncation_constructors() -> None:
    e = OffsetOutOfRangeError(offset_out_of_range_partitions={_TP: 5})
    assert str(e) == (
        "Offsets out of range with no configured reset policy for partitions: {t-0=5}")
    assert e.offset_out_of_range_partitions() == {_TP: 5}
    assert e.partitions() == {_TP}
    oam = OffsetAndMetadata(offset=3)
    e2 = LogTruncationError(fetch_offsets={_TP: 5}, divergent_offsets={_TP: oam})
    assert isinstance(e2, OffsetOutOfRangeError)
    assert e2.offset_out_of_range_partitions() == {_TP: 5}
    assert e2.partitions() == {_TP}
    assert e2.divergent_offsets() == {_TP: oam}
    assert str(LogTruncationError(message="m", fetch_offsets={}, divergent_offsets={})) == "m"


def test_payload_getters_return_javas_defaults() -> None:
    assert ThrottlingQuotaExceededError(message="x").throttle_time_ms() == 0
    assert ThrottlingQuotaExceededError(throttle_time_ms=5, message="x").throttle_time_ms() == 5
    assert GroupAuthorizationError(message="x").group_id() is None
    assert GroupAuthorizationError(message="x", group_id="g").group_id() == "g"
    assert DuplicateResourceError(message="m").resource() is None
    cause = ValueError("v")
    e = ResourceNotFoundError(resource="r", message="m", cause=cause)
    assert (e.resource(), str(e), e.__cause__) == ("r", "m", cause)
    assert RecordTooLargeError(message="m").record_too_large_partitions() is None
    assert RecordTooLargeError(
        message="m", record_too_large_partitions={_TP: 9}).record_too_large_partitions() == {_TP: 9}
    c = CorrelationIdMismatchError(message="m", request_correlation_id=1,
                                   response_correlation_id=2)
    assert (c.request_correlation_id(), c.response_correlation_id()) == (1, 2)


_QUOTA_TEXT = ("'MetricName [name=rate, group=producer, description=, tags={}]' violated "
               "quota. Actual: 9.5, Threshold: 5.0")
_QUOTA_TO_STRING = "confluent_kafka.common.metrics.QuotaViolationError: " + _QUOTA_TEXT


def _quota_violation() -> QuotaViolationError:
    metric = KafkaMetric._snapshot(name="rate", group="producer", value=9.5)
    return QuotaViolationError(metric=metric, value=9.5, bound=5.0)


def test_quota_violation_uses_java_to_string_override() -> None:
    # QuotaViolationException.toString(): getClass().getName() + ": '" +
    # metric.metricName() + "' violated quota. Actual: " + value + ", Threshold: " + bound.
    # Its getMessage() stays null; str() is the override's text after the class name.
    e = _quota_violation()
    assert (e.value(), e.bound()) == (9.5, 5.0)
    assert e.metric().metric_name() == MetricName(name="rate", group="producer",
                                                  description="", tags={})
    assert str(e) == _QUOTA_TEXT
    assert _throwable.java_message(e) is None
    assert _throwable.to_string(e) == _QUOTA_TO_STRING
    # The override dereferences metric (Java's NPE): Types' TypeError.
    with pytest.raises(TypeError) as none:
        str(QuotaViolationError(metric=None, value=1.0, bound=2.0))  # type: ignore[arg-type]
    assert str(none.value) == "metric must not be None"


def test_quota_violation_stores_java_doubles() -> None:
    # Java's value and bound are double: an int prints as 9.0 / 5.0.
    metric = KafkaMetric._snapshot(name="rate", group="producer", value=9)
    e = QuotaViolationError(metric=metric, value=9, bound=5)
    assert (e.value(), e.bound()) == (9.0, 5.0)
    assert type(e.value()) is float and type(e.bound()) is float
    assert str(e).endswith("violated quota. Actual: 9.0, Threshold: 5.0")


def test_quota_violation_without_its_payload_prints_its_message() -> None:
    # _throwable.new's message-only fallback has no metric: str() and toString()
    # print the message, as before the override.
    e = _throwable.new(QuotaViolationError, "over")
    assert str(e) == "over"
    assert _throwable.to_string(e) == "confluent_kafka.common.metrics.QuotaViolationError: over"


def test_quota_violation_to_string_where_java_uses_it() -> None:
    # Throwable(Throwable cause) takes cause.toString() as its message, and so does
    # ExecutionException.
    q = _quota_violation()
    wrapped = KafkaError(cause=q)
    assert str(wrapped) == _QUOTA_TO_STRING and wrapped.__cause__ is q
    impl: _KafkaFutureImpl[None] = _KafkaFutureImpl()
    impl.complete_with_error(q)
    with pytest.raises(ExecutionError) as info:
        KafkaFuture._of_impl(impl).get()
    assert str(info.value) == _QUOTA_TO_STRING and info.value.__cause__ is q


def test_interrupt_error_has_no_interrupted_exception_cause() -> None:
    # Java's (String message) passes `new InterruptedException()`; Python has no
    # such class, so the cause is left empty.
    e = InterruptError(message="m")
    assert str(e) == "m" and e.__cause__ is None


def test_record_deserialization_constructors() -> None:
    origin = RecordDeserializationError.DeserializationExceptionOrigin
    assert [m.value for m in origin] == ["KEY", "VALUE"]
    cause = ValueError("bad")
    e = RecordDeserializationError(
        origin=origin.VALUE, partition=_TP, offset=7, timestamp=10,
        timestamp_type=TimestampType.CREATE_TIME,
        key_buffer=b"k", value_buffer=b"v", headers=[("h", b"1")], message="m", cause=cause)
    assert e.origin() is origin.VALUE
    assert e.topic_partition() == _TP and e.offset() == 7 and e.timestamp() == 10
    assert bytes(e.key_buffer() or b"") == b"k"
    assert bytes(e.value_buffer() or b"") == b"v"
    assert [(h.key(), bytes(h.value() or b"")) for h in e.headers()] == [("h", b"1")]
    assert e.__cause__ is cause
    # Java's null buffers and origin are values of the full constructor.
    # Every Java constructor takes the cause, so a null one is cause=None.
    n = RecordDeserializationError(
        origin=None, partition=_TP, offset=2, timestamp=-1,  # type: ignore[arg-type]
        timestamp_type=TimestampType.NO_TIMESTAMP_TYPE, key_buffer=None, value_buffer=None,
        headers=(), message="m", cause=None)
    assert (n.origin(), n.key_buffer(), n.value_buffer(), n.headers()) == (
        None, None, None, Headers())
    # A record without a key (CompletedFetch passes record.key(), null) or a
    # tombstone keeps the other buffer.
    k = RecordDeserializationError(
        origin=origin.KEY, partition=_TP, offset=3, timestamp=1,
        timestamp_type=TimestampType.CREATE_TIME, key_buffer=None, value_buffer=b"v",
        headers=(), message="m", cause=None)
    assert k.key_buffer() is None and bytes(k.value_buffer() or b"") == b"v"


def test_record_deserialization_has_only_the_full_constructor() -> None:
    # The @Deprecated (partition, offset, message, cause) constructor is not
    # generated (CLAUDE.md, Python Binding Conventions, Class family), so the
    # full constructor's parameters are required: leaving one out is Python's
    # TypeError, the deprecated set included.
    origin = RecordDeserializationError.DeserializationExceptionOrigin.KEY
    for kwargs in [
        {"partition": _TP, "offset": 1, "message": "m", "cause": None},
        {"origin": origin, "partition": _TP, "offset": 1, "message": "m"},
        {"origin": None, "partition": _TP, "offset": 1, "timestamp": 5, "message": "m"},
    ]:
        with pytest.raises(TypeError):
            RecordDeserializationError(**kwargs)  # type: ignore[arg-type]


# Every generated error class with java_forms: each given set Java has no
# constructor for, with the exact message (CLAUDE.md, Tests and typing).
_REJECTED: list[tuple[type[BaseException], dict[str, Any], str]] = [
    (AuthenticationError, {},
     "AuthenticationError() takes one of (message), (cause), (message, cause); got ()"),
    (InterruptError, {},
     "InterruptError() takes one of (cause), (message, cause), (message); got ()"),
    (LogDirNotFoundError, {},
     "LogDirNotFoundError() takes one of (message), (message, cause), (cause); got ()"),
    (ReplicaNotAvailableError, {},
     "ReplicaNotAvailableError() takes one of (message), (message, cause), (cause); got ()"),
    (TransactionAbortedError, {"cause": ValueError("c")},
     "TransactionAbortedError() takes one of (message, cause), (message), (); got (cause)"),
    (InvalidTopicError, {"cause": ValueError("c"), "invalid_topics": ["t"]},
     "InvalidTopicError() takes one of (), (message, cause), (message), (cause), "
     "(invalid_topics), (message, invalid_topics); got (cause, invalid_topics)"),
    (InvalidTopicError, {"message": "m", "cause": ValueError("c"), "invalid_topics": ["t"]},
     "InvalidTopicError() takes one of (), (message, cause), (message), (cause), "
     "(invalid_topics), (message, invalid_topics); got (message, cause, invalid_topics)"),
    (RecordTooLargeError, {"cause": ValueError("c"), "record_too_large_partitions": {_TP: 1}},
     "RecordTooLargeError() takes one of (), (message, cause), (message), (cause), "
     "(message, record_too_large_partitions); got (cause, record_too_large_partitions)"),
    (RecordTooLargeError, {"record_too_large_partitions": {_TP: 1}},
     "RecordTooLargeError() takes one of (), (message, cause), (message), (cause), "
     "(message, record_too_large_partitions); got (record_too_large_partitions)"),
    (NoOffsetForPartitionError, {},
     "NoOffsetForPartitionError() takes one of (partition), (partitions); got ()"),
    (NoOffsetForPartitionError, {"partition": _TP, "partitions": [_TP]},
     "NoOffsetForPartitionError() takes one of (partition), (partitions); "
     "got (partition, partitions)"),
    (TopicAuthorizationError, {},
     "TopicAuthorizationError() takes one of (message, unauthorized_topics), "
     "(unauthorized_topics), (message); got ()"),
    (ConfigError, {}, "ConfigError() takes one of (message), (name, value), "
     "(name, value, message); got ()"),
    (ConfigError, {"name": "n"}, "ConfigError() takes one of (message), (name, value), "
     "(name, value, message); got (name)"),
    (RetriableCommitFailedError, {},
     "RetriableCommitFailedError() takes one of (cause), (message), (message, cause); got ()"),
]


@pytest.mark.parametrize("cls,kwargs,message", _REJECTED,
                         ids=[f"{c.__name__}-{'-'.join(k) or 'none'}" for c, k, _ in _REJECTED])
def test_generated_errors_reject_what_java_has_no_constructor_for(
        cls: type[BaseException], kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        cls(**kwargs)
    assert str(exc.value) == message


def test_every_java_forms_error_class_has_a_rejection_case() -> None:
    # A class gets java_forms only when its union accepts a combination that
    # matches no Java constructor (CLAUDE.md, Signatures), and each such class
    # has its rejections tested above.
    decorated = {cls for cls in error_classes()
                 if getattr(cls.__init__, "__wrapped__", None) is not None}
    covered = {cls for cls, _, _ in _REJECTED}
    assert decorated == covered


def test_throttling_quota_exceeded_tells_its_forms_apart_without_java_forms() -> None:
    # Every combination is a Java constructor, (String) and (int, String), so
    # the class has no decorator and one signature; the body tells the forms
    # apart by whether throttle_time_ms is given.
    assert getattr(ThrottlingQuotaExceededError.__init__, "__wrapped__", None) is None
    assert ThrottlingQuotaExceededError(message="m").throttle_time_ms() == 0
    assert ThrottlingQuotaExceededError(throttle_time_ms=0, message="m").throttle_time_ms() == 0
    assert ThrottlingQuotaExceededError(throttle_time_ms=7, message="m").throttle_time_ms() == 7
    assert str(ThrottlingQuotaExceededError(throttle_time_ms=7, message="m")) == "m"
    clone = pickle.loads(pickle.dumps(ThrottlingQuotaExceededError(throttle_time_ms=7, message="m")))
    assert clone.throttle_time_ms() == 7


def test_group_authorization_for_group_id() -> None:
    # Java's public static GroupAuthorizationException.forGroupId(String).
    e = GroupAuthorizationError.for_group_id(group_id="g")
    assert type(e) is GroupAuthorizationError
    assert str(e) == "Not authorized to access group: g"
    assert e.group_id() == "g"
    with pytest.raises(TypeError):
        GroupAuthorizationError.for_group_id("g")  # type: ignore[misc]


def test_iterable_arguments_are_read_once() -> None:
    # An Iterable argument is materialized as Java's Set.copyOf does, so a
    # generator gives the same message, payload and pickled arguments.
    cases: list[tuple[BaseException, str, Any]] = [
        (TopicAuthorizationError(unauthorized_topics=(t for t in ["a", "b"])),
         "Not authorized to access topics: [a, b]", lambda e: e.unauthorized_topics()),
        (InvalidTopicError(invalid_topics=(t for t in ["x"])), "Invalid topics: [x]",
         lambda e: e.invalid_topics()),
        (NoOffsetForPartitionError(partitions=(p for p in [_TP])),
         "Undefined offset with no reset policy for partitions: [t-0]",
         lambda e: e.partitions()),
    ]
    for error, message, payload in cases:
        assert str(error) == message
        clone = pickle.loads(pickle.dumps(error))
        assert str(clone) == message
        assert payload(clone) == payload(error) and payload(error)


def test_singletons() -> None:
    assert isinstance(DisconnectError.INSTANCE, DisconnectError)
    assert str(DisconnectError.INSTANCE) == ""
    assert isinstance(CoordinatorNotAvailableError.INSTANCE, CoordinatorNotAvailableError)
    # Java's CoordinatorNotAvailableException() is private: only INSTANCE uses it.
    with pytest.raises(TypeError):
        CoordinatorNotAvailableError()  # type: ignore[call-arg]
    assert WakeupError().args == ()
    with pytest.raises(TypeError):
        WakeupError(message="Java's WakeupException has only ()")  # type: ignore[call-arg]


# ----------------------------------------------------------------------------
# copy / pickle rebuild through the keyword-only constructors
# ----------------------------------------------------------------------------


@pytest.mark.parametrize("error", [
    KafkaError(message="m"),
    KafkaError(),
    TopicAuthorizationError(unauthorized_topics={"t"}),
    ConfigError(name="a", value=1, message="bad"),
    IllegalStateError(message="root"),
    NoOffsetForPartitionError(partition=_TP),
    LogTruncationError(message="m", fetch_offsets={_TP: 3},
                       divergent_offsets={_TP: OffsetAndMetadata(offset=2)}),
])
def test_copy_and_pickle_rebuild_from_the_constructor_arguments(error: BaseException) -> None:
    for clone in (copy.copy(error), copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
        assert type(clone) is type(error)
        assert str(clone) == str(error)
        assert clone.args == error.args


def test_pickle_keeps_the_constructor_cause() -> None:
    clone = pickle.loads(pickle.dumps(KafkaError(message="m", cause=ValueError("c"))))
    assert type(clone.__cause__) is ValueError and str(clone.__cause__) == "c"


def test_singletons_copy_and_pickle_as_themselves() -> None:
    for s in (DisconnectError.INSTANCE, CoordinatorNotAvailableError.INSTANCE):
        assert copy.copy(s) is s
        assert pickle.loads(pickle.dumps(s)) is s


# ----------------------------------------------------------------------------
# Core -> Python, through a fake C extension
# ----------------------------------------------------------------------------


def _fake_lib(ffi_id: int, message: str, payload: object = None,
              source: dict[int, tuple[int, str]] | None = None) -> type:
    sources = source or {}

    class _FakeLib:
        @staticmethod
        def KafkaError_code(handle: int) -> int:
            return sources[handle][0] if handle in sources else ffi_id

        @staticmethod
        def KafkaError_message(handle: int) -> str:
            return sources[handle][1] if handle in sources else message

        @staticmethod
        def KafkaError_source(handle: int) -> int:
            return handle + 1 if handle + 1 in sources else 0

        @staticmethod
        def KafkaError_payload(handle: int) -> object:
            return None if handle in sources else payload

        @staticmethod
        def KafkaError_destroy(handle: int) -> None:
            pass

    return _FakeLib


def test_from_ffi_error_unknown_id_is_the_base(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(errmod, "_lib", _fake_lib(999_999, "some message"))
    err = from_ffi_error(0)
    assert type(err) is KafkaError and str(err) == "some message"


def test_from_ffi_error_minus_one_is_unknown_server_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(errmod, "_lib", _fake_lib(-1, "boom"))
    assert type(from_ffi_error(0)) is UnknownServerError


def test_from_ffi_error_chains_the_core_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(errmod, "_lib", _fake_lib(
        -1, "outer", source={1: (TopicAuthorizationError._ffi_id, "inner")}))
    err = from_ffi_error(0)
    assert isinstance(err.__cause__, TopicAuthorizationError)
    assert str(err.__cause__) == "inner"
    explicit = ValueError("wins")
    assert from_ffi_error(0, cause=explicit).__cause__ is explicit


@pytest.mark.parametrize("outer", [UnknownServerError, LogTruncationError])
def test_copy_and_pickle_keep_the_core_cause(monkeypatch: pytest.MonkeyPatch,
                                              outer: type[BaseException]) -> None:
    # The core's cause is set after construction, so the constructor arguments
    # do not carry it. LogTruncationError is built without its constructor
    # (the core reports no offsets), the other path of __reduce__.
    monkeypatch.setattr(errmod, "_lib", _fake_lib(
        outer._ffi_id,  # type: ignore[attr-defined]
        "outer", source={1: (TopicAuthorizationError._ffi_id, "inner")}))
    err = from_ffi_error(0)
    assert type(err) is outer
    for clone in (copy.copy(err), copy.deepcopy(err), pickle.loads(pickle.dumps(err))):
        assert type(clone) is outer and str(clone) == "outer"
        assert type(clone.__cause__) is TopicAuthorizationError
        assert str(clone.__cause__) == "inner"


_PAYLOADS: list[tuple[type[BaseException], dict[str, Any], str, Any]] = [
    (TopicAuthorizationError, {"unauthorized_topics": ["a", "b"]},
     "unauthorized_topics", {"a", "b"}),
    (GroupAuthorizationError, {"group_id": "g1"}, "group_id", "g1"),
    (GroupAuthorizationError, {"group_id": None}, "group_id", None),
    (InvalidTopicError, {"invalid_topics": ["x"]}, "invalid_topics", {"x"}),
    (ThrottlingQuotaExceededError, {"throttle_time_ms": 250}, "throttle_time_ms", 250),
    (DuplicateResourceError, {"resource": "r1"}, "resource", "r1"),
    (ResourceNotFoundError, {"resource": None}, "resource", None),
    (RecordTooLargeError, {"record_too_large_partitions": {("t", 0): 99}},
     "record_too_large_partitions", {_TP: 99}),
    (RecordTooLargeError, {"record_too_large_partitions": None},
     "record_too_large_partitions", None),
    (OffsetOutOfRangeError, {"offset_out_of_range_partitions": {("t", 0): 42}},
     "partitions", {_TP}),
    (LogTruncationError, {"offset_out_of_range_partitions": {("t", 0): 10},
                          "divergent_offsets": {("t", 0): (7, "", None)}},
     "divergent_offsets", {_TP: OffsetAndMetadata(offset=7)}),
    (NoOffsetForPartitionError, {"partitions": [("t", 0)]}, "partitions", {_TP}),
]


@pytest.mark.parametrize("cls,payload,getter,expected", _PAYLOADS)
def test_from_ffi_error_builds_the_payload_through_the_java_constructor(
        monkeypatch: pytest.MonkeyPatch, cls: type[BaseException], payload: dict[str, Any],
        getter: str, expected: Any) -> None:
    ffi_id: int = cls._ffi_id  # type: ignore[attr-defined]
    monkeypatch.setattr(errmod, "_lib", _fake_lib(ffi_id, "core message", payload))
    err = from_ffi_error(0)
    assert type(err) is cls
    assert getattr(err, getter)() == expected


def test_from_ffi_error_quota_violation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(errmod, "_lib", _fake_lib(QuotaViolationError._ffi_id, "over", {
        "metric_name": "rate", "metric_group": "producer", "value": 9.5, "bound": 5.0}))
    err = from_ffi_error(0)
    assert isinstance(err, QuotaViolationError)
    assert err.metric().metric_name().name() == "rate"
    assert err.metric().metric_name().group() == "producer"
    assert err.metric().metric_value() == 9.5
    assert (err.value(), err.bound()) == (9.5, 5.0)


def test_from_ffi_error_keeps_the_message_when_java_needs_more(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # A LogTruncationError the core reports without its offsets: no Java
    # constructor takes a message alone, so only the message is kept.
    monkeypatch.setattr(errmod, "_lib", _fake_lib(LogTruncationError._ffi_id,
                                                  "truncated"))
    err = from_ffi_error(0)
    assert type(err) is LogTruncationError and str(err) == "truncated"
    clone = pickle.loads(pickle.dumps(err))
    assert type(clone) is LogTruncationError and str(clone) == "truncated"


# ----------------------------------------------------------------------------
# An error Java cannot serialize does not pickle or deep-copy
# ----------------------------------------------------------------------------

_RDE_REFUSED = ("RecordDeserializationError cannot be pickled or deep-copied, as Java's "
                "RecordDeserializationException cannot be serialized")
_QUOTA_REFUSED = ("QuotaViolationError cannot be pickled or deep-copied, as Java's "
                  "QuotaViolationException cannot be serialized")


def _record_deserialization_error() -> RecordDeserializationError:
    return RecordDeserializationError(
        origin=RecordDeserializationError.DeserializationExceptionOrigin.KEY, partition=_TP,
        offset=1, timestamp=2, timestamp_type=TimestampType.CREATE_TIME,
        key_buffer=b"k", value_buffer=None, headers=[("h", b"v")], message="m",
        cause=ValueError("c"))


def _add_note(error: BaseException, note: str) -> None:
    """``error.add_note(note)``. ``BaseException.add_note`` is Python 3.11+; on
    3.10 the note is the ``__notes__`` list ``add_note`` would have made."""
    if sys.version_info >= (3, 11):
        error.add_note(note)
    else:
        error.__notes__ = [*getattr(error, "__notes__", ()), note]


@pytest.mark.parametrize("make,refused", [
    # RecordDeserializationException's keyBuffer / valueBuffer (ByteBuffer) and
    # headers (Headers) are not Serializable; nor is QuotaViolationException's
    # metric (KafkaMetric).
    (_record_deserialization_error, _RDE_REFUSED),
    (_quota_violation, _QUOTA_REFUSED),
])
def test_an_error_java_cannot_serialize_refuses_pickle_and_deepcopy(
        make: Callable[[], BaseException], refused: str) -> None:
    error = make()
    _add_note(error, "a note")
    error.extra = 1  # type: ignore[attr-defined]
    with pytest.raises(TypeError) as pickled:
        pickle.dumps(error)
    assert str(pickled.value) == refused
    with pytest.raises(TypeError) as deep:
        copy.deepcopy(error)
    assert str(deep.value) == refused
    # copy.copy rebuilds from the same arguments, which it shares.
    shallow = copy.copy(error)
    assert type(shallow) is type(error) and shallow is not error
    assert str(shallow) == str(error) and shallow.args == error.args
    assert shallow.__notes__ == ["a note"] and shallow.extra == 1  # type: ignore[attr-defined]
    if isinstance(error, RecordDeserializationError):
        assert isinstance(shallow, RecordDeserializationError)
        assert shallow.__cause__ is error.__cause__
        assert shallow.headers() == error.headers()
        assert bytes(shallow.key_buffer() or b"") == b"k"
    else:
        assert isinstance(error, QuotaViolationError) and isinstance(shallow, QuotaViolationError)
        assert shallow.metric() is error.metric()
        assert (shallow.value(), shallow.bound()) == (9.5, 5.0)


def test_a_core_record_deserialization_error_refuses_pickle_and_deepcopy(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Built without its constructor (no payload reported), it still refuses:
    # Java's class cannot be serialized whatever it holds.
    monkeypatch.setattr(errmod, "_lib", _fake_lib(
        RecordDeserializationError._ffi_id, "bad record",
        source={1: (TopicAuthorizationError._ffi_id, "inner")}))
    err = from_ffi_error(0)
    assert type(err) is RecordDeserializationError and str(err) == "bad record"
    for refuse in (pickle.dumps, copy.deepcopy):
        with pytest.raises(TypeError) as refused:
            refuse(err)
        assert str(refused.value) == _RDE_REFUSED
    shallow = copy.copy(err)
    assert type(shallow) is RecordDeserializationError and str(shallow) == "bad record"
    assert shallow.__cause__ is err.__cause__
    assert type(shallow.__cause__) is TopicAuthorizationError


_RDE_PAYLOAD: dict[str, Any] = {
    "origin": "VALUE", "topic_partition": ("t", 0), "offset": 7, "timestamp": 1234,
    "timestamp_type": 1, "key_buffer": b"k", "value_buffer": None,
    "headers": [("h", b"v"), ("n", None)],
}


def test_a_core_record_deserialization_error_carries_its_payload(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The C extension reads the record_deserialization view: Java's getters
    # answer the record the core failed on; the cause is the core's source.
    monkeypatch.setattr(errmod, "_lib", _fake_lib(
        RecordDeserializationError._ffi_id, "bad record", _RDE_PAYLOAD,
        source={1: (TopicAuthorizationError._ffi_id, "inner")}))
    err = from_ffi_error(0)
    assert type(err) is RecordDeserializationError and str(err) == "bad record"
    assert err.origin() is RecordDeserializationError.DeserializationExceptionOrigin.VALUE
    assert err.topic_partition() == _TP
    assert (err.offset(), err.timestamp()) == (7, 1234)
    assert err.timestamp_type() is TimestampType.LOG_APPEND_TIME
    assert bytes(err.key_buffer() or b"") == b"k" and err.value_buffer() is None
    assert [(h.key(), None if h.value() is None else bytes(h.value()))  # type: ignore[arg-type]
            for h in err.headers()] == [("h", b"v"), ("n", None)]
    assert type(err.__cause__) is TopicAuthorizationError
    for refuse in (pickle.dumps, copy.deepcopy):
        with pytest.raises(TypeError) as refused:
            refuse(err)
        assert str(refused.value) == _RDE_REFUSED
    # A borrowed error (an admin result's, a delivery callback's) is built the same way.
    borrowed = from_borrowed_ffi_error((RecordDeserializationError._ffi_id, "bad record",
                                        False, False, _RDE_PAYLOAD, False, None))
    assert type(borrowed) is RecordDeserializationError
    assert borrowed.offset() == 7 and borrowed.topic_partition() == _TP


# ----------------------------------------------------------------------------
# Notes travel with copy and pickle, as on Python's own exceptions
# ----------------------------------------------------------------------------


def test_copy_and_pickle_keep_the_notes(monkeypatch: pytest.MonkeyPatch) -> None:
    built = TopicAuthorizationError(unauthorized_topics={"t"})
    monkeypatch.setattr(errmod, "_lib", _fake_lib(LogTruncationError._ffi_id, "truncated"))
    core = from_ffi_error(0)
    for error in (built, core):
        _add_note(error, "first")
        _add_note(error, "second")
        for clone in (copy.copy(error), copy.deepcopy(error), pickle.loads(pickle.dumps(error))):
            assert type(clone) is type(error) and str(clone) == str(error)
            assert clone.__notes__ == ["first", "second"]


def test_a_core_correlation_id_mismatch_reads_unknown_ids(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The core reports no payload for it (no C accessor, CLAUDE.md §4): the
    # error is still built through its constructor, with -1 for both ids.
    monkeypatch.setattr(errmod, "_lib", _fake_lib(CorrelationIdMismatchError._ffi_id,
                                                  "mismatch"))
    err = from_ffi_error(0)
    assert type(err) is CorrelationIdMismatchError and str(err) == "mismatch"
    assert (err.request_correlation_id(), err.response_correlation_id()) == (-1, -1)
    clone = pickle.loads(pickle.dumps(err))
    assert (clone.request_correlation_id(), clone.response_correlation_id()) == (-1, -1)


def test_real_native_plain_error_has_no_payload() -> None:
    lib = pytest.importorskip("_confluentkafka")
    handle = lib.KafkaError_new(-1, "boom")
    try:
        assert lib.KafkaError_payload(handle) is None
    finally:
        lib.KafkaError_destroy(handle)


def test_real_native_round_trip() -> None:
    lib = pytest.importorskip("_confluentkafka")
    err = from_ffi_error(lib.KafkaError_new(TopicAuthorizationError._ffi_id, "no access"))
    assert type(err) is TopicAuthorizationError
    assert str(err) == "no access"


def test_unsupported_operation_error_is_java_unsupported_operation_exception() -> None:
    # Java's java.lang.UnsupportedOperationException: the core's
    # LOCAL_UNSUPPORTED_OPERATION, a root class subclassing NotImplementedError.
    ffi_id = _ffi_enum()["LOCAL_UNSUPPORTED_OPERATION"]
    assert UnsupportedOperationError._ffi_id == ffi_id
    assert class_for_ffi_id(ffi_id) is UnsupportedOperationError
    assert issubclass(UnsupportedOperationError, NotImplementedError)
    assert str(UnsupportedOperationError()) == ""
    assert str(UnsupportedOperationError(message="m")) == "m"
    cause = ValueError("v")
    e = UnsupportedOperationError(message="m", cause=cause)
    assert e.__cause__ is cause
    # Java's UnsupportedOperationException(Throwable cause): the message is cause.toString().
    assert str(UnsupportedOperationError(cause=cause)) == "builtins.ValueError: v"
    # A bare NotImplementedError is not the Kafka class: it crosses as a foreign one.
    assert to_ffi_id(NotImplementedError("x")) == -1


def test_unsupported_operation_error_from_the_core() -> None:
    ffi_id = _ffi_enum()["LOCAL_UNSUPPORTED_OPERATION"]
    err = from_borrowed_ffi_error((ffi_id, "m", False, False, None, False, None))
    assert type(err) is UnsupportedOperationError
    assert str(err) == "m"


def test_real_native_unsupported_operation_error_round_trip() -> None:
    # Python -> core -> Python: the class's id builds the core's
    # LocalUnsupportedOperationError, which comes back as the same class.
    lib = pytest.importorskip("_confluentkafka")
    injected = UnsupportedOperationError(message="x")
    err = from_ffi_error(lib.KafkaError_new(to_ffi_id(injected), "x"))
    assert type(err) is UnsupportedOperationError
    assert str(err) == "x"


def test_real_native_payload_is_read_through_the_sub_handle() -> None:
    # Java's TopicAuthorizationException(String message) passes emptySet().
    lib = pytest.importorskip("_confluentkafka")
    handle = lib.KafkaError_new(TopicAuthorizationError._ffi_id, "no access")
    try:
        assert lib.KafkaError_payload(handle) == {"unauthorized_topics": []}
    finally:
        lib.KafkaError_destroy(handle)
    err = from_ffi_error(lib.KafkaError_new(TopicAuthorizationError._ffi_id, "no access"))
    assert isinstance(err, TopicAuthorizationError) and err.unauthorized_topics() == set()


# ----------------------------------------------------------------------------
# The type is the predicate
# ----------------------------------------------------------------------------


def test_the_type_is_the_predicate() -> None:
    assert issubclass(NotLeaderOrFollowerError, RetriableError)
    with pytest.raises(RetriableError):
        raise NotLeaderOrFollowerError(message="no leader")
    with pytest.raises(KafkaError):
        raise TopicAuthorizationError(message="nope")
    for name in ("code", "is_retriable", "is_fatal"):
        assert not hasattr(KafkaError(message="x"), name)


def test_illegal_argument_error_is_a_value_error_and_a_runtime_error() -> None:
    # Owner ruling 7 (2026-10-20): Python's class for a bad argument value,
    # then Java's RuntimeException chain, in that order.
    assert IllegalArgumentError.__bases__ == (ValueError, RuntimeError)
    assert not issubclass(IllegalArgumentError, KafkaError)
    for caught in (ValueError, RuntimeError):
        try:
            raise IllegalArgumentError(message="bad")
        except caught as e:  # noqa: PERF203
            assert str(e) == "bad"
    original = IllegalArgumentError(message="bad", cause=ValueError("v"))
    for rebuilt in (pickle.loads(pickle.dumps(original)), copy.copy(original),
                    copy.deepcopy(original)):
        assert type(rebuilt) is IllegalArgumentError
        assert str(rebuilt) == "bad"
        assert isinstance(rebuilt, ValueError) and isinstance(rebuilt, RuntimeError)
        assert type(rebuilt.__cause__) is ValueError


def test_unsupported_operation_error_is_a_not_implemented_error() -> None:
    # Owner ruling 8 (2026-10-20): Python's class for an operation an object
    # does not provide; NotImplementedError subclasses RuntimeError, so Java's
    # RuntimeException chain is kept.
    assert UnsupportedOperationError.__bases__ == (NotImplementedError,)
    assert IndexOutOfBoundsError.__bases__ == (IndexError, RuntimeError)
    assert issubclass(UnsupportedOperationError, RuntimeError)
    assert not issubclass(UnsupportedOperationError, KafkaError)
    for caught in (NotImplementedError, RuntimeError):
        try:
            raise UnsupportedOperationError(message="Not implemented yet.")
        except caught as e:  # noqa: PERF203
            assert str(e) == "Not implemented yet."
    original = UnsupportedOperationError(message="Not implemented yet.")
    for rebuilt in (pickle.loads(pickle.dumps(original)), copy.copy(original),
                    copy.deepcopy(original)):
        assert type(rebuilt) is UnsupportedOperationError
        assert str(rebuilt) == "Not implemented yet."
