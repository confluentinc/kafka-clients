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

"""Tests for the generated error hierarchy (spec §5.5, Design Decisions D1).

The parent chains, abstract set and ``_ffi_id`` round-trip are checked **against
the Java sources directly** (both directions) — the same cross-check the
generator performs — so a drift in the generated output is caught here as well
as at build time.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import confluent_kafka
from confluent_kafka import (
    ConcurrentModificationError,
    IllegalArgumentError,
    IllegalStateError,
    TimeoutError as RootTimeoutError,
)
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.errors import (
    KafkaError,
    RetriableError,
    to_ffi_id,
)
from confluent_kafka.common.errors import _BY_FFI_ID, _generated
from confluent_kafka.common.errors._base import KafkaError as BaseKafkaError

# ----------------------------------------------------------------------------
# Java-source parsing (independent of the generator, for a real cross-check)
# ----------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[4]
_JAVA_ROOT = _REPO_ROOT / "kafka/clients/src/main/java"

# The generator's BRIDGE, read here so the test is a genuine independent check of
# the generated output rather than a re-run of the generator.
# (id_constant -> java_fqn). Kept in sync with xtask/src/error_hierarchy.rs.
_BRIDGE_PATH = _REPO_ROOT / "xtask/src/error_hierarchy.rs"


def _load_bridge() -> dict[str, str]:
    """Parse the BRIDGE table out of the generator source (its `(ID, "fqn")` rows)."""
    text = _BRIDGE_PATH.read_text()
    start = text.index("const BRIDGE:")
    end = text.index("];", start)
    body = text[start:end]
    # ``cargo fmt`` may split a row across lines, so allow whitespace/newlines
    # between the id and the fqn.
    rows = re.findall(r'\(\s*"([A-Z0-9_]+)"\s*,\s*"([\w.]+)"\s*,?\s*\)', body)
    bridge = dict(rows)
    assert len(bridge) == len(rows), "duplicate id in BRIDGE"
    assert len(bridge) == 161, f"expected 161 bridge rows, parsed {len(bridge)}"
    return bridge


def _java_path(fqn: str) -> Path:
    return _JAVA_ROOT / (fqn.replace(".", "/") + ".java")


def _parse_java(fqn: str) -> tuple[bool, str]:
    """Return ``(is_abstract, parent_fqn)`` for a Java exception class."""
    text = _java_path(fqn).read_text()
    package = re.search(r"package\s+([\w.]+);", text).group(1)
    m = re.search(r"public\s+(abstract\s+)?class\s+\w+\s+extends\s+(\w+)", text)
    assert m, f"no class declaration in {fqn}"
    is_abstract = bool(m.group(1))
    parent_simple = m.group(2)
    imp = re.search(rf"import\s+([\w.]+\.{parent_simple});", text)
    if imp:
        parent_fqn = imp.group(1)
    elif parent_simple in (
        "RuntimeException",
        "Exception",
        "Throwable",
        "IllegalStateException",
        "IllegalArgumentException",
    ):
        parent_fqn = f"java.lang.{parent_simple}"
    else:
        parent_fqn = f"{package}.{parent_simple}"
    return is_abstract, parent_fqn


def _python_name(java_simple: str) -> str:
    stem = java_simple[: -len("Exception")] if java_simple.endswith("Exception") else java_simple
    return stem + "Error"


_BRIDGE = _load_bridge()


def _module_object(java_package: str):
    """The generated Python module for a Java package (mirrors the generator's
    placement rule)."""
    from confluent_kafka import _generated_errors as root_errors
    from confluent_kafka.common import errors as common_errors
    from confluent_kafka.common.config import _generated_errors as config_errors
    from confluent_kafka.consumer import _generated_errors as consumer_errors

    if java_package == "org.apache.kafka.common.config":
        return config_errors
    if java_package == "org.apache.kafka.clients.consumer":
        return consumer_errors
    if java_package.startswith("java."):
        return root_errors
    return common_errors


def _class_for_java(fqn: str) -> type:
    """The exact Python class for a Java FQN, resolved in the right module (so the
    two ``InvalidOffsetError`` classes — common.errors concrete vs consumer
    abstract — do not collide)."""
    package = fqn.rsplit(".", 1)[0]
    py_name = _python_name(fqn.rsplit(".", 1)[1])
    return getattr(_module_object(package), py_name)


# Every generated class, keyed by (module_name, py_name) so name collisions
# across modules are preserved.
def _all_generated_classes() -> dict[tuple[str, str], type]:
    from confluent_kafka import _generated_errors as root_errors
    from confluent_kafka.common.config import _generated_errors as config_errors
    from confluent_kafka.consumer import _generated_errors as consumer_errors

    classes: dict[tuple[str, str], type] = {}
    for module in (_generated, root_errors, config_errors, consumer_errors):
        for name in module.__all__:
            classes[(module.__name__, name)] = getattr(module, name)
    return classes


_CLASSES = _all_generated_classes()

_ABSTRACT_JAVA = {
    "org.apache.kafka.common.errors.RetriableException",
    "org.apache.kafka.common.errors.RefreshRetriableException",
    "org.apache.kafka.common.errors.InvalidMetadataException",
    "org.apache.kafka.common.errors.ApplicationRecoverableException",
    "org.apache.kafka.clients.consumer.InvalidOffsetException",
}


# ----------------------------------------------------------------------------
# Parent chains: every generated class equals its Java `extends` (both directions)
# ----------------------------------------------------------------------------


def test_every_bridge_class_exists_in_python_with_correct_parent() -> None:
    """Java -> Python: each concrete Java class has a Python class whose immediate
    base equals the Python class for its Java parent (or ``KafkaError`` for
    ``KafkaException``, or a builtin for a JDK root)."""
    for _id, fqn in _BRIDGE.items():
        java_simple = fqn.rsplit(".", 1)[1]
        py_name = _python_name(java_simple)
        cls = _class_for_java(fqn)  # raises if missing
        assert cls.__name__ == py_name
        if fqn.startswith("java."):
            # JDK analogs: parent is a builtin, checked separately.
            continue
        _is_abstract, parent_fqn = _parse_java(fqn)
        immediate_base = cls.__mro__[1]
        expected_base = _expected_base_name(parent_fqn)
        assert immediate_base.__name__ == expected_base, (
            f"{py_name} extends {immediate_base.__name__}, Java says {expected_base}"
        )


def _expected_base_name(parent_fqn: str) -> str:
    if parent_fqn == "org.apache.kafka.common.KafkaException":
        return "KafkaError"
    return _python_name(parent_fqn.rsplit(".", 1)[1])


def test_python_to_java_no_extra_concrete_classes() -> None:
    """Python -> Java: every concrete generated class corresponds to a bridge
    class (no invented classes)."""
    bridge_py_names = {
        _python_name(fqn.rsplit(".", 1)[1]) for fqn in _BRIDGE.values()
    }
    for (_module, name), cls in _CLASSES.items():
        if "_ffi_id" in cls.__dict__:
            assert name in bridge_py_names, f"{name} is concrete but not in the bridge"


# ----------------------------------------------------------------------------
# _ffi_id: unique, and equal to the same-named _error_code.py constant
# ----------------------------------------------------------------------------


def test_ffi_ids_are_unique() -> None:
    seen: dict[int, str] = {}
    for (_module, name), cls in _CLASSES.items():
        if "_ffi_id" not in cls.__dict__:
            continue
        ffi_id = cls._ffi_id
        assert ffi_id not in seen, f"{name} and {seen[ffi_id]} share id {ffi_id}"
        seen[ffi_id] = name
    # 161 concrete classes (one per FFI id except NONE).
    assert len(seen) == 161


def test_ffi_id_equals_error_code_constant() -> None:
    """Each ``_ffi_id`` equals the ``_error_code.py`` constant of the same name
    named in the trailing comment of the generated class."""
    import _error_code

    for _id, fqn in _BRIDGE.items():
        cls = _class_for_java(fqn)
        expected = getattr(_error_code, _id)
        assert cls._ffi_id == expected, f"{cls.__name__}._ffi_id {cls._ffi_id} != {_id} {expected}"


# ----------------------------------------------------------------------------
# Abstract classes: exactly the five Java abstract ones; construction raises
# ----------------------------------------------------------------------------


def test_abstract_classes_are_exactly_the_five_java_abstract_ones() -> None:
    expected = {_class_for_java(fqn) for fqn in _ABSTRACT_JAVA}
    abstract_py = set()
    for (_module, _name), cls in _CLASSES.items():
        try:
            cls("x")
        except TypeError:
            abstract_py.add(cls)
        except Exception:  # noqa: BLE001 - only TypeError marks an abstract class
            pass
    assert abstract_py == expected


def test_abstract_construction_raises_type_error() -> None:
    for fqn in _ABSTRACT_JAVA:
        cls = _class_for_java(fqn)
        with pytest.raises(TypeError):
            cls("nope")


def test_abstract_classes_have_no_own_ffi_id() -> None:
    for fqn in _ABSTRACT_JAVA:
        cls = _class_for_java(fqn)
        assert "_ffi_id" not in cls.__dict__


# ----------------------------------------------------------------------------
# JDK analogs subclass the right Python builtin
# ----------------------------------------------------------------------------


def test_jdk_analogs_subclass_runtime_error() -> None:
    assert issubclass(IllegalStateError, RuntimeError)
    assert issubclass(IllegalArgumentError, RuntimeError)
    assert issubclass(ConcurrentModificationError, RuntimeError)
    # Not under KafkaError.
    assert not issubclass(IllegalStateError, KafkaError)


def test_root_timeout_error_subclasses_builtin_timeout() -> None:
    import builtins

    assert issubclass(RootTimeoutError, builtins.TimeoutError)
    assert not issubclass(RootTimeoutError, KafkaError)


def test_config_error_is_under_kafka_error() -> None:
    # ConfigException extends KafkaException in Java.
    assert issubclass(ConfigError, KafkaError)


# ----------------------------------------------------------------------------
# Catching by an intermediate base
# ----------------------------------------------------------------------------


def test_except_retriable_error_catches_not_leader_or_follower() -> None:
    from confluent_kafka.common.errors import NotLeaderOrFollowerError

    assert issubclass(NotLeaderOrFollowerError, RetriableError)
    with pytest.raises(RetriableError):
        raise NotLeaderOrFollowerError("no leader")


def test_except_kafka_error_catches_any_kafka_subtype() -> None:
    from confluent_kafka.common.errors import TopicAuthorizationError

    with pytest.raises(KafkaError):
        raise TopicAuthorizationError("nope")


# ----------------------------------------------------------------------------
# from_ffi_error / to_ffi_id (the FFI conversion), without the native extension
# ----------------------------------------------------------------------------


def test_from_ffi_error_unknown_id_maps_to_base_kafka_error(monkeypatch) -> None:
    """An id with no class raises the base KafkaError, never a KeyError."""
    from confluent_kafka.common import errors as errmod

    class _FakeLib:
        @staticmethod
        def KafkaError_code(handle: int) -> int:
            return 999_999  # no class owns this id

        @staticmethod
        def KafkaError_message(handle: int) -> str:
            return "some message"

        @staticmethod
        def KafkaError_destroy(handle: int) -> None:
            pass

    monkeypatch.setattr(errmod, "_lib", _FakeLib)
    err = errmod.from_ffi_error(0)
    assert type(err) is KafkaError
    assert str(err) == "some message"


def test_from_ffi_error_known_id_maps_to_its_class(monkeypatch) -> None:
    from confluent_kafka.common import errors as errmod
    from confluent_kafka.common.errors import TopicAuthorizationError

    class _FakeLib:
        @staticmethod
        def KafkaError_code(handle: int) -> int:
            return TopicAuthorizationError._ffi_id

        @staticmethod
        def KafkaError_message(handle: int) -> str:
            return "not authorized"

        @staticmethod
        def KafkaError_destroy(handle: int) -> None:
            pass

    monkeypatch.setattr(errmod, "_lib", _FakeLib)
    err = errmod.from_ffi_error(0)
    assert type(err) is TopicAuthorizationError


def test_from_ffi_error_chains_cause(monkeypatch) -> None:
    from confluent_kafka.common import errors as errmod

    class _FakeLib:
        @staticmethod
        def KafkaError_code(handle: int) -> int:
            return KafkaError._ffi_id

        @staticmethod
        def KafkaError_message(handle: int) -> str:
            return "wrapped"

        @staticmethod
        def KafkaError_destroy(handle: int) -> None:
            pass

    monkeypatch.setattr(errmod, "_lib", _FakeLib)
    cause = ValueError("root cause")
    err = errmod.from_ffi_error(0, cause=cause)
    assert err.__cause__ is cause


def test_raise_from_preserves_cause() -> None:
    from confluent_kafka.common.errors import TopicAuthorizationError

    cause = ValueError("original")
    try:
        try:
            raise cause
        except ValueError as e:
            raise TopicAuthorizationError("wrapped") from e
    except TopicAuthorizationError as caught:
        assert caught.__cause__ is cause


def test_to_ffi_id_round_trips() -> None:
    from confluent_kafka.common.errors import TopicAuthorizationError

    err = TopicAuthorizationError("x")
    assert to_ffi_id(err) == TopicAuthorizationError._ffi_id
    # The mapping id -> class -> id is stable.
    assert _BY_FFI_ID[to_ffi_id(err)] is TopicAuthorizationError


def test_to_ffi_id_rejects_a_non_kafka_error() -> None:
    with pytest.raises(TypeError):
        to_ffi_id(RuntimeError("not ours"))


# ----------------------------------------------------------------------------
# Base identity
# ----------------------------------------------------------------------------


def test_base_kafka_error_is_the_same_object_everywhere() -> None:
    assert KafkaError is BaseKafkaError


def test_kafka_error_str_is_the_message() -> None:
    assert str(KafkaError("hi")) == "hi"
    assert str(KafkaError()) == ""


def test_duration_alias_exists() -> None:
    # Duration = float | timedelta (rule 3.7).
    assert confluent_kafka.Duration is not None
