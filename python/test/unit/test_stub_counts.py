# Copyright 2026 Confluent Inc.
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

"""One ``@typing.overload`` stub per Java overload, with no defaults (CLAUDE.md,
Python Binding Conventions, Signatures): the stub counts per class, read from the
source with ``ast`` (inline stubs of a ``.py`` file, or a generated ``.pyi``)."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import confluent_kafka

_PACKAGE = Path(confluent_kafka.__file__).parent


def _is_overload(d: ast.expr) -> bool:
    return (isinstance(d, ast.Name) and d.id == "overload") or (
        isinstance(d, ast.Attribute) and d.attr == "overload")


def _stubs(relative: str, cls: str, method: str) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    path = _PACKAGE / relative
    if path.with_suffix(".pyi").exists():
        path = path.with_suffix(".pyi")
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == cls:
            return [item for item in node.body
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and item.name == method and any(_is_overload(d) for d in item.decorator_list)]
    raise AssertionError(f"{relative}: no class {cls}")


_CONSUMER = {"subscribe": 4, "commit": 4, "commit_nowait": 3, "seek": 2, "position": 2,
             "committed": 2, "partitions_for": 2, "list_topics": 2, "offsets_for_times": 2,
             "beginning_offsets": 2, "end_offsets": 2, "close": 2}
_PRODUCER = {"send": 2, "close": 2}

COUNTS: list[tuple[str, str, str, int]] = (
    [("consumer/consumer.py", "Consumer", m, n) for m, n in _CONSUMER.items()]
    + [("consumer/async_consumer.py", "AsyncConsumer", m, n) for m, n in _CONSUMER.items()]
    + [("producer/producer.py", "Producer", m, n) for m, n in _PRODUCER.items()]
    + [("producer/async_producer.py", "AsyncProducer", m, n) for m, n in _PRODUCER.items()]
    + [("producer/mock_producer.py", "MockProducer", m, n) for m, n in _PRODUCER.items()]
    + [("producer/async_mock_producer.py", "AsyncMockProducer", m, n)
       for m, n in _PRODUCER.items()]
    + [
        ("common/kafka_future.py", "KafkaFuture", "get", 2),
        ("common/kafka_future.py", "AsyncKafkaFuture", "get", 2),
        ("consumer/offset_and_metadata.py", "OffsetAndMetadata", "__init__", 3),
        ("consumer/offset_and_timestamp.py", "OffsetAndTimestamp", "__init__", 2),
        ("common/partition_info.py", "PartitionInfo", "__init__", 2),
        ("consumer/consumer_record.py", "ConsumerRecord", "__init__", 12),
        ("producer/producer_record.py", "ProducerRecord", "__init__", 22),
        ("common/node.py", "Node", "__init__", 3),
        ("common/topic_id_partition.py", "TopicIdPartition", "__init__", 2),
        ("common/topic_partition_info.py", "TopicPartitionInfo", "__init__", 2),
        ("common/kafka_principal.py", "KafkaPrincipal", "__init__", 2),
        ("common/token_information.py", "TokenInformation", "__init__", 2),
        ("consumer/consumer_records.py", "ConsumerRecords", "records", 2),
        # RecordHeaders' (), (Header[]) and (Iterable<Header>); Headers.add's two.
        ("common/headers.py", "Headers", "__init__", 3),
        ("common/headers.py", "Headers", "add", 2),
    ]
)


@pytest.mark.parametrize(("relative", "cls", "method", "count"), COUNTS,
                         ids=[f"{c}.{m}" for _, c, m, _ in COUNTS])
def test_one_stub_per_java_overload(relative: str, cls: str, method: str, count: int) -> None:
    assert len(_stubs(relative, cls, method)) == count


def _defaults(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    return [ast.unparse(d) for d in fn.args.kw_defaults if d is not None] + [
        ast.unparse(d) for d in fn.args.defaults]


def test_no_stub_carries_a_default() -> None:
    # A setter-built class's keyword constructor (Java starting values) is one
    # plain signature, not @overload stubs; a descriptor's __get__ stubs are
    # Python's protocol, not a Java overload set.
    offenders: list[str] = []
    for path in sorted(_PACKAGE.rglob("*.py*")):
        if path.suffix not in (".py", ".pyi") or "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            dunder = node.name.startswith("__") and node.name != "__init__" if isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef)) else False
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not dunder and any(
                    _is_overload(d) for d in node.decorator_list):
                if _defaults(node):
                    offenders.append(f"{path.relative_to(_PACKAGE)}:{node.lineno} {node.name}")
    assert offenders == []
