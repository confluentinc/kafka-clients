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
Python Binding Conventions, Signatures), for the hand-written admin classes.

The counts are read from the source with ``ast`` (the ``.pyi`` where one exists):
``Admin.java``'s overloads per method, in ``Admin``, ``AsyncAdmin`` and both mocks;
``deleteTopics`` and ``describeTopics`` have four (two types of one name, each with
and without options), ``listConsumerGroupOffsets`` and
``listPartitionReassignments`` four, ``describeUserScramCredentials`` three, every
other RPC with an options overload two; a method with one Java overload
(``removeMembersFromConsumerGroup``, ``create``) has none.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

import pytest

ADMIN = Path(__file__).resolve().parents[2] / "confluent_kafka" / "admin"

#: Admin.java's overloads per RPC (non-deprecated, generated ones only).
RPC_STUBS: dict[str, int] = {
    "close": 2, "create_topics": 2, "delete_topics": 4, "list_topics": 2,
    "describe_topics": 4, "describe_cluster": 2, "describe_acls": 2, "create_acls": 2,
    "delete_acls": 2, "describe_configs": 2, "incremental_alter_configs": 2,
    "alter_replica_log_dirs": 2, "describe_log_dirs": 2, "describe_replica_log_dirs": 2,
    "create_partitions": 2, "delete_records": 2, "create_delegation_token": 2,
    "renew_delegation_token": 2, "expire_delegation_token": 2,
    "describe_delegation_token": 2, "describe_consumer_groups": 2,
    "list_consumer_group_offsets": 4, "delete_consumer_groups": 2,
    "delete_consumer_group_offsets": 2, "list_groups": 2, "elect_leaders": 2,
    "alter_partition_reassignments": 2, "list_partition_reassignments": 4,
    "alter_consumer_group_offsets": 2, "list_offsets": 2, "describe_client_quotas": 2,
    "alter_client_quotas": 2, "describe_user_scram_credentials": 3,
    "alter_user_scram_credentials": 2, "describe_features": 2, "update_features": 2,
    "unregister_controller": 2, "describe_producers": 2, "describe_transactions": 2,
    "abort_transaction": 2, "list_transactions": 2, "fence_producers": 2,
    "list_config_resources": 2, "describe_classic_groups": 2,
    "force_terminate_transaction": 2,
}
MOCK_STUBS = {**RPC_STUBS, "__init__": 2}

#: (file, class) -> expected stubs per method.
EXPECTED: dict[tuple[str, str], dict[str, int]] = {
    ("admin.py", "Admin"): RPC_STUBS,
    ("async_admin.py", "AsyncAdmin"): RPC_STUBS,
    ("mock_admin_client.py", "MockAdminClient"): MOCK_STUBS,
    ("async_mock_admin_client.py", "AsyncMockAdminClient"): MOCK_STUBS,
    ("_mock_core.py", "MockAdminClientCore"): {"add_topic": 2},
    ("new_topic.py", "NewTopic"): {"__init__": 3, "configs": 2},
    ("new_partitions.py", "NewPartitions"): {"increase_to": 2},
    ("classic_group_description.py", "ClassicGroupDescription"): {"__init__": 2},
    ("config_entry.py", "ConfigEntry"): {"__init__": 2},
    ("log_dir_description.py", "LogDirDescription"): {"__init__": 3},
    ("topic_description.py", "TopicDescription"): {"__init__": 3},
    ("user_scram_credential_upsertion.py", "UserScramCredentialUpsertion"): {"__init__": 3},
    # Java's partitionsToOffsetAndMetadata() and (String groupId), per result kind.
    ("list_consumer_group_offsets_result.pyi", "ListConsumerGroupOffsetsResult"):
        {"partitions_to_offset_and_metadata": 4},
}


def _is_overload(node: ast.AST) -> bool:
    return isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
        isinstance(d, ast.Name) and d.id == "overload" for d in node.decorator_list)


def _class(file: str, name: str) -> ast.ClassDef:
    tree = ast.parse((ADMIN / file).read_text())
    return next(n for n in ast.walk(tree) if isinstance(n, ast.ClassDef) and n.name == name)


@pytest.mark.parametrize(("file", "cls"), list(EXPECTED), ids=[c for _, c in EXPECTED])
def test_stub_count_per_method(file: str, cls: str) -> None:
    counts = Counter(n.name for n in _class(file, cls).body if _is_overload(n))
    expected = EXPECTED[(file, cls)]
    assert {m: counts[m] for m in expected} == expected
    # Only the expected methods have stubs (for the value classes and the result,
    # the kind-binding stubs of the result's other getters are out of scope).
    if cls in ("Admin", "AsyncAdmin", "MockAdminClient", "AsyncMockAdminClient"):
        assert set(counts) == set(expected)


@pytest.mark.parametrize(("file", "cls"), list(EXPECTED), ids=[c for _, c in EXPECTED])
def test_no_stub_carries_a_default(file: str, cls: str) -> None:
    for node in _class(file, cls).body:
        if _is_overload(node):
            assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            assert not node.args.defaults and all(
                d is None for d in node.args.kw_defaults), f"{cls}.{node.name}:{node.lineno}"


@pytest.mark.parametrize(("file", "cls"), [("async_admin.py", "AsyncAdmin"),
                                           ("async_mock_admin_client.py", "AsyncMockAdminClient")])
def test_async_stubs_are_async_only_for_close(file: str, cls: str) -> None:
    for node in _class(file, cls).body:
        if _is_overload(node):
            assert isinstance(node, ast.AsyncFunctionDef) == (node.name == "close"), node.name
