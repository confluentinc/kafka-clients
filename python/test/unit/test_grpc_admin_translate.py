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

"""The gRPC servers' admin translation (``grpc_translate`` / ``grpc_server``):
proto requests become ``confluent_kafka.admin`` calls, results are resolved into
the per-key envelope, and a typed error crosses as its FFI id.

The generated proto stubs are produced by the Docker build (``Dockerfile.grpc``),
so these tests skip when they, or ``grpcio``, are not importable.
"""

from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

grpc = pytest.importorskip("grpc")
apb = pytest.importorskip("admin_service_pb2")
cpb = pytest.importorskip("consumer_service_pb2")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import grpc_server  # noqa: E402
import grpc_server_async  # noqa: E402
import grpc_translate as tr  # noqa: E402
from confluent_kafka import ExecutionError, UnsupportedOperationError  # noqa: E402
from confluent_kafka import admin as ka  # noqa: E402
from confluent_kafka.admin import DeleteAclsResult  # noqa: E402
from confluent_kafka.common import AsyncKafkaFuture, KafkaFuture  # noqa: E402
from confluent_kafka.common.kafka_future import _KafkaFutureImpl  # noqa: E402
from confluent_kafka.common import Node, TopicPartition  # noqa: E402
from confluent_kafka.common.errors import (  # noqa: E402
    ResourceNotFoundError,
    TopicExistsError,
    UnsupportedVersionError,
)


def _done(value=None, error=None, asynchronous=False):
    """A completed ``KafkaFuture`` (``AsyncKafkaFuture``): holding ``value``, or
    failed with ``error``, as a client's future the facade drives with ``get()``."""
    impl = _KafkaFutureImpl()
    if error is not None:
        impl.complete_with_error(error)
    else:
        impl.complete(value)
    return (AsyncKafkaFuture if asynchronous else KafkaFuture)._of_impl(impl)


@pytest.fixture()
def service():
    svc = grpc_server.AdminService()
    resp = svc.CreateAdmin(apb.CreateAdminRequest(num_brokers=2), None)
    assert not resp.HasField("error")
    return svc, resp.admin_id


def _tp(topic="t1", partition=0):
    return cpb.TopicPartition(topic=topic, partition=partition)


def test_create_admin_with_no_brokers_is_illegal_argument():
    resp = grpc_server.AdminService().CreateAdmin(apb.CreateAdminRequest(num_brokers=0), None)
    assert resp.admin_id == 0
    assert resp.error.code == tr._LOCAL_ILLEGAL_ARGUMENT


def test_unknown_admin_id_is_illegal_state():
    resp = grpc_server.AdminService().CreateTopics(apb.CreateTopicsRequest(admin_id=99), None)
    assert resp.error.code == tr._LOCAL_ILLEGAL_STATE
    assert resp.error.message == "unknown admin_id 99"


def test_topics_round_trip(service):
    svc, aid = service
    topic = apb.NewTopic(name="t1", num_partitions=2, replication_factor=1,
                         configs={"cleanup.policy": "compact"})
    created = svc.CreateTopics(apb.CreateTopicsRequest(admin_id=aid, topics=[topic]), None)
    (entry,) = created.entries
    assert entry.key.name == "t1"
    assert entry.value.metadata.num_partitions == 2
    assert entry.value.metadata.replication_factor == 1
    assert entry.value.metadata.topic_id

    # A second create is a per-key failure that crosses as the class's FFI id.
    again = svc.CreateTopics(apb.CreateTopicsRequest(admin_id=aid, topics=[topic]), None)
    assert again.entries[0].error.code == TopicExistsError._ffi_id
    assert again.entries[0].error.message == "Topic t1 exists already."

    listed = svc.ListTopics(apb.AdminListTopicsRequest(admin_id=aid), None)
    assert [t.name for t in listed.listings] == ["t1"]

    described = svc.DescribeTopics(apb.DescribeTopicsRequest(
        admin_id=aid, names=cpb.StringList(values=["t1"])), None)
    (d,) = described.entries
    assert d.value.name == "t1"
    assert d.value.topic_id == entry.value.metadata.topic_id
    assert [p.partition for p in d.value.partitions] == [0, 1]

    configs = svc.DescribeConfigs(apb.DescribeConfigsRequest(
        admin_id=aid, resources=[apb.ConfigResource(resource_type=2, name="t1")]), None)
    (c,) = configs.entries
    assert c.key.config_resource.resource_type == 2
    assert [(e.name, e.value) for e in c.value.entries] == [("cleanup.policy", "compact")]

    deleted = svc.DeleteTopics(apb.DeleteTopicsRequest(
        admin_id=aid, names=cpb.StringList(values=["t1"])), None)
    assert deleted.entries[0].key.name == "t1"
    assert not deleted.entries[0].HasField("error")

    closed = svc.Close(apb.AdminCloseRequest(admin_id=aid), None)
    assert not closed.HasField("error")
    # Close is idempotent: the id is gone, which is a silent success.
    assert not svc.Close(apb.AdminCloseRequest(admin_id=aid), None).HasField("error")


def test_cluster_and_log_dirs(service):
    svc, aid = service
    svc.CreateTopics(apb.CreateTopicsRequest(admin_id=aid, topics=[
        apb.NewTopic(name="t1", num_partitions=1, replication_factor=1)]), None)
    cluster = svc.DescribeCluster(apb.DescribeClusterRequest(admin_id=aid), None)
    assert [n.id for n in cluster.description.nodes] == [0, 1]
    assert cluster.description.controller.id == 0
    dirs = svc.DescribeLogDirs(apb.DescribeLogDirsRequest(admin_id=aid, brokers=[0]), None)
    (entry,) = dirs.entries
    assert entry.key.broker_id == 0
    assert "/tmp/kafka-logs" in entry.value.log_dirs


def test_whole_call_failure_is_the_top_level_error(service):
    svc, aid = service
    # The Java mock's own "Not implemented yet": a whole-call failure.
    resp = svc.DescribeAcls(apb.DescribeAclsRequest(
        admin_id=aid, filter=apb.AclBindingFilter(
            resource_type=1, pattern_type=1, operation=1, permission_type=1)), None)
    assert resp.error.code == UnsupportedOperationError._ffi_id
    assert resp.error.message == "Not implemented yet"
    assert not resp.acls


def test_malformed_offset_spec_is_illegal_argument(service):
    svc, aid = service
    resp = svc.ListOffsets(apb.ListOffsetsRequest(admin_id=aid, specs=[
        apb.OffsetSpecEntry(partition=_tp(), spec=apb.OffsetSpec(kind=apb.OffsetSpec.FOR_TIMESTAMP))]),
        None)
    assert resp.error.code == tr._LOCAL_ILLEGAL_ARGUMENT
    assert "carries no timestamp" in resp.error.message


# -- request translation ----------------------------------------------------------


def test_new_topics_keep_absent_distinct_from_given():
    absent, assigned = tr._admin_new_topics([
        apb.NewTopic(name="a", num_partitions=-1, replication_factor=-1),
        apb.NewTopic(name="b", replicas_assignments=[
            apb.ReplicaAssignment(partition=0, broker_ids=[1, 2])]),
    ])
    assert (absent.name(), absent.num_partitions(), absent.replication_factor()) == ("a", -1, -1)
    assert assigned.replicas_assignments() == {0: [1, 2]}


def test_acl_binding_and_filter_round_trip_through_the_proto():
    proto = apb.AclBinding(resource_type=2, resource_name="t", pattern_type=3,
                           principal="User:a", host="*", operation=3, permission_type=3)
    (binding,) = tr._admin_acl_bindings([proto])
    assert tr._admin_acl_binding_to_proto(binding) == proto

    # Absent nullable strings stay absent (Java's match-any), "" stays "".
    f = apb.AclBindingFilter(resource_type=1, pattern_type=1, operation=1, permission_type=1,
                             principal="")
    out = tr._admin_acl_filter_to_proto(tr._admin_acl_filter(f))
    assert not out.HasField("resource_name")
    assert out.HasField("principal") and out.principal == ""
    assert not out.HasField("host")


def test_client_quota_entity_keeps_default_entity_absent():
    proto = apb.ClientQuotaEntity(entries=[
        apb.ClientQuotaEntityEntry(entity_type="user"),
        apb.ClientQuotaEntityEntry(entity_type="client-id", entity_name="c")])
    entity = tr._admin_quota_entity(proto)
    assert entity.entries() == {"user": None, "client-id": "c"}
    out = tr._admin_quota_entity_to_proto(entity)
    assert [e.HasField("entity_name") for e in out.entries] == [False, True]


def test_offset_specs_are_keyed_by_topic_partition():
    specs = tr._admin_offset_specs([apb.OffsetSpecEntry(
        partition=_tp("t", 3), spec=apb.OffsetSpec(kind=apb.OffsetSpec.EARLIEST))])
    assert list(specs) == [TopicPartition(topic="t", partition=3)]
    assert isinstance(specs[TopicPartition(topic="t", partition=3)], ka.OffsetSpec.EarliestSpec)


def test_unknown_enum_code_is_unknown_or_illegal_argument():
    # ACL enums have an UNKNOWN member (Java's fromCode); ElectionType has none.
    (binding,) = tr._admin_acl_bindings([apb.AclBinding(
        resource_type=99, resource_name="t", pattern_type=99, principal="p", host="h",
        operation=99, permission_type=99)])
    assert binding.is_unknown()
    with pytest.raises(tr.AdminRequestError):
        tr._enum(tr.ElectionType, 7)


# -- error translation ---------------------------------------------------------------


def test_typed_error_crosses_as_its_ffi_id():
    err = tr._kafka_error_to_proto(TopicExistsError(message="x"))
    assert (err.code, err.message) == (TopicExistsError._ffi_id, "x")


def test_jdk_analog_error_keeps_its_own_code():
    from confluent_kafka import IllegalStateError

    err = tr._kafka_error_to_proto(IllegalStateError(message="s"))
    assert err.code == IllegalStateError._ffi_id


# -- the facade's two drivers -----------------------------------------------------------


class _FakeAsyncAdmin:
    """Stands in for ``AsyncAdmin``: its calls return a ``*Result`` at once, its
    futures are ``AsyncKafkaFuture``\\ s."""

    class _Result:
        def __init__(self, futures):
            self._futures = futures

        def names_to_listings(self):
            return self._futures["listings"]

        def values(self):
            return self._futures["values"]

    def list_topics(self, *, options):
        listing = ka.TopicListing(name="t", topic_id=tr.Uuid.random_uuid(), internal=False)
        return self._Result({"listings": _done({"t": listing}, asynchronous=True)})

    def delete_topics(self, *, topics, options):
        result = self._Result({})
        result.topic_name_values = lambda: {
            "ok": _done(None, asynchronous=True),
            "bad": _done(error=TopicExistsError(message="boom"), asynchronous=True)}
        return result

    async def close(self, *, timeout=None):
        self.closed = timeout


def test_async_facade_resolves_awaited_calls_and_per_key_errors():
    async def run():
        facade = tr.AsyncAdminFacade(_FakeAsyncAdmin())
        listings = await facade.list_topics()
        outcomes = await facade.delete_topics(["ok", "bad"])
        await facade.close(None)
        return listings, outcomes

    listings, outcomes = asyncio.run(run())
    assert list(listings) == ["t"]
    assert outcomes["ok"] is None
    assert isinstance(outcomes["bad"], TopicExistsError)


def test_async_service_answers_with_the_per_key_envelope():
    async def run():
        svc = grpc_server_async.AdminService()
        svc._admins[1] = tr.AsyncAdminFacade(_FakeAsyncAdmin())
        return await svc.DeleteTopics(apb.DeleteTopicsRequest(
            admin_id=1, names=cpb.StringList(values=["ok", "bad"])), None)

    resp = asyncio.run(run())
    by_key = {e.key.name: e for e in resp.entries}
    assert not by_key["ok"].HasField("error")
    assert by_key["bad"].error.code == TopicExistsError._ffi_id


def test_sync_driver_passes_thrown_errors_into_the_rpc():
    def rpc():
        try:
            yield _done(error=TopicExistsError(message="e"))
        except TopicExistsError as e:
            return f"caught {e}"

    # get() raises ExecutionError; the RPC sees its cause.
    assert tr._drive_sync(rpc()) == "caught e"


def test_async_driver_passes_the_cause_into_the_rpc():
    def rpc():
        try:
            yield _done(error=TopicExistsError(message="e"), asynchronous=True)
        except TopicExistsError as e:
            return f"caught {e}"

    assert asyncio.run(tr._drive_async(rpc())) == "caught e"


def test_a_failed_get_is_an_execution_error_the_driver_unwraps():
    future = _done(error=TopicExistsError(message="e"))
    with pytest.raises(ExecutionError) as info:
        future.get()
    assert isinstance(tr._unwrapped(info.value), TopicExistsError)


def test_mock_brokers_are_numbered_from_zero():
    brokers = tr._admin_mock_brokers(3)
    assert [n.id() for n in brokers] == [0, 1, 2]
    assert all(isinstance(n, Node) for n in brokers)


def test_async_service_over_the_async_mock():
    async def run():
        svc = grpc_server_async.AdminService()
        aid = (await svc.CreateAdmin(apb.CreateAdminRequest(num_brokers=2), None)).admin_id
        created = await svc.CreateTopics(apb.CreateTopicsRequest(admin_id=aid, topics=[
            apb.NewTopic(name="t1", num_partitions=2, replication_factor=1)]), None)
        again = await svc.CreateTopics(apb.CreateTopicsRequest(admin_id=aid, topics=[
            apb.NewTopic(name="t1", num_partitions=2, replication_factor=1)]), None)
        cluster = await svc.DescribeCluster(apb.DescribeClusterRequest(admin_id=aid), None)
        closed = await svc.Close(apb.AdminCloseRequest(admin_id=aid), None)
        return created, again, cluster, closed

    created, again, cluster, closed = asyncio.run(run())
    assert created.entries[0].value.metadata.num_partitions == 2
    assert again.entries[0].error.code == TopicExistsError._ffi_id
    assert [n.id for n in cluster.description.nodes] == [0, 1]
    assert not closed.HasField("error")


def test_per_partition_failure_stays_per_key():
    """alter/delete group offsets and remove-members answer per key: one failing
    key is that key's error, not a whole-call error."""
    from confluent_kafka.common.errors import GroupSubscribedToTopicError

    good, bad = TopicPartition(topic="t", partition=0), TopicPartition(topic="t", partition=1)

    def done(error=None):
        return _done(None, error)

    class _Res:
        def partition_result(self, *, partition):
            return done(GroupSubscribedToTopicError(message="x") if partition == bad else None)

        def member_result(self, *, member):
            return done(GroupSubscribedToTopicError(message="m") if member.group_instance_id() == "b" else None)

        def all(self):
            return done()

    class _Admin:
        def alter_consumer_group_offsets(self, **kw):
            return _Res()

        def delete_consumer_group_offsets(self, **kw):
            return _Res()

        def remove_members_from_consumer_group(self, **kw):
            return _Res()

    facade = tr.SyncAdminFacade(_Admin())
    for out in (facade.alter_consumer_group_offsets("g", {good: None, bad: None}),
                facade.delete_consumer_group_offsets("g", [good, bad])):
        assert out[good] is None
        assert isinstance(out[bad], GroupSubscribedToTopicError)
    members = [ka.MemberToRemove(group_instance_id="a"), ka.MemberToRemove(group_instance_id="b")]
    out = facade.remove_members_from_consumer_group("g", members)
    assert out["a"] is None and isinstance(out["b"], GroupSubscribedToTopicError)
    assert facade.delete_consumer_group_offsets("g", []) == {}


# ---- the three verify-run failures (V-fix1) ---------------------------------


def test_delete_acls_answers_each_matched_acl():
    """DeleteAcls over the Java result shape: a filter's future holds
    ``FilterResults``, each matched ACL a ``FilterResult`` with its binding or its
    own error. The facade answers the matched ACLs, not the future."""
    proto = apb.AclBinding(resource_type=2, resource_name="orders", pattern_type=3,
                           principal="User:a", host="*", operation=3, permission_type=3)
    (binding,) = tr._admin_acl_bindings([proto])
    results = DeleteAclsResult.FilterResults._new(values=[
        DeleteAclsResult.FilterResult._new(binding=binding, exception=None)])

    class _Res:
        def __init__(self, filters):
            self._filters = filters

        def values(self):
            return {f: _done(results) for f in self._filters}

    class _Admin:
        def delete_acls(self, *, filters, options):
            return _Res(list(filters))

    svc = grpc_server.AdminService()
    svc._admins[1] = tr.SyncAdminFacade(_Admin())
    resp = svc.DeleteAcls(apb.DeleteAclsRequest(admin_id=1, filters=[apb.AclBindingFilter(
        resource_type=1, pattern_type=1, operation=1, permission_type=1)]), None)
    assert not resp.HasField("error"), resp.error
    (entry,) = resp.entries
    (deleted,) = entry.value.values
    assert not deleted.HasField("exception")
    assert deleted.binding == proto


def test_list_config_resources_passes_types_not_codes():
    """ListConfigResources hands the client ``ConfigResource.Type`` members; the
    raw wire ints would fail on ``id()``."""
    from confluent_kafka.common.config import ConfigResource

    seen = []

    class _Res:
        def all(self):
            return _done([ConfigResource(type=ConfigResource.Type.TOPIC, name="t1")])

    class _Admin:
        def list_config_resources(self, *, config_resource_types, options):
            seen.append([t.id() for t in config_resource_types])
            return _Res()

    svc = grpc_server.AdminService()
    svc._admins[1] = tr.SyncAdminFacade(_Admin())
    resp = svc.ListConfigResources(apb.ListConfigResourcesRequest(
        admin_id=1, resource_types=[2, 4, 77]), None)
    assert not resp.HasField("error"), resp.error
    assert [(r.resource_type, r.name) for r in resp.resources] == [(2, "t1")]
    # An unknown code is UNKNOWN (Java's Type.forId), sent as its id 0.
    assert seen == [[2, 4, 0]]
    assert tr._admin_config_resource_types([2, 77]) == [
        ConfigResource.Type.TOPIC, ConfigResource.Type.UNKNOWN]


def _token_round_trip(create, describe):
    principal = apb.KafkaPrincipal(principal_type="User", name="alice")
    # Java's mock makes the first renewer the owner (`renewers.get(0)`).
    created = create(apb.CreateDelegationTokenRequest(renewers=[principal], max_lifetime_ms=-1))
    assert not created.HasField("error"), created.error
    unset = describe(apb.DescribeDelegationTokenRequest())
    empty = describe(apb.DescribeDelegationTokenRequest(owners=apb.KafkaPrincipalList()))
    other = describe(apb.DescribeDelegationTokenRequest(owners=apb.KafkaPrincipalList(
        principals=[apb.KafkaPrincipal(principal_type="User", name="bob")])))
    return created.token.token_information.token_id, unset, empty, other


def _assert_token_round_trip(token_id, unset, empty, other):
    # An absent owners filter is "every token" (admin_service.proto), as an empty
    # one is on the mock; another owner's filter excludes the token.
    for resp in (unset, empty):
        assert not resp.HasField("error"), resp.error
        assert [t.token_information.token_id for t in resp.tokens] == [token_id]
    assert not other.HasField("error") and list(other.tokens) == []


def test_mock_describe_delegation_token_without_owners_describes_every_token():
    svc = grpc_server.AdminService()
    aid = svc.CreateAdmin(apb.CreateAdminRequest(num_brokers=1), None).admin_id

    def create(req):
        req.admin_id = aid
        return svc.CreateDelegationToken(req, None)

    def describe(req):
        req.admin_id = aid
        return svc.DescribeDelegationToken(req, None)

    _assert_token_round_trip(*_token_round_trip(create, describe))


def test_async_mock_describe_delegation_token_without_owners_describes_every_token():
    async def run():
        svc = grpc_server_async.AdminService()
        aid = (await svc.CreateAdmin(apb.CreateAdminRequest(num_brokers=1), None)).admin_id
        loop = asyncio.get_running_loop()

        def call(method):
            def invoke(req):
                req.admin_id = aid
                return asyncio.run_coroutine_threadsafe(method(req, None), loop).result()
            return invoke

        return await loop.run_in_executor(None, lambda: _token_round_trip(
            call(svc.CreateDelegationToken), call(svc.DescribeDelegationToken)))

    _assert_token_round_trip(*asyncio.run(run()))


def test_real_client_describe_delegation_token_keeps_the_unset_filter():
    """Only the mock path reshapes an absent filter: the real client's null
    ``owners()`` is already "every token", so it crosses as None."""
    seen = []

    class _Admin:
        def describe_delegation_token(self, *, options):
            seen.append(options.owners())
            class _Res:
                def delegation_tokens(self):
                    return _done([])
            return _Res()

    assert tr.SyncAdminFacade(_Admin()).describe_delegation_token(owners=None) == []
    assert tr.SyncAdminFacade(_Admin(), mock=True).describe_delegation_token(owners=None) == []
    assert seen == [None, []]


# -- unregisterController and describeUserScramCredentials ------------------------


def test_unregister_controller_over_the_mock_is_a_whole_call_status():
    """Java's `MockAdminClient.unregisterController` fails with
    `UnsupportedVersionException` unless the mock uses a raft controller."""
    svc = grpc_server.AdminService()
    aid = svc.CreateAdmin(apb.CreateAdminRequest(num_brokers=1), None).admin_id
    resp = svc.UnregisterController(apb.UnregisterControllerRequest(
        admin_id=aid, controller_id=3), None)
    assert resp.error.code == UnsupportedVersionError._ffi_id


def test_unregister_controller_succeeds_and_fails_through_all():
    from confluent_kafka.common.errors import ControllerIdNotRegisteredError

    class _Res:
        def __init__(self, error):
            self._error = error

        def all(self):
            return _done(None, self._error)

    class _Admin:
        error = None

        def unregister_controller(self, *, controller_id, options):
            return _Res(self.error)

    admin = _Admin()
    svc = grpc_server.AdminService()
    svc._admins[1] = tr.SyncAdminFacade(admin)
    request = apb.UnregisterControllerRequest(admin_id=1, controller_id=7)
    assert not svc.UnregisterController(request, None).HasField("error")
    admin.error = ControllerIdNotRegisteredError(message="not registered")
    resp = svc.UnregisterController(request, None)
    assert resp.error.code == ControllerIdNotRegisteredError._ffi_id


def _scram_admin(all_error, descriptions):
    """A client whose `all()` fails with `all_error` (or succeeds with every
    description) and whose `users()` lists the non-RESOURCE_NOT_FOUND users."""

    class _Res:
        def all(self):
            if all_error is None:
                return _done({d.name(): d for d in descriptions.values()
                              if not isinstance(d, BaseException)})
            return _done(error=all_error)

        def users(self):
            return _done([u for u, d in descriptions.items()
                          if not isinstance(d, ResourceNotFoundError)])

        def description(self, *, user_name):
            d = descriptions[user_name]
            return _done(error=d) if isinstance(d, BaseException) else _done(d)

    class _Admin:
        def describe_user_scram_credentials(self, *, options, users=None):
            return _Res()

    return _Admin()


def test_scram_failed_all_leaves_a_requested_resource_not_found_user_out():
    from confluent_kafka.common.errors import UnknownServerError

    good = ka.UserScramCredentialsDescription(name="a", credential_infos=[])
    bad = UnknownServerError(message="boom")
    missing = ResourceNotFoundError(message="no creds")
    admin = _scram_admin(bad, {"a": good, "b": bad, "c": missing})
    out = tr.SyncAdminFacade(admin).describe_user_scram_credentials(users=["a", "b", "c"])
    assert out == {"a": good, "b": bad}


def test_scram_successful_all_answers_every_user():
    good = ka.UserScramCredentialsDescription(name="a", credential_infos=[])
    out = tr.SyncAdminFacade(_scram_admin(None, {"a": good})).describe_user_scram_credentials(
        users=["a"])
    assert out == {"a": good}


def test_scram_empty_composition_raises_the_all_error():
    from confluent_kafka.common.errors import UnknownServerError

    all_error = UnknownServerError(message="whole")
    admin = _scram_admin(all_error, {"c": ResourceNotFoundError(message="x")})
    with pytest.raises(UnknownServerError):
        tr.SyncAdminFacade(admin).describe_user_scram_credentials(users=["c"])


# -- the producer servers read a send() future with get() -------------------------


def _send_request(producer_id, topic="t"):
    pb = pytest.importorskip("producer_service_pb2")
    return pb.SendRequest(producer_id=producer_id, record=pb.ProducerRecord(
        topic=topic, value=b"v"))


def test_sync_server_send_reads_the_future_with_get():
    pb = pytest.importorskip("producer_service_pb2")
    svc = grpc_server.ProducerService(None)
    pid = svc.CreateProducer(pb.CreateProducerRequest(), None).producer_id
    resp = svc.Send(_send_request(pid), None)
    assert not resp.HasField("error"), resp.error
    assert resp.metadata.topic == "t"


def test_sync_server_send_failure_crosses_as_the_cause():
    pb = pytest.importorskip("producer_service_pb2")
    svc = grpc_server.ProducerService(None)
    svc._producers[1] = SimpleNamespace(
        send=lambda **kw: _done(error=TopicExistsError(message="no")))
    resp = svc.Send(_send_request(1), None)
    assert resp.error.code == TopicExistsError._ffi_id
    assert resp.error.message == "no"


def test_async_server_send_awaits_the_future_with_get():
    pb = pytest.importorskip("producer_service_pb2")

    async def run():
        svc = grpc_server_async.ProducerService(None)
        pid = (await svc.CreateProducer(pb.CreateProducerRequest(), None)).producer_id
        return await svc.Send(_send_request(pid), None)

    resp = asyncio.run(run())
    assert not resp.HasField("error"), resp.error
    assert resp.metadata.topic == "t"
