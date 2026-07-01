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

"""Shared pytest fixtures for the Python perf tests.

Provides a single-node KRaft Kafka broker via testcontainers (apache/kafka,
matching the Rust integration harness image so KIP-848 `group.protocol=consumer`
works), with two PLAINTEXT listeners:

  * EXTERNAL  — advertised to the host on a fixed mapped port (the perf scripts,
    run on the host, connect here).
  * INTERNAL  — advertised as localhost:9094 for clients running *inside* the
    container (the consumer test's `kafka-producer-perf-test.sh` load).

Both producer and consumer perf tests reuse `kafka_broker`. Everything skips
cleanly when Docker / testcontainers is unavailable.
"""

import socket
import time

import pytest

KAFKA_IMAGE = "apache/kafka:4.2.0"  # matches tests/common/kafka_cluster.rs
CLUSTER_ID = "5L6g3nShT-eMCtK--X86sw"
INTERNAL_BOOTSTRAP = "localhost:9094"
KAFKA_BIN = "/opt/kafka/bin"


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class _Broker:
    def __init__(self, container, external_bootstrap):
        self.container = container
        self.external_bootstrap = external_bootstrap
        self.internal_bootstrap = INTERNAL_BOOTSTRAP

    def exec(self, cmd, detach=False):
        """Run a command inside the broker container (docker exec)."""
        wrapped = self.container.get_wrapped_container()
        return wrapped.exec_run(cmd, detach=detach)


@pytest.fixture(scope="session")
def kafka_broker():
    """Single-node KRaft Kafka broker. Skips if Docker/testcontainers are
    unavailable."""
    testcontainers = pytest.importorskip("testcontainers.core.container")
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    host_port = _free_port()
    container = (
        DockerContainer(KAFKA_IMAGE)
        .with_bind_ports(9092, host_port)
        .with_env("CLUSTER_ID", CLUSTER_ID)
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
        .with_env(
            "KAFKA_LISTENERS",
            "EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093",
        )
        .with_env(
            "KAFKA_ADVERTISED_LISTENERS",
            f"EXTERNAL://127.0.0.1:{host_port},INTERNAL://localhost:9094",
        )
        .with_env(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        )
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        # KIP-848 (new consumer group protocol) — required by the Rust binding.
        .with_env("KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer")
    )

    try:
        container.start()
    except Exception as e:  # docker not running / image pull failure
        pytest.skip(f"could not start Kafka testcontainer: {e}")

    try:
        wait_for_logs(container, r"Kafka Server started", timeout=120)
    except Exception as e:
        container.stop()
        pytest.skip(f"Kafka broker did not become ready: {e}")

    broker = _Broker(container, f"127.0.0.1:{host_port}")
    # Give the coordinator a moment to settle before tests subscribe.
    time.sleep(2)
    yield broker
    container.stop()


def create_topic(broker, topic, partitions=1):
    """Create `topic` via kafka-topics.sh inside the container."""
    rc, out = broker.exec([
        f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", broker.internal_bootstrap,
        "--create", "--if-not-exists", "--topic", topic,
        "--partitions", str(partitions), "--replication-factor", "1",
    ])
    if rc not in (0, None):
        raise RuntimeError(f"create_topic failed (rc={rc}): {out!r}")


class _InContainerProducer:
    def __init__(self, broker):
        self._broker = broker

    def stop(self):
        # Best-effort: the perf producer is bounded (num_records) and exits on
        # its own; the container teardown also reaps it. Nothing to join since
        # it was started detached.
        pass


def produce_perf_in_container(broker, topic, num_records, record_size, throughput):
    """Start kafka-producer-perf-test.sh inside the broker container (detached),
    producing CreateTime-timestamped records against the INTERNAL listener."""
    broker.exec([
        f"{KAFKA_BIN}/kafka-producer-perf-test.sh",
        "--topic", topic,
        "--num-records", str(num_records),
        "--record-size", str(record_size),
        "--throughput", str(throughput),
        "--producer-props", f"bootstrap.servers={broker.internal_bootstrap}", "acks=1",
    ], detach=True)
    return _InContainerProducer(broker)
