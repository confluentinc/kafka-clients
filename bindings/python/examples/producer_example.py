#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#
# Adapted from confluent-kafka-python's examples/producer.py for this
# client's bindings. The API differs from confluent-kafka-python in ways
# that matter here:
#   * send() takes an on_delivery=callback(metadata, exception) — Java's
#     Callback — instead of confluent-kafka-python's callback(err, msg).
#     Exactly one argument is meaningful: `metadata` on success, `exception`
#     on failure. The callback fires exactly once per record.
#   * on_delivery runs on the producer's completion thread the moment the
#     record completes — NOT deferred until poll()/flush() as in
#     confluent-kafka-python. There is no poll() to "serve" callbacks; keep
#     the callback short and non-blocking.
#   * send() also returns a concurrent.futures.Future[RecordMetadata], and
#     never raises BufferError — it blocks internally until buffer space
#     frees up (mirrors Java's buffer.memory backpressure).
#

import sys

from producer import KafkaProducer, ProducerRecord

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write(
            'Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Producer configuration. See bindings/python/README.md for the config
    # keys this client currently accepts. Config values must be strings.
    conf = {'bootstrap.servers': broker}

    # Delivery callback, invoked exactly once when a message has been
    # successfully delivered or permanently failed delivery (after retries).
    # Runs on the producer's background completion thread.
    def delivery_callback(metadata, exception):
        if exception is not None:
            sys.stderr.write('%% Message failed delivery: %s\n' % exception)
        else:
            sys.stderr.write(
                '%% Message delivered to %s [%d] @ %d\n'
                % (metadata.topic(), metadata.partition(), metadata.offset()))

    # Create Producer instance. The `with` block closes it on exit, and
    # close() drains every outstanding send and runs its on_delivery callback
    # before returning — so all delivery reports are guaranteed by the time
    # the block exits. (flush() alone does NOT make that guarantee: on this
    # client the delivery callbacks are dispatched by a separate background
    # thread that flush() does not wait on.)
    with KafkaProducer(conf) as p:
        # Read lines from stdin, send each line to Kafka
        for line in sys.stdin:
            record = ProducerRecord(topic, line.rstrip().encode('utf-8'))
            p.send(record, on_delivery=delivery_callback)

        sys.stderr.write('%% Waiting for deliveries\n')
