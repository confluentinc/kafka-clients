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
# Generates a message every 1/rate seconds until interrupted.
#
# Adapted from confluent-kafka-python's examples/auto_producer.py for this
# client's bindings. See examples/producer.py for the general API
# differences (send() takes on_delivery=callback(metadata, exception) —
# Java's Callback — fired on the completion thread the moment the record
# completes, not deferred until poll()/flush()).
#
# Two additional adaptations here:
#   * 'topic.metadata.refresh.interval.ms' is a librdkafka-specific config
#     key with no effect on this client; the equivalent Java/Rust client key
#     is 'metadata.max.age.ms'. Config values must be strings.
#   * RecordMetadata does not carry the delivered payload back the way
#     confluent-kafka-python's Message does, so the value is captured by
#     closure for logging.
#

import signal
import sys
import time

from producer import KafkaProducer, ProducerRecord

if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.stderr.write(
            'Usage: %s <bootstrap-brokers> <topic> <rate>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    rate = int(sys.argv[3]) if len(sys.argv) > 3 else 1

    # Producer configuration. See bindings/python/README.md for the config
    # keys this client currently accepts. Config values must be strings.
    conf = {
        'bootstrap.servers': broker,
        'metadata.max.age.ms': '10000',
    }

    # Delivery callback factory. on_delivery is callback(metadata, exception)
    # — exactly one is meaningful — and runs on the producer's completion
    # thread. `value` is captured by closure since RecordMetadata does not
    # carry the payload.
    def make_delivery_callback(value):
        def delivery_callback(metadata, exception):
            if exception is not None:
                sys.stderr.write(
                    '%% Message failed delivery: %s\n' % exception)
            else:
                sys.stderr.write(
                    '%% Message delivered to %s [%d] @ %d: %s\n'
                    % (metadata.topic(), metadata.partition(),
                       metadata.offset(), value))
        return delivery_callback

    run = True

    def signal_handler(sig, frame):
        global run
        run = False

    signal.signal(signal.SIGINT, signal_handler)

    # close() (via the `with` block) drains every outstanding send and runs
    # its on_delivery callback before returning, so no manual future tracking
    # is needed to wait for the final deliveries.
    with KafkaProducer(conf) as p:
        message = 0
        sleep_time = 1.0 / rate
        while run:
            value = ("message_" + str(message)).encode('utf-8')
            record = ProducerRecord(topic, value)
            p.send(record, on_delivery=make_delivery_callback(value))
            message += 1
            time.sleep(sleep_time)

        sys.stderr.write('%% Waiting for deliveries\n')
