import datetime
import os
import sys
import time
import random
import signal
import queue
import gc
from threading import Thread


from performance_common import Metrics
from concurrent.futures import CancelledError, Future
from producer import (KafkaProducer, ProducerRecord, RecordMetadata)
from confluent_kafka import Producer as CKProducer, Message as CKMessage


def message_generator(topic, key_size=100, value_size=1024,
                      n=100000, randomness=0.5, limit_rps=None):
    if limit_rps is not None and limit_rps <= 0:
        raise ValueError("limit_rps must be positive")

    ret = []
    rand_bytes = int(value_size * randomness)
    constant_bytes = random.randbytes(value_size - rand_bytes)

    key_constant_bytes = None
    key_rand_bytes = 0
    if key_size > 0:
        key_rand_bytes = int(key_size * randomness)
        key_constant_bytes = random.randbytes(key_size - key_rand_bytes)

    for _ in range(n):
        key = None
        if key_size > 0:
            key = key_constant_bytes + random.randbytes(key_rand_bytes)
        value = constant_bytes + random.randbytes(rand_bytes)
        ret.append((key, value))

    return ret


terminating = False
key_size = 0
value_size = 2048
verified = 0
message_size = key_size + value_size
topic_name = os.getenv("TOPIC_NAME", "test-topic")
limit_rps = os.getenv("LIMIT_RPS", None)
if 'KEY_SIZE' in os.environ:
    key_size = int(os.environ['KEY_SIZE'])
if 'VALUE_SIZE' in os.environ:
    value_size = int(os.environ['VALUE_SIZE'])
if limit_rps is not None:
    limit_rps = int(limit_rps)
v2 = os.getenv("CLIENT_VERSION", "3") == "2"
do_verify = os.getenv("DO_VERIFY", "True") == "True"
warmup_s = os.getenv("WARMUP_SECONDS", None)
if warmup_s is not None:
    warmup_s = int(warmup_s)
else:
    warmup_s = 0
test_duration_s = os.getenv("TEST_DURATION_SECONDS", None)
if test_duration_s is not None:
    test_duration_s = int(test_duration_s)
else:
    test_duration_s = 600

num_messages = 0
if 'NUM_MESSAGES' in os.environ:
    num_messages = int(os.environ['NUM_MESSAGES'])

version_str = "v2 (confluent-kafka)" if v2 else \
    "v3 (confluent-kafka-rust)"
generated_messages = message_generator(topic_name, key_size=key_size,
                        value_size=value_size, n=10000,
                        limit_rps=limit_rps)
if limit_rps is not None:
    num_messages = int(limit_rps * test_duration_s)  # Run for the specified duration
total_size = num_messages * message_size
total_size_mib = total_size / (1024 * 1024)
producer = None
metrics = Metrics()


class CompatibleProducer:
    def __init__(self, configuration):
        self._producer = CKProducer(configuration)
        self._closed = False

        def poll_producer():
            while not self._closed:
                self._producer.poll(1.0)
        self._thread = Thread(target=poll_producer)
        self._thread.start()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def send(self, record):
        fut = Future()

        def delivery_report(err, msg):
            if err is not None:
                fut.set_exception(Exception(err))
            else:
                fut.set_result(msg)
        while not terminating:
            try:
                self._producer.produce(
                    topic=record.topic,
                    key=record.key,
                    value=record.value,
                    callback=delivery_report
                )
                break
            except BufferError as e:
                time.sleep(0.001)
        return fut

    def close(self):
        self._closed = True
        self._thread.join()
        self._producer = None


def verify_record_metadata(r):
    global verified
    if not do_verify:
        verified += 1
        return

    if not isinstance(r, RecordMetadata):
        raise RuntimeError("Unexpected produce call result type")
    if (r.offset() >= 0 and r.partition() >= 0 and
            r.topic() == topic_name and r.timestamp() >= 0):
        verified += 1


def verify_message(m):
    global verified
    if not do_verify:
        verified += 1
        return

    if not isinstance(m, CKMessage):
        raise RuntimeError("Unexpected produce call result type")
    _, timestamp = m.timestamp()
    if (m.offset() >= 0 and m.partition() >= 0 and
            m.topic() == topic_name and timestamp > 0):
        verified += 1


verification_function = verify_message if v2 else verify_record_metadata


def sasl_config_from_env(v2=False):
    SECURITY_PROTOCOL = os.environ.get("SECURITY_PROTOCOL", None)
    SASL_MECHANISM = os.environ.get("SASL_MECHANISM", None)
    SASL_USERNAME = os.environ.get("SASL_USERNAME", None)
    SASL_PASSWORD = os.environ.get("SASL_PASSWORD", None)

    sasl_enabled = SECURITY_PROTOCOL in ("SASL_PLAINTEXT", "SASL_SSL") and \
                   all([SASL_MECHANISM, SASL_USERNAME, SASL_PASSWORD])
    if not sasl_enabled:
        return {}
    
    if not v2:
        sasl_jaas_config = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required \n\t"
            f"username=\"{SASL_USERNAME}\" \n\tpassword=\"{SASL_PASSWORD}\";")
        return {
            'security.protocol': SECURITY_PROTOCOL,
            'sasl.mechanism': SASL_MECHANISM,
            'sasl.jaas.config': sasl_jaas_config
        }
    else:
        return {
            'security.protocol': SECURITY_PROTOCOL,
            'sasl.mechanism': SASL_MECHANISM,
            'sasl.username': SASL_USERNAME,
            'sasl.password': SASL_PASSWORD
        }

def configuration_from_env(common_default_configuration, v2=False):
    batch_size = 1000000
    max_request_size = batch_size * 8
    conf = dict(common_default_configuration)
    conf.update(sasl_config_from_env(v2=v2))

    if 'BOOTSTRAP_SERVERS' in os.environ:
        conf['bootstrap.servers'] = os.environ['BOOTSTRAP_SERVERS']

    if 'BATCH_SIZE' in os.environ:
        conf['batch.size'] = int(os.environ['BATCH_SIZE']) * 1024  # Convert KB to bytes
    else:
        conf['batch.size'] = batch_size

    if 'MAX_REQUEST_SIZE' in os.environ:
        max_request_size = int(os.environ['MAX_REQUEST_SIZE']) * 1024  # Convert KB to bytes
    if not v2:
        conf['max.request.size'] = max_request_size
    else:
        conf['message.max.bytes'] = max_request_size

    if 'COMPRESSION_TYPE' in os.environ:
        conf['compression.type'] = os.environ['COMPRESSION_TYPE']
    else:
        conf['compression.type'] = 'none'

    if 'ENABLE_IDEMPOTENCE' in os.environ:
        conf['enable.idempotence'] = os.environ['ENABLE_IDEMPOTENCE']
    else:
        conf['enable.idempotence'] = 'false'

    if 'MAX_IN_FLIGHT' in os.environ:
        conf['max.in.flight.requests.per.connection'] = os.environ['MAX_IN_FLIGHT']

    if 'BUFFER_MEMORY' in os.environ:
        buffer_memory = int(os.environ['BUFFER_MEMORY']) * 1024 * 1024  # Convert MB to bytes
        if not v2:
            conf['buffer.memory'] = buffer_memory
        else:
            conf['queue.buffering.max.kbytes'] = buffer_memory // 1024  # Convert bytes to KB
            conf['queue.buffering.max.messages'] = 2147483647

    if 'LINGER_MS' in os.environ:
        conf['linger.ms'] = os.environ['LINGER_MS']
    return conf


def print_configuration(conf):
    print(f"Key size: {key_size} bytes")
    print(f"Value size: {value_size} bytes")
    print(f"Verify: {do_verify}")
    print("Producer configuration:")
    for key, value in conf.items():
        if key in ['sasl.jaas.config', 'sasl.password']:
            print(f"  {key}: <hidden>")
        else:
            print(f"  {key}: {value}")

def v3_producer(common_default_configuration):
    conf = configuration_from_env(common_default_configuration, v2=False)
    print_configuration(conf)
    conf = {k: str(v) for k, v in conf.items()}
    return KafkaProducer(conf)

def v2_producer(common_default_configuration):
    conf = configuration_from_env(common_default_configuration, v2=True)
    print_configuration(conf)
    return CompatibleProducer(conf)

def main(v2=False):
    global producer, verified
    total_latency_ms = 0
    max_latency_ms = 0
    completed_messages = 0
    before_ms = None
    first_message_time = None
    record_completed_calls_loop = None

    common_default_configuration = {
        "bootstrap.servers": "localhost:9092",
    }

    if not v2:
        producer = v3_producer(common_default_configuration)
    else:
        producer = v2_producer(common_default_configuration)

    def record_completed_calls(produce_call, start_time):
        nonlocal max_latency_ms, total_latency_ms, completed_messages
        try:
            r = produce_call.result()
            verification_function(r)
        except Exception as e:
            print(f"Produce call resulted in exception: {e}")
        except CancelledError:
            pass
        completed_messages += 1
        if completed_messages % 10000 == 0:
            print(f"Completed messages: {completed_messages}. Rate so far: {completed_messages / ((time.time_ns() - first_message_time) / 1e9):.2f} msg/s", end='\r')  # noqa: E501
        current_latency = int(time.time() * 1000) - start_time
        metrics.latency.add_measurement(current_latency)
        metrics.messages.add_measurement(1)
        metrics.bytes.add_measurement(message_size)
        max_latency_ms = max(max_latency_ms, current_latency)
        total_latency_ms += current_latency

    def start_recording_completed_calls(produce_calls_queue):
        nonlocal record_completed_calls_loop

        def record_completed_calls_worker():
            while record_completed_calls_loop and (num_messages == 0 or completed_messages < num_messages):
                try:
                    produce_call, start_time = produce_calls_queue.get(
                        timeout=1)
                except queue.Empty:
                    continue
                record_completed_calls(produce_call, start_time)

            while True:
                try:
                    produce_call, start_time = produce_calls_queue.get_nowait()
                    record_completed_calls(produce_call, start_time)
                except queue.Empty:
                    break

        record_completed_calls_loop = Thread(
            target=record_completed_calls_worker)
        record_completed_calls_loop.start()

    try:
        with producer:
            generated_messages_len = len(generated_messages)           
            if warmup_s > 0:
                print(f"Warming up for {warmup_s} seconds ...")
                warmup_end_time = time.time_ns() + warmup_s * 1000000000
                i = 0
                while time.time_ns() < warmup_end_time:
                    message = generated_messages[i % generated_messages_len]
                    try:
                        produce_call = producer.send(ProducerRecord(
                            topic=topic_name,
                            key=message[0],
                            value=message[1]
                        ))
                        r = produce_call.result()
                        verification_function(r)
                    except Exception as e:
                        print("Warmup failed due to message verification error")
                        producer = None
                        return
                    time.sleep(0.1)
                    i += 1

            verified = 0

            #max 2BG of messages in the queue
            produce_calls = queue.Queue(maxsize=(1024**3 * 2 // message_size))
            start_recording_completed_calls(produce_calls)
            before_ms = int(time.time() * 1000)
            first_message_time = time.time_ns()
            metrics.measurement_start_ms = before_ms
            print(f"Starting measured interval at {before_ms} ms: {datetime.datetime.now(tz=datetime.timezone.utc)}")  # noqa: E501
            messages_sent = 0
            if num_messages > 0:
                continue_sending = messages_sent < num_messages
            else:
                continue_sending = not terminating

            while continue_sending:
                try:
                    key, value = generated_messages[messages_sent % generated_messages_len]
                    next_message = ProducerRecord(
                        topic=topic_name,
                        key=key,
                        value=value)
                    start_time = int(time.time() * 1000)
                    produce_call = producer.send(next_message)
                    produce_calls.put((produce_call, start_time))
                    messages_sent += 1
                    if messages_sent % 10000 == 0:
                        duration = time.time_ns() - first_message_time
                        exceeded_seconds = num_messages > 0 and 10 or 1
                        if duration > (test_duration_s + exceeded_seconds) * 1e9:
                            print(f"Test duration reached, {duration / 1e9:.2f} seconds. Interrupting...\n")  # noqa: E501
                            break
                except RuntimeError:
                    pass
            
                continue_sending = not terminating 
                if num_messages > 0:
                    continue_sending = continue_sending and messages_sent < num_messages

            t, record_completed_calls_loop = record_completed_calls_loop, None
            t.join()
            after_ms = int(time.time() * 1000)
            after_ns = time.time_ns()

            if verified != completed_messages:
                if not terminating:
                    print(f"Verified messages {verified} "
                          "does not match completed messages "
                          f"{completed_messages}")
            elif num_messages > 0 and completed_messages != num_messages:
                if not terminating:
                    print(f"Completed messages {completed_messages} "
                          f"does not match produced messages {num_messages}")
            else:
                metrics.measurement_end_ms = after_ms
                total_time_ns = after_ns - first_message_time
                total_time_ms = total_time_ns / 1_000_000
                total_time_s = total_time_ms / 1_000
                message_rate = completed_messages / total_time_s if total_time_s > 0 else 0
                external_metrics_aggregations = metrics.external_metrics_aggregations()
                print(f"End time: {after_ms} ms")
                print(f"Duration: {total_time_ms} ms")
                if external_metrics_aggregations["total_external_metrics"] > 0:
                    average_cpu = external_metrics_aggregations['average_cpu']
                    average_rss = external_metrics_aggregations['average_rss'] / 1024
                    print(
                        "Average CPU: "
                        f"{average_cpu:.2f} %")
                    print(
                        "Average RSS: "
                        f"{average_rss :.2f}"
                        " KiB")
                    print(f"CPU Efficiency: {message_rate / (average_cpu if average_cpu > 0 else 1):.2f} msg/(s * 1% CPU)")  # noqa: E501
                    print(f"Memory Efficiency: {message_rate / (average_rss if average_rss > 0 else 1):.2f} msg/(s * KB RSS)")  # noqa: E501
                else:
                    print("No external metrics collected")

                print(
                    "Average time: "
                    f"{total_time_ms / completed_messages:.2f} ms")
                print(
                    "Average rate msg/s: "
                    f"{message_rate:.2f} msg/s")
                print(
                    "Average rate MiB/s: "
                    f"{(completed_messages * message_size) / (1024.0 * 1024.0) / total_time_s:.2f} MiB/s")
                print(
                    "Average latency: "
                    f"{total_latency_ms / completed_messages:.2f} ms")
                print("Max latency: "
                      f"{max_latency_ms:.2f} ms")
    except CancelledError:
        t, record_completed_calls_loop = record_completed_calls_loop, None
        t.join()
        print("Main cancelled")

    producer = None


def signal_handler(sig, frame):
    global terminating
    # Calling print inside a signal handler can lead to
    # a "reentrant call RuntimeError"
    os.write(sys.stdout.fileno(),
             b"Termination signal received, shutting down...\n")
    terminating = True

    try:
        if producer:
            producer.close()
    except Exception as e:
        os.write(sys.stdout.fileno(),
                 f"Exception during close: {e}".encode())


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if not limit_rps:
        if num_messages > 0:
            print(f"Producing {num_messages} messages at max rate")
        else:
            print(f"Producing messages at max rate for {test_duration_s} seconds")
    else:
            print(f"Producing {num_messages} messages at "
                  f"{str(limit_rps)} msg/s")  # noqa: E501

    metrics.start_collecting(interval_s=1)
    print(f"Running sync producer performance test {version_str}...")
    main(v2=v2)

    if not terminating:
        print("Performing garbage collection...")
        gc.collect()
    print("Waiting for final metrics collection...")
    # Wait some time to collect final metrics
    #time.sleep(10)
    last_metrics = metrics.external_metrics_last_values()
    print(f"Final CPU: {last_metrics['last_cpu']:.2f} %")
    print(f"Final RSS: {last_metrics['last_rss'] / 1024 :.2f} KiB")
    metrics.stop_collecting()
    print("Done")
