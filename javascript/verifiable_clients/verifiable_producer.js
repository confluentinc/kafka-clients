#!/usr/bin/env node
// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

// Translated from org.apache.kafka.tools.VerifiableProducer, over the
// @confluentinc/kafka-javascript producer (the twin of
// python/verifiable_clients/verifiable_producer.py).
//
// Primarily intended for use with system testing, this producer prints
// metadata in the form of JSON to stdout on each "send" request. For example,
// this helps with end-to-end correctness tests by making externally visible
// which messages have been acked and which have not.
//
// When used as a command-line tool, it produces increasing integers. It will
// produce a fixed number of messages unless the default max-messages -1 is
// used, in which case it produces indefinitely.
//
// If logging is left enabled, log output on stdout can be easily ignored by
// checking whether a given line is valid JSON.
//
// The JSON printed on stdout is Java's: the same event names, field names and
// field order, since that is what the ducktape VerifiableProducer service
// parses. Run as `node verifiable_producer.js --topic t --bootstrap-server
// host:9092`.
//
// Java's send() returns once the record is handed over (after the metadata
// wait, or the wait for buffer space); here that is the settling of the
// Promise `send()` returns, so the loop awaits it, and a rejection is what
// Java's send() would throw. The keys and values are strings, which the client
// encodes to UTF-8 as Java's StringSerializer does.

const {
  ArgumentParser,
  ArgumentParserError,
  HelpRequested,
  ShutdownHook,
  className,
  errorMessage,
  event,
  loadProps,
  nowMillis,
  printJson,
  printLine,
} = require('./_common');
const { ThroughputThrottler } = require('./throughput_throttler');

const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');

// ---------------------------------------------------------------------------
// The events, Java's nested data classes, in Jackson's field order.
// ---------------------------------------------------------------------------

function startupComplete() {
  return event('startup_complete');
}

function shutdownComplete() {
  return event('shutdown_complete');
}

function successfulSend(key, value, recordMetadata) {
  return event('producer_send_success', {
    key,
    value,
    topic: recordMetadata.topic,
    partition: recordMetadata.partition,
    offset: recordMetadata.offset,
  });
}

function failedSend(key, value, topic, error) {
  return event('producer_send_error', {
    key,
    value,
    topic,
    exception: className(error),
    message: errorMessage(error),
  });
}

function toolData(sent, acked, targetThroughput, avgThroughput) {
  return event('tool_data', {
    sent,
    acked,
    target_throughput: targetThroughput,
    avg_throughput: avgThroughput,
  });
}

/** The producing loop, its counters and the send callback. */
class VerifiableProducer {
  /**
   * @param {object} options
   * @param {{send: Function, close: Function}} options.producer - the
   *   producer, a `KafkaProducer` (or a stand-in with its `send` and `close`)
   * @param {string} options.topic
   * @param {number} options.throughput - messages/sec to throttle to, if >= 0
   * @param {number} options.maxMessages - how many to produce; < 0 forever
   * @param {number | null} options.valuePrefix
   * @param {number | null} options.createTime - ms since epoch, or null
   * @param {number | null} options.repeatingKeys
   */
  constructor({ producer, topic, throughput, maxMessages, valuePrefix = null, createTime = null, repeatingKeys = null }) {
    this.topic = topic;
    this.producer = producer;
    // If maxMessages < 0, produce until the process is killed externally
    this.maxMessages = maxMessages;
    // Number of messages for which acks were received
    this.numAcked = 0;
    // Number of send attempts
    this.numSent = 0;
    // Throttle message throughput if this is set >= 0
    this.throughput = throughput;
    // Hook to trigger producing loop to stop sending messages
    this.stopProducing = false;
    // Prefix (plus a dot separator) added to every value produced by
    // verifiable producer; if null, then values are produced without a prefix
    this.valuePrefix = valuePrefix;
    // Send messages with a key of 0 incrementing by 1 for each message
    // produced when number specified is reached key is reset to 0
    this.repeatingKeys = repeatingKeys;
    this.keyCounter = 0;
    // The create time to set in messages, in milliseconds since epoch
    this.createTime = createTime;
    this.startTime = nowMillis();
  }

  /** Produce a message with given key and value. */
  async send(key, value) {
    // Older versions of ProducerRecord don't include the message create time
    // in the constructor, so Java only includes it when it is explicitly
    // specified; this client always has it, but the two forms are kept.
    let record;
    try {
      if (this.createTime !== null) {
        record = new ProducerRecord({ topic: this.topic, partition: null, timestamp: this.createTime, key, value });
        this.createTime += nowMillis() - this.startTime;
      } else {
        record = new ProducerRecord({ topic: this.topic, key, value });
      }
    } catch (error) {
      // Java builds the record outside its try, so a negative create time ends
      // the producing thread; it is reported as a failed send instead, as the
      // Rust and Python tools do, and not counted as sent.
      printJson(failedSend(key, value, this.topic, error));
      return;
    }

    this.numSent += 1;
    try {
      await this.producer.send({ record, callback: this.#printInfoCallback(key, value) });
    } catch (error) {
      printJson(failedSend(key, value, this.topic, error));
    }
  }

  // Java's PrintInfoCallback: exactly one line per completed send. It runs on
  // the JS thread, inside the producer's callback drain, so the counter needs
  // no lock (Java's `synchronized (System.out)`).
  #printInfoCallback(key, value) {
    return ({ metadata, exception }) => {
      if (exception === null || exception === undefined) {
        this.numAcked += 1;
        printJson(successfulSend(key, value, metadata));
      } else {
        printJson(failedSend(key, value, this.topic, exception));
      }
    };
  }

  /** Returns a string to publish: either "valuePrefix.val" or "val". */
  getValue(val) {
    if (this.valuePrefix !== null) return `${this.valuePrefix}.${val}`;
    return `${val}`;
  }

  /**
   * Returns the next key: null without --repeating-keys, else counting from 0
   * and back to 0 at repeatingKeys.
   */
  getKey() {
    let key = null;
    if (this.repeatingKeys !== null) {
      key = String(this.keyCounter);
      this.keyCounter += 1;
      if (this.keyCounter === this.repeatingKeys) this.keyCounter = 0;
    }
    return key;
  }

  /** Close the producer to flush any remaining messages. */
  async close() {
    await this.producer.close();
    printJson(shutdownComplete());
  }

  /** The producing loop: `maxMessages` sends, or until `stopProducing`. */
  async run(throttler) {
    printJson(startupComplete());
    // negative maxMessages (-1) means "infinite"
    const maxMessages = this.maxMessages < 0 ? Infinity : this.maxMessages;

    for (let i = 0; i < maxMessages; i++) {
      if (this.stopProducing) break;
      const sendStartMs = nowMillis();

      await this.send(this.getKey(), this.getValue(i));

      if (throttler.shouldThrottle(i, sendStartMs)) await throttler.throttle();
    }
  }
}

/** Get the command-line argument parser. */
function argParser() {
  const parser = new ArgumentParser(
    'verifiable-producer',
    'This tool produces increasing integers to the specified topic and prints JSON metadata to stdout on each ' +
      '"send" request, making externally visible which messages have been acked and which have not.',
  );

  parser.addArgument('--topic', { required: true, metavar: 'TOPIC', help: 'Produce messages to this topic.' });
  const connectionGroup = parser.addMutuallyExclusiveGroup({
    title: 'Connection Group',
    description: 'Group of arguments for connection to brokers',
    required: true,
  });
  connectionGroup.addArgument('--bootstrap-server', {
    dest: 'bootstrapServer',
    metavar: 'HOST1:PORT1[,HOST2:PORT2[...]]',
    help:
      'REQUIRED: The server(s) to connect to. Comma-separated list of Kafka brokers in the form ' +
      'HOST1:PORT1,HOST2:PORT2,...',
  });
  parser.addArgument('--max-messages', {
    type: 'int',
    defaultValue: -1,
    dest: 'maxMessages',
    metavar: 'MAX-MESSAGES',
    help: 'Produce this many messages. If -1, produce messages until the process is killed externally.',
  });
  parser.addArgument('--throughput', {
    type: 'int',
    defaultValue: -1,
    dest: 'throughput',
    metavar: 'THROUGHPUT',
    help: 'If set >= 0, throttle maximum message throughput to *approximately* THROUGHPUT messages/sec.',
  });
  parser.addArgument('--acks', {
    type: 'int',
    defaultValue: -1,
    choices: [0, 1, -1],
    dest: 'acks',
    metavar: 'ACKS',
    help: 'Acks required on each produced message. See Kafka docs on acks for details.',
  });
  parser.addArgument('--producer.config', {
    dest: 'producerConfig',
    metavar: 'CONFIG-FILE',
    help:
      '(DEPRECATED) Producer config properties file. This option will be removed in a future version. Use ' +
      '--command-config instead.',
  });
  parser.addArgument('--message-create-time', {
    type: 'int',
    defaultValue: -1,
    dest: 'createTime',
    metavar: 'CREATE-TIME',
    help: "Send messages with creation time starting at the arguments value, in milliseconds since epoch",
  });
  parser.addArgument('--value-prefix', {
    type: 'int',
    dest: 'valuePrefix',
    metavar: 'VALUE-PREFIX',
    help: 'If specified, each produced value will have this prefix with a dot separator',
  });
  parser.addArgument('--repeating-keys', {
    type: 'int',
    dest: 'repeatingKeys',
    metavar: 'REPEATING-KEYS',
    help:
      'If specified, each produced record will have a key starting at 0 increment by 1 up to the number ' +
      'specified (exclusive), then the key is set to 0 again',
  });
  parser.addArgument('--command-config', {
    dest: 'commandConfigFile',
    metavar: 'CONFIG-FILE',
    help: 'Config properties file (config options shared with command line parameters will be overridden).',
  });
  return parser;
}

/**
 * The parsed arguments and the producer's configuration: Java's
 * createFromArgs up to building the producer.
 */
function parseArgs(parser, args) {
  const res = parser.parseArgs(args);
  if (res.producerConfig !== null && res.commandConfigFile !== null) {
    throw new ArgumentParserError('Options --producer.config and --command-config are mutually exclusive.');
  }

  const producerProps = {
    'bootstrap.servers': res.bootstrapServer,
    acks: String(res.acks),
    // No producer retries
    retries: '0',
  };
  // Java also sets key.serializer and value.serializer to StringSerializer;
  // here the keys and values are strings, which the client encodes as UTF-8.
  if (res.producerConfig !== null) {
    printLine(
      'Option --producer.config has been deprecated and will be removed in a future version. Use ' +
        '--command-config instead.',
    );
    Object.assign(producerProps, loadProps(res.producerConfig));
  }
  if (res.commandConfigFile !== null) Object.assign(producerProps, loadProps(res.commandConfigFile));
  return [res, producerProps];
}

/** Construct a VerifiableProducer from command-line arguments. */
function createFromArgs(parser, args) {
  const [res, props] = parseArgs(parser, args);
  // -1 means no explicit create time
  const createTime = res.createTime === -1 ? null : res.createTime;
  const producer = new KafkaProducer({ configs: props });
  return new VerifiableProducer({
    producer,
    topic: res.topic,
    throughput: res.throughput,
    maxMessages: res.maxMessages,
    valuePrefix: res.valuePrefix,
    createTime,
    repeatingKeys: res.repeatingKeys,
  });
}

/** The tool: returns its exit code. */
async function main(args) {
  const parser = argParser();
  if (args.length === 0) {
    process.stdout.write(parser.formatHelp());
    return 0;
  }

  let producer;
  try {
    producer = createFromArgs(parser, args);
  } catch (error) {
    if (error instanceof HelpRequested) {
      process.stdout.write(parser.formatHelp());
      return 0;
    }
    if (!(error instanceof ArgumentParserError)) throw error;
    // argparse4j's handleError: the usage and the error, then exit 1. Any
    // other error, as from Java's main, ends the tool with its stack.
    process.stderr.write(parser.formatUsage());
    process.stderr.write(`${parser.prog}: error: ${error.message}\n`);
    return 1;
  }

  const startMs = nowMillis();
  const throttler = new ThroughputThrottler(producer.throughput, startMs);

  // Java's shutdown hook body, which the JVM runs at the signal or, after a
  // normal end, at exit. Here the signal's hook or the end of the loop runs
  // it, whichever comes first, and the other awaits the same run: once.
  let finishing = null;
  const closeAndSummarize = () => {
    if (finishing === null) {
      finishing = (async () => {
        // Flush any remaining messages. An error closing ends the tool with
        // its stack and no summary, as it ends Java's hook.
        await producer.close();

        // Print a summary
        const stopMs = nowMillis();
        const elapsedMs = stopMs - startMs;
        // When start and stop fall in one millisecond Java's division gives
        // Infinity (NaN with nothing acked), which Jackson prints as the
        // string "Infinity". Reporting 0 instead is this tool's choice, as the
        // Python tool's: the system tests never read avg_throughput.
        const avgThroughput = elapsedMs ? 1000 * (producer.numAcked / elapsedMs) : 0;
        printJson(toolData(producer.numSent, producer.numAcked, producer.throughput, avgThroughput));
      })();
    }
    return finishing;
  };

  const hook = new ShutdownHook(() => {
    // Trigger the loop to stop producing messages, and wake a throttle() that
    // would otherwise keep it parked.
    producer.stopProducing = true;
    throttler.wakeup();
    // Then close the producer from here, as Java's hook does: a send waiting
    // on metadata or buffer space fails at once instead of holding the
    // shutdown for up to max.block.ms.
    return closeAndSummarize();
  });
  try {
    await producer.run(throttler);
    await closeAndSummarize();
    return 0;
  } finally {
    await hook.finish();
  }
}

module.exports = {
  VerifiableProducer,
  argParser,
  createFromArgs,
  failedSend,
  main,
  parseArgs,
  shutdownComplete,
  startupComplete,
  successfulSend,
  toolData,
};

if (require.main === module) {
  main(process.argv.slice(2)).then(
    (code) => {
      process.exitCode = code;
    },
    (error) => {
      process.stderr.write(`${error && error.stack ? error.stack : error}\n`);
      process.exitCode = 1;
    },
  );
}
