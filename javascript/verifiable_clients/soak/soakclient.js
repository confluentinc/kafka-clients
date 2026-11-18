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

// The producer soak for the JavaScript binding: the producer half of
// python/soak/soakclient.py over the public @confluentinc/kafka-javascript
// producer. It produces at a fixed rate for days, writes the Python soak's
// JSONL metrics, and adjudicates what a producer alone can: every record's
// delivery callback fires exactly once (Java's Callback contract), nothing is
// left outstanding at close, and the delivered offsets of a partition only
// grow. The binding has no consumer yet, so loss is judged by delivery
// reports, not by reading the topic back; the records carry the Python soak's
// SoakRecord payload, so a Python or .NET soak consumer pointed at the topic
// does the end-to-end accounting.
//
// Run: `node soakclient.js -i soak1 -t my-soak-topic -r 80 -f ccloud.config`.
// The configuration file is the Python soak's: `key=value` lines, a
// `producer.` prefix routed to the producer, `consumer.` and `admin.` keys
// and the consumer's own keys left out. The exit codes are the Python soak's,
// so its supervisor's restart policy applies: 0 PASS, 1 a delivery-report
// failure (the headline failure), 2 a configuration the client rejects, 4 a
// shutdown that wedged past --shutdown-timeout.

const os = require('node:os');

const { ArgumentParser, ArgumentParserError, HelpRequested, logger: makeLogger, parseProperties } = require('../_common');
const { SoakMetrics } = require('./soak_metrics');

const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
const { RetriableError } = require('@confluentinc/kafka-javascript/common/errors');

// The thing being soaked: the Rust client driven through its JavaScript
// binding, as `rust_python` names the Python soak's (a token that survives the
// OTLP to Prometheus translation unchanged).
const SOAK_CLIENT_TOKEN = 'rust_javascript';
const METRIC_PFX = `kafka.client.soak.${SOAK_CLIENT_TOKEN}.`;

// The Python soak's exit codes, its supervisor's contract.
const EXIT_OK = 0;
const EXIT_DELIVERY_FAILURE = 1;
const EXIT_FATAL = 2;
const EXIT_WEDGED = 4;

// Unprefixed keys of a shared soak configuration that belong to the consumer
// (python's route_shared_config): left out, as the producer would only log
// them as unknown. `client.rack` is not one: it is a producer key too (Java's
// ProducerConfig, for the rack-aware sticky partitioner), as Python's soak keeps it.
const CONSUMER_ONLY_KEYS = new Set([
  'group.id',
  'group.instance.id',
  'group.protocol',
  'group.remote.assignor',
  'auto.offset.reset',
  'enable.auto.commit',
  'auto.commit.interval.ms',
  'session.timeout.ms',
  'heartbeat.interval.ms',
  'max.poll.interval.ms',
  'max.poll.records',
  'fetch.min.bytes',
  'fetch.max.bytes',
  'fetch.max.wait.ms',
  'max.partition.fetch.bytes',
  'isolation.level',
  'partition.assignment.strategy',
  'check.crcs',
]);

/**
 * The record payload of the Python soak, `${msgid}|${sendTimeMs}|${txcnt}|`
 * padded to the configured size, so the soaks' consumers read each other's
 * topics.
 */
class SoakRecord {
  static PAD_UNIT = ' SoakRecord nr #0';

  constructor(msgid, sendTimeMs = Date.now(), txcnt = 1) {
    this.msgid = msgid;
    this.sendTimeMs = sendTimeMs;
    this.txcnt = txcnt;
  }

  /** The record as bytes, padded to `targetSize` (unpadded if the prefix is longer). */
  serialize(targetSize) {
    const prefix = `${this.msgid}|${this.sendTimeMs}|${this.txcnt}|`;
    const padding = targetSize - prefix.length;
    if (padding <= 0) return Buffer.from(prefix, 'latin1');
    const pad = SoakRecord.PAD_UNIT.repeat(Math.ceil(padding / SoakRecord.PAD_UNIT.length)).slice(0, padding);
    return Buffer.from(prefix + pad, 'latin1');
  }

  /** Parses a serialized record; throws a RangeError for any other payload. */
  static deserialize(bytes) {
    if (bytes === null || bytes === undefined) throw new RangeError('empty payload (null)');
    const text = Buffer.from(bytes).toString('latin1');
    const parts = text.split('|');
    if (parts.length < 4 || !/^\d+$/.test(parts[0]) || !/^\d+$/.test(parts[1]) || !/^\d+$/.test(parts[2])) {
      throw new RangeError(`not a SoakRecord payload: ${JSON.stringify(text.slice(0, 64))}`);
    }
    return new SoakRecord(Number(parts[0]), Number(parts[1]), Number(parts[2]));
  }
}

/**
 * The producer's client configuration from the soak's shared one: `consumer.`
 * and `admin.` keys left out, the `producer.` prefix stripped, the consumer's
 * unprefixed keys left out. Returns `[config, routedAway]`.
 */
function producerConfig(conf) {
  const out = {};
  const routed = [];
  for (const [key, value] of Object.entries(conf)) {
    if (key.startsWith('consumer.') || key.startsWith('admin.')) continue;
    const name = key.startsWith('producer.') ? key.slice('producer.'.length) : key;
    if (!key.startsWith('producer.') && CONSUMER_ONLY_KEYS.has(name)) {
      routed.push(name);
      continue;
    }
    out[name] = value;
  }
  return [out, routed.sort()];
}

/** The producer soak: its loop, its accounting and its verdict. */
class SoakClient {
  constructor(args, conf, { createProducer = (configs) => new KafkaProducer({ configs }) } = {}) {
    this.testid = args.testid;
    this.topic = args.topic;
    this.rate = args.rate;
    this.variant = args.variant;
    this.payloadSize = args.payloadSize;
    this.maxSendAttempts = args.maxSendAttempts;
    this.logger = makeLogger(`soak.${args.testid}`);
    this.disprate = Math.max(1, Math.trunc(this.rate * 10));
    this.statusIntervalMs = (1000 * this.disprate) / this.rate;
    const hostname = os.hostname();
    this.metrics = new SoakMetrics({
      path: args.metricsFile,
      baseTags: { host: `${SOAK_CLIENT_TOKEN}-${hostname}-${this.topic}`, testid: this.testid, variant: this.variant },
      prefix: METRIC_PFX,
    });
    this.run = true;
    this.stopWakers = new Set();
    this.msgid = 0;
    this.drCnt = 0;
    this.drErrCnt = 0;
    this.outstanding = 0;
    // msgids sent whose callback has not fired yet, and those that fired twice.
    this.pending = new Set();
    this.duplicateReports = 0;
    // The highest delivered offset of each partition, and the reports below it.
    this.highWaterMarks = new Map();
    this.offsetRegressions = 0;
    this.closeError = null;
    this.startTime = Date.now();
    const [configs, routed] = producerConfig(conf);
    if (routed.length > 0) this.logger.info(`consumer configuration key(s) left out: ${routed.join(', ')}`);
    this.producer = createProducer(configs);
    this.baselineRss = process.memoryUsage().rss;
  }

  /** Waits `ms`, returning early once stop is requested. */
  wait(ms) {
    return new Promise((resolve) => {
      if (!this.run) {
        resolve();
        return;
      }
      const timer = setTimeout(done, ms);
      const stopWakers = this.stopWakers;
      function done() {
        clearTimeout(timer);
        stopWakers.delete(done);
        resolve();
      }
      stopWakers.add(done);
    });
  }

  requestStop() {
    this.run = false;
    for (const wake of [...this.stopWakers]) wake();
  }

  // The delivery callback of `msgid`: Java's Callback, exactly once per record.
  onDelivery(msgid, sentAt, size) {
    return ({ metadata, exception }) => {
      if (!this.pending.delete(msgid)) {
        this.duplicateReports += 1;
        this.metrics.incrCounter('producer.dup_report', 1);
        this.logger.error(`producer: second delivery report for msgid ${msgid}`);
        return;
      }
      this.outstanding -= 1;
      if (exception !== null && exception !== undefined) {
        this.drErrCnt += 1;
        this.logger.warning(`producer: delivery failed: ${exception.name}: ${exception.message}`);
        this.metrics.incrCounter('producer.drerr', 1);
        this.metrics.incrCounter('producer.delivery.failure', 1, { err: exception.name });
        return;
      }
      const latencyMs = Date.now() - sentAt;
      this.drCnt += 1;
      this.metrics.incrCounter('producer.drok', 1);
      this.metrics.setGauge('producer.latency', latencyMs, { partition: String(metadata.partition) });
      this.metrics.observeMessage(size, latencyMs);
      const previous = this.highWaterMarks.get(metadata.partition);
      if (previous !== undefined && metadata.offset <= previous) {
        this.offsetRegressions += 1;
        this.metrics.incrCounter('producer.offset_regression', 1);
      } else {
        this.highWaterMarks.set(metadata.partition, metadata.offset);
      }
      if (this.drCnt % this.disprate === 0) {
        this.logger.debug(
          `producer: delivered message to ${metadata.topic} [${metadata.partition}] at offset ${metadata.offset} ` +
            `in ${latencyMs} ms`,
        );
      }
    };
  }

  /**
   * Produces one record. `send()` resolves at the hand-over (Java's send()
   * returning, after a wait for metadata or buffer space); a retriable
   * rejection is retried up to --max-send-attempts, the record's `txcnt`
   * counting the attempts.
   */
  async produceRecord() {
    const msgid = this.msgid++;
    let txcnt = 0;
    while (this.run && txcnt < this.maxSendAttempts) {
      txcnt += 1;
      const sentAt = Date.now();
      const value = new SoakRecord(msgid, sentAt, txcnt).serialize(this.payloadSize);
      const record = new ProducerRecord({ topic: this.topic, value });
      this.pending.add(msgid);
      this.outstanding += 1;
      try {
        await this.producer.send({ record, callback: this.onDelivery(msgid, sentAt, value.length) });
      } catch (error) {
        // Rejected at the hand-over: the callback does not run for it.
        this.pending.delete(msgid);
        this.outstanding -= 1;
        if (!(error instanceof RetriableError) || txcnt >= this.maxSendAttempts) {
          this.drErrCnt += 1;
          this.logger.error(`producer: giving up on msgid ${msgid}: ${error && error.name}: ${error && error.message}`);
          this.metrics.incrCounter('producer.drerr', 1);
          return;
        }
        this.logger.warning(`producer: send attempt ${txcnt} for msgid ${msgid} failed (retriable): ${error.message}`);
        await this.wait(100);
        continue;
      }
      this.metrics.incrCounter('producer.send', 1);
      return;
    }
  }

  producerStatus() {
    this.logger.info(
      `producer: ${this.msgid} messages produced, ${this.drCnt} delivered, ${this.drErrCnt} failed, ` +
        `0 error_cbs, ${this.outstanding} outstanding`,
    );
  }

  /**
   * The producing loop: batches of `max(1, rate / 100)` records, the batch's
   * remaining time budget slept off (python's batched pacing), until stopped;
   * then flush.
   */
  async producerRun() {
    const batch = Math.max(1, Math.trunc(this.rate / 100));
    const batchIntervalMs = (1000 * batch) / this.rate;
    let nextStatus = Date.now() + this.statusIntervalMs;
    while (this.run) {
      const started = Date.now();
      for (let i = 0; i < batch && this.run; i++) await this.produceRecord();
      const now = Date.now();
      if (now > nextStatus) {
        this.producerStatus();
        nextStatus = now + this.statusIntervalMs;
      }
      const remaining = batchIntervalMs - (Date.now() - started);
      if (remaining > 0) await this.wait(remaining);
    }
    this.logger.info('producer: flushing');
    await this.producer.flush();
    this.producerStatus();
  }

  /** Samples the process's resources into the gauges (python's get_rusage). */
  sampleResources() {
    const memory = process.memoryUsage();
    const mib = 1024 * 1024;
    this.metrics.setGauge('memory.rss', memory.rss / mib);
    this.metrics.setGauge('memory.rss.baseline_constructed', this.baselineRss / mib);
    this.metrics.setGauge('memory.rss.delta', (memory.rss - this.baselineRss) / mib);
    // The JavaScript heap and the memory of Buffers, the counterparts of the
    // Python soak's tracemalloc: RSS climbing while these stay flat points at
    // the addon or the Rust core.
    this.metrics.setGauge('memory.heap_used', memory.heapUsed / mib);
    this.metrics.setGauge('memory.external', memory.external / mib);
    this.metrics.setGauge('producer.outq', this.outstanding);
    // Kept so dashboards ported from the Python soak keep their series: the
    // client takes no error callback.
    this.metrics.incrCounter('producer.errorcb', 0);
  }

  /**
   * The verdict: FAIL when a record's callback never ran or ran twice, or
   * close() failed; offset regressions and failed deliveries are reported,
   * not failed (a failed delivery is a record the client reported, and
   * without idempotence a retry may reorder a partition's batches).
   */
  verdict() {
    if (this.pending.size > 0) return `FAIL (${this.pending.size} missing delivery report(s))`;
    if (this.duplicateReports > 0) return `FAIL (${this.duplicateReports} duplicate delivery report(s))`;
    if (this.closeError !== null) return `FAIL (close failed: ${this.closeError.message})`;
    return 'PASS';
  }

  /** The one-line result, on stdout whatever the log level; returns the verdict. */
  finalReport() {
    const verdict = this.verdict();
    process.stdout.write(
      `SUMMARY variant=${this.variant} testid=${this.testid} topic=${this.topic} produced=${this.msgid} ` +
        `delivered=${this.drCnt} failed=${this.drErrCnt} missing_reports=${this.pending.size} ` +
        `duplicate_reports=${this.duplicateReports} offset_regressions=${this.offsetRegressions} verdict=${verdict}\n`,
    );
    return verdict;
  }

  /** Closes the producer (every outstanding record settles first) and the metrics. */
  async terminate() {
    try {
      await this.producer.close();
    } catch (error) {
      this.closeError = error;
      this.logger.error(`producer: close failed: ${error && error.name}: ${error && error.message}`);
    }
    this.sampleResources();
    this.metrics.close();
  }
}

function buildArgParser() {
  const parser = new ArgumentParser('soakclient', 'Kafka Rust client soak test, the JavaScript producer');
  parser.addArgument('-i', { dest: 'testid', required: true, metavar: 'TESTID', help: 'Test id' });
  parser.addArgument('-b', { dest: 'brokers', metavar: 'BROKERS', help: 'Bootstrap servers' });
  parser.addArgument('-t', { dest: 'topic', required: true, metavar: 'TOPIC', help: 'Topic to use' });
  parser.addArgument('-r', { dest: 'rate', defaultValue: '80', metavar: 'RATE', help: 'Message produce rate per second' });
  parser.addArgument('-f', { dest: 'conffile', metavar: 'CONFFILE', help: 'Configuration file (configprop=value format)' });
  parser.addArgument('--variant', {
    dest: 'variant',
    defaultValue: process.env.SOAK_VARIANT || 'unspecified',
    metavar: 'VARIANT',
    help: 'Variant label, emitted as the "variant" metric tag',
  });
  parser.addArgument('--payload-size', {
    dest: 'payloadSize',
    type: 'int',
    defaultValue: 50,
    metavar: 'PAYLOAD_SIZE',
    help: 'Target serialized record size in bytes',
  });
  parser.addArgument('--metrics-file', {
    dest: 'metricsFile',
    metavar: 'METRICS_FILE',
    help: 'JSONL metrics output path (default: soak-metrics-<variant>-<testid>.jsonl)',
  });
  parser.addArgument('--metrics-interval', {
    dest: 'metricsInterval',
    defaultValue: '10',
    metavar: 'SECONDS',
    help: 'Seconds between metrics windows',
  });
  parser.addArgument('--max-send-attempts', {
    dest: 'maxSendAttempts',
    type: 'int',
    defaultValue: 10,
    metavar: 'N',
    help: 'Attempts before abandoning a record whose send() rejects with a retriable error',
  });
  parser.addArgument('--runtime-seconds', {
    dest: 'runtimeSeconds',
    defaultValue: '0',
    metavar: 'SECONDS',
    help: 'Exit after this many seconds (0: run forever)',
  });
  parser.addArgument('--shutdown-timeout', {
    dest: 'shutdownTimeout',
    defaultValue: '60',
    metavar: 'SECONDS',
    help: 'Hard-exit if shutdown takes longer than this, in seconds',
  });
  return parser;
}

/** A float argument, as Python's argparse `type=float` reads one. */
function float(name, raw) {
  const value = Number(String(raw).trim());
  if (String(raw).trim() === '' || Number.isNaN(value)) {
    throw new ArgumentParserError(`argument ${name}: invalid float value: '${raw}'`);
  }
  return value;
}

/** Parses the command line; throws ArgumentParserError or HelpRequested. */
function parseArgs(argv) {
  const parser = buildArgParser();
  const args = parser.parseArgs(argv);
  args.rate = float('-r', args.rate);
  args.metricsInterval = float('--metrics-interval', args.metricsInterval);
  args.runtimeSeconds = float('--runtime-seconds', args.runtimeSeconds);
  args.shutdownTimeout = float('--shutdown-timeout', args.shutdownTimeout);
  if (args.rate <= 0) throw new ArgumentParserError('-r must be greater than zero');
  if (args.metricsInterval <= 0) throw new ArgumentParserError('--metrics-interval must be greater than zero');
  if (args.metricsFile === null) args.metricsFile = `soak-metrics-${args.variant}-${args.testid}.jsonl`;
  return args;
}

async function main(argv) {
  let args;
  try {
    args = parseArgs(argv);
  } catch (error) {
    if (error instanceof HelpRequested) {
      process.stdout.write(buildArgParser().formatHelp());
      return EXIT_OK;
    }
    if (!(error instanceof ArgumentParserError)) throw error;
    process.stderr.write(buildArgParser().formatUsage());
    process.stderr.write(`soakclient: error: ${error.message}\n`);
    return EXIT_FATAL;
  }

  let conf = {};
  if (args.conffile !== null) {
    try {
      conf = parseProperties(require('node:fs').readFileSync(args.conffile, 'latin1'));
    } catch (error) {
      process.stderr.write(`soakclient: configuration error: ${error.message}\n`);
      return EXIT_FATAL;
    }
  }
  // Overwrite any brokers from the configuration file.
  if (args.brokers !== null) conf['bootstrap.servers'] = args.brokers;

  let soak;
  try {
    soak = new SoakClient(args, conf);
  } catch (error) {
    // A configuration the client rejects (ConfigError and friends) will never
    // work: the supervisor must not restart it.
    process.stderr.write(`soakclient: fatal startup error: ${error && error.name}: ${error && error.message}\n`);
    return EXIT_FATAL;
  }

  let watchdog = null;
  const shutdown = () => {
    if (watchdog !== null) return;
    process.stdout.write('Termination signal received, shutting down...\n');
    // The shutdown watchdog: flush() and close() have no time limit, and a
    // wedged shutdown must not need a human. Exits EXIT_WEDGED, which the
    // supervisor restarts.
    watchdog = setTimeout(() => {
      process.stderr.write('Shutdown watchdog expired, hard-exiting\n');
      process.exit(EXIT_WEDGED);
    }, args.shutdownTimeout * 1000);
    watchdog.unref();
    soak.requestStop();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  soak.sampleResources();
  const windows = setInterval(() => {
    soak.sampleResources();
    soak.metrics.writeRecord(soak.metrics.rollover());
  }, args.metricsInterval * 1000);
  windows.unref();
  let deadline = null;
  if (args.runtimeSeconds > 0) {
    deadline = setTimeout(() => {
      soak.logger.info(`Runtime limit of ${args.runtimeSeconds}s reached`);
      shutdown();
    }, args.runtimeSeconds * 1000);
  }

  try {
    await soak.producerRun();
  } catch (error) {
    soak.logger.error(`producer: fatal error: ${error && error.stack}`);
    shutdown();
  } finally {
    clearInterval(windows);
    if (deadline !== null) clearTimeout(deadline);
  }
  if (watchdog === null) shutdown();
  await soak.terminate();
  const verdict = soak.finalReport();
  return verdict === 'PASS' ? EXIT_OK : EXIT_DELIVERY_FAILURE;
}

module.exports = {
  EXIT_DELIVERY_FAILURE,
  EXIT_FATAL,
  EXIT_OK,
  EXIT_WEDGED,
  METRIC_PFX,
  SoakClient,
  SoakRecord,
  main,
  parseArgs,
  producerConfig,
};

if (require.main === module) {
  main(process.argv.slice(2)).then(
    (code) => {
      process.exitCode = code;
    },
    (error) => {
      process.stderr.write(`${error && error.stack ? error.stack : error}\n`);
      process.exitCode = EXIT_FATAL;
    },
  );
}
