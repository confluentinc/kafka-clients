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

// Java's KafkaMetric, in plain TypeScript (root CLAUDE.md, JavaScript Binding
// Conventions, Implementation over the FFI: only the clients call the addon).
// Its value provider, configuration and time are the untranslated
// MetricValueProvider, MetricConfig and Time, read by their Java methods:
// `value(config, now)` (or a Measurable's `measure(config, now)`) and
// `milliseconds()`. Java's lock guards the reads against other threads; the
// JavaScript thread is the only one, so it is kept and never taken.
//
// Not translated: the package-private measurableValue(long).

import { JavaForms } from '../../internal/args';
import { illegalState } from '../../internal/errors';
import type { Metric } from '../metric';
import type { MetricName } from '../metric_name';
import type { Measurable } from './measurable';
import type { MetricConfig } from './metric_config';
import type { MetricValueProvider } from './metric_value_provider';

const FORMS = new JavaForms('KafkaMetric', [['lock', 'metricName', 'valueProvider', 'config', 'time']]);
const CONFIG_FORMS = new JavaForms('config', [[], ['config']], { bareTypes: { config: () => true } });
const METRIC_NAME_FORMS = new JavaForms('metricName', [[]], { reader: true });
const METRIC_VALUE_FORMS = new JavaForms('metricValue', [[]], { reader: true });
const IS_MEASURABLE_FORMS = new JavaForms('isMeasurable', [[]], { reader: true });
const MEASURABLE_FORMS = new JavaForms('measurable', [[]], { reader: true });

/** The function named `name` of `value`, or undefined. */
function method(value: unknown, name: string): ((...args: unknown[]) => unknown) | undefined {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function')) return undefined;
  const fn: unknown = (value as Record<string, unknown>)[name];
  return typeof fn === 'function' ? (fn as (...args: unknown[]) => unknown) : undefined;
}

/**
 * Java's `Class.getName()` of the provider: its class's name, or null for an
 * anonymous one (a plain object, a function), as Java's synthetic and
 * anonymous classes.
 */
function className(value: unknown): string | null {
  if (value === null || typeof value !== 'object') return null;
  try {
    const proto: unknown = Object.getPrototypeOf(value);
    const ctor: unknown = proto === null ? undefined : (proto as { constructor?: unknown }).constructor;
    if (typeof ctor === 'function' && ctor !== Object && typeof ctor.name === 'string' && ctor.name !== '') return ctor.name;
  } catch {
    // A revoked Proxy: anonymous.
  }
  return null;
}

/**
 * A metric, backed by a value provider.
 */
export class KafkaMetric implements Metric {
  readonly #metricName: MetricName;
  readonly #lock: unknown;
  readonly #time: unknown;
  readonly #metricValueProvider: MetricValueProvider;
  #config: MetricConfig;

  /**
   * Create a metric to monitor an object that implements
   * MetricValueProvider.
   *
   * @param options - the lock, the metric's name, its value provider, its
   *   configuration and the time
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{lock, metricName, valueProvider, config, time}`
   * @throws `TypeError` when `valueProvider` is null ("valueProvider must
   *   not be null", Java's `Objects.requireNonNull`) or `undefined`
   *
   * @remarks The value provider, the configuration and the time are Java
   * types this client does not translate, read by their Java methods
   * (`value(config, now)`, `measure(config, now)`, `milliseconds()`); the
   * lock is kept but never taken, JavaScript having one thread.
   */
  constructor(options: {
    /** The lock used to prevent race condition */
    lock: unknown;
    /** The name of the metric */
    metricName: MetricName;
    /** The metric value provider associated with this metric */
    valueProvider: MetricValueProvider;
    /** The configuration of the metric */
    config: MetricConfig;
    /** The time instance to use with the metrics */
    time: unknown;
  }) {
    FORMS.match(options, arguments.length);
    const valueProvider = FORMS.value(options, 'valueProvider');
    if (valueProvider === null) throw new TypeError('valueProvider must not be null');
    this.#metricName = FORMS.value(options, 'metricName') as MetricName;
    this.#lock = FORMS.value(options, 'lock');
    this.#metricValueProvider = valueProvider;
    this.#config = FORMS.value(options, 'config');
    this.#time = FORMS.value(options, 'time');
    void this.#lock;
  }

  /**
   * Get the configuration of this metric. This is supposed to be used by
   * server only.
   *
   * @returns Return the config of this metric
   */
  config(): MetricConfig;
  /**
   * Set the metric config. This is supposed to be used by server only.
   *
   * @param options - the configuration
   */
  config(options: {
    /** configuration for this metrics */
    config: MetricConfig;
  }): void;
  /**
   * Set the metric config: the configuration on its own, the same as
   * `config({config})`. A plain object is read as the options object.
   *
   * @param config - configuration for this metrics
   */
  config(config: MetricConfig): void;
  config(config?: unknown): unknown {
    const form = CONFIG_FORMS.match(config, arguments.length);
    if (form === 0) return this.#config;
    this.#config = CONFIG_FORMS.value(config, 'config');
    return undefined;
  }

  /**
   * Get the metric name
   *
   * @returns Return the name of this metric
   * @throws `TypeError` when it is given an argument
   */
  metricName(): MetricName {
    METRIC_NAME_FORMS.match(undefined, arguments.length);
    return this.#metricName;
  }

  /**
   * Take the metric and return the value via `MetricValueProvider.value(MetricConfig, long)`.
   *
   * @returns Return the metric value
   * @throws `TypeError` when it is given an argument, or the time is null
   *   (Java's `time.milliseconds()` on a null)
   */
  metricValue(): unknown {
    METRIC_VALUE_FORMS.match(undefined, arguments.length);
    const milliseconds = method(this.#time, 'milliseconds');
    if (milliseconds === undefined) throw new TypeError('time must not be null');
    const now = milliseconds.call(this.#time);
    const provider = this.#metricValueProvider;
    const value = method(provider, 'value');
    // A Measurable's value() is Java's default: measure(config, now).
    if (value === undefined && this.isMeasurable()) return (method(provider, 'measure') as (...args: unknown[]) => unknown).call(provider, this.#config, now);
    if (value === undefined) throw new TypeError('valueProvider must have a value(config, now) method');
    return value.call(provider, this.#config, now);
  }

  /**
   * The method determines if the metric value provider is of type
   * Measurable.
   *
   * @returns true if the metric value provider is of type Measurable, false
   *   otherwise.
   * @throws `TypeError` when it is given an argument
   */
  isMeasurable(): boolean {
    IS_MEASURABLE_FORMS.match(undefined, arguments.length);
    return method(this.#metricValueProvider, 'measure') !== undefined;
  }

  /**
   * Get the underlying metric provider, which should be a `Measurable`
   *
   * @returns Return the metric provider
   * @throws `IllegalStateError` if the underlying metric is not a
   *   `Measurable`.
   * @throws `TypeError` when it is given an argument
   */
  measurable(): Measurable {
    MEASURABLE_FORMS.match(undefined, arguments.length);
    if (this.isMeasurable()) return this.#metricValueProvider;
    // Java's "Not a measurable: " + getClass(): `class <name>`.
    throw illegalState(`Not a measurable: class ${className(this.#metricValueProvider) ?? typeof this.#metricValueProvider}`);
  }

  /**
   * Returns a human-readable representation of this metric.
   *
   * The metricValueProvider is represented by its class name rather than its
   * full toString() to avoid dumping internal stat state (e.g.
   * SampledStat's samples list) into logs, which could be verbose and is
   * rarely useful for identifying which metric changed.
   *
   * @returns the metric's text
   */
  toString(): string {
    const name = className(this.#metricValueProvider);
    if (name === null) return `KafkaMetric [metricName=${String(this.#metricName)}]`;
    return `KafkaMetric [metricName=${String(this.#metricName)}, metricValueProvider=${name}]`;
  }

  /**
   * A metric read when the producer's `metrics()` ran: its value, as the
   * core measured it then, through a provider that returns it (a Measurable
   * one for a measurable metric). Called by the producer.
   *
   * @internal
   */
  static internalSnapshot(metricName: MetricName, value: unknown, measurable: boolean): KafkaMetric {
    const valueProvider = measurable ? { measure: () => value } : { value: () => value };
    return new KafkaMetric({ lock: null, metricName, valueProvider, config: null, time: SYSTEM_TIME });
  }
}

/** Java's Time.SYSTEM: the wall clock's milliseconds. */
const SYSTEM_TIME = { milliseconds: (): number => Date.now() };
