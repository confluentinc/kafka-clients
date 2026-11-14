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

// A stand-in for Java's MockProducer, which the JavaScript binding does not
// translate yet: the Python tests of the verifiable producer run over the
// Python MockProducer, and these run over this one. It does what Java's
// MockProducer does for the calls the tool makes: send() checks the closed
// state, then the injected send error, then records the record in history()
// and, with autoComplete, completes it at once at the next offset of
// partition 0; without it, completeNext() / errorNext() complete the oldest
// pending record. The callback gets Java's `{ metadata, exception }`, with
// RecordMetadata's fields read as data, as the client's RecordMetadata is.

const { IllegalStateError } = require('@confluentinc/kafka-javascript');

class MockProducer {
  constructor({ autoComplete = true } = {}) {
    this.autoComplete = autoComplete;
    this.sendError = null;
    this.records = [];
    this.pending = [];
    this.nextOffset = 0;
    this.isClosed = false;
  }

  /** Java's MockProducer.history(): every record sent, in order. */
  history() {
    return [...this.records];
  }

  /** Java's MockProducer.closed(). */
  closed() {
    return this.isClosed;
  }

  /** Java's MockProducer.sendException, thrown by the next sends. */
  setSendError(error) {
    this.sendError = error;
  }

  send({ record, callback = null }) {
    if (this.isClosed) return Promise.reject(new IllegalStateError('MockProducer is already closed.'));
    if (this.sendError !== null) return Promise.reject(this.sendError);
    this.records.push(record);
    const completion = { record, callback };
    if (this.autoComplete) this.#complete(completion, null);
    else this.pending.push(completion);
    return Promise.resolve(null);
  }

  /** Java's MockProducer.completeNext(): whether a record was pending. */
  completeNext() {
    const completion = this.pending.shift();
    if (completion === undefined) return false;
    this.#complete(completion, null);
    return true;
  }

  /** Java's MockProducer.errorNext(e): whether a record was pending. */
  errorNext(error) {
    const completion = this.pending.shift();
    if (completion === undefined) return false;
    this.#complete(completion, error);
    return true;
  }

  async close() {
    this.isClosed = true;
  }

  #complete({ record, callback }, error) {
    const metadata =
      error === null
        ? { topic: record.topic, partition: 0, offset: this.nextOffset++, timestamp: -1 }
        : { topic: record.topic, partition: -1, offset: -1, timestamp: -1 };
    if (callback !== null) callback({ metadata, exception: error });
  }
}

module.exports = { MockProducer };
