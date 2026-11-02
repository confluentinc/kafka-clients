// Copyright 2025 Confluent Inc.
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

// Java's Callback interface, a single method, is a JS function
// `(metadata, error) => void` (private helpers).
//
// A callback method the user can implement to provide asynchronous handling
// of request completion. This method will be called when the record sent to
// the server has been acknowledged. When error is not null in the callback,
// metadata will contain the special -1 value for all fields. If topicPartition
// cannot be chosen, a -1 value will be assigned.

/** The callback `send` was given, or null; throws a TypeError for a non-function. */
function checkCallback(callback) {
  if (callback === undefined || callback === null) return null;
  if (typeof callback !== 'function') throw new TypeError('callback must be a function (metadata, error) => void');
  return callback;
}

/** The text the core logs for a callback that threw (Java logs a throwing onCompletion). */
function thrownMessage(thrown) {
  if (thrown instanceof Error) return `${thrown.name}: ${thrown.message}`;
  try {
    return String(thrown);
  } catch {
    return 'the callback threw a value that cannot be converted to a string';
  }
}

module.exports = { checkCallback, thrownMessage };
