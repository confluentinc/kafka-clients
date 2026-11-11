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

// The process warnings of the clients' lifecycle (javascript/CLAUDE.md
// "Threads and liveness"): the preview notice, once per process, and the
// count of producers never closed, once, when the event loop is about to
// empty.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');

/** The code of the warning about producers never closed. */
export const NOT_CLOSED_WARNING_CODE = 'CONFLUENT_KAFKA_PRODUCER_NOT_CLOSED';

// The root CLAUDE.md's preview text, for this client.
const PREVIEW_TEXT =
  'The JavaScript client is in preview and not yet recommended for production use. The public API is not ' +
  'stable before the GA release and may change. We welcome your feedback while the design is still open';

let previewWarned = false;
// Producers built and not closed yet (close() called and not refused).
let open = 0;
let exitHooked = false;
let notClosedWarned = false;

/**
 * Emits the preview notice the first time a client is built in the process
 * (the addon, loaded once per process, tells the main thread and the workers
 * apart from the first): one `ExperimentalWarning`, which `--no-warnings` (or
 * `--disable-warning=ExperimentalWarning`) silences.
 */
export function warnPreview(): void {
  if (previewWarned) return;
  previewWarned = true;
  if (!native.previewNoticeDue()) return;
  process.emitWarning(PREVIEW_TEXT, { type: 'ExperimentalWarning' });
}

// The process is about to exit its event loop: a producer still open keeps
// running until then (no garbage collection closes it), and its records not
// delivered yet fail when the environment is torn down.
function warnNotClosed(): void {
  if (open <= 0 || notClosedWarned) return;
  notClosedWarned = true;
  const which = open === 1 ? '1 KafkaProducer was' : `${open} KafkaProducers were`;
  process.emitWarning(
    `${which} never closed. Call close() (or use \`await using\`) before the process exits: a producer not ` +
      'closed keeps running until the process exits, and the records it has not delivered by then fail.',
    { code: NOT_CLOSED_WARNING_CODE },
  );
}

/** A producer was built (or a close it was given was refused): it is open. */
export function producerOpened(): void {
  open += 1;
  if (!exitHooked) {
    exitHooked = true;
    // 'beforeExit': the event loop is empty (an idle producer does not keep
    // it alive), and a warning emitted there is still printed, unlike at 'exit'.
    process.on('beforeExit', warnNotClosed);
  }
}

/** close() was called on an open producer. */
export function producerClosed(): void {
  open -= 1;
}
