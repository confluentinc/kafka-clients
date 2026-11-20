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

// The process warning of the clients' lifecycle (javascript/CLAUDE.md
// "Threads and liveness"): the preview notice, once per process.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');

// The root CLAUDE.md's preview text, for this client.
const PREVIEW_TEXT =
  'The JavaScript client is in preview and not yet recommended for production use. The public API is not ' +
  'stable before the GA release and may change. We welcome your feedback while the design is still open';

let previewWarned = false;

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
