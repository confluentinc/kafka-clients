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

// The process warning of an error with nowhere else to go (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Errors): the function the addon reports
// through (src/internal/native.ts registers it), which src/ calls too.

/**
 * Hands `error`, an error with nowhere else to go, to `process.emitWarning`.
 * It never throws.
 *
 * @param error - the error, or any thrown value
 */
export function warn(error: unknown): void {
  try {
    process.emitWarning(error instanceof Error ? error : String(error));
  } catch {
    // Nothing is left to report it to.
  }
}
