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

// Finds the Node-API addon (private), for `native.ts`. The addon is
// `confluent-kafka-javascript.node`, looked for in this order:
//   1. the platform package of this machine,
//      @confluentinc/kafka-javascript-<key>, which npm installs from the
//      main package's optionalDependencies; it carries the addon with the
//      Rust core statically linked (.semaphore/pack-npm.sh);
//   2. the development build, native/build/Release/ (`make -C javascript
//      build`), beside dist/, which holds this module compiled: the
//      checkout's tests, the gRPC test image.
// The functions take what they read as parameters, so the unit tests can
// drive them without a platform package or another platform.

import { readdirSync } from 'node:fs';
import { join } from 'node:path';

/** The file name of the addon, in a platform package and in native/build/Release/. */
export const ADDON_FILE = 'confluent-kafka-javascript.node';

/** The scope and name prefix of the platform packages. */
export const PLATFORM_PACKAGE_PREFIX = '@confluentinc/kafka-javascript-';

/**
 * The platform keys a platform package is published for: the preview's,
 * linux-x64 with glibc and darwin-arm64, one folder each in `npm/`.
 */
export const SUPPORTED_PLATFORMS: readonly string[] = ['linux-x64-gnu', 'darwin-arm64'];

/** The parts of `process` the platform key is computed from. */
export interface ProcessInfo {
  platform: string;
  arch: string;
  report?: {
    excludeNetwork?: boolean;
    getReport(): unknown;
  };
}

/**
 * Whether this Linux machine's C library is glibc (`gnu`) or musl (`musl`).
 * It reads the runtime glibc version of the process report, with the report's
 * network section excluded, so that no DNS lookup can block it (as Rollup's
 * loader does); `excludeNetwork` is restored afterwards. Without a report it
 * is musl when a musl dynamic loader (/lib/ld-musl-*) exists, else glibc.
 */
export function linuxLibc(proc: ProcessInfo, libDir: () => string[] = () => readdirSync('/lib')): 'gnu' | 'musl' {
  const report = proc.report;
  if (report !== undefined && typeof report.getReport === 'function') {
    const previous = report.excludeNetwork;
    try {
      report.excludeNetwork = true;
      const header = (report.getReport() as { header?: { glibcVersionRuntime?: unknown } }).header;
      if (header !== undefined) return header.glibcVersionRuntime ? 'gnu' : 'musl';
    } catch {
      // Falls back to the loader below.
    } finally {
      try {
        report.excludeNetwork = previous;
      } catch {
        // A Node without the setting.
      }
    }
  }
  let entries: string[];
  try {
    entries = libDir();
  } catch {
    entries = [];
  }
  return entries.some((name) => name.startsWith('ld-musl-')) ? 'musl' : 'gnu';
}

/**
 * The platform key of this machine: `<platform>-<arch>`, plus `-gnu` or
 * `-musl` on Linux (`linux-x64-gnu`, `darwin-arm64`, `win32-x64`).
 */
export function platformKey(proc: ProcessInfo, libc: () => 'gnu' | 'musl' = () => linuxLibc(proc)): string {
  const base = `${proc.platform}-${proc.arch}`;
  return proc.platform === 'linux' ? `${base}-${libc()}` : base;
}

/** The candidates, in order: the platform package's addon, then the development build. */
export function addonCandidates(key: string, devBuildDir: string): string[] {
  return [`${PLATFORM_PACKAGE_PREFIX}${key}/${ADDON_FILE}`, join(devBuildDir, 'native', 'build', 'Release', ADDON_FILE)];
}

/** The addon that loaded, and where from. */
export interface LoadedAddon {
  addon: unknown;
  path: string;
}

/**
 * Loads the first candidate of `addonCandidates(key, devBuildDir)` that
 * `load` (`require`) can load. When none loads, throws an Error naming the
 * platform package and the usual causes (optional dependencies left out at
 * install, a lockfile made on another OS, or a platform with no package), the
 * supported keys and both remedies, caused by the AggregateError of each
 * candidate's failure.
 */
export function loadAddon(load: (id: string) => unknown, key: string, devBuildDir: string): LoadedAddon {
  const failures: unknown[] = [];
  for (const path of addonCandidates(key, devBuildDir)) {
    try {
      return { addon: load(path), path };
    } catch (e) {
      failures.push(e);
    }
  }
  const supported = SUPPORTED_PLATFORMS.includes(key);
  throw new Error(
    `cannot load the native addon of @confluentinc/kafka-javascript for platform ${key}: ` +
      (supported
        ? `${PLATFORM_PACKAGE_PREFIX}${key} is not installed or does not load`
        : `there is no ${PLATFORM_PACKAGE_PREFIX}${key}, the platform is not supported`) +
      `, and the development build ${addonCandidates(key, devBuildDir)[1]} does not load either. ` +
      (supported
        ? 'The usual causes: the optional dependencies were left out at install (--omit=optional), or the ' +
          'lockfile was made on another OS, so it has no entry for this platform package. '
        : '') +
      'Install @confluentinc/kafka-javascript on a supported platform, with its optional dependencies ' +
      '(not --omit=optional); to install it for another machine, or to make a lockfile that also serves ' +
      'another OS, give npm that machine\'s `--os`, `--cpu` and ' +
      '`--libc`. Or build it from source with `make -C javascript build`, after building libconfluent_kafka ' +
      `(\`make -C rust build-all-features\`). Supported platforms: ${SUPPORTED_PLATFORMS.join(', ')}`,
    { cause: new AggregateError(failures, 'every candidate addon failed to load') },
  );
}

