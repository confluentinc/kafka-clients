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

// The interface check (root conventions, Stability): the built `.d.ts` files,
// through api-extractor's API report of each import path, and the `exports`
// map of package.json, compared with the committed baseline in api/, the
// previous release's interface.
//
//   node scripts/api_report.js check    compares the build (dist/) with api/
//   node scripts/api_report.js update   writes the build's reports into api/
//
// `make check-api` and `make update-api` run them, after the build. A change
// to the reports or to the exports map:
//   - warns while the version or the baseline is a pre-release (the preview,
//     GA itself, and development between releases), so a pull request that
//     changes the interface on purpose updates api/ with `make update-api`,
//     and the diff shows it;
//   - warns in a release whose major version is above the baseline's (a major
//     release may break);
//   - fails in any other release, a minor or patch one after GA.
// A change to the types alone counts: it can break a user's `tsc` build.
// api/baseline.json records the version the reports were taken at.

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const API_DIR = path.join(ROOT, 'api');
const BASELINE = path.join(API_DIR, 'baseline.json');
const EXPORTS_REPORT = 'exports.json';

/** The package.json of the package. */
function packageJson() {
  return JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
}

/**
 * The import paths and their compiled declarations, from `exports`: the
 * report of `./common/config` is `common_config.api.md`, the root's
 * `index.api.md`.
 */
function entryPoints(pkg) {
  return Object.entries(pkg.exports).map(([subpath, target]) => {
    const name = subpath === '.' ? 'index' : subpath.slice(2).replace(/\//g, '_');
    return { subpath, name, declarations: path.join(ROOT, target.types), report: `${name}.api.md` };
  });
}

/** Writes the API report of each import path into `dir`; throws on an api-extractor error. */
function writeReports(pkg, dir) {
  // Loaded here, not at the top: `check` with no build fails with the
  // build's message first.
  const { Extractor, ExtractorConfig } = require('@microsoft/api-extractor');
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'api-report-'));
  try {
    for (const entry of entryPoints(pkg)) {
      if (!fs.existsSync(entry.declarations)) {
        throw new Error(`${path.relative(ROOT, entry.declarations)} does not exist: run \`make build-ts\` first`);
      }
      const config = ExtractorConfig.prepare({
        configObject: {
          projectFolder: ROOT,
          mainEntryPointFilePath: entry.declarations,
          bundledPackages: [],
          compiler: {
            overrideTsconfig: {
              compilerOptions: {
                strict: true,
                module: 'node16',
                moduleResolution: 'node16',
                target: 'es2022',
                lib: ['es2022', 'es2024.string'],
                types: ['node'],
                typeRoots: [path.join(ROOT, 'node_modules', '@types')],
                skipLibCheck: false,
              },
              files: [entry.declarations],
            },
          },
          apiReport: {
            enabled: true,
            reportFolder: dir,
            reportTempFolder: temp,
            reportFileName: entry.report,
          },
          docModel: { enabled: false },
          dtsRollup: { enabled: false },
          tsdocMetadata: { enabled: false },
          newlineKind: 'lf',
          messages: {
            compilerMessageReporting: { default: { logLevel: 'error' } },
            // The report is the check, not api-extractor's documentation
            // rules: the package marks no release stage (@public, @beta), its
            // stability rules being the root conventions'; its docs have no
            // lint in the preview; and a package's declarations name another
            // package's types (`KafkaFuture` in `producer`), which its own
            // import path exports.
            extractorMessageReporting: { default: { logLevel: 'none', addToApiReportFile: false } },
            tsdocMessageReporting: { default: { logLevel: 'none', addToApiReportFile: false } },
          },
        },
        configObjectFullPath: path.join(ROOT, 'api-extractor.json'),
        packageJsonFullPath: path.join(ROOT, 'package.json'),
      });
      const errors = [];
      const result = Extractor.invoke(config, {
        localBuild: true,
        showVerboseMessages: false,
        messageCallback: (message) => {
          if (message.logLevel === 'error') errors.push(message.text);
          // The report is the output; the routine lines (a report written)
          // would bury the comparison.
          message.handled = true;
        },
      });
      if (!result.succeeded || errors.length > 0) {
        throw new Error(`api-extractor failed on ${entry.subpath}: ${errors.join('; ') || `${result.errorCount} errors`}`);
      }
    }
  } finally {
    fs.rmSync(temp, { recursive: true, force: true });
  }
  fs.writeFileSync(path.join(dir, EXPORTS_REPORT), `${JSON.stringify(pkg.exports, null, 2)}\n`);
}

/** The files of a report directory, by name, with their text. */
function readReports(dir) {
  const out = new Map();
  for (const name of fs.readdirSync(dir).sort()) {
    if (name.endsWith('.api.md') || name === EXPORTS_REPORT) out.set(name, fs.readFileSync(path.join(dir, name), 'utf8'));
  }
  return out;
}

/** The names whose text differs between two report sets, or that only one has. */
function differences(baseline, current) {
  const names = [...new Set([...baseline.keys(), ...current.keys()])].sort();
  return names.filter((name) => baseline.get(name) !== current.get(name)).map((name) => {
    if (!baseline.has(name)) return `${name}: new`;
    if (!current.has(name)) return `${name}: gone`;
    return `${name}: changed`;
  });
}

/**
 * What a change to the interface means for `version`, the package's, against
 * `baselineVersion`: `warn` or `fail` (see the header).
 */
function severity(version, baselineVersion) {
  // Before GA every baseline is a pre-release: the interface is not frozen yet.
  if (version.includes('-') || String(baselineVersion).includes('-')) return 'warn';
  const major = Number(version.split('.')[0]);
  const baselineMajor = Number(String(baselineVersion).split('.')[0]);
  return major > baselineMajor ? 'warn' : 'fail';
}

function check() {
  const pkg = packageJson();
  if (!fs.existsSync(BASELINE)) {
    console.error('api_report: api/baseline.json is missing: run `make update-api` to record the first baseline');
    return 1;
  }
  const baseline = JSON.parse(fs.readFileSync(BASELINE, 'utf8'));
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'api-current-'));
  try {
    writeReports(pkg, dir);
    const changed = differences(readReports(API_DIR), readReports(dir));
    if (changed.length === 0) {
      console.log(`api_report: the interface matches api/ (the baseline of ${baseline.version})`);
      fs.rmSync(dir, { recursive: true, force: true });
      return 0;
    }
    const level = severity(pkg.version, baseline.version);
    const lines = changed.map((c) => `  ${c}`).join('\n');
    const diff = `diff -r ${path.relative(process.cwd(), API_DIR) || '.'} ${dir}`;
    if (level === 'warn') {
      console.warn(
        `api_report: warning: the interface of ${pkg.version} differs from api/ (the baseline of ${baseline.version}):\n${lines}\n` +
          `A pre-release or a major release may change it. If the change is meant, record it with \`make update-api\`; see it with \`${diff}\`.`,
      );
      // Kept, as on the fail path: the message points at it.
      return 0;
    }
    console.error(
      `api_report: the interface of ${pkg.version}, a release of the same major version as the baseline ` +
        `${baseline.version}, differs from api/:\n${lines}\nA minor or patch release must not change it ` +
        `(root conventions, Stability); see it with \`${diff}\`.`,
    );
    return 1;
  } catch (e) {
    fs.rmSync(dir, { recursive: true, force: true });
    throw e;
  }
}

function update() {
  const pkg = packageJson();
  fs.rmSync(API_DIR, { recursive: true, force: true });
  fs.mkdirSync(API_DIR);
  writeReports(pkg, API_DIR);
  fs.writeFileSync(BASELINE, `${JSON.stringify({ version: pkg.version }, null, 2)}\n`);
  console.log(`api_report: wrote the interface of ${pkg.version} into api/`);
  return 0;
}

if (require.main === module) {
  const command = process.argv[2];
  let status;
  try {
    if (command === 'check') status = check();
    else if (command === 'update') status = update();
    else {
      console.error('Usage: node scripts/api_report.js check|update');
      status = 2;
    }
  } catch (e) {
    console.error(`api_report: ${e.message}`);
    status = 1;
  }
  process.exitCode = status;
}

module.exports = { entryPoints, severity, differences };
