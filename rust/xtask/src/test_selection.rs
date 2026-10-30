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

//! Test selection.
//!
//! `select-tests` gives an agent the changes from a base branch and writes the
//! tests they can affect to `test-selection.json` at the repository root (or
//! the file `TEST_SELECTION` names, relative to the root). While that file
//! exists, `selected-cargo-test` and `selected-run`, which the Makefiles' test
//! recipes go through, run only those tests; without it they run everything.
//!
//! The file holds one section per suite. A section that is missing runs
//! nothing, `[]` runs the whole suite, and a list runs only what it names:
//!
//! - `rust`: test unit (`lib`, `doc`, an integration test target, another
//!   workspace package) -> libtest name filters;
//! - `c.unit`: ctest names; `c.integration`: filters of the `__grpc_c` tests;
//! - `python.<suite>`: pytest paths or node ids, relative to `python/`;
//!   `python.typecheck`: `[]` runs mypy; `python.grpc`: filters of the
//!   `__grpc_python` tests.
//!
//! The file records the change it was made for (`merge_base`,
//! `changed_files`). When the change has grown since, a file changed or added
//! that the agent did not see, the selection no longer applies and every test
//! runs again, so a commit is never checked against a selection made for less
//! than it holds. Further edits to a file the agent already saw keep it.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context as _};
use serde_json::{Map, Value};

use crate::REPO_ROOT;

const SELECTION_FILE: &str = "test-selection.json";
const FORMAT_VERSION: u64 = 1;
const ROOT_PACKAGE: &str = "confluent-kafka";

const DEFAULT_BASE: &str = "origin/master";
const DEFAULT_AGENT: &str = "claude";
const DEFAULT_MODEL: &str = "opus";
/// Claude Code's `--allowedTools`: the agent reads the repository and the
/// history, and changes nothing.
const AGENT_TOOLS: &str = "Read,Grep,Glob,Bash(git diff:*),Bash(git log:*),Bash(git show:*),\
                           Bash(git status:*),Bash(git ls-files:*),Bash(ls:*)";
/// Above this size the changed-file list is left for the agent to read with git.
const MAX_INLINE_CHANGES: usize = 64 * 1024;

/// The pytest suites of `python/`: section name and the directory it runs.
const PYTHON_SUITES: &[(&str, &str)] = &[
    ("unit", "test/unit"),
    ("integration", "test/integration"),
    ("static", "test/static"),
    ("soak", "soak/test"),
    ("verifiable_clients", "verifiable_clients/test"),
    ("performance", "test/performance"),
];

/// The cargo test options that select targets. Without any, `cargo test` also
/// runs the doctests.
const TARGET_FLAGS: &[&str] = &[
    "--lib",
    "--bin",
    "--bins",
    "--test",
    "--tests",
    "--example",
    "--examples",
    "--bench",
    "--benches",
    "--all-targets",
    "--doc",
];

/// The libtest options that take a value.
const HARNESS_VALUE_FLAGS: &[&str] = &[
    "--skip",
    "--test-threads",
    "--format",
    "--color",
    "--logfile",
    "-Z",
    "--shuffle-seed",
];

/// What a section of the selection runs.
#[derive(Debug, PartialEq)]
enum Choice {
    Skip,
    All,
    Only(Vec<String>),
}

fn selection_path() -> PathBuf {
    let file = env::var_os("TEST_SELECTION").unwrap_or_else(|| OsString::from(SELECTION_FILE));
    Path::new(REPO_ROOT).join(file)
}

/// The selection file, `None` when there is none.
fn load() -> anyhow::Result<Option<Map<String, Value>>> {
    let path = selection_path();
    let Some(selection) = read_selection(&path)? else {
        return Ok(None);
    };
    let unseen = unseen_changes_of(&selection, &path)?;
    if !unseen.is_empty() {
        eprintln!(
            "test-selection: {} was made before these changes, so every test runs; run `make select-tests` again:",
            path.display()
        );
        for change in &unseen {
            eprintln!("  {change}");
        }
        return Ok(None);
    }
    eprintln!("test-selection: running only the tests {} selects", path.display());
    Ok(Some(selection))
}

/// The selection file as written, `None` when there is none.
fn read_selection(path: &Path) -> anyhow::Result<Option<Map<String, Value>>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let value: Value = serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    let Value::Object(selection) = value else {
        bail!("{}: not a JSON object", path.display());
    };
    if selection.get("version").and_then(Value::as_u64) != Some(FORMAT_VERSION) {
        bail!(
            "{}: version is not {FORMAT_VERSION}; run `make select-tests` again",
            path.display()
        );
    }
    Ok(Some(selection))
}

/// The changes from the selection's merge base that it was not made for.
fn unseen_changes_of(selection: &Map<String, Value>, path: &Path) -> anyhow::Result<Vec<String>> {
    let Some(merge_base) = selection.get("merge_base").and_then(Value::as_str) else {
        bail!("{}: no merge_base; run `make select-tests` again", path.display());
    };
    let selected = string_list(selection.get("changed_files").unwrap_or(&Value::Null), "changed_files")?;
    let current = changed_files(merge_base)?;
    Ok(unseen_changes(&current, &selected).into_iter().cloned().collect())
}

/// Why the selection file cannot be kept for HEAD, `None` when it can: it was
/// made at HEAD and covers every change there is now.
fn reuse_obstacle(path: &Path, head: &str) -> Option<String> {
    let selection = match read_selection(path) {
        Ok(Some(selection)) => selection,
        Ok(None) => return Some("there is none".into()),
        Err(e) => return Some(format!("{e:#}")),
    };
    match selection.get("head").and_then(Value::as_str) {
        Some(selected) if selected == head => {},
        Some(selected) => return Some(format!("it was made at {selected}")),
        None => return Some("it names no commit".into()),
    }
    match unseen_changes_of(&selection, path) {
        Ok(unseen) if unseen.is_empty() => None,
        Ok(unseen) => Some(format!("it was made before changes to {}", unseen.join(", "))),
        Err(e) => Some(format!("{e:#}")),
    }
}

/// The value of a dotted section (`python.unit`), `None` when it is missing.
fn section<'a>(selection: &'a Map<String, Value>, name: &str) -> Option<&'a Value> {
    let mut parts = name.split('.');
    let mut value = selection.get(parts.next()?)?;
    for part in parts {
        value = value.as_object()?.get(part)?;
    }
    Some(value)
}

fn choice(value: Option<&Value>, name: &str) -> anyhow::Result<Choice> {
    let Some(value) = value else {
        return Ok(Choice::Skip);
    };
    let entries = string_list(value, name)?;
    Ok(if entries.is_empty() {
        Choice::All
    } else {
        Choice::Only(entries)
    })
}

fn string_list(value: &Value, name: &str) -> anyhow::Result<Vec<String>> {
    let Value::Array(items) = value else {
        bail!("{name}: expected a list of strings, got {value}");
    };
    items
        .iter()
        .map(|item| match item.as_str() {
            Some(s) if s.trim().is_empty() => bail!("{name}: empty entry"),
            // An entry is passed as an argument, so it must not read as an option.
            Some(s) if s.starts_with('-') => bail!("{name}: entry {s:?} starts with '-'"),
            Some(s) => Ok(s.to_string()),
            None => bail!("{name}: expected a string, got {item}"),
        })
        .collect()
}

/// The changes from `merge_base`, working tree and untracked files included,
/// one `<status> <path>...` entry each. The selection file is not part of it.
fn changed_files(merge_base: &str) -> anyhow::Result<Vec<String>> {
    let selection_file = env::var("TEST_SELECTION").unwrap_or_else(|_| SELECTION_FILE.to_string());
    let mut changes: Vec<String> = git(&["diff", "--name-status", merge_base])?
        .lines()
        .map(|line| line.replace('\t', " "))
        .collect();
    changes.extend(
        git(&["ls-files", "--others", "--exclude-standard"])?
            .lines()
            .filter(|file| Path::new(file) != Path::new(&selection_file))
            .map(|file| format!("A {file}")),
    );
    Ok(changes)
}

/// The entries of `current` that the selection was not made for.
fn unseen_changes<'a>(current: &'a [String], selected: &[String]) -> Vec<&'a String> {
    let selected: BTreeSet<&String> = selected.iter().collect();
    current.iter().filter(|change| !selected.contains(change)).collect()
}

fn git(args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("git").arg("-C").arg(REPO_ROOT).args(args).output()?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim_end().to_string())
}

// ── Inventory ──────────────────────────────────────────────────────────────

/// The workspace packages and the tests the selection can name.
struct Inventory {
    /// Package name by manifest directory.
    packages: BTreeMap<PathBuf, String>,
    /// `rust` section keys, with a description for the agent.
    rust_units: BTreeMap<String, String>,
    /// The packages with doctests.
    doctest_packages: BTreeSet<String>,
    /// The ctest names of `c/CMakeLists.txt`.
    ctest_names: Vec<String>,
    /// The `python/` directory, which pytest entries are relative to.
    python_dir: PathBuf,
}

fn inventory() -> anyhow::Result<Inventory> {
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(&cargo)
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .stderr(Stdio::inherit())
        .output()?;
    if !output.status.success() {
        bail!("cargo metadata failed");
    }
    let metadata: Value = serde_json::from_slice(&output.stdout)?;
    let mut packages = BTreeMap::new();
    let mut rust_units = BTreeMap::new();
    let mut doctest_packages = BTreeSet::new();
    for package in metadata["packages"].as_array().into_iter().flatten() {
        let name = package["name"].as_str().unwrap_or_default().to_string();
        let manifest = PathBuf::from(package["manifest_path"].as_str().unwrap_or_default());
        let dir = manifest.parent().map(Path::to_path_buf).unwrap_or_default();
        let targets = package["targets"].as_array().into_iter().flatten();
        if targets
            .clone()
            .any(|t| target_kind(&t["kind"]) == "lib" && t["doctest"].as_bool() == Some(true))
        {
            doctest_packages.insert(name.clone());
        }
        if name != ROOT_PACKAGE {
            let relative = dir.strip_prefix(env::current_dir()?).unwrap_or(&dir).display().to_string();
            rust_units.insert(name.clone(), format!("every test of the `{name}` package (rust/{relative})"));
            packages.insert(dir, name);
            continue;
        }
        for target in targets {
            let target_name = target["name"].as_str().unwrap_or_default();
            let src = target["src_path"].as_str().unwrap_or_default();
            let src = Path::new(src)
                .strip_prefix(&dir)
                .unwrap_or(Path::new(src))
                .display()
                .to_string();
            let test = target["test"].as_bool().unwrap_or(false);
            match target_kind(&target["kind"]) {
                "lib" => {
                    rust_units.insert(
                        "lib".into(),
                        "the unit tests in rust/src, named by module path (`producer::kafka_producer::tests::<name>`)"
                            .into(),
                    );
                    if target["doctest"].as_bool().unwrap_or(false) {
                        rust_units.insert("doc".into(), "the doctests of rust/src".into());
                    }
                },
                "test" => {
                    let note = if test {
                        ""
                    } else {
                        "; `test = false`, run only by its own make target"
                    };
                    rust_units.insert(target_name.into(), format!("the integration tests of rust/{src}{note}"));
                },
                kind @ ("bin" | "example" | "bench") if test => {
                    rust_units.insert(format!("{kind}:{target_name}"), format!("the tests of rust/{src}"));
                },
                _ => {},
            }
        }
        packages.insert(dir, name);
    }

    let cmake = fs::read_to_string(Path::new(REPO_ROOT).join("c/CMakeLists.txt"))?;
    let ctest_names = cmake
        .lines()
        .filter_map(|line| line.trim().strip_prefix("add_test(NAME "))
        .filter_map(|rest| rest.split_whitespace().next())
        .map(str::to_string)
        .collect();

    Ok(Inventory {
        packages,
        rust_units,
        doctest_packages,
        ctest_names,
        python_dir: Path::new(REPO_ROOT).join("python"),
    })
}

/// The kind of a cargo target: `lib` for every library crate type.
fn target_kind(kinds: &Value) -> &'static str {
    let kinds: Vec<&str> = kinds.as_array().into_iter().flatten().filter_map(Value::as_str).collect();
    let has = |kind: &str| kinds.contains(&kind);
    if ["lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"]
        .iter()
        .any(|k| has(k))
    {
        "lib"
    } else if has("test") {
        "test"
    } else if has("bin") {
        "bin"
    } else if has("example") {
        "example"
    } else if has("bench") {
        "bench"
    } else {
        "other"
    }
}

/// The `rust` section key of a test unit.
fn unit_key(package: &str, kind: &str, name: &str) -> String {
    match (package, kind) {
        (ROOT_PACKAGE, "lib") => "lib".into(),
        (ROOT_PACKAGE, "doc") => "doc".into(),
        (ROOT_PACKAGE, "test") => name.into(),
        (ROOT_PACKAGE, _) => format!("{kind}:{name}"),
        _ => package.into(),
    }
}

// ── select-tests ───────────────────────────────────────────────────────────

/// `select-tests [--base <ref>] [--agent <command>] [--model <model>]`.
pub fn select(args: &[String]) -> anyhow::Result<()> {
    let mut base = DEFAULT_BASE.to_string();
    let mut agent = DEFAULT_AGENT.to_string();
    let mut model = DEFAULT_MODEL.to_string();
    let mut reuse = false;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let mut value = || args.next().cloned().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--base" => base = value()?,
            "--agent" => agent = value()?,
            "--model" => model = value()?,
            "--reuse" => reuse = true,
            other => bail!("select-tests: unknown argument {other:?}"),
        }
    }
    let agent: Vec<&str> = agent.split_whitespace().collect();
    let Some((program, agent_args)) = agent.split_first() else {
        bail!("select-tests: --agent is empty");
    };

    let path = selection_path();
    let head = git(&["rev-parse", "HEAD"])?;
    if reuse {
        match reuse_obstacle(&path, &head) {
            None => {
                eprintln!("select-tests: keeping {}, made at HEAD for the current changes", path.display());
                return Ok(());
            },
            Some(reason) => eprintln!("select-tests: selecting again, {} cannot be kept: {reason}", path.display()),
        }
    }

    // A failed run must not leave the previous selection in force.
    if path.exists() {
        fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
    }

    let merge_base = git(&["merge-base", &base, "HEAD"])?;
    let changes = changed_files(&merge_base)?;

    let mut selection = Map::new();
    selection.insert("version".into(), FORMAT_VERSION.into());
    selection.insert("base".into(), base.clone().into());
    selection.insert("merge_base".into(), merge_base.clone().into());
    selection.insert("head".into(), head.clone().into());
    selection.insert("changed_files".into(), changes.clone().into());

    if changes.is_empty() {
        eprintln!("select-tests: no change from {base}, so no test is selected");
    } else {
        let inventory = inventory()?;
        let prompt = prompt(&base, &merge_base, &head, &changes, &inventory);
        eprintln!(
            "select-tests: asking `{}` ({} changed files)...",
            agent.join(" "),
            changes.len()
        );
        let output = Command::new(program)
            .args(agent_args)
            .args(["--model", &model, "--allowedTools", AGENT_TOOLS, "-p", &prompt])
            .current_dir(REPO_ROOT)
            .stdin(Stdio::null())
            .stderr(Stdio::inherit())
            .output()
            .with_context(|| format!("running {program}"))?;
        let answer = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            bail!("select-tests: the agent failed ({}):\n{answer}", output.status);
        }
        let answer_json = extract_json(&answer).with_context(|| format!("the agent's answer:\n{answer}"))?;
        let chosen = validate(&answer_json, &inventory).with_context(|| format!("the agent's answer:\n{answer}"))?;
        selection.extend(chosen);
    }

    fs::write(&path, serde_json::to_string_pretty(&Value::Object(selection.clone()))? + "\n")
        .with_context(|| format!("writing {}", path.display()))?;
    eprintln!("select-tests: wrote {}", path.display());
    print_summary(&selection);
    Ok(())
}

fn prompt(base: &str, merge_base: &str, head: &str, changes: &[String], inventory: &Inventory) -> String {
    let joined = changes.join("\n");
    let changes = if joined.len() <= MAX_INLINE_CHANGES {
        format!("The changed files (git status letter, then the path):\n\n{joined}\n")
    } else {
        format!(
            "{} files changed, too many to list here: read them with `git diff --name-status {merge_base}` and `git \
             ls-files --others --exclude-standard`.\n",
            changes.len()
        )
    };
    let rust_units: String = inventory
        .rust_units
        .iter()
        .map(|(key, description)| format!("  - `{key}`: {description}\n"))
        .collect();
    let python_suites: String = PYTHON_SUITES
        .iter()
        .map(|(name, dir)| format!("  - `{name}`: pytest paths or node ids under python/{dir}\n"))
        .collect();
    let ctest_names = inventory.ctest_names.join(", ");
    format!(
        "You choose the tests to run for a change to this repository: a Kafka client in Rust (rust/) with C (c/) and \
         Python (python/) bindings; CLAUDE.md describes the layout. Read the change, the code it touches and the tests \
         that exercise that code, then answer with every test the change can affect and no other.

Rules:
- A test can be affected directly or through another language. The C tests run the Rust core through the C FFI \
         (rust/src/ffi). The Python tests run it through python/_confluentkafka.c and the FFI. The gRPC arms (c.integration, \
         python.grpc) run the Rust integration harness (rust/tests/integration) against the C and Python bindings.
- When in doubt, include the test. A change to build files (Cargo.toml, Cargo.lock, build.rs, Makefiles, CMakeLists.txt, \
         pyproject.toml, setup.py), the code generator, shared test support, or anything you cannot trace includes every \
         suite it can reach.
- Leave out a suite no change can affect. A change only to documentation or comments affects no test.
- Do not build or run anything: read files and use git.

The change is `git diff {merge_base}` run in the repository root, working-tree changes included, plus the untracked files: \
         base `{base}`, merge base {merge_base}, HEAD {head}.
{changes}
Answer with one JSON object in a ```json block, and nothing after it. Every member is optional. In each list, `[]` runs \
         the whole suite, a missing member runs nothing, and an entry runs only what it names; prefer whole files or modules \
         to single tests.

- \"rust\": an object mapping test units to lists of libtest name filters (a filter selects the tests whose full name \
         contains it, such as `producer::` or `common::record::default_record_batch::`). The units:
{rust_units}- \"c\": an object with
  - \"unit\": ctest names, among: {ctest_names}
  - \"integration\": filters of the `__grpc_c` tests of the `integration` unit (names such as `producer::<case>__grpc_c`)
- \"python\": an object with
  - \"typecheck\": `[]` to run mypy --strict over python/confluent_kafka
{python_suites}  - \"grpc\": filters of the `__grpc_python` tests of the `integration` unit
- \"reasons\": an object mapping each member you include (`rust.lib`, `c.unit`, `python.unit`, ...) to one sentence on why.
"
    )
}

/// The JSON object of the agent's answer: its last ```json block, or else the
/// text from its first `{` to its last `}`.
fn extract_json(answer: &str) -> anyhow::Result<Value> {
    let block = answer
        .rsplit_once("```json")
        .and_then(|(_, rest)| rest.split_once("```"))
        .map(|(block, _)| block);
    let text = match block {
        Some(block) => block,
        None => {
            let start = answer.find('{').context("no JSON object in the answer")?;
            let end = answer.rfind('}').context("no JSON object in the answer")?;
            answer.get(start..=end).context("no JSON object in the answer")?
        },
    };
    serde_json::from_str(text).context("the answer's JSON does not parse")
}

/// The sections of the agent's answer, checked against the inventory.
fn validate(answer: &Value, inventory: &Inventory) -> anyhow::Result<Map<String, Value>> {
    let Value::Object(answer) = answer else {
        bail!("the answer is not a JSON object");
    };
    let mut checked = Map::new();
    for (name, value) in answer {
        match name.as_str() {
            "rust" => {
                for (key, filters) in object(value, "rust")? {
                    if !inventory.rust_units.contains_key(key) {
                        bail!("rust: unknown unit {key:?}");
                    }
                    string_list(filters, &format!("rust.{key}"))?;
                }
            },
            "c" => {
                for (key, entries) in object(value, "c")? {
                    let entries = string_list(entries, &format!("c.{key}"))?;
                    match key.as_str() {
                        "unit" => {
                            if let Some(unknown) = entries.iter().find(|name| !inventory.ctest_names.contains(name)) {
                                bail!("c.unit: unknown ctest {unknown:?}");
                            }
                        },
                        "integration" => {},
                        other => bail!("c: unknown suite {other:?}"),
                    }
                }
            },
            "python" => {
                for (key, entries) in object(value, "python")? {
                    let entries = string_list(entries, &format!("python.{key}"))?;
                    match key.as_str() {
                        "typecheck" if entries.is_empty() => {},
                        "typecheck" => bail!("python.typecheck: expected [], got {entries:?}"),
                        "grpc" => {},
                        suite => check_python_entries(&inventory.python_dir, suite, &entries)?,
                    }
                }
            },
            "reasons" => {},
            other => bail!("unknown member {other:?}"),
        }
        checked.insert(name.clone(), value.clone());
    }
    Ok(checked)
}

fn object<'a>(value: &'a Value, name: &str) -> anyhow::Result<&'a Map<String, Value>> {
    value
        .as_object()
        .with_context(|| format!("{name}: expected an object, got {value}"))
}

/// Each entry of a pytest suite names a file or directory inside the suite.
fn check_python_entries(python_dir: &Path, suite: &str, entries: &[String]) -> anyhow::Result<()> {
    let Some((_, dir)) = PYTHON_SUITES.iter().find(|(name, _)| *name == suite) else {
        bail!("python: unknown suite {suite:?}");
    };
    for entry in entries {
        let path = Path::new(entry.split("::").next().unwrap_or(entry));
        let inside = path.starts_with(dir) && path.components().all(|c| matches!(c, std::path::Component::Normal(_)));
        if !inside {
            bail!("python.{suite}: {entry:?} is not under python/{dir}");
        }
        if !python_dir.join(path).exists() {
            bail!("python.{suite}: python/{} does not exist", path.display());
        }
    }
    Ok(())
}

fn print_summary(selection: &Map<String, Value>) {
    let mut lines = Vec::new();
    for language in ["rust", "c", "python"] {
        let Some(Value::Object(sections)) = selection.get(language) else {
            continue;
        };
        for (name, entries) in sections {
            let entries = entries.as_array().map(Vec::len).unwrap_or_default();
            let what = if entries == 0 {
                "all".to_string()
            } else {
                format!("{entries} entries")
            };
            lines.push(format!("  {language}.{name}: {what}"));
        }
    }
    if lines.is_empty() {
        eprintln!("select-tests: no test selected");
    } else {
        eprintln!(
            "select-tests: selected (a suite not listed does not run):\n{}",
            lines.join("\n")
        );
    }
}

// ── selected-cargo-test ────────────────────────────────────────────────────

/// One test executable `cargo test` would run.
struct Unit {
    key: String,
    executable: PathBuf,
    package_dir: PathBuf,
    package: String,
    order: (bool, u8, String),
}

/// `selected-cargo-test --section <section> -- <cargo test arguments>`: `cargo
/// test` restricted to the selection's `section`, which is either the `rust`
/// object of units or a list of filters applying to every unit.
pub fn cargo_test(args: &[String]) -> anyhow::Result<()> {
    let (options, cargo_args) = split_at_separator(args);
    let mut section_name = None;
    let mut options = options.iter();
    while let Some(option) = options.next() {
        match option.as_str() {
            "--section" => section_name = options.next().cloned(),
            other => bail!("selected-cargo-test: unknown argument {other:?}"),
        }
    }
    let section_name = section_name.context("selected-cargo-test: --section is required")?;
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());

    let Some(selection) = load()? else {
        return run(Command::new(&cargo).arg("test").args(cargo_args));
    };
    let Some(selected) = section(&selection, &section_name) else {
        eprintln!(
            "test-selection: skipping `cargo test {}`: {section_name} is not selected",
            cargo_args.join(" ")
        );
        return Ok(());
    };

    let (cargo_options, harness) = split_at_separator(cargo_args);
    let has = |flag: &str| cargo_options.iter().any(|o| o == flag || o.starts_with(&format!("{flag}=")));
    let doc_only = has("--doc");
    let runs_doc = doc_only || !TARGET_FLAGS.iter().any(|flag| has(flag));
    let fail_fast = !has("--no-fail-fast");
    let inventory = inventory()?;

    let mut failed = Vec::new();
    let mut report = |key: &str, ok: bool| -> anyhow::Result<()> {
        if !ok {
            if fail_fast {
                bail!("test-selection: {key} failed");
            }
            failed.push(key.to_string());
        }
        Ok(())
    };

    if !doc_only {
        let (units, library_paths) = build_units(&cargo, cargo_options, &inventory)?;
        for unit in &units {
            match unit_choice(selected, &section_name, &unit.key)? {
                Choice::Skip => {
                    eprintln!(
                        "test-selection: skipping {} ({}): not selected",
                        unit.key,
                        unit.executable.display()
                    )
                },
                choice => report(&unit.key, run_unit(unit, harness, &choice, &library_paths)?)?,
            }
        }
    }

    if runs_doc {
        let packages = invocation_packages(cargo_options, &inventory);
        let choices = packages
            .iter()
            .filter(|package| inventory.doctest_packages.contains(*package))
            .map(|package| unit_choice(selected, &section_name, &unit_key(package, "doc", "")))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let filters = merge_choices(choices);
        match filters {
            Choice::Skip => eprintln!("test-selection: skipping the doctests (not selected)"),
            filters => {
                let mut command = Command::new(&cargo);
                command.arg("test").args(cargo_options);
                if !doc_only {
                    command.arg("--doc");
                }
                command.arg("--").args(harness);
                // Doctests run through cargo, so they cannot be listed and
                // intersected with filters the invocation already gives:
                // those alone select the superset.
                if let Choice::Only(filters) = filters {
                    if split_harness(harness).0.is_empty() {
                        command.args(filters);
                    }
                }
                report("doc", run(&mut command).is_ok())?;
            },
        }
    }

    if !failed.is_empty() {
        bail!("test-selection: failed: {}", failed.join(", "));
    }
    Ok(())
}

fn split_at_separator(args: &[String]) -> (&[String], &[String]) {
    match args.iter().position(|arg| arg == "--") {
        Some(at) => (&args[..at], &args[at + 1..]),
        None => (args, &[]),
    }
}

fn unit_choice(selected: &Value, section_name: &str, key: &str) -> anyhow::Result<Choice> {
    match selected {
        Value::Object(units) => choice(units.get(key), &format!("{section_name}.{key}")),
        list => choice(Some(list), section_name),
    }
}

/// One choice for units run together: everything if one runs everything, the
/// union of the filters otherwise.
fn merge_choices(choices: Vec<Choice>) -> Choice {
    let mut filters = BTreeSet::new();
    let mut any = false;
    for choice in choices {
        match choice {
            Choice::Skip => {},
            Choice::All => return Choice::All,
            Choice::Only(only) => {
                any = true;
                filters.extend(only);
            },
        }
    }
    if any {
        Choice::Only(filters.into_iter().collect())
    } else {
        Choice::Skip
    }
}

/// The packages a cargo invocation covers.
fn invocation_packages(cargo_options: &[String], inventory: &Inventory) -> Vec<String> {
    if cargo_options.iter().any(|o| o == "--workspace" || o == "--all") {
        return inventory.packages.values().cloned().collect();
    }
    let mut packages = Vec::new();
    let mut options = cargo_options.iter();
    while let Some(option) = options.next() {
        if option == "-p" || option == "--package" {
            packages.extend(options.next().cloned());
        } else if let Some(package) = option.strip_prefix("--package=") {
            packages.push(package.to_string());
        }
    }
    if packages.is_empty() {
        packages.push(ROOT_PACKAGE.to_string());
    }
    packages
}

/// Builds what `cargo test <cargo_options>` builds, and returns the test
/// executables it would run, in cargo's order, with the native library
/// directories the build scripts reported.
fn build_units(
    cargo: &str,
    cargo_options: &[String],
    inventory: &Inventory,
) -> anyhow::Result<(Vec<Unit>, Vec<PathBuf>)> {
    let mut child = Command::new(cargo)
        .arg("test")
        .args(cargo_options)
        .args(["--no-run", "--message-format=json-render-diagnostics"])
        .stdout(Stdio::piped())
        .spawn()?;
    let mut units: Vec<Unit> = Vec::new();
    let mut library_paths = Vec::new();
    for line in BufReader::new(child.stdout.take().context("cargo stdout")?).lines() {
        let Ok(message) = serde_json::from_str::<Value>(&line?) else {
            continue;
        };
        match message["reason"].as_str() {
            Some("compiler-artifact") if message["profile"]["test"].as_bool() == Some(true) => {
                let Some(executable) = message["executable"].as_str() else {
                    continue;
                };
                let manifest = Path::new(message["manifest_path"].as_str().unwrap_or_default());
                let package_dir = manifest.parent().map(Path::to_path_buf).unwrap_or_default();
                let package = inventory.packages.get(&package_dir).cloned().unwrap_or_default();
                let kind = target_kind(&message["target"]["kind"]);
                let name = message["target"]["name"].as_str().unwrap_or_default().to_string();
                let kind_order = ["lib", "bin", "test", "example", "bench"]
                    .iter()
                    .position(|k| *k == kind)
                    .unwrap_or(5);
                let executable = PathBuf::from(executable);
                if units.iter().any(|unit| unit.executable == executable) {
                    continue;
                }
                units.push(Unit {
                    key: unit_key(&package, kind, &name),
                    order: (package != ROOT_PACKAGE, kind_order as u8, format!("{package}/{name}")),
                    executable,
                    package_dir,
                    package,
                });
            },
            Some("build-script-executed") => {
                for path in message["linked_paths"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                {
                    // `native=/dir`, `dependency=/dir`, ... or a bare `/dir`.
                    let path = path.split_once('=').map(|(_, dir)| dir).unwrap_or(path);
                    library_paths.push(PathBuf::from(path));
                }
            },
            _ => {},
        }
    }
    if !child.wait()?.success() {
        bail!("`cargo test {} --no-run` failed", cargo_options.join(" "));
    }
    units.sort_by(|a, b| a.order.cmp(&b.order));
    Ok((units, library_paths))
}

/// The positional filters of libtest arguments, and the arguments without
/// them, without `--skip` and without `--exact`.
fn split_harness(harness: &[String]) -> (Vec<String>, Vec<String>) {
    let mut filters = Vec::new();
    let mut rest = Vec::new();
    let mut args = harness.iter();
    while let Some(arg) = args.next() {
        if !arg.starts_with('-') {
            filters.push(arg.clone());
        } else if arg == "--skip" {
            args.next();
        } else if arg.starts_with("--skip=") || arg == "--exact" {
        } else {
            rest.push(arg.clone());
            if HARNESS_VALUE_FLAGS.contains(&arg.as_str()) {
                rest.extend(args.next().cloned());
            }
        }
    }
    (filters, rest)
}

/// Runs one test executable as `cargo test` does, with only the selected tests.
fn run_unit(unit: &Unit, harness: &[String], choice: &Choice, library_paths: &[PathBuf]) -> anyhow::Result<bool> {
    let args: Vec<String> = match choice {
        Choice::Skip => return Ok(true),
        Choice::All => harness.to_vec(),
        Choice::Only(filters) => {
            let (given, rest) = split_harness(harness);
            if given.is_empty() {
                harness.iter().chain(filters).cloned().collect()
            } else {
                // libtest ORs its filters, so the intersection of the
                // invocation's filters and the selection's is listed first.
                let names: Vec<String> = list_tests(unit, harness, library_paths)?
                    .into_iter()
                    .filter(|name| filters.iter().any(|filter| name.contains(filter.as_str())))
                    .collect();
                if names.is_empty() {
                    eprintln!(
                        "test-selection: skipping {}: no selected test matches {}",
                        unit.key,
                        given.join(" ")
                    );
                    return Ok(true);
                }
                rest.into_iter().chain(["--exact".to_string()]).chain(names).collect()
            }
        },
    };
    eprintln!("     Running {} ({})", unit.key, unit.executable.display());
    Ok(unit_command(unit, library_paths).args(&args).status()?.success())
}

/// The names of the tests the executable runs with these libtest arguments.
fn list_tests(unit: &Unit, harness: &[String], library_paths: &[PathBuf]) -> anyhow::Result<Vec<String>> {
    let mut args = Vec::new();
    let mut harness = harness.iter();
    while let Some(arg) = harness.next() {
        if arg == "--format" {
            harness.next();
        } else if !arg.starts_with("--format=") {
            args.push(arg.clone());
        }
    }
    let output = unit_command(unit, library_paths)
        .args(["--list", "--format", "terse"])
        .args(&args)
        .stderr(Stdio::inherit())
        .output()?;
    if !output.status.success() {
        bail!("listing the tests of {} failed", unit.executable.display());
    }
    Ok(String::from_utf8(output.stdout)?
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .map(str::to_string)
        .collect())
}

/// The executable's command in the environment `cargo test` gives it: run from
/// its package directory, with the build's library directories searched.
fn unit_command(unit: &Unit, library_paths: &[PathBuf]) -> Command {
    let variable = if cfg!(target_os = "macos") {
        "DYLD_FALLBACK_LIBRARY_PATH"
    } else {
        "LD_LIBRARY_PATH"
    };
    let deps = unit.executable.parent().map(Path::to_path_buf).unwrap_or_default();
    let profile = deps.parent().map(Path::to_path_buf).unwrap_or_default();
    let mut paths: Vec<PathBuf> = library_paths.iter().cloned().chain([deps, profile]).collect();
    paths.extend(env::var_os(variable).iter().flat_map(env::split_paths));
    let mut command = Command::new(&unit.executable);
    command
        .current_dir(&unit.package_dir)
        .env("CARGO_MANIFEST_DIR", &unit.package_dir)
        .env("CARGO_PKG_NAME", &unit.package)
        .env(variable, env::join_paths(paths).unwrap_or_default());
    command
}

fn run(command: &mut Command) -> anyhow::Result<()> {
    let status = command.status().with_context(|| format!("running {command:?}"))?;
    if !status.success() {
        bail!("{command:?} failed ({status})");
    }
    Ok(())
}

// ── selected-run ───────────────────────────────────────────────────────────

/// `selected-run --section <section> [--default <arg>]... [--ctest] [-C <dir>]
/// -- <command>`: runs the command in `dir` when the selection selects the
/// section (or there is no selection). An argument `{}` stands for the
/// selected entries, or the `--default` arguments when the whole suite is
/// selected; with `--ctest` it stands for `-R <regex>` matching the selected
/// names, or for nothing.
pub fn selected_run(args: &[String]) -> anyhow::Result<()> {
    let (options, command) = split_at_separator(args);
    let mut section_name = None;
    let mut defaults = Vec::new();
    let mut ctest = false;
    let mut dir = PathBuf::from(".");
    let mut options = options.iter();
    while let Some(option) = options.next() {
        let mut value = || options.next().cloned().with_context(|| format!("{option} needs a value"));
        match option.as_str() {
            "--section" => section_name = Some(value()?),
            "--default" => defaults.push(value()?),
            "--ctest" => ctest = true,
            // The last `-C` wins, so a Makefile can append its own.
            "-C" => dir = PathBuf::from(value()?),
            other => bail!("selected-run: unknown argument {other:?}"),
        }
    }
    let section_name = section_name.context("selected-run: --section is required")?;
    let Some((program, command_args)) = command.split_first() else {
        bail!("selected-run: no command after --");
    };

    let choice = match load()? {
        None => Choice::All,
        Some(selection) => choice(section(&selection, &section_name), &section_name)?,
    };
    let selected: Vec<String> = match choice {
        Choice::Skip => {
            eprintln!(
                "test-selection: skipping `{}`: {section_name} is not selected",
                command.join(" ")
            );
            return Ok(());
        },
        Choice::All if ctest => Vec::new(),
        Choice::All => defaults,
        Choice::Only(names) if ctest => vec!["-R".into(), ctest_regex(&names)],
        Choice::Only(entries) => entries,
    };
    let mut argv = Vec::new();
    for arg in command_args {
        if arg == "{}" {
            argv.extend(selected.iter().cloned());
        } else {
            argv.push(arg.clone());
        }
    }
    run(Command::new(program).args(argv).current_dir(dir))
}

/// A ctest `-R` regex matching exactly these test names.
fn ctest_regex(names: &[String]) -> String {
    let escape = |name: &String| {
        name.chars()
            .flat_map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    vec![c]
                } else {
                    vec!['\\', c]
                }
            })
            .collect::<String>()
    };
    format!("^({})$", names.iter().map(escape).collect::<Vec<_>>().join("|"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_choice() {
        assert_eq!(choice(None, "s").unwrap(), Choice::Skip);
        assert_eq!(choice(Some(&json!([])), "s").unwrap(), Choice::All);
        assert_eq!(
            choice(Some(&json!(["a", "b"])), "s").unwrap(),
            Choice::Only(strings(&["a", "b"]))
        );
        assert_eq!(
            choice(Some(&json!(["--skip"])), "s").unwrap_err().to_string(),
            "s: entry \"--skip\" starts with '-'"
        );
        assert!(choice(Some(&json!({})), "s").is_err());
    }

    #[test]
    fn test_section() {
        let selection = json!({"python": {"unit": ["test/unit/x.py"]}});
        let selection = selection.as_object().unwrap();
        assert_eq!(section(selection, "python.unit"), Some(&json!(["test/unit/x.py"])));
        assert_eq!(section(selection, "python.static"), None);
        assert_eq!(section(selection, "c.unit"), None);
    }

    #[test]
    fn test_unit_choice_object_and_list() {
        let units = json!({"lib": ["producer::"], "admin": []});
        assert_eq!(
            unit_choice(&units, "rust", "lib").unwrap(),
            Choice::Only(strings(&["producer::"]))
        );
        assert_eq!(unit_choice(&units, "rust", "admin").unwrap(), Choice::All);
        assert_eq!(unit_choice(&units, "rust", "consumer").unwrap(), Choice::Skip);
        let list = json!(["producer::"]);
        assert_eq!(
            unit_choice(&list, "c.integration", "integration").unwrap(),
            Choice::Only(strings(&["producer::"]))
        );
    }

    #[test]
    fn test_merge_choices() {
        assert_eq!(merge_choices(vec![Choice::Skip, Choice::Skip]), Choice::Skip);
        assert_eq!(merge_choices(vec![Choice::Only(strings(&["a"])), Choice::All]), Choice::All);
        assert_eq!(
            merge_choices(vec![
                Choice::Only(strings(&["b"])),
                Choice::Skip,
                Choice::Only(strings(&["a", "b"]))
            ]),
            Choice::Only(strings(&["a", "b"]))
        );
    }

    #[test]
    fn test_split_harness() {
        let (filters, rest) = split_harness(&strings(&[
            "--skip",
            "__grpc",
            "integration_tests::",
            "--test-threads",
            "2",
            "--exact",
            "--nocapture",
        ]));
        assert_eq!(filters, strings(&["integration_tests::"]));
        assert_eq!(rest, strings(&["--test-threads", "2", "--nocapture"]));
    }

    #[test]
    fn test_unit_key() {
        assert_eq!(unit_key(ROOT_PACKAGE, "lib", "confluent_kafka"), "lib");
        assert_eq!(unit_key(ROOT_PACKAGE, "doc", ""), "doc");
        assert_eq!(unit_key(ROOT_PACKAGE, "test", "admin"), "admin");
        assert_eq!(unit_key(ROOT_PACKAGE, "example", "x"), "example:x");
        assert_eq!(unit_key("generator", "bin", "message_generator"), "generator");
    }

    #[test]
    fn test_target_kind() {
        assert_eq!(target_kind(&json!(["lib", "staticlib", "cdylib"])), "lib");
        assert_eq!(target_kind(&json!(["proc-macro"])), "lib");
        assert_eq!(target_kind(&json!(["test"])), "test");
        assert_eq!(target_kind(&json!(["bin"])), "bin");
    }

    #[test]
    fn test_unseen_changes() {
        let selected = strings(&["M rust/src/lib.rs", "A c/new.c"]);
        assert!(unseen_changes(&strings(&["M rust/src/lib.rs"]), &selected).is_empty());
        assert!(unseen_changes(&selected, &selected).is_empty());
        let current = strings(&["M rust/src/lib.rs", "M python/x.py", "D c/new.c"]);
        assert_eq!(unseen_changes(&current, &selected), vec!["M python/x.py", "D c/new.c"]);
    }

    #[test]
    fn test_extract_json() {
        let answer = "Looking...\n```json\n{\"rust\": {}}\n```\nDone.";
        assert_eq!(extract_json(answer).unwrap(), json!({"rust": {}}));
        assert_eq!(
            extract_json("The answer: {\"c\": {\"unit\": []}}").unwrap(),
            json!({"c": {"unit": []}})
        );
        assert!(extract_json("no object").is_err());
    }

    #[test]
    fn test_ctest_regex() {
        assert_eq!(
            ctest_regex(&strings(&["mock_producer", "kafka_admin"])),
            "^(mock_producer|kafka_admin)$"
        );
        assert_eq!(ctest_regex(&strings(&["a.b"])), "^(a\\.b)$");
    }

    fn inventory_fixture() -> Inventory {
        Inventory {
            packages: BTreeMap::new(),
            rust_units: [("lib", ""), ("admin", "")]
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            doctest_packages: BTreeSet::new(),
            ctest_names: strings(&["mock_producer"]),
            python_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("../../python"),
        }
    }

    #[test]
    fn test_validate() {
        let inventory = inventory_fixture();
        let answer = json!({
            "rust": {"lib": ["producer::"], "admin": []},
            "c": {"unit": ["mock_producer"], "integration": []},
            "python": {"typecheck": [], "unit": ["test/unit"], "grpc": ["producer::"]},
            "reasons": {"rust.lib": "the producer changed"},
        });
        assert_eq!(Value::Object(validate(&answer, &inventory).unwrap()), answer);

        let error = |answer: Value| validate(&answer, &inventory).unwrap_err().to_string();
        assert_eq!(error(json!({"rust": {"consumer": []}})), "rust: unknown unit \"consumer\"");
        assert_eq!(
            error(json!({"c": {"unit": ["kafka_admin"]}})),
            "c.unit: unknown ctest \"kafka_admin\""
        );
        assert_eq!(
            error(json!({"python": {"unit": ["test/static"]}})),
            "python.unit: \"test/static\" is not under python/test/unit"
        );
        assert_eq!(
            error(json!({"python": {"unit": ["test/unit/../static"]}})),
            "python.unit: \"test/unit/../static\" is not under python/test/unit"
        );
        assert_eq!(
            error(json!({"python": {"unit": ["test/unit/no_such_test.py::t"]}})),
            "python.unit: python/test/unit/no_such_test.py does not exist"
        );
        assert_eq!(
            error(json!({"python": {"typecheck": ["x"]}})),
            "python.typecheck: expected [], got [\"x\"]"
        );
        assert_eq!(error(json!({"java": {}})), "unknown member \"java\"");
    }
}
