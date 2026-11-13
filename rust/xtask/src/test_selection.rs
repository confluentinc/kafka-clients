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
//! the file `TEST_SELECTION` names, relative to the root), to be committed with
//! the branch. `selected-cargo-test` and `selected-run`, which the Makefiles'
//! test recipes go through, apply a selection only when `TEST_SELECTION` names
//! one: CI sets it on a pull request build and `make verify-commit` on the
//! pre-commit hook's own file. Without it they run everything.
//!
//! The file holds one section per suite. A section that is missing runs
//! nothing, `[]` runs the whole suite, and a list runs only what it names:
//!
//! - `rust`: test unit (`lib`, `doc`, an integration test target, another
//!   workspace package) -> libtest name filters;
//! - `c.unit`: ctest names, or `<name>:<function>` for one test function of a
//!   program (`c/tests/test_filter.h`); `c.integration`: filters of the
//!   `__grpc_c` tests;
//! - `python.<suite>`: pytest paths or node ids, relative to `python/`;
//!   `python.typecheck`: `[]` runs mypy; `python.grpc`: filters of the
//!   `__grpc_python` tests;
//! - `dotnet.<suite>`: fully qualified name prefixes of .NET tests (a namespace
//!   or a class), run as a `dotnet test --filter`; `dotnet.grpc`: filters of
//!   the `__grpc_dotnet` tests.
//!
//! The file records the change it was made for: `merge_base`,
//! `changed_files`, and `tree`, the git tree of the working tree with the
//! selection files left out. The selection applies only while the working tree
//! is still that tree; after any edit every test runs again, so a commit is
//! never checked against a selection made for other content.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _};
use serde_json::{Map, Value};

use crate::REPO_ROOT;

const SELECTION_FILE: &str = "test-selection.json";
const FORMAT_VERSION: u64 = 2;
const ROOT_PACKAGE: &str = "confluent-kafka";

const DEFAULT_BASE: &str = "origin/master";
const DEFAULT_AGENT: &str = "claude";
const DEFAULT_MODEL: &str = "opus";
/// Claude Code's `--tools`: the only built-in tools the agent has. With
/// `--restricted` the user, project and local settings files are ignored, so
/// their allow rules and default permission mode do not apply.
const AGENT_BUILTIN_TOOLS: &str = "Read,Grep,Glob,Bash";
/// Claude Code's `--allowedTools`: the agent reads the repository and the
/// history, and changes nothing.
const AGENT_TOOLS: &str = "Read,Grep,Glob,Bash(git diff:*),Bash(git log:*),Bash(git show:*),\
                           Bash(git status:*),Bash(git ls-files:*),Bash(ls:*)";
/// Claude Code's `--disallowedTools`: `git diff`, `git log` and `git show`
/// write a file with `--output`, which `AGENT_TOOLS` would otherwise allow.
const AGENT_DENIED_TOOLS: &str = "Bash(git * --output*)";
/// How long the agent may take by default before `select-tests` gives up, so
/// a stalled call cannot hang a commit.
const DEFAULT_AGENT_TIMEOUT_SECS: u64 = 600;
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

/// The `dotnet test` suites of `dotnet/`: section name, what it runs, and the
/// namespaces its tests live in.
const DOTNET_SUITES: &[(&str, &str, &[&str])] = &[
    (
        "solution",
        "the tests of Confluent.Kafka.sln: dotnet/tests/Confluent.Kafka.UnitTests (the mock clients, no broker) and \
         dotnet/tests/Confluent.Kafka.IntegrationTests (a Testcontainers broker)",
        &["Confluent.Kafka.UnitTests", "Confluent.Kafka.IntegrationTests"],
    ),
    ("soak", "dotnet/soak/SoakClient.Tests", &["Confluent.Kafka.Soak.Tests"]),
    (
        "performance",
        "dotnet/tests/Performance/Confluent.Kafka.PerformanceTests",
        &["Confluent.Kafka.Performance.Tests"],
    ),
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

/// The file `select-tests` writes: `$TEST_SELECTION`, by default
/// `test-selection.json`.
fn output_path() -> PathBuf {
    let file = env::var_os("TEST_SELECTION").unwrap_or_else(|| OsString::from(SELECTION_FILE));
    Path::new(REPO_ROOT).join(file)
}

/// The selection to apply, `None` when there is none: `TEST_SELECTION` is not
/// set, the file is missing, or it was made for another tree.
fn load() -> anyhow::Result<Option<Map<String, Value>>> {
    let Some(file) = env::var_os("TEST_SELECTION") else {
        return Ok(None);
    };
    let path = Path::new(REPO_ROOT).join(file);
    let Some(selection) = read_selection(&path)? else {
        eprintln!("test-selection: there is no {}, so every test runs", path.display());
        return Ok(None);
    };
    if let Some(reason) = mismatch(&selection)? {
        eprintln!(
            "test-selection: {} does not apply ({reason}), so every test runs; run `make select-tests` again",
            path.display()
        );
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
    Ok(Some(selection))
}

/// Why the selection does not apply to the working tree, `None` when it does:
/// it has this format and was made for the current tree.
fn mismatch(selection: &Map<String, Value>) -> anyhow::Result<Option<String>> {
    let version = selection.get("version").and_then(Value::as_u64);
    if version != Some(FORMAT_VERSION) {
        return Ok(Some(format!("its version is not {FORMAT_VERSION}")));
    }
    let Some(selected) = selection.get("tree").and_then(Value::as_str) else {
        return Ok(Some("it names no tree".into()));
    };
    let current = working_tree(Path::new(REPO_ROOT), &selection_files())?;
    Ok((selected != current).then(|| format!("it was made for tree {selected}, the working tree is {current}")))
}

/// Why the selection file cannot be kept for HEAD, `None` when it can: it was
/// made at HEAD for the current working tree.
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
    match mismatch(&selection) {
        Ok(reason) => reason,
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

/// The selection files, relative to the repository root: the committed
/// `test-selection.json` and the one `TEST_SELECTION` names. Neither is ever
/// part of a change.
fn selection_files() -> Vec<String> {
    let mut files = vec![SELECTION_FILE.to_string()];
    if let Ok(file) = env::var("TEST_SELECTION") {
        if !files.contains(&file) {
            files.push(file);
        }
    }
    files
}

/// Whether a `<status> <path>...` change touches only selection files.
fn is_selection_change(change: &str, selection_files: &[String]) -> bool {
    let mut paths = change.split(' ').skip(1).peekable();
    paths.peek().is_some() && paths.all(|path| selection_files.iter().any(|file| Path::new(path) == Path::new(file)))
}

/// The changes from `merge_base`, working tree and untracked files included,
/// one `<status> <path>...` entry each. The selection files are not part of
/// it.
fn changed_files(merge_base: &str) -> anyhow::Result<Vec<String>> {
    let selection_files = selection_files();
    let mut changes: Vec<String> = git(&["diff", "--name-status", merge_base])?
        .lines()
        .map(|line| line.replace('\t', " "))
        .collect();
    changes.extend(
        git(&["ls-files", "--others", "--exclude-standard"])?
            .lines()
            .map(|file| format!("A {file}")),
    );
    changes.retain(|change| !is_selection_change(change, &selection_files));
    Ok(changes)
}

/// The id of the git tree holding the working tree of `repo`, untracked files
/// included and ignored ones not, without the `excluded` paths. It is built
/// in a copy of the index, which keeps the index's file stat cache, so the
/// real index is left as it is.
fn working_tree(repo: &Path, excluded: &[String]) -> anyhow::Result<String> {
    // Absolute, because git reads GIT_INDEX_FILE relative to `-C repo`. Not
    // `fs::canonicalize`: on Windows it returns a verbatim `\\?\C:\...` path,
    // which git cannot create its index lock under.
    let repo = &std::path::absolute(repo).with_context(|| format!("resolving {}", repo.display()))?;
    let index = PathBuf::from(git_in(repo, &[], &["rev-parse", "--git-path", "index"])?);
    let index = if index.is_absolute() { index } else { repo.join(index) };
    let mut scratch = index.clone().into_os_string();
    scratch.push(format!(".test-selection-{}", std::process::id()));
    let scratch = PathBuf::from(scratch);
    if index.exists() {
        fs::copy(&index, &scratch).with_context(|| format!("copying {}", index.display()))?;
    }
    let envs = [("GIT_INDEX_FILE", scratch.as_os_str())];
    let tree = (|| {
        git_in(repo, &envs, &["add", "-A"])?;
        let mut rm = vec!["rm", "--cached", "-q", "--ignore-unmatch", "--"];
        rm.extend(excluded.iter().map(String::as_str));
        git_in(repo, &envs, &rm)?;
        git_in(repo, &envs, &["write-tree"])
    })();
    let _ = fs::remove_file(&scratch);
    tree
}

fn git(args: &[&str]) -> anyhow::Result<String> {
    git_in(Path::new(REPO_ROOT), &[], args)
}

fn git_in(repo: &Path, envs: &[(&str, &std::ffi::OsStr)], args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .envs(envs.iter().copied())
        .output()?;
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
    /// The ctest names of `c/CMakeLists.txt`, each with the test functions its
    /// program passes to `RUN_TEST`.
    ctests: BTreeMap<String, BTreeSet<String>>,
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

    let c_dir = Path::new(REPO_ROOT).join("c");
    let ctests = ctests(&fs::read_to_string(c_dir.join("CMakeLists.txt"))?, |source| {
        fs::read_to_string(c_dir.join(source)).with_context(|| format!("reading c/{source}"))
    })?;

    Ok(Inventory {
        packages,
        rust_units,
        doctest_packages,
        ctests,
        python_dir: Path::new(REPO_ROOT).join("python"),
    })
}

/// The ctests of a `CMakeLists.txt`, each with the functions its program's
/// source, read by `read`, passes to `RUN_TEST`. `c/tests/test_filter.h` names
/// a program after its source file, so a ctest `<name>` must run the program
/// built from `test_<name>.c`.
fn ctests(
    cmake: &str,
    read: impl Fn(&str) -> anyhow::Result<String>,
) -> anyhow::Result<BTreeMap<String, BTreeSet<String>>> {
    // `add_executable(<exe> <source>)` and `add_test(NAME <name> COMMAND <exe>)`.
    let call = |line: &str, command: &str| {
        let rest = line.trim().strip_prefix(command)?.strip_prefix('(')?;
        let words: Vec<String> = rest.split(')').next()?.split_whitespace().map(str::to_string).collect();
        Some(words)
    };
    let sources: BTreeMap<String, String> = cmake
        .lines()
        .filter_map(|line| call(line, "add_executable"))
        .filter_map(|words| Some((words.first()?.clone(), words.get(1)?.clone())))
        .collect();
    let mut ctests = BTreeMap::new();
    for words in cmake.lines().filter_map(|line| call(line, "add_test")) {
        let (Some("NAME"), Some(name), Some("COMMAND"), Some(exe)) = (
            words.first().map(String::as_str),
            words.get(1),
            words.get(2).map(String::as_str),
            words.get(3),
        ) else {
            continue;
        };
        let source = sources
            .get(exe)
            .with_context(|| format!("ctest {name:?}: no add_executable for {exe:?}"))?;
        let file_name = Path::new(source).file_name().and_then(|n| n.to_str()).unwrap_or_default();
        if file_name != format!("test_{name}.c") {
            bail!("ctest {name:?} runs {source}, but its source must be test_{name}.c for C_TEST_FILTER");
        }
        let functions = read(source)?
            .lines()
            .filter_map(|line| line.trim().strip_prefix("RUN_TEST("))
            .filter_map(|rest| {
                let function: String = rest
                    .trim_start()
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                (!function.is_empty()).then_some(function)
            })
            .collect();
        ctests.insert(name.clone(), functions);
    }
    Ok(ctests)
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

/// `select-tests [--base <ref>] [--agent <command>] [--model <model>]
/// [--timeout <seconds>] [--reuse]`.
pub fn select(args: &[String]) -> anyhow::Result<()> {
    let mut base = DEFAULT_BASE.to_string();
    let mut agent = DEFAULT_AGENT.to_string();
    let mut model = DEFAULT_MODEL.to_string();
    let mut timeout = Duration::from_secs(DEFAULT_AGENT_TIMEOUT_SECS);
    let mut reuse = false;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let mut value = || args.next().cloned().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--base" => base = value()?,
            "--agent" => agent = value()?,
            "--model" => model = value()?,
            "--timeout" => {
                let secs = value()?;
                let secs: u64 = secs
                    .parse()
                    .with_context(|| format!("--timeout: {secs:?} is not a number of seconds"))?;
                timeout = Duration::from_secs(secs);
            },
            "--reuse" => reuse = true,
            other => bail!("select-tests: unknown argument {other:?}"),
        }
    }
    let agent: Vec<&str> = agent.split_whitespace().collect();
    let Some((program, agent_args)) = agent.split_first() else {
        bail!("select-tests: --agent is empty");
    };

    let path = output_path();
    let head = git(&["rev-parse", "HEAD"])?;
    if reuse {
        match reuse_obstacle(&path, &head) {
            None => {
                eprintln!(
                    "select-tests: keeping {}, made at HEAD for the current working tree",
                    path.display()
                );
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
    let tree = working_tree(Path::new(REPO_ROOT), &selection_files())?;

    let mut selection = Map::new();
    selection.insert("version".into(), FORMAT_VERSION.into());
    selection.insert("base".into(), base.clone().into());
    selection.insert("merge_base".into(), merge_base.clone().into());
    selection.insert("head".into(), head.clone().into());
    selection.insert("tree".into(), tree.into());
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
        let mut command = Command::new(program);
        command
            .args(agent_args)
            .args(["--model", &model])
            .args(["--restricted", "--strict-mcp-config", "--tools", AGENT_BUILTIN_TOOLS])
            .args(["--allowedTools", AGENT_TOOLS, "--disallowedTools", AGENT_DENIED_TOOLS])
            .args(["-p", &prompt])
            .current_dir(REPO_ROOT);
        let (status, answer) = run_with_deadline(&mut command, timeout)?;
        if !status.success() {
            bail!("select-tests: the agent failed ({status}):\n{answer}");
        }
        let answer_json = extract_json(&answer).with_context(|| format!("the agent's answer:\n{answer}"))?;
        let chosen = validate(&answer_json, &inventory).with_context(|| format!("the agent's answer:\n{answer}"))?;
        selection.extend(chosen);
    }

    fs::write(&path, serde_json::to_string_pretty(&Value::Object(selection.clone()))? + "\n")
        .with_context(|| format!("writing {}", path.display()))?;
    eprintln!("select-tests: wrote {}", path.display());
    print_summary("select-tests", &selection);
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
    let ctest_names = inventory.ctests.keys().cloned().collect::<Vec<_>>().join(", ");
    let dotnet_suites: String = DOTNET_SUITES
        .iter()
        .map(|(name, what, namespaces)| {
            let namespaces = namespaces.iter().map(|n| format!("`{n}`")).collect::<Vec<_>>().join(" or ");
            format!("  - `{name}`: {what}; each entry starts with {namespaces}\n")
        })
        .collect();
    format!(
        "You choose the tests to run for a change to this repository: a Kafka client in Rust (rust/) with C (c/), \
         Python (python/) and .NET (dotnet/) bindings; CLAUDE.md describes the layout. Read the change, the code it touches and the tests \
         that exercise that code, then answer with every test the change can affect and no other.

Rules:
- A test can be affected directly or through another language. The C tests run the Rust core through the C FFI \
         (rust/src/ffi). The Python tests run it through python/_confluentkafka.c and the FFI. The .NET tests run it through the \
         P/Invoke declarations of dotnet/src/Confluent.Kafka and the FFI. The gRPC arms (c.integration, python.grpc, \
         dotnet.grpc) run the Rust integration harness (rust/tests/integration) against the C, Python and .NET bindings \
         (the .NET gRPC server is dotnet/grpc-server).
- When in doubt, include the test. A change to build files (Cargo.toml, Cargo.lock, build.rs, Makefiles, CMakeLists.txt, \
         pyproject.toml, setup.py, *.csproj, *.sln, Directory.Build.props), the code generator, shared test support, or anything you cannot trace includes every \
         suite it can reach.
- Leave out a suite no change can affect. A change only to documentation or comments affects no test, except a Rust \
         doc comment with a code block, which is a doctest (`rust.doc`).
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
  - \"unit\": ctest names, among: {ctest_names}; an entry `<name>:<function>` runs only that test function of \
         the program, one passed to `RUN_TEST` in c/tests/test_<name>.c
  - \"integration\": filters of the `__grpc_c` tests of the `integration` unit (names such as `producer::<case>__grpc_c`)
- \"python\": an object with
  - \"typecheck\": `[]` to run mypy --strict over python/confluent_kafka
{python_suites}  - \"grpc\": filters of the `__grpc_python` tests of the `integration` unit
- \"dotnet\": an object whose lists hold fully qualified name prefixes of .NET tests, a namespace or a class (such as \
         `Confluent.Kafka.UnitTests.ProducerRecordTests`), each running the tests whose full name contains it:
{dotnet_suites}  - \"grpc\": filters of the `__grpc_dotnet` tests of the `integration` unit (they also select the \
         `__grpc_dotnet_async` ones)
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

/// Runs `command` with no stdin and its stderr inherited, and returns its exit
/// status and stdout. Kills it and fails when it has not exited within
/// `timeout`.
fn run_with_deadline(command: &mut Command, timeout: Duration) -> anyhow::Result<(std::process::ExitStatus, String)> {
    let program = command.get_program().to_string_lossy().into_owned();
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("running {program}"))?;
    // Read stdout on its own thread, so a full pipe cannot stall the child.
    let mut stdout = child.stdout.take().context("the child's stdout")?;
    let reader = std::thread::spawn(move || {
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut stdout, &mut out).map(|_| out)
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().with_context(|| format!("waiting for {program}"))? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("select-tests: {program} did not answer within {} s", timeout.as_secs());
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let out = reader
        .join()
        .map_err(|_| anyhow::anyhow!("reading {program}'s output"))?
        .with_context(|| format!("reading {program}'s output"))?;
    Ok((status, String::from_utf8_lossy(&out).into_owned()))
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
                            for entry in &entries {
                                let (name, function) = match entry.split_once(':') {
                                    Some((name, function)) => (name, Some(function)),
                                    None => (entry.as_str(), None),
                                };
                                let Some(functions) = inventory.ctests.get(name) else {
                                    bail!("c.unit: unknown ctest {name:?}");
                                };
                                if let Some(function) = function.filter(|f| !functions.contains(*f)) {
                                    bail!("c.unit: ctest {name:?} has no test {function:?}");
                                }
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
            "dotnet" => {
                for (key, entries) in object(value, "dotnet")? {
                    let entries = string_list(entries, &format!("dotnet.{key}"))?;
                    if key != "grpc" {
                        check_dotnet_entries(key, &entries)?;
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

/// Each entry of a `dotnet test` suite is a name prefix inside the suite's
/// namespaces, made of the characters a .NET name can hold.
fn check_dotnet_entries(suite: &str, entries: &[String]) -> anyhow::Result<()> {
    let Some((_, _, namespaces)) = DOTNET_SUITES.iter().find(|(name, ..)| *name == suite) else {
        bail!("dotnet: unknown suite {suite:?}");
    };
    for entry in entries {
        if !entry.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.') {
            bail!("dotnet.{suite}: {entry:?} is not a namespace or class name");
        }
        let inside = namespaces.iter().any(|namespace| {
            entry
                .strip_prefix(namespace)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
        });
        if !inside {
            bail!("dotnet.{suite}: {entry:?} is not in {}", namespaces.join(" or "));
        }
    }
    Ok(())
}

/// The suites `selection` runs, two lines each: the suite with its entries
/// (`all` for the whole suite), then the agent's reason for it. Empty when it
/// selects no suite.
fn describe(selection: &Map<String, Value>) -> Vec<String> {
    let reasons = selection.get("reasons").and_then(Value::as_object);
    let mut lines = Vec::new();
    for language in ["rust", "c", "python", "dotnet"] {
        let Some(Value::Object(sections)) = selection.get(language) else {
            continue;
        };
        for (name, entries) in sections {
            let key = format!("{language}.{name}");
            let entries: Vec<&str> = entries.as_array().into_iter().flatten().filter_map(Value::as_str).collect();
            let what = if entries.is_empty() {
                "all".to_string()
            } else {
                entries.join(", ")
            };
            lines.push(format!("  {key}: {what}"));
            let reason = reasons
                .and_then(|reasons| reasons.get(&key))
                .and_then(Value::as_str)
                .unwrap_or("(no reason given)");
            lines.push(format!("      why: {reason}"));
        }
    }
    lines
}

fn print_summary(prefix: &str, selection: &Map<String, Value>) {
    let lines = describe(selection);
    if lines.is_empty() {
        eprintln!("{prefix}: no test selected; only the builds and checks run");
    } else {
        eprintln!("{prefix}: selected (a suite not listed does not run):\n{}", lines.join("\n"));
    }
}

/// `show-test-selection`: whether the selection `TEST_SELECTION` names applies
/// to the working tree and, when it does, the suites it runs and why. CI runs
/// it first in each job, so the job log opens with what the selection trims.
pub fn show() -> anyhow::Result<()> {
    if env::var_os("TEST_SELECTION").is_none() {
        eprintln!("test-selection: TEST_SELECTION is not set, so every test runs");
        return Ok(());
    }
    if let Some(selection) = load()? {
        print_summary("test-selection", &selection);
    }
    Ok(())
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
    let variable = library_path_variable();
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

/// The variable of the dynamic library search path that cargo sets for the
/// programs it runs.
fn library_path_variable() -> &'static str {
    if cfg!(target_os = "macos") {
        "DYLD_FALLBACK_LIBRARY_PATH"
    } else {
        "LD_LIBRARY_PATH"
    }
}

/// The library search path `paths` without the directories under
/// `build_dir`, `None` when nothing is left.
///
/// `cargo xtask` is `cargo run`, which puts the build directory of this
/// program (`target/debug`, its `deps` and the build scripts' outputs) at the
/// front of the library search path. A command `selected-run` starts must not
/// inherit them: `target/debug/libconfluent_kafka.so` is whatever the last
/// debug build left there, often without the `ffi` feature, and the library
/// path wins over the `RUNPATH` with which the Python extension and the C
/// tests find the library they were built against. The toolchain directories
/// cargo adds hold only Rust's own libraries and stay.
fn without_build_dirs(paths: &std::ffi::OsStr, build_dir: &Path) -> Option<OsString> {
    let kept: Vec<PathBuf> = env::split_paths(paths).filter(|path| !path.starts_with(build_dir)).collect();
    if kept.is_empty() {
        None
    } else {
        env::join_paths(kept).ok()
    }
}

fn run(command: &mut Command) -> anyhow::Result<()> {
    let status = command.status().with_context(|| format!("running {command:?}"))?;
    if !status.success() {
        bail!("{command:?} failed ({status})");
    }
    Ok(())
}

// ── selected-run ───────────────────────────────────────────────────────────

/// `selected-run --section <section> [--default <arg>]... [--ctest]
/// [--dotnet [--dotnet-filter <expr>]] [-C <dir>] -- <command>`: runs the
/// command in `dir` when the selection selects the section (or there is no
/// selection). An argument `{}` stands for the selected entries, or the
/// `--default` arguments when the whole suite is selected; with `--ctest` it
/// stands for `-R <regex>` matching the selected programs, the entries going to
/// the programs in `C_TEST_FILTER` (`c/tests/test_filter.h`), or for nothing; with
/// `--dotnet` for `--filter <expr>` selecting the tests whose full name
/// contains an entry, and-ed with the `--dotnet-filter` expression (which may
/// be empty), or for that expression alone.
pub fn selected_run(args: &[String]) -> anyhow::Result<()> {
    let (options, command) = split_at_separator(args);
    let mut section_name = None;
    let mut defaults = Vec::new();
    let mut ctest = false;
    let mut dotnet = false;
    let mut dotnet_base = String::new();
    let mut dir = PathBuf::from(".");
    let mut options = options.iter();
    while let Some(option) = options.next() {
        let mut value = || options.next().cloned().with_context(|| format!("{option} needs a value"));
        match option.as_str() {
            "--section" => section_name = Some(value()?),
            "--default" => defaults.push(value()?),
            "--ctest" => ctest = true,
            "--dotnet" => dotnet = true,
            "--dotnet-filter" => dotnet_base = value()?,
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
    let mut c_test_filter = None;
    let selected: Vec<String> = match choice {
        Choice::Skip => {
            eprintln!(
                "test-selection: skipping `{}`: {section_name} is not selected",
                command.join(" ")
            );
            return Ok(());
        },
        Choice::All if ctest => Vec::new(),
        Choice::All if dotnet => dotnet_filter(&dotnet_base, None),
        Choice::All => defaults,
        Choice::Only(entries) if ctest => {
            let (args, filter) = ctest_args(&entries);
            c_test_filter = Some(filter);
            args
        },
        Choice::Only(names) if dotnet => dotnet_filter(&dotnet_base, Some(&names)),
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
    let mut command = Command::new(program);
    command.args(argv).current_dir(dir);
    let variable = library_path_variable();
    let build_dir = env::current_exe()?.parent().map(Path::to_path_buf);
    if let (Some(paths), Some(build_dir)) = (env::var_os(variable), build_dir) {
        match without_build_dirs(&paths, &build_dir) {
            Some(kept) => command.env(variable, kept),
            None => command.env_remove(variable),
        };
    }
    if let Some(filter) = c_test_filter {
        command.env("C_TEST_FILTER", filter);
    }
    run(&mut command)
}

/// The ctest arguments and the `C_TEST_FILTER` value (`c/tests/test_filter.h`)
/// running these `c.unit` entries: ctest names, or `<name>:<function>` for one
/// test function of a program.
fn ctest_args(entries: &[String]) -> (Vec<String>, String) {
    let mut names: Vec<String> = Vec::new();
    for entry in entries {
        let name = entry.split_once(':').map_or(entry.as_str(), |(name, _)| name);
        if !names.iter().any(|n| n == name) {
            names.push(name.to_string());
        }
    }
    (vec!["-R".into(), ctest_regex(&names)], entries.join(","))
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

/// The `dotnet test` arguments running the tests `base` (a `--filter`
/// expression, possibly empty) selects whose full name contains one of
/// `names`, or every test `base` selects.
fn dotnet_filter(base: &str, names: Option<&[String]>) -> Vec<String> {
    // The characters a `--filter` value gives a meaning to.
    let escape = |name: &String| {
        name.chars()
            .flat_map(|c| {
                if "\\()&|=!~".contains(c) {
                    vec!['\\', c]
                } else {
                    vec![c]
                }
            })
            .collect::<String>()
    };
    let selected = names.map(|names| {
        names
            .iter()
            .map(|name| format!("FullyQualifiedName~{}", escape(name)))
            .collect::<Vec<_>>()
            .join("|")
    });
    let base = base.trim();
    let expression = match (base.is_empty(), selected) {
        (true, None) => return Vec::new(),
        (true, Some(selected)) => selected,
        (false, None) => base.to_string(),
        (false, Some(selected)) => format!("({base})&({selected})"),
    };
    vec!["--filter".into(), expression]
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
    fn test_without_build_dirs() {
        let build_dir = Path::new("/repo/rust/target/debug");
        let paths = env::join_paths([
            "/repo/rust/target/debug",
            "/repo/rust/target/debug/deps",
            "/repo/rust/target/debug/build/zstd-sys-0123/out",
            "/toolchain/lib",
            "/usr/local/lib",
            "/repo/rust/target/release",
        ])
        .unwrap();
        let kept = without_build_dirs(&paths, build_dir).unwrap();
        assert_eq!(
            env::split_paths(&kept).collect::<Vec<_>>(),
            [
                PathBuf::from("/toolchain/lib"),
                PathBuf::from("/usr/local/lib"),
                PathBuf::from("/repo/rust/target/release"),
            ]
        );

        let only_build = env::join_paths(["/repo/rust/target/debug", "/repo/rust/target/debug/deps"]).unwrap();
        assert_eq!(without_build_dirs(&only_build, build_dir), None);
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
    fn test_is_selection_change() {
        let files = strings(&["test-selection.json", ".test-selection-commit.json"]);
        assert!(is_selection_change("A test-selection.json", &files));
        assert!(is_selection_change("M .test-selection-commit.json", &files));
        assert!(is_selection_change(
            "R100 test-selection.json .test-selection-commit.json",
            &files
        ));
        assert!(!is_selection_change("R100 test-selection.json rust/x.json", &files));
        assert!(!is_selection_change("M rust/test-selection.json", &files));
        assert!(!is_selection_change("M rust/src/lib.rs", &files));
        assert!(!is_selection_change("M", &files));
    }

    #[test]
    fn test_working_tree() {
        let repo = env::temp_dir().join(format!("xtask-test-selection-{}", std::process::id()));
        let _ = fs::remove_dir_all(&repo);
        fs::create_dir_all(&repo).unwrap();
        let git = |args: &[&str]| git_in(&repo, &[], args).unwrap();
        git(&["init", "-q"]);
        fs::write(repo.join(".gitignore"), "*.log\n").unwrap();
        fs::write(repo.join("a.txt"), "a\n").unwrap();
        git(&["add", "-A"]);
        git(&[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "init",
        ]);
        let excluded = strings(&["test-selection.json"]);
        let tree = || working_tree(&repo, &excluded).unwrap();

        let clean = tree();
        assert_eq!(clean, git(&["rev-parse", "HEAD^{tree}"]));
        fs::write(repo.join("test-selection.json"), "{}").unwrap();
        fs::write(repo.join("x.log"), "ignored").unwrap();
        assert_eq!(tree(), clean, "the selection file and ignored files are left out");
        git(&["add", "test-selection.json"]);
        git(&[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "selection",
        ]);
        assert_eq!(tree(), clean, "a committed selection file is left out");

        fs::write(repo.join("a.txt"), "b\n").unwrap();
        let edited = tree();
        assert_ne!(edited, clean, "an edit to a tracked file");
        fs::write(repo.join("new.txt"), "n\n").unwrap();
        assert_ne!(tree(), edited, "an untracked file");
        fs::remove_file(repo.join("new.txt")).unwrap();
        fs::write(repo.join("a.txt"), "a\n").unwrap();
        assert_eq!(tree(), clean);
        assert_eq!(git(&["status", "--porcelain"]), "", "the real index is left as it is");
        fs::remove_dir_all(&repo).unwrap();
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

    #[test]
    fn test_ctest_args() {
        assert_eq!(
            ctest_args(&strings(&["mock_producer:test_a", "kafka_future", "mock_producer:test_b"])),
            (
                strings(&["-R", "^(mock_producer|kafka_future)$"]),
                "mock_producer:test_a,kafka_future,mock_producer:test_b".to_string()
            )
        );
        assert_eq!(
            ctest_args(&strings(&["mock_producer"])),
            (strings(&["-R", "^(mock_producer)$"]), "mock_producer".to_string())
        );
    }

    #[test]
    fn test_ctests() {
        let cmake = "\
add_executable(test_mock_producer tests/test_mock_producer.c)
target_link_libraries(test_mock_producer unity)
add_test(NAME mock_producer COMMAND test_mock_producer)
";
        let source = "int main(void) {\n    RUN_TEST(test_a);\n    RUN_TEST( test_b );\n    RUN_TEST(test_c);\n}\n";
        let read = |path: &str| {
            assert_eq!(path, "tests/test_mock_producer.c");
            Ok(source.to_string())
        };
        let ctests = ctests(cmake, read).unwrap();
        assert_eq!(ctests.keys().collect::<Vec<_>>(), ["mock_producer"]);
        // `#func` drops the spaces of `RUN_TEST( test_b )`, as the parser does.
        assert_eq!(
            ctests["mock_producer"].iter().collect::<Vec<_>>(),
            ["test_a", "test_b", "test_c"]
        );

        let renamed = "add_executable(test_x tests/other.c)\nadd_test(NAME x COMMAND test_x)\n";
        assert_eq!(
            super::ctests(renamed, |_| Ok(String::new())).unwrap_err().to_string(),
            "ctest \"x\" runs tests/other.c, but its source must be test_x.c for C_TEST_FILTER"
        );
        assert_eq!(
            super::ctests("add_test(NAME x COMMAND test_x)\n", |_| Ok(String::new()))
                .unwrap_err()
                .to_string(),
            "ctest \"x\": no add_executable for \"test_x\""
        );
    }

    #[test]
    fn test_ctests_of_the_repository() {
        let c_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../c");
        let ctests = ctests(&fs::read_to_string(c_dir.join("CMakeLists.txt")).unwrap(), |source| {
            Ok(fs::read_to_string(c_dir.join(source))?)
        })
        .unwrap();
        assert!(ctests["mock_producer"].contains("test_destroy_null"));
        assert!(ctests.values().all(|functions| !functions.is_empty()));
    }

    #[test]
    fn test_dotnet_filter() {
        assert!(dotnet_filter("", None).is_empty());
        assert!(dotnet_filter("  ", None).is_empty());
        assert_eq!(dotnet_filter("A!~B", None), strings(&["--filter", "A!~B"]));
        assert_eq!(
            dotnet_filter("", Some(&strings(&["N.C", "N.D"]))),
            strings(&["--filter", "FullyQualifiedName~N.C|FullyQualifiedName~N.D"])
        );
        assert_eq!(
            dotnet_filter("FullyQualifiedName!~N.I", Some(&strings(&["N.C"]))),
            strings(&["--filter", "(FullyQualifiedName!~N.I)&(FullyQualifiedName~N.C)"])
        );
        assert_eq!(
            dotnet_filter("", Some(&strings(&["a(b)|c"]))),
            strings(&["--filter", "FullyQualifiedName~a\\(b\\)\\|c"])
        );
    }

    fn inventory_fixture() -> Inventory {
        Inventory {
            packages: BTreeMap::new(),
            rust_units: [("lib", ""), ("admin", "")]
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            doctest_packages: BTreeSet::new(),
            ctests: [("mock_producer".to_string(), ["test_a".to_string()].into())].into(),
            python_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("../../python"),
        }
    }

    #[test]
    fn test_validate() {
        let inventory = inventory_fixture();
        let answer = json!({
            "rust": {"lib": ["producer::"], "admin": []},
            "c": {"unit": ["mock_producer", "mock_producer:test_a"], "integration": []},
            "python": {"typecheck": [], "unit": ["test/unit"], "grpc": ["producer::"]},
            "dotnet": {
                "solution": ["Confluent.Kafka.UnitTests.ProducerRecordTests", "Confluent.Kafka.IntegrationTests"],
                "soak": [],
                "performance": ["Confluent.Kafka.Performance.Tests"],
                "grpc": ["producer::"],
            },
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
            error(json!({"c": {"unit": ["kafka_admin:test_a"]}})),
            "c.unit: unknown ctest \"kafka_admin\""
        );
        assert_eq!(
            error(json!({"c": {"unit": ["mock_producer:test_b"]}})),
            "c.unit: ctest \"mock_producer\" has no test \"test_b\""
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
        assert_eq!(error(json!({"dotnet": {"unit": []}})), "dotnet: unknown suite \"unit\"");
        assert_eq!(
            error(json!({"dotnet": {"soak": ["Confluent.Kafka.UnitTests"]}})),
            "dotnet.soak: \"Confluent.Kafka.UnitTests\" is not in Confluent.Kafka.Soak.Tests"
        );
        assert_eq!(
            error(json!({"dotnet": {"solution": ["Confluent.Kafka.UnitTestsX"]}})),
            "dotnet.solution: \"Confluent.Kafka.UnitTestsX\" is not in Confluent.Kafka.UnitTests or \
             Confluent.Kafka.IntegrationTests"
        );
        assert_eq!(
            error(json!({"dotnet": {"solution": ["Confluent.Kafka.UnitTests|x"]}})),
            "dotnet.solution: \"Confluent.Kafka.UnitTests|x\" is not a namespace or class name"
        );
        assert_eq!(error(json!({"java": {}})), "unknown member \"java\"");
    }

    #[test]
    fn test_run_with_deadline() {
        let (status, out) =
            run_with_deadline(Command::new("sh").args(["-c", "echo answer"]), Duration::from_secs(10)).unwrap();
        assert!(status.success());
        assert_eq!(out, "answer\n");

        let started = Instant::now();
        let error = run_with_deadline(Command::new("sleep").arg("30"), Duration::from_secs(1)).unwrap_err();
        assert_eq!(error.to_string(), "select-tests: sleep did not answer within 1 s");
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn test_describe() {
        let selection = json!({
            "version": 2,
            "tree": "t",
            "rust": {"lib": ["producer::", "consumer::fetch"], "admin": []},
            "c": {"unit": ["mock_producer:test_a"]},
            "reasons": {"rust.lib": "the producer changed", "c.unit": "the C producer binding changed"},
        });
        assert_eq!(
            describe(selection.as_object().unwrap()),
            strings(&[
                "  rust.admin: all",
                "      why: (no reason given)",
                "  rust.lib: producer::, consumer::fetch",
                "      why: the producer changed",
                "  c.unit: mock_producer:test_a",
                "      why: the C producer binding changed",
            ])
        );
        assert!(describe(json!({"version": 2, "tree": "t"}).as_object().unwrap()).is_empty());
    }
}
