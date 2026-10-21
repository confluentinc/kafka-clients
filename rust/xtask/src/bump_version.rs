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

//! `cargo xtask bump-version <version>`: sets one version on everything
//! released together, each manifest in its own syntax.
//!
//! The version is given as semver, `X.Y.Z` or `X.Y.Z-alpha.N`, `-beta.N`,
//! `-dev.N`, `-rc.N`; PEP 440's `X.Y.ZaN`, `X.Y.ZbN`, `X.Y.Z.devN`,
//! `X.Y.ZrcN` are read as the same versions. Cargo manifests get the semver
//! form, Python packages the PEP 440 one, so `0.3.0-rc.1` is `0.3.0rc1` and
//! `0.3.0-dev.1` is `0.3.0.dev1` in `pyproject.toml`. Versions compare in
//! semver order, as Cargo and crates.io compare them: alpha < beta < dev < rc
//! < the release. PEP 440 agrees except for dev, which it puts before alpha,
//! so going from a beta to a dev version moves the Python packages back;
//! `--python-version` sets another Python version for such a bump.
//!
//! `--python-version <pep440>` gives the Python packages a version of their
//! own instead, any PEP 440 version in normal form, for a Python-only release
//! such as `0.3.0.post1`.
//!
//! What is released together:
//!
//!   - every member of the Rust workspace at the version of `confluent-kafka`
//!     (a member on a version of its own, `ffi-macros`, keeps it), and
//!     `Cargo.lock` with them;
//!   - the Python packages in [`PYPROJECTS`] and the `__version__` of
//!     [`PYTHON_VERSION_MODULES`].

use std::cmp::Ordering;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Context as _;

/// The workspace manifest, relative to the repository root.
const WORKSPACE_MANIFEST: &str = "rust/Cargo.toml";

/// Python packages whose `[project]` version follows the crate's.
const PYPROJECTS: [&str; 2] = ["python/pyproject.toml", "tools/translation_agent/pyproject.toml"];

/// Python modules whose `__version__` follows the crate's.
const PYTHON_VERSION_MODULES: [&str; 1] = ["tools/translation_agent/translation_agent/__init__.py"];

const USAGE: &str =
    "usage: cargo xtask bump-version X.Y.Z[-alpha.N|-beta.N|-dev.N|-rc.N] [--python-version PEP440_VERSION]";

/// A pre-release stage, in semver precedence order (its labels compare as
/// ASCII text).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    Alpha,
    Beta,
    Dev,
    Rc,
}

impl Stage {
    const ALL: [Stage; 4] = [Stage::Alpha, Stage::Beta, Stage::Dev, Stage::Rc];

    fn semver(self) -> &'static str {
        match self {
            Stage::Alpha => "alpha",
            Stage::Beta => "beta",
            Stage::Dev => "dev",
            Stage::Rc => "rc",
        }
    }

    fn pep440(self) -> &'static str {
        match self {
            Stage::Alpha => "a",
            Stage::Beta => "b",
            Stage::Dev => ".dev",
            Stage::Rc => "rc",
        }
    }
}

/// A release or pre-release version, as this command writes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PackageVersion {
    major: u64,
    minor: u64,
    patch: u64,
    pre_release: Option<(Stage, u64)>,
}

impl PackageVersion {
    /// Parses the semver form, or PEP 440's for the same version; anything
    /// else, such as a build suffix or another pre-release label, does not
    /// parse.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let mut release_len = text.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(text.len());
        // The dot of PEP 440's `.devN` belongs to the suffix.
        if text[..release_len].ends_with('.') {
            release_len -= 1;
        }
        let (release, suffix) = text.split_at(release_len);
        let mut parts = release.split('.');
        let mut version = PackageVersion {
            major: number(parts.next()?)?,
            minor: number(parts.next()?)?,
            patch: number(parts.next()?)?,
            pre_release: None,
        };
        if parts.next().is_some() {
            return None;
        }
        if suffix.is_empty() {
            return Some(version);
        }
        let (stage, n) = match suffix.strip_prefix('-') {
            Some(semver) => {
                let (label, n) = semver.split_once('.')?;
                let stage = Stage::ALL.into_iter().find(|s| s.semver() == label)?;
                (stage, n)
            },
            None => {
                let (label, n) = suffix.split_at(suffix.find(|c: char| c.is_ascii_digit())?);
                let stage = Stage::ALL.into_iter().find(|s| s.pep440() == label)?;
                (stage, n)
            },
        };
        version.pre_release = Some((stage, number(n)?));
        Some(version)
    }

    /// The `X.Y.Z` this version is, or is a pre-release of.
    pub(crate) fn release(self) -> (u64, u64, u64) {
        (self.major, self.minor, self.patch)
    }

    pub(crate) fn is_pre_release(self) -> bool {
        self.pre_release.is_some()
    }

    /// The PEP 440 form, for Python packages.
    fn pep440(self) -> String {
        let release = format!("{}.{}.{}", self.major, self.minor, self.patch);
        match self.pre_release {
            Some((stage, n)) => format!("{release}{}{n}", stage.pep440()),
            None => release,
        }
    }
}

/// The semver form, for Cargo manifests.
impl fmt::Display for PackageVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        match self.pre_release {
            Some((stage, n)) => write!(f, "-{}.{n}", stage.semver()),
            None => Ok(()),
        }
    }
}

impl Ord for PackageVersion {
    /// A pre-release comes before its release, as in both semver and PEP 440.
    fn cmp(&self, other: &Self) -> Ordering {
        self.release()
            .cmp(&other.release())
            .then_with(|| match (self.pre_release, other.pre_release) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (Some(a), Some(b)) => a.cmp(&b),
            })
    }
}

impl PartialOrd for PackageVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A decimal number without a leading zero, as both semver and PEP 440's
/// normal form write them.
fn number(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) || (text.len() > 1 && text.starts_with('0')) {
        return None;
    }
    text.parse().ok()
}

pub(crate) fn run(args: &[String]) -> anyhow::Result<()> {
    let (text, python_override) = match args {
        [text] => (text, None),
        [text, flag, python] if flag == "--python-version" => (text, Some(python)),
        _ => anyhow::bail!("{USAGE}"),
    };
    let new = PackageVersion::parse(text).with_context(|| format!("`{text}` is not a version; {USAGE}"))?;
    let python = match python_override {
        Some(python) if is_pep440(python) => python.clone(),
        Some(python) => anyhow::bail!("`{python}` is not a PEP 440 version in normal form, such as 0.3.0.post1"),
        None => new.pep440(),
    };

    let root = repo_root()?;
    let workspace = read(&root.join(WORKSPACE_MANIFEST))?;
    let current_text =
        table_value(&workspace, "package", "version").context("rust/Cargo.toml has no [package] version")?;
    let current = PackageVersion::parse(current_text)
        .with_context(|| format!("the current version `{current_text}` is not one this command writes"))?;
    if new < current {
        anyhow::bail!("{new} is below the current version {current}");
    }
    println!("🔖 Bumping {current} → {new} (Python: {python})");

    // Every edit is computed before any file is written, so a manifest this
    // command cannot read leaves the tree untouched.
    let mut edits: Vec<(PathBuf, String)> = Vec::new();
    let members = workspace_members(&workspace).context("rust/Cargo.toml has no [workspace] members")?;
    for member in members {
        let path = root.join("rust").join(member).join("Cargo.toml");
        let manifest = read(&path)?;
        let version = table_value(&manifest, "package", "version")
            .with_context(|| format!("{} has no [package] version", relative(&root, &path)))?;
        if version != current_text {
            println!(
                "   {} stays at {version}: it is not versioned with confluent-kafka",
                relative(&root, &path)
            );
            continue;
        }
        edits.push((
            path,
            set_table_value(&manifest, "package", "version", &new.to_string()).expect("read above"),
        ));
    }
    for pyproject in PYPROJECTS {
        let path = root.join(pyproject);
        let edited = set_table_value(&read(&path)?, "project", "version", &python)
            .with_context(|| format!("{pyproject} has no [project] version"))?;
        edits.push((path, edited));
    }
    for module in PYTHON_VERSION_MODULES {
        let path = root.join(module);
        let edited =
            set_module_version(&read(&path)?, &python).with_context(|| format!("{module} has no __version__"))?;
        edits.push((path, edited));
    }

    for (path, text) in &edits {
        fs::write(path, text).with_context(|| format!("writing {}", path.display()))?;
        println!("   {}", relative(&root, path));
    }

    // Only the workspace's own entries change: no dependency is updated.
    println!("🔒 Updating the workspace crates in Cargo.lock");
    let status = Command::new("cargo")
        .args(["update", "--workspace"])
        .current_dir(root.join("rust"))
        .status()?;
    if !status.success() {
        anyhow::bail!("cargo update --workspace failed ({status})");
    }
    println!("✅ Version {new} set in {} files and Cargo.lock", edits.len());
    Ok(())
}

/// Whether `text` is a PEP 440 version in normal form:
/// `[N!]N(.N)*[{a|b|rc}N][.postN][.devN][+local]`.
fn is_pep440(text: &str) -> bool {
    let (public, local) = match text.split_once('+') {
        Some((public, local)) => (public, Some(local)),
        None => (text, None),
    };
    let local_ok = local.is_none_or(|local| {
        local
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()))
    });
    let public = match public.split_once('!') {
        Some((epoch, rest)) if number(epoch).is_some() => rest,
        Some(_) => return false,
        None => public,
    };
    // The release runs up to the first letter, less the dot of `.post` or `.dev`.
    let letters = public.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(public.len());
    let release = public[..letters].strip_suffix('.').unwrap_or(&public[..letters]);
    let suffix_ok = || {
        let rest = strip_segment(&public[release.len()..], &["a", "b", "rc"])?;
        let rest = strip_segment(rest, &[".post"])?;
        let rest = strip_segment(rest, &[".dev"])?;
        Some(rest.is_empty())
    };
    local_ok && release.split('.').all(|part| number(part).is_some()) && suffix_ok() == Some(true)
}

/// `rest` without a leading `<label>N` for the first of `labels` it starts
/// with, or unchanged when it starts with none; `None` when the label has no
/// number after it.
fn strip_segment<'a>(rest: &'a str, labels: &[&str]) -> Option<&'a str> {
    let Some(after) = labels.iter().find_map(|label| rest.strip_prefix(label)) else {
        return Some(rest);
    };
    let digits = after.find(|c: char| !c.is_ascii_digit()).unwrap_or(after.len());
    number(&after[..digits])?;
    Some(&after[digits..])
}

/// The string value of `key` in `[table]`.
fn table_value<'a>(text: &'a str, table: &str, key: &str) -> Option<&'a str> {
    let line = table_lines(text, table).find_map(|line| quoted_value(line, key).map(|span| (line, span)));
    line.map(|(line, (start, end))| &line[start..end])
}

/// `text` with the string value of `key` in `[table]` replaced, the rest of the
/// line (indentation, a trailing comment) kept.
fn set_table_value(text: &str, table: &str, key: &str, value: &str) -> Option<String> {
    let header = format!("[{table}]");
    let mut in_table = false;
    let mut found = false;
    let mut edited = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_table = trimmed == header;
        } else if in_table && !found {
            if let Some((start, end)) = quoted_value(line, key) {
                edited.push_str(&line[..start]);
                edited.push_str(value);
                edited.push_str(&line[end..]);
                found = true;
                continue;
            }
        }
        edited.push_str(line);
    }
    found.then_some(edited)
}

/// `text` with the value of its module-level `__version__ = "..."` replaced.
fn set_module_version(text: &str, value: &str) -> Option<String> {
    let mut found = false;
    let mut edited = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        // Module level only: an indented assignment is inside a block.
        match quoted_value(line, "__version__").filter(|_| !found && line.starts_with("__version__")) {
            Some((start, end)) => {
                edited.push_str(&line[..start]);
                edited.push_str(value);
                edited.push_str(&line[end..]);
                found = true;
            },
            None => edited.push_str(line),
        }
    }
    found.then_some(edited)
}

/// The lines of `[table]`, up to the next table header.
fn table_lines<'a>(text: &'a str, table: &str) -> impl Iterator<Item = &'a str> {
    let header = format!("[{table}]");
    text.lines()
        .skip_while(move |line| line.trim() != header)
        .skip(1)
        .take_while(|line| !line.trim().starts_with('['))
}

/// The byte range of the string in a `key = "value"` line.
fn quoted_value(line: &str, key: &str) -> Option<(usize, usize)> {
    let rest = line.trim_start().strip_prefix(key)?;
    let rest = rest.trim_start().strip_prefix('=')?.trim_start().strip_prefix('"')?;
    let start = line.len() - rest.len();
    Some((start, start + rest.find('"')?))
}

/// The paths of `[workspace] members`.
fn workspace_members(manifest: &str) -> Option<Vec<String>> {
    let mut lines = table_lines(manifest, "workspace");
    while let Some(line) = lines.next() {
        let Some(rest) = line.trim().strip_prefix("members") else {
            continue;
        };
        let mut array = rest.trim_start().strip_prefix('=')?.trim_start().strip_prefix('[')?.to_string();
        // The array may span lines.
        while !array.contains(']') {
            array.push_str(lines.next()?);
        }
        let members = array[..array.find(']')?]
            .split(',')
            .map(|member| member.trim().trim_matches('"'))
            .filter(|member| !member.is_empty())
            .map(String::from)
            .collect();
        return Some(members);
    }
    None
}

/// The repository root, asked of Cargo at run time: `CARGO_MANIFEST_DIR` is
/// fixed at build time, and Cargo does not rebuild xtask when the same sources
/// sit at another path, so a binary built in a since-removed worktree would
/// edit, or fail to find, that worktree's files.
fn repo_root() -> anyhow::Result<PathBuf> {
    let output = Command::new("cargo")
        .args(["locate-project", "--workspace", "--message-format", "plain"])
        .output()
        .context("running cargo locate-project")?;
    if !output.status.success() {
        anyhow::bail!(
            "cargo locate-project --workspace failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let manifest = PathBuf::from(String::from_utf8(output.stdout)?.trim());
    manifest
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .with_context(|| format!("{} is not rust/Cargo.toml in a repository", manifest.display()))
}

fn read(path: &Path) -> anyhow::Result<String> {
    fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))
}

fn relative(root: &Path, path: &Path) -> String {
    let path = path.strip_prefix(root).unwrap_or(path);
    // `rust/./Cargo.toml` for the workspace's root member `.`.
    path.components().collect::<PathBuf>().display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(text: &str) -> PackageVersion {
        PackageVersion::parse(text).unwrap_or_else(|| panic!("{text} should parse"))
    }

    #[test]
    fn semver_and_pep440_forms_read_as_the_same_version() {
        for (semver, pep440) in [
            ("0.3.0", "0.3.0"),
            ("0.3.0-alpha.1", "0.3.0a1"),
            ("0.3.0-beta.2", "0.3.0b2"),
            ("1.10.0-rc.0", "1.10.0rc0"),
            ("0.2.0-dev.1", "0.2.0.dev1"),
        ] {
            assert_eq!(v(semver), v(pep440));
            assert_eq!(v(semver).to_string(), semver);
            assert_eq!(v(semver).pep440(), pep440);
            assert_eq!(v(pep440).to_string(), semver);
        }
    }

    #[test]
    fn other_forms_do_not_parse() {
        for text in [
            "",
            "0.3",
            "0.3.0.1",
            "v0.3.0",
            "00.3.0",
            "0.3.0-rc",
            "0.3.0-rc1",
            "0.3.0-rc.01",
            "0.3.0-pre.1",
            "0.3.0-rc.1+build",
            "0.3.0rc",
            "0.3.0c1",
            "0.3.0-a.1",
            "0.3.0alpha1",
            "0.3.0dev1",
            "0.3.0-dev1",
            "0.3.0.dev",
            "0.3.0a1.dev1",
        ] {
            assert_eq!(PackageVersion::parse(text), None, "{text}");
        }
    }

    #[test]
    fn pre_releases_come_before_their_release_in_stage_order() {
        let ordered = [
            "0.2.0",
            "0.3.0-alpha.1",
            "0.3.0-alpha.2",
            "0.3.0-beta.1",
            "0.3.0-dev.1",
            "0.3.0-rc.1",
            "0.3.0",
            "0.3.1-alpha.0",
        ];
        for pair in ordered.windows(2) {
            assert!(v(pair[0]) < v(pair[1]), "{} < {}", pair[0], pair[1]);
        }
        assert!(v("0.3.0-rc.10") > v("0.3.0-rc.9"), "pre-release numbers compare as numbers");
        assert!(v("0.3.0-rc.1").is_pre_release());
        assert_eq!(v("0.3.0-rc.1").release(), (0, 3, 0));
    }

    #[test]
    fn python_override_is_a_pep440_version_in_normal_form() {
        for text in [
            "0.3.0",
            "0.3",
            "1!0.3.0",
            "0.3.0a1",
            "0.3.0rc2",
            "0.3.0.post1",
            "0.3.0rc1.post2.dev3",
            "0.3.0.dev0",
            "0.3.0+cfk.1",
        ] {
            assert!(is_pep440(text), "{text}");
        }
        for text in [
            "",
            "v0.3.0",
            "0.3.0-rc.1",
            "0.3.0.rc1",
            "0.3.0c1",
            "0.3.0rc",
            "0.3.0a1b2",
            "0.3.0.dev1.post1",
            "0.3.0.",
            "01.3.0",
            "0.3.0+",
            "0.3.0+UPPER",
            "x!0.3.0",
        ] {
            assert!(!is_pep440(text), "{text}");
        }
    }

    #[test]
    fn table_value_is_set_only_in_its_table() {
        let manifest = "\
# A comment
[package]
name = \"generator\"
version = \"0.2.0\"  # keep me
edition = \"2024\"

[dependencies]
serde = { version = \"1.0\" }
version = \"9.9.9\"
";
        assert_eq!(table_value(manifest, "package", "version"), Some("0.2.0"));
        assert_eq!(table_value(manifest, "dependencies", "version"), Some("9.9.9"));
        let edited = set_table_value(manifest, "package", "version", "0.3.0-rc.1").expect("has a version");
        assert_eq!(edited, manifest.replace("\"0.2.0\"", "\"0.3.0-rc.1\""));
        assert_eq!(set_table_value(manifest, "project", "version", "1.0.0"), None);
    }

    #[test]
    fn a_longer_key_is_not_the_key() {
        let manifest = "[package]\nversion_note = \"x\"\nversion = \"1.0.0\"\n";
        assert_eq!(table_value(manifest, "package", "version"), Some("1.0.0"));
    }

    #[test]
    fn module_version_is_the_module_level_assignment() {
        let module = "\"\"\"Doc.\"\"\"\n\nif x:\n    __version__ = \"0.0.0\"\n__version__ = \"0.2.0\"\n";
        assert_eq!(
            set_module_version(module, "0.3.0rc1").as_deref(),
            Some("\"\"\"Doc.\"\"\"\n\nif x:\n    __version__ = \"0.0.0\"\n__version__ = \"0.3.0rc1\"\n")
        );
        assert_eq!(set_module_version("x = 1\n", "1.0.0"), None);
    }

    #[test]
    fn workspace_members_are_read_on_one_or_several_lines() {
        let one = "[workspace]\nmembers = [\".\", \"xtask\"]\nresolver = \"2\"\n\n[package]\n";
        assert_eq!(workspace_members(one), Some(vec![".".to_string(), "xtask".to_string()]));
        let several = "[workspace]\nmembers = [\n  \".\",\n  \"tools/verifiable-clients\",\n]\n";
        assert_eq!(
            workspace_members(several),
            Some(vec![".".to_string(), "tools/verifiable-clients".to_string()])
        );
        assert_eq!(workspace_members("[package]\nmembers = [\"x\"]\n"), None);
    }

    #[test]
    fn this_repository_has_every_file_the_command_edits() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let workspace = read(&root.join(WORKSPACE_MANIFEST)).expect("workspace manifest");
        let current = table_value(&workspace, "package", "version").expect("crate version");
        assert!(PackageVersion::parse(current).is_some(), "{current}");
        assert!(workspace_members(&workspace).is_some_and(|members| members.contains(&".".to_string())));
        for pyproject in PYPROJECTS {
            let text = read(&root.join(pyproject)).expect("pyproject");
            assert!(table_value(&text, "project", "version").is_some(), "{pyproject}");
        }
        for module in PYTHON_VERSION_MODULES {
            let text = read(&root.join(module)).expect("module");
            assert!(set_module_version(&text, "1.0.0").is_some(), "{module}");
        }
    }
}
