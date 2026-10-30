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
//! The two are separate version lines (`rfc/versioning.md`): the crates follow
//! semver, the Python packages their own PEP 440 line, which only moves up. A
//! Python version lower, in PEP 440 order, than the one any package has is
//! refused, whether it comes from `--python-version` or from the crate
//! version: so while the crate's PEP 440 form is the lower one (`0.2.0b1`
//! against `4.4.0b1`), `--python-version` is required.
//!
//! `--dotnet-version <semver>` sets the version of the `Confluent.Kafka` NuGet
//! package, the one `<Version>` of [`DOTNET_PROPS`], in the semver form only.
//! It is a third line, on the Apache Kafka version like Python's
//! (`rfc/versioning.md`), and it moves only when given: the crate's version is
//! on another line, so it is never the default. A .NET version lower, in
//! semver order, than the current one is refused.
//!
//! What is released together:
//!
//!   - every member of the Rust workspace at the version of `confluent-kafka`
//!     (a member on a version of its own, `ffi-macros`, keeps it), and
//!     `Cargo.lock` with them;
//!   - the Python packages in [`PYPROJECTS`] and the `__version__` of
//!     [`PYTHON_VERSION_MODULES`];
//!   - with `--dotnet-version`, the .NET package.

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

/// The MSBuild file whose one `<Version>` is the .NET package's version.
const DOTNET_PROPS: &str = "dotnet/Directory.Build.props";

const USAGE: &str =
    "usage: cargo xtask bump-version X.Y.Z[-alpha.N|-beta.N|-dev.N|-rc.N] [--python-version PEP440_VERSION] \
     [--dotnet-version SEMVER]\n\
     The crates get the semver version; the Python packages the PEP 440 version, by default the\n\
     crate's in PEP 440 form. Python versions are their own line and only move up: a version below\n\
     a package's current one is refused, so pass --python-version while the crate's is lower.\n\
     --dotnet-version sets the .NET package's version (dotnet/Directory.Build.props), in the semver\n\
     form; without it that version is left as it is. It only moves up too.";

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

/// The arguments: the crate version, then `--python-version` and
/// `--dotnet-version`, each at most once, in either order.
#[derive(Debug, PartialEq, Eq)]
struct Args<'a> {
    version: &'a str,
    python: Option<&'a str>,
    dotnet: Option<&'a str>,
}

fn parse_args(args: &[String]) -> anyhow::Result<Args<'_>> {
    let Some((version, mut flags)) = args.split_first() else {
        anyhow::bail!("{USAGE}");
    };
    if version.starts_with("--") {
        anyhow::bail!("{USAGE}");
    }
    let mut parsed = Args { version, python: None, dotnet: None };
    while let [flag, value, rest @ ..] = flags {
        let slot = match flag.as_str() {
            "--python-version" => &mut parsed.python,
            "--dotnet-version" => &mut parsed.dotnet,
            _ => anyhow::bail!("{USAGE}"),
        };
        if slot.replace(value).is_some() {
            anyhow::bail!("{flag} is given twice; {USAGE}");
        }
        flags = rest;
    }
    if !flags.is_empty() {
        anyhow::bail!("{USAGE}");
    }
    Ok(parsed)
}

pub(crate) fn run(args: &[String]) -> anyhow::Result<()> {
    let Args { version: text, python: python_override, dotnet } = parse_args(args)?;
    let new = PackageVersion::parse(text).with_context(|| format!("`{text}` is not a version; {USAGE}"))?;
    let dotnet = dotnet.map(parse_dotnet_version).transpose()?;
    let python = match python_override {
        Some(python) if is_pep440(python) => python.to_string(),
        Some(python) => anyhow::bail!("`{python}` is not a PEP 440 version in normal form, such as 0.3.0.post1"),
        None => new.pep440(),
    };

    let target = Pep440::parse(&python).with_context(|| format!("`{python}` is not a PEP 440 version"))?;

    let root = repo_root()?;
    let workspace = read(&root.join(WORKSPACE_MANIFEST))?;
    let current_text =
        table_value(&workspace, "package", "version").context("rust/Cargo.toml has no [package] version")?;
    let current = PackageVersion::parse(current_text)
        .with_context(|| format!("the current version `{current_text}` is not one this command writes"))?;
    if new < current {
        anyhow::bail!("{new} is below the current version {current}");
    }
    match dotnet {
        Some(dotnet) => println!("🔖 Bumping {current} → {new} (Python: {python}, .NET: {dotnet})"),
        None => println!("🔖 Bumping {current} → {new} (Python: {python})"),
    }

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
        let text = read(&path)?;
        let current = table_value(&text, "project", "version")
            .with_context(|| format!("{pyproject} has no [project] version"))?;
        ensure_not_lowered(pyproject, current, &python, &target)?;
        let edited = set_table_value(&text, "project", "version", &python).expect("read above");
        edits.push((path, edited));
    }
    for module in PYTHON_VERSION_MODULES {
        let path = root.join(module);
        let text = read(&path)?;
        let current = module_version(&text).with_context(|| format!("{module} has no __version__"))?;
        ensure_not_lowered(module, current, &python, &target)?;
        let edited = set_module_version(&text, &python).expect("read above");
        edits.push((path, edited));
    }
    match dotnet {
        Some(dotnet) => {
            let path = root.join(DOTNET_PROPS);
            let props = read(&path)?;
            ensure_dotnet_not_lowered(props_version(&props)?, dotnet)?;
            edits.push((path, set_props_version(&props, &dotnet.to_string())?));
        },
        None => println!("   {DOTNET_PROPS} is not changed: pass --dotnet-version to set the .NET version"),
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

/// Refuses to write `python` (parsed: `target`) over `current`, the version
/// `file` has, when it is lower in PEP 440 order: the Python packages only
/// move up.
fn ensure_not_lowered(file: &str, current: &str, python: &str, target: &Pep440) -> anyhow::Result<()> {
    let current_version = Pep440::parse(current)
        .with_context(|| format!("{file} has `{current}`, not a PEP 440 version in normal form"))?;
    if *target < current_version {
        anyhow::bail!(
            "{file} is at {current}: the Python version {python} would lower it. Python versions only move up; \
             pass --python-version with a version not below {current}"
        );
    }
    Ok(())
}

/// The .NET version given with `--dotnet-version`: the semver form only, as
/// NuGet writes it, so `4.4.0-beta.1` and not `4.4.0b1`.
fn parse_dotnet_version(text: &str) -> anyhow::Result<PackageVersion> {
    match PackageVersion::parse(text) {
        Some(version) if version.to_string() == text => Ok(version),
        _ => anyhow::bail!(
            "`{text}` is not a .NET package version: X.Y.Z, or X.Y.Z-alpha.N, -beta.N, -dev.N or -rc.N, such as \
             4.4.0-beta.1"
        ),
    }
}

/// Refuses to write `new` over the .NET version `current`, when it is lower in
/// semver order: the .NET package only moves up.
fn ensure_dotnet_not_lowered(current: &str, new: PackageVersion) -> anyhow::Result<()> {
    let current_version = parse_dotnet_version(current)
        .with_context(|| format!("{DOTNET_PROPS} has `{current}`, not a version this command writes"))?;
    if new < current_version {
        anyhow::bail!(
            "{DOTNET_PROPS} is at {current}: the .NET version {new} would lower it. .NET versions only move up; \
             pass --dotnet-version with a version not below {current}"
        );
    }
    Ok(())
}

/// Whether `text` starts with `prefix`, ignoring ASCII case.
fn starts_with_ignore_case(text: &str, prefix: &str) -> bool {
    text.len() >= prefix.len() && text.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

/// The byte range of the value of the one `<Version>` element of `props`. As
/// for an XML reader, comments do not count, and as for MSBuild, the name's
/// case does not matter, so this finds the element `check-nupkg.py version`
/// reads.
fn version_range(props: &str) -> anyhow::Result<(usize, usize)> {
    let mut found = Vec::new();
    let mut at = 0;
    while let Some(offset) = props[at..].find('<') {
        let start = at + offset;
        let rest = &props[start..];
        at = start + 1;
        if let Some(comment) = rest.strip_prefix("<!--") {
            let end = comment
                .find("-->")
                .with_context(|| format!("{DOTNET_PROPS} has a comment that does not end"))?;
            at = start + "<!--".len() + end + "-->".len();
        } else if starts_with_ignore_case(rest, "<Version>") {
            let value_start = start + "<Version>".len();
            let value_end = props[value_start..]
                .find('<')
                .map_or(props.len(), |offset| value_start + offset);
            if value_start == value_end || !starts_with_ignore_case(&props[value_end..], "</Version>") {
                anyhow::bail!("{DOTNET_PROPS} has a <Version> element that is not <Version>X.Y.Z</Version>");
            }
            found.push((value_start, value_end));
            at = value_end + "</Version>".len();
        } else if starts_with_ignore_case(rest, "<Version")
            && rest["<Version".len()..].starts_with(|c: char| c.is_whitespace() || c == '/')
        {
            anyhow::bail!("{DOTNET_PROPS} has a <Version> element with attributes or no value");
        }
    }
    match found[..] {
        [range] => Ok(range),
        [] => anyhow::bail!("{DOTNET_PROPS} has no <Version> element"),
        _ => anyhow::bail!("{DOTNET_PROPS} has more than one <Version> element"),
    }
}

/// The value of the one `<Version>` element of `props`.
fn props_version(props: &str) -> anyhow::Result<&str> {
    let (start, end) = version_range(props)?;
    Ok(&props[start..end])
}

/// `props` with the value of its one `<Version>` element replaced, the rest of
/// the file kept byte for byte.
fn set_props_version(props: &str, value: &str) -> anyhow::Result<String> {
    let (start, end) = version_range(props)?;
    Ok(format!("{}{value}{}", &props[..start], &props[end..]))
}

/// Whether `text` is a PEP 440 version in normal form:
/// `[N!]N(.N)*[{a|b|rc}N][.postN][.devN][+local]`.
fn is_pep440(text: &str) -> bool {
    Pep440::parse(text).is_some()
}

/// A PEP 440 version in normal form, ordered as PEP 440 orders versions
/// ("Summary of permitted suffixes and relative ordering"): by epoch, then
/// release (trailing zeros ignored, `1.0 == 1.0.0`), then a dev release of
/// the bare release before its pre-releases (`1.0.dev1 < 1.0a1`), the
/// pre-releases `a < b < rc` before the release, a post-release after it,
/// and a dev release before what it is a dev release of
/// (`1.0a1.dev1 < 1.0a1`, `1.0.post1.dev1 < 1.0.post1`). A local label is
/// not compared: `1.0+x` neither lowers nor raises `1.0`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Pep440 {
    epoch: u64,
    /// The release, trailing zeros removed.
    release: Vec<u64>,
    /// `(0, ..)` for a dev release of the bare release, `(1, stage, n)` for a
    /// pre-release, `(2, ..)` for none.
    pre: (u8, u8, u64),
    /// `None`, the lowest, when not a post-release.
    post: Option<u64>,
    /// `(0, n)` for `.devN`, `(1, 0)`, the highest, for none.
    dev: (u8, u64),
}

impl Pep440 {
    fn parse(text: &str) -> Option<Self> {
        let (public, local) = match text.split_once('+') {
            Some((public, local)) => (public, Some(local)),
            None => (text, None),
        };
        let local_ok = local.is_none_or(|local| {
            local
                .split('.')
                .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()))
        });
        if !local_ok {
            return None;
        }
        let (epoch, public) = match public.split_once('!') {
            Some((epoch, rest)) => (number(epoch)?, rest),
            None => (0, public),
        };
        // The release runs up to the first letter, less the dot of `.post` or `.dev`.
        let letters = public.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(public.len());
        let release_text = public[..letters].strip_suffix('.').unwrap_or(&public[..letters]);
        let mut release = release_text.split('.').map(number).collect::<Option<Vec<u64>>>()?;
        while release.len() > 1 && release.last() == Some(&0) {
            release.pop();
        }
        let (pre, rest) = segment(&public[release_text.len()..], &["a", "b", "rc"])?;
        let (post, rest) = segment(rest, &[".post"])?;
        let (dev, rest) = segment(rest, &[".dev"])?;
        if !rest.is_empty() {
            return None;
        }
        let pre = match (pre, post, dev) {
            (Some((stage, n)), _, _) => (1, stage as u8, n),
            (None, None, Some(_)) => (0, 0, 0),
            (None, _, _) => (2, 0, 0),
        };
        Some(Pep440 {
            epoch,
            release,
            pre,
            post: post.map(|(_, n)| n),
            dev: dev.map_or((1, 0), |(_, n)| (0, n)),
        })
    }
}

/// A leading `<label>N` of `rest`, for the first of `labels` it starts with,
/// as the label's index and `N`, and what follows it; `(None, rest)` when it
/// starts with none, `None` when the label has no number after it.
fn segment<'a>(rest: &'a str, labels: &[&str]) -> Option<(Option<(usize, u64)>, &'a str)> {
    let Some((index, after)) = labels
        .iter()
        .enumerate()
        .find_map(|(index, label)| Some((index, rest.strip_prefix(label)?)))
    else {
        return Some((None, rest));
    };
    let digits = after.find(|c: char| !c.is_ascii_digit()).unwrap_or(after.len());
    Some((Some((index, number(&after[..digits])?)), &after[digits..]))
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

/// The value of the module-level `__version__ = "..."` of `text`.
fn module_version(text: &str) -> Option<&str> {
    text.lines()
        .filter(|line| line.starts_with("__version__"))
        .find_map(|line| quoted_value(line, "__version__").map(|(start, end)| &line[start..end]))
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

    /// PEP 440's ordering, each version below the next.
    #[test]
    fn pep440_versions_compare_in_pep440_order() {
        let ordered = [
            "0.2.0b1",
            "0.2.0",
            "1.0.dev0",
            "1.0.dev1",
            "1.0a1.dev1",
            "1.0a1",
            "1.0a1.post1.dev1",
            "1.0a1.post1",
            "1.0a2",
            "1.0b1",
            "1.0rc1",
            "1.0rc10",
            "1.0",
            "1.0.post1.dev1",
            "1.0.post1",
            "1.0.post2",
            "1.0.1",
            "1.1.dev1",
            "1.10",
            "4.4.0b1",
            "4.4.0",
            "1!0.1",
        ];
        for pair in ordered.windows(2) {
            let (low, high) = (Pep440::parse(pair[0]).unwrap(), Pep440::parse(pair[1]).unwrap());
            assert!(low < high, "{} < {}", pair[0], pair[1]);
        }
        // Trailing zeros and a local label do not count.
        for (a, b) in [
            ("1.0", "1.0.0"),
            ("1", "1.0.0.0"),
            ("4.4.0b1", "4.4b1"),
            ("1.0", "1.0+cfk.1"),
        ] {
            assert_eq!(Pep440::parse(a).unwrap(), Pep440::parse(b).unwrap(), "{a} == {b}");
        }
    }

    #[test]
    fn a_lower_python_version_is_refused() {
        let check = |current: &str, python: &str| {
            ensure_not_lowered("python/pyproject.toml", current, python, &Pep440::parse(python).unwrap())
        };
        // The crate's 0.2.0-beta.1 in PEP 440 form would lower Python's 4.4.0b1.
        let error = check("4.4.0b1", &v("0.2.0-beta.1").pep440()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "python/pyproject.toml is at 4.4.0b1: the Python version 0.2.0b1 would lower it. Python versions only \
             move up; pass --python-version with a version not below 4.4.0b1"
        );
        // An explicit lower one is refused too.
        assert!(check("4.4.0", "4.4.0rc1").is_err());
        assert!(check("4.4.0.post1", "4.4.0").is_err());
        // The same or a higher one is accepted.
        check("4.4.0b1", "4.4.0b1").unwrap();
        check("4.4.0b1", "4.4.0b2").unwrap();
        check("4.4.0b1", "4.4.0").unwrap();
        check("4.4.0b1", "4.4.0.post1").unwrap();
        // A current version that is not PEP 440 is an error, not a pass.
        let error = check("four", "4.4.0").unwrap_err();
        assert_eq!(
            error.to_string(),
            "python/pyproject.toml has `four`, not a PEP 440 version in normal form"
        );
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
        assert_eq!(module_version(module), Some("0.2.0"));
        assert_eq!(module_version("x = 1\n"), None);
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
            let current = table_value(&text, "project", "version").expect("project version");
            assert!(Pep440::parse(current).is_some(), "{pyproject}: {current}");
        }
        for module in PYTHON_VERSION_MODULES {
            let text = read(&root.join(module)).expect("module");
            assert!(set_module_version(&text, "1.0.0").is_some(), "{module}");
            let current = module_version(&text).expect("module version");
            assert!(Pep440::parse(current).is_some(), "{module}: {current}");
        }
        let props = read(&root.join(DOTNET_PROPS)).expect("props");
        let current = props_version(&props).expect("one <Version>");
        parse_dotnet_version(current).expect("a semver .NET version");
        let edited = set_props_version(&props, "99.0.0").expect("settable");
        assert_eq!(props_version(&edited).unwrap(), "99.0.0");
        assert_eq!(
            edited,
            props.replacen(&format!("<Version>{current}</Version>"), "<Version>99.0.0</Version>", 1)
        );
    }

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    #[test]
    fn flags_follow_the_version_in_either_order() {
        let args = strings(&[
            "0.3.0",
            "--dotnet-version",
            "4.4.0-beta.1",
            "--python-version",
            "4.4.0b1",
        ]);
        assert_eq!(
            parse_args(&args).unwrap(),
            Args { version: "0.3.0", python: Some("4.4.0b1"), dotnet: Some("4.4.0-beta.1") }
        );
        let args = strings(&["0.3.0", "--python-version", "4.4.0b1"]);
        assert_eq!(
            parse_args(&args).unwrap(),
            Args { version: "0.3.0", python: Some("4.4.0b1"), dotnet: None }
        );
        let args = strings(&["0.3.0"]);
        assert_eq!(
            parse_args(&args).unwrap(),
            Args { version: "0.3.0", python: None, dotnet: None }
        );
        for bad in [
            &[][..],
            &["--dotnet-version", "4.4.0"][..],
            &["0.3.0", "--dotnet-version"][..],
            &["0.3.0", "--net-version", "4.4.0"][..],
            &["0.3.0", "4.4.0"][..],
            &["0.3.0", "--dotnet-version", "4.4.0", "--dotnet-version", "4.4.1"][..],
        ] {
            assert!(parse_args(&strings(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn dotnet_version_is_the_semver_form_only() {
        for text in [
            "4.4.0",
            "4.4.0-alpha.1",
            "4.4.0-beta.2",
            "4.4.0-dev.3",
            "4.4.0-rc.0",
            "10.0.100",
        ] {
            assert_eq!(parse_dotnet_version(text).unwrap().to_string(), text);
        }
        for text in [
            "4.4.0b1",
            "4.4.0rc1",
            "4.4.0.dev1",
            "4.4",
            "v4.4.0",
            "4.4.0-beta",
            "4.4.0+abc",
            "4.4.0-pre.1",
            "",
        ] {
            let error = parse_dotnet_version(text).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!(
                    "`{text}` is not a .NET package version: X.Y.Z, or X.Y.Z-alpha.N, -beta.N, -dev.N or -rc.N, such \
                     as 4.4.0-beta.1"
                )
            );
        }
    }

    #[test]
    fn a_lower_dotnet_version_is_refused() {
        let check = |current: &str, new: &str| ensure_dotnet_not_lowered(current, v(new));
        let error = check("4.4.0-beta.1", "4.4.0-alpha.2").unwrap_err();
        assert_eq!(
            error.to_string(),
            "dotnet/Directory.Build.props is at 4.4.0-beta.1: the .NET version 4.4.0-alpha.2 would lower it. .NET \
             versions only move up; pass --dotnet-version with a version not below 4.4.0-beta.1"
        );
        assert!(check("4.4.0", "4.4.0-rc.9").is_err());
        assert!(check("4.4.1", "4.4.0").is_err());
        // Semver order, as NuGet compares: dev is above beta.
        check("4.4.0-beta.1", "4.4.0-dev.1").unwrap();
        check("4.4.0-beta.1", "4.4.0-beta.1").unwrap();
        check("4.4.0-rc.1", "4.4.0").unwrap();
        check("4.4.0", "4.5.0-alpha.0").unwrap();
        let error = check("4.4.0b1", "4.4.0").unwrap_err();
        assert_eq!(
            error.to_string(),
            "dotnet/Directory.Build.props has `4.4.0b1`, not a version this command writes"
        );
    }

    #[test]
    fn props_version_is_the_one_version_element() {
        let props = "\
<Project>
  <!-- The one
       <Version> element: see below.
  <Version>0.0.0</Version>
  -->
  <PropertyGroup>
    <Version>4.4.0-alpha.1</Version> <!-- <Version>0.0.1</Version> -->
  </PropertyGroup>
  <PropertyGroup>
    <VersionPrefix>1.0.0</VersionPrefix>
    <FileVersion>1.0.0.0</FileVersion>
  </PropertyGroup>
</Project>
";
        assert_eq!(props_version(props).unwrap(), "4.4.0-alpha.1");
        let edited = set_props_version(props, "4.4.0-beta.1").unwrap();
        assert_eq!(
            edited,
            props.replace("<Version>4.4.0-alpha.1</Version>", "<Version>4.4.0-beta.1</Version>")
        );
        let crlf = props.replace('\n', "\r\n");
        assert_eq!(
            set_props_version(&crlf, "4.4.0-beta.1").unwrap(),
            crlf.replace("<Version>4.4.0-alpha.1</Version>", "<Version>4.4.0-beta.1</Version>")
        );
        let lower = props.replace("<Version>4.4.0-alpha.1</Version>", "<version>4.4.0-alpha.1</VERSION>");
        assert_eq!(props_version(&lower).unwrap(), "4.4.0-alpha.1");

        let error = |props: &str| props_version(props).unwrap_err().to_string();
        assert_eq!(error("<Project/>\n"), "dotnet/Directory.Build.props has no <Version> element");
        assert_eq!(
            error("<Project><!-- <Version>4.4.0</Version> --></Project>"),
            "dotnet/Directory.Build.props has no <Version> element"
        );
        let two = props.replace("<FileVersion>1.0.0.0</FileVersion>", "<Version>1.0.0</Version>");
        assert_eq!(error(&two), "dotnet/Directory.Build.props has more than one <Version> element");
        assert!(set_props_version(&two, "4.4.0").is_err());
        for attributes in [
            "<Version Condition=\"x\">1.0.0</Version>",
            "<Version/>",
            "<Version\n>1.0.0</Version>",
        ] {
            assert_eq!(
                error(&props.replace("<FileVersion>1.0.0.0</FileVersion>", attributes)),
                "dotnet/Directory.Build.props has a <Version> element with attributes or no value",
                "{attributes}"
            );
        }
        for value in [
            "<Version></Version>",
            "<Version>1.0.0<!-- x --></Version>",
            "<Version>1.0.0",
        ] {
            assert_eq!(
                error(&props.replace("<FileVersion>1.0.0.0</FileVersion>", value)),
                "dotnet/Directory.Build.props has a <Version> element that is not <Version>X.Y.Z</Version>",
                "{value}"
            );
        }
        assert_eq!(
            error("<Project><Version>4.4.0</Version><!-- no end"),
            "dotnet/Directory.Build.props has a comment that does not end"
        );
    }
}
