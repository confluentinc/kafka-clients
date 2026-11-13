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
//! `--javascript-version <semver>` sets the version of the Node.js client's
//! npm packages: `"version"` in [`JAVASCRIPT_PACKAGE`], both of its copies in
//! [`JAVASCRIPT_LOCK`], and `"version"` in the `package.json` of every
//! platform package under [`JAVASCRIPT_PLATFORM_DIR`], which the published
//! main package pins at exactly that version in its `optionalDependencies`
//! (`.semaphore/check-npm.py manifest` writes the pins from them, as a pin
//! committed in the repository's `package.json` would break `npm ci` once the
//! version is on the registry). It is in the semver form only and 4.0.0 or
//! higher (the npm name is shared with an earlier client whose versions are
//! 1.x); it alone may also be a `-preview.N`, between dev and rc. It is a
//! fourth line, like .NET's: it moves only when given, and a version lower
//! than the current one is refused. Its `MAJOR.MINOR` is the Apache Kafka
//! release of [`KAFKA_GRADLE_PROPERTIES`], and its `PATCH` follows
//! `rfc/versioning.md`: `AK_patch * 100 + core_patch * 10 + binding_patch`,
//! the hundreds that Kafka release's patch, and a digit back at 0 whenever a
//! digit to its left moves up.
//!
//! What is released together:
//!
//!   - every member of the Rust workspace at the version of `confluent-kafka`
//!     (a member on a version of its own, `ffi-macros`, keeps it), and
//!     `Cargo.lock` with them;
//!   - the Python packages in [`PYPROJECTS`] and the `__version__` of
//!     [`PYTHON_VERSION_MODULES`];
//!   - with `--dotnet-version`, the .NET package;
//!   - with `--javascript-version`, the Node.js client's npm package.

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

/// The Node.js client's package manifest, whose top-level `"version"` is its
/// version.
const JAVASCRIPT_PACKAGE: &str = "javascript/package.json";

/// The Node.js client's lockfile, which repeats the version at its top level
/// and in `packages[""]`.
const JAVASCRIPT_LOCK: &str = "javascript/package-lock.json";

/// The lowest major version of the Node.js client: below it, the npm name's
/// versions are the earlier librdkafka-based client's.
const JAVASCRIPT_MIN_MAJOR: u64 = 4;

/// The folder of the Node.js client's platform packages: one folder each,
/// holding the `package.json` it is published with.
const JAVASCRIPT_PLATFORM_DIR: &str = "javascript/npm";

/// The Apache Kafka release the clients are built from: its `version=` line
/// gives the JavaScript version's `MAJOR.MINOR` and the hundreds of its
/// `PATCH` (`rfc/versioning.md`).
const KAFKA_GRADLE_PROPERTIES: &str = "kafka/gradle.properties";

const USAGE: &str =
    "usage: cargo xtask bump-version X.Y.Z[-alpha.N|-beta.N|-dev.N|-rc.N] [--python-version PEP440_VERSION] \
     [--dotnet-version SEMVER] [--javascript-version SEMVER]\n\
     The crates get the semver version; the Python packages the PEP 440 version, by default the\n\
     crate's in PEP 440 form. Python versions are their own line and only move up: a version below\n\
     a package's current one is refused, so pass --python-version while the crate's is lower.\n\
     --dotnet-version sets the .NET package's version (dotnet/Directory.Build.props), in the semver\n\
     form; without it that version is left as it is. It only moves up too.\n\
     --javascript-version sets the Node.js packages' version (javascript/package.json, its\n\
     lockfile and javascript/npm/<platform>/package.json), in the semver form and 4.0.0 or higher,\n\
     -preview.N allowed too; without it that version is left as it is. It only moves up too, its\n\
     MAJOR.MINOR is the Apache Kafka release in kafka/, and its PATCH is\n\
     AK_patch * 100 + core_patch * 10 + binding_patch (rfc/versioning.md).";

/// A pre-release stage, in semver precedence order (its labels compare as
/// ASCII text).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    Alpha,
    Beta,
    Dev,
    /// The JavaScript package's preview releases only (`4.4.0-preview.1`):
    /// no other version takes it.
    Preview,
    Rc,
}

impl Stage {
    /// The stages of every version but the JavaScript package's.
    const ALL: [Stage; 4] = [Stage::Alpha, Stage::Beta, Stage::Dev, Stage::Rc];
    /// The stages of the JavaScript package's version.
    const JAVASCRIPT: [Stage; 5] = [Stage::Alpha, Stage::Beta, Stage::Dev, Stage::Preview, Stage::Rc];

    fn semver(self) -> &'static str {
        match self {
            Stage::Alpha => "alpha",
            Stage::Beta => "beta",
            Stage::Dev => "dev",
            Stage::Preview => "preview",
            Stage::Rc => "rc",
        }
    }

    /// The PEP 440 label. A preview version is a JavaScript one, which is
    /// never written in PEP 440 form, so it is never asked for it.
    fn pep440(self) -> &'static str {
        match self {
            Stage::Alpha => "a",
            Stage::Beta => "b",
            Stage::Dev => ".dev",
            Stage::Preview => unreachable!("a preview version is the JavaScript package's, never a Python one"),
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
        Self::parse_stages(text, &Stage::ALL)
    }

    /// [`Self::parse`] with the pre-release stages `stages` (only the
    /// JavaScript package's version takes `-preview.N`).
    fn parse_stages(text: &str, stages: &[Stage]) -> Option<Self> {
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
                let stage = stages.iter().copied().find(|s| s.semver() == label)?;
                (stage, n)
            },
            None => {
                let (label, n) = suffix.split_at(suffix.find(|c: char| c.is_ascii_digit())?);
                let stage = stages.iter().copied().find(|s| *s != Stage::Preview && s.pep440() == label)?;
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

/// The arguments: the crate version, then `--python-version`,
/// `--dotnet-version` and `--javascript-version`, each at most once, in any
/// order.
#[derive(Debug, PartialEq, Eq)]
struct Args<'a> {
    version: &'a str,
    python: Option<&'a str>,
    dotnet: Option<&'a str>,
    javascript: Option<&'a str>,
}

fn parse_args(args: &[String]) -> anyhow::Result<Args<'_>> {
    let Some((version, mut flags)) = args.split_first() else {
        anyhow::bail!("{USAGE}");
    };
    if version.starts_with("--") {
        anyhow::bail!("{USAGE}");
    }
    let mut parsed = Args { version, python: None, dotnet: None, javascript: None };
    while let [flag, value, rest @ ..] = flags {
        let slot = match flag.as_str() {
            "--python-version" => &mut parsed.python,
            "--dotnet-version" => &mut parsed.dotnet,
            "--javascript-version" => &mut parsed.javascript,
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
    let Args { version: text, python: python_override, dotnet, javascript } = parse_args(args)?;
    let new = PackageVersion::parse(text).with_context(|| format!("`{text}` is not a version; {USAGE}"))?;
    let dotnet = dotnet.map(parse_dotnet_version).transpose()?;
    let javascript = javascript.map(parse_javascript_version).transpose()?;
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
    let dotnet_note = dotnet.map(|dotnet| format!(", .NET: {dotnet}")).unwrap_or_default();
    let javascript_note = javascript
        .map(|javascript| format!(", JavaScript: {javascript}"))
        .unwrap_or_default();
    println!("🔖 Bumping {current} → {new} (Python: {python}{dotnet_note}{javascript_note})");

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
    match javascript {
        Some(javascript) => {
            let package_path = root.join(JAVASCRIPT_PACKAGE);
            let lock_path = root.join(JAVASCRIPT_LOCK);
            let package = read(&package_path)?;
            let lock = read(&lock_path)?;
            let current = javascript_version(&package, &lock)?;
            ensure_javascript_not_lowered(current, javascript)?;
            let kafka = kafka_release(&read(&root.join(KAFKA_GRADLE_PROPERTIES)).with_context(|| {
                format!("the JavaScript version follows the Apache Kafka release of {KAFKA_GRADLE_PROPERTIES}: run `make submodules` first")
            })?)?;
            let current_version = parse_javascript_version(current).expect("checked by ensure_javascript_not_lowered");
            ensure_javascript_patch_digits(current_version, javascript, kafka)?;
            let value = javascript.to_string();
            edits.push((
                package_path,
                set_json_strings(JAVASCRIPT_PACKAGE, &package, &[&["version"]], &value)?,
            ));
            edits.push((
                lock_path,
                set_json_strings(JAVASCRIPT_LOCK, &lock, &[&["version"], &["packages", "", "version"]], &value)?,
            ));
            for platform in javascript_platform_manifests(&root)? {
                let file = relative(&root, &platform);
                let manifest = read(&platform)?;
                let (start, end) = json_string_range(&file, &manifest, &["version"])?;
                if manifest[start..end] != *current {
                    anyhow::bail!(
                        "{file} has `{}`, not `{current}`, the version of {JAVASCRIPT_PACKAGE}: the platform packages \
                         are released with the main package, at its version",
                        &manifest[start..end]
                    );
                }
                edits.push((platform, set_json_strings(&file, &manifest, &[&["version"]], &value)?));
            }
        },
        None => {
            println!("   {JAVASCRIPT_PACKAGE} is not changed: pass --javascript-version to set the JavaScript version")
        },
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

/// The JavaScript version given with `--javascript-version`: the semver form
/// only, as npm writes it, and 4.0.0 or higher.
fn parse_javascript_version(text: &str) -> anyhow::Result<PackageVersion> {
    match PackageVersion::parse_stages(text, &Stage::JAVASCRIPT) {
        Some(version) if version.to_string() == text && version.major >= JAVASCRIPT_MIN_MAJOR => Ok(version),
        Some(version) if version.to_string() == text => anyhow::bail!(
            "`{text}` is below {JAVASCRIPT_MIN_MAJOR}.0.0: the versions of @confluentinc/kafka-javascript below \
             {JAVASCRIPT_MIN_MAJOR} are the earlier librdkafka-based client's"
        ),
        _ => anyhow::bail!(
            "`{text}` is not a JavaScript package version: X.Y.Z, or X.Y.Z-alpha.N, -beta.N, -dev.N, -preview.N or \
             -rc.N, such as 4.4.0-preview.1"
        ),
    }
}

/// The current JavaScript version: the one `"version"` of `package`, which
/// `lock` must repeat at its top level and in `packages[""]`.
fn javascript_version<'a>(package: &'a str, lock: &str) -> anyhow::Result<&'a str> {
    let (start, end) = json_string_range(JAVASCRIPT_PACKAGE, package, &["version"])?;
    let current = &package[start..end];
    for path in [&["version"][..], &["packages", "", "version"][..]] {
        let (start, end) = json_string_range(JAVASCRIPT_LOCK, lock, path)?;
        let locked = &lock[start..end];
        if locked != current {
            anyhow::bail!(
                "{JAVASCRIPT_LOCK} has `{locked}` at {}, not `{current}`, the version of {JAVASCRIPT_PACKAGE}; \
                 run `npm install --package-lock-only` in javascript/ first",
                json_path(path)
            );
        }
    }
    Ok(current)
}

/// Refuses to write `new` over the JavaScript version `current`, when it is
/// lower in semver order: the npm package only moves up.
fn ensure_javascript_not_lowered(current: &str, new: PackageVersion) -> anyhow::Result<()> {
    let current_version = parse_javascript_version(current)
        .with_context(|| format!("{JAVASCRIPT_PACKAGE} has `{current}`, not a version this command writes"))?;
    if new < current_version {
        anyhow::bail!(
            "{JAVASCRIPT_PACKAGE} is at {current}: the JavaScript version {new} would lower it. JavaScript versions \
             only move up; pass --javascript-version with a version not below {current}"
        );
    }
    Ok(())
}

/// The `package.json` of every platform package, one per folder of
/// [`JAVASCRIPT_PLATFORM_DIR`], in name order.
fn javascript_platform_manifests(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let dir = root.join(JAVASCRIPT_PLATFORM_DIR);
    let mut manifests = Vec::new();
    for entry in fs::read_dir(&dir).with_context(|| format!("listing {}", dir.display()))? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let manifest = entry.path().join("package.json");
            if !manifest.is_file() {
                anyhow::bail!("{} has no package.json", relative(root, &entry.path()));
            }
            manifests.push(manifest);
        }
    }
    if manifests.is_empty() {
        anyhow::bail!("{JAVASCRIPT_PLATFORM_DIR} has no platform package");
    }
    manifests.sort();
    Ok(manifests)
}

/// The `MAJOR.MINOR.PATCH` of the Apache Kafka release in the
/// `gradle.properties` text `properties`, from its `version=` line.
fn kafka_release(properties: &str) -> anyhow::Result<(u64, u64, u64)> {
    let version = properties
        .lines()
        .find_map(|line| line.trim().strip_prefix("version="))
        .with_context(|| format!("{KAFKA_GRADLE_PROPERTIES} has no version= line"))?
        .trim();
    let mut parts = version.split('.');
    match (
        parts.next().and_then(number),
        parts.next().and_then(number),
        parts.next().and_then(number),
        parts.next(),
    ) {
        (Some(major), Some(minor), Some(patch), None) => Ok((major, minor, patch)),
        _ => anyhow::bail!("{KAFKA_GRADLE_PROPERTIES} has version={version}, not an Apache Kafka release X.Y.Z"),
    }
}

/// Refuses a JavaScript version `new` whose digits do not follow
/// `rfc/versioning.md`, given the `current` one and the Apache Kafka release
/// `kafka`: `MAJOR.MINOR` is Kafka's, and `PATCH` is `AK_patch * 100 +
/// core_patch * 10 + binding_patch`, the hundreds being Kafka's patch, and
/// each digit back at 0 when a digit to its left moves up.
fn ensure_javascript_patch_digits(
    current: PackageVersion,
    new: PackageVersion,
    kafka: (u64, u64, u64),
) -> anyhow::Result<()> {
    let (kafka_major, kafka_minor, kafka_patch) = kafka;
    if (new.major, new.minor) != (kafka_major, kafka_minor) {
        anyhow::bail!(
            "{new} is not on the Apache Kafka line: its MAJOR.MINOR must be {kafka_major}.{kafka_minor}, the release in \
             kafka/ (rfc/versioning.md)"
        );
    }
    let digits = |patch: u64| (patch / 100, patch / 10 % 10, patch % 10);
    let (ak, core, binding) = digits(new.patch);
    if ak != kafka_patch {
        anyhow::bail!(
            "{new} has the Apache Kafka patch {ak} (PATCH = AK_patch * 100 + core_patch * 10 + binding_patch), but \
             kafka/ is {kafka_major}.{kafka_minor}.{kafka_patch}: its PATCH must be in {}..={}",
            kafka_patch * 100,
            kafka_patch * 100 + 99
        );
    }
    if (current.major, current.minor) == (new.major, new.minor) {
        let (current_ak, current_core, _) = digits(current.patch);
        if ak > current_ak && (core, binding) != (0, 0) {
            anyhow::bail!(
                "{new} moves the Apache Kafka patch from {current_ak} to {ak}, which resets the core and binding \
                 digits: use {}.{}.{}",
                new.major,
                new.minor,
                ak * 100
            );
        }
        if ak == current_ak && core > current_core && binding != 0 {
            anyhow::bail!(
                "{new} moves the core patch from {current_core} to {core}, which resets the binding digit: use \
                 {}.{}.{}",
                new.major,
                new.minor,
                ak * 100 + core * 10
            );
        }
    }
    Ok(())
}

/// A JSON path as a message shows it: `"version"`, `packages[""].version`.
fn json_path(path: &[&str]) -> String {
    match path {
        [key] => format!("\"{key}\""),
        _ => path.iter().enumerate().fold(String::new(), |mut out, (i, key)| {
            if i == 0 {
                out.push_str(key);
            } else if i + 1 == path.len() {
                out.push('.');
                out.push_str(key);
            } else {
                out.push_str(&format!("[\"{key}\"]"));
            }
            out
        }),
    }
}

/// `text` with the string at each of `paths` set to `value`, every other byte
/// kept.
fn set_json_strings(file: &str, text: &str, paths: &[&[&str]], value: &str) -> anyhow::Result<String> {
    let mut ranges = paths
        .iter()
        .map(|path| json_string_range(file, text, path))
        .collect::<anyhow::Result<Vec<_>>>()?;
    ranges.sort_unstable();
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    for (start, end) in ranges {
        out.push_str(&text[at..start]);
        out.push_str(value);
        at = end;
    }
    out.push_str(&text[at..]);
    Ok(out)
}

/// The byte range of the contents of the one string at `path` (object keys
/// from the root) in the JSON document `text`, between its quotes. It must be
/// a plain string, with no escape, as a version is.
fn json_string_range(file: &str, text: &str, path: &[&str]) -> anyhow::Result<(usize, usize)> {
    let mut scanner = JsonScanner { text: text.as_bytes(), pos: 0, path: Vec::new(), want: path, found: Vec::new() };
    scanner
        .document()
        .with_context(|| format!("{file} is not JSON this command can read"))?;
    match scanner.found[..] {
        [Some(range)] => Ok(range),
        [None] => anyhow::bail!("{file} has {} that is not a plain string", json_path(path)),
        [] => anyhow::bail!("{file} has no {}", json_path(path)),
        _ => anyhow::bail!("{file} has more than one {}", json_path(path)),
    }
}

/// A JSON reader that only finds the values at one path: for each, the range
/// of a plain string's contents, or None for any other value.
struct JsonScanner<'a, 'p> {
    text: &'a [u8],
    pos: usize,
    path: Vec<String>,
    want: &'p [&'p str],
    found: Vec<Option<(usize, usize)>>,
}

impl JsonScanner<'_, '_> {
    fn document(&mut self) -> anyhow::Result<()> {
        self.value()?;
        self.skip_whitespace();
        if self.pos != self.text.len() {
            anyhow::bail!("it has text after its value, at byte {}", self.pos);
        }
        Ok(())
    }

    fn skip_whitespace(&mut self) {
        while self.text.get(self.pos).is_some_and(|b| b" \t\r\n".contains(b)) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, byte: u8) -> anyhow::Result<()> {
        self.skip_whitespace();
        if self.text.get(self.pos) != Some(&byte) {
            anyhow::bail!("expected `{}` at byte {}", byte as char, self.pos);
        }
        self.pos += 1;
        Ok(())
    }

    /// Reads a string; returns its contents' range and whether it has an escape.
    fn string(&mut self) -> anyhow::Result<(usize, usize, bool)> {
        self.expect(b'"')?;
        let start = self.pos;
        let mut escaped = false;
        loop {
            match self.text.get(self.pos) {
                None => anyhow::bail!("a string does not end"),
                Some(b'"') => break,
                Some(b'\\') => {
                    escaped = true;
                    self.pos += 2;
                },
                Some(_) => self.pos += 1,
            }
        }
        let end = self.pos;
        self.pos += 1;
        Ok((start, end, escaped))
    }

    fn value(&mut self) -> anyhow::Result<()> {
        self.skip_whitespace();
        let wanted = self.path.iter().map(String::as_str).eq(self.want.iter().copied());
        match self.text.get(self.pos) {
            Some(b'{') => {
                if wanted {
                    self.found.push(None);
                }
                self.pos += 1;
                self.skip_whitespace();
                if self.text.get(self.pos) == Some(&b'}') {
                    self.pos += 1;
                    return Ok(());
                }
                loop {
                    let (start, end, _) = self.string()?;
                    let key = String::from_utf8_lossy(&self.text[start..end]).into_owned();
                    self.expect(b':')?;
                    self.path.push(key);
                    self.value()?;
                    self.path.pop();
                    self.skip_whitespace();
                    match self.text.get(self.pos) {
                        Some(b',') => self.pos += 1,
                        Some(b'}') => {
                            self.pos += 1;
                            return Ok(());
                        },
                        _ => anyhow::bail!("expected `,` or `}}` at byte {}", self.pos),
                    }
                }
            },
            Some(b'[') => {
                if wanted {
                    self.found.push(None);
                }
                self.pos += 1;
                self.skip_whitespace();
                if self.text.get(self.pos) == Some(&b']') {
                    self.pos += 1;
                    return Ok(());
                }
                // An array's elements are on no key path.
                self.path.push("[]".to_string());
                loop {
                    self.value()?;
                    self.skip_whitespace();
                    match self.text.get(self.pos) {
                        Some(b',') => self.pos += 1,
                        Some(b']') => {
                            self.pos += 1;
                            self.path.pop();
                            return Ok(());
                        },
                        _ => anyhow::bail!("expected `,` or `]` at byte {}", self.pos),
                    }
                }
            },
            Some(b'"') => {
                let (start, end, escaped) = self.string()?;
                if wanted {
                    self.found.push((!escaped).then_some((start, end)));
                }
                Ok(())
            },
            Some(_) => {
                if wanted {
                    self.found.push(None);
                }
                let start = self.pos;
                while self.text.get(self.pos).is_some_and(|b| !b" \t\r\n,]}".contains(b)) {
                    self.pos += 1;
                }
                let literal = &self.text[start..self.pos];
                let number = literal.iter().all(|b| b"+-.0123456789eE".contains(b)) && !literal.is_empty();
                if !(number || [&b"true"[..], b"false", b"null"].contains(&literal)) {
                    anyhow::bail!("unexpected `{}` at byte {start}", String::from_utf8_lossy(literal));
                }
                Ok(())
            },
            None => anyhow::bail!("it ends where a value is expected"),
        }
    }
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
        let package = read(&root.join(JAVASCRIPT_PACKAGE)).expect("package.json");
        let lock = read(&root.join(JAVASCRIPT_LOCK)).expect("package-lock.json");
        let current = javascript_version(&package, &lock).expect("one version, the lockfile's too");
        parse_javascript_version(current).expect("a semver JavaScript version");
        let edited = set_json_strings(JAVASCRIPT_PACKAGE, &package, &[&["version"]], "99.0.0").expect("settable");
        assert_eq!(
            edited,
            package.replacen(&format!("\"version\": \"{current}\""), "\"version\": \"99.0.0\"", 1)
        );
        let edited_lock =
            set_json_strings(JAVASCRIPT_LOCK, &lock, &[&["version"], &["packages", "", "version"]], "99.0.0")
                .expect("settable");
        assert_eq!(javascript_version(&edited, &edited_lock).unwrap(), "99.0.0");
        assert_eq!(edited_lock.matches("\"version\": \"99.0.0\"").count(), 2);
        assert_eq!(
            edited_lock.replace("\"version\": \"99.0.0\"", &format!("\"version\": \"{current}\"")),
            lock
        );
        let platforms = javascript_platform_manifests(&root).expect("the platform packages");
        assert!(!platforms.is_empty());
        for platform in platforms {
            let file = relative(&root, &platform);
            let manifest = read(&platform).expect("a platform package.json");
            let (start, end) = json_string_range(&file, &manifest, &["version"]).expect("one version");
            assert_eq!(&manifest[start..end], current, "{file} is at the main package's version");
            let edited = set_json_strings(&file, &manifest, &[&["version"]], "99.0.0").expect("settable");
            assert_eq!(
                edited,
                manifest.replacen(&format!("\"version\": \"{current}\""), "\"version\": \"99.0.0\"", 1)
            );
        }
        let kafka = kafka_release(&read(&root.join(KAFKA_GRADLE_PROPERTIES)).expect("kafka/ is checked out"))
            .expect("a Kafka release");
        let current = parse_javascript_version(current).expect("a JavaScript version");
        assert_eq!(
            (current.major, current.minor),
            (kafka.0, kafka.1),
            "the JavaScript version is on Kafka's line"
        );
        ensure_javascript_patch_digits(current, current, kafka).expect("the current version follows the RFC");
    }

    #[test]
    fn kafka_release_reads_the_version_line() {
        assert_eq!(
            kafka_release("group=org.apache.kafka\nversion=4.4.0\nscalaVersion=2.13\n").unwrap(),
            (4, 4, 0)
        );
        assert_eq!(kafka_release("version=4.3.1\n").unwrap(), (4, 3, 1));
        assert!(kafka_release("version=4.4.0-SNAPSHOT\n").is_err());
        assert!(kafka_release("group=org.apache.kafka\n").is_err());
    }

    #[test]
    fn javascript_patch_digits_follow_the_versioning_rfc() {
        let v = |text: &str| parse_javascript_version(text).expect(text);
        let check = |current: &str, new: &str, kafka| ensure_javascript_patch_digits(v(current), v(new), kafka);
        // rfc/versioning.md's worked examples, on Apache Kafka 4.3.0 and 4.3.1.
        for (current, new) in [("4.3.0", "4.3.1"), ("4.3.1", "4.3.10"), ("4.3.0-preview.1", "4.3.0")] {
            check(current, new, (4, 3, 0)).unwrap_or_else(|e| panic!("{current} -> {new}: {e}"));
        }
        for (current, new) in [("4.3.10", "4.3.100"), ("4.3.120", "4.3.123"), ("4.0.101", "4.0.200")] {
            let kafka = (v(new).major, v(new).minor, v(new).patch / 100);
            check(current, new, kafka).unwrap_or_else(|e| panic!("{current} -> {new}: {e}"));
        }
        // The preview, and GA on the next Kafka line.
        check("4.4.0-preview.1", "4.4.0-preview.2", (4, 4, 0)).unwrap();
        check("4.4.0-preview.2", "4.5.0", (4, 5, 0)).unwrap();
        assert_eq!(
            check("4.4.0", "4.5.0", (4, 4, 0)).unwrap_err().to_string(),
            "4.5.0 is not on the Apache Kafka line: its MAJOR.MINOR must be 4.4, the release in kafka/ (rfc/versioning.md)"
        );
        assert_eq!(
            check("4.3.0", "4.3.100", (4, 3, 0)).unwrap_err().to_string(),
            "4.3.100 has the Apache Kafka patch 1 (PATCH = AK_patch * 100 + core_patch * 10 + binding_patch), but \
             kafka/ is 4.3.0: its PATCH must be in 0..=99"
        );
        assert_eq!(
            check("4.3.12", "4.3.110", (4, 3, 1)).unwrap_err().to_string(),
            "4.3.110 moves the Apache Kafka patch from 0 to 1, which resets the core and binding digits: use 4.3.100"
        );
        assert_eq!(
            check("4.3.12", "4.3.21", (4, 3, 0)).unwrap_err().to_string(),
            "4.3.21 moves the core patch from 1 to 2, which resets the binding digit: use 4.3.20"
        );
        check("4.3.12", "4.3.13", (4, 3, 0)).unwrap();
        check("4.3.12", "4.3.20", (4, 3, 0)).unwrap();
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
            Args {
                version: "0.3.0",
                python: Some("4.4.0b1"),
                dotnet: Some("4.4.0-beta.1"),
                javascript: None
            }
        );
        let args = strings(&["0.3.0", "--python-version", "4.4.0b1"]);
        assert_eq!(
            parse_args(&args).unwrap(),
            Args { version: "0.3.0", python: Some("4.4.0b1"), dotnet: None, javascript: None }
        );
        let args = strings(&["0.3.0"]);
        assert_eq!(
            parse_args(&args).unwrap(),
            Args { version: "0.3.0", python: None, dotnet: None, javascript: None }
        );
        let args = strings(&[
            "0.3.0",
            "--javascript-version",
            "4.4.0-beta.2",
            "--dotnet-version",
            "4.4.0",
        ]);
        assert_eq!(
            parse_args(&args).unwrap(),
            Args {
                version: "0.3.0",
                python: None,
                dotnet: Some("4.4.0"),
                javascript: Some("4.4.0-beta.2")
            }
        );
        for bad in [
            &[][..],
            &["--dotnet-version", "4.4.0"][..],
            &["0.3.0", "--dotnet-version"][..],
            &["0.3.0", "--net-version", "4.4.0"][..],
            &["0.3.0", "4.4.0"][..],
            &["0.3.0", "--dotnet-version", "4.4.0", "--dotnet-version", "4.4.1"][..],
            &["0.3.0", "--javascript-version"][..],
            &[
                "0.3.0",
                "--javascript-version",
                "4.4.0",
                "--javascript-version",
                "4.4.1",
            ][..],
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

    #[test]
    fn javascript_version_is_the_semver_form_only_from_4() {
        for text in [
            "4.4.0",
            "4.4.0-alpha.1",
            "4.4.0-beta.2",
            "4.4.0-dev.3",
            "4.4.0-preview.1",
            "4.4.0-rc.0",
            "10.0.100",
        ] {
            assert_eq!(parse_javascript_version(text).unwrap().to_string(), text);
        }
        // A preview sorts after dev and before rc, as semver compares the labels.
        let [dev, preview, later_preview, rc] = ["4.4.0-dev.9", "4.4.0-preview.1", "4.4.0-preview.2", "4.4.0-rc.1"]
            .map(|t| parse_javascript_version(t).unwrap());
        assert!(dev < preview && preview < later_preview && later_preview < rc);
        // No other version takes it.
        assert_eq!(PackageVersion::parse("4.4.0-preview.1"), None);
        for text in [
            "4.4.0b1",
            "4.4.0.dev1",
            "4.4",
            "v4.4.0",
            "4.4.0-beta",
            "4.4.0+abc",
            "4.4.0-pre.1",
            "",
        ] {
            assert_eq!(
                parse_javascript_version(text).unwrap_err().to_string(),
                format!(
                    "`{text}` is not a JavaScript package version: X.Y.Z, or X.Y.Z-alpha.N, -beta.N, -dev.N, \
                     -preview.N or -rc.N, such as 4.4.0-preview.1"
                )
            );
        }
        for text in ["1.11.0", "3.9.9", "0.1.0-rc.1"] {
            assert_eq!(
                parse_javascript_version(text).unwrap_err().to_string(),
                format!(
                    "`{text}` is below 4.0.0: the versions of @confluentinc/kafka-javascript below 4 are the earlier \
                     librdkafka-based client's"
                )
            );
        }
    }

    #[test]
    fn a_lower_javascript_version_is_refused() {
        let check = |current: &str, new: &str| ensure_javascript_not_lowered(current, v(new));
        assert_eq!(
            check("4.4.0-beta.2", "4.4.0-beta.1").unwrap_err().to_string(),
            "javascript/package.json is at 4.4.0-beta.2: the JavaScript version 4.4.0-beta.1 would lower it. \
             JavaScript versions only move up; pass --javascript-version with a version not below 4.4.0-beta.2"
        );
        assert!(check("4.4.0", "4.4.0-rc.9").is_err());
        // Semver order, as npm compares: dev is above beta.
        check("4.4.0-beta.1", "4.4.0-dev.1").unwrap();
        check("4.4.0-beta.1", "4.4.0-beta.1").unwrap();
        check("4.4.0-rc.1", "4.4.0").unwrap();
        assert_eq!(
            check("1.11.0", "4.4.0").unwrap_err().to_string(),
            "javascript/package.json has `1.11.0`, not a version this command writes"
        );
    }

    #[test]
    fn json_strings_are_set_in_place() {
        let package = "{\n  \"name\": \"@confluentinc/kafka-javascript\",\n  \"version\": \"4.4.0-beta.1\",\n  \"private\": true,\n  \
                       \"scripts\": { \"version\": \"echo x\" },\n  \"files\": [\"version\", {\"version\": 1}]\n}\n";
        assert_eq!(
            set_json_strings(JAVASCRIPT_PACKAGE, package, &[&["version"]], "4.4.0-beta.2").unwrap(),
            package.replace("\"version\": \"4.4.0-beta.1\"", "\"version\": \"4.4.0-beta.2\"")
        );
        let lock = "{\r\n\t\"name\": \"x\", \"version\": \"4.4.0-beta.1\", \"lockfileVersion\": 3,\r\n\t\"packages\": {\r\n\t\t\"\": \
                    {\"version\": \"4.4.0-beta.1\", \"license\": \"Apache-2.0\"},\r\n\t\t\"node_modules/a\": {\"version\": \"1.0.0\"}\r\n\t}\r\n}";
        assert_eq!(javascript_version(package, lock).unwrap(), "4.4.0-beta.1");
        let edited =
            set_json_strings(JAVASCRIPT_LOCK, lock, &[&["version"], &["packages", "", "version"]], "4.5.0").unwrap();
        assert_eq!(edited, lock.replace("\"version\": \"4.4.0-beta.1\"", "\"version\": \"4.5.0\""));
        assert!(edited.contains("\"node_modules/a\": {\"version\": \"1.0.0\"}"));

        let error = |text: &str, path: &[&str]| json_string_range(JAVASCRIPT_LOCK, text, path).unwrap_err().to_string();
        assert_eq!(
            error("{\"name\": \"x\"}", &["version"]),
            "javascript/package-lock.json has no \"version\""
        );
        assert_eq!(
            error("{\"version\": \"1\", \"version\": \"2\"}", &["version"]),
            "javascript/package-lock.json has more than one \"version\""
        );
        assert_eq!(
            error("{\"version\": 4}", &["version"]),
            "javascript/package-lock.json has \"version\" that is not a plain string"
        );
        assert_eq!(
            error("{\"version\": \"4.4\\u002e0\"}", &["version"]),
            "javascript/package-lock.json has \"version\" that is not a plain string"
        );
        assert_eq!(
            error("{\"packages\": {}}", &["packages", "", "version"]),
            "javascript/package-lock.json has no packages[\"\"].version"
        );
        assert_eq!(
            error("{\"version\": \"1\"", &["version"]),
            "javascript/package-lock.json is not JSON this command can read"
        );
        let stale = lock.replacen("{\"version\": \"4.4.0-beta.1\"", "{\"version\": \"4.4.0-alpha.1\"", 1);
        assert_eq!(
            javascript_version(package, &stale).unwrap_err().to_string(),
            "javascript/package-lock.json has `4.4.0-alpha.1` at packages[\"\"].version, not `4.4.0-beta.1`, the \
             version of javascript/package.json; run `npm install --package-lock-only` in javascript/ first"
        );
    }
}
