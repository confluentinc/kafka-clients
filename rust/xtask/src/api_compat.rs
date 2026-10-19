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

//! `cargo xtask api-compat`: compares the public surface with a released tag.
//!
//! Two standard tools, one per surface:
//!
//!   - the Rust API with `cargo semver-checks`, over the crate as crates.io
//!     publishes it (default features only, so not `ffi`);
//!   - the C ABI with libabigail's `abidiff`, over the `ffi` build of the
//!     shared library.
//!
//! Both follow Cargo's semver rule: a breaking change passes only when the
//! version in `Cargo.toml` allows one over the baseline's (a major bump, or a
//! minor bump while the major is 0). `cargo semver-checks` applies the rule
//! itself; for the C ABI this module applies it to every change `abidiff`
//! reports. `abidiff` runs with `--no-added-syms`, so an added function or
//! variable is no change; any change it still reports (exit-status bit 4 or 8:
//! a removed function, a changed parameter or return type, a value struct's
//! layout, a renumbered C enum) is breaking. libabigail sets bit 8 only for a
//! few of these (a removed symbol, a soname or architecture change), so bit 4
//! alone is not "compatible".
//!
//! The `_fn_t` / `_cb_t` function-pointer typedefs are compared in the C
//! header instead, see [`function_pointer_typedefs`].
//!
//! The baseline is `--baseline <tag>`, else `$API_COMPAT_BASELINE`, else the
//! highest `vX.Y.Z` tag whose version does not exceed the current one; for a
//! pre-release such as `0.3.0-rc.1`, the highest one below `0.3.0`, while the
//! semver rule reads it as `0.3.0`. Its
//! `rust/` tree is extracted with `git archive` under `target/api-compat/<tag>`
//! (tags do not move, so an existing extraction is reused) and built there with
//! the toolchain the tag pins.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{exit, Command, Stdio};

use anyhow::Context as _;

use crate::bump_version::PackageVersion;

/// Where the baselines, the suppression file and the reports go.
const API_COMPAT_DIR: &str = "target/api-compat";

/// The shared library `cargo build --lib --features ffi` produces.
const LIBRARY: &str = "libconfluent_kafka.so";

/// The C header `build.rs` writes, relative to the crate directory whatever
/// the target directory is. `confluent_kafka_bindings.h` declares the same
/// types.
const HEADER: &str = "target/include/confluent_kafka.h";

/// libabigail's `abidiff` exit-status bits (`abg-tools-utils.h`).
const ABIDIFF_ERROR: i32 = 1;
const ABIDIFF_USAGE_ERROR: i32 = 2;
/// Set on any change `abidiff` reports, with
/// [`ABIDIFF_ABI_INCOMPATIBLE_CHANGE`] or alone. With `--no-added-syms` an
/// addition is not reported, so alone it is a changed function, type or
/// struct layout: breaking.
const ABIDIFF_ABI_CHANGE: i32 = 4;
/// Set, besides [`ABIDIFF_ABI_CHANGE`], for the changes libabigail is sure are
/// incompatible (a removed symbol, a soname or architecture change).
const ABIDIFF_ABI_INCOMPATIBLE_CHANGE: i32 = 8;

/// A release version, `major.minor.patch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    major: u64,
    minor: u64,
    patch: u64,
}

impl Version {
    /// Parses `X.Y.Z`. A pre-release or build suffix is not a release, so it
    /// does not parse.
    fn parse(text: &str) -> Option<Self> {
        let mut parts = text.split('.');
        let version = Version {
            major: parts.next()?.parse().ok()?,
            minor: parts.next()?.parse().ok()?,
            patch: parts.next()?.parse().ok()?,
        };
        parts.next().is_none().then_some(version)
    }

    /// Whether going from `self` to `new` may break the API, by Cargo's semver
    /// rule: the left-most non-zero component must increase.
    fn allows_breaking_change_to(self, new: Version) -> bool {
        if self.major > 0 {
            new.major > self.major
        } else if self.minor > 0 {
            new.major > 0 || new.minor > self.minor
        } else {
            new > self
        }
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Outcome of one surface's comparison.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Compatible,
    /// Breaking, but the version bump allows it.
    BreakingAllowed,
    /// Not compared: `cargo semver-checks` skips every check when the version
    /// bump allows breaking changes, since none could fail.
    NotChecked,
    Breaking,
}

pub(crate) fn run(args: &[String]) -> anyhow::Result<()> {
    let mut baseline = env::var("API_COMPAT_BASELINE").ok().filter(|tag| !tag.is_empty());
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--baseline" => {
                baseline = Some(args.next().context("--baseline needs a tag")?.clone());
            },
            other => anyhow::bail!("unknown argument `{other}`; usage: cargo xtask api-compat [--baseline <tag>]"),
        }
    }

    let (shown, current, pre_release) = current_version()?;
    let tag = match baseline {
        Some(tag) => tag,
        None => {
            let tags = git_output(&["tag", "--list", "v[0-9]*"])?;
            select_baseline(tags.lines(), current, pre_release).with_context(|| {
                format!(
                    "no `vX.Y.Z` tag at or below {shown}; fetch the tags \
                     (`git fetch --depth=1 origin 'refs/tags/*:refs/tags/*'`) or pass --baseline"
                )
            })?
        },
    };

    require_tool(
        "cargo",
        &["semver-checks", "--version"],
        "cargo install --locked cargo-semver-checks",
    )?;
    require_tool("abidiff", &["--version"], "install libabigail (`apt install abigail-tools`)")?;

    let baseline_dir = extract_baseline(&tag)?;
    let old_package = baseline_version(&baseline_dir)?;
    let old = release_of(old_package);
    println!("🔎 Comparing the public surface of {shown} with {tag} ({old_package})");

    let rust = check_rust(&baseline_dir, old.allows_breaking_change_to(current))?;
    let c = check_c(&tag, &baseline_dir, old, current)?;

    println!();
    for (surface, verdict) in [("Rust API", &rust), ("C ABI", &c)] {
        match verdict {
            Verdict::Compatible => println!("✅ {surface}: compatible with {tag}"),
            Verdict::BreakingAllowed => {
                println!("✅ {surface}: breaking changes since {tag}, allowed by the bump {old_package} → {shown}")
            },
            Verdict::NotChecked => {
                println!(
                    "✅ {surface}: not compared with {tag}, the bump {old_package} → {shown} allows breaking changes"
                )
            },
            Verdict::Breaking => {
                eprintln!("❌ {surface}: breaking changes since {tag} that {shown} does not allow over {old_package}")
            },
        }
    }
    if rust == Verdict::Breaking || c == Verdict::Breaking {
        eprintln!(
            "   Revert the breaking change, or bump the version in Cargo.toml to one that allows it \
             (while the major version is 0, the minor version)."
        );
        exit(1);
    }
    Ok(())
}

/// `cargo semver-checks` over the published surface: the crate's default
/// features, as a crates.io user without `features = [...]` gets them.
/// Without `--default-features` it would also enable features such as `ffi`
/// that such a user does not get; `--only-explicit-features` would drop the
/// default ones, and the API behind them with it.
fn check_rust(baseline_dir: &Path, breaking_allowed: bool) -> anyhow::Result<Verdict> {
    println!("\n🦀 Rust API (cargo semver-checks)");
    let status = child("cargo")
        .args([
            "semver-checks",
            "--package",
            crate::PACKAGE_NAME,
            "--default-features",
            "--baseline-root",
        ])
        .arg(baseline_dir)
        .status()
        .context("running cargo semver-checks")?;
    // It applies the semver rule itself: when the bump allows breaking
    // changes it skips all its checks and succeeds.
    match status.code() {
        Some(0) if breaking_allowed => Ok(Verdict::NotChecked),
        Some(0) => Ok(Verdict::Compatible),
        // Exit status 100 is its "semver violations found".
        Some(100) => Ok(Verdict::Breaking),
        _ => anyhow::bail!("cargo semver-checks failed to run ({status})"),
    }
}

/// `abidiff` over the `ffi` shared library of the baseline and of the working
/// tree, with the opaque handles and the function-pointer wrappers suppressed
/// (see [`opaque_handles`], [`suppression_file`]), plus the comparison of the
/// function-pointer typedefs in the two headers that replaces the latter.
fn check_c(tag: &str, baseline_dir: &Path, old: Version, current: Version) -> anyhow::Result<Verdict> {
    println!("\n🔧 C ABI (abidiff)");
    // The baseline builds in its own tree; the working tree where its other
    // builds go, so this one is incremental.
    let old_library = build_library(baseline_dir, &baseline_dir.join("target"))?;
    let target_dir = env::var_os("CARGO_TARGET_DIR").map_or_else(|| PathBuf::from("target"), PathBuf::from);
    let new_library = build_library(Path::new("."), &env::current_dir()?.join(target_dir))?;

    let old_header = read(&baseline_dir.join(HEADER))?;
    let new_header = read(Path::new(HEADER))?;
    let handles: BTreeSet<String> = opaque_handles(&old_header)
        .intersection(&opaque_handles(&new_header))
        .cloned()
        .collect();
    let suppressions = PathBuf::from(API_COMPAT_DIR).join("opaque-handles.suppr");
    fs::write(&suppressions, suppression_file(&handles))?;

    let typedef_changes = changed_function_pointer_typedefs(&old_header, &new_header);
    for change in &typedef_changes {
        println!("   {change}");
    }

    let output = Command::new("abidiff")
        .args(abidiff_args(&suppressions, &old_library, &new_library))
        .output()
        .context("running abidiff")?;
    let report_path = PathBuf::from(API_COMPAT_DIR).join(format!("abidiff-{tag}.txt"));
    fs::write(&report_path, &output.stdout)?;
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    println!("   Report saved to {}", report_path.display());

    let code = output.status.code().context("abidiff was killed by a signal")?;
    c_verdict(code, !typedef_changes.is_empty(), old.allows_breaking_change_to(current))
}

/// The `abidiff` command line. `--no-added-syms` leaves additions out of the
/// report and of the exit status, so an added function passes and what is
/// left is breaking (see [`c_verdict`]).
fn abidiff_args(suppressions: &Path, old_library: &Path, new_library: &Path) -> Vec<std::ffi::OsString> {
    vec![
        "--no-added-syms".into(),
        "--suppressions".into(),
        suppressions.into(),
        old_library.into(),
        new_library.into(),
    ]
}

/// Reads an `abidiff` exit status and whether a function-pointer typedef
/// changed in the header: a tool error fails the task; any change `abidiff`
/// still reports with `--no-added-syms` ([`ABIDIFF_ABI_CHANGE`], with or
/// without [`ABIDIFF_ABI_INCOMPATIBLE_CHANGE`]), or a changed typedef, is
/// breaking; no change is compatible.
fn c_verdict(code: i32, typedef_changed: bool, breaking_allowed: bool) -> anyhow::Result<Verdict> {
    if code & (ABIDIFF_ERROR | ABIDIFF_USAGE_ERROR) != 0 {
        anyhow::bail!("abidiff failed (exit status {code})");
    }
    let changed = code & (ABIDIFF_ABI_CHANGE | ABIDIFF_ABI_INCOMPATIBLE_CHANGE) != 0 || typedef_changed;
    Ok(if !changed {
        Verdict::Compatible
    } else if breaking_allowed {
        Verdict::BreakingAllowed
    } else {
        Verdict::Breaking
    })
}

/// Builds the `ffi` shared library of the crate in `dir` into the absolute
/// `target_dir`, with the debug info `abidiff` reads the types from, and
/// returns its path.
fn build_library(dir: &Path, target_dir: &Path) -> anyhow::Result<PathBuf> {
    let status = child("cargo")
        .args(["build", "--lib", "--features", "ffi"])
        .current_dir(dir)
        .env("CARGO_TARGET_DIR", target_dir)
        .status()?;
    anyhow::ensure!(status.success(), "building the ffi library in {} failed", dir.display());
    let library = target_dir.join("debug").join(LIBRARY);
    anyhow::ensure!(library.exists(), "{} was not built", library.display());
    Ok(library)
}

/// The names of the handles a header declares opaque, which cbindgen writes as
/// `typedef struct { uint8_t _private[0]; } NAME;`.
///
/// C sees nothing of these structs, but `abidiff` reads rustc's DWARF, where
/// each carries its Rust path (`confluent_kafka::ffi::admin::kafka_admin_X_t`):
/// moving one between Rust modules shows as an incompatible type change of
/// every function taking it. They are suppressed when opaque in both headers
/// only, so a handle that became a value struct, or the reverse, is reported.
fn opaque_handles(header: &str) -> BTreeSet<String> {
    let mut handles = BTreeSet::new();
    let mut body: Option<Vec<&str>> = None;
    for line in header.lines() {
        let line = line.trim();
        match &mut body {
            None if line.starts_with("typedef struct") && line.ends_with('{') => body = Some(Vec::new()),
            None => {},
            Some(lines) => {
                if let Some(name) = line.strip_prefix('}') {
                    if lines.as_slice() == ["uint8_t _private[0];"] {
                        handles.insert(name.trim().trim_end_matches(';').trim().to_string());
                    }
                    body = None;
                } else if !line.is_empty() {
                    lines.push(line);
                }
            },
        }
    }
    handles
}

/// A libabigail suppression specification for `handles`, matching each name
/// with or without a Rust path before it, and for the function-pointer
/// wrappers.
///
/// A C `_fn_t` is a Rust `Option<unsafe extern "C" fn(..)>` alias. rustc
/// emits no DWARF typedef for an alias: `abidiff` sees the `Option<..>`
/// struct, named after the Rust paths of its parameter types
/// (`Option<unsafe extern "C" fn(*const confluent_kafka::ffi::..::kafka_X_t)>`),
/// so moving a handle between Rust modules shows as a change of every function
/// taking a callback. The wrappers are suppressed whatever their kind
/// (no `type_kind`), and [`changed_function_pointer_typedefs`] compares their
/// C declarations in the headers instead, so a changed callback signature is
/// still breaking. The regular expression spells the quotes and spaces of
/// `extern "C" ` as `.`, which libabigail's ini syntax leaves alone.
fn suppression_file(handles: &BTreeSet<String>) -> String {
    let names: Vec<&str> = handles.iter().map(String::as_str).collect();
    format!(
        "# Opaque C handles: generated by `cargo xtask api-compat`.\n\
         [suppress_type]\n  type_kind = struct\n  name_regexp = ^(.*::)?({})$\n\
         \n\
         # Function-pointer typedefs, compared in the C header instead.\n\
         [suppress_type]\n  name_regexp = ^(.*::)?Option<(unsafe.)?extern.....fn\n",
        names.join("|")
    )
}

/// The function-pointer typedefs a header declares, which cbindgen writes as
/// `typedef RET (*NAME)(PARAMS);`, possibly over several lines: each name
/// with its declaration, whitespace collapsed.
fn function_pointer_typedefs(header: &str) -> BTreeMap<String, String> {
    let mut typedefs = BTreeMap::new();
    let mut statement: Option<String> = None;
    for line in header.lines() {
        let line = line.trim();
        let text = match &mut statement {
            Some(text) => text,
            None if line.starts_with("typedef ")
                && !["typedef struct", "typedef enum", "typedef union"]
                    .iter()
                    .any(|kind| line.starts_with(kind)) =>
            {
                statement.insert(String::new())
            },
            None => continue,
        };
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(line);
        if text.ends_with(';') {
            let text = statement.take().unwrap_or_default();
            let name = text
                .split_once("(*")
                .and_then(|(_, rest)| rest.split_once(')'))
                .map(|(name, _)| name.trim().to_string());
            if let Some(name) = name {
                typedefs.insert(name, text.split_whitespace().collect::<Vec<_>>().join(" "));
            }
        }
    }
    typedefs
}

/// The function-pointer typedefs of the old header that the new one changes
/// or removes, as lines for the report. `abidiff` does not see them (see
/// [`suppression_file`]); an added one is no change.
fn changed_function_pointer_typedefs(old_header: &str, new_header: &str) -> Vec<String> {
    let new = function_pointer_typedefs(new_header);
    function_pointer_typedefs(old_header)
        .into_iter()
        .filter_map(|(name, old)| match new.get(&name) {
            None => Some(format!("removed typedef {name}: {old}")),
            Some(new) if *new != old => Some(format!("changed typedef {name}: {old} → {new}")),
            Some(_) => None,
        })
        .collect()
}

/// The highest release tag `vX.Y.Z` not above `current`, or below it when the
/// crate is at a pre-release of `current`, which comes before it.
fn select_baseline<'a>(tags: impl Iterator<Item = &'a str>, current: Version, pre_release: bool) -> Option<String> {
    tags.filter_map(|tag| Some((Version::parse(tag.trim().strip_prefix('v')?)?, tag.trim())))
        .filter(|(version, _)| {
            if pre_release {
                *version < current
            } else {
                *version <= current
            }
        })
        .max()
        .map(|(_, tag)| tag.to_string())
}

/// Extracts the tag's `rust/` tree into `target/api-compat/<tag>`, unless an
/// earlier run did.
fn extract_baseline(tag: &str) -> anyhow::Result<PathBuf> {
    let dir = PathBuf::from(API_COMPAT_DIR).join(tag);
    if dir.join("Cargo.toml").exists() {
        return Ok(dir.canonicalize()?);
    }
    println!("📦 Extracting the rust/ tree of {tag} into {}", dir.display());
    // Extract next to it and rename, so an interrupted run leaves no partial tree.
    let partial = PathBuf::from(API_COMPAT_DIR).join(format!("{tag}.partial"));
    if partial.exists() {
        fs::remove_dir_all(&partial)?;
    }
    fs::create_dir_all(&partial)?;
    // From the repository root: below it `git archive` keeps only the paths
    // under the current directory, which a tree's paths are not.
    let root = git_output(&["rev-parse", "--show-toplevel"])?;
    let mut archive = Command::new("git")
        .current_dir(root.trim())
        .args(["archive", "--format=tar", &format!("{tag}:rust")])
        .stdout(Stdio::piped())
        .spawn()
        .context("running git archive")?;
    let stdout = archive.stdout.take().context("git archive has no stdout")?;
    let untar = Command::new("tar").arg("-x").arg("-C").arg(&partial).stdin(stdout).status()?;
    let archived = archive.wait()?;
    anyhow::ensure!(archived.success(), "git archive {tag}:rust failed; is the tag fetched?");
    anyhow::ensure!(untar.success(), "extracting {tag}:rust failed");
    anyhow::ensure!(partial.join("Cargo.toml").exists(), "{tag}:rust has no Cargo.toml");
    fs::rename(&partial, &dir)?;
    Ok(dir.canonicalize()?)
}

/// The working tree's crate version: as written, its `X.Y.Z`, and whether it
/// is a pre-release of that.
fn current_version() -> anyhow::Result<(String, Version, bool)> {
    let text = crate::package_version()?;
    let version = PackageVersion::parse(&text)
        .with_context(|| format!("`{text}` is not a version `cargo xtask bump-version` writes"))?;
    Ok((text, release_of(version), version.is_pre_release()))
}

/// The `[package]` version in the baseline's `Cargo.toml`, a release or a
/// pre-release.
fn baseline_version(dir: &Path) -> anyhow::Result<PackageVersion> {
    let manifest = read(&dir.join("Cargo.toml"))?;
    package_version_in(&manifest)
        .context("the baseline Cargo.toml has no [package] version `cargo xtask bump-version` writes")
}

/// The `X.Y.Z` a version is, or is a pre-release of, which the semver rule
/// reads.
fn release_of(version: PackageVersion) -> Version {
    let (major, minor, patch) = version.release();
    Version { major, minor, patch }
}

/// The `version` key of the `[package]` table of a manifest.
fn package_version_in(manifest: &str) -> Option<PackageVersion> {
    let mut in_package = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_package = line == "[package]";
        } else if in_package {
            if let Some(value) = line.strip_prefix("version") {
                let value = value.trim_start().strip_prefix('=')?.trim().trim_matches('"');
                return PackageVersion::parse(value);
            }
        }
    }
    None
}

/// A command for a child `cargo` that picks its toolchain from the directory it
/// runs in: rustup exports `RUSTUP_TOOLCHAIN` to `cargo xtask`, which would
/// otherwise override the `rust-toolchain.toml` of the baseline.
fn child(program: &str) -> Command {
    let mut command = Command::new(program);
    command.env_remove("RUSTUP_TOOLCHAIN");
    command
}

fn require_tool(program: &str, args: &[&str], install: &str) -> anyhow::Result<()> {
    let found = Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    anyhow::ensure!(found, "`{program} {}` is not available; {install}", args.join(" "));
    Ok(())
}

fn git_output(args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("git").args(args).output()?;
    anyhow::ensure!(output.status.success(), "git {} failed", args.join(" "));
    Ok(String::from_utf8(output.stdout)?)
}

fn read(path: &Path) -> anyhow::Result<String> {
    fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(text: &str) -> Version {
        Version::parse(text).unwrap()
    }

    #[test]
    fn version_parses_releases_only() {
        assert_eq!(v("0.1.1"), Version { major: 0, minor: 1, patch: 1 });
        assert_eq!(Version::parse("0.2.0-rc.1"), None);
        assert_eq!(Version::parse("0.2"), None);
        assert_eq!(Version::parse("0.2.0.1"), None);
        assert_eq!(v("0.10.0").to_string(), "0.10.0");
    }

    #[test]
    fn breaking_change_follows_cargo_semver() {
        // 0.y.z: the minor version is the breaking component.
        assert!(!v("0.1.1").allows_breaking_change_to(v("0.1.1")));
        assert!(!v("0.1.1").allows_breaking_change_to(v("0.1.2")));
        assert!(v("0.1.1").allows_breaking_change_to(v("0.2.0")));
        assert!(v("0.1.1").allows_breaking_change_to(v("1.0.0")));
        // 0.0.z: every release may break.
        assert!(v("0.0.1").allows_breaking_change_to(v("0.0.2")));
        assert!(!v("0.0.1").allows_breaking_change_to(v("0.0.1")));
        // x.y.z with x > 0: only a major bump.
        assert!(!v("1.2.3").allows_breaking_change_to(v("1.3.0")));
        assert!(v("1.2.3").allows_breaking_change_to(v("2.0.0")));
    }

    #[test]
    fn baseline_is_the_highest_release_tag_not_above_current() {
        let tags = [
            "v0.0.0",
            "v0.1.0",
            "v0.1.1",
            "v0.2.0-rc.1",
            "v0.3.0",
            "other",
            "v0.10.0",
        ];
        assert_eq!(select_baseline(tags.into_iter(), v("0.1.1"), false).as_deref(), Some("v0.1.1"));
        assert_eq!(select_baseline(tags.into_iter(), v("0.2.0"), false).as_deref(), Some("v0.1.1"));
        // Ordered by version, not by name: v0.10.0 is above v0.3.0.
        assert_eq!(select_baseline(tags.into_iter(), v("1.0.0"), false).as_deref(), Some("v0.10.0"));
        assert_eq!(select_baseline(["v1.0.0"].into_iter(), v("0.1.0"), false), None);
        // A pre-release of 0.3.0 comes before v0.3.0, so it compares with v0.1.1.
        assert_eq!(select_baseline(tags.into_iter(), v("0.3.0"), true).as_deref(), Some("v0.1.1"));
        assert_eq!(select_baseline(tags.into_iter(), v("0.3.1"), true).as_deref(), Some("v0.3.0"));
    }

    #[test]
    fn opaque_handles_are_the_private_zero_length_structs() {
        let header = "\
/**
 * Opaque handle.
 */
typedef struct {
  uint8_t _private[0];
} kafka_admin_Admin_t;

typedef struct {
  /**
   * The bytes.
   */
  const uint8_t *data;
  int32_t len;
} kafka_Bytes_t;

typedef struct kafka_common_Node_t {
  uint8_t _private[0];
} kafka_common_Node_t;

typedef enum {
  kafka_common_GroupType_CONSUMER,
} kafka_common_GroupType_e;
";
        let handles: Vec<String> = opaque_handles(header).into_iter().collect();
        assert_eq!(handles, ["kafka_admin_Admin_t", "kafka_common_Node_t"]);
    }

    #[test]
    fn suppression_matches_names_with_or_without_a_rust_path() {
        let handles: BTreeSet<String> = ["kafka_a_t", "kafka_b_t"].map(String::from).into();
        assert_eq!(
            suppression_file(&handles),
            "# Opaque C handles: generated by `cargo xtask api-compat`.\n\
             [suppress_type]\n  type_kind = struct\n  name_regexp = ^(.*::)?(kafka_a_t|kafka_b_t)$\n\
             \n\
             # Function-pointer typedefs, compared in the C header instead.\n\
             [suppress_type]\n  name_regexp = ^(.*::)?Option<(unsafe.)?extern.....fn\n"
        );
    }

    const HEADER_V1: &str = "\
/**
 * A callback.
 */
typedef void (*kafka_a_cb_t)(void *opaque);

typedef double (*kafka_m_measure_fn_t)(void *self_,
                                       const kafka_c_t *config,
                                       int64_t now);

typedef int8_t (*kafka_gone_fn_t)(void *self_);

typedef struct {
  void (*field)(void *x);
} kafka_v_t;

typedef enum {
  kafka_E_A,
} kafka_E_e;
";

    #[test]
    fn function_pointer_typedefs_are_read_over_several_lines() {
        let typedefs = function_pointer_typedefs(HEADER_V1);
        assert_eq!(
            typedefs.into_iter().collect::<Vec<_>>(),
            [
                (
                    "kafka_a_cb_t".to_string(),
                    "typedef void (*kafka_a_cb_t)(void *opaque);".to_string()
                ),
                (
                    "kafka_gone_fn_t".to_string(),
                    "typedef int8_t (*kafka_gone_fn_t)(void *self_);".to_string()
                ),
                (
                    "kafka_m_measure_fn_t".to_string(),
                    "typedef double (*kafka_m_measure_fn_t)(void *self_, const kafka_c_t *config, int64_t now);"
                        .to_string()
                ),
            ]
        );
    }

    /// A `_fn_t` change is suppressed in `abidiff` and caught in the header:
    /// reformatting or adding one is no change, a changed signature or a
    /// removed one is.
    #[test]
    fn function_pointer_typedef_changes_are_breaking() {
        let reformatted = HEADER_V1.replace(
            "(void *self_,\n                                       const kafka_c_t *config,",
            "(void *self_, const kafka_c_t *config,",
        );
        let added = format!("{HEADER_V1}typedef void (*kafka_new_fn_t)(void *self_);\n");
        assert!(changed_function_pointer_typedefs(HEADER_V1, HEADER_V1).is_empty());
        assert!(changed_function_pointer_typedefs(HEADER_V1, &reformatted).is_empty());
        assert!(changed_function_pointer_typedefs(HEADER_V1, &added).is_empty());

        let changed = HEADER_V1
            .replace("int64_t now);", "int32_t now);")
            .replace("typedef int8_t (*kafka_gone_fn_t)(void *self_);\n", "");
        assert_eq!(
            changed_function_pointer_typedefs(HEADER_V1, &changed),
            [
                "removed typedef kafka_gone_fn_t: typedef int8_t (*kafka_gone_fn_t)(void *self_);",
                "changed typedef kafka_m_measure_fn_t: \
                 typedef double (*kafka_m_measure_fn_t)(void *self_, const kafka_c_t *config, int64_t now); → \
                 typedef double (*kafka_m_measure_fn_t)(void *self_, const kafka_c_t *config, int32_t now);",
            ]
        );
        // A changed typedef alone, with `abidiff` seeing nothing, is breaking.
        assert_eq!(c_verdict(0, true, false).unwrap(), Verdict::Breaking);
        assert_eq!(c_verdict(0, true, true).unwrap(), Verdict::BreakingAllowed);
    }

    #[test]
    fn abidiff_runs_without_added_symbols() {
        let args = abidiff_args(Path::new("s.suppr"), Path::new("old.so"), Path::new("new.so"));
        let args: Vec<&str> = args.iter().map(|arg| arg.to_str().unwrap()).collect();
        assert_eq!(args, ["--no-added-syms", "--suppressions", "s.suppr", "old.so", "new.so"]);
    }

    #[test]
    fn abidiff_exit_status_decides_the_verdict() {
        // No change, or only added symbols, which `--no-added-syms` leaves out
        // of the exit status.
        assert_eq!(c_verdict(0, false, false).unwrap(), Verdict::Compatible);
        assert_eq!(c_verdict(0, false, true).unwrap(), Verdict::Compatible);
        // A changed function (a parameter or return type, a struct layout, a
        // renumbered enum) sets bit 4 alone: breaking.
        assert_eq!(c_verdict(ABIDIFF_ABI_CHANGE, false, false).unwrap(), Verdict::Breaking);
        assert_eq!(c_verdict(ABIDIFF_ABI_CHANGE, false, true).unwrap(), Verdict::BreakingAllowed);
        // A removed function sets bit 8 too.
        let incompatible = ABIDIFF_ABI_CHANGE | ABIDIFF_ABI_INCOMPATIBLE_CHANGE;
        assert_eq!(c_verdict(incompatible, false, false).unwrap(), Verdict::Breaking);
        assert_eq!(c_verdict(incompatible, false, true).unwrap(), Verdict::BreakingAllowed);
        let error = c_verdict(ABIDIFF_ERROR, false, true).unwrap_err();
        assert_eq!(error.to_string(), "abidiff failed (exit status 1)");
        let error = c_verdict(ABIDIFF_USAGE_ERROR | ABIDIFF_ABI_CHANGE, false, true).unwrap_err();
        assert_eq!(error.to_string(), "abidiff failed (exit status 6)");
    }

    #[test]
    fn package_version_is_read_from_the_package_table() {
        let manifest = "\
[workspace]
members = [\".\"]

[workspace.package]
version = \"9.9.9\"

[package]
name = \"confluent-kafka\"
version = \"0.1.1\"

[dependencies]
version = \"1.0.0\"
";
        let version = package_version_in(manifest).unwrap();
        assert_eq!(release_of(version), v("0.1.1"));
        assert!(!version.is_pre_release());
        assert_eq!(package_version_in("[package]\nname = \"x\"\n"), None);
        assert_eq!(package_version_in("[package]\nversion = \"0.2.0+build\"\n"), None);
    }

    /// A pre-release crate, as `bump-version` writes it, reads as its release
    /// for the semver rule and compares with the release tag below it.
    #[test]
    fn a_beta_version_finds_its_baseline() {
        let beta = package_version_in("[package]\nname = \"confluent-kafka\"\nversion = \"0.2.0-beta.1\"\n").unwrap();
        assert!(beta.is_pre_release());
        assert_eq!(release_of(beta), v("0.2.0"));
        let tags = ["v0.0.0", "v0.1.0", "v0.1.1", "v0.2.0"];
        assert_eq!(
            select_baseline(tags.into_iter(), release_of(beta), beta.is_pre_release()).as_deref(),
            Some("v0.1.1")
        );
        // 0.1.1 → 0.2.0-beta.1 may break, as 0.1.1 → 0.2.0 may.
        assert!(v("0.1.1").allows_breaking_change_to(release_of(beta)));
        // A baseline that is itself a beta reads as its release: 0.2.0-beta.1
        // → 0.2.0 may not break.
        assert!(!release_of(beta).allows_breaking_change_to(v("0.2.0")));
    }
}
