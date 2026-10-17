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
//! itself; for `abidiff` this module applies it to the "incompatible change"
//! bit of its exit status.
//!
//! The baseline is `--baseline <tag>`, else `$API_COMPAT_BASELINE`, else the
//! highest `vX.Y.Z` tag whose version does not exceed the current one. Its
//! `rust/` tree is extracted with `git archive` under `target/api-compat/<tag>`
//! (tags do not move, so an existing extraction is reused) and built there with
//! the toolchain the tag pins.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{exit, Command, Stdio};

use anyhow::Context as _;

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
/// Set on any change, with [`ABIDIFF_ABI_INCOMPATIBLE_CHANGE`] or alone (an
/// added function, a compatible type change); alone it passes.
#[cfg(test)]
const ABIDIFF_ABI_CHANGE: i32 = 4;
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

    let current = current_version()?;
    let tag = match baseline {
        Some(tag) => tag,
        None => {
            let tags = git_output(&["tag", "--list", "v[0-9]*"])?;
            select_baseline(tags.lines(), current).with_context(|| {
                format!(
                    "no `vX.Y.Z` tag at or below {current}; fetch the tags \
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
    let old = baseline_version(&baseline_dir)?;
    println!("🔎 Comparing the public surface of {current} with {tag} ({old})");

    let rust = check_rust(&baseline_dir, old.allows_breaking_change_to(current))?;
    let c = check_c(&tag, &baseline_dir, old, current)?;

    println!();
    for (surface, verdict) in [("Rust API", &rust), ("C ABI", &c)] {
        match verdict {
            Verdict::Compatible => println!("✅ {surface}: compatible with {tag}"),
            Verdict::BreakingAllowed => {
                println!("✅ {surface}: breaking changes since {tag}, allowed by the bump {old} → {current}")
            },
            Verdict::NotChecked => {
                println!("✅ {surface}: not compared with {tag}, the bump {old} → {current} allows breaking changes")
            },
            Verdict::Breaking => {
                eprintln!("❌ {surface}: breaking changes since {tag} that {current} does not allow over {old}")
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

/// `cargo semver-checks` over the published surface: default features only,
/// as `--only-explicit-features` without `--features` selects. Without the flag
/// it would also enable features such as `ffi` that crates.io users do not get.
fn check_rust(baseline_dir: &Path, breaking_allowed: bool) -> anyhow::Result<Verdict> {
    println!("\n🦀 Rust API (cargo semver-checks)");
    let status = child("cargo")
        .args([
            "semver-checks",
            "--package",
            crate::PACKAGE_NAME,
            "--only-explicit-features",
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
/// tree, with the opaque handles suppressed (see [`opaque_handles`]).
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

    let output = Command::new("abidiff")
        .arg("--suppressions")
        .arg(&suppressions)
        .arg(&old_library)
        .arg(&new_library)
        .output()
        .context("running abidiff")?;
    let report_path = PathBuf::from(API_COMPAT_DIR).join(format!("abidiff-{tag}.txt"));
    fs::write(&report_path, &output.stdout)?;
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    println!("   Report saved to {}", report_path.display());

    let code = output.status.code().context("abidiff was killed by a signal")?;
    abidiff_verdict(code, old.allows_breaking_change_to(current))
}

/// Reads an `abidiff` exit status: a tool error fails the task, an
/// incompatible change is breaking, any other change (an added function, a
/// compatible type change) is not.
fn abidiff_verdict(code: i32, breaking_allowed: bool) -> anyhow::Result<Verdict> {
    if code & (ABIDIFF_ERROR | ABIDIFF_USAGE_ERROR) != 0 {
        anyhow::bail!("abidiff failed (exit status {code})");
    }
    Ok(if code & ABIDIFF_ABI_INCOMPATIBLE_CHANGE == 0 {
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
/// with or without a Rust path before it.
fn suppression_file(handles: &BTreeSet<String>) -> String {
    let names: Vec<&str> = handles.iter().map(String::as_str).collect();
    format!(
        "# Opaque C handles: generated by `cargo xtask api-compat`.\n\
         [suppress_type]\n  type_kind = struct\n  name_regexp = ^(.*::)?({})$\n",
        names.join("|")
    )
}

/// The highest release tag `vX.Y.Z` not above `current`.
fn select_baseline<'a>(tags: impl Iterator<Item = &'a str>, current: Version) -> Option<String> {
    tags.filter_map(|tag| Some((Version::parse(tag.trim().strip_prefix('v')?)?, tag.trim())))
        .filter(|(version, _)| *version <= current)
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

/// The working tree's crate version.
fn current_version() -> anyhow::Result<Version> {
    let version = crate::package_version()?;
    Version::parse(&version).with_context(|| format!("`{version}` is not a release version"))
}

/// The `[package]` version in the baseline's `Cargo.toml`.
fn baseline_version(dir: &Path) -> anyhow::Result<Version> {
    let manifest = read(&dir.join("Cargo.toml"))?;
    package_version_in(&manifest).context("the baseline Cargo.toml has no [package] version")
}

/// The `version` key of the `[package]` table of a manifest.
fn package_version_in(manifest: &str) -> Option<Version> {
    let mut in_package = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_package = line == "[package]";
        } else if in_package {
            if let Some(value) = line.strip_prefix("version") {
                let value = value.trim_start().strip_prefix('=')?.trim().trim_matches('"');
                return Version::parse(value);
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
        assert_eq!(select_baseline(tags.into_iter(), v("0.1.1")).as_deref(), Some("v0.1.1"));
        assert_eq!(select_baseline(tags.into_iter(), v("0.2.0")).as_deref(), Some("v0.1.1"));
        // Ordered by version, not by name: v0.10.0 is above v0.3.0.
        assert_eq!(select_baseline(tags.into_iter(), v("1.0.0")).as_deref(), Some("v0.10.0"));
        assert_eq!(select_baseline(["v1.0.0"].into_iter(), v("0.1.0")), None);
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
             [suppress_type]\n  type_kind = struct\n  name_regexp = ^(.*::)?(kafka_a_t|kafka_b_t)$\n"
        );
    }

    #[test]
    fn abidiff_exit_status_decides_the_verdict() {
        assert_eq!(abidiff_verdict(0, false).unwrap(), Verdict::Compatible);
        // Only additions or compatible changes.
        assert_eq!(abidiff_verdict(ABIDIFF_ABI_CHANGE, false).unwrap(), Verdict::Compatible);
        let incompatible = ABIDIFF_ABI_CHANGE | ABIDIFF_ABI_INCOMPATIBLE_CHANGE;
        assert_eq!(abidiff_verdict(incompatible, false).unwrap(), Verdict::Breaking);
        assert_eq!(abidiff_verdict(incompatible, true).unwrap(), Verdict::BreakingAllowed);
        let error = abidiff_verdict(ABIDIFF_ERROR, true).unwrap_err();
        assert_eq!(error.to_string(), "abidiff failed (exit status 1)");
        assert!(abidiff_verdict(ABIDIFF_USAGE_ERROR, true).is_err());
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
        assert_eq!(package_version_in(manifest), Some(v("0.1.1")));
        assert_eq!(package_version_in("[package]\nname = \"x\"\n"), None);
    }
}
