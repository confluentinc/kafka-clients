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

//! `cargo xtask install-c`: builds the C library for a system package and
//! installs it where the package takes it from.
//!
//! One release build of the `ffi` feature with `CONFLUENT_KAFKA_C_ABI_SONAME=1`,
//! so the shared library carries its versioned name (`build.rs`
//! `C_ABI_MAJOR`), and with rustc's `--print=native-static-libs`, the system
//! libraries a program linking the static library needs; it goes to
//! `<target>/package/`, never to `target/release`, where a library with a
//! soname would break the repository's own builds. Then, under
//! `<destdir><prefix>`:
//!
//!   - the shared library under its versioned file name and its links: on
//!     Linux `libconfluent_kafka.so.<major>.<version>`, `libconfluent_kafka.so.<major>`
//!     (the soname, the runtime package's) and `libconfluent_kafka.so` (the
//!     development package's); on macOS `libconfluent_kafka.<major>.dylib`
//!     (its install name) and `libconfluent_kafka.dylib`; on Windows the DLL
//!     under `bin/` and its import library, as cargo names them;
//!   - the static library, on Linux without the LLVM bitcode rustc embeds in
//!     it (three quarters of its size, which no C or C++ linker reads);
//!   - both C headers;
//!   - `pkgconfig/confluent-kafka.pc`, the static libraries' dependencies as
//!     `Libs.private`.

use std::env;
use std::fs;
use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::Context as _;

const USAGE: &str = "usage: cargo xtask install-c [--destdir <dir>] [--prefix <dir>] [--libdir <dir>] \
                     [--includedir <dir>] [--bindir <dir>] [--target <triple>]";

/// The target directory of the packaging build, under cargo's.
const PACKAGE_TARGET_DIR: &str = "package";

/// The note rustc prints for a static library under `--print=native-static-libs`.
const NATIVE_STATIC_LIBS_NOTE: &str = "note: native-static-libs: ";

/// Where the files go: the directories as a package names them, relative to
/// `prefix` unless absolute (Debian's multiarch `/usr/lib/x86_64-linux-gnu`).
#[derive(Debug)]
struct Layout {
    destdir: PathBuf,
    prefix: String,
    libdir: String,
    includedir: String,
    bindir: String,
    target: Option<String>,
}

impl Layout {
    fn parse(args: &[String]) -> anyhow::Result<Self> {
        let mut layout = Layout {
            destdir: PathBuf::new(),
            prefix: "/usr/local".to_owned(),
            libdir: "lib".to_owned(),
            includedir: "include".to_owned(),
            bindir: "bin".to_owned(),
            target: None,
        };
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            let mut value = || args.next().cloned().with_context(|| format!("{arg} needs a value; {USAGE}"));
            match arg.as_str() {
                "--destdir" => layout.destdir = PathBuf::from(value()?),
                "--prefix" => layout.prefix = value()?,
                "--libdir" => layout.libdir = value()?,
                "--includedir" => layout.includedir = value()?,
                "--bindir" => layout.bindir = value()?,
                "--target" => layout.target = Some(value()?),
                other => anyhow::bail!("unknown argument `{other}`; {USAGE}"),
            }
        }
        anyhow::ensure!(layout.prefix.starts_with('/'), "--prefix must be absolute: {}", layout.prefix);
        Ok(layout)
    }

    /// A directory as the package names it: `dir` itself when absolute, else
    /// under the prefix.
    fn resolved(&self, dir: &str) -> String {
        if dir.starts_with('/') {
            dir.to_owned()
        } else {
            format!("{}/{dir}", self.prefix.trim_end_matches('/'))
        }
    }

    /// Where a resolved directory is staged: under `destdir`.
    fn staged(&self, dir: &str) -> PathBuf {
        let resolved = self.resolved(dir);
        if self.destdir.as_os_str().is_empty() {
            PathBuf::from(resolved)
        } else {
            self.destdir.join(resolved.trim_start_matches('/'))
        }
    }
}

/// How the target's platform names the shared library and identifies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Platform {
    Elf,
    MachO,
    Windows,
}

impl Platform {
    /// The platform of `target`, a Rust target triple, or of the host.
    fn of(target: Option<&str>) -> Self {
        let target = target.map_or_else(|| env::consts::OS.to_owned(), str::to_owned);
        if target.contains("apple") || target == "macos" {
            Platform::MachO
        } else if target.contains("windows") {
            Platform::Windows
        } else {
            Platform::Elf
        }
    }
}

/// One file to install: copied from cargo's output, or a link to another
/// installed file.
#[derive(Debug, PartialEq, Eq)]
enum Entry {
    /// `(dir, name, cargo's file name)`.
    Copy(&'static str, String, &'static str),
    /// `(dir, name, target)`, a relative link.
    Link(&'static str, String, String),
}

/// The library files for `platform`, the C ABI's major `major` and the crate's
/// `version`. `"lib"` and `"bin"` stand for the layout's libdir and bindir.
fn library_entries(platform: Platform, major: u32, version: &str) -> Vec<Entry> {
    match platform {
        Platform::Elf => {
            let real = format!("libconfluent_kafka.so.{major}.{version}");
            let soname = format!("libconfluent_kafka.so.{major}");
            vec![
                Entry::Copy("lib", real.clone(), "libconfluent_kafka.so"),
                Entry::Link("lib", soname.clone(), real),
                Entry::Link("lib", "libconfluent_kafka.so".to_owned(), soname),
                Entry::Copy("lib", "libconfluent_kafka.a".to_owned(), "libconfluent_kafka.a"),
            ]
        },
        Platform::MachO => {
            let real = format!("libconfluent_kafka.{major}.dylib");
            vec![
                Entry::Copy("lib", real.clone(), "libconfluent_kafka.dylib"),
                Entry::Link("lib", "libconfluent_kafka.dylib".to_owned(), real),
                Entry::Copy("lib", "libconfluent_kafka.a".to_owned(), "libconfluent_kafka.a"),
            ]
        },
        Platform::Windows => vec![
            Entry::Copy("bin", "confluent_kafka.dll".to_owned(), "confluent_kafka.dll"),
            Entry::Copy("lib", "confluent_kafka.dll.lib".to_owned(), "confluent_kafka.dll.lib"),
            Entry::Copy("lib", "confluent_kafka.lib".to_owned(), "confluent_kafka.lib"),
        ],
    }
}

/// `C_ABI_MAJOR` as `build.rs` declares it.
pub(crate) fn c_abi_major(build_rs: &str) -> Option<u32> {
    build_rs.lines().find_map(|line| {
        line.trim()
            .strip_prefix("const C_ABI_MAJOR: u32 = ")?
            .strip_suffix(';')?
            .parse()
            .ok()
    })
}

/// The libraries of rustc's native-static-libs note in `output`.
fn native_static_libs(output: &str) -> Option<String> {
    output
        .lines()
        .find_map(|line| line.split_once(NATIVE_STATIC_LIBS_NOTE).map(|(_, libs)| libs.trim().to_owned()))
}

/// `confluent-kafka.pc`.
fn pkg_config(layout: &Layout, version: &str, native_static_libs: &str) -> String {
    let dir = |d: &str| {
        if d.starts_with('/') {
            d.to_owned()
        } else {
            format!("${{prefix}}/{d}")
        }
    };
    format!(
        "prefix={prefix}\n\
         libdir={libdir}\n\
         includedir={includedir}\n\
         \n\
         Name: confluent-kafka\n\
         Description: The C API of the Confluent Kafka client\n\
         Version: {version}\n\
         Libs: -L${{libdir}} -lconfluent_kafka\n\
         Libs.private: {native_static_libs}\n\
         Cflags: -I${{includedir}}\n",
        prefix = layout.prefix.trim_end_matches('/'),
        libdir = dir(&layout.libdir),
        includedir = dir(&layout.includedir),
    )
}

pub(crate) fn run(args: &[String]) -> anyhow::Result<()> {
    let layout = Layout::parse(args)?;
    let platform = Platform::of(layout.target.as_deref());
    let version = crate::package_version()?;
    let major = c_abi_major(&fs::read_to_string("build.rs").context("reading build.rs")?)
        .context("build.rs declares no `const C_ABI_MAJOR: u32 = N;`")?;

    // A target directory of its own: the library it builds carries the soname,
    // and one left in target/release would make the repository's own builds,
    // linked against it, look for a file cargo never writes there.
    let target_dir = env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| PathBuf::from("target"), PathBuf::from)
        .join(PACKAGE_TARGET_DIR);
    let native = build(layout.target.as_deref(), &target_dir)?;
    let built = match &layout.target {
        Some(target) => target_dir.join(target).join("release"),
        None => target_dir.join("release"),
    };

    for entry in library_entries(platform, major, &version) {
        match entry {
            Entry::Copy(dir, name, from) => {
                let dir = layout.staged(if dir == "bin" { &layout.bindir } else { &layout.libdir });
                copy(&built.join(from), &dir.join(&name))?;
                if platform == Platform::Elf && name.ends_with(".a") {
                    remove_bitcode(&dir.join(&name))?;
                }
            },
            Entry::Link(dir, name, to) => {
                let dir = layout.staged(if dir == "bin" { &layout.bindir } else { &layout.libdir });
                link(&to, &dir.join(name))?;
            },
        }
    }
    let include = layout.staged(&layout.includedir);
    for header in ["confluent_kafka.h", "confluent_kafka_bindings.h"] {
        copy(&Path::new("target/include").join(header), &include.join(header))?;
    }
    let pkgconfig = layout.staged(&layout.libdir).join("pkgconfig");
    fs::create_dir_all(&pkgconfig)?;
    fs::write(pkgconfig.join("confluent-kafka.pc"), pkg_config(&layout, &version, &native))?;
    println!(
        "✅ libconfluent_kafka {version} (C ABI {major}) installed under {}",
        layout.staged(&layout.prefix).display()
    );
    Ok(())
}

/// The release build of the `ffi` feature, with the soname and rustc's
/// native-static-libs note, which it returns. cargo's output is shown as it
/// comes.
fn build(target: Option<&str>, target_dir: &Path) -> anyhow::Result<String> {
    let mut command = Command::new(env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned()));
    command.args(["rustc", "--release", "--locked", "--features", "ffi", "--lib"]);
    command.env("CARGO_TARGET_DIR", target_dir);
    if let Some(target) = target {
        command.args(["--target", target]);
    }
    command
        .args(["--", "--print=native-static-libs"])
        .env("CONFLUENT_KAFKA_C_ABI_SONAME", "1")
        .stderr(Stdio::piped());
    let mut child = command.spawn().context("running cargo rustc")?;
    let mut output = String::new();
    for line in BufReader::new(child.stderr.take().context("cargo's stderr")?).lines() {
        let line = line?;
        eprintln!("{line}");
        output.push_str(&line);
        output.push('\n');
    }
    let status = child.wait()?;
    anyhow::ensure!(status.success(), "the release build of the C library failed ({status})");
    native_static_libs(&output).context(
        "rustc printed no native-static-libs note: the static library was not linked by this build \
         (touch src/lib.rs and run again)",
    )
}

/// Removes the LLVM bitcode rustc embeds in every object of a static library
/// for link-time optimization (`.llvmbc`, `.llvmcmd`), three quarters of its
/// size: a C or C++ linker reads only the machine code, which stays.
fn remove_bitcode(archive: &Path) -> anyhow::Result<()> {
    let objcopy = env::var("OBJCOPY").unwrap_or_else(|_| "objcopy".to_owned());
    let status = Command::new(&objcopy)
        .args(["--remove-section=.llvmbc", "--remove-section=.llvmcmd"])
        .arg(archive)
        .status()
        .with_context(|| format!("running {objcopy}"))?;
    anyhow::ensure!(
        status.success(),
        "{objcopy} could not remove the bitcode of {}",
        archive.display()
    );
    Ok(())
}

fn copy(from: &Path, to: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(to.parent().context("a file has a directory")?)?;
    fs::copy(from, to).with_context(|| format!("copying {} to {}", from.display(), to.display()))?;
    Ok(())
}

/// A relative symbolic link `at` → `to`, replacing one already there.
fn link(to: &str, at: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(at.parent().context("a link has a directory")?)?;
    if fs::symlink_metadata(at).is_ok() {
        fs::remove_file(at)?;
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(to, at).with_context(|| format!("linking {} to {to}", at.display()))?;
    #[cfg(not(unix))]
    anyhow::bail!("{}: links are installed on Unix only", at.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(args: &[&str]) -> Layout {
        Layout::parse(&args.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn linux_installs_the_versioned_file_and_both_links() {
        assert_eq!(
            library_entries(Platform::Elf, 0, "1.0.0"),
            vec![
                Entry::Copy("lib", "libconfluent_kafka.so.0.1.0.0".to_owned(), "libconfluent_kafka.so"),
                Entry::Link(
                    "lib",
                    "libconfluent_kafka.so.0".to_owned(),
                    "libconfluent_kafka.so.0.1.0.0".to_owned()
                ),
                Entry::Link("lib", "libconfluent_kafka.so".to_owned(), "libconfluent_kafka.so.0".to_owned()),
                Entry::Copy("lib", "libconfluent_kafka.a".to_owned(), "libconfluent_kafka.a"),
            ]
        );
    }

    #[test]
    fn macos_installs_the_install_name_and_windows_cargo_s_names() {
        assert!(library_entries(Platform::MachO, 2, "1.0.0").contains(&Entry::Copy(
            "lib",
            "libconfluent_kafka.2.dylib".to_owned(),
            "libconfluent_kafka.dylib"
        )));
        assert!(library_entries(Platform::Windows, 2, "1.0.0").contains(&Entry::Copy(
            "bin",
            "confluent_kafka.dll".to_owned(),
            "confluent_kafka.dll"
        )));
    }

    #[test]
    fn platform_is_read_from_the_target() {
        assert_eq!(Platform::of(Some("x86_64-unknown-linux-gnu")), Platform::Elf);
        assert_eq!(Platform::of(Some("aarch64-apple-darwin")), Platform::MachO);
        assert_eq!(Platform::of(Some("x86_64-pc-windows-msvc")), Platform::Windows);
    }

    #[test]
    fn c_abi_major_is_read_from_build_rs() {
        assert_eq!(c_abi_major("/// doc\nconst C_ABI_MAJOR: u32 = 3;\n"), Some(3));
        assert_eq!(c_abi_major("fn main() {}\n"), None);
        assert_eq!(c_abi_major(&fs::read_to_string("../build.rs").unwrap()), Some(0));
    }

    #[test]
    fn native_static_libs_are_read_from_rustc_s_note() {
        let output = "   Compiling confluent-kafka v1.0.0\nnote: Link against the following native artifacts...\n\
                      note: native-static-libs: -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc\n";
        assert_eq!(
            native_static_libs(output).as_deref(),
            Some("-lgcc_s -lutil -lrt -lpthread -lm -ldl -lc")
        );
        assert_eq!(native_static_libs("Finished\n"), None);
    }

    #[test]
    fn pkg_config_names_multiarch_dirs_absolutely_and_others_under_the_prefix() {
        let debian = layout(&["--prefix", "/usr", "--libdir", "/usr/lib/x86_64-linux-gnu"]);
        assert_eq!(
            pkg_config(&debian, "1.0.0", "-lgcc_s -lm"),
            "prefix=/usr\nlibdir=/usr/lib/x86_64-linux-gnu\nincludedir=${prefix}/include\n\n\
             Name: confluent-kafka\nDescription: The C API of the Confluent Kafka client\nVersion: 1.0.0\n\
             Libs: -L${libdir} -lconfluent_kafka\nLibs.private: -lgcc_s -lm\nCflags: -I${includedir}\n"
        );
    }

    #[test]
    fn destdir_stages_under_itself() {
        let staged = layout(&["--destdir", "/build/tmp", "--prefix", "/usr", "--libdir", "/usr/lib/x"]);
        assert_eq!(staged.staged(&staged.libdir), PathBuf::from("/build/tmp/usr/lib/x"));
        assert_eq!(staged.staged(&staged.includedir), PathBuf::from("/build/tmp/usr/include"));
        let direct = layout(&["--prefix", "/opt/ck"]);
        assert_eq!(direct.staged(&direct.libdir), PathBuf::from("/opt/ck/lib"));
    }

    #[test]
    fn a_relative_prefix_is_refused() {
        assert!(Layout::parse(&["--prefix".to_owned(), "usr".to_owned()]).is_err());
    }
}
