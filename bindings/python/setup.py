# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Build configuration for the Confluent Kafka Rust Python bindings.

The extension ``_confluentkafka`` links the Rust FFI library
``libconfluent_kafka``. There are two ways that library is provided:

* **Prebuilt (wheels / CI / local dev):** a prebuilt library is supplied via
  ``CONFLUENT_KAFKA_LIB_DIR`` (set by cibuildwheel in CI) or found in the repo's
  ``target/release``. It is linked dynamically and the wheel-repair tools
  (auditwheel / delocate / delvewheel) vendor it into the wheel. This is the
  primary distribution path and is unchanged.

* **From source (sdist):** when no prebuilt library is available -- i.e. a bare
  ``pip install`` of the sdist on a platform we ship no wheel for -- the bundled
  Rust crate (staged into ``rust/`` for the sdist) is compiled with
  ``cargo build --features ffi`` and **statically** linked into the extension.
  Static linking makes the extension self-contained with no runtime library
  discovery, which matters because none of the wheel-repair tooling runs during
  a bare ``pip install``. Requires the Rust toolchain (>= 1.85) plus cmake/perl
  (and nasm on x86_64) for the aws-lc-rs backend.

See design/current/python-wheel-ci-design.md ("buildable sdist") for the design.
"""

import os
import shutil
import subprocess
import sys

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext as _build_ext
from setuptools.command.sdist import sdist as _sdist

PACKAGE_DIR = os.path.abspath(os.path.dirname(__file__))
# Minimal Rust crate copy bundled into the sdist so a from-source install can
# build it. Present when building from an unpacked sdist; absent in a git
# checkout (where we fall back to the repo-root crate).
VENDORED_RUST = os.path.join(PACKAGE_DIR, 'rust')


def _repo_root():
    """Repo root for a local checkout (bindings/python -> two levels up)."""
    return os.path.abspath(os.path.join(PACKAGE_DIR, '..', '..'))


def _lib_present(directory):
    """True if a link-time libconfluent_kafka exists in `directory`."""
    if sys.platform == 'darwin':
        names = ['libconfluent_kafka.dylib']
    elif sys.platform == 'win32':
        names = ['confluent_kafka.lib', 'confluent_kafka.dll.lib']
    else:
        names = ['libconfluent_kafka.so']
    return any(os.path.exists(os.path.join(directory, n)) for n in names)


def _find_prebuilt_lib_dir():
    """Directory of a prebuilt libconfluent_kafka, or None to build from source.

    Trusts ``CONFLUENT_KAFKA_LIB_DIR`` when set (the CI wheel path); otherwise
    uses the repo's ``target/release`` if a library is there (local dev)."""
    env_dir = os.environ.get('CONFLUENT_KAFKA_LIB_DIR')
    if env_dir:
        if _lib_present(env_dir):
            return os.path.abspath(env_dir)
        sys.stderr.write(
            'confluent-kafka4: CONFLUENT_KAFKA_LIB_DIR=%s is set but no '
            'libconfluent_kafka is there; falling back to a from-source build.\n'
            % env_dir)
    default = os.path.join(_repo_root(), 'target', 'release')
    if _lib_present(default):
        return default
    return None


def _crate_dir():
    """Rust crate to build for a from-source install: the vendored copy (sdist)
    if present, else the repo-root crate (local checkout)."""
    if os.path.exists(os.path.join(VENDORED_RUST, 'Cargo.toml')):
        return VENDORED_RUST
    root = _repo_root()
    if os.path.exists(os.path.join(root, 'Cargo.toml')):
        return root
    return None


def _cargo_build_static(crate):
    """Build the FFI staticlib from source. Returns (include_dir, archive_path)."""
    env = dict(os.environ)
    # A Rust staticlib must be position-independent to link into a shared object
    # on ELF (Linux). macOS/Windows codegen is already PIC. This env applies only
    # to this from-source cargo call.
    if sys.platform not in ('darwin', 'win32'):
        existing = env.get('RUSTFLAGS', '').strip()
        env['RUSTFLAGS'] = (existing + ' -C relocation-model=pic').strip()
    cmd = ['cargo', 'build', '--features', 'ffi', '--release']
    sys.stderr.write(
        'confluent-kafka4: no prebuilt library found; building the Rust FFI '
        'library from source with: %s (cwd=%s)\n' % (' '.join(cmd), crate))
    try:
        subprocess.check_call(cmd, cwd=crate, env=env)
    except FileNotFoundError:
        raise SystemExit(
            'confluent-kafka4: `cargo` was not found on PATH. Building this '
            'package from source requires the Rust toolchain (>= 1.85) plus '
            'cmake/perl (and nasm on x86_64). Install Rust from https://rustup.rs '
            'or install a prebuilt wheel instead.')
    out_dir = os.path.join(crate, 'target', 'release')
    include_dir = os.path.join(crate, 'target', 'include')
    archive = os.path.join(
        out_dir,
        'confluent_kafka.lib' if sys.platform == 'win32'
        else 'libconfluent_kafka.a')
    return include_dir, archive


def _trim_cargo_toml(text):
    """Trim the workspace root Cargo.toml for the sdist copy: keep only
    `.` + `generator` as members and drop the `[dev-dependencies]` and `[[test]]`
    sections (they reference workspace members / a `tests/` tree not shipped in
    the sdist, and are not needed to `cargo build --features ffi`)."""
    out = []
    skipping = False
    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith('['):
            skipping = (stripped.startswith('[dev-dependencies]')
                        or stripped.startswith('[[test]]'))
            if skipping:
                continue
        if skipping:
            continue
        if stripped.startswith('members'):
            out.append('members = [".", "generator"]\n')
            continue
        out.append(line)
    return ''.join(out)


def _stage_rust_crate():
    """Copy the minimal Rust crate into VENDORED_RUST for the sdist (~7.5 MB:
    root crate + `generator` with its 197 message specs). Excludes the `kafka/`
    Java submodule, other workspace members, tests, and build artifacts."""
    root = _repo_root()
    if os.path.isdir(VENDORED_RUST):
        shutil.rmtree(VENDORED_RUST)
    os.makedirs(VENDORED_RUST)

    with open(os.path.join(root, 'Cargo.toml'), 'r') as f:
        cargo_toml = f.read()
    with open(os.path.join(VENDORED_RUST, 'Cargo.toml'), 'w') as f:
        f.write(_trim_cargo_toml(cargo_toml))

    for rel in ['build.rs', 'cbindgen.toml', 'Cargo.lock']:
        shutil.copy2(os.path.join(root, rel), os.path.join(VENDORED_RUST, rel))

    shutil.copytree(os.path.join(root, 'src'),
                    os.path.join(VENDORED_RUST, 'src'))
    gen_dst = os.path.join(VENDORED_RUST, 'generator')
    os.makedirs(gen_dst)
    shutil.copy2(os.path.join(root, 'generator', 'Cargo.toml'),
                 os.path.join(gen_dst, 'Cargo.toml'))
    shutil.copytree(os.path.join(root, 'generator', 'src'),
                    os.path.join(gen_dst, 'src'))
    # generator/messages/*.json are force-tracked despite the `*.json` gitignore;
    # copy the directory wholesale so all 197 specs are included.
    shutil.copytree(os.path.join(root, 'generator', 'messages'),
                    os.path.join(gen_dst, 'messages'))


class build_ext(_build_ext):
    """Link a prebuilt libconfluent_kafka when available; otherwise build the
    Rust FFI library from source and static-link it."""

    def build_extension(self, ext):
        prebuilt = _find_prebuilt_lib_dir()
        if prebuilt is not None:
            # Prebuilt / dynamic link (wheels, CI, local dev) -- unchanged path.
            # On Linux/macOS CI the header is supplied via CFLAGS -I; target/include
            # (below) additionally covers Windows CI and local dev.
            ext.include_dirs.append(os.path.join(_repo_root(), 'target', 'include'))
            ext.library_dirs.append(prebuilt)
            if sys.platform == 'win32':
                import_lib = os.path.join(prebuilt, 'confluent_kafka.dll.lib')
                if os.path.exists(import_lib):
                    ext.extra_objects.append(import_lib)
                else:
                    ext.libraries.append('confluent_kafka')
            else:
                ext.libraries.append('confluent_kafka')
                # rpath rewritten by delocate/auditwheel during wheel repair.
                ext.runtime_library_dirs.append(prebuilt)
        else:
            # From source / static link (bare sdist install).
            crate = _crate_dir()
            if crate is None:
                raise SystemExit(
                    'confluent-kafka4: no prebuilt libconfluent_kafka and no '
                    'Rust crate available to build from source.')
            include_dir, archive = _cargo_build_static(crate)
            ext.include_dirs.append(include_dir)
            # extra_objects forces the archive onto the link line, since a
            # co-located .so/.dylib would otherwise win over the .a.
            ext.extra_objects.append(archive)
            # Native libs the aws-lc-rs / Rust std static link pulls in.
            if sys.platform == 'darwin':
                ext.extra_link_args += ['-framework', 'Security',
                                        '-framework', 'CoreFoundation']
            elif sys.platform == 'win32':
                # TODO: this list is not yet CI-link-tested (no from-source Windows
                # job exists). Regenerate authoritatively on Windows via
                # `cargo rustc --features ffi --release -- --print native-static-libs`
                # once Stage-A Windows builds; current aws-lc-rs/tokio/std may also
                # need e.g. bcryptprimitives, synchronization, ncrypt, crypt32, secur32.
                ext.libraries += ['ntdll', 'bcrypt', 'advapi32', 'userenv',
                                  'kernel32', 'ws2_32']
            else:
                ext.libraries += ['pthread', 'dl', 'm']
        super().build_extension(ext)


class sdist(_sdist):
    """Stage the minimal Rust crate into `rust/` so the sdist is buildable from
    source, then build the source distribution."""

    def run(self):
        _stage_rust_crate()
        super().run()


# MSVC has no rpath (runtime_library_dirs) and rejects the gcc-style -std flag.
if sys.platform == 'win32':
    extra_compile_args = []
else:
    extra_compile_args = ['-std=c99']

ext = Extension(
    '_confluentkafka',
    sources=['_confluentkafka.c', 'tinycthread.c'],
    extra_compile_args=extra_compile_args,
)

setup(ext_modules=[ext], cmdclass={'build_ext': build_ext, 'sdist': sdist})
