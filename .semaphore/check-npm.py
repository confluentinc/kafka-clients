#!/usr/bin/env python3
#
# Copyright 2026 Confluent Inc.
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
#
#
# Checks the npm packages of the Node.js client, and the native addons they carry, before anything
# is recorded, and writes the package.json of each published package. Used by
# .semaphore/build-javascript-native.sh, .semaphore/pack-npm.sh, .semaphore/release-npm.sh,
# .semaphore/check-published-npm.py and create-release-tag.yml. Standard library only.
#
# Usage, from the repository root:
#   check-npm.py version
#       Prints "version" from javascript/package.json, after checking that it is a release version
#       (see release_version_problem) and that javascript/package-lock.json and every platform
#       package of javascript/npm/ have the same one (cargo xtask bump-version writes them all).
#   check-npm.py native <platform> <file> [--dev]
#       Checks that <file> is the statically linked addon of <platform>: its binary format and
#       architecture, the system libraries it loads (no libconfluent_kafka: the Rust core is
#       inside), no rpath, the symbols it exports (the two Node-API entry points, nothing else), and
#       its platform floor (glibc 2.28 on Linux, macOS 11.0 on macOS).
#   check-npm.py manifest <main|platform> [--version <v>]
#       Prints the package.json of the main package, or of the platform package of <platform>, as
#       pack-npm.sh publishes it: the main package's is the repository's without what is not
#       published, with each platform package of javascript/npm/ pinned at exactly the version in
#       its optionalDependencies; a platform package's is javascript/npm/<platform>/package.json,
#       committed as it is published. The version defaults to `version`'s.
#   check-npm.py dist-tag <version> [--tag <tag>]
#       Prints the npm dist-tag <version> is published under: `preview` for a pre-release, which a
#       check refuses to put on `latest`; `latest` for a release; `legacy` for a 1.x version, the
#       earlier client's fixes. With --tag, checks that <tag> is that dist-tag and prints it.
#   check-npm.py path-folders <dir>
#       Writes, into the staged main package <dir>, the folders behind its import paths: for each
#       path of `exports` but the root, <path>/package.json pointing at the path's compiled module
#       and declarations under dist/, for TypeScript setups and tools that do not read `exports`.
#   check-npm.py licenses <file>
#       Checks that <file> is the third-party licence file third-party-licenses.sh builds, which
#       every platform package ships as THIRD_PARTY_LICENSES.txt.
#   check-npm.py package <dir> [--version <v>] [--platforms "<platform> ..."] [--dev] [--published]
#       Checks the tarballs in <dir>, which must hold the main package and the platform package of
#       each platform (all of them by default) and nothing else: their file names, the exact list of
#       their entries and how they are stored, their package.json, and each addon as `native` does.
#
# --dev is for packages packed from a developer's or a CI job's own build (make -C javascript
# pack): the platform floors are not checked, since that addon was not built for them. A release
# package is never checked with --dev.
#
# --published is for tarballs downloaded from the registry by check-published-npm.py, which runs
# from master rather than from the commit that was released: their package.json and dist/ are
# checked for what every release has (the fields, no install scripts, the optional dependencies,
# the files of each entry point) rather than compared with this checkout's javascript/.

import argparse
import json
import os
import re
import struct
import sys
import tarfile

MAIN_PACKAGE = "@confluentinc/kafka-javascript"
PACKAGE_JSON = "javascript/package.json"
PACKAGE_LOCK = "javascript/package-lock.json"
SRC = "javascript/src"
# One folder per platform package, each holding its published package.json and README.md.
PLATFORM_DIR = "javascript/npm"
ADDON = "confluent-kafka-javascript.node"
# The third-party licence file of every platform package (third-party-licenses.sh), and the first
# line of the template it is built from (javascript/npm/about.hbs).
LICENSES = "THIRD_PARTY_LICENSES.txt"
LICENSES_TITLE = "Third-party licences of the native addon of @confluentinc/kafka-javascript"

# The release versions: X.Y.Z, optionally followed by -alpha.N, -beta.N, -dev.N, -preview.N or
# -rc.N. The
# javascript/v<version> tag pattern of semaphore.yml is this pattern after its prefix.
VERSION_PATTERN = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|dev|preview|rc)\.(0|[1-9][0-9]*))?"
# The package name is shared with confluent-kafka-javascript, whose versions are 1.x: this
# client's versions start at 4, the Apache Kafka line, so that none can be taken for one of those.
MIN_MAJOR = 4

# The platform packages of the preview, linux-x64 (glibc) and darwin-arm64: npm's os, cpu and libc
# of each, and the binary the addon must be. Each has its folder in javascript/npm/.
PLATFORMS = {
    "linux-x64-gnu": {"os": ["linux"], "cpu": ["x64"], "libc": ["glibc"]},
    "darwin-arm64": {"os": ["darwin"], "cpu": ["arm64"]},
}
ELF_MACHINES = {"linux-x64-gnu": 62}  # EM_X86_64

# The only symbols the addon exports: Node looks up these two (node_api.h).
ENTRY_POINTS = {"napi_register_module_v1", "node_api_module_get_api_version_v1"}
GLIBC_FLOOR = (2, 28)
MACOS_FLOOR = (11, 0, 0)
# The libraries a Linux addon may load, all of them part of glibc or GCC's runtime on every
# supported distribution. libgcc_s.so.1 is GCC's, whose version nodes are GCC_x.y[.z]: RHEL 8's
# GCC 8 provides up to GCC_7.0.0.
GLIBC_LIBRARIES = {"libc.so.6", "libm.so.6", "libdl.so.2", "libpthread.so.0", "librt.so.1",
                   "ld-linux-x86-64.so.2"}
LIBGCC_FLOOR = (7, 0, 0)
# The directories of the libraries a macOS addon may load: the system's.
MACOS_SYSTEM_PREFIXES = ("/usr/lib/", "/System/Library/")
# The Rust core is in the addon: below this size it cannot be.
MIN_ADDON_BYTES = 1024 * 1024
REPOSITORY = {"type": "git", "url": "git+https://github.com/confluentinc/kafka-clients.git", "directory": "javascript"}


def platform_repository(platform):
    """The repository field of a platform package: its folder of javascript/npm/."""
    return {**REPOSITORY, "directory": f"{PLATFORM_DIR}/{platform}"}
# The fields of the repository's package.json that are not published, and the scripts npm runs
# when a package is installed, none of which a published package may have.
UNPUBLISHED_FIELDS = ("private", "devDependencies", "scripts")
INSTALL_SCRIPTS = ("preinstall", "install", "postinstall", "prepare", "preprepare", "postprepare")
# The files of the main package besides dist/, and of a platform package. NOTICE carries Apache
# Kafka's credit line (javascript/NOTICE); every package ships it, as its "files" names it (npm
# adds the README and the LICENSE itself, but not a NOTICE). A platform package also ships the
# third-party licence file of the crates linked into its addon.
MAIN_FILES = {"package.json", "README.md", "CHANGELOG.md", "MIGRATION.md", "LICENSE", "NOTICE"}
PLATFORM_FILES = {"package.json", "README.md", "LICENSE", "NOTICE", LICENSES, ADDON}
# The "files" of a platform package's package.json: the root conventions' `dist/` and `NOTICE`,
# which every JavaScript package's "files" names (a platform package has no dist/, so that entry
# matches nothing), then the addon and the third-party licence file.
PLATFORM_FILES_FIELD = ["dist/", "NOTICE", ADDON, LICENSES]


def fail(message):
    sys.exit(f"check-npm: {message}")


def release_version_problem(version):
    """Why `version` is not a release version of the package, or None."""
    if not isinstance(version, str) or not re.fullmatch(VERSION_PATTERN, version):
        return f"'{version}' is not X.Y.Z or X.Y.Z-(alpha|beta|dev|preview|rc).N"
    if int(version.split(".")[0]) < MIN_MAJOR:
        return (f"'{version}' is below {MIN_MAJOR}.0.0; the versions of {MAIN_PACKAGE} below {MIN_MAJOR} are "
                "confluent-kafka-javascript's")
    return None


def path_folders(manifest):
    """{<path>/package.json: its content} for each import path of `exports` but the root: the
    folders behind the import paths, which a resolver that does not read `exports` (TypeScript's
    node10 resolution, an older bundler) finds as a directory with a package.json."""
    folders = {}
    for subpath, entry in (manifest.get("exports") or {}).items():
        if subpath == ".":
            continue
        folder = subpath[2:]
        up = "../" * (folder.count("/") + 1)
        if not isinstance(entry, dict) or not isinstance(entry.get("default"), str) or not isinstance(entry.get("types"), str):
            fail(f"the export {subpath} has no types and default targets")
        stub = {
            "main": up + entry["default"].removeprefix("./"),
            "types": up + entry["types"].removeprefix("./"),
        }
        folders[f"{folder}/package.json"] = json.dumps(stub, indent=2) + "\n"
    return folders


def path_folder_tops(manifest):
    """The top-level folders of the import paths, which the main package's "files" must name."""
    return sorted({subpath[2:].split("/")[0] + "/" for subpath in (manifest.get("exports") or {}) if subpath != "."})


def dist_tag(version, requested=None):
    """The dist-tag `version` is published under (see the usage above), after checking that
    `requested`, when given, is that one."""
    if not isinstance(version, str) or not re.fullmatch(VERSION_PATTERN, version):
        fail(f"'{version}' is not X.Y.Z or X.Y.Z-(alpha|beta|dev|preview|rc).N")
    major = int(version.split(".")[0])
    prerelease = "-" in version
    if major < MIN_MAJOR:
        # The earlier client's line, whose fixes keep the name's 1.x users off this client.
        expected = "legacy"
        why = f"{version} is a fix of the earlier client (below {MIN_MAJOR}.0.0), which goes out under legacy"
    elif prerelease:
        expected = "preview"
        why = f"{version} is a pre-release, which goes out under preview only, never latest"
    else:
        expected = "latest"
        why = f"{version} is a release, which goes out under latest"
    if requested is not None and requested != expected:
        fail(f"refusing the dist-tag {requested!r}: {why}")
    return expected


def read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError) as error:
        fail(f"could not read {path}: {error}")


def repo_version():
    package = read_json(PACKAGE_JSON)
    if package.get("name") != MAIN_PACKAGE:
        fail(f"{PACKAGE_JSON} names the package {package.get('name')!r}, not {MAIN_PACKAGE}")
    version = package.get("version")
    problem = release_version_problem(version)
    if problem:
        fail(f'"version" in {PACKAGE_JSON}: {problem}')
    lock = read_json(PACKAGE_LOCK)
    root = lock.get("packages", {}).get("", {})
    if lock.get("version") != version or root.get("version") != version:
        fail(f"{PACKAGE_LOCK} has the versions {lock.get('version')!r} and {root.get('version')!r}, not {version}; "
             "run `npm install --package-lock-only` in javascript/, or `make bump-version`")
    for platform in PLATFORMS:
        committed = committed_platform_manifest(platform)
        if committed.get("version") != version:
            fail(f"{PLATFORM_DIR}/{platform}/package.json has the version {committed.get('version')!r}, not {version}; "
                 "`make bump-version` writes them together")
    return version


def committed_platform_manifest(platform):
    """javascript/npm/<platform>/package.json, after checking that javascript/npm/ holds exactly
    the folders of PLATFORMS and that the manifest is a platform package's."""
    try:
        folders = sorted(entry for entry in os.listdir(PLATFORM_DIR) if os.path.isdir(os.path.join(PLATFORM_DIR, entry)))
    except OSError as error:
        fail(f"could not list {PLATFORM_DIR}: {error}")
    if folders != sorted(PLATFORMS):
        fail(f"{PLATFORM_DIR} has the folders {', '.join(folders)}, not one per platform package: {', '.join(sorted(PLATFORMS))}")
    path = f"{PLATFORM_DIR}/{platform}/package.json"
    manifest = read_json(path)
    if not isinstance(manifest, dict):
        fail(f"{path} is not an object")
    package = read_json(PACKAGE_JSON)
    expected = {
        "name": f"{MAIN_PACKAGE}-{platform}",
        "license": package.get("license"),
        "repository": platform_repository(platform),
        "main": ADDON,
        "files": PLATFORM_FILES_FIELD,
        "engines": package.get("engines"),
        **PLATFORMS[platform],
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            fail(f"{path} has {field} {manifest.get(field)!r}, not {value!r}")
    allowed = set(expected) | {"version", "description"}
    for field in sorted(set(manifest) - allowed):
        fail(f"{path} has {field!r}, which a platform package has none of (no scripts, no dependencies)")
    if not os.path.isfile(f"{PLATFORM_DIR}/{platform}/README.md"):
        fail(f"{PLATFORM_DIR}/{platform} has no README.md")
    return manifest


def main_manifest(version):
    """The package.json of the main package: the repository's, without what is not published, with
    the platform packages as optional dependencies at exactly this version."""
    manifest = {key: value for key, value in read_json(PACKAGE_JSON).items() if key not in UNPUBLISHED_FIELDS}
    manifest["version"] = version
    # The pins: each platform package at exactly this version, with no download fallback. They are
    # written here, not committed in javascript/package.json: there, `npm ci` would fail once the
    # version is on the registry, since the lockfile cannot lock a package not yet published.
    manifest["optionalDependencies"] = {committed_platform_manifest(platform)["name"]: version for platform in PLATFORMS}
    return manifest


def platform_manifest(platform, version):
    """The package.json of the platform package of `platform`: the committed one, at `version`."""
    manifest = dict(committed_platform_manifest(platform))
    manifest["version"] = version
    return manifest


def tarball_name(platform, version):
    """The file `npm pack` writes for the main package (platform None) or a platform package."""
    suffix = f"-{platform}" if platform else ""
    return f"confluentinc-kafka-javascript{suffix}-{version}.tgz"


def uleb128(data, pos):
    value = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return value, pos
        if shift > 63:
            raise ValueError("a ULEB128 number is too long")


def elf_dynamic(data):
    """Returns (DT_NEEDED libraries, {library: [version names]}, exported symbols, tags) of the ELF
    file, read from its dynamic segment, as the dynamic loader reads them, rather than from
    section headers, which a stripped file need not have."""
    (phoff,) = struct.unpack_from("<Q", data, 0x20)
    phentsize, phnum = struct.unpack_from("<HH", data, 0x36)
    loads = []
    dynamic = None
    for i in range(phnum):
        p_type, _flags, offset, vaddr, _paddr, filesz, _memsz, _align = struct.unpack_from("<IIQQQQQQ", data, phoff + i * phentsize)
        if p_type == 1:  # PT_LOAD
            loads.append((vaddr, offset, filesz))
        elif p_type == 2:  # PT_DYNAMIC
            dynamic = (offset, filesz)
    if dynamic is None:
        raise ValueError("it has no dynamic segment")

    def file_offset(address):
        for vaddr, offset, filesz in loads:
            if vaddr <= address < vaddr + filesz:
                return offset + address - vaddr
        raise ValueError(f"its dynamic section points outside its segments ({address:#x})")

    needed_names = []
    tags = {}
    pos, end = dynamic[0], dynamic[0] + dynamic[1]
    while pos + 16 <= end:
        tag, value = struct.unpack_from("<qQ", data, pos)
        pos += 16
        if tag == 0:  # DT_NULL
            break
        if tag == 1:  # DT_NEEDED
            needed_names.append(value)
        else:
            tags.setdefault(tag, value)
    if 5 not in tags or 6 not in tags:  # DT_STRTAB, DT_SYMTAB
        raise ValueError("its dynamic section has no string or symbol table")
    strtab = file_offset(tags[5])

    def string(offset):
        start = strtab + offset
        return data[start:data.index(b"\0", start)].decode("ascii", "replace")

    needed = [string(name) for name in needed_names]
    versions = {}
    if 0x6FFFFFFE in tags:  # DT_VERNEED, with DT_VERNEEDNUM entries
        entry = file_offset(tags[0x6FFFFFFE])
        for _ in range(tags.get(0x6FFFFFFF, 0)):
            _version, count, file, aux, next_entry = struct.unpack_from("<HHIII", data, entry)
            names = versions.setdefault(string(file), [])
            pos = entry + aux
            for _ in range(count):
                _hash, _vflags, _other, name, next_aux = struct.unpack_from("<IHHII", data, pos)
                names.append(string(name))
                if not next_aux:
                    break
                pos += next_aux
            if not next_entry:
                break
            entry += next_entry

    # The number of dynamic symbols, from the hash table the loader looks symbols up in.
    if 0x6FFFFEF5 in tags:  # DT_GNU_HASH
        table = file_offset(tags[0x6FFFFEF5])
        nbuckets, symoffset, bloom_size, _shift = struct.unpack_from("<IIII", data, table)
        buckets = table + 16 + bloom_size * 8
        last = max(struct.unpack_from(f"<{nbuckets}I", data, buckets), default=0)
        count = symoffset
        if last >= symoffset:
            chains = buckets + nbuckets * 4
            while not struct.unpack_from("<I", data, chains + (last - symoffset) * 4)[0] & 1:
                last += 1
            count = last + 1
    elif 4 in tags:  # DT_HASH
        count = struct.unpack_from("<I", data, file_offset(tags[4]) + 4)[0]
    else:
        raise ValueError("its dynamic section has no symbol hash table")
    symtab = file_offset(tags[6])
    entsize = tags.get(11, 24)  # DT_SYMENT
    exported = set()
    for i in range(1, count):
        st_name, st_info, st_other, st_shndx, _value, _size = struct.unpack_from("<IBBHQQ", data, symtab + i * entsize)
        # Defined, global, weak or unique, and default or protected visibility: what another module can bind to.
        if st_shndx != 0 and st_info >> 4 in (1, 2, 10) and st_other & 3 in (0, 3):
            exported.add(string(st_name))
    return needed, versions, exported, tags


def check_elf(platform, name, data, dev):
    problems = []
    if data[7] not in (0, 3):  # ELFOSABI_NONE (System V), ELFOSABI_GNU
        problems.append(f"{platform}: {name} is for ELF OS ABI {data[7]}, not Linux")
    try:
        needed, versions, exported, tags = elf_dynamic(data)
    except (ValueError, struct.error, IndexError) as error:
        return problems + [f"{platform}: the dynamic section of {name} cannot be read: {error}"]
    for library in needed:
        if library not in GLIBC_LIBRARIES and library != "libgcc_s.so.1":
            problems.append(f"{platform}: {name} loads {library}, which is not part of glibc or GCC's runtime, so not on "
                            "every supported distribution; link libconfluent_kafka statically (CONFLUENT_KAFKA_LINK=static)")
    if "libc.so.6" not in needed:
        problems.append(f"{platform}: {name} does not load libc.so.6")
    for tag, label in ((15, "DT_RPATH"), (29, "DT_RUNPATH")):
        if tag in tags:
            problems.append(f"{platform}: {name} has a {label}, which a statically linked addon does not need")
    glibc = []
    for library, names in versions.items():
        for version in names:
            if library in GLIBC_LIBRARIES:
                match = re.fullmatch(r"GLIBC_(\d+)\.(\d+)(?:\.\d+)?", version)
                if not match:
                    # GLIBC_PRIVATE, GLIBC_ABI_DT_RELR (glibc 2.36), ...: no floor can be read.
                    problems.append(f"{platform}: {name} requires {version} of {library}, which is not a glibc release")
                    continue
                glibc.append((int(match.group(1)), int(match.group(2))))
                if not dev and glibc[-1] > GLIBC_FLOOR:
                    problems.append(
                        f"{platform}: {name} requires glibc {version[6:]}, newer than the {GLIBC_FLOOR[0]}.{GLIBC_FLOOR[1]} "
                        "the package supports; build it in the manylinux_2_28 image")
            elif library == "libgcc_s.so.1":
                match = re.fullmatch(r"GCC_(\d+)\.(\d+)(?:\.(\d+))?", version)
                if not match:
                    problems.append(f"{platform}: {name} requires {version} of {library}, which is not a GCC release")
                elif not dev and (int(match.group(1)), int(match.group(2)), int(match.group(3) or 0)) > LIBGCC_FLOOR:
                    problems.append(f"{platform}: {name} requires {version} of {library}, newer than the GCC runtime of RHEL 8")
            else:
                problems.append(f"{platform}: {name} requires {version} of {library}, a library it may not load")
    if not glibc and not dev:
        problems.append(f"{platform}: {name} requires no glibc version of libc.so.6, so its glibc floor cannot be read")
    if exported != ENTRY_POINTS:
        problems.append(exports_problem(platform, name, exported, ENTRY_POINTS))
    return problems


def exports_problem(platform, name, exported, expected):
    extra = sorted(exported - expected)
    missing = sorted(expected - exported)
    shown = ", ".join(extra[:10]) + (f" and {len(extra) - 10} more" if len(extra) > 10 else "")
    return (f"{platform}: {name} must export exactly {', '.join(sorted(expected))}"
            + (f"; it lacks {', '.join(missing)}" if missing else "")
            + (f"; it also exports {shown}" if extra else ""))


def macho_exports(data, offset, size):
    """The names in the Mach-O export trie at `offset`."""
    if offset + size > len(data):
        raise ValueError("its export trie extends beyond the end of the file")
    names = []
    pending = [(0, "")]
    seen = set()
    while pending:
        node, prefix = pending.pop()
        if node >= size or node in seen:
            raise ValueError("its export trie is malformed")
        seen.add(node)
        pos = offset + node
        terminal, pos = uleb128(data, pos)
        if terminal:
            names.append(prefix)
        pos += terminal
        children = data[pos]
        pos += 1
        for _ in range(children):
            end = data.index(b"\0", pos)
            edge = data[pos:end].decode("utf-8", "replace")
            child, pos = uleb128(data, end + 1)
            pending.append((child, prefix + edge))
    return names


def check_macho(platform, name, data, dev):
    cputype, subtype = {"darwin-arm64": (0x0100000C, 0)}[platform]  # CPU_TYPE_ARM64, CPU_SUBTYPE_ARM64_ALL
    file_cputype, file_subtype, filetype, ncmds, sizeofcmds = struct.unpack_from("<IIIII", data, 4)
    problems = []
    if file_cputype != cputype:
        problems.append(f"{platform}: {name} is for Mach-O CPU type {file_cputype:#x}, not {cputype:#x}")
    elif file_subtype & 0x00FFFFFF != subtype:  # without the capability bits; arm64e is 2
        problems.append(f"{platform}: {name} is for Mach-O CPU subtype {file_subtype:#x}, not {subtype:#x}")
    if filetype != 8:  # MH_BUNDLE, what node-gyp links an addon as
        problems.append(f"{platform}: {name} is Mach-O file type {filetype}, not a bundle")
    if problems:
        return problems
    if 32 + sizeofcmds > len(data):
        return [f"{platform}: the load commands of {name} extend beyond the end of the file"]

    def string_at(pos, cmdsize):
        (offset,) = struct.unpack_from("<I", data, pos + 8)
        end = data.find(b"\0", pos + offset, pos + cmdsize)
        return data[pos + offset:end if end >= 0 else pos + cmdsize].decode("utf-8", "replace")

    def version(value):
        return (value >> 16, (value >> 8) & 0xFF, value & 0xFF)

    minimums = []
    signed = False
    tries = []
    pos = 32
    for _ in range(ncmds):
        cmd, cmdsize = struct.unpack_from("<II", data, pos)
        if cmdsize < 8 or pos + cmdsize > 32 + sizeofcmds:
            return problems + [f"{platform}: {name} has a malformed load command"]
        if cmd == 0x32:  # LC_BUILD_VERSION
            build_platform, minos = struct.unpack_from("<II", data, pos + 8)
            if build_platform != 1:  # PLATFORM_MACOS
                problems.append(f"{platform}: {name} is built for Mach-O platform {build_platform}, not macOS")
            minimums.append(version(minos))
        elif cmd == 0x24:  # LC_VERSION_MIN_MACOSX
            minimums.append(version(struct.unpack_from("<I", data, pos + 8)[0]))
        elif cmd in (0x0C, 0x20, 0x80000018, 0x8000001F, 0x80000023):
            # LC_LOAD_DYLIB, LC_LAZY_LOAD_DYLIB, LC_LOAD_WEAK_DYLIB, LC_REEXPORT_DYLIB, LC_LOAD_UPWARD_DYLIB
            path = string_at(pos, cmdsize)
            if not path.startswith(MACOS_SYSTEM_PREFIXES):
                problems.append(f"{platform}: {name} loads {path}, which is not a library of macOS itself; link "
                                "libconfluent_kafka statically (CONFLUENT_KAFKA_LINK=static)")
        elif cmd == 0x8000001C:  # LC_RPATH
            problems.append(f"{platform}: {name} has the rpath {string_at(pos, cmdsize)!r}, which a statically linked "
                            "addon does not need")
        elif cmd in (0x22, 0x80000022):  # LC_DYLD_INFO, LC_DYLD_INFO_ONLY
            tries.append(struct.unpack_from("<II", data, pos + 40))
        elif cmd == 0x80000033:  # LC_DYLD_EXPORTS_TRIE
            tries.append(struct.unpack_from("<II", data, pos + 8))
        elif cmd == 0x19:  # LC_SEGMENT_64
            fileoff, filesize = struct.unpack_from("<QQ", data, pos + 40)
            if fileoff + filesize > len(data):
                problems.append(f"{platform}: a segment of {name} extends beyond the end of the file")
        elif cmd == 0x1D:  # LC_CODE_SIGNATURE
            dataoff, datasize = struct.unpack_from("<II", data, pos + 8)
            signed = dataoff + datasize <= len(data)
        pos += cmdsize
    if not signed:
        # macOS on Apple silicon loads only signed code; an ad hoc signature is enough.
        problems.append(f"{platform}: {name} has no code signature, which macOS on Apple silicon requires")
    if not minimums:
        problems.append(f"{platform}: {name} names no minimum macOS version (LC_BUILD_VERSION or LC_VERSION_MIN_MACOSX)")
    elif not dev and max(minimums) > MACOS_FLOOR:
        problems.append(
            f"{platform}: {name} requires macOS {'.'.join(map(str, max(minimums)))}, newer than the "
            f"{'.'.join(map(str, MACOS_FLOOR))} the package supports; build it with MACOSX_DEPLOYMENT_TARGET=11.0")
    exported = set()
    try:
        for offset, size in tries:
            if size:
                exported.update(macho_exports(data, offset, size))
    except (ValueError, IndexError) as error:
        return problems + [f"{platform}: the exports of {name} cannot be read: {error}"]
    expected = {f"_{symbol}" for symbol in ENTRY_POINTS}
    if exported != expected:
        problems.append(exports_problem(platform, name, exported, expected))
    return problems


def check_native(platform, name, data, dev):
    if platform not in PLATFORMS:
        return [f"{platform} is not one of: {', '.join(PLATFORMS)}"]
    if name != ADDON:
        return [f"{platform}: the addon is {name}, not {ADDON}"]
    if len(data) < MIN_ADDON_BYTES:
        return [f"{platform}: {name} is only {len(data)} bytes, too small to carry the Rust core; link it statically "
                "(CONFLUENT_KAFKA_LINK=static)"]
    try:
        if platform.startswith("linux-"):
            if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
                return [f"{platform}: {name} is not a 64-bit little-endian ELF file"]
            e_type, e_machine = struct.unpack_from("<HH", data, 16)
            if e_type != 3:
                return [f"{platform}: {name} is ELF type {e_type}, not a shared object"]
            if e_machine != ELF_MACHINES[platform]:
                return [f"{platform}: {name} is for ELF machine {e_machine}, not {ELF_MACHINES[platform]}"]
            return check_elf(platform, name, data, dev)
        if data[:4] != b"\xcf\xfa\xed\xfe":
            return [f"{platform}: {name} is not a thin 64-bit little-endian Mach-O file"]
        return check_macho(platform, name, data, dev)
    except struct.error as error:
        return [f"{platform}: {name} is truncated: {error}"]


def read_tarball(path):
    """Returns ({entry name: bytes}, problems) of the tarball, refusing what npm would unpack
    differently from what it lists: anything but regular files, names outside package/, names
    that repeat."""
    problems = []
    files = {}
    try:
        archive = tarfile.open(path, "r:gz")
    except (OSError, tarfile.TarError) as error:
        return None, [f"{path} is not a gzipped tarball: {error}"]
    with archive:
        try:
            members = archive.getmembers()
        except (OSError, tarfile.TarError, EOFError) as error:
            return None, [f"{path} cannot be read: {error}"]
        for member in members:
            name = member.name
            parts = name.split("/")
            if not member.isreg():
                problems.append(f"{os.path.basename(path)}: the entry {name!r} is not a regular file")
                continue
            if parts[0] != "package" or len(parts) < 2 or "" in parts or ".." in parts or "\\" in name or "\0" in name:
                problems.append(f"{os.path.basename(path)}: the entry {name!r} is not a safe path under package/")
                continue
            if name in files:
                problems.append(f"{os.path.basename(path)}: the entry {name!r} appears twice")
                continue
            files[name] = archive.extractfile(member).read()
    lowered = [name.lower() for name in files]
    if len(set(lowered)) != len(lowered):
        problems.append(f"{os.path.basename(path)}: two entries have names that differ only in case")
    return files, problems


def dist_problems(dist, manifest, compare_sources):
    """Checks the files under package/dist/ of the main package: compiled from javascript/src/
    when `compare_sources`, otherwise for what every release has."""
    problems = []
    if compare_sources:
        expected = set()
        for folder, _dirs, names in os.walk(SRC):
            for source in names:
                if not source.endswith(".ts"):
                    continue
                stem = os.path.relpath(os.path.join(folder, source), SRC)[:-3].replace(os.sep, "/")
                expected |= {f"{stem}.js", f"{stem}.js.map"}
                # tsc's declarations of a private module (src/internal/) are not shipped (javascript/Makefile).
                if not stem.startswith("internal/"):
                    expected |= {f"{stem}.d.ts", f"{stem}.d.ts.map"}
        if not expected:
            fail(f"{SRC} holds no TypeScript source; run from the repository root")
        for extra in sorted(set(dist) - expected):
            problems.append(f"the main package has dist/{extra}, which javascript/src/ does not compile to")
        for missing in sorted(expected - set(dist)):
            problems.append(f"the main package has no dist/{missing}")
    else:
        for name in sorted(dist):
            if not re.fullmatch(r"[a-z0-9_/]+\.(js|js\.map|d\.ts|d\.ts\.map)", name):
                problems.append(f"the main package has dist/{name}, which is not compiled TypeScript")
            elif name.startswith("internal/") and ".d.ts" in name:
                problems.append(f"the main package has dist/{name}, the declaration of a private module")
    targets = [manifest.get("main"), manifest.get("types")]
    for entry in (manifest.get("exports") or {}).values():
        targets += list(entry.values()) if isinstance(entry, dict) else [entry]
    for target in targets:
        if not isinstance(target, str) or not re.fullmatch(r"(\./)?dist/.+", target):
            problems.append(f"the main package's package.json points at {target!r}, not a file under dist/")
        elif re.sub(r"^(\./)?dist/", "", target) not in dist:
            problems.append(f"the main package's package.json points at {target}, which it does not hold")
    return problems


def differing(actual, expected):
    """The top-level fields in which two package.json differ, for a message."""
    return ", ".join(sorted(key for key in set(actual) | set(expected) if actual.get(key) != expected.get(key)))


def manifest_problems(label, manifest, version, repository=REPOSITORY):
    """What no published package.json may have, whatever its content."""
    problems = []
    if manifest.get("version") != version:
        problems.append(f"{label} has the version {manifest.get('version')!r}, not {version}")
    for field in UNPUBLISHED_FIELDS:
        if field in manifest:
            problems.append(f"{label} has {field!r}, which is not published")
    for script in INSTALL_SCRIPTS:
        if script in (manifest.get("scripts") or {}):
            problems.append(f"{label} has an install script {script!r}")
    for field in ("dependencies", "peerDependencies", "bundleDependencies", "bundledDependencies"):
        if manifest.get(field):
            problems.append(f"{label} has {field}, which this package has none of")
    if manifest.get("license") != "Apache-2.0":
        problems.append(f"{label} has the license {manifest.get('license')!r}, not Apache-2.0")
    if manifest.get("repository") != repository:
        problems.append(f"{label} has the repository {manifest.get('repository')!r}, not {repository}")
    return problems


def check_main(files, version, published):
    label = f"the package.json of {MAIN_PACKAGE}"
    problems = []
    try:
        manifest = json.loads(files["package/package.json"])
    except ValueError as error:
        return [f"{label} is not JSON: {error}"]
    if not isinstance(manifest, dict):
        return [f"{label} is not an object"]
    if manifest.get("name") != MAIN_PACKAGE:
        problems.append(f"{label} names {manifest.get('name')!r}")
    problems += manifest_problems(label, manifest, version)
    optional = {f"{MAIN_PACKAGE}-{platform}": version for platform in PLATFORMS}
    if manifest.get("optionalDependencies") != optional:
        problems.append(f"{label} has the optionalDependencies {manifest.get('optionalDependencies')!r}, not {optional}")
    if manifest.get("gypfile") is not False:
        problems.append(f'{label} lacks "gypfile": false, which keeps npm from building on install')
    if not published and manifest != main_manifest(version):
        problems.append(f"{label} is not the one pack-npm.sh writes; it differs in {differing(manifest, main_manifest(version))}")
    names = {name[len("package/"):] for name in files}
    dist = {name[len("dist/"):] for name in names if name.startswith("dist/")}
    folders = path_folders(manifest)
    for folder, content in sorted(folders.items()):
        if folder not in names:
            problems.append(f"the main package has no {folder}, the folder behind the import path {folder[:-len('/package.json')]}")
        elif files[f"package/{folder}"].decode("utf-8", "replace") != content:
            problems.append(f"the main package's {folder} is not the one check-npm.py path-folders writes")
        else:
            stub = json.loads(content)
            for field in ("main", "types"):
                target = os.path.normpath(os.path.join(os.path.dirname(folder), stub[field])).replace(os.sep, "/")
                if target not in names:
                    problems.append(f"the main package's {folder} points its {field} at {stub[field]}, which it does not hold")
    for top in path_folder_tops(manifest):
        if top not in (manifest.get("files") or []):
            problems.append(f'{label} has no "{top}" in its files, so npm leaves out the folders behind its import paths')
    for extra in sorted(names - MAIN_FILES - set(folders) - {f"dist/{d}" for d in dist}):
        why = " (a build input, a test or the gRPC harness)" if re.match(
            r"(binding\.gyp|native/|src/|test/|npm/|api/|verifiable_clients/|scripts/|tools/|build/|examples/|grpc|tsconfig|Makefile|.*\.node$)", extra) else ""
        problems.append(f"the main package has {extra}{why}, which it must not carry")
    for missing in sorted(MAIN_FILES - names):
        problems.append(f"the main package has no {missing}")
    for name in ("README.md", "CHANGELOG.md", "MIGRATION.md", "LICENSE", "NOTICE"):
        if name in names and not files[f"package/{name}"].strip():
            problems.append(f"the main package's {name} is empty")
    return problems + dist_problems(dist, manifest, not published)


def check_platform(platform, files, version, dev, published):
    label = f"the package.json of {MAIN_PACKAGE}-{platform}"
    problems = []
    try:
        manifest = json.loads(files["package/package.json"])
    except ValueError as error:
        return [f"{label} is not JSON: {error}"]
    if not isinstance(manifest, dict):
        return [f"{label} is not an object"]
    problems += manifest_problems(label, manifest, version, platform_repository(platform))
    expected = {"name": f"{MAIN_PACKAGE}-{platform}", "main": ADDON, "files": PLATFORM_FILES_FIELD, **PLATFORMS[platform]}
    for field, value in expected.items():
        if manifest.get(field) != value:
            problems.append(f"{label} has {field} {manifest.get(field)!r}, not {value!r}")
    if "libc" in manifest and "libc" not in PLATFORMS[platform]:
        problems.append(f"{label} has a libc, which {platform} has none of")
    if not published and manifest != platform_manifest(platform, version):
        problems.append(f"{label} is not the one pack-npm.sh writes; it differs in "
                        f"{differing(manifest, platform_manifest(platform, version))}")
    names = {name[len("package/"):] for name in files}
    for extra in sorted(names - PLATFORM_FILES):
        problems.append(f"{MAIN_PACKAGE}-{platform} has {extra}, which it must not carry")
    for missing in sorted(PLATFORM_FILES - names):
        problems.append(f"{MAIN_PACKAGE}-{platform} has no {missing}")
    if "NOTICE" in names and not files["package/NOTICE"].strip():
        problems.append(f"{MAIN_PACKAGE}-{platform}'s NOTICE is empty")
    if LICENSES in names:
        problems += [f"{MAIN_PACKAGE}-{platform}: {problem}" for problem in licenses_problems(files[f"package/{LICENSES}"])]
    if ADDON in names:
        problems += check_native(platform, ADDON, files[f"package/{ADDON}"], dev)
    return problems


def licenses_problems(data):
    """What is wrong with a third-party licence file (third-party-licenses.sh), or nothing."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return [f"{LICENSES} is not UTF-8"]
    problems = []
    if text.splitlines()[:1] != [LICENSES_TITLE]:
        problems.append(f"{LICENSES} does not start with {LICENSES_TITLE!r}: it is not the file about.hbs makes")
    # Every crate linked in is listed under a licence ("  - <crate> <version>"); the core alone
    # links dozens, so a file listing none was built from an empty dependency graph.
    if not re.search(r"^  - [A-Za-z0-9_-]+ [0-9]", text, re.MULTILINE):
        problems.append(f"{LICENSES} lists no crate")
    if "Apache License" not in text:
        problems.append(f"{LICENSES} carries no Apache License text")
    if "&quot;" in text or "&amp;" in text or "&lt;" in text:
        problems.append(f"{LICENSES} holds HTML entities: about.hbs must use triple braces")
    return problems


def check_packages(directory, version, platforms, dev, published):
    expected = {tarball_name(None, version): None}
    expected.update({tarball_name(platform, version): platform for platform in platforms})
    try:
        present = set(os.listdir(directory))
    except OSError as error:
        fail(f"could not list {directory}: {error}")
    problems = [f"{directory} holds {name}, which is not one of the expected tarballs: {', '.join(sorted(expected))}"
                for name in sorted(present - set(expected))]
    problems += [f"{directory} has no {name}" for name in sorted(set(expected) - present)]
    for name, platform in expected.items():
        if name not in present:
            continue
        files, read_problems = read_tarball(os.path.join(directory, name))
        problems += read_problems
        if files is None:
            continue
        if "package/package.json" not in files:
            problems.append(f"{name} has no package/package.json")
            continue
        found = check_main(files, version, published) if platform is None else check_platform(platform, files, version, dev, published)
        problems += [f"{name}: {problem}" for problem in found]
    return problems


def main():
    parser = argparse.ArgumentParser(description="Checks the npm packages of the Node.js client.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("version")
    folders = commands.add_parser("path-folders")
    folders.add_argument("directory")
    tag = commands.add_parser("dist-tag")
    tag.add_argument("version")
    tag.add_argument("--tag")
    licenses = commands.add_parser("licenses")
    licenses.add_argument("file")
    native = commands.add_parser("native")
    native.add_argument("platform")
    native.add_argument("file")
    native.add_argument("--dev", action="store_true")
    manifest = commands.add_parser("manifest")
    manifest.add_argument("package", help="main, or a platform")
    manifest.add_argument("--version")
    package = commands.add_parser("package")
    package.add_argument("directory")
    package.add_argument("--version")
    package.add_argument("--platforms", default=" ".join(PLATFORMS))
    package.add_argument("--dev", action="store_true")
    package.add_argument("--published", action="store_true")
    args = parser.parse_args()

    if args.command == "version":
        print(repo_version())
        return
    if args.command == "path-folders":
        manifest = read_json(os.path.join(args.directory, "package.json"))
        for folder, content in path_folders(manifest).items():
            target = os.path.join(args.directory, folder)
            if os.path.exists(target):
                fail(f"{target} exists already")
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "w", encoding="utf-8") as f:
                f.write(content)
        return
    if args.command == "dist-tag":
        print(dist_tag(args.version, args.tag))
        return
    if args.command == "licenses":
        try:
            with open(args.file, "rb") as f:
                data = f.read()
        except OSError as error:
            fail(f"could not read {args.file}: {error}")
        problems = licenses_problems(data)
        if problems:
            fail("\n".join(f"{args.file}: {problem}" for problem in problems))
        print(f"{args.file} is the third-party licence file")
        return
    if args.command == "native":
        try:
            with open(args.file, "rb") as f:
                data = f.read()
        except OSError as error:
            fail(f"could not read {args.file}: {error}")
        problems = check_native(args.platform, os.path.basename(args.file), data, args.dev)
        if problems:
            fail("\n".join(problems))
        print(f"{args.file} is the {args.platform} addon")
        return
    version = args.version or repo_version()
    problem = release_version_problem(version)
    if problem:
        fail(f"--version: {problem}")
    if args.command == "manifest":
        if args.package == "main":
            print(json.dumps(main_manifest(version), indent=2))
        elif args.package in PLATFORMS:
            print(json.dumps(platform_manifest(args.package, version), indent=2))
        else:
            fail(f"{args.package} is neither main nor one of: {', '.join(PLATFORMS)}")
        return
    platforms = args.platforms.split()
    if not platforms or len(set(platforms)) != len(platforms) or not set(platforms) <= set(PLATFORMS):
        fail(f"--platforms must list each of {', '.join(PLATFORMS)} at most once, and at least one")
    if args.published and args.dev:
        fail("--published and --dev exclude each other")
    problems = check_packages(args.directory, version, platforms, args.dev, args.published)
    if problems:
        fail(f"{args.directory}:\n  " + "\n  ".join(problems))
    print(f"{args.directory} holds {MAIN_PACKAGE} {version} with the platform packages of {' '.join(platforms)}")


if __name__ == "__main__":
    main()
