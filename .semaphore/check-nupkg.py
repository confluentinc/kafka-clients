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
# Checks the Confluent.Kafka NuGet package, and the native libraries it carries, before anything
# is recorded or published. Used by .semaphore/pack-nupkg.sh, .semaphore/build-dotnet-native.sh,
# .semaphore/publish-nupkg.sh and create-release-tag.yml. Standard library only.
#
# Usage, from the repository root:
#   check-nupkg.py version
#       Prints <Version> from dotnet/Directory.Build.props, after checking that the file has
#       exactly one and that it is a release version (see release_version_problem).
#   check-nupkg.py native <rid> <file> [--dev]
#       Checks that <file> is the native library of <rid>: its file name, its binary format and
#       architecture, the system libraries it loads, and its platform floor (glibc 2.28 on Linux,
#       macOS 11.0 on macOS).
#   check-nupkg.py package <nupkg> --version <v> --rids "<rid> ..." [--commit <sha>] [--dev]
#                  [--published]
#       Checks the package: its file name, the exact list of its entries and how they are stored,
#       its nuspec, its assemblies, and each native library as `native` does.
#       --published: the package as the gallery serves it, which adds its repository signature.
#
# --dev is for a package built from a developer's or a CI job's own native (make
# package-check-dotnet): the platform floors are not checked, since that native was not built for
# them. A release package is never checked with --dev.
#
# The checks read the files as NuGet and the dynamic loaders do, and refuse what those would read
# differently: a zip entry name with a NUL, a nuspec element or attribute NuGet would act on that
# is not expected here, an ELF version requirement or a Mach-O load command it cannot vouch for.

import argparse
import os
import re
import struct
import sys
import xml.etree.ElementTree as ElementTree
import zipfile

PACKAGE_ID = "Confluent.Kafka"
PROPS = "dotnet/Directory.Build.props"

# The release versions: X.Y.Z, optionally followed by -alpha.N, -beta.N, -dev.N or -rc.N. The
# dotnet/v<version> tag pattern of semaphore.yml is this pattern after its prefix.
VERSION_PATTERN = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|dev|rc)\.(0|[1-9][0-9]*))?"
# The package id is shared with confluent-kafka-dotnet, whose versions are 2.x: this client's
# versions start at 4, the Apache Kafka line, so that none can be taken for one of those.
MIN_MAJOR = 4

# The target frameworks of dotnet/src/Confluent.Kafka/Confluent.Kafka.csproj, as lib/ folders,
# as the nuspec's dependency groups name them, with the dependencies of each, and as the
# assembly's TargetFrameworkAttribute names them.
TARGET_FRAMEWORKS = {
    "netstandard2.0": (".NETStandard2.0", {"System.Memory": "4.5.5", "Microsoft.Bcl.AsyncInterfaces": "8.0.0"},
                       ".NETStandard,Version=v2.0"),
    "net8.0": ("net8.0", {}, ".NETCoreApp,Version=v8.0"),
    "net10.0": ("net10.0", {}, ".NETCoreApp,Version=v10.0"),
}
# The exclude attribute `dotnet pack` gives a package's dependencies.
DEPENDENCY_EXCLUDE = "Build,Analyzers"

# The native library of each RID the package can carry.
NATIVE_FILES = {
    "linux-x64": "libconfluent_kafka.so",
    "linux-arm64": "libconfluent_kafka.so",
    "osx-x64": "libconfluent_kafka.dylib",
    "osx-arm64": "libconfluent_kafka.dylib",
}

GLIBC_FLOOR = (2, 28)
MACOS_FLOOR = (11, 0, 0)
# The libraries a Linux native may load, all of them part of glibc or GCC's runtime on every
# supported distribution, and the version nodes each may require. libgcc_s.so.1 is GCC's, whose
# version nodes are GCC_x.y[.z]: RHEL 8's GCC 8 provides up to GCC_7.0.0.
GLIBC_LIBRARIES = {"libc.so.6", "libm.so.6", "libdl.so.2", "libpthread.so.0", "librt.so.1",
                   "ld-linux-x86-64.so.2", "ld-linux-aarch64.so.1"}
LIBGCC_FLOOR = (7, 0, 0)
# The directories of the libraries a macOS native may load: the system's.
MACOS_SYSTEM_PREFIXES = ("/usr/lib/", "/System/Library/")
REPOSITORY_URL = "https://github.com/confluentinc/kafka-clients"
# nuget.org refuses a package larger than 250 MB.
MAX_PACKAGE_BYTES = 250 * 1000 * 1000
NUSPEC_XMLNS = "http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd"
NUSPEC_NS = "{" + NUSPEC_XMLNS + "}"
# The elements of the nuspec's <metadata> that `dotnet pack` writes for this project, each once.
NUSPEC_METADATA = {"id", "version", "title", "authors", "license", "licenseUrl", "icon", "readme", "projectUrl",
                   "description", "copyright", "tags", "repository", "dependencies"}


def fail(message):
    sys.exit(f"check-nupkg: {message}")


def release_version_problem(version):
    """Why `version` is not a release version of the package, or None."""
    if not re.fullmatch(VERSION_PATTERN, version):
        return f"'{version}' is not X.Y.Z or X.Y.Z-(alpha|beta|dev|rc).N"
    if int(version.split(".")[0]) < MIN_MAJOR:
        return (f"'{version}' is below {MIN_MAJOR}.0.0; the versions of {PACKAGE_ID} below {MIN_MAJOR} are "
                "confluent-kafka-dotnet's")
    return None


def props_version():
    try:
        root = ElementTree.parse(PROPS).getroot()
    except (OSError, ElementTree.ParseError) as error:
        fail(f"could not read {PROPS}: {error}")

    # MSBuild reads element names without regard to case, and with or without its namespace.
    def local_name(element):
        return element.tag.rsplit("}", 1)[-1].lower() if isinstance(element.tag, str) else ""

    def conditional(element):
        return any(name.lower() == "condition" for name in element.attrib)

    parents = {child: parent for parent in root.iter() for child in parent}
    versions = [e for e in root.iter() if local_name(e) == "version"]
    overrides = sorted({e.tag for e in root.iter() if local_name(e) in ("versionprefix", "versionsuffix", "packageversion")})
    if overrides:
        fail(f"{PROPS} sets {', '.join(overrides)}, which would change the package version that <Version> sets")
    if len(versions) != 1:
        fail(f"{PROPS} must hold exactly one <Version>, in a top-level <PropertyGroup>")
    element = versions[0]
    group = parents.get(element)
    if group is None or local_name(group) != "propertygroup" or parents.get(group) is not root:
        fail(f"{PROPS} must hold exactly one <Version>, in a top-level <PropertyGroup>")
    if conditional(element) or conditional(group):
        fail(f"the <Version> of {PROPS} must not be conditional")
    version = (element.text or "").strip()
    problem = release_version_problem(version)
    if problem:
        fail(f"<Version> in {PROPS}: {problem}")
    return version


def elf_dependencies(data):
    """Returns (DT_NEEDED libraries, {library: [version names]}) of the ELF file, read from its
    dynamic segment, as the dynamic loader reads them, rather than from section headers, which a
    stripped file need not have."""
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
    if 5 not in tags:  # DT_STRTAB
        raise ValueError("its dynamic section has no string table")
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
    return needed, versions


def check_elf(rid, name, data, dev):
    problems = []
    if data[7] not in (0, 3):  # ELFOSABI_NONE (System V), ELFOSABI_GNU
        problems.append(f"{rid}: {name} is for ELF OS ABI {data[7]}, not Linux")
    try:
        needed, versions = elf_dependencies(data)
    except (ValueError, struct.error, IndexError) as error:
        return problems + [f"{rid}: the dependencies of {name} cannot be read: {error}"]
    for library in needed:
        if library not in GLIBC_LIBRARIES and library != "libgcc_s.so.1":
            problems.append(f"{rid}: {name} loads {library}, which is not part of glibc or GCC's runtime, so not on "
                            "every supported distribution")
    if "libc.so.6" not in needed:
        problems.append(f"{rid}: {name} does not load libc.so.6")
    glibc = []
    for library, names in versions.items():
        for label in names:
            if library in GLIBC_LIBRARIES:
                match = re.fullmatch(r"GLIBC_(\d+)\.(\d+)(?:\.\d+)?", label)
                if not match:
                    # GLIBC_PRIVATE, GLIBC_ABI_DT_RELR (glibc 2.36), ...: no floor can be read.
                    problems.append(f"{rid}: {name} requires {label} of {library}, which is not a glibc release")
                    continue
                glibc.append((int(match.group(1)), int(match.group(2))))
                if not dev and glibc[-1] > GLIBC_FLOOR:
                    problems.append(
                        f"{rid}: {name} requires glibc {label[6:]}, newer than the {GLIBC_FLOOR[0]}.{GLIBC_FLOOR[1]} the "
                        "package supports; build it in the manylinux_2_28 image")
            elif library == "libgcc_s.so.1":
                match = re.fullmatch(r"GCC_(\d+)\.(\d+)(?:\.(\d+))?", label)
                if not match:
                    problems.append(f"{rid}: {name} requires {label} of {library}, which is not a GCC release")
                elif not dev and (int(match.group(1)), int(match.group(2)), int(match.group(3) or 0)) > LIBGCC_FLOOR:
                    problems.append(f"{rid}: {name} requires {label} of {library}, newer than the GCC runtime of RHEL 8")
            else:
                problems.append(f"{rid}: {name} requires {label} of {library}, a library it may not load")
    if not glibc and not dev:
        problems.append(f"{rid}: {name} requires no glibc version of libc.so.6, so its glibc floor cannot be read")
    return problems


def check_macho(rid, name, data, dev):
    cputype, subtype = {"osx-x64": (0x01000007, 3), "osx-arm64": (0x0100000C, 0)}[rid]  # *_ALL subtypes
    file_cputype, file_subtype, filetype, ncmds, sizeofcmds = struct.unpack_from("<IIIII", data, 4)
    problems = []
    if file_cputype != cputype:
        problems.append(f"{rid}: {name} is for Mach-O CPU type {file_cputype:#x}, not {cputype:#x}")
    elif file_subtype & 0x00FFFFFF != subtype:  # without the capability bits; arm64e is 2
        problems.append(f"{rid}: {name} is for Mach-O CPU subtype {file_subtype:#x}, not {subtype:#x}")
    if filetype != 6:  # MH_DYLIB
        problems.append(f"{rid}: {name} is Mach-O file type {filetype}, not a dynamic library")
    if problems:
        return problems
    if 32 + sizeofcmds > len(data):
        return [f"{rid}: the load commands of {name} extend beyond the end of the file"]

    def string_at(pos, cmdsize):
        (offset,) = struct.unpack_from("<I", data, pos + 8)
        end = data.find(b"\0", pos + offset, pos + cmdsize)
        return data[pos + offset:end if end >= 0 else pos + cmdsize].decode("utf-8", "replace")

    def version(value):
        return (value >> 16, (value >> 8) & 0xFF, value & 0xFF)

    minimums = []
    install_name = None
    signed = False
    pos = 32
    for _ in range(ncmds):
        cmd, cmdsize = struct.unpack_from("<II", data, pos)
        if cmdsize < 8 or pos + cmdsize > 32 + sizeofcmds:
            return problems + [f"{rid}: {name} has a malformed load command"]
        if cmd == 0x32:  # LC_BUILD_VERSION
            platform, minos = struct.unpack_from("<II", data, pos + 8)
            if platform != 1:  # PLATFORM_MACOS
                problems.append(f"{rid}: {name} is built for Mach-O platform {platform}, not macOS")
            minimums.append(version(minos))
        elif cmd == 0x24:  # LC_VERSION_MIN_MACOSX
            minimums.append(version(struct.unpack_from("<I", data, pos + 8)[0]))
        elif cmd == 0x0D:  # LC_ID_DYLIB
            install_name = string_at(pos, cmdsize)
        elif cmd in (0x0C, 0x20, 0x80000018, 0x8000001F, 0x80000023):
            # LC_LOAD_DYLIB, LC_LAZY_LOAD_DYLIB, LC_LOAD_WEAK_DYLIB, LC_REEXPORT_DYLIB, LC_LOAD_UPWARD_DYLIB
            path = string_at(pos, cmdsize)
            if not path.startswith(MACOS_SYSTEM_PREFIXES):
                problems.append(f"{rid}: {name} loads {path}, which is not a library of macOS itself")
        elif cmd == 0x19:  # LC_SEGMENT_64
            fileoff, filesize = struct.unpack_from("<QQ", data, pos + 40)
            if fileoff + filesize > len(data):
                problems.append(f"{rid}: a segment of {name} extends beyond the end of the file")
        elif cmd == 0x1D:  # LC_CODE_SIGNATURE
            dataoff, datasize = struct.unpack_from("<II", data, pos + 8)
            signed = dataoff + datasize <= len(data)
        pos += cmdsize
    if install_name != f"@rpath/{name}":
        problems.append(f"{rid}: {name} has the install name {install_name!r}, not @rpath/{name}")
    if rid == "osx-arm64" and not signed:
        # macOS on Apple silicon loads only signed code; an ad hoc signature is enough.
        problems.append(f"{rid}: {name} has no code signature, which macOS on Apple silicon requires")
    if not minimums:
        problems.append(f"{rid}: {name} names no minimum macOS version (LC_BUILD_VERSION or LC_VERSION_MIN_MACOSX)")
    elif not dev and max(minimums) > MACOS_FLOOR:
        problems.append(
            f"{rid}: {name} requires macOS {'.'.join(map(str, max(minimums)))}, newer than the "
            f"{'.'.join(map(str, MACOS_FLOOR))} the package supports; build it with MACOSX_DEPLOYMENT_TARGET=11.0")
    return problems


def check_native(rid, name, data, dev):
    """Returns the problems of the native library `name` of `rid`, whose bytes are `data`."""
    if rid not in NATIVE_FILES:
        return [f"{rid} is not a RID the package carries ({', '.join(NATIVE_FILES)})"]
    if name != NATIVE_FILES[rid]:
        return [f"{rid}: the library is named {name}, not {NATIVE_FILES[rid]}"]
    if len(data) < 1024 * 1024:
        return [f"{rid}: {name} is only {len(data)} bytes"]
    try:
        if rid.startswith("linux-"):
            machine = {"linux-x64": 62, "linux-arm64": 183}[rid]  # EM_X86_64, EM_AARCH64
            if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
                return [f"{rid}: {name} is not a 64-bit little-endian ELF file"]
            e_type, e_machine = struct.unpack_from("<HH", data, 16)
            if e_type != 3:
                return [f"{rid}: {name} is ELF type {e_type}, not a shared object"]
            if e_machine != machine:
                return [f"{rid}: {name} is for ELF machine {e_machine}, not {machine}"]
            return check_elf(rid, name, data, dev)
        if data[:4] != b"\xcf\xfa\xed\xfe":
            return [f"{rid}: {name} is not a thin 64-bit little-endian Mach-O file"]
        return check_macho(rid, name, data, dev)
    except struct.error as error:
        return [f"{rid}: {name} is truncated: {error}"]


def check_nuspec(text, version, commit):
    # NuGet refuses a DTD, and reads the nuspec namespace only as the default namespace.
    if re.search(rb"<!(DOCTYPE|ENTITY)", text, re.IGNORECASE):
        return ["the nuspec has a DTD"]
    if f'<package xmlns="{NUSPEC_XMLNS}">'.encode() not in text:
        return [f'the nuspec has no <package xmlns="{NUSPEC_XMLNS}">']
    problems = []
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError as error:
        return [f"the nuspec is not XML: {error}"]
    metadata = root.find(f"{NUSPEC_NS}metadata")
    if root.tag != f"{NUSPEC_NS}package" or metadata is None:
        return ["the nuspec has no <package><metadata> in the 2013/05 nuspec namespace"]
    if list(root) != [metadata] or root.attrib or metadata.attrib:
        # minClientVersion, a <files> list, ...
        problems.append("the nuspec's <package> or <metadata> has an element or attribute dotnet pack does not write")
    tags = [child.tag[len(NUSPEC_NS):] if child.tag.startswith(NUSPEC_NS) else child.tag for child in metadata]
    if sorted(tags) != sorted(NUSPEC_METADATA):
        # packageTypes, developmentDependency, frameworkReferences, references, a second <dependencies>, ...
        problems.append(f"the nuspec's <metadata> holds {sorted(tags)}, not each of {sorted(NUSPEC_METADATA)} once")

    def text_of(tag):
        element = metadata.find(f"{NUSPEC_NS}{tag}")
        return None if element is None else (element.text or "").strip()

    expected = {
        "id": PACKAGE_ID,
        "version": version,
        "authors": "Confluent Inc.",
        "readme": "README.md",
        "icon": "icon.png",
        "projectUrl": REPOSITORY_URL,
        "licenseUrl": "https://licenses.nuget.org/Apache-2.0",
    }
    for tag, value in expected.items():
        if text_of(tag) != value:
            problems.append(f"nuspec <{tag}> is {text_of(tag)!r}, not {value!r}")
    license_element = metadata.find(f"{NUSPEC_NS}license")
    if license_element is None or license_element.attrib != {"type": "expression"} or (license_element.text or "").strip() != "Apache-2.0":
        problems.append("nuspec <license> is not the expression Apache-2.0")
    repository = metadata.find(f"{NUSPEC_NS}repository")
    if repository is None or repository.get("type") != "git" or repository.get("url") != REPOSITORY_URL \
            or not set(repository.attrib) <= {"type", "url", "branch", "commit"}:
        problems.append(f"nuspec <repository> is not the git repository {REPOSITORY_URL}")
    elif commit and repository.get("commit") != commit:
        problems.append(f"nuspec <repository> names commit {repository.get('commit')!r}, not {commit}, which was packed")

    groups = {}
    dependencies = metadata.find(f"{NUSPEC_NS}dependencies")
    for group in [] if dependencies is None else list(dependencies):
        if group.tag != f"{NUSPEC_NS}group" or set(group.attrib) != {"targetFramework"}:
            problems.append("nuspec <dependencies> holds something other than a <group targetFramework>")
            continue
        framework = group.get("targetFramework")
        if framework in groups:
            problems.append(f"nuspec has two dependency groups for {framework}")
        groups[framework] = {}
        for dependency in group:
            if dependency.tag != f"{NUSPEC_NS}dependency" or set(dependency.attrib) != {"id", "version", "exclude"} \
                    or dependency.get("exclude") != DEPENDENCY_EXCLUDE:
                problems.append(f"nuspec group {framework} has a dependency other than <dependency id version "
                                f'exclude="{DEPENDENCY_EXCLUDE}">')
            elif dependency.get("id") in groups[framework]:
                problems.append(f"nuspec group {framework} lists {dependency.get('id')} twice")
            else:
                groups[framework][dependency.get("id")] = dependency.get("version")
    expected_groups = {framework: deps for framework, deps, _name in TARGET_FRAMEWORKS.values()}
    if groups != expected_groups:
        problems.append(f"nuspec dependency groups are {groups}, not {expected_groups}")
    return problems


def check_package(path, version, rids, commit, dev, published):
    problems = []
    name = os.path.basename(path)
    if name != f"{PACKAGE_ID}.{version}.nupkg":
        problems.append(f"the package file is {name}, not {PACKAGE_ID}.{version}.nupkg")
    try:
        archive = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile) as error:
        fail(f"{path} is not a zip file: {error}")
    with archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        if os.path.getsize(path) > MAX_PACKAGE_BYTES:
            problems.append(f"the package exceeds nuget.org's size limit of {MAX_PACKAGE_BYTES} bytes")
        for info in infos:
            # Python cuts a name at a NUL; NuGet does not.
            if info.orig_filename != info.filename:
                problems.append(f"the package entry {info.orig_filename!r} has a NUL in its name")
            if info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED) or info.flag_bits & 0x1:
                problems.append(f"the package entry {info.filename!r} is encrypted or compressed in a way NuGet cannot read")
        if len(set(names)) != len(names):
            problems.append("the package has two entries with the same name")
        elif len(set(n.lower() for n in names)) != len(names):
            problems.append("the package has two entries whose names differ only in case")
        for entry in names:
            parts = entry.split("/")
            if entry.startswith("/") or "\\" in entry or ".." in parts or ":" in entry or "" in parts:
                problems.append(f"the package entry {entry!r} is not a safe relative path")

        expected = {"_rels/.rels", "[Content_Types].xml", f"{PACKAGE_ID}.nuspec", "README.md", "icon.png"}
        for folder in TARGET_FRAMEWORKS:
            expected |= {f"lib/{folder}/{PACKAGE_ID}.dll", f"lib/{folder}/{PACKAGE_ID}.xml"}
        for rid in rids:
            if rid not in NATIVE_FILES:
                fail(f"--rids names {rid}, which is not one of {', '.join(NATIVE_FILES)}")
            expected.add(f"runtimes/{rid}/native/{NATIVE_FILES[rid]}")
        if published:
            expected.add(".signature.p7s")
        core_properties = [n for n in names if re.fullmatch(r"package/services/metadata/core-properties/[^/]+\.psmdcp", n)]
        if len(core_properties) != 1:
            problems.append(f"the package has {len(core_properties)} core-properties entries, not 1")
        actual = set(names) - set(core_properties)
        if actual - expected:
            problems.append(f"the package has entries it must not have: {sorted(actual - expected)}")
        if expected - actual:
            problems.append(f"the package lacks entries: {sorted(expected - actual)}")
        if problems:
            return problems
        corrupt = archive.testzip()
        if corrupt is not None:
            return [f"the package entry {corrupt} does not match its checksum"]

        problems += check_nuspec(archive.read(f"{PACKAGE_ID}.nuspec"), version, commit)
        if archive.read("icon.png")[:8] != b"\x89PNG\r\n\x1a\n":
            problems.append("icon.png is not a PNG image")
        if not archive.read("README.md").strip():
            problems.append("README.md is empty")
        # The SDK writes the version, and the commit when SourceLink knows it, into each assembly's
        # AssemblyInformationalVersion attribute, as UTF-8.
        # Each assembly also names its own target framework in its TargetFrameworkAttribute, and
        # no other; a reference assembly carries ReferenceAssemblyAttribute.
        stamp = f"{version}+{commit}" if commit else version
        for folder, (_group, _deps, framework_name) in TARGET_FRAMEWORKS.items():
            dll = archive.read(f"lib/{folder}/{PACKAGE_ID}.dll")
            others = [name for _g, _d, name in TARGET_FRAMEWORKS.values() if name != framework_name]
            if dll[:2] != b"MZ":
                problems.append(f"lib/{folder}/{PACKAGE_ID}.dll is not a PE file")
            elif stamp.encode() not in dll:
                problems.append(f"lib/{folder}/{PACKAGE_ID}.dll does not carry the version {stamp}")
            elif framework_name.encode() not in dll or any(name.encode() in dll for name in others):
                problems.append(f"lib/{folder}/{PACKAGE_ID}.dll is not the assembly built for {framework_name}")
            elif b"ReferenceAssemblyAttribute" in dll:
                problems.append(f"lib/{folder}/{PACKAGE_ID}.dll is a reference assembly")
            try:
                docs = ElementTree.fromstring(archive.read(f"lib/{folder}/{PACKAGE_ID}.xml"))
            except ElementTree.ParseError:
                docs = None
            if docs is None or docs.tag != "doc" or docs.findtext("assembly/name") != PACKAGE_ID \
                    or not docs.findall("members/member"):
                problems.append(f"lib/{folder}/{PACKAGE_ID}.xml is not the XML documentation of {PACKAGE_ID}")
        for rid in rids:
            problems += check_native(rid, NATIVE_FILES[rid], archive.read(f"runtimes/{rid}/native/{NATIVE_FILES[rid]}"), dev)
    return problems


def main():
    parser = argparse.ArgumentParser(description="Checks the Confluent.Kafka NuGet package.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("version")
    native = commands.add_parser("native")
    native.add_argument("rid")
    native.add_argument("file")
    native.add_argument("--dev", action="store_true")
    package = commands.add_parser("package")
    package.add_argument("nupkg")
    package.add_argument("--version", required=True)
    package.add_argument("--rids", required=True)
    package.add_argument("--commit")
    package.add_argument("--dev", action="store_true")
    package.add_argument("--published", action="store_true")
    args = parser.parse_args()

    if args.command == "version":
        print(props_version())
        return
    if args.command == "native":
        with open(args.file, "rb") as f:
            data = f.read()
        problems = check_native(args.rid, os.path.basename(args.file), data, args.dev)
        if problems:
            fail("\n".join(problems))
        print(f"{args.file} is the {args.rid} native library")
        return
    problem = release_version_problem(args.version)
    if problem:
        fail(f"--version: {problem}")
    if args.commit and not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        fail(f"--commit {args.commit} is not a full commit SHA")
    rids = args.rids.split()
    if not rids or len(set(rids)) != len(rids):
        fail("--rids must list each RID once")
    problems = check_package(args.nupkg, args.version, rids, args.commit, args.dev, args.published)
    if problems:
        fail(f"{args.nupkg}:\n  " + "\n  ".join(problems))
    print(f"{args.nupkg} is {PACKAGE_ID} {args.version} with the natives of {' '.join(rids)}")


if __name__ == "__main__":
    main()
