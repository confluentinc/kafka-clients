# Python Bindings

Python bindings for the Confluent Kafka Rust client, backed by a CPython C
extension (`_confluentkafka`) that links against the Rust `cdylib`
(`libconfluent_kafka`).

There is currently no prebuilt wheel to install — see [No prebuilt wheel](#no-prebuilt-wheel)
below. Build from source following the steps for your platform.

## macOS setup (from a fresh machine)

### 1. Prerequisites

```bash
# Xcode command line tools (gives you clang, needed to compile the C extension)
xcode-select --install

# Homebrew, if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 2. Install Rust

The toolchain version and components are pinned in [`rust-toolchain.toml`](../../rust-toolchain.toml)
at the repo root; `rustup` picks it up automatically.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

### 3. Install Python 3.10+

Required by [`pyproject.toml`](pyproject.toml). macOS's system Python (from
Xcode) is often older than 3.10, so install a newer one via Homebrew:

```bash
brew install python@3.12
```

### 4. Clone and init submodules

```bash
git clone --recurse-submodules <repo-url> confluent-kafka-rust
cd confluent-kafka-rust
# or, if already cloned without --recurse-submodules:
git submodule update --init --recursive
```

### 5. One-shot setup + build

Run from the **repo root**:

```bash
make init          # creates ./venv, installs bindings/python[dev] deps into it
make build-python   # cargo build --all-features --release, then builds/installs the C extension
```

`build-python` builds `libconfluent_kafka.dylib`/`.a` under `target/release/`,
generates `target/include/confluent_kafka.h` via `cbindgen`, then compiles
`_confluentkafka.c` against it and `pip install -e .`s it into `./venv`. No
`cmake` or Docker is needed for this path (those are only for the C bindings /
gRPC multilanguage tests).

### 6. Verify the install

```bash
source venv/bin/activate
cd bindings/python
python -c "import producer, consumer; print('ok')"
```

Or run the unit test suite (from repo root):

```bash
make test-python
```

### 7. (Optional) A broker to point it at

The client needs a real Kafka broker — this repo doesn't ship a
`docker-compose.yml`. Easiest with Docker Desktop:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:4.2.0
```

```python
from producer import KafkaProducer
p = KafkaProducer({"bootstrap.servers": "localhost:9092"})
```

**Apple Silicon note:** the root `Makefile`'s `-march=x86-64-v3` compiler flag
only applies on `x86_64`; on `arm64` it falls back to generic tuning
automatically, so this works out of the box on M-series Macs.

## No prebuilt wheel

There is no wheel checked into or produced by this repo today — `make
build-python` only ever does an editable install (`pip install -e .`), never
`pip wheel` / `python -m build`.

Even if one were built, it would not be portable to another machine as-is:
[`setup.py`](setup.py) links `_confluentkafka` against
`libconfluent_kafka.{so,dylib}` via `runtime_library_dirs` pointing at an
**absolute path** under wherever `target/release/` happened to be built. A
wheel built this way embeds that absolute rpath and breaks once copied
elsewhere, because the dylib isn't bundled inside the wheel itself. Producing
a genuinely shareable wheel would need a repair step (`delocate-wheel` on
macOS, `auditwheel repair` on Linux) to bundle the shared library and rewrite
the load path to something relative — not currently wired into the build.
