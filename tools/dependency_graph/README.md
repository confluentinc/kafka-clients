# Dependency Graph Generator

Generates a topologically sorted dependency graph for a Java class by parsing the AST of all Java source files under a given directory. Produces a Graphviz visualization and a sorted class list.

Uses fully qualified class names (FQCN) internally for accurate cross-package resolution, but displays simple names in the graph for readability.

## Prerequisites

- Python 3.8+
- Graphviz system package

```bash
# Ubuntu/Debian
sudo apt install graphviz

# macOS
brew install graphviz
```

## Setup

```bash
cd tools/dependency_graph
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Run from the project root:

```bash
python3 tools/dependency_graph/dependency_graph.py \
  --kafka-dir kafka/ \
  --root-class KafkaProducer \
  --mark-file marked_classes.txt \
  --output-format png \
  --output dependency_graph
```

### Options

| Flag              | Default              | Description                                              |
|-------------------|----------------------|----------------------------------------------------------|
| `--kafka-dir`     | `kafka/`             | Root directory of Kafka Java source files                |
| `--root-class`    | `KafkaProducer`      | Root class to start the dependency analysis from         |
| `--mark-file`     | (none)               | Text file with FQCNs of completed classes (one per line) |
| `--output-format` | `png`                | Output format: `png`, `svg`, or `pdf`                    |
| `--output`        | `dependency_graph`   | Output filename (without extension)                      |

### Mark file format

The `--mark-file` accepts one fully qualified class name per line:

```
org.apache.kafka.common.Uuid
org.apache.kafka.common.message.ProduceRequestData
```

Marked classes appear green in the graph. Unmarked classes are written to `remaining_classes.txt`.

## Output

- **Graph image** (`dependency_graph.png/svg/pdf`) — LR tree layout with the root class on the left. Green nodes are marked classes, light yellow are remaining, light blue is the root.
- **Topological class list** printed to stdout, with cycle information.
- **`remaining_classes.txt`** — FQCNs of unmarked classes in topological order.
- **Completion percentage** printed after generation.
