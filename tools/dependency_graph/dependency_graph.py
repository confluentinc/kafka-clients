#!/usr/bin/env python3
"""
Generates a topologically sorted dependency graph for a Java class.

Scans Java source files under a given directory, builds a dependency graph
using javalang AST parsing, and produces a Graphviz visualization plus a
topologically sorted class list.

Internally uses fully qualified class names (FQCN) for accurate resolution.
The graph displays simple class names for readability.
"""

import argparse
import json
import os
import re
import sys
import heapq
from collections import defaultdict, deque

import graphviz
import javalang


def extract_package(filepath):
    """Extract the package name from a Java source file."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line.startswith("package "):
                    return line.rstrip(";").split("package ", 1)[1].strip()
                if line.startswith("import ") or line.startswith("public ") or line.startswith("class "):
                    break
    except (OSError, IOError):
        pass
    return ""


def find_java_files(kafka_dir):
    """Find all .java files under kafka_dir.

    Returns:
        fqcn_files: dict of fqcn -> file_path
        simple_to_fqcns: dict of simple_name -> set of fqcns
    """
    fqcn_files = {}
    simple_to_fqcns = defaultdict(set)
    for root, _dirs, files in os.walk(kafka_dir):
        for f in files:
            if f.endswith(".java"):
                simple_name = f[:-5]
                filepath = os.path.join(root, f)
                pkg = extract_package(filepath)
                fqcn = f"{pkg}.{simple_name}" if pkg else simple_name
                if fqcn not in fqcn_files:
                    fqcn_files[fqcn] = filepath
                    simple_to_fqcns[simple_name].add(fqcn)
    return fqcn_files, simple_to_fqcns


def extract_type_names(node):
    """Recursively extract all type name strings from a javalang AST node."""
    types = set()
    if node is None:
        return types

    if isinstance(node, javalang.tree.ReferenceType):
        types.add(node.name)
        if node.arguments:
            for arg in node.arguments:
                types.update(extract_type_names(arg))
        if node.sub_type:
            types.update(extract_type_names(node.sub_type))
        return types

    if isinstance(node, javalang.tree.TypeArgument):
        if node.type:
            types.update(extract_type_names(node.type))
        return types

    # MethodInvocation — capture static call targets like ClientUtils.method()
    if isinstance(node, javalang.tree.MethodInvocation):
        if node.qualifier and isinstance(node.qualifier, str) and node.qualifier[0].isupper():
            types.add(node.qualifier)

    # MemberReference — captures things like ClassName.CONSTANT
    if isinstance(node, javalang.tree.MemberReference):
        if node.qualifier and isinstance(node.qualifier, str) and node.qualifier[0].isupper():
            types.add(node.qualifier)

    if isinstance(node, javalang.tree.Node):
        for child in node.children:
            if isinstance(child, javalang.tree.Node):
                types.update(extract_type_names(child))
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, javalang.tree.Node):
                        types.update(extract_type_names(item))

    return types


def parse_dependencies(filepath, fqcn_files, simple_to_fqcns):
    """Parse a Java file and return set of FQCNs it depends on.

    Resolution rules:
    1. Explicit imports map simple names to FQCNs.
    2. Same-package classes are resolved by package.
    3. Only types found in the AST (not just imported) count.
    """
    deps = set()
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except (OSError, IOError):
        return deps

    # Get this file's package
    file_pkg = ""
    pkg_match = re.search(r"^package\s+([\w.]+)\s*;", content, re.MULTILINE)
    if pkg_match:
        file_pkg = pkg_match.group(1)

    try:
        tree = javalang.parse.parse(content)
    except Exception:
        return _parse_imports_fallback(content, fqcn_files, simple_to_fqcns, file_pkg)

    # Build import map: simple_name -> fqcn
    import_map = {}
    for imp in tree.imports:
        parts = imp.path.split(".")
        simple = parts[-1]
        if simple == "*":
            continue
        fqcn = imp.path
        if fqcn in fqcn_files:
            import_map[simple] = fqcn

    # Walk AST to find all type references (excludes Javadoc/comments)
    ast_types = extract_type_names(tree)

    # Resolve each AST type to a FQCN
    for simple_name in ast_types:
        # 1. Explicit import
        if simple_name in import_map:
            deps.add(import_map[simple_name])
            continue
        # 2. Same package
        if file_pkg:
            same_pkg_fqcn = f"{file_pkg}.{simple_name}"
            if same_pkg_fqcn in fqcn_files:
                deps.add(same_pkg_fqcn)

    return deps


def _parse_imports_fallback(content, fqcn_files, simple_to_fqcns, file_pkg):
    """Regex fallback for files that javalang can't parse."""
    deps = set()
    for match in re.finditer(r"^import\s+(?:static\s+)?([\w.]+\.(\w+))\s*;", content, re.MULTILINE):
        fqcn = match.group(1)
        if fqcn in fqcn_files:
            deps.add(fqcn)
    return deps


def build_dependency_graph(root_fqcn, fqcn_files, simple_to_fqcns):
    """Build dependency graph starting from root_fqcn via BFS."""
    graph = defaultdict(set)  # fqcn -> set of fqcns it depends on
    visited = set()
    queue = deque([root_fqcn])

    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        visited.add(current)

        if current not in fqcn_files:
            continue

        deps = parse_dependencies(fqcn_files[current], fqcn_files, simple_to_fqcns)
        deps.discard(current)  # remove self-dependency
        graph[current] = deps

        for dep in deps:
            if dep not in visited:
                queue.append(dep)

    return graph, visited


def _tarjan_sccs(graph, all_nodes):
    """Find strongly connected components using iterative Tarjan's algorithm."""
    index_counter = [0]
    stack = []
    lowlink = {}
    index = {}
    on_stack = {}
    sccs = []

    def strongconnect(root):
        # Iterative DFS to avoid recursion limit
        work = [(root, iter(sorted(graph.get(root, set()) & all_nodes)))]
        index[root] = lowlink[root] = index_counter[0]
        index_counter[0] += 1
        stack.append(root)
        on_stack[root] = True

        while work:
            v, it = work[-1]
            pushed = False
            for w in it:
                if w not in index:
                    index[w] = lowlink[w] = index_counter[0]
                    index_counter[0] += 1
                    stack.append(w)
                    on_stack[w] = True
                    work.append((w, iter(sorted(graph.get(w, set()) & all_nodes))))
                    pushed = True
                    break
                elif on_stack.get(w, False):
                    lowlink[v] = min(lowlink[v], index[w])
            if not pushed:
                if lowlink[v] == index[v]:
                    scc = []
                    while True:
                        w = stack.pop()
                        on_stack[w] = False
                        scc.append(w)
                        if w == v:
                            break
                    sccs.append(scc)
                work.pop()
                if work:
                    parent = work[-1][0]
                    lowlink[parent] = min(lowlink[parent], lowlink[v])

    for v in sorted(all_nodes):
        if v not in index:
            strongconnect(v)

    return sccs


def topological_sort(graph, all_nodes):
    """SCC-based topological sort. Collapses cycles, sorts SCCs, then expands."""
    sccs = _tarjan_sccs(graph, all_nodes)

    # Map each node to its SCC id
    node_to_scc = {}
    for i, scc in enumerate(sccs):
        for node in scc:
            node_to_scc[node] = i

    # Build DAG of SCCs
    scc_graph = defaultdict(set)
    for node in all_nodes:
        for dep in graph.get(node, set()):
            if dep in all_nodes and node_to_scc[node] != node_to_scc[dep]:
                scc_graph[node_to_scc[node]].add(node_to_scc[dep])

    # Topological sort of SCCs (Kahn's on the DAG — guaranteed no cycles)
    scc_in_degree = {i: 0 for i in range(len(sccs))}
    for scc_id, deps in scc_graph.items():
        for dep_id in deps:
            scc_in_degree[scc_id] += 1

    queue = sorted([i for i in range(len(sccs)) if scc_in_degree[i] == 0])
    heapq.heapify(queue)
    scc_order = []
    while queue:
        scc_id = heapq.heappop(queue)
        scc_order.append(scc_id)
        for other_id in range(len(sccs)):
            if scc_id in scc_graph.get(other_id, set()):
                scc_in_degree[other_id] -= 1
                if scc_in_degree[other_id] == 0:
                    heapq.heappush(queue, other_id)

    # Expand: within each SCC, sort alphabetically
    result = []
    cycle_count = 0
    for scc_id in scc_order:
        scc = sccs[scc_id]
        if len(scc) > 1:
            cycle_count += 1
        result.extend(sorted(scc))

    multi_node_sccs = [s for s in sccs if len(s) > 1]
    if multi_node_sccs:
        total_cyclic = sum(len(s) for s in multi_node_sccs)
        print(f"Note: {len(multi_node_sccs)} dependency cycles found ({total_cyclic} classes).",
              file=sys.stderr)

    return result


def simple_name(fqcn):
    """Extract simple class name from FQCN."""
    return fqcn.rsplit(".", 1)[-1]


def read_marked_classes(mark_file, simple_to_fqcns):
    """Read class names from a text file and resolve to FQCNs."""
    if not mark_file or not os.path.exists(mark_file):
        return set()
    marked = set()
    with open(mark_file, "r") as f:
        for line in f:
            name = line.strip()
            if not name or name.startswith("#"):
                continue
            # Try as FQCN first, then as simple name
            if "." in name:
                marked.add(name)
            elif name in simple_to_fqcns:
                marked.update(simple_to_fqcns[name])
            else:
                marked.add(name)
    return marked


def build_tree(graph, root_fqcn, all_nodes):
    """Build a spanning tree from root via BFS."""
    tree_edges = []
    visited = set()
    queue = deque([root_fqcn])
    visited.add(root_fqcn)

    while queue:
        node = queue.popleft()
        for dep in sorted(graph.get(node, set())):
            if dep in all_nodes and dep not in visited:
                visited.add(dep)
                tree_edges.append((node, dep))
                queue.append(dep)

    return tree_edges, visited


def generate_graph(graph, all_nodes, sorted_classes, marked_fqcns, output, output_format, root_fqcn):
    """Generate Graphviz tree graph and render to file."""
    tree_edges, tree_nodes = build_tree(graph, root_fqcn, all_nodes)

    dot = graphviz.Digraph(
        name="KafkaProducer Dependencies",
        format=output_format,
        engine="dot",
        graph_attr={
            "rankdir": "LR",
            "splines": "polyline",
            "fontsize": "10",
            "nodesep": "0.15",
            "ranksep": "0.6",
            "ordering": "out",
        },
        node_attr={
            "shape": "box",
            "style": "filled,rounded",
            "fillcolor": "lightyellow",
            "fontsize": "9",
            "fontname": "Helvetica",
            "height": "0.3",
            "width": "0.1",
        },
        edge_attr={
            "arrowsize": "0.5",
            "color": "gray50",
        },
    )

    # Root node
    dot.node(
        root_fqcn,
        label=simple_name(root_fqcn),
        fillcolor="lightblue",
        color="darkblue",
        penwidth="2",
        fontsize="11",
    )

    for fqcn in tree_nodes:
        if fqcn == root_fqcn:
            continue
        label = simple_name(fqcn)
        if fqcn in marked_fqcns:
            dot.node(
                fqcn,
                label=f"✓ {label}",
                fillcolor="lightgreen",
                color="darkgreen",
                penwidth="2",
                fontcolor="darkgreen",
            )
        else:
            dot.node(fqcn, label=label)

    for parent, child in tree_edges:
        dot.edge(parent, child)

    dot.render(output, cleanup=True)
    marked_in_graph = len(marked_fqcns & tree_nodes)
    total = len(tree_nodes)
    pct = (marked_in_graph / total * 100) if total > 0 else 0
    print(f"Graph rendered to {output}.{output_format}", file=sys.stderr)
    print(f"Tree has {total} nodes and {len(tree_edges)} edges.", file=sys.stderr)
    print(f"Completion: {marked_in_graph}/{total} classes done ({pct:.1f}%)", file=sys.stderr)


def generate_json_tree(graph, root_fqcn, all_nodes, marked_fqcns, output):
    """Generate a JSON tree structure with root_fqcn at the root.

    The JSON structure represents the dependency spanning tree where each node has:
    - fqcn: Fully qualified class name
    - name: Simple class name
    - marked: Whether the class is marked as complete
    - children: Array of dependency nodes (each node appears once in the tree)

    Uses BFS to build a spanning tree, avoiding exponential explosion from
    shared dependencies appearing multiple times.
    """
    # Build spanning tree via BFS (same as the graph visualization)
    tree_structure = {}  # fqcn -> list of child fqcns
    visited = set()
    queue = deque([root_fqcn])
    visited.add(root_fqcn)

    while queue:
        node = queue.popleft()
        tree_structure[node] = []
        for dep in sorted(graph.get(node, set()) & all_nodes):
            if dep not in visited:
                visited.add(dep)
                tree_structure[node].append(dep)
                queue.append(dep)

    def build_node(fqcn):
        """Build a node from the pre-computed spanning tree."""
        return {
            "fqcn": fqcn,
            "name": simple_name(fqcn),
            "marked": fqcn in marked_fqcns,
            "children": [build_node(child) for child in tree_structure.get(fqcn, [])]
        }

    # Build the tree starting from root
    tree = build_node(root_fqcn)

    # Add metadata
    result = {
        "root": root_fqcn,
        "total_classes": len(all_nodes),
        "marked_classes": len(marked_fqcns & all_nodes),
        "tree_nodes": len(visited),
        "tree": tree
    }

    # Write to file
    json_file = f"{output}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"JSON tree written to {json_file}", file=sys.stderr)
    return result


def generate_flat_json(graph, all_nodes, sorted_classes, marked_fqcns, output, root_fqcn):
    """Generate a flat JSON with all classes and their direct dependencies.

    This is useful for tools that need the full dependency information
    without the tree structure deduplication.
    """
    classes = []
    for fqcn in sorted_classes:
        deps = sorted(graph.get(fqcn, set()) & all_nodes)
        classes.append({
            "fqcn": fqcn,
            "name": simple_name(fqcn),
            "marked": fqcn in marked_fqcns,
            "dependencies": deps
        })

    result = {
        "root": root_fqcn,
        "total_classes": len(all_nodes),
        "marked_classes": len(marked_fqcns & all_nodes),
        "completion_percent": round(len(marked_fqcns & all_nodes) / len(all_nodes) * 100, 1) if all_nodes else 0,
        "classes": classes
    }

    json_file = f"{output}_flat.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"Flat JSON written to {json_file}", file=sys.stderr)
    return result


def main():
    parser = argparse.ArgumentParser(description="Generate a dependency graph for a Java class.")
    parser.add_argument("--kafka-dir", default="kafka/", help="Root directory of Kafka Java source")
    parser.add_argument("--root-class", default="KafkaProducer", help="Root class to analyze")
    parser.add_argument("--mark-file", default=None, help="Text file with class names to mark (one per line)")
    parser.add_argument("--output-format", default="png", choices=["png", "svg", "pdf"], help="Output format for graph")
    parser.add_argument("--output", default="dependency_graph", help="Output filename (without extension)")
    parser.add_argument("--json", action="store_true", help="Generate JSON output (tree and flat formats)")
    parser.add_argument("--json-only", action="store_true", help="Generate only JSON output, skip graph generation")

    args = parser.parse_args()

    # Step 1: Find all Java classes
    print(f"Scanning {args.kafka_dir} for Java files...", file=sys.stderr)
    fqcn_files, simple_to_fqcns = find_java_files(args.kafka_dir)
    print(f"Found {len(fqcn_files)} classes.", file=sys.stderr)

    # Resolve root class to FQCN
    root_fqcn = args.root_class
    if "." not in root_fqcn:
        candidates = simple_to_fqcns.get(root_fqcn, set())
        if len(candidates) == 1:
            root_fqcn = next(iter(candidates))
        elif len(candidates) > 1:
            print(f"Multiple classes named '{root_fqcn}':", file=sys.stderr)
            for c in sorted(candidates):
                print(f"  {c}", file=sys.stderr)
            print("Please specify the full package name.", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"Error: root class '{root_fqcn}' not found in {args.kafka_dir}", file=sys.stderr)
            sys.exit(1)

    print(f"Root class: {root_fqcn}", file=sys.stderr)

    # Step 2: Build dependency graph
    print(f"Building dependency graph...", file=sys.stderr)
    graph, reachable = build_dependency_graph(root_fqcn, fqcn_files, simple_to_fqcns)
    print(f"Found {len(reachable)} classes in dependency graph.", file=sys.stderr)

    # Step 3: Topological sort
    sorted_classes = topological_sort(graph, reachable)

    # Step 4: Read marked classes
    marked_fqcns = read_marked_classes(args.mark_file, simple_to_fqcns)
    if marked_fqcns:
        print(f"Marking {len(marked_fqcns)} classes.", file=sys.stderr)

    # Step 5: Generate JSON output if requested
    if args.json or args.json_only:
        generate_json_tree(graph, root_fqcn, reachable, marked_fqcns, args.output)
        generate_flat_json(graph, reachable, sorted_classes, marked_fqcns, args.output, root_fqcn)

    # Step 6: Generate graph (unless --json-only)
    if not args.json_only:
        generate_graph(graph, reachable, sorted_classes, marked_fqcns, args.output, args.output_format, root_fqcn)

    # Step 7: Print topologically sorted list
    print("\n=== Topologically Sorted Classes (no dependencies first) ===")
    for i, fqcn in enumerate(sorted_classes, 1):
        marker = " [✓]" if fqcn in marked_fqcns else ""
        print(f"  {i:3d}. {fqcn}{marker}")

    # Step 8: Write remaining classes (excluding marked) to file
    remaining = [fqcn for fqcn in sorted_classes if fqcn not in marked_fqcns]
    remaining_file = os.path.join(os.path.dirname(args.output) or ".", "remaining_classes.txt")
    with open(remaining_file, "w") as f:
        for fqcn in remaining:
            f.write(f"{fqcn}\n")
    print(f"Wrote {len(remaining)} remaining classes to {remaining_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
