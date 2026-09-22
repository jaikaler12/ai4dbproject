import json
from dataclasses import dataclass, field

from .model import OpNode


@dataclass
class DagNode:
    id: str
    op: str
    out_rows: int
    in_rows: int | None = None
    left_in_rows: int | None = None
    right_in_rows: int | None = None
    table: str | None = None
    predicate: str | None = None
    join_cond: str | None = None
    group_keys: list[str] = field(default_factory=list)
    children: list[str] = field(default_factory=list)
    queries: set[str] = field(default_factory=set)


def build_dag(queries: dict[str, OpNode], merge: bool = True,
              table_sizes: dict[str, int] | None = None) -> dict:
    nodes: dict[str, DagNode] = {}
    sig_index: dict[tuple, str] = {}
    counter = [0]

    def _next_id():
        counter[0] += 1
        return f"n{counter[0]}"

    def walk(node: OpNode, qname: str) -> str:
        child_ids = tuple(walk(c, qname) for c in node.children)
        sig = node.signature(child_ids)
        if merge and sig in sig_index:
            existing = nodes[sig_index[sig]]
            existing.queries.add(qname)
            return existing.id
        nid = _next_id()
        nodes[nid] = DagNode(
            id=nid,
            op=node.op,
            out_rows=node.out_rows,
            in_rows=node.in_rows,
            table=node.table,
            predicate=node.predicate,
            join_cond=node.join_cond,
            group_keys=list(node.group_keys),
            children=list(child_ids),
            queries={qname},
        )
        if merge:
            sig_index[sig] = nid
        return nid

    roots = {q: walk(tree, q) for q, tree in queries.items()}
    annotate(nodes, table_sizes)
    return {"nodes": nodes, "roots": roots,
            "table_sizes": dict(table_sizes or {})}


def topological(nodes: dict[str, DagNode]) -> list[DagNode]:
    seen, order = set(), []

    def visit(nid):
        if nid in seen:
            return
        seen.add(nid)
        for c in nodes[nid].children:
            visit(c)
        order.append(nodes[nid])

    for n in nodes.values():
        visit(n.id)
    return order


def annotate(nodes: dict[str, DagNode],
             table_sizes: dict[str, int] | None = None) -> None:
    sizes = {k.lower(): _rows_of(v) for k, v in (table_sizes or {}).items()}
    for n in topological(nodes):
        if n.op == "scan":
            true_size = sizes.get((n.table or "").lower())
            if true_size is not None:
                # in_rows = the whole table (what the scan started from);
                # out_rows = what the access method actually handed on. Whether
                # that was a full scan or an index-narrowed one is NOT stored --
                # it is derivable by comparing out_rows against in_rows, so
                # storing it would just be a second copy that can go stale.
                n.in_rows = true_size
            elif n.in_rows is None:
                n.in_rows = n.out_rows
        elif len(n.children) == 1:
            n.in_rows = nodes[n.children[0]].out_rows
        elif len(n.children) == 2:
            n.left_in_rows = nodes[n.children[0]].out_rows
            n.right_in_rows = nodes[n.children[1]].out_rows


def _table_aliases(nodes: dict[str, DagNode]) -> dict[str, str]:
    aliases = {}
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    def key(t):
        return t.lower()

    for n in topological(nodes):
        if n.op == "scan" and n.table:
            k = key(n.table)
            if k not in aliases:
                aliases[k] = letters[len(aliases)]
    return aliases


def _rows_of(v):
    """A catalog entry is either a plain row count or {"rows":n,"width":w}."""
    return v["rows"] if isinstance(v, dict) else v


def _aliased_sizes(dag: dict, aliases: dict[str, str]) -> dict:
    sizes = {k.lower(): v for k, v in dag.get("table_sizes", {}).items()}
    return {alias: sizes[t] for t, alias in aliases.items() if t in sizes}


def render_text(dag: dict) -> str:
    aliases = _table_aliases(dag["nodes"])
    lines = []
    sizes = _aliased_sizes(dag, aliases)
    if sizes:
        lines.append("tables:")
        for alias, size in sorted(sizes.items()):
            if isinstance(size, dict):
                lines.append(f"  {alias} = {size['rows']} rows, "
                             f"{size['width']} bytes/row")
            else:
                lines.append(f"  {alias} = {size}")
        lines.append("")
    roots = dag["roots"]
    for n in topological(dag["nodes"]):
        base = f"{n.id}={n.op}"
        if n.op == "scan":
            alias = aliases[n.table.lower()] if n.table else "?"
            base += f"({alias})"
        elif len(n.children) == 2:
            base += f"({','.join(n.children)})"
        elif n.children:
            base += f"({','.join(n.children)})"
        extra = []
        if n.in_rows is not None:
            extra.append(f"in={n.in_rows}")
        if n.left_in_rows is not None:
            extra.append(f"lin={n.left_in_rows}")
        if n.right_in_rows is not None:
            extra.append(f"rin={n.right_in_rows}")
        extra.append(f"out={n.out_rows}")
        line = f"{base}  {' '.join(extra)}"
        if len(n.queries) == 1:
            q = next(iter(n.queries))
            if roots.get(q) == n.id:
                line += f"  === {q} ==="
        elif len(n.queries) > 1:
            line += f"  === shared:{','.join(sorted(n.queries))} ==="
        lines.append(line)
    return "\n".join(lines)


def to_json(dag: dict) -> dict:
    aliases = _table_aliases(dag["nodes"])
    return {
        "tables": _aliased_sizes(dag, aliases),
        "nodes": [
            {
                "id": n.id, "op": n.op, "out_rows": n.out_rows,
                "in_rows": n.in_rows,
                "left_in_rows": n.left_in_rows,
                "right_in_rows": n.right_in_rows,
                "table": aliases[n.table.lower()] if n.table else None,
                "children": n.children,
                "queries": sorted(n.queries),
            }
            for n in topological(dag["nodes"])
        ],
        "query_roots": dag["roots"],
    }


def dump_json(dag: dict, path: str):
    with open(path, "w") as f:
        json.dump(to_json(dag), f, indent=2)
