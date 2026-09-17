from .model import OpNode

SCAN_TYPES = {
    "Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan",
    "Tid Scan", "Function Scan", "Values Scan", "CTE Scan", "Subquery Scan",
    "WorkTable Scan", "Named Tuplestore Scan", "Table Function Scan",
}
JOIN_TYPES = {"Nested Loop", "Hash Join", "Merge Join"}
GROUP_TYPES = {"HashAggregate", "GroupAggregate", "MixedAggregate"}
PASS_TYPES = {"Sort": "sort", "Limit": "limit"}


def parse_plan_json(plan: dict) -> OpNode:
    node = plan["Plan"]
    return _parse_node(node)


def _parse_node(n: dict) -> OpNode:
    ntype = n["Node Type"]
    loops = n.get("Actual Loops", 1)
    rows = int(round(n.get("Actual Rows", 0) * loops))

    children = [_parse_node(c) for c in n.get("Plans", [])]

    if ntype == "Bitmap Heap Scan":
        children = [c for c in children if c.raw_name != "Bitmap Index Scan"]
    if ntype in SCAN_TYPES:
        removed = n.get("Rows Removed by Filter", 0) * loops
        # An index-narrowed scan (Index Cond) reads a different set of rows
        # than a plain full scan of the same table, even when both end up
        # with the residual "Filter" text -- without this, the two scans'
        # signatures collide and merge() silently keeps whichever query's
        # numbers happened to build the shared node first.
        scan = OpNode(
            op="scan",
            out_rows=rows + removed,
            table=n.get("Relation Name") or n.get("Function Name") or "?",
            predicate=n.get("Index Cond"),
            raw_name=ntype,
            children=[],
        )
        filt = n.get("Filter") or n.get("Recheck Cond") or n.get("One-Time Filter")
        if filt is not None:
            return OpNode(
                op="filter",
                out_rows=rows,
                in_rows=scan.out_rows,
                predicate=filt,
                raw_name="Filter",
                children=[scan],
            )
        return scan

    if ntype in JOIN_TYPES:
        cond = n.get("Hash Cond") or n.get("Merge Cond") or n.get("Join Filter")
        return OpNode(op="join", out_rows=rows, join_cond=cond,
                      raw_name=ntype, children=children)

    # FORMAT JSON always says "Aggregate" and puts the strategy in a separate
    # field; HashAggregate/GroupAggregate are TEXT-format spellings only. Key
    # off "Group Key" so a grouped aggregate is not mistaken for a plain one.
    if ntype in GROUP_TYPES or (ntype == "Aggregate" and n.get("Group Key")):
        return OpNode(op="groupby", out_rows=rows, group_keys=n.get("Group Key", []),
                      raw_name=ntype, children=children)
    if ntype == "Aggregate":
        return OpNode(op="aggregate", out_rows=rows, raw_name=ntype, children=children)

    if ntype in PASS_TYPES:
        return OpNode(op=PASS_TYPES[ntype], out_rows=rows, raw_name=ntype,
                      children=children)

    if len(children) == 1 and not n.get("Filter"):
        child = children[0]
        return OpNode(
            op=child.op,
            out_rows=child.out_rows,
            in_rows=child.in_rows,
            table=child.table,
            predicate=child.predicate,
            join_cond=child.join_cond,
            group_keys=child.group_keys,
            raw_name=ntype,
            children=child.children,
        )

    return OpNode(op="other", out_rows=rows, raw_name=ntype, children=children)


def parse_queries(plans: dict[str, list | dict]) -> dict[str, OpNode]:
    out = {}
    for name, p in plans.items():
        root = p[0] if isinstance(p, list) else p
        out[name] = parse_plan_json(root)
    return out
