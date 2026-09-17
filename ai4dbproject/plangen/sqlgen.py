from dataclasses import dataclass, field


@dataclass
class Compiled:
    from_sql: str
    key: str
    table: str
    tabalias: str
    refalias: str
    conds: list = field(default_factory=list)
    group_expr: str | None = None


class SqlGen:
    def __init__(self, plan):
        self.plan = plan

    def query(self, qname: str) -> tuple[str, int]:
        c, exp = self._walk(self.plan.roots[qname])
        if c.group_expr:
            inner = self._select(c, [c.group_expr], group_by=True)
        else:
            inner = self._select(c, [c.key])
        return f"SELECT COUNT(*) FROM ({inner}) q;", exp

    def _select(self, c, cols, group_by=False) -> str:
        s = f"SELECT {', '.join(cols)} FROM {c.from_sql}"
        if c.conds:
            s += " WHERE " + " AND ".join(f"({x})" for x in c.conds)
        if group_by:
            s += f" GROUP BY {c.group_expr}"
        return s

    def _walk(self, nid: str) -> tuple[Compiled, int]:
        node = self.plan.nodes[nid]
        op = node["op"]

        if op == "scan":
            t = node["table"]
            a = f"t{nid}"
            return (Compiled(from_sql=f"tbl_{t.lower()} AS {a}",
                             key=f"{a}.id", table=t, tabalias=a,
                             refalias=a),
                    node["in_rows"] or node["out_rows"])

        if op == "filter":
            c, _ = self._walk(node["children"][0])
            r = self.plan.ranges[nid]
            c.conds.append(f"{c.key} >= {r['lo']} AND {c.key} < {r['hi']}")
            return c, node["out_rows"]

        if op == "join":
            lc, _ = self._walk(node["children"][0])
            rc, _ = self._walk(node["children"][1])
            jp = self.plan.joins[nid]
            if lc.table == jp.fk_table:
                fks, refs = lc, rc
            else:
                fks, refs = rc, lc
            on = f"{fks.tabalias}.{jp.fk_col} = {refs.key}"
            c = Compiled(from_sql=f"{lc.from_sql} JOIN {rc.from_sql} ON {on}",
                         key=refs.key, table=refs.table,
                         tabalias=refs.tabalias, refalias=refs.refalias,
                         conds=lc.conds + rc.conds)
            return c, node["out_rows"]

        if op == "groupby":
            c, _ = self._walk(node["children"][0])
            g = self.plan.groups[nid]
            c.group_expr = f"{c.refalias}.{g.col}"
            return c, node["out_rows"]

        if op in ("aggregate", "sort"):
            return self._walk(node["children"][0])

        raise NotImplementedError(f"cannot compile operator {op} ({nid})")
