from dataclasses import dataclass, field


@dataclass
class Compiled:
    """A partially built query.

    `carrier` is the alias whose rows ARE this result -- the planner hands a
    join back as a contiguous range on its fk-side table, so the fk side's
    alias carries the result's identity onward.

    `from_sql` is an explicit JOIN tree, nested exactly as the DAG nests its
    join nodes, so the written query states the DAG's join order rather than
    handing the optimizer a flat table list to reorder at will.
    """
    from_sql: str = ""
    conds: list = field(default_factory=list)
    carrier: str = ""                            # alias
    carrier_table: str = ""                      # table letter
    group_expr: str | None = None


class SqlGen:
    def __init__(self, plan):
        self.plan = plan

    def query(self, qname: str) -> tuple[str, int]:
        self._n = 0
        c, exp = self._walk(self.plan.roots[qname])
        col = c.group_expr or f"{c.carrier}.id"
        s = f"SELECT {col} FROM {c.from_sql}"
        if c.conds:
            s += " WHERE " + " AND ".join(c.conds)
        if c.group_expr:
            s += f" GROUP BY {c.group_expr}"
        return f"SELECT COUNT(*) FROM ({s}) q;", exp

    def _alias(self) -> str:
        self._n += 1
        return f"t{self._n}"

    def _walk(self, nid: str) -> tuple[Compiled, int]:
        node = self.plan.nodes[nid]
        op = node["op"]

        if op == "scan":
            t = node["table"]
            # a fresh alias per VISIT, not per node: a scan node shared by two
            # filters is two separate table instances in the query.
            a = self._alias()
            return (Compiled(from_sql=f"tbl_{t.lower()} AS {a}",
                             carrier=a, carrier_table=t),
                    node["in_rows"] or node["out_rows"])

        if op == "filter":
            c, _ = self._walk(node["children"][0])
            r = self.plan.ranges[nid]
            if self.plan.indexed.get(nid):
                # the original narrowed this scan with an index: give the pk a
                # range covering the scan's output, then the leftover condition
                # on an unindexed column, exactly as the original plan had it.
                scan_out = self.plan.nodes[node["children"][0]]["out_rows"]
                c.conds.append(f"({c.carrier}.id < {scan_out})")
            c.conds.append(f"({c.carrier}.f_{nid} = 1)")
            return c, node["out_rows"]

        if op == "join":
            lc, _ = self._walk(node["children"][0])
            rc, _ = self._walk(node["children"][1])
            jp = self.plan.joins[nid]
            # whichever side's carrier sits on the fk table holds the fk column
            if lc.carrier_table == jp.fk_table:
                fks, refs = lc, rc
            else:
                fks, refs = rc, lc
            on = f"({fks.carrier}.{jp.fk_col} = {refs.carrier}.id)"
            # left child stays on the left, exactly as the DAG records it
            c = Compiled(
                from_sql=f"({lc.from_sql} JOIN {rc.from_sql} ON {on})",
                conds=lc.conds + rc.conds,
                carrier=fks.carrier,
                carrier_table=fks.carrier_table,
            )
            return c, node["out_rows"]

        if op == "groupby":
            c, _ = self._walk(node["children"][0])
            g = self.plan.groups[nid]
            c.group_expr = f"{c.carrier}.{g.col}"
            return c, node["out_rows"]

        if op in ("aggregate", "sort"):
            return self._walk(node["children"][0])

        raise NotImplementedError(f"cannot compile operator {op} ({nid})")
