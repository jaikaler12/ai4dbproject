from dataclasses import dataclass


@dataclass
class Chain:
    table: str
    lo: int
    hi: int
    tabalias: str = ""
    restricted: bool = False

    @property
    def rows(self):
        return self.hi - self.lo


@dataclass
class Compound:
    table: str
    ids: set
    keycol: str = "id"

    @property
    def rows(self):
        return len(self.ids)


@dataclass
class JoinPlan:
    jid: str
    out_rows: int
    fk_table: str
    fk_col: str
    fk_ids: list
    ref_table: str
    ref_ids: list


@dataclass
class GroupPlan:
    gid: str
    table: str
    col: str
    k: int
    lo: int | None = None
    hi: int | None = None
    ids: list | None = None


class Plan:
    def __init__(self, dag: dict):
        self.nodes = {n["id"]: n for n in dag["nodes"]}
        self.roots = dag["query_roots"]
        self.tables = {}
        self.widths = {}          # table -> target bytes per row, if supplied
        cat = dag.get("tables", {})
        for n in self.nodes.values():
            if n["op"] == "scan":
                t = n["table"]
                e = cat.get(t)
                if isinstance(e, dict):
                    self.tables[t] = e["rows"]
                    self.widths[t] = e.get("width")
                else:
                    self.tables[t] = e or n["in_rows"] or n["out_rows"]
        self.ranges = {}
        # filter node -> True when its scan read fewer rows than the table,
        # i.e. an index narrowed it. Those get a pk-range predicate (indexed);
        # full scans get a predicate on a plain unindexed column instead.
        self.indexed = {}
        self.joins = {}
        self.groups = {}


def solve(plan: Plan) -> Plan:
    avail = {}
    memo = {}

    def walk(nid):
        node = plan.nodes[nid]
        op = node["op"]

        if op == "scan":
            t = node["table"]
            return Chain(table=t, lo=0, hi=plan.tables[t], tabalias=t)

        if op == "filter":
            inner = walk(node["children"][0])
            t = inner.table
            if not isinstance(inner, Chain):
                raise NotImplementedError(
                    f"{nid}: filter directly over a join output")
            if nid in memo:
                lo, hi = memo[nid]
                return Chain(table=t, lo=lo, hi=hi, tabalias=t,
                             restricted=True)
            if inner.restricted:
                raise NotImplementedError(
                    f"{nid}: stacked filters on one chain unsupported")
            iv = avail.setdefault(t, [0, plan.tables[t]])
            k = node["out_rows"]
            if iv[1] - iv[0] < k:
                raise ValueError(
                    f"{nid}: filter out={k} exceeds remaining rows "
                    f"{iv[1] - iv[0]} on table {t}")
            lo, hi = iv[0], iv[0] + k
            iv[0] = hi
            memo[nid] = (lo, hi)
            plan.ranges[nid] = {"table": t, "lo": lo, "hi": hi}
            child = plan.nodes[node["children"][0]]
            if child["op"] == "scan":
                plan.indexed[nid] = child["out_rows"] < plan.tables[t]
            return Chain(table=t, lo=lo, hi=hi, tabalias=t, restricted=True)

        if op == "join":
            if len(node["children"]) != 2:
                raise ValueError(f"{nid}: join must have exactly 2 children")
            lch = walk(node["children"][0])
            rch = walk(node["children"][1])
            return resolve_join(nid, node, lch, rch)

        if op == "groupby":
            inner = walk(node["children"][0])
            k = node["out_rows"]
            if isinstance(inner, Chain):
                if inner.rows < k:
                    raise ValueError(f"{nid}: groups {k} > input {inner.rows}")
                plan.groups[nid] = GroupPlan(gid=nid, table=inner.table,
                                             col=f"g_{nid}", k=k,
                                             lo=inner.lo, hi=inner.hi)
            else:
                ids = sorted(inner.ids)
                if len(ids) < k:
                    raise ValueError(f"{nid}: groups {k} > input {len(ids)}")
                plan.groups[nid] = GroupPlan(gid=nid, table=inner.table,
                                             col=f"g_{nid}", k=k, ids=ids)
            return inner

        if op in ("aggregate", "sort"):
            return walk(node["children"][0])

        if op == "limit":
            raise NotImplementedError("LIMIT")

        raise NotImplementedError(f"operator {op} ({nid})")

    def resolve_join(nid, node, lch, rch):
        # Every side is a Chain: a contiguous id range on one table. The many
        # side carries the fk column; the smaller side is the target, reused
        # round-robin so several fk rows can point at the same target row.
        out = node["out_rows"]
        ref, fk = (lch, rch) if lch.rows <= rch.rows else (rch, lch)

        if out > fk.rows:
            raise ValueError(
                f"{nid}: join out={out} exceeds fk-side pool "
                f"({fk.table}={fk.rows}); out can exceed the ref pool "
                f"(fan-out), but every output pair consumes one fk row")

        pool = range(ref.lo, ref.hi)
        ref_ids = [pool[i % len(pool)] for i in range(out)]
        fk_ids = list(range(fk.lo, fk.lo + out))

        plan.joins[nid] = JoinPlan(jid=nid, out_rows=out,
                                   fk_table=fk.table, fk_col=f"fk_{nid}",
                                   fk_ids=fk_ids, ref_table=ref.table,
                                   ref_ids=ref_ids)
        # The surviving rows are the first `out` of the fk side, so the result
        # is still a contiguous range -- which lets it be the many side of a
        # join further up (bushy joins work, no Compound needed).
        return Chain(table=fk.table, lo=fk.lo, hi=fk.lo + out,
                     tabalias=fk.table, restricted=True)

    for q in sorted(plan.roots):
        walk(plan.roots[q])
    return plan
