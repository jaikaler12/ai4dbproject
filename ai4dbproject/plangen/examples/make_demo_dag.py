import json
from pathlib import Path

from plangen.planner import Plan, solve
from plangen.datagen import ddl, write_data
from plangen.sqlgen import SqlGen

HERE = Path(__file__).parent


def node(nid, op, out, children=None, table=None, in_rows=None,
         lin=None, rin=None):
    return {"id": nid, "op": op, "out_rows": out, "in_rows": in_rows,
            "left_in_rows": lin, "right_in_rows": rin, "table": table,
            "children": children or [], "queries": []}


nodes = [
    node("n1", "scan", 100000, table="A", in_rows=100000),
    node("n2", "scan", 500000, table="B", in_rows=500000),
    node("n3", "scan", 1000000, table="C", in_rows=1000000),
    node("n4", "filter", 30000, ["n1"]),
    node("n5", "filter", 100000, ["n2"]),
    node("n7", "filter", 20000, ["n1"]),
    node("n9", "filter", 150000, ["n2"]),
    node("n11", "filter", 200000, ["n3"]),
    node("n13", "filter", 12000, ["n1"]),
    node("n6", "join", 8000, ["n4", "n5"], lin=30000, rin=100000),
    node("n8", "join", 5000, ["n7", "n5"], lin=20000, rin=100000),
    node("n10", "groupby", 5, ["n9"]),
    node("n12", "join", 20000, ["n5", "n11"], lin=100000, rin=200000),
    node("n14", "join", 3000, ["n13", "n5"], lin=12000, rin=100000),
    node("n15", "join", 900, ["n14", "n11"], lin=3000, rin=200000),
    node("n16", "groupby", 3, ["n15"]),
    node("n17", "filter", 5000, ["n1"]),
    node("n18", "join", 30000, ["n17", "n5"], lin=5000, rin=100000),
]
dag = {"nodes": nodes,
       "query_roots": {"Q1": "n6", "Q2": "n8", "Q3": "n10",
                       "Q4": "n12", "Q5": "n16", "Q6": "n18"}}
(HERE / "demo_dag.json").write_text(json.dumps(dag, indent=2) + "\n")

plan = solve(Plan(dag))
out = HERE / "gen_out"
(out / "queries").mkdir(parents=True, exist_ok=True)
(out / "schema.sql").write_text(ddl(plan))
write_data(plan, out)
gen = SqlGen(plan)
expected = {}
for q in sorted(plan.roots):
    sql, exp = gen.query(q)
    (out / "queries" / f"{q}.sql").write_text(sql + "\n")
    expected[q] = exp
    print(f"--- {q} (expect count={exp}) ---")
    print(sql)
(out / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")

print("\nranges:")
for nid, r in plan.ranges.items():
    print(f"  {nid}: tbl_{r['table'].lower()}[{r['lo']}:{r['hi']})")
print("joins:")
for j in plan.joins.values():
    print(f"  {j.jid}: {j.fk_table}.{j.fk_col} -> {j.ref_table} "
          f"({len(j.fk_ids)} rows)")
print("groups:")
for g in plan.groups.values():
    w = f"[{g.lo}:{g.hi})" if g.ids is None else f"{len(g.ids)} ids"
    print(f"  {g.gid}: {g.table}.{g.col} k={g.k} over {w}")
