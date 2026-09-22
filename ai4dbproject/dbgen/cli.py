import argparse
import json
from pathlib import Path

from .planner import Plan, solve
from .datagen import ddl, write_data
from .sqlgen import SqlGen
from . import verify


def generate(dag_path: str, outdir: str):
    dag = json.loads(Path(dag_path).read_text())
    plan = solve(Plan(dag))
    out = Path(outdir)
    (out / "queries").mkdir(parents=True, exist_ok=True)
    (out / "schema.sql").write_text(ddl(plan))
    write_data(plan, out)
    gen = SqlGen(plan)
    expected = {}
    for q in sorted(plan.roots):
        sql, exp = gen.query(q)
        (out / "queries" / f"{q}.sql").write_text(sql + "\n")
        expected[q] = exp
    (out / "expected.json").write_text(
        json.dumps(expected, indent=2) + "\n")
    return plan, expected


def main():
    ap = argparse.ArgumentParser(prog="dbgen")
    ap.add_argument("--dag", required=True,
                    help="DAG JSON (plan2dag output format)")
    ap.add_argument("-o", "--out", default="generated")
    ap.add_argument("--dsn", help="postgres DSN: load data + verify counts")
    ap.add_argument("--psql", default="psql")
    args = ap.parse_args()

    plan, _ = generate(args.dag, args.out)

    print(f"tables: { {t: s for t, s in sorted(plan.tables.items())} }")
    for nid, r in plan.ranges.items():
        print(f"range {nid}: tbl_{r['table'].lower()}"
              f"[{r['lo']}:{r['hi']})")
    for j in plan.joins.values():
        print(f"join {j.jid}: {j.fk_table}.{j.fk_col} -> "
              f"{j.ref_table} out={j.out_rows}")
    for g in plan.groups.values():
        where = f"[{g.lo}:{g.hi})" if g.ids is None else \
            f"{len(g.ids)} matched rows"
        print(f"group {g.gid}: {g.table}.{g.col} k={g.k} over {where}")
    print(f"artifacts: {Path(args.out).resolve()}")

    if args.dsn:
        verify.load_schema(args.psql, args.dsn, Path(args.out))
        fails = verify.verify(args.psql, args.dsn, Path(args.out))
        raise SystemExit(1 if fails else 0)


if __name__ == "__main__":
    main()
