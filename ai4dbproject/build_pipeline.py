#!/usr/bin/env python3
"""
build_pipeline.py — one end-to-end run of the whole pipeline, laid out in three
sibling folders:

    ground_truth/   the ORIGINAL database: real-ish schema, naturally random
                    data, real SQL with real predicates, and the real Postgres
                    EXPLAIN (ANALYZE, FORMAT JSON) plans those queries produce.

    dag/            what plan2dag distills those plans down to: operators, how
                    they connect, and rows in/out per operator.  This is the
                    ONLY thing plangen is allowed to see -- no table names, no
                    column names, no predicates, no data.

    output/         what plangen builds back out of that DAG: synthetic schema,
                    synthetic data, synthetic SQL, loaded into a second
                    Postgres database, plus the real plans IT produces.

Stages
    1  build + load the original database, capture its plans      -> ground_truth/
    2  plan2dag: captured plans -> merged multi-query DAG         -> dag/
    3  plangen:  DAG -> synthetic schema + data + SQL             -> output/
    4  load the generated database, capture its plans             -> output/plans/
    5  score ground_truth/plans vs output/plans (Picasso rho)

Both captures disable parallelism (max_parallel_workers_per_gather = 0) and both
databases are ANALYZE'd after load, so the two sides are compared on equal
footing: same engine, same stats availability, same worker count.

Usage:  python3 build_pipeline.py [--skip-compare]
"""

import argparse
import json
import random
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from plan2dag.parse_postgres import parse_queries          # noqa: E402
from plan2dag.dag import (build_dag, render_text, to_json,  # noqa: E402
                          _table_aliases)
from plangen.planner import Plan, solve                    # noqa: E402
from plangen.datagen import ddl, write_data                # noqa: E402
from plangen.sqlgen import SqlGen                          # noqa: E402

GT_DIR = HERE / "ground_truth"
DAG_DIR = HERE / "dag"
OUT_DIR = HERE / "output"

GT_DB = "ai4db_ground_truth"
GEN_DB = "ai4db_generated"

# Turning parallelism off makes "Actual Rows" a true total instead of a
# per-worker average (which plan2dag has to multiply back up by Actual Loops,
# introducing rounding noise into the DAG targets). Applied to BOTH sides.
NO_PARALLEL = "SET max_parallel_workers_per_gather = 0;"

SEED = 20260912
N_CUSTOMER = 100_000
N_ORDERS = 500_000
N_LINEITEM = 1_000_000

SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]
NATIONS = ["INDIA", "BRAZIL", "GERMANY", "JAPAN", "KENYA"]
STATUS = [("O", 0.20), ("F", 0.50), ("P", 0.30)]
PRIORITY = ["1-URGENT", "3-MEDIUM", "5-LOW"]
SHIPMODE = [("AIR", 0.20), ("SHIP", 0.20), ("TRUCK", 0.25),
            ("RAIL", 0.20), ("MAIL", 0.15)]
RETURNFLAG = ["R", "N"]

ORIGINAL_SCHEMA = """\
DROP TABLE IF EXISTS lineitem CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customer CASCADE;

CREATE TABLE customer (
  id       INTEGER PRIMARY KEY,
  segment  TEXT,
  nation   TEXT
);

CREATE TABLE orders (
  id        INTEGER PRIMARY KEY,
  cid       INTEGER,
  status    TEXT,
  priority  TEXT
);

CREATE TABLE lineitem (
  id          INTEGER PRIMARY KEY,
  oid         INTEGER,
  shipmode    TEXT,
  returnflag  TEXT
);
"""

# Six original queries. query_1/2/4/5/6 all filter orders on status='O' -- that
# shared subexpression is the multi-query shared-table stress node the whole
# approach exists to handle.
ORIGINAL_QUERIES = {
    "query_1": """\
SELECT COUNT(*)
FROM customer c JOIN orders o ON o.cid = c.id
WHERE c.segment = 'AUTOMOBILE' AND o.status = 'O'""",

    "query_2": """\
SELECT COUNT(*)
FROM customer c JOIN orders o ON o.cid = c.id
WHERE c.segment = 'BUILDING' AND o.status = 'O'""",

    "query_3": """\
SELECT o.priority, COUNT(*)
FROM orders o
WHERE o.status = 'F'
GROUP BY o.priority""",

    "query_4": """\
SELECT COUNT(*)
FROM orders o JOIN lineitem l ON l.oid = o.id
WHERE o.status = 'O' AND l.shipmode = 'AIR'""",

    "query_5": """\
SELECT l.returnflag, COUNT(*)
FROM customer c
JOIN orders o ON o.cid = c.id
JOIN lineitem l ON l.oid = o.id
WHERE c.segment = 'FURNITURE' AND o.status = 'O' AND l.shipmode = 'AIR'
GROUP BY l.returnflag""",

    "query_6": """\
SELECT COUNT(*)
FROM customer c JOIN orders o ON o.cid = c.id
WHERE c.segment = 'MACHINERY' AND o.status = 'O'""",
}

QNAMES = sorted(ORIGINAL_QUERIES)


# ───────────────────────────── shell helpers ────────────────────────────────
def run(cmd, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError("command failed: %s\n%s" % (" ".join(cmd),
                                                       r.stderr.strip()))
    return r.stdout


def psql(db, *args):
    # -q keeps command tags ("SET") out of stdout so EXPLAIN ... FORMAT JSON
    # output parses cleanly.
    return run(["psql", "dbname=" + db, "-q", "-v", "ON_ERROR_STOP=1"]
               + list(args))


def recreate_db(name):
    run(["dropdb", "--if-exists", name])
    run(["createdb", name])


def weighted_picker(rng, choices):
    """Return a 0-arg callable drawing from [(value, probability), ...]."""
    cum, acc = [], 0.0
    for v, p in choices:
        acc += p
        cum.append((acc, v))
    last = cum[-1][1]

    def pick():
        x = rng.random()
        for thresh, v in cum:
            if x < thresh:
                return v
        return last
    return pick


# ─────────────────── stage 1: the ORIGINAL database ─────────────────────────
def stage1_ground_truth():
    print("=" * 72)
    print("STAGE 1  original database -> ground_truth/")
    print("=" * 72)

    data = GT_DIR / "data"
    queries = GT_DIR / "queries"
    plans = GT_DIR / "plans"
    for d in (data, queries, plans):
        d.mkdir(parents=True, exist_ok=True)

    rng = random.Random(SEED)
    pick_status = weighted_picker(rng, STATUS)
    pick_ship = weighted_picker(rng, SHIPMODE)

    # Naturally random data: no engineered id blocks, no pre-placed answers.
    with open(data / "customer.csv", "w") as f:
        for i in range(N_CUSTOMER):
            f.write("%d,%s,%s\n" % (i, rng.choice(SEGMENTS),
                                    rng.choice(NATIONS)))
    with open(data / "orders.csv", "w") as f:
        for i in range(N_ORDERS):
            f.write("%d,%d,%s,%s\n" % (i, rng.randrange(N_CUSTOMER),
                                       pick_status(), rng.choice(PRIORITY)))
    with open(data / "lineitem.csv", "w") as f:
        for i in range(N_LINEITEM):
            f.write("%d,%d,%s,%s\n" % (i, rng.randrange(N_ORDERS),
                                       pick_ship(), rng.choice(RETURNFLAG)))
    print("  data written: customer=%d orders=%d lineitem=%d"
          % (N_CUSTOMER, N_ORDERS, N_LINEITEM))

    (GT_DIR / "schema.sql").write_text(ORIGINAL_SCHEMA)
    load_sql = "\n".join(
        "\\copy %s FROM '%s' WITH (FORMAT csv)" % (t, (data / (t + ".csv")))
        for t in ("customer", "orders", "lineitem")) + "\nANALYZE;\n"
    (GT_DIR / "load.sql").write_text(load_sql)

    for q in QNAMES:
        (queries / (q + ".sql")).write_text(ORIGINAL_QUERIES[q] + ";\n")

    print("  creating database %s ..." % GT_DB)
    recreate_db(GT_DB)
    psql(GT_DB, "-f", str(GT_DIR / "schema.sql"))
    psql(GT_DB, "-f", str(GT_DIR / "load.sql"))
    print("  loaded + ANALYZE'd")

    results = capture_plans(GT_DB, ORIGINAL_QUERIES, plans)
    (GT_DIR / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return results


def capture_plans(db, queries, plans_dir):
    """Run each query for real, then capture its plan as JSON + readable text."""
    results = {}
    for q in sorted(queries):
        sql = queries[q].rstrip().rstrip(";")
        rows = psql(db, "-At", "-c", NO_PARALLEL,
                    "-c", sql).strip().splitlines()
        js = psql(db, "-At", "-c", NO_PARALLEL,
                  "-c", "EXPLAIN (ANALYZE, FORMAT JSON) " + sql)
        txt = psql(db, "-At", "-c", NO_PARALLEL,
                   "-c", "EXPLAIN (ANALYZE) " + sql)
        (plans_dir / (q + ".json")).write_text(js)
        (plans_dir / (q + ".txt")).write_text(txt)
        plan = json.loads(js)[0]["Plan"]
        results[q] = {"result_rows": len(rows),
                      "first_row": rows[0] if rows else None,
                      "root_op": plan["Node Type"],
                      "root_actual_rows": plan.get("Actual Rows")}
        print("  %-9s root=%-16s rows_out=%-8s result_lines=%d"
              % (q, plan["Node Type"], plan.get("Actual Rows"), len(rows)))
    return results


# ───────────────────── stage 2: plans -> DAG (plan2dag) ─────────────────────
def stage2_dag():
    print()
    print("=" * 72)
    print("STAGE 2  captured plans -> DAG  (plan2dag) -> dag/")
    print("=" * 72)
    DAG_DIR.mkdir(parents=True, exist_ok=True)

    captured = {q: json.loads((GT_DIR / "plans" / (q + ".json")).read_text())
                for q in QNAMES}
    (DAG_DIR / "captured_plans.json").write_text(
        json.dumps(captured, indent=2) + "\n")

    # Supplied up front, not derived from the plans: a plan that used an index
    # never reveals how big its table was, so without this a scan's output
    # would be mistaken for the whole table.
    table_sizes = {"customer": N_CUSTOMER, "orders": N_ORDERS,
                   "lineitem": N_LINEITEM}
    (DAG_DIR / "table_sizes.json").write_text(
        json.dumps(table_sizes, indent=2) + "\n")

    trees = parse_queries(captured)
    dag = build_dag(trees, table_sizes=table_sizes)   # merges shared subexprs
    js = to_json(dag)
    (DAG_DIR / "dag.json").write_text(json.dumps(js, indent=2) + "\n")
    (DAG_DIR / "dag.txt").write_text(render_text(dag) + "\n\nquery roots: "
                                     + json.dumps(dag["roots"], indent=2) + "\n")

    # plan2dag strips real table names down to A/B/C; plangen turns those into
    # tbl_a/tbl_b/tbl_c.  Record the round trip so the plan comparator can line
    # the two sides' leaves back up instead of scoring a pure rename as a
    # structural difference.
    aliases = _table_aliases(dag["nodes"])            # {orders: A, ...}
    relmap = {"tbl_" + letter.lower(): real
              for real, letter in aliases.items()}    # {tbl_a: orders, ...}
    (DAG_DIR / "table_aliases.json").write_text(
        json.dumps({"original_to_alias": aliases,
                    "generated_to_original": relmap}, indent=2) + "\n")

    shared = [n for n in js["nodes"] if len(n["queries"]) > 1]
    print(render_text(dag))
    print()
    print("  nodes=%d  shared-by->1-query nodes=%d  tables=%s"
          % (len(js["nodes"]), len(shared),
             sorted({n["table"] for n in js["nodes"] if n["table"]})))
    for n in shared:
        print("    %s %-9s out=%-8d shared by %s"
              % (n["id"], n["op"], n["out_rows"], ",".join(n["queries"])))
    return js


# ─────────────────── stage 3: DAG -> generated database ─────────────────────
def stage3_generate(dag_js):
    print()
    print("=" * 72)
    print("STAGE 3  DAG -> synthetic schema + data + SQL  (plangen) -> output/")
    print("=" * 72)
    data = OUT_DIR / "data"
    queries = OUT_DIR / "queries"
    for d in (data, queries):
        d.mkdir(parents=True, exist_ok=True)

    plan = solve(Plan(dag_js))
    (OUT_DIR / "schema.sql").write_text(ddl(plan))
    write_data(plan, data)

    gen = SqlGen(plan)
    expected, gen_queries = {}, {}
    for q in sorted(plan.roots):
        sql, exp = gen.query(q)
        (queries / (q + ".sql")).write_text(sql + "\n")
        expected[q] = exp
        gen_queries[q] = sql
    (OUT_DIR / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")

    print("  tables: %s" % {t: s for t, s in sorted(plan.tables.items())})
    for nid, r in plan.ranges.items():
        print("    range %-4s tbl_%s[%d:%d)" % (nid, r["table"].lower(),
                                                r["lo"], r["hi"]))
    for j in plan.joins.values():
        print("    join  %-4s %s.%s -> %s  out=%d"
              % (j.jid, j.fk_table, j.fk_col, j.ref_table, j.out_rows))
    for g in plan.groups.values():
        where = ("[%d:%d)" % (g.lo, g.hi)) if g.ids is None \
            else "%d matched rows" % len(g.ids)
        print("    group %-4s %s.%s k=%d over %s"
              % (g.gid, g.table, g.col, g.k, where))
    return plan, expected, gen_queries


# ────────────── stage 4: load generated db + capture its plans ──────────────
def stage4_capture_generated(expected, gen_queries):
    print()
    print("=" * 72)
    print("STAGE 4  load generated database, capture its plans -> output/plans/")
    print("=" * 72)
    plans = OUT_DIR / "plans"
    plans.mkdir(parents=True, exist_ok=True)

    print("  creating database %s ..." % GEN_DB)
    recreate_db(GEN_DB)
    psql(GEN_DB, "-f", str(OUT_DIR / "schema.sql"))
    psql(GEN_DB, "-f", str(OUT_DIR / "data" / "load.sql"))
    # plangen's own verify path skips this; without it Postgres plans the
    # generated database with default guesses (the old gen_out/ capture shows
    # rows=9 estimated against rows=8000 actual).
    psql(GEN_DB, "-c", "ANALYZE;")
    print("  loaded + ANALYZE'd")

    results = capture_plans(GEN_DB, gen_queries, plans)

    print()
    print("  row-count check (generated query result vs DAG target):")
    fails = 0
    for q in QNAMES:
        got = results[q]["first_row"]
        exp = expected[q]
        ok = str(got) == str(exp)
        fails += 0 if ok else 1
        print("    %-9s target=%-8s got=%-8s %s"
              % (q, exp, got, "PASS" if ok else "FAIL"))
    (OUT_DIR / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return fails


# ──────────────────────── stage 5: score the match ──────────────────────────
def stage5_compare():
    print()
    print("=" * 72)
    print("STAGE 5  plan comparison  (Picasso rho)")
    print("=" * 72)
    comparator = HERE.parent / "postgres_plan_tree_diff_cost.py"
    if not comparator.exists():
        print("  [!] comparator not found at %s" % comparator)
        return
    relmap = DAG_DIR / "table_aliases.json"
    args = [sys.executable, str(comparator),
            str(GT_DIR / "plans"), str(OUT_DIR / "plans"),
            "--csv", str(HERE / "plan_comparison.csv")]
    if relmap.exists():
        gen_to_orig = json.loads(relmap.read_text())["generated_to_original"]
        flat = DAG_DIR / "relation_map.json"
        flat.write_text(json.dumps(gen_to_orig, indent=2) + "\n")
        args += ["--relation-map", str(flat)]
    print(run(args, check=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-compare", action="store_true")
    ap.add_argument("--clean", action="store_true",
                    help="remove the three folders before rebuilding")
    args = ap.parse_args()

    if args.clean:
        for d in (GT_DIR, DAG_DIR, OUT_DIR):
            if d.exists():
                shutil.rmtree(d)

    stage1_ground_truth()
    dag_js = stage2_dag()
    plan, expected, gen_queries = stage3_generate(dag_js)
    fails = stage4_capture_generated(expected, gen_queries)
    if not args.skip_compare:
        stage5_compare()

    print()
    print("=" * 72)
    print("folders:")
    print("  ground_truth/  %s" % GT_DIR)
    print("  dag/           %s" % DAG_DIR)
    print("  output/        %s" % OUT_DIR)
    print("databases: %s (original), %s (generated)" % (GT_DB, GEN_DB))
    if fails:
        print("row-count mismatches: %d" % fails)
    print("=" * 72)


if __name__ == "__main__":
    main()
