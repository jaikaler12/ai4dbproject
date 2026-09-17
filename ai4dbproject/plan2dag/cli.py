import argparse
import json
import subprocess
from pathlib import Path

from .model import OpNode
from .parse_postgres import parse_queries as pg_parse
from .dag import build_dag, render_text, dump_json


def capture_postgres(queries: dict[str, str], dsn: str, psql_bin: str = "psql") -> dict[str, OpNode]:
    plans = {}
    for name, sql in queries.items():
        out = subprocess.run(
            [psql_bin, dsn, "-At", "-c", f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}"],
            capture_output=True, text=True, check=True,
        )
        plans[name] = json.loads(out.stdout)
    return pg_parse(plans)


def load_captured(path: str, default_name: str = "Q1") -> dict[str, OpNode]:
    data = json.loads(Path(path).read_text())
    if isinstance(data, list):
        data = {default_name: data}
    return pg_parse(data)


def main():
    ap = argparse.ArgumentParser(prog="plan2dag")
    ap.add_argument("--dsn", help="postgres DSN, e.g. dbname=bench")
    ap.add_argument("--psql", default="psql", help="path to psql binary")
    ap.add_argument("--query", action="append",
                    metavar="NAME=FILE.sql or NAME=SQL",
                    help="query; prefix value with @ to read from file")
    ap.add_argument("--explain-json", help="skip live capture, load captured EXPLAIN JSON")
    ap.add_argument("--table-sizes", metavar="FILE",
                    help='JSON {"orders": 500000, ...} of true table row counts. '
                         "A scan whose output equals its table's size read the "
                         "whole table (access=full); fewer means an index "
                         "narrowed it first (access=partial).")
    ap.add_argument("-o", "--out", help="write DAG JSON here")
    args = ap.parse_args()

    table_sizes = json.loads(Path(args.table_sizes).read_text()) \
        if args.table_sizes else None

    queries = {}
    for spec in args.query or []:
        name, _, src = spec.partition("=")
        queries[name] = Path(src[1:]).read_text() if src.startswith("@") else src

    if args.explain_json:
        trees = load_captured(args.explain_json, next(iter(queries), "Q1"))
    elif args.dsn and queries:
        trees = capture_postgres(queries, args.dsn, args.psql)
    else:
        ap.error("provide --explain-json, or --dsn with --query")

    dag = build_dag(trees, table_sizes=table_sizes)
    print(render_text(dag))
    print()
    print("query roots:", dag["roots"])
    if args.out:
        dump_json(dag, args.out)
        print(f"written: {args.out}")


if __name__ == "__main__":
    main()
