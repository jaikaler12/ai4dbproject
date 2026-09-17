import json
from pathlib import Path

from plan2dag.parse_postgres import parse_queries
from plan2dag.dag import build_dag, render_text, dump_json

EXAMPLES = Path(__file__).parent / "examples"

with open(EXAMPLES / "pg_q1.json") as f:
    q1 = json.load(f)
plans = {"Q1": q1}
if (EXAMPLES / "pg_q2.json").exists():
    with open(EXAMPLES / "pg_q2.json") as f:
        plans["Q2"] = json.load(f)

trees = parse_queries(plans)
dag = build_dag(trees)
print(render_text(dag))
dump_json(dag, EXAMPLES / "demo_dag.json")
