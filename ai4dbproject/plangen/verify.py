import json
import subprocess
from pathlib import Path


def _run(psql: str, dsn: str, args: list) -> str:
    r = subprocess.run([psql, dsn, "-v", "ON_ERROR_STOP=1"] + args,
                       capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr.strip())
    return r.stdout


def load_schema(psql: str, dsn: str, outdir: Path):
    _run(psql, dsn, ["-f", str(outdir / "schema.sql")])
    _run(psql, dsn, ["-f", str(outdir / "load.sql")])


def verify(psql: str, dsn: str, outdir: Path) -> int:
    expected = json.loads((outdir / "expected.json").read_text())
    pdir = outdir / "plans"
    pdir.mkdir(exist_ok=True)
    fails = 0
    for q, exp in sorted(expected.items()):
        sql = (outdir / "queries" / f"{q}.sql").read_text()
        got = _run(psql, dsn, ["-At", "-c", sql]).strip()
        plan_txt = _run(psql, dsn, ["-At", "-c",
                                    "EXPLAIN (ANALYZE) " + sql])
        (pdir / f"{q}.txt").write_text(plan_txt + "\n")
        ok = got == str(exp)
        fails += 0 if ok else 1
        print(f"{q}: expected={exp} got={got} "
              f"{'PASS' if ok else 'FAIL'}")
    return fails
