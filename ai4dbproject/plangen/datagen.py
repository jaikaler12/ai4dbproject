from pathlib import Path


def ddl(plan) -> str:
    parts = []
    for t in sorted(plan.tables):
        cols = ["id INTEGER PRIMARY KEY"]
        for j in plan.joins.values():
            if j.fk_table == t:
                cols.append(f"{j.fk_col} INTEGER")
        for g in plan.groups.values():
            if g.table == t:
                cols.append(f"{g.col} INTEGER")
        body = ",\n  ".join(cols)
        parts.append(f"DROP TABLE IF EXISTS tbl_{t.lower()} CASCADE;\n"
                     f"CREATE TABLE tbl_{t.lower()} (\n  {body}\n);")
    return "\n\n".join(parts) + "\n"


def rows_iter(plan, t, size):
    fks = {}
    for j in plan.joins.values():
        if j.fk_table == t:
            fks[j.fk_col] = dict(zip(j.fk_ids, j.ref_ids))
    grps = []
    for g in plan.groups.values():
        if g.table != t:
            continue
        if g.ids is not None:
            grps.append((g.col, {iid: idx % g.k
                                 for idx, iid in enumerate(g.ids)}))
        else:
            grps.append((g.col, (g.lo, g.hi, g.k)))
    for i in range(size):
        row = [i]
        for d in fks.values():
            row.append(d.get(i))
        for _, spec in grps:
            if isinstance(spec, dict):
                row.append(spec.get(i))
            else:
                lo, hi, k = spec
                row.append((i - lo) % k if lo <= i < hi else None)
        yield row


def write_data(plan, outdir: Path) -> Path:
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    copies = []
    for t, size in sorted(plan.tables.items()):
        name = f"tbl_{t.lower()}"
        csv = (outdir / f"{name}.csv").resolve()
        with open(csv, "w") as f:
            for row in rows_iter(plan, t, size):
                f.write(",".join("" if v is None else str(v)
                                 for v in row) + "\n")
        cols = ",".join(["id"] +
                        [j.fk_col for j in plan.joins.values()
                         if j.fk_table == t] +
                        [g.col for g in plan.groups.values()
                         if g.table == t])
        copies.append(f"\\copy {name} ({cols}) FROM '{csv}' "
                      f"WITH (FORMAT csv, NULL '')")
    load_sql = outdir / "load.sql"
    load_sql.write_text("\n".join(copies) + "\n")
    return load_sql
