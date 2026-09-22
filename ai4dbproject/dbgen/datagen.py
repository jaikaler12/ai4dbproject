from pathlib import Path

# measured on this Postgres: bytes_per_row ~= 31 + 4.16 * (number of int columns).
# Anything beyond that has to come from a padding column.
ROW_OVERHEAD = 31.0
INT_WIDTH = 4.16


def _pad_len(plan, t, ncols):
    """How many filler characters to add so a row reaches its target width."""
    target = getattr(plan, "widths", {}).get(t)
    if not target:
        return 0
    have = ROW_OVERHEAD + INT_WIDTH * ncols + 1   # +1 varlena header
    return max(0, int(round(target - have)))


def _filter_cols(plan, t):
    """One plain int column per filter on this table, holding 1 for the rows
    the filter keeps. Filtering on this instead of on `id` is deliberate: `id`
    is the primary key, so an id-range predicate gets served by the PK index
    and turns what was a sequential scan in the original plan into an index
    scan. A column with no index forces the full read the original did."""
    return [(f"f_{nid}", r["lo"], r["hi"])
            for nid, r in plan.ranges.items() if r["table"] == t]


def _col_names(plan, t):
    cols = (["id"]
            + [j.fk_col for j in plan.joins.values() if j.fk_table == t]
            + [g.col for g in plan.groups.values() if g.table == t]
            + [c for c, _, _ in _filter_cols(plan, t)])
    if _pad_len(plan, t, len(cols)):
        cols.append("pad")
    return cols


def ddl(plan) -> str:
    parts = []
    for t in sorted(plan.tables):
        # PRIMARY KEY on id, matching the original schema: every join target is
        # a pk, so the index is what lets the planner do index lookups on the
        # join side. Filters live on their own unindexed f_ columns, so the
        # index cannot hijack them -- a filtered scan still goes sequential.
        names = _col_names(plan, t)
        cols = ["id INTEGER PRIMARY KEY"]
        cols += [("pad TEXT" if c == "pad" else f"{c} INTEGER")
                 for c in names[1:]]
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
    flts = _filter_cols(plan, t)
    npad = _pad_len(plan, t, len(_col_names(plan, t)))
    for i in range(size):
        row = [i]
        for d in fks.values():
            # -1 rather than NULL for rows that take no part in this join: no
            # row has id -1 so it still matches nothing, but every row now
            # occupies the same bytes, which keeps the width predictable.
            row.append(d.get(i, -1))
        for _, spec in grps:
            if isinstance(spec, dict):
                row.append(spec.get(i))
            else:
                lo, hi, k = spec
                row.append((i - lo) % k if lo <= i < hi else None)
        for _, lo, hi in flts:
            row.append(1 if lo <= i < hi else 0)
        if npad:
            # varied, not a repeated character -- repeated filler compresses
            # away and the row stops occupying the bytes we are paying for.
            s = ""
            v = i
            while len(s) < npad:
                v = (v * 2654435761 + 12345) & 0xFFFFFFFF
                s += "%08x" % v
            row.append(s[:npad])
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
        cols = ",".join(_col_names(plan, t))
        copies.append(f"\\copy {name} ({cols}) FROM '{csv}' "
                      f"WITH (FORMAT csv, NULL '')")
    load_sql = outdir / "load.sql"
    load_sql.write_text("\n".join(copies) + "\n")
    return load_sql
