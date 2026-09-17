# Session summary: plan-driven synthetic database generation

## 0. Starting point

Investigated **Touchstone** (SIGMOD/ATC'18 query-aware data generator) as a candidate tool for the thesis, by extracting and reading its actual source (not just the README).

**What Touchstone takes as input:**
- `touchstone.conf` — cluster/runtime config (controller & data-gen IPs/ports/threads, tuning knobs). Deploy plumbing, not data.
- **Schema file** — `T[table; size; col,type...; P(pk); F(fk, ref)]` + optional `D[table.col; nullratio; stats...]` per-column stats.
- **Cardinality constraints file** — per-table constraint chain of filter/PK/FK nodes, each carrying a target selectivity/join-rate.
- Non-equi join constraints file (referenced in the format).

**What can be removed, verified against the actual code:**
- `D[...]` lines — fully optional per column; missing entries fall back to hardcoded defaults (`SchemaReader.java` → `newTouchstoneDataType(type, null)`).
- Non-equi join constraints file — **dead code**. `RunController.java` has the reader call commented out and passes `null` unconditionally.
- Mathematica/JLink path — only touched for filter expressions spanning >1 attribute (`ComputingThreadPool.subSolve2`); single-attribute filters + equi-joins never invoke it.
- Per-table constraint chains — optional per table (`Preprocessor.getPartialOrder()` derives generation order from the FK graph alone); a table can be a bare `[name];` with no cardinality shaping.
- Cluster config collapses to one controller + one data-gen node for local runs.

**Hard mechanism, worth keeping:** the `num1`/`num2` power-of-2 tag encoding on PK/FK nodes (`ConstraintChainsReader.java`) — this is how Touchstone lets a base table simultaneously satisfy multiple constraint chains (multiple queries) without the eligible-row sets colliding.

**Structural ceiling, confirmed via code:** Touchstone's node types only cover scan → filter → equi-join (inner/left-outer). No node type exists for GROUP BY/aggregation cardinality, sort, LIMIT, semi-/anti-join, or true non-equi joins. If a target query set includes these, no input configuration reproduces them — it's a tool limitation, not a config gap.

Also verified the exact cardinality-propagation mechanism in `QueryInstantiator.java`: each chain node's `probability` = **output_rows / input_rows_at_that_point**, propagated sequentially (`inputDataSize *= node.getProbability()`). This became the basis for everything after.

## 1. Reframing the actual goal

The real objective isn't reproducing Touchstone's realistic-data behavior — it's: **given a set of queries and their target logical plans (operator shape + per-operator input/output row counts), generate a database + queries that reproduce those plans**, with **no requirement that the underlying data, schema, or column semantics resemble anything real.**

This reframing simplifies the problem a lot: since fidelity to real data doesn't matter, most of Touchstone's machinery (real-looking value distributions, Mathematica-based range solving for complex predicates) becomes unnecessary. What's left is pure row-bookkeeping:

- **Filter**: carve out an ID range of the target size.
- **Join**: point a foreign key at a specific target ID range instead of a random one.
- **GROUP BY**: assign a synthetic grouping value taking exactly *k* distinct values.
- **Semi-/anti-join, non-equi join**: same eligibility-flag mechanism as an equi-join — since real predicate semantics don't matter, only the count does.
- **Sort / LIMIT / aggregate-without-GROUP-BY**: cardinality no-ops, trivial.

This is strictly more general than Touchstone's node vocabulary (covers GROUP BY, semi/anti-join, "non-equi" joins) while needing none of Touchstone's real-data machinery.

**The one hard part carried over from Touchstone:** when two queries share a base table, a row's eligibility for query A's constraint and query B's constraint have to be resolved *jointly*, not independently — otherwise they collide. This is the multi-query row-tagging problem, and it recurs at every scale tested below.

## 2. Experiment 1 — 3 queries, 2 tables (`customer`, `orders`)

Built a minimal generator (`generate.py`) from a plan spec with no schema file, no PK/FK declarations, no column names — just:
```python
TABLES = {"customer": 100_000, "orders": 500_000}
QUERIES = {
  "Q1": {"plan": [filter(customer, tag=A, out=30000), filter(orders, tag=X, out=100000),
                   join(A, X, out=8000)]},
  "Q2": {"plan": [filter(customer, tag=B, out=20000), filter(orders, tag=X, out=100000),  # tag shared with Q1
                   join(B, X, out=5000)]},
  "Q3": {"plan": [filter(orders, tag=Y, out=150000)]},
}
```
**Mechanism:** derive per-table filter partitions from all plans, carve each table's ID space into contiguous ranges per tag (this stands in for Touchstone's bitmask, exploiting that filter tags on one table are mutually exclusive categories). Within a shared partition (`orders[X]`, used by both Q1 and Q2's joins), sub-slice it further: first 8,000 IDs get `cid` drawn from customer pool A, next 5,000 from pool B, remainder from neither pool (explicitly excluded, to prevent accidental extra matches).

**Verified:** loaded into DuckDB, ran the real SQL (`WHERE`, `JOIN`, `COUNT(*)`), all 7 target numbers matched exactly, including the two queries sharing the same `orders.status='ord_pool_X'` filter but needing different join outcomes from it.

**Bug caught during this experiment:** an early version let unconstrained "filler" rows draw a random FK from anywhere, which could accidentally land inside a range that *was* being tracked elsewhere, silently inflating a join count. Fix: filler rows must be excluded from the entire constrained ID region, not just the exact sub-range they were nominally not part of.

## 3. Side investigations: columns, bytes, and compression

**Columns as a cleaner tagging mechanism.** A columnar store lets you add an explicit bitmask/tag column per row instead of relying on ID-range slicing — this is literally Touchstone's `num1`/`num2` encoding made explicit, and generalizes better to many overlapping constraints (each becomes a bit). The join side still needs a real FK value pointing at a real key in the right pool — tag columns fix the filter side, not the join side.

**Bytes vs. rows.** Since bytes-per-node = rows × width, and width is nearly free (filler columns), tested whether row-count fidelity could be swapped for cheaper generation: built two versions of a table hitting the same **800,000 bytes** — one with 8,000 rows × 100-byte rows, one with 80 rows × 10,000-byte rows. **Confirmed identical logical byte totals.** Caveats found:
- Row-level correctness (which specific rows match) is still mandatory at whichever row count you pick — bytes-fabrication doesn't remove that work, only lets you do it at smaller scale.
- A cost-based optimizer's plan choice is driven by cardinality estimates, not byte totals — shrinking row count to save generation cost can make the optimizer pick a genuinely different plan than the one being reproduced.
- **Compression breaks the shortcut**: persisted both versions as Parquet — the 8,000-row version was 33.5KB on disk, the 80-row version (using `repeat('x', n)` filler) was only 2.4KB — a 14x gap, because repeated-character filler compresses away almost entirely. Logical byte-count matching only survives real storage/network measurement if the filler has genuine entropy, not a repeated pattern.

## 4. Literature check

Searched and confirmed this exact problem — query + per-operator cardinality constraints in, database out — is an established research line, not a novel formulation:

- **QAGen** (Binnig, Kossmann, Lo, Özsu — SIGMOD 2007) — the closest prior work. Single query, handles joins + filters + **GROUP BY (controls output group count) + HAVING**, by propagating constraints symbolically back through whichever base tables the aggregation/filter columns come from. This is the paper whose problem statement matches this thesis's goal most closely, just scoped to one query at a time.
- **MyBenchmark** (VLDB) — extends QAGen to a **set** of queries sharing tables, generating one database approximately satisfying cardinality constraints across the whole workload — i.e., exactly the multi-query shared-table problem hit in every experiment here.
- **HYDRA** — declarative database *summary* used to dynamically generate data at query-execution time, rather than materializing one fixed database upfront.
- An IBM-assigned patent generalizes the "overlapping constraints" problem properly: builds a maximum-entropy joint distribution over a multi-table workload's annotated subplans, solved via iterative proportional fitting (IPF) — the rigorous version of the one-pinned-cell contingency-table trick used by hand in Experiment 2 below.
- "*Understanding Queries by Conditional Instances*" (2022) has a related-work section mapping this whole lineage.

## 5. Experiment 2 — 5 queries, 3 tables, deliberately crossing constraints

Designed specifically to break the "disjoint partition" shortcut from Experiment 1. Tables: `customer` (100k), `orders` (500k), `lineitem` (1M). The `orders[status='O']` filter node (100,000 rows) is reused by **four** different downstream consumers across the five queries, and two of those consumers cross on two independent dimensions simultaneously (which customer-segment pool a row's `cid` points to, and whether it has a matching AIR-shipmode `lineitem` child) — a genuine 2D contingency-table problem, not a partition problem.

### 5a. Pure-DAG input format (no tags)

After first specifying this with explicit string "tags" linking shared nodes across queries, the requirement was tightened: **input must be only `operator name, input rows, output rows`, plus a structural join graph** — no tag strings, no smuggled-in linking metadata. Since node reuse (the same subexpression having multiple parents) can't be expressed in a tree, this forced the representation to be a **DAG**, with reuse expressed purely through a node having multiple parents — nothing added to the field list:

```
n1=scan(customer,100000)  n2=scan(orders,500000)  n3=scan(lineitem,1000000)
n4=filter(n1) out=30000        # Q1 customer filter
n5=filter(n2) out=100000       # reused by n6, n8, n12, n14  <-- the stress node
n7=filter(n1) out=20000        # Q2 customer filter
n9=filter(n2) out=150000       # Q3, independent
n11=filter(n3) out=200000      # reused by n12, n15
n13=filter(n1) out=12000       # Q5 customer filter

n6=join(n4,n5)  out=8000   === Q1 ===
n8=join(n7,n5)  out=5000   === Q2 ===
n10=groupby(n9,5)          === Q3 ===
n12=join(n5,n11) out=20000 === Q4 ===
n14=join(n13,n5) out=3000
n15=join(n14,n11) out=900
n16=groupby(n15,3)         === Q5 ===
```

### 5b. Solving it (`dag_generate.py`)

- `customer` (100k): disjoint contiguous ID ranges per segment pool — segA=30k(n4), segB=20k(n7), segC=12k(n13), filler.
- `orders` (500k): `status='O'` block (100k, ids 0–99999) sub-partitioned by **customer-pool** dimension (blk_A=8000→n6, blk_B=5000→n8, blk_C=3000→n14, filler=84000) — mutually exclusive, easy. Then a **second, crossing dimension** on the same block: which rows also need an AIR-shipmode `lineitem` child (`S_air`, 20,000 total, n12's marginal). The one cell the query set actually constrains is `|blk_C ∩ S_air| = 900` (n15's requirement) — fixed that cell first (900 of blk_C's 3,000), parked the remaining 19,100 of the `S_air` quota in the otherwise-unconstrained filler block, since nothing constrained segA/segB's intersection with `S_air`.
- `lineitem` (1M): first 20,000 AIR rows 1:1-matched to the resolved `S_air` order IDs (sorted, so indices 0–899 land on blk_C's slice, giving `n16`'s grouping target for free); remaining 180,000 AIR filler rows deliberately excluded from the **entire** `status='O'` ID range (not just `S_air` specifically) — same class of bug as Experiment 1, caught the same way.

**Verified:** all 16 nodes (every filter, every join, every intermediate stage, both GROUP BY root group-counts) matched exactly against real DuckDB SQL execution.

### 5c. Checking real EXPLAIN ANALYZE plans, not just counts

Ran `EXPLAIN ANALYZE` for the real Q1–Q5 SQL text against the generated DuckDB database. Every operator's *actual rows* matched the DAG exactly, and DuckDB's own cost-based optimizer — unforced — chose join order and parallelism on its own. Notable finding: DuckDB **injected dynamic range filters** (e.g. `custkey BETWEEN 0 AND 29999`) into downstream scans, exploiting the artificial ID-contiguity from the generation method — a real optimizer behavior, but a reminder that the contiguous-ID shortcut leaves a statistical fingerprint a real dataset wouldn't have.

### 5d. Re-verifying on a row store (Postgres)

To rule out DuckDB's columnar block-skipping/dynamic-filter pushdown as a confound, installed PostgreSQL 16 in the sandbox, reloaded the identical generated data, ran the same 5 queries with `EXPLAIN ANALYZE`. Every plan showed genuine `Seq Scan` + `Rows Removed by Filter: N` (proof every row was actually evaluated, no skipping), used a completely different cost model (its own parallel-worker decisions, different from DuckDB's), and **every operator cardinality still landed exactly on target.** Confirms the row-level correctness holds under brute-force full-scan execution, not just under an engine that could theoretically exploit shortcuts.

## 6. Does matching rows (and bytes) automatically match plan shape?

**Initial claim (correct in general, but tested with the wrong example the first time):** no — a cost-based optimizer picks join order/algorithm from its **pre-execution estimate** (derived from column statistics: histograms, n-distinct, correlation), never from the *true* post-execution row count. Two databases can have identical true node-level cardinalities and still get different physical plans if their statistics differ.

**First attempted demonstration (flawed — this was the wrong test):** invented an artificial "given" topology (`customer⋈orders` fully, unfiltered, before a highly-selective region filter) that no real cost-based optimizer would ever choose for that data, and showed Postgres's default optimizer overrode it, requiring `SET join_collapse_limit=1` + explicit nested-join syntax to force. This proved only that *inventing* a bad topology needs forcing — not the general claim.

**Corrected experiment, addressing the actual question** ("if the topology/cardinalities come from a plan Postgres *itself* produced, does regenerating data to match reproduce the same plan, unforced?"):
1. Generated a **naturally random** (non-engineered) baseline dataset (`region`/`customer`/`orders`, random FK assignment, no ID tricks).
2. Ran the query unforced, captured Postgres's **own** chosen plan: region-filter first (out 5,009), then join up to orders (final 50,065).
3. Built a **separate, independently-generated** synthetic database (different seed, ID-block engineering) targeting exactly those captured numbers (5,009 and 50,065).
4. Ran the same query **unforced** on the fresh synthetic database.
5. **Result: identical plan** — same join order, same operators, same parallelism, same exact cardinalities, with no hints or forcing.

**Conclusion:** confirmed the claim. A real captured plan is already the *output* of a cost model; a synthetic database whose true cardinalities agree with it gives that same cost model the same story it believed originally, so there's nothing to fight. The earlier "you have to force it" finding was a fact about *adversarial/invented* topologies specifically, not about topologies that are themselves legitimate cost-model outputs.

**Standing caveat, not yet stress-tested:** near cost ties between two candidate plans. If a real target plan's chosen order was only marginally cheaper than the runner-up, small statistical differences between synthetic and real data (not row counts — the shape ANALYZE sees: n-distinct, histogram buckets, cross-column correlation) could flip which plan the optimizer picks. Not encountered in either test case run so far because both had one clearly-cheaper option; an intentionally-tied-cost case is the logical next stress test.

## 7. Query generation (SQL text), not just the database

Clarified division of labor for producing the actual SQL alongside the database:

- **DAG → SQL structural translation is mechanical**, not an LLM task: `scan`→`FROM`, `filter`→`WHERE`, `join`→`JOIN...ON`, `groupby`→`GROUP BY`. Deterministic, code-driven, easily verified.
- **LLM's actual role is cosmetic realism**: choosing plausible column names, predicate literals/shapes (`=` vs `BETWEEN` vs `IN`), and phrasing variety across the workload — none of which affects plan structure, all of which affects whether the workload *looks* like a real captured one instead of an obviously templated one.
- **Real coupling requirement**: the LLM's column/schema choices and the data generator's tagging must be decided once, upstream, and shared — can't be generated independently and hoped to align.
- **The verification step doesn't go away**: syntactically-DAG-shaped SQL still has to be checked against a real `EXPLAIN ANALYZE` on the generated database, exactly as in Section 5c/5d — nothing about LLM-generated surface realism guarantees the target engine executes the intended shape.

## 8. Project framing produced this session

Drafted, then the user finalized, this description for the MTech project (CS25MTECH14006, Jaideep Singh Kaler — *LLM-Driven Database and Query Generation Pipeline*):

> Real database traces and queries are hard to get because of privacy and access limits. To work around this, we want to generate synthetic benchmarks that closely mimic how real databases and queries behave. Existing workload generators in this space (built on things like Redset and Snowset) can't achieve plan-level matching, because the fields those datasets carry — table sizes, row widths, per-table selectivity — say nothing about how a query's operators are actually connected or how many rows flow through each one, which is what a query plan actually is.
>
> Our approach takes a query plan directly as input: its operators (scan, filter, join, group-by), how they're connected, and how many rows go in and out of each one — the same information already visible in a captured EXPLAIN ANALYZE, and derivable from Redset/Snowset once augmented with per-operator cardinality rather than just per-table stats. An LLM generates the actual SQL text and schema for that structure, and a separate data generator builds a database engineered so its real row counts satisfy every operator's target simultaneously, even when the same table is shared across several queries. Success is measured by running the generated query on the generated database on a real engine and checking whether the resulting plan — join order, operators, and per-operator row counts — matches the original ground-truth plan.

## 9. Artifacts produced

All in `/home/claude/plangen/` (sandbox, not yet exported except this file):
- `generate.py` — Experiment 1 generator (3 queries, 2 tables).
- `bytes_test.py`, `bytes_test2.py`, `compress_test.py` — rows-vs-width and compression tests.
- `dag_generate.py` — Experiment 2 generator + verification (5 queries, 3 tables, full DAG) + `EXPLAIN ANALYZE` capture.
- `pg_generate.py` — Postgres reload of Experiment 2's data for row-store verification.
- `order_test_setup.py` — the flawed adversarial-topology test (kept for record; superseded by the corrected test below).
- `baseline_setup.py` + `synth_setup.py` — the corrected topology-reproduction test (naturally-random baseline → captured plan → independently-generated synthetic match → confirmed identical unforced plan).
- `dag.mermaid` — rendered DAG diagram of the 5-query structure (`n1`–`n16`), with shared/reused nodes and query roots color-coded.

## 10. Open threads for next session

- Stress-test the near-cost-tie caveat from Section 6: construct a case where two join orders are genuinely close in estimated cost and check how much statistical divergence (not row-count divergence) it takes to flip the optimizer's choice.
- Extend the DAG formalism to semi-join/anti-join and non-equi join cases explicitly (mechanism sketched in Section 1, not yet built/verified).
- Build the actual DAG→SQL structural compiler (Section 7's mechanical half) as real code, not description.
- Decide and implement the LLM-facing interface for the cosmetic-realism half (column naming, predicate style) and the upstream schema-decision coupling it requires.
