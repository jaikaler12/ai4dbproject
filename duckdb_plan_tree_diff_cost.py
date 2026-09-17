#!/usr/bin/env python3
"""
duckdb_plan_tree_diff.py
========================

Compare the query-plan trees of two folders of **DuckDB EXPLAIN ANALYZE** plans
and report, for every matching query, a plan-tree difference score rho.

    rho = 1 - 2*M / (|T1| + |T2|)

where |Ti| is the number of nodes in plan i's operator tree and M is the matched
mass.  rho = 0 => the plans are identical; rho -> 1 => completely different.

COST WEIGHTING (default on)
    Plain Picasso counts each matched node as exactly 1, so two structurally
    identical plans always score rho = 0 no matter how much data flowed through
    them.  Here, a matched PAIR instead contributes  (1 - cost_diff)  to M, where
    cost_diff in [0,1] compares the BYTES FED INTO the two nodes -- and the bytes
    fed into a node are the result_set_size of its child(ren):

        cost_diff(a, b) = |in(a) - in(b)| / max(in(a), in(b))

      * unary node  -> in = the single child's result_set_size
      * binary node -> cost_diff = MEAN of the left-child and right-child scores
      * leaf / scan -> no operator input (its child is the synthetic relation
                       leaf) -> cost_diff = 0, so it stays a full structural match

    A difference in what an operator outputs thus shows up once, at the operator
    that consumes it.  When inputs match (or the metric is 0/absent) cost_diff = 0
    and rho collapses back to the structural value.  The field is --cost-metric
    (default 'result_set_size'; e.g. 'operator_cardinality' for output rows).
    Turn weighting off entirely with --no-cost.

The MATCHING itself is the EXACT tree-difference algorithm from Picasso
(iisc.dsl.picasso.server.sampling.GSPQO -> getMeanTreeDiff / treeDiff /
getBestMatching / setEditNodes, with iisc.dsl.picasso.common.TreeUtil and
PicassoConstants).  The Java matcher is ported here 1:1.  Only the *parser* is
new: instead of Picasso's PostgreSQL EXPLAIN-text reader, this file builds the
same style of operator tree from DuckDB's EXPLAIN ANALYZE JSON.

Folder convention (exactly like the folders provided):
    folderA/query_1.json   folderB/query_1.json      -> compared
    folderA/query_2.json   folderB/query_2.json      -> compared
    ... query_N is compared with query_N ...

Base-relation normalisation: the algorithm's leaf/join matching keys off base
relation names, and DuckDB names the augmented tables lineitem_0, orders_0, ...
By request, `table` and `table_0` are treated as the SAME base relation, so a
trailing `_<digits>` suffix is stripped from every table name before matching
(lineitem_0 -> lineitem, orders_0 -> orders, ...).  Toggle with --no-normalize.

USAGE
    python duckdb_plan_tree_diff.py FOLDER_A FOLDER_B
    python duckdb_plan_tree_diff.py FOLDER_A FOLDER_B --cost-metric operator_cardinality
    python duckdb_plan_tree_diff.py FOLDER_A FOLDER_B --no-cost          # structural only
    python duckdb_plan_tree_diff.py FOLDER_A FOLDER_B --csv out.csv
    python duckdb_plan_tree_diff.py FOLDER_A FOLDER_B --show 5           # annotated trees + costs
    python duckdb_plan_tree_diff.py --selftest FOLDER                    # sanity: folder vs itself -> all 0

Only depends on the Python standard library.
"""

import os
import re
import sys
import json
import glob
import argparse

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


# ════════════════════════════════════════════════════════════════════════════
#  PicassoConstants  (iisc.dsl.picasso.common.PicassoConstants)
#  Verbatim integer values from the Picasso source.
# ════════════════════════════════════════════════════════════════════════════
T_IS_SIMILAR         = 0
T_SUB_OP_DIF         = 1
T_LEFT_EQ_RIGHT      = 3
T_LEFT_SIMILAR       = 4
T_RIGHT_SIMILAR      = 5
T_LR_SIMILAR         = 6
T_RL_SIMILAR         = 7
T_LEFT_EQ            = 6
T_RIGHT_EQ           = 7
T_NO_CHILD_SIMILAR   = 8
T_NP_SIMILAR         = 9
T_NP_LEFT_EQ_RIGHT   = 10
T_NP_LEFT_SIMILAR    = 11
T_NP_RIGHT_SIMILAR   = 12
T_NP_LR_SIMILAR      = 13
T_NP_RL_SIMILAR      = 14
T_NP_LEFT_EQ         = 13
T_NP_RIGHT_EQ        = 14
T_NP_NOT_SIMILAR     = 15
T_NO_DIFF_DONE       = 16          # initial similarity of every node
SO_BASE              = 20          # SUBOP_OFFSET
T_SO_LEFT_EQ_RIGHT   = SO_BASE + T_LEFT_EQ_RIGHT
T_SO_LEFT_SIMILAR    = SO_BASE + T_LEFT_SIMILAR
T_SO_RIGHT_SIMILAR   = SO_BASE + T_RIGHT_SIMILAR
T_SO_NO_CHILD_SIMILAR= SO_BASE + T_NO_CHILD_SIMILAR
T_SO_LR_SIMILAR      = SO_BASE + T_LR_SIMILAR
T_SO_RL_SIMILAR      = SO_BASE + T_RL_SIMILAR

OPERATOR_LEVEL       = 0
SUB_OPERATOR_LEVEL   = 1
SUBOP_OFFSET         = SO_BASE


# ════════════════════════════════════════════════════════════════════════════
#  TreeNode  (iisc.dsl.picasso.common.ds.TreeNode) — only the fields the
#  matcher touches.
# ════════════════════════════════════════════════════════════════════════════
class TNode:
    __slots__ = ("name", "children", "parent", "sim", "match_num", "attrs", "cost")

    def __init__(self, name, attrs=None):
        self.name = name
        self.children = []
        self.parent = None
        self.sim = T_NO_DIFF_DONE
        self.match_num = 0
        self.attrs = attrs if attrs is not None else {}
        # raw per-node cost metric (e.g. total_bytes_read); 0 when unavailable
        self.cost = 0.0

    def __repr__(self):
        return self.name


# ════════════════════════════════════════════════════════════════════════════
#  TreeUtil  (iisc.dsl.picasso.common.TreeUtil)
# ════════════════════════════════════════════════════════════════════════════
def get_left_tree(t):
    if not t.children:
        return None
    return t.children[0]


def get_right_tree(t):
    if t is None or len(t.children) < 2:
        return None
    return t.children[1]


def _stack_collect(root, keep):
    """Java Stack DFS (push root, pop top, push children left->right).  Returns
    the list of nodes for which keep(node) is True, in Picasso's traversal
    order (so duplicate-leaf tie-breaks match)."""
    out = []
    stack = [root]
    while stack:
        curr = stack.pop()
        if keep(curr):
            out.append(curr)
        for ch in curr.children:          # push in order 0..n (pop reverses)
            stack.append(ch)
    return out


def get_leaves(root):
    # leaves == nodes with no children (relation-name / index-name leaves)
    return _stack_collect(root, lambda n: len(n.children) == 0)


def get_join_nodes(root):
    # Picasso's join test is "node touches >= 2 relations" (getNodeCard>=2).
    # For a physical plan that is exactly "an internal node with >= 2 children"
    # (the commented-out original test in TreeUtil.getJoinNodes) — in DuckDB the
    # only such operators are the join / set / union operators.  Scans have a
    # single child (their relation leaf) and never qualify.
    return _stack_collect(root, lambda n: len(n.children) >= 2)


def get_relations(root):
    # names of all leaf descendants (base relations under a subtree)
    if root is None:
        return []
    return [n.name for n in _stack_collect(root, lambda n: len(n.children) == 0)]


def get_subtree(root, node):
    if root is node:
        return root
    for ch in root.children:
        r = get_subtree(ch, node)
        if r is not None:
            return r
    return None


def is_equals(t1, t2, diff_type):
    """TreeUtil.isEquals: operator-level == name equality; sub-operator-level ==
    name AND attribute-map equality.  (Nodes here carry no attributes, so the
    two levels coincide, exactly as in the verified Picasso port.)"""
    if t1.name != t2.name:
        return False
    if diff_type != SUB_OPERATOR_LEVEL:
        return True
    a1, a2 = t1.attrs, t2.attrs
    if not a1 and not a2:
        return True
    if len(a1) != len(a2):
        return False
    for k, v in a1.items():
        if a2.get(k) != v:
            return False
    return True


def are_trees_equal(t1, t2):
    if t1 is None and t2 is None:
        return True
    if t1 is None or t2 is None:
        return False
    if is_equals(t1, t2, OPERATOR_LEVEL):
        return (are_trees_equal(get_left_tree(t1), get_left_tree(t2)) and
                are_trees_equal(get_right_tree(t1), get_right_tree(t2)))
    return False


# ════════════════════════════════════════════════════════════════════════════
#  The matcher  (GSPQO tree-difference methods).  One instance per plan pair so
#  the mutable `matchNum` counter is isolated, exactly like a fresh treeDiff().
# ════════════════════════════════════════════════════════════════════════════
class Matcher:
    def __init__(self):
        self.match_num = 1

    # ---- small numeric helpers (GSPQO.getMin / getMinNum) ------------------
    @staticmethod
    def get_min(v1, v2, v3):
        m = v1
        if m > v2:
            m = v2
        if m > v3:
            m = v3
        return m

    @staticmethod
    def get_min_num(val1, val2, val3, orig):
        ret = -1
        if orig == val1:
            ret = 0
        if orig == val1 + 1:
            ret = 1
        if orig == (val2 + 2):
            if ret != -1:
                if val2 < val1:
                    ret = 2
            else:
                ret = 2
        if orig == (val3 + 2):
            if ret != -1:
                if val3 < val1:
                    ret = 3
            else:
                ret = 3
        return ret

    def set_match_number(self, n1, n2):
        n1.match_num = self.match_num
        n2.match_num = self.match_num
        self.match_num += 1

    # ---- singleton-branch length / edit distance --------------------------
    _SCAN_NAMES = ("FETCH", "Seq Scan", "TABLE ACCESS", "TABLE_SCAN", "SEQ_SCAN")

    def get_singleton_tree_length(self, n1, length):
        p1 = n1.parent
        if p1 is not None and p1.name in self._SCAN_NAMES:
            return self.get_singleton_tree_length(p1, length + 1)
        if p1 is None or get_right_tree(p1) is not None:
            return length
        return self.get_singleton_tree_length(p1, length + 1)

    def get_edit_distance(self, n1, n2):
        len1 = self.get_singleton_tree_length(n1, 0)
        len2 = self.get_singleton_tree_length(n2, 0)
        if len1 == 0:
            return len2
        if len2 == 0:
            return len1
        ed = [[0] * (len2 + 1) for _ in range(len1 + 1)]
        for i in range(len1 + 1):
            ed[i][0] = i * 2
        for j in range(len2 + 1):
            ed[0][j] = j * 2
        t1 = n1
        for i in range(1, len1 + 1):
            t1 = t1.parent
            t2 = n2
            for j in range(1, len2 + 1):
                t2 = t2.parent
                val = 0 if is_equals(t1, t2, SUB_OPERATOR_LEVEL) else 1
                ed[i][j] = self.get_min(ed[i - 1][j] + 2,
                                        ed[i][j - 1] + 2,
                                        ed[i - 1][j - 1] + val)
        return ed[len1][len2]

    def set_edit_nodes(self, n1, n2):
        """GSPQO.setEditNodes — align the two singleton branches above the
        matched pair (n1,n2) and mark aligned intermediate nodes T_IS_SIMILAR."""
        len1 = self.get_singleton_tree_length(n1, 0)
        len2 = self.get_singleton_tree_length(n2, 0)

        if len1 == 0 or len2 == 0:
            t1 = n1
            for _ in range(len1):
                t1 = t1.parent
                t1.sim = T_NO_DIFF_DONE
            t2 = n2
            for _ in range(len2):
                t2 = t2.parent
                t2.sim = T_NO_DIFF_DONE
            return

        ed = [[0] * (len2 + 1) for _ in range(len1 + 1)]
        nodes1 = [None] * (len1 + 1)
        nodes2 = [None] * (len2 + 1)

        t1 = n1
        for i in range(len1 + 1):
            ed[i][0] = i * 2
            nodes1[i] = t1
            t1 = t1.parent
        t2 = n2
        for j in range(len2 + 1):
            ed[0][j] = j * 2
            nodes2[j] = t2
            t2 = t2.parent

        t1 = n1
        for i in range(1, len1 + 1):
            t1 = t1.parent
            t2 = n2
            for j in range(1, len2 + 1):
                t2 = t2.parent
                val = 0 if is_equals(t1, t2, SUB_OPERATOR_LEVEL) else 1
                ed[i][j] = self.get_min(ed[i - 1][j] + 2,
                                        ed[i][j - 1] + 2,
                                        ed[i - 1][j - 1] + val)

        m, n = len1, len2
        while m != -1 or n != -1:
            cur = ed[m][n]
            if cur == 0:                        # everything above here matches
                for i in range(1, m + 1):
                    nodes1[i].sim = T_IS_SIMILAR
                    nodes1[i].match_num = self.match_num
                    self.match_num += 1
                self.match_num -= m
                for j in range(1, n + 1):
                    nodes2[j].sim = T_IS_SIMILAR
                    nodes2[j].match_num = self.match_num
                    self.match_num += 1
                break

            val1 = val2 = val3 = -1
            if m != 0 and n != 0:
                val1 = ed[m - 1][n - 1]
            if n != 0:
                val2 = ed[m][n - 1]
            if m != 0:
                val3 = ed[m - 1][n]

            num = self.get_min_num(val1, val2, val3, ed[m][n])
            if num == 0:
                if not is_equals(nodes1[m], nodes2[n], SUB_OPERATOR_LEVEL):
                    nodes1[m].sim = T_SUB_OP_DIF
                    nodes2[n].sim = T_SUB_OP_DIF
                else:
                    nodes1[m].sim = T_IS_SIMILAR
                    nodes2[n].sim = T_IS_SIMILAR
                nodes1[m].match_num = self.match_num
                nodes2[n].match_num = self.match_num
                self.match_num += 1
                m -= 1
                n -= 1
            elif num == 1:
                if nodes1[m].name == nodes2[n].name:
                    nodes1[m].sim = T_SUB_OP_DIF
                    nodes2[n].sim = T_SUB_OP_DIF
                    nodes1[m].match_num = self.match_num
                    nodes2[n].match_num = self.match_num
                    self.match_num += 1
                else:
                    nodes1[m].sim = T_NO_DIFF_DONE
                    nodes2[n].sim = T_NO_DIFF_DONE
                m -= 1
                n -= 1
            elif num == 2:
                nodes2[n].sim = T_NO_DIFF_DONE
                n -= 1
            elif num == 3:
                nodes1[m].sim = T_NO_DIFF_DONE
                m -= 1
            else:
                # Defensive: no valid backtrack direction (should not happen for
                # well-formed edit matrices); stop to avoid an infinite loop.
                break

    # ---- join similarity typing (truth table) -----------------------------
    def get_relation_match(self, node1, node2):
        rel1 = get_relations(node1)
        rel2 = get_relations(node2)
        if len(rel1) != len(rel2):
            return False
        done2 = [False] * len(rel2)
        is_matching = True
        for a in rel1:
            found = False
            for jj in range(len(rel2)):
                if a == rel2[jj] and not done2[jj]:
                    done2[jj] = True
                    found = True
                    break
            if not found:
                is_matching = False
                break
        return is_matching

    def get_similarity_type(self, n1, n2, relations):
        l1, l2 = get_left_tree(n1), get_left_tree(n2)
        r1, r2 = get_right_tree(n1), get_right_tree(n2)
        if relations:
            ll = self.get_relation_match(l1, l2)
            rr = self.get_relation_match(r1, r2)
            lr = self.get_relation_match(l1, r2)
            rl = self.get_relation_match(r1, l2)
        else:
            ll = are_trees_equal(l1, l2)
            rr = are_trees_equal(r1, r2)
            lr = are_trees_equal(l1, r2)
            rl = are_trees_equal(r1, l2)

        subop = 0
        if is_equals(n1, n2, OPERATOR_LEVEL):
            if not is_equals(n1, n2, SUB_OPERATOR_LEVEL):
                subop = SUBOP_OFFSET
            if ll is False:
                if lr is True:
                    if rl is True:
                        return T_LEFT_EQ_RIGHT + subop
                    else:
                        return T_LR_SIMILAR + subop
                elif rl is True:
                    return T_RL_SIMILAR + subop
                if rr is False:
                    return T_NO_CHILD_SIMILAR + subop
                else:
                    return T_RIGHT_SIMILAR + subop
            else:
                if rr is False:
                    return T_LEFT_SIMILAR + subop
                elif is_equals(n1, n2, SUB_OPERATOR_LEVEL) is False:
                    return T_SUB_OP_DIF
                else:
                    return T_IS_SIMILAR
        else:
            if ll is False:
                if lr is True:
                    if rl is True:
                        return T_NP_LEFT_EQ_RIGHT
                    else:
                        return T_NP_LR_SIMILAR
                elif rl is True:
                    return T_NP_RL_SIMILAR
                if rr is False:
                    return T_NP_NOT_SIMILAR
                else:
                    return T_NP_RIGHT_SIMILAR
            else:
                if rr is False:
                    return T_NP_LEFT_SIMILAR
                else:
                    return T_NP_SIMILAR

    def set_join_sim_type(self, node1, node2, sim_type):
        if sim_type == T_NP_LR_SIMILAR:
            node1.sim = T_NP_LEFT_EQ
            node2.sim = T_NP_RIGHT_EQ
        elif sim_type == T_NP_RL_SIMILAR:
            node1.sim = T_NP_RIGHT_EQ
            node2.sim = T_NP_LEFT_EQ
        elif sim_type == T_RL_SIMILAR:
            node1.sim = T_RIGHT_EQ
            node2.sim = T_LEFT_EQ
        elif sim_type == T_LR_SIMILAR:
            node1.sim = T_LEFT_EQ
            node2.sim = T_RIGHT_EQ
        elif sim_type == T_SO_RL_SIMILAR:
            node1.sim = T_SO_RIGHT_SIMILAR
            node2.sim = T_SO_LEFT_SIMILAR
        elif sim_type == T_SO_LR_SIMILAR:
            node1.sim = T_SO_LEFT_SIMILAR
            node2.sim = T_SO_RIGHT_SIMILAR
        else:
            node1.sim = sim_type
            node2.sim = sim_type
        node1.match_num = self.match_num
        node2.match_num = self.match_num
        self.match_num += 1

    # ---- join matching passes (setJoinSimilarity) -------------------------
    def get_exact_relation_match(self, joins1, joins2, idone, jdone):
        for i in range(len(joins1)):
            if idone[i]:
                continue
            node1 = joins1[i]
            for j in range(len(joins2)):
                if jdone[j]:
                    continue
                node2 = joins2[j]
                if self.get_relation_match(node1, node2) is False:
                    continue
                csim = self.get_similarity_type(node1, node2, True)
                self.set_join_sim_type(node1, node2, csim)
                jdone[j] = True
                idone[i] = True
                self.set_edit_nodes(node1, node2)
                break

    def get_count_relation_match(self, joins1, joins2, idone, jdone):
        for i in range(len(joins1)):
            if idone[i]:
                continue
            node1 = joins1[i]
            csim = T_NO_DIFF_DONE
            rel1 = get_relations(node1)
            cnode2 = None
            jindex = -1
            for j in range(len(joins2)):
                if jdone[j]:
                    continue
                node2 = joins2[j]
                rel2 = get_relations(node2)
                if len(rel1) != len(rel2):
                    continue
                sim = self.get_similarity_type(node1, node2, True)
                subop = 0
                if not is_equals(node1, node2, SUB_OPERATOR_LEVEL):
                    subop = SUBOP_OFFSET
                if sim < csim + subop:
                    csim = sim
                    cnode2 = node2
                    jindex = j
            if cnode2 is not None:
                self.set_join_sim_type(node1, cnode2, csim)
                jdone[jindex] = True
                idone[i] = True
                self.set_edit_nodes(node1, cnode2)

    def get_non_exact_join_match(self, joins1, joins2, idone, jdone):
        for i in range(len(joins1)):
            if not idone[i]:
                joins1[i].sim = T_NO_DIFF_DONE
        for j in range(len(joins2)):
            if not jdone[j]:
                joins2[j].sim = T_NO_DIFF_DONE

    def set_join_similarity(self, joins1, joins2, idone, jdone):
        self.get_exact_relation_match(joins1, joins2, idone, jdone)
        self.get_count_relation_match(joins1, joins2, idone, jdone)
        self.get_non_exact_join_match(joins1, joins2, idone, jdone)

    # ---- duplicate leaf matching (min edit distance) ----------------------
    def do_duplicate_leaf_match(self, matching, leaves1, leaves2, idone, jdone):
        for i in range(len(leaves1)):
            if idone[i]:
                continue
            jdup = []
            for j in range(len(leaves2)):
                if leaves1[i].name == leaves2[j].name and not jdone[j]:
                    jdup.append(j)
            if len(jdup) > 1:
                idup = []
                for j in range(i, len(leaves1)):
                    if leaves1[i].name == leaves1[j].name:
                        idup.append(j)
                icount = len(idup)
                jcount = len(jdup)
                min_ed = 100
                mi = mj = -1
                edist = [[0] * jcount for _ in range(icount)]
                for l in range(icount):
                    for j in range(jcount):
                        edist[l][j] = self.get_edit_distance(leaves1[idup[l]],
                                                             leaves2[jdup[j]])
                        if min_ed > edist[l][j]:
                            min_ed = edist[l][j]
                            mi, mj = l, j
                while icount != 0:
                    matching[leaves1[idup[mi]]] = leaves2[jdup[mj]]
                    self.set_match_number(leaves1[idup[mi]], leaves2[jdup[mj]])
                    jdone[jdup[mj]] = True
                    idone[idup[mi]] = True
                    edist[mi][mj] = icount + jcount
                    min_ed = icount + jcount
                    mi = mj = -1
                    for l in range(len(idup)):
                        for j in range(len(jdup)):
                            if (min_ed > edist[l][j]
                                    and not idone[idup[l]]
                                    and not jdone[jdup[j]]):
                                min_ed = edist[l][j]
                                mi, mj = l, j
                    if mi == -1 or mj == -1:
                        break
                    icount -= 1

    # ---- the top-level matcher (getBestMatching) --------------------------
    def get_best_matching(self, tree1, tree2, diff_type):
        matching = {}

        # 1) leaves (base relations / index names)
        leaves1 = get_leaves(tree1)
        leaves2 = get_leaves(tree2)
        jdone = [False] * len(leaves2)
        idone = [False] * len(leaves1)
        self.do_duplicate_leaf_match(matching, leaves1, leaves2, idone, jdone)
        for i in range(len(leaves1)):
            if idone[i]:
                continue
            for j in range(len(leaves2)):
                if jdone[j]:
                    continue
                if is_equals(leaves1[i], leaves2[j], diff_type):
                    matching[leaves1[i]] = leaves2[j]
                    self.set_match_number(leaves1[i], leaves2[j])
                    jdone[j] = True
                    break

        # 2) joins matched exactly (same op + same relation set on each side)
        joins1 = get_join_nodes(tree1)
        joins2 = get_join_nodes(tree2)
        jdone = [False] * len(joins2)
        idone = [False] * len(joins1)
        for i in range(len(joins1)):
            for j in range(len(joins2)):
                if jdone[j]:
                    continue
                if is_equals(joins1[i], joins2[j], diff_type) is False:
                    continue
                t1 = get_subtree(tree1, joins1[i])
                t2 = get_subtree(tree2, joins2[j])
                is_matching = True

                left1, left2 = get_left_tree(t1), get_left_tree(t2)
                rel1, rel2 = get_relations(left1), get_relations(left2)
                if len(rel1) != len(rel2):
                    is_matching = False
                ii = 0
                while is_matching and ii < len(rel1):
                    jj = 0
                    while jj < len(rel2):
                        if rel1[ii] == rel2[jj]:
                            break
                        jj += 1
                    if jj == len(rel2):
                        is_matching = False
                    ii += 1
                if is_matching is False:
                    continue

                right1, right2 = get_right_tree(t1), get_right_tree(t2)
                rel1, rel2 = get_relations(right1), get_relations(right2)
                if len(rel1) != len(rel2):
                    is_matching = False
                ii = 0
                while is_matching and ii < len(rel1):
                    jj = 0
                    while jj < len(rel2):
                        if rel1[ii] == rel2[jj]:
                            break
                        jj += 1
                    if jj == len(rel2):
                        is_matching = False
                    ii += 1

                if is_matching:
                    matching[joins1[i]] = joins2[j]
                    self.set_match_number(joins1[i], joins2[j])
                    jdone[j] = True
                    idone[i] = True
                    break

        # 3) the remaining joins (swapped / partial / count-only)
        self.set_join_similarity(joins1, joins2, idone, jdone)

        # 4) linear root
        if len(tree1.children) <= 1 and tree1 not in matching:
            matching[tree1] = tree2
            self.set_match_number(tree1, tree2)

        # 5) edit-distance the singleton branch above every matched pair
        for k in list(matching.keys()):
            self.set_edit_nodes(k, matching[k])

        return matching

    # ---- treeDiff wrapper -------------------------------------------------
    def set_similarity(self, matching):
        for k, v in matching.items():
            k.sim = T_IS_SIMILAR
            v.sim = T_IS_SIMILAR

    def count_similar(self, root):
        c = 1 if root.sim == T_IS_SIMILAR else 0
        for ch in root.children:
            c += self.count_similar(ch)
        return c

    def tree_diff(self, root1, root2):
        """One run of Picasso's treeDiff: returns M = #nodes in root1 marked
        T_IS_SIMILAR after matching."""
        matching = self.get_best_matching(root1, root2, SUB_OPERATOR_LEVEL)
        self.set_similarity(matching)
        return self.count_similar(root1)


# ════════════════════════════════════════════════════════════════════════════
#  Public comparison entry point
# ════════════════════════════════════════════════════════════════════════════
def size(root):
    return 1 + sum(size(ch) for ch in root.children)


# ── cost-weighted matching ──────────────────────────────────────────────────
# The plain Picasso score counts each matched node as exactly 1.  The
# cost-weighted variant counts a matched pair as (1 - cost_diff), where cost_diff
# in [0,1] compares the BYTES FED INTO the two nodes.  The bytes fed into a node
# are the result_set_size of its child(ren):
#
#     cost_diff(a, b) = |in(a) - in(b)| / max(in(a), in(b))
#
#   * unary node   -> in = the single child's result_set_size
#   * binary node  -> cost_diff = mean( score(left children), score(right children) )
#   * leaf / scan  -> no operator input (its only child is the synthetic relation
#                     leaf, whose result_set_size is 0) -> cost_diff = 0, full match
#
# A difference in what an operator outputs therefore shows up once, at the
# operator that consumes it.  When inputs are equal (or the metric is 0/absent)
# cost_diff = 0 and rho collapses back to the structural value.


def _byte_score(a, b):
    """Normalized cost difference in [0,1]: 0 when equal (or both 0)."""
    hi = a if a > b else b
    if hi <= 0:
        return 0.0
    return abs(a - b) / hi


def _input_cost_diff(n1, n2):
    """cost_diff in [0,1] from the bytes fed into the pair = children's
    result_set_size; mean of both sides for a binary (join) node."""
    c1, c2 = n1.children, n2.children
    if len(c1) >= 2 and len(c2) >= 2:                 # binary: mean of L and R
        left = _byte_score(c1[0].cost, c2[0].cost)
        right = _byte_score(c1[1].cost, c2[1].cost)
        return (left + right) / 2.0
    if c1 and c2:                                     # unary: single child
        return _byte_score(c1[0].cost, c2[0].cost)
    return 0.0                                        # leaf: no operator input


def _any_cost(root):
    stack = [root]
    while stack:
        n = stack.pop()
        if n.cost:
            return True
        stack.extend(n.children)
    return False


def _weighted_matched(root1, root2):
    """Sum of (1 - cost_diff) over every matched pair, matched by match_num.
    Returns (weighted_M, structural_M)."""
    partner = {}                                  # match_num -> tree2 node
    stack = [root2]
    while stack:
        n = stack.pop()
        if n.sim == T_IS_SIMILAR:
            partner[n.match_num] = n
        stack.extend(n.children)

    weighted = 0.0
    struct = 0
    stack = [root1]
    while stack:
        n = stack.pop()
        if n.sim == T_IS_SIMILAR:
            struct += 1
            p = partner.get(n.match_num)
            if p is None:                          # no twin found -> full match
                weighted += 1.0
            else:
                weighted += 1.0 - _input_cost_diff(n, p)
        stack.extend(n.children)
    return weighted, struct


def compare_trees(tree1, tree2, cost_weighted=True):
    """rho = 1 - 2M/(|T1|+|T2|) for two operator trees (Picasso GSPQO).

    With cost_weighted=True, M is the sum of per-pair match quality
    (1 - cost_diff) instead of a plain matched-node count, so structurally
    identical plans that scanned very different amounts of data score > 0.

    Leaves the per-node .sim / .match_num flags set on both trees so callers can
    render which nodes matched (T_IS_SIMILAR)."""
    s1 = size(tree1)
    s2 = size(tree2)
    Matcher().tree_diff(tree1, tree2)             # sets .sim + .match_num
    m_weighted, m_struct = _weighted_matched(tree1, tree2)
    m = m_weighted if cost_weighted else m_struct
    denom = s1 + s2
    rho = 1.0 - 2.0 * m / denom if denom else 0.0
    return {"rho": rho, "matched": m, "matched_struct": m_struct,
            "size1": s1, "size2": s2}


def ascii_tree(root, prefix="", last=True, show_cost=False):
    """Render a matched tree; [M] marks a node the matcher counted as similar.
    If show_cost, each node's own output size (result_set_size) is shown."""
    conn = "`- " if last else "|- "
    tag = "M" if root.sim == T_IS_SIMILAR else "."
    extra = "   (rss=%g)" % root.cost if show_cost else ""
    lines = ["%s%s[%s] %s%s" % (prefix, conn, tag, root.name, extra)]
    cp = prefix + ("   " if last else "|  ")
    for i, ch in enumerate(root.children):
        lines += ascii_tree(ch, cp, i == len(root.children) - 1, show_cost)
    return lines


# ════════════════════════════════════════════════════════════════════════════
#  DuckDB EXPLAIN ANALYZE JSON  ->  Picasso operator tree
# ════════════════════════════════════════════════════════════════════════════
_SUFFIX_RE = re.compile(r"_\d+$")     # strips a single trailing _<digits>

# DuckDB operator types that scan a base relation (become: scan-operator node
# + a relation-name leaf, mirroring Picasso's Seq-Scan handling).
_SCAN_TYPES = {"SEQ_SCAN", "TABLE_SCAN", "INDEX_SCAN", "COLUMN_DATA_SCAN",
               "PARQUET_SCAN", "READ_CSV", "READ_PARQUET", "ARROW_SCAN",
               "DELIM_SCAN"}
# Wrapper nodes injected by EXPLAIN ANALYZE that are not real query operators.
_WRAPPER_TYPES = {"EXPLAIN_ANALYZE", "RESULT_COLLECTOR"}


def _norm_table(name, normalize):
    name = str(name).strip().strip('"')
    # keep only the final identifier if fully-qualified (db.schema.table)
    if "." in name:
        name = name.split(".")[-1]
    if normalize:
        name = _SUFFIX_RE.sub("", name)      # lineitem_0 -> lineitem
    return name


def _op_type(node):
    for key in ("operator_type", "operator_name", "name"):
        v = node.get(key)
        if v:
            return str(v).strip()
    return ""


def _op_name(node, normalize):
    """Canonical operator-node name.  Join type is folded into the name for join
    operators (as Picasso folds it into 'Hash Left Join'); an INNER join keeps
    the bare operator name."""
    t = _op_type(node)
    ei = node.get("extra_info", {}) or {}
    jt = ei.get("Join Type")
    if jt and str(jt).strip().upper() not in ("", "INNER"):
        return "%s %s" % (t, str(jt).strip().upper())
    return t


def _table_of(node):
    ei = node.get("extra_info", {}) or {}
    for key in ("Table", "table", "Relation Name", "Text"):
        if key in ei and ei[key]:
            return ei[key]
    return None


# Which numeric JSON field is each node's OUTPUT size.  The bytes fed into a node
# are its child(ren)'s value of this field.  'result_set_size' (output bytes) is
# populated on ~every operator; alternatives: 'operator_cardinality' (output
# rows) or 'operator_rows_scanned' (rows a scan pulls, nonzero only on scans).
COST_METRIC = "result_set_size"


def _node_cost(node):
    try:
        v = node.get(COST_METRIC)
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _build(node, normalize):
    tn = TNode(_op_name(node, normalize))
    tn.cost = _node_cost(node)
    t = _op_type(node)

    # A scan gets its relation-name leaf FIRST (like Picasso's CreateNode), then
    # any real plan children.
    if t in _SCAN_TYPES or _table_of(node) is not None:
        tbl = _table_of(node)
        if tbl is not None:
            leaf = TNode(_norm_table(tbl, normalize))
            leaf.parent = tn
            tn.children.append(leaf)

    for ch in node.get("children", []) or []:
        c = _build(ch, normalize)
        c.parent = tn
        tn.children.append(c)
    return tn


def _find_plan_root(obj):
    """Descend past the EXPLAIN-ANALYZE container/wrapper nodes to the first real
    operator node."""
    node = obj
    if isinstance(node, list):                     # EXPLAIN (FORMAT JSON) array
        node = node[0] if node else {}
    if isinstance(node, dict) and "explain_analyze" in node:
        node = node["explain_analyze"]
    # container with no operator + the EXPLAIN_ANALYZE / RESULT_COLLECTOR wrappers
    guard = 0
    while isinstance(node, dict) and guard < 1000:
        guard += 1
        t = _op_type(node)
        if t == "" or t in _WRAPPER_TYPES:
            kids = node.get("children", []) or []
            if not kids:
                break
            node = kids[0]
            continue
        break
    return node


def parse_duckdb_plan(path_or_obj, normalize=True):
    """Load a DuckDB EXPLAIN ANALYZE JSON (file path or already-parsed object)
    and return its Picasso operator tree (root TNode)."""
    if isinstance(path_or_obj, (str, bytes, os.PathLike)):
        with open(path_or_obj, "r", encoding="utf-8") as f:
            obj = json.load(f)
    else:
        obj = path_or_obj
    root = _find_plan_root(obj)
    if not isinstance(root, dict):
        raise ValueError("could not locate a plan root in %r" % (path_or_obj,))
    return _build(root, normalize)


# ════════════════════════════════════════════════════════════════════════════
#  Folder pairing + CLI
# ════════════════════════════════════════════════════════════════════════════
_QNUM_RE = re.compile(r"(?:^|[^0-9])query[_\-]?(\d+)", re.IGNORECASE)


def _index_folder(folder):
    """Map query number -> plan file path for every query_<n>.json in a folder
    (recurses one level so an inner 'explain_analyze_30/' subdir is found too)."""
    out = {}
    candidates = glob.glob(os.path.join(folder, "**", "*.json"), recursive=True)
    for p in sorted(candidates):
        base = os.path.basename(p)
        m = _QNUM_RE.search(base)
        if not m:
            continue
        q = int(m.group(1))
        # prefer the shallowest path if the same query number appears twice
        if q not in out or p.count(os.sep) < out[q].count(os.sep):
            out[q] = p
    return out


def run_folders(folder_a, folder_b, normalize=True, cost_weighted=True):
    idx_a = _index_folder(folder_a)
    idx_b = _index_folder(folder_b)
    if not idx_a:
        print("  [!] no query_<n>.json plan files found under: %s" % folder_a,
              file=sys.stderr)
    if not idx_b:
        print("  [!] no query_<n>.json plan files found under: %s" % folder_b,
              file=sys.stderr)
    common = sorted(set(idx_a) & set(idx_b))

    only_a = sorted(set(idx_a) - set(idx_b))
    only_b = sorted(set(idx_b) - set(idx_a))

    rows = []
    cost_seen = False
    for q in common:
        try:
            t1 = parse_duckdb_plan(idx_a[q], normalize)
            t2 = parse_duckdb_plan(idx_b[q], normalize)
            if _any_cost(t1) or _any_cost(t2):
                cost_seen = True
            r = compare_trees(t1, t2, cost_weighted)
            rows.append((q, r["rho"], r["matched"], r["matched_struct"],
                         r["size1"], r["size2"], None))
        except Exception as e:                       # keep going on a bad file
            rows.append((q, None, None, None, None, None, str(e)))
    return rows, only_a, only_b, cost_seen


def _print_report(folder_a, folder_b, rows, only_a, only_b, normalize,
                  cost_weighted=True, cost_seen=True):
    print("Plan-tree difference  (Picasso GSPQO rho = 1 - 2M/(|A|+|B|))")
    print("  folder A : %s" % folder_a)
    print("  folder B : %s" % folder_b)
    print("  base-relation normalization (table == table_0) : %s"
          % ("ON" if normalize else "OFF"))
    if cost_weighted:
        print("  cost-weighted M using '%s' fed-in (M += 1 - "
              "|in_A-in_B|/max(in_A,in_B), in = child's %s)"
              % (COST_METRIC, COST_METRIC))
        if not cost_seen:
            print("  [!] '%s' is 0 in every node here, so weighting has no "
                  "effect (rho == structural)." % COST_METRIC)
            print("      try --cost-metric operator_cardinality  (or "
                  "operator_rows_scanned).")
    else:
        print("  cost weighting : OFF (structural node-count M)")
    print()
    print("  %-9s %8s %9s %8s %8s %8s" %
          ("query", "rho", "M", "M_struct", "|A|", "|B|"))
    print("  " + "-" * 56)
    vals = []
    for q, rho, m, m_struct, s1, s2, err in rows:
        if err is not None:
            print("  query_%-3d  ERROR: %s" % (q, err))
            continue
        vals.append(rho)
        flag = "  identical" if rho == 0 else ""
        print("  query_%-3d %8.4f %9.3f %8d %8d %8d%s"
              % (q, rho, m, m_struct, s1, s2, flag))
    print("  " + "-" * 56)
    if vals:
        mean = sum(vals) / len(vals)
        print("  mean rho over %d queries : %.4f   (max %.4f, min %.4f)"
              % (len(vals), mean, max(vals), min(vals)))
    if only_a:
        print("\n  [!] only in A: %s" % ", ".join("query_%d" % q for q in only_a))
    if only_b:
        print("  [!] only in B: %s" % ", ".join("query_%d" % q for q in only_b))


def _write_csv(path, rows):
    import csv
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["query", "rho", "M_weighted", "M_struct",
                    "size_A", "size_B", "error"])
        for q, rho, m, m_struct, s1, s2, err in rows:
            w.writerow(["query_%d" % q,
                        "" if rho is None else "%.6f" % rho,
                        "" if m is None else "%.4f" % m,
                        "" if m_struct is None else m_struct,
                        "" if s1 is None else s1,
                        "" if s2 is None else s2,
                        err or ""])


def main():
    global COST_METRIC
    ap = argparse.ArgumentParser(
        description="Pairwise plan-tree difference (Picasso rho) between two "
                    "folders of DuckDB EXPLAIN ANALYZE JSON plans.")
    ap.add_argument("folder_a", nargs="?", help="first folder of query_<n>.json plans")
    ap.add_argument("folder_b", nargs="?", help="second folder of query_<n>.json plans")
    ap.add_argument("--no-normalize", action="store_true",
                    help="do NOT treat 'table' and 'table_0' as the same relation")
    ap.add_argument("--no-cost", action="store_true",
                    help="disable cost weighting; use plain structural node-count M")
    ap.add_argument("--cost-metric", metavar="FIELD", default=COST_METRIC,
                    help="JSON field for a node's output size; the bytes fed "
                         "into a node are its child(ren)'s value of this field "
                         "(default: %(default)s; e.g. operator_cardinality)")
    ap.add_argument("--csv", metavar="FILE", help="also write per-query scores to CSV")
    ap.add_argument("--show", metavar="N", type=int,
                    help="also print the two annotated plan trees for query N "
                         "([M] marks matched nodes; shows per-node cost)")
    ap.add_argument("--selftest", metavar="FOLDER",
                    help="compare a folder against itself; every rho must be 0")
    args = ap.parse_args()

    normalize = not args.no_normalize
    cost_weighted = not args.no_cost
    COST_METRIC = args.cost_metric

    if args.selftest:
        rows, oa, ob, seen = run_folders(args.selftest, args.selftest,
                                         normalize, cost_weighted)
        _print_report(args.selftest, args.selftest, rows, oa, ob, normalize,
                      cost_weighted, seen)
        bad = [q for q, rho, *_ in rows if rho not in (0.0, None)]
        print("\nSELFTEST:", "PASS (all rho == 0)" if not bad
              else "FAIL for queries %s" % bad)
        return

    if not args.folder_a or not args.folder_b:
        ap.error("provide FOLDER_A and FOLDER_B (or --selftest FOLDER)")

    rows, oa, ob, seen = run_folders(args.folder_a, args.folder_b,
                                     normalize, cost_weighted)
    _print_report(args.folder_a, args.folder_b, rows, oa, ob, normalize,
                  cost_weighted, seen)
    if args.csv:
        _write_csv(args.csv, rows)
        print("\nwrote CSV -> %s" % args.csv)

    if args.show is not None:
        ia, ib = _index_folder(args.folder_a), _index_folder(args.folder_b)
        q = args.show
        if q in ia and q in ib:
            t1 = parse_duckdb_plan(ia[q], normalize)
            t2 = parse_duckdb_plan(ib[q], normalize)
            r = compare_trees(t1, t2, cost_weighted)   # leaves .sim flags set
            print("\nquery_%d   rho=%.4f  M=%.3f  M_struct=%d  |A|=%d  |B|=%d"
                  % (q, r["rho"], r["matched"], r["matched_struct"],
                     r["size1"], r["size2"]))
            print("  (rss = this node's %s = the bytes it feeds to its parent)"
                  % COST_METRIC)
            print("\nfolder A  (%s):" % os.path.basename(ia[q]))
            print("\n".join(ascii_tree(t1, show_cost=True)))
            print("\nfolder B  (%s):" % os.path.basename(ib[q]))
            print("\n".join(ascii_tree(t2, show_cost=True)))
        else:
            print("\n--show: query_%d not present in both folders" % q)


if __name__ == "__main__":
    main()
