SELECT COUNT(*) FROM (SELECT tn2.id FROM tbl_b AS tn2 JOIN tbl_c AS tn3 ON tn3.fk_n12 = tn2.id WHERE (tn2.id >= 0 AND tn2.id < 100000) AND (tn3.id >= 0 AND tn3.id < 200000)) q;
