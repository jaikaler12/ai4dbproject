SELECT COUNT(*) FROM (SELECT tn1.id FROM tbl_a AS tn1 JOIN tbl_b AS tn2 ON tn2.fk_n18 = tn1.id WHERE (tn1.id >= 62000 AND tn1.id < 67000) AND (tn2.id >= 0 AND tn2.id < 100000)) q;
