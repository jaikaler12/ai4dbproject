SELECT COUNT(*) FROM (SELECT tn1.id FROM tbl_a AS tn1 JOIN tbl_b AS tn2 ON tn2.fk_n8 = tn1.id WHERE (tn1.id >= 30000 AND tn1.id < 50000) AND (tn2.id >= 0 AND tn2.id < 100000)) q;
