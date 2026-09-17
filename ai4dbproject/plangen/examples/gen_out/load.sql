\copy tbl_a (id,g_n16) FROM '/Users/jaideepkaler/Desktop/ai4dbproject/plangen/examples/gen_out/tbl_a.csv' WITH (FORMAT csv, NULL '')
\copy tbl_b (id,fk_n6,fk_n8,fk_n14,fk_n18,g_n10) FROM '/Users/jaideepkaler/Desktop/ai4dbproject/plangen/examples/gen_out/tbl_b.csv' WITH (FORMAT csv, NULL '')
\copy tbl_c (id,fk_n12,fk_n15) FROM '/Users/jaideepkaler/Desktop/ai4dbproject/plangen/examples/gen_out/tbl_c.csv' WITH (FORMAT csv, NULL '')
