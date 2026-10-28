-- Query templates over the synthetic fact/dim family (gen.py --suite synthetic). {fact} and {dim} are the table locations.
-- name: filter_groupby
SELECT g, count(*), sum(v) FROM fact WHERE m < {selectivity_cut} GROUP BY g ORDER BY g;
-- name: groupby_key
SELECT k, sum(v), count(*) FROM fact GROUP BY k ORDER BY k LIMIT 100;
-- name: join_selective_dim
SELECT d.cat, count(*), sum(f.v) FROM fact f JOIN dim d ON f.k = d.k WHERE d.w < 0.01 GROUP BY d.cat ORDER BY d.cat;
-- name: join_agg_same_key
SELECT d.cat, sum(a.s) FROM (SELECT k, sum(v) s FROM fact GROUP BY k) a JOIN dim d ON a.k = d.k GROUP BY d.cat ORDER BY d.cat;
-- name: topn
SELECT k, v FROM fact ORDER BY v DESC LIMIT 100;
-- name: count_distinct
SELECT count(DISTINCT k) FROM fact WHERE m < {selectivity_cut};
