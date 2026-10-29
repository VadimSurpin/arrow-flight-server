-- synthetic
SELECT d.cat, sum(a.s) FROM (SELECT k, sum(v) s FROM fact GROUP BY k) a JOIN dim d ON a.k = d.k GROUP BY d.cat ORDER BY d.cat
