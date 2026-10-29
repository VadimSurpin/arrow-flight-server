-- synthetic
SELECT d.cat, count(*), sum(f.v) FROM fact f JOIN dim d ON f.k = d.k WHERE d.w < 0.01 GROUP BY d.cat ORDER BY d.cat
