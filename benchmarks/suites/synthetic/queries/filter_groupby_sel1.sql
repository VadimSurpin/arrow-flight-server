-- synthetic, filter selectivity 1.0%
SELECT g, count(*), sum(v) FROM fact WHERE m < 10000 GROUP BY g ORDER BY g
