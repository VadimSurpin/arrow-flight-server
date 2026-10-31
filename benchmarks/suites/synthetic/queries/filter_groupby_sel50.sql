-- synthetic, filter selectivity 50.0%
SELECT g, count(*), sum(v) FROM fact WHERE m < 500000 GROUP BY g ORDER BY g
