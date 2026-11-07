-- synthetic, filter selectivity 1.0%
SELECT count(DISTINCT k) FROM fact WHERE m < 10000
