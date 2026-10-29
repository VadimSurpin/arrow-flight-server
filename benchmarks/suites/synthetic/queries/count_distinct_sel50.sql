-- synthetic, filter selectivity 50.0%
SELECT count(DISTINCT k) FROM fact WHERE m < 500000
