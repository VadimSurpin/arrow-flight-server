-- synthetic
SELECT k, sum(v), count(*) FROM fact GROUP BY k ORDER BY k LIMIT 100
