-- ClickBench query 16 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10
