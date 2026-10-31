-- ClickBench query 34 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10
