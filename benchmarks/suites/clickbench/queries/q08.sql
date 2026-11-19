-- ClickBench query 8 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC
