-- ClickBench query 9 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10
