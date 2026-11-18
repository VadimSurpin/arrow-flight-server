-- ClickBench query 33 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10
