-- ClickBench query 3 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits
