-- ClickBench query 24 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10
