-- ClickBench query 18 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10
