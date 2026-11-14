-- ClickBench query 17 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
