-- ClickBench query 15 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10
