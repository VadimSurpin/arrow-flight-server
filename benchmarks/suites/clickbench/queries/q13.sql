-- ClickBench query 13 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
