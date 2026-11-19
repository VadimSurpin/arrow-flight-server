-- ClickBench query 26 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10
