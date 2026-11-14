-- ClickBench query 25 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10
