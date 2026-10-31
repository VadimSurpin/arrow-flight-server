-- ClickBench query 27 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10
