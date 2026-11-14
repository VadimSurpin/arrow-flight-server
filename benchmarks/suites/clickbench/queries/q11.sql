-- ClickBench query 11 (Spark dialect, ClickHouse/ClickBench spark/queries.sql)
SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10
