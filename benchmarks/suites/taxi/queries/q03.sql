-- NYC yellow taxi: busiest pickup zones
SELECT PULocationID, count(*) AS trips FROM yellow WHERE tpep_pickup_datetime >= '2015-01-01' AND tpep_pickup_datetime < '2025-01-01' GROUP BY PULocationID ORDER BY trips DESC, PULocationID LIMIT 20
