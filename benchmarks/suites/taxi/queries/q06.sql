-- NYC yellow taxi: top-N most expensive trips
SELECT tpep_pickup_datetime, PULocationID, DOLocationID, total_amount FROM yellow WHERE tpep_pickup_datetime >= '2021-01-01' AND tpep_pickup_datetime < '2022-01-01' AND total_amount < 10000 ORDER BY total_amount DESC, tpep_pickup_datetime LIMIT 100
