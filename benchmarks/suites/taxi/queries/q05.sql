-- NYC yellow taxi: hour of day by passenger count
SELECT hour(tpep_pickup_datetime) AS h, count(*) AS trips, avg(trip_distance) AS avg_miles FROM yellow WHERE tpep_pickup_datetime >= '2022-01-01' AND tpep_pickup_datetime < '2023-01-01' GROUP BY 1 ORDER BY 1
