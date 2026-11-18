-- NYC yellow taxi: distance distribution, selective filter
SELECT floor(trip_distance) AS miles, count(*) AS trips, avg(total_amount) AS avg_total FROM yellow WHERE trip_distance BETWEEN 0 AND 50 AND tpep_pickup_datetime >= '2019-01-01' AND tpep_pickup_datetime < '2020-01-01' GROUP BY 1 ORDER BY 1
