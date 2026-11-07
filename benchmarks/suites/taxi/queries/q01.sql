-- NYC yellow taxi: trips and revenue per month
SELECT date_trunc('month', tpep_pickup_datetime) AS m, count(*) AS trips, sum(total_amount) AS revenue FROM yellow WHERE tpep_pickup_datetime >= '2015-01-01' AND tpep_pickup_datetime < '2025-01-01' GROUP BY 1 ORDER BY 1
