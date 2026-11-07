-- NYC yellow taxi: tip share by payment type
SELECT payment_type, count(*) AS trips, avg(tip_amount / nullif(fare_amount, 0)) AS tip_share FROM yellow WHERE fare_amount > 0 AND tpep_pickup_datetime >= '2015-01-01' AND tpep_pickup_datetime < '2025-01-01' GROUP BY payment_type ORDER BY payment_type
