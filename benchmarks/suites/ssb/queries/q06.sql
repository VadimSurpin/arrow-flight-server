-- SSB-shaped flight 2.3: star schema built from TPC-H data by gen.py (value domains differ from the SSB spec)
SELECT sum(lo_revenue) AS revenue, d_year, p_brand FROM lineorder JOIN dates ON lo_orderdate=d_datekey JOIN part ON lo_partkey=p_partkey JOIN supplier ON lo_suppkey=s_suppkey WHERE p_brand='Brand#23' AND s_region='EUROPE' GROUP BY d_year, p_brand ORDER BY d_year, p_brand
