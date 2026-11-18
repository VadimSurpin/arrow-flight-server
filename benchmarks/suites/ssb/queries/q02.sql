-- SSB-shaped flight 1.2: star schema built from TPC-H data by gen.py (value domains differ from the SSB spec)
SELECT sum(lo_extendedprice*lo_discount) AS revenue FROM lineorder JOIN dates ON lo_orderdate=d_datekey WHERE d_yearmonth='199401' AND lo_discount BETWEEN 0.04 AND 0.06 AND lo_quantity BETWEEN 26 AND 35
