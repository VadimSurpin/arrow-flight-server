-- SSB-shaped flight 1.1: star schema built from TPC-H data by gen.py (value domains differ from the SSB spec)
SELECT sum(lo_extendedprice*lo_discount) AS revenue FROM lineorder JOIN dates ON lo_orderdate=d_datekey WHERE d_year=1993 AND lo_discount BETWEEN 0.01 AND 0.03 AND lo_quantity<25
