SELECT SUM(ps_supplycost), ps_suppkey
FROM partsupp 
WHERE ps_suppkey > 0