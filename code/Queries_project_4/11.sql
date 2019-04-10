SELECT SUM(ps_supplycost * ps_suppkey), ps_suppkey
FROM partsupp 
WHERE ps_suppkey > 2 AND ps_suppkey < 100
GROUP BY ps_suppkey
