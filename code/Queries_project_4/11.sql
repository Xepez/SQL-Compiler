SELECT SUM(l_extendedprice * l_discount * l_tax * l_tax), l_suppkey
FROM lineitem
WHERE l_discount < 0.07 AND l_quantity < 12.0
GROUP BY l_suppkey
