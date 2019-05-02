SELECT SUM(c_acctbal), c_name 
FROM orders, customer
WHERE o_custkey = c_custkey
GROUP BY c_name
