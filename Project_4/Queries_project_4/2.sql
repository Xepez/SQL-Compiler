SELECT SUM(c_acctbal), c_name 
FROM customer
WHERE c_custkey > 0
GROUP BY c_name

