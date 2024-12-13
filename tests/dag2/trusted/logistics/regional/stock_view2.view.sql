SELECT region, SUM(quantity) as total_quantity
FROM raw_logistics_regional_stock
GROUP BY region;