SELECT product_id, SUM(quantity) as total_quantity
FROM raw_logistics_regional_stock
GROUP BY product_id;