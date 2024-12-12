-- SQL script for creating the monthly_sales table
-- This table depends on the financial_trusted_sales view
SELECT
    date_trunc('month', sale_date) AS month,
    SUM(amount) AS total_sales
FROM
    `{{project_id}}.{{dataset}}.financial_trusted_sales`
GROUP BY
    month;