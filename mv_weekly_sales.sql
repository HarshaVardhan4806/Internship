# Materialized view

CREATE MATERIALIZED VIEW weekly_sales_mv
AS
SELECT 
    product_id, 
    region, 
    date_trunc('week', order_date) AS week_start,
    SUM(qty) AS total_qty, 
    SUM(total_sales) AS total_sales
FROM fact_sales
GROUP BY product_id, region, date_trunc('week', order_date);

-- Refresh: Nightly or incremental
REFRESH MATERIALIZED VIEW weekly_sales_mv;