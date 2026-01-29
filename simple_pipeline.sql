
CREATE OR REFRESH STREAMING TABLE test_catalog.example_bronze_schema.orders_bronze_demo2
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(  
    "${source}/orders",  
    format => 'JSON');



CREATE OR REFRESH STREAMING TABLE test_catalog.example_silver_schema.orders_silver_demo2
AS 
SELECT 
  order_id,
  timestamp(order_timestamp) AS order_timestamp, 
  customer_id,
  notifications
FROM STREAM test_catalog.example_bronze_schema.orders_bronze_demo2 ; -- References the streaming orders_bronze table for incrementally processing



-- C. Create the materialized view aggregation from the orders_silver table with the summarization
CREATE OR REFRESH MATERIALIZED VIEW test_catalog.example_gold_schema.gold_orders_by_date_demo2 
AS 
SELECT 
  date(order_timestamp) AS order_date, 
  count(*) AS total_daily_orders
FROM test_catalog.example_silver_schema.orders_silver_demo2  -- Aggregates the full orders_silver streaming table with optimizations where applicable
GROUP BY date(order_timestamp);



