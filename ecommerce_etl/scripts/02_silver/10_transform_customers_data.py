spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW v_customers_distinct
AS
SELECT
DISTINCT *
FROM gizmobox.bronze.v_customers
WHERE customer_id IS NOT NULL
ORDER BY customer_id;
""")


spark.sql("""
CREATE TABLE gizmobox.silver.customers
AS
WITH cte_customers_max_distinct (
  SELECT
  customer_id,
  MAX(created_timestamp) as max_created_timestamp
  FROM v_customers_distinct
  GROUP BY customer_id
)
SELECT
CAST(t.created_timestamp AS TIMESTAMP),
t.customer_id,
t.customer_name,
CAST(t.date_of_birth AS DATE),
t.email,
CAST(t.member_since AS DATE),
t.telephone,
t.file_path
FROM v_customers_distinct as t
JOIN cte_customers_max_distinct as cte
ON t.customer_id = cte.customer_id
AND t.created_timestamp = cte.max_created_timestamp
ORDER BY customer_id;
""")
