spark.sql("""
CREATE TABLE gizmobox.silver.addresses
AS
SELECT *
FROM (
  SELECT
  customer_id,
  address_type,
  address_line_1,
  city,
  state,
  postcode
  FROM gizmobox.bronze.v_addresses
)
PIVOT (MAX(address_line_1) AS address_line_1,
       MAX(city) AS city,
       MAX(state) AS state,
       MAX(postcode) AS postcode
       FOR address_type IN ('shipping', 'billing')
       );
""")
