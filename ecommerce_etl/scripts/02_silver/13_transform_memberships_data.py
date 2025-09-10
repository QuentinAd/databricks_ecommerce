import sys

# Set parameter values
catalog = sys.argv[1]

spark.sql(f"""
CREATE TABLE {catalog}.silver.memberships
AS
SELECT
regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
content as membership_card
FROM {catalog}.bronze.v_memberships;
          """)
