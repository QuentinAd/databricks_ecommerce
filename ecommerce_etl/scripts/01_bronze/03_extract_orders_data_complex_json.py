import sys

# Set parameter values
catalog = sys.argv[1]

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.bronze.v_orders
AS
SELECT * FROM text.`/Volumes/gizmobox/landing/operational_data/orders`;
""")
