import sys

# Set parameter values
catalog = sys.argv[1]

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.bronze.v_memberships
AS
SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;
          """)
