spark.sql("""

CREATE OR REPLACE VIEW gizmobox.bronze.v_customers
AS
SELECT *,
       _metadata.file_path AS file_path 
FROM json.`/Volumes/gizmobox/landing/operational_data/customers`
          """)


spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW tv_customers
AS
SELECT *,
_metadata.file_path AS file_path 
FROM json.`/Volumes/gizmobox/landing/operational_data/customers`
          """)