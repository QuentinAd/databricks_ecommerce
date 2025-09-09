spark.sql("""
CREATE OR REPLACE VIEW gizmobox.bronze.v_orders
AS
SELECT * FROM text.`/Volumes/gizmobox/landing/operational_data/orders`;
""")
