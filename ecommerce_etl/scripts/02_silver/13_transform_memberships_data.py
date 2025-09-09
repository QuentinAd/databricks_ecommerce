spark.sql("""
CREATE TABLE gizmobox.silver.memberships
AS
SELECT
regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
content as membership_card
FROM gizmobox.bronze.v_memberships;
          """)
