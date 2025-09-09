spark.sql("""
CREATE OR REPLACE VIEW gizmobox.bronze.v_memberships
AS
SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;
          """)
