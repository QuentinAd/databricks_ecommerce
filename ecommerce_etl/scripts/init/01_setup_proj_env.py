
spark.sql("""
    CREATE EXTERNAL LOCATION IF NOT EXISTS dea_course_ext_dl_gizmobox
    URL 's3://databricks-dea-demo/gizmobox/'
    WITH (STORAGE CREDENTIAL `db_s3_credentials_databricks-s3-ingest-f4c94`)
    COMMENT 'External Location For the Gizmobox Data Lakehouse'
""")

spark.sql("""
        CREATE CATALOG IF NOT EXISTS gizmobox
            MANAGED LOCATION 's3://databricks-dea-demo/gizmobox'
            COMMENT 'This is the catalog for the Gizmobox Data Lakehouse';
""")

spark.sql("""
USE CATALOG gizmobox;
          """)

spark.sql("""
CREATE SCHEMA IF NOT EXISTS landing
     MANAGED LOCATION 's3://databricks-dea-demo/gizmobox/landing'; 
CREATE SCHEMA IF NOT EXISTS bronze
     MANAGED LOCATION 's3://databricks-dea-demo/gizmobox/bronze';
CREATE SCHEMA IF NOT EXISTS silver
     MANAGED LOCATION 's3://databricks-dea-demo/gizmobox/silver';
CREATE SCHEMA IF NOT EXISTS gold
     MANAGED LOCATION 's3://databricks-dea-demo/gizmobox/gold';
""")


spark.sql("""
USE CATALOG gizmobox;
USE SCHEMA landing;

CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
    LOCATION 's3://databricks-dea-demo/gizmobox/landing/operational_data';
""")