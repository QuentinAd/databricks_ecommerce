password = dbutils.secrets.get(scope='dea_scope', key='pg_password')
username = dbutils.secrets.get(scope='dea_scope', key='pg_user')


# Escape single quotes for safe SQL embedding
def _sql_quote(val: str) -> str:
    return val.replace("'", "''")


spark.sql("""
DROP CATALOG IF EXISTS rds_pg;
          """)

spark.sql("""
DROP CONNECTION IF EXISTS rds_pg_conn;
          """)

spark.sql(f"""
CREATE CONNECTION rds_pg_conn TYPE postgresql
OPTIONS (
  host 'database-1.c7qyscmoupbs.ca-central-1.rds.amazonaws.com',
  port '5432',
  user '{_sql_quote(username)}',
  password '{_sql_quote(password)}'
  )
  """)

spark.sql("""
CREATE FOREIGN CATALOG rds_pg
USING CONNECTION rds_pg_conn
OPTIONS (database 'postgres');
""")

spark.sql("""
CREATE OR REPLACE VIEW gizmobox.bronze.v_refunds
AS
SELECT * FROM rds_pg.public.refunds;
""")

spark.sql("""
CREATE OR REPLACE TABLE gizmobox.bronze.refunds
AS
SELECT * FROM rds_pg.public.refunds;
""")

spark.sql("""
INSERT OVERWRITE gizmobox.bronze.refunds
SELECT * FROM rds_pg.public.refunds;
""")
