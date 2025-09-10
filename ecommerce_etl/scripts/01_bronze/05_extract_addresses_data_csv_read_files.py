import sys

# Set parameter values
catalog = sys.argv[1]

spark.sql("""
CREATE OR REPLACE VIEW {catalog}.bronze.v_addresses
AS
SELECT *
FROM read_files('/Volumes/gizmobox/landing/operational_data/addresses/addresses_*.tsv',
                format => 'csv',
                delimiter => '\t',
                header => true);
""")
