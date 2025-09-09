spark.sql("""
CREATE OR REPLACE VIEW gizmobox.bronze.v_addresses
AS
SELECT *
FROM read_files('/Volumes/gizmobox/landing/operational_data/addresses/addresses_*.tsv',
                format => 'csv',
                delimiter => '\t',
                header => true);
""")
