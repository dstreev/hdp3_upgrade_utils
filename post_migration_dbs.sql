-- Run against ALL Managed Tables.

USE ${DB};

WITH post_migration AS (
                       SELECT DISTINCT
                           db_name
                         , tbl_name
                         , tbl_type
                         , part_name
                       FROM
                           hms_dump_${ENV}
                       WHERE
                             db_name != "information_schema"
                         AND tbl_type = "MANAGED_TABLE"
                         AND db_name != "sys"
                         AND tbl_name IS NOT NULL
                       )
SELECT
    db_name
  , count(tbl_name) AS num_of_managed_table_parts
FROM
    post_migration
GROUP BY db_name
ORDER BY db_name;
