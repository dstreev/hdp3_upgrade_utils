--     Produce a list of tables and directory locations that need to be checked for ownership.
--
--     If 'hive' is the owner, then these 'managed' table will be 'migrated' to the new warehouse location:
--     `/warehouse/tablespace/managed/hive`
--
--         Variables:
--         DB - The database you placed the hms dump table.
--         ENV - IE: dev,qa,prod.  Used to support multiple
--                 environment dump files in the same database.

USE ${DB};
WITH migrations AS (
                   SELECT
                       db_name
                     , tbl_name
                     , tbl_type
                     , tbl_serde_slib
                     , regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2) AS hdfs_path
                     ,
                       -- Look for Manage table that are NOT 'transaction' AND are ORC format AND are MANAGED.
                       CASE
                           WHEN tbl_type = "MANAGED_TABLE"
                               -- If the base directory is the warehouse, then it may be migrated if owned by 'hive'.
                               AND
                                instr(regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2), '/apps/hive/warehouse') =
                                1 AND tbl_serde_slib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
                               THEN "ACIDv2/Migrate"
                           WHEN tbl_type = "MANAGED_TABLE"
                               -- If the base directory is the warehouse, then it may be migrated if owned by 'hive'.
                               AND
                                instr(regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2), '/apps/hive/warehouse') =
                                1 AND tbl_serde_slib != "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
                               THEN "ACIDv2(native/append)/Migrate"
                           ELSE "NO"
                       END                                                   AS CONVERSION_POSSIBLE
                   FROM
                       hms_dump_${ENV}
                   WHERE
                         db_name != "information_schema"
                     AND db_name != "sys"
                     AND tbl_name IS NOT NULL
                   GROUP BY db_name, tbl_name, tbl_type, tbl_location, tbl_serde_slib
                   ORDER BY db_name, tbl_name
                   )
SELECT
    db_name
  , tbl_name
  , tbl_type
  , tbl_serde_slib
  , CONVERSION_POSSIBLE
  , hdfs_path
FROM
    migrations
WHERE
    CONVERSION_POSSIBLE != 'NO';
