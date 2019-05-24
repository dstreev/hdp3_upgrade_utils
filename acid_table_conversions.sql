--     Find 'Managed' Tables that COULD be converted to ACID.
--
--     COULD... means that the tables need to be owned by 'hive'. So additional checks are
--     required via HDFS to determine the outcome.
--
--     The 'CONVERSION_POSSIBLE' column will have a 'Migrate' entry that indicates possible movement
--     IF OWNED BY HIVE AND IN A STANDARD LOCATION.
--
--     If the tables are NOT owned by 'hive', they would be convert to 'EXTERNAL' tables
--     and have 'external.table.purge=true' added to the tables properties to ensure a consistent
--     behavior with legacy 'managed' tables.
--
--     If the tables are in the standard base directory, and will be converted, they will ALSO be
--     MOVED to the 'new' warehouse directory.
--
--     If a table is actually being accessed by Spark-Sql and 'may' be migrated, you should make adjustments
--     to the table to ensure that Spark works the way you expect it to, post upgrade.
--
--         Variables:
--         DB - The database you placed the hms dump table.
--         ENV - IE: dev,qa,prod.  Used to support multiple
--                 environment dump files in the same database.

USE ${DB};

SELECT
    db_name
  , tbl_name
  , tbl_type
  , tbl_serde_slib
  , regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2) AS hdfs_path
  ,
    -- Look for Manage table that are NOT 'transaction' AND are ORC format AND are MANAGED.
    CASE
        WHEN !array_contains(collect_set(concat_ws(":", tbl_param_key, tbl_param_value)), "transactional:true") AND
             tbl_type = "MANAGED_TABLE"
            -- If the base directory is the warehouse, then it may be migrated if owned by 'hive'.
            AND instr(regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2), '/apps/hive/warehouse') = 1 AND
             tbl_serde_slib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
            THEN "ACIDv2/Migrate from Non-ACID"
        WHEN !array_contains(collect_set(concat_ws(":", tbl_param_key, tbl_param_value)), "transactional:true") AND
             tbl_type = "MANAGED_TABLE" AND tbl_serde_slib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
            THEN "ACIDv2 from Non-ACID"
        WHEN !array_contains(collect_set(concat_ws(":", tbl_param_key, tbl_param_value)), "transactional:true") AND
             tbl_type = "MANAGED_TABLE"
            -- If the base directory is the warehouse, then it may be migrated if owned by 'hive'.
            AND instr(regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2), '/apps/hive/warehouse') = 1 AND
             tbl_serde_slib != "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
            THEN "ACIDv2(append)/Migrate from Non-ACID"
        WHEN !array_contains(collect_set(concat_ws(":", tbl_param_key, tbl_param_value)), "transactional:true") AND
             tbl_type = "MANAGED_TABLE" AND tbl_serde_slib != "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
            THEN "ACIDv2(append) from Non-ACID"
        WHEN array_contains(collect_set(concat_ws(":", tbl_param_key, tbl_param_value)), "transactional:true") AND
             tbl_type = "MANAGED_TABLE" -- If the base directory is the warehouse, then it may be migrated if owned by 'hive'.
            --AND instr(regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)',2),'/apps/hive/warehouse') = 1
            AND tbl_serde_slib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
            THEN "ACIDv2 from ACIDv1"
        ELSE "NO"
    END                                                   AS CONVERSION_POSSIBLE
FROM
    hms_dump_${ENV}
WHERE
      db_name != "information_schema"
  AND tbl_type != 'VIRTUAL_VIEW'
  AND tbl_type != 'EXTERNAL_TABLE'
  AND db_name != "sys"
  AND tbl_name IS NOT NULL
GROUP BY db_name, tbl_name, tbl_type, tbl_location, tbl_serde_slib
ORDER BY db_name, tbl_name;
