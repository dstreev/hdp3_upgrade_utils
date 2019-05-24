--     Hive Migration Scripts will fail against tables without supporting
--     filesystem directories.
--
--     List all distinct Tbl and Partition Directories
--     Used the resulting 'hdfs_path' items to validate existence.
--
--     With this list, find the missing directories and do
--         one or the following:
--
--     1. Create the missing directory (Hive needs RWX permissions on the directory)
--     2. Remove the Table Schema
--
--
--         Variables:
--         DB - The database you placed the hms dump table.
--         ENV - IE: dev,qa,prod.  Used to support multiple
--                 environment dump files in the same database.


USE ${DB};
WITH TBL_LOCATIONS  AS (
                       SELECT
                           db_name
                         , tbl_name
                         , tbl_type
                         , NULL                                                                          AS part_name
                         , concat('test ', '-e ',
                                  regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2))                 AS hdfs_path_check
                         , count(1)
                       FROM
                           hms_dump_${ENV}
                       WHERE
                             part_name IS NULL
                         AND tbl_type != 'VIRTUAL_VIEW'
                       GROUP BY db_name, tbl_name, tbl_type, part_name
                                       , regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2)
                       )
   , PART_LOCATIONS AS (
                       SELECT
                           db_name
                         , tbl_name
                         , tbl_type
                         , part_name
                         , concat('test ', '-e ',
                                  regexp_extract(part_location, 'hdfs://([^/]+)(.*)', 2)) AS hdfs_path_check
                         , count(1)
                       FROM
                           hms_dump_${ENV}
                       WHERE
                             part_name IS NOT NULL
                         AND tbl_type != 'VIRTUAL_VIEW'
                       GROUP BY db_name, tbl_name, tbl_type, part_name
                                       , regexp_extract(part_location, 'hdfs://([^/]+)(.*)', 2)
                       )

SELECT
    hdfs_path_check
FROM
    TBL_LOCATIONS
UNION ALL
SELECT
    hdfs_path_check
FROM
    PART_LOCATIONS;
