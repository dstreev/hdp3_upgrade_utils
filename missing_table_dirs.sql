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


use ${DB};

SELECT hdfs_path_check
FROM (
         SELECT db_name,
                tbl_name,
                tbl_type,
                null                                                                          AS part_name,
                concat('test ', '-e ', regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2)) AS hdfs_path_check,
                count(1)
         from hms_dump_${ENV}
         where part_name is null
           and tbl_type != 'VIRTUAL_VIEW'
         group by db_name, tbl_name, tbl_type, part_name,
                  regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2)
         union all
         select db_name,
                tbl_name,
                tbl_type,
                part_name,
                concat('test ', '-e ', regexp_extract(part_location, 'hdfs://([^/]+)(.*)', 2)) AS hdfs_path_path_check,
                count(1)
         from hms_dump_${ENV}
         where part_name is not null
           and tbl_type != 'VIRTUAL_VIEW'
         group by db_name, tbl_name, tbl_type, part_name,
                  regexp_extract(part_location, 'hdfs://([^/]+)(.*)', 2)
     ) sub;
