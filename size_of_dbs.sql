USE ${DB};

WITH HMS AS (
            SELECT DISTINCT
                db_name
              , tbl_name
              , regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2) AS tbl_location
            FROM
                hms_dump_${ENV}
            )
SELECT
    hms.db_name
  , count(DISTINCT tbl_name) tbl_count
  , SUM(num_of_folders) AS fldr_count
  , SUM(num_of_files)   AS file_count
  , SUM(size)           AS total_size
FROM
    HMS hms LEFT JOIN dir_size_${ENV} ds
                      ON hms.tbl_location = ds.directory
WHERE
      hms.db_name != 'sys'
  AND hms.db_name != 'information_schema'
GROUP BY hms.db_name
ORDER BY hms.db_name;
