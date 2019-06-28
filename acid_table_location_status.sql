-- Identify ALL Managed tables, both Transactional and non-transactional.

USE ${DB};
WITH MANAGED_LOCATION      AS (
                              SELECT DISTINCT
                                  db_name
                                , tbl_name
                                , regexp_extract(tbl_location, 'hdfs://([^/]+)(.*)', 2) AS path
                                , CASE
                                      WHEN locate('/warehouse/tablespace/managed/hive', tbl_location) +
                                           locate('/apps/hive/warehouse', tbl_location) > 0
                                          THEN 'YES'
                                      ELSE 'NO'
                                  END                                                   AS POTENTIAL_MOVE
                              FROM
                                  hms_dump_${ENV}
                              WHERE
                                  tbl_type = 'MANAGED_TABLE'
                              )
   , MANAGED_TRANSACTIONAL AS (
                              SELECT DISTINCT
                                  db_name
                                , tbl_name
                                , "YES" AS TRANSACTIONAL
                              FROM
                                  hms_dump_${ENV}
                              WHERE
                                    tbl_type = 'MANAGED_TABLE'
                                AND tbl_param_key = 'transactional'
                                AND tbl_param_value = 'true'
                              )
SELECT
    ML.db_name
  , ML.tbl_name
  , ML.path
  , ML.POTENTIAL_MOVE
  , IF(MT.Transactional IS NULL, "NO", MT.Transactional) AS TRANSACTIONAL
FROM
    MANAGED_LOCATION ML LEFT OUTER JOIN MANAGED_TRANSACTIONAL MT
                                        ON ML.db_name = MT.db_name AND ML.tbl_name = MT.tbl_name;


--   AND tbl_param_key = 'transactional'
--   AND tbl_param_value = 'true'
--AND locate('/warehouse/tablespace/managed/hive', tbl_location) + locate('/apps/hive/warehouse', tbl_location) = 0;